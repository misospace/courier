package observability_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"sigs.k8s.io/yaml"

	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/source"
)

type alertRule struct {
	Alert       string            `json:"alert"`
	Expr        string            `json:"expr"`
	For         string            `json:"for"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

type ruleGroup struct {
	Name     string      `json:"name"`
	Interval string      `json:"interval"`
	Rules    []alertRule `json:"rules"`
}

type ruleFile struct {
	Groups []ruleGroup `json:"groups"`
}

var expectedAlerts = []struct {
	name     string
	severity string
}{
	{"CourierSourceDiscoveryFailing", "warning"},
	{"CourierLaneSaturatedQueueWaitRising", "info"},
	{"CourierLaneReservationWedge", "warning"},
	{"CourierRunFailureRateElevated", "warning"},
	{"CourierLifecycleReportsStuck", "warning"},
	{"CourierTerminalDiagnosticConcentration", "warning"},
	{"CourierMetricsScrapeErrors", "warning"},
	{"CourierControllerDown", "critical"},
}

var allowedExprLabels = map[string]struct{}{
	"source": {}, "lane": {}, "mode": {}, "phase": {}, "from": {}, "to": {},
	"category": {}, "result": {}, "kind": {}, "reason": {}, "job": {}, "le": {},
}

func loadRuleFile(t *testing.T) ruleFile {
	t.Helper()
	data, err := os.ReadFile("rules/courier-alerts.yml")
	if err != nil {
		t.Fatalf("read rule file: %v", err)
	}
	var rf ruleFile
	if err := yaml.Unmarshal(data, &rf); err != nil {
		t.Fatalf("parse rule file: %v", err)
	}
	return rf
}

func alertsByName(t *testing.T) map[string]alertRule {
	t.Helper()
	rf := loadRuleFile(t)
	byName := make(map[string]alertRule, len(rf.Groups[0].Rules))
	for _, r := range rf.Groups[0].Rules {
		if _, dup := byName[r.Alert]; dup {
			t.Fatalf("duplicate alert %q", r.Alert)
		}
		byName[r.Alert] = r
	}
	return byName
}

func exprFor(t *testing.T, name string) string {
	t.Helper()
	r, ok := alertsByName(t)[name]
	if !ok {
		t.Fatalf("alert %q missing from rule file", name)
	}
	return r.Expr
}

func TestRuleFileStructure(t *testing.T) {
	rf := loadRuleFile(t)
	if len(rf.Groups) != 1 {
		t.Fatalf("want exactly one group, got %d", len(rf.Groups))
	}
	g := rf.Groups[0]
	if g.Name != "courier" {
		t.Fatalf("want group name courier, got %q", g.Name)
	}
	if g.Interval != "30s" {
		t.Fatalf("want group interval 30s, got %q", g.Interval)
	}
	byName := make(map[string]alertRule, len(g.Rules))
	for _, r := range g.Rules {
		byName[r.Alert] = r
	}
	if len(g.Rules) != len(expectedAlerts) {
		t.Fatalf("want %d rules, got %d", len(expectedAlerts), len(g.Rules))
	}
	for _, want := range expectedAlerts {
		r, ok := byName[want.name]
		if !ok {
			t.Fatalf("alert %q missing", want.name)
		}
		if strings.TrimSpace(r.Expr) == "" {
			t.Errorf("%s: empty expr", want.name)
		}
		if strings.TrimSpace(r.For) == "" {
			t.Errorf("%s: missing for duration", want.name)
		}
		if r.Labels["severity"] != want.severity {
			t.Errorf("%s: want severity %q, got %q", want.name, want.severity, r.Labels["severity"])
		}
		for _, ann := range []string{"summary", "description"} {
			if strings.TrimSpace(r.Annotations[ann]) == "" {
				t.Errorf("%s: missing %q annotation", want.name, ann)
			}
		}
	}
}

var exprParser = parser.NewParser(parser.Options{})

func walkExprs(t *testing.T) (map[string]struct{}, map[string]struct{}) {
	t.Helper()
	labels := map[string]struct{}{}
	metrics := map[string]struct{}{}
	for _, want := range expectedAlerts {
		expr := exprFor(t, want.name)
		node, err := exprParser.ParseExpr(expr)
		if err != nil {
			t.Fatalf("%s: expr does not parse: %v\n%s", want.name, err, expr)
		}
		collect(node, labels, metrics)
	}
	return labels, metrics
}

func collect(node parser.Node, labels, metrics map[string]struct{}) {
	switch n := node.(type) {
	case *parser.VectorSelector:
		for _, m := range n.LabelMatchers {
			if m.Name != "__name__" {
				labels[m.Name] = struct{}{}
			}
		}
		if n.Name != "" {
			metrics[n.Name] = struct{}{}
		}
	case *parser.AggregateExpr:
		for _, l := range n.Grouping {
			labels[l] = struct{}{}
		}
	case *parser.BinaryExpr:
		if n.VectorMatching != nil {
			for _, l := range n.VectorMatching.MatchingLabels {
				labels[l] = struct{}{}
			}
			for _, l := range n.VectorMatching.Include {
				labels[l] = struct{}{}
			}
		}
	}
	for _, child := range parser.Children(node) {
		collect(child, labels, metrics)
	}
}

func TestRuleExpressionsUseBoundedLabels(t *testing.T) {
	labels, _ := walkExprs(t)
	for l := range labels {
		if _, ok := allowedExprLabels[l]; !ok {
			t.Errorf("expr uses unbounded label %q", l)
		}
	}
}

var fqNameRe = regexp.MustCompile(`fqName: "([^"]+)"`)

func describedNames(c prometheus.Collector) []string {
	ch := make(chan *prometheus.Desc)
	go func() {
		defer close(ch)
		c.Describe(ch)
	}()
	var names []string
	for d := range ch {
		m := fqNameRe.FindStringSubmatch(d.String())
		if m != nil {
			names = append(names, m[1])
		}
	}
	return names
}

func knownMetricNames(t *testing.T) map[string]struct{} {
	t.Helper()
	reg := prometheus.NewRegistry()
	rec := controller.NewRunRecorder(reg)
	dm := source.NewDiscoveryMetrics(reg)
	rec.RunDuration.WithLabelValues("l", "m", "Done").Observe(1)
	rec.QueueWait.WithLabelValues("l", "m", "Done").Observe(1)
	rec.Total.WithLabelValues("l", "m", "Done").Inc()
	rec.ReapRequests.WithLabelValues("Done").Inc()
	rec.PhaseTransitions.WithLabelValues("l", "m", "Pending", "Claimed").Inc()
	rec.PhaseDuration.WithLabelValues("l", "m", "Done").Observe(1)
	rec.TerminalDiagnostics.WithLabelValues("l", "m", "unknown").Inc()
	rec.InfraRestarts.WithLabelValues("l", "m").Inc()
	rec.VerificationOutcomes.WithLabelValues("l", "m", "green").Inc()
	rec.Tokens.WithLabelValues("l", "m", "input").Inc()
	rec.LifecycleReportOutcomes.WithLabelValues("s", "l", "m", "published").Inc()
	dm.IncAttempts("s", "l")
	dm.IncSuccesses("s", "l")
	dm.IncFailures("s", "l")
	dm.AddOffered("s", "l", 1)
	dm.IncSkipped("s", "l", source.DiscoverySkipReasons[0])
	dm.IncMaterialized("s", "l")
	dm.SetLastAttempt("s", "l", 1)
	dm.SetLastSuccess("s", "l", 1)
	dm.SetPaused("s", "l", source.DiscoveryPauseReasons[0])
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather probe registry: %v", err)
	}
	known := map[string]struct{}{}
	for _, f := range families {
		known[f.GetName()] = struct{}{}
		if f.GetType() == dto.MetricType_HISTOGRAM {
			known[f.GetName()+"_bucket"] = struct{}{}
			known[f.GetName()+"_sum"] = struct{}{}
			known[f.GetName()+"_count"] = struct{}{}
		}
	}
	for _, c := range []prometheus.Collector{
		controller.NewLaneCollector(nil),
		controller.NewTerminalAgeCollector(nil),
		controller.NewTerminalBacklogCollector(nil),
	} {
		for _, name := range describedNames(c) {
			known[name] = struct{}{}
		}
	}
	known["up"] = struct{}{}
	return known
}

func TestRuleExprMetricsAreRegistered(t *testing.T) {
	_, metrics := walkExprs(t)
	known := knownMetricNames(t)
	for m := range metrics {
		if _, ok := known[m]; !ok {
			t.Errorf("expr references unknown metric %q", m)
		}
	}
}

var testOrigin = time.Unix(0, 0).UTC()

func loadStorage(t *testing.T, script string) storage.Queryable {
	t.Helper()
	return promqltest.LoadedStorage(t, script)
}

func evalAt(t *testing.T, st storage.Queryable, expr string, at time.Duration) promql.Vector {
	t.Helper()
	engine := promqltest.NewTestEngine(t, false, 0, promqltest.DefaultMaxSamplesPerQuery)
	q, err := engine.NewInstantQuery(context.Background(), st, nil, expr, testOrigin.Add(at))
	if err != nil {
		t.Fatalf("build instant query: %v", err)
	}
	defer q.Close()
	res := q.Exec(context.Background())
	if res.Err != nil {
		t.Fatalf("eval %q at %s: %v", expr, at, res.Err)
	}
	vec, err := res.Vector()
	if err != nil {
		t.Fatalf("eval %q at %s: %v", expr, at, err)
	}
	return vec
}

func sampleKeys(v promql.Vector) []string {
	keys := make([]string, 0, len(v))
	for _, s := range v {
		keys = append(keys, fmt.Sprintf("%s/%s/%s/%s",
			s.Metric.Get("source"), s.Metric.Get("lane"),
			s.Metric.Get("mode"), s.Metric.Get("phase")))
	}
	sort.Strings(keys)
	return keys
}

func assertFiring(t *testing.T, v promql.Vector, want []string) {
	t.Helper()
	got := sampleKeys(v)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("firing set mismatch\nwant %v\ngot  %v", want, got)
	}
}

func assertSilent(t *testing.T, v promql.Vector) {
	t.Helper()
	if len(v) != 0 {
		t.Fatalf("expected no firing samples, got %v", sampleKeys(v))
	}
}

func stalePrefix(n int) string { return strings.Repeat("stale ", n) }

const baseLane = `  courier_lane_reservations{lane="main"} 0x44
  courier_lane_concurrency{lane="main"} 2x44
  courier_lane_suspended{lane="main"} 0x44
`

func TestAlertScenarios(t *testing.T) {
	t.Run("A1 healthy idle stays silent", func(t *testing.T) {
		script := `load 30s
  courier_source_discovery_failures_total{source="src",lane="main"} 0x44
  courier_source_last_success_timestamp_seconds{source="src",lane="main"} 30+30x44
  courier_source_discovery_paused{source="src",lane="main",reason="lane-suspended"} 0x44
` + baseLane
		st := loadStorage(t, script)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierSourceDiscoveryFailing"), 600*time.Second))
	})

	t.Run("A1 discovery outage fires and recovers after a success", func(t *testing.T) {
		script := `load 30s
  courier_source_discovery_failures_total{source="src",lane="main"} 0+1x44
  courier_source_last_success_timestamp_seconds{source="src",lane="main"} ` + stalePrefix(21) + `630+30x23
  courier_source_discovery_paused{source="src",lane="main",reason="lane-suspended"} 0x44
` + baseLane
		st := loadStorage(t, script)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierSourceDiscoveryFailing"), 300*time.Second),
			[]string{"src/main//"})
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierSourceDiscoveryFailing"), 720*time.Second))
	})

	t.Run("A1 transient blip stays silent", func(t *testing.T) {
		script := `load 30s
  courier_source_discovery_failures_total{source="src",lane="main"} 0x9 1x35
  courier_source_last_success_timestamp_seconds{source="src",lane="main"} 30+30x44
  courier_source_discovery_paused{source="src",lane="main",reason="lane-suspended"} 0x44
` + baseLane
		st := loadStorage(t, script)
		for _, at := range []time.Duration{300 * time.Second, 330 * time.Second, 390 * time.Second} {
			assertSilent(t, evalAt(t, st, exprFor(t, "CourierSourceDiscoveryFailing"), at))
		}
	})

	t.Run("A1 suspended source stays silent", func(t *testing.T) {
		script := `load 30s
  courier_source_discovery_failures_total{source="src",lane="main"} 0+1x44
  courier_source_discovery_paused{source="src",lane="main",reason="lane-suspended"} 1x44
  courier_lane_reservations{lane="main"} 0x44
  courier_lane_concurrency{lane="main"} 2x44
  courier_lane_suspended{lane="main"} 1x44
`
		st := loadStorage(t, script)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierSourceDiscoveryFailing"), 300*time.Second))
	})

	t.Run("A1 ever-succeeded source on paused binding stays silent", func(t *testing.T) {
		script := `load 30s
  courier_source_discovery_failures_total{source="src",lane="main"} 0+1x44
  courier_source_last_success_timestamp_seconds{source="src",lane="main"} 30+30x44
  courier_source_discovery_paused{source="src",lane="main",reason="lane-at-capacity"} 1x44
  courier_lane_reservations{lane="main"} 2x44
  courier_lane_concurrency{lane="main"} 2x44
  courier_lane_suspended{lane="main"} 0x44
`
		st := loadStorage(t, script)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierSourceDiscoveryFailing"), 300*time.Second))
	})

	t.Run("A1 ever-succeeded source on failing lane fires", func(t *testing.T) {
		script := `load 30s
  courier_source_discovery_failures_total{source="src",lane="main"} 0+1x44
  courier_source_last_success_timestamp_seconds{source="src",lane="main"} 100x44
  courier_source_discovery_paused{source="src",lane="main",reason="lane-suspended"} 0x44
` + baseLane
		st := loadStorage(t, script)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierSourceDiscoveryFailing"), 300*time.Second),
			[]string{"src/main//"})
	})

	t.Run("A2 saturated lane with rising queue wait fires", func(t *testing.T) {
		script := `load 30s
  courier_coderuns_in_flight{lane="main"} 2x44
  courier_lane_concurrency{lane="main"} 2x44
  courier_lane_suspended{lane="main"} 0x44
  courier_coderun_queue_wait_seconds_sum{lane="main",mode="resolve",phase="Failed"} 0+4x20 92+12x23
  courier_coderun_queue_wait_seconds_count{lane="main",mode="resolve",phase="Failed"} 0+2x44
`
		st := loadStorage(t, script)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierLaneSaturatedQueueWaitRising"), 1320*time.Second),
			[]string{"/main//"})
	})

	t.Run("A2 saturated lane with falling queue wait stays silent", func(t *testing.T) {
		script := `load 30s
  courier_coderuns_in_flight{lane="main"} 2x44
  courier_lane_concurrency{lane="main"} 2x44
  courier_lane_suspended{lane="main"} 0x44
  courier_coderun_queue_wait_seconds_sum{lane="main",mode="resolve",phase="Failed"} 0+12x20 244+4x23
  courier_coderun_queue_wait_seconds_count{lane="main",mode="resolve",phase="Failed"} 0+2x44
`
		st := loadStorage(t, script)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierLaneSaturatedQueueWaitRising"), 1320*time.Second))
	})

	t.Run("A3 fully reserved pending wedge fires via absent-transition fallback", func(t *testing.T) {
		script := `load 30s
  courier_lane_concurrency{lane="main"} 1x44
  courier_coderuns_in_flight{lane="main"} 0x44
  courier_lane_reservations{lane="main"} 1x44
  courier_lane_suspended{lane="main"} 0x44
  courier_lane_oldest_reservation_age_seconds{lane="main"} 0+30x44
`
		st := loadStorage(t, script)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierLaneReservationWedge"), 960*time.Second),
			[]string{"/main//"})
	})

	t.Run("A3 progressing lane stays silent", func(t *testing.T) {
		script := `load 30s
  courier_lane_concurrency{lane="main"} 1x44
  courier_coderuns_in_flight{lane="main"} 0x44
  courier_lane_reservations{lane="main"} 1x44
  courier_lane_suspended{lane="main"} 0x44
  courier_lane_oldest_reservation_age_seconds{lane="main"} 0+30x44
  courier_coderun_phase_transitions_total{lane="main",mode="resolve",from="Pending",to="Claimed"} 0x30 1x14
`
		st := loadStorage(t, script)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierLaneReservationWedge"), 960*time.Second))
	})

	t.Run("A5 lifecycle report backlog fires and drains silent", func(t *testing.T) {
		backlogged := loadStorage(t, `load 30s
  courier_coderuns_lifecycle_pending{lane="main"} 2x44
  courier_coderuns_lifecycle_pending_oldest_age_seconds{lane="main"} 2000x44
`)
		assertFiring(t, evalAt(t, backlogged, exprFor(t, "CourierLifecycleReportsStuck"), 300*time.Second),
			[]string{"/main//"})
		drained := loadStorage(t, `load 30s
  courier_coderuns_lifecycle_pending{lane="main"} 0x44
  courier_coderuns_lifecycle_pending_oldest_age_seconds{lane="main"} 2000x44
`)
		assertSilent(t, evalAt(t, drained, exprFor(t, "CourierLifecycleReportsStuck"), 300*time.Second))
	})

	t.Run("A6 diagnostic concentration fires above threshold", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  courier_coderun_terminal_diagnostics_total{lane="main",mode="resolve",category="kubernetes-failure"} 0+1x44
`)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierTerminalDiagnosticConcentration"), 300*time.Second),
			[]string{"/main/resolve/"})
	})

	t.Run("A6 diagnostic concentration stays silent below threshold", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  courier_coderun_terminal_diagnostics_total{lane="main",mode="resolve",category="kubernetes-failure"} 0+1x3 3x41
`)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierTerminalDiagnosticConcentration"), 300*time.Second))
	})

	t.Run("A6 unknown category stays silent", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  courier_coderun_terminal_diagnostics_total{lane="main",mode="resolve",category="unknown"} 0+1x44
`)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierTerminalDiagnosticConcentration"), 300*time.Second))
	})

	t.Run("retained failed runs gauge counts the retained run", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  courier_coderuns_awaiting_reap{phase="Failed"} 1x44
`)
		v := evalAt(t, st, `courier_coderuns_awaiting_reap{phase="Failed"}`, 300*time.Second)
		if len(v) != 1 || v[0].Metric.Get("phase") != "Failed" || v[0].F != 1 {
			t.Fatalf("want one retained Failed run valued 1, got %v", sampleKeys(v))
		}
	})

	t.Run("A8 telemetry loss fires via the absent arm", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  some_unrelated_metric 1x44
`)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierControllerDown"), 300*time.Second),
			[]string{"///"})
	})

	t.Run("A8 target reporting down fires", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  up{job="courier"} 0x44
`)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierControllerDown"), 300*time.Second),
			[]string{"///"})
	})

	t.Run("A8 healthy target stays silent", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  up{job="courier"} 1x44
`)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierControllerDown"), 300*time.Second))
	})

	t.Run("infra restart counter query increases", func(t *testing.T) {
		st := loadStorage(t, `load 30s
  courier_coderun_infra_restarts_total{lane="main",mode="resolve"} 0+1x20
`)
		v := evalAt(t, st, `increase(courier_coderun_infra_restarts_total[10m])`, 300*time.Second)
		if len(v) != 1 || v[0].F <= 0 {
			t.Fatalf("want a positive restart increase over 10m, got %v", sampleKeys(v))
		}
	})

	t.Run("A7 scrape errors fire on increments and stay silent when flat", func(t *testing.T) {
		incrementing := loadStorage(t, `load 30s
  courier_metrics_scrape_errors_total 0x5 1x39
`)
		assertFiring(t, evalAt(t, incrementing, exprFor(t, "CourierMetricsScrapeErrors"), 300*time.Second),
			[]string{"///"})
		flat := loadStorage(t, `load 30s
  courier_metrics_scrape_errors_total 5x44
`)
		assertSilent(t, evalAt(t, flat, exprFor(t, "CourierMetricsScrapeErrors"), 300*time.Second))
	})

	t.Run("A4 failure ratio fires above threshold and floor", func(t *testing.T) {
		script := `load 30s
  courier_coderuns_total{lane="main",mode="resolve",phase="Failed"} 0x59 4x61
  courier_coderuns_total{lane="main",mode="resolve",phase="Done"} 0x59 2x61
`
		st := loadStorage(t, script)
		assertFiring(t, evalAt(t, st, exprFor(t, "CourierRunFailureRateElevated"), 3000*time.Second),
			[]string{"/main/resolve/"})
	})

	t.Run("A4 failure ratio stays silent below absolute floor", func(t *testing.T) {
		script := `load 30s
  courier_coderuns_total{lane="main",mode="resolve",phase="Failed"} 0x59 1x61
  courier_coderuns_total{lane="main",mode="resolve",phase="Done"} 0x59 10x61
`
		st := loadStorage(t, script)
		assertSilent(t, evalAt(t, st, exprFor(t, "CourierRunFailureRateElevated"), 3000*time.Second))
	})
}
