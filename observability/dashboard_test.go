package observability_test

import (
	"encoding/json"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
)

type dashRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type dashTarget struct {
	RefID        string  `json:"refId"`
	Expr         string  `json:"expr"`
	LegendFormat string  `json:"legendFormat"`
	Datasource   dashRef `json:"datasource"`
}

type dashPanel struct {
	ID          int          `json:"id"`
	Type        string       `json:"type"`
	Title       string       `json:"title"`
	Description string       `json:"description"`
	Datasource  dashRef      `json:"datasource"`
	Targets     []dashTarget `json:"targets"`
}

type dashVar struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Regex   string `json:"regex"`
	Current struct {
		Text  string `json:"text"`
		Value string `json:"value"`
	} `json:"current"`
}

type dashDoc struct {
	UID           string `json:"uid"`
	Title         string `json:"title"`
	SchemaVersion int    `json:"schemaVersion"`
	Timezone      string `json:"timezone"`
	Refresh       string `json:"refresh"`
	Templating    struct {
		List []dashVar `json:"list"`
	} `json:"templating"`
	Panels []dashPanel `json:"panels"`
}

func loadDashboard(t *testing.T) dashDoc {
	t.Helper()
	data, err := os.ReadFile("dashboards/courier-overview.json")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	var d dashDoc
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatalf("parse dashboard: %v", err)
	}
	return d
}

func TestDashboardHeader(t *testing.T) {
	d := loadDashboard(t)
	if d.UID != "courier-overview" {
		t.Errorf("uid = %q, want courier-overview", d.UID)
	}
	if d.Title != "Courier Overview" {
		t.Errorf("title = %q, want Courier Overview", d.Title)
	}
	if d.SchemaVersion != 39 {
		t.Errorf("schemaVersion = %d, want 39", d.SchemaVersion)
	}
	if d.Timezone != "utc" {
		t.Errorf("timezone = %q, want utc", d.Timezone)
	}
	if d.Refresh != "1m" {
		t.Errorf("refresh = %q, want 1m", d.Refresh)
	}
}

func TestDashboardDatasourceVariable(t *testing.T) {
	d := loadDashboard(t)
	vars := d.Templating.List
	if len(vars) != 1 {
		t.Fatalf("want exactly one template variable, got %d", len(vars))
	}
	v := vars[0]
	if v.Name != "DS_PROMETHEUS" {
		t.Errorf("variable name = %q, want DS_PROMETHEUS", v.Name)
	}
	if v.Type != "datasource" {
		t.Errorf("variable type = %q, want datasource", v.Type)
	}
	if v.Regex != ".*Prometheus.*|.*VictoriaMetrics.*" {
		t.Errorf("variable regex = %q", v.Regex)
	}
	if v.Current.Value != "Prometheus" {
		t.Errorf("variable current value = %q, want Prometheus", v.Current.Value)
	}
}

func TestDashboardPanelsWellFormed(t *testing.T) {
	d := loadDashboard(t)
	if len(d.Panels) == 0 {
		t.Fatal("dashboard has no panels")
	}
	allowedTypes := map[string]struct{}{
		"timeseries": {}, "stat": {}, "table": {}, "gauge": {},
	}
	seen := map[int]struct{}{}
	for _, p := range d.Panels {
		if _, dup := seen[p.ID]; dup {
			t.Errorf("panel %d: duplicate id", p.ID)
		}
		seen[p.ID] = struct{}{}
		if _, ok := allowedTypes[p.Type]; !ok {
			t.Errorf("panel %q: type %q is not a native panel type", p.Title, p.Type)
		}
		if strings.TrimSpace(p.Title) == "" {
			t.Errorf("panel %d: missing title", p.ID)
		}
		if strings.TrimSpace(p.Description) == "" {
			t.Errorf("panel %d (%s): missing description", p.ID, p.Title)
		}
		if p.Datasource.UID != "${DS_PROMETHEUS}" {
			t.Errorf("panel %q: datasource uid %q, want ${DS_PROMETHEUS}", p.Title, p.Datasource.UID)
		}
		if len(p.Targets) == 0 {
			t.Errorf("panel %q: no targets", p.Title)
			continue
		}
		for _, tgt := range p.Targets {
			if strings.TrimSpace(tgt.Expr) == "" {
				t.Errorf("panel %q target %q: empty expr", p.Title, tgt.LegendFormat)
			}
			if tgt.Datasource.UID != "${DS_PROMETHEUS}" {
				t.Errorf("panel %q target %q: datasource uid %q, want ${DS_PROMETHEUS}",
					p.Title, tgt.LegendFormat, tgt.Datasource.UID)
			}
		}
	}
}

func TestDashboardExprMetricsAreRegistered(t *testing.T) {
	d := loadDashboard(t)
	known := knownMetricNames(t)
	known["process_start_time_seconds"] = struct{}{}
	labels := map[string]struct{}{}
	metrics := map[string]struct{}{}
	for _, p := range d.Panels {
		for _, tgt := range p.Targets {
			node, err := exprParser.ParseExpr(tgt.Expr)
			if err != nil {
				t.Errorf("panel %q: expr does not parse: %v\n%s", p.Title, err, tgt.Expr)
				continue
			}
			collect(node, labels, metrics)
		}
	}
	for m := range metrics {
		if _, ok := known[m]; !ok {
			t.Errorf("panel expr references unknown metric %q", m)
		}
		if strings.Contains(m, "connector") || strings.Contains(m, "dispatch") {
			t.Errorf("panel expr references excluded metric %q", m)
		}
	}
}

func TestDashboardExprLabelsAreBounded(t *testing.T) {
	d := loadDashboard(t)
	labels := map[string]struct{}{}
	metrics := map[string]struct{}{}
	for _, p := range d.Panels {
		for _, tgt := range p.Targets {
			node, err := exprParser.ParseExpr(tgt.Expr)
			if err != nil {
				t.Errorf("panel %q: expr does not parse: %v\n%s", p.Title, err, tgt.Expr)
				continue
			}
			collect(node, labels, metrics)
		}
	}
	for l := range labels {
		if _, ok := allowedExprLabels[l]; !ok {
			t.Errorf("dashboard expr uses unbounded label %q", l)
		}
	}
}

func panelByID(t *testing.T, d dashDoc, id int) dashPanel {
	t.Helper()
	for _, p := range d.Panels {
		if p.ID == id {
			return p
		}
	}
	t.Fatalf("dashboard has no panel %d", id)
	return dashPanel{}
}

func targetByRefID(t *testing.T, p dashPanel, refID string) dashTarget {
	t.Helper()
	for _, tgt := range p.Targets {
		if tgt.RefID == refID {
			return tgt
		}
	}
	t.Fatalf("panel %d (%q) has no target %q", p.ID, p.Title, refID)
	return dashTarget{}
}

// assertNaNIdentity asserts that the vector holds exactly one sample, that it
// belongs to the given identity, and that its value is NaN: the value the
// backend emits for a missing state. It must never be a measured zero.
func assertNaNIdentity(t *testing.T, v promql.Vector, want map[string]string) {
	t.Helper()
	if len(v) != 1 {
		t.Fatalf("want exactly one sample, got %v", sampleKeys(v))
	}
	s := v[0]
	for label, value := range want {
		if got := s.Metric.Get(label); got != value {
			t.Fatalf("want %s=%q, got %q", label, value, got)
		}
	}
	if !math.IsNaN(s.F) {
		t.Fatalf("want a NaN (no value) sample for %v, got %v", want, s.F)
	}
}

// assertSingleIdentity asserts that the vector holds exactly one sample and
// that it belongs to the given identity.
func assertSingleIdentity(t *testing.T, v promql.Vector, want map[string]string) {
	t.Helper()
	if len(v) != 1 {
		t.Fatalf("want exactly one sample, got %v", sampleKeys(v))
	}
	for label, value := range want {
		if got := v[0].Metric.Get(label); got != value {
			t.Fatalf("want %s=%q, got %q", label, value, got)
		}
	}
}

// assertNoIdentity asserts that the vector carries no sample for the given
// identity: the primary target omits the missing state, and the fallback
// target is what carries it.
func assertNoIdentity(t *testing.T, v promql.Vector, label, unwanted string) {
	t.Helper()
	for _, s := range v {
		if s.Metric.Get(label) == unwanted {
			t.Fatalf("want no sample for %s=%q, got %v", label, unwanted, s.F)
		}
	}
}

func TestDashboardHasNoGlobalAbsentFallback(t *testing.T) {
	d := loadDashboard(t)
	for _, p := range d.Panels {
		for _, tgt := range p.Targets {
			if strings.Contains(tgt.Expr, "absent(") {
				t.Errorf("panel %d (%q) target %q uses absent(): a global fallback hides a peer identity",
					p.ID, p.Title, tgt.LegendFormat)
			}
		}
	}
}

// TestDashboardMissingStateKeepsEveryIdentity covers the three panels that
// render a state with no metric of its own. The missing state must be shown
// per identity: a source/lane binding that never succeeded, a lane whose
// unsettled report has no age, a lane with no unsettled report, a phase
// retained without an age, a phase with no retained run. A global absent()
// would drop the missing identity as soon as any peer identity had a series.
func TestDashboardMissingStateKeepsEveryIdentity(t *testing.T) {
	d := loadDashboard(t)

	t.Run("discovery freshness keeps the never-succeeded binding", func(t *testing.T) {
		script := `load 30s
  courier_source_discovery_attempts_total{source="dispatch",lane="main"} 4x20
  courier_source_discovery_attempts_total{source="dispatch",lane="other"} 7x20
  courier_source_last_success_timestamp_seconds{source="dispatch",lane="main"} 1700000000x20
`
		st := loadStorage(t, script)
		panel := panelByID(t, d, 3)
		primary := evalAt(t, st, targetByRefID(t, panel, "A").Expr, 300*time.Second)
		assertSingleIdentity(t, primary, map[string]string{"source": "dispatch", "lane": "main"})
		assertNoIdentity(t, primary, "lane", "other")
		assertNaNIdentity(t, evalAt(t, st, targetByRefID(t, panel, "B").Expr, 300*time.Second),
			map[string]string{"source": "dispatch", "lane": "other"})
	})

	t.Run("lifecycle keeps the lane with an unknown age and the lane with no unsettled report", func(t *testing.T) {
		script := `load 30s
  courier_lane_concurrency{lane="main"} 2x20
  courier_lane_concurrency{lane="unfinished"} 2x20
  courier_lane_concurrency{lane="settled"} 2x20
  courier_coderuns_lifecycle_pending_oldest_age_seconds{lane="main"} 900x20
  courier_coderuns_lifecycle_pending{lane="main"} 1x20
  courier_coderuns_lifecycle_pending{lane="unfinished"} 1x20
`
		st := loadStorage(t, script)
		panel := panelByID(t, d, 17)
		primary := evalAt(t, st, targetByRefID(t, panel, "A").Expr, 300*time.Second)
		assertSingleIdentity(t, primary, map[string]string{"lane": "main"})
		assertNoIdentity(t, primary, "lane", "unfinished")
		// An unsettled report whose run has no FinishedAt yet: pending, no age.
		assertNaNIdentity(t, evalAt(t, st, targetByRefID(t, panel, "B").Expr, 300*time.Second),
			map[string]string{"lane": "unfinished"})
		// No unsettled report at all.
		assertNaNIdentity(t, evalAt(t, st, targetByRefID(t, panel, "C").Expr, 300*time.Second),
			map[string]string{"lane": "settled"})
	})

	t.Run("retention keeps the phase without an age and the phase without a retained run", func(t *testing.T) {
		script := `load 30s
  courier_coderuns_oldest_terminal_age_seconds{phase="Done"} 120x20
  courier_coderuns_awaiting_reap{phase="Done"} 1x20
  courier_coderuns_awaiting_reap{phase="Failed"} 1x20
  courier_coderuns_total{lane="main",mode="resolve",phase="Done"} 3x20
  courier_coderuns_total{lane="main",mode="resolve",phase="Failed"} 1x20
  courier_coderuns_total{lane="main",mode="resolve",phase="Cancelled"} 2x20
`
		st := loadStorage(t, script)
		panel := panelByID(t, d, 21)
		primary := evalAt(t, st, targetByRefID(t, panel, "A").Expr, 300*time.Second)
		assertSingleIdentity(t, primary, map[string]string{"phase": "Done"})
		assertNoIdentity(t, primary, "phase", "Failed")
		// Retained, but its age is unknown.
		assertNaNIdentity(t, evalAt(t, st, targetByRefID(t, panel, "B").Expr, 300*time.Second),
			map[string]string{"phase": "Failed"})
		// Terminalized, but nothing is retained any more.
		assertNaNIdentity(t, evalAt(t, st, targetByRefID(t, panel, "C").Expr, 300*time.Second),
			map[string]string{"phase": "Cancelled"})
	})
}

func TestDashboardSummaryGolden(t *testing.T) {
	d := loadDashboard(t)
	lines := make([]string, 0, len(d.Panels))
	for _, p := range d.Panels {
		for _, tgt := range p.Targets {
			lines = append(lines, p.Title+"|"+tgt.Expr)
		}
	}
	sort.Strings(lines)
	got := strings.Join(lines, "\n") + "\n"

	const goldenPath = "testdata/dashboard-summary.golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with UPDATE_GOLDEN=1 to regenerate): %v", err)
	}
	if got != string(want) {
		t.Fatalf("dashboard summary drifted from golden\nwant:\n%s\ngot:\n%s", want, got)
	}
}
