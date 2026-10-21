package main

// This file implements the scrape-time assertions of
// docs/connector-diagnostics.md §11.4:
//
//   - the no-correlation-label invariant: the manager's real registry is
//     exported while a run_id rides every connector diagnostic log line, and
//     no gathered metric family may carry a correlation identifier as a
//     label — the collector's label dump is the evidence;
//   - the untraced-deployment compatibility assertion: with no tracing
//     backend configured, the capability catalogue, the connector counters
//     and gauges, and the bounded connector.diagnostic log stream still
//     emit.
//
// The scrape surface is the manager's own registry
// (sigs.k8s.io/controller-runtime/pkg/metrics.Registry) — the one the
// /metrics endpoint serves — and the fixture registers the same families
// the manager registers, so the assertions read the exposition surface a
// deployment really publishes.
//
// The invariant is about the metric exposition surface, not about cluster
// state, so the fixture needs no API server: the test always runs.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/controller"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
)

const (
	// scrapeRunID is the correlation identifier the fixture puts on every
	// connector diagnostic log line. It is the precondition of the
	// no-correlation-label invariant: the log half carries it, the metric
	// half must not.
	scrapeRunID = "run-scrape-1"
	// scrapeConnector is the bounded connector label the fixture emits
	// under.
	scrapeConnector = "dispatch"
	// scrapeSource is the source name the discovery families emit under.
	scrapeSource = "dispatch"
	// scrapeLane is the lane the fixture's cluster state belongs to.
	scrapeLane = "local"
	// scrapeNamespace is the namespace the fixture's objects live in.
	scrapeNamespace = "courier"
)

// scrapeExpectedFamilies lists the families the manager's registry must
// expose at scrape time. The list is what "the full metric set" means for
// this assertion: a family that is missing means the fixture stopped
// exporting it, which fails the test before any label assertion on that
// family could be trusted.
var scrapeExpectedFamilies = []string{
	// The connector diagnostic envelope (§2, §5).
	connector.MetricDiagnosticsTotal,
	connector.MetricCapabilityInfo,
	connector.MetricExclusionRequested,
	connector.MetricExclusionAccepted,
	connector.MetricExclusionOverflow,
	connector.MetricExclusionPressure,
	connector.MetricExclusionLimit,
	// The core discovery families (§3.2).
	"courier_source_discovery_attempts_total",
	"courier_source_discovery_successes_total",
	"courier_source_discovery_failures_total",
	"courier_source_discovery_skipped_total",
	"courier_source_discovery_paused_total",
	"courier_source_discovery_paused",
	"courier_source_items_offered_total",
	"courier_source_items_materialized_total",
	"courier_source_last_attempt_timestamp_seconds",
	"courier_source_last_success_timestamp_seconds",
	// The controller's scrape-time collectors (cmd/main.go).
	"courier_coderuns_in_flight",
	"courier_lane_suspended",
	"courier_lane_concurrency",
	"courier_lane_reservations",
	"courier_coderuns_pending",
	"courier_lane_oldest_reservation_age_seconds",
	"courier_coderuns_oldest_terminal_age_seconds",
	"courier_coderuns_awaiting_reap",
	"courier_coderuns_lifecycle_pending",
	"courier_coderuns_lifecycle_pending_oldest_age_seconds",
	// The controller's run recorder, the family set the reconciler
	// registers on the manager's registry.
	"courier_coderun_run_duration_seconds",
	"courier_coderun_queue_wait_seconds",
	"courier_coderuns_total",
	"courier_coderuns_reap_requests_total",
	"courier_coderun_phase_transitions_total",
	"courier_coderun_phase_duration_seconds",
	"courier_coderun_terminal_diagnostics_total",
	"courier_coderun_infra_restarts_total",
	"courier_coderun_verification_outcomes_total",
	"courier_coderun_tokens_total",
	"courier_source_lifecycle_report_outcomes_total",
}

// scrapeConnectorFamilies lists the connector envelope's families. The
// untraced-deployment assertion requires every one of them to carry a series
// when no tracing backend is configured.
var scrapeConnectorFamilies = []string{
	connector.MetricDiagnosticsTotal,
	connector.MetricCapabilityInfo,
	connector.MetricExclusionRequested,
	connector.MetricExclusionAccepted,
	connector.MetricExclusionOverflow,
	connector.MetricExclusionPressure,
	connector.MetricExclusionLimit,
}

// recordingDiagnosticSink captures every connector diagnostic event that
// crosses the envelope's log half and forwards it to the envelope's real
// structured-log sink, so the test can assert on the captured event and on
// the line the deployment's log stack would receive.
type recordingDiagnosticSink struct {
	mu      sync.Mutex
	events  []connector.DiagnosticEvent
	forward connector.DiagnosticLogSink
}

// EmitDiagnostic records the event and forwards it to the real sink.
func (s *recordingDiagnosticSink) EmitDiagnostic(event connector.DiagnosticEvent) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	if s.forward != nil {
		s.forward.EmitDiagnostic(event)
	}
}

// snapshot returns the events recorded so far.
func (s *recordingDiagnosticSink) snapshot() []connector.DiagnosticEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.events)
}

// syncBuffer collects the emitted log lines. The log emitter writes
// atomically, but the test reads the buffer afterwards, so the buffer keeps
// its own lock.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines returns the collected log lines.
func (b *syncBuffer) lines() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out [][]byte
	for _, line := range bytes.Split(b.buf.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		out = append(out, line)
	}
	return out
}

// scrapeFixture is the scrape-time surface under test: the manager's real
// registry, populated with the families the manager registers, and the
// recording sink that captured the envelope's log half.
//
// No tracing backend is configured anywhere in the fixture: the emitter is
// wired to a log sink only, so the assertions hold for a deployment that has
// no tracing pipeline at all.
type scrapeFixture struct {
	registry crmetrics.RegistererGatherer
	sink     *recordingDiagnosticSink
	lines    *syncBuffer
}

var (
	scrapeFixtureOnce sync.Once
	scrapeFixtureVal  *scrapeFixture
	scrapeFixtureErr  error
)

// newScrapeFixture builds the scrape-time fixture once per process. The
// manager's registry is process-global, so its families are registered once
// and every run of the assertions reads the same exposition surface.
func newScrapeFixture() (*scrapeFixture, error) {
	scrapeFixtureOnce.Do(func() {
		scrapeFixtureVal, scrapeFixtureErr = buildScrapeFixture()
	})
	return scrapeFixtureVal, scrapeFixtureErr
}

// buildScrapeFixture registers the manager's families on the manager's
// registry and gives every family a series, so the gathered set is the
// deployment's full metric set rather than an empty shell.
func buildScrapeFixture() (*scrapeFixture, error) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add courier scheme: %w", err)
	}
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(scrapeLaneProfile(), scrapePendingRun(), scrapeTerminalRun()).
		Build()

	// The same scrape-time collectors the manager registers on its registry.
	crmetrics.Registry.MustRegister(controller.NewLaneCollector(kube))
	crmetrics.Registry.MustRegister(controller.NewTerminalAgeCollector(kube))
	crmetrics.Registry.MustRegister(controller.NewTerminalBacklogCollector(kube))
	// The run recorder the reconciler registers on the manager's registry.
	recorder := controller.NewRunRecorder(crmetrics.Registry)

	// The envelope's log half: a recording sink in front of the real
	// structured-log sink, so the test sees both the captured event and the
	// emitted line.
	lines := &syncBuffer{}
	sink := &recordingDiagnosticSink{
		forward: connector.NewEventLogSink(courierlog.NewEmitter(lines, courierlog.LevelInfo)),
	}
	emitter := connector.NewEmitter(crmetrics.Registry, sink)
	emitter.RegisterConnector(scrapeConnector, map[string][]string{
		connector.SignalExclusion: {connector.ReasonOverflow, connector.ReasonOther},
	})
	discovery := source.NewDiscoveryMetrics(crmetrics.Registry)

	fixture := &scrapeFixture{registry: crmetrics.Registry, sink: sink, lines: lines}

	// Every emission below goes through the run-scoped view, so every
	// connector diagnostic log line carries the run_id.
	scoped := emitter.WithRunID(scrapeRunID)
	scoped.Capability(scrapeConnector, connector.CapabilityDiscovery, connector.CapabilitySupported)
	scoped.ExclusionLimit(scrapeConnector, 32)
	scoped.ExclusionCycle(scrapeConnector, 34, 32)
	scoped.Availability(scrapeConnector, connector.AvailabilityAvailable)

	// The core discovery families.
	discovery.IncAttempts(scrapeSource, scrapeLane)
	discovery.IncSuccesses(scrapeSource, scrapeLane)
	discovery.IncFailures(scrapeSource, scrapeLane)
	discovery.AddOffered(scrapeSource, scrapeLane, 3)
	discovery.IncSkipped(scrapeSource, scrapeLane, "missing-id")
	discovery.IncMaterialized(scrapeSource, scrapeLane)
	discovery.SetLastAttempt(scrapeSource, scrapeLane, time.Now().Unix())
	discovery.SetLastSuccess(scrapeSource, scrapeLane, time.Now().Unix())
	discovery.SetPaused(scrapeSource, scrapeLane, "lane-waiting")

	// The controller's run recorder.
	terminal := scrapeTerminalRun()
	recorder.ObserveTerminal(terminal)
	recorder.ObserveReapRequest(terminal.Status.Phase)
	recorder.ObservePhaseTransition(scrapeLane, courierv1alpha1.ModeResolveIssue, courierv1alpha1.PhasePending, courierv1alpha1.PhaseRunning)
	recorder.ObservePhaseDuration(scrapeLane, courierv1alpha1.ModeResolveIssue, courierv1alpha1.PhasePending, time.Minute)
	recorder.ObserveTerminalDiagnostic(terminal)
	recorder.ObserveInfraRestarts(scrapeSource, courierv1alpha1.ModeResolveIssue, 1)
	recorder.ObserveVerificationOutcome(scrapeLane, courierv1alpha1.ModeResolveIssue, "green")
	recorder.ObserveTokenDelta(scrapeLane, courierv1alpha1.ModeResolveIssue, "input", 10)
	recorder.ObserveLifecycleReportOutcome(scrapeSource, scrapeLane, courierv1alpha1.ModeResolveIssue, "published")

	return fixture, nil
}

// scrapeLaneProfile is the lane the fixture's cluster state belongs to.
func scrapeLaneProfile() *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: scrapeNamespace, Name: scrapeLane},
		Spec:       courierv1alpha1.LaneProfileSpec{Concurrency: 2},
	}
}

// scrapePendingRun is a run that reserves discovery capacity, so the lane's
// reservation and pending gauges have a series.
func scrapePendingRun() *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         scrapeNamespace,
			Name:              "run-scrape-pending",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
		},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     scrapeSource,
			WorkItemID: "work-scrape-pending",
			Repo:       "acme/widgets",
			Ref:        42,
			Lane:       scrapeLane,
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhasePending},
	}
}

// scrapeTerminalRun is a terminal run, so the retention and backlog
// collectors have a series to report.
func scrapeTerminalRun() *courierv1alpha1.CoderRun {
	started := metav1.NewTime(time.Now().Add(-30 * time.Minute))
	admitted := metav1.NewTime(time.Now().Add(-29 * time.Minute))
	finished := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         scrapeNamespace,
			Name:              "run-scrape-terminal",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-31 * time.Minute)),
		},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     scrapeSource,
			WorkItemID: "work-scrape-terminal",
			Repo:       "acme/widgets",
			Ref:        43,
			Lane:       scrapeLane,
		},
		Status: courierv1alpha1.CoderRunStatus{
			Phase:      courierv1alpha1.PhaseFailed,
			AdmittedAt: &admitted,
			StartedAt:  &started,
			FinishedAt: &finished,
			TerminalDiagnostic: &courierv1alpha1.TerminalDiagnostic{
				Category: courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash,
			},
		},
	}
}

// labelDump maps each gathered metric family to the sorted, de-duplicated
// label names its series carry. It is the evidence the scrape-time
// assertions are judged on.
func labelDump(families []*dto.MetricFamily) map[string][]string {
	dump := make(map[string][]string, len(families))
	for _, family := range families {
		seen := map[string]struct{}{}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				seen[label.GetName()] = struct{}{}
			}
		}
		keys := make([]string, 0, len(seen))
		for key := range seen {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		dump[family.GetName()] = keys
	}
	return dump
}

// sortedFamilyNames returns the gathered family names in a stable order.
func sortedFamilyNames(dump map[string][]string) []string {
	names := make([]string, 0, len(dump))
	for name := range dump {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// renderLabelDump renders the family -> label-names dump, the evidence a
// failing assertion prints.
func renderLabelDump(dump map[string][]string) string {
	var b strings.Builder
	for _, name := range sortedFamilyNames(dump) {
		fmt.Fprintf(&b, "\n  %s: %s", name, strings.Join(dump[name], ", "))
	}
	return b.String()
}

// correlationShapedLabel reports whether a label name is correlation-shaped:
// one of the envelope's declared correlation identifiers, or any
// identifier-shaped name (*_id, *_ids) that could carry a run, trace, or
// span identity into the metric half.
func correlationShapedLabel(name string) bool {
	if slices.Contains(connector.CorrelationKeys, name) {
		return true
	}
	return strings.HasSuffix(name, "_id") || strings.HasSuffix(name, "_ids")
}

// TestScrapeExportsFullMetricSetWithoutCorrelationLabels is the
// no-correlation-label invariant (§11.4): the manager's registry is exported
// while a run_id rides every connector diagnostic log line, the full metric
// set is present, and no gathered family carries a correlation identifier as
// a label.
func TestScrapeExportsFullMetricSetWithoutCorrelationLabels(t *testing.T) {
	fixture, err := newScrapeFixture()
	if err != nil {
		t.Fatalf("scrape fixture: %v", err)
	}
	families, err := fixture.registry.Gather()
	if err != nil {
		t.Fatalf("gather the manager registry: %v", err)
	}
	dump := labelDump(families)

	gathered := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		gathered[family.GetName()] = family
	}
	var missing []string
	for _, name := range scrapeExpectedFamilies {
		if _, ok := gathered[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the exported metric set is incomplete: %d of %d expected families are missing: %s\ngathered label dump:%s",
			len(missing), len(scrapeExpectedFamilies), strings.Join(missing, ", "), renderLabelDump(dump))
	}

	var correlationViolations []string
	for _, name := range sortedFamilyNames(dump) {
		for _, key := range dump[name] {
			if correlationShapedLabel(key) {
				correlationViolations = append(correlationViolations, fmt.Sprintf("%s{%s}", name, key))
			}
		}
	}
	if len(correlationViolations) > 0 {
		t.Errorf("the exported metric set carries correlation identifiers: %s\ngathered label dump:%s",
			strings.Join(correlationViolations, ", "), renderLabelDump(dump))
	}

	for _, name := range sortedFamilyNames(dump) {
		if !strings.HasPrefix(name, "courier_connector_") {
			continue
		}
		if err := connector.CheckLabelKeys(name, dump[name]...); err != nil {
			t.Errorf("connector family uses a label key outside the allowlist: %v\ngathered label dump:%s", err, renderLabelDump(dump))
		}
	}
}

// TestScrapeWithoutTracingBackendStillEmitsBoundedSignals is the
// untraced-deployment compatibility assertion (§11.4): with no tracing
// backend configured, the capability catalogue, the connector counters and
// gauges, and the bounded connector.diagnostic log stream still emit.
func TestScrapeWithoutTracingBackendStillEmitsBoundedSignals(t *testing.T) {
	fixture, err := newScrapeFixture()
	if err != nil {
		t.Fatalf("scrape fixture: %v", err)
	}
	families, err := fixture.registry.Gather()
	if err != nil {
		t.Fatalf("gather the manager registry: %v", err)
	}
	gathered := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		gathered[family.GetName()] = family
	}

	capability, ok := gathered[connector.MetricCapabilityInfo]
	if !ok {
		t.Fatalf("%s was not exported without a tracing backend", connector.MetricCapabilityInfo)
	}
	if len(capability.GetMetric()) == 0 {
		t.Errorf("%s exported no series without a tracing backend", connector.MetricCapabilityInfo)
	}
	for _, name := range scrapeConnectorFamilies {
		family, ok := gathered[name]
		if !ok {
			t.Errorf("connector family %s was not exported without a tracing backend", name)
			continue
		}
		if len(family.GetMetric()) == 0 {
			t.Errorf("connector family %s exported no series without a tracing backend", name)
		}
	}

	events := fixture.sink.snapshot()
	if len(events) == 0 {
		t.Fatal("no connector diagnostic crossed the log half without a tracing backend")
	}
	for _, event := range events {
		if event.RunID != scrapeRunID {
			t.Errorf("connector diagnostic log line lost its run_id: %q, want %q", event.RunID, scrapeRunID)
		}
	}

	lines := fixture.lines.lines()
	found := false
	for _, line := range lines {
		var parsed struct {
			Event string `json:"event"`
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(line, &parsed); err != nil {
			t.Errorf("emitted log line is not JSON: %v", err)
			continue
		}
		if parsed.Event != connector.EventConnectorDiagnostic {
			continue
		}
		found = true
		if parsed.RunID != scrapeRunID {
			t.Errorf("emitted %s line lost its run_id: %q, want %q", connector.EventConnectorDiagnostic, parsed.RunID, scrapeRunID)
		}
	}
	if !found {
		t.Errorf("no %s line was emitted without a tracing backend; %d log lines were emitted", connector.EventConnectorDiagnostic, len(lines))
	}
}
