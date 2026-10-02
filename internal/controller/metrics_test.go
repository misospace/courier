package controller

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

func terminalRunLabels() map[string]string {
	return map[string]string{"lane": "local", "mode": "resolve-issue", "phase": "Failed"}
}

func gatherMetric(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != len(labels) {
				continue
			}
			match := true
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					match = false
					break
				}
			}
			if match {
				return m
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return nil
}

func gaugeValue(t *testing.T, m *dto.Metric, name string) float64 {
	t.Helper()
	if m.GetGauge() == nil {
		t.Fatalf("metric %s is not a gauge", name)
	}
	return m.GetGauge().GetValue()
}

func histogramSample(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (uint64, float64) {
	t.Helper()
	m := gatherMetric(t, reg, name, labels)
	if m.GetHistogram() == nil {
		t.Fatalf("metric %s is not a histogram", name)
	}
	h := m.GetHistogram()
	return h.GetSampleCount(), h.GetSampleSum()
}

// histogramSampleCount reports the sample count of a histogram sample, or 0
// when no sample exists for the labels, so a test can assert an observation
// did not happen.
func histogramSampleCount(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != len(labels) {
				continue
			}
			match := true
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					match = false
					break
				}
			}
			if match && m.GetHistogram() != nil {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func TestTerminalRunObservesMetricsOnce(t *testing.T) {
	run := admissionRun("run1", "local", courierv1alpha1.PhaseRunning)
	run.CreationTimestamp = metav1.NewTime(timestampClock.Add(-1 * time.Hour))
	run.Status.AdmittedAt = &metav1.Time{Time: timestampClock.Add(-50 * time.Minute)}

	// The pod records the coordinator's own start 30m before it exits, so the
	// terminal reconcile pairs a 1800s run duration with the 12:00 clock.
	pod := coordinatorPod(run, 17)
	pod.Status.ContainerStatuses[0].State.Terminated.StartedAt = metav1.NewTime(timestampClock.Add(-30 * time.Minute))

	registry := prometheus.NewRegistry()
	recorder := NewRunRecorder(registry)

	client := phaseClient(t, run, admissionLane("local", 2), pod)
	r := timestampReconciler(client, timestampClock)
	r.Metrics = recorder

	if _, err := r.Reconcile(context.Background(), admissionRequest("run1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	labels := terminalRunLabels()
	if got := gaugeCounter(t, registry, labels); got != 1 {
		t.Fatalf("courier_coderuns_total = %v, want 1", got)
	}
	if count, sum := histogramSample(t, registry, "courier_coderun_run_duration_seconds", labels); count != 1 || sum != 1800 {
		t.Fatalf("run duration = (count %d, sum %v), want (1, 1800)", count, sum)
	}
	if count, sum := histogramSample(t, registry, "courier_coderun_queue_wait_seconds", labels); count != 1 || sum != 600 {
		t.Fatalf("queue wait = (count %d, sum %v), want (1, 600)", count, sum)
	}

	// Re-reconciling an already-terminal run must not observe the metrics
	// again.
	if _, err := r.Reconcile(context.Background(), admissionRequest("run1")); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if got := gaugeCounter(t, registry, labels); got != 1 {
		t.Fatalf("courier_coderuns_total after re-reconcile = %v, want 1", got)
	}
	if count, _ := histogramSample(t, registry, "courier_coderun_run_duration_seconds", labels); count != 1 {
		t.Fatalf("run duration count after re-reconcile = %d, want 1", count)
	}
	if count, _ := histogramSample(t, registry, "courier_coderun_queue_wait_seconds", labels); count != 1 {
		t.Fatalf("queue wait count after re-reconcile = %d, want 1", count)
	}
}

func gaugeCounter(t *testing.T, reg *prometheus.Registry, labels map[string]string) float64 {
	t.Helper()
	m := gatherMetric(t, reg, "courier_coderuns_total", labels)
	if m.GetCounter() == nil {
		t.Fatalf("courier_coderuns_total is not a counter")
	}
	return m.GetCounter().GetValue()
}

func TestTerminalRunWithoutStartSkipsRunDuration(t *testing.T) {
	run := admissionRun("run1", "local", courierv1alpha1.PhaseRunning)
	run.CreationTimestamp = metav1.NewTime(timestampClock.Add(-1 * time.Hour))
	run.Status.AdmittedAt = &metav1.Time{Time: timestampClock.Add(-45 * time.Minute)}

	// coordinatorPod records no start time, so the run's StartedAt stays
	// unset and only the queue wait is observable.
	pod := coordinatorPod(run, 17)

	registry := prometheus.NewRegistry()
	recorder := NewRunRecorder(registry)

	client := phaseClient(t, run, admissionLane("local", 2), pod)
	r := timestampReconciler(client, timestampClock)
	r.Metrics = recorder

	if _, err := r.Reconcile(context.Background(), admissionRequest("run1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	labels := terminalRunLabels()
	if got := gaugeCounter(t, registry, labels); got != 1 {
		t.Fatalf("courier_coderuns_total = %v, want 1", got)
	}
	if count := histogramSampleCount(t, registry, "courier_coderun_run_duration_seconds", labels); count != 0 {
		t.Fatalf("run duration sample count = %d, want 0", count)
	}
	if count, sum := histogramSample(t, registry, "courier_coderun_queue_wait_seconds", labels); count != 1 || sum != 900 {
		t.Fatalf("queue wait = (count %d, sum %v), want (1, 900)", count, sum)
	}
}

func TestLaneCollectorInFlightAndSuspended(t *testing.T) {
	local := admissionLane("local", 2)
	local.Annotations = map[string]string{courierv1alpha1.SuspendAnnotation: "true"}
	cloud := admissionLane("cloud", 3)

	client := phaseClient(t,
		local,
		cloud,
		admissionRun("local-claimed", "local", courierv1alpha1.PhaseClaimed),
		admissionRun("local-running", "local", courierv1alpha1.PhaseRunning),
		admissionRun("local-verifying", "local", courierv1alpha1.PhaseVerifying),
		admissionRun("local-pending", "local", courierv1alpha1.PhasePending),
		admissionRun("local-done", "local", courierv1alpha1.PhaseDone),
		admissionRun("cloud-running", "cloud", courierv1alpha1.PhaseRunning),
		admissionRun("cloud-failed", "cloud", courierv1alpha1.PhaseFailed),
	)

	registry := prometheus.NewRegistry()
	registry.MustRegister(NewLaneCollector(client))

	if got := gaugeValue(t, gatherMetric(t, registry, "courier_coderuns_in_flight", map[string]string{"lane": "local"}), "courier_coderuns_in_flight"); got != 2 {
		t.Fatalf("in-flight local = %v, want 2 (Claimed + Running only)", got)
	}
	if got := gaugeValue(t, gatherMetric(t, registry, "courier_coderuns_in_flight", map[string]string{"lane": "cloud"}), "courier_coderuns_in_flight"); got != 1 {
		t.Fatalf("in-flight cloud = %v, want 1", got)
	}
	if got := gaugeValue(t, gatherMetric(t, registry, "courier_lane_suspended", map[string]string{"lane": "local"}), "courier_lane_suspended"); got != 1 {
		t.Fatalf("suspended local = %v, want 1", got)
	}
	if got := gaugeValue(t, gatherMetric(t, registry, "courier_lane_suspended", map[string]string{"lane": "cloud"}), "courier_lane_suspended"); got != 0 {
		t.Fatalf("suspended cloud = %v, want 0", got)
	}
}

// The metrics must stay aggregatable: no label may carry work-item identity
// (repo, ref, work item, run name, PR), or cardinality would grow with the
// number of runs instead of staying bounded by lanes, modes, and phases. The
// label name sets are asserted against gathered output, not the label
// constants the metrics were constructed from.
func TestMetricLabelsExcludeIdentity(t *testing.T) {
	run := admissionRun("run1", "local", courierv1alpha1.PhaseFailed)
	run.Spec.Repo = "acme/widgets"
	run.Status.StartedAt = &metav1.Time{Time: timestampClock.Add(-30 * time.Minute)}
	run.Status.FinishedAt = &metav1.Time{Time: timestampClock}
	run.Status.AdmittedAt = &metav1.Time{Time: timestampClock.Add(-50 * time.Minute)}

	registry := prometheus.NewRegistry()
	recorder := NewRunRecorder(registry)
	recorder.ObserveTerminal(run)
	for _, name := range []string{
		"courier_coderun_run_duration_seconds",
		"courier_coderun_queue_wait_seconds",
		"courier_coderuns_total",
	} {
		assertGatheredLabelSet(t, registry, name, "lane", "mode", "phase")
	}

	// A drained lane must still appear on both lane gauges, reporting 0.
	runsClient := phaseClient(t,
		admissionLane("local", 2),
		admissionRun("run2", "local", courierv1alpha1.PhaseDone),
	)
	laneRegistry := prometheus.NewRegistry()
	laneRegistry.MustRegister(NewLaneCollector(runsClient))
	for _, name := range []string{"courier_coderuns_in_flight", "courier_lane_suspended"} {
		assertGatheredLabelSet(t, laneRegistry, name, "lane")
	}
	if got := gaugeValue(t, gatherMetric(t, laneRegistry, "courier_coderuns_in_flight", map[string]string{"lane": "local"}), "courier_coderuns_in_flight"); got != 0 {
		t.Fatalf("in-flight for a drained lane = %v, want 0", got)
	}

	// No label value may carry work-item identity.
	identity := map[string]bool{
		run.Spec.Repo:       true,
		run.Spec.WorkItemID: true,
		run.Name:            true,
		"1":                 true,
	}
	for _, g := range []prometheus.Gatherer{registry, laneRegistry} {
		families, err := g.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}
		for _, family := range families {
			for _, m := range family.GetMetric() {
				for _, lp := range m.GetLabel() {
					if identity[lp.GetValue()] {
						t.Fatalf("metric %s label %q carries identity value %q", family.GetName(), lp.GetName(), lp.GetValue())
					}
				}
			}
		}
	}
}

// assertGatheredLabelSet gathers the named family and asserts the observed
// label names are exactly want.
func assertGatheredLabelSet(t *testing.T, g prometheus.Gatherer, name string, want ...string) {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]bool{}
	found := false
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		found = true
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = true
			}
		}
	}
	if !found {
		t.Fatalf("metric family %s not found in gathered output", name)
	}
	if len(got) != len(want) {
		t.Fatalf("%s label names = %v, want %v", name, sortedNames(got), want)
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("%s label names = %v, want %v", name, sortedNames(got), want)
		}
	}
}

func sortedNames(set map[string]bool) []string {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TestObserveTerminalClampsNegativeDurations checks the clock-skew guard:
// timestamps that disagree about the ordering of the run's own events fold
// into a zero sample, not a negative one.
func TestObserveTerminalClampsNegativeDurations(t *testing.T) {
	run := admissionRun("run1", "local", courierv1alpha1.PhaseFailed)
	run.CreationTimestamp = metav1.NewTime(timestampClock.Add(-1 * time.Hour))
	run.Status.StartedAt = &metav1.Time{Time: timestampClock.Add(-30 * time.Minute)}
	run.Status.FinishedAt = &metav1.Time{Time: timestampClock.Add(-40 * time.Minute)}
	run.Status.AdmittedAt = &metav1.Time{Time: timestampClock.Add(-70 * time.Minute)}

	registry := prometheus.NewRegistry()
	recorder := NewRunRecorder(registry)
	recorder.ObserveTerminal(run)

	labels := terminalRunLabels()
	if count, sum := histogramSample(t, registry, "courier_coderun_run_duration_seconds", labels); count != 1 || sum != 0 {
		t.Fatalf("run duration = (count %d, sum %v), want (1, 0) after clamping", count, sum)
	}
	if count, sum := histogramSample(t, registry, "courier_coderun_queue_wait_seconds", labels); count != 1 || sum != 0 {
		t.Fatalf("queue wait = (count %d, sum %v), want (1, 0) after clamping", count, sum)
	}
}

// TestTerminalStatusPatchFailureRecordsNoMetrics checks the ordering that
// keeps the metrics honest: the terminal phase is persisted before it is
// observed, so a failed status patch records nothing.
func TestTerminalStatusPatchFailureRecordsNoMetrics(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.StartedAt = &metav1.Time{Time: timestampClock.Add(-30 * time.Minute)}
	pod := coordinatorPod(run, 17)

	base := phaseClient(t, run, pod)
	withWatch, ok := base.(client.WithWatch)
	if !ok {
		t.Fatal("fake client does not implement client.WithWatch")
	}
	failing := interceptor.NewClient(withWatch, interceptor.Funcs{
		SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			return errors.New("status patch failed")
		},
	})

	registry := prometheus.NewRegistry()
	recorder := NewRunRecorder(registry)
	reconciler := &CoderRunReconciler{
		Client:  failing,
		Sources: NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		Metrics: recorder,
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err == nil {
		t.Fatal("Reconcile() error = nil, want the failed terminal status patch")
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "courier_coderuns_total" {
			t.Fatalf("courier_coderuns_total observed after a failed terminal status patch: %#v", f)
		}
	}
}
