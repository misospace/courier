package source

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// metricsClock is the fixed runner clock used across these tests, so the
// timestamp gauges are asserted against known unix seconds.
var metricsClock = time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

// newInstrumentedRunner builds a Runner on a fresh registry with Metrics
// attached and the clock pinned, so tests never sleep and every registry
// starts empty.
func newInstrumentedRunner(t *testing.T, reg *prometheus.Registry, kubeClient client.Client, adapter Adapter, config RunnerConfig) *Runner {
	t.Helper()
	runner := NewRunner(kubeClient, adapter, config)
	runner.Metrics = NewDiscoveryMetrics(reg)
	runner.Now = func() time.Time { return metricsClock }
	return runner
}

// counterValueIfPresent reports the named counter's value for the given labels
// and whether the series exists at all, for assertions that a series may be
// legitimately absent.
func counterValueIfPresent(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return findCounter(families, name, labels)
}

func findCounter(families []*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !labelsMatch(m, labels) {
				continue
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func labelsMatch(m *dto.Metric, labels map[string]string) bool {
	if len(m.GetLabel()) != len(labels) {
		return false
	}
	for _, lp := range m.GetLabel() {
		if labels[lp.GetName()] != lp.GetValue() {
			return false
		}
	}
	return true
}

func gaugeSeriesIfPresent(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
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
			if labelsMatch(m, labels) && m.GetGauge() != nil {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func gatherFamilies(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return families
}

// pausedGaugeSamples gathers every paused-gauge sample for the binding keyed
// by reason, so tests can assert the exactly-one-reason invariant and the
// all-zero unpaused state without hard-coding the reason list twice.
func pausedGaugeSamples(t *testing.T, reg *prometheus.Registry, sourceName, lane string) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	samples := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "courier_source_discovery_paused" {
			continue
		}
		for _, m := range f.GetMetric() {
			byName := map[string]string{}
			for _, lp := range m.GetLabel() {
				byName[lp.GetName()] = lp.GetValue()
			}
			if byName["source"] != sourceName || byName["lane"] != lane {
				continue
			}
			if m.GetGauge() != nil {
				samples[byName["reason"]] = m.GetGauge().GetValue()
			}
		}
	}
	return samples
}

// assertExactlyOnePausedReason asserts the current-state invariant: while
// paused, exactly one reason series is 1 and it is wantReason; all others are
// 0.
func assertExactlyOnePausedReason(t *testing.T, reg *prometheus.Registry, sourceName, lane, wantReason string) {
	t.Helper()
	samples := pausedGaugeSamples(t, reg, sourceName, lane)
	// Absent is not zero (§4.3): every reason of the closed vocabulary must
	// exist, so a helper that never created the non-active series cannot pass
	// vacuously.
	if len(samples) != len(DiscoveryPauseReasons) {
		t.Fatalf("paused gauge samples %v, want one series per reason %v", samples, DiscoveryPauseReasons)
	}
	sum := 0.0
	for reason, value := range samples {
		if reason == wantReason {
			if value != 1 {
				t.Fatalf("paused gauge[%s] = %v, want 1", reason, value)
			}
		} else if value != 0 {
			t.Fatalf("paused gauge[%s] = %v, want 0 while paused for %s", reason, value, wantReason)
		}
		sum += value
	}
	if sum != 1 {
		t.Fatalf("paused gauge samples sum = %v, want exactly one reason at 1: %v", sum, samples)
	}
}

// assertPausedAllZero asserts the unpaused invariant: every reason series is
// present and 0.
func assertPausedAllZero(t *testing.T, reg *prometheus.Registry, sourceName, lane string) {
	t.Helper()
	samples := pausedGaugeSamples(t, reg, sourceName, lane)
	if len(samples) != len(DiscoveryPauseReasons) {
		t.Fatalf("paused gauge samples %v, want one series per reason %v", samples, DiscoveryPauseReasons)
	}
	for reason, value := range samples {
		if value != 0 {
			t.Fatalf("paused gauge[%s] = %v, want every reason 0 when not paused", reason, value)
		}
	}
}

func TestRunnerMetricsSuccessfulPoll(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	lane := testLane()
	lane.Spec.Concurrency = 2
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, newTestClient(t, lane), adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"source": "dispatch", "lane": "local"}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_attempts_total", labels); !ok || v != 1 {
		t.Fatalf("attempts = (%v, %v), want (1, true)", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_successes_total", labels); !ok || v != 1 {
		t.Fatalf("successes = (%v, %v), want (1, true)", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_failures_total", labels); ok {
		t.Fatalf("failures series = (%v, %v), want absent after only successes", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_materialized_total", labels); !ok || v != 1 {
		t.Fatalf("materialized = (%v, %v), want (1, true)", v, ok)
	}
	if v, _ := gaugeSeriesIfPresent(t, reg, "courier_source_last_attempt_timestamp_seconds", labels); v != float64(metricsClock.Unix()) {
		t.Fatalf("last attempt = %v, want %v", v, float64(metricsClock.Unix()))
	}
	if v, ok := gaugeSeriesIfPresent(t, reg, "courier_source_last_success_timestamp_seconds", labels); !ok || v != float64(metricsClock.Unix()) {
		t.Fatalf("last success = (%v, %v), want (%v, true)", v, ok, float64(metricsClock.Unix()))
	}
	assertPausedAllZero(t, reg, "dispatch", "local")

	// A second poll re-offers the same item: it attempts and succeeds again
	// but materializes nothing new.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_attempts_total", labels); !ok || v != 2 {
		t.Fatalf("attempts after second poll = (%v, %v), want (2, true)", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_successes_total", labels); !ok || v != 2 {
		t.Fatalf("successes after second poll = (%v, %v), want (2, true)", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_materialized_total", labels); !ok || v != 1 {
		t.Fatalf("materialized after dedupe = (%v, %v), want (1, true)", v, ok)
	}
}

func TestRunnerMetricsFailedDiscoverCountsFailureNotSuccess(t *testing.T) {
	want := errors.New("temporary upstream failure")
	adapter := &testAdapter{err: want}
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, newTestClient(t, testLane()), adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Poll() error = %v, want %v", err, want)
	}
	labels := map[string]string{"source": "test", "lane": "local"}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_failures_total", labels); !ok || v != 1 {
		t.Fatalf("failures = (%v, %v), want (1, true)", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_successes_total", labels); ok {
		t.Fatalf("successes series = (%v, %v), want absent after a failure", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_attempts_total", labels); !ok || v != 1 {
		t.Fatalf("attempts = (%v, %v), want (1, true)", v, ok)
	}
	if _, ok := gaugeSeriesIfPresent(t, reg, "courier_source_last_attempt_timestamp_seconds", labels); !ok {
		t.Fatal("last attempt series absent after a failed attempt, want present")
	}
	// Absence is starting/unknown, never idle: the success timestamp must
	// stay absent until the first success.
	if _, ok := gaugeSeriesIfPresent(t, reg, "courier_source_last_success_timestamp_seconds", labels); ok {
		t.Fatal("last success series present before any success, want absent")
	}
}

func TestRunnerMetricsZeroItemSuccessKeepsOfferedAtZero(t *testing.T) {
	adapter := &testAdapter{}
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, newTestClient(t, testLane()), adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"source": "test", "lane": "local"}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_offered_total", labels); !ok || v != 0 {
		t.Fatalf("offered = (%v, %v), want (0, true)", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_successes_total", labels); !ok || v != 1 {
		t.Fatalf("successes = (%v, %v), want (1, true): zero items is still a success", v, ok)
	}
	if v, ok := gaugeSeriesIfPresent(t, reg, "courier_source_last_success_timestamp_seconds", labels); !ok || v != float64(metricsClock.Unix()) {
		t.Fatalf("last success = (%v, %v), want (%v, true)", v, ok, float64(metricsClock.Unix()))
	}
}

func TestRunnerMetricsOfferedSkippedAndMaterialized(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{
		{ID: "", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1},
		{ID: "unsupported", Mode: "unknown", Repo: "acme/widgets", Ref: 1},
		{ID: "invalid-ref", Mode: "resolve-issue", Repo: "", Ref: 0},
		{ID: "valid-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1},
		{ID: "valid-b", Mode: "fix-pr", Repo: "acme/widgets", Ref: 2},
	}}
	lane := testLane()
	lane.Spec.Concurrency = 2
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, newTestClient(t, lane), adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"source": "test", "lane": "local"}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_offered_total", labels); !ok || v != 5 {
		t.Fatalf("offered = (%v, %v), want (5, true): offered counts pre-validation items", v, ok)
	}
	for _, reason := range DiscoverySkipReasons {
		reasonLabels := map[string]string{"source": "test", "lane": "local", "reason": reason}
		if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_skipped_total", reasonLabels); !ok || v != 1 {
			t.Fatalf("skipped[%s] = (%v, %v), want (1, true)", reason, v, ok)
		}
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_materialized_total", labels); !ok || v != 2 {
		t.Fatalf("materialized = (%v, %v), want (2, true)", v, ok)
	}
}

func TestRunnerMetricsMaterializedCountsCreatedRunsOnly(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	lane := testLane()
	lane.Spec.Concurrency = 2
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, newTestClient(t, lane), adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The re-offer takes the dedupe path: the re-listed run is in the
	// existing map, so Create is never called again. Materialized stays at 1.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_materialized_total", map[string]string{"source": "test", "lane": "local"}); !ok || v != 1 {
		t.Fatalf("materialized = (%v, %v), want (1, true)", v, ok)
	}
}

// TestRunnerMetricsMaterializedIgnoresAlreadyExists pins the other not-counted
// path: a Create that loses a race (the run already exists under the same
// deterministic name) is not a new materialization.
func TestRunnerMetricsMaterializedIgnoresAlreadyExists(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	lane := testLane()
	lane.Spec.Concurrency = 2
	// Interceptor rejects the Create as an AlreadyExists conflict, as a run
	// materialized concurrently by another binding would.
	kubeClient := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(lane).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*courierv1alpha1.CoderRun); ok {
					return apierrors.NewAlreadyExists(schema.GroupResource{Group: "courier.misospace.dev", Resource: "coderuns"}, obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, kubeClient, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_materialized_total", map[string]string{"source": "test", "lane": "local"}); ok && v != 0 {
		t.Fatalf("materialized = (%v, %v), want absent or 0", v, ok)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_successes_total", map[string]string{"source": "test", "lane": "local"}); !ok || v != 1 {
		t.Fatalf("successes = (%v, %v), want (1, true)", v, ok)
	}
}

func TestRunnerMetricsPausedGatesAndResume(t *testing.T) {
	now := metricsClock
	exhausted := testLane()
	exhausted.Name = "pool-lane"
	exhausted.Spec.CapacityGroup = "pool-a"
	exhausted.Status.CapacityCooldown = &courierv1alpha1.CapacityCooldown{
		CapacityID: "pool-a", NextEligibleAt: metav1.NewTime(now.Add(time.Hour)),
	}
	occupied := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	item := WorkItem{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}

	for _, tc := range []struct {
		name   string
		reason string
		objs   []client.Object
		lane   string
		resume func(t *testing.T, runner *Runner, kubeClient client.Client)
	}{
		{
			name:   "lane waiting",
			reason: "lane-waiting",
			lane:   "local",
			resume: func(t *testing.T, runner *Runner, kubeClient client.Client) {
				if err := kubeClient.Create(context.Background(), testLane()); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "lane suspended",
			reason: "lane-suspended",
			lane:   "local",
			objs: func() []client.Object {
				l := testLane()
				l.Annotations = map[string]string{courierv1alpha1.SuspendAnnotation: "true"}
				return []client.Object{l}
			}(),
			resume: func(t *testing.T, runner *Runner, kubeClient client.Client) {
				l := testLane()
				if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(l), l); err != nil {
					t.Fatal(err)
				}
				delete(l.Annotations, courierv1alpha1.SuspendAnnotation)
				if err := kubeClient.Update(context.Background(), l); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "lane at capacity",
			reason: "lane-at-capacity",
			lane:   "local",
			objs: func() []client.Object {
				l := testLane()
				l.Spec.Concurrency = 1
				return []client.Object{l, occupied.DeepCopy()}
			}(),
			resume: func(t *testing.T, runner *Runner, kubeClient client.Client) {
				if err := kubeClient.Delete(context.Background(), occupied.DeepCopy()); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "capacity cooldown",
			reason: "capacity-cooldown",
			lane:   "pool-lane",
			objs:   []client.Object{exhausted},
			resume: func(t *testing.T, runner *Runner, kubeClient client.Client) {
				runner.Now = func() time.Time { return now.Add(2 * time.Hour) }
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &testAdapter{items: []WorkItem{item}}
			kubeClient := newTestClient(t, tc.objs...)
			reg := prometheus.NewRegistry()
			runner := newInstrumentedRunner(t, reg, kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: tc.lane, Namespace: "courier"})

			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if adapter.discoverCalls != 0 {
				t.Fatalf("discover calls while paused for %s = %d, want 0", tc.reason, adapter.discoverCalls)
			}
			pausedLabels := map[string]string{"source": "dispatch", "lane": tc.lane, "reason": tc.reason}
			if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_paused_total", pausedLabels); !ok || v != 1 {
				t.Fatalf("paused_total[%s] = (%v, %v), want (1, true)", tc.reason, v, ok)
			}
			assertExactlyOnePausedReason(t, reg, "dispatch", tc.lane, tc.reason)
			if _, ok := counterValueIfPresent(t, reg, "courier_source_discovery_attempts_total", map[string]string{"source": "dispatch", "lane": tc.lane}); ok {
				t.Fatal("attempts series exists for a skipped poll, want absent")
			}
			if _, ok := gaugeSeriesIfPresent(t, reg, "courier_source_last_attempt_timestamp_seconds", map[string]string{"source": "dispatch", "lane": tc.lane}); ok {
				t.Fatal("last attempt series exists for a skipped poll, want absent")
			}

			tc.resume(t, runner, kubeClient)
			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertPausedAllZero(t, reg, "dispatch", tc.lane)
			// The pause counter is cumulative across pause and resume.
			if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_paused_total", pausedLabels); !ok || v != 1 {
				t.Fatalf("paused_total[%s] after resume = (%v, %v), want (1, true)", tc.reason, v, ok)
			}
			if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_attempts_total", map[string]string{"source": "dispatch", "lane": tc.lane}); !ok || v != 1 {
				t.Fatalf("attempts after resume = (%v, %v), want (1, true)", v, ok)
			}
		})
	}
}

func TestRunnerMetricsLastSuccessAbsentBeforeFirstSuccess(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	// No LaneProfile in the cluster: every poll pauses at lane-waiting.
	kubeClient := newTestClient(t)
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, f := range gatherFamilies(t, reg) {
		if f.GetName() == "courier_source_last_success_timestamp_seconds" {
			t.Fatalf("last success family present before any success: %#v", f)
		}
	}
	// A paused poll never calls the source: no attempt timestamp either.
	if _, ok := gaugeSeriesIfPresent(t, reg, "courier_source_last_attempt_timestamp_seconds", map[string]string{"source": "dispatch", "lane": "local"}); ok {
		t.Fatal("last attempt series exists for a paused poll, want absent")
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_paused_total", map[string]string{"source": "dispatch", "lane": "local", "reason": "lane-waiting"}); !ok || v != 1 {
		t.Fatalf("paused_total[lane-waiting] = (%v, %v), want (1, true)", v, ok)
	}

	// The lane appears and the next poll succeeds: the series is born here.
	if err := kubeClient.Create(context.Background(), testLane()); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, ok := gaugeSeriesIfPresent(t, reg, "courier_source_last_success_timestamp_seconds", map[string]string{"source": "dispatch", "lane": "local"}); !ok || v != float64(metricsClock.Unix()) {
		t.Fatalf("last success = (%v, %v), want (%v, true)", v, ok, float64(metricsClock.Unix()))
	}
}

func TestDiscoveryMetricsFreshRegistryStartsAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	kubeClient := newTestClient(t, testLane())
	runner := newInstrumentedRunner(t, reg, kubeClient, &testAdapter{}, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	// No poll has happened: counter series for the binding do not exist, so
	// nothing can leak in from another registry or test.
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_attempts_total", map[string]string{"source": "dispatch", "lane": "local"}); ok {
		t.Fatalf("attempts series on a fresh registry = %v, want absent", v)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_paused_total", map[string]string{"source": "dispatch", "lane": "local", "reason": "lane-waiting"}); ok {
		t.Fatalf("paused_total series on a fresh registry = %v, want absent", v)
	}
	for _, reason := range DiscoveryPauseReasons {
		if v, ok := gaugeSeriesIfPresent(t, reg, "courier_source_discovery_paused", map[string]string{"source": "dispatch", "lane": "local", "reason": reason}); ok && v != 0 {
			t.Fatalf("paused gauge[%s] on a fresh registry = %v, want absent or 0", reason, v)
		}
	}

	// A zero-item success poll leaves the offered counter exactly at zero on
	// this registry: counters count from zero, not from inherited state.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_items_offered_total", map[string]string{"source": "dispatch", "lane": "local"}); !ok || v != 0 {
		t.Fatalf("offered on a fresh registry = (%v, %v), want (0, true)", v, ok)
	}
}
