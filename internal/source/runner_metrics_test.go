package source

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
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
	return pausedSamplesFromFamilies(families, sourceName, lane)
}

// pausedSamplesFromFamilies extracts the paused-gauge samples of one binding
// from already-gathered families, so a concurrent scraper can assert on a
// snapshot without gathering the registry a second time.
func pausedSamplesFromFamilies(families []*dto.MetricFamily, sourceName, lane string) map[string]float64 {
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

// checkPausedInvariant asserts the current-state invariant on one snapshot of
// the paused gauge: the binding carries one series per reason of the closed
// vocabulary, every series is 0 or 1, and at most one reason is 1 — a paused
// binding has exactly one reason at 1, an unpaused one has none. It returns an
// empty string when the snapshot holds, and the violation otherwise.
func checkPausedInvariant(samples map[string]float64) string {
	if len(samples) != len(DiscoveryPauseReasons) {
		return fmt.Sprintf("paused gauge samples %v, want one series per reason %v", samples, DiscoveryPauseReasons)
	}
	sum := 0.0
	for _, reason := range DiscoveryPauseReasons {
		value, ok := samples[reason]
		if !ok {
			return fmt.Sprintf("paused gauge is missing reason %q: %v", reason, samples)
		}
		if value != 0 && value != 1 {
			return fmt.Sprintf("paused gauge[%s] = %v, want 0 or 1", reason, value)
		}
		sum += value
	}
	for reason, value := range samples {
		if value == 1 && !slices.Contains(DiscoveryPauseReasons, reason) {
			return fmt.Sprintf("paused gauge reason %q is at 1 but outside the closed vocabulary %v", reason, DiscoveryPauseReasons)
		}
	}
	if sum > 1 {
		return fmt.Sprintf("paused gauge samples sum = %v, want at most one reason at 1: %v", sum, samples)
	}
	return ""
}

// checkPausedSum asserts the binding carries one series per reason of the
// closed vocabulary and that the reason series sum to exactly want: one while
// the binding is paused, none while it is not. It returns an empty string when
// the snapshot holds, and the violation otherwise.
func checkPausedSum(samples map[string]float64, want float64) string {
	if len(samples) != len(DiscoveryPauseReasons) {
		return fmt.Sprintf("paused gauge samples %v, want one series per reason %v", samples, DiscoveryPauseReasons)
	}
	sum := 0.0
	for _, reason := range DiscoveryPauseReasons {
		value, ok := samples[reason]
		if !ok {
			return fmt.Sprintf("paused gauge is missing reason %q: %v", reason, samples)
		}
		if value != 0 && value != 1 {
			return fmt.Sprintf("paused gauge[%s] = %v, want 0 or 1", reason, value)
		}
		sum += value
	}
	if sum != want {
		return fmt.Sprintf("paused gauge samples sum = %v, want exactly %v: %v", sum, want, samples)
	}
	return ""
}

// pausedGaugePhase drives one phase of the paused-gauge concurrency test:
// seed puts the binding into the phase's starting state, every worker then
// calls apply — which only pauses or only clears the binding — while the
// scraper gathers the registry. It returns the first invariant violation, or
// an empty string when every snapshot held.
func pausedGaugePhase(t *testing.T, reg *prometheus.Registry, sourceName, laneName string, wantSum float64, seed func(), apply func(worker int)) string {
	t.Helper()
	seed()

	stop := make(chan struct{})
	var writers sync.WaitGroup
	for worker := 0; worker < len(DiscoveryPauseReasons); worker++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				apply(worker)
				// Yield, so the scraper samples the binding while the
				// writers transition it instead of after them.
				runtime.Gosched()
			}
		}()
	}

	const scrapes = 1000
	failure := ""
	for i := 0; i < scrapes && failure == ""; i++ {
		families, err := reg.Gather()
		if err != nil {
			failure = fmt.Sprintf("gather: %v", err)
			break
		}
		samples := pausedSamplesFromFamilies(families, sourceName, laneName)
		if msg := checkPausedSum(samples, wantSum); msg != "" {
			failure = msg
			break
		}
	}
	close(stop)
	writers.Wait()
	return failure
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
	// The fixture never dedupes, so only the validation reasons fire. The
	// duplicate reason belongs to the dedupe path and its series stays
	// absent here.
	for _, reason := range []string{"missing-id", "unsupported-mode", "invalid-ref"} {
		reasonLabels := map[string]string{"source": "test", "lane": "local", "reason": reason}
		if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_skipped_total", reasonLabels); !ok || v != 1 {
			t.Fatalf("skipped[%s] = (%v, %v), want (1, true)", reason, v, ok)
		}
	}
	if _, ok := counterValueIfPresent(t, reg, "courier_source_discovery_skipped_total", map[string]string{"source": "test", "lane": "local", "reason": "duplicate"}); ok {
		t.Fatal("skipped[duplicate] series present, want absent: this fixture never dedupes")
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

// TestDiscoveryMetricsPausedGaugeConcurrentTransitions pins the paused gauge
// while one binding flips between pause reasons and the unpaused state.
//
// The test runs two sequential phases on one binding. Phase 1 only pauses —
// rotating over the closed reason vocabulary — so every snapshot must carry
// exactly one reason at 1 and every reason series must be present. Phase 2
// only clears, so every snapshot must carry no reason at 1 while the series
// stay present.
//
// The old implementation zeroed the non-active reasons and raised the active
// one as separate gauge writes, so a scrape between the writes saw every
// reason at 0: a half-applied transition. Phase 1 catches exactly that — it
// fails on a snapshot with no reason at 1.
func TestDiscoveryMetricsPausedGaugeConcurrentTransitions(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewDiscoveryMetrics(reg)
	const (
		sourceName = "dispatch"
		laneName   = "local"
	)

	// Phase 1: paused. The writers rotate over the reasons of the closed
	// vocabulary, so the active reason keeps changing under the scraper.
	if failure := pausedGaugePhase(t, reg, sourceName, laneName, 1,
		func() { metrics.SetPaused(sourceName, laneName, DiscoveryPauseReasons[0]) },
		func(worker int) {
			metrics.SetPaused(sourceName, laneName, DiscoveryPauseReasons[worker%len(DiscoveryPauseReasons)])
		},
	); failure != "" {
		t.Fatalf("paused gauge invariant violated under concurrent pauses: %s", failure)
	}

	// Phase 2: unpaused. The writers only clear the binding, so every
	// snapshot must show every reason at 0.
	if failure := pausedGaugePhase(t, reg, sourceName, laneName, 0,
		func() { metrics.ClearPaused(sourceName, laneName) },
		func(int) { metrics.ClearPaused(sourceName, laneName) },
	); failure != "" {
		t.Fatalf("paused gauge invariant violated under concurrent clears: %s", failure)
	}
}

// TestDiscoveryMetricsPausedGaugeSharedBindingTwoRunners pins the same
// invariant for two runners bound to one (source, lane) pair: the pair carries
// a single pause state, so the gauge can never show two reasons at 1 for it.
func TestDiscoveryMetricsPausedGaugeSharedBindingTwoRunners(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewDiscoveryMetrics(reg)
	const (
		sourceName = "dispatch"
		laneName   = "local"
	)

	// Two runners on the same binding, in two namespaces: one waits for a
	// LaneProfile that does not exist there, the other polls a suspended
	// lane, so the one binding carries two different pause reasons.
	waiting := NewRunner(newTestClient(t), &testAdapter{}, RunnerConfig{Source: sourceName, LaneProfile: laneName, Namespace: "missing-ns"})
	waiting.Metrics = metrics
	suspendedLane := testLane()
	suspendedLane.Namespace = "suspended-ns"
	suspendedLane.Annotations = map[string]string{courierv1alpha1.SuspendAnnotation: "true"}
	suspended := NewRunner(newTestClient(t, suspendedLane), &testAdapter{}, RunnerConfig{Source: sourceName, LaneProfile: laneName, Namespace: "suspended-ns"})
	suspended.Metrics = metrics
	runners := []*Runner{waiting, suspended}

	// Seed the binding, so its series exist before the scraper starts.
	for _, runner := range runners {
		if err := runner.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	violation := make(chan string, 1)
	done := make(chan struct{})
	scraped := make(chan struct{})
	go func() {
		defer close(scraped)
		for {
			select {
			case <-done:
				return
			default:
			}
			families, err := reg.Gather()
			if err != nil {
				select {
				case violation <- fmt.Sprintf("gather: %v", err):
				default:
				}
				return
			}
			samples := pausedSamplesFromFamilies(families, sourceName, laneName)
			if msg := checkPausedInvariant(samples); msg != "" {
				select {
				case violation <- msg:
				default:
				}
				return
			}
		}
	}()

	const rounds = 300
	for round := 0; round < rounds; round++ {
		var polls sync.WaitGroup
		for _, runner := range runners {
			polls.Add(1)
			go func(runner *Runner) {
				defer polls.Done()
				if err := runner.Poll(context.Background()); err != nil {
					t.Errorf("poll: %v", err)
				}
			}(runner)
		}
		polls.Wait()
	}
	close(done)
	<-scraped

	select {
	case msg := <-violation:
		t.Fatalf("paused gauge invariant violated for a shared binding: %s", msg)
	default:
	}
	// The settled state of the shared binding is a single reason at 1.
	samples := pausedGaugeSamples(t, reg, sourceName, laneName)
	sum := 0.0
	for _, value := range samples {
		sum += value
	}
	if sum > 1 {
		t.Fatalf("paused gauge samples %v sum to %v, want at most one reason at 1", samples, sum)
	}
}

// discoveryLabelSets pins the label keys every discovery family carries. The
// binding families carry source and lane; the per-reason families carry
// source, lane, and reason. Nothing else may appear on them.
var discoveryLabelSets = map[string][]string{
	"courier_source_discovery_attempts_total":       {"source", "lane"},
	"courier_source_discovery_successes_total":      {"source", "lane"},
	"courier_source_discovery_failures_total":       {"source", "lane"},
	"courier_source_items_offered_total":            {"source", "lane"},
	"courier_source_items_materialized_total":       {"source", "lane"},
	"courier_source_last_attempt_timestamp_seconds": {"source", "lane"},
	"courier_source_last_success_timestamp_seconds": {"source", "lane"},
	"courier_source_discovery_skipped_total":        {"source", "lane", "reason"},
	"courier_source_discovery_paused_total":         {"source", "lane", "reason"},
	"courier_source_discovery_paused":               {"source", "lane", "reason"},
}

// discoveryReasonVocabularies pins the closed vocabulary of every reason
// label the discovery layer emits.
var discoveryReasonVocabularies = map[string][]string{
	"courier_source_discovery_skipped_total": DiscoverySkipReasons,
	"courier_source_discovery_paused_total":  DiscoveryPauseReasons,
	"courier_source_discovery_paused":        DiscoveryPauseReasons,
}

// assertDiscoveryLabelVocabulary pins the label vocabulary of the discovery
// layer: every courier_source_* family carries exactly its documented label
// keys, the source and lane labels name the binding the runner was built
// with, and every reason label is a member of that family's closed
// vocabulary.
func assertDiscoveryLabelVocabulary(t *testing.T, families []*dto.MetricFamily, sourceName, lane string) {
	t.Helper()
	seen := 0
	for _, f := range families {
		name := f.GetName()
		if !strings.HasPrefix(name, "courier_source_") {
			continue
		}
		wantKeys, ok := discoveryLabelSets[name]
		if !ok {
			t.Fatalf("discovery family %q is not part of the documented signal set", name)
		}
		vocabulary, hasReason := discoveryReasonVocabularies[name]
		for _, m := range f.GetMetric() {
			seen++
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				if _, duplicate := labels[lp.GetName()]; duplicate {
					t.Fatalf("family %q repeats label %q", name, lp.GetName())
				}
				labels[lp.GetName()] = lp.GetValue()
			}
			if len(labels) != len(wantKeys) {
				t.Fatalf("family %q carries labels %v, want exactly %v", name, labels, wantKeys)
			}
			for _, key := range wantKeys {
				if _, ok := labels[key]; !ok {
					t.Fatalf("family %q is missing label %q: %v", name, key, labels)
				}
			}
			if labels["source"] != sourceName {
				t.Fatalf("family %q source label = %q, want %q", name, labels["source"], sourceName)
			}
			if labels["lane"] != lane {
				t.Fatalf("family %q lane label = %q, want %q", name, labels["lane"], lane)
			}
			if hasReason && !slices.Contains(vocabulary, labels["reason"]) {
				t.Fatalf("family %q reason label = %q, want a member of %v", name, labels["reason"], vocabulary)
			}
		}
	}
	if seen == 0 {
		t.Fatal("no discovery metrics gathered, want the discovery families")
	}
}

// TestDiscoveryMetricsLabelVocabulary pins the label vocabulary of the
// discovery layer: the source and lane labels name the binding the runner
// was built with, and every reason label is a member of its closed
// vocabulary.
func TestDiscoveryMetricsLabelVocabulary(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewDiscoveryMetrics(reg)
	const (
		sourceName = "dispatch"
		laneName   = "local"
	)

	// A successful poll with a skipped item: exercises the binding families
	// and the skip-reason vocabulary.
	adapter := &testAdapter{items: []WorkItem{
		{ID: "", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1},
		{ID: "unsupported", Mode: "unknown", Repo: "acme/widgets", Ref: 1},
		{ID: "invalid-ref", Mode: "resolve-issue", Repo: "", Ref: 0},
		{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1},
	}}
	runner := NewRunner(newTestClient(t, testLane()), adapter, RunnerConfig{Source: sourceName, LaneProfile: laneName, Namespace: "courier"})
	runner.Metrics = metrics
	runner.Now = func() time.Time { return metricsClock }
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A failing poll: exercises the failure family.
	failing := NewRunner(newTestClient(t, testLane()), &testAdapter{err: errors.New("upstream failure")}, RunnerConfig{Source: sourceName, LaneProfile: laneName, Namespace: "courier"})
	failing.Metrics = metrics
	failing.Now = func() time.Time { return metricsClock }
	if err := failing.Poll(context.Background()); err == nil {
		t.Fatal("failing poll returned no error")
	}

	// A paused poll: exercises the paused families.
	paused := NewRunner(newTestClient(t), &testAdapter{}, RunnerConfig{Source: sourceName, LaneProfile: laneName, Namespace: "courier"})
	paused.Metrics = metrics
	paused.Now = func() time.Time { return metricsClock }
	if err := paused.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	families := gatherFamilies(t, reg)
	assertDiscoveryLabelVocabulary(t, families, sourceName, laneName)

	// Every documented family was gathered, so the vocabulary check above
	// covered the whole discovery layer and not just part of it.
	gathered := map[string]bool{}
	for _, f := range families {
		gathered[f.GetName()] = true
	}
	for name := range discoveryLabelSets {
		if !gathered[name] {
			t.Fatalf("family %q absent from the registry, want it gathered", name)
		}
	}
}
