package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// observationBase is a plain source adapter with no optional capabilities.
type observationBase struct{}

func (s *observationBase) Discover(context.Context) ([]source.WorkItem, error) {
	return nil, nil
}

func (s *observationBase) Claim(context.Context, source.WorkItem) error {
	return nil
}

func (s *observationBase) Release(context.Context, source.WorkItem) error {
	return nil
}

func (s *observationBase) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}

func (s *observationBase) Resolve(context.Context, source.WorkItem) error {
	return nil
}

// observationRecorder is an observationBase that records the execution
// observations it is given, along with the deadline of each call's context.
type observationRecorder struct {
	observationBase
	observations []source.ExecutionObservation
	err          error
	deadline     time.Time
}

func (s *observationRecorder) ReportExecution(ctx context.Context, obs source.ExecutionObservation) error {
	if d, ok := ctx.Deadline(); ok {
		s.deadline = d
	}
	s.observations = append(s.observations, obs)
	return s.err
}

// observationIdentifierAdapter is an observationRecorder with the optional
// WorkIdentifier capability.
type observationIdentifierAdapter struct {
	observationRecorder
}

func (s *observationIdentifierAdapter) WorkIdentity(workItemID string) string {
	return "identity:" + workItemID
}

func (s *observationRecorder) count() int {
	return len(s.observations)
}

func observationRun(name, namespace string, phase courierv1alpha1.Phase) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("uid-" + name)},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Repo:       "acme/widgets",
			Ref:        1,
			Lane:       "lane",
			Source:     "obs",
			WorkItemID: "work-1",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: phase, PR: "acme/widgets#42"},
	}
}

func observationReconciler(adapters map[string]source.Adapter) *CoderRunReconciler {
	return &CoderRunReconciler{
		Sources: NewSourceRegistry(adapters),
		Now:     func() time.Time { return time.Now().UTC() },
	}
}

func TestExecutionObservationStateMapping(t *testing.T) {
	tests := []struct {
		phase courierv1alpha1.Phase
		state source.ExecutionState
		ok    bool
	}{
		{"", "", false},
		{courierv1alpha1.PhasePending, "", false},
		{courierv1alpha1.PhaseClaimed, "", false},
		{courierv1alpha1.PhaseRunning, source.ExecutionRunning, true},
		{courierv1alpha1.PhaseVerifying, source.ExecutionVerifying, true},
		{courierv1alpha1.PhaseAwaitingReview, source.ExecutionAwaitingReview, true},
		{courierv1alpha1.PhaseDone, source.ExecutionSucceeded, true},
		{courierv1alpha1.PhaseFailed, source.ExecutionFailed, true},
		{courierv1alpha1.PhaseNeedsHuman, source.ExecutionBlocked, true},
		{courierv1alpha1.PhaseBlockedExternal, source.ExecutionBlockedExternal, true},
		{courierv1alpha1.PhaseHandedOff, source.ExecutionHandedOff, true},
	}
	for _, tc := range tests {
		state, ok := executionObservationState(tc.phase)
		if state != tc.state || ok != tc.ok {
			t.Errorf("executionObservationState(%q) = (%q, %v), want (%q, %v)",
				tc.phase, state, ok, tc.state, tc.ok)
		}
	}
}

func TestPublishExecutionObservationReportsStateChangesOnly(t *testing.T) {
	adapter := &observationRecorder{}
	r := observationReconciler(map[string]source.Adapter{"obs": adapter})
	ctx := context.Background()
	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)

	r.publishExecutionObservation(ctx, run)
	r.publishExecutionObservation(ctx, run)
	if got := adapter.count(); got != 1 {
		t.Fatalf("reports after running plus same-state re-reconcile = %d, want 1", got)
	}
	if adapter.observations[0].State != source.ExecutionRunning {
		t.Fatalf("first state = %q, want running", adapter.observations[0].State)
	}

	run.Status.Phase = courierv1alpha1.PhaseVerifying
	r.publishExecutionObservation(ctx, run)
	r.publishExecutionObservation(ctx, run)
	if got := adapter.count(); got != 2 {
		t.Fatalf("reports after verifying = %d, want 2", got)
	}
	if adapter.observations[1].State != source.ExecutionVerifying {
		t.Fatalf("second state = %q, want verifying", adapter.observations[1].State)
	}

	run.Status.Phase = courierv1alpha1.PhaseDone
	r.publishExecutionObservation(ctx, run)
	r.publishExecutionObservation(ctx, run)
	if got := adapter.count(); got != 3 {
		t.Fatalf("reports after done = %d, want 3", got)
	}
	if adapter.observations[2].State != source.ExecutionSucceeded {
		t.Fatalf("third state = %q, want succeeded", adapter.observations[2].State)
	}
}

func TestPublishExecutionObservationUnsupportedDegradesSourceOnly(t *testing.T) {
	obs := &observationRecorder{err: source.ErrUnsupportedExecutionObservations}
	other := &observationRecorder{}
	r := observationReconciler(map[string]source.Adapter{"obs": obs, "other": other})
	ctx := context.Background()

	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	r.publishExecutionObservation(ctx, run)
	run.Status.Phase = courierv1alpha1.PhaseVerifying
	r.publishExecutionObservation(ctx, run)
	if got := obs.count(); got != 1 {
		t.Fatalf("reports to a degraded source = %d, want 1 (latched)", got)
	}

	otherRun := observationRun("run2", "default", courierv1alpha1.PhaseRunning)
	otherRun.Spec.Source = "other"
	r.publishExecutionObservation(ctx, otherRun)
	if got := other.count(); got != 1 {
		t.Fatalf("reports to the second source = %d, want 1 (unaffected)", got)
	}
}

// TestPublishExecutionObservationTransientFailureCooldown pins the bounded
// retry policy: a failed report costs at most one attempt per run per cooldown
// window, the window holds back only a repeat of the exact attempt (UID and
// state) whose report failed, a UID or state change always bypasses it, and a
// successful report clears it.
func TestPublishExecutionObservationTransientFailureCooldown(t *testing.T) {
	adapter := &observationRecorder{}
	r := observationReconciler(map[string]source.Adapter{"obs": adapter})
	ctx := context.Background()

	first := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	snap := first.DeepCopy()
	r.publishExecutionObservation(ctx, first)
	if !reflect.DeepEqual(first, snap) {
		t.Fatal("run mutated by a publish")
	}
	if adapter.count() != 1 || adapter.observations[0].State != source.ExecutionRunning {
		t.Fatalf("initial report = %d (state %q), want 1 (running)",
			adapter.count(), adapter.observations[0].State)
	}

	// A recreated run is a new attempt: its same-state report fails.
	recreated := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	recreated.UID = types.UID("uid-run1-recreated")
	adapter.err = errors.New("source unavailable")
	snap = recreated.DeepCopy()
	r.publishExecutionObservation(ctx, recreated)
	if !reflect.DeepEqual(recreated, snap) {
		t.Fatal("run mutated by a failed publish")
	}
	if adapter.count() != 2 {
		t.Fatalf("reports after recreation = %d, want 2", adapter.count())
	}

	// The failed report's cooldown suppresses the next repeat in the window.
	r.publishExecutionObservation(ctx, recreated)
	if adapter.count() != 2 {
		t.Fatalf("reports within the cooldown window = %d, want 2 (suppressed)", adapter.count())
	}

	// After the window passes, the repeat is attempted again — and fails.
	key := observationKey("default", "run1")
	r.obsMu.Lock()
	entry := r.obsRetry[key]
	entry.notBefore = time.Now().Add(-time.Second)
	r.obsRetry[key] = entry
	r.obsMu.Unlock()
	r.publishExecutionObservation(ctx, recreated)
	if adapter.count() != 3 {
		t.Fatalf("reports after the window = %d, want 3", adapter.count())
	}

	// A real state change bypasses the fresh cooldown the last failure set:
	// the endpoint is still failing, so only the bypass explains this attempt.
	recreated.Status.Phase = courierv1alpha1.PhaseVerifying
	r.publishExecutionObservation(ctx, recreated)
	if adapter.count() != 4 {
		t.Fatalf("reports after a state change = %d, want 4 (bypassed the cooldown)", adapter.count())
	}

	// The bypassed state's own failure now holds its window: a repeat of the
	// same failed (UID, state) pair is throttled, so a wedged endpoint cannot
	// spend one bounded attempt per reconcile on the run's new state.
	r.publishExecutionObservation(ctx, recreated)
	if adapter.count() != 4 {
		t.Fatalf("repeats of the failed verifying report = %d, want 4 (the failed pair is throttled)", adapter.count())
	}

	// Once the window passes and the endpoint recovers, the verifying state is
	// reported and the window clears.
	r.obsMu.Lock()
	entry = r.obsRetry[key]
	entry.notBefore = time.Now().Add(-time.Second)
	r.obsRetry[key] = entry
	r.obsMu.Unlock()
	adapter.err = nil
	snap = recreated.DeepCopy()
	r.publishExecutionObservation(ctx, recreated)
	if !reflect.DeepEqual(recreated, snap) {
		t.Fatal("run mutated by a publish")
	}
	if adapter.count() != 5 {
		t.Fatalf("reports after recovery = %d, want 5", adapter.count())
	}
	if adapter.observations[4].State != source.ExecutionVerifying {
		t.Fatalf("recovered state = %q, want verifying", adapter.observations[4].State)
	}
	r.obsMu.Lock()
	_, hasRetry := r.obsRetry[key]
	r.obsMu.Unlock()
	if hasRetry {
		t.Fatal("a successful report must clear the run's failure window")
	}
}

// TestPublishExecutionObservationRecreatedAttemptReportsSameState pins the
// stale-marker fix: a recreated run (same namespace/name, new UID) is a new
// attempt and must report even when it coalesces into the same state its
// predecessor already reported in this process.
func TestPublishExecutionObservationRecreatedAttemptReportsSameState(t *testing.T) {
	adapter := &observationRecorder{}
	r := observationReconciler(map[string]source.Adapter{"obs": adapter})
	ctx := context.Background()

	r.publishExecutionObservation(ctx, observationRun("run1", "default", courierv1alpha1.PhaseRunning))
	recreated := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	recreated.UID = types.UID("uid-run1-recreated")
	r.publishExecutionObservation(ctx, recreated)

	if got := adapter.count(); got != 2 {
		t.Fatalf("reports for a recreated attempt in the same state = %d, want 2 (a UID change never dedupes)", got)
	}
	if adapter.observations[1].RunUID != "uid-run1-recreated" {
		t.Fatalf("recreated RunUID = %q, want uid-run1-recreated", adapter.observations[1].RunUID)
	}
}

func TestPublishExecutionObservationAdapterWithoutObserver(t *testing.T) {
	r := observationReconciler(map[string]source.Adapter{"test": &admissionSource{}})
	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	run.Spec.Source = "test"
	r.publishExecutionObservation(context.Background(), run)

	r.obsMu.Lock()
	_, hasLast := r.obsLast[observationKey("default", "run1")]
	_, hasRetry := r.obsRetry[observationKey("default", "run1")]
	degraded := r.obsDegraded["test"]
	r.obsMu.Unlock()
	if hasLast || hasRetry || degraded {
		t.Fatalf("no-observer adapter left state (last=%v, retry=%v, degraded=%v), want none",
			hasLast, hasRetry, degraded)
	}
}

func TestPublishExecutionObservationWorkIdentity(t *testing.T) {
	ctx := context.Background()

	plain := &observationRecorder{}
	r := observationReconciler(map[string]source.Adapter{"obs": plain})
	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	r.publishExecutionObservation(ctx, run)
	if got := plain.observations[0].WorkIdentity; got != "work-1" {
		t.Fatalf("WorkIdentity without identifier = %q, want the work item ID", got)
	}
	if got := plain.observations[0].WorkItemID; got != "work-1" {
		t.Fatalf("WorkItemID = %q, want work-1", got)
	}

	identified := &observationIdentifierAdapter{}
	r = observationReconciler(map[string]source.Adapter{"obs": identified})
	run = observationRun("run2", "default", courierv1alpha1.PhaseRunning)
	r.publishExecutionObservation(ctx, run)
	if got := identified.observations[0].WorkIdentity; got != "identity:work-1" {
		t.Fatalf("WorkIdentity with identifier = %q, want identity:work-1", got)
	}
}

// TestRestartExecutionObservationRepublishes pins that a fresh reconciler —
// the shape of a process restart, with no process-local memory — re-publishes
// the run's current observation, which converges server-side because reports
// are idempotent current-state upserts.
func TestRestartExecutionObservationRepublishes(t *testing.T) {
	adapter := &observationRecorder{}
	ctx := context.Background()
	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)

	first := observationReconciler(map[string]source.Adapter{"obs": adapter})
	first.publishExecutionObservation(ctx, run)
	if adapter.count() != 1 {
		t.Fatalf("reports from the first process = %d, want 1", adapter.count())
	}

	restarted := observationReconciler(map[string]source.Adapter{"obs": adapter})
	restarted.publishExecutionObservation(ctx, run)
	if adapter.count() != 2 {
		t.Fatalf("reports after a restart = %d, want 2 (re-published)", adapter.count())
	}
	if adapter.observations[1].State != source.ExecutionRunning {
		t.Fatalf("re-published state = %q, want running", adapter.observations[1].State)
	}
}

func TestForgetExecutionObservationClearsState(t *testing.T) {
	adapter := &observationRecorder{}
	r := observationReconciler(map[string]source.Adapter{"obs": adapter})
	ctx := context.Background()
	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)

	r.publishExecutionObservation(ctx, run)
	r.publishExecutionObservation(ctx, run)
	if got := adapter.count(); got != 1 {
		t.Fatalf("reports before forget = %d, want 1", got)
	}

	// A recreated run with the same name must re-report once its predecessor
	// is deleted.
	r.forgetExecutionObservation(types.NamespacedName{Namespace: "default", Name: "run1"})
	recreated := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	r.publishExecutionObservation(ctx, recreated)
	if got := adapter.count(); got != 2 {
		t.Fatalf("reports after delete plus recreate = %d, want 2", got)
	}
}

func TestPublishExecutionObservationFields(t *testing.T) {
	adapter := &observationRecorder{}
	r := observationReconciler(map[string]source.Adapter{"obs": adapter})
	run := observationRun("run1", "default", courierv1alpha1.PhaseAwaitingReview)

	r.publishExecutionObservation(context.Background(), run)
	if got := adapter.count(); got != 1 {
		t.Fatalf("reports = %d, want 1", got)
	}
	if adapter.deadline.IsZero() {
		t.Fatal("ReportExecution did not receive a bounded context")
	}
	// The attempt's deadline must be within one timeout from now: it bounds
	// this single attempt, not the retry policy.
	if elapsed := time.Since(adapter.deadline); elapsed > executionObservationTimeout {
		t.Errorf("deadline is %v from now, want within %v", elapsed, executionObservationTimeout)
	}
	obs := adapter.observations[0]
	if obs.RunUID != "uid-run1" {
		t.Errorf("RunUID = %q, want uid-run1", obs.RunUID)
	}
	if obs.RunNamespace != "default" || obs.RunName != "run1" {
		t.Errorf("RunNamespace/RunName = %q/%q, want default/run1", obs.RunNamespace, obs.RunName)
	}
	if obs.WorkItemID != "work-1" {
		t.Errorf("WorkItemID = %q, want work-1", obs.WorkItemID)
	}
	if obs.Mode != "resolve-issue" {
		t.Errorf("Mode = %q, want resolve-issue", obs.Mode)
	}
	if obs.Source != "obs" {
		t.Errorf("Source = %q, want obs", obs.Source)
	}
	if obs.State != source.ExecutionAwaitingReview {
		t.Errorf("State = %q, want awaiting-review", obs.State)
	}
	if obs.PR != "acme/widgets#42" {
		t.Errorf("PR = %q, want acme/widgets#42", obs.PR)
	}
	if obs.ObservedAt.IsZero() {
		t.Error("ObservedAt is zero")
	}
}

// TestReconcileOutcomeInvarianceUnderExecutionObservationFailure drives the
// real Reconcile through the fake-client harness: a parked AwaitingReview run
// with no settled LifecycleReported condition deterministically returns no
// result and no error. The only difference between the two setups is the
// adapter's ExecutionObserver capability — the failing setup pays the bounded
// telemetry attempt — and its Reconcile outcome must equal the no-observer
// setup's.
func TestReconcileOutcomeInvarianceUnderExecutionObservationFailure(t *testing.T) {
	ctx := context.Background()
	name := "obs-invariant-run"

	mkRun := func() *courierv1alpha1.CoderRun {
		run := admissionRun(name, "lane", courierv1alpha1.PhaseAwaitingReview)
		run.UID = types.UID("uid-obs-invariant")
		return run
	}

	plain := phaseClient(t, mkRun())
	plainReconciler := observationReconciler(map[string]source.Adapter{"test": &observationBase{}})
	plainReconciler.Client = plain
	plainResult, plainErr := plainReconciler.Reconcile(ctx, admissionRequest(name))

	failing := &observationRecorder{err: errors.New("observation endpoint wedged")}
	failingClient := phaseClient(t, mkRun())
	failingReconciler := observationReconciler(map[string]source.Adapter{"test": failing})
	failingReconciler.Client = failingClient
	failingResult, failingErr := failingReconciler.Reconcile(ctx, admissionRequest(name))

	if failing.count() != 1 {
		t.Fatalf("failing observer attempted %d times, want 1 (the failure path must be exercised)",
			failing.count())
	}
	if !reflect.DeepEqual(plainResult, failingResult) {
		t.Fatalf("Reconcile result changed with a failing observer: %v vs %v",
			plainResult, failingResult)
	}
	if plainErr != nil || failingErr != nil {
		t.Fatalf("Reconcile returned errors: plain=%v, failing=%v", plainErr, failingErr)
	}
	if plainResult != (ctrl.Result{}) {
		t.Fatalf("parked awaiting-review run reconciled to %+v, want no result", plainResult)
	}
}
