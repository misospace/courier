package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// observationRecorder is a source adapter that records execution observations
// it is given.
type observationRecorder struct {
	observations []source.ExecutionObservation
	err          error
	hadDeadline  bool
}

func (s *observationRecorder) Discover(context.Context) ([]source.WorkItem, error) {
	return nil, nil
}

func (s *observationRecorder) Claim(context.Context, source.WorkItem) error {
	return nil
}

func (s *observationRecorder) Release(context.Context, source.WorkItem) error {
	return nil
}

func (s *observationRecorder) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}

func (s *observationRecorder) Resolve(context.Context, source.WorkItem) error {
	return nil
}

func (s *observationRecorder) ReportExecution(ctx context.Context, obs source.ExecutionObservation) error {
	if _, ok := ctx.Deadline(); ok {
		s.hadDeadline = true
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
	return &CoderRunReconciler{Sources: NewSourceRegistry(adapters)}
}

func (s *observationRecorder) count() int {
	return len(s.observations)
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

func TestPublishExecutionObservationTransientFailureRetries(t *testing.T) {
	adapter := &observationRecorder{err: errors.New("source unavailable")}
	r := observationReconciler(map[string]source.Adapter{"obs": adapter})
	ctx := context.Background()
	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)

	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseRunning, courierv1alpha1.PhaseRunning,
	} {
		run.Status.Phase = phase
		snap := run.DeepCopy()
		r.publishExecutionObservation(ctx, run)
		if !reflect.DeepEqual(run, snap) {
			t.Fatalf("run mutated by a failed publish in phase %s", phase)
		}
	}
	// A failed report is not recorded, so the next reconcile re-attempts.
	if got := adapter.count(); got != 2 {
		t.Fatalf("reports with a failing source = %d, want 2 (retried, no dedupe record)", got)
	}

	adapter.err = nil
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying, courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseDone, courierv1alpha1.PhaseDone,
	} {
		run.Status.Phase = phase
		snap := run.DeepCopy()
		r.publishExecutionObservation(ctx, run)
		if !reflect.DeepEqual(run, snap) {
			t.Fatalf("run mutated by a publish in phase %s", phase)
		}
	}
	// Recovery records verifying and done; repeats then dedupe.
	if got := adapter.count(); got != 4 {
		t.Fatalf("reports after recovery = %d, want 4 (deduped once recorded)", got)
	}
	if adapter.observations[2].State != source.ExecutionVerifying ||
		adapter.observations[3].State != source.ExecutionSucceeded {
		t.Fatalf("recovery states = %q, %q; want verifying, succeeded",
			adapter.observations[2].State, adapter.observations[3].State)
	}
}

func TestPublishExecutionObservationAdapterWithoutObserver(t *testing.T) {
	r := observationReconciler(map[string]source.Adapter{"test": &admissionSource{}})
	run := observationRun("run1", "default", courierv1alpha1.PhaseRunning)
	run.Spec.Source = "test"
	r.publishExecutionObservation(context.Background(), run)

	r.obsMu.Lock()
	last := r.obsLast[observationKey("default", "run1")]
	degraded := r.obsDegraded["test"]
	r.obsMu.Unlock()
	if last != "" || degraded {
		t.Fatalf("no-observer adapter left state (last=%q, degraded=%v), want none", last, degraded)
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
	if !adapter.hadDeadline {
		t.Fatal("ReportExecution did not receive a bounded context")
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
