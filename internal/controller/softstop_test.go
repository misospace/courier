package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// frozenClock returns a time source fixed at now so soft-stop tests never
// sleep and the requestedAt timestamp is deterministic.
func frozenClock(now time.Time) func() time.Time {
	return func() time.Time { return now }
}

// softStopReconciler builds a legacy reconciler that writes operator status
// through the real JSON merge-patch path (StatusWriter left nil), so the
// control request is persisted exactly as a deployment would.
func softStopReconciler(t *testing.T, c client.Client, now time.Time) *CoderRunReconciler {
	t.Helper()
	return &CoderRunReconciler{
		Client:  c,
		Now:     frozenClock(now),
		Sources: NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
	}
}

// getRun reads a run back from the client.
func getRun(t *testing.T, c client.Client, name string) *courierv1alpha1.CoderRun {
	t.Helper()
	var run courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: admissionNamespace, Name: name}, &run); err != nil {
		t.Fatalf("get run %q: %v", name, err)
	}
	return &run
}

// liveCoordinator is a running, non-deleting coordinator pod for run with a
// stable UID, so a stamp has a live incarnation to fence to.
func liveCoordinator(run *courierv1alpha1.CoderRun, uid string) *corev1.Pod {
	pod := runningCoordinatorPod(run)
	pod.UID = types.UID(uid)
	return pod
}

// softStopRequest builds a control request as the operator would stamp it.
func softStopRequest(id, podUID string, at time.Time) *courierv1alpha1.ControlRequest {
	return &courierv1alpha1.ControlRequest{
		ID:           id,
		Kind:         courierv1alpha1.ControlRequestSoftStop,
		TargetPodUID: podUID,
		RequestedAt:  metav1.NewTime(at),
	}
}

func TestSoftStopStampsRecordWithLiveCoordinatorUID(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: "req-123"}
	pod := liveCoordinator(run, "11111111-2222-3333-4444-555555555555")
	c := phaseClient(t, run, pod)
	r := softStopReconciler(t, c, now)

	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := getRun(t, c, "run").Status.ControlRequest
	if got == nil {
		t.Fatalf("controlRequest = nil, want a stamped record")
	}
	if got.ID != "req-123" {
		t.Fatalf("record ID = %q, want req-123", got.ID)
	}
	if got.Kind != courierv1alpha1.ControlRequestSoftStop {
		t.Fatalf("record Kind = %q, want %q", got.Kind, courierv1alpha1.ControlRequestSoftStop)
	}
	if got.TargetPodUID != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("record TargetPodUID = %q, want the live coordinator UID", got.TargetPodUID)
	}
	if !got.RequestedAt.Time.Equal(now) {
		t.Fatalf("record RequestedAt = %v, want %v", got.RequestedAt, now)
	}
}

func TestSoftStopSameValueDoesNotRestamp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stamp := now.Add(-5 * time.Minute)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: "req-123"}
	run.Status.ControlRequest = &courierv1alpha1.ControlRequest{
		ID:           "req-123",
		Kind:         courierv1alpha1.ControlRequestSoftStop,
		TargetPodUID: "00000000-0000-0000-0000-000000000old",
		RequestedAt:  metav1.NewTime(stamp),
	}
	pod := liveCoordinator(run, "11111111-2222-3333-4444-555555555555")
	c := phaseClient(t, run, pod)
	r := softStopReconciler(t, c, now)

	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := getRun(t, c, "run").Status.ControlRequest
	if got == nil {
		t.Fatalf("controlRequest = nil, want the existing record preserved")
	}
	if !got.RequestedAt.Time.Equal(stamp) {
		t.Fatalf("record RequestedAt = %v, want the original %v (no re-stamp)", got.RequestedAt, stamp)
	}
	if got.TargetPodUID != "00000000-0000-0000-0000-000000000old" {
		t.Fatalf("record TargetPodUID = %q, want the original (no re-targeting)", got.TargetPodUID)
	}
}

func TestSoftStopChangedValueReplacesAndClearsCondition(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: "req-new"}
	run.Status.ControlRequest = softStopRequest("req-old", "old-pod", now.Add(-5*time.Minute))
	// A running run carries other conditions; keeping one here means removing
	// SoftStopObserved leaves a non-empty array that the conditions merge patch
	// can actually clear (an empty slice is dropped by omitempty).
	run.Status.Conditions = []metav1.Condition{
		{
			Type:               courierv1alpha1.ConditionSoftStopObserved,
			Status:             metav1.ConditionTrue,
			Reason:             courierv1alpha1.ConditionReasonUnacknowledged,
			Message:            softStopDisarmMessage("req-old"),
			LastTransitionTime: metav1.NewTime(now.Add(-4 * time.Minute)),
		},
		{
			Type:               courierv1alpha1.ConditionLifecycleReported,
			Status:             metav1.ConditionFalse,
			Reason:             "Pending",
			Message:            "terminal report not yet published",
			LastTransitionTime: metav1.NewTime(now.Add(-4 * time.Minute)),
		},
	}
	pod := liveCoordinator(run, "11111111-2222-3333-4444-555555555555")
	c := phaseClient(t, run, pod)
	r := softStopReconciler(t, c, now)

	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := getRun(t, c, "run")
	if got.Status.ControlRequest == nil {
		t.Fatalf("controlRequest = nil, want the replacement record")
	}
	if got.Status.ControlRequest.ID != "req-new" {
		t.Fatalf("record ID = %q, want req-new", got.Status.ControlRequest.ID)
	}
	if got.Status.ControlRequest.TargetPodUID != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("record TargetPodUID = %q, want the live coordinator UID", got.Status.ControlRequest.TargetPodUID)
	}
	if cond := apimeta.FindStatusCondition(got.Status.Conditions, courierv1alpha1.ConditionSoftStopObserved); cond != nil {
		t.Fatalf("SoftStopObserved condition survived a fresh annotation: %+v", cond)
	}
}

func TestSoftStopClearedAnnotationClearsRecord(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// The annotation is absent (cleared) while a record is still recorded.
	run.Status.ControlRequest = softStopRequest("req-123", "11111111-2222-3333-4444-555555555555", now.Add(-5*time.Minute))
	pod := liveCoordinator(run, "11111111-2222-3333-4444-555555555555")
	c := phaseClient(t, run, pod)
	r := softStopReconciler(t, c, now)

	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getRun(t, c, "run").Status.ControlRequest; got != nil {
		t.Fatalf("controlRequest = %+v, want nil after the annotation is cleared", got)
	}
}

func TestSoftStopNoLivePodDoesNotStamp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: "req-123"}
	// No coordinator pod: there is no live incarnation to fence the request to.
	c := phaseClient(t, run)
	r := softStopReconciler(t, c, now)

	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getRun(t, c, "run").Status.ControlRequest; got != nil {
		t.Fatalf("controlRequest = %+v, want nil (no live coordinator to address it to)", got)
	}
}

func TestSoftStopRelaunchUnacknowledgedClearsAndObserves(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = 1
	run.Status.ControlRequest = softStopRequest("req-1", "11111111-2222-3333-4444-555555555555", now.Add(-5*time.Minute))
	// No acknowledgement from the named incarnation.
	c := phaseClient(t, run)
	r := softStopReconciler(t, c, now)

	if _, err := r.relaunchAfterInfraLoss(context.Background(), run, podLostReason, podLostEventReason, nil); err != nil {
		t.Fatalf("relaunchAfterInfraLoss: %v", err)
	}
	if run.Status.ControlRequest != nil {
		t.Fatalf("controlRequest = %+v, want cleared after relaunch", run.Status.ControlRequest)
	}
	cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionSoftStopObserved)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != courierv1alpha1.ConditionReasonUnacknowledged {
		t.Fatalf("SoftStopObserved = %+v, want True/Unacknowledged", cond)
	}
	if cond.Message != softStopDisarmMessage("req-1") {
		t.Fatalf("SoftStopObserved message = %q, want the disarmed value %q recorded", cond.Message, "req-1")
	}
	if run.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %s, want Claimed after an infra relaunch", run.Status.Phase)
	}
	// The disarm must survive the actual merge-patch write, not just live
	// in memory: read the run back from the API.
	got := getRun(t, c, "run")
	if got.Status.ControlRequest != nil {
		t.Fatalf("persisted controlRequest = %+v, want cleared", got.Status.ControlRequest)
	}
	if cond := apimeta.FindStatusCondition(got.Status.Conditions, courierv1alpha1.ConditionSoftStopObserved); cond == nil || cond.Reason != courierv1alpha1.ConditionReasonUnacknowledged {
		t.Fatalf("persisted SoftStopObserved = %+v, want True/Unacknowledged", cond)
	}
}

func TestSoftStopRelaunchAcknowledgedClearsSilently(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = 1
	run.Status.ControlRequest = softStopRequest("req-1", "11111111-2222-3333-4444-555555555555", now.Add(-5*time.Minute))
	// The named incarnation acknowledged the request before it was torn down.
	run.Status.ControlRequestAck = &courierv1alpha1.ControlRequestAck{
		ID: "req-1",
		At: metav1.NewTime(now.Add(-2 * time.Minute)),
	}
	c := phaseClient(t, run)
	r := softStopReconciler(t, c, now)

	if _, err := r.relaunchAfterInfraLoss(context.Background(), run, podLostReason, podLostEventReason, nil); err != nil {
		t.Fatalf("relaunchAfterInfraLoss: %v", err)
	}
	if run.Status.ControlRequest != nil {
		t.Fatalf("controlRequest = %+v, want cleared after relaunch", run.Status.ControlRequest)
	}
	// The acknowledgement survives as the settled-value memory: the same
	// annotation must not stamp the replacement.
	if run.Status.ControlRequestAck == nil || run.Status.ControlRequestAck.ID != "req-1" {
		t.Fatalf("controlRequestAck = %+v, want the settled ack retained", run.Status.ControlRequestAck)
	}
	if cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionSoftStopObserved); cond != nil {
		t.Fatalf("SoftStopObserved = %+v, want none for an acknowledged request", cond)
	}
	if run.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %s, want Claimed after an infra relaunch", run.Status.Phase)
	}
	if got := getRun(t, c, "run"); got.Status.ControlRequest != nil {
		t.Fatalf("persisted controlRequest = %+v, want cleared", got.Status.ControlRequest)
	}
}

// TestSoftStopDisarmedValueNeverRetargets pins the fence across a relaunch:
// a replacement incarnation is never handed its predecessor's request. The
// same annotation value stays disarmed while its observation stands, and a
// fresh request requires a new value — or a withdrawal and re-add.
func TestSoftStopDisarmedValueNeverRetargets(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: "req-1"}
	run.Status.Restarts = 1
	run.Status.ControlRequest = softStopRequest("req-1", "00000000-0000-0000-0000-000000000old", now.Add(-5*time.Minute))
	c := phaseClient(t, run)
	r := softStopReconciler(t, c, now)
	if _, err := r.relaunchAfterInfraLoss(context.Background(), run, podLostReason, podLostEventReason, nil); err != nil {
		t.Fatalf("relaunchAfterInfraLoss: %v", err)
	}
	if got := getRun(t, c, "run"); got.Status.ControlRequest != nil {
		t.Fatalf("persisted controlRequest = %+v, want disarmed", got.Status.ControlRequest)
	}

	// The replacement reaches Running with the same annotation value: it
	// must not be handed the predecessor's request.
	replacement := getRun(t, c, "run")
	replacement.Status.Phase = courierv1alpha1.PhaseRunning
	replacement.ResourceVersion = ""
	c2 := phaseClient(t, replacement, liveCoordinator(replacement, "22222222-2222-3333-4444-555555555555"))
	r2 := softStopReconciler(t, c2, now)
	if err := r2.syncSoftStop(context.Background(), replacement, []corev1.Pod{*liveCoordinator(replacement, "22222222-2222-3333-4444-555555555555")}); err != nil {
		t.Fatalf("syncSoftStop: %v", err)
	}
	if got := getRun(t, c2, "run"); got.Status.ControlRequest != nil {
		t.Fatalf("replacement was handed the predecessor's request: %+v", got.Status.ControlRequest)
	}

	// Withdrawing the annotation disarms the memory; re-adding the same
	// value is then a fresh request.
	withdrawn := getRun(t, c2, "run")
	withdrawn.Annotations = nil
	if err := r2.syncSoftStop(context.Background(), withdrawn, []corev1.Pod{*liveCoordinator(withdrawn, "22222222-2222-3333-4444-555555555555")}); err != nil {
		t.Fatalf("syncSoftStop withdraw: %v", err)
	}
	readded := getRun(t, c2, "run")
	readded.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: "req-1"}
	if err := r2.syncSoftStop(context.Background(), readded, []corev1.Pod{*liveCoordinator(readded, "22222222-2222-3333-4444-555555555555")}); err != nil {
		t.Fatalf("syncSoftStop re-add: %v", err)
	}
	got := getRun(t, c2, "run")
	if got.Status.ControlRequest == nil || got.Status.ControlRequest.ID != "req-1" || got.Status.ControlRequest.TargetPodUID != "22222222-2222-3333-4444-555555555555" {
		t.Fatalf("controlRequest after withdrawal = %+v, want a fresh request fenced to the replacement", got.Status.ControlRequest)
	}
}

func TestSoftStopTerminalClearsRequestAndAck(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.ControlRequest = softStopRequest("req-1", "11111111-2222-3333-4444-555555555555", now.Add(-5*time.Minute))
	run.Status.ControlRequestAck = &courierv1alpha1.ControlRequestAck{
		ID: "req-1",
		At: metav1.NewTime(now.Add(-2 * time.Minute)),
	}
	c := phaseClient(t, run)
	r := softStopReconciler(t, c, now)

	if _, err := r.transitionTerminal(context.Background(), run, courierv1alpha1.PhaseNeedsHuman, "", terminalLifecycleIntent{}, nil); err != nil {
		t.Fatalf("transitionTerminal: %v", err)
	}
	if run.Status.ControlRequest != nil {
		t.Fatalf("controlRequest = %+v, want cleared on terminalization", run.Status.ControlRequest)
	}
	if run.Status.ControlRequestAck != nil {
		t.Fatalf("controlRequestAck = %+v, want cleared on terminalization", run.Status.ControlRequestAck)
	}
	if run.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %s, want NeedsHuman", run.Status.Phase)
	}
	got := getRun(t, c, "run")
	if got.Status.ControlRequest != nil || got.Status.ControlRequestAck != nil {
		t.Fatalf("persisted control pair = %+v / %+v, want both cleared on terminalization", got.Status.ControlRequest, got.Status.ControlRequestAck)
	}
}

func TestSoftStopOversizedAnnotationSkipsStampWithoutFailing(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: strings.Repeat("x", maxSoftStopRequestIDLength+1)}
	pod := liveCoordinator(run, "11111111-2222-3333-4444-555555555555")
	c := phaseClient(t, run, pod)
	r := softStopReconciler(t, c, now)

	if err := r.syncSoftStop(context.Background(), run, []corev1.Pod{*pod}); err != nil {
		t.Fatalf("syncSoftStop must not fail the reconcile for an over-bounds value: %v", err)
	}
	if got := getRun(t, c, "run"); got.Status.ControlRequest != nil {
		t.Fatalf("controlRequest = %+v, want no stamp for a value over the CRD bound", got.Status.ControlRequest)
	}
}
