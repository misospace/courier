package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
)

var livenessClock = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func runningCoordinatorPod(run *courierv1alpha1.CoderRun) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      run.Name + "-coordinator",
			Namespace: run.Namespace,
			UID:       types.UID(run.Name + "-coordinator"),
			Labels:    map[string]string{executorRunLabel: run.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: courierv1alpha1.GroupVersion.String(),
				Kind:       "CoderRun",
				Name:       run.Name,
				UID:        run.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "coordinator", Image: "example/test"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "coordinator",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}},
	}
}

// evictedCoordinatorPod is the shape an eviction leaves behind: the pod
// object survives in a terminal phase with no container statuses recorded,
// so no coordinator exit state is observable.
func evictedCoordinatorPod(run *courierv1alpha1.CoderRun) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      run.Name + "-coordinator",
			Namespace: run.Namespace,
			UID:       types.UID(run.Name + "-coordinator"),
			Labels:    map[string]string{executorRunLabel: run.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: courierv1alpha1.GroupVersion.String(),
				Kind:       "CoderRun",
				Name:       run.Name,
				UID:        run.UID,
				Controller: &controller,
			}},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "coordinator", Image: "example/test"}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "The node was low on resource: ephemeral-storage."},
	}
}

func livenessReconciler(c client.Client, src *admissionSource, now time.Time) *CoderRunReconciler {
	return &CoderRunReconciler{
		Client:         c,
		Sources:        NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter:   fakeStatusWriter{client: c},
		LivenessWindow: 5 * time.Minute,
		MaxRestarts:    3,
		Now:            func() time.Time { return now },
	}
}

func withHeartbeat(run *courierv1alpha1.CoderRun, at time.Time) *courierv1alpha1.CoderRun {
	run.Status.Heartbeat = &courierv1alpha1.Heartbeat{
		At:   metav1.NewTime(at),
		Kind: "stream",
	}
	return run
}

// failingAPIReader is the smallest client.Reader for the live-read failure
// case: it fails every list, so a missing coordinator must be re-observed,
// not relaunched.
type failingAPIReader struct {
	client.Client
}

func (failingAPIReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("api server unavailable")
}

func TestLivenessStaleHeartbeatReapsAndRelaunches(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := runningCoordinatorPod(run)
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a reaped run must relaunch from its checkpoint")
	}
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("get pod: %v, want NotFound after reap", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	// A reaped live coordinator relaunches with the generic event, no
	// pod-loss reason.
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != "" {
		t.Fatalf("relaunch event reason = %q, want none; a reaped live coordinator relaunches with the generic event, no pod-loss reason", reason)
	}
}

func TestLivenessStaleHeartbeatNoPodRelaunches(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	// No coordinator pod at all: the missing/relaunch case the crashloop
	// counter bounds.
	client := phaseClient(t, run)
	reconciler := livenessReconciler(client, src, livenessClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a run with no pod must relaunch from its checkpoint")
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
}

func TestClaimedRunWithNoPodDoesNotCharge(t *testing.T) {
	src := &admissionSource{}
	// A Claimed run has been admitted but not launched: it has no pod and no
	// heartbeat. It must not be treated as pod loss and charged against the
	// restart ceiling; it stays Claimed for the launcher.
	run := admissionRun("run", "local", courierv1alpha1.PhaseClaimed)
	cached := phaseClient(t, run)
	api := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; an admitted run that has not launched must not churn the ceiling", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	if len(src.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none", src.transitions)
	}
}

func TestLivenessCrashLoopBackOffPodReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := runningCoordinatorPod(run)
	// The pod is old (2 hours ago, past the liveness window) and its
	// coordinator is crashlooping, not running: a genuine wedge.
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
	}
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a crashlooping coordinator must be reaped and relaunched")
	}
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("get pod: %v, want NotFound after reap", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
}

func TestLivenessCrashloopTransitionsToNeedsHuman(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	run.Status.Restarts = 3
	pod := runningCoordinatorPod(run)
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("get pod: %v, want NotFound after reap", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3 (the ceiling is not re-incremented)", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
	}
	if len(src.transitions) != 1 || src.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want [needs-human]", src.transitions)
	}
	if len(src.reports) != 1 || src.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v, want one blocked report", src.reports)
	}
	if src.reports[0].Error != "run requires human intervention" {
		t.Fatalf("reported error = %q, want the generic needs-human message; a reaped live coordinator is not pod loss", src.reports[0].Error)
	}
}

func TestWedgeCeilingPreservesUnrecoverableCoordinator(t *testing.T) {
	for _, tc := range []struct {
		name          string
		restarts      int
		wantPreserved bool
	}{
		{name: "below ceiling", restarts: 0},
		{name: "at ceiling", restarts: 3, wantPreserved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			// A stale heartbeat and a cached evicted coordinator: the wedge
			// path reaps it before the disappearance backstop can route it.
			withHeartbeat(run, livenessClock.Add(-10*time.Minute))
			run.Status.Restarts = tc.restarts
			pod := evictedCoordinatorPod(run)
			// The pod is old (2 hours ago, past the liveness window), so no
			// freshness grace applies.
			pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
			client := phaseClient(t, run, pod)
			reconciler := livenessReconciler(client, src, livenessClock)

			result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			var got corev1.Pod
			podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
			err = client.Get(context.Background(), podKey, &got)
			if !tc.wantPreserved {
				if !result.Requeue {
					t.Fatalf("Requeue = false, want true; a run below the ceiling must relaunch")
				}
				if !apierrors.IsNotFound(err) {
					t.Fatalf("get pod: %v, want NotFound; the inert object must be deleted so the pod name is free for the replacement", err)
				}
				if updated.Status.Restarts != 1 {
					t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
				}
				if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
					t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
				}
				if len(src.transitions) != 0 {
					t.Fatalf("transitions = %#v, want none; a relaunch is not a terminal publish", src.transitions)
				}
				if len(src.reports) != 0 {
					t.Fatalf("reports = %#v, want none; a relaunch is not a terminal publish", src.reports)
				}
				return
			}
			// At the ceiling the inert object survives — the NeedsHuman
			// hand-off keeps the only record of the cause.
			if result.Requeue || result.RequeueAfter != 0 {
				t.Fatalf("result = %#v, want no requeue; the terminal hand-off takes no requeue", result)
			}
			if apierrors.IsNotFound(err) {
				t.Fatal("get pod: NotFound, want the inert object to survive at the ceiling; the NeedsHuman hand-off keeps the only record of the cause")
			}
			if got.Status.Reason != "Evicted" {
				t.Fatalf("pod reason = %q, want Evicted; the surviving object is the only record of the cause", got.Status.Reason)
			}
			if updated.Status.Restarts != 3 {
				t.Fatalf("restarts = %d, want 3 (the ceiling is not re-incremented)", updated.Status.Restarts)
			}
			if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
				t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
			}
			if len(src.transitions) != 1 || src.transitions[0] != source.StateNeedsHuman {
				t.Fatalf("transitions = %#v, want [needs-human]", src.transitions)
			}
			if len(src.reports) != 1 || src.reports[0].Result != source.ResultBlocked {
				t.Fatalf("reports = %#v, want one blocked report", src.reports)
			}
			if src.reports[0].Error != "run requires human intervention" {
				t.Fatalf("reported error = %q, want the generic needs-human message; a reaped wedge is not pod loss", src.reports[0].Error)
			}
		})
	}
}

func TestFreshHeartbeatLostCoordinatorAtCeilingStillNeedsHuman(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Second))
	run.Status.Restarts = 3
	// No pod in the cache and none in the API server: the fresh heartbeat is
	// evidence of the recent past, not of current liveness, so the reset is
	// suspended and the confirmed loss takes the ceiling.
	cached := phaseClient(t, run)
	api := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman; a fresh heartbeat with no observable coordinator must not relaunch at the ceiling", updated.Status.Phase)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3; a fresh heartbeat with no observable coordinator must not reset the ceiling", updated.Status.Restarts)
	}
	if len(src.reports) != 1 || src.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v, want one blocked report", src.reports)
	}
	if !strings.Contains(src.reports[0].Error, "pod lost") {
		t.Fatalf("reported error = %q, want the human-facing cause to mention pod loss", src.reports[0].Error)
	}
}

func TestLivenessCeilingEnrichesObservedPR(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	run.Status.Restarts = 3
	// A run can open a PR and then wedge before Verifying ever observes
	// it: the ceiling path must still publish the PR the world shows.
	run.Status.Branch = "courier/acme/widgets/issue-1"
	pod := runningCoordinatorPod(run)
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)
	reconciler.Observer = fakeWorldObserver{observation: PRObservation{PR: "42", Checks: []CheckObservation{{Name: "check", State: CheckStateFailed}}}}

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
	}
	if updated.Status.PR != "42" {
		t.Fatalf("PR = %q, want 42", updated.Status.PR)
	}
	if len(src.reports) != 1 || src.reports[0].PR != "42" || src.reports[0].State != source.StateNeedsHuman {
		t.Fatalf("reports = %#v, want one report with PR 42 and state %q", src.reports, source.StateNeedsHuman)
	}
}

func TestLivenessJustBelowCeilingRelaunches(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	run.Status.Restarts = 2
	pod := runningCoordinatorPod(run)
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a run below the ceiling must relaunch")
	}
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("get pod: %v, want NotFound after reap", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
}

func TestLivenessNewlyCreatedPodNotReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// Heartbeat is 10 minutes old (stale), but the pod was just created and
	// its coordinator has not started yet.
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := runningCoordinatorPod(run)
	pod.CreationTimestamp = metav1.NewTime(livenessClock)
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
	}
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatal("Requeue = true, want false; a just-created pod must not be reaped")
	}
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &got); err != nil {
		t.Fatalf("get pod: %v, want the just-created coordinator to survive", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0", updated.Status.Restarts)
	}
}

func TestLivenessTerminatingPodNotReCounted(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := runningCoordinatorPod(run)
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	// A finalizer is what keeps a terminating pod alive; the fake client
	// refuses a DeletionTimestamp without one.
	pod.Finalizers = []string{"courier.misospace.dev/test"}
	pod.DeletionTimestamp = &metav1.Time{Time: livenessClock.Add(-time.Minute)}
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a finalizer-stuck terminating pod must keep being re-observed", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; an already-terminating pod must not be re-counted", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if len(src.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none", src.transitions)
	}
}

func TestLivenessFreshPodNotImmediatelyReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = 2
	// Heartbeat is 10 minutes old (stale), but the pod just started.
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := runningCoordinatorPod(run)
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-time.Minute))
	c := phaseClient(t, run, pod)
	reconciler := livenessReconciler(c, src, livenessClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatal("Requeue = true, want false; a freshly started pod must not be reaped")
	}
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := c.Get(context.Background(), podKey, &got); err != nil {
		t.Fatalf("get pod: %v, want the freshly started coordinator to survive", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 2 {
		t.Fatalf("restarts = %d, want 2; a freshly started pod is not liveness evidence and must not reset the counter", updated.Status.Restarts)
	}
}

func TestLivenessFreshHeartbeatNotReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-2*time.Minute))
	client := phaseClient(t, run, runningCoordinatorPod(run))
	reconciler := livenessReconciler(client, src, livenessClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var pod corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &pod); err != nil {
		t.Fatalf("get pod: %v, want the coordinator pod to survive", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0", updated.Status.Restarts)
	}
}

func TestLivenessFreshHeartbeatResetsCrashloopCounter(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-2*time.Minute))
	run.Status.Restarts = 2
	client := phaseClient(t, run, runningCoordinatorPod(run))
	reconciler := livenessReconciler(client, src, livenessClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var pod corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &pod); err != nil {
		t.Fatalf("get pod: %v, want the coordinator pod to survive", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a fresh heartbeat resets the consecutive-crashloop counter", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
}

func TestLivenessNilHeartbeatNotReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = 1
	client := phaseClient(t, run, runningCoordinatorPod(run))
	reconciler := livenessReconciler(client, src, livenessClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var pod corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &pod); err != nil {
		t.Fatalf("get pod: %v, want the coordinator pod to survive", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1; a nil heartbeat must not reset the counter", updated.Status.Restarts)
	}
}

func TestLivenessExactlyAtWindowNotReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-5*time.Minute))
	client := phaseClient(t, run, runningCoordinatorPod(run))
	reconciler := livenessReconciler(client, src, livenessClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var pod corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := client.Get(context.Background(), podKey, &pod); err != nil {
		t.Fatalf("get pod: %v, want the coordinator pod to survive at the window boundary", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
}

func TestCoordinatorPodDeletedRelaunchesWithoutHeartbeat(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// No pod in the reconciler's client and none in the API server, and no
	// heartbeat: the loss is confirmed against the world, so the run
	// relaunches even though the operator never observed the pod — including
	// every run orphaned before this backstop was deployed.
	cached := phaseClient(t, run)
	api := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a loss confirmed by the API server must relaunch from the checkpoint")
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
	}
}

func TestCoordinatorLiveThoughCacheMissedItIsNotRelaunched(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	pod := runningCoordinatorPod(run)
	// The cache has the run only; the pod exists in the API but has not
	// reached this reconcile's cache yet. The live read finds it running, so
	// the cache lagged and nothing is charged.
	cached := phaseClient(t, run)
	api := phaseClient(t, run, pod)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want no requeue; a coordinator found live is not a loss", result)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a cache lag is not infrastructure loss", updated.Status.Restarts)
	}
}

func TestTerminatingCoordinatorIsNeitherLiveNorLost(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	pod := runningCoordinatorPod(run)
	// A finalizer is what keeps a terminating pod alive; the fake client
	// refuses a DeletionTimestamp without one.
	pod.Finalizers = []string{"courier.misospace.dev/test"}
	pod.DeletionTimestamp = &metav1.Time{Time: livenessClock}
	cached := phaseClient(t, run)
	api := phaseClient(t, run, pod)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; a terminating coordinator is not a confirmed loss")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a terminating coordinator is re-observed", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0", updated.Status.Restarts)
	}
}

func TestPodLossAtCeilingNeedsHumanWithPodLossReason(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = 3
	client := phaseClient(t, run)
	reconciler := livenessReconciler(client, src, livenessClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3 (the ceiling is not re-incremented)", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
	}
	if len(src.transitions) != 1 || src.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want [needs-human]", src.transitions)
	}
	if len(src.reports) != 1 || src.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v, want one blocked report", src.reports)
	}
	if !strings.Contains(src.reports[0].Error, "pod lost") {
		t.Fatalf("reported error = %q, want the human-facing cause to mention pod loss", src.reports[0].Error)
	}
}

func TestTerminalCoordinatorWithoutContainerStatusesRelaunches(t *testing.T) {
	for _, tc := range []struct {
		name   string
		podFor func(run *courierv1alpha1.CoderRun) *corev1.Pod
	}{
		{
			name:   "evicted",
			podFor: evictedCoordinatorPod,
		},
		{
			name: "failed scheduling",
			podFor: func(run *courierv1alpha1.CoderRun) *corev1.Pod {
				pod := evictedCoordinatorPod(run)
				pod.Status.Reason = "Unschedulable"
				pod.Status.Message = "0/3 nodes are available: 3 node(s) had taints that the pod didn't tolerate."
				return pod
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			// No heartbeat: a legacy run whose only trace of its coordinator is
			// a terminal pod object with no container statuses.
			cached := phaseClient(t, run, tc.podFor(run))
			api := phaseClient(t, run, tc.podFor(run))
			reconciler := livenessReconciler(cached, src, livenessClock)
			reconciler.APIReader = api

			result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if !result.Requeue {
				t.Fatalf("Requeue = false, want true; an unobservable dead coordinator must relaunch from its checkpoint")
			}
			var got corev1.Pod
			podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
			if err := cached.Get(context.Background(), podKey, &got); !apierrors.IsNotFound(err) {
				t.Fatalf("get pod: %v, want NotFound; the inert object must be deleted so the pod name is free for the replacement", err)
			}
			var updated courierv1alpha1.CoderRun
			if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
				t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
			}
			if updated.Status.Restarts != 1 {
				t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
			}
		})
	}
}

func TestTerminalCoordinatorLiveReadShowsLiveCoordinatorNotCharged(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// The cache shows only the inert terminal object, but the API server shows
	// the coordinator running: a live coordinator wins over the stale cache,
	// so nothing is charged and nothing is deleted.
	cached := phaseClient(t, run, evictedCoordinatorPod(run))
	api := phaseClient(t, run, runningCoordinatorPod(run))
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want no requeue; a live coordinator is not a loss", result)
	}
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := cached.Get(context.Background(), podKey, &got); err != nil {
		t.Fatalf("get pod: %v, want the cached pod object to survive; a live coordinator is not deleted", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a cache lag is not infrastructure loss", updated.Status.Restarts)
	}
}

func TestTerminalCoordinatorAtCeilingNeedsHuman(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = 3
	// No heartbeat, and the run is already at the ceiling: the confirmed dead
	// coordinator goes to a human with the pod loss cause, not one further
	// relaunch.
	cached := phaseClient(t, run, evictedCoordinatorPod(run))
	api := phaseClient(t, run, evictedCoordinatorPod(run))
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3 (the ceiling is not re-incremented)", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
	}
	if len(src.transitions) != 1 || src.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want [needs-human]", src.transitions)
	}
	if len(src.reports) != 1 || src.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v, want one blocked report", src.reports)
	}
	if !strings.Contains(src.reports[0].Error, "pod lost") {
		t.Fatalf("reported error = %q, want the human-facing cause to mention pod loss", src.reports[0].Error)
	}
	// At the ceiling the inert object survives — the NeedsHuman hand-off keeps
	// the only record of the cause.
	var lost corev1.Pod
	if err := cached.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "run-coordinator"}, &lost); err != nil {
		t.Fatalf("get pod: %v, want the terminal object to survive at the ceiling", err)
	}
	if lost.Status.Reason != "Evicted" {
		t.Fatalf("pod reason = %q, want Evicted; the surviving object is the only record of the cause", lost.Status.Reason)
	}
}

func TestSetupWithManagerRequiresAPIReader(t *testing.T) {
	// The validation must run before the manager is touched, so a nil manager
	// exercises exactly the missing-reader path.
	err := (&CoderRunReconciler{}).SetupWithManager(nil)
	if err == nil {
		t.Fatal("SetupWithManager() error = nil, want an error; a missing APIReader must fail setup")
	}
	if !strings.Contains(err.Error(), "APIReader") {
		t.Fatalf("error = %q, want it to name the missing APIReader", err)
	}
}

func TestPodLossRelaunchRetainsBranch(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Branch = "courier/acme/widgets/issue-1"
	run.Status.HeadRepo = "acme/widgets"
	run.Status.HeadSHA = "abc123"
	client := phaseClient(t, run)
	reconciler := livenessReconciler(client, src, livenessClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a lost coordinator must relaunch from its checkpoint")
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	if updated.Status.Branch != "courier/acme/widgets/issue-1" {
		t.Fatalf("branch = %q, want the work branch adopted, not discarded", updated.Status.Branch)
	}
	if updated.Status.HeadRepo != "acme/widgets" {
		t.Fatalf("headRepo = %q, want the recorded head repository retained", updated.Status.HeadRepo)
	}
	if updated.Status.HeadSHA != "abc123" {
		t.Fatalf("headSHA = %q, want the recorded head commit retained", updated.Status.HeadSHA)
	}
}

func TestLiveReadFailureReobserves(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	cached := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = failingAPIReader{}

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a failed live read is not evidence of a loss", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0", updated.Status.Restarts)
	}
}

func TestStaleHeartbeatMissingPodPublishesPodLossReason(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	// No pod in the reconciler's client and none in the API server: a stale
	// heartbeat with no pod anywhere is pod loss by definition, whatever the
	// disappearance backstop would have said.
	cached := phaseClient(t, run)
	api := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a stale heartbeat with no pod must relaunch from the checkpoint")
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
	}
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != podLostEventReason {
		t.Fatalf("relaunch event reason = %q, want %q", reason, podLostEventReason)
	}
}

func TestRelaunchEventCarriesPodLossReason(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// No pod in the cache and none in the API: the disappearance backstop
	// relaunches, and the Claimed transition event must carry the
	// human-facing reason so a pod loss and a reaped wedge stay
	// distinguishable in the event stream.
	cached := phaseClient(t, run)
	api := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != podLostEventReason {
		t.Fatalf("relaunch event reason = %q, want %q", reason, podLostEventReason)
	}
}

// eventReason returns the detail.reason of the first phase.transition event
// to the given phase written to eventsOut.
func eventReason(t *testing.T, eventsOut *bytes.Buffer, phase string) string {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(eventsOut.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %q is not valid JSON: %v", line, err)
		}
		if event["event"] != courierlog.EventPhaseTransition || event["status"] != phase {
			continue
		}
		detail, _ := event["detail"].(map[string]any)
		reason, _ := detail["reason"].(string)
		return reason
	}
	t.Fatalf("no phase.transition event to %q found in %q", phase, eventsOut.String())
	return ""
}
