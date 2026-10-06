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
	"k8s.io/apimachinery/pkg/runtime/schema"
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
		At:                metav1.NewTime(at),
		Kind:              "stream",
		CoordinatorPodUID: run.Name + "-coordinator",
	}
	return run
}

// failingAPIReader is the smallest client.Reader for the live-read failure
// case: it fails every read, so the reap decision and the disappearance
// backstop must both defer — a failed API read is not evidence of a wedge,
// of health, or of a loss.
type failingAPIReader struct {
	client.Client
}

func (failingAPIReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("api server unavailable")
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
	// pod-loss reason: the charge is taken in place, because the fake
	// delete completes synchronously; a charge the live read defers
	// arrives with the disappearance backstop's pod-loss reason.
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

func TestWedgeChargeDeferredUntilDeadObjectIsGone(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// A stale heartbeat and a cached unobservable dead coordinator: the wedge
	// path reaps it before the disappearance backstop can route it.
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := evictedCoordinatorPod(run)
	// The pod is old (2 hours ago, past the liveness window), so no
	// freshness grace applies.
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	// A finalizer is what keeps the deleted object alive as a terminating
	// object on the fake client, so the wedge's delete is in flight rather
	// than a disappearance.
	pod.Finalizers = []string{"courier.misospace.dev/test"}
	// One client serves both the reconciler's cached client and its API
	// reader, so the wedge's post-delete re-confirmation reads the same store
	// the delete wrote to.
	c := phaseClient(t, run, pod)
	reconciler := livenessReconciler(c, src, livenessClock)
	reconciler.APIReader = c
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; a still-terminating dead object defers the relaunch")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; the charge is deferred until the dead object is gone", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; a deferred charge must not transition the run", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a still-terminating dead object must not be charged", updated.Status.Restarts)
	}
	if len(src.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none; a deferred charge is not a terminal publish", src.transitions)
	}
	if len(src.reports) != 0 {
		t.Fatalf("reports = %#v, want none; a deferred charge is not a terminal publish", src.reports)
	}
	if out := eventsOut.String(); out != "" {
		t.Fatalf("events = %q, want none; a deferred charge emits no relaunch event", out)
	}
	// The dead object must now be terminating (deletionTimestamp set), not
	// gone: the finalizer holds it while the delete is in flight.
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := c.Get(context.Background(), podKey, &got); err != nil {
		t.Fatalf("get pod: %v, want the deleted object to survive as terminating", err)
	}
	if got.DeletionTimestamp == nil {
		t.Fatal("deletionTimestamp = nil, want set; the finalizer must keep the deleted object terminating")
	}

	// A second reconcile while the finalizer still holds the object defers
	// the same way: the deletion is in flight, so nothing is charged.
	result, err = reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() in-flight pass error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; an in-flight delete defers the relaunch")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; an in-flight delete is re-observed", result.RequeueAfter, observationRequeueDelay)
	}
	var inFlight courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &inFlight); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if inFlight.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; an in-flight delete must not transition the run", inFlight.Status.Phase)
	}
	if inFlight.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; an in-flight delete must not be charged", inFlight.Status.Restarts)
	}

	// Release the finalizer so the object is gone, then reconcile again: once
	// the object is gone the cached coordinator is absent, the wedge finds no
	// pod, and the disappearance backstop takes the charge with its pod-loss
	// cause — the deferred wedge charge lands on the backstop, the path that
	// confirms against the API server.
	got.Finalizers = nil
	if err := c.Update(context.Background(), &got); err != nil {
		t.Fatalf("release finalizer: %v", err)
	}
	if err := c.Get(context.Background(), podKey, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("get pod after finalizer release: %v, want NotFound; the object must be gone", err)
	}

	result, err = reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() final pass error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a confirmed-gone loss must relaunch from its checkpoint")
	}
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run final pass: %v", err)
	}
	if after.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed; the loss is charged once the dead object is gone", after.Status.Phase)
	}
	if after.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1; one physical loss is charged exactly once", after.Status.Restarts)
	}
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != podLostEventReason {
		t.Fatalf("relaunch event reason = %q, want %q; the backstop charges the deferred loss with the pod-loss cause", reason, podLostEventReason)
	}
}

func TestWedgeChargeDeferredWhenCacheLagsBehindTermination(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := runningCoordinatorPod(run)
	// The cached pod is old (past the liveness window) with no deletion
	// timestamp: a sibling reconcile's delete has not reached this
	// reconcile's cache yet.
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	cached := phaseClient(t, run, pod)
	// The API server already shows the object terminating: a finalizer is
	// what keeps it alive there past the sibling's delete, and the fake
	// client refuses a DeletionTimestamp without one.
	apiPod := runningCoordinatorPod(run)
	apiPod.CreationTimestamp = pod.CreationTimestamp
	apiPod.Status.ContainerStatuses[0].State.Running.StartedAt = pod.Status.ContainerStatuses[0].State.Running.StartedAt
	apiPod.Finalizers = []string{"courier.misospace.dev/test"}
	apiPod.DeletionTimestamp = &metav1.Time{Time: livenessClock.Add(-time.Minute)}
	api := phaseClient(t, run, apiPod)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; a coordinator surviving the live read defers the relaunch")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a cached view that missed a sibling's delete must not count a second wedge", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; a deferred charge must not transition the run", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a cached view that missed a sibling's delete must not count a second wedge", updated.Status.Restarts)
	}
	if len(src.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none; a deferred charge is not a terminal publish", src.transitions)
	}
	if len(src.reports) != 0 {
		t.Fatalf("reports = %#v, want none; a deferred charge is not a terminal publish", src.reports)
	}
	if out := eventsOut.String(); out != "" {
		t.Fatalf("events = %q, want none; a deferred charge emits no relaunch event", out)
	}
}

func TestWedgeChargeDeferredWhenLiveWorldDisagreesWithCache(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	pod := runningCoordinatorPod(run)
	// The cached pod is old (past the liveness window): the cache calls it a
	// genuine wedge.
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	cached := phaseClient(t, run, pod)
	// The API server shows no coordinator at all: the live world and the
	// cache disagree about the pod this reconcile would reap.
	api := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; a contradictory observation defers, it does not act")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a stale cached snapshot must not authorize a deletion, and the live view alone must not charge a loss the cache has not caught up with", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; a contradictory observation must not transition the run", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a contradictory observation must not be charged", updated.Status.Restarts)
	}
	if len(src.transitions) != 0 || len(src.reports) != 0 {
		t.Fatalf("transitions = %#v, reports = %#v, want none; a deferred decision publishes nothing", src.transitions, src.reports)
	}
	if out := eventsOut.String(); out != "" {
		t.Fatalf("events = %q, want none; a deferred decision emits no relaunch event", out)
	}

	// Once the informer catches up — both views agree the coordinator is
	// gone — the disappearance backstop takes the confirmed loss with its
	// pod-loss cause.
	caughtUp := phaseClient(t, run)
	settled := livenessReconciler(caughtUp, src, livenessClock)
	settled.APIReader = caughtUp
	settled.Events = reconciler.Events

	result, err = settled.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() after cache catch-up error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a confirmed-gone loss must relaunch from its checkpoint")
	}
	var after courierv1alpha1.CoderRun
	if err := caughtUp.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run after cache catch-up: %v", err)
	}
	if after.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed; the loss is charged once both views agree", after.Status.Phase)
	}
	if after.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1; one physical loss is charged exactly once", after.Status.Restarts)
	}
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != podLostEventReason {
		t.Fatalf("relaunch event reason = %q, want %q; the backstop charges the deferred loss with the pod-loss cause", reason, podLostEventReason)
	}
}

func TestWedgeChargeDeferredWhenLiveReadFails(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	// An aged, wedged coordinator: a genuine wedge.
	pod := runningCoordinatorPod(run)
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	c := phaseClient(t, run, pod)
	reconciler := livenessReconciler(c, src, livenessClock)
	// The live read fails: the delete is re-confirmed against the API
	// server, and a failed read is not evidence the name is free.
	reconciler.APIReader = failingAPIReader{}
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; a failed live read defers the relaunch")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a failed live read is not evidence the name is free", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; a deferred charge must not transition the run", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a failed live read is not evidence the name is free", updated.Status.Restarts)
	}
	if out := eventsOut.String(); out != "" {
		t.Fatalf("events = %q, want none; a deferred charge emits no relaunch event", out)
	}
}

func TestWorkerReapDeferredWhileCoordinatorSurvivesLive(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	// The cached client holds an aged, running worker pod: it belongs to
	// the run, but it is not the coordinator.
	worker := runningCoordinatorPod(run)
	worker.Name = "run-worker"
	worker.Spec.Containers[0].Name = "worker"
	worker.Status.ContainerStatuses[0].Name = "worker"
	worker.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	worker.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	cached := phaseClient(t, run, worker)
	// The API server shows the real coordinator terminating: a finalizer
	// is what keeps it alive there, and the fake client refuses a
	// DeletionTimestamp without one.
	apiPod := runningCoordinatorPod(run)
	apiPod.CreationTimestamp = worker.CreationTimestamp
	apiPod.Status.ContainerStatuses[0].State.Running.StartedAt = worker.Status.ContainerStatuses[0].State.Running.StartedAt
	apiPod.Finalizers = []string{"courier.misospace.dev/test"}
	apiPod.DeletionTimestamp = &metav1.Time{Time: livenessClock.Add(-time.Minute)}
	api := phaseClient(t, run, apiPod)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; a coordinator surviving the live read defers the relaunch")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a worker delete must not charge the ceiling while a coordinator survives the live read", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; a deferred charge must not transition the run", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a worker delete must not charge the ceiling while a coordinator survives the live read", updated.Status.Restarts)
	}
	if out := eventsOut.String(); out != "" {
		t.Fatalf("events = %q, want none; a deferred charge emits no relaunch event", out)
	}
}

func TestWedgeAtCeilingTerminalizesWithoutLiveReConfirmation(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-10*time.Minute))
	// The run is already at the crashloop ceiling.
	run.Status.Restarts = 3
	pod := runningCoordinatorPod(run)
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-10 * time.Minute))
	cached := phaseClient(t, run, pod)
	// The API server still shows the pod terminating: at the ceiling there
	// is no charge to defer, so the hand-off must not wait for it.
	apiPod := runningCoordinatorPod(run)
	apiPod.CreationTimestamp = pod.CreationTimestamp
	apiPod.Status.ContainerStatuses[0].State.Running.StartedAt = pod.Status.ContainerStatuses[0].State.Running.StartedAt
	apiPod.Finalizers = []string{"courier.misospace.dev/test"}
	apiPod.DeletionTimestamp = &metav1.Time{Time: livenessClock.Add(-time.Minute)}
	api := phaseClient(t, run, apiPod)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	// At the ceiling nothing defers, and nothing is deleted either: the live
	// object's deletion is already in flight, so the hand-off completes
	// without re-deleting it. The informer-held cached object is the cache's
	// artifact, not the operator's target.
	var surviving corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := api.Get(context.Background(), podKey, &surviving); err != nil {
		t.Fatalf("get pod on the API server: %v, want the terminating object to survive; at the ceiling the hand-off completes without a delete", err)
	}
	if surviving.DeletionTimestamp == nil {
		t.Fatal("deletionTimestamp = nil, want set; the surviving object is the deletion already in flight")
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3 (the ceiling is not re-incremented)", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman; at the ceiling the hand-off completes in this reconcile", updated.Status.Phase)
	}
	if len(src.transitions) != 1 || src.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want [needs-human]", src.transitions)
	}
	if len(src.reports) != 1 || src.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v, want one blocked report", src.reports)
	}
	if !strings.Contains(src.reports[0].Error, "pod lost") {
		t.Fatalf("reported error = %q, want the pod-loss cause; the live read confirmed the coordinator object is gone or terminating", src.reports[0].Error)
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
			// One client for both the cached client and the API reader, so the
			// backstop's post-delete re-confirmation reads the same store the
			// delete wrote to — in production both are the API server.
			cached := phaseClient(t, run, tc.podFor(run))
			reconciler := livenessReconciler(cached, src, livenessClock)
			reconciler.APIReader = cached

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

func TestPodLossChargeDeferredUntilDeadObjectIsGone(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// A status-less terminal coordinator: the unobservable dead shape the
	// disappearance backstop confirms against the API server. A finalizer is
	// what keeps the deleted object alive as a terminating object on the
	// fake client, so the delete is in flight rather than a disappearance.
	pod := evictedCoordinatorPod(run)
	pod.Finalizers = []string{"courier.misospace.dev/test"}
	// One client serves both the reconciler's cached client and its API
	// reader: the production delete goes through the cached client and the
	// re-confirmation reads the API server directly, both over the same
	// object, so the delete leaves a deletionTimestamp the reader can see.
	c := phaseClient(t, run, pod)
	reconciler := livenessReconciler(c, src, livenessClock)
	reconciler.APIReader = c
	var eventsOut bytes.Buffer
	reconciler.Events = courierlog.NewEmitter(&eventsOut, courierlog.LevelDebug)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatalf("Requeue = true, want false; a still-terminating dead object defers the relaunch")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; the charge is deferred until the dead object is gone", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; a deferred charge must not transition the run", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; a still-terminating dead object must not be charged", updated.Status.Restarts)
	}
	if len(src.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none; a deferred charge is not a terminal publish", src.transitions)
	}
	if len(src.reports) != 0 {
		t.Fatalf("reports = %#v, want none; a deferred charge is not a terminal publish", src.reports)
	}
	if out := eventsOut.String(); out != "" {
		t.Fatalf("events = %q, want none; a deferred charge emits no relaunch event", out)
	}
	// The dead object must now be terminating (deletionTimestamp set), not
	// gone: the finalizer holds it while the delete is in flight.
	var got corev1.Pod
	podKey := types.NamespacedName{Name: "run-coordinator", Namespace: "default"}
	if err := c.Get(context.Background(), podKey, &got); err != nil {
		t.Fatalf("get pod: %v, want the deleted object to survive as terminating", err)
	}
	if got.DeletionTimestamp == nil {
		t.Fatal("deletionTimestamp = nil, want set; the finalizer must keep the deleted object terminating")
	}

	// Release the finalizer so the object is gone, then reconcile again: the
	// single physical loss is now confirmed against the API server and
	// charged exactly once.
	got.Finalizers = nil
	if err := c.Update(context.Background(), &got); err != nil {
		t.Fatalf("release finalizer: %v", err)
	}
	if err := c.Get(context.Background(), podKey, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("get pod after finalizer release: %v, want NotFound; the object must be gone", err)
	}

	result, err = reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() second pass error = %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a confirmed-gone loss must relaunch from its checkpoint")
	}
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run second pass: %v", err)
	}
	if after.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed; the loss is charged once the dead object is gone", after.Status.Phase)
	}
	if after.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1; one physical loss is charged exactly once", after.Status.Restarts)
	}
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != podLostEventReason {
		t.Fatalf("relaunch event reason = %q, want %q; a confirmed pod loss relaunches with the pod-loss reason", reason, podLostEventReason)
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

// --- §6 decision table: active-operation suppression -----------------------

func workerPodFor(run *courierv1alpha1.CoderRun) *corev1.Pod {
	worker := runningCoordinatorPod(run)
	worker.Name = run.Name + "-worker"
	worker.UID = types.UID(run.Name + "-worker")
	worker.Spec.Containers[0].Name = "worker"
	worker.Status.ContainerStatuses[0].Name = "worker"
	return worker
}

func withOperation(run *courierv1alpha1.CoderRun, opID, controlUID, workerUID string) *courierv1alpha1.CoderRun {
	if run.Status.ActiveOperations == nil {
		run.Status.ActiveOperations = map[string]courierv1alpha1.ActiveOperation{}
	}
	run.Status.ActiveOperations[opID] = courierv1alpha1.ActiveOperation{
		BriefID:           "b1",
		CoordinatorPodUID: controlUID,
		WorkerPodUID:      workerUID,
		DispatchedAt:      metav1.NewTime(livenessClock.Add(-time.Hour)),
	}
	return run
}

func assertNotReaped(t *testing.T, c client.Client, src *admissionSource) {
	t.Helper()
	var pod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "run-coordinator"}, &pod); err != nil {
		t.Fatalf("get pod: %v, want the coordinator pod to survive; a valid in-flight operation suppresses stall reaping", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running; suppression never transitions the run", updated.Status.Phase)
	}
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0; suppression never charges the ceiling", updated.Status.Restarts)
	}
	if len(src.transitions) != 0 || len(src.reports) != 0 {
		t.Fatalf("transitions = %#v, reports = %#v, want none; suppression is not a lifecycle event", src.transitions, src.reports)
	}
}

func TestActiveOperationSuppressesStaleHeartbeatReap(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	// A silent build three hours into a heartbeat-stale run: no stream, no
	// tool boundary, but one acknowledged in-flight operation.
	withHeartbeat(run, livenessClock.Add(-3*time.Hour))
	withOperation(run, "shell.b1.aaaa", "run-coordinator", "run-worker")
	coordinator := runningCoordinatorPod(run)
	coordinator.CreationTimestamp = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	coordinator.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	worker := workerPodFor(run)
	worker.CreationTimestamp = coordinator.CreationTimestamp
	client := phaseClient(t, run, coordinator, worker)
	reconciler := livenessReconciler(client, src, livenessClock)
	reconciler.APIReader = client

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Fatalf("result = %#v, want no requeue; a valid entry suppresses stall reaping for any duration", result)
	}
	assertNotReaped(t, client, src)
}

func TestInvalidOperationEntriesDoNotSuppressReap(t *testing.T) {
	for _, tc := range []struct {
		name        string
		controlUID  string
		workerUID   string
		workerState func(*corev1.Pod)
	}{
		{
			name:       "foreign control incarnation",
			controlUID: "previous-incarnation-uid",
			workerUID:  "run-worker",
		},
		{
			name:       "dead worker",
			controlUID: "run-coordinator",
			workerUID:  "run-worker",
			workerState: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodFailed
			},
		},
		{
			name:       "terminating worker",
			controlUID: "run-coordinator",
			workerUID:  "run-worker",
			workerState: func(p *corev1.Pod) {
				p.Finalizers = []string{"courier.misospace.dev/test"}
				p.DeletionTimestamp = &metav1.Time{Time: livenessClock.Add(-time.Minute)}
			},
		},
		{
			name:       "worker shell not running",
			controlUID: "run-coordinator",
			workerUID:  "run-worker",
			workerState: func(p *corev1.Pod) {
				p.Status.ContainerStatuses[0].State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 137},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			withHeartbeat(run, livenessClock.Add(-3*time.Hour))
			withOperation(run, "shell.b1.aaaa", tc.controlUID, tc.workerUID)
			coordinator := runningCoordinatorPod(run)
			coordinator.CreationTimestamp = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
			coordinator.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
			worker := workerPodFor(run)
			worker.CreationTimestamp = coordinator.CreationTimestamp
			worker.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
			if tc.workerState != nil {
				tc.workerState(worker)
			}
			client := phaseClient(t, run, coordinator, worker)
			reconciler := livenessReconciler(client, src, livenessClock)
			reconciler.APIReader = client

			result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if !result.Requeue {
				t.Fatalf("Requeue = false, want true; an invalid entry must not suppress the stale-heartbeat reap")
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.Restarts != 1 || updated.Status.Phase != courierv1alpha1.PhaseClaimed {
				t.Fatalf("state = phase %q restarts %d, want the reap charged exactly once", updated.Status.Phase, updated.Status.Restarts)
			}
		})
	}
}

func TestForeignHeartbeatNeitherResetsNorReaps(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = 2
	// The heartbeat is fresh, but it belongs to the previous incarnation: it
	// must not reset the streak, and — being stale for this pod — must not
	// authorize a reap either.
	run.Status.Heartbeat = &courierv1alpha1.Heartbeat{
		At:                metav1.NewTime(livenessClock.Add(-2 * time.Minute)),
		Kind:              "stream",
		CoordinatorPodUID: "previous-incarnation-uid",
	}
	pod := runningCoordinatorPod(run)
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	client := phaseClient(t, run, pod)
	reconciler := livenessReconciler(client, src, livenessClock)
	reconciler.APIReader = client

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Restarts != 2 {
		t.Fatalf("restarts = %d, want 2; a previous incarnation's heartbeat never resets the streak", updated.Status.Restarts)
	}
	var survived corev1.Pod
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "run-coordinator"}, &survived); err != nil {
		t.Fatalf("get pod: %v, want the current pod to survive; an unattributable heartbeat is not stall evidence", err)
	}
}

// --- §6: the delete carries the observed pod's UID -------------------------

// uidRewriteReader models an API server whose coordinator pod still carries
// the UID this reconcile observed.
type uidRewriteReader struct {
	client.Client
	visibleUID types.UID
}

func (r uidRewriteReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	r.rewrite(obj)
	return nil
}

func (r uidRewriteReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		for i := range pods.Items {
			r.rewrite(&pods.Items[i])
		}
	}
	return nil
}

func (r uidRewriteReader) rewrite(obj client.Object) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || !hasCoordinatorContainer(pod) {
		return
	}
	pod.UID = r.visibleUID
}

// uidEnforcingClient models the API server's delete-time UID check: a delete
// whose precondition names a UID the live object no longer has conflicts
// instead of deleting by name.
type uidEnforcingClient struct {
	client.Client
	preconditions []types.UID
}

func (c *uidEnforcingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	for _, opt := range opts {
		pre, ok := opt.(client.Preconditions)
		if !ok || pre.UID == nil {
			continue
		}
		c.preconditions = append(c.preconditions, *pre.UID)
		stored := &corev1.Pod{}
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(obj), stored); err == nil && stored.UID != *pre.UID {
			return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, obj.GetName(), errors.New("the pod was replaced between observation and deletion"))
		}
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestReapDeleteConflictsWhenPodWasReplaced(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-3*time.Hour))
	// The store holds the replacement pod (delete/recreate already happened);
	// the live read this reconcile makes still observes the old UID.
	replaced := runningCoordinatorPod(run)
	replaced.UID = "replacement-uid"
	replaced.CreationTimestamp = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	replaced.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	store := phaseClient(t, run, replaced)
	enforcing := &uidEnforcingClient{Client: store}
	reconciler := livenessReconciler(enforcing, src, livenessClock)
	reconciler.APIReader = uidRewriteReader{Client: store, visibleUID: "run-coordinator"}

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue {
		t.Fatal("Requeue = true, want a bounded re-observation after the conflict")
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a conflicted delete re-observes instead of acting", result.RequeueAfter, observationRequeueDelay)
	}
	if len(enforcing.preconditions) == 0 {
		t.Fatal("the reap delete carried no UID precondition; a pod must never be deleted by name alone")
	}
	var updated courierv1alpha1.CoderRun
	if err := store.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Restarts != 0 || updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("state = phase %q restarts %d, want no charge; the decision was made on a world that no longer exists", updated.Status.Phase, updated.Status.Restarts)
	}
	var survived corev1.Pod
	if err := store.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "run-coordinator"}, &survived); err != nil {
		t.Fatalf("get pod: %v, want the replacement pod to survive the stale decision", err)
	}
	if survived.UID != "replacement-uid" {
		t.Fatalf("pod UID = %q, want the store's own object untouched by the stale decision", survived.UID)
	}
}

func TestParallelOperationsRemainProtectedWhenOneCompletes(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-3*time.Hour))
	// Two parallel subagent operations: one has completed (its entry
	// cleared), the other is still dispatched. The surviving sibling alone
	// suppresses stall reaping — a completed sibling's cleared entry neither
	// protects nor un-protects the run.
	withOperation(run, "shell.b1.aaaa", "run-coordinator", "run-worker")
	withOperation(run, "shell.b2.bbbb", "run-coordinator", "run-worker")
	delete(run.Status.ActiveOperations, "shell.b1.aaaa")
	coordinator := runningCoordinatorPod(run)
	coordinator.CreationTimestamp = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	coordinator.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	worker := workerPodFor(run)
	worker.CreationTimestamp = coordinator.CreationTimestamp
	client := phaseClient(t, run, coordinator, worker)
	reconciler := livenessReconciler(client, src, livenessClock)
	reconciler.APIReader = client

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Fatalf("result = %#v, want no requeue; the surviving operation suppresses stall reaping", result)
	}
	assertNotReaped(t, client, src)
}

// TestCacheDisagreementAtCeilingHandsOffWithPodLossCause pins the at-ceiling
// branch of the cache-disagreement defer: the API server confirms the
// coordinator object is gone while the cache still shows it live, so the
// NeedsHuman hand-off completes in this reconcile carrying the pod-loss
// cause — and nothing is deleted, because there is nothing left to delete.
func TestCacheDisagreementAtCeilingHandsOffWithPodLossCause(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	withHeartbeat(run, livenessClock.Add(-3*time.Hour))
	run.Status.Restarts = 3
	// The cache still holds a live-looking coordinator; the API server has
	// no coordinator at all: the live world wins and the object is gone.
	pod := runningCoordinatorPod(run)
	pod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(livenessClock.Add(-3 * time.Hour))
	cached := phaseClient(t, run, pod)
	api := phaseClient(t, run)
	reconciler := livenessReconciler(cached, src, livenessClock)
	reconciler.APIReader = api

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := cached.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman; at the ceiling the hand-off completes without deferring", updated.Status.Phase)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3 (the ceiling is not re-incremented)", updated.Status.Restarts)
	}
	if len(src.transitions) != 1 || src.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want [needs-human]", src.transitions)
	}
	if len(src.reports) != 1 || src.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v, want one blocked report", src.reports)
	}
	if !strings.Contains(src.reports[0].Error, "pod lost") {
		t.Fatalf("reported error = %q, want the pod-loss cause; the live read confirmed the coordinator object is gone", src.reports[0].Error)
	}
}
