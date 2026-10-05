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
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != podLostReason {
		t.Fatalf("relaunch event reason = %q, want %q", reason, podLostReason)
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
	if reason := eventReason(t, &eventsOut, string(courierv1alpha1.PhaseClaimed)); reason != podLostReason {
		t.Fatalf("relaunch event reason = %q, want %q", reason, podLostReason)
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
