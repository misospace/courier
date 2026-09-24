package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

var livenessClock = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func runningCoordinatorPod(run *courierv1alpha1.CoderRun) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      run.Name + "-coordinator",
			Namespace: run.Namespace,
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
	if updated.Status.Restarts != 4 {
		t.Fatalf("restarts = %d, want 4", updated.Status.Restarts)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
	}
	if len(src.transitions) != 1 || src.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want [needs-human]", src.transitions)
	}
}

func TestLivenessFreshPodNotImmediatelyReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
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
	if updated.Status.Restarts != 0 {
		t.Fatalf("restarts = %d, want 0", updated.Status.Restarts)
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

func TestLivenessNilHeartbeatNotReaped(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
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
