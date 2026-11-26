package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/topology"
)

// staleGuardRun builds a Pending run carrying an explicit UID so seeded
// guard objects can name it as owner, mirroring what the API server would
// stamp on a real run.
func staleGuardRun(name string) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "local", courierv1alpha1.PhasePending)
	run.UID = types.UID("uid-" + name)
	return run
}

// seedGuardResources returns a live guard Pod held by a finalizer — the
// authoritative-absence gate's stand-in for a deletion kubelet has not
// completed — and the guard Service the run owns. A partial launch that
// failed after provisioning left exactly this shape under the run's
// ownerRef.
func seedGuardResources(run *courierv1alpha1.CoderRun) (*corev1.Pod, *corev1.Service) {
	labels := map[string]string{
		topology.LabelRun:       topology.RunLabelValue(run.Name),
		executor.LabelComponent: executor.GitGuardComponentName,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            executor.GitGuardPodName(run.Name),
			Namespace:       run.Namespace,
			Labels:          labels,
			OwnerReferences: runOwnerReferences(run),
			Finalizers:      []string{"courier.test/hold-guard-pod"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: executor.GitEgressContainerName, Image: "example/guard:test"}}},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            executor.GitGuardServiceName(run.Name),
			Namespace:       run.Namespace,
			OwnerReferences: runOwnerReferences(run),
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    []corev1.ServicePort{{Name: "git", Protocol: corev1.ProtocolTCP, Port: executor.GitEgressPort}},
		},
	}
	return pod, service
}

func guardDeletionLauncher(c client.Client) func(context.Context, *courierv1alpha1.CoderRun) error {
	launcher := &CoordinatorLauncher{Client: c, APIReader: c}
	return launcher.TeardownLegacyGitGuard
}

// releaseGuardPod clears the hold-finalizer; with the finalizer gone the
// client completes the deferred delete, the way garbage collection does once
// kubelet confirms termination.
func releaseGuardPod(t *testing.T, c client.Client, run *courierv1alpha1.CoderRun) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, pod); err != nil {
		t.Fatalf("get guard pod: %v", err)
	}
	pod.Finalizers = nil
	if err := c.Update(context.Background(), pod); err != nil {
		t.Fatalf("release guard pod: %v", err)
	}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, pod)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("guard pod after finalizer release: %v, want deleted", err)
	}
}

// A stale-work claim refusal must not delete a run whose guard still
// serves: the delete cascades through garbage collection, which applies no
// ordering between the guard's ingress policy and the pod it bounds. The
// delete waits for the same authoritative absence gate the terminal reap
// uses, then proceeds once the guard is gone.
func TestStaleClaimDeleteHoldsUntilGuardPodGone(t *testing.T) {
	ctx := context.Background()
	run := staleGuardRun("stale-drop")
	guardPod, guardService := seedGuardResources(run)
	c := launcherClientForAdmission(t, admissionLane("local", 1), run, guardPod, guardService)
	reconciler := admissionReconciler(c)
	reconciler.Sources = NewSourceRegistry(map[string]source.Adapter{"test": &refusingReceiptAdmissionSource{}})
	reconciler.GitGuardTeardown = guardDeletionLauncher(c)

	result, err := reconciler.Reconcile(ctx, admissionRequest("stale-drop"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v; a held teardown is an expected wait, not a failure", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("Reconcile() returned no requeue; the held delete must re-drive itself")
	}
	var held courierv1alpha1.CoderRun
	if err := c.Get(ctx, admissionKey("stale-drop"), &held); err != nil {
		t.Fatalf("get run while guard is live: %v, want the run preserved", err)
	}
	var pod corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, &pod); err != nil {
		t.Fatalf("get guard pod: %v", err)
	}
	if pod.DeletionTimestamp == nil {
		t.Fatal("guard pod delete was not staged; the hold must already be retiring the guard")
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name)}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("guard Service get = %v, want deleted by the staged teardown", err)
	}

	releaseGuardPod(t, c, run)
	if _, err := reconciler.Reconcile(ctx, admissionRequest("stale-drop")); err != nil {
		t.Fatalf("Reconcile() after the guard was released: %v", err)
	}
	err = c.Get(ctx, admissionKey("stale-drop"), &courierv1alpha1.CoderRun{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("run get after guard cleared = %v, want deleted", err)
	}
}

// The rejectClaim half: a stale refusal at pre-launch gates the delete the
// same way. While the guard is live the run keeps its admission state and
// the refusal rides back as an error so the workqueue re-drives; after the
// guard is gone the next pass deletes.
func TestRejectClaimStaleWorkHoldsDeletionUntilGuardPodGone(t *testing.T) {
	ctx := context.Background()
	run := staleGuardRun("stale-reject")
	guardPod, guardService := seedGuardResources(run)
	c := launcherClientForAdmission(t, admissionLane("local", 1), run, guardPod, guardService)
	reconciler := admissionReconciler(c)
	reconciler.Sources = NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{preLaunchErr: source.ErrStaleWork}})
	reconciler.GitGuardTeardown = guardDeletionLauncher(c)
	launched := false
	reconciler.Launch = func(context.Context, *courierv1alpha1.CoderRun) error {
		launched = true
		return nil
	}

	_, err := reconciler.Reconcile(ctx, admissionRequest("stale-reject"))
	if !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("Reconcile() error = %v, want the stale refusal to drive the requeue", err)
	}
	if launched {
		t.Fatal("launch ran despite the stale refusal")
	}
	var held courierv1alpha1.CoderRun
	if err := c.Get(ctx, admissionKey("stale-reject"), &held); err != nil {
		t.Fatalf("get run while guard is live: %v, want the run preserved", err)
	}
	var pod corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, &pod); err != nil {
		t.Fatalf("get guard pod: %v", err)
	}
	if pod.DeletionTimestamp == nil {
		t.Fatal("guard pod delete was not staged")
	}

	releaseGuardPod(t, c, run)
	if _, err := reconciler.Reconcile(ctx, admissionRequest("stale-reject")); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("Reconcile() after the guard cleared: %v, want the settled stale error", err)
	}
	err = c.Get(ctx, admissionKey("stale-reject"), &courierv1alpha1.CoderRun{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("run get after guard cleared = %v, want deleted", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name)}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("guard Service get = %v, want deleted with the run", err)
	}
}

// Without guard resources — no CIDRs configured, or a run that never
// provisioned — the gate is transparent and the stale-work deletes behave
// exactly as before (the existing drop tests cover the delete itself; this
// pins that the wiring added no stall).
func TestStaleClaimDeleteWithoutGuardIsTransparent(t *testing.T) {
	ctx := context.Background()
	run := staleGuardRun("stale-no-guard")
	c := launcherClientForAdmission(t, admissionLane("local", 1), run)
	reconciler := admissionReconciler(c)
	reconciler.Sources = NewSourceRegistry(map[string]source.Adapter{"test": &refusingReceiptAdmissionSource{}})
	reconciler.GitGuardTeardown = guardDeletionLauncher(c)

	result, err := reconciler.Reconcile(ctx, admissionRequest("stale-no-guard"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want the delete to run without waiting", result.RequeueAfter)
	}
	if err := c.Get(ctx, admissionKey("stale-no-guard"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("run get = %v, want deleted immediately", err)
	}
}
