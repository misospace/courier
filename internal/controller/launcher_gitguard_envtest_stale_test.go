package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

// staleGuardSource admits like a normal source until the ownership gate is
// switched on, then refuses every claim the way the stale-work contract does
// — the shape a run takes after a rollback when its work has gone stale.
type staleGuardSource struct {
	refuse      bool
	claims      int
	released    int
	transitions []source.State
}

func (*staleGuardSource) Discover(context.Context) ([]source.WorkItem, error) { return nil, nil }
func (s *staleGuardSource) Claim(context.Context, source.WorkItem) error      { s.claims++; return nil }
func (s *staleGuardSource) ClaimWithReceipt(_ context.Context, item source.WorkItem) (source.WorkItem, error) {
	s.claims++
	if s.refuse {
		return source.WorkItem{}, source.ErrStaleWork
	}
	return item, nil
}
func (s *staleGuardSource) Release(context.Context, source.WorkItem) error {
	s.released++
	return nil
}
func (s *staleGuardSource) Transition(_ context.Context, _ source.WorkItem, state source.State) error {
	s.transitions = append(s.transitions, state)
	return nil
}
func (*staleGuardSource) Resolve(context.Context, source.WorkItem) error { return nil }

// coordinatorCreateFailureClient passes every write through except the
// coordinator Pod's create, which fails the way quota admission does: the
// guard is already provisioned and the attempt must roll back retryably.
type coordinatorCreateFailureClient struct {
	client.Client
	guardPodName string
}

func (f coordinatorCreateFailureClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && pod.Name != f.guardPodName {
		return apierrors.NewServiceUnavailable("simulated admission failure on the coordinator pod")
	}
	return f.Client.Create(ctx, obj, opts...)
}

// The full boundary sequence a rollback leaves behind, against the real
// reconciler: a partial launch provisions the dedicated guard, the retryable
// coordinator-create failure releases the claim back to Pending with the
// guard still live, and the subsequent stale-work refusal must not delete
// the run outright — garbage collection would then race the guard's ingress
// policy against the pod it bounds. The delete waits for the staged
// teardown's authoritative absence gate and only then proceeds.
func TestEnvtestStaleWorkDeleteWaitsForLiveGuard(t *testing.T) {
	if envtestServer == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	const laneName, runName = "gitguard-stale-lane", "gitguard-stale"
	if err := c.Create(ctx, envtestLane(laneName)); err != nil {
		t.Fatal(err)
	}
	run := envtestRun(runName, laneName)
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, envtestKey(runName), run); err != nil {
		t.Fatal(err)
	}

	launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "stale", expiresAt: time.Now().Add(time.Hour)}, time.Now(), plainExecutor{})
	launcher.GitGuardEgressCIDRs = []string{"140.82.112.0/20"}
	launcher.GitGuardKubernetesAPIIP = "10.96.0.1"
	guardPodName := executor.GitGuardPodName(runName)
	launcher.Client = coordinatorCreateFailureClient{Client: c, guardPodName: guardPodName}

	adapter := &staleGuardSource{}
	reconciler := &CoderRunReconciler{
		Client:           c,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"manual": adapter}),
		StatusWriter:     status.KubePatchWriter{Client: c},
		Launch:           launcher.Launch,
		GitGuardTeardown: launcher.TeardownLegacyGitGuard,
	}

	// Attempt one: the guard is provisioned, and the launch parks on the
	// guard's running gate — nothing of the coordinator exists yet.
	result, err := reconciler.Reconcile(ctx, envtestRequest(runName))
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("first Reconcile did not wait on provisioning")
	}
	guardKey := client.ObjectKey{Namespace: run.Namespace, Name: guardPodName}
	var guard corev1.Pod
	if err := c.Get(ctx, guardKey, &guard); err != nil {
		t.Fatalf("guard pod after the provisioning wait: %v", err)
	}
	// Hold the pod the way a node-held termination does, then mark it Running
	// so attempt two gets past the gate and fails where quota bites.
	guard.Finalizers = append(guard.Finalizers, "courier.misospace.dev/test-hold")
	if err := c.Update(ctx, &guard); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, guardKey, &guard); err != nil {
		t.Fatal(err)
	}
	guard.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: executor.GitEgressContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := c.Status().Update(ctx, &guard); err != nil {
		t.Fatal(err)
	}

	// Attempt two: the coordinator create fails retryably, and the rollback
	// releases the claim — the run returns to Pending with the guard live.
	if _, err := reconciler.Reconcile(ctx, envtestRequest(runName)); err == nil {
		t.Fatal("second Reconcile returned nil, want the retryable coordinator-create failure")
	}
	if err := c.Get(ctx, envtestKey(runName), run); err != nil {
		t.Fatal(err)
	}
	if run.Status.Phase != courierv1alpha1.PhasePending {
		t.Fatalf("phase after the rollback = %q, want Pending", run.Status.Phase)
	}
	if adapter.released != 1 {
		t.Fatalf("source releases = %d, want the rollback to release the claim once", adapter.released)
	}
	if err := c.Get(ctx, guardKey, &guard); err != nil {
		t.Fatalf("guard pod after the rollback: %v", err)
	}
	if guard.DeletionTimestamp != nil {
		t.Fatal("the retryable rollback deleted the guard; the live guard is the whole point")
	}

	// The work goes stale. The refusal must stage the teardown but not take
	// the run with it while the guard pod still exists.
	adapter.refuse = true
	result, err = reconciler.Reconcile(ctx, envtestRequest(runName))
	if err != nil {
		t.Fatalf("stale Reconcile while the guard is live: %v", err)
	}
	if result.RequeueAfter != provisioningInFlightRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want the teardown hold to re-drive at %v", result.RequeueAfter, provisioningInFlightRequeueDelay)
	}
	if err := c.Get(ctx, envtestKey(runName), run); err != nil {
		t.Fatalf("run get while the guard is live: %v, want the delete held", err)
	}
	if err := c.Get(ctx, guardKey, &guard); err != nil {
		t.Fatal(err)
	}
	if guard.DeletionTimestamp == nil {
		t.Fatal("the held delete did not stage the guard pod retirement")
	}
	svcKey := client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardServiceName(runName)}
	if err := c.Get(ctx, svcKey, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("guard Service get = %v, want deleted by the staged teardown", err)
	}

	// The guard is conclusively gone; the next pass deletes the run.
	if err := c.Get(ctx, guardKey, &guard); err != nil {
		t.Fatal(err)
	}
	guard.Finalizers = nil
	if err := c.Update(ctx, &guard); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, envtestRequest(runName)); err != nil {
		t.Fatalf("Reconcile after the guard was released: %v", err)
	}
	if err := c.Get(ctx, envtestKey(runName), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("run get after the guard cleared = %v, want deleted", err)
	}
	if err := c.Get(ctx, svcKey, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("guard Service get after the run cleared = %v, want deleted", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: runName + "-coordinator"}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("coordinator pod get = %v, want never created", err)
	}
}
