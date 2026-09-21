package controller

import (
	"context"
	"errors"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

var (
	envtestClient client.Client
	envtestServer *envtest.Environment
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}

	envtestServer = &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../charts/courier/crd-manifests"},
		ErrorIfCRDPathMissing: true,
	}
	config, err := envtestServer.Start()
	if err != nil {
		panic(err)
	}
	envtestClient, err = client.New(config, client.Options{Scheme: envtestScheme()})
	if err != nil {
		_ = envtestServer.Stop()
		panic(err)
	}
	exitCode := m.Run()
	if err := envtestServer.Stop(); err != nil && exitCode == 0 {
		exitCode = 1
	}
	os.Exit(exitCode)
}

func TestEnvtestResolveIssueLifecycle(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	name := "manual-lifecycle"
	item := &admissionSource{}
	run := envtestRun(name, "envtest-lifecycle")
	lane := envtestLane("envtest-lifecycle")
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}

	reconciler := &CoderRunReconciler{
		Client: envtestClient,
		Sources: NewSourceRegistry(map[string]source.Adapter{
			"manual": item,
		}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "check-a", State: CheckStatePending}}},
			greenObservation("sha-1", "check-a", "check-b"),
			greenObservation("sha-1", "check-a", "check-b"),
		}},

		Launch: func(ctx context.Context, run *courierv1alpha1.CoderRun) error {
			var claimed courierv1alpha1.CoderRun
			if err := envtestClient.Get(ctx, envtestKey(name), &claimed); err != nil {
				t.Fatal(err)
			}
			if claimed.Status.Phase != courierv1alpha1.PhaseClaimed {
				t.Fatalf("phase during launch = %q, want Claimed", claimed.Status.Phase)
			}
			controller := true
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      run.Name + "-coordinator",
					Namespace: run.Namespace,
					Labels:    map[string]string{executor.LabelRun: run.Name},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: courierv1alpha1.GroupVersion.String(),
						Kind:       "CoderRun",
						Name:       run.Name,
						UID:        run.UID,
						Controller: &controller,
					}},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "coordinator", Image: "example/test"}}},
			}
			if err := envtestClient.Create(ctx, pod); err != nil {
				return err
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  "coordinator",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
			}}
			return envtestClient.Status().Update(ctx, pod)

		},
	}

	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, envtestRequest(name)); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	assertEnvtestPhase(t, name, courierv1alpha1.PhaseRunning)
	reconcile()
	assertEnvtestPhase(t, name, courierv1alpha1.PhaseVerifying)
	var pending courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(name), &pending); err != nil {
		t.Fatal(err)
	}
	if pending.Status.PR != "" {
		t.Fatalf("pending PR = %q, want empty before PR registration", pending.Status.PR)
	}
	reconcile()
	assertEnvtestPhase(t, name, courierv1alpha1.PhaseVerifying)
	var updated courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(name), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.PR != "42" {
		t.Fatalf("run PR = %q, want 42", updated.Status.PR)
	}
	// A second check registers while the first is green: the check set grew,
	// so the run must stay Verifying even though every visible check passed.
	reconcile()
	assertEnvtestPhase(t, name, courierv1alpha1.PhaseVerifying)
	// Only a second look at the identical all-green check set settles it.
	reconcile()
	if err := envtestClient.Get(ctx, envtestKey(name), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("run phase = %q, want AwaitingReview", updated.Status.Phase)
	}
	if updated.Status.CheckFingerprint == "" {
		t.Fatal("checkFingerprint = empty at AwaitingReview, want the settled check set")
	}
	if len(item.transitions) != 2 || item.transitions[0] != source.StateInProgress || item.transitions[1] != source.StateInReview {
		t.Fatalf("source transitions = %#v", item.transitions)
	}
}

func TestEnvtestVerifyingRunDoesNotConsumeCapacity(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	laneName := "envtest-verifying-capacity"
	firstName := "envtest-verifying-first"
	secondName := "envtest-verifying-second"
	lane := envtestLane(laneName)
	first := envtestRun(firstName, laneName)
	first.Status.Phase = courierv1alpha1.PhaseVerifying
	second := envtestRun(secondName, laneName)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Create(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Create(ctx, second); err != nil {
		t.Fatal(err)
	}
	reconciler := &CoderRunReconciler{
		Client: envtestClient,
		Sources: NewSourceRegistry(map[string]source.Adapter{
			"manual": &admissionSource{},
		}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
	}
	if _, err := reconciler.Reconcile(ctx, envtestRequest(secondName)); err != nil {
		t.Fatal(err)
	}
	assertEnvtestPhase(t, secondName, courierv1alpha1.PhaseClaimed)
}

func TestEnvtestLaunchFailureReleasesClaim(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	name := "launch-failure"
	item := &admissionSource{}
	run := envtestRun(name, "envtest-failure")
	lane := envtestLane("envtest-failure")
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("pod launch failed")
	reconciler := &CoderRunReconciler{
		Client:       envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Launch:       func(context.Context, *courierv1alpha1.CoderRun) error { return wantErr },
	}
	if _, err := reconciler.Reconcile(ctx, envtestRequest(name)); !errors.Is(err, wantErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, wantErr)
	}
	var updated courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(name), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courierv1alpha1.PhasePending || updated.Status.Branch != "" {
		t.Fatalf("run status = phase %q branch %q, want Pending and empty branch", updated.Status.Phase, updated.Status.Branch)
	}
	if len(item.claimed) != 1 || len(item.released) != 1 {
		t.Fatalf("source claim/release = %#v/%#v", item.claimed, item.released)
	}
}

func envtestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = courierv1alpha1.AddToScheme(scheme)
	return scheme
}

func envtestRun(name, lane string) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     "manual",
			WorkItemID: name,
			Repo:       "acme/widgets",
			Ref:        14,
			Lane:       lane,
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhasePending},
	}
}

func envtestLane(name string) *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Concurrency: 1, Roles: map[string]string{"coordinator": "test"}},
	}
}

func envtestKey(name string) types.NamespacedName {
	return types.NamespacedName{Name: name, Namespace: "default"}
}

func envtestRequest(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: envtestKey(name)}
}

func assertEnvtestPhase(t *testing.T, name string, want courierv1alpha1.Phase) {
	t.Helper()
	var run courierv1alpha1.CoderRun
	if err := envtestClient.Get(context.Background(), envtestKey(name), &run); err != nil {
		t.Fatal(err)
	}
	if run.Status.Phase != want {
		t.Fatalf("phase = %q, want %q", run.Status.Phase, want)
	}
}

type sequenceWorldObserver struct {
	observations []PRObservation
	calls        int
}

func (o *sequenceWorldObserver) Observe(context.Context, string, string) (PRObservation, error) {
	if o.calls >= len(o.observations) {
		return PRObservation{}, errors.New("unexpected observation")
	}
	observation := o.observations[o.calls]
	o.calls++
	return observation, nil
}
