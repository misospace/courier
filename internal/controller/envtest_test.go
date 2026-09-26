package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

func TestEnvtestDispatchRunnerLifecycle(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	namespace := "envtest-dispatch"
	laneName := "dispatch-lane"
	if err := envtestClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	adapter := &envtestDispatchAdapter{items: []source.WorkItem{
		{ID: "implement/issue-17", Mode: string(courierv1alpha1.ModeResolveIssue), Repo: "acme/widgets", Ref: 17},
		{ID: "followup/queue-9/generation-2", Mode: string(courierv1alpha1.ModeFixPR), Repo: "acme/widgets", Ref: 42},
	}}
	runner := source.NewRunner(envtestClient, adapter, source.RunnerConfig{
		Source:      "dispatch",
		LaneProfile: laneName,
		Namespace:   namespace,
	})
	lane := envtestLaneInNamespace(laneName, namespace)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if adapter.claimed != nil {
		t.Fatalf("discovery claimed work: %#v", adapter.claimed)
	}
	var runs courierv1alpha1.CoderRunList
	if err := envtestClient.List(ctx, &runs, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("discovery created %d runs, want 2", len(runs.Items))
	}
	for _, run := range runs.Items {
		if run.Spec.Source != "dispatch" || run.Spec.WorkItemID == "" {
			t.Fatalf("run identity = %#v", run.Spec)
		}
		if run.Status.Phase != "" {
			t.Fatalf("new run phase = %q, want empty", run.Status.Phase)
		}
	}
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.List(ctx, &runs, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 || adapter.claimed != nil {
		t.Fatalf("repeat discovery created or claimed work: runs=%d claims=%#v", len(runs.Items), adapter.claimed)
	}

	implementName := sourceRunName("dispatch", adapter.items[0].ID)
	implementKey := types.NamespacedName{Name: implementName, Namespace: namespace}
	var implement courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, implementKey, &implement); err != nil {
		t.Fatal(err)
	}
	if implement.Spec.Mode != courierv1alpha1.ModeResolveIssue || implement.Spec.Source != "dispatch" || implement.Spec.WorkItemID != "implement/issue-17" || implement.Spec.Repo != "acme/widgets" || implement.Spec.Ref != 17 || implement.Spec.Lane != laneName {
		t.Fatalf("implement spec = %#v, want resolve-issue dispatch opaque ID with lane %q", implement.Spec, laneName)
	}
	implementReconciler := &CoderRunReconciler{
		Client:       envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"dispatch": adapter}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			greenObservation("sha-17", "build", "review"),
			greenObservation("sha-17", "build", "review"),
		}},
		Launch: envtestLaunch(t, adapter, false),
	}
	if _, err := implementReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: implementKey}); err != nil {
		t.Fatal(err)
	}
	if len(adapter.claimed) != 1 || adapter.claimed[0] != adapter.items[0].ID {
		t.Fatalf("claims = %#v, want claim after discovery", adapter.claimed)
	}
	if len(adapter.transitions) != 1 || adapter.transitions[0] != source.StateInProgress {
		t.Fatalf("launch transitions = %#v, want in-progress", adapter.transitions)
	}
	if _, err := implementReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: implementKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := implementReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: implementKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := implementReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: implementKey}); err != nil {
		t.Fatal(err)
	}
	assertEnvtestPhaseInNamespace(t, implementKey, courierv1alpha1.PhaseAwaitingReview)
	if len(adapter.reports) != 1 || adapter.reports[0].Result != source.ResultReady || adapter.reports[0].PR != "42" {
		t.Fatalf("lifecycle reports = %#v, want ready PR 42", adapter.reports)
	}
	if len(adapter.transitions) != 2 || adapter.transitions[1] != source.StateInReview {
		t.Fatalf("lifecycle transitions = %#v, want in-progress then in-review", adapter.transitions)
	}

	followupName := sourceRunName("dispatch", adapter.items[1].ID)
	followupKey := types.NamespacedName{Name: followupName, Namespace: namespace}
	followupReconciler := &CoderRunReconciler{
		Client:       envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"dispatch": adapter}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		PRHeadResolver: ExistingPRHeadResolverFunc(func(_ context.Context, run *courierv1alpha1.CoderRun) (HeadRef, error) {
			return HeadRef{Repo: run.Spec.Repo, Branch: "feature/existing-pr-head", SHA: "abc123"}, nil
		}),
		Launch: envtestLaunch(t, adapter, false),
	}
	if _, err := followupReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: followupKey}); err != nil {
		t.Fatal(err)
	}
	var followup courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, followupKey, &followup); err != nil {
		t.Fatal(err)
	}
	if followup.Status.Branch != "feature/existing-pr-head" || followup.Status.HeadRepo != "acme/widgets" || followup.Status.HeadSHA != "abc123" {
		t.Fatalf("followup head = branch %q repo %q sha %q, want existing PR head", followup.Status.Branch, followup.Status.HeadRepo, followup.Status.HeadSHA)
	}
	if followup.Spec.Mode != courierv1alpha1.ModeFixPR || followup.Spec.WorkItemID != adapter.items[1].ID || followup.Spec.Ref != 42 {
		t.Fatalf("followup spec = %#v, want fix-pr with opaque ID and PR ref", followup.Spec)
	}
	if len(adapter.claimed) != 2 || adapter.claimed[1] != adapter.items[1].ID {
		t.Fatalf("followup claims = %#v, want opaque followup ID", adapter.claimed)
	}
	if len(adapter.transitions) != 3 || adapter.transitions[2] != source.StateInProgress {
		t.Fatalf("followup launch transitions = %#v, want in-progress after implement lifecycle", adapter.transitions)
	}

	staleName := "envtest-stale-prelaunch"
	staleLaneName := "stale-lane"
	if err := envtestClient.Create(ctx, envtestLaneInNamespace(staleLaneName, namespace)); err != nil {
		t.Fatal(err)
	}
	stale := envtestRunInNamespace(staleName, staleLaneName, namespace)
	stale.Spec.Source = "dispatch"
	stale.Spec.WorkItemID = "stale/opaque-id"
	if err := envtestClient.Create(ctx, stale); err != nil {
		t.Fatal(err)
	}
	adapter.items = append(adapter.items, source.WorkItem{ID: stale.Spec.WorkItemID, Mode: string(stale.Spec.Mode), Repo: stale.Spec.Repo, Ref: stale.Spec.Ref})
	adapter.preLaunchErr = source.ErrStaleWork
	staleReconciler := &CoderRunReconciler{
		Client:       envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"dispatch": adapter}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Launch:       envtestLaunch(t, adapter, false),
	}
	staleKey := types.NamespacedName{Name: staleName, Namespace: namespace}
	if _, err := staleReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: staleKey}); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("stale Reconcile() error = %v, want %v", err, source.ErrStaleWork)
	}
	var deleted courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, staleKey, &deleted); !apierrors.IsNotFound(err) {
		t.Fatalf("stale run get error = %v, want deleted", err)
	}
	if adapter.launched != 2 {
		t.Fatalf("launches = %d, want implement and followup only", adapter.launched)
	}

	failureName := "envtest-launch-failure"
	failureLaneName := "failure-lane"
	if err := envtestClient.Create(ctx, envtestLaneInNamespace(failureLaneName, namespace)); err != nil {
		t.Fatal(err)
	}
	failure := envtestRunInNamespace(failureName, failureLaneName, namespace)
	failure.Spec.Source = "dispatch"
	failure.Spec.WorkItemID = "failure/opaque-id"
	if err := envtestClient.Create(ctx, failure); err != nil {
		t.Fatal(err)
	}
	failureAdapter := &envtestDispatchAdapter{}
	failureErr := errors.New("envtest launch failed")
	failureReconciler := &CoderRunReconciler{
		Client:       envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"dispatch": failureAdapter}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Launch: func(context.Context, *courierv1alpha1.CoderRun) error {
			return failureErr
		},
	}
	failureKey := types.NamespacedName{Name: failureName, Namespace: namespace}
	if _, err := failureReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: failureKey}); !errors.Is(err, failureErr) {
		t.Fatalf("launch failure Reconcile() error = %v, want %v", err, failureErr)
	}
	var released courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, failureKey, &released); err != nil {
		t.Fatal(err)
	}
	if released.Status.Phase != courierv1alpha1.PhasePending || released.Status.Branch != "" {
		t.Fatalf("failed launch status = phase %q branch %q, want Pending and empty branch", released.Status.Phase, released.Status.Branch)
	}
	if len(failureAdapter.claimed) != 1 || len(failureAdapter.released) != 1 {
		t.Fatalf("failed launch claim/release = %#v/%#v", failureAdapter.claimed, failureAdapter.released)
	}
}

func envtestLaunch(t *testing.T, adapter *envtestDispatchAdapter, fail bool) LaunchFunc {
	t.Helper()
	return func(ctx context.Context, run *courierv1alpha1.CoderRun) error {
		adapter.launched++
		var claimed courierv1alpha1.CoderRun
		if err := envtestClient.Get(ctx, types.NamespacedName{Name: run.Name, Namespace: run.Namespace}, &claimed); err != nil {
			return err
		}
		if claimed.Status.Phase != courierv1alpha1.PhaseClaimed {
			return errors.New("envtest launch observed non-claimed run")
		}
		if fail {
			return errors.New("envtest launch failed")
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
	}
}

type envtestDispatchAdapter struct {
	items        []source.WorkItem
	claimed      []string
	released     []string
	transitions  []source.State
	reports      []source.Lifecycle
	preLaunchErr error
	launched     int
}

func (a *envtestDispatchAdapter) Discover(context.Context) ([]source.WorkItem, error) {
	return a.items, nil
}
func (a *envtestDispatchAdapter) Claim(_ context.Context, item source.WorkItem) error {
	a.claimed = append(a.claimed, item.ID)
	return nil
}
func (a *envtestDispatchAdapter) Release(_ context.Context, item source.WorkItem) error {
	a.released = append(a.released, item.ID)
	return nil
}
func (a *envtestDispatchAdapter) Transition(_ context.Context, _ source.WorkItem, state source.State) error {
	a.transitions = append(a.transitions, state)
	return nil
}
func (a *envtestDispatchAdapter) Report(_ context.Context, _ source.WorkItem, lifecycle source.Lifecycle) error {
	a.reports = append(a.reports, lifecycle)
	return nil
}
func (a *envtestDispatchAdapter) PreLaunch(context.Context, source.WorkItem) error {
	return a.preLaunchErr
}
func (a *envtestDispatchAdapter) Resolve(context.Context, source.WorkItem) error { return nil }

func sourceRunName(sourceName, workItemID string) string {
	digest := sha256.Sum256([]byte(sourceName + "\x00" + workItemID))
	return "courier-" + hex.EncodeToString(digest[:])[:32]
}

func envtestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = courierv1alpha1.AddToScheme(scheme)
	return scheme
}

func envtestRun(name, lane string) *courierv1alpha1.CoderRun {
	return envtestRunInNamespace(name, lane, "default")
}

func envtestRunInNamespace(name, lane, namespace string) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
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
	return envtestLaneInNamespace(name, "default")
}

func envtestLaneInNamespace(name, namespace string) *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
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
	assertEnvtestPhaseInNamespace(t, envtestKey(name), want)
}

func assertEnvtestPhaseInNamespace(t *testing.T, key types.NamespacedName, want courierv1alpha1.Phase) {
	t.Helper()
	var run courierv1alpha1.CoderRun
	if err := envtestClient.Get(context.Background(), key, &run); err != nil {
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
