package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

type admissionSource struct {
	claimed     []string
	released    []string
	transitions []source.State
	resolved    []string
}

func (s *admissionSource) Discover(context.Context) ([]source.WorkItem, error) { return nil, nil }
func (s *admissionSource) Claim(_ context.Context, item source.WorkItem) error {
	s.claimed = append(s.claimed, item.ID)
	return nil
}
func (s *admissionSource) Release(_ context.Context, item source.WorkItem) error {
	s.released = append(s.released, item.ID)
	return nil
}
func (s *admissionSource) Transition(_ context.Context, _ source.WorkItem, state source.State) error {
	s.transitions = append(s.transitions, state)
	return nil
}
func (s *admissionSource) Resolve(_ context.Context, item source.WorkItem) error {
	s.resolved = append(s.resolved, item.ID)
	return nil
}

func TestSourceRegistryUsesSpecSourceAndDurableWorkItemID(t *testing.T) {
	first := &admissionSource{}
	second := &admissionSource{}
	registry := NewSourceRegistry(map[string]source.Adapter{"first": first, "second": second})
	if got, err := registry.lookup("first"); err != nil || got != first {
		t.Fatalf("lookup(first) = %v, %v; want first adapter", got, err)
	}
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	run.Spec.Source = "first"
	run.Spec.WorkItemID = "opaque/source/id"
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{Client: client, Sources: registry, StatusWriter: fakeStatusWriter{client: client}}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(first.claimed) != 1 || first.claimed[0] != "opaque/source/id" {
		t.Fatalf("claimed IDs = %#v, want durable source ID", first.claimed)
	}
	if len(second.claimed) != 0 {
		t.Fatalf("wrong adapter claimed work: %#v", second.claimed)
	}
}

func TestOperatorStatusPatchesPreserveHarnessFields(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	run.Status.Checkpoint = &courierv1alpha1.Checkpoint{Plan: "keep this plan"}
	run.Status.Heartbeat = &courierv1alpha1.Heartbeat{Kind: "stream"}
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Checkpoint == nil || updated.Status.Checkpoint.Plan != "keep this plan" {
		t.Fatalf("checkpoint = %#v, operator patch overwrote harness checkpoint", updated.Status.Checkpoint)
	}
	if updated.Status.Heartbeat == nil || updated.Status.Heartbeat.Kind != "stream" {
		t.Fatalf("heartbeat = %#v, operator patch overwrote harness heartbeat", updated.Status.Heartbeat)
	}
}

func TestPendingClaimsBeforeBranchResolutionAndLaunch(t *testing.T) {
	item := &admissionSource{}
	var order []string
	resolver := ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (string, error) {
		order = append(order, "resolve")
		return "feature/existing-pr", nil
	})
	run := admissionRun("fix", "local", courierv1alpha1.PhasePending)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	run.Spec.Source = "dispatch"
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:         client,
		Sources:        NewSourceRegistry(map[string]source.Adapter{"dispatch": item}),
		PRHeadResolver: resolver,
		StatusWriter:   fakeStatusWriter{client: client},
		Launch: func(_ context.Context, run *courierv1alpha1.CoderRun) error {
			order = append(order, "launch")
			run.Status.Phase = courierv1alpha1.PhaseRunning
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("fix")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(item.claimed) != 1 || len(order) != 2 || order[0] != "resolve" || order[1] != "launch" {
		t.Fatalf("claim/resolve/launch order = claim %#v, order %#v", item.claimed, order)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("fix"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Branch != "feature/existing-pr" {
		t.Fatalf("branch = %q, want existing PR head", updated.Status.Branch)
	}
}

func TestFixPRRequiresInjectedHeadResolver(t *testing.T) {
	run := admissionRun("fix", "local", courierv1alpha1.PhasePending)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	_, err := resolveRunBranch(context.Background(), run, nil)
	if !errors.Is(err, ErrPRHeadResolverRequired) {
		t.Fatalf("resolveRunBranch() error = %v, want resolver-required error", err)
	}
}

func TestLaunchFailureReleasesSourceAndCapacity(t *testing.T) {
	item := &admissionSource{}
	wantErr := errors.New("pod launch failed")
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch:       func(context.Context, *courierv1alpha1.CoderRun) error { return wantErr },
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); !errors.Is(err, wantErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, wantErr)
	}
	if len(item.released) != 1 || item.released[0] != "run" {
		t.Fatalf("released IDs = %#v, want run", item.released)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhasePending || updated.Status.Branch != "" {
		t.Fatalf("run after failed launch = phase %q branch %q, want Pending and empty branch", updated.Status.Phase, updated.Status.Branch)
	}
}

func TestClaimedRunRetriesLaunchAfterPartialAdmission(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseClaimed)
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch: func(_ context.Context, run *courierv1alpha1.CoderRun) error {
			run.Status.Phase = courierv1alpha1.PhaseRunning
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if len(item.claimed) != 0 || len(item.released) != 0 {
		t.Fatalf("partial-admission retry changed claim state: claimed=%#v released=%#v", item.claimed, item.released)
	}
}

func TestRunningPodExitMapsPhaseAndSourceState(t *testing.T) {
	tests := []struct {
		name       string
		exitCode   int32
		wantPhase  courierv1alpha1.Phase
		wantSource source.State
	}{
		{name: "success", exitCode: 0, wantPhase: courierv1alpha1.PhaseAwaitingReview, wantSource: source.StateInReview},
		{name: "needs-human", exitCode: 2, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantSource: source.StateNeedsHuman},
		{name: "failure", exitCode: 17, wantPhase: courierv1alpha1.PhaseFailed, wantSource: source.StateNeedsHuman},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			run.Spec.Source = "test"
			run.Spec.WorkItemID = "opaque-work-item"
			pod := coordinatorPod(run, tt.exitCode)
			client := phaseClient(t, run, pod)
			reconciler := &CoderRunReconciler{
				Client:       client,
				Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
				StatusWriter: fakeStatusWriter{client: client},
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != tt.wantPhase {
				t.Fatalf("phase = %q, want %q", updated.Status.Phase, tt.wantPhase)
			}
			if len(item.transitions) != 1 || item.transitions[0] != tt.wantSource {
				t.Fatalf("source transitions = %#v, want %#v", item.transitions, []source.State{tt.wantSource})
			}
		})
	}
}

func TestDoneRunResolvesSourceWorkItem(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseDone)
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(item.resolved) != 1 || item.resolved[0] != "run" {
		t.Fatalf("resolved IDs = %#v, want run", item.resolved)
	}
}

func phaseClient(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(objects...).
		Build()
}

func coordinatorPod(run *courierv1alpha1.CoderRun, exitCode int32) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-coordinator",
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
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "coordinator",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode}},
		}}},
	}
}

const executorRunLabel = "courier.misospace.dev/coderrun"
