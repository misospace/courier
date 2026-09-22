package source

import (
	"context"
	"errors"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRunnerPollMaterializesAndDeduplicatesByOpaqueID(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{
		{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1, Lane: "source-lane"},
		{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/other", Ref: 99, Lane: "source-lane"},
		{ID: "opaque-b", Mode: "fix-pr", Repo: "acme/widgets", Ref: 2, Lane: "source-lane"},
	}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("created %d runs, want 2", len(runs.Items))
	}
	for _, run := range runs.Items {
		if run.Spec.Lane != "local" {
			t.Fatalf("lane = %q, want local", run.Spec.Lane)
		}
		if run.Spec.Source != "dispatch" || run.Spec.WorkItemID == "" {
			t.Fatalf("run identity = %#v", run.Spec)
		}
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("second poll created duplicate runs: %d", len(runs.Items))
	}
}

func TestRunnerCreatesFreshRunWhenWorkItemGenerationChanges(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "pr-fix/queue-item/generation-1", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	adapter.items[0].ID = "pr-fix/queue-item/generation-2"
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("created %d runs, want one run per generation", len(runs.Items))
	}
	seen := map[string]bool{}
	for _, run := range runs.Items {
		seen[run.Spec.WorkItemID] = true
	}
	if !seen["pr-fix/queue-item/generation-1"] || !seen["pr-fix/queue-item/generation-2"] {
		t.Fatalf("runs have work item IDs %#v", seen)
	}
}

func TestRunnerDoesNotClaimDuringDiscovery(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.claims != 0 {
		t.Fatalf("discovery claimed %d items", adapter.claims)
	}
}

func TestRunnerPollReturnsTransientDiscoveryFailure(t *testing.T) {
	want := errors.New("temporary upstream failure")
	adapter := &testAdapter{err: want}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Poll() error = %v, want %v", err, want)
	}
}

func TestRunnerPollsAreSerialized(t *testing.T) {
	discoverStarted := make(chan struct{}, 2)
	releaseDiscover := make(chan struct{})
	adapter := &testAdapter{discoverStarted: discoverStarted, releaseDiscover: releaseDiscover}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	firstDone := make(chan error, 1)
	go func() { firstDone <- runner.Poll(context.Background()) }()
	select {
	case <-discoverStarted:
	case <-time.After(time.Second):
		t.Fatal("first poll did not start discovery")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- runner.Poll(context.Background()) }()
	select {
	case <-discoverStarted:
		t.Fatal("second poll overlapped discovery")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseDiscover)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestRunnerPollHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll() error = %v, want context.Canceled", err)
	}
}

func TestRunnerSkipsMalformedItemsAndCreatesValidItems(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{
		{ID: "missing-mode", Mode: "unknown", Repo: "acme/widgets", Ref: 1},
		{ID: "missing-repo", Mode: "resolve-issue", Ref: 1},
		{ID: "valid", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1},
	}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Spec.WorkItemID != "valid" {
		t.Fatalf("runs = %#v, want only valid item", runs.Items)
	}
}

func TestRunnerSkipsInvalidModeWithoutCreatingRun(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "unknown", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v, want nil", err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("created %d runs, want none", len(runs.Items))
	}
}

func TestRunnerPollSkipsDiscoveryWhenLaneProfileMissing(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 {
		t.Fatalf("discovery ran %d times, want 0 while LaneProfile is missing", adapter.discoverCalls)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("created %d runs, want none", len(runs.Items))
	}
}

func TestRunnerPollDiscoversWhenLaneProfileExists(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 1 {
		t.Fatalf("discovery ran %d times, want 1", adapter.discoverCalls)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}
}

func TestRunnerPollResumesWhenLaneProfileAppears(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 {
		t.Fatalf("discovery ran %d times before LaneProfile existed", adapter.discoverCalls)
	}
	if err := kubeClient.Create(context.Background(), testLane()); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 1 {
		t.Fatalf("discovery ran %d times after LaneProfile appeared, want 1", adapter.discoverCalls)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}
}

func TestRunnerPollPausesWhenLaneProfileDeleted(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	lane := testLane()
	kubeClient := newTestClient(t, lane)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 1 {
		t.Fatalf("discovery ran %d times, want 1", adapter.discoverCalls)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}

	if err := kubeClient.Delete(context.Background(), lane); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 1 {
		t.Fatalf("discovery ran %d times after LaneProfile was deleted, want 1", adapter.discoverCalls)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("runs = %d, want the previously created run left untouched", len(runs.Items))
	}
}

func TestRunnerValidation(t *testing.T) {
	if err := (*Runner)(nil).Poll(context.Background()); err != ErrRunnerClientRequired {
		t.Fatalf("nil runner error = %v", err)
	}
	kubeClient := newTestClient(t)
	if err := NewRunner(kubeClient, nil, RunnerConfig{Source: "dispatch", Namespace: "courier"}).Poll(context.Background()); err != ErrRunnerAdapterRequired {
		t.Fatalf("nil adapter error = %v", err)
	}
	if err := NewRunner(kubeClient, &testAdapter{}, RunnerConfig{Namespace: "courier"}).Poll(context.Background()); err != ErrRunnerSourceRequired {
		t.Fatalf("empty source error = %v", err)
	}
	if err := NewRunner(kubeClient, &testAdapter{}, RunnerConfig{Source: "test", Namespace: "courier"}).Poll(context.Background()); err == nil {
		t.Fatal("missing LaneProfile was accepted")
	}
}

type testAdapter struct {
	items           []WorkItem
	claims          int
	discoverCalls   int
	err             error
	discoverStarted chan<- struct{}
	releaseDiscover <-chan struct{}
}

func (a *testAdapter) Discover(context.Context) ([]WorkItem, error) {
	a.discoverCalls++
	if a.discoverStarted != nil {
		a.discoverStarted <- struct{}{}
		<-a.releaseDiscover
	}
	return a.items, a.err
}
func (a *testAdapter) Claim(context.Context, WorkItem) error             { a.claims++; return nil }
func (a *testAdapter) Release(context.Context, WorkItem) error           { return nil }
func (a *testAdapter) Transition(context.Context, WorkItem, State) error { return nil }
func (a *testAdapter) Resolve(context.Context, WorkItem) error           { return nil }

func newTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
}

func testLane() *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "local"},
	}
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = courierv1alpha1.AddToScheme(scheme)
	return scheme
}
