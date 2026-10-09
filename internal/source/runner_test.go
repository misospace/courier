package source

import (
	"context"
	"errors"
	"strings"
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

func TestRunnersWithDifferentBindingsMaterializeOneAtomicRun(t *testing.T) {
	item := WorkItem{ID: "queue-item/generation-1", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}
	kubeClient := newTestClient(t, testLane())
	first := NewRunner(kubeClient, &testAdapter{items: []WorkItem{item}}, RunnerConfig{Source: "dispatch", SourceAgent: "courier-local", LaneProfile: "local", Namespace: "courier"})
	second := NewRunner(kubeClient, &testAdapter{items: []WorkItem{item}}, RunnerConfig{Source: "dispatch", SourceAgent: "courier-cloud", LaneProfile: "local", Namespace: "courier"})
	if err := first.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := second.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs across bindings, want atomic single run", len(runs.Items))
	}
}

func TestConcurrentBindingsDeduplicateByCanonicalIdentity(t *testing.T) {
	itemA := WorkItem{ID: "raw-pr-url", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}
	itemB := WorkItem{ID: "raw-ci-job-url", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}
	baseClient := newTestClient(t, testLane())
	createBarrier := make(chan struct{})
	barrierClient := &createBarrierClient{Client: baseClient, arrived: make(chan struct{}, 2), release: createBarrier}
	first := NewRunner(barrierClient, &mappedIdentityAdapter{testAdapter: testAdapter{items: []WorkItem{itemA}}, identity: "prfix:queue-item:1"}, RunnerConfig{Source: "dispatch", SourceAgent: "agent-a", LaneProfile: "local", Namespace: "courier"})
	second := NewRunner(barrierClient, &mappedIdentityAdapter{testAdapter: testAdapter{items: []WorkItem{itemB}}, identity: "prfix:queue-item:1"}, RunnerConfig{Source: "dispatch", SourceAgent: "agent-b", LaneProfile: "local", Namespace: "courier"})

	results := make(chan error, 2)
	go func() { results <- first.Poll(context.Background()) }()
	go func() { results <- second.Poll(context.Background()) }()
	<-barrierClient.arrived
	<-barrierClient.arrived
	close(createBarrier)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}

	var runs courierv1alpha1.CoderRunList
	if err := baseClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs for one canonical identity, want 1", len(runs.Items))
	}
	run := runs.Items[0]
	if run.Spec.SourceAgent != "agent-a" && run.Spec.SourceAgent != "agent-b" {
		t.Fatalf("sourceAgent = %q, want one of the competing bindings", run.Spec.SourceAgent)
	}
	if run.Spec.WorkItemID != itemA.ID && run.Spec.WorkItemID != itemB.ID {
		t.Fatalf("workItemID = %q, want the original opaque ID from the winning binding", run.Spec.WorkItemID)
	}
	if (run.Spec.SourceAgent == "agent-a" && run.Spec.WorkItemID != itemA.ID) || (run.Spec.SourceAgent == "agent-b" && run.Spec.WorkItemID != itemB.ID) {
		t.Fatalf("winner fields do not match: sourceAgent=%q workItemID=%q", run.Spec.SourceAgent, run.Spec.WorkItemID)
	}
}

type createBarrierClient struct {
	client.Client
	arrived chan struct{}
	release <-chan struct{}
}

func (c *createBarrierClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.arrived <- struct{}{}
	<-c.release
	return c.Client.Create(ctx, obj, opts...)
}

func TestRunnerPreservesRetainedLegacyDispatchRun(t *testing.T) {
	item := WorkItem{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}
	legacy := &courierv1alpha1.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: runName("dispatch:local", item.ID), Namespace: "courier"}, Spec: courierv1alpha1.CoderRunSpec{Source: "dispatch:local", WorkItemID: item.ID, Mode: courierv1alpha1.ModeResolveIssue, Repo: item.Repo, Ref: item.Ref, Lane: "local"}}
	kubeClient := newTestClient(t, testLane(), legacy)
	runner := NewRunner(kubeClient, &testAdapter{items: []WorkItem{item}}, RunnerConfig{Source: "dispatch", SourceAgent: "courier-local", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Spec.Source != "dispatch:local" {
		t.Fatalf("retained dispatch run was duplicated: %#v", runs.Items)
	}
}

func TestRunnerPersistsSourceAgentWithoutChangingSourceIdentity(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", SourceAgent: "courier-local", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Spec.Source != "dispatch" || runs.Items[0].Spec.SourceAgent != "courier-local" {
		t.Fatalf("materialized identity = %#v, want dispatch/courier-local", runs.Items)
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

func TestRunnerDeduplicatesByAdapterWorkIdentity(t *testing.T) {
	adapter := &identityAdapter{testAdapter: testAdapter{items: []WorkItem{{ID: "attempt-1|via-pr-url", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The same attempt offered again with different incidental metadata.
	adapter.items[0].ID = "attempt-1|via-ci-job-url"
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs for one attempt, want 1", len(runs.Items))
	}

	adapter.items[0].ID = "attempt-2|via-pr-url"
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("created %d runs, want a fresh run for a new attempt", len(runs.Items))
	}
}

// identityAdapter treats everything after "|" as incidental metadata.
type identityAdapter struct {
	testAdapter
}

type mappedIdentityAdapter struct {
	testAdapter
	identity string
}

func (a *mappedIdentityAdapter) WorkIdentity(string) string { return a.identity }

func (a *identityAdapter) WorkIdentity(workItemID string) string {
	identity, _, _ := strings.Cut(workItemID, "|")
	return identity
}

func TestRunnerSkipsDiscoveryWhileLaneSuspended(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	lane := testLane()
	lane.Annotations = map[string]string{courierv1alpha1.SuspendAnnotation: "true"}
	kubeClient := newTestClient(t, lane)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 {
		t.Fatalf("discover called %d times on a suspended lane, want 0", adapter.discoverCalls)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("created %d runs on a suspended lane, want 0", len(runs.Items))
	}

	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(lane), lane); err != nil {
		t.Fatal(err)
	}
	delete(lane.Annotations, courierv1alpha1.SuspendAnnotation)
	if err := kubeClient.Update(context.Background(), lane); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 1 {
		t.Fatalf("discover called %d times after resume, want 1", adapter.discoverCalls)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs after resume, want 1", len(runs.Items))
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

func TestRunnerLaneWaitingStateTransitions(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{{ID: "x", Mode: "resolve-issue", Repo: "a/b", Ref: 1}}}
	kubeClient := newTestClient(t)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	// Poll 1: lane missing → waiting state entered (first log fires).
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !runner.laneWaiting {
		t.Fatal("expected laneWaiting=true after first poll with missing lane")
	}

	// Poll 2: lane still missing → waiting state maintained (log suppressed).
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !runner.laneWaiting {
		t.Fatal("expected laneWaiting=true after second poll with still-missing lane")
	}
	if adapter.discoverCalls != 0 {
		t.Fatalf("discovery ran %d times while lane missing", adapter.discoverCalls)
	}

	// Lane appears → recovery logged, waiting state cleared.
	if err := kubeClient.Create(context.Background(), testLane()); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runner.laneWaiting {
		t.Fatal("expected laneWaiting=false after lane appeared")
	}
	if adapter.discoverCalls != 1 {
		t.Fatalf("discovery ran %d times after lane appeared, want 1", adapter.discoverCalls)
	}

	// Poll again: lane present → no spurious transition.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runner.laneWaiting {
		t.Fatal("expected laneWaiting=false on steady-state poll with lane present")
	}

	// Lane deleted → new waiting transition (log fires again).
	if err := kubeClient.Delete(context.Background(), testLane()); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !runner.laneWaiting {
		t.Fatal("expected laneWaiting=true after lane deleted")
	}
	if adapter.discoverCalls != 2 {
		t.Fatalf("discovery ran %d times after deletion, want 2 (from polls 3 and 4)", adapter.discoverCalls)
	}

	// Poll again after deletion → suppressed.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !runner.laneWaiting {
		t.Fatal("expected laneWaiting=true on sustained absence")
	}
	if adapter.discoverCalls != 2 {
		t.Fatalf("discovery ran %d times on sustained absence, want 2", adapter.discoverCalls)
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

func TestRunnerPollSkipsWorkThatAlreadyHasARun(t *testing.T) {
	existing := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "courier-existing"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     "dispatch",
			WorkItemID: "opaque-a",
			Repo:       "acme/widgets",
			Ref:        1,
			Lane:       "local",
		},
	}
	adapter := &testAdapter{items: []WorkItem{
		{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1},
		{ID: "opaque-b", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 2},
	}}
	kubeClient := newTestClient(t, testLane(), existing)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("created %d runs, want the pre-existing run plus one for the new item", len(runs.Items))
	}
	newRuns := 0
	for _, run := range runs.Items {
		if run.Spec.WorkItemID == "opaque-b" {
			newRuns++
		}
	}
	if newRuns != 1 {
		t.Fatalf("found %d runs for the new item, want 1", newRuns)
	}

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("second poll created %d runs, want none new", len(runs.Items))
	}
}

func TestRunnerBindingsDedupeIndependently(t *testing.T) {
	laneA := testLane()
	laneA.Name = "default"
	laneB := testLane()
	laneB.Name = "escalation"
	kubeClient := newTestClient(t, laneA, laneB)
	adapterA := &testAdapter{items: []WorkItem{{ID: "item-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	adapterB := &testAdapter{items: []WorkItem{{ID: "item-b", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 2}}}
	runnerA := NewRunner(kubeClient, adapterA, RunnerConfig{Source: "dispatch", LaneProfile: "default", Namespace: "courier"})
	runnerB := NewRunner(kubeClient, adapterB, RunnerConfig{Source: "dispatch", LaneProfile: "escalation", Namespace: "courier"})

	ctx := context.Background()
	if err := runnerA.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}
	if runs.Items[0].Spec.Lane != "default" {
		t.Fatalf("run lane = %q, want default", runs.Items[0].Spec.Lane)
	}

	if err := runnerB.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("created %d runs, want 2 (one per binding)", len(runs.Items))
	}
	lanes := map[string]int{}
	for _, run := range runs.Items {
		lanes[run.Spec.Lane]++
	}
	if lanes["default"] != 1 || lanes["escalation"] != 1 {
		t.Fatalf("run lanes = %#v, want one default and one escalation", lanes)
	}

	if err := runnerA.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runnerB.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("re-poll of both bindings created %d runs, want 2", len(runs.Items))
	}

	// The same work item offered through both bindings is one CoderRun.
	adapterB.items = []WorkItem{{ID: "item-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}
	if err := runnerB.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("same item across bindings created %d runs, want 2", len(runs.Items))
	}
	lanes = map[string]int{}
	for _, run := range runs.Items {
		lanes[run.Spec.Lane]++
	}
	if lanes["default"] != 1 || lanes["escalation"] != 1 {
		t.Fatalf("run lanes = %#v, want the existing default-lane run kept, not re-created on escalation", lanes)
	}
}

func TestRunnerBindingsSuspendIndependently(t *testing.T) {
	laneA := testLane()
	laneA.Name = "default"
	laneA.Annotations = map[string]string{courierv1alpha1.SuspendAnnotation: "true"}
	laneB := testLane()
	laneB.Name = "escalation"
	kubeClient := newTestClient(t, laneA, laneB)
	adapterA := &testAdapter{items: []WorkItem{{ID: "susp-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	adapterB := &testAdapter{items: []WorkItem{{ID: "susp-b", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 2}}}
	runnerA := NewRunner(kubeClient, adapterA, RunnerConfig{Source: "dispatch", LaneProfile: "default", Namespace: "courier"})
	runnerB := NewRunner(kubeClient, adapterB, RunnerConfig{Source: "dispatch", LaneProfile: "escalation", Namespace: "courier"})

	ctx := context.Background()
	if err := runnerA.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runnerB.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1 (the suspended binding discovers nothing)", len(runs.Items))
	}
	if runs.Items[0].Spec.Lane != "escalation" {
		t.Fatalf("run lane = %q, want escalation", runs.Items[0].Spec.Lane)
	}

	var laneProfile courierv1alpha1.LaneProfile
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(laneA), &laneProfile); err != nil {
		t.Fatal(err)
	}
	laneProfile.Annotations[courierv1alpha1.SuspendAnnotation] = "false"
	if err := kubeClient.Update(ctx, &laneProfile); err != nil {
		t.Fatal(err)
	}
	if err := runnerA.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("created %d runs after resume, want 2", len(runs.Items))
	}
}
