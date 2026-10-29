package source

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// requeueFake models the fenced AttemptRequeuer contract the controller envtests
// drive: one atomic requeue per committed idempotency key, whose original result
// replays forever without mutating the now-newer owner, and whose stale fence is
// refused without touching that owner.
type requeueFake struct {
	ownerFence string
	committed  map[string]AttemptRequeueResult
}

var _ AttemptRequeuer = (*requeueFake)(nil)

func newRequeueFake(initialFence string) *requeueFake {
	return &requeueFake{ownerFence: initialFence, committed: map[string]AttemptRequeueResult{}}
}

func (f *requeueFake) RequeueAttempt(_ context.Context, _ WorkItem, req AttemptRequeueRequest) (AttemptRequeueResult, error) {
	// A committed key replays its original success first: the response may have
	// been lost after the source committed, so the fence has already advanced and
	// the replay must not be reclassified as a stale refusal.
	if result, ok := f.committed[req.IdempotencyKey]; ok {
		return result, nil
	}
	// A fence the source no longer records belongs to a superseded attempt: refuse
	// without minting or mutating the current owner.
	if req.FenceToken != f.ownerFence {
		return AttemptRequeueResult{}, &RetryRefusalError{Reason: courierv1alpha1.RetryRefusalStaleGeneration}
	}
	result := AttemptRequeueResult{AttemptID: "attempt"}
	if req.Reason != "" {
		result.AttemptID += "-" + req.Reason
	}
	f.committed[req.IdempotencyKey] = result
	// The source re-offers the work as a fresh attempt under a fresh fence.
	f.ownerFence = "fence-for-" + result.AttemptID
	return result, nil
}

func TestRequeueFakeCommittedKeyReplaysWithoutMutatingOwner(t *testing.T) {
	fake := newRequeueFake("fence-A")
	first, err := fake.RequeueAttempt(context.Background(), WorkItem{ID: "old-1"}, AttemptRequeueRequest{FenceToken: "fence-A", IdempotencyKey: "run-uid/req-1", Reason: "req-1"})
	if err != nil {
		t.Fatalf("first requeue: %v", err)
	}
	if first.AttemptID == "" {
		t.Fatal("first requeue returned an empty attempt identity")
	}
	ownerAfterCommit := fake.ownerFence

	// A replay under the committed key presents the now-stale original fence; it
	// must return the original result and leave the advanced owner untouched.
	replay, err := fake.RequeueAttempt(context.Background(), WorkItem{ID: "old-1"}, AttemptRequeueRequest{FenceToken: "fence-A", IdempotencyKey: "run-uid/req-1", Reason: "req-1"})
	if err != nil {
		t.Fatalf("replay of committed key: %v", err)
	}
	if replay != first {
		t.Fatalf("replay = %+v, want original %+v", replay, first)
	}
	if fake.ownerFence != ownerAfterCommit {
		t.Fatalf("replay mutated owner fence to %q, want %q", fake.ownerFence, ownerAfterCommit)
	}
}

func TestRequeueFakeStaleFenceRefusesWithoutTouchingOwner(t *testing.T) {
	fake := newRequeueFake("fence-A")
	if _, err := fake.RequeueAttempt(context.Background(), WorkItem{ID: "old-1"}, AttemptRequeueRequest{FenceToken: "fence-A", IdempotencyKey: "run-uid/req-1", Reason: "req-1"}); err != nil {
		t.Fatalf("seed requeue: %v", err)
	}
	ownerBefore := fake.ownerFence
	committedBefore := len(fake.committed)

	// A different request (new key) presenting the superseded fence is a stale
	// generation refusal, and it must not commit or advance the current owner.
	_, err := fake.RequeueAttempt(context.Background(), WorkItem{ID: "old-1"}, AttemptRequeueRequest{FenceToken: "fence-A", IdempotencyKey: "run-uid/req-2", Reason: "req-2"})
	var refusal *RetryRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("stale-fence error = %v, want a RetryRefusalError", err)
	}
	if refusal.Reason != courierv1alpha1.RetryRefusalStaleGeneration {
		t.Fatalf("refusal reason = %q, want %q", refusal.Reason, courierv1alpha1.RetryRefusalStaleGeneration)
	}
	if refusal.Moot {
		t.Fatal("a stale fence is a refusal, not a moot classification")
	}
	if fake.ownerFence != ownerBefore || len(fake.committed) != committedBefore {
		t.Fatalf("stale refusal mutated owner state: fence %q, committed %d", fake.ownerFence, len(fake.committed))
	}
}

// requeuePredecessor returns a terminal run carrying a settled retry record.
func requeuePredecessor(name, workItemID string, uid types.UID, created time.Time, record *courierv1alpha1.RetryRequest) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "courier", UID: uid, CreationTimestamp: metav1.NewTime(created)},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "test", WorkItemID: workItemID, Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseFailed, RetryRequest: record},
	}
}

func TestRunnerRetrySettledPredecessorNamesSuccessorAndDedupesOldIdentity(t *testing.T) {
	now := time.Now()
	predecessor := requeuePredecessor("predecessor", "old-1", "pred-uid", now.Add(-time.Hour), &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(now.Add(-time.Hour)), State: courierv1alpha1.RetryRequestStateRequeued, AttemptID: "fresh-2", SettledAt: ptrMetaTime(now.Add(-time.Minute)),
	})
	lane := testLane()
	lane.Spec.Concurrency = 2
	// The fresh attempt is what the source re-offers; it is a different work
	// identity from the predecessor's consumed one.
	adapter := &excludingTestAdapter{testAdapter: testAdapter{items: []WorkItem{{ID: "fresh-2", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}}
	kube := newTestClient(t, lane, predecessor)
	runner := NewRunner(kube, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	ctx := context.Background()
	// Read the settled status back through the client so the auditability
	// comparison is against the same serialization the later read returns.
	var before courierv1alpha1.CoderRun
	if err := kube.Get(ctx, client.ObjectKeyFromObject(predecessor), &before); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}

	var runs courierv1alpha1.CoderRunList
	if err := runner.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("runs = %d, want predecessor plus one successor", len(runs.Items))
	}
	wantName := successorRunName("test", "fresh-2", types.UID("pred-uid"))
	var successor *courierv1alpha1.CoderRun
	for i := range runs.Items {
		if runs.Items[i].Name == wantName {
			successor = &runs.Items[i]
		}
	}
	if successor == nil {
		t.Fatalf("successor %q not materialized; runs = %v", wantName, runNames(runs.Items))
	}
	if successor.Spec.WorkItemID != "fresh-2" || successor.Status.Phase != "" {
		t.Fatalf("successor = %#v, want fresh attempt in no phase", successor)
	}

	// The predecessor's consumed WorkItemID remains in the discovery exclusions:
	// the retry predicate never joins the exclusion scan.
	if !containsID(adapter.exclusions, "old-1") {
		t.Fatalf("exclusions = %#v, want the consumed predecessor identity retained", adapter.exclusions)
	}

	// Historical auditability: the runner never mutated the settled predecessor.
	var stored courierv1alpha1.CoderRun
	if err := runner.Get(ctx, client.ObjectKeyFromObject(predecessor), &stored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored.Status, before.Status) {
		t.Fatalf("predecessor status mutated:\n got %#v\nwant %#v", stored.Status, before.Status)
	}

	// A re-offer of the fresh identity dedupes to the one successor, and a stale
	// re-offer of the consumed identity stays deduped: neither adds a run.
	adapter.items = []WorkItem{{ID: "fresh-2", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}, {ID: "old-1", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runner.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("after re-offer runs = %d, want still 2; runs = %v", len(runs.Items), runNames(runs.Items))
	}
}

func TestRunnerRetryConcurrentRunnersCreateOneSuccessor(t *testing.T) {
	now := time.Now()
	predecessor := requeuePredecessor("predecessor", "old-1", "pred-uid", now.Add(-time.Hour), &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(now.Add(-time.Hour)), State: courierv1alpha1.RetryRequestStateRequeued, AttemptID: "fresh-2",
	})
	kube := newTestClient(t, testLane(), predecessor)
	config := RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"}
	runnerA := NewRunner(kube, &testAdapter{items: []WorkItem{{ID: "fresh-2", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}, config)
	runnerB := NewRunner(kube, &testAdapter{items: []WorkItem{{ID: "fresh-2", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}, config)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, r := range []*Runner{runnerA, runnerB} {
		wg.Add(1)
		go func(r *Runner) { defer wg.Done(); errs <- r.Poll(ctx) }(r)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var runs courierv1alpha1.CoderRunList
	if err := kube.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("runs = %d, want predecessor plus exactly one successor", len(runs.Items))
	}
	// Exactly one run carries the predecessor-UID successor name: the absorbed
	// AlreadyExists landed on the successor path, not an ordinary fallback.
	wantName := successorRunName("test", "fresh-2", types.UID("pred-uid"))
	successors := 0
	for _, run := range runs.Items {
		if run.Name == wantName {
			successors++
		}
	}
	if successors != 1 {
		t.Fatalf("successor %q count = %d, want exactly one; runs = %v", wantName, successors, runNames(runs.Items))
	}
}

// TestRunnerRetryCorrelatesPredecessorByNormalizedAttemptID proves the retry
// link is correlated through the adapter's WorkIdentity normalization, not raw
// string equality — the Dispatch case, where a fresh generation keeps a work
// identity the predecessor's consumed attempt can never match.
func TestRunnerRetryCorrelatesPredecessorByNormalizedAttemptID(t *testing.T) {
	now := time.Now()
	// identityAdapter strips incidental metadata after "|": WorkIdentity is the
	// part before it. The predecessor's consumed attempt ("issue-7@gen1") and the
	// requeued fresh attempt ("issue-7@gen2") are different generations, so the
	// fresh offer is NOT deduped against the predecessor; only normalizing the
	// recorded attempt ID links them. The adapter must return a successor named
	// from the normalized identity, never the ordinary name an unlinked offer takes.
	predecessor := requeuePredecessor("predecessor", "issue-7@gen1|consumed-url", "pred-uid", now.Add(-time.Hour), &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(now.Add(-time.Hour)), State: courierv1alpha1.RetryRequestStateRequeued, AttemptID: "issue-7@gen2|settled-url",
	})
	lane := testLane()
	lane.Spec.Concurrency = 2
	adapter := &identityAdapter{testAdapter: testAdapter{items: []WorkItem{{ID: "issue-7@gen2|reoffer-url", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}}
	runner := NewRunner(newTestClient(t, lane, predecessor), adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	ctx := context.Background()
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := runner.List(ctx, &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("runs = %d, want predecessor plus one successor (the fresh generation must not dedupe against the predecessor)", len(runs.Items))
	}
	wantName := successorRunName("test", "issue-7@gen2", types.UID("pred-uid"))
	if !containsName(runs.Items, wantName) {
		t.Fatalf("successor under the normalized identity %q not materialized; runs = %v", wantName, runNames(runs.Items))
	}
	if containsName(runs.Items, runName("test", "issue-7@gen2")) {
		t.Fatalf("offer took the ordinary name; normalization-based correlation did not fire; runs = %v", runNames(runs.Items))
	}
}

func TestRunnerNonRequeueRecordsKeepTheBarrierAndNeverNameSuccessor(t *testing.T) {
	now := time.Now()
	// A Moot record and a Requeued-without-attemptID record must both leave the
	// predecessor's identity as a barrier (its own re-offer dedupes) and must not
	// lend the predecessor UID to a fresh offer.
	cases := []struct {
		name   string
		record *courierv1alpha1.RetryRequest
	}{
		{"moot", &courierv1alpha1.RetryRequest{ID: "req-1", RequestedAt: metav1.NewTime(now.Add(-time.Hour)), State: courierv1alpha1.RetryRequestStateMoot, Reason: courierv1alpha1.RetryRefusalAlreadyRequeued, AttemptID: "fresh-9", SettledAt: ptrMetaTime(now)}},
		{"requeued-no-attempt", &courierv1alpha1.RetryRequest{ID: "req-1", RequestedAt: metav1.NewTime(now.Add(-time.Hour)), State: courierv1alpha1.RetryRequestStateRequeued, SettledAt: ptrMetaTime(now)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			predecessor := requeuePredecessor("predecessor", "old-1", "pred-uid", now.Add(-time.Hour), tc.record)
			lane := testLane()
			lane.Spec.Concurrency = 2
			adapter := &excludingTestAdapter{testAdapter: testAdapter{items: []WorkItem{{ID: "fresh-9", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}}
			runner := NewRunner(newTestClient(t, lane, predecessor), adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
			ctx := context.Background()
			if err := runner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			var runs courierv1alpha1.CoderRunList
			if err := runner.List(ctx, &runs, client.InNamespace("courier")); err != nil {
				t.Fatal(err)
			}
			// fresh-9 materializes under the ordinary deterministic name: the
			// non-requeue record claims no predecessor link.
			ordinary := runName("test", "fresh-9")
			if successorRunName("test", "fresh-9", types.UID("pred-uid")) == ordinary {
				t.Fatal("test fixture collision between ordinary and successor names")
			}
			if !containsName(runs.Items, ordinary) {
				t.Fatalf("fresh offer did not materialize under ordinary name %q; runs = %v", ordinary, runNames(runs.Items))
			}
			if containsName(runs.Items, successorRunName("test", "fresh-9", types.UID("pred-uid"))) {
				t.Fatalf("a non-requeue record named a successor by predecessor UID; runs = %v", runNames(runs.Items))
			}
			// The consumed identity stays excluded; a stale re-offer of it adds no run.
			if !containsID(adapter.exclusions, "old-1") {
				t.Fatalf("exclusions = %#v, want the consumed predecessor identity retained", adapter.exclusions)
			}
			adapter.items = []WorkItem{{ID: "old-1", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}
			if err := runner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			if err := runner.List(ctx, &runs, client.InNamespace("courier")); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("re-offer of the consumed identity added a run: %v", runNames(runs.Items))
			}
		})
	}
}

func ptrMetaTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

func runNames(runs []courierv1alpha1.CoderRun) []string {
	names := make([]string, 0, len(runs))
	for _, run := range runs {
		names = append(names, run.Name)
	}
	return names
}

func containsName(runs []courierv1alpha1.CoderRun, name string) bool {
	for _, run := range runs {
		if run.Name == name {
			return true
		}
	}
	return false
}

func containsID(items []WorkItem, id string) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}
