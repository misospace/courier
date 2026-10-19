package source

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
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
		{ID: "opaque-a", Context: `{"opaque":"receipt"}`, Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1, Lane: "source-lane"},
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
		if run.Spec.WorkItemID == "opaque-a" && run.Spec.SourceContext != `{"opaque":"receipt"}` {
			t.Fatalf("source context = %q, want opaque context preserved", run.Spec.SourceContext)
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

func TestRunnerDiscoveryExclusionsAreOpaqueAndSourceScoped(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 2
	retained := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "retained", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch:local", WorkItemID: "opaque/generation-7", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseDone},
	}
	unrelated := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "other", WorkItemID: "do-not-exclude", Mode: courierv1alpha1.ModeResolveIssue, Repo: "acme/other", Ref: 9, Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseDone},
	}
	adapter := &excludingTestAdapter{testAdapter: testAdapter{items: []WorkItem{{ID: "opaque/generation-8", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}}
	runner := NewRunner(newTestClient(t, lane, retained, unrelated), adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 || adapter.excludingCalls != 1 {
		t.Fatalf("Discover/DiscoverExcluding calls = %d/%d, want 0/1", adapter.discoverCalls, adapter.excludingCalls)
	}
	if len(adapter.exclusions) != 1 {
		t.Fatalf("exclusions = %#v, want only the retained dispatch run", adapter.exclusions)
	}
	want := (WorkItem{ID: "opaque/generation-7", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42})
	if !reflect.DeepEqual(adapter.exclusions[0], want) {
		t.Fatalf("exclusion = %#v, want %#v", adapter.exclusions[0], want)
	}
	var runs courierv1alpha1.CoderRunList
	if err := runner.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	foundNewer := false
	for _, run := range runs.Items {
		foundNewer = foundNewer || run.Spec.WorkItemID == "opaque/generation-8"
	}
	if !foundNewer {
		t.Fatal("newer generation was not eligible for materialization")
	}
}

func TestRunnerOrdersDiscoveryExclusionsMostRelevantFirst(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 8
	now := time.Now()
	run := func(name, id string, phase courierv1alpha1.Phase, age time.Duration) *courierv1alpha1.CoderRun {
		return &courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "courier", CreationTimestamp: metav1.NewTime(now.Add(-age))},
			Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: id, Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
			Status:     courierv1alpha1.CoderRunStatus{Phase: phase},
		}
	}
	// Unsettled runs sort ahead of terminal ones regardless of age; within a
	// group, newest first. Names deliberately do not sort in the wanted order,
	// so equal timestamps cannot pass by accident.
	runs := []*courierv1alpha1.CoderRun{
		run("z-done", "done-old", courierv1alpha1.PhaseDone, 5*time.Hour),
		run("a-running", "running-old", courierv1alpha1.PhaseRunning, 3*time.Hour),
		run("y-review", "review-new", courierv1alpha1.PhaseAwaitingReview, 30*time.Minute),
		run("b-verifying", "verifying-mid", courierv1alpha1.PhaseVerifying, 2*time.Hour),
		run("x-pending", "pending-new", courierv1alpha1.PhasePending, 1*time.Hour),
	}
	adapter := &excludingTestAdapter{}
	runner := NewRunner(newTestClient(t, lane, runs[0], runs[1], runs[2], runs[3], runs[4]), adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"pending-new", "verifying-mid", "running-old", "review-new", "done-old"}
	got := make([]string, 0, len(adapter.exclusions))
	for _, exclusion := range adapter.exclusions {
		got = append(got, exclusion.ID)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exclusion order = %v, want %v", got, want)
	}
}

func TestRunnerSkipsDiscoveryWhenLaneCapacityIsFull(t *testing.T) {
	item := WorkItem{ID: "queue-item/generation-1", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}
	localLane := testLane()
	localLane.Spec.Concurrency = 1
	minimaxLane := testLane()
	minimaxLane.Name = "minimax"
	minimaxLane.Spec.Concurrency = 1
	occupied := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "local-running", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "other-item", Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	kubeClient := newTestClient(t, localLane, minimaxLane, occupied)
	localAdapter := &testAdapter{items: []WorkItem{item}}
	cloudAdapter := &testAdapter{items: []WorkItem{item}}
	local := NewRunner(kubeClient, localAdapter, RunnerConfig{Source: "dispatch", SourceAgent: "courier-local", LaneProfile: "local", Namespace: "courier"})
	cloud := NewRunner(kubeClient, cloudAdapter, RunnerConfig{Source: "dispatch", SourceAgent: "courier-cloud", LaneProfile: "minimax", Namespace: "courier"})

	// The full local binding polls first; it must not claim the shared queue item
	// by materializing it before the cloud binding gets its turn.
	if err := local.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cloud.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if localAdapter.discoverCalls != 0 || cloudAdapter.discoverCalls != 1 {
		t.Fatalf("Discover() calls local/cloud = %d/%d, want 0/1", localAdapter.discoverCalls, cloudAdapter.discoverCalls)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("found %d runs, want the occupied local run plus one new run", len(runs.Items))
	}
	for _, run := range runs.Items {
		if run.Spec.WorkItemID == item.ID && (run.Spec.Lane != "minimax" || run.Spec.SourceAgent != "courier-cloud") {
			t.Fatalf("new run binding = lane %q, agent %q; want minimax/courier-cloud", run.Spec.Lane, run.Spec.SourceAgent)
		}
	}
}

func TestRunnerExcludesPendingHeadSoFreeCloudBindingGetsNextItem(t *testing.T) {
	localLane := testLane()
	localLane.Spec.Concurrency = 1
	cloudLane := testLane()
	cloudLane.Name = "cloud"
	cloudLane.Spec.Concurrency = 1
	pending := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-a", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "queue-a", Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhasePending},
	}
	adapter := &excludingTestAdapter{testAdapter: testAdapter{items: []WorkItem{{ID: "queue-b", Mode: "fix-pr", Repo: "acme/widgets", Ref: 43}}}}
	kubeClient := newTestClient(t, localLane, cloudLane, pending)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "cloud", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 || adapter.excludingCalls != 1 || len(adapter.exclusions) != 1 || adapter.exclusions[0].ID != "queue-a" {
		t.Fatalf("discovery/exclusions = %d/%d/%#v", adapter.discoverCalls, adapter.excludingCalls, adapter.exclusions)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("runs = %d, want pending A and new B", len(runs.Items))
	}
	for _, run := range runs.Items {
		if run.Spec.WorkItemID == "queue-a" && run.Spec.Lane != "local" {
			t.Fatalf("pending A changed: %#v", run.Spec)
		}
	}
}

func TestRunnerExclusionOmitsOnlySettledCapacityRuns(t *testing.T) {
	phases := []courierv1alpha1.Phase{
		"",
		courierv1alpha1.PhasePending,
		courierv1alpha1.PhaseClaimed,
		courierv1alpha1.PhaseRunning,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
		courierv1alpha1.PhaseHandedOff,
		courierv1alpha1.PhaseNeedsHuman,
		courierv1alpha1.PhaseDone,
		courierv1alpha1.PhaseFailed,
		courierv1alpha1.PhaseBlockedExternal,
	}
	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			lane := testLane()
			lane.Spec.Concurrency = 2
			run := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "retained", Namespace: "courier"},
				Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "opaque/generation-7", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
				Status:     courierv1alpha1.CoderRunStatus{Phase: phase},
			}
			adapter := &excludingTestAdapter{testAdapter: testAdapter{items: []WorkItem{{ID: "opaque/generation-8", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}}
			runner := NewRunner(newTestClient(t, lane, run), adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if adapter.excludingCalls != 1 {
				t.Fatalf("DiscoverExcluding calls = %d, want 1", adapter.excludingCalls)
			}
			if phase == courierv1alpha1.PhaseBlockedExternal {
				if len(adapter.exclusions) != 1 {
					t.Fatalf("exclusions = %#v, unsettled BlockedExternal must remain a barrier", adapter.exclusions)
				}
				return
			}
			want := (WorkItem{ID: "opaque/generation-7", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42})
			if len(adapter.exclusions) != 1 || !reflect.DeepEqual(adapter.exclusions[0], want) {
				t.Fatalf("exclusions = %#v, want the retained %s run", adapter.exclusions, phase)
			}
		})
	}
}

func TestRunnerSettledCapacityExhaustionAllowsSameOfferWithSuccessorUIDName(t *testing.T) {
	for _, category := range []courierv1alpha1.TerminalDiagnosticCategory{
		courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
		courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable,
	} {
		t.Run(string(category), func(t *testing.T) {
			lane := testLane()
			lane.Spec.Concurrency = 3
			old := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "old-attempt", Namespace: "courier", UID: "prior-uid"},
				Spec:       courierv1alpha1.CoderRunSpec{Source: "source", WorkItemID: "settled-token", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
				Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseBlockedExternal,
					TerminalDiagnostic: &courierv1alpha1.TerminalDiagnostic{Category: category},
					Conditions: []metav1.Condition{
						{Type: courierv1alpha1.ConditionCapacityRecovery, Status: metav1.ConditionTrue, Reason: "Settled"},
						{Type: courierv1alpha1.ConditionLifecycleReported, Status: metav1.ConditionTrue, Reason: "Published"},
					}},
			}
			item := WorkItem{ID: old.Spec.WorkItemID, Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}
			c := newTestClient(t, lane, old)
			runner := NewRunner(c, &testAdapter{items: []WorkItem{item}}, RunnerConfig{Source: "source", LaneProfile: "local", Namespace: "courier"})
			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			var runs courierv1alpha1.CoderRunList
			if err := c.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("runs=%d, want retained predecessor and successor", len(runs.Items))
			}
			wantName := successorRunName("source", item.ID, old.UID)
			found := false
			for _, run := range runs.Items {
				found = found || run.Name == wantName
			}
			if !found {
				t.Fatalf("successor %q not materialized; runs=%#v", wantName, runs.Items)
			}
		})
	}
}

func TestRunnerSettledCapacityExhaustionMaterializesSuccessor(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 3
	old := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "old-attempt", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "consumed-token", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseBlockedExternal,
			TerminalDiagnostic: &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion},
			Conditions: []metav1.Condition{
				{Type: courierv1alpha1.ConditionCapacityRecovery, Status: metav1.ConditionTrue, Reason: "Settled"},
				{Type: courierv1alpha1.ConditionLifecycleReported, Status: metav1.ConditionTrue, Reason: "Published"},
			}},
	}
	for _, tc := range []struct {
		name, id string
		wantRuns int
	}{{"same consumed token", "consumed-token", 2}, {"fresh token", "new-opaque-token", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &testAdapter{items: []WorkItem{{ID: tc.id, Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}
			c := newTestClient(t, lane, old.DeepCopy())
			runner := NewRunner(c, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			var runs courierv1alpha1.CoderRunList
			if err := c.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != tc.wantRuns {
				t.Fatalf("runs=%d, want %d", len(runs.Items), tc.wantRuns)
			}
		})
	}
}

// A blocked run whose lifecycle report is still pending keeps suppressing its
// identity, even when the source re-offers it.
func TestRunnerKeepsPendingBlockedExternalRunSuppressed(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 2
	blocked := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: runName("dispatch", "opaque/generation-7"), Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "opaque/generation-7", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
		Status: courierv1alpha1.CoderRunStatus{
			Phase: courierv1alpha1.PhaseBlockedExternal,
			Conditions: []metav1.Condition{{
				Type:               courierv1alpha1.ConditionLifecycleReported,
				Status:             metav1.ConditionFalse,
				Reason:             "Pending",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque/generation-7", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}
	kubeClient := newTestClient(t, lane, blocked)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("runs = %#v, want the pending blocked run kept (no delete, no create)", runs.Items)
	}
	if runs.Items[0].Status.Phase != courierv1alpha1.PhaseBlockedExternal {
		t.Fatalf("run phase = %q, want the original blocked run untouched", runs.Items[0].Status.Phase)
	}
}

// A published blocked run that the source does not re-offer is left in place.
func TestRunnerLeavesUnofferedPublishedBlockedExternalRun(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 2
	blocked := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: runName("dispatch", "opaque/generation-7"), Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "opaque/generation-7", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
		Status: courierv1alpha1.CoderRunStatus{
			Phase: courierv1alpha1.PhaseBlockedExternal,
			Conditions: []metav1.Condition{{
				Type:               courierv1alpha1.ConditionLifecycleReported,
				Status:             metav1.ConditionTrue,
				Reason:             "Published",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque/generation-8", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}
	kubeClient := newTestClient(t, lane, blocked)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("runs = %#v, want the published blocked run plus one run for the new identity", runs.Items)
	}
	kept := false
	for _, run := range runs.Items {
		if run.Spec.WorkItemID == "opaque/generation-7" && run.Status.Phase == courierv1alpha1.PhaseBlockedExternal {
			kept = true
		}
	}
	if !kept {
		t.Fatal("published blocked run was deleted although the source did not offer it")
	}
}

// Supersede is limited to blocked runs: a published run in a retained phase
// (the #263 duty) still suppresses its identity and is never deleted.
func TestRunnerKeepsPublishedAwaitingReviewRunSuppressed(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 2
	retained := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: runName("dispatch", "opaque/generation-7"), Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "opaque/generation-7", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
		Status: courierv1alpha1.CoderRunStatus{
			Phase: courierv1alpha1.PhaseAwaitingReview,
			Conditions: []metav1.Condition{{
				Type:               courierv1alpha1.ConditionLifecycleReported,
				Status:             metav1.ConditionTrue,
				Reason:             "Published",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque/generation-7", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}
	kubeClient := newTestClient(t, lane, retained)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("runs = %#v, want the published awaiting-review run kept, not superseded", runs.Items)
	}
	if runs.Items[0].Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("run phase = %q, want the original awaiting-review run", runs.Items[0].Status.Phase)
	}
}

func TestRunnerSkipsDiscoveryWhenPendingRunReservesLaneCapacity(t *testing.T) {
	item := WorkItem{ID: "queue-item/generation-2", Mode: "fix-pr", Repo: "acme/widgets", Ref: 43}
	localLane := testLane()
	localLane.Spec.Concurrency = 1
	minimaxLane := testLane()
	minimaxLane.Name = "minimax"
	minimaxLane.Spec.Concurrency = 1
	reserved := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "local-pending", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: "reserved-item", Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhasePending},
	}
	kubeClient := newTestClient(t, localLane, minimaxLane, reserved)
	localAdapter := &testAdapter{items: []WorkItem{item}}
	cloudAdapter := &testAdapter{items: []WorkItem{item}}
	local := NewRunner(kubeClient, localAdapter, RunnerConfig{Source: "dispatch", SourceAgent: "courier-local", LaneProfile: "local", Namespace: "courier"})
	cloud := NewRunner(kubeClient, cloudAdapter, RunnerConfig{Source: "dispatch", SourceAgent: "courier-cloud", LaneProfile: "minimax", Namespace: "courier"})

	if err := local.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cloud.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if localAdapter.discoverCalls != 0 || cloudAdapter.discoverCalls != 1 {
		t.Fatalf("Discover() calls local/cloud = %d/%d, want 0/1", localAdapter.discoverCalls, cloudAdapter.discoverCalls)
	}

	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("found %d runs, want pending local reservation plus one cloud run", len(runs.Items))
	}
	for _, run := range runs.Items {
		if run.Spec.WorkItemID == item.ID && (run.Spec.Lane != "minimax" || run.Spec.SourceAgent != "courier-cloud") {
			t.Fatalf("new run binding = lane %q, agent %q; want minimax/courier-cloud", run.Spec.Lane, run.Spec.SourceAgent)
		}
	}
}

func TestRunnerCapacityGateUsesAdmissionReservations(t *testing.T) {
	for _, phase := range []courierv1alpha1.Phase{
		"",
		courierv1alpha1.PhasePending,
		courierv1alpha1.PhaseClaimed,
		courierv1alpha1.PhaseRunning,
	} {
		t.Run(string(phase), func(t *testing.T) {
			lane := testLane()
			lane.Spec.Concurrency = 1
			occupied := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "reserved", Namespace: "courier"},
				Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
				Status:     courierv1alpha1.CoderRunStatus{Phase: phase},
			}
			adapter := &testAdapter{items: []WorkItem{{ID: "new", Mode: "fix-pr", Repo: "acme/widgets", Ref: 1}}}
			runner := NewRunner(newTestClient(t, lane, occupied), adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if adapter.discoverCalls != 0 {
				t.Fatalf("Discover() calls = %d, want 0 while phase %q reserves capacity", adapter.discoverCalls, phase)
			}
		})
	}
}

func TestRunnerCapacityGateIgnoresNonAdmissionPhasesAndResumes(t *testing.T) {
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
		courierv1alpha1.PhaseDone,
		courierv1alpha1.PhaseFailed,
		courierv1alpha1.PhaseNeedsHuman,
	} {
		t.Run(string(phase), func(t *testing.T) {
			lane := testLane()
			lane.Spec.Concurrency = 1
			oldRun := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "courier"},
				Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
				Status:     courierv1alpha1.CoderRunStatus{Phase: phase},
			}
			adapter := &testAdapter{items: []WorkItem{{ID: "new", Mode: "fix-pr", Repo: "acme/widgets", Ref: 1}}}
			kubeClient := newTestClient(t, lane, oldRun)
			runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if adapter.discoverCalls != 1 {
				t.Fatalf("Discover() calls = %d, want 1 for non-capacity phase %q", adapter.discoverCalls, phase)
			}
		})
	}
}

func TestRunnerCapacityGateResumesAfterCapacityFrees(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 1
	occupied := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	adapter := &testAdapter{items: []WorkItem{{ID: "new", Mode: "fix-pr", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, lane, occupied)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 {
		t.Fatalf("Discover() calls while full = %d, want 0", adapter.discoverCalls)
	}
	if err := kubeClient.Delete(context.Background(), occupied); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 1 {
		t.Fatalf("Discover() calls after capacity freed = %d, want 1", adapter.discoverCalls)
	}
}

func TestRunnerCapacityGateAllowsConfiguredConcurrency(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 2
	occupied := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	pending := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhasePending},
	}
	adapter := &testAdapter{items: []WorkItem{{ID: "new", Mode: "fix-pr", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, lane, occupied, pending)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 {
		t.Fatalf("Discover() calls = %d, want 0 with two reservations at concurrency 2", adapter.discoverCalls)
	}
}

func TestRunnersSharingLaneProfileShareCapacityGate(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 1
	occupied := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	adapterA := &testAdapter{items: []WorkItem{{ID: "same", Mode: "fix-pr", Repo: "acme/widgets", Ref: 1}}}
	adapterB := &testAdapter{items: []WorkItem{{ID: "same", Mode: "fix-pr", Repo: "acme/widgets", Ref: 1}}}
	kubeClient := newTestClient(t, lane, occupied)
	runnerA := NewRunner(kubeClient, adapterA, RunnerConfig{Source: "dispatch", SourceAgent: "courier-local", LaneProfile: "local", Namespace: "courier"})
	runnerB := NewRunner(kubeClient, adapterB, RunnerConfig{Source: "dispatch", SourceAgent: "courier-cloud", LaneProfile: "local", Namespace: "courier"})
	if err := runnerA.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runnerB.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapterA.discoverCalls != 0 || adapterB.discoverCalls != 0 {
		t.Fatalf("Discover() calls for shared profile = %d/%d, want 0/0", adapterA.discoverCalls, adapterB.discoverCalls)
	}
}

func TestRunnerSkipsDiscoveryWhenLaneConcurrencyIsZero(t *testing.T) {
	lane := testLane()
	lane.Spec.Concurrency = 0
	occupied := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "courier"},
		Spec:       courierv1alpha1.CoderRunSpec{Lane: "local"},
		Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	adapter := &testAdapter{items: []WorkItem{{ID: "new", Mode: "fix-pr", Repo: "acme/widgets", Ref: 1}}}
	runner := NewRunner(newTestClient(t, lane, occupied), adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.discoverCalls != 0 {
		t.Fatalf("Discover() calls = %d, want 0 with default concurrency one", adapter.discoverCalls)
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
	lane := testLane()
	lane.Spec.Concurrency = 2
	kubeClient := newTestClient(t, lane)
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
	lane := testLane()
	lane.Spec.Concurrency = 2
	kubeClient := newTestClient(t, lane)
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

func TestRunnerPollCooldownUsesSharedCapacityIdentity(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	exhausted := testLane()
	exhausted.Name = "lane-a"
	exhausted.Spec.CapacityGroup = "pool-a"
	exhausted.Status.CapacityCooldown = &courierv1alpha1.CapacityCooldown{
		CapacityID: "pool-a", NextEligibleAt: metav1.NewTime(now.Add(time.Hour)),
	}
	sameGroup := testLane()
	sameGroup.Name = "lane-b"
	sameGroup.Spec.CapacityGroup = "pool-a"
	unrelated := testLane()
	unrelated.Name = "lane-c"
	unrelated.Spec.CapacityGroup = "pool-c"

	for _, tc := range []struct {
		name string
		lane *courierv1alpha1.LaneProfile
		want int
	}{{"shared group is paused", sameGroup, 0}, {"unrelated group discovers", unrelated, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
			kubeClient := newTestClient(t, exhausted, sameGroup, unrelated)
			runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: tc.lane.Name, Namespace: "courier"})
			runner.Now = func() time.Time { return now }
			if err := runner.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if adapter.discoverCalls != tc.want {
				t.Fatalf("discovery ran %d times, want %d", adapter.discoverCalls, tc.want)
			}
		})
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
	lane := testLane()
	lane.Spec.Concurrency = 2
	if err := kubeClient.Create(context.Background(), lane); err != nil {
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
	if err := kubeClient.Delete(context.Background(), lane); err != nil {
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

type excludingTestAdapter struct {
	testAdapter
	excludingCalls int
	exclusions     []WorkItem
}

func (a *excludingTestAdapter) DiscoverExcluding(_ context.Context, exclusions []WorkItem) ([]WorkItem, error) {
	a.excludingCalls++
	a.exclusions = append([]WorkItem(nil), exclusions...)
	return a.items, a.err
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

// resolvingDispatchAdapter mirrors Dispatch's terminal path: a resolved item
// is permanently excluded from later discovery, because done issues are not
// re-offered in the next-task queue.
type resolvingDispatchAdapter struct {
	testAdapter
	resolutions map[string]int
}

func (a *resolvingDispatchAdapter) Discover(context.Context) ([]WorkItem, error) {
	a.discoverCalls++
	items := make([]WorkItem, 0, len(a.items))
	for _, item := range a.items {
		if a.resolutions[item.ID] == 0 {
			items = append(items, item)
		}
	}
	return items, a.err
}

func (a *resolvingDispatchAdapter) Resolve(_ context.Context, item WorkItem) error {
	if a.resolutions == nil {
		a.resolutions = make(map[string]int)
	}
	a.resolutions[item.ID]++
	return nil
}

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
	lane := testLane()
	lane.Spec.Concurrency = 2
	kubeClient := newTestClient(t, lane, existing)
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

// The runner dedupes only against existing runs, so a reaped run must not be
// re-created once the source has resolved the work behind it.
func TestRunnerDoesNotRecreateResolvedWorkAfterReap(t *testing.T) {
	item := WorkItem{ID: "issue-101", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}
	adapter := &resolvingDispatchAdapter{testAdapter: testAdapter{items: []WorkItem{item}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}

	// The controller's terminal path resolves the source work, then reaps the run.
	if err := adapter.Resolve(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Delete(context.Background(), &runs.Items[0]); err != nil {
		t.Fatal(err)
	}

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("reaped resolved work re-created %d runs, want 0", len(runs.Items))
	}
	if adapter.resolutions[item.ID] != 1 {
		t.Fatalf("Resolve calls for %q = %d, want exactly 1", item.ID, adapter.resolutions[item.ID])
	}
}

// A source that still offers the identity after the run is reaped means a
// fresh intended dispatch, so the run is re-created.
func TestRunnerRecreatesStillOfferedWorkAfterReap(t *testing.T) {
	item := WorkItem{ID: "issue-102", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}
	adapter := &testAdapter{items: []WorkItem{item}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}

	// The run is reaped but the source never resolves the item, so it stays offered.
	if err := kubeClient.Delete(context.Background(), &runs.Items[0]); err != nil {
		t.Fatal(err)
	}

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("found %d runs, want 1 re-created", len(runs.Items))
	}
	if runs.Items[0].Spec.WorkItemID != item.ID {
		t.Fatalf("re-created run workItemID = %q, want %q", runs.Items[0].Spec.WorkItemID, item.ID)
	}
}

type ackingAdapter struct {
	testAdapter
	// mu guards the ack record and the failure budget: the runner drains
	// pending acks concurrently, so a single Poll can append from several
	// goroutines at once.
	mu        sync.Mutex
	acks      []WorkItem
	ackError  error
	failFirst int
}

func (a *ackingAdapter) AckHandout(_ context.Context, item WorkItem) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failFirst > 0 {
		a.failFirst--
		a.acks = append(a.acks, item)
		return errors.New("ack transport blip")
	}
	a.acks = append(a.acks, item)
	return a.ackError
}

// ackCount reports how many ack attempts the adapter has recorded.
func (a *ackingAdapter) ackCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.acks)
}

// ackedIDs returns the acked work item IDs. Ordering between distinct items is
// not part of the contract: the runner drains pending acks concurrently, so
// assertions must match by work item ID rather than by position.
func (a *ackingAdapter) ackedIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]string, 0, len(a.acks))
	for _, item := range a.acks {
		ids = append(ids, item.ID)
	}
	return ids
}

func TestRunnerPollAcksHandOutWhenRunMaterializes(t *testing.T) {
	item := WorkItem{ID: "opaque-a", Context: `{"opaque":"receipt"}`, Mode: "fix-pr", Repo: "acme/widgets", Ref: 2, Lane: "source-lane"}
	adapter := &ackingAdapter{testAdapter: testAdapter{items: []WorkItem{item}}}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ids := adapter.ackedIDs(); len(ids) != 1 || ids[0] != item.ID {
		t.Fatalf("acks = %v, want one ack for %q", ids, item.ID)
	}
}

func TestRunnerPollAckFailureDoesNotBlockMaterialization(t *testing.T) {
	items := []WorkItem{
		{ID: "opaque-a", Mode: "fix-pr", Repo: "acme/widgets", Ref: 2, Lane: "source-lane"},
		{ID: "opaque-b", Mode: "fix-pr", Repo: "acme/widgets", Ref: 3, Lane: "source-lane"},
	}
	adapter := &ackingAdapter{testAdapter: testAdapter{items: items}, ackError: errors.New("ack refused")}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	runner.ackRetryDelay = 0

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := adapter.ackCount(); got != len(items)*maxHandoutAckAttempts {
		t.Fatalf("acks = %d, want %d bounded retries per materialized item", got, len(items)*maxHandoutAckAttempts)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("created %d runs, want 2 despite ack failure", len(runs.Items))
	}
}

func TestRunnerPollRetriesFailedHandOutAckUntilSuccess(t *testing.T) {
	item := WorkItem{ID: "opaque-a", Mode: "fix-pr", Repo: "acme/widgets", Ref: 2, Lane: "source-lane"}
	// The first two ack attempts fail, the third succeeds: a transient
	// failure at materialization must not strand the hand-out (#456).
	adapter := &ackingAdapter{testAdapter: testAdapter{items: []WorkItem{item}}, failFirst: 2}
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	runner.ackRetryDelay = 0

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := adapter.ackCount(); got != maxHandoutAckAttempts {
		t.Fatalf("acks = %d, want %d attempts", got, maxHandoutAckAttempts)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}
}

func TestRunnerPollReAcksHandOutWhenRunAlreadyExists(t *testing.T) {
	item := WorkItem{ID: "opaque-a", Mode: "fix-pr", Repo: "acme/widgets", Ref: 2, Lane: "source-lane"}
	adapter := &ackingAdapter{testAdapter: testAdapter{items: []WorkItem{item}}}
	lane := testLane()
	lane.Spec.Concurrency = 2
	kubeClient := newTestClient(t, lane)
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	runner.ackRetryDelay = 0

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	firstAcks := adapter.ackCount()
	// The source keeps offering the item whose run already exists
	// (exclusions are hints): the ack must be re-attempted, because an
	// earlier ack failure leaves the hand-out outstanding (#456).
	adapter.failFirst = 1
	adapter.ackError = nil
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reAcks := adapter.ackCount() - firstAcks; reAcks != 2 {
		t.Fatalf("re-acks = %d, want a failed attempt plus a successful retry on the skip path", reAcks)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("found %d runs, want 1 (skip path must not duplicate)", len(runs.Items))
	}
}

func TestRunnerPollSkipsAckForAdaptersWithoutCapability(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{
		{ID: "opaque-a", Mode: "fix-pr", Repo: "acme/widgets", Ref: 2, Lane: "source-lane"},
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
	if len(runs.Items) != 1 {
		t.Fatalf("created %d runs, want 1", len(runs.Items))
	}
}

// ackGateAdapter is an acking adapter whose AckHandout blocks until released
// or until its context expires, and which records how many acks were ever in
// flight at once.
type ackGateAdapter struct {
	testAdapter
	release     chan struct{}
	once        sync.Once
	mu          sync.Mutex
	inFlight    int
	maxInFlight int
	settled     map[string]bool
	calls       int
}

func (a *ackGateAdapter) AckHandout(ctx context.Context, item WorkItem) error {
	a.mu.Lock()
	a.calls++
	a.inFlight++
	if a.inFlight > a.maxInFlight {
		a.maxInFlight = a.inFlight
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.inFlight--
		a.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return errors.New("ack deadline expired")
	case <-a.release:
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.settled == nil {
		a.settled = map[string]bool{}
	}
	a.settled[item.ID] = true
	return nil
}

// releaseGate unblocks the adapter's acks exactly once. Tests register it with
// t.Cleanup so a failing assertion can never leave a drain goroutine blocked
// and holding the runner's poll mutex for the rest of the run.
func (a *ackGateAdapter) releaseGate() {
	a.once.Do(func() { close(a.release) })
}

func (a *ackGateAdapter) maxConcurrent() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.maxInFlight
}

func (a *ackGateAdapter) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func (a *ackGateAdapter) acked() map[string]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	snapshot := make(map[string]bool, len(a.settled))
	for id := range a.settled {
		snapshot[id] = true
	}
	return snapshot
}

func ackGateItems(count int) []WorkItem {
	items := make([]WorkItem, 0, count)
	for i := 1; i <= count; i++ {
		items = append(items, WorkItem{
			ID:   fmt.Sprintf("opaque-%d", i),
			Mode: "fix-pr", Repo: "acme/widgets", Ref: i, Lane: "source-lane",
		})
	}
	return items
}

func countRuns(t *testing.T, kubeClient client.Client) int {
	t.Helper()
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	return len(runs.Items)
}

// TestRunnerPollMaterializesEveryItemBeforeAckReturns pins the two-phase
// discovery contract: a Dispatch outage must not delay the materialization of
// another item's CoderRun. Both budgets are set past the end of the test, so a
// blocked ack can never return on its own — under inline acking the first item
// would still be waiting when the last item is due to exist, and the run count
// would never reach the offered count.
func TestRunnerPollMaterializesEveryItemBeforeAckReturns(t *testing.T) {
	items := ackGateItems(3)
	adapter := &ackGateAdapter{testAdapter: testAdapter{items: items}, release: make(chan struct{})}
	t.Cleanup(adapter.releaseGate)
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	runner.ackAttemptTimeout = time.Minute
	runner.ackDrainTimeout = time.Minute

	pollDone := make(chan error, 1)
	go func() { pollDone <- runner.Poll(context.Background()) }()

	// The gate stays closed: every run must exist while no ack has returned.
	deadline := time.After(20 * time.Second)
	for {
		if got := countRuns(t, kubeClient); got == len(items) {
			break
		}
		select {
		case err := <-pollDone:
			t.Fatalf("Poll returned (%v) before every item was materialized while acks were blocked", err)
		case <-deadline:
			t.Fatalf("materialized %d of %d runs while acks were blocked", countRuns(t, kubeClient), len(items))
		case <-time.After(5 * time.Millisecond):
		}
	}
	adapter.releaseGate()
	if err := <-pollDone; err != nil {
		t.Fatalf("Poll: %v", err)
	}
}

// TestRunnerPollAbandonsAcksWhenDrainBudgetExpires pins the drain budget as the
// only thing that can end a stuck ack. The per-attempt deadline is set past the
// end of the test, so no attempt can complete: if the drain budget did not bound
// the pass, Poll would hang instead of abandoning one attempt per item.
func TestRunnerPollAbandonsAcksWhenDrainBudgetExpires(t *testing.T) {
	items := ackGateItems(2)
	adapter := &ackGateAdapter{testAdapter: testAdapter{items: items}, release: make(chan struct{})}
	t.Cleanup(adapter.releaseGate)
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	runner.ackAttemptTimeout = time.Minute
	runner.ackDrainTimeout = 300 * time.Millisecond

	start := time.Now()
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("Poll took %s, want the 300ms drain budget to end it", elapsed)
	}
	if got := countRuns(t, kubeClient); got != len(items) {
		t.Fatalf("created %d runs, want %d despite a hanging acker", got, len(items))
	}
	// Exactly one attempt per item: the budget expires inside the first attempt,
	// and the between-attempt guard stops the retries. A pass that kept retrying
	// would show more calls, and a pass that dropped the queue would show fewer.
	if calls := adapter.callCount(); calls != len(items) {
		t.Fatalf("ack calls = %d, want exactly %d — one abandoned attempt per offered item", calls, len(items))
	}
	if settled := adapter.acked(); len(settled) != 0 {
		t.Fatalf("acked %v, want none: the gate never opened", settled)
	}
}

// TestRunnerPollBoundsEachAckAttempt pins the per-attempt deadline
// independently of the drain budget. One item with a drain budget long enough
// to hold every retry still gets all three attempts, each killed by its own
// deadline: without that deadline a single stalled attempt would swallow the
// whole budget and the item would be abandoned after one attempt.
func TestRunnerPollBoundsEachAckAttempt(t *testing.T) {
	item := ackGateItems(1)[0]
	adapter := &ackGateAdapter{testAdapter: testAdapter{items: []WorkItem{item}}, release: make(chan struct{})}
	t.Cleanup(adapter.releaseGate)
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	runner.ackAttemptTimeout = 100 * time.Millisecond
	runner.ackDrainTimeout = 10 * time.Second
	runner.ackRetryDelay = 0

	start := time.Now()
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	elapsed := time.Since(start)
	if calls := adapter.callCount(); calls != 3 {
		t.Fatalf("ack calls = %d, want the 3 attempts the budget could hold", calls)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Poll took %s, want the 100ms attempt deadlines rather than the 10s drain budget to end them", elapsed)
	}
	if got := countRuns(t, kubeClient); got != 1 {
		t.Fatalf("created %d runs, want 1", got)
	}
}

// TestRunnerPollBoundsAckConcurrency pins the fan-out cap against literals, not
// against the constant under test: acks must overlap (so a cap of 1, i.e. no
// bounded drain at all, fails) and must never exceed four in flight (so a cap
// that does not exist fails).
func TestRunnerPollBoundsAckConcurrency(t *testing.T) {
	const offered = 9
	items := ackGateItems(offered)
	adapter := &ackGateAdapter{testAdapter: testAdapter{items: items}, release: make(chan struct{})}
	t.Cleanup(adapter.releaseGate)
	kubeClient := newTestClient(t, testLane())
	runner := NewRunner(kubeClient, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
	runner.ackAttemptTimeout = time.Minute
	runner.ackDrainTimeout = time.Minute

	pollDone := make(chan error, 1)
	go func() { pollDone <- runner.Poll(context.Background()) }()

	// Two acks in flight at once is the lower bound the cap must still allow.
	deadline := time.After(20 * time.Second)
	for adapter.maxConcurrent() < 2 {
		select {
		case err := <-pollDone:
			t.Fatalf("Poll returned (%v) before acks overlapped", err)
		case <-deadline:
			t.Fatalf("observed max %d concurrent acks, want at least 2", adapter.maxConcurrent())
		case <-time.After(5 * time.Millisecond):
		}
	}
	adapter.releaseGate()
	if err := <-pollDone; err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := adapter.maxConcurrent(); got > 4 {
		t.Fatalf("observed %d concurrent acks, want at most 4", got)
	}
	acked := adapter.acked()
	for _, item := range items {
		if !acked[item.ID] {
			t.Fatalf("work item %q was never acknowledged", item.ID)
		}
	}
}

// TestRunnerAckBudgetProductionDefaults pins the shipped budget numbers as
// literals: they are a tuning decision, and a silent change to any of them
// would quietly rewrite the worst-case ack latency of every poll.
func TestRunnerAckBudgetProductionDefaults(t *testing.T) {
	if handoutAckAttemptTimeout != 3*time.Second {
		t.Fatalf("handoutAckAttemptTimeout = %s, want 3s", handoutAckAttemptTimeout)
	}
	if handoutAckDrainTimeout != 15*time.Second {
		t.Fatalf("handoutAckDrainTimeout = %s, want 15s", handoutAckDrainTimeout)
	}
	if handoutAckConcurrency != 4 {
		t.Fatalf("handoutAckConcurrency = %d, want 4", handoutAckConcurrency)
	}
	// A single item's worst case (three 3s attempts plus two 100ms backoffs)
	// must still fit inside the 15s budget, so an item draining on its own
	// keeps all three attempts.
	if worstCase := 3*3*time.Second + 2*100*time.Millisecond; worstCase >= 15*time.Second {
		t.Fatalf("worst-case item ack cost %s does not fit the 15s drain budget", worstCase)
	}
}

// TestRunnerZeroValueAckBudgetsStillBound pins the injectable-budget
// fallback: tests and callers build Runner values field by field, so a zero
// budget must resolve to the production default rather than mean "no timeout".
func TestRunnerZeroValueAckBudgetsStillBound(t *testing.T) {
	item := WorkItem{ID: "opaque-a", Mode: "fix-pr", Repo: "acme/widgets", Ref: 2, Lane: "source-lane"}
	adapter := &ackingAdapter{testAdapter: testAdapter{items: []WorkItem{item}}, ackError: errors.New("ack refused")}
	kubeClient := newTestClient(t, testLane())
	runner := &Runner{Client: kubeClient, Adapter: adapter, Config: RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"}}

	if got := runner.ackAttemptBudget(); got != handoutAckAttemptTimeout {
		t.Fatalf("attempt budget = %s, want the production default %s", got, handoutAckAttemptTimeout)
	}
	if got := runner.ackDrainBudget(); got != handoutAckDrainTimeout {
		t.Fatalf("drain budget = %s, want the production default %s", got, handoutAckDrainTimeout)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := adapter.ackCount(); got != maxHandoutAckAttempts {
		t.Fatalf("acks = %d, want %d attempts", got, maxHandoutAckAttempts)
	}
}
