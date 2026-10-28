package cron

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/misospace/courier/internal/source"
)

// TestRunnerMaterializesOccurrences proves the adapter through the real
// source runner: occurrences materialize as CoderRuns on the source-neutral
// path with the cron source identity, repeated polls deduplicate, and the
// runner's lane-capacity gate defers materialization without advancing the
// cursor — the due occurrence materializes once capacity returns, so the
// paused gap never loses the latest boundary.
func TestRunnerMaterializesOccurrences(t *testing.T) {
	current := utc(t, "2026-01-05T10:00:00Z")
	now := func() time.Time { return current }
	adapter := New([]Schedule{hourly("triage")}, nil, WithNow(now))

	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "frontier"},
	}
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(lane).Build()
	runner := source.NewRunner(kubeClient, adapter, source.RunnerConfig{
		Source:      "cron",
		SourceAgent: "frontier",
		LaneProfile: "frontier",
		Namespace:   "courier",
	})
	runner.Now = now

	countRuns := func() int {
		t.Helper()
		var runs courierv1alpha1.CoderRunList
		if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
			t.Fatal(err)
		}
		return len(runs.Items)
	}
	mustRun := func() courierv1alpha1.CoderRun {
		t.Helper()
		var runs courierv1alpha1.CoderRunList
		if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
			t.Fatal(err)
		}
		if len(runs.Items) != 1 {
			t.Fatalf("found %d runs, want 1", len(runs.Items))
		}
		return runs.Items[0]
	}

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("poll before boundary: %v", err)
	}
	if got := countRuns(); got != 0 {
		t.Fatalf("poll before boundary created %d runs, want 0", got)
	}

	current = utc(t, "2026-01-05T11:00:01Z")
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("poll at boundary: %v", err)
	}
	run := mustRun()
	boundary := utc(t, "2026-01-05T11:00:00Z")
	wantID := fmt.Sprintf("triage@%d", boundary.Unix())
	if run.Spec.WorkItemID != wantID {
		t.Fatalf("work item ID = %q, want %q", run.Spec.WorkItemID, wantID)
	}
	if run.Spec.Source != "cron" || run.Spec.SourceAgent != "frontier" || run.Spec.Lane != "frontier" {
		t.Fatalf("run identity = %#v", run.Spec)
	}
	if run.Spec.Mode != courierv1alpha1.ModeResolveIssue || run.Spec.Repo != "misospace/courier" || run.Spec.Ref != 15 {
		t.Fatalf("run spec = %#v", run.Spec)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("repeat poll: %v", err)
	}
	if got := countRuns(); got != 1 {
		t.Fatalf("repeat poll created duplicate runs: %d", got)
	}

	// Lane at capacity: discovery pauses before Discover is called, so the
	// 12:00 boundary is not offered and the cursor does not move. A blocker
	// run on the lane holds the capacity slot.
	blocker := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "courier-blocker"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     "cron",
			WorkItemID: "other@1",
			Repo:       "misospace/courier",
			Ref:        15,
			Lane:       "frontier",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	if err := kubeClient.Create(context.Background(), blocker); err != nil {
		t.Fatal(err)
	}
	current = utc(t, "2026-01-05T12:30:00Z")
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("poll at capacity: %v", err)
	}
	// blocker plus the already-materialized 11:00 occurrence.
	if got := countRuns(); got != 2 {
		t.Fatalf("poll at capacity left %d runs, want blocker + earlier occurrence only", got)
	}

	// Capacity returns once the earlier occurrence's run no longer reserves
	// discovery capacity (a terminal phase) and the blocker is gone: the
	// latest due boundary (12:00) materializes. In production the operator
	// advances the phase; the test writes it directly.
	if err := kubeClient.Delete(context.Background(), blocker); err != nil {
		t.Fatal(err)
	}
	// Fetch the run through a List (the same read the runner's gates use)
	// and clear its discovery-capacity reservation with a terminal phase.
	// In production the operator advances the phase; the test writes it
	// directly.
	var existing courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &existing, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	advanced := false
	for i := range existing.Items {
		if existing.Items[i].Name != run.Name {
			continue
		}
		existing.Items[i].Status.Phase = courierv1alpha1.PhaseDone
		if err := kubeClient.Update(context.Background(), &existing.Items[i]); err != nil {
			t.Fatal(err)
		}
		advanced = true
	}
	if !advanced {
		t.Fatalf("materialized run %q vanished from the list", run.Name)
	}
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("poll after capacity: %v", err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("poll after capacity left %d runs, want 2", len(runs.Items))
	}
	wantTwelve := utc(t, "2026-01-05T12:00:00Z")
	found := false
	for _, item := range runs.Items {
		if item.Spec.WorkItemID == fmt.Sprintf("triage@%d", wantTwelve.Unix()) {
			found = true
		}
	}
	if !found {
		t.Fatalf("deferred occurrence did not materialize: %v", runs.Items)
	}
}

// TestRunnerAdmitsDueOccurrencesWithinLaneCapacity is the overlap-contract
// regression: two cron jobs due on the same boundary with lane concurrency 1
// admit at most one occurrence per free lane slot per poll. The job the lane
// cannot admit is deferred without consuming its cursor, and once capacity
// returns its latest due occurrence materializes — under the same
// latest-only rule as any other gap, never replayed as catch-up and never
// lost.
func TestRunnerAdmitsDueOccurrencesWithinLaneCapacity(t *testing.T) {
	// The adapter is constructed before the first boundary so the 11:00
	// occurrences are due rather than pre-startup (which the cursor would
	// skip by design).
	current := utc(t, "2026-01-05T10:00:00Z")
	now := func() time.Time { return current }
	adapter := New([]Schedule{hourly("triage"), hourly("audit")}, nil, WithNow(now))
	current = utc(t, "2026-01-05T11:00:01Z")

	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "frontier"},
		Spec:       courierv1alpha1.LaneProfileSpec{Concurrency: 1},
	}
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(lane).Build()
	runner := source.NewRunner(kubeClient, adapter, source.RunnerConfig{
		Source:      "cron",
		SourceAgent: "frontier",
		LaneProfile: "frontier",
		Namespace:   "courier",
	})
	runner.Now = now

	listRuns := func() []courierv1alpha1.CoderRun {
		t.Helper()
		var runs courierv1alpha1.CoderRunList
		if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
			t.Fatal(err)
		}
		return runs.Items
	}
	settle := func(workItemID string) {
		t.Helper()
		var runs courierv1alpha1.CoderRunList
		if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
			t.Fatal(err)
		}
		settled := false
		for i := range runs.Items {
			if runs.Items[i].Spec.WorkItemID != workItemID {
				continue
			}
			// In production the operator advances the phase; the test writes
			// it directly. A terminal phase releases the lane's discovery
			// reservation.
			runs.Items[i].Status.Phase = courierv1alpha1.PhaseDone
			if err := kubeClient.Update(context.Background(), &runs.Items[i]); err != nil {
				t.Fatal(err)
			}
			settled = true
		}
		if !settled {
			t.Fatalf("run for %q vanished before it could settle", workItemID)
		}
	}
	workItemIDs := func(runs []courierv1alpha1.CoderRun) []string {
		out := make([]string, 0, len(runs))
		for _, run := range runs {
			out = append(out, run.Spec.WorkItemID)
		}
		return out
	}

	eleven := utc(t, "2026-01-05T11:00:00Z")
	// Both jobs are due at the same 11:00 boundary, but the concurrency-1
	// lane has room for exactly one new discovery reservation: the bounded
	// adapter admits the first job in config order and exactly one run
	// materializes.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("poll with two jobs due on a saturated-boundary lane: %v", err)
	}
	runs := listRuns()
	if len(runs) != 1 {
		t.Fatalf("poll materialized %d runs, want exactly the lane's one free slot: %v", len(runs), workItemIDs(runs))
	}
	if runs[0].Spec.WorkItemID != fmt.Sprintf("triage@%d", eleven.Unix()) {
		t.Fatalf("admitted work item = %q, want the first configured job's occurrence", runs[0].Spec.WorkItemID)
	}

	// A repeat poll while capacity is held stops at the runner's pause gate
	// before the adapter is called: still exactly one run, nothing
	// materialized behind it. The adapter-side deferral is exercised below,
	// once capacity returns and discovery runs again with a bound.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("repeat poll at capacity: %v", err)
	}
	if got := len(listRuns()); got != 1 {
		t.Fatalf("repeat poll left %d runs, want 1", got)
	}

	// Capacity returns (the run settles) and the next boundary passes: the
	// lane still admits only one occurrence per poll, so the triage job's
	// 12:00 boundary materializes and the audit job stays deferred.
	settle(runs[0].Spec.WorkItemID)
	current = utc(t, "2026-01-05T12:00:01Z")
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("poll after capacity returned: %v", err)
	}
	twelve := utc(t, "2026-01-05T12:00:00Z")
	runs = listRuns()
	if len(runs) != 2 {
		t.Fatalf("poll after capacity returned left %d runs, want 2: %v", len(runs), workItemIDs(runs))
	}
	if !slices.Contains(workItemIDs(runs), fmt.Sprintf("triage@%d", twelve.Unix())) {
		t.Fatalf("newly due triage occurrence did not materialize: %v", workItemIDs(runs))
	}
	for _, run := range runs {
		if run.Spec.WorkItemID == fmt.Sprintf("audit@%d", twelve.Unix()) {
			t.Fatalf("audit occurrence materialized beyond the lane's single slot: %v", workItemIDs(runs))
		}
	}

	// The triage run settles too; the audit job — deferred twice with its
	// cursor untouched — now offers its latest due boundary. The 11:00
	// occurrence it held while deferred is gone under the same latest-only
	// rule as any other gap; it is not replayed as catch-up and not lost
	// silently: the 12:00 occurrence is the one that materializes.
	settle(fmt.Sprintf("triage@%d", twelve.Unix()))
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatalf("poll admitting the deferred job: %v", err)
	}
	runs = listRuns()
	if len(runs) != 3 {
		t.Fatalf("final poll left %d runs, want 3: %v", len(runs), workItemIDs(runs))
	}
	if !slices.Contains(workItemIDs(runs), fmt.Sprintf("audit@%d", twelve.Unix())) {
		t.Fatalf("deferred job's latest due occurrence did not materialize: %v", workItemIDs(runs))
	}
	for _, run := range runs {
		if run.Spec.WorkItemID == fmt.Sprintf("audit@%d", eleven.Unix()) {
			t.Fatalf("deferred occurrence replayed as catch-up: %v", workItemIDs(runs))
		}
	}
}
