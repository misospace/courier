package cron

import (
	"context"
	"fmt"
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
