package source

import (
	"context"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A rebase work item materializes a rebase CoderRun: the rebase task type
// survives run creation as its own mode, never remapped onto fix-pr (#715).
func TestRunnerPollMaterializesRebaseRuns(t *testing.T) {
	adapter := &testAdapter{items: []WorkItem{
		{ID: "rebase-attempt", Context: `{"prFixId":"fix-1","generation":3,"handoutToken":"hand-9","fixType":"REBASE"}`, Mode: "rebase", Repo: "acme/widgets", Ref: 42, Lane: "rebase-lane"},
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
	run := runs.Items[0]
	if run.Spec.Mode != courierv1alpha1.ModeRebase {
		t.Fatalf("mode = %q, want rebase", run.Spec.Mode)
	}
	if run.Spec.Ref != 42 || run.Spec.Repo != "acme/widgets" {
		t.Fatalf("run identity = %#v", run.Spec)
	}
	if run.Spec.SourceContext == "" || run.Spec.AssignmentKind != "" {
		t.Fatalf("run context = %#v", run.Spec)
	}
	// A second poll is absorbed by the same run: one attempt per identity.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("re-offer duplicated the run: %d", len(runs.Items))
	}
}
