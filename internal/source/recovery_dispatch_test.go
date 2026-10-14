package source_test

import (
	"context"
	"sync"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/dispatch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// These acceptance tests intentionally cross the package boundary: identities
// are generated and interpreted by the production Dispatch adapter.
type dispatchRecoveryAdapter struct{ items []source.WorkItem }

func (a *dispatchRecoveryAdapter) Discover(context.Context) ([]source.WorkItem, error) {
	return append([]source.WorkItem(nil), a.items...), nil
}
func (*dispatchRecoveryAdapter) WorkIdentity(id string) string                  { return dispatch.WorkIdentity(id) }
func (*dispatchRecoveryAdapter) Claim(context.Context, source.WorkItem) error   { return nil }
func (*dispatchRecoveryAdapter) Release(context.Context, source.WorkItem) error { return nil }
func (*dispatchRecoveryAdapter) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}
func (*dispatchRecoveryAdapter) Resolve(context.Context, source.WorkItem) error { return nil }

func TestDispatchCapacityRecoveryCreatesOnlyFreshSuccessors(t *testing.T) {
	ctx := context.Background()
	issueID := dispatch.EncodeWorkID(dispatch.Task{Type: "implement", Issue: &dispatch.Issue{ID: "issue-17", Repo: "acme/widgets", Number: 17}})
	prID := func(token string) string {
		return dispatch.EncodeWorkID(dispatch.Task{Type: "followup-pr", PullRequest: &dispatch.PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &dispatch.PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: token}})
	}
	for _, tc := range []struct {
		name, initial, successor string
		mode                     courierv1alpha1.Mode
		ref                      int
	}{
		{"issue", issueID, issueID, courierv1alpha1.ModeResolveIssue, 17},
		{"prfix-handout-token", prID("old-token"), prID("new-handout-token"), courierv1alpha1.ModeFixPR, 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := settledDispatchRun(tc.initial, tc.name+"-old", tc.mode, tc.ref)
			kube := recoveryTestClient(t, old)
			adapter := &dispatchRecoveryAdapter{items: []source.WorkItem{{ID: tc.initial, Mode: string(tc.mode), Repo: "acme/widgets", Ref: tc.ref}}}
			runner := source.NewRunner(kube, adapter, source.RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
			poll := func() {
				t.Helper()
				if err := runner.Poll(ctx); err != nil {
					t.Fatal(err)
				}
			}
			adapter.items[0].ID = tc.successor
			poll()
			var runs courierv1alpha1.CoderRunList
			if err := kube.List(ctx, &runs, client.InNamespace("courier")); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("runs = %d, want retained predecessor and successor", len(runs.Items))
			}
			var successor *courierv1alpha1.CoderRun
			for i := range runs.Items {
				if runs.Items[i].Spec.WorkItemID == tc.successor && runs.Items[i].Name != old.Name {
					successor = &runs.Items[i]
				}
			}
			if successor == nil || successor.Name == old.Name {
				t.Fatalf("successor = %#v; predecessor name %q", successor, old.Name)
			}
			poll()
			if err := kube.List(ctx, &runs, client.InNamespace("courier")); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("repeated successor poll created overlap: %d runs", len(runs.Items))
			}
		})
	}
}

func TestDispatchCapacityRecoveryConcurrentRunnersCreateOneSuccessor(t *testing.T) {
	ctx := context.Background()
	oldID := dispatch.EncodeWorkID(dispatch.Task{Type: "implement", Issue: &dispatch.Issue{ID: "issue-17", Repo: "acme/widgets", Number: 17}})
	newID := oldID
	old := settledDispatchRun(oldID, "predecessor", courierv1alpha1.ModeResolveIssue, 17)
	kube := recoveryTestClient(t, old)
	adapter := &dispatchRecoveryAdapter{items: []source.WorkItem{{ID: newID, Mode: "resolve-issue", Repo: "acme/widgets", Ref: 17}}}
	config := source.RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"}
	runners := []*source.Runner{source.NewRunner(kube, adapter, config), source.NewRunner(kube, adapter, config)}
	var wg sync.WaitGroup
	errs := make(chan error, len(runners))
	for _, runner := range runners {
		wg.Add(1)
		go func(r *source.Runner) { defer wg.Done(); errs <- r.Poll(ctx) }(runner)
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
}

func recoveryTestLane() *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "courier"}, Spec: courierv1alpha1.LaneProfileSpec{Concurrency: 4}}
}

func recoveryTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	all := append([]client.Object{recoveryTestLane()}, objects...)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(all...).Build()
}

func settledDispatchRun(id string, uid string, mode courierv1alpha1.Mode, ref int) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "retained-" + uid, Namespace: "courier", UID: types.UID(uid)},
		Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: id, Mode: mode, Repo: "acme/widgets", Ref: ref, Lane: "local"},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseBlockedExternal,
			TerminalDiagnostic: &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion},
			Conditions:         []metav1.Condition{{Type: courierv1alpha1.ConditionCapacityRecovery, Status: metav1.ConditionTrue, Reason: "Settled"}, {Type: courierv1alpha1.ConditionLifecycleReported, Status: metav1.ConditionTrue, Reason: "Published"}}},
	}
}
