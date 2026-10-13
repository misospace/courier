package source

import (
	"context"
	"reflect"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBlockedRecoveryRequiresFreshSourceAttemptAndRetainsEvidence(t *testing.T) {
	for _, binding := range []string{"dispatch", "dispatch:nightly"} {
		t.Run(binding, func(t *testing.T) {
			ctx := context.Background()
			lane := testLane()
			lane.Spec.Concurrency = 4
			blocked := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: runName(binding, "generation-7"), Namespace: "courier", UID: types.UID("retained-attempt")},
				Spec:       courierv1alpha1.CoderRunSpec{Source: binding, WorkItemID: "generation-7", Mode: courierv1alpha1.ModeFixPR, Repo: "acme/widgets", Ref: 42, Lane: "local"},
				Status: courierv1alpha1.CoderRunStatus{
					Phase:                           courierv1alpha1.PhaseBlockedExternal,
					TerminalDiagnostic:              &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal, Reason: "dependency unavailable"},
					Conditions:                      []metav1.Condition{{Type: courierv1alpha1.ConditionLifecycleReported, Status: metav1.ConditionTrue, Reason: "Published", LastTransitionTime: metav1.Now()}},
					LifecycleReportActiveRetryToken: "retained-retry-intent",
				},
			}
			evidence := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "attempt-evidence", Namespace: "courier", OwnerReferences: []metav1.OwnerReference{{APIVersion: courierv1alpha1.GroupVersion.String(), Kind: "CoderRun", Name: blocked.Name, UID: blocked.UID}}}, Data: map[string][]byte{"history": []byte("original evidence")}}
			scheme := testScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(lane, blocked, evidence).Build()
			if err := kube.Get(ctx, client.ObjectKeyFromObject(blocked), blocked); err != nil {
				t.Fatal(err)
			}
			adapter := &testAdapter{items: []WorkItem{{ID: "generation-7", Mode: "fix-pr", Repo: "acme/widgets", Ref: 42}}}
			runner := NewRunner(kube, adapter, RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})
			// A published wait is not permission to retry the same attempt every poll.
			for i := 0; i < 4; i++ {
				if err := runner.Poll(ctx); err != nil {
					t.Fatal(err)
				}
			}
			var runs courierv1alpha1.CoderRunList
			if err := kube.List(ctx, &runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 1 {
				t.Fatalf("same-identity reoffers created %d runs", len(runs.Items))
			}
			adapter.items[0].ID = "generation-8"
			for i := 0; i < 3; i++ {
				if err := runner.Poll(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := kube.List(ctx, &runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("fresh source attempt created %d runs, want 2", len(runs.Items))
			}
			var retained courierv1alpha1.CoderRun
			if err := kube.Get(ctx, client.ObjectKeyFromObject(blocked), &retained); err != nil {
				t.Fatal(err)
			}
			if retained.UID != blocked.UID || !reflect.DeepEqual(retained.Status, blocked.Status) {
				t.Fatalf("retained history mutated: %#v", retained.Status)
			}
			var secret corev1.Secret
			if err := kube.Get(ctx, client.ObjectKeyFromObject(evidence), &secret); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(secret.OwnerReferences, evidence.OwnerReferences) || !reflect.DeepEqual(secret.Data, evidence.Data) {
				t.Fatal("owned evidence mutated")
			}
			for _, run := range runs.Items {
				if run.Name != blocked.Name && (run.Spec.WorkItemID != "generation-8" || run.Status.Phase != "") {
					t.Fatalf("replacement is not a fresh runnable source attempt: %#v", run)
				}
			}
		})
	}
}
