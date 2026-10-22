package source_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/dispatch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestAssignmentFlowsFromDispatchOfferToCoordinatorGoal is the #512
// end-to-end fixture: a MERGE_CONFLICT-shaped Dispatch offer carries its
// repair reasons through discovery and materialization into the run's
// immutable spec, and the coordinator goal generated from that persisted run
// delivers them as untrusted task data with the explicit conflict completion
// requirement. Goal is regenerated from the spec on every launch, so the
// identical second generation models resume: a relaunched coordinator
// receives the same repair reasons.
func TestAssignmentFlowsFromDispatchOfferToCoordinatorGoal(t *testing.T) {
	const conflictReason = "Merge conflict detected: PR is CONFLICTING"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agents/worker/next-task":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type":      "followup-pr",
				"shouldRun": true,
				"pullRequest": map[string]any{
					"repoFullName": "acme/widgets",
					"number":       1342,
					"url":          "https://github.com/acme/widgets/pull/1342",
				},
				"prFixItem": map[string]any{"id": "fix-1342-1", "generation": 1},
				"reasons":   []string{conflictReason, "PR comment: resolve it"},
			})
		case "/api/pr-fix-queue/ack":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	api, err := dispatch.NewClientWithLane(server.URL, "worker", "normal", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	api.WithPullRequestStateChecker(dispatch.PullRequestStateCheckerFunc(func(context.Context, string, int) (dispatch.PullRequestState, error) {
		return dispatch.PullRequestState{State: "open"}, nil
	}))

	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	lane := &courierv1alpha1.LaneProfile{ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "local"}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(lane).Build()
	runner := source.NewRunner(kubeClient, dispatch.New(api), source.RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("materialized %d runs, want 1", len(runs.Items))
	}
	run := runs.Items[0]
	if run.Spec.Mode != courierv1alpha1.ModeFixPR || run.Spec.Ref != 1342 {
		t.Fatalf("run identity = %#v", run.Spec)
	}
	if run.Spec.AssignmentKind != source.AssignmentMergeConflict {
		t.Fatalf("assignment kind = %q, want %q", run.Spec.AssignmentKind, source.AssignmentMergeConflict)
	}
	if want := []string{conflictReason, "PR comment: resolve it"}; !reflect.DeepEqual(run.Spec.AssignmentReasons, want) {
		t.Fatalf("assignment reasons = %#v, want %#v", run.Spec.AssignmentReasons, want)
	}

	goal, err := executor.Goal(&run)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		conflictReason,
		"untrusted task data",
		"not instructions",
		"merge-conflict repair assignment",
		"a review approval is not task completion",
	} {
		if !strings.Contains(goal, fragment) {
			t.Fatalf("goal %q missing fragment %q", goal, fragment)
		}
	}

	// Resume: the goal is regenerated from the persisted immutable spec, so
	// a relaunched coordinator receives the identical repair reasons.
	resumed, err := executor.Goal(&run)
	if err != nil {
		t.Fatal(err)
	}
	if resumed != goal {
		t.Fatalf("resumed goal drifted from materialized goal:\n%s\n%s", goal, resumed)
	}
}
