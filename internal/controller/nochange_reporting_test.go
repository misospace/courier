package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/dispatch"
)

// An unchanged, green head with outstanding queued review feedback must never
// reach Dispatch's already_addressed head-guard bypass. The observer exposes
// CI, not review resolution, so the actual wire outcome must remain a wait.
func TestNoChangeActualDispatchReportDoesNotAuthorizeFeedbackSettlement(t *testing.T) {
	for _, scenario := range []struct {
		name string
		exit int32
		want string
	}{
		{"unresolved review on unchanged green head", 3, "waiting-external"},
		{"pushed work keeps ordinary head guard", 0, "pr_updated"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var outcomes []string
			fixed := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				switch req.URL.Path {
				case "/api/agents/worker/tasks/report":
					outcome, _ := body["outcome"].(string)
					outcomes = append(outcomes, outcome)
					// Dispatch's dangerous bypass authorizes FIXED even on an unchanged
					// head when the worker asserts already_addressed.
					if outcome == "already_addressed" {
						fixed = true
					}
					token, _ := body["prFixItem"].(map[string]any)
					if token["generation"] != float64(7) {
						t.Errorf("lost generation fence: %#v", body)
					}
				case "/api/pr-fix-queue/mark":
					if body["status"] != "WAITING_EXTERNAL" || body["generation"] != float64(7) {
						t.Errorf("unsafe queue mark: %#v", body)
					}
				default:
					t.Errorf("unexpected endpoint %s", req.URL.Path)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			wire, err := dispatch.NewClientWithLane(server.URL, "worker", "normal", "token", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			adapter := dispatch.New(wire)
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			run.Spec.Mode = courierv1alpha1.ModeFixPR
			run.Spec.Source = "dispatch"
			run.Spec.WorkItemID = dispatch.EncodeWorkID(dispatch.Task{Type: "followup-pr", PullRequest: &dispatch.PullRequest{Repo: run.Spec.Repo, Number: run.Spec.Ref}, PRFixItem: &dispatch.PRFixItem{ID: "queued-review", Generation: 7}})
			run.Status.HeadSHA = "unchanged-head"
			kube := phaseClient(t, run, coordinatorPodWithOutcome(run, scenario.exit, "no_change_needed", "clean workspace"))
			r := &CoderRunReconciler{Client: kube, Sources: NewSourceRegistry(map[string]source.Adapter{"dispatch": adapter}), StatusWriter: fakeStatusWriter{client: kube}, Observer: fakeWorldObserver{observation: greenObservation("unchanged-head", "test")}}
			for i := 0; i < 3; i++ {
				if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
					t.Fatal(err)
				}
			}
			if len(outcomes) != 1 || outcomes[0] != scenario.want || fixed {
				t.Fatalf("outcomes=%v fixed=%v, want %s without already_addressed", outcomes, fixed, scenario.want)
			}
		})
	}
}
