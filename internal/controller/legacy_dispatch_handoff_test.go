package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/dispatch"
)

func TestLegacyDispatchCapacityHandoffAdmitsSuccessor(t *testing.T) {
	for _, tc := range []struct {
		name             string
		mode             courierv1alpha1.Mode
		id               string
		receipt          string
		wantIssueHandoff bool
	}{
		{
			name:             "issue",
			mode:             courierv1alpha1.ModeResolveIssue,
			id:               dispatch.EncodeWorkID(dispatch.Task{Type: "implement", Issue: &dispatch.Issue{ID: "issue-327", Repo: "acme/widgets", Number: 327}}),
			receipt:          `{"claimToken":"claim-327"}`,
			wantIssueHandoff: true,
		},
		{
			name:    "existing-pr-fix",
			mode:    courierv1alpha1.ModeFixPR,
			id:      dispatch.EncodeWorkID(dispatch.Task{Type: "followup-pr", PullRequest: &dispatch.PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &dispatch.PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: "handout-3"}}),
			receipt: `{"prFixId":"fix-42","generation":3,"handoutToken":"handout-3"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var mu sync.Mutex
			var discoveries, settlements int
			var serverErr string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Header.Get("Authorization") != "Bearer dispatch-token" {
					t.Errorf("authorization = %q", r.Header.Get("Authorization"))
				}
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/agents/worker/next-task":
					discoveries++
					if tc.mode == courierv1alpha1.ModeResolveIssue {
						_ = json.NewEncoder(w).Encode(map[string]any{"type": "implement", "shouldRun": true, "issue": map[string]any{"id": "issue-327", "repoFullName": "acme/widgets", "number": 327}})
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"type": "followup-pr", "shouldRun": true, "pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42}, "prFixItem": map[string]any{"id": "fix-42", "generation": 4, "handoutToken": "handout-4"}})
					}
				case r.Method == http.MethodPost && r.URL.Path == "/api/issues/claim":
					_ = json.NewEncoder(w).Encode(map[string]any{"claimToken": "successor-claim"})
				case r.Method == http.MethodGet && r.URL.Path == "/api/issues/state":
					_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-327", "repoFullName": "acme/widgets", "number": 327})
				case r.Method == http.MethodGet && r.URL.Path == "/api/audit":
					_ = json.NewEncoder(w).Encode([]any{})
				case r.Method == http.MethodPost && r.URL.Path == "/api/issues/capacity-handoff":
					settlements++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode handoff: %v", err)
					}
					if body["claimToken"] != "claim-327" || body["capacity"] != "pool-a" || body["agentName"] != "worker" {
						t.Errorf("issue handoff body = %#v", body)
					}
					if settlements == 1 {
						serverErr = "temporary issue settlement failure"
						http.Error(w, serverErr, http.StatusServiceUnavailable)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
				case r.Method == http.MethodPost && r.URL.Path == "/api/agents/worker/tasks/report":
					settlements++
					var body struct {
						Outcome  string             `json:"outcome"`
						Capacity string             `json:"capacity"`
						Item     dispatch.PRFixItem `json:"prFixItem"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode PR handoff: %v", err)
					}
					if body.Outcome != "capacity-exhausted" || body.Capacity != "pool-a" || body.Item.ID != "fix-42" || body.Item.Generation != 3 || body.Item.HandoutToken != "handout-3" {
						t.Errorf("PR handoff body = %#v", body)
					}
					if settlements == 1 {
						serverErr = "temporary PR settlement failure"
						http.Error(w, serverErr, http.StatusServiceUnavailable)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"prFixResolution": map[string]any{"matched": true, "itemId": "fix-42", "action": "deferred"}})
				default:
					t.Errorf("unexpected Dispatch request %s %s", r.Method, r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			httpClient, err := dispatch.NewClient(server.URL, "worker", "dispatch-token", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			httpClient = httpClient.WithPullRequestStateChecker(dispatch.PullRequestStateCheckerFunc(func(context.Context, string, int) (dispatch.PullRequestState, error) {
				return dispatch.PullRequestState{State: "open"}, nil
			}))
			adapter := dispatch.NewAdapter(httpClient)
			lane := admissionLane("pool-a", 2)
			predecessor := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "exhausted-" + tc.name, Namespace: admissionNamespace, UID: types.UID("uid-" + tc.name)},
				Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", WorkItemID: tc.id, Mode: tc.mode, Repo: "acme/widgets", Ref: 42, Lane: lane.Name},
				Status: courierv1alpha1.CoderRunStatus{
					Phase:              courierv1alpha1.PhaseBlockedExternal,
					SourceReceipt:      tc.receipt,
					Branch:             "run-branch",
					WorkspaceSafety:    &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"},
					TerminalDiagnostic: &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion},
					Conditions: []metav1.Condition{
						{Type: courierv1alpha1.ConditionLifecycleReported, Status: metav1.ConditionTrue, Reason: "Published", LastTransitionTime: metav1.Now()},
						{Type: courierv1alpha1.ConditionCapacityRecovery, Status: metav1.ConditionFalse, Reason: capacityRecoveryRetryable, LastTransitionTime: metav1.Now()},
					},
				},
			}
			// Real terminal pod objects ensure the operator's topology check observes
			// no active writer before crossing the source ownership boundary.
			terminalPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "terminal-" + tc.name, Namespace: admissionNamespace, Labels: map[string]string{executor.LabelRun: executor.RunLabelValue(predecessor.Name)}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
			scheme := runtime.NewScheme()
			if err := courierv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courierv1alpha1.CoderRun{}, &courierv1alpha1.LaneProfile{}).WithObjects(lane, predecessor, terminalPod).Build()
			reconciler := &CoderRunReconciler{Client: kube, Sources: NewSourceRegistry(map[string]source.Adapter{"dispatch": adapter}), StatusWriter: fakeStatusWriter{client: kube}, Now: func() time.Time { return time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC) }}

			for attempt := 0; attempt < 2; attempt++ {
				if _, err := reconciler.Reconcile(ctx, admissionRequest(predecessor.Name)); err != nil {
					t.Fatalf("Reconcile attempt %d: %v", attempt+1, err)
				}
			}
			var settled courierv1alpha1.CoderRun
			if err := kube.Get(ctx, admissionKey(predecessor.Name), &settled); err != nil {
				t.Fatal(err)
			}
			recovery := apimeta.FindStatusCondition(settled.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
			if recovery == nil || recovery.Status != metav1.ConditionTrue || recovery.Reason != capacityRecoverySettled {
				t.Fatalf("recovery condition after retry = %#v", recovery)
			}
			var storedLane courierv1alpha1.LaneProfile
			if err := kube.Get(ctx, client.ObjectKeyFromObject(lane), &storedLane); err != nil {
				t.Fatal(err)
			}
			if !storedLane.CooldownActive(reconciler.clock()) {
				t.Fatal("capacity cooldown not retained")
			}

			// The cooldown must prevent an immediate discovery request; after it
			// expires, the real runner can discover and admit the successor.
			runner := source.NewRunner(kube, adapter, source.RunnerConfig{Source: "dispatch", LaneProfile: lane.Name, Namespace: admissionNamespace})
			now := reconciler.Now()
			runner.Now = func() time.Time { return now }
			if err := runner.Poll(ctx); err != nil {
				t.Fatalf("cooldown Poll: %v", err)
			}
			mu.Lock()
			if discoveries != 0 {
				mu.Unlock()
				t.Fatalf("cooldown allowed discovery (%d requests)", discoveries)
			}
			mu.Unlock()
			// After expiry, discovery resumes and the real runner admits a successor.
			now = now.Add(16 * time.Minute)
			if err := runner.Poll(ctx); err != nil {
				t.Fatalf("successor Poll: %v", err)
			}
			var runs courierv1alpha1.CoderRunList
			if err := kube.List(ctx, &runs, client.InNamespace(admissionNamespace)); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("runs = %d, want retained predecessor plus successor (discoveries=%d settlements=%d)", len(runs.Items), discoveries, settlements)
			}
			var successor *courierv1alpha1.CoderRun
			for i := range runs.Items {
				if runs.Items[i].Name != predecessor.Name {
					successor = &runs.Items[i]
				}
			}
			wantRef := 42
			if tc.mode == courierv1alpha1.ModeResolveIssue {
				wantRef = 327
			}
			if successor == nil || successor.Spec.Mode != tc.mode || successor.Spec.Repo != "acme/widgets" || successor.Spec.Ref != wantRef {
				t.Fatalf("successor did not preserve work identity: %#v", successor)
			}
			mu.Lock()
			defer mu.Unlock()
			if settlements != 2 || discoveries != 1 {
				t.Fatalf("Dispatch settlements=%d discoveries=%d", settlements, discoveries)
			}
			if tc.wantIssueHandoff && serverErr == "" {
				t.Fatal("issue transient settlement failure was not observed")
			}
		})
	}
}
