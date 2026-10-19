package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestCrossCapacityHandoffCoolsExhaustedProfileOnly pins the deployed user
// invariant behind #327: a MiniMax-like profile and a Qwen-like profile draw
// the same Dispatch queue under different capacity identities. When the
// cloud profile ends its attempt on a recoverable capacity exhaustion, the
// fenced settlement cools only the cloud identity — the local profile
// discovers and admits the successor for the same issue or the same existing
// PR (number and head branch carried by the queue identity) while the cloud
// cooldown is still open, and repeated polls can neither duplicate the run
// nor let the cooled capacity take ownership again.
func TestCrossCapacityHandoffCoolsExhaustedProfileOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    courierv1alpha1.Mode
		id      string
		receipt string
		ref     int
	}{
		{
			name:    "issue",
			mode:    courierv1alpha1.ModeResolveIssue,
			id:      dispatch.EncodeWorkID(dispatch.Task{Type: "implement", Issue: &dispatch.Issue{ID: "issue-327", Repo: "acme/widgets", Number: 327}}),
			receipt: `{"claimToken":"claim-327"}`,
			ref:     327,
		},
		{
			name:    "existing-pr-fix",
			mode:    courierv1alpha1.ModeFixPR,
			id:      dispatch.EncodeWorkID(dispatch.Task{Type: "followup-pr", PullRequest: &dispatch.PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &dispatch.PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: "handout-3"}}),
			receipt: `{"prFixId":"fix-42","generation":3,"handoutToken":"handout-3"}`,
			ref:     42,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var cloudOffers, localOffers, issueHandoffs, reportSettlements, handoutAcks int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				task := func() map[string]any {
					if tc.mode == courierv1alpha1.ModeResolveIssue {
						return map[string]any{"shouldRun": true, "type": "implement", "issue": map[string]any{"id": "issue-327", "repoFullName": "acme/widgets", "number": 327}}
					}
					return map[string]any{"shouldRun": true, "type": "followup-pr", "pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42}, "prFixItem": map[string]any{"id": "fix-42", "generation": 3, "handoutToken": "handout-3"}}
				}
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/agents/courier-cloud/next-task":
					cloudOffers++
					_ = json.NewEncoder(w).Encode(task())
				case r.Method == http.MethodGet && r.URL.Path == "/api/agents/courier-local/next-task":
					localOffers++
					_ = json.NewEncoder(w).Encode(task())
				case r.Method == http.MethodGet && r.URL.Path == "/api/issues/state":
					_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-327", "repoFullName": "acme/widgets", "number": 327})
				case r.Method == http.MethodGet && r.URL.Path == "/api/audit":
					_ = json.NewEncoder(w).Encode([]any{})
				case r.Method == http.MethodPost && r.URL.Path == "/api/issues/capacity-handoff":
					var body struct {
						ClaimToken string `json:"claimToken"`
						AgentName  string `json:"agentName"`
						Capacity   string `json:"capacity"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode issue handoff: %v", err)
					}
					if body.ClaimToken != "claim-327" || body.AgentName != "courier-cloud" || body.Capacity != "minimax-pool" {
						t.Errorf("issue handoff fence = %#v, want the cloud receipt token, cloud agent, cloud capacity", body)
					}
					issueHandoffs++
					if issueHandoffs == 1 {
						http.Error(w, "temporary settlement failure", http.StatusServiceUnavailable)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
				case r.Method == http.MethodPost && r.URL.Path == "/api/agents/courier-cloud/tasks/report":
					var body struct {
						Outcome  string             `json:"outcome"`
						Capacity string             `json:"capacity"`
						Item     dispatch.PRFixItem `json:"prFixItem"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode PR settlement: %v", err)
					}
					if body.Outcome != "capacity-exhausted" || body.Capacity != "minimax-pool" || body.Item.ID != "fix-42" || body.Item.Generation != 3 || body.Item.HandoutToken != "handout-3" {
						t.Errorf("PR settlement body = %#v", body)
					}
					reportSettlements++
					if reportSettlements == 1 {
						http.Error(w, "temporary settlement failure", http.StatusServiceUnavailable)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"prFixResolution": map[string]any{"matched": true, "itemId": "fix-42", "action": "deferred"}})
				case r.Method == http.MethodPost && r.URL.Path == "/api/pr-fix-queue/ack":
					var body struct {
						Repo       string `json:"repo"`
						PR         int    `json:"pr"`
						Generation int    `json:"generation"`
						AgentName  string `json:"agentName"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode hand-out ack: %v", err)
					}
					if body.Repo != "acme/widgets" || body.PR != 42 || body.Generation != 3 || body.AgentName != "courier-local" {
						t.Errorf("hand-out ack body = %#v, want the queue identity pinned to the admitting agent", body)
					}
					handoutAcks++
					_ = json.NewEncoder(w).Encode(map[string]any{"acknowledged": true})
				default:
					t.Errorf("unexpected Dispatch request %s %s", r.Method, r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			newAdapter := func(agent string) source.Adapter {
				httpClient, err := dispatch.NewClient(server.URL, agent, "dispatch-token", time.Second)
				if err != nil {
					t.Fatal(err)
				}
				httpClient = httpClient.WithPullRequestStateChecker(dispatch.PullRequestStateCheckerFunc(func(context.Context, string, int) (dispatch.PullRequestState, error) {
					return dispatch.PullRequestState{State: "open"}, nil
				}))
				return dispatch.NewAdapter(httpClient)
			}

			t0 := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
			cloud := admissionLane("minimax-cloud", 1)
			cloud.Spec.CapacityGroup = "minimax-pool"
			local := admissionLane("qwen-local", 1)
			local.Spec.CapacityGroup = "qwen-box"
			predecessor := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "cloud-attempt", Namespace: admissionNamespace, UID: types.UID("uid-cloud-attempt")},
				Spec:       courierv1alpha1.CoderRunSpec{Source: "dispatch", SourceAgent: "courier-cloud", WorkItemID: tc.id, Mode: tc.mode, Repo: "acme/widgets", Ref: tc.ref, Lane: cloud.Name},
				Status: courierv1alpha1.CoderRunStatus{
					Phase:           courierv1alpha1.PhaseBlockedExternal,
					SourceReceipt:   tc.receipt,
					Branch:          "run-branch",
					WorkspaceSafety: &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"},
					TerminalDiagnostic: &courierv1alpha1.TerminalDiagnostic{
						Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
					},
					Conditions: []metav1.Condition{
						{Type: courierv1alpha1.ConditionLifecycleReported, Status: metav1.ConditionTrue, Reason: "Published", LastTransitionTime: metav1.NewTime(t0)},
					},
				},
			}
			terminalPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cloud-attempt-pod", Namespace: admissionNamespace, Labels: map[string]string{executor.LabelRun: executor.RunLabelValue(predecessor.Name)}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
			scheme := runtime.NewScheme()
			if err := courierv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courierv1alpha1.CoderRun{}, &courierv1alpha1.LaneProfile{}).WithObjects(cloud, local, predecessor, terminalPod).Build()
			reconciler := &CoderRunReconciler{Client: kube, Sources: NewSourceRegistry(map[string]source.Adapter{"dispatch": newAdapter("courier-cloud"), "dispatch:courier-cloud": newAdapter("courier-cloud")}), StatusWriter: fakeStatusWriter{client: kube}, Now: func() time.Time { return t0 }}
			ctx := context.Background()
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := reconciler.Reconcile(ctx, admissionRequest(predecessor.Name)); err != nil {
					t.Fatalf("Reconcile attempt %d: %v", attempt+1, err)
				}
			}
			var settled courierv1alpha1.CoderRun
			if err := kube.Get(ctx, admissionKey(predecessor.Name), &settled); err != nil {
				t.Fatal(err)
			}
			if recovery := apimeta.FindStatusCondition(settled.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery); recovery == nil || recovery.Status != metav1.ConditionTrue || recovery.Reason != capacityRecoverySettled {
				t.Fatalf("recovery condition after retry = %#v", recovery)
			}

			// Five minutes into the fifteen-minute window: the exhausted
			// cloud capacity is still cooling, the local capacity is not.
			now := t0.Add(5 * time.Minute)
			var storedCloud, storedLocal courierv1alpha1.LaneProfile
			if err := kube.Get(ctx, client.ObjectKeyFromObject(cloud), &storedCloud); err != nil {
				t.Fatal(err)
			}
			if err := kube.Get(ctx, client.ObjectKeyFromObject(local), &storedLocal); err != nil {
				t.Fatal(err)
			}
			if !storedCloud.CooldownActive(now) {
				t.Fatal("cloud capacity cooldown not retained inside the window")
			}
			if storedLocal.CooldownActive(now) {
				t.Fatal("local capacity must not inherit the cloud identity's cooldown")
			}

			// The cooled cloud profile may not even ask for work.
			cloudRunner := source.NewRunner(kube, newAdapter("courier-cloud"), source.RunnerConfig{Source: "dispatch", SourceAgent: "courier-cloud", LaneProfile: cloud.Name, Namespace: admissionNamespace})
			cloudRunner.Now = func() time.Time { return now }
			if err := cloudRunner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			cloudAsk := cloudOffers
			mu.Unlock()
			if cloudAsk != 0 {
				t.Fatalf("cooled cloud capacity asked Dispatch for work %d times", cloudAsk)
			}

			// The local profile discovers and admits the successor while
			// the cloud cooldown is still open.
			localRunner := source.NewRunner(kube, newAdapter("courier-local"), source.RunnerConfig{Source: "dispatch", SourceAgent: "courier-local", LaneProfile: local.Name, Namespace: admissionNamespace})
			localRunner.Now = func() time.Time { return now }
			if err := localRunner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			var runs courierv1alpha1.CoderRunList
			if err := kube.List(ctx, &runs, client.InNamespace(admissionNamespace)); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("runs = %d, want retained cloud predecessor plus local successor", len(runs.Items))
			}
			var successor *courierv1alpha1.CoderRun
			for i := range runs.Items {
				if runs.Items[i].Name != predecessor.Name {
					successor = &runs.Items[i]
				}
			}
			if successor == nil {
				t.Fatal("successor not materialized")
			}
			if successor.Spec.Lane != local.Name || successor.Spec.SourceAgent != "courier-local" {
				t.Fatalf("successor admitted on the wrong capacity: lane=%q agent=%q", successor.Spec.Lane, successor.Spec.SourceAgent)
			}
			if successor.Spec.Mode != tc.mode || successor.Spec.Repo != "acme/widgets" || successor.Spec.Ref != tc.ref {
				t.Fatalf("successor did not preserve the work identity: %#v", successor.Spec)
			}
			if tc.mode == courierv1alpha1.ModeFixPR && !strings.Contains(successor.Spec.SourceContext, `"prFixId":"fix-42"`) {
				t.Fatalf("PR-fix successor lost the queue identity that pins the original PR and head branch: %q", successor.Spec.SourceContext)
			}
			if predecessor.Status.Phase != courierv1alpha1.PhaseBlockedExternal {
				t.Fatalf("predecessor terminal history not retained: %#v", predecessor.Status)
			}

			// Repeated polls cannot duplicate the attempt or let the cooled
			// capacity re-engage; the settlement already happened exactly
			// once (after one transient retry) and never re-fires.
			if err := localRunner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			if err := cloudRunner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(ctx, admissionRequest(predecessor.Name)); err != nil {
				t.Fatal(err)
			}
			if err := kube.List(ctx, &runs, client.InNamespace(admissionNamespace)); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("runs = %d after repeated polls, want no duplicate attempt", len(runs.Items))
			}
			mu.Lock()
			defer mu.Unlock()
			if cloudOffers != 0 {
				t.Fatalf("cloud capacity engaged Dispatch after cooldown began: offers=%d", cloudOffers)
			}
			if localOffers != 1 {
				t.Fatalf("local offers = %d, want one: the pending successor fills the local lane's capacity, so repeat polls must not ask again", localOffers)
			}
			if tc.mode == courierv1alpha1.ModeResolveIssue && (issueHandoffs != 2 || reportSettlements != 0) {
				t.Fatalf("issue settlements = handoffs %d reports %d, want one transient retry then success", issueHandoffs, reportSettlements)
			}
			if tc.mode == courierv1alpha1.ModeFixPR && (reportSettlements != 2 || issueHandoffs != 0) {
				t.Fatalf("PR settlements = reports %d handoffs %d, want one transient retry then success", reportSettlements, issueHandoffs)
			}
			wantAcks := 0
			if tc.mode == courierv1alpha1.ModeFixPR {
				wantAcks = 1
			}
			if handoutAcks != wantAcks {
				t.Fatalf("hand-out acks = %d, want %d: exactly the admitting materialization", handoutAcks, wantAcks)
			}
		})
	}
}
