package source_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/dispatch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestRunnerMakesProgressAfterExclusionOverflow pins the end-to-end invariant
// behind #286: when retained PR-fix exclusions exceed Dispatch's 32-entry
// contract cap, truncation must not reintroduce the all-lanes-idle starvation.
// Progress has two halves. The Dispatch-side half is the server's per-agent,
// per-generation handout ledger (next-task scans past candidates the agent was
// already handed); this test pins Courier's half against a scripted server: a
// re-offered, truncated-out generation is absorbed by same-identity dedupe
// without a duplicate CoderRun, a missing exclusion acknowledgment still fails
// closed under overflow, and the next genuinely new offer is materialized.
func TestRunnerMakesProgressAfterExclusionOverflow(t *testing.T) {
	const retainedTotal = 40
	const omittedNumber = 40 // oldest terminal retained run: guaranteed truncated out
	const freshNumber = 9001 // genuinely new work
	followupID := func(number int) string {
		return dispatch.EncodeWorkID(dispatch.Task{
			Type:        "followup-pr",
			PullRequest: &dispatch.PullRequest{Repo: "acme/widgets", Number: number},
			PRFixItem:   &dispatch.PRFixItem{ID: fmt.Sprintf("item-%d", number), Generation: 1},
		})
	}

	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "local"},
		Spec:       courierv1alpha1.LaneProfileSpec{Concurrency: 8},
	}
	objs := []client.Object{lane}
	now := time.Now()
	for i := 1; i <= retainedTotal; i++ {
		// The first four runs are unsettled; the rest are terminal retained
		// work with staggered ages, so run 40 is the oldest and ranks last.
		phase := courierv1alpha1.PhaseAwaitingReview
		age := time.Duration(100+i) * time.Hour
		if i <= 4 {
			phase = courierv1alpha1.PhaseRunning
			age = time.Duration(i) * time.Hour
		}
		objs = append(objs, &courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:              fmt.Sprintf("retained-%d", i),
				Namespace:         "courier",
				CreationTimestamp: metav1.NewTime(now.Add(-age)),
			},
			Spec: courierv1alpha1.CoderRunSpec{
				Source:     "dispatch",
				WorkItemID: followupID(i),
				Mode:       courierv1alpha1.ModeFixPR,
				Repo:       "acme/widgets",
				Ref:        i,
				Lane:       "local",
			},
			Status: courierv1alpha1.CoderRunStatus{Phase: phase},
		})
	}

	followupTask := func(number int) map[string]any {
		return map[string]any{
			"type":        "followup-pr",
			"shouldRun":   true,
			"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": number},
			"prFixItem":   map[string]any{"id": fmt.Sprintf("item-%d", number), "generation": 1},
		}
	}
	var mu sync.Mutex
	var wire [][]string
	step := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		wire = append(wire, r.URL.Query()["exclude_pr_fix"])
		current := step
		step++
		if current == 1 {
			// Overflow request without the required acknowledgment.
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "idle", "shouldRun": false})
			return
		}
		payload := followupTask(omittedNumber)
		if current == 2 {
			payload = followupTask(freshNumber)
		}
		payload["prFixExclusions"] = map[string]any{"version": 1, "skipped": []any{}}
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer server.Close()

	httpClient, err := dispatch.NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	httpClient.WithPRFixExclusions(true).WithPullRequestStateChecker(dispatch.PullRequestStateCheckerFunc(
		func(context.Context, string, int) (dispatch.PullRequestState, error) {
			return dispatch.PullRequestState{State: "open"}, nil
		}))
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	runner := source.NewRunner(kubeClient, dispatch.New(httpClient), source.RunnerConfig{Source: "dispatch", LaneProfile: "local", Namespace: "courier"})

	listRuns := func() []courierv1alpha1.CoderRun {
		t.Helper()
		var runs courierv1alpha1.CoderRunList
		if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
			t.Fatal(err)
		}
		return runs.Items
	}
	countWorkItem := func(id string) int {
		t.Helper()
		count := 0
		for _, run := range listRuns() {
			if run.Spec.WorkItemID == id {
				count++
			}
		}
		return count
	}
	exclusionValue := func(number int) string {
		return fmt.Sprintf("acme/widgets#%d@1", number)
	}

	// Poll 1: Dispatch re-hands the truncated-out generation. The overflow
	// request carries exactly the contract maximum, the omitted generation is
	// absent from it, and the re-offer is suppressed without a duplicate run.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	firstWire := append([]string(nil), wire[0]...)
	mu.Unlock()
	if len(firstWire) != 32 {
		t.Fatalf("first request carried %d exclusions, want 32", len(firstWire))
	}
	for _, value := range firstWire {
		if value == exclusionValue(omittedNumber) {
			t.Fatal("omitted generation was excluded despite truncation")
		}
	}
	if countWorkItem(followupID(omittedNumber)) != 1 {
		t.Fatal("re-handed omitted generation was not absorbed by same-identity dedupe")
	}
	if got := len(listRuns()); got != retainedTotal {
		t.Fatalf("runs = %d after re-handed generation, want %d", got, retainedTotal)
	}

	// Poll 2: a missing exclusion acknowledgment still fails closed under
	// overflow, with no work materialized.
	if err := runner.Poll(context.Background()); err == nil {
		t.Fatal("missing exclusion acknowledgment was accepted under overflow")
	}
	if got := len(listRuns()); got != retainedTotal {
		t.Fatalf("runs = %d after failed acknowledgment, want %d", got, retainedTotal)
	}

	// Poll 3: the next genuinely new offer is materialized.
	if err := runner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if countWorkItem(followupID(freshNumber)) != 1 {
		t.Fatal("genuinely new work was not materialized after overflow")
	}
	if got := len(listRuns()); got != retainedTotal+1 {
		t.Fatalf("runs = %d after new work, want %d", got, retainedTotal+1)
	}
	mu.Lock()
	requests := len(wire)
	mu.Unlock()
	if requests != 3 {
		t.Fatalf("discovery requests = %d, want 3", requests)
	}
}
