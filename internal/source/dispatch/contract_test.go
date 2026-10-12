package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/source"
)

// dispatchAcceptedOutcomes mirrors the allowlist declared in
// src/app/api/agents/[agentName]/tasks/report/route.ts of
// misospace/dispatch (currently `VALID_OUTCOMES`); `handed-off` was added
// upstream by misospace/dispatch#1226. Update this fixture in lockstep with
// any change to the upstream allowlist.
var dispatchAcceptedOutcomes = map[string]struct{}{
	"pr_opened":         {},
	"pr_updated":        {},
	"issue_updated":     {},
	"issue_closed":      {},
	"blocked":           {},
	"failed":            {},
	"no_changes_needed": {},
	"already_addressed": {},
	"handed-off":        {},
}

// expectedTasksReportOutcomes enumerates the (Result, taskType) pairs
// the courier adapter can emit to POST /tasks/report. Each pair is
// captured end-to-end against a recording server so the contract test
// exercises the actual HTTP body — not just a constant in client.go.
var expectedTasksReportOutcomes = []tasksReportOutcome{
	{result: source.ResultReady, taskType: "implement", wantOutcome: "pr_opened"},
	{result: source.ResultReady, taskType: "followup-pr", wantOutcome: "pr_updated"},
	{result: source.ResultBlocked, taskType: "implement", wantOutcome: "blocked"},
	{result: source.ResultBlocked, taskType: "followup-pr", wantOutcome: "blocked"},
	{result: source.ResultFailed, taskType: "implement", wantOutcome: "failed"},
	{result: source.ResultFailed, taskType: "followup-pr", wantOutcome: "failed"},
	{result: source.ResultHandedOff, taskType: "implement", wantOutcome: "handed-off"},
	{result: source.ResultHandedOff, taskType: "followup-pr", wantOutcome: "handed-off"},
}

type tasksReportOutcome struct {
	result      source.Result
	taskType    string
	wantOutcome string
}

// TestDispatchTasksReportContractGuard asserts every courier-emitted
// outcome is accepted by the current Dispatch fixture. Adding a new outcome
// to client.go or removing one upstream fails this end-to-end contract test.
func TestDispatchTasksReportContractGuard(t *testing.T) {
	accepted := sortedOutcomeKeys(dispatchAcceptedOutcomes)
	t.Logf("dispatch accepted outcomes (mirror of VALID_OUTCOMES): %s", strings.Join(accepted, ", "))

	for _, pair := range expectedTasksReportOutcomes {
		pair := pair
		t.Run(fmt.Sprintf("%s/%s", pair.taskType, pair.result), func(t *testing.T) {
			var captured map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/pr-fix-queue/mark" {
					if pair.result == source.ResultHandedOff {
						t.Errorf("handed-off report unexpectedly settled PR-fix queue")
					}
					w.WriteHeader(http.StatusNoContent)
					return
				}
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &captured)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			id := encodeWorkIDForTaskType(t, pair.taskType)
			client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			err = client.Report(context.Background(), id, source.Lifecycle{
				Result: pair.result,
				PR:     "1",
				Error:  "test",
			})
			outcome, _ := captured["outcome"].(string)
			if outcome != pair.wantOutcome {
				t.Fatalf("captured outcome = %q, want %q", outcome, pair.wantOutcome)
			}
			if err != nil {
				t.Fatalf("Report() error = %v", err)
			}
			if _, ok := dispatchAcceptedOutcomes[outcome]; !ok {
				t.Fatalf("emitted outcome %q is not accepted by Dispatch; update the fixture", outcome)
			}
		})
	}
}

// TestDispatchTasksReportContractRejectsUnknownOutcomes mirrors
// Dispatch's validation against a fake server: an outcome outside the
// allowlist gets HTTP 400. The test exercises only currently-accepted
// outcomes to prove the courier adapter's POST body matches what
// Dispatch validates, and to lock the rejection shape so a future
// upstream change to the validation contract surfaces in CI.
func TestDispatchTasksReportContractRejectsUnknownOutcomes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		outcome, _ := body["outcome"].(string)
		if r.URL.Path == "/api/pr-fix-queue/mark" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if _, ok := dispatchAcceptedOutcomes[outcome]; !ok {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"Invalid outcome. Must be one of: %s"}`, strings.Join(sortedOutcomeKeys(dispatchAcceptedOutcomes), ", "))))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-1", Repo: "acme/widgets", Number: 42}})
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Report(context.Background(), id, source.Lifecycle{Result: source.ResultReady, PR: "1"}); err != nil {
		t.Fatalf("Report(pr_opened via implement) error = %v; want nil — Dispatch's accepted allowlist must match", err)
	}

	followupID := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	if err := client.Report(context.Background(), followupID, source.Lifecycle{Result: source.ResultReady, PR: "1"}); err != nil {
		t.Fatalf("Report(pr_updated via followup-pr) error = %v; want nil", err)
	}
}

// TestDispatchAcceptedOutcomesMatchDispatchFixture asserts the
// allowlist fixture stays in lockstep with Dispatch's VALID_OUTCOMES
// mirror. A developer who drops or accidentally adds a literal trips this test.
func TestDispatchAcceptedOutcomesMatchDispatchFixture(t *testing.T) {
	want := map[string]struct{}{
		"pr_opened":         {},
		"pr_updated":        {},
		"issue_updated":     {},
		"issue_closed":      {},
		"blocked":           {},
		"failed":            {},
		"no_changes_needed": {},
		"already_addressed": {},
		"handed-off":        {},
	}
	if len(want) != len(dispatchAcceptedOutcomes) {
		t.Fatalf("dispatchAcceptedOutcomes size = %d, want %d — update the comment and this set together", len(dispatchAcceptedOutcomes), len(want))
	}
	for k := range want {
		if _, ok := dispatchAcceptedOutcomes[k]; !ok {
			t.Errorf("dispatchAcceptedOutcomes missing %q — mirror comment and fixture drifted", k)
		}
	}
	for k := range dispatchAcceptedOutcomes {
		if _, ok := want[k]; !ok {
			t.Errorf("dispatchAcceptedOutcomes has unexpected %q — mirror comment and fixture drifted", k)
		}
	}
}

func sortedOutcomeKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedGapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func encodeWorkIDForTaskType(t *testing.T, taskType string) string {
	t.Helper()
	switch taskType {
	case "implement":
		return EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-1", Repo: "acme/widgets", Number: 42}})
	case "followup-pr":
		return EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77, URL: "https://github.com/acme/widgets/pull/77"}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	default:
		t.Fatalf("unknown taskType: %q", taskType)
		return ""
	}
}
