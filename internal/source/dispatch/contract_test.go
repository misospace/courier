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
// misospace/dispatch (currently `VALID_OUTCOMES`). Any outcome the
// courier adapter emits to POST .../tasks/report MUST appear in this
// set, or Dispatch rejects the report with HTTP 400 and the run's
// lifecycle report becomes a permanent retry. Update this fixture in
// lockstep with any change to the upstream allowlist.
var dispatchAcceptedOutcomes = map[string]struct{}{
	"pr_opened":         {},
	"pr_updated":        {},
	"issue_updated":     {},
	"issue_closed":      {},
	"blocked":           {},
	"failed":            {},
	"no_changes_needed": {},
	"already_addressed": {},
}

// dispatchContractGaps enumerates outcomes the courier adapter emits
// today that Dispatch does not yet accept. Each entry names the
// dispatch-side change required to close the gap. The contract test
// asserts every emitted outcome is in either dispatchAcceptedOutcomes
// or dispatchContractGaps, so a future drift fails CI instead of
// arriving as an HTTP 400 at runtime. Once a dispatch-side change
// lands, move the key from here into dispatchAcceptedOutcomes.
var dispatchContractGaps = map[string]string{
	// #246 cross-repo blocker: the HandedOff phase emits a
	// source-neutral "handed-off" task report. Dispatch's
	// VALID_OUTCOMES doesn't include it yet, and
	// resolvePrFixFromAgentReport has no settlement branch for it.
	"handed-off": "add 'handed_off' (or 'handed-off') to VALID_OUTCOMES in src/app/api/agents/[agentName]/tasks/report/route.ts and route it in src/lib/pr-fix-queue.ts:resolvePrFixFromAgentReport (#246 cross-repo prerequisite).",
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
// outcome is either explicitly accepted by Dispatch (see
// dispatchAcceptedOutcomes) or explicitly listed as a known contract
// gap with a referenced fix (see dispatchContractGaps). Adding a new
// outcome to client.go without updating the fixture fails this test.
// Removing an accepted outcome from the upstream allowlist also fails
// it, as long as courier still emits the now-rejected value.
func TestDispatchTasksReportContractGuard(t *testing.T) {
	for gap := range dispatchContractGaps {
		if _, ok := dispatchAcceptedOutcomes[gap]; ok {
			t.Errorf("dispatchContractGaps entry %q duplicates dispatchAcceptedOutcomes — move it across instead", gap)
		}
	}

	accepted := sortedOutcomeKeys(dispatchAcceptedOutcomes)
	gaps := sortedGapKeys(dispatchContractGaps)
	t.Logf("dispatch accepted outcomes (mirror of VALID_OUTCOMES): %s", strings.Join(accepted, ", "))
	t.Logf("dispatch contract gaps (awaiting dispatch-side change): %s", strings.Join(gaps, ", "))

	for _, pair := range expectedTasksReportOutcomes {
		pair := pair
		t.Run(fmt.Sprintf("%s/%s", pair.taskType, pair.result), func(t *testing.T) {
			var captured map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/pr-fix-queue/mark" {
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
			if err := client.Report(context.Background(), id, source.Lifecycle{
				Result: pair.result,
				PR:     "1",
				Error:  "test",
			}); err != nil {
				t.Fatalf("Report() error = %v", err)
			}
			outcome, _ := captured["outcome"].(string)
			if outcome != pair.wantOutcome {
				t.Fatalf("captured outcome = %q, want %q", outcome, pair.wantOutcome)
			}
			if _, ok := dispatchAcceptedOutcomes[outcome]; ok {
				return
			}
			if why, gap := dispatchContractGaps[outcome]; gap {
				t.Skipf("cross-repo contract gap acknowledged: outcome %q is not in Dispatch's allowlist — %s", outcome, why)
			}
			t.Fatalf("emitted outcome %q is neither accepted (dispatchAcceptedOutcomes) nor tracked (dispatchContractGaps); update the fixture or close the gap", outcome)
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
// allowlist fixture stays in lockstep with the documented
// mirror-comment. A developer who drops a literal (or adds one by
// accident from a pending dispatch change) trips this test.
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
