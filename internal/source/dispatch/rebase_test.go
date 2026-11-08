package dispatch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/source"
)

// rebaseTask is the Dispatch-shaped next-task response for one REBASE
// followup on acme/widgets#42, generation 3, with the handout the offer
// stamped.
func rebaseTask() Task {
	return Task{
		Type:        "followup-pr",
		ShouldRun:   true,
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42, URL: "https://github.com/acme/widgets/pull/42", State: "open"},
		PRFixItem:   &PRFixItem{ID: "fix-1", Generation: 3, HandoutToken: "hand-9", FixType: "REBASE"},
		Reasons:     []string{"mechanical rebase only: lockfiles, version strings, import order; never force-push outside the leased publication"},
	}
}

// openPRChecker satisfies the discovery-time PR-state read with an open PR.
func openPRChecker() PullRequestStateChecker {
	return PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
		return PullRequestState{State: "open"}, nil
	})
}

// newTestClient builds a client pointed at the server with the standard
// test wiring; rebase toggles the versioned rebase-only intake declaration.
func newTestClient(t *testing.T, serverURL string, rebase bool) *HTTPClient {
	t.Helper()
	client, err := NewClientWithLane(serverURL, "worker", "rebase", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(openPRChecker())
	// The configured-ownership gate (#761) runs before the rebase
	// classification, so the rebase fixtures carry a same-lane claimant: the
	// "local" agent is bound to this connector's own "rebase" queue lane.
	client.WithOwnershipBindings(map[string]string{"local": "rebase"})
	if rebase {
		client.WithRebaseCapability(true)
	}
	return client
}

func decodeDescriptor(t *testing.T, id string) workDescriptor {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		t.Fatalf("decode work id: %v", err)
	}
	var d workDescriptor
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal work id: %v", err)
	}
	return d
}

// rebaseIntakeAckBody is the exact intake acknowledgment Dispatch returns for
// the v1 rebase-only negotiation (misospace/dispatch#1533): contract version
// 1, the rebase-only scope, enforced, and rebase-executor granted at the
// requested version.
func rebaseIntakeAckBody() map[string]any {
	return map[string]any{
		"version":       1,
		"scope":         "rebase-only",
		"scopeEnforced": true,
		"capabilities": []any{
			map[string]any{"name": "rebase-executor", "requested": 1, "granted": true},
		},
	}
}

// serveRebaseOffer answers the next-task poll with one REBASE followup and
// the given intake acknowledgment (nil means Dispatch sent none, the old
// deployment shape), and accepts the decline release the client issues for
// the handout old Dispatch stamped before returning. It asserts the poll
// carried the exact v1 rebase-only intake declaration.
func serveRebaseOffer(t *testing.T, intake map[string]any) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/pr-fix-queue/mark" {
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			return
		}
		q := r.URL.Query()
		if q.Get("work_scope") != "rebase-only" || q.Get("work_scope_version") != "1" || q.Get("capabilities") != "rebase-executor:1" {
			t.Fatalf("rebase-only intake declaration missing or wrong: %s", r.URL.RawQuery)
		}
		body := map[string]any{
			"type": "followup-pr", "shouldRun": true,
			"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42, "state": "open"},
			"prFixItem":   map[string]any{"id": "fix-1", "generation": 3, "handoutToken": "hand-9", "fixType": "REBASE", "originalClaimant": "local"},
		}
		if intake != nil {
			body["intake"] = intake
		}
		_ = json.NewEncoder(w).Encode(body)
	}
}

// A negotiated rebase executor accepts REBASE work as rebase mode, keeping
// the followup-pr transport type and the full receipt intact (#715). The
// negotiation is Dispatch's real intake acknowledgment: the declaration rides
// the request (work_scope, work_scope_version, capabilities) and the exact v1
// acknowledgment rides the task.
func TestRebaseDiscoveryAcceptsRebaseWorkUnderNegotiatedIntake(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(serveRebaseOffer(t, rebaseIntakeAckBody())))
	defer server.Close()
	items, err := newTestClient(t, server.URL, true).Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %#v", items)
	}
	if items[0].Mode != "rebase" {
		t.Fatalf("mode = %q, want rebase (never fix-pr)", items[0].Mode)
	}
	d := decodeDescriptor(t, items[0].ID)
	if d.Type != "followup-pr" || d.PRFixID != "fix-1" || d.Generation != 3 || d.HandoutToken != "hand-9" || d.FixType != "REBASE" {
		t.Fatalf("descriptor = %#v", d)
	}
	// The durable receipt preserves the fix type through claim and recovery.
	var receipt dispatchContext
	if err := json.Unmarshal([]byte(items[0].Context), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.PRFixID != "fix-1" || receipt.Generation != 3 || receipt.HandoutToken != "hand-9" || receipt.FixType != "REBASE" {
		t.Fatalf("receipt = %#v", receipt)
	}
	if items[0].Repo != "acme/widgets" || items[0].Ref != 42 {
		t.Fatalf("item identity = %#v", items[0])
	}
	if !strings.Contains(WorkIdentity(items[0].ID), "followup-pr/acme/widgets#42/fix-1@3") {
		t.Fatalf("work identity = %q", WorkIdentity(items[0].ID))
	}
}

// A connector that did not negotiate the rebase capability must never map
// REBASE work onto fix-pr: it declines the offer through the defined
// recoverable release, presenting the exact receipt, and surfaces why.
func TestRebaseDiscoveryDeclinesUnnegotiatedRebaseWork(t *testing.T) {
	var marks []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			// No intake acknowledgment at all: a deployment that predates the
			// contract, offering REBASE work exactly as 0.5.73 does.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
				"prFixItem":   map[string]any{"id": "fix-1", "generation": 3, "handoutToken": "hand-9", "fixType": "REBASE", "originalClaimant": "local"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			marks = append(marks, body)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, false)
	_, err := client.Discover(context.Background())
	if !strings.Contains(fmt.Sprintf("%v", err), "rebase work declined") {
		t.Fatalf("err = %v, want a rebase decline", err)
	}
	if len(marks) != 1 {
		t.Fatalf("marks = %#v, want exactly one release", marks)
	}
	if marks[0]["status"] != "WAITING_EXTERNAL" || marks[0]["repo"] != "acme/widgets" || marks[0]["pr"] != float64(42) || marks[0]["generation"] != float64(3) {
		t.Fatalf("release mark = %#v, want the exact receipt", marks[0])
	}
}

// A rebase offer whose receipt is malformed fails discovery without
// acceptance: the receipt is the settlement contract, and an attempt without
// one could never be settled truthfully. No release is attempted, because
// there is nothing valid to release against.
func TestRebaseDiscoveryDeclinesReceiptlessRebaseOffer(t *testing.T) {
	var marks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
				"prFixItem":   map[string]any{"id": "", "generation": 0, "fixType": "REBASE", "originalClaimant": "local"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, false)
	_, err := client.Discover(context.Background())
	if err == nil {
		t.Fatal("a malformed rebase offer must not be accepted")
	}
	if marks != 0 {
		t.Fatalf("release attempts = %d, want none for an unreleasable receipt", marks)
	}
}

// A negotiated connector that receives ordinary work is looking at an ignored
// rebase-only scope. The work is a protocol error, never accepted, and never
// released on the ordinary item's behalf — even with a fully affirmative
// intake acknowledgment on the task.
func TestRebaseDiscoveryNeverAcceptsOrdinaryWorkUnderRebaseOnly(t *testing.T) {
	var marks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
				"prFixItem":   map[string]any{"id": "fix-2", "generation": 1, "fixType": "ci-failure", "originalClaimant": "local"},
				"intake":      rebaseIntakeAckBody(),
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "scope was ignored") {
		t.Fatalf("err = %v, want the ignored-scope protocol error", err)
	}
	if marks != 0 {
		t.Fatalf("release marks = %d, want none for ordinary work", marks)
	}
}

// An absent intake acknowledgment fails negotiation: nothing is accepted.
// This is the old-Dispatch shape — the deployment predates dispatch#1533 and
// never answers the rebase-only declaration.
func TestRebaseDiscoveryFailsClosedOnMissingIntakeAck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(serveRebaseOffer(t, nil)))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
		t.Fatalf("err = %v, want failed negotiation", err)
	}
}

// A different intake contract version fails negotiation: an unknown contract
// is never guessed at.
func TestRebaseDiscoveryFailsClosedOnWrongContractVersion(t *testing.T) {
	ack := rebaseIntakeAckBody()
	ack["version"] = 2
	server := httptest.NewServer(http.HandlerFunc(serveRebaseOffer(t, ack)))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
		t.Fatalf("err = %v, want failed negotiation", err)
	}
}

// A scope other than rebase-only fails negotiation: Courier declared a
// narrower queue and must never accept work served from a different one.
func TestRebaseDiscoveryFailsClosedOnWrongScope(t *testing.T) {
	ack := rebaseIntakeAckBody()
	ack["scope"] = "all-work"
	server := httptest.NewServer(http.HandlerFunc(serveRebaseOffer(t, ack)))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
		t.Fatalf("err = %v, want failed negotiation", err)
	}
}

// A scope Dispatch did not enforce fails negotiation: "declared but not
// enforceable" is never read as honored.
func TestRebaseDiscoveryFailsClosedOnUnenforcedScope(t *testing.T) {
	ack := rebaseIntakeAckBody()
	ack["scopeEnforced"] = false
	server := httptest.NewServer(http.HandlerFunc(serveRebaseOffer(t, ack)))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
		t.Fatalf("err = %v, want failed negotiation", err)
	}
}

// A rebase-executor verdict that is not granted fails negotiation: the
// capability is the proof Courier can execute the work, and an ungranted
// verdict unlocks nothing.
func TestRebaseDiscoveryFailsClosedOnCapabilityNotGranted(t *testing.T) {
	ack := rebaseIntakeAckBody()
	ack["capabilities"] = []any{
		map[string]any{"name": "rebase-executor", "requested": 1, "granted": false, "reason": "unknown-capability"},
	}
	server := httptest.NewServer(http.HandlerFunc(serveRebaseOffer(t, ack)))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
		t.Fatalf("err = %v, want failed negotiation", err)
	}
}

// A rebase-executor verdict requested at a different version fails
// negotiation: rebase-executor:1 and rebase-executor:2 are different claims.
func TestRebaseDiscoveryFailsClosedOnCapabilityWrongRequestedVersion(t *testing.T) {
	ack := rebaseIntakeAckBody()
	ack["capabilities"] = []any{
		map[string]any{"name": "rebase-executor", "requested": 2, "granted": true},
	}
	server := httptest.NewServer(http.HandlerFunc(serveRebaseOffer(t, ack)))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
		t.Fatalf("err = %v, want failed negotiation", err)
	}
}

// Old Dispatch with a rebase-requesting new Courier: the deployment ignores
// the intake declaration and its shared pr-fix hand-out path stamps the
// handout before returning (confirmed against dispatch@9e289b8~1 — the
// agentHandouts/handoutReceipts/attemptType queue write commits before the
// response, and every handed followup-pr candidate, REBASE or ordinary,
// ships prFixItem{id, generation, handoutToken}). The ordinary offer is
// therefore an ISSUED handout: Courier refuses execution and releases that
// exact receipt through the fenced decline path, once per generation.
func TestRebaseDiscoveryFailsClosedOnOldDispatchOrdinaryWork(t *testing.T) {
	var marks []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
				"prFixItem":   map[string]any{"id": "fix-7", "generation": 2, "handoutToken": "hand-7", "fixType": "ci-failure"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			marks = append(marks, body)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	for i := 0; i < 2; i++ {
		_, err := client.Discover(context.Background())
		if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") || !strings.Contains(err.Error(), "rebase work declined") {
			t.Fatalf("poll %d: err = %v, want the declined negotiation failure against old Dispatch", i, err)
		}
	}
	if len(marks) != 1 {
		t.Fatalf("release marks = %#v, want exactly one for the ordinary issued handout", marks)
	}
	if marks[0]["status"] != "WAITING_EXTERNAL" || marks[0]["repo"] != "acme/widgets" || marks[0]["pr"] != float64(42) || marks[0]["generation"] != float64(2) {
		t.Fatalf("release mark = %#v, want the exact receipt", marks[0])
	}
}

// Old Dispatch stamps the handout BEFORE next-task returns: the queue write
// (agentHandouts push, handout receipt, attemptType lane stamp) is committed
// before the response ships (confirmed against dispatch@9e289b8~1). A
// REBASE-marked queue-backed task with no intake acknowledgment is therefore
// an ISSUED handout, not a mere offer: Courier refuses execution AND
// releases that exact receipt through the bounded decline path. The release
// is fenced once per generation — repeating the poll against the same
// un-negotiated deployment must not issue a second release.
func TestRebaseDiscoveryReleasesIssuedHandoutWhenIntakeNegotiationFails(t *testing.T) {
	var marks []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			// Old Dispatch shape: queue-backed REBASE followup, handout
			// already stamped, no intake acknowledgment (dispatch#1533 only
			// shipped in 9e289b8).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42, "state": "open"},
				"prFixItem":   map[string]any{"id": "fix-1", "generation": 3, "handoutToken": "hand-9", "fixType": "REBASE"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			marks = append(marks, body)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	for i := 0; i < 2; i++ {
		_, err := client.Discover(context.Background())
		if err == nil {
			t.Fatalf("poll %d: negotiation failure did not surface", i)
		}
		if !strings.Contains(err.Error(), "intake negotiation failed") || !strings.Contains(err.Error(), "rebase work declined") {
			t.Fatalf("poll %d: err = %v, want the declined negotiation failure", i, err)
		}
	}
	if len(marks) != 1 {
		t.Fatalf("release marks = %#v, want exactly one per generation", marks)
	}
	if marks[0]["status"] != "WAITING_EXTERNAL" || marks[0]["repo"] != "acme/widgets" || marks[0]["pr"] != float64(42) || marks[0]["generation"] != float64(3) {
		t.Fatalf("release mark = %#v, want the exact receipt", marks[0])
	}
}

// Old Dispatch without the waiting-external mark refuses the release. The
// decline must stay bounded: one release attempt for the generation, the
// diagnostic preserved, and the documented stale-handout reclaimer left as
// the recovery path — never an indefinite release loop.
func TestRebaseDiscoveryReleaseRefusalUnderOldDispatchIsBounded(t *testing.T) {
	var marks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42, "state": "open"},
				"prFixItem":   map[string]any{"id": "fix-1", "generation": 3, "handoutToken": "hand-9", "fixType": "REBASE"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			// An older deployment that refuses the recoverable wait status.
			http.Error(w, `{"error":"Invalid status. Must be one of: BLOCKED, STALE"}`, http.StatusBadRequest)
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	for i := 0; i < 3; i++ {
		_, err := client.Discover(context.Background())
		if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
			t.Fatalf("poll %d: err = %v, want the negotiation failure with the refusal preserved", i, err)
		}
	}
	if marks != 1 {
		t.Fatalf("release attempts = %d, want exactly one per generation despite the refusal", marks)
	}
}

// Old Dispatch can also hand a rebase-declaring connector issue
// implementation work. That path cannot produce an issued pr-fix receipt —
// createImplementTask carries no prFixItem and Dispatch stamps no handout
// for issue work (confirmed against dispatch@9e289b8~1) — so there is
// nothing to release: the negotiation failure surfaces without any release
// call, and nothing is fabricated.
func TestRebaseDiscoveryNegotiationFailureWithoutReceiptReleasesNothing(t *testing.T) {
	var marks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "implement", "shouldRun": true,
				"issue": map[string]any{"repoFullName": "acme/widgets", "number": 7, "title": "t", "url": "https://example.com/7"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "intake negotiation failed") {
		t.Fatalf("err = %v, want the negotiation failure", err)
	}
	if marks != 0 {
		t.Fatalf("release attempts = %d, want none without a valid receipt", marks)
	}
}

// Old Dispatch with an ordinary (non-rebase) connector keeps working exactly
// as before: no intake declaration is sent at all, ordinary followups still
// map to fix-pr, and REBASE work never arrives to be mishandled.
func TestOrdinaryDiscoveryUnchangedWithoutRebaseCapability(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("capabilities") || r.URL.Query().Has("work_scope") || r.URL.Query().Has("work_scope_version") {
			t.Fatalf("ordinary connector sent the intake declaration: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "followup-pr", "shouldRun": true,
			"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
			"prFixItem":   map[string]any{"id": "fix-3", "generation": 5, "originalClaimant": "local"},
		})
	}))
	defer server.Close()
	items, err := newTestClient(t, server.URL, false).Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Mode != "fix-pr" {
		t.Fatalf("items = %#v, want one fix-pr item", items)
	}
}

// An idle response from a deployment that cannot answer the intake
// declaration is idle — never successful negotiation — and does not fail the
// poll, because there is no work to decline.
func TestRebaseDiscoveryIdleIsNotNegotiation(t *testing.T) {
	var polls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		polls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "idle", "shouldRun": false})
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	items, err := client.Discover(context.Background())
	if err != nil {
		t.Fatalf("idle poll failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("items = %#v, want none", items)
	}
	if polls != 1 {
		t.Fatalf("polls = %d", polls)
	}
}

// A rebase task whose receipt is malformed is a protocol error even under a
// negotiated intake: the receipt is the settlement contract and an attempt
// without one can never be settled truthfully.
func TestRebaseDiscoveryRejectsInvalidReceiptUnderNegotiation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "followup-pr", "shouldRun": true,
			"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
			"prFixItem":   map[string]any{"id": "", "generation": 0, "fixType": "REBASE"},
			"intake":      rebaseIntakeAckBody(),
		})
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	_, err := client.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid PR-fix item") {
		t.Fatalf("err = %v, want the receipt protocol error", err)
	}
}

// The safe release is attempted exactly once per generation: a deployment
// that refuses it must not turn every later poll into another release
// attempt, and the refusal preserves the ownership evidence.
func TestRebaseDeclineReleaseIsAttemptedOncePerGeneration(t *testing.T) {
	var marks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
				"prFixItem":   map[string]any{"id": "fix-1", "generation": 3, "handoutToken": "hand-9", "fixType": "REBASE", "originalClaimant": "local"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			// An older deployment that refuses the recoverable wait status.
			http.Error(w, `{"error":"Invalid status. Must be one of: BLOCKED, STALE"}`, http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, false)
	for i := 0; i < 3; i++ {
		if _, err := client.Discover(context.Background()); err == nil {
			t.Fatalf("poll %d: decline did not surface", i)
		}
	}
	if marks != 1 {
		t.Fatalf("release attempts = %d, want exactly one per generation", marks)
	}
}

// A successful decline release surfaces the decline once and does not
// re-release on later polls of the same generation.
func TestRebaseDeclineReleaseSucceedsOnce(t *testing.T) {
	var marks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42},
				"prFixItem":   map[string]any{"id": "fix-1", "generation": 3, "fixType": "REBASE", "originalClaimant": "local"},
			})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, false)
	for i := 0; i < 2; i++ {
		if _, err := client.Discover(context.Background()); err == nil {
			t.Fatalf("poll %d: decline did not surface", i)
		}
	}
	if marks != 1 {
		t.Fatalf("release attempts = %d, want one successful release", marks)
	}
}

// Settlement of a rebase attempt speaks the same task report contract as any
// queue-backed followup: taskType followup-pr, the outcome, and the exact
// receipt with the handout token, so Dispatch's generation and handout
// contracts are preserved verbatim (#715).
func TestRebaseOutcomeReportPreservesReceipt(t *testing.T) {
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/tasks/report") {
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, true)
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42, URL: "https://github.com/acme/widgets/pull/42"},
		PRFixItem:   &PRFixItem{ID: "fix-1", Generation: 3, HandoutToken: "hand-9", FixType: "REBASE"},
	})
	if err := client.Report(context.Background(), id, source.Lifecycle{Result: source.ResultReady, PR: "42"}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 {
		t.Fatalf("reports = %#v", bodies)
	}
	body := bodies[0]
	if body["taskType"] != "followup-pr" || body["outcome"] != "pr_updated" {
		t.Fatalf("report = %#v", body)
	}
	item, ok := body["prFixItem"].(map[string]any)
	if !ok || item["id"] != "fix-1" || item["generation"] != float64(3) || item["handoutToken"] != "hand-9" {
		t.Fatalf("prFixItem = %#v, want the exact receipt", body["prFixItem"])
	}
}
