package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
	"github.com/prometheus/client_golang/prometheus"
)

// ownershipTaskBody is the Dispatch-shaped next-task response for one queued
// fix-pr handout on acme/widgets#42, generation 4, with the handout stamped
// and the original claimant as the ownership evidence Dispatch supplies
// (#761). An empty claimant reproduces the older-peer shape that carries no
// evidence at all.
func ownershipTaskBody(claimant string) map[string]any {
	body := map[string]any{
		"type": "followup-pr", "shouldRun": true,
		"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42, "state": "open"},
		"prFixItem":   map[string]any{"id": "fix-9", "generation": 4, "handoutToken": "hand-4", "fixType": "ci-failure"},
	}
	if claimant != "" {
		body["prFixItem"].(map[string]any)["originalClaimant"] = claimant
	}
	return body
}

// ownershipServer answers every next-task poll with the body the served
// callback returns (so a test can vary the handout between polls) and records
// the /api/pr-fix-queue/mark bodies the client's release attempts carry.
func ownershipServer(t *testing.T, served func() map[string]any) (*httptest.Server, *[]map[string]any) {
	t.Helper()
	var marks []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(served())
		case r.URL.Path == "/api/pr-fix-queue/mark":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			marks = append(marks, body)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)
	return server, &marks
}

// newOwnershipClient builds a lane-scoped client over serverURL with the
// configured ownership bindings and an always-open PR state checker.
func newOwnershipClient(t *testing.T, serverURL, lane string, bindings map[string]string) *HTTPClient {
	t.Helper()
	client, err := NewClientWithLane(serverURL, "worker", lane, "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithOwnershipBindings(bindings)
	client.WithPullRequestStateChecker(openPRChecker())
	return client
}

// attachOwnershipDiagnostics wires a fresh diagnostic envelope onto the
// client once — with the connector's own vocabulary registered, the way
// adapter construction does — and returns a reader for one protocol-error
// reason's counter.
func attachOwnershipDiagnostics(t *testing.T, client *HTTPClient) func(reason string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	em.RegisterConnector(defaultConnectorName, reasonVocabulary())
	client.WithConnectorDiagnostics(em, defaultConnectorName)
	return func(reason string) float64 {
		got, ok := counterValue(t, reg, map[string]string{
			"connector": defaultConnectorName, "signal": connector.SignalProtocolError, "reason": reason,
		})
		if !ok {
			return -1
		}
		return got
	}
}

// Cross-agent repair inside one lane is preserved: every claimant bound to
// the receiving connector's own queue lane is accepted, whatever agent
// originally claimed the item.
func TestOwnershipGateAcceptsEveryClaimantInSharedLane(t *testing.T) {
	claimants := []string{"local", "hyper", "cloud"}
	index := 0
	server, marks := ownershipServer(t, func() map[string]any {
		body := ownershipTaskBody(claimants[index])
		index++
		return body
	})
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "hyper": "normal", "cloud": "normal"})
	var lastContext string
	for _, claimant := range claimants {
		items, err := client.Discover(context.Background())
		if err != nil {
			t.Fatalf("claimant %q: %v", claimant, err)
		}
		if len(items) != 1 || items[0].Mode != "fix-pr" || items[0].Repo != "acme/widgets" || items[0].Ref != 42 {
			t.Fatalf("claimant %q: items = %#v", claimant, items)
		}
		lastContext = items[0].Context
	}
	if len(*marks) != 0 {
		t.Fatalf("marks = %#v, want none for accepted handouts", *marks)
	}
	var receipt dispatchContext
	if err := json.Unmarshal([]byte(lastContext), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.OriginalClaimant != "cloud" {
		t.Fatalf("receipt claimant = %q, want the evidence carried into the durable receipt", receipt.OriginalClaimant)
	}
}

// A claimant bound to a different queue lane is declined: the exact receipt
// is released once through the recoverable waiting-external mark, nothing is
// materialized, and the error carries the ownership sentinel with the
// receipt evidence.
func TestOwnershipGateDeclinesCrossLaneClaimantOnNormalClient(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("frontier") })
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "frontier": "frontier"})
	items, err := client.Discover(context.Background())
	if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("items = %#v err = %v, want the ownership decline", items, err)
	}
	if !strings.Contains(err.Error(), "receipt fix-9@4") || !strings.Contains(err.Error(), `handout "hand-4"`) {
		t.Fatalf("err = %v, want the exact receipt evidence", err)
	}
	if len(*marks) != 1 {
		t.Fatalf("marks = %#v, want exactly one release", *marks)
	}
	if mark := (*marks)[0]; mark["status"] != "WAITING_EXTERNAL" || mark["repo"] != "acme/widgets" || mark["pr"] != float64(42) || mark["generation"] != float64(4) {
		t.Fatalf("release mark = %#v, want the exact receipt", mark)
	}
}

// The mirror denial: the frontier lane's connector must not accept a handout
// whose claimant is bound to the normal lane.
func TestOwnershipGateDeclinesCrossLaneClaimantOnFrontierClient(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("local") })
	client := newOwnershipClient(t, server.URL, "frontier", map[string]string{"local": "normal", "frontier": "frontier"})
	items, err := client.Discover(context.Background())
	if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("items = %#v err = %v, want the ownership decline", items, err)
	}
	if len(*marks) != 1 || (*marks)[0]["status"] != "WAITING_EXTERNAL" {
		t.Fatalf("marks = %#v, want exactly one waiting-external release", *marks)
	}
}

// An arbitrary external claimant outside Courier's bindings is unknown
// ownership, whatever its Dispatch lane: declined with the claimant-unknown
// diagnostic, and a client with no bindings at all declines everything.
func TestOwnershipGateDeclinesUnknownClaimant(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bindings map[string]string
		reason   string
	}{
		{name: "external claimant not in bindings", bindings: map[string]string{"local": "normal"}, reason: reasonOwnershipClaimantUnknown},
		{name: "no bindings configured", bindings: nil, reason: reasonOwnershipClaimantUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("pr-reviewer-action") })
			client := newOwnershipClient(t, server.URL, "normal", tc.bindings)
			counter := attachOwnershipDiagnostics(t, client)
			if got := counter(tc.reason); got > 0 {
				t.Fatalf("diagnostic %s = %v before the poll, want none", tc.reason, got)
			}
			items, err := client.Discover(context.Background())
			if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
				t.Fatalf("items = %#v err = %v, want the ownership decline", items, err)
			}
			if got := counter(tc.reason); got != 1 {
				t.Fatalf("diagnostic %s = %v, want exactly one", tc.reason, got)
			}
			if len(*marks) != 1 || (*marks)[0]["status"] != "WAITING_EXTERNAL" {
				t.Fatalf("marks = %#v, want exactly one waiting-external release", *marks)
			}
		})
	}
}

// An older Dispatch peer supplies no original-claimant evidence. The handout
// is declined gracefully: the valid receipt still releases the item back
// through the waiting-external mark so the lane stays available.
func TestOwnershipGateDeclinesMissingEvidence(t *testing.T) {
	for _, claimant := range []string{"", "   "} {
		server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody(claimant) })
		client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal"})
		counter := attachOwnershipDiagnostics(t, client)
		if got := counter(reasonOwnershipEvidenceMissing); got > 0 {
			t.Fatalf("diagnostic %s = %v before the poll, want none", reasonOwnershipEvidenceMissing, got)
		}
		items, err := client.Discover(context.Background())
		if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
			t.Fatalf("items = %#v err = %v, want the ownership decline", items, err)
		}
		if got := counter(reasonOwnershipEvidenceMissing); got != 1 {
			t.Fatalf("diagnostic %s = %v, want exactly one", reasonOwnershipEvidenceMissing, got)
		}
		if len(*marks) != 1 {
			t.Fatalf("marks = %#v, want the graceful waiting-external release", *marks)
		}
		if mark := (*marks)[0]; mark["status"] != "WAITING_EXTERNAL" || mark["generation"] != float64(4) {
			t.Fatalf("release mark = %#v, want the exact receipt", mark)
		}
	}
}

// A followup-pr handout with no PR-fix item at all carries neither receipt
// nor evidence: declined without acceptance and without any release call.
func TestOwnershipGateDeclinesReceiptlessHandoutWithoutRelease(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any {
		return map[string]any{
			"type": "followup-pr", "shouldRun": true,
			"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42, "state": "open"},
		}
	})
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal"})
	items, err := client.Discover(context.Background())
	if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("items = %#v err = %v, want the ownership decline", items, err)
	}
	if !strings.Contains(err.Error(), "followup-pr offer carries no valid PR-fix receipt") {
		t.Fatalf("err = %v, want the receipt-less wording", err)
	}
	if len(*marks) != 0 {
		t.Fatalf("marks = %#v, want no release call without a receipt", *marks)
	}
}

// The decline latches per generation: after one settled decline, a later poll
// of the same item makes no further release call but still surfaces the
// settled ownership decline.
func TestOwnershipGateDeclineLatchesPerGeneration(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("frontier") })
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "frontier": "frontier"})
	for i := 0; i < 2; i++ {
		items, err := client.Discover(context.Background())
		if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
			t.Fatalf("poll %d: items = %#v err = %v, want the ownership decline", i, items, err)
		}
	}
	if len(*marks) != 1 {
		t.Fatalf("marks = %#v, want exactly one release per generation", *marks)
	}
}

// The ownership gate runs before the rebase classification: a REBASE-marked
// handout under a fully negotiated intake is declined by ownership when its
// claimant is foreign, never by the capability contract.
func TestOwnershipGateRunsBeforeRebaseClassification(t *testing.T) {
	var marks []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "followup-pr", "shouldRun": true,
				"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42, "state": "open"},
				"prFixItem":   map[string]any{"id": "fix-1", "generation": 3, "handoutToken": "hand-9", "fixType": "REBASE", "originalClaimant": "frontier"},
				"intake":      rebaseIntakeAckBody(),
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
	counter := attachOwnershipDiagnostics(t, client)
	if got := counter(reasonOwnershipClaimantUnknown); got > 0 {
		t.Fatalf("diagnostic %s = %v before the poll, want none", reasonOwnershipClaimantUnknown, got)
	}
	items, err := client.Discover(context.Background())
	if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("items = %#v err = %v, want the ownership decline", items, err)
	}
	if errors.Is(err, ErrRebaseDeclined) {
		t.Fatalf("err = %v, want an ownership decline, not a rebase decline", err)
	}
	if got := counter(reasonOwnershipClaimantUnknown); got != 1 {
		t.Fatalf("diagnostic %s = %v, want exactly one", reasonOwnershipClaimantUnknown, got)
	}
	if got := counter(reasonRebaseDeclined); got != -1 {
		t.Fatalf("diagnostic %s = %v, want none", reasonRebaseDeclined, got)
	}
	if len(marks) != 1 || marks[0]["status"] != "WAITING_EXTERNAL" || marks[0]["generation"] != float64(3) {
		t.Fatalf("marks = %#v, want exactly one exact-receipt release", marks)
	}
}

// The gate applies on the capacity/exclusion discovery path too: a cross-lane
// claimant riding a capacity poll is declined through the same fenced path.
func TestOwnershipGateAppliesThroughDiscoverForCapacity(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any {
		body := ownershipTaskBody("frontier")
		body["prFixExclusions"] = map[string]any{"version": 1}
		return body
	})
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "frontier": "frontier"})
	client.WithPRFixExclusions(true)
	oldID := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "fix-9", Generation: 4}})
	items, err := client.DiscoverForCapacity(context.Background(), "pool-a", []source.WorkItem{{ID: oldID}})
	if items != nil || !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("items = %#v err = %v, want the ownership decline", items, err)
	}
	if len(*marks) != 1 || (*marks)[0]["status"] != "WAITING_EXTERNAL" || (*marks)[0]["generation"] != float64(4) {
		t.Fatalf("marks = %#v, want exactly one exact-receipt release", *marks)
	}
}
