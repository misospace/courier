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
	client, err := newTestDispatchClient(serverURL, "worker", lane, "token", time.Second)
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
	var lastItem source.WorkItem
	for _, claimant := range claimants {
		items, err := client.Discover(context.Background())
		if err != nil {
			t.Fatalf("claimant %q: %v", claimant, err)
		}
		if len(items) != 1 || items[0].Mode != "fix-pr" || items[0].Repo != "acme/widgets" || items[0].Ref != 42 {
			t.Fatalf("claimant %q: items = %#v", claimant, items)
		}
		lastItem = items[0]
	}
	if len(*marks) != 0 {
		t.Fatalf("marks = %#v, want none for accepted handouts", *marks)
	}
	descriptor, err := decodeWorkID(lastItem.ID)
	if err != nil {
		t.Fatalf("decode work ID: %v", err)
	}
	if descriptor.OriginalClaimant != "cloud" {
		t.Fatalf("attempt claimant = %q, want the evidence carried into the durable attempt identity", descriptor.OriginalClaimant)
	}
}

// A claimant bound to a different queue lane is declined: the exact receipt
// is released once through the recoverable waiting-external mark, nothing is
// materialized, and the poll itself stays clean so the lane keeps discovering.
func TestOwnershipGateDeclinesCrossLaneClaimantOnNormalClient(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("frontier") })
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "frontier": "frontier"})
	items, err := client.Discover(context.Background())
	if items != nil || err != nil {
		t.Fatalf("items = %#v err = %v, want the handout declined without failing the poll", items, err)
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
	if items != nil || err != nil {
		t.Fatalf("items = %#v err = %v, want the handout declined without failing the poll", items, err)
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
			if items != nil || err != nil {
				t.Fatalf("items = %#v err = %v, want the handout declined without failing the poll", items, err)
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
		if items != nil || err != nil {
			t.Fatalf("items = %#v err = %v, want the handout declined without failing the poll", items, err)
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
// nor evidence: declined without acceptance and without any release call,
// and the poll stays clean.
func TestOwnershipGateDeclinesReceiptlessHandoutWithoutRelease(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any {
		return map[string]any{
			"type": "followup-pr", "shouldRun": true,
			"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 42, "state": "open"},
		}
	})
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal"})
	items, err := client.Discover(context.Background())
	if items != nil || err != nil {
		t.Fatalf("items = %#v err = %v, want the handout declined without failing the poll", items, err)
	}
	if len(*marks) != 0 {
		t.Fatalf("marks = %#v, want no release call without a receipt", *marks)
	}
}

// The decline latches per generation: after one settled decline, a later poll
// of the same item makes no further release call and still materializes
// nothing.
func TestOwnershipGateDeclineLatchesPerGeneration(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("frontier") })
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "frontier": "frontier"})
	counter := attachOwnershipDiagnostics(t, client)
	for i := 0; i < 2; i++ {
		items, err := client.Discover(context.Background())
		if items != nil || err != nil {
			t.Fatalf("poll %d: items = %#v err = %v, want the handout declined without failing the poll", i, items, err)
		}
	}
	if got := counter(reasonOwnershipLaneMismatch); got != 2 {
		t.Fatalf("diagnostic %s = %v, want one per poll while the release stays latched", reasonOwnershipLaneMismatch, got)
	}
	if len(*marks) != 1 {
		t.Fatalf("marks = %#v, want exactly one release per generation", *marks)
	}
}

// A release the deployment refuses still settles the decline: the mark call
// is attempted once, latched for the generation, and the poll stays clean so
// the lane keeps discovering while the source's stale-handout reclaimer
// remains the bounded recovery path.
func TestOwnershipGateDeclineSurvivesRefusedRelease(t *testing.T) {
	var marks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/next-task"):
			_ = json.NewEncoder(w).Encode(ownershipTaskBody("frontier"))
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "frontier": "frontier"})
	counter := attachOwnershipDiagnostics(t, client)
	for i := 0; i < 2; i++ {
		items, err := client.Discover(context.Background())
		if items != nil || err != nil {
			t.Fatalf("poll %d: items = %#v err = %v, want the handout declined without failing the poll", i, items, err)
		}
	}
	if got := counter(reasonOwnershipLaneMismatch); got != 2 {
		t.Fatalf("diagnostic %s = %v, want one per poll", reasonOwnershipLaneMismatch, got)
	}
	if marks != 1 {
		t.Fatalf("marks = %d, want exactly one refused release attempt per generation", marks)
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
	if items != nil || err != nil {
		t.Fatalf("items = %#v err = %v, want the handout declined without failing the poll", items, err)
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
	if items != nil || err != nil {
		t.Fatalf("items = %#v err = %v, want the handout declined without failing the poll", items, err)
	}
	if len(*marks) != 1 || (*marks)[0]["status"] != "WAITING_EXTERNAL" || (*marks)[0]["generation"] != float64(4) {
		t.Fatalf("marks = %#v, want exactly one exact-receipt release", *marks)
	}
}

// Ownership can change between discovery and admission: the claim consults
// the configured bindings again, so a handout that was authorized when it was
// discovered is refused when the attempt is admitted. The refusal releases the
// reservation with the exact receipt and is terminal for the run, so the core
// drops the attempt instead of re-admitting it into the same refusal.
func TestOwnershipGateRechecksOwnershipAtClaimBoundary(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("cloud") })
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"cloud": "normal"})
	items, err := client.Discover(context.Background())
	if err != nil || len(items) != 1 {
		t.Fatalf("Discover() items = %#v err = %v, want the authorized handout", items, err)
	}
	// The deployment's binding map changes before the attempt is admitted.
	client.WithOwnershipBindings(map[string]string{"cloud": "frontier"})
	_, err = client.ClaimWithReceipt(context.Background(), items[0].ID, items[0].Context)
	if !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("ClaimWithReceipt() error = %v, want the ownership decline", err)
	}
	if !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("ClaimWithReceipt() error = %v, want a refusal that is terminal for the run", err)
	}
	if len(*marks) != 1 {
		t.Fatalf("marks = %#v, want exactly one release", *marks)
	}
	if mark := (*marks)[0]; mark["status"] != "WAITING_EXTERNAL" || mark["repo"] != "acme/widgets" || mark["pr"] != float64(42) || mark["generation"] != float64(4) {
		t.Fatalf("release mark = %#v, want the exact receipt", mark)
	}
}

// An attempt materialized before the gate existed carries no ownership
// evidence. The pre-launch revalidation refuses it before any coordinator
// starts, releases the exact receipt, and never marks the item stale: the
// work stays discoverable for the lane that owns it.
func TestOwnershipGateRejectsEvidenceLessAttemptAtPreLaunch(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("cloud") })
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"cloud": "normal"})
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
		PRFixItem:   &PRFixItem{ID: "fix-9", Generation: 4, HandoutToken: "hand-4"},
	})
	err := client.PreLaunch(context.Background(), id)
	if !errors.Is(err, ErrOwnershipDeclined) || !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("PreLaunch() error = %v, want the terminal ownership refusal", err)
	}
	if len(*marks) != 1 {
		t.Fatalf("marks = %#v, want exactly one release", *marks)
	}
	if mark := (*marks)[0]; mark["status"] != "WAITING_EXTERNAL" || mark["generation"] != float64(4) {
		t.Fatalf("release mark = %#v, want the exact receipt and no stale mark", mark)
	}
}

// Ownership that changed after discovery is refused at the pre-launch
// revalidation too: the attempt was authorized when it was offered and is not
// authorized any more when a coordinator would start.
func TestOwnershipGateRechecksOwnershipAtPreLaunch(t *testing.T) {
	server, marks := ownershipServer(t, func() map[string]any { return ownershipTaskBody("cloud") })
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"cloud": "normal"})
	items, err := client.Discover(context.Background())
	if err != nil || len(items) != 1 {
		t.Fatalf("Discover() items = %#v err = %v, want the authorized handout", items, err)
	}
	client.WithOwnershipBindings(map[string]string{"cloud": "frontier"})
	if err := client.PreLaunch(context.Background(), items[0].ID); !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("PreLaunch() error = %v, want the ownership decline", err)
	}
	if len(*marks) != 1 || (*marks)[0]["status"] != "WAITING_EXTERNAL" {
		t.Fatalf("marks = %#v, want exactly one waiting-external release", *marks)
	}
}

// The hand-out acknowledgement is a boundary too: it tells Dispatch this
// agent owns the attempt, so an attempt the configured bindings do not
// authorize is never acknowledged. The refusal does not release the
// reservation: the attempt may already be executing, and the claim and
// pre-launch boundaries release the exact receipt for every attempt this
// connector refuses to execute.
func TestOwnershipGateRejectsForeignAttemptAtHandoutAcknowledgement(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "frontier": "frontier"})
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
		PRFixItem:   &PRFixItem{ID: "fix-9", Generation: 4, HandoutToken: "hand-4", OriginalClaimant: "frontier"},
	})
	err := client.AckHandout(context.Background(), source.WorkItem{ID: id})
	if !errors.Is(err, ErrOwnershipDeclined) {
		t.Fatalf("AckHandout() error = %v, want the ownership decline", err)
	}
	if !strings.Contains(err.Error(), "receipt fix-9@4") {
		t.Fatalf("AckHandout() error = %v, want the exact receipt evidence", err)
	}
	if len(paths) != 0 {
		t.Fatalf("paths = %#v, want no acknowledgement and no release of a possibly live attempt", paths)
	}
}

// Cross-agent repair inside one lane survives every boundary: a handout
// claimed through another binding of the same lane is accepted at the claim,
// at the hand-out acknowledgement, and at the pre-launch revalidation, and no
// reservation is released for it.
func TestOwnershipGateAcceptsSharedLaneClaimantThroughTheLifecycle(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := newOwnershipClient(t, server.URL, "normal", map[string]string{"local": "normal", "cloud": "normal"})
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
		PRFixItem:   &PRFixItem{ID: "fix-9", Generation: 4, HandoutToken: "hand-4", OriginalClaimant: "cloud"},
	})
	if _, err := client.ClaimWithReceipt(context.Background(), id, ""); err != nil {
		t.Fatalf("ClaimWithReceipt() error = %v, want the shared-lane acceptance", err)
	}
	if err := client.AckHandout(context.Background(), source.WorkItem{ID: id}); err != nil {
		t.Fatalf("AckHandout() error = %v, want the shared-lane acceptance", err)
	}
	if err := client.PreLaunch(context.Background(), id); err != nil {
		t.Fatalf("PreLaunch() error = %v, want the shared-lane acceptance", err)
	}
	for _, path := range paths {
		if path == "/api/pr-fix-queue/mark" {
			t.Fatalf("paths = %#v, want no release for an authorized attempt", paths)
		}
	}
}
