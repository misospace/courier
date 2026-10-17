package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/misospace/courier/internal/source"
)

func TestCapacityHandoffUsesIssuedReceiptAndChecksResolution(t *testing.T) {
	for _, action := range []string{"deferred", "skipped", ""} {
		t.Run(action, func(t *testing.T) {
			id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "fix-a", Generation: 3}})
			receipt, _ := json.Marshal(dispatchContext{PRFixID: "fix-a", Generation: 3, HandoutToken: "issued-a"})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/agents/worker-a/tasks/report" {
					t.Errorf("path = %s", r.URL.Path)
				}
				var body struct {
					Outcome  string    `json:"outcome"`
					Capacity string    `json:"capacity"`
					Key      string    `json:"idempotencyKey"`
					Item     PRFixItem `json:"prFixItem"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body.Outcome != "capacity-exhausted" || body.Capacity != "capacity-a" || body.Key != "attempt-a" || body.Item.HandoutToken != "issued-a" || body.Item.Generation != 3 {
					t.Errorf("body = %#v", body)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"prFixResolution": map[string]any{"matched": true, "itemId": "fix-a", "action": action}})
			}))
			defer server.Close()
			c, err := NewClient(server.URL, "worker-a", "auth", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			proof, err := c.PrepareCapacityRecovery(context.Background(), id, string(receipt))
			if err != nil {
				t.Fatal(err)
			}
			err = c.SettleCapacityExhaustion(context.Background(), id, string(receipt), source.CapacityHandoff{FenceToken: proof.FenceToken, WorkIdentity: proof.WorkIdentity, CapacityIdentity: "capacity-a", IdempotencyKey: "attempt-a"})
			if action == "deferred" && err != nil {
				t.Fatal(err)
			}
			if action != "deferred" && !errors.Is(err, source.ErrStaleCapacitySettlement) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestCapacityPreparationRejectsMismatchedGeneration(t *testing.T) {
	c, err := NewClient("http://example.test", "worker-a", "auth", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "fix-a", Generation: 4}})
	receipt, _ := json.Marshal(dispatchContext{PRFixID: "fix-a", Generation: 3, HandoutToken: "old"})
	_, err = c.PrepareCapacityRecovery(context.Background(), id, string(receipt))
	if !errors.Is(err, source.ErrUnsupportedCapacityRecovery) {
		t.Fatalf("err = %v", err)
	}
}

func TestIssueCapacityHandoffLostResponseReplaysFromAudit(t *testing.T) {
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-a", Repo: "acme/widgets", Number: 42}})
	// The post handler and the audit handler run on separate server goroutines,
	// and the lost-response replay overlaps them, so the shared counters and the
	// recorded marker are guarded by a mutex.
	var mu sync.Mutex
	var posts, audits int
	marker := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/issues/capacity-handoff":
			var body struct {
				Note string `json:"note"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			posts++
			first := posts == 1
			marker = body.Note
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			if first {
				panic(http.ErrAbortHandler)
			}
		case "/api/audit":
			mu.Lock()
			audits++
			first := audits == 1
			notes := marker
			mu.Unlock()
			if r.URL.Query().Get("repo") != "acme/widgets" || r.URL.Query().Get("limit") != "100" {
				t.Errorf("query = %v", r.URL)
			}
			if first {
				_ = json.NewEncoder(w).Encode([]map[string]any{})
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{"action": "issue_capacity_handoff", "success": true, "repoFullName": "acme/widgets", "issueNumber": 42, "issueId": "issue-a", "notes": "capacity=capacity-a; cooldownUntil=none; lease lease-a released by claim-token fence; " + notes}})
		default:
			if r.URL.Path == "/api/issues/state" {
				_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-a"})
				return
			}
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c, _ := NewClient(server.URL, "worker-a", "auth", time.Second)
	receipt := `{"claimToken":"issued-a"}`
	handoff := source.CapacityHandoff{FenceToken: "issued-a", WorkIdentity: WorkIdentity(id), CapacityIdentity: "capacity-a", IdempotencyKey: "stable-key"}
	// The first request loses its response after the server records the handoff.
	if err := c.SettleCapacityExhaustion(context.Background(), id, receipt, handoff); err != nil {
		t.Fatal(err)
	}
	// A reconstructed client can settle from the same exact positive receipt without posting again.
	c, _ = NewClient(server.URL, "worker-a", "auth", time.Second)
	if err := c.SettleCapacityExhaustion(context.Background(), id, receipt, handoff); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	gotPosts, gotAudits, gotMarker := posts, audits, marker
	mu.Unlock()
	if gotPosts != 1 || gotAudits != 3 || gotMarker != capacityReceiptMarker("issued-a", "stable-key", WorkIdentity(id)) {
		t.Fatalf("posts=%d audits=%d marker=%q", gotPosts, gotAudits, gotMarker)
	}
}

func TestIssueCapacityHandoffUsesClaimReceipt(t *testing.T) {
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-a", Repo: "acme/widgets", Number: 42}})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/issues/state":
			_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-a", "repoFullName": "acme/widgets", "number": 42})
		case "/api/issues/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"claimToken": "issued-a"})
		case "/api/audit":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/api/issues/capacity-handoff":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["claimToken"] != "issued-a" || body["capacity"] != "capacity-a" || body["agentName"] != "worker-a" {
				t.Errorf("body = %#v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c, err := NewClient(server.URL, "worker-a", "auth", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := c.ClaimWithReceipt(context.Background(), id, "")
	if err != nil {
		t.Fatal(err)
	}
	proof, err := c.PrepareCapacityRecovery(context.Background(), id, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SettleCapacityExhaustion(context.Background(), id, receipt, source.CapacityHandoff{FenceToken: proof.FenceToken, WorkIdentity: proof.WorkIdentity, CapacityIdentity: "capacity-a"}); err != nil {
		t.Fatal(err)
	}
}

// TestIssueCapacityRecoveryResolvesMissingIssueID is the deployed-smoke
// regression (#327): Dispatch's task offer may encode the work ID with only
// repo+number. Recovery must resolve the internal issue ID through the same
// state lookup Claim uses — the claim receipt remains the fence — instead of
// holding the work as unsupported.
func TestIssueCapacityRecoveryResolvesMissingIssueID(t *testing.T) {
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{Repo: "acme/widgets", Number: 42}})
	var handedOff map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/issues/state":
			if r.URL.Query().Get("repo") != "acme/widgets" || r.URL.Query().Get("number") != "42" {
				t.Errorf("state query = %v", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-a", "repoFullName": "acme/widgets", "number": 42})
		case "/api/audit":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/api/issues/capacity-handoff":
			if err := json.NewDecoder(r.Body).Decode(&handedOff); err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c, err := NewClient(server.URL, "worker-a", "auth", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	receipt := `{"claimToken":"issued-a"}`
	proof, err := c.PrepareCapacityRecovery(context.Background(), id, receipt)
	if err != nil {
		t.Fatalf("missing encoded issue ID must not strand recovery: %v", err)
	}
	if proof.FenceToken != "issued-a" || proof.WorkIdentity != WorkIdentity(id) {
		t.Fatalf("proof = %#v", proof)
	}
	if err := c.SettleCapacityExhaustion(context.Background(), id, receipt, source.CapacityHandoff{FenceToken: "issued-a", WorkIdentity: proof.WorkIdentity, CapacityIdentity: "capacity-a"}); err != nil {
		t.Fatal(err)
	}
	if handedOff["issueId"] != "issue-a" || handedOff["claimToken"] != "issued-a" {
		t.Fatalf("handoff body = %#v", handedOff)
	}
}
