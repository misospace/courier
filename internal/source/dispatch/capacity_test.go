package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

func TestIssueCapacityHandoffUsesClaimReceipt(t *testing.T) {
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-a", Repo: "acme/widgets", Number: 42}})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/issues/state":
			_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-a", "repoFullName": "acme/widgets", "number": 42})
		case "/api/issues/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"claimToken": "issued-a"})
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
