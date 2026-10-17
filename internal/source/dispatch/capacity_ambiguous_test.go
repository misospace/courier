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

func TestIssueCapacityHandoffLostResponseWithoutAuditPreservesReplacement(t *testing.T) {
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-a", Repo: "acme/widgets", Number: 42}})
	owner, token := "worker-a", "issued-a"
	posts, claimAttempts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/audit":
			_ = json.NewEncoder(w).Encode([]any{})
		case "/api/issues/claim":
			// #388 settlement adoption re-presents the claim on a rotated-fence
			// refusal. Real Dispatch fences this here: worker-a may not claim
			// over worker-b's active lease, so adoption fails and settlement
			// falls back to the original stale refusal.
			claimAttempts++
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Issue is actively leased by worker-b. Use force=true override."})
		case "/api/issues/capacity-handoff":
			posts++
			var body struct {
				Token string `json:"claimToken"`
				Agent string `json:"agentName"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			// Model the source's exact-owner fence, including a replacement claiming
			// immediately after the original release commits but its response is lost.
			if body.Token != token || body.Agent != owner {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"code": "stale_claim_token"})
				return
			}
			owner, token = "worker-b", "issued-b"
			panic(http.ErrAbortHandler)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	handoff := source.CapacityHandoff{FenceToken: "issued-a", WorkIdentity: WorkIdentity(id), CapacityIdentity: "capacity-a", IdempotencyKey: "attempt-a"}
	receipt := `{"claimToken":"issued-a"}`
	c, err := NewClient(server.URL, "worker-a", "auth", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SettleCapacityExhaustion(context.Background(), id, receipt, handoff); err == nil {
		t.Fatal("ambiguous release was falsely confirmed")
	}
	// Reconstruct the client as after a controller restart. The consumed receipt
	// must not release the replacement, nor be misrepresented as proven success.
	c, err = NewClient(server.URL, "worker-a", "auth", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SettleCapacityExhaustion(context.Background(), id, receipt, handoff); !errors.Is(err, source.ErrStaleCapacitySettlement) {
		t.Fatalf("retry = %v, want conservative stale hold", err)
	}
	if owner != "worker-b" || token != "issued-b" || posts != 2 {
		t.Fatalf("owner=%s token=%s posts=%d", owner, token, posts)
	}
}
