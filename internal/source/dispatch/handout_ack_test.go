package dispatch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/misospace/courier/internal/source"
)

func TestHTTPClientAckHandoutPostsGenerationPinnedReceipt(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/pr-fix-queue/ack" {
			t.Fatalf("request = %s %s, want POST /api/pr-fix-queue/ack", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("Authorization = %q", got)
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true,"item":{}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "worker", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "Acme/Widgets", Number: 42},
		PRFixItem:   &PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: "handout-3"},
	})
	if err := client.AckHandout(context.Background(), source.WorkItem{ID: id}); err != nil {
		t.Fatalf("AckHandout: %v", err)
	}
	if body["repo"] != "acme/widgets" || body["pr"] != float64(42) || body["generation"] != float64(3) {
		t.Fatalf("ack body = %#v, want queue-normalized repo/pr/generation", body)
	}
	if body["agentName"] != "worker" {
		t.Fatalf("agentName = %#v, want authenticated agent", body["agentName"])
	}
}

func TestHTTPClientAckHandoutTreatsMovedOnHandOutAsNoOp(t *testing.T) {
	refusals := []struct {
		code   int
		reason string
	}{
		{http.StatusNotFound, ""},
		{http.StatusForbidden, "not-handed"},
		{http.StatusConflict, "generation-mismatch"},
		{http.StatusConflict, "not-queued"},
		{http.StatusConflict, "not-stamped"},
	}
	for _, refusal := range refusals {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := map[string]string{"error": "pr-fix hand-out ack refused"}
			if refusal.reason != "" {
				body["reason"] = refusal.reason
			}
			payload, _ := json.Marshal(body)
			http.Error(w, string(payload), refusal.code)
		}))
		client, err := NewClient(server.URL, "worker", "token", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		id := EncodeWorkID(Task{
			Type:        "followup-pr",
			PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
			PRFixItem:   &PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: "handout-3"},
		})
		if err := client.AckHandout(context.Background(), source.WorkItem{ID: id}); err != nil {
			t.Fatalf("status %d/%s: AckHandout = %v, want settled no-op", refusal.code, refusal.reason, err)
		}
		server.Close()
	}
}

func TestHTTPClientAckHandoutPropagatesUnknownConflictReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"pr fix queue item generation mismatch","reason":"ledger-diverged"}`, http.StatusConflict)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
		PRFixItem:   &PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: "handout-3"},
	})
	if err := client.AckHandout(context.Background(), source.WorkItem{ID: id}); err == nil {
		t.Fatal("AckHandout on unknown 409 reason = nil, want error")
	}
}

func TestHTTPClientAckHandoutRejectsInvalidDescriptor(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
		PRFixItem:   &PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: "handout-3"},
	})
	// Truncating drops the closing delimiter so decodeWorkID fails.
	if err := client.AckHandout(context.Background(), source.WorkItem{ID: id[:len(id)-1]}); err == nil {
		t.Fatal("AckHandout on malformed work ID = nil, want error")
	}
	if calls != 0 {
		t.Fatalf("ack requests = %d, want 0 for malformed IDs", calls)
	}
}

func TestHTTPClientAckHandoutPropagatesServerFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"Failed to acknowledge pr-fix hand-out"}`, http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
		PRFixItem:   &PRFixItem{ID: "fix-42", Generation: 3, HandoutToken: "handout-3"},
	})
	if err := client.AckHandout(context.Background(), source.WorkItem{ID: id}); err == nil {
		t.Fatal("AckHandout on 500 = nil, want error")
	}
}

func TestHTTPClientAckHandoutIgnoresNonHandOutWork(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	implementID := EncodeWorkID(Task{
		Type:  "implement",
		Issue: &Issue{ID: "issue-1", Repo: "acme/widgets", Number: 7},
	})
	linkedID := EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
	})
	for _, id := range []string{implementID, linkedID} {
		if err := client.AckHandout(context.Background(), source.WorkItem{ID: id}); err != nil {
			t.Fatalf("AckHandout on %q: %v", id, err)
		}
	}
	if calls != 0 {
		t.Fatalf("ack requests = %d, want 0 for non-queue-backed work", calls)
	}
}
