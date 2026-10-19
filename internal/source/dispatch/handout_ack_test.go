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
		code int
		body string
	}{
		{http.StatusNotFound, `{"error":"pr-fix item not found"}`},
		{http.StatusForbidden, `{"error":"pr-fix hand-out ack refused","reason":"not-handed"}`},
		{http.StatusConflict, `{"error":"pr-fix hand-out ack refused","reason":"generation-mismatch"}`},
		{http.StatusConflict, `{"error":"pr-fix hand-out ack refused","reason":"not-queued"}`},
		{http.StatusConflict, `{"error":"pr-fix hand-out ack refused","reason":"not-stamped"}`},
	}
	for _, refusal := range refusals {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			http.Error(w, refusal.body, refusal.code)
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
			t.Fatalf("status %d body %s: AckHandout = %v, want settled no-op", refusal.code, refusal.body, err)
		}
		server.Close()
	}
}

func TestHTTPClientAckHandoutPropagatesUnexpectedRefusals(t *testing.T) {
	refusals := []struct {
		code int
		body string
	}{
		// Auth/scope failures are configuration problems, not proof the
		// hand-out moved on.
		{http.StatusForbidden, `{"error":"agent scope does not permit this endpoint"}`},
		{http.StatusForbidden, `{"error":"unauthorized","reason":"invalid-auth-mode"}`},
		{http.StatusForbidden, `{"error":"pr-fix hand-out ack refused"}`},
		// A 404 from a missing route or older deployment is not ledger
		// evidence.
		{http.StatusNotFound, `{"error":"Cannot POST /api/pr-fix-queue/ack"}`},
		{http.StatusNotFound, `{"error":"not found"}`},
		{http.StatusNotFound, `{"error":"pr-fix hand-out ack refused"}`},
		{http.StatusNotFound, "route not found"},
	}
	for _, refusal := range refusals {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, refusal.body, refusal.code)
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
		if err := client.AckHandout(context.Background(), source.WorkItem{ID: id}); err == nil {
			t.Fatalf("status %d body %s: AckHandout = nil, want error", refusal.code, refusal.body)
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
