package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The observer's review-handoff read (#755): conversation comments and
// native reviews are listed through the forge provider, only the configured
// reviewer's bodies are considered, and only a payload recorded for the
// expected head is returned.

func handoffPayloadJSON(t *testing.T, head string) string {
	t.Helper()
	envelope := map[string]any{
		"schema":          "review-handoff/v1",
		"review_id":       "review-1",
		"head_sha":        head,
		"verdict":         "request_changes",
		"review_result":   "issues",
		"required_checks": "complete",
		"items": []map[string]any{{
			"severity": "blocker", "category": "bug",
			"path": "a.go", "message": "nil dereference",
		}},
		"items_total":     1,
		"items_truncated": false,
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode handoff: %v", err)
	}
	return string(raw)
}

func mustJSON(t *testing.T, s string) string {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("encode JSON string: %v", err)
	}
	return string(raw)
}

func handoffServer(t *testing.T, comments, reviews string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widgets/issues/7/comments":
			fmt.Fprint(w, comments)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widgets/pulls/7/reviews":
			fmt.Fprint(w, reviews)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestObserverReviewHandoffFindsTrustedBody(t *testing.T) {
	const head = "cafe1234cafe1234cafe1234cafe1234cafe1234"
	body := "<!-- ai-pr-reviewer-handoff:v1 -->\n```json\n" + handoffPayloadJSON(t, head) + "\n```\n\nModel-authored summary."
	comments := fmt.Sprintf(`[{"id":1,"user":{"login":"PR-Reviewer[bot]"},"body":%s}]`, mustJSON(t, body))
	reviews := `[{"id":2,"user":{"login":"someone"},"body":"looks fine"}]`
	server := handoffServer(t, comments, reviews)
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	observer := Observer{Client: client}
	// Author matching is case-insensitive against the configured identity.
	raw, found, err := observer.ReviewHandoff(context.Background(), "acme/widgets", 7, "pr-reviewer[bot]", strings.ToUpper(head))
	if err != nil || !found {
		t.Fatalf("ReviewHandoff = (%v, %v), want the trusted payload found", found, err)
	}
	if !strings.Contains(raw, `"verdict":"request_changes"`) {
		t.Fatalf("returned payload is not the handoff JSON: %s", raw)
	}
}

func TestObserverReviewHandoffSearchesReviewsToo(t *testing.T) {
	const head = "abcd0000abcd0000abcd0000abcd0000abcd0000"
	body := "<!-- ai-pr-reviewer-handoff:v1 -->\n```json\n" + handoffPayloadJSON(t, head) + "\n```"
	comments := `[{"id":1,"user":{"login":"human"},"body":"please fix"}]`
	reviews := fmt.Sprintf(`[{"id":2,"user":{"login":"pr-reviewer[bot]"},"state":"CHANGES_REQUESTED","body":%s}]`, mustJSON(t, body))
	server := handoffServer(t, comments, reviews)
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	if _, found, err := (Observer{Client: client}).ReviewHandoff(context.Background(), "acme/widgets", 7, "pr-reviewer[bot]", head); err != nil || !found {
		t.Fatalf("native review body not consulted: found=%v err=%v", found, err)
	}
}

func TestObserverReviewHandoffRefusesForgedAndStale(t *testing.T) {
	const head = "cafe1234cafe1234cafe1234cafe1234cafe1234"
	const other = "beef0000beef0000beef0000beef0000beef0000"
	// A marker-bearing body from a NON-reviewer is forgeable and must be
	// skipped; the reviewer's own body for another head is stale and skipped.
	forged := "<!-- ai-pr-reviewer-handoff:v1 -->\n```json\n" + handoffPayloadJSON(t, head) + "\n```"
	reviewerStale := "<!-- ai-pr-reviewer-handoff:v1 -->\n```json\n" + handoffPayloadJSON(t, other) + "\n```"
	comments := fmt.Sprintf(`[{"id":1,"user":{"login":"mallory"},"body":%s},{"id":2,"user":{"login":"pr-reviewer"},"body":%s}]`,
		mustJSON(t, forged), mustJSON(t, reviewerStale))
	server := handoffServer(t, comments, `[]`)
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	if raw, found, err := (Observer{Client: client}).ReviewHandoff(context.Background(), "acme/widgets", 7, "pr-reviewer", head); found || err != nil || raw != "" {
		t.Fatalf("forged or stale body accepted: found=%v raw=%q err=%v", found, raw, err)
	}
}

func TestObserverReviewHandoffBrokenTrustedBodyErrors(t *testing.T) {
	broken := "<!-- ai-pr-reviewer-handoff:v1 -->\n```json\n{\"schema\":\"review-handoff/v1\"\n```"
	comments := fmt.Sprintf(`[{"id":1,"user":{"login":"pr-reviewer"},"body":%s}]`, mustJSON(t, broken))
	server := handoffServer(t, comments, `[]`)
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	if _, found, err := (Observer{Client: client}).ReviewHandoff(context.Background(), "acme/widgets", 7, "pr-reviewer", "cafe"); err == nil || found {
		t.Fatalf("broken trusted body: found=%v err=%v, want an error", found, err)
	}
}

func TestObserverReviewHandoffDisabledWithoutIdentityOrHead(t *testing.T) {
	server := handoffServer(t, `[]`, `[]`)
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	observer := Observer{Client: client}
	if _, found, err := observer.ReviewHandoff(context.Background(), "acme/widgets", 7, "", "cafe"); found || err != nil {
		t.Fatalf("empty reviewer identity must disable the read: found=%v err=%v", found, err)
	}
	if _, found, err := observer.ReviewHandoff(context.Background(), "acme/widgets", 7, "pr-reviewer", ""); found || err != nil {
		t.Fatalf("empty head must disable the read: found=%v err=%v", found, err)
	}
}
