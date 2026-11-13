package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func TestObserverReviewHandoffRoutesToRegistrationEndpoint(t *testing.T) {
	// The handoff read must follow the registry-selected endpoint like every
	// other observer read: a GitHub Enterprise registration's listings never
	// leave for the public API host.
	transport := &recordingListTransport{}
	client, err := New("token", transport)
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	observer := Observer{Client: client, Registrations: testForgeRegistry(t, "https://ghes.example.test/api/v3")}
	if _, found, err := observer.ReviewHandoff(context.Background(), "acme/demo", 7, "pr-reviewer", "cafe"); err != nil || found {
		t.Fatalf("ReviewHandoff = (found %v, %v), want a routed empty read", found, err)
	}
	if len(transport.requests) != 2 {
		t.Fatalf("recorded %d requests, want the comment and review listings", len(transport.requests))
	}
	for _, req := range transport.requests {
		if req.URL.Host != "ghes.example.test" {
			t.Fatalf("handoff read for %s went to %q, want the registration host", req.URL.Path, req.URL.Host)
		}
		if !strings.HasPrefix(req.URL.Path, "/api/v3/repos/acme/demo") {
			t.Fatalf("handoff read path = %q, want the registration API prefix", req.URL.Path)
		}
	}
}

// recordingListTransport answers every request with an empty JSON array and
// records where each went.
type recordingListTransport struct {
	requests []*http.Request
}

func (t *recordingListTransport) Do(req *http.Request) (*http.Response, error) {
	t.requests = append(t.requests, req.Clone(context.Background()))
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader("[]")),
	}, nil
}

func TestConversationListPageCapBoundsScan(t *testing.T) {
	// An inflated conversation must not turn one launch into an unbounded
	// paged read: the scan stops at the cap and the oversized listing simply
	// yields no handoff rather than a runaway read.
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/acme/widgets/issues/7/comments" {
			pages++
			// Every page is full, so an unbounded scan would never stop.
			items := make([]string, 100)
			for i := range items {
				items[i] = `{"id":1,"user":{"login":"bot"},"body":"x"}`
			}
			fmt.Fprintf(w, "[%s]", strings.Join(items, ","))
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	comments, err := client.ListIssueComments(context.Background(), "acme", "widgets", 7)
	if err != nil {
		t.Fatalf("ListIssueComments: %v", err)
	}
	if pages != conversationListPageCap {
		t.Fatalf("pages fetched = %d, want the cap %d", pages, conversationListPageCap)
	}
	if len(comments) != conversationListPageCap*100 {
		t.Fatalf("listed %d comments, want cap*100", len(comments))
	}
}
