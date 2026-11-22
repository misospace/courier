package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/forge/forgetest"
)

type forgeCallReader struct{}

func (forgeCallReader) WorkItem(_ context.Context, repo, id string) (forge.WorkItem, error) {
	return forge.WorkItem{ID: id, Repo: repo}, nil
}
func (forgeCallReader) ReadPullRequest(_ context.Context, repo string, number int) (forge.PullRequest, error) {
	return forge.PullRequest{Repo: repo, Number: number}, nil
}
func (forgeCallReader) Reviews(context.Context, string, int) ([]forge.Review, error) {
	return nil, nil
}
func (forgeCallReader) Comments(context.Context, string, int) ([]forge.Comment, error) {
	return nil, nil
}
func (forgeCallReader) Checks(context.Context, string, int, string) ([]forge.Check, error) {
	return nil, nil
}

func TestForgeCallRoute(t *testing.T) {
	s, auth := serverForTest(t)
	repository := forge.Repository{ID: "repo-id", Canonical: "org/repo", DefaultRef: "main"}
	pull := forge.PullRequest{Number: 3, Repo: repository.Canonical}
	provider := forgetest.New(forgetest.Config{
		Provider:     forge.ProviderConfig{Name: "stub"},
		Repositories: []forge.Repository{repository},
		WorkItems: map[forge.WorkItemRef]forge.WorkItem{
			{Repo: repository.Canonical, ID: "1"}: {ID: "1", Repo: repository.Canonical, Title: "item"},
		},
		PullRequests: []forge.PullRequest{pull},
		Reviews:      map[forge.PullRequestRef][]forge.Review{{Repo: repository.Canonical, Number: 3}: {{ID: "review"}}},
		Comments:     map[forge.PullRequestRef][]forge.Comment{{Repo: repository.Canonical, Number: 3}: {{ID: "comment"}}},
		Checks:       map[forge.PullRequestRef][]forge.Check{{Repo: repository.Canonical, Number: 3}: {{Name: "ci"}}},
	})
	policy := Policy{RunUID: "uid", Mode: ModeResolveIssue, Provider: "stub", BaseRepo: repository.Canonical, BaseRef: "main", WorkRepo: repository.Canonical, WorkRef: "branch", SourceIssue: SourceIssue{Owner: "org", Name: "repo", Number: 1}}
	adapter, err := NewForgeAdapter(policy, provider, provider, repository, repository)
	if err != nil {
		t.Fatal(err)
	}
	s.forgeReader = adapter
	tests := []struct {
		name string
		body string
		want int
	}{
		{"unauthenticated", `{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`, http.StatusUnauthorized},
		{"unknown operation", `{"operation":"raw","arguments":{"repo":"org/repo"}}`, http.StatusBadRequest},
		{"write operation", `{"operation":"create-pull-request","arguments":{"repo":"org/repo"}}`, http.StatusBadRequest},
		{"missing repo", `{"operation":"read-work-item","arguments":{"id":"1"}}`, http.StatusBadRequest},
		{"non-decimal work item id", `{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"abc"}}`, http.StatusBadRequest},
		{"query string", `{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`, http.StatusBadRequest},
		{"negative number", `{"operation":"read-pull-request","arguments":{"repo":"org/repo","number":-1}}`, http.StatusBadRequest},
		{"wrong number type", `{"operation":"read-pull-request","arguments":{"repo":"org/repo","number":"1"}}`, http.StatusBadRequest},
		{"extra argument", `{"operation":"read-pull-request","arguments":{"repo":"org/repo","number":1,"extra":true}}`, http.StatusBadRequest},
		{"cross repository", `{"operation":"read-work-item","arguments":{"repo":"other/repo","id":"1"}}`, http.StatusUnprocessableEntity},
		{"work item", `{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`, http.StatusOK},
		{"pull request", `{"operation":"read-pull-request","arguments":{"repo":"org/repo","number":3}}`, http.StatusOK},
		{"reviews", `{"operation":"list-reviews","arguments":{"repo":"org/repo","number":3}}`, http.StatusOK},
		{"comments", `{"operation":"list-comments","arguments":{"repo":"org/repo","number":3}}`, http.StatusOK},
		{"malformed headSHA", `{"operation":"read-checks","arguments":{"repo":"org/repo","number":3,"headSHA":"sha"}}`, http.StatusBadRequest},
		{"checks", `{"operation":"read-checks","arguments":{"repo":"org/repo","number":3,"headSHA":"0123456789012345678901234567890123456789"}}`, http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := "Bearer valid-token"
			target := PathForgeCall
			if test.name == "unauthenticated" {
				header = ""
			}
			if test.name == "query string" {
				target = PathForgeCall + "?unexpected=1"
			}
			req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(test.body))
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if rec.Code != test.want {
				t.Fatalf("status = %d, want %d; body %q", rec.Code, test.want, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "secret") {
				t.Fatalf("response leaked provider details: %q", rec.Body.String())
			}
		})
	}
	if auth.calls == 0 {
		t.Fatal("authenticator was not consulted")
	}
	for _, op := range provider.Ops() {
		if op.Kind == forgetest.OpCreatePullRequest || op.Kind == forgetest.OpUpdatePullRequest || op.Kind == forgetest.OpCommentPullRequest || op.Kind == forgetest.OpCommentWorkItem {
			t.Fatalf("forge call route attempted a write: %+v", op)
		}
	}
}

type transientForgeError struct{}

func (transientForgeError) Error() string               { return "forge unavailable" }
func (transientForgeError) TransientAvailability() bool { return true }

type failingForgeReader struct {
	forgeCallReader
	err error
}

func (r failingForgeReader) WorkItem(context.Context, string, string) (forge.WorkItem, error) {
	return forge.WorkItem{}, r.err
}

// TestForgeCallAvailabilityClassification pins the retryability contract: a
// transient provider failure means the read never happened and answers
// retryable (503), while a completed-but-failed read is a definite denial
// (422). The harness client classifies 5xx as retryable and 422 as definite.
func TestForgeCallAvailabilityClassification(t *testing.T) {
	s, _ := serverForTest(t)
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"transient provider failure", transientForgeError{}, http.StatusServiceUnavailable},
		{"definite read failure", errors.New("boom"), http.StatusUnprocessableEntity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s.forgeReader = failingForgeReader{err: tc.err}
			req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(`{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`))
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d; body %q", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

type oversizeForgeReader struct {
	forgeCallReader
}

func (oversizeForgeReader) WorkItem(context.Context, string, string) (forge.WorkItem, error) {
	return forge.WorkItem{ID: "1", Repo: "org/repo", Title: strings.Repeat("x", maxForgeCallResponseBytes+1)}, nil
}

// TestForgeCallOversizeResponseBoundsEarly pins the response bound: a read
// whose answer outgrows the bound answers a clean 500 with no partial JSON
// body, never a truncated 200.
func TestForgeCallOversizeResponseBoundsEarly(t *testing.T) {
	s, _ := serverForTest(t)
	s.forgeReader = oversizeForgeReader{}
	req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(`{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`))
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if rec.Body.Len() > 1<<10 {
		t.Fatalf("oversize denial leaked a large body: %d bytes", rec.Body.Len())
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err == nil {
		t.Fatalf("oversize denial unexpectedly returned JSON: %v", result)
	}
}

type capturingForgeReader struct {
	forgeCallReader
	id string
}

func (r *capturingForgeReader) WorkItem(_ context.Context, repo, id string) (forge.WorkItem, error) {
	r.id = id
	return forge.WorkItem{ID: id, Repo: repo}, nil
}

// TestForgeCallCanonicalizesWorkItemID pins that a syntactically valid but
// non-canonical model-supplied id ("007", "+5") is forwarded to the reader in
// canonical decimal form, never as the raw string.
func TestForgeCallCanonicalizesWorkItemID(t *testing.T) {
	s, _ := serverForTest(t)
	reader := &capturingForgeReader{}
	s.forgeReader = reader
	cases := []struct{ raw, want string }{{"007", "7"}, {"+5", "5"}, {"42", "42"}}
	for _, tc := range cases {
		body := `{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"` + tc.raw + `"}}`
		req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("id %q: status = %d, want 200; body %q", tc.raw, rec.Code, rec.Body.String())
		}
		if reader.id != tc.want {
			t.Fatalf("id %q forwarded as %q, want %q", tc.raw, reader.id, tc.want)
		}
	}
	for _, raw := range []string{"0", "-1", " 5", "abc"} {
		reader.id = ""
		body := `{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"` + raw + `"}}`
		req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("id %q: status = %d, want 400; body %q", raw, rec.Code, rec.Body.String())
		}
		if reader.id != "" {
			t.Fatalf("invalid id %q reached the reader as %q", raw, reader.id)
		}
	}
}

func TestForgeCallStrictTopLevelDecode(t *testing.T) {
	s, _ := serverForTest(t)
	req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(`{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"},"extra":true}`))
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err == nil {
		t.Fatalf("malformed denial unexpectedly returned JSON: %v", result)
	}
}
