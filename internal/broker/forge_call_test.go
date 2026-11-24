package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// TestForgeCallFailureClassification pins the status vocabulary a model-facing
// read answers for each failure kind, and with it the retryability the harness
// client derives from the status: only a failure a retry can change is
// retryable. A transient provider failure, a read cancelled before it
// completed, and a read the route budget cut short all mean the world was
// never observed (503). A policy refusal or a
// completed-but-failed read is definite (422). A provider that never
// implemented the read answers 501, and an answer too large to deliver answers
// 413; neither changes on retry, so neither may retry.
func TestForgeCallFailureClassification(t *testing.T) {
	s, _ := serverForTest(t)
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"transient provider failure", transientForgeError{}, http.StatusServiceUnavailable},
		{"wrapped transient provider failure", fmt.Errorf("listing comments: %w", transientForgeError{}), http.StatusServiceUnavailable},
		{"route budget expired", context.DeadlineExceeded, http.StatusServiceUnavailable},
		{"wrapped route budget expiry", fmt.Errorf("listing comments after 3 pages: %w", context.DeadlineExceeded), http.StatusServiceUnavailable},
		{"read cancelled by client disconnect", context.Canceled, http.StatusServiceUnavailable},
		{"wrapped cancelled read", fmt.Errorf("fetching page 2: %w", context.Canceled), http.StatusServiceUnavailable},
		{"provider lacks the capability", forge.ErrUnsupported, http.StatusNotImplemented},
		{"wrapped missing capability", fmt.Errorf("read checks: %w", forge.ErrUnsupported), http.StatusNotImplemented},
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
			if strings.Contains(rec.Body.String(), "boom") {
				t.Fatalf("failure detail leaked to the model: %q", rec.Body.String())
			}
		})
	}
}

type oversizeForgeReader struct {
	forgeCallReader
	title string
}

func (r oversizeForgeReader) WorkItem(context.Context, string, string) (forge.WorkItem, error) {
	return forge.WorkItem{ID: "1", Repo: "org/repo", Title: r.title}, nil
}

// TestForgeCallResponseBound pins the response bound in both directions: a read
// whose answer outgrows the bound answers a clean, non-retryable 413 with no
// partial JSON body — never a truncated 200 and never a retryable 5xx that a
// permanently too-large read would loop on — while an answer that still fits is
// delivered whole.
func TestForgeCallResponseBound(t *testing.T) {
	cases := []struct {
		name       string
		title      string
		wantStatus int
	}{
		{"over the bound answers 413", strings.Repeat("x", maxForgeCallResponseBytes+1), http.StatusRequestEntityTooLarge},
		{"under the bound is delivered", strings.Repeat("x", 1<<10), http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := serverForTest(t)
			s.forgeReader = oversizeForgeReader{title: tc.title}
			req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(`{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`))
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantStatus == http.StatusOK {
				var item forge.WorkItem
				if err := json.Unmarshal(rec.Body.Bytes(), &item); err != nil || item.Title != tc.title {
					t.Fatalf("fitting answer was not delivered whole: %v", err)
				}
				return
			}
			if rec.Body.Len() > 1<<10 {
				t.Fatalf("oversize denial leaked a large body: %d bytes", rec.Body.Len())
			}
			var result map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err == nil {
				t.Fatalf("oversize denial unexpectedly returned JSON: %v", result)
			}
		})
	}
}

// TestForgeCallEncodeFailureAnswersRetryable pins the branch the oversize split
// leaves behind: a value the encoder cannot marshal at all is a broker fault, so
// it answers a retryable 500 with a fixed body. Only an answer that merely
// outgrows the bound is definite.
func TestForgeCallEncodeFailureAnswersRetryable(t *testing.T) {
	rec := httptest.NewRecorder()
	respondForgeRead(rec, make(chan int), nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) == "" {
		t.Fatal("encode failure answered with no body at all")
	}
}

// TestForgeReadBudgetFitsInsideWriteDeadline pins that the read budget ends
// inside the listener's write deadline with the response reserve left over to
// write the answer, so a read the budget cut short is still answered whole.
func TestForgeReadBudgetFitsInsideWriteDeadline(t *testing.T) {
	if ForgeReadBudget <= 0 {
		t.Fatalf("forge read budget %v must be positive", ForgeReadBudget)
	}
	if reserve := InboundWriteTimeout - ForgeReadBudget; reserve < apiResponseReserve {
		t.Fatalf("forge read budget %v leaves only %v of the %v write deadline for the answer", ForgeReadBudget, reserve, InboundWriteTimeout)
	}
}

// TestForgeCallBoundsReadWithDeadline pins that the route hands the reader a
// deadline rather than the bare request context, so a listing that fans out
// into paginated provider requests is cancelled by the route budget instead of
// running past the listener's write deadline.
func TestForgeCallBoundsReadWithDeadline(t *testing.T) {
	s, _ := serverForTest(t)
	reader := &deadlineForgeReader{}
	s.forgeReader = reader
	req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(`{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`))
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %q", rec.Code, rec.Body.String())
	}
	if !reader.hasDeadline {
		t.Fatal("the read reached the provider without a deadline")
	}
	if remaining := time.Until(reader.deadline); remaining <= 0 || remaining > InboundWriteTimeout {
		t.Fatalf("read deadline leaves %v, which cannot answer inside the %v write deadline", remaining, InboundWriteTimeout)
	}
}

type deadlineForgeReader struct {
	forgeCallReader
	hasDeadline bool
	deadline    time.Time
}

func (r *deadlineForgeReader) WorkItem(ctx context.Context, repo, id string) (forge.WorkItem, error) {
	r.deadline, r.hasDeadline = ctx.Deadline()
	return forge.WorkItem{ID: id, Repo: repo}, nil
}

type capturingForgeReader struct {
	forgeCallReader
	id string
}

func (r *capturingForgeReader) WorkItem(_ context.Context, repo, id string) (forge.WorkItem, error) {
	r.id = id
	return forge.WorkItem{ID: id, Repo: repo}, nil
}

// stallingAuthenticator stands in for a slow identity backend: it answers only
// once its context ends, so a route that reached it without a deadline would
// hang the test instead of failing it.
type stallingAuthenticator struct {
	hasDeadline bool
	deadline    time.Duration
}

func (a *stallingAuthenticator) Authenticate(ctx context.Context, _ string) (Identity, error) {
	deadline, ok := ctx.Deadline()
	a.hasDeadline, a.deadline = ok, time.Until(deadline)
	<-ctx.Done()
	return Identity{}, &IdentityUnavailableError{Err: ctx.Err()}
}

// TestForgeCallSlowAuthenticationRunsInsideRouteBudget pins that
// authentication receives the route's budgeted context, not the bare request
// context: a slow identity backend is cut short by ForgeReadBudget and
// answered with the retryable 503 the unavailability path already promises,
// so it can never consume the response reserve on its own.
func TestForgeCallSlowAuthenticationRunsInsideRouteBudget(t *testing.T) {
	s, _ := serverForTest(t)
	stall := &stallingAuthenticator{}
	s.authenticator = stall
	s.forgeReadBudget = 100 * time.Millisecond
	reader := &capturingForgeReader{}
	s.forgeReader = reader
	req := httptest.NewRequest(http.MethodPost, PathForgeCall, strings.NewReader(`{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}`))
	req.Header.Set("Authorization", "Bearer valid-token")
	// The outer bound is the failure margin: against a route that handed
	// authentication the bare request context, the deadline the test observes
	// is this one — above the route budget — so the assertion fires instead
	// of the test hanging on the defect it pins.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	started := time.Now()
	s.Handler().ServeHTTP(rec, req)
	elapsed := time.Since(started)
	if !stall.hasDeadline {
		t.Fatal("authentication reached the identity backend without a deadline")
	}
	if stall.deadline <= 0 || stall.deadline > s.forgeReadBudget {
		t.Fatalf("authentication was given %v, outside the %v route budget", stall.deadline, s.forgeReadBudget)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("slow authentication took %v, past the compressed %v budget", elapsed, s.forgeReadBudget)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; a backend that never answered is availability, not denial", rec.Code)
	}
	if reader.id != "" {
		t.Fatal("a forge read was dispatched although authentication never succeeded")
	}
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
