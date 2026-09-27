package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/forge"
)

func TestProviderImplementsContract(t *testing.T) {
	var _ forge.Provider = NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: "https://api.github.com/", CredentialRef: "secret/gh"}, nil)
	p := NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: "https://api.github.com/", CredentialRef: "secret/gh"}, nil)
	if got := p.Config(); got.Name != "gh" || got.Endpoint != "https://api.github.com/" || got.CredentialRef != "secret/gh" {
		t.Fatalf("Config() = %#v, want the supplied config", got)
	}
	if got := p.Capabilities().Available(forge.CapabilityComment); !got.Available {
		t.Fatalf("CapabilityComment availability = %#v, want available", got)
	}
	if got := p.Capabilities().Available(forge.CapabilityListReviews); got.Available {
		t.Fatalf("CapabilityListReviews availability = %#v, want unavailable", got)
	}
}

func TestProviderCoveredOperations(t *testing.T) {
	var paths []string
	var patchBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/repos/acme/demo/pulls":
			_, _ = io.WriteString(w, `{"number":7,"state":"open","title":"t","body":"b","draft":false,"html_url":"https://github.com/acme/demo/pull/7","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","sha":"base123","repo":{"full_name":"acme/demo"}}}`)
		case r.Method == http.MethodPatch && r.URL.Path == "/repos/acme/demo/pulls/7":
			patchBody, _ = io.ReadAll(r.Body)
			_, _ = io.WriteString(w, `{"number":7,"state":"open","title":"updated","body":"b","draft":false,"html_url":"https://github.com/acme/demo/pull/7","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","sha":"base123","repo":{"full_name":"acme/demo"}}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/repos/acme/demo/issues/7/comments":
			_, _ = io.WriteString(w, `{"id":9,"body":"looks good","html_url":"https://github.com/acme/demo/pull/7#issuecomment-9"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/demo/pulls/7":
			_, _ = io.WriteString(w, `{"number":7,"state":"open","title":"t","body":"b","draft":false,"html_url":"https://github.com/acme/demo/pull/7","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","sha":"base123","repo":{"full_name":"acme/demo"}}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/demo/commits/abc/check-runs":
			_, _ = io.WriteString(w, `{"total_count":1,"check_runs":[{"name":"ci","status":"completed","conclusion":"success","html_url":"https://github.com/acme/demo/check/1"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: server.URL, CredentialRef: "secret/gh"}, client)
	ctx := context.Background()
	ref := forge.PullRequestRef{Repo: "acme/demo", Number: 7}

	created, err := p.CreatePullRequest(ctx, forge.CreatePullRequestInput{Repo: "acme/demo", Title: "t", Head: "work", Base: "main", Body: "b"})
	if err != nil {
		t.Fatalf("CreatePullRequest() error = %v", err)
	}
	if created.Number != 7 || created.HeadRef != "work" || created.Merged {
		t.Fatalf("CreatePullRequest() = %#v, want PR 7 with head ref work and not merged", created)
	}

	got, err := p.ReadPullRequest(ctx, ref)
	if err != nil {
		t.Fatalf("ReadPullRequest() error = %v", err)
	}
	if got.State != "open" || got.Repo != "acme/demo" || got.BaseRepo != "acme/demo" || got.BaseRef != "main" || got.BaseSHA != "base123" || got.HeadRepo != "acme/demo" || got.HeadRef != "work" || got.HeadSHA != "abc" {
		t.Fatalf("ReadPullRequest() = %#v, want full base and head identity", got)
	}

	checks, err := p.ReadChecks(ctx, ref, "abc")
	if err != nil {
		t.Fatalf("ReadChecks() error = %v", err)
	}
	if len(checks) != 1 || checks[0].Name != "ci" || checks[0].Conclusion != "success" {
		t.Fatalf("ReadChecks() = %#v, want one ci check with conclusion success", checks)
	}

	updated, err := p.UpdatePullRequest(ctx, ref, forge.UpdatePullRequestInput{Title: "updated"})
	if err != nil {
		t.Fatalf("UpdatePullRequest() error = %v", err)
	}
	if updated.Title != "updated" {
		t.Fatalf("UpdatePullRequest() = %#v, want title updated", updated)
	}
	// GitHub's PATCH endpoint does not support draft mutation, so the
	// provider must never send a draft field on update; draft is only set
	// at creation time.
	if strings.Contains(string(patchBody), "draft") {
		t.Fatalf("recorded PATCH body %q, must not contain a draft field", patchBody)
	}

	comment, err := p.CommentPullRequest(ctx, ref, "looks good")
	if err != nil {
		t.Fatalf("CommentPullRequest() error = %v", err)
	}
	if comment.Body != "looks good" || comment.ID != "9" {
		t.Fatalf("CommentPullRequest() = %#v, want comment 9 with body looks good", comment)
	}

	for _, path := range paths {
		if strings.Contains(path, "merge") {
			t.Fatalf("recorded request path %q touches a merge endpoint", path)
		}
	}
}

// A credential in an API error must not cross the provider boundary: the
// error string and the typed *APIError fields are sanitized, and the typed
// error itself survives the sanitization.
func TestProviderRedactsCredentialInAPIError(t *testing.T) {
	t.Run("error response message", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"message":"bad credentials: bearer abcdef123456"}`)
		}))
		defer server.Close()

		client, err := NewClient(server.URL, "token")
		if err != nil {
			t.Fatal(err)
		}
		p := NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: server.URL, CredentialRef: "secret/gh"}, client)
		_, err = p.ReadPullRequest(context.Background(), forge.PullRequestRef{Repo: "acme/demo", Number: 7})
		if err == nil {
			t.Fatal("ReadPullRequest() error = nil, want an API error")
		}
		if strings.Contains(err.Error(), "abcdef123456") {
			t.Fatalf("error leaks credential: %q", err)
		}
		if !strings.Contains(err.Error(), "[REDACTED]") {
			t.Fatalf("error = %q, want [REDACTED]", err)
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("errors.As(*APIError) = false, want the typed error preserved: %v", err)
		}
		if apiErr.StatusCode != http.StatusUnauthorized {
			t.Fatalf("APIError.StatusCode = %d, want 401", apiErr.StatusCode)
		}
		if strings.Contains(apiErr.Message, "abcdef123456") || strings.Contains(apiErr.Body, "abcdef123456") {
			t.Fatalf("APIError fields leak credential: message=%q body=%q", apiErr.Message, apiErr.Body)
		}
	})

	t.Run("transport error with URL userinfo", func(t *testing.T) {
		client, err := NewClient("https://user:secrettoken@gh.example.com/", "token", urlDoer{})
		if err != nil {
			t.Fatal(err)
		}
		p := NewProvider(forge.ProviderConfig{Name: "gh"}, client)
		_, err = p.ReadPullRequest(context.Background(), forge.PullRequestRef{Repo: "acme/demo", Number: 7})
		if err == nil {
			t.Fatal("ReadPullRequest() error = nil, want a transport error")
		}
		if strings.Contains(err.Error(), "secrettoken") {
			t.Fatalf("error leaks credential: %q", err)
		}
		if !strings.Contains(err.Error(), "[REDACTED]") {
			t.Fatalf("error = %q, want [REDACTED]", err)
		}
	})
}

// urlDoer fails every request with the request URL in the error, like a real
// transport failure (for example a dial error) does.
type urlDoer struct{}

func (urlDoer) Do(req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Get %q: dial tcp: connection refused", req.URL.String())
}

func TestProviderForkIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/repos/acme/demo/pulls/7" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"number":7,"head":{"ref":"work","sha":"fork123","repo":{"full_name":"other/demo"}},"base":{"ref":"work","sha":"base123","repo":{"full_name":"acme/demo"}}}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: server.URL}, client)
	got, err := p.ReadPullRequest(context.Background(), forge.PullRequestRef{Repo: "acme/demo", Number: 7})
	if err != nil {
		t.Fatal(err)
	}
	if got.Repo != "acme/demo" || got.BaseRepo != "acme/demo" || got.BaseRef != "work" || got.BaseSHA != "base123" || got.HeadRepo != "other/demo" || got.HeadRef != "work" || got.HeadSHA != "fork123" {
		t.Fatalf("fork identity lost across provider boundary: %#v", got)
	}
}

func TestProviderMissingBaseRepository(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"number":7,"head":{"ref":"work","sha":"fork123","repo":{"full_name":"other/demo"}},"base":{"ref":"main","sha":"base123","repo":null}}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: server.URL}, client)
	if _, err := p.ReadPullRequest(context.Background(), forge.PullRequestRef{Repo: "acme/demo", Number: 7}); err == nil || !strings.Contains(err.Error(), "missing the base repository identity") {
		t.Fatalf("ReadPullRequest() error = %v, want missing base repository", err)
	}
}

func TestProviderMissingHeadRepository(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"number":7,"head":{"ref":"work","sha":"fork123","repo":null},"base":{"ref":"main","sha":"base123","repo":{"full_name":"acme/demo"}}}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: server.URL}, client)
	if _, err := p.ReadPullRequest(context.Background(), forge.PullRequestRef{Repo: "acme/demo", Number: 7}); err == nil || !strings.Contains(err.Error(), "missing the head repository identity") {
		t.Fatalf("ReadPullRequest() error = %v, want missing head repository", err)
	}
}

func TestProviderRepositoryPolicyReadsFailClosed(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo", "/repos/Acme/Demo":
			_, _ = io.WriteString(w, `{"id":123,"full_name":"Acme/Demo","default_branch":"main","permissions":{"push":true}}`)
		case "/repos/Acme/Demo/git/ref/heads/main":
			_, _ = io.WriteString(w, `{"ref":"refs/heads/main","object":{"sha":"deadbeef"}}`)
		case "/repos/Acme/Demo/rules/branches/main":
			_, _ = io.WriteString(w, `[{"type":"required_status_checks"}]`)
		case "/repos/Acme/Demo/branches/main/protection":
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "secret")
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(forge.ProviderConfig{}, client)
	ctx := context.Background()
	repository, err := p.ResolveRepository(ctx, "acme/demo")
	if err != nil || repository.ID != "123" || repository.Canonical != "Acme/Demo" || repository.DefaultRef != "main" {
		t.Fatalf("ResolveRepository() = %#v, %v", repository, err)
	}
	ref, err := p.ReadRef(ctx, repository, "main")
	if err != nil || !ref.Exists || ref.OID != "deadbeef" || ref.Ref != "main" {
		t.Fatalf("ReadRef() = %#v, %v (requests %v)", ref, err, paths)
	}
	protection, err := p.ReadEffectiveProtection(ctx, repository, "main")
	if err != nil || !protection.Protected {
		t.Fatalf("ReadEffectiveProtection() = %#v, %v", protection, err)
	}
	writable, err := p.CanWriteRepository(ctx, repository)
	if err != nil || !writable {
		t.Fatalf("CanWriteRepository() = %v, %v", writable, err)
	}
	if len(paths) != 4 {
		t.Fatalf("requests = %v, want repository, ref, rules, and permissions reads", paths)
	}
}

func TestProviderProtectionEstablishesUnprotectedOnlyFromBothAPIs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/rules/branches/main":
			_, _ = io.WriteString(w, `[]`)
		case "/repos/acme/demo/branches/main/protection":
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"message":"Not Found"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(forge.ProviderConfig{}, client)
	_, err = p.ReadEffectiveProtection(context.Background(), forge.Repository{Canonical: "acme/demo"}, "main")
	if err != nil {
		t.Fatalf("ReadEffectiveProtection() error = %v, want authoritative unprotected result", err)
	}
}

func TestProviderUnsupportedOperations(t *testing.T) {
	p := NewProvider(forge.ProviderConfig{Name: "gh"}, nil)
	ctx := context.Background()
	if _, err := p.ReadWorkItem(ctx, forge.WorkItemRef{Repo: "acme/demo", ID: "1"}); !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("ReadWorkItem() error = %v, want ErrUnsupported", err)
	}
	if _, err := p.ListReviews(ctx, forge.PullRequestRef{Repo: "acme/demo", Number: 7}); !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("ListReviews() error = %v, want ErrUnsupported", err)
	}
	if _, err := p.ListComments(ctx, forge.PullRequestRef{Repo: "acme/demo", Number: 7}); !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("ListComments() error = %v, want ErrUnsupported", err)
	}
}
