package github

import (
	"context"
	"errors"
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
			_, _ = io.WriteString(w, `{"number":7,"state":"open","title":"t","body":"b","draft":false,"html_url":"https://github.com/acme/demo/pull/7","head":{"ref":"work","sha":"abc"},"base":{"ref":"main"}}`)
		case r.Method == http.MethodPatch && r.URL.Path == "/repos/acme/demo/pulls/7":
			patchBody, _ = io.ReadAll(r.Body)
			_, _ = io.WriteString(w, `{"number":7,"state":"open","title":"updated","body":"b","draft":false,"html_url":"https://github.com/acme/demo/pull/7","head":{"ref":"work","sha":"abc"},"base":{"ref":"main"}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/repos/acme/demo/issues/7/comments":
			_, _ = io.WriteString(w, `{"id":9,"body":"looks good","html_url":"https://github.com/acme/demo/pull/7#issuecomment-9"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/demo/pulls/7":
			_, _ = io.WriteString(w, `{"number":7,"state":"open","title":"t","body":"b","draft":false,"html_url":"https://github.com/acme/demo/pull/7","head":{"ref":"work","sha":"abc"},"base":{"ref":"main"}}`)
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
	if got.State != "open" || got.HeadRef != "work" || got.HeadSHA != "abc" {
		t.Fatalf("ReadPullRequest() = %#v, want open PR with head work at abc", got)
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

	if _, err := p.UpdatePullRequest(ctx, ref, forge.UpdatePullRequestInput{Draft: true}); err != nil {
		t.Fatalf("UpdatePullRequest() draft error = %v", err)
	}
	if !strings.Contains(string(patchBody), `"draft":true`) {
		t.Fatalf("recorded PATCH body %q, want it to contain \"draft\":true", patchBody)
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
