package forge

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// stubProvider is a minimal, non-GitHub forge provider built for tests. It
// satisfies the whole Provider contract with canned in-memory data and
// declines with ErrUnsupported any capability it was not registered with.
type stubProvider struct {
	cfg          ProviderConfig
	caps         Capabilities
	workItems    map[WorkItemRef]WorkItem
	pullRequests map[PullRequestRef]PullRequest
	reviews      map[PullRequestRef][]Review
	comments     map[PullRequestRef][]Comment
	checks       map[PullRequestRef][]Check
}

// Compile-time proof that the stub satisfies the forge contract.
var _ Provider = (*stubProvider)(nil)

func (s *stubProvider) Config() ProviderConfig { return s.cfg }

func (s *stubProvider) Capabilities() Capabilities { return s.caps }

func (s *stubProvider) ReadWorkItem(ctx context.Context, ref WorkItemRef) (WorkItem, error) {
	if !s.caps.Has(CapabilityReadWorkItem) {
		return WorkItem{}, ErrUnsupported
	}
	item, ok := s.workItems[ref]
	if !ok {
		return WorkItem{}, errors.New("forge: work item not found")
	}
	return item, nil
}

func (s *stubProvider) ReadPullRequest(ctx context.Context, ref PullRequestRef) (PullRequest, error) {
	if !s.caps.Has(CapabilityReadPullRequest) {
		return PullRequest{}, ErrUnsupported
	}
	pr, ok := s.pullRequests[ref]
	if !ok {
		return PullRequest{}, errors.New("forge: pull request not found")
	}
	return pr, nil
}

func (s *stubProvider) ListReviews(ctx context.Context, ref PullRequestRef) ([]Review, error) {
	if !s.caps.Has(CapabilityListReviews) {
		return nil, ErrUnsupported
	}
	return s.reviews[ref], nil
}

func (s *stubProvider) ListComments(ctx context.Context, ref PullRequestRef) ([]Comment, error) {
	if !s.caps.Has(CapabilityListComments) {
		return nil, ErrUnsupported
	}
	return s.comments[ref], nil
}

func (s *stubProvider) ReadChecks(ctx context.Context, ref PullRequestRef, headSHA string) ([]Check, error) {
	if !s.caps.Has(CapabilityReadChecks) {
		return nil, ErrUnsupported
	}
	return s.checks[ref], nil
}

func (s *stubProvider) CreatePullRequest(ctx context.Context, in CreatePullRequestInput) (PullRequest, error) {
	if !s.caps.Has(CapabilityCreatePullRequest) {
		return PullRequest{}, ErrUnsupported
	}
	return PullRequest{
		Number:  1,
		Repo:    in.Repo,
		Title:   in.Title,
		Body:    in.Body,
		Draft:   in.Draft,
		HeadRef: in.Head,
		BaseRef: in.Base,
	}, nil
}

func (s *stubProvider) UpdatePullRequest(ctx context.Context, ref PullRequestRef, in UpdatePullRequestInput) (PullRequest, error) {
	if !s.caps.Has(CapabilityUpdatePullRequest) {
		return PullRequest{}, ErrUnsupported
	}
	pr, ok := s.pullRequests[ref]
	if !ok {
		return PullRequest{}, errors.New("forge: pull request not found")
	}
	if in.Title != "" {
		pr.Title = in.Title
	}
	if in.Body != "" {
		pr.Body = in.Body
	}
	if in.State != "" {
		pr.State = in.State
	}
	if in.Draft {
		pr.Draft = true
	}
	if s.pullRequests != nil {
		s.pullRequests[ref] = pr
	}
	return pr, nil
}

func (s *stubProvider) CommentPullRequest(ctx context.Context, ref PullRequestRef, body string) (Comment, error) {
	if !s.caps.Has(CapabilityComment) {
		return Comment{}, ErrUnsupported
	}
	c := Comment{ID: "c1", Author: "stub", Body: body}
	if s.comments != nil {
		s.comments[ref] = append(s.comments[ref], c)
	}
	return c, nil
}

func TestStubProviderSatisfiesContract(t *testing.T) {
	stub := &stubProvider{
		cfg: ProviderConfig{
			Name:          "stub",
			Endpoint:      "https://stub.example",
			CredentialRef: "broker://stub",
		},
		caps: NewCapabilities(
			CapabilityReadPullRequest,
			CapabilityComment,
		),
		pullRequests: map[PullRequestRef]PullRequest{
			{Repo: "acme/widget", Number: 7}: {
				Number: 7,
				Repo:   "acme/widget",
				Title:  "Fix thing",
			},
		},
	}
	if got := stub.Config(); got.Name != "stub" {
		t.Fatalf("Config().Name = %q, want %q", got.Name, "stub")
	}
	if got := stub.Capabilities().List(); len(got) != 2 {
		t.Fatalf("Capabilities().List() = %v, want 2 entries", got)
	}
	pr, err := stub.ReadPullRequest(context.Background(), PullRequestRef{Repo: "acme/widget", Number: 7})
	if err != nil {
		t.Fatalf("ReadPullRequest: %v", err)
	}
	if pr.Title != "Fix thing" {
		t.Fatalf("ReadPullRequest title = %q, want %q", pr.Title, "Fix thing")
	}
}

func TestCapabilitiesZeroValue(t *testing.T) {
	var zero Capabilities
	if zero.Has(CapabilityReadWorkItem) {
		t.Fatal("zero Capabilities.Has = true, want false")
	}
	a := zero.Available(CapabilityReadWorkItem)
	if a.Available {
		t.Fatal("zero Capabilities.Available().Available = true, want false")
	}
	if a.Detail == "" {
		t.Fatal("zero Capabilities.Available().Detail is empty, want non-empty")
	}
}

func TestNewCapabilities(t *testing.T) {
	c := NewCapabilities(CapabilityReadWorkItem)
	if !c.Has(CapabilityReadWorkItem) {
		t.Fatal("Has = false after NewCapabilities, want true")
	}
	if !c.Available(CapabilityReadWorkItem).Available {
		t.Fatal("Available().Available = false after NewCapabilities, want true")
	}
}

func TestCapabilitiesClone(t *testing.T) {
	original := NewCapabilities(CapabilityReadWorkItem)
	clone := original.Clone()
	if !clone.Has(CapabilityReadWorkItem) {
		t.Fatal("clone.Has = false after Clone, want true")
	}
	clone.Register(CapabilityReadChecks, available())
	if original.Has(CapabilityReadChecks) {
		t.Fatal("original.Has = true after Register on the clone, want false")
	}
}

func TestRegisterRedactsDetail(t *testing.T) {
	var c Capabilities
	c.Register(CapabilityReadChecks,
		unavailable("endpoint https://user:hunter2@internal.example/api is down"))
	a := c.Available(CapabilityReadChecks)
	if a.Available {
		t.Fatal("Available = true, want false")
	}
	if strings.Contains(a.Detail, "hunter2") {
		t.Fatalf("Detail leaks credential: %q", a.Detail)
	}
	if !strings.Contains(a.Detail, "[REDACTED]") {
		t.Fatalf("Detail = %q, want [REDACTED]", a.Detail)
	}
}

func TestRegisterRedactsRawDetail(t *testing.T) {
	var c Capabilities
	c.Register(CapabilityReadChecks,
		Availability{Available: false, Detail: "https://user:hunter2@host/x"})
	a := c.Available(CapabilityReadChecks)
	if a.Available {
		t.Fatal("Available = true, want false")
	}
	if strings.Contains(a.Detail, "hunter2") {
		t.Fatalf("Detail leaks credential: %q", a.Detail)
	}
	if !strings.Contains(a.Detail, "[REDACTED]") {
		t.Fatalf("Detail = %q, want [REDACTED]", a.Detail)
	}
}

func TestCapabilitiesListSorted(t *testing.T) {
	c := NewCapabilities(CapabilityComment, CapabilityReadWorkItem, CapabilityReadChecks)
	got := c.List()
	want := []Capability{CapabilityComment, CapabilityReadChecks, CapabilityReadWorkItem}
	if len(got) != len(want) {
		t.Fatalf("List() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List() = %v, want %v", got, want)
		}
	}
}

func TestRedactDetail(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantExact string
		want      string
		wantNot   string
	}{
		{
			name:    "url userinfo",
			in:      "failed to reach https://user:hunter2@internal.example/api",
			want:    "[REDACTED]",
			wantNot: "hunter2",
		},
		{
			name:    "bearer token",
			in:      "Authorization: bearer abcdef123456",
			want:    "[REDACTED]",
			wantNot: "abcdef123456",
		},
		{
			name:      "safe unchanged",
			in:        "endpoint is down",
			wantExact: "endpoint is down",
		},
		{
			name:      "short bearer text unchanged",
			in:        "missing bearer token",
			wantExact: "missing bearer token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactDetail(tt.in)
			if tt.wantExact != "" && got != tt.wantExact {
				t.Fatalf("RedactDetail(%q) = %q, want %q", tt.in, got, tt.wantExact)
			}
			if tt.want != "" && !strings.Contains(got, tt.want) {
				t.Fatalf("RedactDetail(%q) = %q, want to contain %q", tt.in, got, tt.want)
			}
			if tt.wantNot != "" && strings.Contains(got, tt.wantNot) {
				t.Fatalf("RedactDetail(%q) = %q, must not contain %q", tt.in, got, tt.wantNot)
			}
		})
	}
}

// Unrepresentability is enforced by the interface shape; this test only guards against a future forbidden capability entering the vocabulary.
func TestCapabilityVocabularyExcludesMergeAndRaw(t *testing.T) {
	all := []Capability{
		CapabilityReadWorkItem,
		CapabilityReadPullRequest,
		CapabilityListReviews,
		CapabilityListComments,
		CapabilityReadChecks,
		CapabilityCreatePullRequest,
		CapabilityUpdatePullRequest,
		CapabilityComment,
	}
	forbidden := []string{"merge", "raw", "exec", "passthrough", "do"}
	for _, cap := range all {
		for _, f := range forbidden {
			if string(cap) == f {
				t.Errorf("capability %q is a forbidden verb", cap)
			}
		}
	}
}

func TestStubDeclinesUnregisteredComment(t *testing.T) {
	stub := &stubProvider{
		// CapabilityComment is deliberately not registered.
		caps: NewCapabilities(CapabilityReadPullRequest),
	}
	_, err := stub.CommentPullRequest(context.Background(), PullRequestRef{Repo: "acme/widget", Number: 7}, "hello")
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("CommentPullRequest error = %v, want ErrUnsupported", err)
	}
}
