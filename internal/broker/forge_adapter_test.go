package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/misospace/courier/internal/forge"
)

var errMissing = errors.New("missing")

type adapterForge struct {
	cfg     forge.ProviderConfig
	pr      forge.PullRequest
	created int
}

func (f *adapterForge) Config() forge.ProviderConfig { return f.cfg }
func (f *adapterForge) Capabilities() forge.Capabilities {
	return forge.NewCapabilities(forge.CapabilityReadPullRequest, forge.CapabilityCreatePullRequest, forge.CapabilityUpdatePullRequest)
}
func (f *adapterForge) ReadWorkItem(context.Context, forge.WorkItemRef) (forge.WorkItem, error) {
	return forge.WorkItem{}, forge.ErrUnsupported
}
func (f *adapterForge) ReadPullRequest(_ context.Context, r forge.PullRequestRef) (forge.PullRequest, error) {
	if r.Number != f.pr.Number {
		return forge.PullRequest{}, errMissing
	}
	return f.pr, nil
}
func (f *adapterForge) ListReviews(context.Context, forge.PullRequestRef) ([]forge.Review, error) {
	return nil, forge.ErrUnsupported
}
func (f *adapterForge) ListComments(context.Context, forge.PullRequestRef) ([]forge.Comment, error) {
	return nil, forge.ErrUnsupported
}
func (f *adapterForge) ReadChecks(context.Context, forge.PullRequestRef, string) ([]forge.Check, error) {
	return nil, forge.ErrUnsupported
}
func (f *adapterForge) CreatePullRequest(_ context.Context, in forge.CreatePullRequestInput) (forge.PullRequest, error) {
	f.created++
	f.pr = forge.PullRequest{Number: f.created, Repo: in.Repo, BaseRepo: in.Repo, BaseRef: in.Base, BaseSHA: "base-oid", HeadRepo: in.HeadRepo, HeadRef: in.HeadRef, HeadSHA: "work-oid", State: "open", Title: in.Title, Body: in.Body, Draft: in.Draft}
	return f.pr, nil
}
func (f *adapterForge) UpdatePullRequest(context.Context, forge.PullRequestRef, forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	return f.pr, nil
}
func (f *adapterForge) CommentPullRequest(context.Context, forge.PullRequestRef, string) (forge.Comment, error) {
	return forge.Comment{}, forge.ErrUnsupported
}
func (f *adapterForge) ListPullRequestsByHead(_ context.Context, base forge.PullRequestRef, repo, ref string) ([]forge.PullRequest, error) {
	if f.pr.Number > 0 && f.pr.BaseRepo == base.Repo && f.pr.HeadRepo == repo && f.pr.HeadRef == ref {
		return []forge.PullRequest{f.pr}, nil
	}
	return nil, nil
}

type adapterRepos struct{ repos map[string]forge.Repository }

func (r adapterRepos) ResolveRepository(_ context.Context, name string) (forge.Repository, error) {
	got, ok := r.repos[name]
	if !ok {
		return forge.Repository{}, errMissing
	}
	return got, nil
}
func (r adapterRepos) ReadRef(_ context.Context, repo forge.Repository, ref string) (forge.RefState, error) {
	return forge.RefState{Ref: ref, OID: "work-oid", Exists: true}, nil
}
func (r adapterRepos) ReadEffectiveProtection(context.Context, forge.Repository, string) (forge.Protection, error) {
	return forge.Protection{}, nil
}
func (r adapterRepos) CanWriteRepository(context.Context, forge.Repository) (bool, error) {
	return true, nil
}

type adapterPusher struct{}

func (adapterPusher) IsAncestor(context.Context, string, string, string) (bool, error) {
	return true, nil
}
func (adapterPusher) Push(context.Context, string, string, string) error { return nil }

func TestForgeAdapterResolveCreateAndVerifyPullRequest(t *testing.T) {
	ctx := context.Background()
	base := forge.Repository{ID: "1", Canonical: "org/repo", DefaultRef: "main"}
	work := forge.Repository{ID: "1", Canonical: "org/repo", DefaultRef: "main"}
	provider := &adapterForge{cfg: forge.ProviderConfig{Name: "test"}}
	policy := Policy{RunUID: "uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: base.Canonical, BaseRef: "main", BaseOID: "base-oid", WorkRepo: work.Canonical, WorkRef: "courier/issue-1", WorkInitiallyAbsent: false, WorkAnchorOID: "work-oid", SourceIssue: SourceIssue{Owner: "org", Name: "repo", Number: 1}}
	repos := adapterRepos{repos: map[string]forge.Repository{base.Canonical: base}}
	adapter, err := NewForgeAdapter(policy, provider, repos, base, work)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewPolicyEngine(policy, adapter, adapterPusher{})
	if err != nil {
		t.Fatal(err)
	}
	created, err := engine.CreatePullRequest(ctx, "work-oid", "title", "Closes org/repo#1", false)
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if created.Number != 1 || created.BaseRepo != base.Canonical || created.BaseRef != "main" || created.HeadRepo != work.Canonical || created.HeadRef != policy.WorkRef || created.HeadOID != "work-oid" {
		t.Fatalf("created PR identity = %#v", created)
	}
}
