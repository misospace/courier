package forgetest

// This file promotes the local-git-backed forge double that the broker
// integration suite grew into the shared test-support home, so every suite
// that needs a real git-backed forge (the broker's own integration tests and
// the end-to-end regression harness) exercises one implementation instead of
// two drifting copies.
//
// The double is a forge.Provider, a forge.RepositoryPolicyProvider, and a
// forge.PullRequestHeadLister over local git directories, plus the structural
// Endpoint resolver the broker's git transport pins its destinations with.
// It reads live refs out of the local git directory with the real git
// binary, so a publication that claims a tip is checked against the actual
// object store, and it keeps pull requests in memory.
//
// Like every other double in this package it is test support only: no
// production package imports it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/misospace/courier/internal/forge"
)

// LocalGitOpKind names one provider call the double records.
type LocalGitOpKind string

// The recorded call kinds. The vocabulary is the forge contract's own: it
// has no merge and no raw-passthrough entry, so a recorded op can never name
// one.
const (
	LocalGitOpResolveRepository       LocalGitOpKind = "resolve-repository"
	LocalGitOpReadRef                 LocalGitOpKind = "read-ref"
	LocalGitOpReadEffectiveProtection LocalGitOpKind = "read-effective-protection"
	LocalGitOpCanWriteRepository      LocalGitOpKind = "can-write-repository"
	LocalGitOpReadWorkItem            LocalGitOpKind = "read-work-item"
	LocalGitOpReadPullRequest         LocalGitOpKind = "read-pull-request"
	LocalGitOpListReviews             LocalGitOpKind = "list-reviews"
	LocalGitOpListComments            LocalGitOpKind = "list-comments"
	LocalGitOpReadChecks              LocalGitOpKind = "read-checks"
	LocalGitOpListPullRequestsByHead  LocalGitOpKind = "list-pull-requests-by-head"
	LocalGitOpCreatePullRequest       LocalGitOpKind = "create-pull-request"
	LocalGitOpUpdatePullRequest       LocalGitOpKind = "update-pull-request"
	LocalGitOpCommentPullRequest      LocalGitOpKind = "comment-pull-request"
	LocalGitOpCommentWorkItem         LocalGitOpKind = "comment-work-item"
)

// LocalGitOp records one provider call in call order. Only the fields
// matching Kind carry meaning.
type LocalGitOp struct {
	// Kind is the contract method invoked.
	Kind LocalGitOpKind
	// Repo is the repository identity the call named.
	Repo string
	// Ref is the ref the call named, when it named one.
	Ref string
	// PR is the pull-request reference the call named.
	PR forge.PullRequestRef
	// Number is the pull-request number a create or update named.
	Number int
	// HeadRepo and HeadRef are the head identity a head listing named.
	HeadRepo string
	// WorkItem is the work-item reference the call named.
	WorkItem forge.WorkItemRef
}

// LocalGitConfig seeds a LocalGit double.
type LocalGitConfig struct {
	// Provider is the forge-agnostic provider configuration the double
	// reports. Its Name is the identity the broker's policy pins.
	Provider forge.ProviderConfig
	// Repositories maps a canonical repository identity to its identity.
	Repositories map[string]forge.Repository
	// Paths maps a canonical repository identity to the local git directory
	// that holds it. The broker's git transport resolves its pinned
	// destinations through exactly this mapping.
	Paths map[string]string
	// Protected maps "repository:ref" to its effective write protection.
	Protected map[string]bool
	// WorkItems maps "repository/id" to the work item the double serves.
	// A work item the map does not name is unsupported, exactly like the
	// operations this double does not offer.
	WorkItems map[string]forge.WorkItem
	// PullRequests are the pull requests the double knows before any
	// create. A create joins them.
	PullRequests []forge.PullRequest
	// NextPullRequest is the number the first created pull request takes.
	// Zero starts at 10.
	NextPullRequest int
	// RacePullRequest makes a create record a competing pull request for
	// the same head before the requested one, standing in for a concurrent
	// create that the duplicate guard must catch.
	RacePullRequest bool
}

// LocalGit is the local-git-backed forge double. All methods are safe for
// concurrent use.
//
// The world's mutable state is private to the double and reachable only
// through locked accessors, so a caller can never read it without the
// double's lock.
type LocalGit struct {
	mu sync.Mutex

	cfg  forge.ProviderConfig
	caps forge.Capabilities

	// repositories maps a canonical identity to its repository identity.
	repositories map[string]forge.Repository
	// paths maps a canonical identity to its local git directory.
	paths map[string]string
	// protected maps "repository:ref" to its effective write protection.
	protected map[string]bool
	// workItems maps "repository/id" to its work item.
	workItems map[string]forge.WorkItem
	// pullRequests is the known pull-request set; created requests join it.
	pullRequests []forge.PullRequest
	// nextPullRequest is the number the next created request takes.
	nextPullRequest int
	// racePullRequest makes a create record a competing request for the
	// same head before the requested one.
	racePullRequest bool

	ops []LocalGitOp
}

var (
	_ forge.Provider                 = (*LocalGit)(nil)
	_ forge.RepositoryPolicyProvider = (*LocalGit)(nil)
	_ forge.PullRequestHeadLister    = (*LocalGit)(nil)
)

// NewLocalGit builds the double from seeded state. Every seed is copied, so
// the caller may mutate its own maps afterwards without affecting the
// double's view of them.
func NewLocalGit(cfg LocalGitConfig) *LocalGit {
	repositories := make(map[string]forge.Repository, len(cfg.Repositories))
	for name, repo := range cfg.Repositories {
		repositories[name] = repo
	}
	paths := make(map[string]string, len(cfg.Paths))
	for name, path := range cfg.Paths {
		paths[name] = path
	}
	protected := make(map[string]bool, len(cfg.Protected))
	for name, value := range cfg.Protected {
		protected[name] = value
	}
	workItems := make(map[string]forge.WorkItem, len(cfg.WorkItems))
	for name, item := range cfg.WorkItems {
		workItems[name] = item
	}
	pulls := make([]forge.PullRequest, len(cfg.PullRequests))
	for i, pr := range cfg.PullRequests {
		pulls[i] = copyPullRequest(pr)
	}
	next := cfg.NextPullRequest
	if next == 0 {
		next = 10
	}
	return &LocalGit{
		cfg: cfg.Provider,
		caps: forge.NewCapabilities(
			forge.CapabilityReadWorkItem,
			forge.CapabilityReadPullRequest,
			forge.CapabilityListReviews,
			forge.CapabilityListComments,
			forge.CapabilityReadChecks,
			forge.CapabilityCreatePullRequest,
			forge.CapabilityUpdatePullRequest,
			forge.CapabilityComment,
			forge.CapabilityCommentWorkItem,
		),
		repositories:    repositories,
		paths:           paths,
		protected:       protected,
		workItems:       workItems,
		pullRequests:    pulls,
		nextPullRequest: next,
		racePullRequest: cfg.RacePullRequest,
	}
}

// Config returns the seeded provider configuration. It is not recorded.
func (g *LocalGit) Config() forge.ProviderConfig {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cfg
}

// Capabilities returns the registered capabilities. It is not recorded.
func (g *LocalGit) Capabilities() forge.Capabilities {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.caps.Clone()
}

// ResolveRepository resolves a canonical identity (or a repository ID) to its
// repository identity.
func (g *LocalGit) ResolveRepository(_ context.Context, name string) (forge.Repository, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpResolveRepository, Repo: name})
	for _, repo := range g.repositories {
		if repo.ID == name || repo.Canonical == name {
			return repo, nil
		}
	}
	return forge.Repository{}, errors.New("unknown repository")
}

// ReadRef reads the live state of a ref out of the local git directory. An
// absent ref is an ordinary absent state, never an error.
func (g *LocalGit) ReadRef(_ context.Context, repo forge.Repository, ref string) (forge.RefState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpReadRef, Repo: repo.Canonical, Ref: ref})
	oid, ok := g.tipLocked(repo.Canonical, ref)
	if !ok {
		return forge.RefState{Ref: ref}, nil
	}
	return forge.RefState{Ref: ref, OID: oid, Exists: true}, nil
}

// ReadEffectiveProtection reports the seeded protection of a ref.
func (g *LocalGit) ReadEffectiveProtection(_ context.Context, repo forge.Repository, ref string) (forge.Protection, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpReadEffectiveProtection, Repo: repo.Canonical, Ref: ref})
	return forge.Protection{Protected: g.protected[repo.Canonical+":"+ref]}, nil
}

// CanWriteRepository reports that the double's repositories are writable.
func (g *LocalGit) CanWriteRepository(_ context.Context, repo forge.Repository) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpCanWriteRepository, Repo: repo.Canonical})
	return true, nil
}

// ReadWorkItem reads a seeded work item. An unseeded work item is
// unsupported, like every operation the double does not offer.
func (g *LocalGit) ReadWorkItem(_ context.Context, ref forge.WorkItemRef) (forge.WorkItem, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpReadWorkItem, Repo: ref.Repo, WorkItem: ref})
	item, ok := g.workItems[ref.Repo+"/"+ref.ID]
	if !ok {
		return forge.WorkItem{}, forge.ErrUnsupported
	}
	return item, nil
}

// ReadPullRequest reads a known pull request and refreshes its head from the
// live work ref, exactly as a live branch does.
func (g *LocalGit) ReadPullRequest(_ context.Context, ref forge.PullRequestRef) (forge.PullRequest, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpReadPullRequest, Repo: ref.Repo, PR: ref})
	for _, pr := range g.pullRequests {
		if pr.Number == ref.Number && pr.BaseRepo == ref.Repo {
			if tip, ok := g.tipLocked(pr.HeadRepo, pr.HeadRef); ok {
				pr.HeadSHA = tip
			}
			return pr, nil
		}
	}
	return forge.PullRequest{}, errors.New("pull request not found")
}

// ListReviews is unsupported: the double serves no reviews.
func (g *LocalGit) ListReviews(context.Context, forge.PullRequestRef) ([]forge.Review, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpListReviews})
	return nil, forge.ErrUnsupported
}

// ListComments is unsupported: the double serves no comments.
func (g *LocalGit) ListComments(context.Context, forge.PullRequestRef) ([]forge.Comment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpListComments})
	return nil, forge.ErrUnsupported
}

// ReadChecks is unsupported: the double serves no checks.
func (g *LocalGit) ReadChecks(context.Context, forge.PullRequestRef, string) ([]forge.Check, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpReadChecks})
	return nil, forge.ErrUnsupported
}

// CreatePullRequest opens a pull request in the double's in-memory set. The
// base and head SHAs are read from the live local git directories.
//
// When RacePullRequest is set, a competing request for the same head is
// recorded before the requested one: the duplicate guard must catch the
// concurrent create rather than bless it.
func (g *LocalGit) CreatePullRequest(_ context.Context, in forge.CreatePullRequestInput) (forge.PullRequest, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpCreatePullRequest, Repo: in.Repo, Number: g.nextPullRequest})
	if g.racePullRequest {
		g.nextPullRequest++
		other := forge.PullRequest{
			Number:   g.nextPullRequest,
			Repo:     in.Repo,
			BaseRepo: in.Repo,
			BaseRef:  in.Base,
			BaseSHA:  g.tipOrEmpty(in.Repo, in.Base),
			HeadRepo: in.HeadRepo,
			HeadRef:  in.HeadRef,
			HeadSHA:  g.tipOrEmpty(in.HeadRepo, in.HeadRef),
			State:    "open",
			Title:    "concurrent",
		}
		g.pullRequests = append(g.pullRequests, other)
	}
	g.nextPullRequest++
	pr := forge.PullRequest{
		Number:   g.nextPullRequest,
		Repo:     in.Repo,
		BaseRepo: in.Repo,
		BaseRef:  in.Base,
		BaseSHA:  g.tipOrEmpty(in.Repo, in.Base),
		HeadRepo: in.HeadRepo,
		HeadRef:  in.HeadRef,
		HeadSHA:  g.tipOrEmpty(in.HeadRepo, in.HeadRef),
		State:    "open",
		Title:    in.Title,
		Body:     in.Body,
		Draft:    in.Draft,
	}
	g.pullRequests = append(g.pullRequests, pr)
	return pr, nil
}

// UpdatePullRequest applies the non-empty fields of the input to a known
// pull request.
func (g *LocalGit) UpdatePullRequest(_ context.Context, ref forge.PullRequestRef, in forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpUpdatePullRequest, Repo: ref.Repo, PR: ref, Number: ref.Number})
	for i := range g.pullRequests {
		if g.pullRequests[i].Number == ref.Number {
			if in.Title != "" {
				g.pullRequests[i].Title = in.Title
			}
			if in.Body != "" {
				g.pullRequests[i].Body = in.Body
			}
			return g.pullRequests[i], nil
		}
	}
	return forge.PullRequest{}, errors.New("pull request not found")
}

// CommentPullRequest is unsupported: the double serves no comments.
func (g *LocalGit) CommentPullRequest(context.Context, forge.PullRequestRef, string) (forge.Comment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpCommentPullRequest})
	return forge.Comment{}, forge.ErrUnsupported
}

// CommentWorkItem is unsupported: the double serves no comments.
func (g *LocalGit) CommentWorkItem(context.Context, forge.WorkItemRef, string) (forge.Comment, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpCommentWorkItem})
	return forge.Comment{}, forge.ErrUnsupported
}

// ListPullRequestsByHead lists every known pull request whose base
// repository, head repository, and head ref match. Each result's head is
// refreshed from the live work ref.
func (g *LocalGit) ListPullRequestsByHead(_ context.Context, base forge.PullRequestRef, repo, ref string) ([]forge.PullRequest, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.record(LocalGitOp{Kind: LocalGitOpListPullRequestsByHead, Repo: repo, PR: base, HeadRepo: repo, Ref: ref})
	var out []forge.PullRequest
	for _, pr := range g.pullRequests {
		if pr.BaseRepo != base.Repo || pr.HeadRepo != repo || pr.HeadRef != ref {
			continue
		}
		if tip, ok := g.tipLocked(pr.HeadRepo, pr.HeadRef); ok {
			pr.HeadSHA = tip
		}
		out = append(out, pr)
	}
	return out, nil
}

// Endpoint resolves a canonical repository identity to the local git
// directory that holds it. It is the trusted resolver the broker's git
// transport pins its destinations with; an identity the double does not know
// is refused.
func (g *LocalGit) Endpoint(_ context.Context, identity string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	path := g.paths[identity]
	if path == "" {
		return "", errors.New("unknown endpoint")
	}
	return path, nil
}

// Tip reports the live OID of a ref in a repository, or an empty string when
// the ref does not exist.
func (g *LocalGit) Tip(repo, ref string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tipOrEmpty(repo, ref)
}

// Path reports the local git directory that holds a canonical repository
// identity, or an empty string when the double does not know the identity.
func (g *LocalGit) Path(identity string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paths[identity]
}

// PullRequests returns a copy of the known pull-request set. The copy shares
// nothing with the double's own state, so a caller cannot reach the double
// through the returned records.
func (g *LocalGit) PullRequests() []forge.PullRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]forge.PullRequest, len(g.pullRequests))
	for i, pr := range g.pullRequests {
		out[i] = copyPullRequest(pr)
	}
	return out
}

// SetProtected sets the effective write protection of one ref. It is a world
// mutation and is not recorded as an op.
func (g *LocalGit) SetProtected(repo, ref string, protected bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.protected[repo+":"+ref] = protected
}

// SetRepository replaces a repository's identity. It is a world mutation and
// is not recorded as an op.
func (g *LocalGit) SetRepository(repo forge.Repository) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.repositories[repo.Canonical] = repo
}

// SetPullRequests replaces the known pull-request set. It is a world
// mutation and is not recorded as an op.
func (g *LocalGit) SetPullRequests(pulls []forge.PullRequest) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pullRequests = make([]forge.PullRequest, len(pulls))
	for i, pr := range pulls {
		g.pullRequests[i] = copyPullRequest(pr)
	}
}

// SetRacePullRequest arms the concurrent-create behavior: the next create
// records a competing request for the same head before the requested one. It
// is a world mutation and is not recorded as an op.
func (g *LocalGit) SetRacePullRequest(race bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.racePullRequest = race
}

// Ops returns the recorded provider calls in call order.
func (g *LocalGit) Ops() []LocalGitOp {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]LocalGitOp, len(g.ops))
	copy(out, g.ops)
	return out
}

// OpsOf returns the recorded calls of one kind, in call order.
func (g *LocalGit) OpsOf(kind LocalGitOpKind) []LocalGitOp {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := []LocalGitOp{}
	for _, op := range g.ops {
		if op.Kind == kind {
			out = append(out, op)
		}
	}
	return out
}

// tipOrEmpty returns the live tip of a ref, or an empty string when the ref
// does not exist. Callers must hold g.mu.
func (g *LocalGit) tipOrEmpty(repo, ref string) string {
	oid, _ := g.tipLocked(repo, ref)
	return oid
}

// tipLocked reads the live tip of a ref out of the local git directory. The
// caller's lock is held.
func (g *LocalGit) tipLocked(repo, ref string) (string, bool) {
	path := g.paths[repo]
	if path == "" {
		return "", false
	}
	cmd := exec.Command("git", "--git-dir", path, "rev-parse", "--verify", "refs/heads/"+ref)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}

// record appends an op. Callers must hold g.mu.
func (g *LocalGit) record(op LocalGitOp) {
	g.ops = append(g.ops, op)
}

// LocalGitWorkItemKey renders the key a work item is seeded under.
func LocalGitWorkItemKey(repo, id string) string {
	return repo + "/" + id
}

// LocalGitDescribe renders one recorded op for a failure message.
func LocalGitDescribe(op LocalGitOp) string {
	return fmt.Sprintf("%s(repo=%q ref=%q pr=%d)", op.Kind, op.Repo, op.Ref, op.Number)
}
