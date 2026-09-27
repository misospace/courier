package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/misospace/courier/internal/forge"
)

// ForgeAdapter translates typed forge reads into the broker policy observer.
// Mutations are delegated to PolicyEngine, which owns authorization and
// post-mutation verification; provider credentials never leave the provider.
type ForgeAdapter struct {
	provider forge.Provider
	policy   forge.RepositoryPolicyProvider
	heads    forge.PullRequestHeadLister
	mu       sync.Mutex
	created  map[int]struct{}
	p        Policy
	base     forge.Repository
	work     forge.Repository
}

var _ Observer = (*ForgeAdapter)(nil)

// NewForgeAdapter binds provider reads to canonical repositories pinned by
// trusted admission. The repositories must come from ResolveRepository, not
// model input. A separate policy provider may be supplied for provider-neutral
// adapters; provider is used as the head lister when it implements that facet.
func NewForgeAdapter(p Policy, provider forge.Provider, repositories forge.RepositoryPolicyProvider, base, work forge.Repository) (*ForgeAdapter, error) {
	if provider == nil || repositories == nil {
		return nil, errors.New("forge adapter: provider and repository policy provider are required")
	}
	if p.BaseRepo == "" || p.WorkRepo == "" || base.Canonical != p.BaseRepo || work.Canonical != p.WorkRepo || base.ID == "" || work.ID == "" {
		return nil, errors.New("forge adapter: pinned canonical repository identities are required")
	}
	if provider.Config().Name != p.Provider {
		return nil, errors.New("forge adapter: provider does not match pinned policy")
	}
	caps := provider.Capabilities()
	for _, capability := range []forge.Capability{forge.CapabilityReadPullRequest, forge.CapabilityCreatePullRequest, forge.CapabilityUpdatePullRequest} {
		if !caps.Available(capability).Available {
			return nil, fmt.Errorf("forge adapter: provider capability %q is unavailable", capability)
		}
	}
	headLister, _ := provider.(forge.PullRequestHeadLister)
	return &ForgeAdapter{provider: provider, policy: repositories, heads: headLister, created: make(map[int]struct{}), p: p, base: base, work: work}, nil
}

func (a *ForgeAdapter) checkedRepository(ctx context.Context, pinned forge.Repository) (forge.Repository, error) {
	live, err := a.policy.ResolveRepository(ctx, pinned.Canonical)
	if err != nil {
		return forge.Repository{}, errors.New("forge adapter: canonical repository revalidation failed")
	}
	if live.ID == "" || live.Canonical != pinned.Canonical || live.ID != pinned.ID || live.DefaultRef == "" {
		return forge.Repository{}, errors.New("forge adapter: canonical repository identity changed")
	}
	return live, nil
}

func (a *ForgeAdapter) Repository(ctx context.Context, repo, ref string) (RepositoryState, error) {
	pinned, ok := a.repository(repo)
	if !ok || ref == "" {
		return RepositoryState{}, errors.New("forge adapter: repository or ref is outside pinned policy")
	}
	live, err := a.checkedRepository(ctx, pinned)
	if err != nil {
		return RepositoryState{}, err
	}
	state, err := a.policy.ReadRef(ctx, live, ref)
	if err != nil {
		return RepositoryState{}, errors.New("forge adapter: live ref read failed")
	}
	if state.Ref != ref || state.Exists && state.OID == "" {
		return RepositoryState{}, errors.New("forge adapter: provider returned incomplete or different ref identity")
	}
	out := RepositoryState{Repo: live.Canonical, Ref: ref, Exists: state.Exists, OID: state.OID, Default: ref == live.DefaultRef}
	protection, protectionErr := a.policy.ReadEffectiveProtection(ctx, live, ref)
	writable, writeErr := a.policy.CanWriteRepository(ctx, live)
	if protectionErr == nil {
		out.Protected, out.ProtectionKnown = protection.Protected, true
	}
	if writeErr == nil {
		out.Writable, out.WriteKnown = writable, true
	}
	// Unknown facts are conveyed as unknown; PolicyEngine consistently denies them.
	return out, nil
}

func (a *ForgeAdapter) PullRequest(ctx context.Context, number int) (PullRequestState, error) {
	a.mu.Lock()
	_, createdByAdapter := a.created[number]
	a.mu.Unlock()
	if number <= 0 || (a.p.Mode == ModeFixPR && number != a.p.PRNumber) || (a.p.Mode == ModeResolveIssue && !createdByAdapter) {
		return PullRequestState{}, errors.New("forge adapter: pull request is outside pinned policy")
	}
	base, err := a.checkedRepository(ctx, a.base)
	if err != nil {
		return PullRequestState{}, err
	}
	work, err := a.checkedRepository(ctx, a.work)
	if err != nil {
		return PullRequestState{}, err
	}
	pr, err := a.provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: base.Canonical, Number: number})
	if err != nil {
		return PullRequestState{}, errors.New("forge adapter: pull request read failed")
	}
	if pr.Number != number || pr.BaseRepo == "" || pr.BaseRef == "" || pr.BaseSHA == "" || pr.HeadRepo == "" || pr.HeadRef == "" || pr.HeadSHA == "" {
		return PullRequestState{}, errors.New("forge adapter: pull request identity is incomplete")
	}
	// Resolve both identities afresh; same-name substitutions are not accepted.
	actualBase, err := a.policy.ResolveRepository(ctx, pr.BaseRepo)
	if err != nil || actualBase.ID == "" || actualBase.ID != base.ID || actualBase.Canonical != base.Canonical {
		return PullRequestState{}, errors.New("forge adapter: pull request base identity mismatch")
	}
	actualHead, err := a.policy.ResolveRepository(ctx, pr.HeadRepo)
	if err != nil || actualHead.ID == "" || actualHead.ID != work.ID || actualHead.Canonical != work.Canonical {
		return PullRequestState{}, errors.New("forge adapter: pull request head identity mismatch")
	}
	return PullRequestState{Number: pr.Number, State: pr.State, BaseRepo: actualBase.Canonical, BaseRef: pr.BaseRef, BaseOID: pr.BaseSHA, HeadRepo: actualHead.Canonical, HeadRef: pr.HeadRef, HeadOID: pr.HeadSHA, Title: pr.Title, Body: pr.Body, Draft: pr.Draft}, nil
}

func (a *ForgeAdapter) FindPullRequest(ctx context.Context, headRepo, headRef string) ([]PullRequestState, error) {
	if a.heads == nil {
		return nil, forge.ErrUnsupported
	}
	if headRepo != a.p.WorkRepo || headRef != a.p.WorkRef {
		return nil, errors.New("forge adapter: head is outside pinned policy")
	}
	base, err := a.checkedRepository(ctx, a.base)
	if err != nil {
		return nil, err
	}
	work, err := a.checkedRepository(ctx, a.work)
	if err != nil {
		return nil, err
	}
	pulls, err := a.heads.ListPullRequestsByHead(ctx, forge.PullRequestRef{Repo: base.Canonical}, work.Canonical, headRef)
	if err != nil {
		return nil, errors.New("forge adapter: pull request head listing failed")
	}
	out := make([]PullRequestState, 0, len(pulls))
	for _, pr := range pulls {
		if pr.BaseRepo != base.Canonical || pr.HeadRepo != work.Canonical || pr.HeadRef != headRef {
			continue
		}
		if pr.Number <= 0 || pr.BaseRef == "" || pr.BaseSHA == "" || pr.HeadSHA == "" {
			return nil, fmt.Errorf("forge adapter: listed pull request has incomplete identity")
		}
		out = append(out, PullRequestState{Number: pr.Number, State: pr.State, BaseRepo: pr.BaseRepo, BaseRef: pr.BaseRef, BaseOID: pr.BaseSHA, HeadRepo: pr.HeadRepo, HeadRef: pr.HeadRef, HeadOID: pr.HeadSHA, Title: pr.Title, Body: pr.Body, Draft: pr.Draft})
	}
	return out, nil
}

func (a *ForgeAdapter) CreatePullRequest(ctx context.Context, in CreatePullRequest) (int, error) {
	if a.p.Mode != ModeResolveIssue || in.BaseRepo != a.p.BaseRepo || in.BaseRef != a.p.BaseRef || in.HeadRepo != a.p.WorkRepo || in.HeadRef != a.p.WorkRef {
		return 0, errors.New("forge adapter: pull request destination differs from pinned policy")
	}
	base, err := a.checkedRepository(ctx, a.base)
	if err != nil {
		return 0, err
	}
	work, err := a.checkedRepository(ctx, a.work)
	if err != nil {
		return 0, err
	}
	if _, err := a.Repository(ctx, a.p.BaseRepo, a.p.BaseRef); err != nil {
		return 0, err
	}
	if _, err := a.Repository(ctx, a.p.WorkRepo, a.p.WorkRef); err != nil {
		return 0, err
	}
	before, err := a.FindPullRequest(ctx, work.Canonical, in.HeadRef)
	if err != nil {
		return 0, err
	}
	if len(before) != 0 {
		return 0, errors.New("forge adapter: pull request already exists for pinned full head")
	}
	head := work.Canonical + ":" + in.HeadRef
	created, err := a.provider.CreatePullRequest(ctx, forge.CreatePullRequestInput{Repo: base.Canonical, Head: head, HeadRepo: work.Canonical, HeadRef: in.HeadRef, Base: in.BaseRef, Title: in.Title, Body: in.Body, Draft: in.Draft})
	if err != nil {
		return 0, errors.New("forge adapter: create pull request failed")
	}
	if created.Number <= 0 {
		return 0, errors.New("forge adapter: provider returned invalid pull request number")
	}
	if created.BaseRepo != base.Canonical || created.BaseRef != in.BaseRef || created.HeadRepo != work.Canonical || created.HeadRef != in.HeadRef || created.HeadSHA == "" {
		return 0, errors.New("forge adapter: created pull request does not match pinned full identity")
	}
	// Register the exact result of this create so the read-back below and any
	// later PolicyEngine verification may observe it; nothing else may.
	a.mu.Lock()
	a.created[created.Number] = struct{}{}
	a.mu.Unlock()
	if _, err := a.PullRequest(ctx, created.Number); err != nil {
		return 0, err
	}
	// Resolve identities again after creation; repository replacement during the
	// mutation must not be accepted as a valid result.
	if _, err := a.checkedRepository(ctx, a.base); err != nil {
		return 0, err
	}
	if _, err := a.checkedRepository(ctx, a.work); err != nil {
		return 0, err
	}
	matches, err := a.FindPullRequest(ctx, work.Canonical, in.HeadRef)
	if err != nil {
		return 0, err
	}
	if len(matches) != 1 || matches[0].Number != created.Number {
		return 0, errors.New("forge adapter: duplicate or mismatched pull request appeared during creation")
	}
	return created.Number, nil
}

func (a *ForgeAdapter) UpdatePullRequest(ctx context.Context, number int, in UpdatePullRequest) error {
	if number != a.p.PRNumber || a.p.Mode != ModeFixPR {
		return errors.New("forge adapter: pull request update is outside pinned policy")
	}
	if in.Draft != nil {
		return forge.ErrUnsupported
	}
	before, err := a.PullRequest(ctx, number)
	if err != nil {
		return err
	}
	_, err = a.provider.UpdatePullRequest(ctx, forge.PullRequestRef{Repo: a.base.Canonical, Number: number}, forge.UpdatePullRequestInput{Title: in.Title, Body: in.Body})
	if err != nil {
		return errors.New("forge adapter: update pull request failed")
	}
	after, err := a.PullRequest(ctx, number)
	if err != nil {
		return errors.New("forge adapter: post-update pull request read failed")
	}
	if before.BaseRepo != after.BaseRepo || before.BaseRef != after.BaseRef || before.HeadRepo != after.HeadRepo || before.HeadRef != after.HeadRef || before.HeadOID != after.HeadOID {
		return errors.New("forge adapter: pull request identity changed during metadata update")
	}
	return nil
}

func (a *ForgeAdapter) repository(name string) (forge.Repository, bool) {
	if name == a.p.BaseRepo {
		return a.base, true
	}
	if name == a.p.WorkRepo {
		return a.work, true
	}
	return forge.Repository{}, false
}
