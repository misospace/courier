// Package forgetest provides an in-memory fake forge test double for tests
// across the repository. It implements forge.Provider,
// forge.RepositoryPolicyProvider, and forge.PullRequestHeadLister over
// in-memory state seeded by New, so a test can assert both the results of
// forge operations and the absence of forbidden writes.
//
// It is test support only: no production package imports it.
//
// Every provider call — successful read, write, failure, and ErrUnsupported
// alike — is recorded as an Op in call order before its effect is applied.
// Recorded ops make reads and writes independently observable: a test asserts
// the exact sequence of operations a component performed, and WriteOps shows
// whether a write was attempted at all, independent of whether the effect
// landed. Forbidden mutations (issue claim, labeling, assignment, merge, raw
// passthrough) are unrepresentable here for the same reason they are
// unrepresentable in the real contract: the forge interfaces define no such
// method, so the fake cannot be asked to perform one.
package forgetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/misospace/courier/internal/forge"
)

// ErrNotFound is returned by reads when the referenced object is not present
// in the fake's state.
var ErrNotFound = errors.New("forgetest: not found")

// OpKind names one forge contract method for recording.
type OpKind string

const (
	OpResolveRepository       OpKind = "resolve-repository"
	OpReadRef                 OpKind = "read-ref"
	OpReadEffectiveProtection OpKind = "read-effective-protection"
	OpCanWriteRepository      OpKind = "can-write-repository"
	OpReadWorkItem            OpKind = "read-work-item"
	OpReadPullRequest         OpKind = "read-pull-request"
	OpListReviews             OpKind = "list-reviews"
	OpListComments            OpKind = "list-comments"
	OpReadChecks              OpKind = "read-checks"
	OpListPullRequestsByHead  OpKind = "list-pull-requests-by-head"
	OpCreatePullRequest       OpKind = "create-pull-request"
	OpUpdatePullRequest       OpKind = "update-pull-request"
	OpCommentPullRequest      OpKind = "comment-pull-request"
)

// Op records one provider call. Only the fields matching Kind are meaningful;
// the rest hold zero values.
type Op struct {
	// Kind is the contract method invoked.
	Kind OpKind
	// Repo is the repository identity argument: the name passed to
	// ResolveRepository, or the canonical identity of the Repository argument
	// of the policy and pull-request methods.
	Repo string
	// Ref is the ref argument of ReadRef and ReadEffectiveProtection, and the
	// head ref argument of ListPullRequestsByHead.
	Ref string
	// Head is the head SHA argument of ReadChecks.
	Head string
	// WorkItem is the reference argument of ReadWorkItem.
	WorkItem forge.WorkItemRef
	// PR is the reference argument of the pull-request reads and writes, and
	// the base reference argument of ListPullRequestsByHead.
	PR forge.PullRequestRef
	// Base is the base repository named by the base reference of
	// ListPullRequestsByHead.
	Base string
	// HeadRepo is the head repository string argument of
	// ListPullRequestsByHead.
	HeadRepo string
	// Input is the create payload of CreatePullRequest.
	Input forge.CreatePullRequestInput
	// Update is the update payload of UpdatePullRequest.
	Update forge.UpdatePullRequestInput
	// Body is the comment body of CommentPullRequest.
	Body string
}

// Config seeds a Fake. New deep-copies every field, so the caller may mutate
// the Config after the call without affecting the Fake.
type Config struct {
	Provider forge.ProviderConfig
	// Capabilities lists the capabilities the fake registers. Nil registers
	// the full forge vocabulary; a non-nil slice registers exactly those
	// listed.
	Capabilities []forge.Capability
	Repositories []forge.Repository
	// Refs maps repository identity to ref to OID.
	Refs map[string]map[string]string
	// Protected and Writable map repository identity to its policy. A missing
	// key means not protected and writable, respectively.
	Protected map[string]bool
	Writable  map[string]bool
	WorkItems map[forge.WorkItemRef]forge.WorkItem
	// PullRequests is the seeded pull-request set; created requests join it.
	PullRequests []forge.PullRequest
	Reviews      map[forge.PullRequestRef][]forge.Review
	Comments     map[forge.PullRequestRef][]forge.Comment
	Checks       map[forge.PullRequestRef][]forge.Check
}

// allCapabilities is the full closed forge vocabulary.
var allCapabilities = []forge.Capability{
	forge.CapabilityReadWorkItem,
	forge.CapabilityReadPullRequest,
	forge.CapabilityListReviews,
	forge.CapabilityListComments,
	forge.CapabilityReadChecks,
	forge.CapabilityCreatePullRequest,
	forge.CapabilityUpdatePullRequest,
	forge.CapabilityComment,
}

// failure is one queued error injection.
type failure struct {
	err         error
	afterEffect bool
}

// Fake is the in-memory forge test double. All methods are safe for
// concurrent use.
type Fake struct {
	mu sync.Mutex

	cfg          forge.ProviderConfig
	caps         forge.Capabilities
	repositories []forge.Repository

	refs        map[string]map[string]string
	protected   map[string]bool
	writable    map[string]bool
	workItems   map[forge.WorkItemRef]forge.WorkItem
	prs         map[forge.PullRequestRef]forge.PullRequest
	reviews     map[forge.PullRequestRef][]forge.Review
	comments    map[forge.PullRequestRef][]forge.Comment
	checks      map[forge.PullRequestRef][]forge.Check
	nextNumber  int
	nextComment int

	ops   []Op
	fails map[OpKind][]failure
}

var (
	_ forge.Provider                 = (*Fake)(nil)
	_ forge.RepositoryPolicyProvider = (*Fake)(nil)
	_ forge.PullRequestHeadLister    = (*Fake)(nil)
)

// New builds a Fake from seeded state, deep-copying every seed. A nil
// Capabilities slice registers the full forge vocabulary; a non-nil one
// registers exactly the capabilities listed.
func New(cfg Config) *Fake {
	caps := cfg.Capabilities
	if caps == nil {
		caps = allCapabilities
	}
	f := &Fake{
		cfg:          cfg.Provider,
		caps:         forge.NewCapabilities(caps...),
		repositories: append([]forge.Repository(nil), cfg.Repositories...),

		refs:        map[string]map[string]string{},
		protected:   map[string]bool{},
		writable:    map[string]bool{},
		workItems:   map[forge.WorkItemRef]forge.WorkItem{},
		prs:         map[forge.PullRequestRef]forge.PullRequest{},
		reviews:     map[forge.PullRequestRef][]forge.Review{},
		comments:    map[forge.PullRequestRef][]forge.Comment{},
		checks:      map[forge.PullRequestRef][]forge.Check{},
		nextNumber:  101,
		nextComment: 1,

		fails: map[OpKind][]failure{},
	}
	for repo, refs := range cfg.Refs {
		inner := make(map[string]string, len(refs))
		for ref, oid := range refs {
			inner[ref] = oid
		}
		f.refs[repo] = inner
	}
	for key, v := range cfg.Protected {
		f.protected[key] = v
	}
	for key, v := range cfg.Writable {
		f.writable[key] = v
	}
	for ref, item := range cfg.WorkItems {
		f.workItems[ref] = copyWorkItem(item)
	}
	for _, pr := range cfg.PullRequests {
		f.prs[prKey(pr)] = copyPullRequest(pr)
	}
	for ref, rs := range cfg.Reviews {
		f.reviews[ref] = copySlice(rs)
	}
	for ref, cs := range cfg.Comments {
		f.comments[ref] = copySlice(cs)
	}
	for ref, cs := range cfg.Checks {
		f.checks[ref] = copySlice(cs)
	}
	return f
}

// Config returns the seeded provider configuration. It is not recorded.
func (f *Fake) Config() forge.ProviderConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cfg
}

// Capabilities returns a clone of the registered capabilities. It is not
// recorded; mutating the returned clone does not affect the fake.
func (f *Fake) Capabilities() forge.Capabilities {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.caps.Clone()
}

// ResolveRepository resolves name against the seeded repositories by ID or
// Canonical.
func (f *Fake) ResolveRepository(_ context.Context, name string) (forge.Repository, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpResolveRepository, Repo: name})
	fail, failed := f.popFailure(OpResolveRepository)
	if failed && !fail.afterEffect {
		return forge.Repository{}, fail.err
	}
	for _, repo := range f.repositories {
		if repo.ID == name || repo.Canonical == name {
			if failed {
				return forge.Repository{}, fail.err
			}
			return repo, nil
		}
	}
	if failed {
		return forge.Repository{}, fail.err
	}
	return forge.Repository{}, ErrNotFound
}

// ReadRef returns the live seeded state of the ref: an OID and Exists for a
// seeded ref, or Exists false otherwise.
func (f *Fake) ReadRef(_ context.Context, repo forge.Repository, ref string) (forge.RefState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpReadRef, Repo: repoKey(repo), Ref: ref})
	fail, failed := f.popFailure(OpReadRef)
	if failed && !fail.afterEffect {
		return forge.RefState{}, fail.err
	}
	oid, ok := f.refs[repoKey(repo)][ref]
	state := forge.RefState{Ref: ref}
	if ok {
		state.OID = oid
		state.Exists = true
	}
	if failed {
		return forge.RefState{}, fail.err
	}
	return state, nil
}

// ReadEffectiveProtection returns the seeded protection for the repository,
// defaulting to not protected.
func (f *Fake) ReadEffectiveProtection(_ context.Context, repo forge.Repository, ref string) (forge.Protection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpReadEffectiveProtection, Repo: repoKey(repo), Ref: ref})
	fail, failed := f.popFailure(OpReadEffectiveProtection)
	if failed && !fail.afterEffect {
		return forge.Protection{}, fail.err
	}
	prot := forge.Protection{Protected: f.protected[repoKey(repo)]}
	if failed {
		return forge.Protection{}, fail.err
	}
	return prot, nil
}

// CanWriteRepository returns the seeded writability for the repository,
// defaulting to writable.
func (f *Fake) CanWriteRepository(_ context.Context, repo forge.Repository) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpCanWriteRepository, Repo: repoKey(repo)})
	fail, failed := f.popFailure(OpCanWriteRepository)
	if failed && !fail.afterEffect {
		return false, fail.err
	}
	writable, ok := f.writable[repoKey(repo)]
	if !ok {
		writable = true
	}
	if failed {
		return false, fail.err
	}
	return writable, nil
}

// ReadWorkItem returns the seeded work item, or ErrNotFound.
func (f *Fake) ReadWorkItem(_ context.Context, ref forge.WorkItemRef) (forge.WorkItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpReadWorkItem, Repo: ref.Repo, WorkItem: ref})
	fail, failed := f.popFailure(OpReadWorkItem)
	if failed && !fail.afterEffect {
		return forge.WorkItem{}, fail.err
	}
	item, ok := f.workItems[ref]
	if !ok {
		if failed {
			return forge.WorkItem{}, fail.err
		}
		return forge.WorkItem{}, ErrNotFound
	}
	if failed {
		return forge.WorkItem{}, fail.err
	}
	return copyWorkItem(item), nil
}

// ReadPullRequest returns the seeded or created pull request, or ErrNotFound.
func (f *Fake) ReadPullRequest(_ context.Context, ref forge.PullRequestRef) (forge.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpReadPullRequest, Repo: ref.Repo, PR: ref})
	fail, failed := f.popFailure(OpReadPullRequest)
	if failed && !fail.afterEffect {
		return forge.PullRequest{}, fail.err
	}
	pr, ok := f.prs[ref]
	if !ok {
		if failed {
			return forge.PullRequest{}, fail.err
		}
		return forge.PullRequest{}, ErrNotFound
	}
	if failed {
		return forge.PullRequest{}, fail.err
	}
	return copyPullRequest(pr), nil
}

// ListReviews returns the seeded reviews on the pull request.
func (f *Fake) ListReviews(_ context.Context, ref forge.PullRequestRef) ([]forge.Review, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpListReviews, Repo: ref.Repo, PR: ref})
	fail, failed := f.popFailure(OpListReviews)
	if failed && !fail.afterEffect {
		return nil, fail.err
	}
	reviews := copySlice(f.reviews[ref])
	if failed {
		return nil, fail.err
	}
	return reviews, nil
}

// ListComments returns the seeded and appended comments on the pull request.
func (f *Fake) ListComments(_ context.Context, ref forge.PullRequestRef) ([]forge.Comment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpListComments, Repo: ref.Repo, PR: ref})
	fail, failed := f.popFailure(OpListComments)
	if failed && !fail.afterEffect {
		return nil, fail.err
	}
	comments := copySlice(f.comments[ref])
	if failed {
		return nil, fail.err
	}
	return comments, nil
}

// ReadChecks returns the seeded checks for the pull request. The head SHA is
// recorded, not filtered on.
func (f *Fake) ReadChecks(_ context.Context, ref forge.PullRequestRef, headSHA string) ([]forge.Check, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpReadChecks, Repo: ref.Repo, PR: ref, Head: headSHA})
	fail, failed := f.popFailure(OpReadChecks)
	if failed && !fail.afterEffect {
		return nil, fail.err
	}
	checks := copySlice(f.checks[ref])
	if failed {
		return nil, fail.err
	}
	return checks, nil
}

// ListPullRequestsByHead returns every known pull request — seeded and
// created, in any state — whose base repository matches the base reference's
// repository and whose head repository and head ref match the arguments. Each
// result's HeadSHA is refreshed from the live seeded refs map for the head
// repository and ref when present; the stored request is not mutated.
func (f *Fake) ListPullRequestsByHead(_ context.Context, base forge.PullRequestRef, repo, ref string) ([]forge.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpListPullRequestsByHead, Repo: repo, PR: base, Base: base.Repo, HeadRepo: repo, Ref: ref})
	fail, failed := f.popFailure(OpListPullRequestsByHead)
	if failed && !fail.afterEffect {
		return nil, fail.err
	}
	var out []forge.PullRequest
	for _, pr := range f.prs {
		if pr.BaseRepo == base.Repo && pr.HeadRepo == repo && pr.HeadRef == ref {
			result := copyPullRequest(pr)
			if oid, ok := f.refs[repo][ref]; ok {
				result.HeadSHA = oid
			}
			out = append(out, result)
		}
	}
	if failed {
		return nil, fail.err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out, nil
}

// CreatePullRequest opens a pull request in the fake's state. It returns
// forge.ErrUnsupported when the create capability is unregistered; the call
// is recorded either way. Numbers are assigned from 101 upward, and the base
// and head SHAs are read from the live seeded refs map.
func (f *Fake) CreatePullRequest(_ context.Context, in forge.CreatePullRequestInput) (forge.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpCreatePullRequest, Repo: in.Repo, Input: in})
	fail, failed := f.popFailure(OpCreatePullRequest)
	if failed && !fail.afterEffect {
		return forge.PullRequest{}, fail.err
	}
	if !f.caps.Has(forge.CapabilityCreatePullRequest) {
		return forge.PullRequest{}, forge.ErrUnsupported
	}
	number := f.nextNumber
	f.nextNumber++
	baseSHA, _ := f.refs[in.Repo][in.Base]
	headSHA, _ := f.refs[in.HeadRepo][in.HeadRef]
	pr := forge.PullRequest{
		Number:   number,
		Repo:     in.Repo,
		BaseRepo: in.Repo,
		BaseRef:  in.Base,
		BaseSHA:  baseSHA,
		HeadRepo: in.HeadRepo,
		HeadRef:  in.HeadRef,
		HeadSHA:  headSHA,
		State:    "open",
		Draft:    in.Draft,
		Title:    in.Title,
		Body:     in.Body,
		URL:      fmt.Sprintf("https://forge.invalid/%s/pulls/%d", in.Repo, number),
	}
	f.prs[prKey(pr)] = copyPullRequest(pr)
	if failed {
		return forge.PullRequest{}, fail.err
	}
	return copyPullRequest(pr), nil
}

// UpdatePullRequest applies the non-zero fields of the input to the stored
// pull request and returns the updated request, or ErrNotFound.
func (f *Fake) UpdatePullRequest(_ context.Context, ref forge.PullRequestRef, in forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpUpdatePullRequest, Repo: ref.Repo, PR: ref, Update: in})
	fail, failed := f.popFailure(OpUpdatePullRequest)
	if failed && !fail.afterEffect {
		return forge.PullRequest{}, fail.err
	}
	pr, ok := f.prs[ref]
	if !ok {
		if failed {
			return forge.PullRequest{}, fail.err
		}
		return forge.PullRequest{}, ErrNotFound
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
	f.prs[ref] = copyPullRequest(pr)
	if failed {
		return forge.PullRequest{}, fail.err
	}
	return copyPullRequest(pr), nil
}

// CommentPullRequest appends a comment to the stored pull request and returns
// it, or ErrNotFound.
func (f *Fake) CommentPullRequest(_ context.Context, ref forge.PullRequestRef, body string) (forge.Comment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(Op{Kind: OpCommentPullRequest, Repo: ref.Repo, PR: ref, Body: body})
	fail, failed := f.popFailure(OpCommentPullRequest)
	if failed && !fail.afterEffect {
		return forge.Comment{}, fail.err
	}
	if _, ok := f.prs[ref]; !ok {
		if failed {
			return forge.Comment{}, fail.err
		}
		return forge.Comment{}, ErrNotFound
	}
	id := fmt.Sprintf("comment-%d", f.nextComment)
	f.nextComment++
	comment := forge.Comment{ID: id, Author: "forgetest", Body: body}
	f.comments[ref] = append(f.comments[ref], comment)
	if failed {
		return forge.Comment{}, fail.err
	}
	return comment, nil
}

// FailNext queues err so that the next call of kind returns it with no effect.
// Queue entries of a kind are consumed FIFO.
func (f *Fake) FailNext(kind OpKind, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails[kind] = append(f.fails[kind], failure{err: err})
}

// FailNextAfterEffect queues err so that the next call of kind applies its
// default effect fully and then returns err with no response: the write
// landed, but the caller loses the reply. Queue entries of a kind are
// consumed FIFO.
func (f *Fake) FailNextAfterEffect(kind OpKind, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails[kind] = append(f.fails[kind], failure{err: err, afterEffect: true})
}

// SetRef sets the live OID for a ref of a repository. It is a world mutation
// and is not recorded as an Op.
func (f *Fake) SetRef(repo, ref, oid string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refs[repo] == nil {
		f.refs[repo] = map[string]string{}
	}
	f.refs[repo][ref] = oid
}

// DeleteRef removes a ref from a repository. It is a world mutation and is
// not recorded as an Op.
func (f *Fake) DeleteRef(repo, ref string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m := f.refs[repo]; m != nil {
		delete(m, ref)
	}
}

// SetWritable sets the seeded writability of a repository. It is a world
// mutation and is not recorded as an Op.
func (f *Fake) SetWritable(repo string, writable bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writable[repo] = writable
}

// SetProtected sets the seeded protection of a repository. It is a world
// mutation and is not recorded as an Op.
func (f *Fake) SetProtected(repo string, protected bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.protected[repo] = protected
}

// AddRepository appends a repository to the seeded set. It is a world
// mutation and is not recorded as an Op.
func (f *Fake) AddRepository(repo forge.Repository) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repositories = append(f.repositories, repo)
}

// AddPullRequest adds a pull request to the known set. It is a world mutation
// and is not recorded as an Op.
func (f *Fake) AddPullRequest(pr forge.PullRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prs[prKey(pr)] = copyPullRequest(pr)
}

// Ops returns the recorded operations in call order.
func (f *Fake) Ops() []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Op, len(f.ops))
	copy(out, f.ops)
	return out
}

// OpsOf returns the recorded operations of kind, in call order.
func (f *Fake) OpsOf(kind OpKind) []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Op{}
	for _, op := range f.ops {
		if op.Kind == kind {
			out = append(out, op)
		}
	}
	return out
}

// writeKinds is the set of recorded kinds that are writes.
var writeKinds = map[OpKind]bool{
	OpCreatePullRequest:  true,
	OpUpdatePullRequest:  true,
	OpCommentPullRequest: true,
}

// WriteOps returns only the recorded write operations — create, update, and
// comment — in call order.
func (f *Fake) WriteOps() []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Op{}
	for _, op := range f.ops {
		if writeKinds[op.Kind] {
			out = append(out, op)
		}
	}
	return out
}

// WriteCount reports the number of recorded write operations.
func (f *Fake) WriteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, op := range f.ops {
		if writeKinds[op.Kind] {
			n++
		}
	}
	return n
}

// PullRequests returns every known pull request — seeded and created —
// sorted by number.
func (f *Fake) PullRequests() []forge.PullRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]forge.PullRequest, 0, len(f.prs))
	for _, pr := range f.prs {
		out = append(out, copyPullRequest(pr))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out
}

// record appends an op. Callers must hold f.mu.
func (f *Fake) record(op Op) {
	f.ops = append(f.ops, op)
}

// popFailure returns the next queued failure for kind, if any. Callers must
// hold f.mu.
func (f *Fake) popFailure(kind OpKind) (failure, bool) {
	q := f.fails[kind]
	if len(q) == 0 {
		return failure{}, false
	}
	head := q[0]
	f.fails[kind] = q[1:]
	return head, true
}

// repoKey is the repository identity string used for policy and ref lookups:
// the canonical identity, or the ID when no canonical is set.
func repoKey(repo forge.Repository) string {
	if repo.Canonical != "" {
		return repo.Canonical
	}
	return repo.ID
}

// prKey is the storage key for a pull request: its repository identity and
// number, using BaseRepo when Repo is unset.
func prKey(pr forge.PullRequest) forge.PullRequestRef {
	repo := pr.Repo
	if repo == "" {
		repo = pr.BaseRepo
	}
	return forge.PullRequestRef{Repo: repo, Number: pr.Number}
}

func copyPullRequest(pr forge.PullRequest) forge.PullRequest {
	if pr.Mergeable != nil {
		v := *pr.Mergeable
		pr.Mergeable = &v
	}
	return pr
}

func copyWorkItem(item forge.WorkItem) forge.WorkItem {
	if item.Labels != nil {
		item.Labels = append([]string(nil), item.Labels...)
	}
	return item
}

func copySlice[T any](in []T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	copy(out, in)
	return out
}
