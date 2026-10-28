package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Mode identifies the trusted admission path that resolved a publication policy.
type Mode string

const (
	ModeResolveIssue Mode = "resolve-issue"
	ModeFixPR        Mode = "fix-pr"
	// ModeContribute publishes a contribution from an approved fork: the
	// base repository is read-only, every push goes only to the distinct
	// work (head) repository, and the pull request opens from the fork
	// head into the base.
	ModeContribute Mode = "contribute"
)

// Policy is a resolved, run-UID-bound publication policy. Treat it as immutable
// after passing it to NewPolicyEngine; the engine takes its own value copy.
type Policy struct {
	RunUID              string
	Mode                Mode
	Provider            string
	BaseRepo            string
	BaseRef             string
	BaseOID             string
	WorkRepo            string
	WorkRef             string
	WorkInitiallyAbsent bool
	WorkAnchorOID       string
	PRNumber            int
	HeadAnchorOID       string
	// SourceIssue is the trusted source-side issue identity for
	// resolve-issue runs. It is resolved at admission from the
	// immutable CoderRun spec, never from a model input, and the
	// engine refuses a resolve-issue create whose PR body does not
	// carry a closing reference to this exact identity. fix-pr
	// runs leave it zero because the existing PR's identity is
	// already pinned through PRNumber and HeadAnchorOID.
	SourceIssue SourceIssue
	// AllowClosingLinkage records an explicit admission-time policy choice
	// that permits a contribute-mode pull request body to carry closing
	// keywords ("Fixes #N"). Contribute bodies default to non-closing
	// wording ("Refs #N" or a plain issue URL); the flag is set only by
	// trusted admission into the publication policy, never inferred from
	// goal text or model output, and requires a trusted source-issue
	// identity. resolve-issue linkage is always closing and fix-pr carries
	// no body-linkage contract, so the flag is meaningful only for
	// contribute.
	AllowClosingLinkage bool
	// Criteria is the acceptance-criterion set pinned at admission
	// (#249/#280), reconstructed from the persisted publication policy and
	// verified by digest at binding. A zero CriterionSet means no set was
	// pinned (fix-pr runs, policies admitted before the criterion
	// contract); an empty pinned set is carried as a set with no criteria
	// and a digest. Carriage is the identification boundary's whole job:
	// enforcing the set at a publication boundary is #281 (native) and
	// #282 (legacy handoff).
	Criteria CriterionSet
}

// RepositoryState is a complete live observation. Unknown protection or write
// permission is represented by Known=false and is always denied.
type RepositoryState struct {
	Repo            string
	Ref             string
	Exists          bool
	OID             string
	Default         bool
	Protected       bool
	ProtectionKnown bool
	Writable        bool
	WriteKnown      bool
}

// PullRequestState carries full identities; branch names alone are insufficient.
type PullRequestState struct {
	Number   int
	State    string
	BaseRepo string
	BaseRef  string
	BaseOID  string
	HeadRepo string
	HeadRef  string
	HeadOID  string
	Title    string
	Body     string
	Draft    bool
}

// Observer is the typed, authoritative forge read surface required for safe publication.
type Observer interface {
	Repository(context.Context, string, string) (RepositoryState, error)
	PullRequest(context.Context, int) (PullRequestState, error)
	FindPullRequest(context.Context, string, string) ([]PullRequestState, error)
	CreatePullRequest(context.Context, CreatePullRequest) (int, error)
	UpdatePullRequest(context.Context, int, UpdatePullRequest) error
}

// Pusher deliberately exposes only normal non-force publication to one explicit ref.
type Pusher interface {
	IsAncestor(context.Context, string, string, string) (bool, error)
	Push(context.Context, string, string, string) error
}

type CreatePullRequest struct {
	BaseRepo, BaseRef, HeadRepo, HeadRef, Title, Body string
	Draft                                             bool
}

type UpdatePullRequest struct {
	Title, Body string
	Draft       *bool
}

type PublicationRequest struct {
	RunUID          string
	ExpectedWorkOID string // empty only when the pinned resolve-issue ref was absent
	ProposedOID     string
}

type PublicationResult struct {
	OID              string
	AlreadyPublished bool
}

type PolicyEngine struct {
	mu          sync.Mutex
	policy      Policy
	observer    Observer
	pusher      Pusher
	snapshotter Snapshotter
	// confirmed is the single exact tip confirmed by this process. It is not
	// durable evidence: after restart, only the admission anchor is trusted.
	confirmed string
}

// NewPolicyEngine fails closed when any required provider capability is absent.
// The snapshotter is optional at construction; a snapshot request without one
// fails closed at request time.
func NewPolicyEngine(policy Policy, observer Observer, pusher Pusher) (*PolicyEngine, error) {
	return newPolicyEngine(policy, observer, pusher, nil)
}

// NewPolicyEngineWithSnapshotter wires the seed-snapshot capability beside
// the publication engine.
func NewPolicyEngineWithSnapshotter(policy Policy, observer Observer, pusher Pusher, snapshotter Snapshotter) (*PolicyEngine, error) {
	if snapshotter == nil {
		return nil, errors.New("publication policy: snapshotter is required")
	}
	return newPolicyEngine(policy, observer, pusher, snapshotter)
}

func newPolicyEngine(policy Policy, observer Observer, pusher Pusher, snapshotter Snapshotter) (*PolicyEngine, error) {
	if observer == nil || pusher == nil {
		return nil, errors.New("publication policy: observer and normal pusher capabilities are required")
	}
	if err := validatePolicy(policy); err != nil {
		return nil, err
	}
	return &PolicyEngine{policy: policy, observer: observer, pusher: pusher, snapshotter: snapshotter}, nil
}

func validatePolicy(p Policy) error {
	if p.RunUID == "" || p.Provider == "" || p.BaseRepo == "" || p.BaseRef == "" || p.BaseOID == "" || p.WorkRepo == "" || p.WorkRef == "" {
		return errors.New("publication policy: run UID, provider, repositories, refs, and base OID are required")
	}
	if p.BaseRepo == p.WorkRepo && p.BaseRef == p.WorkRef {
		return errors.New("publication policy: base ref cannot also be the publication destination")
	}
	switch p.Mode {
	case ModeResolveIssue:
		if p.PRNumber != 0 || p.HeadAnchorOID != "" || (!p.WorkInitiallyAbsent && p.WorkAnchorOID == "") {
			return errors.New("publication policy: invalid resolve-issue anchors")
		}
		// A resolve-issue create must enforce authoritative
		// source-issue linkage. Without SourceIssue the engine
		// cannot prove the PR body references the same issue the
		// operator admitted; admission refusing to set it is a
		// configuration error, not a model-error recovery path.
		if p.SourceIssue.IsZero() {
			return errors.New("publication policy: resolve-issue requires a trusted source-issue identity for PR linkage")
		}
		if p.SourceIssue.Number <= 0 {
			return errors.New("publication policy: resolve-issue source-issue number must be positive")
		}
		if p.SourceIssue.Canonical() != p.BaseRepo {
			return fmt.Errorf("publication policy: resolve-issue source-issue repository %q does not match base repository %q", p.SourceIssue.Canonical(), p.BaseRepo)
		}
		// Criterion pin integrity at the engine boundary, mirroring the
		// binding: a carried set is either fully zero (unpinned, or
		// admitted before the contract) or a digest-carrying pin naming
		// exactly this policy's trusted source issue. A half-set must
		// never reach a publication boundary.
		if p.Criteria.IsPinned() {
			if p.Criteria.Issue.IsZero() || p.Criteria.Issue != p.SourceIssue {
				return errors.New("publication policy: pinned criterion set does not name the policy's trusted source issue")
			}
		} else if !p.Criteria.Issue.IsZero() || len(p.Criteria.Criteria) > 0 {
			return errors.New("publication policy: criterion set carries an identity or rows without a set digest")
		}
	case ModeFixPR:
		if p.PRNumber <= 0 || p.HeadAnchorOID == "" || p.WorkInitiallyAbsent || p.WorkAnchorOID != p.HeadAnchorOID {
			return errors.New("publication policy: fix-pr requires an exact PR head anchor")
		}
		if p.Criteria.IsPinned() || !p.Criteria.Issue.IsZero() || len(p.Criteria.Criteria) > 0 {
			return errors.New("publication policy: fix-pr policies carry no criterion contract")
		}
	case ModeContribute:
		if p.PRNumber != 0 || p.HeadAnchorOID != "" {
			return errors.New("publication policy: contribute carries fix-pr anchors")
		}
		// The structural fork invariant: a contribute policy pushes only to
		// the work repository, so a contribute policy whose work repository
		// is the base would make the base writable at this boundary.
		// Admission requires an approved fork; the engine refuses to model
		// anything else.
		if p.WorkRepo == p.BaseRepo {
			return errors.New("publication policy: contribute requires a head repository distinct from the base")
		}
		// Upstream issue context is optional for a contribution, but when
		// present it must be a coherent identity on the base repository:
		// it is the only identity a linkage reference may name.
		if !p.SourceIssue.IsZero() {
			if p.SourceIssue.Number <= 0 {
				return errors.New("publication policy: contribute source-issue number must be positive")
			}
			if p.SourceIssue.Canonical() != p.BaseRepo {
				return fmt.Errorf("publication policy: contribute source-issue repository %q does not match base repository %q", p.SourceIssue.Canonical(), p.BaseRepo)
			}
		}
		// Closing wording gates on a trusted identity: without one there is
		// nothing a "Fixes #N" reference could provably name, so an
		// admission that sets the gate without one is a configuration
		// error, not a publication path.
		if p.AllowClosingLinkage && p.SourceIssue.IsZero() {
			return errors.New("publication policy: closing linkage requires a trusted source-issue identity")
		}
		// The criterion pin integrity rule is the resolve-issue rule: a
		// contribution with pinned criteria pins them against its optional
		// upstream issue identity, and a half-set never reaches a
		// publication boundary.
		if p.Criteria.IsPinned() {
			if p.SourceIssue.IsZero() || p.Criteria.Issue != p.SourceIssue {
				return errors.New("publication policy: pinned criterion set does not name the policy's trusted source issue")
			}
		} else if !p.Criteria.Issue.IsZero() || len(p.Criteria.Criteria) > 0 {
			return errors.New("publication policy: criterion set carries an identity or rows without a set digest")
		}
	default:
		return fmt.Errorf("publication policy: unsupported mode %q", p.Mode)
	}
	return nil
}

// Publish performs fresh preflight, one ordinary push, and mandatory postflight.
// The push's own success/failure report never decides the outcome: an uncertain
// push is confirmed idempotently only when the live ref equals this exact
// proposed OID and every policy check still passes, and rejected otherwise.
func (e *PolicyEngine) Publish(ctx context.Context, req PublicationRequest) (PublicationResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.policy
	if req.RunUID == "" || req.RunUID != p.RunUID {
		return PublicationResult{}, errors.New("publication denied: run UID does not match bound policy")
	}
	if req.ProposedOID == "" || req.ExpectedWorkOID == "" && !p.WorkInitiallyAbsent {
		return PublicationResult{}, errors.New("publication denied: proposed OID and expected work tip are required")
	}
	base, work, pr, err := e.observe(ctx)
	if err != nil {
		return PublicationResult{}, err
	}
	if err = e.checkBase(base); err != nil {
		return PublicationResult{}, err
	}
	if err = e.checkDestination(work); err != nil {
		return PublicationResult{}, err
	}
	if err = e.checkPR(pr, work.OID, ""); err != nil {
		return PublicationResult{}, err
	}

	if work.Exists {
		confirmed := e.confirmed == work.OID
		anchor := p.WorkAnchorOID
		if p.Mode == ModeFixPR {
			anchor = p.HeadAnchorOID
		}
		if !confirmed && work.OID != anchor {
			return PublicationResult{}, errors.New("publication denied: live work tip is not an admitted or previously confirmed run tip")
		}
	}
	if work.Exists && work.OID == req.ProposedOID {
		// A live OID alone cannot establish run ownership after a restart. Only
		// this process's post-push confirmation may make it idempotent.
		if e.confirmed != work.OID {
			return PublicationResult{}, errors.New("publication denied: exact live proposal lacks trusted run evidence")
		}
		if pr != nil && pr.HeadOID != work.OID {
			return PublicationResult{}, errors.New("publication denied: pull request head differs from live work tip")
		}
		if !p.WorkInitiallyAbsent && work.OID == p.WorkAnchorOID {
			return PublicationResult{}, errors.New("publication denied: proposed OID is the admitted anchor, not a new publication")
		}
		e.confirmed = work.OID
		return PublicationResult{OID: req.ProposedOID, AlreadyPublished: true}, nil
	}
	if req.ExpectedWorkOID == "" {
		if !p.WorkInitiallyAbsent || work.Exists {
			return PublicationResult{}, errors.New("publication denied: pinned work ref is no longer absent")
		}
	} else if !work.Exists || work.OID != req.ExpectedWorkOID {
		return PublicationResult{}, errors.New("publication denied: live work tip differs from expected tip")
	}
	// A moved base is safe only when the proposed integration already includes
	// its current tip; policy identity remains pinned to the admitted ref.
	baseIncluded, err := e.pusher.IsAncestor(ctx, p.BaseRepo, base.OID, req.ProposedOID)
	if err != nil {
		return PublicationResult{}, errors.New("publication denied: live base ancestry capability failed")
	}
	if !baseIncluded {
		return PublicationResult{}, errors.New("publication denied: proposed commit does not include the live base tip")
	}
	ancestor := req.ExpectedWorkOID
	if ancestor == "" {
		ancestor = p.BaseOID
	}
	ok, err := e.pusher.IsAncestor(ctx, p.WorkRepo, ancestor, req.ProposedOID)
	if err != nil {
		return PublicationResult{}, errors.New("publication denied: fast-forward ancestry capability failed")
	}
	if !ok {
		return PublicationResult{}, errors.New("publication denied: proposed commit is not a fast-forward")
	}

	// A failed postflight must not leave an older in-process confirmation usable
	// as evidence for the newly observed world.
	e.confirmed = ""
	pushErr := e.pusher.Push(ctx, p.WorkRepo, p.WorkRef, req.ProposedOID)
	// Re-read everything whether push returned success, failure, or uncertainty.
	base, work, pr, observeErr := e.observe(ctx)
	if observeErr != nil {
		e.confirmed = ""
		return PublicationResult{}, errors.New("post-push observation failed; publication outcome is unconfirmed")
	}
	if err = e.checkBase(base); err != nil {
		e.confirmed = ""
		return PublicationResult{}, fmt.Errorf("post-push policy check: %w", err)
	}
	if err = e.checkDestination(work); err != nil {
		e.confirmed = ""
		return PublicationResult{}, errors.New("post-push policy check failed")
	}
	if !work.Exists || work.OID != req.ProposedOID {
		e.confirmed = ""
		if pushErr != nil {
			return PublicationResult{}, errors.New("push failed or was uncertain and exact proposed OID is not live")
		}
		return PublicationResult{}, errors.New("publication denied: live work tip does not equal proposed OID")
	}
	// The exact proposed OID is live, so publication is confirmed idempotently.
	// The transport's own report does not decide this: whether Push returned
	// success, failure or an uncertain outcome, the mandatory post-push
	// observation and the policy checks above decide. This call holds trusted
	// pre-push evidence — the proposal was imported into the broker store and
	// passed ancestry checks against the expected work tip and live base — so an
	// exact live match is sufficient to confirm. A fresh broker after a crash
	// never reaches here for a caller-supplied OID: the pre-push equality check
	// above rejects that without trusted in-process confirmation.
	if err = e.checkPR(pr, work.OID, req.ProposedOID); err != nil {
		e.confirmed = ""
		return PublicationResult{}, errors.New("post-push PR identity check failed")
	}
	baseIncluded, err = e.pusher.IsAncestor(ctx, p.BaseRepo, base.OID, req.ProposedOID)
	if err != nil || !baseIncluded {
		e.confirmed = ""
		return PublicationResult{}, errors.New("post-push proposed commit does not include the live base tip")
	}
	e.confirmed = req.ProposedOID
	return PublicationResult{OID: req.ProposedOID, AlreadyPublished: pushErr != nil}, nil
}

func (e *PolicyEngine) observe(ctx context.Context) (RepositoryState, RepositoryState, *PullRequestState, error) {
	p := e.policy
	base, err := e.observer.Repository(ctx, p.BaseRepo, p.BaseRef)
	if err != nil {
		return RepositoryState{}, RepositoryState{}, nil, errors.New("observe pinned base failed")
	}
	work, err := e.observer.Repository(ctx, p.WorkRepo, p.WorkRef)
	if err != nil {
		return RepositoryState{}, RepositoryState{}, nil, errors.New("observe pinned work ref failed")
	}
	var pr *PullRequestState
	if p.Mode == ModeFixPR {
		observed, err := e.observer.PullRequest(ctx, p.PRNumber)
		if err != nil {
			return RepositoryState{}, RepositoryState{}, nil, errors.New("observe pinned pull request failed")
		}
		pr = &observed
	}
	return base, work, pr, nil
}

func (e *PolicyEngine) checkBase(s RepositoryState) error {
	p := e.policy
	if s.Repo != p.BaseRepo || s.Ref != p.BaseRef || !s.Exists || s.OID == "" {
		return errors.New("publication denied: pinned base identity or ref is missing or changed")
	}
	// The base is an input, not a publication destination; default and
	// protected base branches are expected and must remain readable.
	return nil
}

func (e *PolicyEngine) checkDestination(s RepositoryState) error {
	p := e.policy
	if s.Repo != p.WorkRepo || s.Ref != p.WorkRef {
		return errors.New("publication denied: observer returned a different work repository or ref")
	}
	if !s.ProtectionKnown || !s.WriteKnown {
		return errors.New("publication denied: work protection or write permission is unknown")
	}
	if s.Default || s.Protected {
		return errors.New("publication denied: work ref is default or protected")
	}
	if !s.Writable {
		return errors.New("publication denied: broker cannot write pinned work repository")
	}
	if !s.Exists && !p.WorkInitiallyAbsent {
		return errors.New("publication denied: admitted work ref was deleted")
	}
	if s.Exists && s.OID == "" {
		return errors.New("publication denied: work ref has no observed OID")
	}
	return nil
}

func (e *PolicyEngine) checkPR(pr *PullRequestState, workOID, trustedTip string) error {
	p := e.policy
	if p.Mode != ModeFixPR {
		return nil
	}
	if pr == nil || pr.Number != p.PRNumber || pr.BaseRepo != p.BaseRepo || pr.BaseRef != p.BaseRef || pr.HeadRepo != p.WorkRepo || pr.HeadRef != p.WorkRef {
		return errors.New("publication denied: live pull request base or full head identity changed")
	}
	if pr.State != "open" {
		return errors.New("publication denied: pinned pull request is not open")
	}
	if workOID == "" || pr.HeadOID != workOID {
		return errors.New("publication denied: pull request head differs from exact live work tip")
	}
	if e.confirmed != pr.HeadOID && trustedTip != pr.HeadOID && pr.HeadOID != p.HeadAnchorOID {
		return errors.New("publication denied: pull request head is not an admitted or previously confirmed run tip")
	}
	return nil
}

// CreatePullRequest is resolve-issue- and contribute-only and adopts no head
// whose full identity the policy does not pin, including a same-name fork PR
// whose head repository differs from the pinned work repository. For a
// contribution the pinned work repository is the approved fork, so the create
// opens a cross-repository pull request from fork head into base, and the
// returned PR is re-read and fully verified.
//
// The body contract is mode-aware. A resolve-issue body must carry a
// closing-keyword reference to the trusted source issue (validatePolicy
// rejects a policy with an unset SourceIssue for resolve-issue runs). A
// contribute body defaults to non-closing wording: closing keywords require
// the admission-time AllowClosingLinkage choice and, when present, must name
// the trusted upstream issue identity. The check runs before the forge write
// so a model that picks the wrong issue number, a different repository, or a
// closing keyword the policy never admitted never gets to create a PR. Both
// the cross-repo form ("Closes owner/repo#N") and the bare form ("Closes #N")
// are accepted when the trusted source identity matches.
//
// When the policy carries a criterion set pinned at admission (#249/#281),
// the body must additionally satisfy ValidateCriterionEvidence against that
// exact pin. Both body contracts run before any forge read or write, so a
// refusal has nothing to undo and a corrected body is a fresh call; the
// duplicate-PR guard still precedes creation. A refusal returns the
// structured *CriteriaError so the caller sees each violation (missing IDs,
// unknown IDs, stale digests, bad status shape, coverage lie) rather than a
// bare denial. An unpinned policy (pre-contract runs) and a pinned empty set
// are vacuous passes: linkage is then the only body contract. On success the
// parsed report is returned (nil when no block was required) so a partial
// honest record surfaces its derived coverage to the caller; carriage of
// that result into outcome comments and telemetry is the executor boundary's
// job, not the engine's.
//
// Discovery before creation is the durable idempotency boundary: a create
// that succeeded but was never recorded is recovered by base/head repos and
// refs, never by title text. Exactly one open pull request at the exact
// pinned full identity (including head OID) is adopted as this run's PR
// without a forge write; zero proceeds to create; anything else — multiple
// PRs claiming the pinned head, a closed or merged PR, or a PR whose head
// differs from the published tip — fails closed rather than guessing.
func (e *PolicyEngine) CreatePullRequest(ctx context.Context, oid, title, body string, draft bool) (PullRequestState, *CriteriaReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.policy
	if p.Mode != ModeResolveIssue && p.Mode != ModeContribute {
		return PullRequestState{}, nil, errors.New("publication denied: fix-pr cannot create or retarget a pull request")
	}
	if err := validateBodyLinkage(p, body); err != nil {
		return PullRequestState{}, nil, fmt.Errorf("publication denied: %w", err)
	}
	// The criterion-evidence boundary (#281) is checked beside linkage,
	// before any forge observation, so a deficient body reaches no forge
	// write and names every violation for the repair loop.
	var report *CriteriaReport
	if p.Criteria.IsPinned() {
		var err error
		report, err = ValidateCriterionEvidence(body, p.Criteria)
		if err != nil {
			return PullRequestState{}, nil, err
		}
	}
	base, work, _, err := e.observe(ctx)
	if err != nil {
		return PullRequestState{}, nil, err
	}
	if err = e.checkBase(base); err != nil {
		return PullRequestState{}, nil, err
	}
	if err = e.checkDestination(work); err != nil {
		return PullRequestState{}, nil, err
	}
	if !work.Exists || oid == "" || work.OID != oid {
		return PullRequestState{}, nil, errors.New("publication denied: proposed OID is not the exact live head")
	}
	pulls, err := e.observer.FindPullRequest(ctx, p.WorkRepo, p.WorkRef)
	if err != nil {
		return PullRequestState{}, nil, errors.New("check existing pull request failed")
	}
	existing := pullsForPinnedHead(p, pulls)
	if len(existing) > 1 {
		return PullRequestState{}, nil, errors.New("publication denied: multiple pull requests claim the pinned head")
	}
	if len(existing) == 1 {
		// The PR's existence is itself durable evidence of this run's
		// earlier create: the pinned head identity is run-owned, so an
		// exact match at the published tip is rediscovered and returned
		// instead of duplicating. A non-match is not this run's PR as
		// published (closed, merged, moved head, changed base) and the
		// recovery is a human decision, not a second create.
		if !matchesPR(p, existing[0], oid) {
			return PullRequestState{}, nil, errors.New("publication denied: an existing pull request for the pinned head does not match the pinned identity")
		}
		return existing[0], report, nil
	}
	if e.confirmed != oid && oid != p.WorkAnchorOID {
		return PullRequestState{}, nil, errors.New("publication denied: pull request head is not an admitted or previously confirmed run tip")
	}
	number, err := e.observer.CreatePullRequest(ctx, CreatePullRequest{BaseRepo: p.BaseRepo, BaseRef: p.BaseRef, HeadRepo: p.WorkRepo, HeadRef: p.WorkRef, Title: title, Body: body, Draft: draft})
	if err != nil {
		return PullRequestState{}, nil, errors.New("create pull request failed")
	}
	if number <= 0 {
		return PullRequestState{}, nil, errors.New("publication denied: provider returned invalid pull request number")
	}
	created, err := e.observer.PullRequest(ctx, number)
	if err != nil {
		return PullRequestState{}, nil, errors.New("verify created pull request failed")
	}
	liveBase, err := e.observer.Repository(ctx, p.BaseRepo, p.BaseRef)
	if err != nil || e.checkBase(liveBase) != nil {
		return PullRequestState{}, nil, errors.New("publication denied: pinned base changed while creating pull request")
	}
	liveWork, err := e.observer.Repository(ctx, p.WorkRepo, p.WorkRef)
	if err != nil || e.checkDestination(liveWork) != nil || !liveWork.Exists || liveWork.OID != oid {
		return PullRequestState{}, nil, errors.New("publication denied: pinned work tip changed while creating pull request")
	}
	if !matchesPR(p, created, oid) {
		return PullRequestState{}, nil, errors.New("publication denied: created pull request identity or head differs from pinned policy")
	}
	// Close the create race: a provider may return one PR while another matching
	// PR is created concurrently. Confirm the complete pinned head list again.
	pulls, err = e.observer.FindPullRequest(ctx, p.WorkRepo, p.WorkRef)
	if err != nil {
		return PullRequestState{}, nil, errors.New("recheck pull requests after create failed")
	}
	found := false
	for _, candidate := range pullsForPinnedHead(p, pulls) {
		if candidate.Number == number && matchesPR(p, candidate, oid) {
			found = true
			continue
		}
		return PullRequestState{}, nil, errors.New("publication denied: concurrent pull request exists for pinned head")
	}
	if !found {
		return PullRequestState{}, nil, errors.New("publication denied: created pull request is absent from pinned head recheck")
	}
	return created, report, nil
}

// validateBodyLinkage applies the mode-aware pull request body contract:
// resolve-issue requires closing linkage to the trusted source issue, and
// contribute defaults to non-closing wording with closing keywords gated by
// the admission-time AllowClosingLinkage choice. fix-pr has no body-linkage
// contract and never reaches a body-creating path.
func validateBodyLinkage(p Policy, body string) error {
	if p.Mode == ModeContribute {
		return ValidateContributeLinkage(body, p.SourceIssue, p.AllowClosingLinkage)
	}
	return ValidateLinkage(body, p.SourceIssue)
}

// pullsForPinnedHead keeps the records that claim the pinned full head
// identity. The forge observer contract already filters foreign records out;
// the engine filters again so discovery, adoption, and the post-create race
// recheck all reason over the same pinned identity, never over a record the
// observer should not have returned.
func pullsForPinnedHead(p Policy, pulls []PullRequestState) []PullRequestState {
	out := make([]PullRequestState, 0, len(pulls))
	for _, candidate := range pulls {
		if candidate.HeadRepo == p.WorkRepo && candidate.HeadRef == p.WorkRef {
			out = append(out, candidate)
		}
	}
	return out
}

// UpdateFixPR updates metadata only after confirming the pinned PR and exact live head.
//
// There is deliberately no resolve-issue body-update surface: a resolve-issue
// run publishes its body only through CreatePullRequest, where both body
// contracts are enforced, so the criterion-evidence boundary (#281) has no
// update path to guard. fix-pr runs carry no criterion contract at all
// (validatePolicy and the binding reject criterion pins on a fix-pr policy),
// so body updates there remain governed by nothing but the pinned identity.
func (e *PolicyEngine) UpdateFixPR(ctx context.Context, update UpdatePullRequest) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.policy
	if p.Mode != ModeFixPR {
		return errors.New("publication denied: metadata update requires fix-pr policy")
	}
	if update.Title == "" && update.Body == "" && update.Draft == nil {
		return errors.New("publication denied: empty pull request update")
	}
	base, work, pr, err := e.observe(ctx)
	if err != nil {
		return err
	}
	if err = e.checkBase(base); err != nil {
		return err
	}
	if err = e.checkDestination(work); err != nil {
		return err
	}
	// checkPR enforces the exact live head (pr.HeadOID == work.OID) and that the
	// head is an admitted or previously confirmed run tip, which is sufficient for
	// metadata: a tip this run published is admitted, a foreign tip is not.
	if err = e.checkPR(pr, work.OID, e.confirmed); err != nil {
		return err
	}
	if err = e.observer.UpdatePullRequest(ctx, p.PRNumber, update); err != nil {
		return errors.New("update pinned pull request failed")
	}
	liveBase, live, livePR, err := e.observe(ctx)
	if err != nil {
		return err
	}
	if err = e.checkBase(liveBase); err != nil {
		return errors.New("post-update pinned base check failed")
	}
	if err = e.checkDestination(live); err != nil {
		return errors.New("post-update work policy check failed")
	}
	if err = e.checkPR(livePR, live.OID, e.confirmed); err != nil {
		return errors.New("post-update pull request identity check failed")
	}
	return nil
}

func matchesPR(p Policy, pr PullRequestState, oid string) bool {
	return pr.Number > 0 && pr.State == "open" && pr.BaseRepo == p.BaseRepo && pr.BaseRef == p.BaseRef && pr.HeadRepo == p.WorkRepo && pr.HeadRef == p.WorkRef && pr.HeadOID == oid
}

// EngineSnapshot is one fresh world observation plus the seed bundle
// rendered from it for trusted control.
type EngineSnapshot struct {
	Tip           string
	BaseTip       string
	WorkRefExists bool
	// AtAnchor reports whether the observed work tip is the admission
	// anchor: world truth a relaying control cannot derive itself, and the
	// distinction between "no work was integrated or published" and "the
	// seed tip may be this run's own unconfirmed publication".
	AtAnchor bool
	Data     []byte
}

// snapshotRenderRounds bounds the re-observe/re-render loop when a pinned
// ref moves between observation and fetch. It is a race-recovery bound, not
// a duration limit.
const snapshotRenderRounds = 3

// SnapshotForControl renders the sanitized seed snapshot for trusted
// control: the pinned work tip when the work ref exists, the pinned base
// tip when it was admitted absent, the base tip, and a self-contained
// bundle of both. The world is observed fresh under the engine lock and the
// fetches are verified against the observation, so a racing remote is
// re-observed instead of guessed.
func (e *PolicyEngine) SnapshotForControl(ctx context.Context) (EngineSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.policy
	if e.snapshotter == nil {
		return EngineSnapshot{}, errors.New("publication policy: snapshot capability is not configured")
	}
	for attempt := 0; attempt < snapshotRenderRounds; attempt++ {
		base, work, _, err := e.observe(ctx)
		if err != nil {
			return EngineSnapshot{}, errors.New("publication denied: observe pinned refs for snapshot failed")
		}
		if err = e.checkBase(base); err != nil {
			return EngineSnapshot{}, err
		}
		workExists := work.Exists
		if workExists && (work.Repo != p.WorkRepo || work.Ref != p.WorkRef || work.OID == "") {
			return EngineSnapshot{}, errors.New("publication denied: observer returned a different work repository or ref")
		}
		if !workExists && !p.WorkInitiallyAbsent {
			return EngineSnapshot{}, errors.New("publication denied: admitted work ref was deleted")
		}
		tip := base.OID
		if workExists {
			tip = work.OID
		}
		data, err := e.snapshotter.Snapshot(ctx, SnapshotRequest{
			WorkRepo: p.WorkRepo, WorkRef: p.WorkRef, WorkOID: work.OID, WorkExists: workExists,
			BaseRepo: p.BaseRepo, BaseRef: p.BaseRef, BaseOID: base.OID,
		})
		if errors.Is(err, ErrSnapshotTipMoved) {
			continue
		}
		if err != nil {
			return EngineSnapshot{}, err
		}
		return EngineSnapshot{
			Tip:           tip,
			BaseTip:       base.OID,
			WorkRefExists: workExists,
			AtAnchor:      workExists && tip == p.WorkAnchorOID,
			Data:          data,
		}, nil
	}
	return EngineSnapshot{}, fmt.Errorf("publication denied: pinned refs kept moving while rendering the snapshot (%d attempts): %w", snapshotRenderRounds, ErrSnapshotTipMoved)
}
