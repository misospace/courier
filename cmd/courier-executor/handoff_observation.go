package main

import (
	"context"
	"fmt"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/github"
	"strings"
)

// splitOwnerRepo splits an owner/name repository identity on the final "/".
func splitOwnerRepo(value string) (owner, name string, ok bool) {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "/")
	if idx <= 0 || idx == len(value)-1 {
		return "", "", false
	}
	return value[:idx], value[idx+1:], true
}

// prLinkageObservation is one PR observed for the run branch during
// the legacy executor's authoritative-linkage check. It carries the
// fields the legacy path needs without exposing the full GitHub
// PullRequest shape to the rest of the executor.
type prLinkageObservation struct {
	// Number is the PR's number on the base repository.
	Number int
	// State is the PR's state, lowercased.
	State string
	// Body is the raw PR body, exactly as the forge returned it.
	Body string
	// Draft reports whether the forge marked the PR a draft.
	Draft bool
}

// runBranchPRKind classifies the executor's identity-checked observation of
// the pull requests whose head carries the run branch, taken while the
// OpenCode session is still alive (#247).
type runBranchPRKind int

const (
	// prStateOpen: exactly one open, non-draft PR carries the verified
	// head; its body still faces the authoritative-linkage check (#248).
	prStateOpen runBranchPRKind = iota
	// prStateDraft: exactly one open PR carries the verified head and
	// the forge marks it a draft; the run may not hand off until it is
	// ready for review.
	prStateDraft
	// prStateMissing: no pull request of any state carries the verified
	// head; the caller confirms remote publication before treating it
	// as a confirmed missing PR (#247).
	prStateMissing
	// prStateAmbiguous: more than one open pull request claims the
	// verified head; the executor refuses to adopt one (#247 fail-closed).
	prStateAmbiguous
	// prStateClosedOnly: the head's only pull requests are closed or
	// merged; the executor refuses adoption, reopening, or a duplicate
	// creation (#247 fail-closed, matching the adoption guard).
	prStateClosedOnly
	// prStateForeignHead: the forge's owner:branch head selector surfaced
	// a pull request whose repository identity is not the run's own head;
	// the executor cannot reconcile that world, so it fails closed — no
	// adoption, no creation prompt, and never a "missing" claim built by
	// silently deleting the foreign record (#247 fail-closed).
	prStateForeignHead
	// prStateWrongBase: an open pull request carries the verified head
	// but targets a base other than the run's pinned base; adopting it
	// would hand off against the wrong contract, re-targeting it is
	// forbidden, and creating another PR would paper over the world the
	// forge reported (#247 fail-closed).
	prStateWrongBase
)

// runBranchPRObservation is the classified result of the executor's
// identity-checked PR observation. open and draft carry the single PR in
// that state; closed carries the closed or merged PRs when the kind is
// prStateClosedOnly, so terminal reasons can name the actual PR; the other
// fail-closed kinds carry only the kind.
type runBranchPRObservation struct {
	kind   runBranchPRKind
	open   *prLinkageObservation
	draft  *prLinkageObservation
	closed []*prLinkageObservation
}

// handoffCheckOutcome is the result of the shared pre-handoff check that
// the legacy executor runs against the live PRs for the run branch. It
// carries the identity-checked forge observation (the #247 fields) plus
// the body-contract checks that share the plan — authoritative linkage
// (#248) and, when a criterion set is pinned (#280), the criterion-
// evidence contract (#249). It is consumed by the run loop to decide
// between Verifying, a finite continuation to create or ready the PR or
// repair its body, and a fail-closed NeedsHuman. Every field here is
// fail-closed at the boundary: the model can neither skip the check nor
// argue against its verdict.
type handoffCheckOutcome struct {
	// pr records the live PR observed for the run branch. nil
	// when no PR was found.
	pr *prLinkageObservation
	// err is the structured rejection from the linkage validator,
	// or nil when the body satisfies the contract.
	err *broker.LinkageError
	// missing reports that the confirmed-published branch has no pull
	// request at all: a bounded continuation asks the session to
	// open one (#247).
	missing bool
	// draft reports that the branch's only open PR is a draft: a
	// bounded continuation asks the session to ready it (#247).
	draft bool
	// unpublished reports that the remote does not carry the run's
	// current commit — the ref is absent or holds a different commit —
	// so the handoff is fail-closed and no create prompt or Verifying
	// handoff may cover work that never landed (#247). publication
	// carries the observed local/remote pair for the terminal reason.
	unpublished bool
	// publication is the git-side evidence behind unpublished.
	publication remotePublicationState
	// ambiguous reports more than one open PR on the verified head:
	// fail-closed, never a silent adoption (#247).
	ambiguous bool
	// closedOnly reports a head whose only PRs are closed or merged:
	// fail-closed, never a duplicate creation (#247). For a fix-pr run
	// this is the pinned pull request confirmed closed-unmerged: the
	// authoritative merged/closed discrimination in checkFixPRHandoff
	// settles a merged pinned request as pinnedMerged instead (#582).
	closedOnly bool
	// pinnedMerged reports a fix-pr run whose pinned pull request — the
	// exact assignment the run was admitted against — merged during the
	// run before the handoff (#582). The merge is the authoritative
	// settlement of the repair assignment: the verdict ends the run as
	// superseded terminal work, never a NeedsHuman solely because it
	// merged, and never a Verifying handoff claiming post-merge commits
	// shipped in the merged head. pr carries the pinned observation for
	// the diagnostics.
	pinnedMerged bool
	// foreignHead reports a forge record that claims the head selector
	// from another repository: fail-closed, never deleted toward a
	// missing claim (#247).
	foreignHead bool
	// wrongBase reports an open PR on the verified head that targets a
	// base other than the run's pinned base: fail-closed, never adopted or
	// re-targeted (#247).
	wrongBase bool
	// pinnedDraft reports a fix-pr run's pinned pull request still a
	// draft at handoff (#517): the resolve-issue ready-continuation is
	// deliberately unavailable here, because a fix-pr run is admitted
	// against a live PR, so a draft at handoff is a settled-by-human
	// anomaly, and the draft continuation's message is source-issue-
	// shaped. pr carries the draft observation for the verdict.
	pinnedDraft bool
	// pinnedMissing reports a fix-pr run whose pinned head carries no
	// pull request at handoff (#517): the resolve-issue creation
	// continuation is deliberately unavailable here, because the
	// executor never opens a replacement for a pinned PR — that would
	// duplicate or silently re-target the assignment.
	pinnedMissing bool
	// pinnedMismatch reports a fix-pr run whose observed pull request is
	// not the pinned assignment (#517): the forge reports another pull
	// request on the run's head, so the pinned pull request the run was
	// admitted against is unverified. The executor verifies the pinned
	// assignment only and never adopts another pull request. pr carries
	// the observed pull request so the terminal reason can name both the
	// observed and the pinned number.
	pinnedMismatch bool
	// criteria is the criterion-evidence report when the criteria check
	// ran against the open PR's body and the pinned set was non-empty.
	// nil when no set was pinned, when the set was empty (vacuous pass),
	// or when the observation is anything other than prStateOpen — the
	// body contract only applies where there is a body to check. When
	// the body fails the contract, criteria still carries the parsed
	// report (rows, derived coverage) so the repair path can quote the
	// pinned digest and surface per-status counts to the operator
	// audit.
	criteria *broker.CriteriaReport
	// criteriaErr is the structured rejection from the criterion validator
	// when the body failed the contract; nil otherwise.
	criteriaErr *broker.CriteriaError
	// coverage mirrors the criteria report's claim ("full" or "partial")
	// when the check ran; empty when the check did not.
	coverage string
	// pinnedSet is the trusted criterion set the criteria check ran
	// against. It is carried on the outcome (not just inside
	// checkCriterionEvidence) so the repair path can quote its digest,
	// IDs, and texts verbatim — the model cannot re-derive them and
	// the operator can audit the contract that was pinned. nil when no
	// set was pinned or when the set was empty (vacuous pass).
	pinnedSet *broker.CriterionSet
	// missingEvidencePaths names the file paths cited by the body's
	// implementation-evidence and regression-check rows that are absent
	// from the published head the observation verified (#367). It rides
	// the same bounded body-repair instruction the validator's own
	// CriteriaError rides (#249, #282) — the shared #170 budget covers
	// them together, so a citation of a file the published head does not
	// carry costs one repair turn, never a second one. It is the one
	// finding on this outcome that is *not* fail-closed: the check is
	// explicitly non-semantic (existence, not meaning — auditing whether
	// a cited check actually enforces its criterion stays the reviewer's
	// job), every probe that cannot be completed is treated as absence
	// of a finding, and no probe failure is ever returned as an error,
	// so the model is never handed a citation defect it cannot repair
	// by editing the body. Empty whenever the criteria check did not
	// run, when it failed (the validator's defect already names
	// everything the repair must fix), or when every cited path exists.
	missingEvidencePaths []string
	// probedHead is the exact published commit the missing-path probe read
	// from the run's own workspace (#367), kept so the repair instruction
	// and the terminal NeedsHuman reason name the revision whose tree was
	// checked rather than leaving the operator to guess which head was
	// verified. It travels with its finding and only with it: empty
	// whenever missingEvidencePaths is empty, including a probe that ran
	// and found nothing, because a passing verdict drops this outcome and
	// nothing reads the field in that case.
	probedHead string
}

// remotePublicationState is the git-side proof that the run's current commit
// is published to the remote branch (#247). A ref merely existing is not
// publication: only the remote advertising the run's own head commit proves
// the work landed, and a remote holding a different commit means local
// commits never pushed.
type remotePublicationState struct {
	published  bool
	localHead  string
	remoteHead string // empty when the branch ref is absent entirely
}

// observeRunBranchPullRequest reads every pull request whose head carries
// the run's branch through the configured forge capability
// (forge.PullRequestHeadLister, never a forge-specific CLI) and classifies
// the set (#247). The queried head identity is runHeadRepo — the run's own
// repository, or the pinned fork head repository for fix-pr runs — so the
// observation settles the fix-pr head the same identity-checked way as the
// resolve-issue head rather than skipping it (#517). The provider
// compares the full base- and head-repository
// and ref identities carried in the forge response and refuses a response
// missing either identity, so a foreign or fork head can never be mistaken
// for the run's own PR. A record that claims the owner:branch head selector
// from a different repository is kept visible and fails the observation
// closed: the executor may neither adopt it nor delete it toward a "missing"
// claim, because the listing then settles nothing about the run's own head
// (#247). An open candidate is adopted only when its actual base ref is the
// run's pinned base; an open PR on the verified head that targets another
// base fails closed rather than being adopted or duplicated (#247). A read
// failure is returned as an error: an unverifiable observation is not a
// confirmed missing PR, and the caller must fail closed or retry inside the
// bounded budget rather than claim absence (#247).
func (r reporter) observeRunBranchPullRequest(ctx context.Context) (runBranchPRObservation, error) {
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		return runBranchPRObservation{}, err
	}
	provider := github.NewProvider(forge.ProviderConfig{Name: "github", Endpoint: r.cfg.GitHubAPIBase}, client)
	pulls, foreign, err := provider.ListPullRequestsWithForeign(ctx, forge.PullRequestRef{Repo: r.cfg.Repo}, runHeadRepo(r.cfg), r.cfg.Branch)
	if err != nil {
		return runBranchPRObservation{}, err
	}
	if len(foreign) > 0 {
		return runBranchPRObservation{kind: prStateForeignHead}, nil
	}
	var opens, drafts, wrongBases, closed []*prLinkageObservation
	for _, pull := range pulls {
		observed := &prLinkageObservation{Number: pull.Number, State: strings.ToLower(pull.State), Body: pull.Body, Draft: pull.Draft}
		switch {
		case strings.EqualFold(pull.State, "open") && pull.BaseRef != r.cfg.Base:
			wrongBases = append(wrongBases, observed)
		case strings.EqualFold(pull.State, "open") && !pull.Draft:
			opens = append(opens, observed)
		case strings.EqualFold(pull.State, "open"):
			drafts = append(drafts, observed)
		default:
			closed = append(closed, observed)
		}
	}
	switch {
	case len(wrongBases) > 0:
		return runBranchPRObservation{kind: prStateWrongBase}, nil
	case len(opens)+len(drafts) > 1:
		return runBranchPRObservation{kind: prStateAmbiguous}, nil
	case len(opens) == 1:
		return runBranchPRObservation{kind: prStateOpen, open: opens[0]}, nil
	case len(drafts) == 1:
		return runBranchPRObservation{kind: prStateDraft, draft: drafts[0]}, nil
	case len(closed) > 0:
		return runBranchPRObservation{kind: prStateClosedOnly, closed: closed}, nil
	default:
		return runBranchPRObservation{kind: prStateMissing}, nil
	}
}

// verifyNoChangeClaim checks a declared no_change_needed ending against the
// assigned repair condition using current authoritative forge state before
// the executor publishes the success termination (#513). For a fix-pr run
// the pinned pull request is the repair condition: a request the forge
// reports merged or closed makes the claim moot, and an open request must be
// mergeable into its actual base — a forge-reported conflict is unsatisfied,
// and an unknown answer is not proof, because review approval and an
// unchanged head prove nothing about mergeability. The read goes through the
// configured forge read-pull-request capability, never a forge-specific CLI.
// The method returns "" when the claim may stand and otherwise a rejection
// reason for a failed ending; a claim the forge cannot verify fails closed
// and never publishes the success, and the rejection reason stands on the
// forge state, never on the coordinator's model-authored evidence.
func (r reporter) verifyNoChangeClaim(ctx context.Context) string {
	// A resolve-issue run pins no pull request: its repair condition is the
	// issue itself, and the controller settles that ending against the world
	// through AwaitingReview's human gate, so there is nothing to verify
	// here without inferring a PR from the issue number.
	if r.cfg.Mode != "fix-pr" || r.cfg.Ref < 1 {
		return ""
	}
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		return fmt.Sprintf("could not verify the no-change claim against pull request %s#%d: %v", r.cfg.Repo, r.cfg.Ref, err)
	}
	provider := github.NewProvider(forge.ProviderConfig{Name: "github", Endpoint: r.cfg.GitHubAPIBase}, client)
	if !provider.Capabilities().Available(forge.CapabilityReadPullRequest).Available {
		return fmt.Sprintf("could not verify the no-change claim against pull request %s#%d: the forge read-pull-request capability is unavailable", r.cfg.Repo, r.cfg.Ref)
	}
	pull, err := provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: r.cfg.Repo, Number: r.cfg.Ref})
	if err != nil {
		return fmt.Sprintf("could not verify the no-change claim against pull request %s#%d: %v", r.cfg.Repo, r.cfg.Ref, err)
	}
	if pull.Merged || strings.EqualFold(strings.TrimSpace(pull.State), "closed") {
		// The repair condition is moot: a merged or closed request can no
		// longer be conflicted, so the claim stands without manufacturing a
		// commit.
		return ""
	}
	if pull.Mergeable == nil {
		return fmt.Sprintf("opencode declared no change needed but pull request %s#%d mergeability against base %s is unknown; an unverified mergeability cannot settle the assignment", r.cfg.Repo, r.cfg.Ref, pull.BaseRef)
	}
	if !*pull.Mergeable {
		return fmt.Sprintf("opencode declared no change needed but pull request %s#%d still conflicts with base %s; the assignment is not satisfied", r.cfg.Repo, r.cfg.Ref, pull.BaseRef)
	}
	return ""
}

// confirmRemotePublication proves the run's current commit is published on
// the remote before the executor claims a confirmed missing PR or hands off
// as Verifying (#247). A branch ref merely existing is not publication: the
// remote must advertise the run's own head commit, so a stale remote branch
// (local commits never pushed) fails the check exactly like an absent one.
// Only a published head can be missing a pull request or carry publishable
// review work, and a failed check is unverifiable, not proof either way.
func (r reporter) confirmRemotePublication(ctx context.Context) (remotePublicationState, error) {
	localHead, err := (&git.Workspace{Directory: r.cfg.Directory}).Head(ctx)
	if err != nil {
		return remotePublicationState{}, fmt.Errorf("read local head of run workspace: %w", err)
	}
	remoteHead, exists, err := git.RemoteBranchHead(ctx, r.cfg.RemoteURL, r.cfg.Branch)
	if err != nil {
		return remotePublicationState{}, fmt.Errorf("read remote head of run branch %s: %w", r.cfg.Branch, err)
	}
	return remotePublicationState{
		published:  exists && remoteHead == localHead,
		localHead:  localHead,
		remoteHead: remoteHead,
	}, nil
}

// carriesPullRequestNumber reports whether the observed pull requests
// include the run's pinned pull request number (#517). The fix-pr handoff
// uses it to tell a closed pinned pull request from a head that never
// carried the pinned assignment at all.
func carriesPullRequestNumber(observed []*prLinkageObservation, number int) bool {
	for _, pull := range observed {
		if pull != nil && pull.Number == number {
			return true
		}
	}
	return false
}

// carryRecordOfPullRequest returns the observation of the given pinned
// number within a closed-only observation set, for the diagnostics a
// superseded terminal records (#582). The fix-pr handoff only calls it
// after carriesPullRequestNumber confirmed the record exists; a set that
// somehow no longer names the number yields nil rather than a foreign
// record standing in for the pinned assignment.
func carryRecordOfPullRequest(observed []*prLinkageObservation, number int) *prLinkageObservation {
	for _, pull := range observed {
		if pull != nil && pull.Number == number {
			return pull
		}
	}
	return nil
}
