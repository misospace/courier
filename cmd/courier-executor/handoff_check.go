package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/github"
	"strings"
)

// trustedSourceIssue derives the source-issue identity the run was
// admitted for. For resolve-issue runs, it is the run's spec
// repository and ref; for fix-pr runs, it is the zero value (the
// pinned PR's existing body is the contract, and the broker's
// UpdateFixPR path enforces any change). Empty inputs fail closed.
func trustedSourceIssue(cfg config) (broker.SourceIssue, bool) {
	if !strings.EqualFold(cfg.Mode, "resolve-issue") {
		return broker.SourceIssue{}, false
	}
	owner, name, ok := splitOwnerRepo(cfg.Repo)
	if !ok || cfg.Ref <= 0 {
		return broker.SourceIssue{}, false
	}
	return broker.SourceIssue{Owner: owner, Name: name, Number: cfg.Ref}, true
}

// runHeadRepo returns the repository identity whose owner:branch head
// selector the forge listing queries: the run's own repository for
// resolve-issue runs, and the pinned fork head repository for fix-pr
// runs (guardFixPRHead's cross-fork invariant). Empty HEAD_REPO inputs
// fall back to the run repository (#517).
func runHeadRepo(cfg config) string {
	if strings.TrimSpace(cfg.HeadRepo) != "" {
		return cfg.HeadRepo
	}
	return cfg.Repo
}

// isFixPRRun reports whether the run is a genuine fix-pr run admitted
// against a pinned pull request: a fix-pr mode with a positive pinned
// reference (#517). readConfig rejects a fix-pr run that cannot name
// its pinned pull request, so the pinned reference is an admission
// invariant, not a runtime hope — a mode without one is not a fix-pr
// run this executor may verify, because there is no pinned pull
// request to re-verify.
func isFixPRRun(cfg config) bool {
	return strings.EqualFold(cfg.Mode, "fix-pr") && cfg.Ref >= 1
}

// checkHandoff is the legacy executor's pre-handoff forge observation:
// it classifies the pull requests whose head carries the run branch
// (#247) and, for the single open PR, runs the body's two shared
// contracts — authoritative source-issue linkage (#248) and, when an
// acceptance-criterion set is pinned at admission (#280), the criterion-
// evidence block contract (#249). Both contracts are evaluated
// independently against the same live body the forge read returned, so
// a body that violates both earns one bounded continuation that names
// both defects instead of two sequential ones that would burn the
// shared #170 budget in turn. The criterion half additionally runs the
// bounded, explicitly non-semantic referential check of #367 — a file
// path cited by an implementation-evidence or regression-check row must
// exist at the exact pushed head this observation just verified — which
// fails open on any probe it cannot complete and rides that same single
// bounded repair instead of a budget of its own. The forge read goes through the
// configured forge capability — an identity-checked head listing that
// compares full base and head repository identity, keeps foreign head
// claims visible, and includes every state — never through a
// forge-specific CLI, so the legacy path enforces the same contract as
// the native broker and never relies on a model-supplied PR number.
//
// The function returns (nil, nil) when a single open, non-draft PR on the
// pinned base carries the verified head, the remote advertises the run's
// current commit, and the body satisfies every applicable contract —
// the run loop may proceed. A non-nil outcome carries one of: a
// confirmed missing PR on a confirmed-published head (a continuation to
// open one, #247), a draft PR (a continuation to ready it, #247), one
// or both body-contract repairs (a single bounded continuation names
// every applicable defect), or a fail-closed world the executor must
// not paper over with a retry or a creation — a head whose current
// commits the remote does not advertise (absent or stale ref), an
// ambiguous set of open PRs, only closed/merged PRs, a foreign record
// claiming the head selector from another repository, or an open PR
// whose actual base differs from the pinned base (#247). A failed
// live observation is returned as an error, never as a confirmed
// missing PR: an unreadable forge is not evidence of absence (#247).
//
// The operator-pinned criterion envelope is settled before the forge is
// read and before the observation is classified (see pinnedCriterionSet):
// an envelope that cannot be bound to the run's trusted source identity,
// or that does not decode at all, is an admission-time fault no PR-body
// edit can repair and no retry can outlive, so it is returned as an error
// rather than as an observation-specific outcome — it must never be spent
// a #170 continuation slot (#282).
func (r reporter) checkHandoff(ctx context.Context) (*handoffCheckOutcome, error) {
	pinned, err := r.pinnedCriterionSet()
	if err != nil {
		return nil, err
	}
	source, ok := trustedSourceIssue(r.cfg)
	if !ok {
		// fix-pr runs use the broker's existing identity check, and a
		// resolve-issue run with malformed identity carries no linkage
		// contract to enforce — but it may still carry a pinned
		// criterion envelope, and an operator-admitted contract that
		// cannot be bound to any trusted issue must not be skipped
		// silently: the run would otherwise hand off as Verifying
		// having enforced neither body contract. pinnedCriterionSet
		// already settled that case above, so the only envelope-free
		// way to reach here is a run with nothing pinned to enforce —
		// or a fix-pr run, whose pinned pull request is the contract it
		// must verify.
		//
		// A fix-pr run carries no source-issue body contract, but the
		// pinned pull request itself is the handoff contract, so the
		// same identity-checked observation re-verifies it at handoff
		// (#517); an envelope already failed closed above via
		// pinnedCriterionSet.
		if isFixPRRun(r.cfg) {
			return r.checkFixPRHandoff(ctx)
		}
		return nil, nil
	}
	observation, err := r.observeRunBranchPullRequest(ctx)
	if err != nil {
		return nil, err
	}
	switch observation.kind {
	case prStateOpen:
		// The PR head's published state is part of the world being
		// verified: an open PR on a stale remote ref must not carry a
		// Verifying handoff over local commits that never pushed.
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			return nil, err
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		// Evaluate both body contracts against the same live body so a
		// single bounded continuation can name every defect the model
		// must repair in one turn (#282, #249, #248). linkageErr and
		// the criterion result aggregate on the same outcome: each
		// is non-nil exactly when its contract fails, and a body
		// that violates both earns a single combined-repair verdict
		// rather than two sequential ones.
		outcome := &handoffCheckOutcome{pr: observation.open}
		if linkErr := broker.ValidateLinkage(observation.open.Body, source); linkErr != nil {
			var le *broker.LinkageError
			if errors.As(linkErr, &le) {
				outcome.err = le
			} else {
				return nil, linkErr
			}
		}
		criteriaOutcome, critErr := r.checkCriterionEvidence(ctx, publication.remoteHead, pinned, observation.open)
		if critErr != nil {
			return nil, critErr
		}
		if criteriaOutcome != nil {
			outcome.criteria = criteriaOutcome.criteria
			outcome.criteriaErr = criteriaOutcome.criteriaErr
			outcome.coverage = criteriaOutcome.coverage
			outcome.pinnedSet = criteriaOutcome.pinnedSet
			outcome.missingEvidencePaths = criteriaOutcome.missingEvidencePaths
			outcome.probedHead = criteriaOutcome.probedHead
		}
		// A passing combined outcome — neither contract fired — means
		// the body is acceptable and the run may hand off as Verifying.
		// The nil-outcome signal the verdict path recognizes is "no
		// defects", which only happens when both validators passed
		// (or the criterion check was a vacuous pass with no defects).
		if outcome.err == nil && outcome.criteriaErr == nil && outcome.criteria == nil && outcome.pinnedSet == nil {
			return nil, nil
		}
		return outcome, nil
	case prStateDraft:
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			return nil, err
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		return &handoffCheckOutcome{draft: true, pr: observation.draft}, nil
	case prStateAmbiguous:
		return &handoffCheckOutcome{ambiguous: true}, nil
	case prStateClosedOnly:
		return &handoffCheckOutcome{closedOnly: true}, nil
	case prStateForeignHead:
		return &handoffCheckOutcome{foreignHead: true}, nil
	case prStateWrongBase:
		return &handoffCheckOutcome{wrongBase: true}, nil
	case prStateMissing:
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			// The head's publication state is unverifiable: that is not
			// proof either way, so it surfaces as a read failure the
			// caller fails closed on, never as a confirmed missing PR.
			return nil, fmt.Errorf("confirm remote publication of run branch %s: %w", r.cfg.Branch, err)
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		return &handoffCheckOutcome{missing: true}, nil
	}
	// Unreachable: every observation kind is handled above.
	return nil, fmt.Errorf("unclassified pull-request observation state for run branch %s", r.cfg.Branch)
}

// checkFixPRHandoff is the fix-pr run's pre-handoff observation (#517):
// the same identity-checked head listing as the resolve-issue branch
// (#247) classifies the pull requests that carry the run's pinned head,
// and every world that does not match the handoff contract — an open,
// non-draft pull request on the pinned base with the run's commit
// published (the same #247 publication sha-proof) — fails closed to a
// NeedsHuman world the executor never papers over with a retry or a
// creation. Draft and missing differ deliberately from resolve-issue:
// a fix-pr run is admitted against a live PR, so a draft at handoff is
// a settled-by-human anomaly — no readying continuation — and the
// executor never opens a replacement for a pinned PR, which would
// duplicate or silently re-target the assignment. An unverifiable read
// is returned as an error, never as a confirmed missing PR (#247).
//
// The listing settles the run's HEAD, not the run's ASSIGNMENT: a fix-pr
// run is admitted against one pinned pull request (COURIER_REF), so the
// observed pull request must be that exact pull request before the
// handoff may verify it — the same exact-number, base, and head identity
// the native broker path enforces in checkPR/matchesPR. A head whose
// pull request is some other pull request fails closed (pinnedMismatch)
// instead of handing off a verification of an assignment nobody made.
// An unverifiable read is returned as an error, never as a confirmed
// missing PR (#247).
func (r reporter) checkFixPRHandoff(ctx context.Context) (*handoffCheckOutcome, error) {
	observation, err := r.observeRunBranchPullRequest(ctx)
	if err != nil {
		return nil, err
	}
	switch observation.kind {
	case prStateOpen:
		// The pinned assignment is the pull request the run was admitted
		// against, so the head's only open pull request must be that
		// pull request: verifying a different one would hand off a
		// contract the operator never pinned (#517).
		if observation.open.Number != r.cfg.Ref {
			return &handoffCheckOutcome{pinnedMismatch: true, pr: observation.open}, nil
		}
		// Same #247 rule as resolve-issue: the pinned head's published
		// state is part of the world being verified, so no Verifying
		// handoff may carry local commits the remote never advertised.
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			return nil, err
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		// The pinned PR is open, non-draft, on the pinned base, under
		// the verified head, and the run's commit is published: hand off.
		return nil, nil
	case prStateWrongBase:
		return &handoffCheckOutcome{wrongBase: true}, nil
	case prStateAmbiguous:
		return &handoffCheckOutcome{ambiguous: true}, nil
	case prStateClosedOnly:
		// A closed-only head settles the assignment only when the closed
		// pull request IS the pinned one; a head that never carried the
		// pinned pull request leaves the assignment unverified rather
		// than closed (#517).
		if !carriesPullRequestNumber(observation.closed, r.cfg.Ref) {
			return &handoffCheckOutcome{pinnedMismatch: true, pr: observation.closed[0]}, nil
		}
		// The listing settles that the pinned request is terminal, not
		// which terminal state: GitHub reports a merged request as
		// closed. The authoritative read of the pinned assignment
		// discriminates the two worlds (#582): a pinned request that
		// merged during the run superseded the repair assignment while
		// the run worked, the same moot world the startup guard settles
		// as NoChangeNeeded before a turn ever starts (#515), so the
		// verdict ends the run as superseded terminal work instead of a
		// verifier-rejection that demands a human for a world the world
		// already settled. A closed-unmerged request keeps the #247
		// fail-closed ending — reopening or duplicating it is a human
		// settlement. A read that cannot complete proves nothing about
		// which terminal state the pinned request holds, so it surfaces
		// as the bounded retry/forge-unavailable path (#247, #455) and
		// never a guessed merge.
		merged, err := r.pinnedPullRequestMerged(ctx)
		if err != nil {
			return nil, err
		}
		if merged {
			return &handoffCheckOutcome{pinnedMerged: true, pr: carryRecordOfPullRequest(observation.closed, r.cfg.Ref)}, nil
		}
		return &handoffCheckOutcome{closedOnly: true}, nil
	case prStateForeignHead:
		return &handoffCheckOutcome{foreignHead: true}, nil
	case prStateDraft:
		// A draft is a settled-by-human anomaly only when it is the
		// pinned pull request; a draft on the run's head that is some
		// other pull request leaves the assignment unverified (#517).
		if observation.draft.Number != r.cfg.Ref {
			return &handoffCheckOutcome{pinnedMismatch: true, pr: observation.draft}, nil
		}
		return &handoffCheckOutcome{pinnedDraft: true, pr: observation.draft}, nil
	case prStateMissing:
		return &handoffCheckOutcome{pinnedMissing: true}, nil
	}
	// Unreachable: every observation kind is handled above.
	return nil, fmt.Errorf("unclassified fix-pr pull-request observation state for run branch %s", r.cfg.Branch)
}

// pinnedPullRequestMerged authoritatively reads the run's pinned pull
// request through the configured forge read-pull-request capability —
// never a forge-specific CLI — and reports whether the forge says it
// merged (#582). The head listing reports a merged request as closed, so
// only this exact-number read tells the merged world (the repair
// assignment superseded by the merge) from the closed-unmerged world
// (reopening or duplicating is a human settlement). Any read failure —
// an unavailable capability, a transport or decode fault, a response
// naming a different pull request — is returned as an error: a read that
// did not complete proves nothing about which terminal state the pinned
// request holds, and the caller must retry inside the bounded budget or
// fail closed, never guess a merge.
func (r reporter) pinnedPullRequestMerged(ctx context.Context) (bool, error) {
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		return false, err
	}
	provider := github.NewProvider(forge.ProviderConfig{Name: "github", Endpoint: r.cfg.GitHubAPIBase}, client)
	if !provider.Capabilities().Available(forge.CapabilityReadPullRequest).Available {
		return false, fmt.Errorf("the forge read-pull-request capability is unavailable, so pull request #%d of %q cannot be authoritatively read", r.cfg.Ref, r.cfg.Repo)
	}
	pr, err := provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: r.cfg.Repo, Number: r.cfg.Ref})
	if err != nil {
		return false, err
	}
	if pr.Number != r.cfg.Ref {
		return false, fmt.Errorf("pull request read for %s#%d returned number %d; the response does not name the pinned assignment", r.cfg.Repo, r.cfg.Ref, pr.Number)
	}
	return pr.Merged, nil
}

// pinnedCriterionSet returns the run's operator-pinned criterion envelope,
// decoded from COURIER_CRITERIA and bound to the run's trusted
// COURIER_REPO/COURIER_REF source identity, or nil when no envelope is
// pinned. It is the admission-time projection check the pre-handoff plan
// runs before it reads the forge (#282): the envelope is operator-authored
// from the persisted publication policy and read once from the pod
// environment at process start, so no live observation can change it, no
// PR-body edit can repair it, and no retry inside the bounded budget can
// outlive it. Settling it here — rather than inside the open-PR body check
// — is what keeps an envelope fault from hiding behind a missing, draft,
// unpublished, or otherwise fail-closed forge world and spending a #170
// slot on a fault no continuation can fix.
//
// Failure classes:
//   - No envelope pinned: nil, nil. The run carries no evidence contract.
//   - No bindable trusted identity, with an envelope pinned:
//     criteriaEnvelopeIdentityErr. An unbound contract must not be skipped
//     silently, or the run hands off Verifying having enforced neither
//     body contract.
//   - The envelope does not decode into a broker.CriterionSet:
//     criteriaEnvelopeDecodeErr. applyHandoffPlan emits the shared
//     EventLinkageReadFailed with kind:"criteria" — distinct from the
//     linkage observation read failure — so an operator can tell which
//     pre-handoff read failed without parsing error text.
//   - The envelope names a source issue other than the run's trusted one:
//     criteriaEnvelopeIdentityErr, naming both identities so the operator
//     can tell which admission input was wrong. Checked before the
//     pinned-empty shortcut, so a foreign envelope cannot slip through as
//     a vacuous pass.
//
// All three faults fail closed to NeedsHuman without consuming the #170
// budget (#282); the genuine linkage forge-read failure keeps its
// retry-inside-budget path.
func (r reporter) pinnedCriterionSet() (*broker.CriterionSet, error) {
	if strings.TrimSpace(r.cfg.Criteria) == "" {
		return nil, nil
	}
	source, ok := trustedSourceIssue(r.cfg)
	if !ok {
		return nil, fmt.Errorf("%w: the run carries no valid trusted source identity, so the pinned criterion envelope cannot be bound",
			criteriaEnvelopeIdentityErr)
	}
	var set broker.CriterionSet
	if err := json.Unmarshal([]byte(r.cfg.Criteria), &set); err != nil {
		return nil, fmt.Errorf("%w: %v", criteriaEnvelopeDecodeErr, err)
	}
	if !criteriaSetMatchesSource(set, source) {
		envelopeTarget := "(no source issue in envelope)"
		if !set.Issue.IsZero() {
			envelopeTarget = sourceIssueTarget(set.Issue)
		}
		return nil, fmt.Errorf("%w: envelope names %s, the run's trusted source is %s",
			criteriaEnvelopeIdentityErr, envelopeTarget, sourceIssueTarget(source))
	}
	return &set, nil
}

// checkCriterionEvidence validates the live PR body's evidence block
// against the already-decoded, already-bound pinned criterion set (#249,
// #280). It is the second body contract the legacy executor applies to
// the open PR's body after authoritative linkage (#248): a head whose
// linkage already failed cannot pass a body check, but a head whose
// linkage passed but whose evidence block does not satisfy the pinned set
// still must be repaired before handoff. The set is operator-authored at
// admission (pinnedCriterionSet); the validator's comparison target is
// never model-controlled.
//
// Behavior:
//   - No envelope pinned, or an empty pinned set (an issue without an
//     acceptance section, or a set with zero criteria): vacuous pass.
//     Return nil so the planner carries forward as if no set were pinned;
//     linkage is the only PR-body contract for that run.
//   - Body validation error: a *broker.CriteriaError is recorded on the
//     outcome (criteriaErr); a partial-success report is also kept on
//     the outcome (criteria) so the repair path can quote the rows and
//     per-status counts, and any non-CriteriaError is returned as a plain
//     error. The pinned set is always attached (pinnedSet) so the repair
//     path can name its digest and the verbatim criterion texts the model
//     must satisfy (#282).
//   - On success, populate outcome.criteria with the report and
//     outcome.coverage with its claimed coverage, and run the referential
//     check of #367 against head (see missingEvidencePaths below).
//
// checkCriterionEvidence is only called inside the prStateOpen branch of
// checkHandoff (#249): no PR — or a draft, ambiguous, closed-only,
// foreign, wrong-base, or unpublished head — has a body that the
// validator can compare against the pinned set. The envelope faults
// themselves are settled before the observation (pinnedCriterionSet), so
// this function never decodes and never fails on the envelope.

// criteriaEnvelopeDecodeErr is the typed sentinel the criterion-envelope
// decoder returns when the operator-authored COURIER_CRITERIA envelope is
// not a valid CriterionSet (#280, #282). The pre-handoff plan emits the
// shared EventLinkageReadFailed event with kind:"criteria" for this class,
// distinct from the linkage observation read failure, so an operator can
// tell which pre-handoff read failed without parsing the error text. The
// envelope is read once from the pod environment at process start, so no
// retry can change it: the plan fails closed to NeedsHuman without
// spending the #170 continuation budget rather than retrying inside it.
var criteriaEnvelopeDecodeErr = errors.New("decode COURIER_CRITERIA envelope")

// criteriaEnvelopeIdentityErr is the typed sentinel the criterion-envelope
// decoder returns when the operator-authored COURIER_CRITERIA envelope
// names a source issue other than the run's trusted source identity, or
// when the run carries no bindable trusted identity at all (#282). It is a
// policy mismatch between two operator-side inputs — the admission
// projection and the run's pinned repo/ref — not a defect the model can
// repair by editing the PR body, so the pre-handoff plan fails closed to
// NeedsHuman on sight rather than spending a bounded continuation on it.
var criteriaEnvelopeIdentityErr = errors.New("COURIER_CRITERIA envelope disagrees with the run's trusted source issue")

// criteriaSetMatchesSource reports whether a decoded criterion envelope
// carries the run's trusted source-issue identity. The comparison is the
// executor's trust binding: the envelope is operator-authored at
// admission, but a stale or misprojected envelope naming a different
// issue must never become the contract a PR body is validated against,
// because the linkage contract and the evidence contract must anchor to
// the same issue (#282). Ownership letters are compared case-
// insensitively, mirroring the forge's own repository identity.
func criteriaSetMatchesSource(set broker.CriterionSet, source broker.SourceIssue) bool {
	return strings.EqualFold(set.Issue.Owner, source.Owner) &&
		strings.EqualFold(set.Issue.Name, source.Name) &&
		set.Issue.Number == source.Number
}

func (r reporter) checkCriterionEvidence(ctx context.Context, head string, set *broker.CriterionSet, observation *prLinkageObservation) (*handoffCheckOutcome, error) {
	if set == nil || set.IsEmpty() {
		return nil, nil
	}
	// Carry the pinned set on the outcome so the repair path can
	// quote its digest, IDs, and texts verbatim. The set is
	// operator-authored; passing it through the outcome is a
	// reference hand-off, not a copy that the model could rewrite.
	report, err := broker.ValidateCriterionEvidence(observation.Body, *set)
	if err != nil {
		var ce *broker.CriteriaError
		if errors.As(err, &ce) {
			coverage := ""
			if report != nil {
				coverage = report.Coverage
			}
			// The validator already failed the block, and its own
			// structured reason names every defect the repair must fix,
			// so no path is probed here (#367): a row that cites a
			// missing file is only reported on the success path, never
			// double-reported behind a rejection the model has not
			// reached yet.
			return &handoffCheckOutcome{
				pr:          observation,
				criteria:    report,
				criteriaErr: ce,
				coverage:    coverage,
				pinnedSet:   set,
			}, nil
		}
		return nil, err
	}
	coverage := ""
	if report != nil {
		coverage = report.Coverage
	}
	// Only a block the validator accepted is probed for existence
	// (#367), and the probe is skipped entirely when there is no
	// published head to read: the observation only reaches this branch
	// once confirmRemotePublication proved the remote advertises the
	// run's own head commit, so head is a real commit in the local
	// workspace, and an empty one means the caller had nothing to
	// verify — failing open there keeps the vacuous case (no head, no
	// rows, no pinned set) exactly as it was before the check existed.
	missing, probed := r.missingEvidencePaths(ctx, report, head)
	return &handoffCheckOutcome{
		pr:                   observation,
		criteria:             report,
		coverage:             coverage,
		pinnedSet:            set,
		missingEvidencePaths: missing,
		probedHead:           probed,
	}, nil
}
