package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	courierlog "github.com/misospace/courier/internal/log"
)

// fixPRUnsettledReason composes the NeedsHuman reason for a fix-pr
// pre-handoff observation that named none of the known fail-closed worlds
// (#517). The fix-pr observation settles exactly one world or passes, so
// an outcome this plan cannot read is one the executor must not turn into
// a Verifying handoff over an unverified pull request.
func (r reporter) fixPRUnsettledReason(outcome *handoffCheckOutcome) string {
	number := 0
	if outcome != nil && outcome.pr != nil {
		number = outcome.pr.Number
	}
	return fmt.Sprintf("the fix-pr pre-handoff observation did not settle a known pull-request world (observed pull request #%d on branch %q); the executor will not hand off over a pull request it did not settle; the run needs a human", number, r.cfg.Branch)
}

// handoffVerdict is the legacy executor's pre-handoff forge-observation
// decision for a Verifying candidate. Pass and FailReason are mutually
// exclusive with ContinueMsg: the run loop interprets exactly one signal
// at a time. Pass means the observation verified an open, non-draft PR
// on the confirmed head with a body that satisfies linkage (and the
// criterion-evidence contract when a set is pinned), and the run may
// hand off as Verifying. ContinueMsg names the bounded instruction the
// next continuation should feed the captured session — open the missing
// PR, ready the draft PR, repair a disagreeing body or criterion block,
// or re-verify after an unreadable forge read; Kind names that
// instruction for the continuation event audit. PRNumber names the PR
// observed during the check, when exactly one was observed. Coverage
// mirrors the criterion-evidence block's claimed coverage when the
// criteria check ran (empty otherwise); it is event-detail only and
// never flows into the Verifying Reason line. FailReason names the
// precise NeedsHuman reason when the bounded budget is exhausted, there
// is no captured session to resume, or the observation is fail-closed
// (ambiguous, closed-only, foreign head, wrong base, a head whose
// current commits the remote does not advertise, or a body whose
// criterion-evidence block cannot be repaired) (#247, #248, #249).
// ForgeUnavailable is true when the forge read failed on a transient
// availability condition — a rate limit, a server error, or a network
// failure — and no in-session retry is possible, so the world was never
// observed: the run then ends blocked_external for the operator's
// bounded retry rather than NeedsHuman, which is reserved for worlds
// the observation actually read and found contradictory (#455). It is
// never set alongside a retryable verdict.
type handoffVerdict struct {
	Pass        bool
	ContinueMsg string
	Kind        string
	PRNumber    int
	// Coverage is set when the criteria check ran against the open PR's
	// body and the pinned set was non-empty; it mirrors the validator's
	// claimed coverage ("full" or "partial"). Empty when no set was
	// pinned, when the set was empty (vacuous pass), or when the
	// observation was anything other than prStateOpen.
	Coverage string
	// StatusCounts is set when the criteria check produced a parseable
	// evidence block; it is the per-status record count from the
	// CriteriaReport (#249, #282). The run.exit event detail carries
	// it alongside criteria.coverage so an operator audits the run's
	// honesty without the legacy handoff falsely claiming fully
	// verified completion outside the PR body. nil when the
	// criteria check did not run.
	StatusCounts map[broker.CriterionStatus]int
	FailReason   string
	// ForgeUnavailable marks a transient forge-availability ending
	// (#455); see the struct doc above.
	ForgeUnavailable bool
	// Superseded marks the fix-pr world where the pinned pull request
	// merged during the run (#582): the merge authoritatively settles
	// the repair assignment, so the run ends as truthful terminal work
	// (the same NoChangeNeeded ending the startup guard gives a pinned
	// request already merged at admission, #515) rather than a
	// NeedsHuman solely because it merged. It is never a Verifying
	// handoff: commits the merged head does not carry are not claimed
	// as shipped. PRNumber carries the pinned pull request.
	Superseded bool
}

// applyHandoffPlan runs the identity-checked pre-handoff forge
// observation and returns the verdict the run loop should follow. It is
// shared by the declared (post-classify) and the undeclared-Verifying
// paths so both routes share one bounded mechanism (#248, #247, #249):
//   - A single open, non-draft PR on the verified head passes when its
//     body satisfies the trusted source-issue linkage (#248) and, when
//     a criterion set is pinned at admission, the criterion-evidence
//     block contract (#249, #280). It fails to a bounded body-repair
//     continuation when either body contract fails.
//   - A confirmed missing PR on a confirmed-published head asks the
//     captured session to open the intended PR (#247); a confirmed
//     draft PR asks it to ready that exact PR (#247). Both consume the
//     #170 continuation budget and terminate with a bounded reason on
//     true exhaustion or when no session can be resumed.
//   - An ambiguous set of open PRs, a head with only closed/merged
//     PRs, a head whose current commits the remote does not advertise
//     (absent or stale ref), a forge record claiming the head selector
//     from another repository, and an open PR that targets a base other
//     than the pinned base are fail-closed on sight: no retry and no
//     creation over an unverifiable or foreign world (#247).
//   - A failed live observation is never represented as a confirmed
//     missing PR: it retries inside the bounded budget with a factual
//     "could not verify" message. When no retry is possible, a read
//     that failed on a transient availability condition — a rate limit,
//     a server error, or a network failure — ends blocked_external for
//     the operator's bounded retry, because the world was never
//     observed (#455); any other read failure fails closed to
//     NeedsHuman (#247, #248).
//   - A fix-pr run has no trusted source issue to enforce the body
//     contracts against (#517), so checkHandoff instead re-verifies
//     its pinned head through the same listing: only the run's pinned
//     pull request — the exact number the run was admitted against,
//     open, non-draft, on the pinned base, with the run's commit
//     published — passes, every other world fails closed to
//     NeedsHuman (a pull request that is not the pinned assignment,
//     draft and missing without the resolve-issue continuations), a
//     closed-only head carrying the pinned request is read
//     authoritatively so a request that merged during the run ends the
//     run as Superseded terminal work rather than a human-required
//     failure (#582), and a resumable read failure retries with the
//     fix-pr unverified message naming the pinned identity.
//   - Two operator-side envelope faults are not read failures and not
//     model-repairable defects, so they fail closed to NeedsHuman on
//     sight without consuming the #170 budget (#282). The
//     linkage.read.failed event keeps its kind detail as the
//     discriminator — "criteria-identity" for an envelope that cannot be
//     bound to the run's trusted source identity (wrong issue named, or
//     no valid trusted identity at all), "criteria" for an envelope that
//     does not decode at all, "linkage" for the genuine forge-read
//     failure that retries inside the budget. Both envelope faults are
//     admission-time projections read once from the pod environment, so
//     the model can neither repair them by editing the PR body nor outlast
//     them with a retry.
//
// The call site signals the budget consumed so far through continuations
// (so the helper consumes one slot when it asks for a continue); ctxErr
// is the run context's error as the caller observed it (non-nil when the
// run is already shutting down), and the helper folds in the live
// context error observed after the forge read so a cancellation that
// occurred during the read still gates the resume path. sessionID gates
// the resume path.
func (r reporter) applyHandoffPlan(ctx context.Context, continuations int, maxContinuations int, sessionID string, ctxErr error) handoffVerdict {
	linkOutcome, linkErr := r.checkHandoff(ctx)
	// The caller's ctxErr snapshot predates the forge read: shutdown can
	// cancel the run context while that read is in flight, so the live
	// error is folded in before any verdict decision — a continuation
	// decided on a stale nil would hand a canceled context back to the
	// session loop.
	if ctxErr == nil {
		ctxErr = ctx.Err()
	}
	if verdict, faulted := r.envelopeFaultVerdict(linkErr); faulted {
		return verdict
	}
	if linkErr != nil {
		return r.linkFailureVerdict(linkErr, continuations, maxContinuations, sessionID, ctxErr)
	}
	if linkOutcome == nil {
		return handoffVerdict{Pass: true}
	}
	// Fail-closed worlds first: none of these is a condition the model
	// may resolve by retrying or creating (#247).
	if verdict, failClosed := r.failClosedWorldVerdict(linkOutcome); failClosed {
		return verdict
	}
	// A fix-pr observation settles one world or none: it either passes
	// (no outcome at all, handled above) or names exactly one of the
	// fail-closed worlds above. An outcome that names none of them is a
	// world this plan cannot read, and the resolve-issue machinery below
	// — the ready/create continuations and the body-contract verdict —
	// does not apply to a fix-pr run, whose assignment is the pinned
	// pull request rather than a pull-request body. Reading it as a
	// Verifying handoff would verify a pull request nobody pinned, so
	// it fails closed instead (#517).
	if isFixPRRun(r.cfg) {
		return handoffVerdict{FailReason: r.fixPRUnsettledReason(linkOutcome)}
	}
	resumable := sessionID != "" && ctxErr == nil && continuations < maxContinuations
	return r.openWorldVerdict(linkOutcome, resumable, sessionID, ctxErr, continuations, maxContinuations)
}

// envelopeFaultVerdict settles the two operator-side envelope faults: a
// pinned criteria envelope that disagrees with the run's trusted source
// identity, and one that did not decode. Neither is a forge read failure
// the captured session can re-verify nor a body defect it can repair, so
// both fail closed on sight without consuming the #170 budget (#282).
func (r reporter) envelopeFaultVerdict(linkErr error) (handoffVerdict, bool) {
	if errors.Is(linkErr, criteriaEnvelopeIdentityErr) {
		// The operator-pinned envelope disagrees with the run's trusted
		// source identity. That is not a forge read failure the captured
		// session can re-verify, and not a body defect it can repair:
		// the two operator-side inputs must be reconciled by a human, so
		// the plan fails closed on sight without consuming the #170
		// budget (#282).
		r.eventVerbose(courierlog.EventLinkageReadFailed, courierlog.StatusError, map[string]any{
			"kind":  "criteria-identity",
			"error": linkErr.Error(),
		})
		return handoffVerdict{FailReason: r.criteriaIdentityMismatchReason(linkErr)}, true
	}
	if errors.Is(linkErr, criteriaEnvelopeDecodeErr) {
		// The pinned envelope did not decode. It was read once from the
		// pod environment at process start, so no live observation can
		// change it and no retry inside the #170 budget can succeed;
		// spending a shared slot on it would also misattribute an
		// admission-time projection fault to the pull-request body.
		// Fail closed on sight like the identity mismatch above, keeping
		// the same event and kind:"criteria" discriminator so an operator
		// can still tell the envelope decode from the linkage read.
		r.eventVerbose(courierlog.EventLinkageReadFailed, courierlog.StatusError, map[string]any{
			"kind":  "criteria",
			"error": linkErr.Error(),
		})
		return handoffVerdict{FailReason: r.criteriaEnvelopeDecodeReason(linkErr)}, true
	}
	return handoffVerdict{}, false
}

// linkFailureVerdict settles a failed linkage forge read: the
// identity-checked PR listing or the remote-publication confirmation
// behind the observation. The envelope decode and identity-mismatch
// classes were already handled fail-closed by the caller, so this path
// is the linkage observation alone and kind:"linkage" is the
// discriminator.
func (r reporter) linkFailureVerdict(linkErr error, continuations, maxContinuations int, sessionID string, ctxErr error) handoffVerdict {
	r.eventVerbose(courierlog.EventLinkageReadFailed, courierlog.StatusError, map[string]any{
		"kind":  "linkage",
		"error": linkErr.Error(),
	})
	if continuations >= maxContinuations || sessionID == "" || ctxErr != nil {
		if forge.IsTransientAvailability(linkErr) {
			// The read could not complete on a transient
			// availability condition, so the world was never
			// observed: the run ends blocked_external for the
			// operator's bounded retry instead of escalating to
			// a human as a verification rejection (#455).
			return handoffVerdict{FailReason: r.forgeUnavailableReason(linkErr), ForgeUnavailable: true}
		}
		// A fix-pr run has no trusted source issue for the linkage
		// failure reason to cite (#517): its unresolvable read
		// failure names the pinned head identity in the same
		// not-a-missing-PR terms (#247).
		if isFixPRRun(r.cfg) {
			return handoffVerdict{FailReason: r.fixPRForgeFailureReason(linkErr)}
		}
		return handoffVerdict{FailReason: r.linkageForgeFailureReason(linkErr)}
	}
	// The verifier could not read the forge state at all. That is
	// not a confirmed missing PR (#247), so the retry message says
	// what is known — the observation failed — rather than asserting
	// absence; the captured session re-verifies and repairs inside
	// the bounded budget. A fix-pr run has no source issue for that
	// message to name (#517), so its instruction asserts the pinned
	// head identity instead, under the same "pr-unverified" kind
	// for the event audit.
	next := continuations + 1
	if isFixPRRun(r.cfg) {
		return handoffVerdict{Kind: "pr-unverified", ContinueMsg: r.fixPRUnverifiedMessage(linkErr, next)}
	}
	return handoffVerdict{Kind: "pr-unverified", ContinueMsg: r.prUnverifiedMessage(r.sourceIssue(), linkErr, next)}
}

// failClosedWorldVerdict maps an observation that names one of the
// fail-closed worlds to its fixed consequence: an ambiguous set, a
// closed-only head, a foreign head, a wrong base, a head the remote does
// not advertise, or a fix-pr pinned world (missing, mismatched, or draft)
// no resolve-issue continuation may stand in for (#247, #517). A merged
// pinned assignment is not a failure world: the merge settled the repair
// condition during the run, so the verdict ends superseded (#582).
func (r reporter) failClosedWorldVerdict(linkOutcome *handoffCheckOutcome) (handoffVerdict, bool) {
	switch {
	case linkOutcome.pinnedMerged:
		// Not a failure world at all: the authoritative read of the
		// pinned assignment settled the repair condition as merged
		// during the run, so the verdict ends the run as superseded
		// terminal work instead of demanding a human for a world the
		// merge already settled (#582). It still short-circuits the
		// switch because no Verifying handoff may follow.
		return handoffVerdict{PRNumber: r.cfg.Ref, Superseded: true}, true
	case linkOutcome.ambiguous:
		return handoffVerdict{FailReason: r.prAmbiguousReason()}, true
	case linkOutcome.closedOnly:
		return handoffVerdict{FailReason: r.closedOnlyPRReason()}, true
	case linkOutcome.foreignHead:
		return handoffVerdict{FailReason: r.foreignHeadPRReason()}, true
	case linkOutcome.wrongBase:
		return handoffVerdict{FailReason: r.wrongBasePRReason()}, true
	case linkOutcome.unpublished:
		return handoffVerdict{FailReason: r.unpublishedBranchReason(linkOutcome.publication)}, true
	case linkOutcome.pinnedMissing:
		// fix-pr-only worlds whose fixed consequences no resolve-issue
		// continuation may stand in for (#517): a missing pinned PR is
		// never opened as a replacement, and a draft pinned PR is
		// never readied unilaterally.
		return handoffVerdict{FailReason: r.pinnedPRMissingReason()}, true
	case linkOutcome.pinnedMismatch:
		// The observed pull request is not the pinned assignment (#517):
		// the head's pull request is never a substitute for the pull
		// request the run was admitted against, so the handoff fails
		// closed instead of verifying an assignment nobody pinned. The
		// verdict carries no PR number: the observed pull request is
		// not this run's pull request, and the reason names both
		// numbers instead.
		return handoffVerdict{FailReason: r.pinnedPRNumberMismatchReason(r.cfg.Ref, linkOutcome.pr.Number)}, true
	case linkOutcome.pinnedDraft:
		return handoffVerdict{PRNumber: linkOutcome.pr.Number, FailReason: r.pinnedPRDraftReason(linkOutcome.pr.Number)}, true
	}
	return handoffVerdict{}, false
}

// openWorldVerdict settles a prStateOpen observation: the PR is open, the
// remote is published, and zero, one, or both body contracts fired. The
// verdict path aggregates every applicable defect into one bounded
// continuation so a body that violates both contracts earns one repair
// turn (with linkage named first as the structurally prior contract)
// rather than two sequential repairs that would burn the shared #170
// budget in turn (#282).
//
// The criterion half of that contract is no longer the validator's
// error alone since #367: a block the validator accepted whose citations
// name files absent from the published head is the same repairable defect
// class, so it is folded into one predicate and the three cases below keep
// their meaning. One predicate, three call sites: a fourth spelling of
// "did the criterion half find something" is exactly how the combined and
// single-contract verdicts would drift apart again.
func (r reporter) openWorldVerdict(linkOutcome *handoffCheckOutcome, resumable bool, sessionID string, ctxErr error, continuations, maxContinuations int) handoffVerdict {
	switch {
	case linkOutcome.draft:
		if !resumable {
			return handoffVerdict{PRNumber: linkOutcome.pr.Number, FailReason: r.draftUnresolvedReason(linkOutcome.pr.Number, sessionID, ctxErr)}
		}
		return handoffVerdict{Kind: "pr-draft", PRNumber: linkOutcome.pr.Number, ContinueMsg: r.prDraftMessage(r.sourceIssue(), linkOutcome.pr.Number, continuations+1)}
	case linkOutcome.missing:
		if !resumable {
			return handoffVerdict{FailReason: r.linkageExhaustedReason(linkOutcome)}
		}
		return handoffVerdict{Kind: "pr-missing", ContinueMsg: r.prMissingMessage(r.sourceIssue(), continuations+1)}
	}
	criteriaDefect := linkOutcome.criteriaErr != nil || len(linkOutcome.missingEvidencePaths) > 0
	switch {
	case linkOutcome.err != nil && criteriaDefect:
		if !resumable {
			// Budget exhausted: name every defect so the operator
			// can settle the run with a single read.
			return handoffVerdict{
				PRNumber:   linkOutcome.pr.Number,
				FailReason: r.combinedBodyExhaustedReason(linkOutcome),
			}
		}
		return handoffVerdict{
			Kind:        "body-repair",
			PRNumber:    linkOutcome.pr.Number,
			ContinueMsg: combinedBodyRepairMessage(r.sourceIssue(), linkOutcome, continuations+1, maxContinuations),
		}
	case linkOutcome.err != nil:
		if !resumable {
			return handoffVerdict{PRNumber: linkOutcome.pr.Number, FailReason: r.linkageExhaustedReason(linkOutcome)}
		}
		return handoffVerdict{Kind: "linkage-repair", ContinueMsg: linkageRepairMessage(r.sourceIssue(), linkOutcome, continuations+1, maxContinuations), PRNumber: linkOutcome.pr.Number}
	case criteriaDefect:
		if !resumable {
			return handoffVerdict{
				PRNumber:   linkOutcome.pr.Number,
				FailReason: r.criteriaExhaustedReason(linkOutcome),
			}
		}
		return handoffVerdict{
			Kind:        "criteria-repair",
			PRNumber:    linkOutcome.pr.Number,
			ContinueMsg: criteriaRepairMessage(r.sourceIssue(), linkOutcome, continuations+1, maxContinuations),
		}
	}
	// Both contracts passed: either the criteria check ran and
	// produced a report (so the verdict carries its coverage claim
	// and per-status counts for the run.exit audit), or no criteria
	// set was pinned and the body contract was vacuous. In either
	// case the run may hand off as Verifying — the coverage claim
	// rides the event detail for the operator audit but never the
	// Reason line.
	return handoffVerdict{
		Pass:         true,
		PRNumber:     linkOutcome.pr.Number,
		Coverage:     linkOutcome.coverage,
		StatusCounts: criteriaStatusCounts(linkOutcome.criteria),
	}
}
