package main

import (
	"fmt"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/broker/grammar"
	"strconv"
	"strings"
)

// sourceIssue returns the trusted source-issue identity for the run, or
// the zero value when the run is not a resolve-issue. It is the
// executor's only source of truth for the source identity; a model
// cannot influence it.
func (r reporter) sourceIssue() broker.SourceIssue {
	source, _ := trustedSourceIssue(r.cfg)
	return source
}

// sourceIssueTarget renders the trusted source-issue identity as the
// canonical owner/name#N form the continuation messages name.
func sourceIssueTarget(source broker.SourceIssue) string {
	return fmt.Sprintf("%s#%d", source.Canonical(), source.Number)
}

// linkageRepairMessage composes the precise OpenCode continuation
// message that asks the captured session to fix a published PR's body.
// The message names the exact trusted issue and the supported closing
// keyword forms so the model can repair the body deterministically
// rather than re-derive the requirement. The text is bounded to a few
// hundred bytes so it stays under the legacy executor's continuation
// message budget. The continuation number, the total budget, and the
// slots left after this turn are named so a model can see the remaining
// budget (#282).
func linkageRepairMessage(source broker.SourceIssue, outcome *handoffCheckOutcome, continuation, maxContinuations int) string {
	target := sourceIssueTarget(source)
	bare := fmt.Sprintf("#%d", source.Number)
	cross := target
	return fmt.Sprintf(
		"Courier refused the handoff: PR #%d's body does not carry an authoritative closing reference to %s. "+
			"Update the PR body through the forge capability so it carries a supported closing keyword (Closes, Fixes, or Resolves) followed by %s, or the cross-repo form %s when the trusted source repository differs from the PR base. "+
			"Then re-declare the same outcome. (continuation %s)",
		outcome.pr.Number, target, bare, cross, continuationBudget(continuation, maxContinuations),
	)
}

// criteriaRepairMessage composes the precise OpenCode continuation
// message for a criterion-evidence violation (#282). It names the
// offending PR number, the trusted source issue, the pinned set digest
// and the verbatim pinned criteria texts so a model can verify what
// it must satisfy, and the continuation budget. The vocabulary is
// composed verbatim from the broker error's reason (which
// describeCriteriaFailure built) so any change to the validator's
// vocabulary surfaces in both paths. The header shape, row shape,
// status vocabulary, and per-status rules are rendered from the broker's
// validator (criteria.go) rather than restated, so the model can repair
// the body without re-deriving the contract.
//
// A block the validator accepted but whose citations do not exist in the
// published head (#367) reaches the same instruction: the reason clause
// names the referential defect rather than falling back to the vague
// "did not satisfy the pinned set" (which would send the model hunting
// through a block the validator already passed), and one bounded
// observation sentence follows it naming the paths and the head verbatim
// plus the two honest ways out. The sentence is informational — what was
// seen, and the model's options — and introduces no rule the pinned
// contract does not already carry: citing a path that exists, or
// recording the criterion honestly.
func criteriaRepairMessage(source broker.SourceIssue, outcome *handoffCheckOutcome, continuation, maxContinuations int) string {
	target := sourceIssueTarget(source)
	reason := criteriaRepairReason(outcome)
	observation := missingEvidenceNotice(outcome)
	setDigest, criteriaLines := pinnedSetRepairSnippet(outcome.pinnedSet, outcome.criteria)
	return fmt.Sprintf(
		"Courier refused the handoff: PR #%d's body does not satisfy the criterion-evidence contract pinned at admission for %s (pinned set %s): %s.%s "+
			"Update the PR body through the forge capability so it carries the criterion-evidence block: a single line starting with %q followed by one %q row per pinned criterion (each row's digest must match the pinned criterion digest below), each carrying a closed status from the vocabulary %s; %s. Pinned criteria: %s. "+
			"Then re-declare the same outcome. (continuation %s)",
		outcome.pr.Number, target,
		nonEmptyOr(setDigest, "the admitted set"),
		reason,
		observation,
		grammar.CriteriaHeaderShape(strings.ToLower(source.Canonical())+"#"+strconv.Itoa(source.Number)),
		grammar.CriteriaRowGrammar(),
		grammar.StatusVocabularySentence(),
		grammar.StatusReferenceRuleSentence(),
		nonEmptyOr(criteriaLines, "(no pinned criteria on this run)"),
		continuationBudget(continuation, maxContinuations),
	)
}

// missingEvidenceRepairReason is the reason clause the criterion-repair
// instruction carries when the block passed the validator and only its
// citations failed the referential check of #367. It states the
// observation without listing it — the paths and the head follow in
// missingEvidenceNotice — so the reason reads as a defect while the
// detail is bounded to one sentence.
const missingEvidenceRepairReason = "the criterion-evidence block cites file paths that are absent from the published head"

// missingEvidenceNotice renders the single bounded sentence the
// criterion-repair instruction appends for a referential finding (#367):
// the paths verbatim, the short sha of the head whose tree was probed,
// an explicit statement that the check was existence-only and fetched
// nothing, and the two honest responses — cite a path that exists in
// the published head, or record the criterion honestly (a
// manual-verification row carrying a note). It is phrased as what was
// observed and what the model may do, not as a new rule: a citation the
// model believes is real must not be relabelled to silence the check.
// Returns "" when the outcome carries no finding, which leaves the
// existing message byte-identical.
func missingEvidenceNotice(outcome *handoffCheckOutcome) string {
	if len(outcome.missingEvidencePaths) == 0 {
		return ""
	}
	return fmt.Sprintf(
		" Observed at published head %s: cited file path(s) %s are not in that tree; the check was existence only, so no citation was read and no URL was fetched. "+
			"Cite a path that exists in the published head, or record the criterion honestly, for example as manual-verification with a note saying what was verified by hand.",
		shortCommitHead(outcome.probedHead), strings.Join(outcome.missingEvidencePaths, ", "),
	)
}

// missingEvidencePathsClause names the referential finding for terminal
// operator prose (#367): the paths verbatim and the short sha of the
// probed head, so a NeedsHuman the budget exhausted settles on the
// observation the run made rather than asking the operator to re-derive
// it from the body.
func missingEvidencePathsClause(outcome *handoffCheckOutcome) string {
	return fmt.Sprintf("cited file path(s) %s are absent from the published head %s",
		strings.Join(outcome.missingEvidencePaths, ", "), shortCommitHead(outcome.probedHead))
}

// criteriaRepairReason is the reason the criterion-repair instruction
// names. The validator's own reason is always preferred when it has one
// (#249); the referential check of #367 supplies the reason only for a
// block the validator already passed, so the model is never sent to
// repair a defect the validator did not find. With neither, the stable
// fallback stands (#282).
func criteriaRepairReason(outcome *handoffCheckOutcome) string {
	if outcome.criteriaErr == nil && len(outcome.missingEvidencePaths) > 0 {
		return missingEvidenceRepairReason
	}
	return describeCriteriaFailureForRepair(outcome.criteriaErr)
}

// terminalCriteriaDefectReason is the criterion half of a terminal
// NeedsHuman reason (#367): it names the validator's reason when there
// is one, otherwise the referential finding in full — paths and probed
// head — because the run is ending and the operator has no repair turn
// left to read the observation from.
func terminalCriteriaDefectReason(outcome *handoffCheckOutcome) string {
	if outcome.criteriaErr != nil {
		return describeCriteriaFailureForRepair(outcome.criteriaErr)
	}
	if len(outcome.missingEvidencePaths) > 0 {
		return missingEvidencePathsClause(outcome)
	}
	return describeCriteriaFailureForRepair(nil)
}

// continuationBudget renders the bounded-repair budget a repair
// instruction must name (#282): the number of this continuation, the
// shared #170 total, and the slots left after this turn, so a model can
// see the remaining budget rather than only its ordinal.
func continuationBudget(continuation, maxContinuations int) string {
	remaining := maxContinuations - continuation
	if remaining < 0 {
		remaining = 0
	}
	return fmt.Sprintf("%d of %d; %d remaining", continuation, maxContinuations, remaining)
}

// combinedBodyRepairMessage composes one OpenCode continuation that
// names every body-contract defect the legacy pre-handoff plan found on
// a single live body read (#282). A body that violates both linkage
// (#248) and the criterion-evidence contract (#249) earns a single
// bounded repair that names both — the shared #170 budget covers them
// together, and one turn can fix both — instead of two sequential
// continuations that burn the budget in turn. The linkage defect is
// named first because the closing reference is structurally prior: a
// body without a closing reference does not address a specific issue
// for criteria to evaluate, so the model needs to know the linkage
// contract lands first. The criterion-evidence contract is named
// second so a body that fixes both earns a single repair pass and
// passes both validators on the next observation.
func combinedBodyRepairMessage(source broker.SourceIssue, outcome *handoffCheckOutcome, continuation, maxContinuations int) string {
	linkPart := linkageRepairMessage(source, outcome, continuation, maxContinuations)
	critPart := criteriaRepairMessage(source, outcome, continuation, maxContinuations)
	return linkPart + " " + critPart
}

// describeCriteriaFailureForRepair returns the precise broker-defined
// reason the validator recorded on the outcome, or a stable fallback
// when the error is nil so the model can repair without re-deriving.
func describeCriteriaFailureForRepair(cerr *broker.CriteriaError) string {
	if cerr == nil {
		return "criterion evidence block did not satisfy the pinned set"
	}
	if cerr.Reason == "" {
		return "criterion evidence block did not satisfy the pinned set"
	}
	return cerr.Reason
}

func nonEmptyOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// pinnedSetRepairSnippet renders the pinned criterion set as a
// model-quotable snippet the repair path pastes into the criteria
// continuation message (#282). The digest is the trust anchor — the
// model must satisfy the exact pinned set, not a re-derivation — and
// the per-criterion IDs, digests, and texts are the contract a body
// row references. Both are operator-authored, never model-controlled.
// The set is reproduced verbatim, each criterion text included in full:
// #282 requires the pinned criteria verbatim, so no truncation,
// elision, or byte cap is applied. Returns (digest, snippet) — snippet
// is empty when the outcome carries no pinned set so callers can
// fall back.
func pinnedSetRepairSnippet(set *broker.CriterionSet, report *broker.CriteriaReport) (string, string) {
	if set == nil {
		// The validator may have produced a report even when no
		// pinned set was carried on the outcome (e.g. older
		// callers); the report's SetDigest is the only signal the
		// repair path has. Surface it so the message does not
		// fall back to "the admitted set" without the operator
		// being able to tell what was admitted.
		if report == nil {
			return "", ""
		}
		return report.SetDigest, ""
	}
	digest := strings.TrimSpace(set.Digest)
	if digest == "" && report != nil {
		digest = strings.TrimSpace(report.SetDigest)
	}
	if len(set.Criteria) == 0 {
		return digest, ""
	}
	parts := make([]string, 0, len(set.Criteria))
	for _, c := range set.Criteria {
		text := strings.TrimSpace(c.Text)
		if text == "" {
			text = "(criterion text not embedded in envelope)"
		}
		// The broker's Criterion.Digest already includes the
		// "sha256:" prefix; render the bare hex so the snippet
		// matches the row-shape a body must carry. Strip a
		// leading scheme to be tolerant of future renames.
		digestHex := strings.TrimPrefix(strings.TrimSpace(c.Digest), "sha256:")
		parts = append(parts, fmt.Sprintf("%s sha256:%s — %s", c.ID, digestHex, text))
	}
	return digest, strings.Join(parts, "; ")
}

// prMissingMessage composes the #247 continuation message for a
// confirmed-published run branch that carries no pull request. The
// message is factual: it names the pinned repository, the run branch,
// the base ref, and the trusted source issue, and it forbids adopting
// another actor's PR or moving an existing PR's base/head. It leaves a
// genuine external block reachable through a blocked_external
// declaration rather than forcing a creation the model cannot make.
func (r reporter) prMissingMessage(source broker.SourceIssue, continuation int) string {
	target := sourceIssueTarget(source)
	bare := fmt.Sprintf("#%d", source.Number)
	return fmt.Sprintf(
		"Courier refused the handoff: the run branch %q is published to %s, but the forge shows no pull request for that head, so the run cannot be handed off as Verifying. "+
			"Open the pull request through the forge capability with head %s against base %s, and give its body an authoritative closing reference to %s: a supported closing keyword (Closes, Fixes, or Resolves) followed by %s, or the cross-repo form %s when the trusted source repository differs from the PR base. "+
			"Do not adopt a pull request whose head belongs to another repository, and do not change an existing pull request's base or head. "+
			"If a genuine external block prevents opening the pull request, declare blocked_external naming the block. "+
			"Then re-declare the same outcome. (continuation %d)",
		r.cfg.Branch, r.cfg.Repo, r.cfg.Repo+":"+r.cfg.Branch, r.cfg.Base, target, bare, target, continuation,
	)
}

// prDraftMessage composes the #247 continuation message for a draft PR
// on the run branch: it asks the captured session to mark that exact PR
// ready for review — never to create a duplicate or move base/head —
// and leaves a genuine external block reachable through a
// blocked_external declaration.
func (r reporter) prDraftMessage(source broker.SourceIssue, number, continuation int) string {
	target := sourceIssueTarget(source)
	bare := fmt.Sprintf("#%d", source.Number)
	return fmt.Sprintf(
		"Courier refused the handoff: pull request #%d for the run branch %q on %s is still a draft, so the run cannot be handed off as Verifying. "+
			"Mark pull request #%d ready for review through the forge capability; do not create a duplicate pull request, and do not change its base or head. "+
			"Confirm its body carries an authoritative closing reference to %s: a supported closing keyword (Closes, Fixes, or Resolves) followed by %s, or the cross-repo form %s when the trusted source repository differs from the PR base. "+
			"If a genuine external block prevents readying it, declare blocked_external naming the block. "+
			"Then re-declare the same outcome. (continuation %d)",
		number, r.cfg.Branch, r.cfg.Repo, number, target, bare, target, continuation,
	)
}

// prUnverifiedMessage composes the continuation message for a failed
// live forge observation (#247). It is explicitly not a missing-PR
// claim: the read failed, which proves nothing about the pull request's
// state. The message asks the session to verify and repair through the
// forge capability and keeps the honest endings — a forge that stays
// unreachable — reachable through blocked_external.
func (r reporter) prUnverifiedMessage(source broker.SourceIssue, observationErr error, continuation int) string {
	target := sourceIssueTarget(source)
	bare := fmt.Sprintf("#%d", source.Number)
	return fmt.Sprintf(
		"Courier could not verify the pull-request state for the run branch %q on %s (%s); this is not proof that the pull request is missing. "+
			"Confirm through the forge capability that the run branch is published and carries exactly one open, non-draft pull request whose body carries an authoritative closing reference to %s: a supported closing keyword (Closes, Fixes, or Resolves) followed by %s, or the cross-repo form %s when the trusted source repository differs from the PR base. "+
			"Ready, open, or repair it as the observation requires; do not adopt a pull request whose head belongs to another repository. "+
			"If the forge stays unreachable, declare blocked_external naming the block. "+
			"Then re-declare the same outcome. (continuation %d)",
		r.cfg.Branch, r.cfg.Repo, observationErr.Error(), target, bare, target, continuation,
	)
}

// linkageExhaustedReason composes the human-readable NeedsHuman reason
// for a run whose continuation budget ran out before the model
// repaired the PR body. The reason names the trusted source identity
// and the model's attempted references so the operator can settle
// the run without re-reading the issue.
func (r reporter) linkageExhaustedReason(outcome *handoffCheckOutcome) string {
	source := r.sourceIssue()
	target := sourceIssueTarget(source)
	if outcome.missing {
		return fmt.Sprintf("the model never opened a pull request for the run branch, so the handoff was refused; the run needs a human to open one with the trusted source-issue linkage to %s", target)
	}
	if outcome.err != nil {
		observed := broker.FormatLinkageObservations(outcome.err.Observed)
		if observed == "" {
			return fmt.Sprintf("the published pull request body did not satisfy the trusted source-issue linkage to %s after the bounded repair budget: %s", target, outcome.err.Error())
		}
		return fmt.Sprintf("the published pull request body did not satisfy the trusted source-issue linkage to %s after the bounded repair budget (model wrote: %s): %s", target, observed, outcome.err.Error())
	}
	return fmt.Sprintf("the published pull request body did not satisfy the trusted source-issue linkage to %s; the run needs a human", target)
}

// criteriaIdentityMismatchReason composes the NeedsHuman reason for a run
// whose operator-pinned COURIER_CRITERIA envelope names a source issue
// other than the run's trusted COURIER_REPO/COURIER_REF identity (#282).
// The reason names both identities so the operator can settle which
// admission input was wrong, and it states plainly that no model repair
// can fix a policy mismatch — the run never spends a continuation here.
func (r reporter) criteriaIdentityMismatchReason(err error) string {
	return fmt.Sprintf("the operator-pinned criterion envelope disagrees with the run's trusted source issue and the criterion-evidence contract cannot be enforced against the wrong pinned set; this is an admission-time policy mismatch no model can repair by editing the pull-request body; the run needs a human to settle which admission input was wrong: %s", err.Error())
}

// criteriaEnvelopeDecodeReason composes the NeedsHuman reason for a run
// whose operator-pinned COURIER_CRITERIA envelope does not decode into a
// criterion set, so the criterion-evidence contract has no
// operator-authored target to enforce (#282). The envelope is read once
// from the pod environment at process start: this is an admission-time
// projection fault that no model can repair by editing the
// pull-request body and no live observation can change, so the run fails
// closed without consuming the #170 continuation budget rather than
// retrying an input that cannot move. The reason carries the decoder's
// own error so the operator can see what the envelope failed to parse.
func (r reporter) criteriaEnvelopeDecodeReason(err error) string {
	return fmt.Sprintf("the operator-pinned criterion envelope could not be decoded into a criterion set, so the criterion-evidence contract has no operator-authored target to enforce; this is an admission-time projection fault no model can repair by editing the pull-request body and no live observation can change; the run needs a human to settle the pinned envelope: %s", err.Error())
}

// criteriaExhaustedReason composes the human-readable NeedsHuman reason
// for a run whose continuation budget ran out before the model repaired
// the criterion-evidence block (#282). The reason names the trusted
// source identity so the operator can settle the run without re-reading
// the issue, and the broker-defined reason the validator produced — or a
// stable fallback when the validator's structured error is missing.
// Always surfaces the trusted source issue via sourceIssueTarget(r.sourceIssue()).
// A run that exhausted the budget on a referential finding alone (#367) names
// the absent paths and the probed head instead of the vacuous-set fallback, so
// the terminal reason states the observation the run actually made.
func (r reporter) criteriaExhaustedReason(outcome *handoffCheckOutcome) string {
	target := sourceIssueTarget(r.sourceIssue())
	if outcome != nil && outcome.criteriaErr != nil && strings.TrimSpace(outcome.criteriaErr.Reason) != "" {
		return "incomplete handoff: criterion evidence " + outcome.criteriaErr.Reason + " (trusted source issue " + target + ")"
	}
	if outcome != nil && len(outcome.missingEvidencePaths) > 0 {
		return "incomplete handoff: criterion evidence " + missingEvidencePathsClause(outcome) + " (trusted source issue " + target + ")"
	}
	return "incomplete handoff: criterion evidence block did not satisfy the pinned set (trusted source issue " + target + ")"
}

// combinedBodyExhaustedReason composes the human-readable NeedsHuman
// reason for a run whose continuation budget ran out before the model
// repaired both body contracts at once (#282). The reason names every
// defect the validator surfaced so the operator can settle the run
// without re-reading the body: the trusted source identity, the
// linkage contract rejection, and the criterion-evidence contract
// rejection. The two are joined with " and " so the operator reads
// the failure as a single combined defect, not as two parallel
// failures; the trusted source identity is named in a single trailing
// parenthetical so it cannot disagree between the two halves. The
// criterion half is the validator's reason when it has one and the
// referential finding of #367 when it does not, so a combined ending
// names every defect the run found without the model needing a repair
// turn to read them.
func (r reporter) combinedBodyExhaustedReason(outcome *handoffCheckOutcome) string {
	target := sourceIssueTarget(r.sourceIssue())
	linkPart := "linkage: " + describeLinkageFailureForReason(outcome.err)
	critPart := "criterion evidence " + terminalCriteriaDefectReason(outcome)
	return "incomplete handoff: " + linkPart + " and " + critPart + " (trusted source issue " + target + ")"
}

// describeLinkageFailureForReason returns the precise broker-defined
// reason the linkage validator recorded on the outcome, or a stable
// fallback when the error is nil so the operator can settle the run
// without re-reading the body.
func describeLinkageFailureForReason(lerr *broker.LinkageError) string {
	if lerr == nil {
		return "the published pull request body did not carry an authoritative closing reference"
	}
	if strings.TrimSpace(lerr.Error()) == "" {
		return "the published pull request body did not carry an authoritative closing reference"
	}
	return strings.TrimPrefix(strings.TrimSpace(lerr.Error()), "publication denied: ")
}

// criteriaStatusCounts returns the per-status record counts the
// run.exit event detail rides, or nil when the criteria check did
// not run. The map is JSON-marshalled with status names as keys; an
// empty map and a nil map both serialize, so the verdict never emits
// a misleading "no records" map for runs where the criteria check did
// not run. The status names mirror the broker vocabulary
// (implementation-evidence, regression-check, manual-verification,
// not-implemented, unknown) so the operator audit reads the same
// vocabulary across the legacy handoff, the native broker, and the
// PR body itself.
func criteriaStatusCounts(report *broker.CriteriaReport) map[broker.CriterionStatus]int {
	if report == nil {
		return nil
	}
	counts := report.StatusCounts()
	if len(counts) == 0 {
		return nil
	}
	return counts
}

// handoffCoverageComment renders the body the executor posts to the
// run's source issue on a passing handoff with a non-empty pinned
// criterion set (#282, step 4). The coverage and per-status counts are
// the derived values the validator produced from the PR body's evidence
// block — never a restatement of the model's claim — so the run's
// honesty is visible outside the PR body, and a partial record is
// labelled partial: the comment never presents the run as fully
// verified completion. It is rendered from the same verdict that drives
// the run.exit event detail, so telemetry and comment cannot disagree.
func handoffCoverageComment(runID, coverage string, counts map[broker.CriterionStatus]int) string {
	body := "**Courier: criterion-evidence handoff** (run " + runID + ")\n\n" +
		"The Verifying handoff passed the criterion-evidence contract pinned at admission with derived coverage `" + coverage + "`."
	if len(counts) > 0 {
		parts := make([]string, 0, len(counts))
		for _, status := range []broker.CriterionStatus{
			broker.StatusImplementationEvidence,
			broker.StatusRegressionCheck,
			broker.StatusManualVerification,
			broker.StatusNotImplemented,
			broker.StatusUnknown,
		} {
			if count, ok := counts[status]; ok && count > 0 {
				parts = append(parts, fmt.Sprintf("%s: %d", status, count))
			}
		}
		if len(parts) > 0 {
			body += " Per-status evidence records: " + strings.Join(parts, ", ") + "."
		}
	}
	return body
}

// linkageForgeFailureReason composes the human-readable NeedsHuman reason
// when the live PR state could not be verified at all — the forge read
// or the remote-publication confirmation behind the observation failed,
// so the verifier cannot prove linkage or PR presence on an unverifiable
// world (#248, #247). The run escalates to a human rather than being
// handed off as Verifying and rather than being told the PR is missing.
func (r reporter) linkageForgeFailureReason(err error) string {
	source := r.sourceIssue()
	target := sourceIssueTarget(source)
	return fmt.Sprintf("the live pull-request state could not be verified to confirm an open, non-draft pull request with authoritative source-issue linkage to %s: %s; the run needs a human", target, err.Error())
}

// forgeUnavailableReason composes the blocked-external reason for a Verifying
// candidate whose live forge read failed on a transient availability
// condition — a rate limit, a server error, or a network failure — with no
// in-session retry possible (#455). The forge was temporarily unavailable,
// so the world was never observed: the run is parked for bounded retry by
// the operator rather than failed as a verification rejection, and the
// reason never claims the pull request is missing.
func (r reporter) forgeUnavailableReason(err error) string {
	source := r.sourceIssue()
	target := sourceIssueTarget(source)
	return fmt.Sprintf("the live pull-request state could not be verified to confirm an open, non-draft pull request with authoritative source-issue linkage to %s: %s; the forge was temporarily unavailable (rate limit, server error, or network failure), so the run is parked for bounded retry by the operator", target, err.Error())
}

// draftExhaustedReason composes the NeedsHuman reason for a run whose
// continuation budget ran out while the run branch's pull request was
// still a draft (#247).
func (r reporter) draftExhaustedReason(number int) string {
	source := r.sourceIssue()
	target := sourceIssueTarget(source)
	return fmt.Sprintf("pull request #%d for the run branch %q remained a draft after the bounded continuation budget, so the handoff was refused; the run needs a human to mark it ready for review (trusted source issue %s)", number, r.cfg.Branch, target)
}

// draftUnresolvedReason names why a draft pull request could not be
// resumed to ready it, without claiming a cause the run did not hit:
// a shutdown is not budget exhaustion, and a missing session is not a
// spent budget (#247).
func (r reporter) draftUnresolvedReason(number int, sessionID string, ctxErr error) string {
	switch {
	case ctxErr != nil:
		source := r.sourceIssue()
		return fmt.Sprintf("pull request #%d for the run branch %q is still a draft and the run is shutting down, so the handoff was refused; the run needs a human to mark it ready for review (trusted source issue %s)", number, r.cfg.Branch, sourceIssueTarget(source))
	case sessionID == "":
		source := r.sourceIssue()
		return fmt.Sprintf("pull request #%d for the run branch %q is still a draft and no captured session exists to ready it, so the handoff was refused; the run needs a human to mark it ready for review (trusted source issue %s)", number, r.cfg.Branch, sourceIssueTarget(source))
	default:
		return r.draftExhaustedReason(number)
	}
}

// adoptionDraftPRReason composes the NeedsHuman reason for a pre-existing
// draft pull request on the run branch at the adoption guard (#343): the
// guard neither adopts a draft nor reports it as reviewable, and no captured
// session exists yet to ready it, so the reason names the actual state and a
// human settles readiness.
func (r reporter) adoptionDraftPRReason(number int) string {
	return fmt.Sprintf("resolve branch already has draft PR #%d; refusing adoption; the run needs a human to mark it ready for review before it can be reported", number)
}

// unpublishedBranchReason composes the NeedsHuman reason for a run whose
// current commits the remote branch does not advertise (#247): the ref is
// absent entirely, or it holds a different commit than the run's head, so
// local commits never pushed. The executor neither hands off as Verifying
// nor asks for a pull request over work that never landed, whatever the
// forge otherwise reports for the branch.
func (r reporter) unpublishedBranchReason(publication remotePublicationState) string {
	if publication.remoteHead == "" {
		return fmt.Sprintf("the run branch %q is not published on the remote, so the handoff was refused; the executor will not ask for a pull request over unpublished work; the run needs a human", r.cfg.Branch)
	}
	return fmt.Sprintf("the run branch %q on the remote is at %s while the run's current commit is %s, so the remote does not carry the run's pushed work; the executor will not hand off as Verifying or ask for a pull request over commits that never pushed; the run needs a human", r.cfg.Branch, publication.remoteHead, publication.localHead)
}

// foreignHeadPRReason composes the NeedsHuman reason for a forge record
// that claims the run branch's runHeadRepo:head selector from a different
// repository (#247). The executor neither adopts such a pull request nor
// deletes it toward a "missing" claim that would prompt a creation over a
// world it cannot reconcile; for a fix-pr run the queried head selector is
// the pinned fork head (runHeadRepo), not the base repository (#517).
func (r reporter) foreignHeadPRReason() string {
	return fmt.Sprintf("the forge reported a pull request claiming the head selector %s:%s from a different repository, so the observation cannot settle this run's own pull-request state; the executor refuses to adopt a foreign head or open another pull request; the run needs a human", runHeadRepo(r.cfg), r.cfg.Branch)
}

// wrongBasePRReason composes the NeedsHuman reason for an open pull request
// that carries the verified head but targets a base other than the run's
// pinned base (#247). Adoption would hand off against the wrong contract,
// re-targeting is forbidden, and creating a parallel PR would bury the
// discrepancy a human must settle.
func (r reporter) wrongBasePRReason() string {
	return fmt.Sprintf("an open pull request on the run branch %q of %s targets a base other than the pinned base %q; the executor refuses to adopt it, re-target it, or create a parallel pull request; the run needs a human to settle the base", r.cfg.Branch, r.cfg.Repo, r.cfg.Base)
}

// prAmbiguousReason composes the NeedsHuman reason for a run branch the
// forge reports with more than one open pull request (#247). Choosing
// one would be a silent adoption.
func (r reporter) prAmbiguousReason() string {
	return fmt.Sprintf("the forge reports more than one open pull request for the run branch %q on %s; the executor refuses to adopt one or create another; the run needs a human to settle which pull request belongs to this run", r.cfg.Branch, r.cfg.Repo)
}

// closedOnlyPRReason composes the NeedsHuman reason for a run branch
// whose only pull requests are closed or merged (#247), mirroring the
// adoption guard's refusal to reopen or duplicate a closed PR.
func (r reporter) closedOnlyPRReason() string {
	return fmt.Sprintf("the run branch %q on %s has only closed or merged pull requests and no open one; the executor refuses adoption, reopening, or duplicate creation; the run needs a human", r.cfg.Branch, r.cfg.Repo)
}

// mergedPinnedPRReason composes the terminal reason for a fix-pr run
// whose pinned pull request merged during the run (#582). The reason is
// deliberately not a delivery claim: the authoritative merge settled the
// repair assignment, but commits the run pushed after the merge are not
// proven to be part of the merged head, so the run states that the
// assignment was superseded and names the branch whose later commits are
// not claimed as shipped. It also states the restraint the run kept —
// no reopening, no duplicate — so the diagnostics stand on their own.
func (r reporter) mergedPinnedPRReason() string {
	return fmt.Sprintf("pull request #%d of %s merged during the run, so the repair assignment was superseded by the merge and the run ends as terminal without requiring a human; commits the run branch %q carries beyond the merged head are not claimed as shipped; the executor did not reopen the pull request or create a duplicate", r.cfg.Ref, r.cfg.Repo, r.cfg.Branch)
}

// fixPRUnverifiedMessage composes the fix-pr run's retry continuation
// message for a failed live forge observation (#517). Like prUnverified
// message (#247) it is explicitly not a missing-PR claim — a failed
// read proves nothing about the pinned pull request — but it stays
// source-issue-free, because a fix-pr run has no trusted source issue:
// it names the pinned head identity (runHeadRepo) and the pinned pull
// request number instead, and demands, through the forge capability,
// that the pinned pull request is the one open, non-draft pull request
// whose full head identity matches that head and whose base ref is the
// pinned base. Any other pull request on the head is not the pinned
// assignment, so it is never the one the model verifies.
func (r reporter) fixPRUnverifiedMessage(err error, continuation int) string {
	return fmt.Sprintf(
		"Courier could not verify the pull-request state for the run branch %q on head %s (%s); this is not proof that the pinned pull request is missing. "+
			"Confirm through the forge capability that the run branch is published and that the pinned pull request #%d is the one open, non-draft pull request with the run's full head identity against base %q. "+
			"Any other pull request on that head is not the pinned assignment: do not adopt it. "+
			"Do not adopt another repository's pull request, and do not change an existing pull request's base or head. "+
			"If the forge stays unreachable, declare blocked_external naming the block. "+
			"Then re-declare the same outcome. (continuation %d)",
		r.cfg.Branch, runHeadRepo(r.cfg), err.Error(), r.cfg.Ref, r.cfg.Base, continuation,
	)
}

// fixPRForgeFailureReason composes the NeedsHuman reason when a fix-pr
// run's pre-handoff forge read failed with no retry left — the budget
// is spent or no captured session exists (#517). Like
// linkageForgeFailureReason (#248) it never claims the pinned pull
// request is missing — a read that did not happen proves nothing about
// it — but it names the pinned branch/head identity instead of a
// trusted source issue, because a fix-pr run has none.
func (r reporter) fixPRForgeFailureReason(err error) string {
	return fmt.Sprintf("the live pull-request state could not be verified for the run branch %q on %s (head %s): %s; a read that did not happen proves nothing about the pinned pull request; the run needs a human", r.cfg.Branch, r.cfg.Repo, runHeadRepo(r.cfg), err.Error())
}

// pinnedPRMissingReason composes the NeedsHuman reason for a fix-pr run
// whose pinned head carries no pull request of any state at handoff
// (#517). The pinned pull request is the assignment, so the executor
// neither opens a replacement — which would duplicate or silently
// re-target the assignment — nor adopts another pull request; the
// unverified pinned contract is left for a human to settle.
func (r reporter) pinnedPRMissingReason() string {
	return fmt.Sprintf("no pull request carries the run's head branch %q (head %s) on the base repository %s, so the pinned pull request is unverified; the executor neither opens a replacement nor adopts another pull request; the run needs a human", r.cfg.Branch, runHeadRepo(r.cfg), r.cfg.Repo)
}

// pinnedPRDraftReason composes the NeedsHuman reason for a fix-pr run
// whose pinned pull request is still a draft at handoff (#517). Readiness
// is a human settlement, never a continuation's unilateral flip, so no
// readying continuation runs here.
func (r reporter) pinnedPRDraftReason(number int) string {
	return fmt.Sprintf("the pinned pull request #%d for the run branch %q is a draft at handoff, so the handoff was refused; readiness is a human settlement, never a continuation's unilateral flip; the run needs a human", number, r.cfg.Branch)
}

// pinnedPRNumberMismatchReason composes the NeedsHuman reason for a fix-pr
// run whose observed pull request is not the pinned assignment (#517): the
// forge reports another pull request on the run's head, so the pinned pull
// request the run was admitted against is unverified. The executor verifies
// the pinned assignment only — the same exact-number, base, and head identity
// the native broker path enforces in checkPR/matchesPR — and never adopts
// another pull request in its place.
func (r reporter) pinnedPRNumberMismatchReason(pinned, observed int) string {
	return fmt.Sprintf("the forge reports pull request #%d on the run's head branch %q (head %s), not the pinned pull request #%d the run was admitted against; the executor verifies the pinned assignment only and never adopts another pull request; the run needs a human", observed, r.cfg.Branch, runHeadRepo(r.cfg), pinned)
}
