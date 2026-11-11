package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/github"
	courierlog "github.com/misospace/courier/internal/log"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

// The terminal-diagnostic categories the executor may declare. They are
// bound to the operator's vocabulary, so a category can never drift from the
// value the operator persists: an unmatched category would be dropped by the
// operator and the ending would silently lose its cause.
const (
	terminalCategoryExecutorCrash          = courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash
	terminalCategoryModelTransportError    = courierv1alpha1.TerminalDiagnosticCategoryModelTransportError
	terminalCategoryContinuationExhaustion = courierv1alpha1.TerminalDiagnosticCategoryContinuationExhaustion
	terminalCategoryVerifierRejection      = courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection
	terminalCategoryUnknown                = courierv1alpha1.TerminalDiagnosticCategoryUnknown
	terminalCategoryNeedsDecision          = courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision
	terminalCategoryBlockedExternal        = courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal
	terminalCategoryCapacityExhaustion     = courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion
	terminalCategoryProviderUnavailable    = courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable
	terminalCategoryForgeUnavailable       = courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable
)

// The outcome kinds the coordinator may declare (#169).
const (
	outcomeChanges         = "changes"
	outcomeNoChangeNeeded  = "no_change_needed"
	outcomeNeedsDecision   = "needs_decision"
	outcomeBlockedExternal = "blocked_external"
)

// outcomeDeclaration is the coordinator's declared run ending, read from the
// per-run scratch file outside the checkout. Only the field matching the
// outcome kind is required; the executor validates that per kind.
type outcomeDeclaration struct {
	Outcome  string `json:"outcome"`
	Evidence string `json:"evidence"`
	Question string `json:"question"`
	Missing  string `json:"missing"`
}

// readOutcome reads and validates the coordinator's outcome declaration from
// its per-run scratch path. A missing file returns (zero, false, nil): the run
// was simply undeclared. A read error or unparsable JSON returns (zero, true,
// err). A declaration that fails its per-outcome field rule, or names an
// unknown outcome kind, returns the parsed declaration with (decl, true, err)
// describing the invalid declaration.
func readOutcome(path string) (outcomeDeclaration, bool, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return outcomeDeclaration{}, false, nil
	}
	if err != nil {
		return outcomeDeclaration{}, true, err
	}
	var decl outcomeDeclaration
	if err := json.Unmarshal(data, &decl); err != nil {
		return outcomeDeclaration{}, true, err
	}
	switch strings.TrimSpace(decl.Outcome) {
	case outcomeChanges:
	case outcomeNoChangeNeeded:
		if strings.TrimSpace(decl.Evidence) == "" {
			return decl, true, errors.New("evidence is required for outcome no_change_needed")
		}
	case outcomeNeedsDecision:
		if strings.TrimSpace(decl.Question) == "" {
			return decl, true, errors.New("question is required for outcome needs_decision")
		}
	case outcomeBlockedExternal:
		if strings.TrimSpace(decl.Missing) == "" {
			return decl, true, errors.New("missing is required for outcome blocked_external")
		}
	default:
		return decl, true, fmt.Errorf("unknown outcome %q", decl.Outcome)
	}
	return decl, true, nil
}

// truncateTerminationReason applies the Kubernetes handoff bound after
// redaction, preserving UTF-8 and reserving room for the truncation marker.
// The marker itself is the shared termination suffix from internal/executor
// so the executor and the native control producer cannot drift on the
// truncation marker either (#210).
func truncateTerminationReason(reason string) string {
	if len(reason) <= maxTerminationReasonBytes {
		return reason
	}
	limit := maxTerminationReasonBytes - len(executor.TerminationTruncationSuffix)
	for limit > 0 && !utf8.ValidString(reason[:limit]) {
		limit--
	}
	return reason[:limit] + executor.TerminationTruncationSuffix
}

// failed builds a Failed termination with the standard failure exit code.
func failed(reason string) termination {
	return failedWith(reason, terminalCategoryExecutorCrash)
}

func failedWith(reason string, category courierv1alpha1.TerminalDiagnosticCategory) termination {
	return termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: reason, Category: category}
}

// classify decides the run's terminal outcome from the coordinator's
// declaration (when present) and the verified world state. It is the single
// place the run's ending is decided, so every way the run can end is spelled
// out here (#169).
func (r reporter) classify(ctx context.Context, workspace *git.Workspace, branch, startCommit string, workState git.WorkState, decl outcomeDeclaration, declared bool, declErr error, caps []executor.MCPCapability) termination {
	if declErr != nil {
		// An invalid declaration is a failed run, never a human-attention
		// signal: the world does not say the coordinator asked for a decision.
		return failedWith("opencode wrote an invalid outcome declaration: "+declErr.Error(), terminalCategoryUnknown)
	}
	if declared {
		return r.classifyDeclared(ctx, workspace, branch, startCommit, workState, decl)
	}
	return r.classifyUndeclared(ctx, workspace, branch, startCommit, workState, caps)
}

// classifyDeclared resolves a valid coordinator declaration. A declared
// changes ending is confirmed against the world: it succeeds only when the
// #134 verification shows committed work reachable from the run branch.
// The shared authoritative-linkage check runs separately, after
// classify, so the declared path participates in the same bounded
// continuation/repair mechanism (#248) as the undeclared-Verifying path;
// classifyDeclared itself must not refuse the handoff on linkage alone,
// because a refused declared handoff bypasses the model's last repair
// opportunity inside the captured session (#170).
func (r reporter) classifyDeclared(ctx context.Context, workspace *git.Workspace, branch, startCommit string, workState git.WorkState, decl outcomeDeclaration) termination {
	switch strings.TrimSpace(decl.Outcome) {
	case outcomeChanges:
		r.emitOutcomeDeclared(outcomeChanges)
		switch workState {
		case git.WorkStateDirty:
			return failedWith("opencode declared changes but left uncommitted workspace changes", terminalCategoryVerifierRejection)
		case git.WorkStateNone:
			return failedWith("opencode declared changes but produced no commit", terminalCategoryVerifierRejection)
		default:
			// The run branch's own ref, not where HEAD points, is the world:
			// committed work counts only when reachable from the run branch. A
			// branch-read error must not fail the run, so an unreadable ref
			// falls through to the success path.
			ahead, branchErr := workspace.CommitsOnBranchSince(ctx, branch, startCommit)
			if branchErr == nil && ahead == 0 {
				return failedWith(r.offBranchReason(ctx, workspace, branch)+"; the run branch has no new commits", terminalCategoryVerifierRejection)
			}
			return termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: "opencode completed with committed work", Outcome: outcomeChanges}
		}
	case outcomeNoChangeNeeded:
		r.emitOutcomeDeclared(outcomeNoChangeNeeded)
		// The workspace check stays local: commits or dirty paths are a
		// rejection on their own, before any forge read.
		if workState != git.WorkStateNone {
			return failedWith("opencode declared no change needed but the workspace has new commits or uncommitted changes", terminalCategoryVerifierRejection)
		}
		// A clean workspace alone is not a verified claim: before the
		// success termination is published, the declared no-change ending
		// is checked against the assigned repair condition using current
		// authoritative forge state (#513).
		if reason := r.verifyNoChangeClaim(ctx); reason != "" {
			return failedWith(reason, terminalCategoryVerifierRejection)
		}
		// No outcome comment: the executor cannot authoritatively verify a
		// success claim, and the controller settles this ending against the
		// world (a fix-pr run routes to Verifying and can still end
		// BlockedExternal when queued feedback is unverified, #474). The
		// evidence rides the termination reason — run.exit telemetry and the
		// COURIER_TERMINATION handoff — and the public record stays the
		// coordinator's own handoff plus the controller's lifecycle report.
		return termination{Phase: "NoChangeNeeded", Result: "success", ExitCode: exitNoChangeNeeded, Reason: "opencode declared no change needed: " + decl.Evidence, Outcome: outcomeNoChangeNeeded}
	case outcomeNeedsDecision:
		r.emitOutcomeDeclared(outcomeNeedsDecision)
		r.postOutcomeComment(ctx, "**Courier needs a decision** (run "+r.cfg.RunID+")\n\n"+decl.Question)
		return termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: "coordinator needs a decision: " + decl.Question, Outcome: outcomeNeedsDecision, Category: terminalCategoryNeedsDecision}
	case outcomeBlockedExternal:
		r.emitOutcomeDeclared(outcomeBlockedExternal)
		r.postOutcomeComment(ctx, "**Courier is blocked on an external prerequisite** (run "+r.cfg.RunID+")\n\n"+decl.Missing)
		return termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: "coordinator is blocked on an external prerequisite: " + decl.Missing, Outcome: outcomeBlockedExternal, Category: terminalCategoryBlockedExternal}
	}
	// Unreachable: readOutcome rejects any unknown outcome kind above.
	return failedWith("opencode wrote an invalid outcome declaration: unknown outcome", terminalCategoryUnknown)
}

// classifyUndeclared resolves a run the coordinator ended without a valid
// declaration. Only a genuine decision may ask for a human, so none of these
// endings is NeedsHuman: they fail as incomplete. (Issue #170 turns these
// recoverable endings into session continuations instead of failures.)
func (r reporter) classifyUndeclared(ctx context.Context, workspace *git.Workspace, branch, startCommit string, workState git.WorkState, caps []executor.MCPCapability) termination {
	switch workState {
	case git.WorkStateDirty:
		return failedWith("opencode exited successfully without declaring an outcome and left uncommitted workspace changes", terminalCategoryUnknown)
	case git.WorkStateCommitted:
		// Fail closed on an unreadable branch (#344): only a read that
		// positively shows work ahead of the start commit can carry the run
		// to Verifying, never an inconclusive one.
		ahead, branchErr := workspace.CommitsOnBranchSince(ctx, branch, startCommit)
		if branchErr != nil {
			return failedWith("could not verify committed work on the run branch: "+branchErr.Error(), terminalCategoryVerifierRejection)
		}
		if ahead == 0 {
			return failedWith(r.offBranchReason(ctx, workspace, branch)+"; no outcome was declared", terminalCategoryVerifierRejection)
		}
		// The world shows verified work on the run branch; that ending stands
		// on its own, declaration or not.
		return termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: "opencode completed with committed work"}
	default:
		reason := "opencode exited successfully without declaring an outcome and produced no commit or workspace changes"
		if names := unavailableCapabilities(caps); len(names) > 0 {
			reason += "; configured capability unavailable: " + strings.Join(names, ", ")
		}
		return failedWith(reason, terminalCategoryUnknown)
	}
}

// offBranchReason names where committed work actually landed when it is not
// reachable from the run branch.
func (r reporter) offBranchReason(ctx context.Context, workspace *git.Workspace, branch string) string {
	whereClause := "a detached HEAD"
	if cb, err := workspace.CurrentBranch(ctx); err == nil && cb != "" {
		whereClause = fmt.Sprintf("branch %q", cb)
	}
	return fmt.Sprintf("opencode committed work that is not on the run branch %q (HEAD is on %s)", branch, whereClause)
}

// emitOutcomeDeclared records that a valid declaration classified the ending.
// Emission is best-effort like every other event.
func (r reporter) emitOutcomeDeclared(kind string) {
	status := courierlog.StatusOK
	switch kind {
	case outcomeBlockedExternal:
		status = courierlog.StatusError
	case outcomeNeedsDecision:
		status = courierlog.StatusNeedsHuman
	}
	r.event(courierlog.EventOutcomeDeclared, status, map[string]any{"outcome": kind})
}

// postOutcomeComment posts a terminal outcome's explanation onto the run's
// issue or pull request. It is best-effort: a failure is recorded as an
// outcome.comment error event and never changes the run's ending. The body is
// model-authored and passes through the redactor before it leaves the
// process, like every other external path.
//
// Publication contract (#474): declared-outcome comments carry human-attention
// requests the executor can authoritatively stand behind — needs_decision and
// blocked_external, whose claims match the run's NeedsHuman/BlockedExternal
// lifecycle. A success claim (no_change_needed) is never published here: the
// controller settles it against the world, and a generic terminal comment
// would duplicate the coordinator's detailed handoff or contradict the final
// lifecycle state. The criterion-coverage handoff comment (Verifying path) is
// a separate publication: it restates validator-derived verdict data, not a
// declared-outcome claim.
func (r reporter) postOutcomeComment(ctx context.Context, body string) {
	if r.cfg.Ref < 1 {
		r.event(courierlog.EventOutcomeComment, courierlog.StatusError, map[string]any{"status": "error", "reason": "no issue/PR reference to comment on"})
		return
	}
	body = r.red.Redact(body)
	owner, name, ok := splitOwnerRepo(r.cfg.Repo)
	if !ok {
		r.event(courierlog.EventOutcomeComment, courierlog.StatusError, map[string]any{"status": "error", "reason": "no owner/repo to comment on"})
		return
	}
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		r.event(courierlog.EventOutcomeComment, courierlog.StatusError, map[string]any{"status": "error", "reason": err.Error()})
		return
	}
	if err := client.CreateComment(ctx, owner, name, r.cfg.Ref, body); err != nil {
		r.event(courierlog.EventOutcomeComment, courierlog.StatusError, map[string]any{"status": "error", "reason": err.Error()})
		return
	}
	r.event(courierlog.EventOutcomeComment, courierlog.StatusOK, map[string]any{"status": "ok"})
}

// maxConflictNotePaths bounds how many conflicted paths the goal lists.
const maxConflictNotePaths = 20

// conflictNote tells the coordinator that adoption stopped mid-merge and that
// resolving it comes before any other work.
func conflictNote(conflict *git.MergeConflictError) string {
	paths := conflict.Paths
	more := ""
	if len(paths) > maxConflictNotePaths {
		more = fmt.Sprintf(" (and %d more)", len(paths)-maxConflictNotePaths)
		paths = paths[:maxConflictNotePaths]
	}
	return fmt.Sprintf("The run branch is mid-merge: syncing it with %s stopped on conflicts in %s%s. Resolve those conflicts, run the tests, and commit the merge before any other change or push. Keep the branch's existing work: never reset, rebase, force-push, or discard it. If you cannot resolve the conflicts safely, stop and report that the run needs a human.", conflict.Base, strings.Join(paths, ", "), more)
}

// unresolvedBaseSync returns why a conflicted base sync is still unfinished
// after the coordinator exits, or "" once the base is merged into HEAD. Any
// doubt keeps the run out of a success phase.
func unresolvedBaseSync(ctx context.Context, workspace *git.Workspace) string {
	base := workspace.Conflict.Base
	pending, err := workspace.MergeInProgress(ctx)
	if err != nil {
		return "could not verify the base-sync merge with " + base + ": " + err.Error()
	}
	if pending {
		paths, _ := workspace.UnmergedPaths(ctx)
		if len(paths) > 0 {
			return fmt.Sprintf("the merge of %s is still unresolved; conflicts remain in %s", base, strings.Join(paths, ", "))
		}
		return "the merge of " + base + " was resolved but never committed"
	}
	merged, err := workspace.ContainsBase(ctx)
	if err != nil {
		return "could not verify the base-sync merge with " + base + ": " + err.Error()
	}
	if !merged {
		return "the run branch does not contain " + base + "; the conflicted base-sync merge was abandoned instead of resolved"
	}
	return ""
}

// maxContinuationPaths bounds the number of dirty paths included in a resume message.
const maxContinuationPaths = 20

// summarizeHistory numbers each recorded state for a looping termination reason.
func summarizeHistory(history []string) string {
	entries := make([]string, len(history))
	for i, entry := range history {
		entries[i] = fmt.Sprintf("%d) %s", i+1, entry)
	}
	return strings.Join(entries, "; ")
}

type continuationFingerprintState struct {
	WorkState git.WorkState `json:"work_state"`
	Branch    string        `json:"branch"`
	Head      string        `json:"head"`
	Dirty     []string      `json:"dirty"`
	LastText  string        `json:"last_text"`
}

func continuationFingerprint(workState git.WorkState, branch, head string, dirty []string, lastText string) (string, error) {
	encoded, err := json.Marshal(continuationFingerprintState{
		WorkState: workState,
		Branch:    branch,
		Head:      head,
		Dirty:     dirty,
		LastText:  lastText,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func stateSummary(kind, head, lastText string, paths []string) string {
	summary := kind
	if kind == "uncommitted" {
		summary += " [" + strings.Join(paths, ", ") + "]"
	}
	details := make([]string, 0, 2)
	if head != "" {
		details = append(details, "head "+head)
	}
	details = append(details, "last message "+strconv.Quote(lastText))
	return summary + " (" + strings.Join(details, ", ") + ")"
}
