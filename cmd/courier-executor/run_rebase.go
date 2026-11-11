package main

import (
	"context"
	"fmt"
	"github.com/misospace/courier/internal/git"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/rebase"
	"strings"
)

// runRebaseMode is the rebase execution path (#715). It runs in trusted
// deterministic code: the workspace is adopted without the fix-pr base-sync
// merge, the trusted mechanical rebase engine replays the PR branch onto the
// pull request's live base, and the outcome maps onto the operator's
// termination contract exactly like every other ending. No model session
// starts, so only the engine's closed mechanical rules — plus, when
// explicitly configured, the one-attempt-per-episode model-assisted
// resolution the engine validates itself (#732) — can ever change a byte,
// and the publication is leased to the head observed at admission. The
// rebase outcome decides the phase: published and up-to-date hand off to
// the operator's world verification, a semantic conflict parks for a human,
// and writer contention or an unattributable failure ends the attempt
// Failed for the source's attempt cap to govern.
func runRebaseMode(ctx context.Context, report reporter, cfg config) int {
	workspace, err := git.Prepare(ctx, git.PrepareOptions{
		RemoteURL:     cfg.RemoteURL,
		BaseRemoteURL: cfg.BaseRemoteURL,
		Directory:     cfg.Directory,
		Base:          cfg.Base,
		Branch:        cfg.Branch,
		SkipBaseSync:  true,
	})
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: "prepare the rebase workspace: " + err.Error(), Category: terminalCategoryExecutorCrash})
		return exitFailed
	}
	report.event(courierlog.EventWorkspaceReady, courierlog.StatusOK, map[string]any{
		"adopted":       workspace.Adopted,
		"base":          cfg.Base,
		"branch":        cfg.Branch,
		"baseSyncSkips": true,
	})
	resolver, err := newRebaseAssistResolver(cfg)
	if err != nil {
		// Enabled but unconfigured fails closed, never silently mechanical:
		// a deployment that turned the capability on and misconfigured its
		// transport must see the run fail, not get outcomes produced under
		// different rules than it asked for.
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: "configure model-assisted conflict resolution: " + err.Error(), Category: terminalCategoryExecutorCrash})
		return exitFailed
	}
	outcome, err := rebase.Execute(ctx, rebase.Options{
		Repo:            cfg.Repo,
		Base:            cfg.Base,
		Branch:          cfg.Branch,
		ExpectedHead:    cfg.HeadSHA,
		Workspace:       cfg.Directory,
		RemoteURL:       cfg.RemoteURL,
		BaseRemoteURL:   cfg.BaseRemoteURL,
		Goal:            cfg.Goal,
		ModelResolver:   resolver,
		LockfileTimeout: rebase.DefaultLockfileTimeout,
	})
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return exitFailed
	}
	if outcome.Assist != nil && outcome.Assist.Attempted {
		// The per-turn evidence that the configured model actually ran on
		// this task: distinct from run.start's configured model field, and
		// emitted for published and unresolved endings alike, because the
		// attempt's spend is real whatever the outcome was. Verbose so the
		// evidence reaches the log store on every run that ran the assist.
		report.eventVerbose(courierlog.EventRebaseModelAssist, courierlog.StatusOK, map[string]any{
			"model":          outcome.Assist.Model,
			"turns":          outcome.Assist.Turns,
			"files_resolved": outcome.Assist.FilesResolved,
			"input_tokens":   outcome.Assist.InputTokens,
			"output_tokens":  outcome.Assist.OutputTokens,
		})
	}
	switch outcome.Outcome {
	case rebase.OutcomePublished:
		report.event(courierlog.EventRunSummary, courierlog.StatusOK, map[string]any{
			"summary": outcome.Reason,
			"head":    outcome.Head,
		})
		// Still only a handoff: the operator's own PR/head/check observation
		// remains the authority for Verifying.
		report.terminate(termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: outcome.Reason})
		return exitSuccess
	case rebase.OutcomeUpToDate:
		report.terminate(termination{Phase: "NoChangeNeeded", Result: "success", ExitCode: exitNoChangeNeeded, Reason: outcome.Reason, Outcome: outcomeNoChangeNeeded})
		return exitNoChangeNeeded
	case rebase.OutcomeSemanticConflict:
		reason := outcome.Reason
		if outcome.Conflict != nil && len(outcome.Conflict.Files) > 0 {
			reason = fmt.Sprintf("%s (files: %s)", reason, strings.Join(outcome.Conflict.Files, ", "))
		}
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Outcome: outcomeNeedsDecision, Category: terminalCategoryNeedsDecision})
		return exitNeedsHuman
	case rebase.OutcomeRemoteHeadMoved:
		// Writer contention, not a human decision: the source's attempt cap
		// governs any reissue, and the remote branch was never mutated.
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: outcome.Reason, Category: terminalCategoryVerifierRejection})
		return exitFailed
	default:
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: outcome.Reason, Category: terminalCategoryUnknown})
		return exitFailed
	}
}
