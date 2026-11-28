package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/github"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/rebase"
)

// rebaseConflictingFixPRBranch is the fix-pr pre-flight of #887: a run that
// takes over a pull request the forge reports conflicting with its live base
// replays the branch onto that base mechanically before any review fix runs,
// so a conflict a trusted rule can resynchronize never reaches the coordinator
// as a mid-merge workspace. The merge state rides the same forge
// read-pull-request capability the launch guards use — never a forge-specific
// CLI — and the replay opens only on the forge's machine-readable conflict
// verdict, because a bare mergeable no also answers requests merely blocked by
// required checks or reviews. Anything else — clean, pending, blocked, or
// otherwise unverified — changes nothing: no replay, no push, today's path.
//
// The replay is the rebase lane's engine (#715) with its closed mechanical
// rules, its bounded checks, and its publication leased to the head admission
// pinned: never a merge, never an unleased rewrite. A semantic conflict ends
// the run for a human with the conflicting files named and the remote branch
// untouched; a writer that moved the branch declines the replay, and the run
// continues on the ordinary path, whose pushes are fast-forward only. It
// returns (0, false) when the run may proceed to workspace preparation.
func rebaseConflictingFixPRBranch(ctx context.Context, report reporter, cfg config) (int, bool) {
	if !strings.EqualFold(cfg.Mode, "fix-pr") || cfg.Ref < 1 {
		return 0, false
	}
	client, err := github.NewClient(cfg.GitHubAPIBase, cfg.GitHubToken)
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return exitFailed, true
	}
	provider := github.NewProvider(forge.ProviderConfig{Name: "github", Endpoint: cfg.GitHubAPIBase}, client)
	// Fail closed before reading: an unoffered read-pull-request capability
	// means the conflict was never observed, and the pre-flight never replays blind.
	if !provider.Capabilities().Available(forge.CapabilityReadPullRequest).Available {
		reason := fmt.Sprintf("pull request #%d of %q could not be read to check its merge state before the fix: the forge read-pull-request capability is unavailable; the run refuses to replay on a world it cannot observe", cfg.Ref, cfg.Repo)
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Outcome: outcomeBlockedExternal, Category: terminalCategoryForgeUnavailable})
		return exitNeedsHuman, true
	}
	pr, err := provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: cfg.Repo, Number: cfg.Ref})
	if err != nil {
		if forge.IsTransientAvailability(err) {
			// The merge state was never observed, so there is nothing to
			// decide from: park for the operator's bounded retry exactly as
			// the launch guards do, and never replay on an unobserved world.
			reason := fmt.Sprintf("pull request #%d of %q could not be read to check its merge state before the fix: %s; the forge was temporarily unavailable, so the run is parked for bounded retry by the operator", cfg.Ref, cfg.Repo, err.Error())
			report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Outcome: outcomeBlockedExternal, Category: terminalCategoryForgeUnavailable})
			return exitNeedsHuman, true
		}
		var apiErr *github.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			// The pinned request vanished between the guard's read and this
			// one: the assignment is gone, which a human settles. The run
			// neither opens a replacement nor replays anything else.
			reason := fmt.Sprintf("pull request #%d of %q does not exist, so the pinned pull request is unverified; the executor neither opens a replacement nor adopts another pull request; the run needs a human", cfg.Ref, cfg.Repo)
			report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Category: terminalCategoryUnknown})
			return exitNeedsHuman, true
		}
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return exitFailed, true
	}
	// The replay publishes a rewrite, so it re-asserts the state and base
	// identity from its own read rather than trusting the launch guard's
	// earlier one: a retarget or close between the two reads must stop the
	// force-push, not rebase onto a base the request no longer names (#514).
	switch {
	case !strings.EqualFold(pr.State, "open"):
		observed := pr.State
		if pr.Merged {
			observed = "merged"
		}
		reason := fmt.Sprintf("pull request #%d on %s is %s, not open; the repair condition is moot, so the run ends without changing it", cfg.Ref, cfg.Repo, observed)
		report.terminate(termination{Phase: "NoChangeNeeded", Result: "success", ExitCode: exitNoChangeNeeded, Reason: reason})
		return exitNoChangeNeeded, true
	case pr.BaseRepo == "" || pr.BaseRef == "" || pr.BaseRepo != cfg.Repo:
		reason := fmt.Sprintf("pull request #%d of %q has an incomplete or retargeted base identity, so the pre-flight refuses to replay it", cfg.Ref, cfg.Repo)
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	case pr.BaseRef != cfg.Base:
		reason := fmt.Sprintf("pull request #%d targets base %q, not the run's pinned base %q; the pre-flight refuses to replay onto a base the pull request does not name", cfg.Ref, pr.BaseRef, cfg.Base)
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	}
	if !fixPRBranchConflictsWithBase(pr) {
		return 0, false
	}
	if reason := conflictingFixPRHeadReason(pr, cfg); reason != "" {
		// The replay publishes through a force-with-lease, so it may target
		// only the head the pull request itself names: an answer whose head
		// identity differs is the fail-closed world the base guard refuses for
		// a retargeted base.
		reason := fmt.Sprintf("pull request #%d of %q conflicts with its base, but %s; the run refuses to replay or publish another head", cfg.Ref, cfg.Repo, reason)
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	}
	// The publication leases to the head admission pinned, exactly as the
	// rebase lane does; a run whose admission pinned no head leases to the
	// head this read observed. Either way a writer that moved the branch
	// between the read and the push declines the replay.
	leaseHead := strings.TrimSpace(cfg.HeadSHA)
	if leaseHead == "" {
		leaseHead = pr.HeadSHA
	}
	if leaseHead == "" {
		// No observed head means no lease, and an unleased replay is the one
		// publication this path must never attempt. The ordinary path keeps
		// the behavior the run had before the pre-flight existed.
		report.eventVerbose(courierlog.EventRebasePreflight, courierlog.StatusOK, map[string]any{
			"status": "skipped",
			"reason": "neither the admission pin nor the forge answer named a head commit to lease the replay to",
		})
		return 0, false
	}

	workspace, err := git.Prepare(ctx, git.PrepareOptions{
		RemoteURL:     cfg.RemoteURL,
		BaseRemoteURL: cfg.BaseRemoteURL,
		Directory:     cfg.Directory,
		Base:          cfg.Base,
		Branch:        cfg.Branch,
		// The replay adopts the branch verbatim: a base-sync merge here would
		// contradict the rebase the same way it does for the rebase lane.
		SkipBaseSync: true,
	})
	if err != nil {
		reason := "prepare the pre-flight rebase workspace: " + err.Error()
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: reason, Category: terminalCategoryExecutorCrash})
		return exitFailed, true
	}
	resolver, err := newRebaseAssistResolver(cfg)
	if err != nil {
		reason := "configure model-assisted conflict resolution: " + err.Error()
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: reason, Category: terminalCategoryExecutorCrash})
		return exitFailed, true
	}
	result, err := rebase.Execute(ctx, rebase.Options{
		Repo:            cfg.Repo,
		Base:            cfg.Base,
		Branch:          cfg.Branch,
		ExpectedHead:    leaseHead,
		Workspace:       workspace.Directory,
		RemoteURL:       cfg.RemoteURL,
		BaseRemoteURL:   cfg.BaseRemoteURL,
		Goal:            cfg.Goal,
		ModelResolver:   resolver,
		LockfileTimeout: rebase.DefaultLockfileTimeout,
	})
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return exitFailed, true
	}
	reportRebaseAssist(report, result)

	switch result.Outcome {
	case rebase.OutcomePublished, rebase.OutcomeUpToDate:
		// The run continues on the rebased branch; the ordinary preparation
		// re-adopts it from the remote, and its base sync is a no-op because
		// the replay already contains the base tip.
		report.eventVerbose(courierlog.EventRebasePreflight, courierlog.StatusOK, preflightEventPayload(result))
		return 0, false
	case rebase.OutcomeSemanticConflict:
		reason := result.Reason
		if result.Conflict != nil && len(result.Conflict.Files) > 0 {
			reason = fmt.Sprintf("%s (files: %s)", reason, strings.Join(result.Conflict.Files, ", "))
		}
		report.eventVerbose(courierlog.EventRebasePreflight, courierlog.StatusNeedsHuman, preflightEventPayload(result))
		terminated := fmt.Sprintf("pull request #%d of %q conflicts with base %q and the replay declined it as non-mechanical: %s; the remote branch was left untouched, so the run needs a human", cfg.Ref, cfg.Repo, cfg.Base, reason)
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: terminated, Outcome: outcomeNeedsDecision, Category: terminalCategoryNeedsDecision})
		return exitNeedsHuman, true
	case rebase.OutcomeRemoteHeadMoved:
		// A writer moved the branch between this run's read and its leased
		// push, and nothing was published. The ordinary path adopts the live
		// head and never force-pushes, so the declined replay does not decide
		// the run's ending.
		report.eventVerbose(courierlog.EventRebasePreflight, courierlog.StatusOK, preflightEventPayload(result))
		return 0, false
	default:
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: exitFailed, Reason: result.Reason, Category: terminalCategoryUnknown})
		return exitFailed, true
	}
}

// preflightEventPayload describes one pre-flight replay verdict for the run's
// event stream: the engine's outcome, the published head when the replay
// landed, and the files a semantic conflict stopped on.
func preflightEventPayload(result rebase.Result) map[string]any {
	payload := map[string]any{"status": string(result.Outcome)}
	if result.Head != "" {
		payload["head"] = result.Head
	}
	if result.Reason != "" {
		payload["reason"] = result.Reason
	}
	if result.Conflict != nil && len(result.Conflict.Files) > 0 {
		payload["files"] = result.Conflict.Files
	}
	return payload
}

// githubMergeStateConflict is GitHub's mergeable_state value for a real
// merge conflict against the base branch.
const githubMergeStateConflict = "dirty"

// fixPRBranchConflictsWithBase reports only the forge's unambiguous conflict
// answer: the authoritative mergeable no whose reported state names the
// conflict. A bare mergeable=false also answers requests blocked by required
// checks or reviews, GitHub has been observed pairing it with such answers,
// and a replay would rewrite a branch that needs no rewrite; a missing,
// pending, or non-conflict state gives the pre-flight no license to
// force-push, and the run continues on the ordinary path.
func fixPRBranchConflictsWithBase(pr forge.PullRequest) bool {
	return pr.Mergeable != nil && !*pr.Mergeable && pr.MergeableState == githubMergeStateConflict
}

// conflictingFixPRHeadReason names why the conflicting pull request's head is
// not the branch this run may replay, or "" when it is. A force-with-lease
// rewrite may target only the exact head the pull request names, on the
// repository the run was admitted against.
func conflictingFixPRHeadReason(pr forge.PullRequest, cfg config) string {
	headRepo := cfg.Repo
	if strings.TrimSpace(cfg.HeadRepo) != "" {
		headRepo = cfg.HeadRepo
	}
	switch {
	case pr.HeadRef == "" || pr.HeadRepo == "":
		return "the forge answer carries no head repository or ref"
	case pr.HeadRef != cfg.Branch:
		return fmt.Sprintf("its head ref %q is not the run's branch %q", pr.HeadRef, cfg.Branch)
	case pr.HeadRepo != headRepo:
		return fmt.Sprintf("its head repository %q is not the run's %q", pr.HeadRepo, headRepo)
	}
	return ""
}
