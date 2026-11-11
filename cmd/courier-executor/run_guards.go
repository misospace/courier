package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/github"
	"net/http"
	"strings"
)

// transientLaunchReadTermination builds the recoverable ending for a launch
// guard whose remote branch read could not complete on a transient availability
// condition (#580). The world the read was meant to observe was never observed,
// so — mirroring the pre-handoff forge observation's #455 refinement — the run
// ends on the recovery channel: a blocked_external ending with the honest
// forge-unavailable category for the operator's bounded retry, never the
// executor-crash a guard gives a read that reached the forge and returned its
// own verdict. It reuses forgeUnavailableTermination so the launch guards share
// the handoff path's exact ending and its git-verified workspace-safety
// snapshot. The launch guards run before git.Prepare, so that snapshot carries
// no start commit and reports unknown unless the workspace directory already
// holds verifiable work.
func (r reporter) transientLaunchReadTermination(reason string) termination {
	workspace := &git.Workspace{Directory: r.cfg.Directory, RemoteURL: r.cfg.RemoteURL, Branch: r.cfg.Branch}
	return r.forgeUnavailableTermination(workspace, r.cfg, "", reason)
}

// guardAdoption enforces the resolve-issue invariant that a deterministic
// branch may be adopted only when it is orphaned. When the branch already
// exists on the remote, the guard reads the run branch's pull requests
// through the same identity-checked forge capability as the pre-handoff
// observation (#343) — never through a forge-specific listing — so a
// same-owner fork, a re-targeted base, or a draft cannot masquerade as a
// reviewable PR. Only an open, non-draft PR whose full base- and head
// identity matches the run's repository and branch and whose base ref is the
// pinned base is reported for review by exiting 0, so the operator's
// world-verification publishes the existing pull request to the source as
// in-review. Foreign head claims, wrong-base PRs, drafts, ambiguous sets,
// and heads whose only pull requests are closed or merged terminate
// NeedsHuman with the precise state, mirroring the observation's fixed
// consequences (#247). Body linkage is not checked here: the controller's
// `Verifying` observation settles what this early handoff leaves open. The
// launch-time read that decides whether the branch exists at all is the same
// git-side remote read the handoff classifies: when it could not complete on a
// transient availability condition the world was never observed, so the guard
// ends the run on the recovery channel — a forge-unavailable blocked_external
// for the operator's bounded retry — rather than as an executor crash, while a
// read that reached the forge and returned its own verdict keeps the fail-closed
// crash ending (#580). It returns (0, false) when the run may proceed to Prepare,
// or a terminal (code, true) once it has emitted the run's handoff.
func (r reporter) guardAdoption(ctx context.Context) (int, bool) {
	if r.cfg.Mode != "resolve-issue" {
		return 0, false
	}
	exists, err := git.RemoteBranchExists(ctx, r.cfg.RemoteURL, r.cfg.Branch)
	if err != nil {
		if forge.IsTransientAvailability(err) {
			// The read never reached the forge's answer, so adoption cannot
			// be decided from it: park for the operator's bounded retry under
			// the forge-unavailable category rather than crash, and never
			// adopt or proceed on an unobserved remote (#580).
			reason := fmt.Sprintf("the run branch %s could not be read on the remote to decide adoption: %s; the forge was temporarily unavailable, so the run is parked for bounded retry by the operator", r.cfg.Branch, err.Error())
			r.terminate(r.transientLaunchReadTermination(reason))
			return exitNeedsHuman, true
		}
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1, true
	}
	if !exists {
		return 0, false
	}
	observation, err := r.observeRunBranchPullRequest(ctx)
	if err != nil {
		if forge.IsTransientAvailability(err) {
			// The read never reached the forge's answer, so the pull
			// request set on the run branch was never observed: park for
			// the operator's bounded retry under the forge-unavailable
			// category rather than crash, and never adopt on an
			// unobserved world (#585).
			reason := fmt.Sprintf("the pull requests on run branch %s could not be read to decide adoption: %s; the forge was temporarily unavailable, so the run is parked for bounded retry by the operator", r.cfg.Branch, err.Error())
			r.terminate(r.transientLaunchReadTermination(reason))
			return exitNeedsHuman, true
		}
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1, true
	}
	switch observation.kind {
	case prStateMissing:
		return 0, false
	case prStateOpen:
		r.terminate(termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: fmt.Sprintf("resolve branch already has open PR #%d on base %q; reporting the existing pull request to the source without adopting", observation.open.Number, r.cfg.Base)})
		return exitSuccess, true
	case prStateDraft:
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: r.adoptionDraftPRReason(observation.draft.Number), Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	case prStateWrongBase:
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: r.wrongBasePRReason(), Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	case prStateForeignHead:
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: r.foreignHeadPRReason(), Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	case prStateAmbiguous:
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: r.prAmbiguousReason(), Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	case prStateClosedOnly:
		closed := observation.closed[0]
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("resolve branch already has PR #%d (%s); refusing adoption", closed.Number, closed.State), Category: terminalCategoryUnknown})
		return exitNeedsHuman, true
	}
	return 0, false
}

// isPRRepairMode reports whether the mode operates on an existing pull
// request's own branch: fix-pr and rebase (#715). Both share the branch
// ownership guard below; they differ in everything execution does after it.
func isPRRepairMode(mode string) bool {
	return strings.EqualFold(mode, "fix-pr") || strings.EqualFold(mode, "rebase")
}

// guardFixPRHead enforces the fix-pr invariant that a run must operate on the
// fork that owns the PR head. If the head branch is missing on that fork, the
// run must stop and ask for a human; it must never adopt a same-named base
// branch or force-push elsewhere. The launch-time head read is the same git-side
// remote read the handoff classifies: a transient availability failure parks the
// run for the operator's bounded retry under the forge-unavailable category
// rather than crashing it, while a read that reached the fork and returned its
// own verdict keeps the fail-closed crash ending (#580). It returns 0 when the
// run may proceed. The guard assumes pod.go points COURIER_REPO_URL at the head
// repo for fix-pr runs; single-remote deployments without a %s placeholder are
// out of scope. Rebase runs take the same guard: a mechanical rebase publishes
// only to the head branch it was admitted against (#715).
func (r reporter) guardFixPRHead(ctx context.Context) int {
	if !isPRRepairMode(r.cfg.Mode) || r.cfg.HeadRepo == "" || strings.EqualFold(r.cfg.HeadRepo, r.cfg.Repo) {
		return 0
	}
	exists, err := git.RemoteBranchExists(ctx, r.cfg.RemoteURL, r.cfg.Branch)
	if err != nil {
		if forge.IsTransientAvailability(err) {
			// The read never reached the fork's answer, so the head's
			// presence is unobserved: park for the operator's bounded retry
			// under the forge-unavailable category rather than crash, and
			// never fall back to a same-named base branch (#580).
			reason := fmt.Sprintf("the PR head branch %s could not be read on the fork head repo %s: %s; the forge was temporarily unavailable, so the run is parked for bounded retry by the operator", r.cfg.Branch, r.cfg.HeadRepo, err.Error())
			r.terminate(r.transientLaunchReadTermination(reason))
			return exitNeedsHuman
		}
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}
	if !exists {
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("PR head branch %s was not found on the fork head repo %s; refusing to fall back to a same-named base branch", r.cfg.Branch, r.cfg.HeadRepo), Category: terminalCategoryUnknown})
		return exitNeedsHuman
	}
	return 0
}

// guardFixPRBase enforces the fix-pr invariant that the run's pinned base is
// the pull request's live target ref — stacked (non-default) and fork pull
// requests included — before workspace preparation fetches or merges
// anything. The target ref is read through the configured forge capability,
// never a forge-specific CLI, and compared by ref rather than by SHA: the
// base tip legitimately moves and the preparation step fetches it fresh.
// Repository identity is compared byte-exactly, as admission compared it when
// it pinned the base. A mismatch is fail-closed, because continuing under a
// wrong base silently rewrites the diff contract the pull request was opened
// against. A read that never completed proves nothing about the world, so it
// parks the run for the operator's bounded retry instead of failing it
// (#455). A pinned request that is no longer open is a moot repair condition:
// there is nothing left to repair, so the run ends without changing it, the
// same world the no-change claim is verified against (#515). It returns 0
// when the run may proceed.
func (r reporter) guardFixPRBase(ctx context.Context) int {
	if !isPRRepairMode(r.cfg.Mode) {
		return 0
	}
	if r.cfg.Ref < 1 {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "COURIER_REF is not set; a run against a pull request without its number cannot be validated", Category: terminalCategoryExecutorCrash})
		return 1
	}
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}
	provider := github.NewProvider(forge.ProviderConfig{Name: "github", Endpoint: r.cfg.GitHubAPIBase}, client)
	pr, err := provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: r.cfg.Repo, Number: r.cfg.Ref})
	if err != nil {
		if forge.IsTransientAvailability(err) {
			// The read could not complete, so the base was never
			// observed: park for the operator's bounded retry under the
			// forge-unavailable category rather than Failed, and never
			// proceed on an unverified base (#455).
			reason := fmt.Sprintf("pull request #%d of %q could not be read to validate the run's base: %s; the forge was temporarily unavailable, so the run is parked for bounded retry by the operator", r.cfg.Ref, r.cfg.Repo, err.Error())
			r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Outcome: outcomeBlockedExternal, Category: terminalCategoryForgeUnavailable})
			return exitNeedsHuman
		}
		var apiErr *github.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			// The pinned pull request does not exist: the assignment the
			// run was admitted against is gone, so there is no live base
			// to validate and nothing to repair. The run fails closed for
			// a human, stating the same restraint the handoff states — it
			// neither opens a replacement nor adopts another pull request.
			r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("pull request #%d of %q does not exist, so the pinned pull request is unverified; the executor neither opens a replacement nor adopts another pull request; the run needs a human", r.cfg.Ref, r.cfg.Repo), Category: terminalCategoryUnknown})
			return exitNeedsHuman
		}
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}
	observedState := pr.State
	if pr.Merged {
		// GitHub reports a merged pull request as closed; name the state
		// the operator can act on.
		observedState = "merged"
	}
	switch {
	case !strings.EqualFold(pr.State, "open"):
		// A pinned request the forge no longer reports open is a moot
		// repair condition: a merged or closed request cannot be
		// conflicted, so there is nothing for this run to repair. The
		// world settles this ending exactly as it settles a verified
		// no-change claim (#515), and the controller still settles it
		// against the world afterwards (#267). The run ends here rather
		// than preparing a workspace and starting a coordinator turn
		// against a request it may not touch. The ending is classified by
		// the world alone, so it carries no declared outcome.
		r.terminate(termination{Phase: "NoChangeNeeded", Result: "success", ExitCode: exitNoChangeNeeded, Reason: fmt.Sprintf("pull request #%d on %s is %s, not open; the repair condition is moot, so the run ends without changing it", r.cfg.Ref, r.cfg.Repo, observedState)})
		return exitNoChangeNeeded
	case pr.BaseRepo == "" || pr.BaseRef == "" || pr.BaseRepo != r.cfg.Repo:
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("pull request #%d of %q has an incomplete or retargeted base identity", r.cfg.Ref, r.cfg.Repo), Category: terminalCategoryUnknown})
		return exitNeedsHuman
	case pr.BaseRef != r.cfg.Base:
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("pull request #%d targets base %q, not the run's pinned base %q; refusing to synchronize against or complete under a base the pull request does not name", r.cfg.Ref, pr.BaseRef, r.cfg.Base), Category: terminalCategoryUnknown})
		return exitNeedsHuman
	}
	return 0
}
