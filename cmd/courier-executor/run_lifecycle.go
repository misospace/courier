package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	courierlog "github.com/misospace/courier/internal/log"

	"github.com/misospace/courier/internal/devenv"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/git"
)

// run sequences the executor's lifecycle phases: configuration, the run
// start event, the git environment, the launch guards, the rebase
// early-exit, workspace preparation with the outcome gate, the
// bootstrap-contract receipt, capability preflight, and the session loop.
// Each phase is a named unit beside it; run alone keeps the ordering
// contract, including the rebase early-exit between the guards and any
// workspace preparation.
func run(ctx context.Context, stdout, stderr io.Writer) int {
	cfg, err := readConfig(os.Getenv)
	report := newReporter(stdout, stderr, cfg)
	if err != nil {
		// Configuration failed before identity could be validated; fall back
		// to the raw environment so the exit event still carries run scope.
		report.cfg = envIdentity()
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}

	report.event(courierlog.EventRunStart, courierlog.StatusOK, map[string]any{
		"workspace": cfg.Directory,
		"base":      cfg.Base,
		"branch":    cfg.Branch,
		"executor":  cfg.OpenCodeBinary,
	})

	askpassPath, err := os.Executable()
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "resolve executable: " + err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}
	restoreIdentity, restoreEnv, code, stop := installRunEnvironment(ctx, report, askpassPath)
	defer restoreIdentity()
	defer restoreEnv()
	if stop {
		return code
	}

	if code, stop := runStartupGuards(ctx, orderedLaunchGuards(report)); stop {
		return code
	}

	// The rebase execution path is explicit and never shares fix-pr's flow
	// (#715): the workspace is adopted without the base-sync merge, execution
	// is the trusted deterministic rebase engine, and no model session starts.
	// A rebase run therefore cannot fall into merge preparation whose conflict
	// instructions prohibit rebase and force-push.
	if strings.EqualFold(cfg.Mode, "rebase") {
		return runRebaseMode(ctx, report, cfg)
	}

	workspace, startCommit, code := prepareWorkspace(ctx, report, cfg)
	if code != 0 {
		return code
	}
	outcomePath, code := prepareRunScratch(ctx, report, cfg, workspace, startCommit)
	if code != 0 {
		return code
	}

	cfg.Goal = applyOutcomeGateGoal(cfg.Goal, workspace.Conflict, outcomePath)
	ready := map[string]any{
		"adopted": workspace.Adopted,
		"base":    cfg.Base,
		"branch":  cfg.Branch,
	}
	if workspace.Conflict != nil {
		ready["conflicted_paths"] = len(workspace.Conflict.Paths)
	}
	report.event(courierlog.EventWorkspaceReady, courierlog.StatusOK, ready)

	// Child output is redacted before it reaches the run streams. The session
	// tap observes OpenCode's JSON events without changing their bytes. The
	// wrapping happens once, here: the model child streams through the same
	// writers.
	stdoutRedacted := courierlog.NewRedactingWriter(stdout, report.red)
	stderrTransport := courierlog.NewRedactingWriter(stderr, report.red)

	emitBootstrapContractReceipt(ctx, report, stdoutRedacted, stderrTransport, workspace, startCommit)

	runtime := executor.OpenCode{Binary: cfg.OpenCodeBinary, Format: cfg.OpenCodeFormat, Agent: cfg.OpenCodeAgent}
	caps := preflightCapabilities(ctx, runtime.MCPStatusCommand(), workspace.Directory)
	if len(caps) > 0 {
		report.emitCapabilityStatus(caps)
	}
	if note := capabilityNote(caps); note != "" {
		cfg.Framing = strings.TrimSpace(cfg.Framing + "\n\n" + report.red.Redact(note))
	}
	tap := newSessionTap(stdoutRedacted, parseExhaustionSignals(cfg.ExhaustionCodes, cfg.ExhaustionTypes))
	// Publish tel once, before any attempt can run: this write does not
	// take the tap lock, so reassigning it inside the retry loop would
	// race scan's read. See sessionTap.tel.
	tap.tel = report.tel

	s := &sessionRun{
		ctx:             ctx,
		cfg:             cfg,
		report:          report,
		workspace:       workspace,
		startCommit:     startCommit,
		outcomePath:     outcomePath,
		stdoutRedacted:  stdoutRedacted,
		stderrTransport: stderrTransport,
		runtime:         runtime,
		tap:             tap,
		caps:            caps,
	}
	return s.loop()
}

// installRunEnvironment installs the run's git identity and askpass helper,
// then trusts the workspace directory. The restore closures return to run so
// it owns their deferral: askpass unwinds before identity, exactly as the
// inline sequence did. A workspace that cannot be trusted terminates the run
// and stop is true.
func installRunEnvironment(ctx context.Context, report reporter, askpassPath string) (restoreIdentity, restoreEnv func(), code int, stop bool) {
	restoreIdentity = installGitIdentity()
	restoreEnv = installAskpass(askpassPath)
	if err := git.TrustDirectory(ctx, report.cfg.Directory); err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "trust workspace: " + err.Error(), Category: terminalCategoryExecutorCrash})
		return restoreIdentity, restoreEnv, 1, true
	}
	return restoreIdentity, restoreEnv, 0, false
}

// launchGuard is one startup invariant checked before workspace preparation.
// It returns a terminal exit code with stop true once it has emitted the
// run's handoff; (0, false) means the run may proceed.
type launchGuard func(context.Context) (int, bool)

// orderedLaunchGuards returns the run's launch guards in the exact order run
// applies them: branch adoption, the PR-head fork-ownership check, then the
// PR-base target check.
func orderedLaunchGuards(r reporter) []launchGuard {
	return []launchGuard{
		r.guardAdoption,
		func(ctx context.Context) (int, bool) { code := r.guardFixPRHead(ctx); return code, code != 0 },
		func(ctx context.Context) (int, bool) { code := r.guardFixPRBase(ctx); return code, code != 0 },
	}
}

// runStartupGuards applies the ordered guards and short-circuits on the
// first terminal verdict, the same stop semantics the inline sequence gave.
func runStartupGuards(ctx context.Context, guards []launchGuard) (int, bool) {
	for _, guard := range guards {
		if code, stop := guard(ctx); stop {
			return code, true
		}
	}
	return 0, false
}

// prepareWorkspace prepares or adopts the run workspace and records its
// start commit. Failed preparation or an unreadable head terminates the run
// and returns a nonzero exit code.
func prepareWorkspace(ctx context.Context, report reporter, cfg config) (*git.Workspace, string, int) {
	workspace, err := git.Prepare(ctx, git.PrepareOptions{
		RemoteURL:     cfg.RemoteURL,
		BaseRemoteURL: cfg.BaseRemoteURL,
		Directory:     cfg.Directory,
		Base:          cfg.Base,
		Branch:        cfg.Branch,
	})
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return nil, "", 1
	}
	startCommit, err := workspace.Head(ctx)
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "record workspace start: " + err.Error(), Category: terminalCategoryExecutorCrash})
		return nil, "", 1
	}
	return workspace, startCommit, 0
}

// prepareRunScratch arms the outcome gate: the run scratch directory exists
// and no stale declaration survives. Both terminal sites sit after the
// workspace exists and its start commit is recorded, so they take the gate's
// look: an adopted workspace can be mid-merge (dirty) before the first turn.
func prepareRunScratch(ctx context.Context, report reporter, cfg config, workspace *git.Workspace, startCommit string) (string, int) {
	outcomePath := filepath.Join(cfg.ScratchDirectory, defaultOutcomeFilename)
	if err := os.MkdirAll(cfg.ScratchDirectory, 0o700); err != nil {
		report.terminateCaptured(ctx, workspace, startCommit, failed("prepare run scratch directory: "+err.Error()))
		return "", exitFailed
	}
	if err := os.Remove(outcomePath); err != nil && !os.IsNotExist(err) {
		report.terminateCaptured(ctx, workspace, startCommit, failed("clear stale outcome declaration: "+err.Error()))
		return "", exitFailed
	}
	return outcomePath, 0
}

// applyOutcomeGateGoal rewrites the run goal for the outcome gate: the
// default scratch path the goal names is replaced with this run's actual
// outcome path, and a conflict note is appended when preparation left the
// workspace mid-merge so the model resolves it before anything else.
func applyOutcomeGateGoal(goal string, conflict *git.MergeConflictError, outcomePath string) string {
	goal = strings.ReplaceAll(goal, filepath.Join(defaultScratchDirectory, defaultOutcomeFilename), outcomePath)
	if conflict != nil {
		goal = strings.TrimSpace(goal + "\n\n" + conflictNote(conflict))
	}
	return goal
}

// emitBootstrapContractReceipt classifies the repository-owned development
// bootstrap contract (#414) with pure git reads and emits one receipt event
// per verdict. The legacy executor is credential-bearing, so it NEVER
// executes repository contract content: execution happens only in the secure
// worker's credential-free context, and an approved contract here is skipped
// with a receipt that names exactly that. The run's redacted stream writers
// are flushed after the receipt, as the inline sequence flushed them.
func emitBootstrapContractReceipt(ctx context.Context, report reporter, stdoutRedacted, stderrTransport *courierlog.RedactingWriter, workspace *git.Workspace, startCommit string) {
	state, stateErr := bootstrapContractState(ctx, workspace.Directory, workspace.BaseRevisionRef(), startCommit)
	switch {
	case stateErr != nil:
		reason := report.red.Redact("bootstrap contract state unreadable: " + stateErr.Error())
		report.eventVerbose(courierlog.EventWorkspaceBootstrap, courierlog.StatusError, map[string]any{
			"status": "skipped",
			"reason": truncatedRunes(reason, bootstrapReasonLimit),
		})
	case state == devenv.StateAbsent:
		report.eventVerbose(courierlog.EventWorkspaceBootstrap, courierlog.StatusOK, map[string]any{
			"status": "skipped",
			"reason": "",
		})
	case state == devenv.StateModified:
		report.eventVerbose(courierlog.EventWorkspaceBootstrap, courierlog.StatusOK, map[string]any{
			"status": "skipped",
			"reason": "bootstrap contract is not provably identical to the approved base revision; not executed",
		})
	case state == devenv.StateApproved:
		// The contract is approved, but this executor is credential-bearing
		// and must never execute repository contract code (#414 review): the
		// run continues unbootstrapped — exactly like the absent and
		// modified branches — and the secure worker topology owns
		// execution. No exec, no environment application, no termination.
		// The receipt still names whether the environment could provision
		// the contract's tools at all, so an underprovisioned runtime image
		// is visible before model work instead of surfacing as a mid-run
		// missing-tool hunt.
		_, miseErr := exec.LookPath("mise")
		report.eventVerbose(courierlog.EventWorkspaceBootstrap, courierlog.StatusOK, map[string]any{
			"status": "skipped",
			"reason": approvedContractSkipReason(miseErr == nil),
		})
	}
	_ = stdoutRedacted.Flush()
	_ = stderrTransport.Flush()
}

func processExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}
