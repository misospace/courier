package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	courierlog "github.com/misospace/courier/internal/log"

	"github.com/misospace/courier/internal/evidence"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/git"
)

// sessionRun carries one executor session loop: the prepared workspace,
// reporter, runtime, and tap, plus the continuation bookkeeping the loop
// phases read and mutate. Threading the state through one struct keeps each
// phase — attempt, exhaustion, failure recovery, verification, guidance —
// an individually testable unit instead of a closure over run's locals.
type sessionRun struct {
	ctx             context.Context
	cfg             config
	report          reporter
	workspace       *git.Workspace
	startCommit     string
	outcomePath     string
	stdoutRedacted  *courierlog.RedactingWriter
	stderrTransport *courierlog.RedactingWriter
	runtime         executor.OpenCode
	tap             *sessionTap
	caps            []executor.MCPCapability
	exhaustion      atomic.Bool

	// Recoverable workspace endings and crashes share one continuation
	// budget; provider availability failures keep their own backed-off
	// budget (#327).
	continuations   int
	crashes         int
	availability    int
	stateMessage    string
	prevFingerprint string
	history         []string
}

// loop runs model attempts until one produces a terminal handoff, applying
// the shared recovery path to child failures, the outcome gate to declared
// endings, and the verified git state to undeclared ones.
func (s *sessionRun) loop() int {
	for {
		s.report.tel.SetContinuations(s.continuations)
		if s.continuations > 0 {
			// A declaration belongs to one completed turn; never let a stale
			// declaration from an earlier turn decide the resumed run. A
			// previous turn may have left the workspace dirty, so this
			// post-workspace terminal site takes the gate's look.
			if err := os.Remove(s.outcomePath); err != nil && !os.IsNotExist(err) {
				s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, failed("clear stale outcome declaration: "+err.Error()))
				return exitFailed
			}
		}

		processErr := s.attempt()

		if result, exhausted := s.exhaustionTermination(); exhausted {
			s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, result)
			return exitNeedsHuman
		}

		if processErr != nil {
			if cont, exitCode := s.recoverAttemptFailure(processErr); !cont {
				return exitCode
			}
			continue
		}

		if s.workspace.Conflict != nil {
			if reason := unresolvedBaseSync(s.ctx, s.workspace); reason != "" {
				s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Category: terminalCategoryUnknown})
				return exitNeedsHuman
			}
		}

		decl, declared, declErr := readOutcome(s.outcomePath)
		if err := os.Remove(s.outcomePath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(s.report.stderr, "courier: could not remove outcome declaration: %s\n", s.report.red.Redact(err.Error()))
		}
		workState, err := s.workspace.WorkState(s.ctx, s.startCommit)
		if err != nil {
			s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, failed("inspect workspace result: "+err.Error()))
			return exitFailed
		}
		if declErr != nil || declared {
			result := s.report.classify(s.ctx, s.workspace, s.cfg.Branch, s.startCommit, workState, decl, declared, declErr, s.caps)
			// Authoritative source-issue linkage is a run-ending contract
			// (#248) regardless of how the run was declared: a Verifying
			// candidate must satisfy the same shared repair/budget
			// mechanism the undeclared-Verifying path uses. The declared
			// path therefore participates in the same continuation/repair
			// loop as the undeclared path — without losing the declared
			// outcome tag, which classifyVerifyingCandidate preserves on the
			// verifier-rejection terminal (a transient forge read instead
			// ends blocked_external, #455). This keeps the legacy executor
			// from forcing NeedsHuman on a declared run whose captured
			// session still has unused continuation budget.
			if result.Phase == "Verifying" {
				head, headErr := s.workspace.Head(s.ctx)
				_ = headErr
				cont, exitCode, terminal := s.classifyVerifyingCandidate(result, head)
				if cont {
					continue
				}
				s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, terminal)
				return exitCode
			}
			s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, result)
			return result.ExitCode
		}

		// With no explicit declaration, the verified git state decides whether
		// the turn succeeded or needs another opportunity in the same session.
		if cont, exitCode := s.settleUndeclaredTurn(workState); !cont {
			return exitCode
		}
	}
}

// settleUndeclaredTurn applies the undeclared-outcome contract to the
// verified git state: committed work on the run branch runs the shared
// pre-handoff plan, and off-branch, uncommitted, or no-work endings either
// resume the captured session with guidance or classify the ending from the
// world alone. It returns cont true when the loop should run another attempt
// with the instruction it recorded, and otherwise the exit code of the
// handoff it already emitted.
func (s *sessionRun) settleUndeclaredTurn(workState git.WorkState) (bool, int) {
	head, headErr := s.workspace.Head(s.ctx)
	branch, branchErr := s.workspace.CurrentBranch(s.ctx)
	dirty, dirtyErr := s.workspace.StatusPorcelain(s.ctx)
	slices.Sort(dirty)
	worldReadFailed := headErr != nil || branchErr != nil || dirtyErr != nil

	var kind, message, summary string
	switch workState {
	case git.WorkStateCommitted:
		ahead, branchReadErr := s.workspace.CommitsOnBranchSince(s.ctx, s.cfg.Branch, s.startCommit)
		if branchReadErr != nil || ahead > 0 {
			// Committed work on the run branch is a candidate for
			// the Verifying handoff, but the legacy executor must
			// still observe the forge before claiming success: the
			// shared applyHandoffPlan check (#248 linkage, #247
			// presence/draft, #249 criteria) is the single mechanism
			// for both the declared and undeclared Verifying paths.
			// A verified open, non-draft PR with a valid body
			// passes; a missing PR, a draft PR, an invalid body, an
			// invalid criterion-evidence block, or an unverifiable
			// forge read retries inside the captured session's
			// bounded continuation budget; a confirmed ambiguous,
			// closed-only, foreign-head, wrong-base, or not-
			// currently-published head, an exhausted budget, or a
			// session that cannot be resumed escalates to
			// NeedsHuman.
			cont, exitCode, terminal := s.classifyVerifyingCandidate(termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: "opencode completed with committed work"}, head)
			if !cont {
				s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, terminal)
			}
			return cont, exitCode
		}
		kind, message, summary = offBranchGuidance(s.cfg.Branch, branch, branchErr, head, s.tap.text())
	case git.WorkStateDirty:
		kind, message, summary = uncommittedGuidance(s.cfg.Branch, head, dirty, dirtyErr, s.tap.text())
	default:
		kind, message, summary = noWorkGuidance(s.cfg.Branch, s.caps, head, s.tap.text())
	}

	if s.tap.session() == "" {
		// Without a captured session there is nothing to resume; apply the
		// ordinary undeclared-outcome contract to the verified world
		// state. The committed-work Verifying ending was already settled
		// above through the shared pre-handoff plan (#247, #248), which
		// fails closed without a session, so classifyUndeclared here can
		// only return non-Verifying endings.
		result := s.report.classifyUndeclared(s.ctx, s.workspace, s.cfg.Branch, s.startCommit, workState, s.caps)
		s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, result)
		return false, result.ExitCode
	}

	fingerprint, err := continuationFingerprint(workState, branch, head, dirty, s.tap.text())
	if err != nil {
		s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, failed("fingerprint workspace state: "+err.Error()))
		return false, exitFailed
	}
	if !worldReadFailed && s.prevFingerprint != "" && fingerprint == s.prevFingerprint {
		s.history = append(s.history, summary)
		s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: "looping: " + summarizeHistory(s.history), Category: terminalCategoryContinuationExhaustion})
		return false, exitNeedsHuman
	}
	if s.continuations >= s.cfg.MaxContinuations {
		s.history = append(s.history, summary)
		s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("looping after %d continuations: %s", s.continuations, summarizeHistory(s.history)), Category: terminalCategoryContinuationExhaustion})
		return false, exitNeedsHuman
	}
	s.continuations++
	s.report.event(courierlog.EventExecutorContinuation, courierlog.StatusOK, map[string]any{
		"kind":         kind,
		"fingerprint":  fingerprint,
		"continuation": s.continuations,
	})
	s.history = append(s.history, summary)
	if worldReadFailed {
		s.prevFingerprint = ""
	} else {
		s.prevFingerprint = fingerprint
	}
	s.stateMessage = message
	// The next turn may clobber this state; the gate takes its own look
	// and decides whether it is worth preserving.
	s.report.maybeCaptureEvidence(s.ctx, s.workspace, s.startCommit, evidence.TriggerContinuation)
	return true, 0
}

// attempt runs one executor turn: it builds the invocation (a resumed
// instruction replaces goal and framing when the loop has state to convey),
// streams the child through the redacting writers with the tap observing,
// and returns the child's error. The flushes keep the inline order so no
// output is re-sequenced.
func (s *sessionRun) attempt() error {
	invocation := executor.Invocation{
		Goal:      s.cfg.Goal,
		Model:     s.cfg.Model,
		Framing:   s.cfg.Framing,
		Workspace: s.workspace.Directory,
	}
	if s.stateMessage != "" {
		invocation = executor.Invocation{
			Goal:      s.stateMessage,
			Model:     s.cfg.Model,
			Workspace: s.workspace.Directory,
			Session:   s.tap.session(),
		}
	}
	command := s.runtime.Command(invocation)
	modelStdout := s.tap.beginAttempt()
	s.report.event(courierlog.EventExecutorStart, courierlog.StatusOK, map[string]any{
		"executor": s.runtime.Name(),
		"format":   s.cfg.OpenCodeFormat,
		"resumed":  s.stateMessage != "",
	})
	process := exec.CommandContext(s.ctx, command.Binary, command.Args...)
	// The model child runs in its own process group so cancellation kills
	// the whole tree: exec.CommandContext today kills only the direct
	// child, and a grandchild (git, a compiler, a stuck tool) would keep
	// writing into the workspace while a cancellation-triggered capture
	// snapshots it (DESIGN.md, #115).
	process.SysProcAttr = processGroupSysProcAttr()
	process.Cancel = func() error {
		if process.Process == nil {
			return os.ErrProcessDone
		}
		return killProcessGroup(process.Process.Pid, syscall.SIGKILL)
	}
	s.tap.setKill(func() { s.exhaustion.Store(true); _ = process.Cancel() })
	// Bound the wait for the group after the context is done; the margin
	// keeps the capture inside the 45-second termination grace.
	process.WaitDelay = 10 * time.Second
	process.Dir = s.workspace.Directory
	process.Stdout = modelStdout
	process.Stderr = s.stderrTransport
	processErr := childReaper().run(process)
	_ = s.tap.Flush()
	s.tap.setKill(nil)
	_ = s.stdoutRedacted.Flush()
	_ = s.stderrTransport.Flush()
	return processErr
}

// exhaustionTermination decides the confirmed provider-exhaustion ending
// after an attempt whose kill callback fired. The callback fires on the
// structured error event itself, causing the command context to kill the
// whole process group; any later output from that killed turn is ignored
// and cannot trigger a continuation.
func (s *sessionRun) exhaustionTermination() (termination, bool) {
	if s.ctx.Err() != nil || !s.exhaustion.Load() {
		return termination{}, false
	}
	class, code, exhausted := s.tap.confirmedExhaustion()
	if !exhausted {
		class, code, exhausted = s.tap.lastExhaustion()
	}
	if !exhausted {
		return termination{}, false
	}
	return s.report.capacityExhaustedTermination(s.ctx, s.workspace, s.cfg, s.startCommit, class, code), true
}

// recoverAttemptFailure applies the shared recovery path to a failed child
// attempt. Every raw nonzero child status — including 2, which is only
// Courier's own exit constant and never a declaration by the child — takes
// this path. NeedsHuman is reserved for verified outcome handoffs; a
// provider outage or crash gets its bounded budget and, when spent, the
// recoverable availability handoff instead of a stranding Failed.
// Availability failures (#327 Scenario D): a provider outage makes OpenCode
// exit without ever serving a session id for this attempt. Those failures
// get their own bounded, backed-off budget: they never consume the
// task-repair continuation budget, and exhausting them ends the run on the
// recoverable capacity-handoff channel — as an availability handoff, never
// pretending a structured quota signal fired — so another capacity can take
// the work instead of it stranding as Failed. Any attempt that got a session
// proves the provider served us and resets the budget. It returns cont true
// when the loop should run another attempt (stateMessage is then set), and
// otherwise the exit code of the handoff it already emitted.
func (s *sessionRun) recoverAttemptFailure(processErr error) (bool, int) {
	code := processExitCode(processErr)
	if code < 0 {
		code = exitFailed
	}
	if !s.tap.sawSessionThisAttempt() {
		if s.ctx.Err() == nil && s.availability < s.cfg.MaxAvailabilityRetries {
			s.availability++
			s.report.event(courierlog.EventExecutorContinuation, courierlog.StatusOK, map[string]any{
				"kind":    "availability-retry",
				"code":    code,
				"attempt": s.availability,
			})
			// Same gate as the crash continuation: a dying tool may
			// have left uncommitted work from an earlier session-
			// serving attempt; a retry is exactly the moment the
			// next turn could clobber it.
			s.report.maybeCaptureEvidence(s.ctx, s.workspace, s.startCommit, evidence.TriggerCrash)
			delay := s.cfg.ResumeBackoff * time.Duration(1<<min(s.availability-1, maxAvailabilityBackoffCap))
			select {
			case <-time.After(delay):
			case <-s.ctx.Done():
			}
			if s.stateMessage == "" && s.tap.session() != "" {
				s.stateMessage = fmt.Sprintf("Your previous turn ended unexpectedly (opencode exited with status %d). Continue working toward the goal.", code)
			}
			return true, 0
		}
		if s.ctx.Err() == nil {
			s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, s.report.availabilityRecoveryTermination(s.ctx, s.workspace, s.cfg, s.startCommit, terminalCategoryProviderUnavailable, fmt.Sprintf("model provider failed to serve %d consecutive attempts without a session (last exit %d)", s.cfg.MaxAvailabilityRetries, code)))
			return false, exitNeedsHuman
		}
	} else {
		s.availability = 0
	}
	if s.tap.session() != "" && s.ctx.Err() == nil && s.continuations < s.cfg.MaxContinuations {
		s.continuations++
		s.crashes++
		s.report.event(courierlog.EventExecutorContinuation, courierlog.StatusOK, map[string]any{
			"kind":         "crash",
			"code":         code,
			"continuation": s.continuations,
		})
		// A run that crashes dirty would be left uncovered; the gate
		// decides whether this moment is worth preserving before the
		// next turn may clobber it.
		s.report.maybeCaptureEvidence(s.ctx, s.workspace, s.startCommit, evidence.TriggerCrash)
		delay := s.cfg.ResumeBackoff * time.Duration(1<<min(s.crashes-1, 10))
		select {
		case <-time.After(delay):
		case <-s.ctx.Done():
		}
		if s.ctx.Err() != nil {
			s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, failedWith(fmt.Sprintf("opencode exited with status %d and the context was canceled", code), terminalCategoryModelTransportError))
			return false, exitFailed
		}
		s.stateMessage = fmt.Sprintf("Your previous turn ended unexpectedly (opencode exited with status %d). Continue working toward the goal.", code)
		return true, 0
	}
	// The task-repair budget is spent. Repeated crashes with no
	// provable task defect must not become a permanent Failed: a
	// Failed run holds its claim and strands the work (#327). End
	// on the recoverable availability-handoff channel — the same
	// honest child-process verdict the canceled-context path already
	// uses, never a quota claim — so fenced recovery can move the
	// work to a healthy capacity. A canceled context is not provider
	// trouble: it stays a plain Failed, since the operator chose to
	// stop this run.
	reason := fmt.Sprintf("opencode exited with status %d", code)
	if s.continuations > 0 {
		reason += fmt.Sprintf(" after %d continuations", s.continuations)
	}
	if s.ctx.Err() != nil {
		reason += " and the context was canceled"
		s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, failedWith(reason, terminalCategoryModelTransportError))
		return false, exitFailed
	}
	s.report.terminateCaptured(s.ctx, s.workspace, s.startCommit, s.report.availabilityRecoveryTermination(s.ctx, s.workspace, s.cfg, s.startCommit, terminalCategoryModelTransportError, reason))
	return false, exitNeedsHuman
}

// classifyVerifyingCandidate settles a Verifying handoff candidate —
// declared or undeclared — through the shared pre-handoff plan (#248). It
// returns cont true after recording a continuation for the captured
// session; otherwise it returns the terminal the run ends on. fallback is
// the ending to emit when the plan passes: the classify result on the
// declared path (whose outcome tag the verifier-rejection terminal also
// preserves), the plain Verifying success on the undeclared one.
func (s *sessionRun) classifyVerifyingCandidate(fallback termination, head string) (bool, int, termination) {
	verdict := s.report.applyHandoffPlan(s.ctx, s.continuations, s.cfg.MaxContinuations, s.tap.session(), s.ctx.Err())
	if verdict.ContinueMsg != "" {
		s.continuations++
		s.stateMessage = verdict.ContinueMsg
		detail := map[string]any{
			"kind":         verdict.Kind,
			"continuation": s.continuations,
			"instruction":  s.stateMessage,
		}
		if verdict.PRNumber > 0 {
			detail["pr"] = verdict.PRNumber
		}
		s.report.eventVerbose(courierlog.EventExecutorContinuation, courierlog.StatusOK, detail)
		s.history = append(s.history, stateSummary(verdict.Kind, head, s.tap.text(), nil))
		return true, 0, termination{}
	}
	if verdict.FailReason != "" {
		if verdict.ForgeUnavailable {
			return false, exitNeedsHuman, s.report.forgeUnavailableTermination(s.workspace, s.cfg, s.startCommit, verdict.FailReason)
		}
		return false, exitNeedsHuman, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: verdict.FailReason, Outcome: fallback.Outcome, Category: terminalCategoryVerifierRejection}
	}
	if verdict.Superseded {
		// The pinned pull request merged during the run: the world settled
		// the repair assignment, so this ending is classified from the world
		// alone and carries no declared outcome (#582) — the same
		// NoChangeNeeded terminal the startup guard gives a pinned request
		// already merged at admission (#515), never a delivery claim for
		// post-merge commits.
		return false, exitNoChangeNeeded, termination{Phase: "NoChangeNeeded", Result: "success", ExitCode: exitNoChangeNeeded, Reason: s.report.mergedPinnedPRReason()}
	}
	if verdict.Pass {
		if verdict.Coverage != "" {
			s.report.handoffCoverage = verdict.Coverage
			// #282 step 4: a passing pinned handoff surfaces the derived
			// coverage and per-status counts on the run's source issue as
			// well as in the run.exit telemetry. Best-effort like every
			// outcome comment: a forge failure never blocks the handoff.
			s.report.postOutcomeComment(s.ctx, handoffCoverageComment(s.report.cfg.RunID, verdict.Coverage, verdict.StatusCounts))
		}
		if len(verdict.StatusCounts) > 0 {
			s.report.handoffStatusCounts = verdict.StatusCounts
		}
	}
	return false, fallback.ExitCode, fallback
}

// offBranchGuidance composes the continuation guidance for committed work
// that is not on the run branch.
func offBranchGuidance(runBranch, branch string, branchErr error, head, lastText string) (string, string, string) {
	whereClause := "a detached HEAD"
	if branchErr != nil {
		whereClause = "an unknown branch"
	} else if branch != "" {
		whereClause = fmt.Sprintf("branch %q", branch)
	}
	kind := "off-branch"
	message := fmt.Sprintf("You committed work that is not on the run branch %q (HEAD is on %s). Move your commits onto %q and push, or declare needs_decision or blocked_external if a human is needed.", runBranch, whereClause, runBranch)
	summary := stateSummary(kind, head, lastText, nil)
	return kind, message, summary
}

// uncommittedGuidance composes the continuation guidance for a dirty
// workspace, listing at most maxContinuationPaths changed paths.
func uncommittedGuidance(runBranch, head string, dirty []string, dirtyErr error, lastText string) (string, string, string) {
	listed := dirty
	more := ""
	if len(listed) > maxContinuationPaths {
		more = fmt.Sprintf(" (and %d more)", len(listed)-maxContinuationPaths)
		listed = listed[:maxContinuationPaths]
	}
	kind := "uncommitted"
	var message string
	if dirtyErr != nil {
		message = fmt.Sprintf("You ended with uncommitted changes; commit and push them to %q, or discard them and declare needs_decision or blocked_external if a human is needed.", runBranch)
	} else {
		message = fmt.Sprintf("You ended with uncommitted changes in %s%s; commit and push them to %q, or discard them and declare needs_decision or blocked_external if a human is needed.", strings.Join(listed, ", "), more, runBranch)
	}
	summary := stateSummary(kind, head, lastText, listed)
	return kind, message, summary
}

// noWorkGuidance composes the continuation guidance for a turn that produced
// neither a commit nor workspace changes, naming any unavailable configured
// capability.
func noWorkGuidance(runBranch string, caps []executor.MCPCapability, head, lastText string) (string, string, string) {
	kind := "no-work"
	message := fmt.Sprintf("You ended without producing a commit or workspace changes and did not declare an outcome. Continue the work and push it to %q, or declare needs_decision or blocked_external if a human is needed.", runBranch)
	summary := stateSummary(kind, head, lastText, nil)
	if names := unavailableCapabilities(caps); len(names) > 0 {
		suffix := "; configured capability unavailable: " + strings.Join(names, ", ")
		message += suffix
		summary += suffix
	}
	return kind, message, summary
}
