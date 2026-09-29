// Command courier-executor is the executable bootstrap for the temporary
// OpenCode runtime. It prepares the ephemeral git workspace, then delegates
// the actual model run to headless OpenCode.
//
// The bootstrap owns the run's structured event log: one JSON event line per
// lifecycle boundary (run start, workspace ready, executor start, run exit)
// on stdout, redacted before emission, alongside the legacy
// COURIER_TERMINATION line that records the terminal handoff.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/github"
	courierlog "github.com/misospace/courier/internal/log"
)

const (
	exitSuccess    = 0
	exitNeedsHuman = 2
	defaultBase    = "main"
	defaultWork    = "/workspace"
	defaultFormat  = "json"

	defaultMaxContinuations = 3
	defaultResumeBackoff    = 5 * time.Second
)

type config struct {
	RemoteURL        string
	BaseRemoteURL    string
	Directory        string
	Base             string
	Branch           string
	Repo             string
	HeadRepo         string
	HeadSHA          string
	Mode             string
	RunID            string
	Ref              int
	Goal             string
	Model            string
	Framing          string
	GitHubAPIBase    string
	OpenCodeBinary   string
	OpenCodeFormat   string
	OpenCodeAgent    string
	MaxContinuations int
	ResumeBackoff    time.Duration
	TerminationFile  string
	GitUsername      string
	GitToken         string
	GitHubToken      string
}

type termination struct {
	Phase    string `json:"phase"`
	Result   string `json:"result"`
	ExitCode int    `json:"exit_code"`
	Reason   string `json:"reason"`
}

func main() {
	if os.Getenv("COURIER_ASKPASS") == "1" {
		os.Exit(askpass(os.Args[1:], os.Getenv("COURIER_GIT_USERNAME"), os.Getenv("COURIER_GIT_TOKEN"), os.Stdout))
	}
	os.Exit(run(context.Background(), os.Stdout, os.Stderr))
}

func readConfig(getenv func(string) string) (config, error) {
	remoteURL := strings.TrimSpace(getenv("COURIER_REPO_URL"))
	if remoteURL == "" {
		remoteURL = strings.TrimSpace(getenv("COURIER_REMOTE_URL"))
	}
	ref, _ := strconv.Atoi(strings.TrimSpace(getenv("COURIER_REF")))
	maxContinuations, _ := strconv.Atoi(strings.TrimSpace(getenv("COURIER_MAX_CONTINUATIONS")))
	if maxContinuations < 1 {
		maxContinuations = defaultMaxContinuations
	}
	resumeBackoff, err := strconv.ParseFloat(strings.TrimSpace(getenv("COURIER_RESUME_BACKOFF_SECONDS")), 64)
	if err != nil || math.IsNaN(resumeBackoff) || math.IsInf(resumeBackoff, 0) || resumeBackoff < 0 {
		resumeBackoff = defaultResumeBackoff.Seconds()
	}
	cfg := config{
		RemoteURL:        remoteURL,
		BaseRemoteURL:    strings.TrimSpace(getenv("COURIER_BASE_REPO_URL")),
		Directory:        strings.TrimSpace(getenv("COURIER_WORKSPACE")),
		Base:             strings.TrimSpace(getenv("COURIER_BASE")),
		Branch:           strings.TrimSpace(getenv("COURIER_BRANCH")),
		Repo:             strings.TrimSpace(getenv("COURIER_REPO")),
		HeadRepo:         strings.TrimSpace(getenv("COURIER_HEAD_REPO")),
		HeadSHA:          strings.TrimSpace(getenv("COURIER_HEAD_SHA")),
		Goal:             strings.TrimSpace(getenv("COURIER_GOAL")),
		Model:            strings.TrimSpace(getenv("COURIER_MODEL")),
		Framing:          getenv("COURIER_FRAMING"),
		Mode:             strings.TrimSpace(getenv("COURIER_MODE")),
		RunID:            strings.TrimSpace(getenv("COURIER_RUN_NAME")),
		Ref:              ref,
		OpenCodeBinary:   strings.TrimSpace(getenv("COURIER_OPENCODE_BINARY")),
		OpenCodeFormat:   strings.TrimSpace(getenv("COURIER_OPENCODE_FORMAT")),
		OpenCodeAgent:    strings.TrimSpace(getenv("COURIER_OPENCODE_AGENT")),
		MaxContinuations: maxContinuations,
		ResumeBackoff:    time.Duration(resumeBackoff * float64(time.Second)),
		TerminationFile:  strings.TrimSpace(getenv("COURIER_TERMINATION_FILE")),
		GitUsername:      getenv("COURIER_GIT_USERNAME"),
		GitToken:         getenv("COURIER_GIT_TOKEN"),
		GitHubToken:      getenv("GITHUB_TOKEN"),
		GitHubAPIBase:    strings.TrimSpace(getenv("COURIER_GITHUB_API_BASE")),
	}
	if strings.TrimSpace(cfg.GitHubToken) == "" {
		cfg.GitHubToken = cfg.GitToken
	}
	if cfg.Directory == "" {
		cfg.Directory = defaultWork
	}
	if cfg.Base == "" {
		cfg.Base = defaultBase
	}
	if cfg.OpenCodeBinary == "" {
		cfg.OpenCodeBinary = "opencode"
	}
	if cfg.OpenCodeFormat == "" {
		cfg.OpenCodeFormat = defaultFormat
	}
	if cfg.GitHubAPIBase == "" {
		cfg.GitHubAPIBase = "https://api.github.com/"
	}
	for _, required := range []struct {
		name  string
		value string
	}{
		{name: "COURIER_REPO_URL", value: cfg.RemoteURL},
		{name: "COURIER_BRANCH", value: cfg.Branch},
		{name: "COURIER_GOAL", value: cfg.Goal},
		{name: "COURIER_MODEL", value: cfg.Model},
	} {
		if required.value == "" {
			return config{}, fmt.Errorf("%s is required", required.name)
		}
	}
	if strings.EqualFold(cfg.Mode, "fix-pr") && cfg.HeadRepo == "" {
		return config{}, errors.New("COURIER_HEAD_REPO is required for fix-pr runs")
	}
	if parsed, err := url.Parse(cfg.RemoteURL); err == nil && parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return config{}, errors.New("COURIER_REPO_URL must not contain credentials")
		}
	}
	return cfg, nil
}

// reporter emits the run's structured events and the terminal handoff. Every
// externally visible reason passes through the redactor before it is written.
type reporter struct {
	stdout io.Writer
	stderr io.Writer
	events *courierlog.Emitter
	red    *courierlog.Redactor
	cfg    config
}

// newReporter builds the run's reporter and registers every credential the
// process holds. Secret-shaped environment values (injected tokens and any
// deployment-provided provider keys) are registered by name shape, so no
// provider or deployment is special-cased here. The emitter level comes from
// COURIER_LOG_LEVEL, which the operator sets from the run's spec.debug.
func newReporter(stdout, stderr io.Writer, cfg config) reporter {
	events := courierlog.NewEmitter(stdout, courierlog.ParseLevel(os.Getenv("COURIER_LOG_LEVEL")))
	red := events.Redactor()
	red.RegisterEnvironment(os.Environ())
	red.Register(cfg.GitToken)
	red.Register(cfg.GitHubToken)
	return reporter{stdout: stdout, stderr: stderr, events: events, red: red, cfg: cfg}
}

// event writes one structured run event. Emission is best-effort: a failure
// is noted on stderr and never fails the run. Events without run identity
// are dropped by the emitter's contract.
func (r reporter) event(eventType, status string, detail map[string]any) {
	err := r.events.Emit(courierlog.Event{
		Type:   eventType,
		RunID:  r.cfg.RunID,
		Repo:   r.cfg.Repo,
		Ref:    r.cfg.Ref,
		Mode:   r.cfg.Mode,
		Model:  r.cfg.Model,
		Status: status,
		Detail: detail,
	})
	if err != nil {
		fmt.Fprintf(r.stderr, "courier: dropped %s event: %v\n", eventType, err)
	}
}

// terminate publishes the terminal handoff: the legacy COURIER_TERMINATION
// line with a redacted reason, plus the run.exit event.
func (r reporter) terminate(result termination) {
	result.Reason = r.red.Redact(result.Reason)
	emitTermination(r.stdout, r.cfg, result)
	status := courierlog.StatusOK
	switch result.Phase {
	case "Failed":
		status = courierlog.StatusError
	case "NeedsHuman":
		status = courierlog.StatusNeedsHuman
	}
	r.event(courierlog.EventRunExit, status, map[string]any{
		"exit_code": result.ExitCode,
		"phase":     result.Phase,
		"reason":    result.Reason,
	})
}

const mcpPreflightTimeout = 30 * time.Second

// preflightCapabilities runs a bounded `opencode mcp list` with the run's
// OpenCode binary and environment and returns parsed capability status. It
// never fails the run: on exec error, non-zero exit, timeout, or unparsable
// output it returns what it could parse (often nil). Probe stderr is
// discarded and only stdout is parsed, so unrelated CLI error text is never
// parsed as a capability; nothing is routed to the run's streams, so raw
// tool output stays out of the run log and away from the model.
func preflightCapabilities(ctx context.Context, command executor.Command, dir string) []executor.MCPCapability {
	probeCtx, cancel := context.WithTimeout(ctx, mcpPreflightTimeout)
	defer cancel()
	probe := exec.CommandContext(probeCtx, command.Binary, command.Args...)
	// Bound waiting for a grandchild that holds the output pipe after the
	// context is done.
	probe.WaitDelay = 5 * time.Second
	if strings.TrimSpace(dir) != "" {
		probe.Dir = dir
	}
	var out bytes.Buffer
	probe.Stdout = &out
	probe.Stderr = io.Discard
	_ = probe.Run()
	return executor.ParseMCPStatus(out.String())
}

// unavailableCapabilities returns the names of capabilities found configured
// but unavailable.
func unavailableCapabilities(caps []executor.MCPCapability) []string {
	var names []string
	for _, c := range caps {
		if !c.Available {
			names = append(names, c.Name)
		}
	}
	return names
}

// capabilityNote composes a concise, safe framing note for unavailable
// capabilities, or "" when none. Names are configuration identifiers and
// reasons are connection-error text, never config or credentials.
func capabilityNote(caps []executor.MCPCapability) string {
	var items []string
	for _, c := range caps {
		if c.Available {
			continue
		}
		if reason := strings.TrimSpace(c.Reason); reason != "" {
			items = append(items, c.Name+" ("+reason+")")
		} else {
			items = append(items, c.Name)
		}
	}
	if len(items) == 0 {
		return ""
	}
	return "Capability status: these configured tool servers are UNAVAILABLE: " + strings.Join(items, ", ") + ". Do not probe the environment or config to find their tools; if the goal cannot proceed without them, report that the run is blocked on the missing capability instead of improvising."
}

// emitCapabilityStatus reports the preflight as a structured diagnostic.
// Status is error when any configured capability is unavailable, else ok.
// Detail carries each server's name, availability, and safe reason and is
// marked Verbose so it is visible on non-debug runs; the emitter redacts it.
func (r reporter) emitCapabilityStatus(caps []executor.MCPCapability) {
	status := courierlog.StatusOK
	servers := make([]map[string]any, 0, len(caps))
	for _, c := range caps {
		if !c.Available {
			status = courierlog.StatusError
		}
		servers = append(servers, map[string]any{"name": c.Name, "available": c.Available, "reason": c.Reason})
	}
	err := r.events.Emit(courierlog.Event{
		Type:    courierlog.EventCapabilityStatus,
		RunID:   r.cfg.RunID,
		Repo:    r.cfg.Repo,
		Ref:     r.cfg.Ref,
		Mode:    r.cfg.Mode,
		Model:   r.cfg.Model,
		Status:  status,
		Verbose: true,
		Detail:  map[string]any{"servers": servers},
	})
	if err != nil {
		fmt.Fprintf(r.stderr, "courier: dropped %s event: %v\n", courierlog.EventCapabilityStatus, err)
	}
}

// tapMaxLine bounds the session tap's buffer for an unfinished line, so a
// child emitting one enormous line cannot grow it without limit.
const tapMaxLine = 1 << 20

// tapLastTextLimit bounds how much of the last assistant text part the tap
// keeps, in runes.
const tapLastTextLimit = 200

// sessionTap is a pass-through stdout transport that scans the child's
// newline-delimited JSON event lines while they pass. It captures the first
// sessionID and the last assistant text part so a recoverable exit can
// resume the same session; it never alters the bytes it forwards. lastText
// intentionally persists across resumed turns when a new turn emits no text.
type sessionTap struct {
	w         io.Writer
	pending   []byte
	sessionID string
	lastText  string
}

func newSessionTap(w io.Writer) *sessionTap {
	return &sessionTap{w: w}
}

func (t *sessionTap) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	t.pending = append(t.pending, p...)
	for {
		idx := bytes.IndexByte(t.pending, '\n')
		if idx < 0 {
			break
		}
		t.scan(t.pending[:idx])
		t.pending = t.pending[idx+1:]
	}
	if len(t.pending) > tapMaxLine {
		t.pending = nil
	}
	return n, err
}

// Flush scans a trailing partial line after the child exits.
func (t *sessionTap) Flush() error {
	if len(t.pending) > 0 {
		t.scan(t.pending)
		t.pending = nil
	}
	return nil
}

// scan extracts the session id and assistant text from one JSON event line.
// Lines that are not JSON events are ignored.
func (t *sessionTap) scan(line []byte) {
	var event struct {
		SessionID string `json:"sessionID"`
		Part      *struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"part"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return
	}
	if event.SessionID != "" && t.sessionID == "" {
		t.sessionID = event.SessionID
	}
	if event.Part != nil && event.Part.Type == "text" {
		t.lastText = truncatedRunes(event.Part.Text, tapLastTextLimit)
	}
}

func truncatedRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func run(ctx context.Context, stdout, stderr io.Writer) int {
	cfg, err := readConfig(os.Getenv)
	report := newReporter(stdout, stderr, cfg)
	if err != nil {
		// Configuration failed before identity could be validated; fall back
		// to the raw environment so the exit event still carries run scope.
		report.cfg = envIdentity()
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error()})
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
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "resolve executable: " + err.Error()})
		return 1
	}
	restoreIdentity := installGitIdentity()
	defer restoreIdentity()
	restoreEnv := installAskpass(askpassPath)
	defer restoreEnv()
	if err := git.TrustDirectory(ctx, cfg.Directory); err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "trust workspace: " + err.Error()})
		return 1
	}

	if code, stop := report.guardAdoption(ctx); stop {
		return code
	}

	if code := report.guardFixPRHead(ctx); code != 0 {
		return code
	}

	workspace, err := git.Prepare(ctx, git.PrepareOptions{
		RemoteURL:     cfg.RemoteURL,
		BaseRemoteURL: cfg.BaseRemoteURL,
		Directory:     cfg.Directory,
		Base:          cfg.Base,
		Branch:        cfg.Branch,
	})
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error()})
		return 1
	}
	startCommit, err := workspace.Head(ctx)
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "record workspace start: " + err.Error()})
		return 1
	}
	ready := map[string]any{
		"adopted": workspace.Adopted,
		"base":    cfg.Base,
		"branch":  cfg.Branch,
	}
	if workspace.Conflict != nil {
		ready["conflicted_paths"] = len(workspace.Conflict.Paths)
		cfg.Goal = strings.TrimSpace(cfg.Goal + "\n\n" + conflictNote(workspace.Conflict))
	}
	report.event(courierlog.EventWorkspaceReady, courierlog.StatusOK, ready)

	runtime := executor.OpenCode{Binary: cfg.OpenCodeBinary, Format: cfg.OpenCodeFormat, Agent: cfg.OpenCodeAgent}
	caps := preflightCapabilities(ctx, runtime.MCPStatusCommand(), workspace.Directory)
	if len(caps) > 0 {
		report.emitCapabilityStatus(caps)
	}
	if note := capabilityNote(caps); note != "" {
		cfg.Framing = strings.TrimSpace(cfg.Framing + "\n\n" + report.red.Redact(note))
	}
	// The child's output is untrusted verbose tool I/O: route both streams
	// through the run's redactor before they reach the real stdout/stderr
	// (DESIGN.md: redact before stdout). stdout is additionally passed through
	// a session tap that scans the JSON event lines for the session id and the
	// last assistant message; it never alters the bytes. Lines buffered across
	// Write chunks are flushed after the child exits.
	stdoutRedacted := courierlog.NewRedactingWriter(stdout, report.red)
	stderrTransport := courierlog.NewRedactingWriter(stderr, report.red)
	tap := newSessionTap(stdoutRedacted)

	// continuations is shared by crash retries and recoverable resumes;
	// crashes only drives the crash backoff.
	continuations, crashes := 0, 0
	var (
		stateMessage    string
		prevFingerprint string
		history         []string
	)

	for {
		var invocation executor.Invocation
		if stateMessage != "" {
			invocation = executor.Invocation{
				Goal:      stateMessage,
				Model:     cfg.Model,
				Workspace: workspace.Directory,
				Session:   tap.sessionID,
			}
		} else {
			invocation = executor.Invocation{
				Goal:      cfg.Goal,
				Model:     cfg.Model,
				Framing:   cfg.Framing,
				Workspace: workspace.Directory,
			}
		}
		command := runtime.Command(invocation)
		report.event(courierlog.EventExecutorStart, courierlog.StatusOK, map[string]any{
			"executor": runtime.Name(),
			"format":   cfg.OpenCodeFormat,
			"resumed":  stateMessage != "",
		})
		process := exec.CommandContext(ctx, command.Binary, command.Args...)
		process.Dir = workspace.Directory
		process.Stdout = tap
		process.Stderr = stderrTransport
		err := process.Run()
		// Release whatever the child left as a final partial line before the
		// outcome is reported, so output ordering stays faithful.
		_ = tap.Flush()
		_ = stdoutRedacted.Flush()
		_ = stderrTransport.Flush()

		if err != nil {
			code := processExitCode(err)
			if code < 0 {
				code = 1
			}
			if code == exitNeedsHuman {
				report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: code, Reason: "opencode requested human attention"})
				return code
			}
			// A canceled context ends the run; it is not a crash to resume.
			if tap.sessionID != "" && ctx.Err() == nil && continuations < cfg.MaxContinuations {
				continuations++
				crashes++
				report.event(courierlog.EventExecutorContinuation, courierlog.StatusOK, map[string]any{
					"kind":         "crash",
					"code":         code,
					"continuation": continuations,
				})
				d := cfg.ResumeBackoff * time.Duration(1<<min(crashes-1, 10))
				select {
				case <-time.After(d):
				case <-ctx.Done():
				}
				if ctx.Err() != nil {
					report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: code, Reason: fmt.Sprintf("opencode exited with status %d and the context was canceled", code)})
					return code
				}
				stateMessage = fmt.Sprintf("Your previous turn ended unexpectedly (opencode exited with status %d). Continue working toward the goal.", code)
				continue
			}
			reason := fmt.Sprintf("opencode exited with status %d", code)
			if continuations > 0 {
				reason += fmt.Sprintf(" after %d continuations", continuations)
			}
			report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: code, Reason: reason})
			return code
		}

		if workspace.Conflict != nil {
			if reason := unresolvedBaseSync(ctx, workspace); reason != "" {
				report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason})
				return exitNeedsHuman
			}
		}

		workState, err := workspace.WorkState(ctx, startCommit)
		if err != nil {
			report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "inspect workspace result: " + err.Error()})
			return 1
		}

		// The fingerprint and the state message are read from the world on
		// every pass; a read failure degrades them, never the run.
		head, headErr := workspace.Head(ctx)
		branch, branchErr := workspace.CurrentBranch(ctx)
		dirty, dirtyErr := workspace.StatusPorcelain(ctx)
		slices.Sort(dirty)
		// A failed world read degrades the fingerprint and the state message,
		// never the run: with a captured session the run still resumes.
		worldReadFailed := headErr != nil || branchErr != nil || dirtyErr != nil

		var kind, message, summary string
		switch workState {
		case git.WorkStateCommitted:
			// The run branch's own ref, not where HEAD happens to point, is the
			// world: committed work counts only when it is reachable from the run
			// branch. A branch-read error must not fail the run: liveness over
			// strictness, so an unreadable ref falls through to the success path.
			ahead, branchReadErr := workspace.CommitsOnBranchSince(ctx, cfg.Branch, startCommit)
			if branchReadErr != nil || ahead > 0 {
				report.terminate(termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: "opencode completed with committed work"})
				return exitSuccess
			}
			whereClause := "a detached HEAD"
			if branchErr != nil {
				whereClause = "an unknown branch"
			} else if branch != "" {
				whereClause = fmt.Sprintf("branch %q", branch)
			}
			kind = "off-branch"
			message = fmt.Sprintf("You committed work that is not on the run branch %q (HEAD is on %s). Move your commits onto %q and push, or exit with code 2 to request human attention.", cfg.Branch, whereClause, cfg.Branch)
			summary = stateSummary(kind, head, tap.lastText, nil)
		case git.WorkStateDirty:
			listed := dirty
			more := ""
			if len(listed) > maxContinuationPaths {
				more = fmt.Sprintf(" (and %d more)", len(listed)-maxContinuationPaths)
				listed = listed[:maxContinuationPaths]
			}
			kind = "uncommitted"
			if dirtyErr != nil {
				// The change list is unknown; say so rather than quoting an
				// empty list as if the worktree were clean.
				message = fmt.Sprintf("You ended with uncommitted changes; commit and push them to %q, or discard them and exit with code 2 explaining why.", cfg.Branch)
			} else {
				message = fmt.Sprintf("You ended with uncommitted changes in %s%s; commit and push them to %q, or discard them and exit with code 2 explaining why.", strings.Join(listed, ", "), more, cfg.Branch)
			}
			summary = stateSummary(kind, head, tap.lastText, listed)
		default:
			kind = "no-work"
			message = fmt.Sprintf("You ended without producing a commit or workspace changes and did not declare an outcome. Continue the work and push it to %q, or exit with code 2 to request human attention.", cfg.Branch)
			summary = stateSummary(kind, head, tap.lastText, nil)
			if names := unavailableCapabilities(caps); len(names) > 0 {
				suffix := "; configured capability unavailable: " + strings.Join(names, ", ")
				message += suffix
				summary += suffix
			}
		}

		if tap.sessionID == "" {
			// Nothing to resume: a recoverable ending without a captured
			// session terminates with the specific pre-resume reason, not a
			// new session whose whole prompt is the state message.
			report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: recoverableReason(kind, cfg.Branch, branch, caps)})
			return exitNeedsHuman
		}

		fingerprint := strings.Join([]string{
			string(workState), branch, head, strings.Join(dirty, ","), tap.lastText,
		}, "|")

		if !worldReadFailed {
			if prevFingerprint != "" && fingerprint == prevFingerprint {
				// Include the state that re-triggered the guard, so the
				// history names the exact state the run is looping on.
				history = append(history, summary)
				report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: "looping: " + summarizeHistory(history)})
				return exitNeedsHuman
			}
		}
		if continuations >= cfg.MaxContinuations {
			// Same: the state that hit the budget belongs in the history.
			history = append(history, summary)
			report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("looping after %d continuations: %s", continuations, summarizeHistory(history))})
			return exitNeedsHuman
		}
		continuations++
		report.event(courierlog.EventExecutorContinuation, courierlog.StatusOK, map[string]any{
			"kind":         kind,
			"fingerprint":  fingerprint,
			"continuation": continuations,
		})
		history = append(history, summary)
		if worldReadFailed {
			// A mangled fingerprint must not arm the guard: forget it.
			prevFingerprint = ""
		} else {
			prevFingerprint = fingerprint
		}
		stateMessage = message
	}
}

// maxConflictNotePaths bounds how many conflicted paths the goal lists.
const maxConflictNotePaths = 20

// maxContinuationPaths bounds how many dirty paths a continuation message
// lists.
const maxContinuationPaths = 20

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

// summarizeHistory numbers each recorded state for a looping termination
// reason, oldest first.
func summarizeHistory(history []string) string {
	entries := make([]string, len(history))
	for i, entry := range history {
		entries[i] = fmt.Sprintf("%d) %s", i+1, entry)
	}
	return strings.Join(entries, "; ")
}

// stateSummary renders a loop-guard history entry, keeping it sane when a
// degraded world read left a value empty.
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

// recoverableReason returns the pre-resume termination reason for a
// recoverable ending classified without a captured session id, keeping the
// specific wording the run used before continuations existed.
func recoverableReason(kind, runBranch, headBranch string, caps []executor.MCPCapability) string {
	switch kind {
	case "uncommitted":
		return "opencode exited successfully with uncommitted workspace changes"
	case "off-branch":
		whereClause := "a detached HEAD"
		if headBranch != "" {
			whereClause = fmt.Sprintf("branch %q", headBranch)
		}
		return fmt.Sprintf("opencode committed work that is not on the run branch %q (HEAD is on %s); the run branch has no new commits", runBranch, whereClause)
	}
	reason := "opencode exited successfully without producing a commit or workspace changes"
	if names := unavailableCapabilities(caps); len(names) > 0 {
		reason += "; configured capability unavailable: " + strings.Join(names, ", ")
	}
	return reason
}

// envIdentity reconstructs run identity from the environment for exit paths
// that run before configuration validation completed.
func envIdentity() config {
	ref, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("COURIER_REF")))
	return config{
		RunID: os.Getenv("COURIER_RUN_NAME"),
		Repo:  os.Getenv("COURIER_REPO"),
		Ref:   ref,
		Mode:  os.Getenv("COURIER_MODE"),
	}
}

// guardAdoption enforces the resolve-issue invariant that a deterministic
// branch may be adopted only when it is orphaned. If the branch already exists
// on the remote with an open pull request, the run refuses to adopt it but
// reports that PR for review by exiting 0, so the operator's world-verification
// publishes the existing pull request to the source as in-review. A branch
// whose only pull requests are closed or merged is handed to a human. It
// returns (0, false) when the run may proceed to Prepare, or a terminal
// (code, true) once it has emitted the run's handoff.
func (r reporter) guardAdoption(ctx context.Context) (int, bool) {
	if r.cfg.Mode != "resolve-issue" {
		return 0, false
	}
	exists, err := git.RemoteBranchExists(ctx, r.cfg.RemoteURL, r.cfg.Branch)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error()})
		return 1, true
	}
	if !exists {
		return 0, false
	}
	owner, name, ok := splitOwnerRepo(r.cfg.Repo)
	if !ok {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "resolve branch already exists on the remote and COURIER_REPO does not name an owner/repo; refusing adoption"})
		return 1, true
	}
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error()})
		return 1, true
	}
	pulls, err := client.PullRequestsForHead(ctx, owner, name, owner, r.cfg.Branch)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error()})
		return 1, true
	}
	for _, pull := range pulls {
		if pull.Head.Ref == r.cfg.Branch && strings.EqualFold(pull.State, "open") {
			r.terminate(termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: fmt.Sprintf("resolve branch already has open PR #%d; reporting the existing pull request to the source without adopting", pull.Number)})
			return exitSuccess, true
		}
	}
	for _, pull := range pulls {
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("resolve branch already has PR #%d (%s); refusing adoption", pull.Number, pull.State)})
		return exitNeedsHuman, true
	}
	return 0, false
}

// guardFixPRHead enforces the fix-pr invariant that a run must operate on the
// fork that owns the PR head. If the head branch is missing on that fork, the
// run must stop and ask for a human; it must never adopt a same-named base
// branch or force-push elsewhere. It returns 0 when the run may proceed. The
// guard assumes pod.go points COURIER_REPO_URL at the head repo for fix-pr
// runs; single-remote deployments without a %s placeholder are out of scope.
func (r reporter) guardFixPRHead(ctx context.Context) int {
	if r.cfg.Mode != "fix-pr" || r.cfg.HeadRepo == "" || strings.EqualFold(r.cfg.HeadRepo, r.cfg.Repo) {
		return 0
	}
	exists, err := git.RemoteBranchExists(ctx, r.cfg.RemoteURL, r.cfg.Branch)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error()})
		return 1
	}
	if !exists {
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("PR head branch %s was not found on the fork head repo %s; refusing to fall back to a same-named base branch", r.cfg.Branch, r.cfg.HeadRepo)})
		return exitNeedsHuman
	}
	return 0
}

// splitOwnerRepo splits an owner/name repository identity on the final "/".
func splitOwnerRepo(value string) (owner, name string, ok bool) {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "/")
	if idx <= 0 || idx == len(value)-1 {
		return "", "", false
	}
	return value[:idx], value[idx+1:], true
}

func installGitIdentity() func() {
	defaults := map[string]string{
		"GIT_AUTHOR_NAME":     "Courier",
		"GIT_AUTHOR_EMAIL":    "courier@localhost",
		"GIT_COMMITTER_NAME":  "Courier",
		"GIT_COMMITTER_EMAIL": "courier@localhost",
	}
	type previousValue struct {
		value string
		set   bool
	}
	previous := make(map[string]previousValue, len(defaults))
	for name, fallback := range defaults {
		value, set := os.LookupEnv(name)
		previous[name] = previousValue{value: value, set: set}
		if !set || strings.TrimSpace(value) == "" {
			_ = os.Setenv(name, fallback)
		}
	}
	return func() {
		for name, value := range previous {
			if !value.set {
				_ = os.Unsetenv(name)
				continue
			}
			_ = os.Setenv(name, value.value)
		}
	}
}

func processExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func installAskpass(executable string) func() {
	type environment struct {
		value string
		set   bool
	}
	previous := map[string]environment{}
	for _, name := range []string{"GIT_ASKPASS", "GIT_TERMINAL_PROMPT", "COURIER_ASKPASS"} {
		value, set := os.LookupEnv(name)
		previous[name] = environment{value: value, set: set}
	}
	_ = os.Setenv("GIT_ASKPASS", executable)
	_ = os.Setenv("GIT_TERMINAL_PROMPT", "0")
	_ = os.Setenv("COURIER_ASKPASS", "1")
	return func() {
		for name, value := range previous {
			if !value.set {
				_ = os.Unsetenv(name)
				continue
			}
			_ = os.Setenv(name, value.value)
		}
	}
}

func askpass(args []string, username, token string, stdout io.Writer) int {
	prompt := strings.ToLower(strings.Join(args, " "))
	value := token
	if strings.Contains(prompt, "username") {
		value = username
	}
	_, _ = io.WriteString(stdout, value)
	return 0
}

func emitTermination(stdout io.Writer, cfg config, result termination) {
	payload, err := json.Marshal(result)
	if err != nil {
		return
	}
	line := "COURIER_TERMINATION " + string(payload) + "\n"
	_, _ = io.WriteString(stdout, line)
	if cfg.TerminationFile == "" {
		return
	}
	// The file is an optional local handoff for an operator or sidecar. It is
	// never used for credentials and is replaced atomically enough for a single
	// writer in the ephemeral workspace.
	_ = os.WriteFile(cfg.TerminationFile, []byte(line), 0o600)
}
