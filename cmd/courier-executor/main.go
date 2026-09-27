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
	"net/url"
	"os"
	"os/exec"
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
)

type config struct {
	RemoteURL       string
	BaseRemoteURL   string
	Directory       string
	Base            string
	Branch          string
	Repo            string
	HeadRepo        string
	HeadSHA         string
	Mode            string
	RunID           string
	Ref             int
	Goal            string
	Model           string
	Framing         string
	GitHubAPIBase   string
	OpenCodeBinary  string
	OpenCodeFormat  string
	OpenCodeAgent   string
	TerminationFile string
	GitUsername     string
	GitToken        string
	GitHubToken     string
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
	cfg := config{
		RemoteURL:       remoteURL,
		BaseRemoteURL:   strings.TrimSpace(getenv("COURIER_BASE_REPO_URL")),
		Directory:       strings.TrimSpace(getenv("COURIER_WORKSPACE")),
		Base:            strings.TrimSpace(getenv("COURIER_BASE")),
		Branch:          strings.TrimSpace(getenv("COURIER_BRANCH")),
		Repo:            strings.TrimSpace(getenv("COURIER_REPO")),
		HeadRepo:        strings.TrimSpace(getenv("COURIER_HEAD_REPO")),
		HeadSHA:         strings.TrimSpace(getenv("COURIER_HEAD_SHA")),
		Goal:            strings.TrimSpace(getenv("COURIER_GOAL")),
		Model:           strings.TrimSpace(getenv("COURIER_MODEL")),
		Framing:         getenv("COURIER_FRAMING"),
		Mode:            strings.TrimSpace(getenv("COURIER_MODE")),
		RunID:           strings.TrimSpace(getenv("COURIER_RUN_NAME")),
		Ref:             ref,
		OpenCodeBinary:  strings.TrimSpace(getenv("COURIER_OPENCODE_BINARY")),
		OpenCodeFormat:  strings.TrimSpace(getenv("COURIER_OPENCODE_FORMAT")),
		OpenCodeAgent:   strings.TrimSpace(getenv("COURIER_OPENCODE_AGENT")),
		TerminationFile: strings.TrimSpace(getenv("COURIER_TERMINATION_FILE")),
		GitUsername:     getenv("COURIER_GIT_USERNAME"),
		GitToken:        getenv("COURIER_GIT_TOKEN"),
		GitHubToken:     getenv("GITHUB_TOKEN"),
		GitHubAPIBase:   strings.TrimSpace(getenv("COURIER_GITHUB_API_BASE")),
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
	command := runtime.Command(executor.Invocation{
		Goal:      cfg.Goal,
		Model:     cfg.Model,
		Framing:   cfg.Framing,
		Workspace: workspace.Directory,
	})
	report.event(courierlog.EventExecutorStart, courierlog.StatusOK, map[string]any{
		"executor": runtime.Name(),
		"format":   cfg.OpenCodeFormat,
	})
	// The child's output is untrusted verbose tool I/O: route both streams
	// through the run's redactor before they reach the real stdout/stderr
	// (DESIGN.md: redact before stdout). The transport is transparent — no
	// semantic parsing, ordering and stream separation preserved. Lines
	// buffered across Write chunks are flushed after the child exits.
	stdoutTransport := courierlog.NewRedactingWriter(stdout, report.red)
	stderrTransport := courierlog.NewRedactingWriter(stderr, report.red)
	process := exec.CommandContext(ctx, command.Binary, command.Args...)
	process.Dir = workspace.Directory
	process.Stdout = stdoutTransport
	process.Stderr = stderrTransport
	err = process.Run()
	// Release whatever the child left as a final partial line before the
	// outcome is reported, so output ordering stays faithful.
	_ = stdoutTransport.Flush()
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
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: code, Reason: fmt.Sprintf("opencode exited with status %d", code)})
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
	switch workState {
	case git.WorkStateCommitted:
		// The run branch's own ref, not where HEAD happens to point, is the
		// world: committed work counts only when it is reachable from the run
		// branch. A branch-read error must not fail the run: liveness over
		// strictness, so an unreadable ref falls through to the success path.
		ahead, branchErr := workspace.CommitsOnBranchSince(ctx, cfg.Branch, startCommit)
		if branchErr == nil && ahead == 0 {
			// Committed work is not on the run branch. Report it specifically so the
			// operator does not later read an empty run branch as "no work".
			whereClause := "a detached HEAD"
			if cb, err := workspace.CurrentBranch(ctx); err == nil && cb != "" {
				whereClause = fmt.Sprintf("branch %q", cb)
			}
			reason := fmt.Sprintf("opencode committed work that is not on the run branch %q (HEAD is on %s); the run branch has no new commits", cfg.Branch, whereClause)
			report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason})
			return exitNeedsHuman
		}
		report.terminate(termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: "opencode completed with committed work"})
		return exitSuccess
	case git.WorkStateDirty:
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: "opencode exited successfully with uncommitted workspace changes"})
		return exitNeedsHuman
	default:
		reason := "opencode exited successfully without producing a commit or workspace changes"
		if names := unavailableCapabilities(caps); len(names) > 0 {
			reason += "; configured capability unavailable: " + strings.Join(names, ", ")
		}
		report.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason})
		return exitNeedsHuman
	}
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
