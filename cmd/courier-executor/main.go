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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/broker/grammar"
	"github.com/misospace/courier/internal/devenv"
	"github.com/misospace/courier/internal/evidence"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/github"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/telemetry"
)

const (
	exitSuccess    = 0
	exitFailed     = 1
	exitNeedsHuman = 2
	// The operator decodes this exit in phaseForExit (internal/controller);
	// DESIGN.md's exit contract is the authority.
	exitNoChangeNeeded        = 3
	defaultBase               = "main"
	defaultWork               = "/workspace"
	defaultScratchDirectory   = "/var/tmp/courier-scratch"
	defaultOutcomeFilename    = "outcome.json"
	maxTerminationReasonBytes = 1024
	// terminationTruncationSuffix / terminationLinePrefix /
	// maxTerminationLineBytes live in internal/executor so the two
	// COURIER_TERMINATION producers (here and courier-control) cannot
	// drift on the wire prefix or the kubelet budget (#210).
	defaultFormat           = "json"
	defaultMaxContinuations = 3
	defaultResumeBackoff    = 5 * time.Second
	// defaultMaxAvailabilityRetries bounds consecutive no-progress provider
	// failures before the run ends recoverably for another capacity (#327).
	// With defaultResumeBackoff doubling up to maxAvailabilityBackoffCap the
	// budget spans roughly fifteen minutes of an outage — long enough to ride
	// out brief blips, short enough that a healthy sibling capacity is not
	// starved of the work.
	defaultMaxAvailabilityRetries = 10
	maxAvailabilityBackoffCap     = 4
	maxResumeBackoffSeconds       = 3600
)

type config struct {
	RemoteURL        string
	BaseRemoteURL    string
	Directory        string
	ScratchDirectory string
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
	// MaxAvailabilityRetries bounds consecutive no-progress provider
	// failures (#327 Scenario D): an outage keeps the run recoverable on a
	// separate, backed-off budget instead of consuming task-repair
	// continuations or terminalizing as Failed.
	MaxAvailabilityRetries int
	TerminationFile        string
	GitUsername            string
	GitToken               string
	GitHubToken            string
	EvidenceURL            string
	EvidenceToken          string
	PodUID                 string
	// ExhaustionCodes/ExhaustionTypes are the deployment-extended provider
	// exhaustion signals (#320 policy): structured tokens added to the
	// provider-neutral defaults, never replacing them (#327).
	ExhaustionCodes string
	ExhaustionTypes string
	// Criteria is the bounded JSON envelope of the acceptance-criterion set
	// pinned at admission (#280). Empty means no set was pinned; presence
	// (even of an empty set) is meaningful and turns the criterion check
	// into a vacuous pass instead of a no-op (#249).
	Criteria string
}

// evidenceEnabled reports whether failure-evidence capture is armed for this
// run: both the intake endpoint and the per-incarnation token must be present
// (already trimmed). Absent configuration is a silent no-op, not an error
// (#198).
func (c config) evidenceEnabled() bool {
	return strings.TrimSpace(c.EvidenceURL) != "" && strings.TrimSpace(c.EvidenceToken) != ""
}

type termination struct {
	Phase    string `json:"phase"`
	Result   string `json:"result"`
	ExitCode int    `json:"exit_code"`
	Reason   string `json:"reason"`
	// Outcome carries the coordinator's declared outcome kind when a valid
	// declaration drove the ending (#169); empty when classified from the
	// world alone.
	Outcome string `json:"outcome,omitempty"`
	// Category is the stable, machine-readable cause classification (#272).
	// It is typed as the operator's TerminalDiagnosticCategory vocabulary, so
	// the executor can only declare a cause the operator persists.
	Category courierv1alpha1.TerminalDiagnosticCategory `json:"category,omitempty"`
	// Summary is the compact per-run telemetry tallied from the OpenCode event
	// stream (#172). Omitted when the run recorded no activity.
	Summary *courierv1alpha1.RunTelemetry `json:"summary,omitempty"`
	// WorkspaceSafety reports post-stop Git observations on capacity endings.
	// Legacy observations are not authority: the controller keeps recovery held
	// until an independent trusted observer can validate them.
	WorkspaceSafety *courierv1alpha1.WorkspaceSafetyEvidence `json:"workspace_safety,omitempty"`
}

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "observe-seed" || os.Args[1] == "observe-workspace") {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		if err := observerMode(ctx, os.Args[1] == "observe-seed"); err != nil {
			fmt.Fprintln(os.Stderr, "courier: workspace observation failed:", err)
			os.Exit(1)
		}
		return
	}
	if os.Getenv("COURIER_ASKPASS") == "1" {
		os.Exit(askpass(os.Args[1:], os.Getenv("COURIER_GIT_USERNAME"), os.Getenv("COURIER_GIT_TOKEN"), os.Stdout))
	}
	// SIGTERM cancels the run context only: a handler that captured would
	// race os.Exit, and the cancellation path funnels the child kill and the
	// capture through the run's ordinary exit codes (DESIGN.md, #115).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	// Armed for the whole run, before any child exists: this process is PID 1
	// of the coordinator pod, so the kernel hands it every orphaned descendant
	// and PID 1 reaps none of them on its own (#347).
	installAdoptedChildReaper()
	os.Exit(run(ctx, os.Stdout, os.Stderr))
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
	availabilityRetries, _ := strconv.Atoi(strings.TrimSpace(getenv("COURIER_MAX_AVAILABILITY_RETRIES")))
	if availabilityRetries < 1 {
		availabilityRetries = defaultMaxAvailabilityRetries
	}
	resumeBackoff, err := strconv.ParseFloat(strings.TrimSpace(getenv("COURIER_RESUME_BACKOFF_SECONDS")), 64)
	if err != nil || math.IsNaN(resumeBackoff) || math.IsInf(resumeBackoff, 0) || resumeBackoff < 0 {
		resumeBackoff = defaultResumeBackoff.Seconds()
	}
	if resumeBackoff > maxResumeBackoffSeconds {
		resumeBackoff = maxResumeBackoffSeconds
	}
	cfg := config{
		RemoteURL:              remoteURL,
		BaseRemoteURL:          strings.TrimSpace(getenv("COURIER_BASE_REPO_URL")),
		Directory:              strings.TrimSpace(getenv("COURIER_WORKSPACE")),
		ScratchDirectory:       strings.TrimSpace(getenv("COURIER_SCRATCH_DIR")),
		Base:                   strings.TrimSpace(getenv("COURIER_BASE")),
		Branch:                 strings.TrimSpace(getenv("COURIER_BRANCH")),
		Repo:                   strings.TrimSpace(getenv("COURIER_REPO")),
		HeadRepo:               strings.TrimSpace(getenv("COURIER_HEAD_REPO")),
		HeadSHA:                strings.TrimSpace(getenv("COURIER_HEAD_SHA")),
		Goal:                   strings.TrimSpace(getenv("COURIER_GOAL")),
		Model:                  strings.TrimSpace(getenv("COURIER_MODEL")),
		Framing:                getenv("COURIER_FRAMING"),
		Mode:                   strings.TrimSpace(getenv("COURIER_MODE")),
		RunID:                  strings.TrimSpace(getenv("COURIER_RUN_NAME")),
		Ref:                    ref,
		OpenCodeBinary:         strings.TrimSpace(getenv("COURIER_OPENCODE_BINARY")),
		OpenCodeFormat:         strings.TrimSpace(getenv("COURIER_OPENCODE_FORMAT")),
		OpenCodeAgent:          strings.TrimSpace(getenv("COURIER_OPENCODE_AGENT")),
		MaxContinuations:       maxContinuations,
		ResumeBackoff:          time.Duration(resumeBackoff * float64(time.Second)),
		MaxAvailabilityRetries: availabilityRetries,
		TerminationFile:        strings.TrimSpace(getenv("COURIER_TERMINATION_FILE")),
		GitUsername:            getenv("COURIER_GIT_USERNAME"),
		GitToken:               getenv("COURIER_GIT_TOKEN"),
		GitHubToken:            getenv("GITHUB_TOKEN"),
		GitHubAPIBase:          strings.TrimSpace(getenv("COURIER_GITHUB_API_BASE")),
		EvidenceURL:            strings.TrimSpace(getenv(executor.EnvEvidenceURL)),
		EvidenceToken:          strings.TrimSpace(getenv(executor.EnvEvidenceToken)),
		PodUID:                 strings.TrimSpace(getenv(executor.EnvPodUID)),
		ExhaustionCodes:        getenv("COURIER_MODEL_EXHAUSTION_CODES"),
		ExhaustionTypes:        getenv("COURIER_MODEL_EXHAUSTION_TYPES"),
		Criteria:               strings.TrimSpace(getenv(executor.EnvCriteria)),
	}
	if strings.TrimSpace(cfg.GitHubToken) == "" {
		cfg.GitHubToken = cfg.GitToken
	}
	if cfg.Directory == "" {
		cfg.Directory = defaultWork
	}
	if cfg.ScratchDirectory == "" {
		cfg.ScratchDirectory = defaultScratchDirectory
	}
	if !filepath.IsAbs(cfg.Directory) {
		absolute, err := filepath.Abs(cfg.Directory)
		if err != nil {
			return config{}, fmt.Errorf("resolve COURIER_WORKSPACE: %w", err)
		}
		cfg.Directory = absolute
	}
	if !filepath.IsAbs(cfg.ScratchDirectory) {
		absolute, err := filepath.Abs(cfg.ScratchDirectory)
		if err != nil {
			return config{}, fmt.Errorf("resolve COURIER_SCRATCH_DIR: %w", err)
		}
		cfg.ScratchDirectory = absolute
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
	if strings.EqualFold(cfg.Mode, "fix-pr") && cfg.Ref < 1 {
		// A fix-pr run is admitted against one pinned pull request, and
		// the pre-handoff observation verifies exactly that pull request
		// (#517). Without a usable pinned reference there is nothing to
		// verify, so the run must not proceed to a Verifying handoff it
		// can never prove: it fails closed here, where the malformed
		// input is still visible, instead of silently skipping the
		// verification it exists to perform.
		return config{}, errors.New("COURIER_REF must be the pinned pull request number for fix-pr runs")
	}
	scratchRelative, err := filepath.Rel(cfg.Directory, cfg.ScratchDirectory)
	if err != nil {
		return config{}, fmt.Errorf("compare scratch and workspace paths: %w", err)
	}
	if scratchRelative == "." || (scratchRelative != ".." && !strings.HasPrefix(scratchRelative, ".."+string(filepath.Separator))) {
		return config{}, errors.New("COURIER_SCRATCH_DIR must be outside COURIER_WORKSPACE")
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
	tel    *telemetry.Tracker
	cfg    config
	// handoffCoverage carries the criterion-evidence block's claimed
	// coverage ("full" or "partial") from the pre-handoff plan to the
	// run.exit event (#249, #282). Set by applyHandoffPlan when a Verifying
	// pass carries criteria data; empty when no set was pinned or when the
	// criteria check did not run. It rides event detail only — never the
	// Verifying Reason line, never the COURIER_TERMINATION payload.
	handoffCoverage string
	// handoffStatusCounts carries the per-status record counts of the
	// criterion-evidence block from the pre-handoff plan to the
	// run.exit event detail (#249, #282). Set by applyHandoffPlan
	// when a Verifying pass carries a parseable criteria report;
	// nil when no set was pinned or when the criteria check did not
	// run. It rides event detail only — never the Verifying Reason
	// line, never the COURIER_TERMINATION payload — alongside
	// handoffCoverage so an operator audits the run's honesty
	// without the legacy handoff falsely claiming fully verified
	// completion outside the PR body.
	handoffStatusCounts map[broker.CriterionStatus]int
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
	return reporter{stdout: stdout, stderr: stderr, events: events, red: red, tel: telemetry.New(), cfg: cfg}
}

// eventVerbose writes one structured run event whose detail is included
// even when the emitter runs at info level. Use it for diagnostic
// payloads that must survive a non-debug run: the linkage read-failure
// reason, for example, so an operator triaging from logs can see why
// the executor skipped the linkage check.
func (r reporter) eventVerbose(eventType, status string, detail map[string]any) {
	err := r.events.Emit(courierlog.Event{
		Type:    eventType,
		RunID:   r.cfg.RunID,
		Repo:    r.cfg.Repo,
		Ref:     r.cfg.Ref,
		Mode:    r.cfg.Mode,
		Model:   r.cfg.Model,
		Status:  status,
		Verbose: true,
		Detail:  detail,
	})
	if err != nil {
		fmt.Fprintf(r.stderr, "courier: dropped %s event: %v\n", eventType, err)
	}
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
// line with a redacted reason, plus the run.exit event. The run.exit detail
// rides the criterion-evidence coverage claim ("full" or "partial") and the
// per-status record counts when the pre-handoff check produced them (#249,
// #282): a partial record is the run ending honestly, an operator can audit
// whether the run stayed consistent with the linked criteria, and the Reason
// line is untouched.
func (r reporter) terminate(result termination) {
	if r.tel != nil {
		if compact := r.tel.Compact(); compact != nil {
			result.Summary = compact
			r.emitRunSummary(r.tel.Summary())
		}
	}
	result.Reason = truncateTerminationReason(r.red.Redact(result.Reason))
	emitTermination(r.stdout, r.cfg, result)
	status := courierlog.StatusOK
	switch result.Phase {
	case "Failed":
		status = courierlog.StatusError
	case "NeedsHuman":
		status = courierlog.StatusNeedsHuman
	}
	detail := map[string]any{
		"exit_code": result.ExitCode,
		"phase":     result.Phase,
		"reason":    result.Reason,
		"category":  result.Category,
	}
	if r.handoffCoverage != "" {
		detail["criteria.coverage"] = r.handoffCoverage
	}
	if len(r.handoffStatusCounts) > 0 {
		counts := make(map[string]int, len(r.handoffStatusCounts))
		for statusName, count := range r.handoffStatusCounts {
			counts[string(statusName)] = count
		}
		detail["criteria.status_counts"] = counts
	}
	r.event(courierlog.EventRunExit, status, detail)
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
	// In its own process group so the descendants it leaves behind stay
	// recognizable as its own after it exits, and the reaper can collect them
	// instead of retaining them on PID 1 (#347).
	probe.SysProcAttr = processGroupSysProcAttr()
	// Bound waiting for a grandchild that holds the output pipe after the
	// context is done.
	probe.WaitDelay = 5 * time.Second
	if strings.TrimSpace(dir) != "" {
		probe.Dir = dir
	}
	var out bytes.Buffer
	probe.Stdout = &out
	probe.Stderr = io.Discard
	_ = childReaper().run(probe)
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

// emitRunSummary reports the run's tallied telemetry as a structured event.
// The summary detail is always included (Verbose) so it reaches the log store
// on every finished run, not only on debug runs; the emitter redacts it.
func (r reporter) emitRunSummary(s telemetry.Summary) {
	err := r.events.Emit(courierlog.Event{
		Type:    courierlog.EventRunSummary,
		RunID:   r.cfg.RunID,
		Repo:    r.cfg.Repo,
		Ref:     r.cfg.Ref,
		Mode:    r.cfg.Mode,
		Model:   r.cfg.Model,
		Status:  courierlog.StatusOK,
		Verbose: true,
		Detail:  map[string]any{"summary": s},
	})
	if err != nil {
		fmt.Fprintf(r.stderr, "courier: dropped %s event: %v\n", courierlog.EventRunSummary, err)
	}
}

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
	restoreIdentity := installGitIdentity()
	defer restoreIdentity()
	restoreEnv := installAskpass(askpassPath)
	defer restoreEnv()
	if err := git.TrustDirectory(ctx, cfg.Directory); err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "trust workspace: " + err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}

	if code, stop := report.guardAdoption(ctx); stop {
		return code
	}

	if code := report.guardFixPRHead(ctx); code != 0 {
		return code
	}

	if code := report.guardFixPRBase(ctx); code != 0 {
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
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}
	startCommit, err := workspace.Head(ctx)
	if err != nil {
		report.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "record workspace start: " + err.Error(), Category: terminalCategoryExecutorCrash})
		return 1
	}

	outcomePath := filepath.Join(cfg.ScratchDirectory, defaultOutcomeFilename)
	// Both terminal sites sit after the workspace exists and its start commit
	// is recorded, so they take the gate's look: an adopted workspace can be
	// mid-merge (dirty) before the first turn.
	if err := os.MkdirAll(cfg.ScratchDirectory, 0o700); err != nil {
		report.terminateCaptured(ctx, workspace, startCommit, failed("prepare run scratch directory: "+err.Error()))
		return exitFailed
	}
	if err := os.Remove(outcomePath); err != nil && !os.IsNotExist(err) {
		report.terminateCaptured(ctx, workspace, startCommit, failed("clear stale outcome declaration: "+err.Error()))
		return exitFailed
	}
	cfg.Goal = strings.ReplaceAll(cfg.Goal, filepath.Join(defaultScratchDirectory, defaultOutcomeFilename), outcomePath)
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

	// Child output is redacted before it reaches the run streams. The session
	// tap observes OpenCode's JSON events without changing their bytes. The
	// wrapping happens once, here: the model child streams through the same
	// writers.
	stdoutRedacted := courierlog.NewRedactingWriter(stdout, report.red)
	stderrTransport := courierlog.NewRedactingWriter(stderr, report.red)

	// The repository-owned development bootstrap contract (#414): the legacy
	// executor is credential-bearing, so it NEVER executes repository
	// contract content — it classifies the contract with pure git reads and
	// emits one receipt event per verdict. Execution happens only in the
	// secure worker's credential-free context; an approved contract here is
	// skipped with a receipt that names exactly that.
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
	var exhaustionSignal atomic.Bool

	// Recoverable workspace endings and crashes share one continuation budget.
	continuations, crashes, availability := 0, 0, 0
	var (
		stateMessage    string
		prevFingerprint string
		history         []string
	)

	for {
		report.tel.SetContinuations(continuations)
		if continuations > 0 {
			// A declaration belongs to one completed turn; never let a stale
			// declaration from an earlier turn decide the resumed run. A
			// previous turn may have left the workspace dirty, so this
			// post-workspace terminal site takes the gate's look.
			if err := os.Remove(outcomePath); err != nil && !os.IsNotExist(err) {
				report.terminateCaptured(ctx, workspace, startCommit, failed("clear stale outcome declaration: "+err.Error()))
				return exitFailed
			}
		}

		invocation := executor.Invocation{
			Goal:      cfg.Goal,
			Model:     cfg.Model,
			Framing:   cfg.Framing,
			Workspace: workspace.Directory,
		}
		if stateMessage != "" {
			invocation = executor.Invocation{
				Goal:      stateMessage,
				Model:     cfg.Model,
				Workspace: workspace.Directory,
				Session:   tap.session(),
			}
		}
		command := runtime.Command(invocation)
		modelStdout := tap.beginAttempt()
		report.event(courierlog.EventExecutorStart, courierlog.StatusOK, map[string]any{
			"executor": runtime.Name(),
			"format":   cfg.OpenCodeFormat,
			"resumed":  stateMessage != "",
		})
		process := exec.CommandContext(ctx, command.Binary, command.Args...)
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
		tap.setKill(func() { exhaustionSignal.Store(true); _ = process.Cancel() })
		// Bound the wait for the group after the context is done; the margin
		// keeps the capture inside the 45-second termination grace.
		process.WaitDelay = 10 * time.Second
		process.Dir = workspace.Directory
		process.Stdout = modelStdout
		process.Stderr = stderrTransport
		processErr := childReaper().run(process)
		_ = tap.Flush()
		tap.setKill(nil)
		_ = stdoutRedacted.Flush()
		_ = stderrTransport.Flush()

		if ctx.Err() == nil && exhaustionSignal.Load() {
			// The callback fires on the structured error event itself, causing
			// the command context to kill the whole process group. Ignore any
			// later output from that killed turn; it cannot trigger a continuation.
			class, code, exhausted := tap.confirmedExhaustion()
			if !exhausted {
				class, code, exhausted = tap.lastExhaustion()
			}
			if exhausted {
				result := report.capacityExhaustedTermination(ctx, workspace, cfg, startCommit, class, code)
				report.terminateCaptured(ctx, workspace, startCommit, result)
				return exitNeedsHuman
			}
		}

		if processErr != nil {
			code := processExitCode(processErr)
			if code < 0 {
				code = exitFailed
			}
			// Every raw nonzero child status — including 2, which is only
			// Courier's own exit constant and never a declaration by the
			// child — takes the shared recovery path below. NeedsHuman is
			// reserved for verified outcome handoffs; a provider outage or
			// crash gets its bounded budget and, when spent, the recoverable
			// availability handoff instead of a stranding Failed.
			// Availability failures (#327 Scenario D): a provider outage makes
			// OpenCode exit without ever serving a session id for this
			// attempt. Those failures get their own bounded, backed-off
			// budget: they never consume the task-repair continuation budget,
			// and exhausting them ends the run on the recoverable
			// capacity-handoff channel — as an availability handoff, never
			// pretending a structured quota signal fired — so another
			// capacity can take the work instead of it stranding as Failed.
			// Any attempt that got a session proves the provider served us
			// and resets the budget.
			if !tap.sawSessionThisAttempt() {
				if ctx.Err() == nil && availability < cfg.MaxAvailabilityRetries {
					availability++
					report.event(courierlog.EventExecutorContinuation, courierlog.StatusOK, map[string]any{
						"kind":    "availability-retry",
						"code":    code,
						"attempt": availability,
					})
					// Same gate as the crash continuation: a dying tool may
					// have left uncommitted work from an earlier session-
					// serving attempt; a retry is exactly the moment the
					// next turn could clobber it.
					report.maybeCaptureEvidence(ctx, workspace, startCommit, evidence.TriggerCrash)
					delay := cfg.ResumeBackoff * time.Duration(1<<min(availability-1, maxAvailabilityBackoffCap))
					select {
					case <-time.After(delay):
					case <-ctx.Done():
					}
					if stateMessage == "" && tap.session() != "" {
						stateMessage = fmt.Sprintf("Your previous turn ended unexpectedly (opencode exited with status %d). Continue working toward the goal.", code)
					}
					continue
				}
				if ctx.Err() == nil {
					report.terminateCaptured(ctx, workspace, startCommit, report.availabilityRecoveryTermination(ctx, workspace, cfg, startCommit, terminalCategoryProviderUnavailable, fmt.Sprintf("model provider failed to serve %d consecutive attempts without a session (last exit %d)", cfg.MaxAvailabilityRetries, code)))
					return exitNeedsHuman
				}
			} else {
				availability = 0
			}
			if tap.session() != "" && ctx.Err() == nil && continuations < cfg.MaxContinuations {
				continuations++
				crashes++
				report.event(courierlog.EventExecutorContinuation, courierlog.StatusOK, map[string]any{
					"kind":         "crash",
					"code":         code,
					"continuation": continuations,
				})
				// A run that crashes dirty would be left uncovered; the gate
				// decides whether this moment is worth preserving before the
				// next turn may clobber it.
				report.maybeCaptureEvidence(ctx, workspace, startCommit, evidence.TriggerCrash)
				delay := cfg.ResumeBackoff * time.Duration(1<<min(crashes-1, 10))
				select {
				case <-time.After(delay):
				case <-ctx.Done():
				}
				if ctx.Err() != nil {
					report.terminateCaptured(ctx, workspace, startCommit, failedWith(fmt.Sprintf("opencode exited with status %d and the context was canceled", code), terminalCategoryModelTransportError))
					return exitFailed
				}
				stateMessage = fmt.Sprintf("Your previous turn ended unexpectedly (opencode exited with status %d). Continue working toward the goal.", code)
				continue
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
			if continuations > 0 {
				reason += fmt.Sprintf(" after %d continuations", continuations)
			}
			if ctx.Err() != nil {
				reason += " and the context was canceled"
				report.terminateCaptured(ctx, workspace, startCommit, failedWith(reason, terminalCategoryModelTransportError))
				return exitFailed
			}
			report.terminateCaptured(ctx, workspace, startCommit, report.availabilityRecoveryTermination(ctx, workspace, cfg, startCommit, terminalCategoryModelTransportError, reason))
			return exitNeedsHuman
		}

		if workspace.Conflict != nil {
			if reason := unresolvedBaseSync(ctx, workspace); reason != "" {
				report.terminateCaptured(ctx, workspace, startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason, Category: terminalCategoryUnknown})
				return exitNeedsHuman
			}
		}

		decl, declared, declErr := readOutcome(outcomePath)
		if err := os.Remove(outcomePath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(report.stderr, "courier: could not remove outcome declaration: %s\n", report.red.Redact(err.Error()))
		}
		workState, err := workspace.WorkState(ctx, startCommit)
		if err != nil {
			report.terminateCaptured(ctx, workspace, startCommit, failed("inspect workspace result: "+err.Error()))
			return exitFailed
		}
		if declErr != nil || declared {
			result := report.classify(ctx, workspace, cfg.Branch, startCommit, workState, decl, declared, declErr, caps)
			// Authoritative source-issue linkage is a run-ending contract
			// (#248) regardless of how the run was declared: a Verifying
			// candidate must satisfy the same shared repair/budget
			// mechanism the undeclared-Verifying path uses. The declared
			// path therefore participates in the same continuation/repair
			// loop as the undeclared path — without losing the declared
			// outcome tag, which applyHandoffPlan preserves on the
			// verifier-rejection terminal (a transient forge read instead
			// ends blocked_external, #455). This keeps the legacy executor from
			// forcing NeedsHuman on a declared run whose captured
			// session still has unused continuation budget.
			if result.Phase == "Verifying" {
				head, headErr := workspace.Head(ctx)
				_ = headErr
				verdict := report.applyHandoffPlan(ctx, continuations, cfg.MaxContinuations, tap.session(), ctx.Err())
				if verdict.ContinueMsg != "" {
					continuations++
					stateMessage = verdict.ContinueMsg
					detail := map[string]any{
						"kind":         verdict.Kind,
						"continuation": continuations,
						"instruction":  stateMessage,
					}
					if verdict.PRNumber > 0 {
						detail["pr"] = verdict.PRNumber
					}
					report.eventVerbose(courierlog.EventExecutorContinuation, courierlog.StatusOK, detail)
					history = append(history, stateSummary(verdict.Kind, head, tap.text(), nil))
					continue
				}
				if verdict.FailReason != "" {
					if verdict.ForgeUnavailable {
						report.terminateCaptured(ctx, workspace, startCommit, report.forgeUnavailableTermination(workspace, cfg, startCommit, verdict.FailReason))
						return exitNeedsHuman
					}
					report.terminateCaptured(ctx, workspace, startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: verdict.FailReason, Outcome: result.Outcome, Category: terminalCategoryVerifierRejection})
					return exitNeedsHuman
				}
				if verdict.Pass {
					if verdict.Coverage != "" {
						report.handoffCoverage = verdict.Coverage
						// #282 step 4: a passing pinned handoff surfaces
						// the derived coverage and per-status counts on
						// the run's source issue as well as in the
						// run.exit telemetry. Best-effort like every
						// outcome comment: a forge failure never blocks
						// the handoff.
						report.postOutcomeComment(ctx, handoffCoverageComment(report.cfg.RunID, verdict.Coverage, verdict.StatusCounts))
					}
					if len(verdict.StatusCounts) > 0 {
						report.handoffStatusCounts = verdict.StatusCounts
					}
				}
			}
			report.terminateCaptured(ctx, workspace, startCommit, result)
			return result.ExitCode
		}

		// With no explicit declaration, the verified git state decides whether
		// the turn succeeded or needs another opportunity in the same session.
		head, headErr := workspace.Head(ctx)
		branch, branchErr := workspace.CurrentBranch(ctx)
		dirty, dirtyErr := workspace.StatusPorcelain(ctx)
		slices.Sort(dirty)
		worldReadFailed := headErr != nil || branchErr != nil || dirtyErr != nil

		var kind, message, summary string
		switch workState {
		case git.WorkStateCommitted:
			ahead, branchReadErr := workspace.CommitsOnBranchSince(ctx, cfg.Branch, startCommit)
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
				verdict := report.applyHandoffPlan(ctx, continuations, cfg.MaxContinuations, tap.session(), ctx.Err())
				if verdict.ContinueMsg != "" {
					continuations++
					stateMessage = verdict.ContinueMsg
					detail := map[string]any{
						"kind":         verdict.Kind,
						"continuation": continuations,
						"instruction":  stateMessage,
					}
					if verdict.PRNumber > 0 {
						detail["pr"] = verdict.PRNumber
					}
					report.eventVerbose(courierlog.EventExecutorContinuation, courierlog.StatusOK, detail)
					history = append(history, stateSummary(verdict.Kind, head, tap.text(), nil))
					continue
				}
				if verdict.FailReason != "" {
					if verdict.ForgeUnavailable {
						report.terminateCaptured(ctx, workspace, startCommit, report.forgeUnavailableTermination(workspace, cfg, startCommit, verdict.FailReason))
						return exitNeedsHuman
					}
					report.terminateCaptured(ctx, workspace, startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: verdict.FailReason, Category: terminalCategoryVerifierRejection})
					return exitNeedsHuman
				}
				if verdict.Pass {
					if verdict.Coverage != "" {
						report.handoffCoverage = verdict.Coverage
						// #282 step 4, as on the declared path: the
						// derived coverage and per-status counts reach
						// the run's source issue as a best-effort
						// comment before the Verifying terminal.
						report.postOutcomeComment(ctx, handoffCoverageComment(report.cfg.RunID, verdict.Coverage, verdict.StatusCounts))
					}
					if len(verdict.StatusCounts) > 0 {
						report.handoffStatusCounts = verdict.StatusCounts
					}
				}
				report.terminateCaptured(ctx, workspace, startCommit, termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: "opencode completed with committed work"})
				return exitSuccess
			}
			whereClause := "a detached HEAD"
			if branchErr != nil {
				whereClause = "an unknown branch"
			} else if branch != "" {
				whereClause = fmt.Sprintf("branch %q", branch)
			}
			kind = "off-branch"
			message = fmt.Sprintf("You committed work that is not on the run branch %q (HEAD is on %s). Move your commits onto %q and push, or declare needs_decision or blocked_external if a human is needed.", cfg.Branch, whereClause, cfg.Branch)
			summary = stateSummary(kind, head, tap.text(), nil)
		case git.WorkStateDirty:
			listed := dirty
			more := ""
			if len(listed) > maxContinuationPaths {
				more = fmt.Sprintf(" (and %d more)", len(listed)-maxContinuationPaths)
				listed = listed[:maxContinuationPaths]
			}
			kind = "uncommitted"
			if dirtyErr != nil {
				message = fmt.Sprintf("You ended with uncommitted changes; commit and push them to %q, or discard them and declare needs_decision or blocked_external if a human is needed.", cfg.Branch)
			} else {
				message = fmt.Sprintf("You ended with uncommitted changes in %s%s; commit and push them to %q, or discard them and declare needs_decision or blocked_external if a human is needed.", strings.Join(listed, ", "), more, cfg.Branch)
			}
			summary = stateSummary(kind, head, tap.text(), listed)
		default:
			kind = "no-work"
			message = fmt.Sprintf("You ended without producing a commit or workspace changes and did not declare an outcome. Continue the work and push it to %q, or declare needs_decision or blocked_external if a human is needed.", cfg.Branch)
			summary = stateSummary(kind, head, tap.text(), nil)
			if names := unavailableCapabilities(caps); len(names) > 0 {
				suffix := "; configured capability unavailable: " + strings.Join(names, ", ")
				message += suffix
				summary += suffix
			}
		}

		if tap.session() == "" {
			// Without a captured session there is nothing to resume; apply the
			// ordinary undeclared-outcome contract to the verified world
			// state. The committed-work Verifying ending was already settled
			// above through the shared pre-handoff plan (#247, #248), which
			// fails closed without a session, so classifyUndeclared here can
			// only return non-Verifying endings.
			result := report.classifyUndeclared(ctx, workspace, cfg.Branch, startCommit, workState, caps)
			report.terminateCaptured(ctx, workspace, startCommit, result)
			return result.ExitCode
		}

		fingerprint, err := continuationFingerprint(workState, branch, head, dirty, tap.text())
		if err != nil {
			report.terminateCaptured(ctx, workspace, startCommit, failed("fingerprint workspace state: "+err.Error()))
			return exitFailed
		}
		if !worldReadFailed && prevFingerprint != "" && fingerprint == prevFingerprint {
			history = append(history, summary)
			report.terminateCaptured(ctx, workspace, startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: "looping: " + summarizeHistory(history), Category: terminalCategoryContinuationExhaustion})
			return exitNeedsHuman
		}
		if continuations >= cfg.MaxContinuations {
			history = append(history, summary)
			report.terminateCaptured(ctx, workspace, startCommit, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("looping after %d continuations: %s", continuations, summarizeHistory(history)), Category: terminalCategoryContinuationExhaustion})
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
			prevFingerprint = ""
		} else {
			prevFingerprint = fingerprint
		}
		stateMessage = message
		// The next turn may clobber this state; the gate takes its own look
		// and decides whether it is worth preserving.
		report.maybeCaptureEvidence(ctx, workspace, startCommit, evidence.TriggerContinuation)
	}

}

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
// `Verifying` observation settles what this early handoff leaves open. It
// returns (0, false) when the run may proceed to Prepare, or a terminal
// (code, true) once it has emitted the run's handoff.
func (r reporter) guardAdoption(ctx context.Context) (int, bool) {
	if r.cfg.Mode != "resolve-issue" {
		return 0, false
	}
	exists, err := git.RemoteBranchExists(ctx, r.cfg.RemoteURL, r.cfg.Branch)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1, true
	}
	if !exists {
		return 0, false
	}
	observation, err := r.observeRunBranchPullRequest(ctx)
	if err != nil {
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
	if !strings.EqualFold(r.cfg.Mode, "fix-pr") {
		return 0
	}
	if r.cfg.Ref < 1 {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "COURIER_REF is not set; a fix-pr run without a pull request number cannot be validated", Category: terminalCategoryExecutorCrash})
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

// splitOwnerRepo splits an owner/name repository identity on the final "/".
func splitOwnerRepo(value string) (owner, name string, ok bool) {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "/")
	if idx <= 0 || idx == len(value)-1 {
		return "", "", false
	}
	return value[:idx], value[idx+1:], true
}

// prLinkageObservation is one PR observed for the run branch during
// the legacy executor's authoritative-linkage check. It carries the
// fields the legacy path needs without exposing the full GitHub
// PullRequest shape to the rest of the executor.
type prLinkageObservation struct {
	// Number is the PR's number on the base repository.
	Number int
	// State is the PR's state, lowercased.
	State string
	// Body is the raw PR body, exactly as the forge returned it.
	Body string
	// Draft reports whether the forge marked the PR a draft.
	Draft bool
}

// runBranchPRKind classifies the executor's identity-checked observation of
// the pull requests whose head carries the run branch, taken while the
// OpenCode session is still alive (#247).
type runBranchPRKind int

const (
	// prStateOpen: exactly one open, non-draft PR carries the verified
	// head; its body still faces the authoritative-linkage check (#248).
	prStateOpen runBranchPRKind = iota
	// prStateDraft: exactly one open PR carries the verified head and
	// the forge marks it a draft; the run may not hand off until it is
	// ready for review.
	prStateDraft
	// prStateMissing: no pull request of any state carries the verified
	// head; the caller confirms remote publication before treating it
	// as a confirmed missing PR (#247).
	prStateMissing
	// prStateAmbiguous: more than one open pull request claims the
	// verified head; the executor refuses to adopt one (#247 fail-closed).
	prStateAmbiguous
	// prStateClosedOnly: the head's only pull requests are closed or
	// merged; the executor refuses adoption, reopening, or a duplicate
	// creation (#247 fail-closed, matching the adoption guard).
	prStateClosedOnly
	// prStateForeignHead: the forge's owner:branch head selector surfaced
	// a pull request whose repository identity is not the run's own head;
	// the executor cannot reconcile that world, so it fails closed — no
	// adoption, no creation prompt, and never a "missing" claim built by
	// silently deleting the foreign record (#247 fail-closed).
	prStateForeignHead
	// prStateWrongBase: an open pull request carries the verified head
	// but targets a base other than the run's pinned base; adopting it
	// would hand off against the wrong contract, re-targeting it is
	// forbidden, and creating another PR would paper over the world the
	// forge reported (#247 fail-closed).
	prStateWrongBase
)

// runBranchPRObservation is the classified result of the executor's
// identity-checked PR observation. open and draft carry the single PR in
// that state; closed carries the closed or merged PRs when the kind is
// prStateClosedOnly, so terminal reasons can name the actual PR; the other
// fail-closed kinds carry only the kind.
type runBranchPRObservation struct {
	kind   runBranchPRKind
	open   *prLinkageObservation
	draft  *prLinkageObservation
	closed []*prLinkageObservation
}

// handoffCheckOutcome is the result of the shared pre-handoff check that
// the legacy executor runs against the live PRs for the run branch. It
// carries the identity-checked forge observation (the #247 fields) plus
// the body-contract checks that share the plan — authoritative linkage
// (#248) and, when a criterion set is pinned (#280), the criterion-
// evidence contract (#249). It is consumed by the run loop to decide
// between Verifying, a finite continuation to create or ready the PR or
// repair its body, and a fail-closed NeedsHuman. Every field here is
// fail-closed at the boundary: the model can neither skip the check nor
// argue against its verdict.
type handoffCheckOutcome struct {
	// pr records the live PR observed for the run branch. nil
	// when no PR was found.
	pr *prLinkageObservation
	// err is the structured rejection from the linkage validator,
	// or nil when the body satisfies the contract.
	err *broker.LinkageError
	// missing reports that the confirmed-published branch has no pull
	// request at all: a bounded continuation asks the session to
	// open one (#247).
	missing bool
	// draft reports that the branch's only open PR is a draft: a
	// bounded continuation asks the session to ready it (#247).
	draft bool
	// unpublished reports that the remote does not carry the run's
	// current commit — the ref is absent or holds a different commit —
	// so the handoff is fail-closed and no create prompt or Verifying
	// handoff may cover work that never landed (#247). publication
	// carries the observed local/remote pair for the terminal reason.
	unpublished bool
	// publication is the git-side evidence behind unpublished.
	publication remotePublicationState
	// ambiguous reports more than one open PR on the verified head:
	// fail-closed, never a silent adoption (#247).
	ambiguous bool
	// closedOnly reports a head whose only PRs are closed or merged:
	// fail-closed, never a duplicate creation (#247).
	closedOnly bool
	// foreignHead reports a forge record that claims the head selector
	// from another repository: fail-closed, never deleted toward a
	// missing claim (#247).
	foreignHead bool
	// wrongBase reports an open PR on the verified head that targets a
	// base other than the run's pinned base: fail-closed, never adopted or
	// re-targeted (#247).
	wrongBase bool
	// pinnedDraft reports a fix-pr run's pinned pull request still a
	// draft at handoff (#517): the resolve-issue ready-continuation is
	// deliberately unavailable here, because a fix-pr run is admitted
	// against a live PR, so a draft at handoff is a settled-by-human
	// anomaly, and the draft continuation's message is source-issue-
	// shaped. pr carries the draft observation for the verdict.
	pinnedDraft bool
	// pinnedMissing reports a fix-pr run whose pinned head carries no
	// pull request at handoff (#517): the resolve-issue creation
	// continuation is deliberately unavailable here, because the
	// executor never opens a replacement for a pinned PR — that would
	// duplicate or silently re-target the assignment.
	pinnedMissing bool
	// pinnedMismatch reports a fix-pr run whose observed pull request is
	// not the pinned assignment (#517): the forge reports another pull
	// request on the run's head, so the pinned pull request the run was
	// admitted against is unverified. The executor verifies the pinned
	// assignment only and never adopts another pull request. pr carries
	// the observed pull request so the terminal reason can name both the
	// observed and the pinned number.
	pinnedMismatch bool
	// criteria is the criterion-evidence report when the criteria check
	// ran against the open PR's body and the pinned set was non-empty.
	// nil when no set was pinned, when the set was empty (vacuous pass),
	// or when the observation is anything other than prStateOpen — the
	// body contract only applies where there is a body to check. When
	// the body fails the contract, criteria still carries the parsed
	// report (rows, derived coverage) so the repair path can quote the
	// pinned digest and surface per-status counts to the operator
	// audit.
	criteria *broker.CriteriaReport
	// criteriaErr is the structured rejection from the criterion validator
	// when the body failed the contract; nil otherwise.
	criteriaErr *broker.CriteriaError
	// coverage mirrors the criteria report's claim ("full" or "partial")
	// when the check ran; empty when the check did not.
	coverage string
	// pinnedSet is the trusted criterion set the criteria check ran
	// against. It is carried on the outcome (not just inside
	// checkCriterionEvidence) so the repair path can quote its digest,
	// IDs, and texts verbatim — the model cannot re-derive them and
	// the operator can audit the contract that was pinned. nil when no
	// set was pinned or when the set was empty (vacuous pass).
	pinnedSet *broker.CriterionSet
	// missingEvidencePaths names the file paths cited by the body's
	// implementation-evidence and regression-check rows that are absent
	// from the published head the observation verified (#367). It rides
	// the same bounded body-repair instruction the validator's own
	// CriteriaError rides (#249, #282) — the shared #170 budget covers
	// them together, so a citation of a file the published head does not
	// carry costs one repair turn, never a second one. It is the one
	// finding on this outcome that is *not* fail-closed: the check is
	// explicitly non-semantic (existence, not meaning — auditing whether
	// a cited check actually enforces its criterion stays the reviewer's
	// job), every probe that cannot be completed is treated as absence
	// of a finding, and no probe failure is ever returned as an error,
	// so the model is never handed a citation defect it cannot repair
	// by editing the body. Empty whenever the criteria check did not
	// run, when it failed (the validator's defect already names
	// everything the repair must fix), or when every cited path exists.
	missingEvidencePaths []string
	// probedHead is the exact published commit the missing-path probe read
	// from the run's own workspace (#367), kept so the repair instruction
	// and the terminal NeedsHuman reason name the revision whose tree was
	// checked rather than leaving the operator to guess which head was
	// verified. It travels with its finding and only with it: empty
	// whenever missingEvidencePaths is empty, including a probe that ran
	// and found nothing, because a passing verdict drops this outcome and
	// nothing reads the field in that case.
	probedHead string
}

// remotePublicationState is the git-side proof that the run's current commit
// is published to the remote branch (#247). A ref merely existing is not
// publication: only the remote advertising the run's own head commit proves
// the work landed, and a remote holding a different commit means local
// commits never pushed.
type remotePublicationState struct {
	published  bool
	localHead  string
	remoteHead string // empty when the branch ref is absent entirely
}

// trustedSourceIssue derives the source-issue identity the run was
// admitted for. For resolve-issue runs, it is the run's spec
// repository and ref; for fix-pr runs, it is the zero value (the
// pinned PR's existing body is the contract, and the broker's
// UpdateFixPR path enforces any change). Empty inputs fail closed.
func trustedSourceIssue(cfg config) (broker.SourceIssue, bool) {
	if !strings.EqualFold(cfg.Mode, "resolve-issue") {
		return broker.SourceIssue{}, false
	}
	owner, name, ok := splitOwnerRepo(cfg.Repo)
	if !ok || cfg.Ref <= 0 {
		return broker.SourceIssue{}, false
	}
	return broker.SourceIssue{Owner: owner, Name: name, Number: cfg.Ref}, true
}

// runHeadRepo returns the repository identity whose owner:branch head
// selector the forge listing queries: the run's own repository for
// resolve-issue runs, and the pinned fork head repository for fix-pr
// runs (guardFixPRHead's cross-fork invariant). Empty HEAD_REPO inputs
// fall back to the run repository (#517).
func runHeadRepo(cfg config) string {
	if strings.TrimSpace(cfg.HeadRepo) != "" {
		return cfg.HeadRepo
	}
	return cfg.Repo
}

// isFixPRRun reports whether the run is a genuine fix-pr run admitted
// against a pinned pull request: a fix-pr mode with a positive pinned
// reference (#517). readConfig rejects a fix-pr run that cannot name
// its pinned pull request, so the pinned reference is an admission
// invariant, not a runtime hope — a mode without one is not a fix-pr
// run this executor may verify, because there is no pinned pull
// request to re-verify.
func isFixPRRun(cfg config) bool {
	return strings.EqualFold(cfg.Mode, "fix-pr") && cfg.Ref >= 1
}

// observeRunBranchPullRequest reads every pull request whose head carries
// the run's branch through the configured forge capability
// (forge.PullRequestHeadLister, never a forge-specific CLI) and classifies
// the set (#247). The queried head identity is runHeadRepo — the run's own
// repository, or the pinned fork head repository for fix-pr runs — so the
// observation settles the fix-pr head the same identity-checked way as the
// resolve-issue head rather than skipping it (#517). The provider
// compares the full base- and head-repository
// and ref identities carried in the forge response and refuses a response
// missing either identity, so a foreign or fork head can never be mistaken
// for the run's own PR. A record that claims the owner:branch head selector
// from a different repository is kept visible and fails the observation
// closed: the executor may neither adopt it nor delete it toward a "missing"
// claim, because the listing then settles nothing about the run's own head
// (#247). An open candidate is adopted only when its actual base ref is the
// run's pinned base; an open PR on the verified head that targets another
// base fails closed rather than being adopted or duplicated (#247). A read
// failure is returned as an error: an unverifiable observation is not a
// confirmed missing PR, and the caller must fail closed or retry inside the
// bounded budget rather than claim absence (#247).
func (r reporter) observeRunBranchPullRequest(ctx context.Context) (runBranchPRObservation, error) {
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		return runBranchPRObservation{}, err
	}
	provider := github.NewProvider(forge.ProviderConfig{Name: "github", Endpoint: r.cfg.GitHubAPIBase}, client)
	pulls, foreign, err := provider.ListPullRequestsWithForeign(ctx, forge.PullRequestRef{Repo: r.cfg.Repo}, runHeadRepo(r.cfg), r.cfg.Branch)
	if err != nil {
		return runBranchPRObservation{}, err
	}
	if len(foreign) > 0 {
		return runBranchPRObservation{kind: prStateForeignHead}, nil
	}
	var opens, drafts, wrongBases, closed []*prLinkageObservation
	for _, pull := range pulls {
		observed := &prLinkageObservation{Number: pull.Number, State: strings.ToLower(pull.State), Body: pull.Body, Draft: pull.Draft}
		switch {
		case strings.EqualFold(pull.State, "open") && pull.BaseRef != r.cfg.Base:
			wrongBases = append(wrongBases, observed)
		case strings.EqualFold(pull.State, "open") && !pull.Draft:
			opens = append(opens, observed)
		case strings.EqualFold(pull.State, "open"):
			drafts = append(drafts, observed)
		default:
			closed = append(closed, observed)
		}
	}
	switch {
	case len(wrongBases) > 0:
		return runBranchPRObservation{kind: prStateWrongBase}, nil
	case len(opens)+len(drafts) > 1:
		return runBranchPRObservation{kind: prStateAmbiguous}, nil
	case len(opens) == 1:
		return runBranchPRObservation{kind: prStateOpen, open: opens[0]}, nil
	case len(drafts) == 1:
		return runBranchPRObservation{kind: prStateDraft, draft: drafts[0]}, nil
	case len(closed) > 0:
		return runBranchPRObservation{kind: prStateClosedOnly, closed: closed}, nil
	default:
		return runBranchPRObservation{kind: prStateMissing}, nil
	}
}

// verifyNoChangeClaim checks a declared no_change_needed ending against the
// assigned repair condition using current authoritative forge state before
// the executor publishes the success termination (#513). For a fix-pr run
// the pinned pull request is the repair condition: a request the forge
// reports merged or closed makes the claim moot, and an open request must be
// mergeable into its actual base — a forge-reported conflict is unsatisfied,
// and an unknown answer is not proof, because review approval and an
// unchanged head prove nothing about mergeability. The read goes through the
// configured forge read-pull-request capability, never a forge-specific CLI.
// The method returns "" when the claim may stand and otherwise a rejection
// reason for a failed ending; a claim the forge cannot verify fails closed
// and never publishes the success, and the rejection reason stands on the
// forge state, never on the coordinator's model-authored evidence.
func (r reporter) verifyNoChangeClaim(ctx context.Context) string {
	// A resolve-issue run pins no pull request: its repair condition is the
	// issue itself, and the controller settles that ending against the world
	// through AwaitingReview's human gate, so there is nothing to verify
	// here without inferring a PR from the issue number.
	if r.cfg.Mode != "fix-pr" || r.cfg.Ref < 1 {
		return ""
	}
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		return fmt.Sprintf("could not verify the no-change claim against pull request %s#%d: %v", r.cfg.Repo, r.cfg.Ref, err)
	}
	provider := github.NewProvider(forge.ProviderConfig{Name: "github", Endpoint: r.cfg.GitHubAPIBase}, client)
	if !provider.Capabilities().Available(forge.CapabilityReadPullRequest).Available {
		return fmt.Sprintf("could not verify the no-change claim against pull request %s#%d: the forge read-pull-request capability is unavailable", r.cfg.Repo, r.cfg.Ref)
	}
	pull, err := provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: r.cfg.Repo, Number: r.cfg.Ref})
	if err != nil {
		return fmt.Sprintf("could not verify the no-change claim against pull request %s#%d: %v", r.cfg.Repo, r.cfg.Ref, err)
	}
	if pull.Merged || strings.EqualFold(strings.TrimSpace(pull.State), "closed") {
		// The repair condition is moot: a merged or closed request can no
		// longer be conflicted, so the claim stands without manufacturing a
		// commit.
		return ""
	}
	if pull.Mergeable == nil {
		return fmt.Sprintf("opencode declared no change needed but pull request %s#%d mergeability against base %s is unknown; an unverified mergeability cannot settle the assignment", r.cfg.Repo, r.cfg.Ref, pull.BaseRef)
	}
	if !*pull.Mergeable {
		return fmt.Sprintf("opencode declared no change needed but pull request %s#%d still conflicts with base %s; the assignment is not satisfied", r.cfg.Repo, r.cfg.Ref, pull.BaseRef)
	}
	return ""
}

// confirmRemotePublication proves the run's current commit is published on
// the remote before the executor claims a confirmed missing PR or hands off
// as Verifying (#247). A branch ref merely existing is not publication: the
// remote must advertise the run's own head commit, so a stale remote branch
// (local commits never pushed) fails the check exactly like an absent one.
// Only a published head can be missing a pull request or carry publishable
// review work, and a failed check is unverifiable, not proof either way.
func (r reporter) confirmRemotePublication(ctx context.Context) (remotePublicationState, error) {
	localHead, err := (&git.Workspace{Directory: r.cfg.Directory}).Head(ctx)
	if err != nil {
		return remotePublicationState{}, fmt.Errorf("read local head of run workspace: %w", err)
	}
	remoteHead, exists, err := git.RemoteBranchHead(ctx, r.cfg.RemoteURL, r.cfg.Branch)
	if err != nil {
		return remotePublicationState{}, fmt.Errorf("read remote head of run branch %s: %w", r.cfg.Branch, err)
	}
	return remotePublicationState{
		published:  exists && remoteHead == localHead,
		localHead:  localHead,
		remoteHead: remoteHead,
	}, nil
}

// checkHandoff is the legacy executor's pre-handoff forge observation:
// it classifies the pull requests whose head carries the run branch
// (#247) and, for the single open PR, runs the body's two shared
// contracts — authoritative source-issue linkage (#248) and, when an
// acceptance-criterion set is pinned at admission (#280), the criterion-
// evidence block contract (#249). Both contracts are evaluated
// independently against the same live body the forge read returned, so
// a body that violates both earns one bounded continuation that names
// both defects instead of two sequential ones that would burn the
// shared #170 budget in turn. The criterion half additionally runs the
// bounded, explicitly non-semantic referential check of #367 — a file
// path cited by an implementation-evidence or regression-check row must
// exist at the exact pushed head this observation just verified — which
// fails open on any probe it cannot complete and rides that same single
// bounded repair instead of a budget of its own. The forge read goes through the
// configured forge capability — an identity-checked head listing that
// compares full base and head repository identity, keeps foreign head
// claims visible, and includes every state — never through a
// forge-specific CLI, so the legacy path enforces the same contract as
// the native broker and never relies on a model-supplied PR number.
//
// The function returns (nil, nil) when a single open, non-draft PR on the
// pinned base carries the verified head, the remote advertises the run's
// current commit, and the body satisfies every applicable contract —
// the run loop may proceed. A non-nil outcome carries one of: a
// confirmed missing PR on a confirmed-published head (a continuation to
// open one, #247), a draft PR (a continuation to ready it, #247), one
// or both body-contract repairs (a single bounded continuation names
// every applicable defect), or a fail-closed world the executor must
// not paper over with a retry or a creation — a head whose current
// commits the remote does not advertise (absent or stale ref), an
// ambiguous set of open PRs, only closed/merged PRs, a foreign record
// claiming the head selector from another repository, or an open PR
// whose actual base differs from the pinned base (#247). A failed
// live observation is returned as an error, never as a confirmed
// missing PR: an unreadable forge is not evidence of absence (#247).
//
// The operator-pinned criterion envelope is settled before the forge is
// read and before the observation is classified (see pinnedCriterionSet):
// an envelope that cannot be bound to the run's trusted source identity,
// or that does not decode at all, is an admission-time fault no PR-body
// edit can repair and no retry can outlive, so it is returned as an error
// rather than as an observation-specific outcome — it must never be spent
// a #170 continuation slot (#282).
func (r reporter) checkHandoff(ctx context.Context) (*handoffCheckOutcome, error) {
	pinned, err := r.pinnedCriterionSet()
	if err != nil {
		return nil, err
	}
	source, ok := trustedSourceIssue(r.cfg)
	if !ok {
		// fix-pr runs use the broker's existing identity check, and a
		// resolve-issue run with malformed identity carries no linkage
		// contract to enforce — but it may still carry a pinned
		// criterion envelope, and an operator-admitted contract that
		// cannot be bound to any trusted issue must not be skipped
		// silently: the run would otherwise hand off as Verifying
		// having enforced neither body contract. pinnedCriterionSet
		// already settled that case above, so the only envelope-free
		// way to reach here is a run with nothing pinned to enforce —
		// or a fix-pr run, whose pinned pull request is the contract it
		// must verify.
		//
		// A fix-pr run carries no source-issue body contract, but the
		// pinned pull request itself is the handoff contract, so the
		// same identity-checked observation re-verifies it at handoff
		// (#517); an envelope already failed closed above via
		// pinnedCriterionSet.
		if isFixPRRun(r.cfg) {
			return r.checkFixPRHandoff(ctx)
		}
		return nil, nil
	}
	observation, err := r.observeRunBranchPullRequest(ctx)
	if err != nil {
		return nil, err
	}
	switch observation.kind {
	case prStateOpen:
		// The PR head's published state is part of the world being
		// verified: an open PR on a stale remote ref must not carry a
		// Verifying handoff over local commits that never pushed.
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			return nil, err
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		// Evaluate both body contracts against the same live body so a
		// single bounded continuation can name every defect the model
		// must repair in one turn (#282, #249, #248). linkageErr and
		// the criterion result aggregate on the same outcome: each
		// is non-nil exactly when its contract fails, and a body
		// that violates both earns a single combined-repair verdict
		// rather than two sequential ones.
		outcome := &handoffCheckOutcome{pr: observation.open}
		if linkErr := broker.ValidateLinkage(observation.open.Body, source); linkErr != nil {
			var le *broker.LinkageError
			if errors.As(linkErr, &le) {
				outcome.err = le
			} else {
				return nil, linkErr
			}
		}
		criteriaOutcome, critErr := r.checkCriterionEvidence(ctx, publication.remoteHead, pinned, observation.open)
		if critErr != nil {
			return nil, critErr
		}
		if criteriaOutcome != nil {
			outcome.criteria = criteriaOutcome.criteria
			outcome.criteriaErr = criteriaOutcome.criteriaErr
			outcome.coverage = criteriaOutcome.coverage
			outcome.pinnedSet = criteriaOutcome.pinnedSet
			outcome.missingEvidencePaths = criteriaOutcome.missingEvidencePaths
			outcome.probedHead = criteriaOutcome.probedHead
		}
		// A passing combined outcome — neither contract fired — means
		// the body is acceptable and the run may hand off as Verifying.
		// The nil-outcome signal the verdict path recognizes is "no
		// defects", which only happens when both validators passed
		// (or the criterion check was a vacuous pass with no defects).
		if outcome.err == nil && outcome.criteriaErr == nil && outcome.criteria == nil && outcome.pinnedSet == nil {
			return nil, nil
		}
		return outcome, nil
	case prStateDraft:
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			return nil, err
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		return &handoffCheckOutcome{draft: true, pr: observation.draft}, nil
	case prStateAmbiguous:
		return &handoffCheckOutcome{ambiguous: true}, nil
	case prStateClosedOnly:
		return &handoffCheckOutcome{closedOnly: true}, nil
	case prStateForeignHead:
		return &handoffCheckOutcome{foreignHead: true}, nil
	case prStateWrongBase:
		return &handoffCheckOutcome{wrongBase: true}, nil
	case prStateMissing:
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			// The head's publication state is unverifiable: that is not
			// proof either way, so it surfaces as a read failure the
			// caller fails closed on, never as a confirmed missing PR.
			return nil, fmt.Errorf("confirm remote publication of run branch %s: %w", r.cfg.Branch, err)
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		return &handoffCheckOutcome{missing: true}, nil
	}
	// Unreachable: every observation kind is handled above.
	return nil, fmt.Errorf("unclassified pull-request observation state for run branch %s", r.cfg.Branch)
}

// checkFixPRHandoff is the fix-pr run's pre-handoff observation (#517):
// the same identity-checked head listing as the resolve-issue branch
// (#247) classifies the pull requests that carry the run's pinned head,
// and every world that does not match the handoff contract — an open,
// non-draft pull request on the pinned base with the run's commit
// published (the same #247 publication sha-proof) — fails closed to a
// NeedsHuman world the executor never papers over with a retry or a
// creation. Draft and missing differ deliberately from resolve-issue:
// a fix-pr run is admitted against a live PR, so a draft at handoff is
// a settled-by-human anomaly — no readying continuation — and the
// executor never opens a replacement for a pinned PR, which would
// duplicate or silently re-target the assignment. An unverifiable read
// is returned as an error, never as a confirmed missing PR (#247).
//
// The listing settles the run's HEAD, not the run's ASSIGNMENT: a fix-pr
// run is admitted against one pinned pull request (COURIER_REF), so the
// observed pull request must be that exact pull request before the
// handoff may verify it — the same exact-number, base, and head identity
// the native broker path enforces in checkPR/matchesPR. A head whose
// pull request is some other pull request fails closed (pinnedMismatch)
// instead of handing off a verification of an assignment nobody made.
// An unverifiable read is returned as an error, never as a confirmed
// missing PR (#247).
func (r reporter) checkFixPRHandoff(ctx context.Context) (*handoffCheckOutcome, error) {
	observation, err := r.observeRunBranchPullRequest(ctx)
	if err != nil {
		return nil, err
	}
	switch observation.kind {
	case prStateOpen:
		// The pinned assignment is the pull request the run was admitted
		// against, so the head's only open pull request must be that
		// pull request: verifying a different one would hand off a
		// contract the operator never pinned (#517).
		if observation.open.Number != r.cfg.Ref {
			return &handoffCheckOutcome{pinnedMismatch: true, pr: observation.open}, nil
		}
		// Same #247 rule as resolve-issue: the pinned head's published
		// state is part of the world being verified, so no Verifying
		// handoff may carry local commits the remote never advertised.
		publication, err := r.confirmRemotePublication(ctx)
		if err != nil {
			return nil, err
		}
		if !publication.published {
			return &handoffCheckOutcome{unpublished: true, publication: publication}, nil
		}
		// The pinned PR is open, non-draft, on the pinned base, under
		// the verified head, and the run's commit is published: hand off.
		return nil, nil
	case prStateWrongBase:
		return &handoffCheckOutcome{wrongBase: true}, nil
	case prStateAmbiguous:
		return &handoffCheckOutcome{ambiguous: true}, nil
	case prStateClosedOnly:
		// A closed-only head settles the assignment only when the closed
		// pull request IS the pinned one; a head that never carried the
		// pinned pull request leaves the assignment unverified rather
		// than closed (#517).
		if !carriesPullRequestNumber(observation.closed, r.cfg.Ref) {
			return &handoffCheckOutcome{pinnedMismatch: true, pr: observation.closed[0]}, nil
		}
		return &handoffCheckOutcome{closedOnly: true}, nil
	case prStateForeignHead:
		return &handoffCheckOutcome{foreignHead: true}, nil
	case prStateDraft:
		// A draft is a settled-by-human anomaly only when it is the
		// pinned pull request; a draft on the run's head that is some
		// other pull request leaves the assignment unverified (#517).
		if observation.draft.Number != r.cfg.Ref {
			return &handoffCheckOutcome{pinnedMismatch: true, pr: observation.draft}, nil
		}
		return &handoffCheckOutcome{pinnedDraft: true, pr: observation.draft}, nil
	case prStateMissing:
		return &handoffCheckOutcome{pinnedMissing: true}, nil
	}
	// Unreachable: every observation kind is handled above.
	return nil, fmt.Errorf("unclassified fix-pr pull-request observation state for run branch %s", r.cfg.Branch)
}

// carriesPullRequestNumber reports whether the observed pull requests
// include the run's pinned pull request number (#517). The fix-pr handoff
// uses it to tell a closed pinned pull request from a head that never
// carried the pinned assignment at all.
func carriesPullRequestNumber(observed []*prLinkageObservation, number int) bool {
	for _, pull := range observed {
		if pull != nil && pull.Number == number {
			return true
		}
	}
	return false
}

// pinnedCriterionSet returns the run's operator-pinned criterion envelope,
// decoded from COURIER_CRITERIA and bound to the run's trusted
// COURIER_REPO/COURIER_REF source identity, or nil when no envelope is
// pinned. It is the admission-time projection check the pre-handoff plan
// runs before it reads the forge (#282): the envelope is operator-authored
// from the persisted publication policy and read once from the pod
// environment at process start, so no live observation can change it, no
// PR-body edit can repair it, and no retry inside the bounded budget can
// outlive it. Settling it here — rather than inside the open-PR body check
// — is what keeps an envelope fault from hiding behind a missing, draft,
// unpublished, or otherwise fail-closed forge world and spending a #170
// slot on a fault no continuation can fix.
//
// Failure classes:
//   - No envelope pinned: nil, nil. The run carries no evidence contract.
//   - No bindable trusted identity, with an envelope pinned:
//     criteriaEnvelopeIdentityErr. An unbound contract must not be skipped
//     silently, or the run hands off Verifying having enforced neither
//     body contract.
//   - The envelope does not decode into a broker.CriterionSet:
//     criteriaEnvelopeDecodeErr. applyHandoffPlan emits the shared
//     EventLinkageReadFailed with kind:"criteria" — distinct from the
//     linkage observation read failure — so an operator can tell which
//     pre-handoff read failed without parsing error text.
//   - The envelope names a source issue other than the run's trusted one:
//     criteriaEnvelopeIdentityErr, naming both identities so the operator
//     can tell which admission input was wrong. Checked before the
//     pinned-empty shortcut, so a foreign envelope cannot slip through as
//     a vacuous pass.
//
// All three faults fail closed to NeedsHuman without consuming the #170
// budget (#282); the genuine linkage forge-read failure keeps its
// retry-inside-budget path.
func (r reporter) pinnedCriterionSet() (*broker.CriterionSet, error) {
	if strings.TrimSpace(r.cfg.Criteria) == "" {
		return nil, nil
	}
	source, ok := trustedSourceIssue(r.cfg)
	if !ok {
		return nil, fmt.Errorf("%w: the run carries no valid trusted source identity, so the pinned criterion envelope cannot be bound",
			criteriaEnvelopeIdentityErr)
	}
	var set broker.CriterionSet
	if err := json.Unmarshal([]byte(r.cfg.Criteria), &set); err != nil {
		return nil, fmt.Errorf("%w: %v", criteriaEnvelopeDecodeErr, err)
	}
	if !criteriaSetMatchesSource(set, source) {
		envelopeTarget := "(no source issue in envelope)"
		if !set.Issue.IsZero() {
			envelopeTarget = sourceIssueTarget(set.Issue)
		}
		return nil, fmt.Errorf("%w: envelope names %s, the run's trusted source is %s",
			criteriaEnvelopeIdentityErr, envelopeTarget, sourceIssueTarget(source))
	}
	return &set, nil
}

// checkCriterionEvidence validates the live PR body's evidence block
// against the already-decoded, already-bound pinned criterion set (#249,
// #280). It is the second body contract the legacy executor applies to
// the open PR's body after authoritative linkage (#248): a head whose
// linkage already failed cannot pass a body check, but a head whose
// linkage passed but whose evidence block does not satisfy the pinned set
// still must be repaired before handoff. The set is operator-authored at
// admission (pinnedCriterionSet); the validator's comparison target is
// never model-controlled.
//
// Behavior:
//   - No envelope pinned, or an empty pinned set (an issue without an
//     acceptance section, or a set with zero criteria): vacuous pass.
//     Return nil so the planner carries forward as if no set were pinned;
//     linkage is the only PR-body contract for that run.
//   - Body validation error: a *broker.CriteriaError is recorded on the
//     outcome (criteriaErr); a partial-success report is also kept on
//     the outcome (criteria) so the repair path can quote the rows and
//     per-status counts, and any non-CriteriaError is returned as a plain
//     error. The pinned set is always attached (pinnedSet) so the repair
//     path can name its digest and the verbatim criterion texts the model
//     must satisfy (#282).
//   - On success, populate outcome.criteria with the report and
//     outcome.coverage with its claimed coverage, and run the referential
//     check of #367 against head (see missingEvidencePaths below).
//
// checkCriterionEvidence is only called inside the prStateOpen branch of
// checkHandoff (#249): no PR — or a draft, ambiguous, closed-only,
// foreign, wrong-base, or unpublished head — has a body that the
// validator can compare against the pinned set. The envelope faults
// themselves are settled before the observation (pinnedCriterionSet), so
// this function never decodes and never fails on the envelope.

// criteriaEnvelopeDecodeErr is the typed sentinel the criterion-envelope
// decoder returns when the operator-authored COURIER_CRITERIA envelope is
// not a valid CriterionSet (#280, #282). The pre-handoff plan emits the
// shared EventLinkageReadFailed event with kind:"criteria" for this class,
// distinct from the linkage observation read failure, so an operator can
// tell which pre-handoff read failed without parsing the error text. The
// envelope is read once from the pod environment at process start, so no
// retry can change it: the plan fails closed to NeedsHuman without
// spending the #170 continuation budget rather than retrying inside it.
var criteriaEnvelopeDecodeErr = errors.New("decode COURIER_CRITERIA envelope")

// criteriaEnvelopeIdentityErr is the typed sentinel the criterion-envelope
// decoder returns when the operator-authored COURIER_CRITERIA envelope
// names a source issue other than the run's trusted source identity, or
// when the run carries no bindable trusted identity at all (#282). It is a
// policy mismatch between two operator-side inputs — the admission
// projection and the run's pinned repo/ref — not a defect the model can
// repair by editing the PR body, so the pre-handoff plan fails closed to
// NeedsHuman on sight rather than spending a bounded continuation on it.
var criteriaEnvelopeIdentityErr = errors.New("COURIER_CRITERIA envelope disagrees with the run's trusted source issue")

// criteriaSetMatchesSource reports whether a decoded criterion envelope
// carries the run's trusted source-issue identity. The comparison is the
// executor's trust binding: the envelope is operator-authored at
// admission, but a stale or misprojected envelope naming a different
// issue must never become the contract a PR body is validated against,
// because the linkage contract and the evidence contract must anchor to
// the same issue (#282). Ownership letters are compared case-
// insensitively, mirroring the forge's own repository identity.
func criteriaSetMatchesSource(set broker.CriterionSet, source broker.SourceIssue) bool {
	return strings.EqualFold(set.Issue.Owner, source.Owner) &&
		strings.EqualFold(set.Issue.Name, source.Name) &&
		set.Issue.Number == source.Number
}

func (r reporter) checkCriterionEvidence(ctx context.Context, head string, set *broker.CriterionSet, observation *prLinkageObservation) (*handoffCheckOutcome, error) {
	if set == nil || set.IsEmpty() {
		return nil, nil
	}
	// Carry the pinned set on the outcome so the repair path can
	// quote its digest, IDs, and texts verbatim. The set is
	// operator-authored; passing it through the outcome is a
	// reference hand-off, not a copy that the model could rewrite.
	report, err := broker.ValidateCriterionEvidence(observation.Body, *set)
	if err != nil {
		var ce *broker.CriteriaError
		if errors.As(err, &ce) {
			coverage := ""
			if report != nil {
				coverage = report.Coverage
			}
			// The validator already failed the block, and its own
			// structured reason names every defect the repair must fix,
			// so no path is probed here (#367): a row that cites a
			// missing file is only reported on the success path, never
			// double-reported behind a rejection the model has not
			// reached yet.
			return &handoffCheckOutcome{
				pr:          observation,
				criteria:    report,
				criteriaErr: ce,
				coverage:    coverage,
				pinnedSet:   set,
			}, nil
		}
		return nil, err
	}
	coverage := ""
	if report != nil {
		coverage = report.Coverage
	}
	// Only a block the validator accepted is probed for existence
	// (#367), and the probe is skipped entirely when there is no
	// published head to read: the observation only reaches this branch
	// once confirmRemotePublication proved the remote advertises the
	// run's own head commit, so head is a real commit in the local
	// workspace, and an empty one means the caller had nothing to
	// verify — failing open there keeps the vacuous case (no head, no
	// rows, no pinned set) exactly as it was before the check existed.
	missing, probed := r.missingEvidencePaths(ctx, report, head)
	return &handoffCheckOutcome{
		pr:                   observation,
		criteria:             report,
		coverage:             coverage,
		pinnedSet:            set,
		missingEvidencePaths: missing,
		probedHead:           probed,
	}, nil
}

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
//     draft and missing without the resolve-issue continuations), and
//     a resumable read failure retries with the fix-pr unverified
//     message naming the pinned identity.
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
		return handoffVerdict{FailReason: r.criteriaIdentityMismatchReason(linkErr)}
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
		return handoffVerdict{FailReason: r.criteriaEnvelopeDecodeReason(linkErr)}
	}
	if linkErr != nil {
		// The linkage forge read failed: the identity-checked PR listing
		// or the remote-publication confirmation behind the observation.
		// The envelope decode and identity-mismatch classes were already
		// handled fail-closed above, so this path is the linkage
		// observation alone and kind:"linkage" is the discriminator.
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
	if linkOutcome == nil {
		return handoffVerdict{Pass: true}
	}
	// Fail-closed worlds first: none of these is a condition the model
	// may resolve by retrying or creating (#247).
	switch {
	case linkOutcome.ambiguous:
		return handoffVerdict{FailReason: r.prAmbiguousReason()}
	case linkOutcome.closedOnly:
		return handoffVerdict{FailReason: r.closedOnlyPRReason()}
	case linkOutcome.foreignHead:
		return handoffVerdict{FailReason: r.foreignHeadPRReason()}
	case linkOutcome.wrongBase:
		return handoffVerdict{FailReason: r.wrongBasePRReason()}
	case linkOutcome.unpublished:
		return handoffVerdict{FailReason: r.unpublishedBranchReason(linkOutcome.publication)}
	case linkOutcome.pinnedMissing:
		// fix-pr-only worlds whose fixed consequences no resolve-issue
		// continuation may stand in for (#517): a missing pinned PR is
		// never opened as a replacement, and a draft pinned PR is
		// never readied unilaterally.
		return handoffVerdict{FailReason: r.pinnedPRMissingReason()}
	case linkOutcome.pinnedMismatch:
		// The observed pull request is not the pinned assignment (#517):
		// the head's pull request is never a substitute for the pull
		// request the run was admitted against, so the handoff fails
		// closed instead of verifying an assignment nobody pinned. The
		// verdict carries no PR number: the observed pull request is
		// not this run's pull request, and the reason names both
		// numbers instead.
		return handoffVerdict{FailReason: r.pinnedPRNumberMismatchReason(r.cfg.Ref, linkOutcome.pr.Number)}
	case linkOutcome.pinnedDraft:
		return handoffVerdict{PRNumber: linkOutcome.pr.Number, FailReason: r.pinnedPRDraftReason(linkOutcome.pr.Number)}
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
	// A prStateOpen outcome: the PR is open, the remote is published,
	// and zero, one, or both body contracts fired. The verdict path
	// aggregates every applicable defect into one bounded
	// continuation so a body that violates both contracts earns one
	// repair turn (with linkage named first as the structurally prior
	// contract) rather than two sequential repairs that would burn
	// the shared #170 budget in turn (#282).
	//
	// The criterion half of that contract is no longer the validator's
	// error alone since #367: a block the validator accepted whose
	// citations name files absent from the published head is the same
	// repairable defect class, so it is folded into one predicate and the
	// three cases below keep their meaning. One predicate, three call
	// sites: a fourth spelling of "did the criterion half find something"
	// is exactly how the combined and single-contract verdicts would
	// drift apart again.
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

// installGitIdentity supplies the default git identity for any GIT_* variable
// the process does not already carry. A deployment-provided value — a shell
// export or a key of the executor environment Secret — always wins; only
// unset or blank names fall back, so the standalone path overrides without an
// operator (or upstream) hard-coding a particular account (#317).
func installGitIdentity() func() {
	defaults := map[string]string{
		executor.EnvGitAuthorName:     executor.DefaultGitAuthorName,
		executor.EnvGitAuthorEmail:    executor.DefaultGitAuthorEmail,
		executor.EnvGitCommitterName:  executor.DefaultGitAuthorName,
		executor.EnvGitCommitterEmail: executor.DefaultGitAuthorEmail,
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

// emitTermination writes the legacy COURIER_TERMINATION line to stdout (and to
// cfg.TerminationFile when set). The whole line is bounded by the Kubernetes
// pod termination-message budget: when the marshalled payload would push the
// line past executor.MaxTerminationLineBytes, the compact telemetry summary
// is shrunk — subagent entries dropped lowest-runtime first, then the summary
// key itself — so phase, result, exit_code, outcome and reason always survive.
func emitTermination(stdout io.Writer, cfg config, result termination) {
	line := fitTerminationLine(result)
	if line == "" {
		// A marshal failure leaves no handoff line; write nothing rather than
		// an empty handoff that would truncate the termination file.
		return
	}
	_, _ = io.WriteString(stdout, line)
	if cfg.TerminationFile == "" {
		return
	}
	// The file is an optional local handoff for an operator or sidecar. It is
	// never used for credentials and is replaced atomically enough for a single
	// writer in the ephemeral workspace.
	_ = os.WriteFile(cfg.TerminationFile, []byte(line), 0o600)
}

// fitTerminationLine renders the handoff line, shrinking the telemetry summary
// until the line fits the termination-message budget, and — only once the
// summary is fully gone — delegating the reason-shrink step to the shared
// executor.FitTerminationLine so the two COURIER_TERMINATION producers
// (here and courier-control) cannot drift on the wire prefix, the budget,
// or the truncation marker (#210). The summary always gives ground first;
// the reason is cut last and only as much as needed to fit, and it is kept
// non-empty. phase/result/exit_code/outcome are never touched. It may shrink
// result.Summary in place: a caller must not reuse the passed Summary
// pointer expecting the untouched payload.
func fitTerminationLine(result termination) string {
	line := marshalTerminationLine(result)
	for len(line) > executor.MaxTerminationLineBytes && result.Summary != nil {
		if len(result.Summary.Subagents) > 0 {
			// Subagents are ordered highest-runtime first, so the final entry
			// is the lowest-runtime; drop it and re-measure.
			result.Summary.Subagents = result.Summary.Subagents[:len(result.Summary.Subagents)-1]
		} else {
			// The scalar summary fields alone are tiny; with an empty subagent
			// list the summary earns no place in the budget.
			result.Summary = nil
		}
		line = marshalTerminationLine(result)
	}
	// Last resort: the summary is gone yet the line still overruns the
	// budget. An escape-heavy reason marshals to far more bytes than its raw
	// length (each <, > or & becomes a six-byte \uXXXX escape), so the
	// raw-byte truncation in terminate cannot see the overrun. The shared
	// FitTerminationLine cuts the reason until the line fits; it is the only
	// field left allowed to give ground and it always stays non-empty.
	return executor.FitTerminationLine(&result)
}

// marshalTerminationLine renders the handoff line for a fixed termination.
// It is a thin alias for the shared executor.MarshalTerminationLine, kept
// so the executor's own summary-shrink loop and any future local helper
// stay self-explanatory.
func marshalTerminationLine(result termination) string {
	return executor.MarshalTerminationLine(result)
}
