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
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/executor"
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
	// RebaseAssist is the raw COURIER_REBASE_MODEL_ASSIST enable flag for
	// model-assisted conflict resolution (#732); parsed by the resolver
	// adapter, not here, so the on/off vocabulary lives beside the transport.
	RebaseAssist string
	// ModelGatewayURL/ModelGatewayAPIKey are the OpenAI-compatible gateway
	// the rebase assist resolves through. The key is credential material:
	// it reaches only the gateway adapter's HTTP header.
	ModelGatewayURL    string
	ModelGatewayAPIKey string
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
		RebaseAssist:           getenv("COURIER_REBASE_MODEL_ASSIST"),
		ModelGatewayURL:        strings.TrimSpace(getenv("COURIER_MODEL_GATEWAY_URL")),
		ModelGatewayAPIKey:     getenv("COURIER_MODEL_GATEWAY_API_KEY"),
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
	if isPRRepairMode(cfg.Mode) && cfg.HeadRepo == "" {
		return config{}, errors.New("COURIER_HEAD_REPO is required for " + cfg.Mode + " runs")
	}
	if isPRRepairMode(cfg.Mode) && cfg.Ref < 1 {
		// A fix-pr run is admitted against one pinned pull request, and
		// the pre-handoff observation verifies exactly that pull request
		// (#517). A rebase run is admitted the same way and additionally
		// leases its publication to the head observed then (#715). Without a
		// usable pinned reference there is nothing to verify, so the run
		// must not proceed to a Verifying handoff it can never prove: it
		// fails closed here, where the malformed input is still visible,
		// instead of silently skipping the verification it exists to
		// perform.
		return config{}, errors.New("COURIER_REF must be the pinned pull request number for " + cfg.Mode + " runs")
	}
	if strings.EqualFold(cfg.Mode, "rebase") && cfg.HeadSHA == "" {
		// The lease is the point of the rebase publication contract: without
		// the admission head there is no compare-and-set, so the run refuses
		// to start rather than publishing unleased (#715).
		return config{}, errors.New("COURIER_HEAD_SHA is required for rebase runs")
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
