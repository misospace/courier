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
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
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
	maxResumeBackoffSeconds = 3600
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
	TerminationFile  string
	GitUsername      string
	GitToken         string
	GitHubToken      string
	EvidenceURL      string
	EvidenceToken    string
	PodUID           string
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
	// Category is the stable, machine-readable cause classification (#272),
	// matching the TerminalDiagnosticCategory values the operator persists.
	Category string `json:"category,omitempty"`
	// Summary is the compact per-run telemetry tallied from the OpenCode event
	// stream (#172). Omitted when the run recorded no activity.
	Summary *courierv1alpha1.RunTelemetry `json:"summary,omitempty"`
}

func main() {
	if os.Getenv("COURIER_ASKPASS") == "1" {
		os.Exit(askpass(os.Args[1:], os.Getenv("COURIER_GIT_USERNAME"), os.Getenv("COURIER_GIT_TOKEN"), os.Stdout))
	}
	// SIGTERM cancels the run context only: a handler that captured would
	// race os.Exit, and the cancellation path funnels the child kill and the
	// capture through the run's ordinary exit codes (DESIGN.md, #115).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
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
	resumeBackoff, err := strconv.ParseFloat(strings.TrimSpace(getenv("COURIER_RESUME_BACKOFF_SECONDS")), 64)
	if err != nil || math.IsNaN(resumeBackoff) || math.IsInf(resumeBackoff, 0) || resumeBackoff < 0 {
		resumeBackoff = defaultResumeBackoff.Seconds()
	}
	if resumeBackoff > maxResumeBackoffSeconds {
		resumeBackoff = maxResumeBackoffSeconds
	}
	cfg := config{
		RemoteURL:        remoteURL,
		BaseRemoteURL:    strings.TrimSpace(getenv("COURIER_BASE_REPO_URL")),
		Directory:        strings.TrimSpace(getenv("COURIER_WORKSPACE")),
		ScratchDirectory: strings.TrimSpace(getenv("COURIER_SCRATCH_DIR")),
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
		EvidenceURL:      strings.TrimSpace(getenv(executor.EnvEvidenceURL)),
		EvidenceToken:    strings.TrimSpace(getenv(executor.EnvEvidenceToken)),
		PodUID:           strings.TrimSpace(getenv(executor.EnvPodUID)),
		Criteria:         strings.TrimSpace(getenv("COURIER_CRITERIA")),
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

// Bounds for the failure-evidence capture family (#198). They are variables,
// not constants, so an in-process test can shrink them and exercise the
// deadline and retry paths in milliseconds.

// evidenceOperationDeadline bounds one capture: world gate, snapshot, and
// delivery together. It is an operation bound, not a run wall clock (the
// same kind of bound as the MCP preflight), and it must fit inside the
// pod's 45-second termination grace so a SIGTERM-triggered capture completes
// before the kubelet force-kills the process (DESIGN.md, #115).
var evidenceOperationDeadline = 20 * time.Second

// evidenceDegradedDeliveryDeadline bounds the delivery of the manifest-only
// degraded bundle once the operation deadline has already failed the
// capture: the operation-bound context can no longer carry the POST, but
// the degradation guarantee outranks the operation bound for the manifest
// alone — a deadline miss must still deliver the manifest (DESIGN.md,
// #115). Five seconds keeps the total (20s + 5s) inside the pod's
// 45-second termination grace.
var evidenceDegradedDeliveryDeadline = 5 * time.Second

// evidenceDeliveryAttempts bounds the delivery POSTs inside that deadline:
// at most two, then stop — no cross-reconcile retry, no requeue; loss is
// recorded in the redacted evidence.capture event instead.
var evidenceDeliveryAttempts = 2

// evidenceNamespaceFile is the downward-API file carrying the pod's
// namespace for the capture's run identity.
const evidenceNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// podNamespace reads the pod's namespace from the downward-API file, or ""
// when the file is absent: a missing identity field degrades the manifest,
// it never fails the capture.
func podNamespace() string {
	data, err := os.ReadFile(evidenceNamespaceFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// askpassEnvNames are the process-control variables installAskpass sets on
// this executor process so its git children authenticate non-interactively.
// They are operator-control flags the executor sets for itself, not
// credentials, so processEnvironment drops them before the evidence scanner
// sees the environment. COURIER_ASKPASS is secret-shaped by name (suffix
// "PASS") and carries the one-byte literal "1"; the scanner retains
// secret-shaped values fail-closed regardless of length, and "1" occurs in
// every git format-patch, so registering it withholds the entire captured
// bundle. Real credentials stay registered and still fail closed.
var askpassEnvNames = map[string]bool{
	"GIT_ASKPASS":         true,
	"GIT_TERMINAL_PROMPT": true,
	"COURIER_ASKPASS":     true,
}

// processEnvironment returns the process environment as a name→value map for
// the evidence scanner, which registers only secret-shaped names fail
// closed. The executor's own process-control variables (askpassEnvNames) are
// skipped so a retained control literal cannot match every patch text and
// withhold the whole bundle; everything else is registered so real credential
// values still fail closed.
func processEnvironment() map[string]string {
	env := make(map[string]string)
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || askpassEnvNames[name] {
			continue
		}
		env[name] = value
	}
	return env
}

// fetchRunBranch fetches the remote run branch into FETCH_HEAD with the
// default process environment: the run's GIT_ASKPASS is already installed by
// run(), so the fetch authenticates like every other git call in the run.
// Output is discarded; only the exit code decides.
func fetchRunBranch(ctx context.Context, dir, remoteURL, branch string) error {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "fetch", "--no-tags", remoteURL, "refs/heads/"+branch)
	// The capture operation bound must stay enforceable when a spawned git remote
	// helper or askpass grandchild holds the output pipe after the context fires.
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

// revListCount counts the commits in a rev-list range expression.
func revListCount(ctx context.Context, dir, span string) (int, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "rev-list", "--count", span)
	// The capture operation bound must stay enforceable when a spawned git remote
	// helper or askpass grandchild holds the output pipe after the context fires.
	cmd.WaitDelay = 2 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return 0, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(stdout.String()))
	if err != nil {
		return 0, err
	}
	return count, nil
}

// captureGate is the single place deciding whether a capture is warranted at
// a moment. A clean, remote-held worktree never captures: only unrecoverable
// state — uncommitted edits, local commits the remote run branch does not
// hold, or a failed world read that leaves the state unknown — is worth
// preserving, and the world is the remote, not the local ref, so the
// committed case is checked with one bounded fetch-and-compare. It returns
// (unrecoverable, remoteFetched); remoteFetched means FETCH_HEAD now names a
// fresh remote run-branch tip a capture can use as its comparison point.
func captureGate(ctx context.Context, workspace *git.Workspace, remoteURL, branch, startCommit string) (unrecoverable bool, remoteFetched bool) {
	ws, err := workspace.WorkState(ctx, startCommit)
	if err != nil {
		// A failed world read leaves the state unknown; unknown is
		// unrecoverable by definition.
		return true, false
	}
	if ws == git.WorkStateDirty {
		// Dirty is unrecoverable without the remote, but a successful fetch
		// still tells the capture which commits the remote holds, so
		// remote-held work is not duplicated (DESIGN #115 principle 8).
		if fetchRunBranch(ctx, workspace.Directory, remoteURL, branch) == nil {
			return true, true
		}
		return true, false
	}
	if ws == git.WorkStateNone {
		return false, false
	}
	if err := fetchRunBranch(ctx, workspace.Directory, remoteURL, branch); err != nil {
		// The remote does not confirm the commits — including the branch
		// missing on the remote.
		return true, false
	}
	count, err := revListCount(ctx, workspace.Directory, "FETCH_HEAD..HEAD")
	if err != nil {
		return true, false
	}
	if count > 0 {
		// Local commits the remote run branch does not hold.
		return true, true
	}
	return false, true
}

// maybeCaptureEvidence is the one capture family: every gated moment funnels
// through it, and it is a silent no-op unless capture is configured and the
// gate finds unrecoverable state, so a clean, remote-held worktree captures
// nothing and emits nothing. When the gate fires, it snapshots the worktree
// and local commits under a deadline derived from the run context with
// cancellation stripped — the SIGTERM path arrives with the run context
// already canceled, and the capture must still live inside the pod's
// termination grace — degrades to the manifest alone when the capture
// itself fails (DESIGN.md, #115), and when the operation deadline itself
// caused that failure delivers the manifest under a fresh short window,
// because the degradation guarantee outranks the operation bound for the
// manifest alone: a deadline miss must still reach the intake — then emits
// exactly one redacted evidence.capture event. It never touches the run's
// ending.
func (r reporter) maybeCaptureEvidence(runCtx context.Context, workspace *git.Workspace, startCommit, trigger string) {
	if !r.cfg.evidenceEnabled() || workspace == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(runCtx), evidenceOperationDeadline)
	defer cancel()
	unrecoverable, remoteFetched := captureGate(ctx, workspace, r.cfg.RemoteURL, r.cfg.Branch, startCommit)
	if !unrecoverable {
		return
	}
	headSHA, _ := workspace.Head(ctx)
	upstreamRef := ""
	if remoteFetched {
		// FETCH_HEAD names the fresh remote run-branch tip, so the captured
		// local commits are exactly the ones the remote lacks; empty falls
		// back to StartSHA.
		upstreamRef = "FETCH_HEAD"
	}
	scanner := evidence.NewScanner()
	scanner.RegisterCredentials(processEnvironment())
	opts := evidence.Options{
		Directory:   workspace.Directory,
		UpstreamRef: upstreamRef,
		StartSHA:    startCommit,
		Run: evidence.RunIdentity{
			Name:      r.cfg.RunID,
			Namespace: podNamespace(),
			// The downward API cannot expose the CoderRun UID; the
			// per-incarnation bearer token binds it, and the intake treats
			// the token as authoritative.
			RunUID: "",
			PodUID: r.cfg.PodUID,
		},
		Workspace: evidence.WorkspaceIdentity{
			BaseRepo: r.cfg.Repo,
			HeadRepo: r.cfg.HeadRepo,
			Branch:   r.cfg.Branch,
			StartSHA: startCommit,
			HeadSHA:  headSHA,
		},
		Trigger: trigger,
		Scanner: scanner,
	}
	bundle, err := evidence.Capture(ctx, opts)
	deliveryCtx := ctx
	if err != nil {
		// Git lock contention, a read failure, or the operation deadline
		// degrades the capture to the manifest alone (DESIGN.md, #115).
		// Delivery stays inside the same operation deadline unless the
		// deadline itself caused the failure: then the operation-bound
		// context can no longer carry the POST, but the degradation
		// guarantee outranks the operation bound for the manifest alone —
		// a deadline miss must still deliver the manifest — so the
		// manifest gets a fresh short window derived from the run context
		// with cancellation stripped. Five seconds keeps the total (20s +
		// 5s) inside the pod's 45-second termination grace.
		bundle = &evidence.Bundle{
			Manifest: evidence.Manifest{
				SchemaVersion: evidence.SchemaVersion,
				Run:           opts.Run,
				Workspace:     opts.Workspace,
				CapturedAt:    time.Now().UTC(),
				Trigger:       trigger,
				Entries:       []evidence.Entry{},
				Totals:        evidence.Totals{},
			},
		}
		if ctx.Err() != nil {
			var cancel context.CancelFunc
			deliveryCtx, cancel = context.WithTimeout(context.WithoutCancel(runCtx), evidenceDegradedDeliveryDeadline)
			defer cancel()
		}
	}
	delivered, _ := deliverEvidenceBundle(deliveryCtx, r.cfg.EvidenceURL, r.cfg.EvidenceToken, bundle)
	outcome := "lost"
	status := courierlog.StatusError
	if delivered {
		outcome = "delivered"
		if err != nil {
			outcome = "degraded"
		}
		status = courierlog.StatusOK
	}
	totals := bundle.Manifest.Totals
	// A loss record must reach the log store on every run, not only on debug:
	// the emitter drops Detail unless the event marks itself Verbose, and
	// r.event never sets it.
	emitErr := r.events.Emit(courierlog.Event{
		Type:    courierlog.EventEvidenceCapture,
		RunID:   r.cfg.RunID,
		Repo:    r.cfg.Repo,
		Ref:     r.cfg.Ref,
		Mode:    r.cfg.Mode,
		Model:   r.cfg.Model,
		Status:  status,
		Verbose: true,
		Detail: map[string]any{
			"trigger": trigger,
			"outcome": outcome,
			"totals": map[string]any{
				"files":              totals.Files,
				"commits":            totals.Commits,
				"stored":             totals.Stored,
				"withheld":           totals.Withheld,
				"omitted_binary":     totals.OmittedBinary,
				"omitted_over_limit": totals.OmittedOverLimit,
				"omitted_by_budget":  totals.OmittedByBudget,
				"deleted":            totals.Deleted,
				"symlinks":           totals.Symlinks,
				"stored_bytes":       totals.StoredBytes,
			},
		},
	})
	if emitErr != nil {
		fmt.Fprintf(r.stderr, "courier: dropped %s event: %v\n", courierlog.EventEvidenceCapture, emitErr)
	}
}

// deliverEvidenceBundle POSTs one bundle to the intake inside the operation
// deadline. The wire contract shared with the #200 intake: a POST of
// multipart/form-data with a part named "manifest" carrying the canonical
// JSON manifest and, when the capture admitted content, a part named
// "archive" carrying the gzip'd tar verbatim, authenticated with
// Authorization: Bearer <token>. Success is any 2xx. At most
// evidenceDeliveryAttempts attempts: the context is checked before each
// attempt, there is no attempt after the deadline, no retry after success,
// and a non-2xx or a transport error consumes an attempt. It returns whether
// the bundle was delivered and how many attempts were made.
func deliverEvidenceBundle(ctx context.Context, endpoint, token string, bundle *evidence.Bundle) (delivered bool, attempts int) {
	for attempts < evidenceDeliveryAttempts {
		if ctx.Err() != nil {
			break
		}
		attempts++
		if postEvidenceBundle(ctx, endpoint, token, bundle) {
			return true, attempts
		}
	}
	return false, attempts
}

// postEvidenceBundle performs one delivery POST and reports whether it
// landed with a 2xx.
func postEvidenceBundle(ctx context.Context, endpoint, token string, bundle *evidence.Bundle) bool {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	manifestPart, err := writer.CreateFormFile("manifest", "manifest.json")
	if err != nil {
		return false
	}
	manifestJSON, err := bundle.Manifest.MarshalCanonical()
	if err != nil {
		return false
	}
	if _, err := manifestPart.Write(manifestJSON); err != nil {
		return false
	}
	if bundle.Archive != nil {
		archivePart, err := writer.CreateFormFile("archive", "archive.tar.gz")
		if err != nil {
			return false
		}
		if _, err := archivePart.Write(bundle.Archive); err != nil {
			return false
		}
	}
	if err := writer.Close(); err != nil {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// terminateCaptured is the terminal exit for a run whose workspace exists
// and whose start commit is recorded. It takes the gate's final look at the
// world before the ordinary terminal handoff, so an ending that leaves
// unrecoverable state is captured — under the trigger "cancellation" when
// the run context is already canceled, "terminal" otherwise. The capture
// never changes the ending: the handoff line and the run.exit event follow
// unchanged, in the same order.
func (r reporter) terminateCaptured(ctx context.Context, workspace *git.Workspace, startCommit string, result termination) {
	trigger := evidence.TriggerTerminal
	if ctx.Err() != nil {
		trigger = evidence.TriggerCancellation
	}
	r.maybeCaptureEvidence(ctx, workspace, startCommit, trigger)
	r.terminate(result)
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
	tel       *telemetry.Tracker
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
	if t.tel != nil {
		t.tel.Observe(line)
	}
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

	runtime := executor.OpenCode{Binary: cfg.OpenCodeBinary, Format: cfg.OpenCodeFormat, Agent: cfg.OpenCodeAgent}
	caps := preflightCapabilities(ctx, runtime.MCPStatusCommand(), workspace.Directory)
	if len(caps) > 0 {
		report.emitCapabilityStatus(caps)
	}
	if note := capabilityNote(caps); note != "" {
		cfg.Framing = strings.TrimSpace(cfg.Framing + "\n\n" + report.red.Redact(note))
	}
	// Child output is redacted before it reaches the run streams. The session tap
	// observes OpenCode's JSON events without changing their bytes.
	stdoutRedacted := courierlog.NewRedactingWriter(stdout, report.red)
	stderrTransport := courierlog.NewRedactingWriter(stderr, report.red)
	tap := newSessionTap(stdoutRedacted)
	tap.tel = report.tel

	// Recoverable workspace endings and crashes share one continuation budget.
	continuations, crashes := 0, 0
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
				Session:   tap.sessionID,
			}
		}
		command := runtime.Command(invocation)
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
		process.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		process.Cancel = func() error {
			return syscall.Kill(-process.Process.Pid, syscall.SIGKILL)
		}
		// Bound the wait for the group after the context is done; the margin
		// keeps the capture inside the 45-second termination grace.
		process.WaitDelay = 10 * time.Second
		process.Dir = workspace.Directory
		process.Stdout = tap
		process.Stderr = stderrTransport
		processErr := process.Run()
		_ = tap.Flush()
		_ = stdoutRedacted.Flush()
		_ = stderrTransport.Flush()

		if processErr != nil {
			code := processExitCode(processErr)
			if code < 0 {
				code = exitFailed
			}
			// Raw child exit 2 is not a declaration; only the verified outcome
			// handoff may request NeedsHuman.
			if code == exitNeedsHuman {
				report.terminateCaptured(ctx, workspace, startCommit, failedWith(fmt.Sprintf("opencode exited with status %d", code), terminalCategoryModelTransportError))
				return exitFailed
			}
			if tap.sessionID != "" && ctx.Err() == nil && continuations < cfg.MaxContinuations {
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
			reason := fmt.Sprintf("opencode exited with status %d", code)
			if continuations > 0 {
				reason += fmt.Sprintf(" after %d continuations", continuations)
			}
			report.terminateCaptured(ctx, workspace, startCommit, failedWith(reason, terminalCategoryModelTransportError))
			return exitFailed
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
			// terminating terminal. This keeps the legacy executor from
			// forcing NeedsHuman on a declared run whose captured
			// session still has unused continuation budget.
			if result.Phase == "Verifying" {
				head, headErr := workspace.Head(ctx)
				_ = headErr
				verdict := report.applyHandoffPlan(ctx, continuations, cfg.MaxContinuations, tap.sessionID, ctx.Err())
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
					history = append(history, stateSummary(verdict.Kind, head, tap.lastText, nil))
					continue
				}
				if verdict.FailReason != "" {
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
				verdict := report.applyHandoffPlan(ctx, continuations, cfg.MaxContinuations, tap.sessionID, ctx.Err())
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
					history = append(history, stateSummary(verdict.Kind, head, tap.lastText, nil))
					continue
				}
				if verdict.FailReason != "" {
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
				message = fmt.Sprintf("You ended with uncommitted changes; commit and push them to %q, or discard them and declare needs_decision or blocked_external if a human is needed.", cfg.Branch)
			} else {
				message = fmt.Sprintf("You ended with uncommitted changes in %s%s; commit and push them to %q, or discard them and declare needs_decision or blocked_external if a human is needed.", strings.Join(listed, ", "), more, cfg.Branch)
			}
			summary = stateSummary(kind, head, tap.lastText, listed)
		default:
			kind = "no-work"
			message = fmt.Sprintf("You ended without producing a commit or workspace changes and did not declare an outcome. Continue the work and push it to %q, or declare needs_decision or blocked_external if a human is needed.", cfg.Branch)
			summary = stateSummary(kind, head, tap.lastText, nil)
			if names := unavailableCapabilities(caps); len(names) > 0 {
				suffix := "; configured capability unavailable: " + strings.Join(names, ", ")
				message += suffix
				summary += suffix
			}
		}

		if tap.sessionID == "" {
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

		fingerprint, err := continuationFingerprint(workState, branch, head, dirty, tap.lastText)
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

const (
	terminalCategoryExecutorCrash          = "executor-crash"
	terminalCategoryModelTransportError    = "model-transport-error"
	terminalCategoryContinuationExhaustion = "continuation-exhaustion"
	terminalCategoryVerifierRejection      = "verifier-rejection"
	terminalCategoryUnknown                = "unknown"
	terminalCategoryNeedsDecision          = "needs-decision"
	terminalCategoryBlockedExternal        = "blocked-external"
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

func failedWith(reason, category string) termination {
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
		if workState == git.WorkStateNone {
			r.postOutcomeComment(ctx, "**Courier: already addressed** (run "+r.cfg.RunID+")\n\n"+decl.Evidence)
			return termination{Phase: "NoChangeNeeded", Result: "success", ExitCode: exitNoChangeNeeded, Reason: "opencode declared no change needed: " + decl.Evidence, Outcome: outcomeNoChangeNeeded}
		}
		return failedWith("opencode declared no change needed but the workspace has new commits or uncommitted changes", terminalCategoryVerifierRejection)
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
		ahead, branchErr := workspace.CommitsOnBranchSince(ctx, branch, startCommit)
		if branchErr == nil && ahead == 0 {
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
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1, true
	}
	if !exists {
		return 0, false
	}
	owner, name, ok := splitOwnerRepo(r.cfg.Repo)
	if !ok {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "resolve branch already exists on the remote and COURIER_REPO does not name an owner/repo; refusing adoption", Category: terminalCategoryExecutorCrash})
		return 1, true
	}
	client, err := github.NewClient(r.cfg.GitHubAPIBase, r.cfg.GitHubToken)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1, true
	}
	pulls, err := client.PullRequestsForHead(ctx, owner, name, owner, r.cfg.Branch)
	if err != nil {
		r.terminate(termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error(), Category: terminalCategoryExecutorCrash})
		return 1, true
	}
	for _, pull := range pulls {
		if pull.Head.Ref == r.cfg.Branch && strings.EqualFold(pull.State, "open") {
			r.terminate(termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: fmt.Sprintf("resolve branch already has open PR #%d; reporting the existing pull request to the source without adopting", pull.Number)})
			return exitSuccess, true
		}
	}
	for _, pull := range pulls {
		r.terminate(termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: fmt.Sprintf("resolve branch already has PR #%d (%s); refusing adoption", pull.Number, pull.State), Category: terminalCategoryUnknown})
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
// that state; the fail-closed kinds carry only the kind.
type runBranchPRObservation struct {
	kind  runBranchPRKind
	open  *prLinkageObservation
	draft *prLinkageObservation
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

// observeRunBranchPullRequest reads every pull request whose head carries
// the run's branch through the configured forge capability
// (forge.PullRequestHeadLister, never a forge-specific CLI) and classifies
// the set (#247). The provider compares the full base- and head-repository
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
	pulls, foreign, err := provider.ListPullRequestsWithForeign(ctx, forge.PullRequestRef{Repo: r.cfg.Repo}, r.cfg.Repo, r.cfg.Branch)
	if err != nil {
		return runBranchPRObservation{}, err
	}
	if len(foreign) > 0 {
		return runBranchPRObservation{kind: prStateForeignHead}, nil
	}
	var opens, drafts, wrongBases []*prLinkageObservation
	closed := 0
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
			closed++
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
	case closed > 0:
		return runBranchPRObservation{kind: prStateClosedOnly}, nil
	default:
		return runBranchPRObservation{kind: prStateMissing}, nil
	}
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
// shared #170 budget in turn. The forge read goes through the
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
func (r reporter) checkHandoff(ctx context.Context) (*handoffCheckOutcome, error) {
	source, ok := trustedSourceIssue(r.cfg)
	if !ok {
		// fix-pr runs use the broker's existing identity check;
		// resolve-issue with malformed identity is a
		// configuration error and not something the model can
		// repair.
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
		criteriaOutcome, critErr := r.checkCriterionEvidence(ctx, observation.open)
		if critErr != nil {
			return nil, critErr
		}
		if criteriaOutcome != nil {
			outcome.criteria = criteriaOutcome.criteria
			outcome.criteriaErr = criteriaOutcome.criteriaErr
			outcome.coverage = criteriaOutcome.coverage
			outcome.pinnedSet = criteriaOutcome.pinnedSet
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

// checkCriterionEvidence decodes the pinned criterion envelope from the
// run's COURIER_CRITERIA environment and validates the live PR body's
// evidence block against it (#249, #280). It is the second body contract
// the legacy executor applies to the open PR's body after authoritative
// linkage (#248): a head whose linkage already failed cannot pass a
// body check, but a head whose linkage passed but whose evidence block
// does not satisfy the pinned set still must be repaired before
// handoff. The envelope is operator-authored from the persisted
// publication policy at admission; the validator's comparison target is
// never model-controlled.
//
// Behavior:
//   - No envelope: vacuous pass; nothing to check, return nil.
//   - Empty pinned set (an issue without an acceptance section, or a
//     set with zero criteria): vacuous pass; linkage is the only PR-body
//     contract for that run. Return nil so the planner carries forward
//     as if no set were pinned.
//   - Decode failure of the envelope into a broker.CriterionSet: an
//     operator-side forge-read style failure. The same fail-closed
//     ordering the linkage check uses applies: return as an error so the
//     caller fails closed exactly like a linkage read failure; the
//     applyHandoffPlan helper emits EventLinkageReadFailed with
//     kind:"criteria" so an operator can tell which pre-handoff read
//     failed.
//   - Envelope naming a source issue other than the run's trusted
//     COURIER_REPO/COURIER_REF identity: a policy mismatch between two
//     operator-side inputs, returned as criteriaEnvelopeIdentityErr so
//     applyHandoffPlan fails closed to NeedsHuman immediately — the
//     model cannot repair a wrong pinned envelope by editing the PR
//     body, and no continuation is spent on it (#282). The binding is
//     checked before the pinned-empty shortcut, so a foreign envelope
//     cannot slip through as a vacuous pass.
//   - Body validation error: a *broker.CriteriaError is recorded on the
//     outcome (criteriaErr); a partial-success report is also kept on
//     the outcome (criteria) so the repair path can quote the rows
//     and per-status counts, and any non-CriteriaError is returned as
//     a plain error. The pinned set is always attached (pinnedSet) so
//     the repair path can name its digest and the verbatim criterion
//     texts the model must satisfy (#282).
//   - On success, populate outcome.criteria with the report and
//     outcome.coverage with its claimed coverage.
//
// checkCriterionEvidence is only called inside the prStateOpen branch of
// checkHandoff (#249): no PR — or a draft, ambiguous, closed-only,
// foreign, wrong-base, or unpublished head — has a body that the
// validator can compare against the pinned set.
// criteriaEnvelopeDecodeErr is the typed sentinel the criterion-envelope
// decoder returns when the operator-authored COURIER_CRITERIA envelope is
// not a valid CriterionSet (#280, #282). The pre-handoff plan emits the
// shared EventLinkageReadFailed event with kind:"criteria" for this
// class, distinct from the linkage observation read failure, so an
// operator can tell which pre-handoff read failed without parsing the
// error text.
var criteriaEnvelopeDecodeErr = errors.New("decode COURIER_CRITERIA envelope")

// criteriaEnvelopeIdentityErr is the typed sentinel the criterion-envelope
// decoder returns when the operator-authored COURIER_CRITERIA envelope
// names a source issue other than the run's trusted source identity
// (#282). It is a policy mismatch between two operator-side inputs —
// the admission projection and the run's pinned repo/ref — not a defect
// the model can repair by editing the PR body, so the pre-handoff plan
// fails closed to NeedsHuman on sight rather than spending a bounded
// continuation on it.
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

func (r reporter) checkCriterionEvidence(ctx context.Context, observation *prLinkageObservation) (*handoffCheckOutcome, error) {
	_ = ctx
	if strings.TrimSpace(r.cfg.Criteria) == "" {
		return nil, nil
	}
	var set broker.CriterionSet
	if err := json.Unmarshal([]byte(r.cfg.Criteria), &set); err != nil {
		return nil, fmt.Errorf("%w: %v", criteriaEnvelopeDecodeErr, err)
	}
	// Bind the envelope to the run's trusted source identity before the
	// set is consulted at all — including the pinned-empty case, where a
	// foreign envelope must not silently become this run's vacuous
	// pass (#282). The envelope's issue identity is operator-authored;
	// a disagreement with COURIER_REPO/COURIER_REF is a policy mismatch
	// to settle by hand, not a body defect to repair.
	source, ok := trustedSourceIssue(r.cfg)
	if ok && !criteriaSetMatchesSource(set, source) {
		envelopeTarget := "(no source issue in envelope)"
		if !set.Issue.IsZero() {
			envelopeTarget = sourceIssueTarget(set.Issue)
		}
		return nil, fmt.Errorf("%w: envelope names %s, the run's trusted source is %s",
			criteriaEnvelopeIdentityErr, envelopeTarget, sourceIssueTarget(source))
	}
	if set.IsEmpty() {
		return nil, nil
	}
	// Carry the pinned set on the outcome so the repair path can
	// quote its digest, IDs, and texts verbatim. The set is
	// operator-authored; passing it through the outcome is a
	// reference hand-off, not a copy that the model could rewrite.
	pinned := set
	report, err := broker.ValidateCriterionEvidence(observation.Body, set)
	if err != nil {
		var ce *broker.CriteriaError
		if errors.As(err, &ce) {
			coverage := ""
			if report != nil {
				coverage = report.Coverage
			}
			return &handoffCheckOutcome{
				pr:          observation,
				criteria:    report,
				criteriaErr: ce,
				coverage:    coverage,
				pinnedSet:   &pinned,
			}, nil
		}
		return nil, err
	}
	coverage := ""
	if report != nil {
		coverage = report.Coverage
	}
	return &handoffCheckOutcome{pr: observation, criteria: report, coverage: coverage, pinnedSet: &pinned}, nil
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
// message budget. The continuation number is named so a model can see
// the remaining budget.
func linkageRepairMessage(source broker.SourceIssue, outcome *handoffCheckOutcome, continuation int) string {
	target := sourceIssueTarget(source)
	bare := fmt.Sprintf("#%d", source.Number)
	cross := target
	return fmt.Sprintf(
		"Courier refused the handoff: PR #%d's body does not carry an authoritative closing reference to %s. "+
			"Update the PR body through the forge capability so it carries a supported closing keyword (Closes, Fixes, or Resolves) followed by %s, or the cross-repo form %s when the trusted source repository differs from the PR base. "+
			"Then re-declare the same outcome. (continuation %d)",
		outcome.pr.Number, target, bare, cross, continuation,
	)
}

// criteriaRepairMessage composes the precise OpenCode continuation
// message for a criterion-evidence violation (#282). It names the
// offending PR number, the trusted source issue, the pinned set digest
// and the verbatim pinned criteria texts so a model can verify what
// it must satisfy, and the continuation budget. The vocabulary is
// composed verbatim from the broker error's reason (which
// describeCriteriaFailure built) so any change to the validator's
// vocabulary surfaces in both paths. The required shape rules echo
// the documented header and per-status rules from criteria.go so the
// model can repair the body without re-deriving the contract.
func criteriaRepairMessage(source broker.SourceIssue, outcome *handoffCheckOutcome, continuation int) string {
	target := sourceIssueTarget(source)
	reason := describeCriteriaFailureForRepair(outcome.criteriaErr)
	setDigest, criteriaLines := pinnedSetRepairSnippet(outcome.pinnedSet, outcome.criteria)
	return fmt.Sprintf(
		"Courier refused the handoff: PR #%d's body does not satisfy the criterion-evidence contract pinned at admission for %s (pinned set %s): %s. "+
			"Update the PR body through the forge capability so it carries the criterion-evidence block: a single line starting with %q followed by %q, then one %q row per pinned criterion (each row's digest must match the pinned criterion digest below), each carrying a closed status from the vocabulary implementation-evidence, regression-check, manual-verification, not-implemented, or unknown, with regression-check and implementation-evidence referencing a file path or check URL and manual-verification and not-implemented referencing a note. Pinned criteria: %s. "+
			"Then re-declare the same outcome. (continuation %d)",
		outcome.pr.Number, target,
		nonEmptyOr(setDigest, "the admitted set"),
		reason,
		"courier-criteria:v1 source="+strings.ToLower(source.Canonical())+"#"+strconv.Itoa(source.Number),
		"set=sha256:<64hex> coverage=full|partial",
		"- AC-<n> sha256:<64hex>: <status> - <reference>",
		nonEmptyOr(criteriaLines, "(no pinned criteria on this run)"),
		continuation,
	)
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
func combinedBodyRepairMessage(source broker.SourceIssue, outcome *handoffCheckOutcome, continuation int) string {
	linkPart := linkageRepairMessage(source, outcome, continuation)
	critPart := criteriaRepairMessage(source, outcome, continuation)
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

// pinnedSetRepairSnippet renders the pinned criterion set as a short,
// model-quotable snippet the repair path pastes into the criteria
// continuation message (#282). The digest is the trust anchor — the
// model must satisfy the exact pinned set, not a re-derivation — and
// the per-criterion IDs, digests, and texts are the contract a body
// row references. Both are operator-authored, never model-controlled;
// the snippet is bounded to keep the legacy continuation message
// inside its byte budget. Returns (digest, snippet) — snippet is empty
// when the outcome carries no pinned set so callers can fall back.
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

// criteriaExhaustedReason composes the human-readable NeedsHuman reason
// for a run whose continuation budget ran out before the model repaired
// the criterion-evidence block (#282). The reason names the trusted
// source identity so the operator can settle the run without re-reading
// the issue, and the broker-defined reason the validator produced — or a
// stable fallback when the validator's structured error is missing.
// Always surfaces the trusted source issue via sourceIssueTarget(r.sourceIssue()).
func (r reporter) criteriaExhaustedReason(outcome *handoffCheckOutcome) string {
	target := sourceIssueTarget(r.sourceIssue())
	if outcome != nil && outcome.criteriaErr != nil && strings.TrimSpace(outcome.criteriaErr.Reason) != "" {
		return "incomplete handoff: criterion evidence " + outcome.criteriaErr.Reason + " (trusted source issue " + target + ")"
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
// parenthetical so it cannot disagree between the two halves.
func (r reporter) combinedBodyExhaustedReason(outcome *handoffCheckOutcome) string {
	target := sourceIssueTarget(r.sourceIssue())
	linkPart := "linkage: " + describeLinkageFailureForReason(outcome.err)
	critPart := "criterion evidence " + describeCriteriaFailureForRepair(outcome.criteriaErr)
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
// that claims the run branch's owner:head selector from a different
// repository (#247). The executor neither adopts such a pull request nor
// deletes it toward a "missing" claim that would prompt a creation over a
// world it cannot reconcile.
func (r reporter) foreignHeadPRReason() string {
	return fmt.Sprintf("the forge reported a pull request claiming the head selector %s:%s from a different repository, so the observation cannot settle this run's own pull-request state; the executor refuses to adopt a foreign head or open another pull request; the run needs a human", r.cfg.Repo, r.cfg.Branch)
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
//     "could not verify" message, and fails closed to NeedsHuman when
//     no retry is possible (#247, #248). A forged-side decode failure
//     of COURIER_CRITERIA is the same fail-closed ordering: the
//     linkage.read.failed event surfaces with kind:"criteria" so an
//     operator can distinguish the two forge-read failures; the
//     captured session retries inside the bounded budget and the run
//     fails closed when no retry is possible. An envelope that decodes
//     but names a source issue other than the run's trusted identity is
//     not a read failure and not a model-repairable defect: the event
//     surfaces with kind:"criteria-identity" and the run fails closed to
//     NeedsHuman on sight, without consuming the #170 budget (#282).
//
// The call site signals the budget consumed so far through continuations
// (so the helper consumes one slot when it asks for a continue); ctxErr
// is the run context's current error (non-nil when the run is shutting
// down) and sessionID gates the resume path.
func (r reporter) applyHandoffPlan(ctx context.Context, continuations int, maxContinuations int, sessionID string, ctxErr error) handoffVerdict {
	linkOutcome, linkErr := r.checkHandoff(ctx)
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
	if linkErr != nil {
		// One event, two remaining origins: the EventLinkageReadFailed
		// constant serves the linkage forge-read failure (PR listing or
		// remote publication confirmation) and the criterion envelope
		// decode failure (#282); the envelope identity mismatch was
		// already handled fail-closed above. The "kind" detail field is
		// the discriminator — "linkage" for the identity-checked PR
		// observation and "criteria" for the COURIER_CRITERIA envelope
		// decode — so an operator can tell which pre-handoff read
		// failed without parsing the body.
		kind := "linkage"
		if errors.Is(linkErr, criteriaEnvelopeDecodeErr) {
			kind = "criteria"
		}
		r.eventVerbose(courierlog.EventLinkageReadFailed, courierlog.StatusError, map[string]any{
			"kind":  kind,
			"error": linkErr.Error(),
		})
		if continuations >= maxContinuations || sessionID == "" || ctxErr != nil {
			return handoffVerdict{FailReason: r.linkageForgeFailureReason(linkErr)}
		}
		// The verifier could not read the forge state at all. That is
		// not a confirmed missing PR (#247), so the retry message says
		// what is known — the observation failed — rather than asserting
		// absence; the captured session re-verifies and repairs inside
		// the bounded budget.
		next := continuations + 1
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
	switch {
	case linkOutcome.err != nil && linkOutcome.criteriaErr != nil:
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
			ContinueMsg: combinedBodyRepairMessage(r.sourceIssue(), linkOutcome, continuations+1),
		}
	case linkOutcome.err != nil:
		if !resumable {
			return handoffVerdict{PRNumber: linkOutcome.pr.Number, FailReason: r.linkageExhaustedReason(linkOutcome)}
		}
		return handoffVerdict{Kind: "linkage-repair", ContinueMsg: linkageRepairMessage(r.sourceIssue(), linkOutcome, continuations+1), PRNumber: linkOutcome.pr.Number}
	case linkOutcome.criteriaErr != nil:
		if !resumable {
			return handoffVerdict{
				PRNumber:   linkOutcome.pr.Number,
				FailReason: r.criteriaExhaustedReason(linkOutcome),
			}
		}
		return handoffVerdict{
			Kind:        "criteria-repair",
			PRNumber:    linkOutcome.pr.Number,
			ContinueMsg: criteriaRepairMessage(r.sourceIssue(), linkOutcome, continuations+1),
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
