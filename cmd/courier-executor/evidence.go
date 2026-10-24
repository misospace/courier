package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/misospace/courier/internal/evidence"
	"github.com/misospace/courier/internal/git"
	courierlog "github.com/misospace/courier/internal/log"
)

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
	return childReaper().run(cmd)
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
	if err := childReaper().run(cmd); err != nil {
		return 0, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(stdout.String()))
	if err != nil {
		return 0, err
	}
	return count, nil
}

// revParse resolves one rev to its full object name.
func revParse(ctx context.Context, dir, rev string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", rev)
	// Same WaitDelay bound as every spawned git call: a remote helper or
	// askpass grandchild holding the pipe must not outlive the context.
	cmd.WaitDelay = 2 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := childReaper().run(cmd); err != nil {
		return "", err
	}
	return strings.TrimSpace(stdout.String()), nil
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
