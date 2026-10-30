package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/misospace/courier/internal/protocol"
)

// maxTrackedReceipts bounds how many check receipts the registry retains.
// The bound is a memory bound on control-side bookkeeping, not a work limit:
// evicted keys simply re-dispatch on their next request.
const maxTrackedReceipts = 32

// checkReceipt is the tracked record of one completed check dispatch. The
// tails are exactly the bounded tails the worker returned — they are never
// truncated further and never grown here.
type checkReceipt struct {
	opID       string
	command    string
	tip        string
	envDigest  string
	status     string
	exitCode   int
	stdoutTail string
	stderrTail string
}

// reusable reports whether a repeat request at the same state key may be
// served from this receipt without re-executing. Only a verified completion
// is reusable: a cancelled or failed worker status stays recorded for
// observability, and a repeat request at its key is a miss that reruns. A
// completed receipt with a nonzero exit code is still reusable — an
// unchanged failing suite is reprinted, never rerun, and never presented as
// passing.
func (r *checkReceipt) reusable() bool {
	return r.status == protocol.ResultCompleted
}

// checkEntry holds one state key's tracked check: an in-flight dispatch
// signals completion by closing done, and the receipt lands just before the
// close. A nil receipt on a closed entry means the dispatch never produced a
// result.
type checkEntry struct {
	key     string
	receipt *checkReceipt
	done    chan struct{}
}

// checkRegistry tracks check receipts by state key. The registry is what
// makes concurrent runCheck calls safe: the first caller at a key installs
// the active entry before dispatching, so every other caller with the same
// key waits on its done channel instead of dispatching a second operation.
type checkRegistry struct {
	mu      sync.Mutex
	order   []string
	entries map[string]*checkEntry
}

func newCheckRegistry() *checkRegistry {
	return &checkRegistry{entries: make(map[string]*checkEntry)}
}

// claimOutcome classifies what a claim found at a state key.
type claimOutcome int

const (
	// claimReuse: a reusable terminal receipt exists at the key.
	claimReuse claimOutcome = iota
	// claimActive: a dispatch is in flight at the key; wait on its done.
	claimActive
	// claimFresh: no reusable receipt and no in-flight dispatch; the caller
	// owns the installed active entry and must dispatch.
	claimFresh
)

// claim classifies the key and, on a miss, installs a fresh active entry.
// A terminal entry whose receipt is not reusable (cancelled, worker-failed,
// or a failed launch with no receipt) is replaced: the new active entry
// takes the key, and any waiter still holding the old entry serves the old
// receipt.
func (r *checkRegistry) claim(key string) (*checkEntry, claimOutcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[key]; ok {
		select {
		case <-entry.done:
			if entry.receipt != nil && entry.receipt.reusable() {
				return entry, claimReuse
			}
		default:
			return entry, claimActive
		}
	}
	entry := &checkEntry{key: key, done: make(chan struct{})}
	r.install(key, entry)
	return entry, claimFresh
}

// complete records the entry's terminal outcome and closes its done channel,
// waking every waiter. A nil receipt records a failed launch. The registry
// is then bounded: when the map holds more entries than the receipt cap, the
// oldest terminal entries are evicted — never an active one, whose dispatch
// a waiter is still relying on.
func (r *checkRegistry) complete(entry *checkEntry, receipt *checkReceipt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry.receipt = receipt
	close(entry.done)
	for len(r.entries) > maxTrackedReceipts {
		evicted := false
		for _, key := range r.order {
			candidate := r.entries[key]
			if candidate == entry {
				continue
			}
			select {
			case <-candidate.done:
				delete(r.entries, key)
				r.dropOrder(key)
				evicted = true
			default:
			}
			if evicted {
				break
			}
		}
		if !evicted {
			break
		}
	}
}

// install puts the entry at the key, recency-ordered: a replaced key moves to
// the end of the ordering slice, so eviction always removes the least
// recently tracked terminal entry.
func (r *checkRegistry) install(key string, entry *checkEntry) {
	if _, ok := r.entries[key]; ok {
		r.dropOrder(key)
	}
	r.entries[key] = entry
	r.order = append(r.order, key)
}

func (r *checkRegistry) dropOrder(key string) {
	for i, candidate := range r.order {
		if candidate == key {
			r.order = append(r.order[:i], r.order[i+1:]...)
			return
		}
	}
}

// runCheck executes one tracked check on the sandbox worker. The check's
// identity is the normalized command together with the workspace state it ran
// against — the snapshot tip and the applied bootstrap environment — so an
// identical command against an unchanged state serves the saved receipt
// without a second dispatch, while any state change invalidates it. Like
// runShell this is coordinator-only, binds to the synthetic "control" work
// unit, and dispatches over the signed protocol onto the untrusted worker.
func (c *Coordinator) runCheck(ctx context.Context, call ToolCall, briefID string, result ToolResult) ToolResult {
	if c.worker == nil {
		result.IsError = true
		result.Content = "no worker is configured in this build"
		return result
	}
	if briefID == "" {
		briefID = "control"
	}
	var args struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil || strings.TrimSpace(args.Command) == "" {
		result.IsError = true
		result.Content = "check arguments must carry a command string"
		return result
	}
	tip, err := c.prepareSnapshot(ctx, briefID)
	if err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	command := normalizeCheckCommand(args.Command)
	env := c.currentBootstrapEnv()
	key := checkStateKey(command, tip, bootstrapEnvDigest(env))

	entry, outcome := c.checks.claim(key)
	switch outcome {
	case claimReuse:
		return renderCheckResult(entry.receipt, false, result)
	case claimActive:
		select {
		case <-entry.done:
		case <-ctx.Done():
			result.IsError = true
			result.Content = safeToolError(ctx.Err())
			return result
		}
		if entry.receipt == nil {
			// The winner's dispatch never produced a result; its own tool
			// result already carries the sanitized failure. Serve a fixed
			// error here rather than dispatching a second operation.
			result.IsError = true
			result.Content = "the tracked check failed to launch; no receipt was recorded"
			return result
		}
		return renderCheckResult(entry.receipt, false, result)
	}

	task := protocol.Task{Command: []string{"sh", "-c", args.Command}, Env: env}
	opID := newOpID("check", briefID)
	workerResult, err := c.dispatchAndAwait(ctx, opID, briefID, task)
	if err != nil {
		c.checks.complete(entry, nil)
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	receipt := &checkReceipt{
		opID:       opID,
		command:    command,
		tip:        tip,
		envDigest:  bootstrapEnvDigest(env),
		status:     workerResult.Status,
		exitCode:   workerResult.ExitCode,
		stdoutTail: workerResult.StdoutTail,
		stderrTail: workerResult.StderrTail,
	}
	c.checks.complete(entry, receipt)
	return renderCheckResult(receipt, true, result)
}

// renderCheckResult renders a receipt as the tool result. IsError follows the
// receipt: a nonzero exit code, or any terminal status other than a verified
// completion, is never presented as passing.
func renderCheckResult(receipt *checkReceipt, fresh bool, result ToolResult) ToolResult {
	var b strings.Builder
	verb := "reused"
	if fresh {
		verb = "fresh"
	}
	if receipt.exitCode != 0 || receipt.status != protocol.ResultCompleted {
		result.IsError = true
	}
	fmt.Fprintf(&b, "check receipt: %s op=%s exit=%d\n", verb, receipt.opID, receipt.exitCode)
	if receipt.status != protocol.ResultCompleted {
		fmt.Fprintf(&b, "status: %s\n", receipt.status)
	}
	if receipt.stdoutTail != "" {
		b.WriteString("stdout:\n" + receipt.stdoutTail + "\n")
	}
	if receipt.stderrTail != "" {
		b.WriteString("stderr:\n" + receipt.stderrTail + "\n")
	}
	result.Content = b.String()
	return result
}

// normalizeCheckCommand collapses whitespace runs so commands that differ
// only in formatting share one receipt: the model's intent, not its spacing,
// is what the receipt tracks.
func normalizeCheckCommand(command string) string {
	return strings.Join(strings.Fields(command), " ")
}

// bootstrapEnvDigest digests the applied bootstrap environment delta in a
// canonical (sorted, NUL-joined) form, so a delta that changes in content or
// order invalidates every tracked receipt.
func bootstrapEnvDigest(env []string) string {
	sorted := append([]string(nil), env...)
	sort.Strings(sorted)
	return digestOf(strings.Join(sorted, "\x00"))
}

// checkStateKey digests the check's full identity: the normalized command and
// the workspace state it ran against.
func checkStateKey(command, tip, envDigest string) string {
	return digestOf(command + "\x00" + tip + "\x00" + envDigest)
}

// digestOf is the canonical SHA-256 hex digest for registry keys.
func digestOf(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
