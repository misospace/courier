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
// served from this receipt without re-executing. Only a completed run is
// reusable (completedRun): a cancelled dispatch and a run that never
// produced an exit status stay recorded for observability, and a repeat
// request at their key is a miss that reruns. A completed run with a
// nonzero exit code is reusable — an unchanged failing suite is reprinted,
// never rerun, and never presented as passing.
func (r *checkReceipt) reusable() bool {
	return completedRun(r.status, r.exitCode)
}

// completedRun reports whether a worker result records an invocation that
// ran to completion and produced its own exit status. The worker reports a
// nonzero exit as ResultFailed (protocol runTask), so a failing exit is a
// completed run and real evidence, not a dispatch failure. ResultFailed
// carrying the sentinel exit code -1 means the task never started or died
// to a signal: the run produced no exit status, so it is not evidence.
// Cancellation is never evidence.
func completedRun(status string, exitCode int) bool {
	switch status {
	case protocol.ResultCompleted:
		return true
	case protocol.ResultFailed:
		return exitCode > 0
	default:
		return false
	}
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
	// waitHook, when non-nil, receives one value when a claim returns
	// claimActive — the caller is committed to waiting on that entry's
	// done channel. Test-only observability of the in-flight-sharing
	// window; the send never blocks, so a test that stops receiving
	// cannot deadlock the registry's callers.
	waitHook chan string
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
// receipt. A forced claim (an explicit invalidation of the stored receipt)
// treats a terminal entry the same way — a receipt the caller doubted is
// replaced even when it was reusable, and if the forced dispatch then fails
// to launch, no receipt remains and the next request reruns, exactly like
// missing evidence. An in-flight dispatch is still waited on, so a duplicate
// launch never happens.
func (r *checkRegistry) claim(key string, force bool) (*checkEntry, claimOutcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[key]; ok {
		select {
		case <-entry.done:
			if !force && entry.receipt != nil && entry.receipt.reusable() {
				return entry, claimReuse
			}
		default:
			if r.waitHook != nil {
				select {
				case r.waitHook <- key:
				default:
				}
			}
			return entry, claimActive
		}
	}
	entry := &checkEntry{key: key, done: make(chan struct{})}
	r.install(key, entry)
	return entry, claimFresh
}

// invalidate drops the terminal entry at the key, if any, so the next claim
// dispatches fresh. It is the concrete-concern invalidation path: fresher
// evidence exists or the stored receipt is no longer trusted. An in-flight
// entry is left alone — its dispatch is real evidence in progress, and
// dropping it would strand its waiters.
func (r *checkRegistry) invalidate(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[key]
	if !ok {
		return
	}
	select {
	case <-entry.done:
		delete(r.entries, key)
		r.dropOrder(key)
	default:
	}
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
// identity is the exact command bytes together with the workspace state it
// ran against — the snapshot tip and the applied bootstrap environment — so
// an identical command against an unchanged state serves the saved receipt
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
		Refresh bool   `json:"refresh"`
	}
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil || strings.TrimSpace(args.Command) == "" {
		result.IsError = true
		result.Content = "check arguments must carry a command string"
		return result
	}
	if strings.ContainsRune(args.Command, '\x00') {
		// An embedded NUL byte cannot survive the exec boundary: the
		// argument would reach sh truncated at the NUL, so the program
		// executed would not be the one this identity records.
		result.IsError = true
		result.Content = "a check command must not contain a NUL byte"
		return result
	}
	tip, err := c.prepareSnapshot(ctx, briefID)
	if err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	// The identity is the exact command bytes because the task runs as
	// `sh -c <command>`: no tokenization can witness what sh will execute,
	// and a quote-resolved form aliases distinct programs — `echo "$HOME"`
	// and `echo '$HOME'` tokenize alike but expand differently. Exact
	// bytes plus state is the complete identity; any future canonical form
	// must prove semantic equivalence, not merely collapse formatting.
	command := args.Command
	env := c.currentBootstrapEnv()
	key := checkStateKey(command, tip, bootstrapEnvDigest(env))

	// A forced request is the concrete-concern invalidation path (#635): the
	// caller doubts the stored receipt, so it is ignored and replaced by a
	// fresh dispatch. An in-flight dispatch is still shared — a concern
	// never justifies a duplicate launch.
	entry, outcome := c.checks.claim(key, args.Refresh)
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

// bootstrapEnvDigest digests the applied bootstrap environment delta in a
// canonical (sorted, NUL-joined) form, so a delta that changes in content or
// order invalidates every tracked receipt.
func bootstrapEnvDigest(env []string) string {
	sorted := append([]string(nil), env...)
	sort.Strings(sorted)
	return digestOf(strings.Join(sorted, "\x00"))
}

// checkStateKey digests the check's full identity: the exact command bytes
// and the workspace state it ran against. Commands are keyed verbatim
// because the worker executes them through `sh -c`: only identical bytes
// guarantee an identical program, so no two spellings ever share a receipt.
// The NUL join is collision-free as written because tip and envDigest are
// hex digests that cannot carry one; a command carrying a NUL is rejected
// before the key is built.
//
// The workspace half of the identity is sound because every check dispatch
// first prepares a fresh snapshot: control re-renders the bundle from the
// integration head and the worker's unpack re-materializes the workspace at
// that tip (reset --hard plus clean -fdx) before anything runs. The
// workspace a check runs against is therefore a pure function of the tip —
// a worktree change that does not change the tip cannot exist on this path,
// because worker-side edits never survive the next unpack, and every
// relevant change reaches the workspace through artifact integration, which
// advances the tip. The digest also covers the applied bootstrap environment
// delta, the declared dependency/config/environment scope.
//
// The dispatch carries no working directory and the worker runs every task in
// its fixed sanitized workspace root (protocol.WorkerConfig.WorkspaceDir), so
// cwd is a per-worker constant and the command-plus-state identity is
// complete as stands. If a cwd surface is ever added to the check tool or to
// protocol.Task, it must become part of this key — otherwise the same command
// string from a different directory would reuse a stale receipt. The same
// applies to any future surface that mutates undeclared worker-side state
// outside the workspace root: such state is worker-ephemeral, never part of
// the dispatched snapshot, and a receipt identity cannot vouch for it.
func checkStateKey(command, tip, envDigest string) string {
	return digestOf(command + "\x00" + tip + "\x00" + envDigest)
}

// digestOf is the canonical SHA-256 hex digest for registry keys.
func digestOf(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
