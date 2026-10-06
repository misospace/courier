package harness

import (
	"context"
	"errors"
	"sync"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// statusRetryRounds bounds retries of one trusted status write inside a
// single harness operation, and statusRetryDelay spaces them. They are
// availability retries for a broker that is momentarily unreachable, never a
// duration limit on work.
const (
	statusRetryRounds = 3
	statusRetryDelay  = 2 * time.Second
)

// defaultHeartbeatCadence bounds heartbeat status writes while successful
// activity is continuous (§7: coalescing to at most one patch per cadence).
const defaultHeartbeatCadence = 15 * time.Second

// StatusPatch is one trusted status request to the run's broker. The broker
// validates it against the authenticated control incarnation and applies it
// under resourceVersion CAS against a fresh read, so adding or clearing one
// active operation can never drop another entry or another incarnation's
// state.
type StatusPatch struct {
	Heartbeat  *courierv1alpha1.Heartbeat  `json:"heartbeat,omitempty"`
	Checkpoint *courierv1alpha1.Checkpoint `json:"checkpoint,omitempty"`
	LastCommit string                      `json:"lastCommit,omitempty"`

	AddOperation        *OperationAddition `json:"addOperation,omitempty"`
	ClearOperation      string             `json:"clearOperation,omitempty"`
	ReconcileOperations bool               `json:"reconcileOperations,omitempty"`
}

// OperationAddition carries the operation ID (the set's key) and its entry.
type OperationAddition struct {
	OpID      string                          `json:"opID"`
	Operation courierv1alpha1.ActiveOperation `json:"operation"`
}

// StatusTransport is the trusted status path to the run's broker. The broker
// alone holds the status-write identity; the harness only ever asks.
type StatusTransport interface {
	PostStatus(ctx context.Context, patch StatusPatch) error
}

// StatusReporter is the harness's earned-activity and operation-evidence
// writer (HARNESS.md §6). It is the ActivitySink: a stream chunk from a
// role-bound model session and a verified tool boundary are the only earned
// signals, coalesced to at most one patch per cadence — retries, raw output,
// dispatch, and cancellation never call it. It also owns the active-operation
// set: an entry is persisted before its task is dispatched and cleared only
// on independently observed termination, with the earned tool heartbeat
// written in the same update on completion.
type StatusReporter struct {
	transport  StatusTransport
	controlUID string
	workerUID  string
	now        func() time.Time
	cadence    time.Duration

	mu            sync.Mutex
	lastWritten   time.Time
	writing       bool
	pendingClears []string
	// statusDebt holds the newest failed CheckpointPublished patch: at least
	// one completed unit lacks its durable status record. It supersedes any
	// older debt (the ledger only grows), and only a successful write of the
	// current full checkpoint clears it.
	statusDebt *StatusPatch
}

// NewStatusReporter builds the reporter for one control incarnation. A
// non-positive cadence uses status.DefaultHeartbeatCadence.
func NewStatusReporter(transport StatusTransport, controlUID, workerUID string, cadence time.Duration) *StatusReporter {
	if cadence <= 0 {
		cadence = defaultHeartbeatCadence
	}
	return &StatusReporter{
		transport:  transport,
		controlUID: controlUID,
		workerUID:  workerUID,
		now:        time.Now,
		cadence:    cadence,
	}
}

// StreamChunk records an earned stream heartbeat.
func (r *StatusReporter) StreamChunk() {
	r.recordHeartbeat("stream")
}

// ToolBoundary records an earned tool heartbeat.
func (r *StatusReporter) ToolBoundary() {
	r.recordHeartbeat("tool")
}

// heartbeatWriteTimeout bounds one inline heartbeat attempt. The model
// stream path calls this sink synchronously, so a hung broker must never
// hold it: the attempt is dropped on timeout and retried by the next earned
// activity. It is a liveness bound on the write, not a work limit.
const heartbeatWriteTimeout = 10 * time.Second

func (r *StatusReporter) recordHeartbeat(kind string) {
	if r == nil || r.transport == nil {
		return
	}
	// The cadence decision is taken under the lock, but the write is not:
	// an in-flight marker coalesces concurrent earned signals while the
	// transport call runs lock-free, so a slow broker can never stall the
	// model stream path under the mutex. A failed write leaves lastWritten
	// untouched, so the next earned activity retries it.
	r.mu.Lock()
	now := r.now()
	if r.writing || (!r.lastWritten.IsZero() && now.Sub(r.lastWritten) < r.cadence) {
		r.mu.Unlock()
		return
	}
	r.writing = true
	patch := StatusPatch{Heartbeat: &courierv1alpha1.Heartbeat{
		At:                metav1.NewTime(now),
		Kind:              kind,
		CoordinatorPodUID: r.controlUID,
	}}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), heartbeatWriteTimeout)
	defer cancel()
	err := r.transport.PostStatus(ctx, patch)

	r.mu.Lock()
	r.writing = false
	if err == nil {
		r.lastWritten = now
	}
	r.mu.Unlock()
}

// ReconcileOperations drops every persisted active-operation entry at control
// process start. A fresh process holds no in-flight operation in memory, and
// the broker serves only the one live control incarnation, so any persisted
// entry refers to process memory that no longer exists. Persistent failure is
// not fatal: stale entries are ignored by the operator and by the validity
// rules, so hygiene is best-effort.
func (r *StatusReporter) ReconcileOperations(ctx context.Context) error {
	if r == nil || r.transport == nil {
		return nil
	}
	return r.retried(ctx, StatusPatch{ReconcileOperations: true})
}

// BeginOperation persists the active-operation entry before the task is
// dispatched. The write must be acknowledged before dispatch; a transport
// failure is retried because the idempotent re-add resolves an uncertain
// acknowledgment, and a persistent failure fails the dispatch — an operation
// without dispatch evidence must never run.
func (r *StatusReporter) BeginOperation(ctx context.Context, opID, briefID string) error {
	if r == nil || r.transport == nil {
		return nil
	}
	// Flush earlier clears opportunistically: a retained entry whose
	// operation has been verified terminated would otherwise suppress
	// stall reaping with nothing live behind it.
	r.flushPendingClears(ctx)
	now := r.now()
	patch := StatusPatch{AddOperation: &OperationAddition{
		OpID: opID,
		Operation: courierv1alpha1.ActiveOperation{
			BriefID:           briefID,
			CoordinatorPodUID: r.controlUID,
			WorkerPodUID:      r.workerUID,
			DispatchedAt:      metav1.NewTime(now),
		},
	}}
	return r.retried(ctx, patch)
}

// EndOperation clears one operation entry after independently observed
// termination. A verified completion also writes the earned tool heartbeat in
// the same update; a verified cancellation or death clears without a
// heartbeat. A failed clear retains the entry for opportunistic retry — it is
// never claimed cleared while it may still be executing, and a retained
// entry errs toward suppression, never toward reaping live work.
func (r *StatusReporter) EndOperation(ctx context.Context, opID string, completed bool) error {
	if r == nil || r.transport == nil {
		return nil
	}
	patch := StatusPatch{ClearOperation: opID}
	if completed {
		patch.Heartbeat = &courierv1alpha1.Heartbeat{
			At:                metav1.NewTime(r.now()),
			Kind:              "tool",
			CoordinatorPodUID: r.controlUID,
		}
	}
	err := r.retried(ctx, patch)
	if err == nil {
		if completed {
			r.mu.Lock()
			r.lastWritten = r.now()
			r.mu.Unlock()
		}
		return nil
	}
	// The entry is retained for the flush: it is never claimed cleared while
	// it may still be executing, and retention errs toward suppression, never
	// toward reaping live work. On a completed operation the heartbeat half
	// may still be earned through the coalescing sink on the next boundary.
	r.mu.Lock()
	r.pendingClears = append(r.pendingClears, opID)
	r.mu.Unlock()
	return err
}

// CheckpointPublished records the completed-brief checkpoint and — after a
// broker-confirmed remote publication — the confirmed OID as lastCommit. The
// caller does not acknowledge the unit complete until this write is durable.
// A failed write is retained as status debt: the run cannot reach a
// successful ending while any completed unit lacks its durable record,
// because finish reconciles the debt in trusted control before honoring any
// declared outcome (§7) — a model declaration is never the gate.
func (r *StatusReporter) CheckpointPublished(ctx context.Context, checkpoint *courierv1alpha1.Checkpoint, lastCommit string) error {
	if r == nil || r.transport == nil {
		return nil
	}
	patch := StatusPatch{Checkpoint: checkpoint, LastCommit: lastCommit}
	if err := r.retried(ctx, patch); err != nil {
		r.mu.Lock()
		r.statusDebt = &patch
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	r.statusDebt = nil
	r.mu.Unlock()
	return nil
}

// ReconcileDebt retries a retained checkpoint write. The broker's
// lastCommit validation re-checks the recorded OID against the live work
// ref on every attempt, so the retry is the revalidation, and rewriting the
// same checkpoint is idempotent. A persistent failure keeps the debt and
// reports an infrastructure failure — recovery is a relaunch that
// reconciles against the world, never a model-visible choice.
func (r *StatusReporter) ReconcileDebt(ctx context.Context) error {
	if r == nil || r.transport == nil {
		return nil
	}
	r.mu.Lock()
	debt := r.statusDebt
	r.mu.Unlock()
	if debt == nil {
		return nil
	}
	if err := r.retried(ctx, *debt); err != nil {
		return err
	}
	r.mu.Lock()
	r.statusDebt = nil
	r.mu.Unlock()
	return nil
}

// flushPendingClears retries retained clear operations, best-effort: a clear
// that fails again stays pending for the next opportunity.
func (r *StatusReporter) flushPendingClears(ctx context.Context) {
	if r == nil || r.transport == nil {
		return
	}
	r.mu.Lock()
	pending := r.pendingClears
	r.pendingClears = nil
	r.mu.Unlock()
	for _, opID := range pending {
		if err := r.transport.PostStatus(ctx, StatusPatch{ClearOperation: opID}); err != nil {
			r.mu.Lock()
			r.pendingClears = append(r.pendingClears, opID)
			r.mu.Unlock()
		}
	}
}

// Flush drops every pending clear and reports whether all succeeded. The
// coordinator calls it on its finish path so a run that ends cleanly does not
// leave suppression evidence behind.
func (r *StatusReporter) Flush(ctx context.Context) error {
	if r == nil || r.transport == nil {
		return nil
	}
	r.flushPendingClears(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pendingClears) > 0 {
		return errors.New("harness: active-operation clears remain unresolved")
	}
	return nil
}

func (r *StatusReporter) retried(ctx context.Context, patch StatusPatch) error {
	var err error
	for round := 0; ; round++ {
		err = r.transport.PostStatus(ctx, patch)
		if err == nil || !isBrokerRetryable(err) {
			return err
		}
		if round >= statusRetryRounds {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(statusRetryDelay):
		}
	}
}
