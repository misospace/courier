package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/executor"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// recordingStatusTransport records every patch and serves a scripted error
// queue: each PostStatus pops the next error or succeeds when the queue is
// empty.
type recordingStatusTransport struct {
	mu      sync.Mutex
	patches []StatusPatch
	errs    []error
}

func (r *recordingStatusTransport) PostStatus(_ context.Context, patch StatusPatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.patches = append(r.patches, patch)
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		return err
	}
	return nil
}

func (r *recordingStatusTransport) sent() []StatusPatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]StatusPatch(nil), r.patches...)
}

func brokerUnavailable() error {
	return &BrokerCallError{Category: "is temporarily unavailable", Retryable: true}
}

func brokerDenied() error {
	return &BrokerCallError{Category: "denied the operation"}
}

const (
	testControlUID = "control-pod-uid"
	testWorkerUID  = "worker-pod-uid"
)

func testReporter(transport *recordingStatusTransport) *StatusReporter {
	return NewStatusReporter(transport, testControlUID, testWorkerUID, 0)
}

func TestStatusReporterCoalescesEarnedHeartbeats(t *testing.T) {
	transport := &recordingStatusTransport{}
	reporter := testReporter(transport)
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	now := base
	reporter.now = func() time.Time { return now }

	reporter.StreamChunk()
	patches := transport.sent()
	if len(patches) != 1 || patches[0].Heartbeat == nil {
		t.Fatalf("first earned activity must write immediately: %#v", patches)
	}
	if patches[0].Heartbeat.Kind != "stream" || patches[0].Heartbeat.CoordinatorPodUID != testControlUID {
		t.Fatalf("heartbeat = %#v, want a UID-fenced stream heartbeat", patches[0].Heartbeat)
	}

	// Continuous activity inside the cadence coalesces: no second patch.
	now = base.Add(5 * time.Second)
	reporter.ToolBoundary()
	if got := len(transport.sent()); got != 1 {
		t.Fatalf("patches = %d, want 1; activity inside the cadence must coalesce", got)
	}

	// After the cadence the newest earned kind is written.
	now = base.Add(defaultHeartbeatCadence + time.Second)
	reporter.ToolBoundary()
	patches = transport.sent()
	if len(patches) != 2 || patches[1].Heartbeat == nil || patches[1].Heartbeat.Kind != "tool" {
		t.Fatalf("patches = %#v, want the newest earned kind after the cadence", patches)
	}
}

func TestStatusReporterFailedHeartbeatRetriesOnNextActivity(t *testing.T) {
	transport := &recordingStatusTransport{errs: []error{brokerUnavailable()}}
	reporter := testReporter(transport)

	reporter.StreamChunk()
	if got := len(transport.sent()); got != 1 {
		t.Fatalf("patches = %d, want the failed attempt recorded", got)
	}
	// The failed write is not counted: the next earned activity retries it,
	// carrying the newest kind.
	reporter.ToolBoundary()
	patches := transport.sent()
	if len(patches) != 2 || patches[1].Heartbeat == nil || patches[1].Heartbeat.Kind != "tool" {
		t.Fatalf("patches = %#v, want the retried heartbeat with the newest kind", patches)
	}
}

func TestStatusReporterBeginOperationIsUIDFencedAndRetried(t *testing.T) {
	transport := &recordingStatusTransport{errs: []error{brokerUnavailable(), brokerUnavailable()}}
	reporter := testReporter(transport)

	if err := reporter.BeginOperation(context.Background(), "shell.b1.aaaa", "b1"); err != nil {
		t.Fatalf("BeginOperation() error = %v; retryable broker failures must be retried", err)
	}
	patches := transport.sent()
	if len(patches) != 3 {
		t.Fatalf("patches = %d, want three attempts (two retries)", len(patches))
	}
	add := patches[len(patches)-1].AddOperation
	if add == nil || add.OpID != "shell.b1.aaaa" {
		t.Fatalf("last patch = %#v, want the retried operation addition", patches[len(patches)-1])
	}
	if add.Operation.CoordinatorPodUID != testControlUID || add.Operation.WorkerPodUID != testWorkerUID {
		t.Fatalf("operation = %#v, want it attributed to this incarnation and its worker", add.Operation)
	}
	if add.Operation.BriefID != "b1" || add.Operation.DispatchedAt.IsZero() {
		t.Fatalf("operation = %#v, want the brief ID and dispatch time", add.Operation)
	}
}

func TestStatusReporterBeginOperationFailsClosedOnDenial(t *testing.T) {
	transport := &recordingStatusTransport{errs: []error{brokerDenied()}}
	reporter := testReporter(transport)

	if err := reporter.BeginOperation(context.Background(), "shell.b1.aaaa", "b1"); err == nil {
		t.Fatal("BeginOperation() error = nil, want a definite denial to fail the dispatch immediately")
	}
}

func TestStatusReporterEndOperationWritesHeartbeatOnlyOnCompletion(t *testing.T) {
	transport := &recordingStatusTransport{}
	reporter := testReporter(transport)

	if err := reporter.EndOperation(context.Background(), "shell.b1.aaaa", true); err != nil {
		t.Fatal(err)
	}
	patches := transport.sent()
	if len(patches) != 1 {
		t.Fatalf("patches = %d, want one update", len(patches))
	}
	if patches[0].ClearOperation != "shell.b1.aaaa" {
		t.Fatalf("patch = %#v, want the entry cleared", patches[0])
	}
	if patches[0].Heartbeat == nil || patches[0].Heartbeat.Kind != "tool" || patches[0].Heartbeat.CoordinatorPodUID != testControlUID {
		t.Fatalf("patch = %#v, want the earned tool heartbeat in the same update", patches[0])
	}

	if err := reporter.EndOperation(context.Background(), "shell.b2.bbbb", false); err != nil {
		t.Fatal(err)
	}
	patches = transport.sent()
	if patches[1].Heartbeat != nil || patches[1].ClearOperation != "shell.b2.bbbb" {
		t.Fatalf("patch = %#v, want a clear without a heartbeat for a cancelled operation", patches[1])
	}
}

func TestStatusReporterRetainsFailedClearUntilFlush(t *testing.T) {
	// Four transient failures exhaust the retry rounds: the entry is
	// retained, never claimed cleared, and a later flush retries the clear.
	transport := &recordingStatusTransport{errs: []error{
		brokerUnavailable(), brokerUnavailable(), brokerUnavailable(), brokerUnavailable(), nil,
	}}
	reporter := testReporter(transport)

	if err := reporter.EndOperation(context.Background(), "shell.b1.aaaa", true); err == nil {
		t.Fatal("EndOperation() error = nil, want exhausted retries to report the retained entry")
	}
	// The entry is retained, never claimed cleared, and a later flush
	// retries the clear.
	if err := reporter.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v, want the retained clear to succeed", err)
	}
	patches := transport.sent()
	last := patches[len(patches)-1]
	if last.ClearOperation != "shell.b1.aaaa" || last.Heartbeat != nil {
		t.Fatalf("flush patch = %#v, want a clear-only retry", last)
	}
	if err := reporter.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush() error = %v, want no retained entries", err)
	}
}

func TestStatusReporterReconcileAndCheckpointPatches(t *testing.T) {
	transport := &recordingStatusTransport{}
	reporter := testReporter(transport)

	if err := reporter.ReconcileOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !transport.sent()[0].ReconcileOperations {
		t.Fatalf("patch = %#v, want the reconcile request", transport.sent()[0])
	}
	checkpoint := &courierv1alpha1.Checkpoint{CompletedBriefs: []courierv1alpha1.Brief{{ID: "b1", Summary: "done", Commit: "abc"}}}
	if err := reporter.CheckpointPublished(context.Background(), checkpoint, "abc"); err != nil {
		t.Fatal(err)
	}
	patch := transport.sent()[1]
	if patch.Checkpoint == nil || patch.LastCommit != "abc" {
		t.Fatalf("patch = %#v, want the checkpoint and the confirmed OID", patch)
	}
}

// TestStatusPatchWireContract pins the harness's status wire format to the
// broker's trusted status decoder: the broker decodes with unknown fields
// disallowed, so any drift between the two sides fails here instead of
// producing a silent 400 at runtime.
func TestStatusPatchWireContract(t *testing.T) {
	patch := StatusPatch{
		Heartbeat:  &courierv1alpha1.Heartbeat{At: metav1Now(), Kind: "tool", CoordinatorPodUID: testControlUID},
		Checkpoint: &courierv1alpha1.Checkpoint{Plan: "plan"},
		LastCommit: "abc",
		AddOperation: &OperationAddition{
			OpID: "shell.b1.aaaa",
			Operation: courierv1alpha1.ActiveOperation{
				BriefID:           "b1",
				CoordinatorPodUID: testControlUID,
				WorkerPodUID:      testWorkerUID,
				DispatchedAt:      metav1Now(),
			},
		},
		ClearOperation:      "shell.b2.bbbb",
		ReconcileOperations: true,
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	var decoded broker.HarnessPatch
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err != nil {
		t.Fatalf("broker cannot decode the harness status patch: %v\npayload: %s", err, raw)
	}
	if decoded.Heartbeat == nil || decoded.Heartbeat.CoordinatorPodUID != testControlUID || decoded.Heartbeat.Kind != "tool" {
		t.Fatalf("decoded heartbeat = %#v", decoded.Heartbeat)
	}
	if decoded.AddOperation == nil || decoded.AddOperation.OpID != "shell.b1.aaaa" || decoded.AddOperation.Operation.WorkerPodUID != testWorkerUID {
		t.Fatalf("decoded operation = %#v", decoded.AddOperation)
	}
	if decoded.ClearOperation != "shell.b2.bbbb" || !decoded.ReconcileOperations {
		t.Fatalf("decoded clear/reconcile = %q/%t", decoded.ClearOperation, decoded.ReconcileOperations)
	}
	if decoded.Checkpoint == nil || decoded.Checkpoint.Plan != "plan" || decoded.LastCommit == nil || *decoded.LastCommit != "abc" {
		t.Fatalf("decoded checkpoint/lastCommit = %#v", decoded.Checkpoint)
	}
}

func metav1Now() metav1.Time {
	return metav1.NewTime(time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC))
}

// TestStatusPersistenceFailurePreventsDispatch replays §6's ordering rule: a
// broker that will not acknowledge the active-operation entry means the task
// is never dispatched — the worker sees nothing, and the failure is the
// tool-visible result.
func TestStatusPersistenceFailurePreventsDispatch(t *testing.T) {
	delegateOK := gatewayResponse{body: sse(
		contentChunk("delegating"),
		toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		finishChunk("tool_calls"),
	)}
	gateway, _, cleanup := gatewayFor(
		delegateOK,
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"status path refused the operation"}`), finishChunk("stop"))},
	)
	defer cleanup()
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	transport := &recordingStatusTransport{errs: []error{brokerDenied()}}
	reporter := NewStatusReporter(transport, testControlUID, testWorkerUID, 0)
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: &mockPublisher{},
		Status:    reporter,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	if got := worker.dispatchRequests; got != 0 {
		t.Fatalf("worker dispatch requests = %d, want 0; an operation without persisted dispatch evidence must never be sent", got)
	}
	if len(transport.sent()) == 0 || transport.sent()[0].AddOperation == nil {
		t.Fatalf("patches = %#v, want the attempted operation addition first", transport.sent())
	}
}

// TestCompletedBriefCheckpointRecordsConfirmedOID replays §7's cadence: a
// completed brief is integrated and published, then the completed-brief
// checkpoint and the broker-confirmed OID are recorded through the trusted
// status path in one durable write before the unit is acknowledged.
func TestCompletedBriefCheckpointRecordsConfirmedOID(t *testing.T) {
	delegateOK := gatewayResponse{body: sse(
		contentChunk("delegating"),
		toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		finishChunk("tool_calls"),
	)}
	gateway, _, cleanup := gatewayFor(
		delegateOK,
		// The brief executes one shell task and finishes.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo work"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk("work done"), finishChunk("stop"))},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	defer cleanup()
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.packArtifact = []byte("bundle-bytes")
	publisher := &mockPublisher{}
	transport := &recordingStatusTransport{}
	reporter := NewStatusReporter(transport, testControlUID, testWorkerUID, 0)
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: publisher,
		Status:    reporter,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeChanges || result.Err != nil {
		t.Fatalf("result = %+v, want a changes outcome", result)
	}
	// Every dispatched operation earned its entry: one addition before the
	// dispatch and one clear after verified termination, for each of the
	// unpack, shell, and pack tasks.
	patches := transport.sent()
	adds, clears := 0, 0
	for _, patch := range patches {
		if patch.AddOperation != nil {
			adds++
		}
		if patch.ClearOperation != "" {
			clears++
		}
	}
	if adds != 3 || clears != 3 {
		t.Fatalf("operation patches: adds = %d, clears = %d, want 3/3 for unpack, shell, pack", adds, clears)
	}
	// The checkpoint write carries the completed brief and the confirmed
	// remote OID as lastCommit.
	var checkpoint *StatusPatch
	for i := range patches {
		if patches[i].Checkpoint != nil {
			checkpoint = &patches[i]
			break
		}
	}
	if checkpoint == nil {
		t.Fatalf("patches = %#v, want a durable checkpoint write", patches)
	}
	if len(checkpoint.Checkpoint.CompletedBriefs) != 1 || checkpoint.Checkpoint.CompletedBriefs[0].ID != "b1" {
		t.Fatalf("checkpoint = %#v, want the completed brief recorded", checkpoint.Checkpoint)
	}
	if checkpoint.Checkpoint.CompletedBriefs[0].Commit != "commit-1" {
		t.Fatalf("brief commit = %q, want the integration commit the broker confirmed", checkpoint.Checkpoint.CompletedBriefs[0].Commit)
	}
	if checkpoint.LastCommit != "commit-1" {
		t.Fatalf("lastCommit = %q, want the confirmed published OID", checkpoint.LastCommit)
	}
	// The brief was acknowledged only after the durable write: the tool
	// result the model saw carries the integration outcome.
	if got, ok := coordinator.Briefs().Result("b1"); !ok || got.Commit != "commit-1" {
		t.Fatalf("brief result = %#v, want the recorded integration commit", got)
	}
}

// TestAmbiguousDispatchClearsEntryAfterVerifiedNonExecution replays §6
// step 3's canonical transient: the dispatch transport blips, control
// reconciles through cancellation, and the worker definitively reports the
// operation never ran. The entry must be cleared — an observation of
// non-execution is verified termination, and retaining it would leave
// suppression evidence with nothing live behind it. Redelivery never
// happens.
func TestAmbiguousDispatchClearsEntryAfterVerifiedNonExecution(t *testing.T) {
	gateway, _, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo work"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"worker"}`), finishChunk("stop"))},
	)
	defer cleanup()
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// The dispatch transport blips on the shell task (the snapshot unpack
	// dispatch is scripted healthy): the worker may or may not have accepted.
	worker.dispatchPlan = []int{0, http.StatusServiceUnavailable}
	worker.snapshotTip = "snap-tip"
	transport := &recordingStatusTransport{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: &mockPublisher{},
		Status:    NewStatusReporter(transport, testControlUID, testWorkerUID, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.Run(context.Background(), testInvocation())

	// The worker saw exactly one dispatch attempt for the shell task — the
	// snapshot unpack plus that single attempt, never a redelivery;
	// reconciliation went through cancellation.
	if got := worker.dispatchRequests; got != 2 {
		t.Fatalf("worker dispatch requests = %d, want 2 (unpack and one shell attempt, never redelivered)", got)
	}
	var add, clear *StatusPatch
	for i := range transport.patches {
		patch := &transport.patches[i]
		// Coordinator-level shell operations bind brief ID "control", so
		// their opIDs are "shell.control.<nonce>".
		if patch.AddOperation != nil && strings.HasPrefix(patch.AddOperation.OpID, "shell.control.") {
			add = patch
		}
		if patch.ClearOperation != "" && strings.HasPrefix(patch.ClearOperation, "shell.control.") {
			clear = patch
		}
	}
	if add == nil || clear == nil {
		t.Fatalf("patches = %#v, want the entry persisted before dispatch and cleared after verified non-execution", transport.patches)
	}
	if clear.Heartbeat != nil {
		t.Fatalf("clear patch = %#v, want no success heartbeat for a cancelled operation", clear)
	}
}

// TestFailedModelStreamNeverRecordsHeartbeat pins the earned-activity
// boundary at the reporter seam: a failed model stream is active, not alive,
// and must never reach the status path as a heartbeat.
func TestFailedModelStreamNeverRecordsHeartbeat(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{status: http.StatusForbidden})
	defer cleanup()
	transport := &recordingStatusTransport{}
	reporter := NewStatusReporter(transport, testControlUID, testWorkerUID, 0)
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, reporter)
	if err != nil {
		t.Fatal(err)
	}
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want a stream error", last)
	}
	for _, patch := range transport.patches {
		if patch.Heartbeat != nil {
			t.Fatalf("patches = %#v, want no heartbeat from a failed stream", transport.patches)
		}
	}
}

// TestHeartbeatWriteNeverBlocksTheStreamPath pins the bot-review minor: the
// cadence decision is atomic, but the transport write runs lock-free and
// bounded, so a slow broker can never hold the model stream path under the
// reporter's mutex, and concurrent earned signals coalesce onto the
// in-flight write.
func TestHeartbeatWriteNeverBlocksTheStreamPath(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	transport := &blockingStatusTransport{started: started, release: release}
	reporter := NewStatusReporter(transport, testControlUID, testWorkerUID, 0)

	go reporter.StreamChunk()
	<-started // the write is in flight

	// A concurrent earned signal during the in-flight write coalesces and
	// returns without waiting on the transport.
	done := make(chan struct{})
	go func() {
		reporter.ToolBoundary()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the concurrent earned signal blocked on the in-flight write; the stream path must never wait under the reporter's lock")
	}

	close(release)
	// Wait for the in-flight write's bookkeeping to settle, then the next
	// earned signal past the cadence writes again.
	deadline := time.Now().Add(time.Second)
	for {
		reporter.mu.Lock()
		settled := !reporter.writing
		reporter.mu.Unlock()
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the in-flight write never settled")
		}
		time.Sleep(time.Millisecond)
	}
	reporter.mu.Lock()
	reporter.lastWritten = time.Time{}
	reporter.mu.Unlock()
	reporter.StreamChunk()
	if got := len(transport.patches); got != 2 {
		t.Fatalf("patches = %d, want 2 (the in-flight write and the post-cadence retry)", got)
	}
}

// blockingStatusTransport blocks the first PostStatus until released.
type blockingStatusTransport struct {
	started   chan struct{}
	release   chan struct{}
	patches   []StatusPatch
	mu        sync.Mutex
	unblocked bool
}

func (b *blockingStatusTransport) PostStatus(_ context.Context, patch StatusPatch) error {
	b.mu.Lock()
	unblocked := b.unblocked
	b.patches = append(b.patches, patch)
	b.mu.Unlock()
	if !unblocked {
		close(b.started)
		<-b.release
		b.mu.Lock()
		b.unblocked = true
		b.mu.Unlock()
	}
	return nil
}

// TestFailedCheckpointWriteBlocksSuccessfulChangesEnding pins the §7
// terminal boundary mechanically: a checkpoint/lastCommit write that failed
// through its retry budget leaves status debt, the tool result is an error,
// and when the coordinator ignores that error and declares changes anyway,
// finish MUST NOT return a successful OutcomeChanges — the debt is
// reconciled in trusted control, and a persistent failure is an
// infrastructure failure for the operator to relaunch. A model declaration
// can never turn an unpersisted completed unit into a successful ending.
func TestFailedCheckpointWriteBlocksSuccessfulChangesEnding(t *testing.T) {
	gateway, _, cleanup := gatewayFor(
		delegateChangesScript()...,
	)
	defer cleanup()
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.packArtifact = []byte("bundle-bytes")
	publisher := &mockPublisher{}
	// The checkpoint write after the completed brief is denied, and the
	// finish-path reconciliation is denied too: the debt cannot land. The
	// six nils let the unpack/shell/pack operation adds and clears succeed.
	transport := &recordingStatusTransport{errs: []error{
		nil, nil, nil, nil, nil, nil, brokerDenied(), brokerDenied(),
	}}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: publisher,
		Status:    NewStatusReporter(transport, testControlUID, testWorkerUID, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err == nil {
		t.Fatalf("result = %+v, want an infrastructure failure; a model declaration can never turn an unpersisted completed unit into a successful ending", result)
	}
	if result.Outcome == executor.OutcomeChanges && result.Err == nil {
		t.Fatal("a successful changes ending escaped the debt gate")
	}
	// The gate runs before the idempotent confirm: the ending never reached
	// the publisher again after the brief's own publication.
	if got := publisher.count(); got != 1 {
		t.Fatalf("publications = %d, want 1 (the brief's own); the gated ending must not publish", got)
	}
	// The checkpoint write was attempted exactly twice: the brief's failed
	// write and the finish-path reconciliation.
	attempts := 0
	for _, patch := range transport.patches {
		if patch.Checkpoint != nil {
			attempts++
		}
	}
	if attempts != 2 {
		t.Fatalf("checkpoint attempts = %d, want 2 (the failed write and the reconciliation)", attempts)
	}
}

// TestRecoveredCheckpointDebtLetsChangesEndingLand pins the recovery half:
// after the status transport recovers, the retained debt is reconciled
// idempotently — the broker revalidates the recorded OID against the live
// work ref — and only then does the changes ending succeed.
func TestRecoveredCheckpointDebtLetsChangesEndingLand(t *testing.T) {
	gateway, _, cleanup := gatewayFor(
		delegateChangesScript()...,
	)
	defer cleanup()
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.packArtifact = []byte("bundle-bytes")
	publisher := &mockPublisher{}
	// The checkpoint write fails once; the finish-path reconciliation
	// succeeds now that the transport recovered. The six nils let the
	// unpack/shell/pack operation adds and clears succeed.
	transport := &recordingStatusTransport{errs: []error{
		nil, nil, nil, nil, nil, nil, brokerDenied(), nil,
	}}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: publisher,
		Status:    NewStatusReporter(transport, testControlUID, testWorkerUID, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err != nil || result.Outcome != executor.OutcomeChanges {
		t.Fatalf("result = %+v, want a successful changes ending after the debt reconciled", result)
	}
	// The reconciled write carried the completed brief and the confirmed
	// published OID as lastCommit.
	var reconciled *StatusPatch
	for i := range transport.patches {
		if transport.patches[i].Checkpoint != nil {
			reconciled = &transport.patches[i]
		}
	}
	if reconciled == nil || reconciled.LastCommit != "commit-1" {
		t.Fatalf("reconciled patch = %#v, want the checkpoint with the confirmed OID", reconciled)
	}
}

// delegateChangesScript is the four-turn flow: delegate a brief, the brief
// runs one shell task and finishes, and the coordinator declares changes.
func delegateChangesScript() []gatewayResponse {
	return []gatewayResponse{
		{body: sse(
			contentChunk("delegating"),
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
			finishChunk("tool_calls"),
		)},
		{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo work"}`),
			finishChunk("tool_calls"),
		)},
		{body: sse(contentChunk("work done"), finishChunk("stop"))},
		{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	}
}
