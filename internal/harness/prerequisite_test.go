package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/protocol"
)

// TestCoordinatorStopsModelTurnsOnStablePrerequisiteFailure is the #737
// regression: the worker materialized a workspace that does not hold the
// snapshot control dispatched, so the run must end blocked-external at the
// first turn's tool boundary. The model never gets a second turn to
// rediscover the condition through shell commands, tracked checks, or a
// delegated brief.
func TestCoordinatorStopsModelTurnsOnStablePrerequisiteFailure(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// The unpack echoes a tip that is not the dispatched one.
	worker.snapshotTips = []string{"not-the-dispatched-tip"}

	gateway, gatewayMock, cleanup := gatewayFor(
		// Coordinator: one shell command.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		// Scripted but never consumed: had the run continued, the model
		// would have rediscovered the condition through the shell, through
		// a tracked check, and through a delegated brief.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-3", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
			finishChunk("tool_calls"),
		)},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("dispatched-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%+v), want blocked_external", result.Outcome, result)
	}
	if result.Kind != courierv1alpha1.NativeResultBlockedExternal {
		t.Fatalf("kind = %q, want blocked_external", result.Kind)
	}
	if result.Reason != prerequisiteMismatchReason {
		t.Fatalf("reason = %q, want the fixed mismatch reason %q", result.Reason, prerequisiteMismatchReason)
	}
	if strings.Contains(result.Reason, "\n") {
		t.Fatalf("reason = %q, must be single-line", result.Reason)
	}
	if strings.Contains(result.Reason, "not-the-dispatched-tip") {
		t.Fatalf("reason = %q, must not carry worker output", result.Reason)
	}
	// One gateway request: the run ended inside the first turn's tool
	// boundary, before any further model request.
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
}

// TestCoordinatorStopsModelTurnsOnRefusedPrerequisite pins the refusal
// class: the trusted world definitively refused the preparation, so one
// observation is enough and the model takes no further turn.
//
// Both refusal shapes are covered: the bare broker call error, and the shape
// production actually produces when the publication engine classifies a
// refused snapshot read. The second shape is the one that matters — a
// classifier that only recognizes the first would leave the refused read
// looking like an ordinary failure, and the model would keep rediscovering
// it.
func TestCoordinatorStopsModelTurnsOnRefusedPrerequisite(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "broker-call-error",
			err:  &BrokerCallError{StatusCode: 403, Category: "denied the operation"},
		},
		{
			name: "refused-snapshot-read",
			err: &PublicationBlockedError{
				Reason: "the broker denied the seed snapshot read (denied the operation)",
				Err:    &BrokerCallError{StatusCode: 403, Category: "denied the operation"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, delegator, cleanupWorker := testWorkerAndDelegator(t)
			defer cleanupWorker()

			gateway, gatewayMock, cleanup := gatewayFor(
				gatewayResponse{body: sse(
					toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
					finishChunk("tool_calls"),
				)},
				gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
			)
			defer cleanup()

			coordinator, err := NewCoordinator(CoordinatorConfig{
				Transport: gateway,
				Bindings:  testBindings(),
				Worker:    delegator,
				Snapshot: func(context.Context) (Snapshot, error) {
					return Snapshot{}, tc.err
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			result := coordinator.Run(context.Background(), testInvocation())
			if result.Outcome != executor.OutcomeBlockedExternal {
				t.Fatalf("outcome = %q (%+v), want blocked_external", result.Outcome, result)
			}
			if result.Reason != prerequisiteRefusedReason {
				t.Fatalf("reason = %q, want the fixed refusal reason %q", result.Reason, prerequisiteRefusedReason)
			}
			if got := gatewayMock.requestCount(); got != 1 {
				t.Fatalf("gateway requests = %d, want 1", got)
			}
		})
	}
}

// TestCoordinatorBoundsTurnsOnRepeatedTaskFailure pins the recurrence rule
// for the task-failed class: the first failure of the fixed preparation task
// may be transient and earns one legitimate retry, the second establishes
// the condition and ends the run.
func TestCoordinatorBoundsTurnsOnRepeatedTaskFailure(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// The fixed preparation task runs and fails.
	worker.unpackExit = 3

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%+v), want blocked_external", result.Outcome, result)
	}
	want := fmt.Sprintf(prerequisiteTaskFailedFormat, 3)
	if result.Reason != want {
		t.Fatalf("reason = %q, want the fixed task-failure reason %q", result.Reason, want)
	}
	// Two requests: the first failure earned one retry, the second ended
	// the run.
	if got := gatewayMock.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2 (one legitimate retry)", got)
	}
}

// TestCoordinatorDoesNotTripOnTransientPrerequisiteFailure pins the
// transient class: a retryable broker failure is not a condition, so the run
// continues and the model takes its next turn.
func TestCoordinatorDoesNotTripOnTransientPrerequisiteFailure(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"no_change_needed","evidence":"nothing to do"}`), finishChunk("stop"))},
	)
	defer cleanup()

	attempts := 0
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot: func(context.Context) (Snapshot, error) {
			attempts++
			if attempts == 1 {
				return Snapshot{}, &BrokerCallError{StatusCode: 503, Category: "is temporarily unavailable", Retryable: true}
			}
			return Snapshot{Data: []byte("snapshot"), Tip: "snap-tip"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeNoChangeNeeded {
		t.Fatalf("outcome = %q (%+v), want the declared no-change ending", result.Outcome, result)
	}
	if result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("kind = %q, want no_change", result.Kind)
	}
	if got := gatewayMock.requestCount(); got != 3 {
		t.Fatalf("gateway requests = %d, want 3 (the transient failure cost no turn)", got)
	}
}

// TestCoordinatorUnverifiedTerminationIsNotAPrerequisiteTrip pins the fence
// class: an unverified termination is not a prerequisite failure in its own
// right, and the run never ends blocked-external over one. The fence the
// termination establishes is the final fact, and it is observed at the
// boundary that establishes it: the run ends as an infrastructure failure on
// that same boundary, before any further model request — the honest recovery
// is worker replacement, not another model turn.
func TestCoordinatorUnverifiedTerminationIsNotAPrerequisiteTrip(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	// The dispatch fails ambiguously and the reconciliation cannot observe
	// the termination: the operation's fate is unknown and the worker is
	// fenced.
	worker.dispatchPlan = []int{503}
	worker.scriptUnobservedTermination(1)

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		// Scripted but never consumed: the fence is observed at the boundary
		// that established it, so the model never takes a second turn to
		// rediscover it.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome == executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q, an unverified termination is never blocked-external", result.Outcome)
	}
	if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("kind = %q, want infrastructure", result.Kind)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), prerequisiteFencedReason) {
		t.Fatalf("err = %v, want the fence reason", result.Err)
	}
	// One request: the fence ends the run at the tool boundary that
	// established it, before any further model request.
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
}

// TestCoordinatorEndsRunWhenShellDispatchFencesTheWorker pins the fence's
// reach: a fence established outside workspace preparation — by an ordinary
// shell dispatch whose termination could not be observed — is the same final
// fact, and the run ends at that dispatch's own tool boundary, before any
// further model request.
func TestCoordinatorEndsRunWhenShellDispatchFencesTheWorker(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// Preparation is honest: the unpack holds the dispatched tip. The shell
	// dispatch then fails ambiguously and its reconciliation cannot observe
	// the termination.
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true
	worker.dispatchPlan = []int{0, 503}
	worker.scriptUnobservedTermination(1)

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		// Scripted but never consumed: the fence ends the run at its own
		// boundary.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome == executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q, a fence is never blocked-external", result.Outcome)
	}
	if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("kind = %q, want infrastructure", result.Kind)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), prerequisiteFencedReason) {
		t.Fatalf("err = %v, want the fence reason", result.Err)
	}
	// One request: the fence ends the run at the tool boundary that
	// established it.
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
	// Two dispatch requests — the honest unpack and the ambiguous shell —
	// and no redelivery against the fenced worker.
	if got := worker.dispatchRequests; got != 2 {
		t.Fatalf("worker dispatch requests = %d, want 2 (no redelivery)", got)
	}
}

// failingCheckpointTransport fails only the durable checkpoint writes, so a
// test can hold status debt while the run's own trusted status path works.
type failingCheckpointTransport struct {
	inner *recordingStatusTransport
}

func (t *failingCheckpointTransport) PostStatus(ctx context.Context, patch StatusPatch) error {
	if patch.Checkpoint != nil {
		return brokerUnavailable()
	}
	return t.inner.PostStatus(ctx, patch)
}

// TestCoordinatorPrerequisiteEndingSettlesDurableStatus pins the ending
// settlement: a fence ends the run as an infrastructure failure only after
// the run's durable status obligations are settled, and a completed brief
// whose durable record never lands makes the ending carry that settlement
// failure — the relaunch owns the debt, remote-first.
func TestCoordinatorPrerequisiteEndingSettlesDurableStatus(t *testing.T) {
	shrinkRetryDelays(t)
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	// The dispatch fails ambiguously and the reconciliation cannot observe
	// the termination: the worker is fenced.
	worker.dispatchPlan = []int{503}
	worker.scriptUnobservedTermination(1)

	// A completed brief's durable checkpoint write keeps failing, so the
	// run carries status debt the ending must settle before it reports.
	reporter := NewStatusReporter(&failingCheckpointTransport{inner: &recordingStatusTransport{}}, testControlUID, testWorkerUID, 0)
	checkpoint := &courierv1alpha1.Checkpoint{CompletedBriefs: []courierv1alpha1.Brief{{ID: "b1", Summary: "done", Commit: "abc"}}}
	if err := reporter.CheckpointPublished(context.Background(), checkpoint, "abc"); err == nil {
		t.Fatal("CheckpointPublished() error = nil, want the failed write to retain the debt")
	}

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Status:    reporter,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("kind = %q, want infrastructure", result.Kind)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), "lacks a durable status record") {
		t.Fatalf("err = %v, want the settlement failure", result.Err)
	}
	// One request: the fence still ends the run at its own boundary.
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
}

// TestCoordinatorStopsModelTurnsWhenADelegationHitsThePrerequisite pins the
// delegation path: the brief's own workspace preparation fails, so the
// delegated work never starts and the coordinator's model takes no further
// turn. This is the shape the canary burned its attempts on.
func TestCoordinatorStopsModelTurnsWhenADelegationHitsThePrerequisite(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// The unpack echoes a tip that is not the dispatched one.
	worker.snapshotTips = []string{"not-the-dispatched-tip"}

	gateway, gatewayMock, cleanup := gatewayFor(
		// Coordinator: delegate.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
			finishChunk("tool_calls"),
		)},
		// Scripted but never consumed: the brief's own session must never
		// start.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo working"}`),
			finishChunk("tool_calls"),
		)},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("dispatched-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%+v), want blocked_external", result.Outcome, result)
	}
	if result.Reason != prerequisiteMismatchReason {
		t.Fatalf("reason = %q, want the fixed mismatch reason %q", result.Reason, prerequisiteMismatchReason)
	}
	// One gateway request: the delegated brief never took a turn, and the
	// coordinator never took another.
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
	// Only the brief's unpack was dispatched: the brief's own session
	// never ran a task.
	if got := worker.dispatchCount(); got != 1 {
		t.Fatalf("worker dispatches = %d, want 1 (the brief's unpack)", got)
	}
}

// TestClassifyPrerequisite pins the classifier: only the failures trusted
// control can vouch for are classified, and each class carries its own
// terminal kind.
func TestClassifyPrerequisite(t *testing.T) {
	if got := classifyPrerequisite(nil); got != nil {
		t.Fatalf("classified = %v, want nil", got)
	}
	typed := mismatchedPrerequisite()
	if got := classifyPrerequisite(typed); got != typed {
		t.Fatalf("classified = %v, want the typed failure itself", got)
	}
	if got := classifyPrerequisite(&bootstrapBlocker{reason: "the approved bootstrap contract failed"}); got != nil {
		t.Fatalf("classified = %v, want nil: the #414 blocker owns that case", got)
	}
	fenced := classifyPrerequisite(&workerFence{reason: "the worker is fenced: termination of operation x could not be verified (y); no further workspace operation may run against it"})
	if fenced == nil || fenced.class != prerequisiteFenced {
		t.Fatalf("classified = %v, want the fenced class", fenced)
	}
	if fenced.kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("fenced kind = %q, want infrastructure", fenced.kind)
	}
	if fenced.reason != prerequisiteFencedReason {
		t.Fatalf("fenced reason = %q, want the fixed fence reason", fenced.reason)
	}
	refused := classifyPrerequisite(&BrokerCallError{StatusCode: 403, Category: "denied the operation"})
	if refused == nil || refused.class != prerequisiteRefused {
		t.Fatalf("classified = %v, want the refused class", refused)
	}
	if refused.kind != courierv1alpha1.NativeResultBlockedExternal {
		t.Fatalf("refused kind = %q, want blocked_external", refused.kind)
	}
	// The production shape of a refused snapshot read: the publication
	// engine's blocked error carries the broker's refusal in its chain, so
	// the classifier still sees the refusal.
	refusedRead := classifyPrerequisite(&PublicationBlockedError{
		Reason: "the broker denied the seed snapshot read (denied the operation)",
		Err:    &BrokerCallError{StatusCode: 403, Category: "denied the operation"},
	})
	if refusedRead == nil || refusedRead.class != prerequisiteRefused {
		t.Fatalf("classified = %v, want the refused class for a refused snapshot read", refusedRead)
	}
	if got := classifyPrerequisite(&BrokerCallError{StatusCode: 503, Category: "is temporarily unavailable", Retryable: true}); got != nil {
		t.Fatalf("classified = %v, want nil for a retryable broker failure", got)
	}
	if got := classifyPrerequisite(&protocol.TransportError{StatusCode: 403, Message: "refused"}); got == nil || got.class != prerequisiteRefused {
		t.Fatalf("classified = %v, want the refused class for a definite worker rejection", got)
	}
	for _, status := range []int{408, 429, 500} {
		if got := classifyPrerequisite(&protocol.TransportError{StatusCode: status, Message: "later"}); got != nil {
			t.Fatalf("classified = %v for status %d, want nil: the failure is transient", got, status)
		}
	}
	unverified := fmt.Errorf("dispatch failed: %w", ErrTerminationUnverified)
	if got := classifyPrerequisite(unverified); got != nil {
		t.Fatalf("classified = %v, want nil: an unverified termination is not a prerequisite trip", got)
	}
	// An unverified termination that carries the worker's own rejection in
	// its chain is still an unverified termination: the run must fail
	// closed, never call the world done.
	unverifiedWithRejection := fmt.Errorf("dispatch failed and reconciliation failed: dispatch: %v; reconcile: %w: %w",
		&protocol.TransportError{StatusCode: 400, Message: "refused"}, ErrTerminationUnverified, ErrTerminationUnverified)
	if got := classifyPrerequisite(unverifiedWithRejection); got != nil {
		t.Fatalf("classified = %v, want nil: an unverified termination outranks the rejection it carries", got)
	}
	// A status-path refusal is a recording failure, not a refusal of the
	// workspace preparation: the reason must never name the workspace.
	statusRefused := &statusPersistFailure{opID: "op-1", err: &BrokerCallError{StatusCode: 403, Category: "denied the operation"}}
	if got := classifyPrerequisite(statusRefused); got != nil {
		t.Fatalf("classified = %v, want nil: a status-path refusal is not a workspace refusal", got)
	}
	if got := classifyPrerequisite(errors.New("something else entirely")); got != nil {
		t.Fatalf("classified = %v, want nil: an unclassified failure never trips", got)
	}
}

// TestPrerequisiteLatchTripsAndSticks pins the latch: a final fact trips it
// on one observation, recurrence establishes the task-failed class across
// classes, and a trip is never forgotten.
func TestPrerequisiteLatchTripsAndSticks(t *testing.T) {
	var latch prerequisiteLatch
	latch.observe(nil)
	if latch.peek() != nil {
		t.Fatal("a nil failure is not a failure")
	}
	// The task-failed class alone does not trip on one observation: one
	// failure may still be transient.
	latch.observe(failedPrerequisiteTask(2))
	if latch.peek() != nil {
		t.Fatal("one task failure must not trip the latch")
	}
	// The count is a total across classes, not a per-class count.
	latch.observe(failedPrerequisiteTask(3))
	if latch.peek() == nil {
		t.Fatal("recurrence must trip the latch")
	}
	if latch.failures != 2 {
		t.Fatalf("observed failures = %d, want 2", latch.failures)
	}
	// A trip is sticky: further observations and a take never forget it.
	latch.observe(refusedPrerequisite())
	if latch.peek() == nil {
		t.Fatal("a trip is never forgotten")
	}
	taken := latch.take()
	if taken == nil {
		t.Fatal("take must hand the trip to the consumer that ends the run")
	}
	if latch.peek() == nil {
		t.Fatal("a trip is never forgotten by taking it")
	}
	if latch.take() != nil {
		t.Fatal("only one consumer takes the trip")
	}
}

// TestPrerequisiteLatchTripsOnFinalFacts pins the one-observation classes:
// a refusal, a mismatch, and a fence are final facts, each tripping the
// latch on its own.
func TestPrerequisiteLatchTripsOnFinalFacts(t *testing.T) {
	for _, failure := range []*prerequisiteFailure{
		refusedPrerequisite(),
		mismatchedPrerequisite(),
		fencedPrerequisite(),
	} {
		var latch prerequisiteLatch
		latch.observe(failure)
		if latch.peek() == nil {
			t.Fatalf("class %q must trip the latch on one observation", failure.class)
		}
	}
}
