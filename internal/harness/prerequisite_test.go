package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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

// refusedStatusTransport refuses every trusted status write with the broker's
// definite denial, so a test can hold the run's own status path down the way
// a misconfigured or cross-run caller earns: the write never lands, and no
// retry of it can.
type refusedStatusTransport struct{}

func (refusedStatusTransport) PostStatus(context.Context, StatusPatch) error {
	return brokerDenied()
}

// refusedShellBeginTransport serves the trusted status path honestly except
// for one operation kind's begin write, which it refuses definitively — the
// shape of a broker that accepts the preparation records and refuses the
// model-influenced operation's record.
type refusedShellBeginTransport struct{}

func (refusedShellBeginTransport) PostStatus(_ context.Context, patch StatusPatch) error {
	if patch.AddOperation != nil && strings.HasPrefix(patch.AddOperation.OpID, "shell.") {
		return brokerDenied()
	}
	return nil
}

// TestCoordinatorStopsModelTurnsOnDefiniteStatusPathRefusal is the #741
// regression: the trusted status path definitively refused to persist the
// operation's status record, so the operation never dispatched. The run must
// end as an infrastructure failure at the first turn's tool boundary — the
// model never gets a second turn to re-ask for the same refused operation
// through shell commands, tracked checks, or a delegated brief. A recording
// failure of trusted control's own plumbing is never blocked-external: the
// world did not refuse the work, the run's own status plumbing did.
//
// Both refusal sites are covered: the preparation dispatch's own record, and
// the shell dispatch's record after honest preparation. The second is the
// one that matters — only the dispatch boundary itself witnesses it, so a
// breaker that observed at the preparation callers alone would let the model
// take another turn over it.
func TestCoordinatorStopsModelTurnsOnDefiniteStatusPathRefusal(t *testing.T) {
	for _, tc := range []struct {
		name           string
		transport      StatusTransport
		wantDispatches int
	}{
		{
			name:           "the preparation dispatch's record is refused",
			transport:      refusedStatusTransport{},
			wantDispatches: 0,
		},
		{
			name:           "the shell dispatch's record is refused after honest preparation",
			transport:      refusedShellBeginTransport{},
			wantDispatches: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
			defer cleanupWorker()
			worker.snapshotTip = "snap-tip"
			worker.completeAll = true

			gateway, gatewayMock, cleanup := gatewayFor(
				// Coordinator: one shell command. Whichever write is
				// refused, the operation behind it never dispatches.
				gatewayResponse{body: sse(
					toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
					finishChunk("tool_calls"),
				)},
				// Scripted but never consumed: had the run continued, the
				// model would have re-asked for the same refused operation
				// through shell commands and tracked checks.
				gatewayResponse{body: sse(
					toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
					finishChunk("tool_calls"),
				)},
				// Scripted but never consumed: the latch outranks the
				// model's own outcome declaration, whatever it declared
				// over the refusal.
				gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
			)
			defer cleanup()

			coordinator, err := NewCoordinator(CoordinatorConfig{
				Transport: gateway,
				Bindings:  testBindings(),
				Worker:    delegator,
				Snapshot:  snapshotProvider("snap-tip"),
				Status:    NewStatusReporter(tc.transport, testControlUID, testWorkerUID, 0),
			})
			if err != nil {
				t.Fatal(err)
			}
			result := coordinator.Run(context.Background(), testInvocation())
			if result.Outcome == executor.OutcomeBlockedExternal {
				t.Fatalf("outcome = %q, a status-path refusal is never blocked-external", result.Outcome)
			}
			if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
				t.Fatalf("kind = %q, want infrastructure: a recording failure of trusted control's own plumbing is not an external prerequisite", result.Kind)
			}
			if result.Err == nil || !strings.Contains(result.Err.Error(), prerequisiteStatusRefusedReason) {
				t.Fatalf("err = %v, want the fixed status-path reason %q", result.Err, prerequisiteStatusRefusedReason)
			}
			if text := result.Err.Error(); strings.Contains(text, "\n") || strings.Contains(text, "echo one") {
				t.Fatalf("err = %q, must be single-line fixed text carrying neither model prose nor worker output", text)
			}
			// One gateway request: the run ended inside the first turn's
			// tool boundary, before any further model request.
			if got := gatewayMock.requestCount(); got != 1 {
				t.Fatalf("gateway requests = %d, want 1", got)
			}
			// The refused operation never dispatched: the unpack may have
			// run on the honest-preparation path, the shell never did.
			if got := worker.dispatchRequests; got != tc.wantDispatches {
				t.Fatalf("worker dispatch requests = %d, want %d", got, tc.wantDispatches)
			}
		})
	}
}

// flakyBeginTransport fails the active-operation writes with a retryable
// unavailability until the reporter's own bounded retry rounds are exhausted,
// then serves every write — including the retry — successfully. The failure
// budget is exactly one BeginOperation's retry rounds, so the test holds the
// status path down for one operation's begin and no further.
type flakyBeginTransport struct {
	inner    *recordingStatusTransport
	failures int
}

func (t *flakyBeginTransport) PostStatus(ctx context.Context, patch StatusPatch) error {
	if patch.AddOperation != nil && t.failures > 0 {
		t.failures--
		return brokerUnavailable()
	}
	return t.inner.PostStatus(ctx, patch)
}

// TestCoordinatorDoesNotTripOnRetryableStatusPathFailure pins the transient
// half of #741: a retryable status-path failure — the broker momentarily
// unavailable through the reporter's own bounded retry rounds — is an
// ordinary tool error, the latch never trips, and the model's legitimate
// retry stands: its next turn re-runs the operation and the run reaches its
// declared ending.
func TestCoordinatorDoesNotTripOnRetryableStatusPathFailure(t *testing.T) {
	shrinkRetryDelays(t)
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true

	transport := &flakyBeginTransport{
		inner:    &recordingStatusTransport{},
		failures: statusRetryRounds + 1,
	}

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		// The model's legitimate retry: the same operation again, against a
		// status path that now serves the write.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"no_change_needed","evidence":"nothing to do"}`), finishChunk("stop"))},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Status:    NewStatusReporter(transport, testControlUID, testWorkerUID, 0),
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
	if coordinator.prerequisite.peek() != nil {
		t.Fatal("a retryable status-path failure must never trip the latch")
	}
	// Three requests: the retryable failure surfaced as an ordinary tool
	// error, the model took its legitimate retry, and the run reached its
	// declared ending.
	if got := gatewayMock.requestCount(); got != 3 {
		t.Fatalf("gateway requests = %d, want 3 (the model's legitimate retry stands)", got)
	}
	// The retried operation ran: the write landed and the dispatch followed.
	if got := worker.dispatchRequests; got != 2 {
		t.Fatalf("worker dispatch requests = %d, want 2 (the retried unpack and the shell)", got)
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

// TestCoordinatorLocalBrokerFailureIsNeverBlockedExternal pins the refusal
// narrowing (#737 review) on the first observation: a non-retryable broker
// failure without a status — an unreadable CA, a malformed request or
// response, an oversized snapshot — is a local construction or decoding
// failure the trusted world never refused. It surfaces as an ordinary tool
// error and the model takes its next turn; the run never ends
// blocked-external over it, at the first failure or after recurrence
// (#744 makes the recurring disposition an infrastructure failure, pinned by
// TestCoordinatorBoundsTurnsOnRecurringLocalBrokerFailure).
func TestCoordinatorLocalBrokerFailureIsNeverBlockedExternal(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"no_change_needed","evidence":"nothing to do"}`), finishChunk("stop"))},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot: func(context.Context) (Snapshot, error) {
			// The production shape: the publication engine classifies the
			// local construction failure as an infrastructure failure —
			// never a denial — and the local error rides in the chain.
			return Snapshot{}, &TransientPublicationError{
				Err: &BrokerCallError{Category: "snapshot exceeds the bundle bound"},
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome == executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q, a local broker failure is never blocked-external", result.Outcome)
	}
	if result.Outcome != executor.OutcomeNoChangeNeeded {
		t.Fatalf("outcome = %q (%+v), want the declared no-change ending", result.Outcome, result)
	}
	// Two requests: the failing preparation surfaced as an ordinary tool
	// error and the model took its next turn.
	if got := gatewayMock.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2", got)
	}
}

// TestCoordinatorBoundsTurnsOnRecurringLocalBrokerFailure is the #744
// regression: a stable local broker failure — an oversized snapshot, an
// unreadable CA, a malformed request or response — never becomes a refusal,
// but it cannot be rediscovered indefinitely either. The first failure earns
// one legitimate rediscovery turn; the second establishes the condition and
// ends the run as an infrastructure failure at that turn's own tool
// boundary, before any further model request. The ending's reason is the
// fixed code-owned text: it never repeats the publication engine's "the
// broker denied" framing, because the trusted world never answered.
func TestCoordinatorBoundsTurnsOnRecurringLocalBrokerFailure(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true

	gateway, gatewayMock, cleanup := gatewayFor(
		// Coordinator: one shell command; its preparation fails locally.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		// The model rediscovers the condition through the shell: the second
		// preparation failure trips the latch at this turn's own tool
		// boundary.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
		// Scripted but never consumed: the recurrence bound ends the run
		// here — the model never takes a third turn to rediscover the same
		// condition, and never gets to declare an outcome over it.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-3", toolShell, `{"command":"echo three"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot: func(context.Context) (Snapshot, error) {
			// The production shape: the publication engine classifies the
			// local construction failure as an infrastructure failure —
			// never a denial — and the local error rides in the chain.
			return Snapshot{}, &TransientPublicationError{
				Err: &BrokerCallError{Category: "snapshot exceeds the bundle bound"},
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome == executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q, a local broker failure is never blocked-external", result.Outcome)
	}
	if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("kind = %q, want infrastructure: the world never refused, so the run cannot wait on an external prerequisite either", result.Kind)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), prerequisiteLocalFailReason) {
		t.Fatalf("err = %v, want the fixed local-failure reason %q", result.Err, prerequisiteLocalFailReason)
	}
	if text := result.Err.Error(); strings.Contains(text, "denied") {
		t.Fatalf("err = %q, must never call the broker's world-state a denial", text)
	}
	if strings.Contains(result.Err.Error(), "\n") {
		t.Fatalf("err = %q, must be single-line", result.Err)
	}
	// Two requests: the first failure earned one legitimate rediscovery
	// turn, the second ended the run at its own tool boundary. Without the
	// recurrence bound the model would keep rediscovering the condition for
	// as long as the gateway kept answering.
	if got := gatewayMock.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2 (bounded: one rediscovery turn)", got)
	}
	// The preparation fails before anything is dispatched: no worker
	// dispatch ever runs against the never-prepared workspace.
	if got := worker.dispatchRequests; got != 0 {
		t.Fatalf("worker dispatch requests = %d, want 0", got)
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

// TestCoordinatorDelegatedBriefStopsAtItsOwnToolBoundary pins the delegated
// path's fence reach (#737): a fence established by the brief's own shell
// dispatch trips the latch at the tool boundary that witnessed it, the brief
// aborts without a second model request, and the coordinator's own loop ends
// the run at the delegate call as an infrastructure failure. (That no
// dispatch follows the fence is enforced by the fence gate itself; the
// boundary-level stop is pinned by
// TestDelegatedToolBoundaryStopsRemainingCallsOnTrippedLatch.)
func TestCoordinatorDelegatedBriefStopsAtItsOwnToolBoundary(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// The brief's unpack is honest; its first shell dispatch then fails
	// ambiguously and its reconciliation cannot observe the termination.
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true
	worker.dispatchPlan = []int{0, 503}
	worker.scriptUnobservedTermination(1)

	gateway, gatewayMock, cleanup := gatewayFor(
		// Coordinator: delegate.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
			finishChunk("tool_calls"),
		)},
		// The brief's first turn asks for two shell commands; the first
		// fences the worker, and the second must never run.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo one"}`),
			toolCallChunk(1, "call-3", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
		// Scripted but never consumed: the latch has already ended the run.
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
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
	// Two requests: the coordinator's delegate turn and the brief's one
	// turn. The brief never takes a second turn over a tripped latch.
	if got := gatewayMock.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2", got)
	}
	// The brief's unpack and its first shell: the count pins that the run
	// ended at the delegate call without redelivery (the second call, where
	// it runs at all, is refused by the fence gate before any request).
	if got := worker.dispatchRequests; got != 2 {
		t.Fatalf("worker dispatch requests = %d, want 2", got)
	}
}

// TestDelegatedToolBoundaryStopsRemainingCallsOnTrippedLatch pins the
// delegated pump's own tool boundary (#737): a delegated turn that requests
// several tool calls stops at the call that trips the latch, so no later
// call in the same turn runs and the transcript holds only the earned
// result. The boundary peeks, never takes — the trip stays unconsumed for
// the coordinator's own loop.
func TestDelegatedToolBoundaryStopsRemainingCallsOnTrippedLatch(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// The brief's first shell dispatch fails ambiguously and its
	// reconciliation cannot observe the termination.
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true
	worker.dispatchPlan = []int{0, 503}
	worker.scriptUnobservedTermination(1)

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-a", toolShell, `{"command":"echo one"}`),
			toolCallChunk(1, "call-b", toolShell, `{"command":"echo two"}`),
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
	session, err := coordinator.newSession("coder")
	if err != nil {
		t.Fatal(err)
	}
	turn := coordinator.pump(context.Background(), session, "b1", coordinator.subagentTools())
	if coordinator.prerequisite.peek() == nil {
		t.Fatal("the latch must be tripped by the boundary that witnessed the fence")
	}
	if len(turn.toolResults) != 1 {
		t.Fatalf("tool results = %d, want 1: the turn stops at the call that tripped the latch", len(turn.toolResults))
	}
	if got := turn.toolResults[0].Result.ToolCallID; got != "call-a" {
		t.Fatalf("tool result call = %q, want the call that tripped the latch", got)
	}
	toolMessages := 0
	for _, message := range session.Messages {
		if message.Role == "tool" {
			toolMessages++
		}
	}
	if toolMessages != 1 {
		t.Fatalf("transcript tool messages = %d, want 1: the abandoned call earns no result", toolMessages)
	}
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
	// The brief's unpack and its first shell; the abandoned call never
	// dispatched.
	if got := worker.dispatchRequests; got != 2 {
		t.Fatalf("worker dispatch requests = %d, want 2", got)
	}
}

// TestDelegatedBriefEntryGuardNeverBuysATurnOverTrippedLatch pins the
// delegated brief's entry guard (#737): a brief entered over a latch the
// incarnation has already tripped aborts before its first model request.
func TestDelegatedBriefEntryGuardNeverBuysATurnOverTrippedLatch(t *testing.T) {
	gateway, gatewayMock, cleanup := gatewayFor(
		// Scripted but never consumed: the entry guard aborts the brief
		// before any model request.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.prerequisite.observe(refusedPrerequisite())
	summary, completed, unavailable := coordinator.runBrief(context.Background(), Brief{ID: "b1", Role: "coder", Objective: "Do the work", SuccessCheck: "tests pass"})
	if completed {
		t.Fatal("completed = true, want the aborted brief")
	}
	if unavailable != nil {
		t.Fatalf("unavailable = %v, want nil", unavailable)
	}
	if summary != "workspace prerequisite failed; the brief is aborted" {
		t.Fatalf("summary = %q, want the fixed abort summary", summary)
	}
	if got := gatewayMock.requestCount(); got != 0 {
		t.Fatalf("gateway requests = %d, want 0: a tripped latch never buys a turn", got)
	}
}

// TestDelegatedToolBoundaryRunsEveryCallWhenTheLatchHolds pins the boundary's
// negative case: a delegated turn whose calls never trip the latch runs every
// call it asked for, so the stop is a trip response, never a standing
// truncation of delegated turns.
func TestDelegatedToolBoundaryRunsEveryCallWhenTheLatchHolds(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	// Honest preparation and honest shell tasks: nothing trips.
	worker.snapshotTip = "snap-tip"
	worker.completeAll = true

	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-a", toolShell, `{"command":"echo one"}`),
			toolCallChunk(1, "call-b", toolShell, `{"command":"echo two"}`),
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
	session, err := coordinator.newSession("coder")
	if err != nil {
		t.Fatal(err)
	}
	turn := coordinator.pump(context.Background(), session, "b1", coordinator.subagentTools())
	if coordinator.prerequisite.peek() != nil {
		t.Fatal("an honest turn must not trip the latch")
	}
	if len(turn.toolResults) != 2 {
		t.Fatalf("tool results = %d, want 2: every requested call runs while the latch holds", len(turn.toolResults))
	}
	toolMessages := 0
	for _, message := range session.Messages {
		if message.Role == "tool" {
			toolMessages++
		}
	}
	if toolMessages != 2 {
		t.Fatalf("transcript tool messages = %d, want 2", toolMessages)
	}
	if got := worker.dispatchRequests; got != 3 {
		t.Fatalf("worker dispatch requests = %d, want 3 (unpack plus both shells)", got)
	}
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
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
	// A non-retryable broker error that states no denial is a local
	// construction or decoding failure — an unreadable CA, a malformed
	// request or response, an oversized snapshot — the trusted world never
	// refused (#737 review). It is observed as the local-failure class so
	// recurrence can establish it (#744), and it never becomes a refusal:
	// the ending is an infrastructure failure, never blocked-external.
	// Every category here is produced by the real client;
	// TestBrokerLocalFailuresAreNeverPrerequisiteRefusals pins the producer
	// side of that invariant.
	for _, local := range []*BrokerCallError{
		{Category: "CA is unreadable"},
		{Category: "request is malformed"},
		{Category: "snapshot exceeds the bundle bound"},
		{Category: "snapshot observation is malformed"},
	} {
		got := classifyPrerequisite(local)
		if got == nil || got.class != prerequisiteLocalFailure {
			t.Fatalf("classified = %v for %q, want the local-failure class: a local broker failure is not a refusal", got, local.Category)
		}
		if got.kind != courierv1alpha1.NativeResultInfrastructureFailed {
			t.Fatalf("class %q kind = %q, want infrastructure: the world never refused", got.class, got.kind)
		}
		if got.reason != prerequisiteLocalFailReason {
			t.Fatalf("class %q reason = %q, want the fixed local-failure reason", got.class, got.reason)
		}
	}
	// The production shape of an oversized snapshot read: the publication
	// engine classifies the local failure as its transient error, and the
	// classifier still sees the local failure, not a refusal. A blocked
	// wrapper around a status-less broker error — a shape the engine no
	// longer produces (#743) — must never read as a refusal either.
	localRead := classifyPrerequisite(&TransientPublicationError{
		Err: &BrokerCallError{Category: "snapshot exceeds the bundle bound"},
	})
	if localRead == nil || localRead.class != prerequisiteLocalFailure {
		t.Fatalf("classified = %v, want the local-failure class: a local snapshot failure is not a refusal", localRead)
	}
	if got := classifyPrerequisite(&PublicationBlockedError{
		Reason: "a snapshot read failed locally",
		Err:    &BrokerCallError{Category: "snapshot exceeds the bundle bound"},
	}); got == nil || got.class != prerequisiteLocalFailure {
		t.Fatalf("classified = %v, want the local-failure class: a blocked wrapper never upgrades a local failure to a refusal", got)
	}
	// A non-retryable status outside the definite-denial range is not a
	// refusal — the rule is the same 4xx rule the worker side applies — and
	// it is observed as the local-failure class too, so a broker that
	// stably answers without an API answer cannot burn turns either
	// (#744).
	notRefused := classifyPrerequisite(&BrokerCallError{StatusCode: http.StatusFound, Category: "denied the operation"})
	if notRefused == nil || notRefused.class != prerequisiteLocalFailure {
		t.Fatalf("classified = %v, want the local-failure class: a status outside the denial range is not a refusal", notRefused)
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
	// A definite status-path refusal is a recording failure of trusted
	// control's own plumbing: it is its own class, and the ending is an
	// infrastructure failure — never the blocked-external wait on an
	// external prerequisite, and never a refusal of the workspace the model
	// could rediscover (#741). The classifier reads the wrapped error's
	// retryability, so a refused write and a locally unwritable one are the
	// same class: the status path definitively did not land, whatever
	// refused it.
	statusRefused := &statusPersistFailure{opID: "op-1", err: &BrokerCallError{StatusCode: 403, Category: "denied the operation"}}
	got := classifyPrerequisite(statusRefused)
	if got == nil || got.class != prerequisiteStatusRefused {
		t.Fatalf("classified = %v, want the status-refused class", got)
	}
	if got.kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("class %q kind = %q, want infrastructure: a recording failure is never blocked-external", got.class, got.kind)
	}
	if got.reason != prerequisiteStatusRefusedReason {
		t.Fatalf("class %q reason = %q, want the fixed status-path reason", got.class, got.reason)
	}
	localStatus := &statusPersistFailure{opID: "op-2", err: &BrokerCallError{Category: "status request is malformed"}}
	if got := classifyPrerequisite(localStatus); got == nil || got.class != prerequisiteStatusRefused {
		t.Fatalf("classified = %v, want the status-refused class: a definite recording failure ends the run whatever refused it", got)
	}
	// A status-path failure that is not even a broker error is definite too:
	// the trusted plumbing failed in a way nothing can call transient, and
	// failing closed is the breaker's contract.
	if got := classifyPrerequisite(&statusPersistFailure{opID: "op-3", err: errors.New("recorder stopped")}); got == nil || got.class != prerequisiteStatusRefused {
		t.Fatalf("classified = %v, want the status-refused class: an unknown status-path failure is never transient", got)
	}
	// A retryable status-path failure is transient: the broker is momentarily
	// unreachable, the write already retried inside the reporter's bounded
	// rounds, and the model's legitimate retry stands (#741).
	retryableStatus := &statusPersistFailure{opID: "op-4", err: brokerUnavailable()}
	if got := classifyPrerequisite(retryableStatus); got != nil {
		t.Fatalf("classified = %v, want nil: a retryable status-path failure still retries", got)
	}
	if got := classifyPrerequisite(errors.New("something else entirely")); got != nil {
		t.Fatalf("classified = %v, want nil: an unclassified failure never trips", got)
	}
}

// cannedTransport answers every request with one canned response, so a test
// can drive the real broker client's decoding paths without a listener.
type cannedTransport struct {
	status int
	header http.Header
	body   io.ReadCloser
}

func (t *cannedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.status,
		Header:     t.header,
		Body:       t.body,
		ProtoMajor: 1,
		ProtoMinor: 1,
	}, nil
}

// zeroBody is a body of n zero bytes without holding them in memory.
type zeroBody struct {
	remaining int64
}

func (b *zeroBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > b.remaining {
		n = b.remaining
	}
	for i := range p[:n] {
		p[i] = 0
	}
	b.remaining -= n
	return int(n), nil
}

// TestBrokerLocalFailuresAreNeverPrerequisiteRefusals pins the invariant the
// classifier's refusal rule rests on: the broker client's local construction
// and decoding failures — an unreadable CA, a malformed request, an oversized
// snapshot body, a malformed snapshot observation — carry no status and are
// never retryable, because the trusted world never answered. Each error is
// produced by the real client, and each is observed as the local-failure
// class (#744) — never a refusal.
func TestBrokerLocalFailuresAreNeverPrerequisiteRefusals(t *testing.T) {
	validTip := strings.Repeat("ab", 20)
	serving := func(header http.Header, body io.ReadCloser) *BrokerClient {
		return &BrokerClient{
			BaseURL:     "https://broker.invalid",
			TokenSource: NewStaticBrokerTokenSource([]byte("token")),
			HTTP:        &http.Client{Transport: &cannedTransport{status: http.StatusOK, header: header, body: body}},
		}
	}

	tests := []struct {
		name string
		snap func(context.Context) (BrokerSnapshot, error)
	}{
		{
			// Fails when the client builds its own transport, before any
			// request exists.
			name: "CA is unreadable",
			snap: (&BrokerClient{
				BaseURL:     "https://broker.invalid",
				TokenSource: NewStaticBrokerTokenSource([]byte("token")),
				CA:          []byte("not a pem block"),
			}).Snapshot,
		},
		{
			// The request builder cannot parse the base URL; the canned
			// transport is wired so the CA failure cannot preempt it.
			name: "request is malformed",
			snap: (&BrokerClient{
				BaseURL:     "https://broker.invalid/%zz",
				TokenSource: NewStaticBrokerTokenSource([]byte("token")),
				HTTP:        &http.Client{Transport: &cannedTransport{status: http.StatusOK}},
			}).Snapshot,
		},
		{
			name: "snapshot exceeds the bundle bound",
			snap: serving(
				http.Header{headerSnapshotTip: []string{validTip}, headerSnapshotBaseTip: []string{validTip}},
				io.NopCloser(&zeroBody{remaining: MaxArtifactBundleBytes + 1<<20}),
			).Snapshot,
		},
		{
			name: "snapshot observation is malformed",
			snap: serving(
				http.Header{headerSnapshotTip: []string{"not-an-oid"}, headerSnapshotBaseTip: []string{"also-not"}},
				io.NopCloser(strings.NewReader("bundle")),
			).Snapshot,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.snap(context.Background())
			var callErr *BrokerCallError
			if !errors.As(err, &callErr) {
				t.Fatalf("err = %v, want a BrokerCallError", err)
			}
			if callErr.Category != tc.name {
				t.Fatalf("category = %q, want %q", callErr.Category, tc.name)
			}
			if callErr.Retryable {
				t.Fatalf("category %q is retryable, want a permanent local failure", callErr.Category)
			}
			if callErr.StatusCode != 0 {
				t.Fatalf("category %q carries status %d, want none: the trusted world never answered", callErr.Category, callErr.StatusCode)
			}
			if got := classifyPrerequisite(err); got == nil || got.class != prerequisiteLocalFailure {
				t.Fatalf("classified = %v for %q, want the local-failure class: a local broker failure is not a refusal", got, callErr.Category)
			}
		})
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
// a refusal, a mismatch, a fence, and a definite status-path refusal are
// final facts, each tripping the latch on its own.
func TestPrerequisiteLatchTripsOnFinalFacts(t *testing.T) {
	for _, failure := range []*prerequisiteFailure{
		refusedPrerequisite(),
		mismatchedPrerequisite(),
		fencedPrerequisite(),
		statusPathRefused(),
	} {
		var latch prerequisiteLatch
		latch.observe(failure)
		if latch.peek() == nil {
			t.Fatalf("class %q must trip the latch on one observation", failure.class)
		}
	}
}

// TestPrerequisiteLatchTripsOnRecurringLocalFailure pins the recurrence rule
// for the local-failure class (#744): one local broker failure may still be
// a passing artifact and earns one rediscovery turn, the second establishes
// the condition, and the trip carries the infrastructure ending — the world
// never refused, so the latch's disposition is never blocked-external.
func TestPrerequisiteLatchTripsOnRecurringLocalFailure(t *testing.T) {
	var latch prerequisiteLatch
	latch.observe(localBrokerFailure())
	if latch.peek() != nil {
		t.Fatal("one local broker failure must not trip the latch")
	}
	latch.observe(localBrokerFailure())
	tripped := latch.peek()
	if tripped == nil {
		t.Fatal("recurrence must trip the latch")
	}
	if tripped.class != prerequisiteLocalFailure {
		t.Fatalf("tripped class = %q, want %q", tripped.class, prerequisiteLocalFailure)
	}
	if tripped.kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("tripped kind = %q, want infrastructure: the world never refused", tripped.kind)
	}
	// The count is a total across classes: a task failure and a local
	// failure together establish the condition too, so alternating classes
	// cannot keep the run alive.
	var mixed prerequisiteLatch
	mixed.observe(failedPrerequisiteTask(2))
	mixed.observe(localBrokerFailure())
	if mixed.peek() == nil {
		t.Fatal("alternating recurrence classes must trip the latch")
	}
}
