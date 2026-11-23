package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

// budgetToolTurn scripts one model turn that requests a single tool call.
func budgetToolTurn(id, name, arguments string) gatewayResponse {
	return gatewayResponse{body: sse(toolCallChunk(0, id, name, arguments), finishChunk("tool_calls"))}
}

func budgetFinalTurn(text string) gatewayResponse {
	return gatewayResponse{body: sse(contentChunk(text), finishChunk("stop"))}
}

// budgetCancelTurn is a tool call the trusted tools can answer without a
// worker, so the budget tests stay about the budget: a cancel_brief of an
// unknown ID executes (and is counted) as a tool error.
func budgetCancelTurn(id string) gatewayResponse {
	return budgetToolTurn("call-"+id, toolCancelBrief, fmt.Sprintf(`{"id":%q}`, "unknown-"+id))
}

const budgetNoChange = `{"outcome":"no_change_needed","evidence":"nothing left to do"}`

// TestRunBudgetToolCeilingEndsTheRunWithBudgetExhausted is the done-condition
// core of #901: a coordinator that keeps requesting tool calls past the cap
// ENDS with budget_exhausted rather than hanging — the fake gateway scripts
// exactly the turns the enforced grace allows (one allowed call, the
// tripping call, and budgetGraceTurns further turns), and the run must stop
// there even though the model never declares.
func TestRunBudgetToolCeilingEndsTheRunWithBudgetExhausted(t *testing.T) {
	sink := &recordingBudgetSink{}
	gateway, mocker, cleanup := gatewayFor(
		budgetCancelTurn("1"), budgetCancelTurn("2"), budgetCancelTurn("3"),
		budgetCancelTurn("4"), budgetCancelTurn("5"),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Budget:          RunBudget{MaxToolCalls: 1},
		BudgetExhausted: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultBudgetExhausted {
		t.Fatalf("result = %+v, want the budget_exhausted ending", result)
	}
	if !strings.Contains(result.Reason, "coordination budget exhausted: tool_calls 2/1") {
		t.Fatalf("reason = %q, want the axis, count, and limit named", result.Reason)
	}
	if result.Err != nil || result.Outcome != "" {
		t.Fatalf("the budget ending carries no error and no model outcome: %+v", result)
	}
	// The fake scripted more turns than the run consumed: the harness ended
	// the run itself once the grace ran out, never the model.
	if got, want := mocker.requestCount(), budgetGraceTurns+2; got != want {
		t.Fatalf("model requests = %d, want %d (allowed call, tripping call, then exactly the grace)", got, want)
	}
	// (b) the session.budget_exhausted seam: one event with axis/count/limit
	// and the tripping session's role.
	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("events = %+v, want exactly one budget_exhausted event", events)
	}
	if events[0].Axis != budgetAxisToolCalls || events[0].Count != 2 || events[0].Limit != 1 || events[0].Role != RoleCoordinator {
		t.Fatalf("event = %+v, want tool_calls 2/1 role coordinator", events[0])
	}
	// The refused calls earn a fixed tool-visible refusal and the stop note
	// lands in the transcript at the next step boundary.
	mocker.mu.Lock()
	defer mocker.mu.Unlock()
	var sawRefusal, sawNote bool
	for _, call := range mocker.calls {
		if strings.Contains(call.rawBody, noticeBudgetCeiling) {
			sawRefusal = true
		}
		if strings.Contains(call.rawBody, budgetTurnPrefix) {
			sawNote = true
		}
	}
	if !sawRefusal {
		t.Fatal("the refused tool call must carry the fixed budget refusal")
	}
	if !sawNote {
		t.Fatal("the trip must inject its stop instruction into the coordinator transcript")
	}
}

// TestRunBudgetDeclaredOutcomeInsideTheGraceIsHonored proves the grace is a
// real integration window, not a doomed run: a coordinator that declares a
// valid outcome after the stop instruction finishes through the normal
// finish path — never budget_exhausted.
func TestRunBudgetDeclaredOutcomeInsideTheGraceIsHonored(t *testing.T) {
	sink := &recordingBudgetSink{}
	gateway, mocker, cleanup := gatewayFor(
		budgetCancelTurn("1"), // 1 of 1: allowed
		budgetCancelTurn("2"), // 2 of 1: refused, the trip
		budgetFinalTurn(budgetNoChange),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Budget:          RunBudget{MaxToolCalls: 1},
		BudgetExhausted: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("result = %+v, want the declared no_change ending, not a budget ending", result)
	}
	if result.Err != nil {
		t.Fatalf("result = %+v, want a clean declared ending", result)
	}
	if events := sink.all(); len(events) != 1 {
		t.Fatalf("events = %+v, want the one trip event", events)
	}
	// The declaring turn's request carried the injected stop note.
	mocker.mu.Lock()
	defer mocker.mu.Unlock()
	if len(mocker.calls) != 3 || !strings.Contains(mocker.calls[2].rawBody, budgetTurnPrefix) {
		t.Fatalf("requests = %d, want the final declaration to answer the injected stop note", len(mocker.calls))
	}
}

// TestRunBudgetTrippedFinalMessageIsNotClarified proves the budget ending
// outranks the outcome-clarification theater: a final message that is not a
// valid declaration while tripped ends as budget_exhausted, not as the
// generic undeclared-ending workload failure and not with a clarification
// round-trip.
func TestRunBudgetTrippedFinalMessageIsNotClarified(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		budgetCancelTurn("1"),
		budgetCancelTurn("2"),
		budgetFinalTurn("I think I am done?"),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Budget: RunBudget{MaxToolCalls: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultBudgetExhausted {
		t.Fatalf("result = %+v, want the budget ending over the undeclared-ending failure", result)
	}
	if strings.Contains(result.Reason, "undeclared ending") {
		t.Fatalf("reason = %q, want the budget reason, not the generic undeclared ending", result.Reason)
	}
	if got := mocker.requestCount(); got != 3 {
		t.Fatalf("model requests = %d, want 3: no clarification turn after a tripped invalid declaration", got)
	}
}

// TestRunBudgetCountsDelegateToolCallsTowardRunTotal proves the bound is
// total for the run: the delegate session's shell call spends part of the
// ceiling, so the coordinator's next call trips with a count that includes
// every session's calls.
func TestRunBudgetCountsDelegateToolCallsTowardRunTotal(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	sink := &recordingBudgetSink{}
	gateway, _, cleanup := gatewayFor(
		// Coordinator: delegate to coder. (reserve 1 of 2)
		budgetToolTurn("call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		// Coder: shell on the worker. (reserve 2 of 2)
		budgetToolTurn("call-2", toolShell, `{"command":"echo work"}`),
		// Coder: final summary.
		budgetFinalTurn("work done"),
		// Coordinator: a third call trips with the delegate's call counted.
		budgetCancelTurn("3"),
		// Coordinator: declare inside the grace.
		budgetFinalTurn(budgetNoChange),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: &mockPublisher{},
		Budget:    RunBudget{MaxToolCalls: 2}, BudgetExhausted: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("result = %+v, want the declared ending", result)
	}
	// Without the delegate's call the coordinator's third-request total
	// would have been 2 of 2 and never tripped; the trip proves the total.
	events := sink.all()
	if len(events) != 1 || events[0].Axis != budgetAxisToolCalls || events[0].Count != 3 || events[0].Limit != 2 {
		t.Fatalf("events = %+v, want one tool_calls 3/2 event counting the delegate's call", events)
	}
	// The brief still completed and integrated through its terminal chain.
	if got := worker.dispatchCount(); got != 3 {
		t.Fatalf("worker dispatches = %d, want 3 (unpack, shell, pack)", got)
	}
	if completed := coordinator.Briefs().CompletedBriefs(); len(completed) != 1 || completed[0].ID != "b1" {
		t.Fatalf("completed briefs = %+v, want b1 through its terminal chain", completed)
	}
}

// TestRunBudgetRefusesNewBriefDispatchAfterTripKeepsInFlightBrief proves the
// dispatch half of the trip: a brief that was in flight when the ceiling
// tripped reaches its terminal and integrates, while a NEW dispatch after
// the trip is refused with a fixed notice and never reaches the worker.
func TestRunBudgetRefusesNewBriefDispatchAfterTripKeepsInFlightBrief(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	gateway, mocker, cleanup := gatewayFor(
		// Coordinator: delegate b1. (reserve 1 of 2)
		budgetToolTurn("call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		// Coder: shell. (reserve 2 of 2)
		budgetToolTurn("call-2", toolShell, `{"command":"echo work"}`),
		// Coder: a second shell trips the ceiling MID-brief — the brief is
		// never aborted, the call is just refused. (reserve 3 -> trip)
		budgetToolTurn("call-3", toolShell, `{"command":"echo more"}`),
		// Coder: final summary anyway; the brief integrates.
		budgetFinalTurn("work done"),
		// Coordinator: a NEW brief after the trip — refused, not dispatched.
		budgetToolTurn("call-4", toolDelegate, `{"id":"b2","role":"coder","objective":"More work","successCheck":"tests pass"}`),
		// Coordinator: declare inside the grace.
		budgetFinalTurn(budgetNoChange),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: &mockPublisher{},
		Budget:    RunBudget{MaxToolCalls: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("result = %+v, want the declared ending", result)
	}
	// b1 ran its full chain (unpack, shell, pack) — the trip did not abort
	// it; b2 never reached the worker at all.
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if got := len(worker.dispatches); got != 3 {
		t.Fatalf("worker dispatches = %d, want 3 (b1's unpack, shell, pack only)", got)
	}
	for _, d := range worker.dispatches {
		if d.envelope.BriefID != "b1" {
			t.Fatalf("dispatch brief = %q, want only the in-flight b1", d.envelope.BriefID)
		}
	}
	// The refused dispatch earned a fixed budget refusal in the transcript:
	// with the ceiling declared and spent, the pump's ceiling refusal is the
	// boundary that refuses b2 before runDelegate can — either way the call
	// is tool-visibly refused and never dispatched.
	mocker.mu.Lock()
	defer mocker.mu.Unlock()
	var sawDispatchRefusal bool
	for _, call := range mocker.calls {
		if strings.Contains(call.rawBody, "the run's coordination budget is exhausted") {
			sawDispatchRefusal = true
		}
	}
	if !sawDispatchRefusal {
		t.Fatal("the post-trip delegate call must carry a fixed budget refusal")
	}
}

// TestRunBudgetWallClockTripRefusesDelegateDispatchAtRunDelegate pins the
// dispatch refusal in runDelegate itself: on the wall-clock axis there is no
// tool-call ceiling, so the delegate call executes through to runDelegate,
// which refuses the new brief with its fixed notice and never registers it
// or touches the worker.
func TestRunBudgetWallClockTripRefusesDelegateDispatchAtRunDelegate(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	gateway, mocker, cleanup := gatewayFor(
		// Coordinator: delegate b1 after the boundary trip — refused by
		// runDelegate, never dispatched.
		budgetToolTurn("call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		// Coordinator: declare inside the grace.
		budgetFinalTurn(budgetNoChange),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: &mockPublisher{},
		Budget:    RunBudget{WallClock: 30 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	coordinator.runBudget.started = base
	coordinator.runBudget.now = func() time.Time { return base.Add(60 * time.Second) }
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("result = %+v, want the declared ending inside the grace", result)
	}
	if got := worker.dispatchCount(); got != 0 {
		t.Fatalf("worker dispatches = %d, want 0: the refused brief never reached the worker", got)
	}
	if completed := coordinator.Briefs().CompletedBriefs(); len(completed) != 0 {
		t.Fatalf("completed briefs = %+v, want none: b1 was never registered", completed)
	}
	mocker.mu.Lock()
	defer mocker.mu.Unlock()
	var sawDispatchRefusal bool
	for _, call := range mocker.calls {
		if strings.Contains(call.rawBody, noticeBudgetDispatchRefused) {
			sawDispatchRefusal = true
		}
	}
	if !sawDispatchRefusal {
		t.Fatal("the post-trip delegate call must carry the fixed dispatch refusal")
	}
}

// TestRunBudgetWallClockTripsAtStepBoundaryWithInjectedClock proves the
// wall-clock axis through the wiring: an injected clock past the bound trips
// at the next step boundary, the stop note lands, the grace runs, and the
// run ends budget_exhausted naming the wall_clock axis.
func TestRunBudgetWallClockTripsAtStepBoundaryWithInjectedClock(t *testing.T) {
	sink := &recordingBudgetSink{}
	gateway, mocker, cleanup := gatewayFor(
		budgetCancelTurn("1"), budgetCancelTurn("2"), budgetCancelTurn("3"),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Budget:          RunBudget{WallClock: 30 * time.Second},
		BudgetExhausted: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	coordinator.runBudget.started = base
	coordinator.runBudget.now = func() time.Time { return base.Add(90 * time.Second) }
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultBudgetExhausted {
		t.Fatalf("result = %+v, want the budget_exhausted ending", result)
	}
	if !strings.Contains(result.Reason, "coordination budget exhausted: wall_clock 90/30") {
		t.Fatalf("reason = %q, want the wall_clock axis with elapsed and bound", result.Reason)
	}
	if got, want := mocker.requestCount(), budgetGraceTurns; got != want {
		t.Fatalf("model requests = %d, want %d: the trip landed at the first boundary and the grace ran out", got, want)
	}
	events := sink.all()
	if len(events) != 1 || events[0].Axis != budgetAxisWallClock || events[0].Count != 90 || events[0].Limit != 30 || events[0].Role != RoleCoordinator {
		t.Fatalf("events = %+v, want one wall_clock 90/30 event from the coordinator boundary", events)
	}
}

// TestRunBudgetTripNeverPreemptsOperatorSoftStop pins the precedence rule:
// when the operator soft-stop latch is set, that path owns the ending even
// while the coordination budget is tripped. A fix-pr run whose work ref holds
// a tip beyond the anchor finishes through the world-derived soft-stop
// publication — never the budget ending — because the latch's contract, not
// the budget, decides how a stopped run ends.
func TestRunBudgetTripNeverPreemptsOperatorSoftStop(t *testing.T) {
	sink := &recordingBudgetSink{}
	gateway, _, cleanup := gatewayFor(
		budgetCancelTurn("1"), // 1 of 1: allowed
		budgetCancelTurn("2"), // 2 of 1: refused, the budget trip
		budgetFinalTurn(`{"outcome":"changes"}`),
	)
	defer cleanup()
	watcher := NewControlWatcher(&stubControlSource{requests: []*courierv1alpha1.ControlRequest{controlRequest("req-1")}}, &stubAckWriter{})
	watcher.Poll(context.Background()) // the operator request landed before the first turn
	publisher := &stubPublisher{result: Publication{OID: "abc123"}}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Budget:              RunBudget{MaxToolCalls: 1},
		BudgetExhausted:     sink,
		Control:             watcher,
		Publisher:           publisher,
		WorkRefBeyondAnchor: func(context.Context) (bool, error) { return true, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	inv := testInvocation()
	inv.Mode = courierv1alpha1.ModeFixPR
	result := coordinator.Run(context.Background(), inv)
	if result.Kind != courierv1alpha1.NativeResultPublished || result.Outcome != executor.OutcomeChanges {
		t.Fatalf("result = %+v, want the soft-stop world-derived changes ending, not a budget ending", result)
	}
	if publisher.count() != 1 {
		t.Fatalf("publishes = %d, want the one world-derived publication", publisher.count())
	}
	// The trip still happened and is still observable — it just never owns
	// the ending the latch owns.
	if events := sink.all(); len(events) != 1 {
		t.Fatalf("events = %+v, want the one trip event", events)
	}
}

// TestRunBudgetDeclaredBoundsArePinnedIntoTheFraming proves the advisory half
// of #901: a declaring lane's ceilings ride the coordinator transcript from
// the first request, so the model is warned before the runtime refuses a
// call — and an undeclared lane pins nothing, keeping today's transcript
// byte-identical.
func TestRunBudgetDeclaredBoundsArePinnedIntoTheFraming(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		budgetCancelTurn("1"),
		budgetFinalTurn(budgetNoChange),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Budget: RunBudget{MaxToolCalls: 1, WallClock: 300 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("result = %+v, want the declared ending", result)
	}
	mocker.mu.Lock()
	first := mocker.calls[0].rawBody
	mocker.mu.Unlock()
	if !strings.Contains(first, "Coordination budget for this run") || !strings.Contains(first, "1 tool calls") || !strings.Contains(first, "300 seconds") {
		t.Fatalf("the first request must carry the pinned budget advisory with both bounds: %q", first)
	}

	gateway2, mocker2, cleanup2 := gatewayFor(
		budgetCancelTurn("1"),
		budgetFinalTurn(budgetNoChange),
	)
	defer cleanup2()
	coordinator2, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway2, Bindings: testBindings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result := coordinator2.Run(context.Background(), testInvocation()); result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("result = %+v, want the declared ending", result)
	}
	mocker2.mu.Lock()
	defer mocker2.mu.Unlock()
	if strings.Contains(mocker2.calls[0].rawBody, "Coordination budget for this run") {
		t.Fatal("an undeclared lane pins no budget section: the transcript stays byte-identical")
	}
}

// TestRunBudgetNearLimitWarningCrossesBeforeTheTrip proves the warning seam
// through the wiring: a reservation reaching the warning share emits one
// session warning, and the later trip is still the single trip event.
func TestRunBudgetNearLimitWarningCrossesBeforeTheTrip(t *testing.T) {
	sink := &recordingBudgetSink{}
	gateway, _, cleanup := gatewayFor(
		budgetCancelTurn("1"), budgetCancelTurn("2"), budgetCancelTurn("3"), // 1..3 of 3 allowed; 3/3 warns
		budgetCancelTurn("4"), // 4 of 3: refused, the trip
		budgetFinalTurn(budgetNoChange),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Budget:          RunBudget{MaxToolCalls: 3},
		BudgetExhausted: sink,
		BudgetNearLimit: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result := coordinator.Run(context.Background(), testInvocation()); result.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("result = %+v, want the declaration inside the grace", result)
	}
	warnings := sink.allWarnings()
	if len(warnings) != 1 {
		t.Fatalf("warnings = %+v, want exactly one at the warning share", warnings)
	}
	if warnings[0].Axis != budgetAxisToolCalls || warnings[0].Count != 3 || warnings[0].Limit != 3 {
		t.Fatalf("warning = %+v, want tool_calls 3/3", warnings[0])
	}
	if events := sink.all(); len(events) != 1 || events[0].Count != 4 {
		t.Fatalf("events = %+v, want the one 4/3 trip beside the warning", events)
	}
}

// TestRunBudgetEndingCarriesStructuredHandoff proves #901's done-condition
// for the ending itself: a run that exhausts the ceiling without declaring
// ends budget_exhausted carrying the structured handoff — its integrated
// brief as a finding, decisions, unfinished briefs as remaining work — not
// a bare reason string.
func TestRunBudgetEndingCarriesStructuredHandoff(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	sink := &recordingBudgetSink{}
	gateway, _, cleanup := gatewayFor(
		// Coordinator: delegate b1. (reserve 1 of 2)
		budgetToolTurn("call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","decisions":[{"summary":"shape","value":"pure seam"}],"successCheck":"tests pass"}`),
		// Coder: shell, then final summary. (reserve 2 of 2)
		budgetToolTurn("call-2", toolShell, `{"command":"echo work"}`),
		budgetFinalTurn("work done"),
		// Coordinator: a third call trips. (reserve 3 of 2)
		budgetCancelTurn("3"),
		// The grace burns without a declaration; the harness ends the run.
		budgetCancelTurn("4"), budgetCancelTurn("5"), budgetCancelTurn("6"),
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway, Bindings: testBindings(),
		Worker:          delegator,
		Snapshot:        snapshotProvider("snap-tip"),
		Publisher:       &mockPublisher{},
		Budget:          RunBudget{MaxToolCalls: 2},
		BudgetExhausted: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Kind != courierv1alpha1.NativeResultBudgetExhausted {
		t.Fatalf("result = %+v, want the budget_exhausted ending", result)
	}
	payload, found := strings.CutPrefix(result.Reason, "coordination budget exhausted: tool_calls 3/2; "+budgetHandoffPrefix)
	if !found {
		t.Fatalf("reason = %q, want the trip line and the structured handoff", result.Reason)
	}
	var doc budgetHandoff
	if err := json.Unmarshal([]byte(payload), &doc); err != nil {
		t.Fatalf("handoff payload is not valid JSON: %v: %q", err, payload)
	}
	if doc.Axis != budgetAxisToolCalls || doc.Count != 3 || doc.Limit != 2 || doc.GraceTurns != budgetGraceTurns {
		t.Fatalf("handoff trip facts = %+v, want tool_calls 3/2 after the full grace", doc)
	}
	if len(doc.Decisions) != 1 || !strings.Contains(doc.Decisions[0], "b1: shape=pure seam") {
		t.Fatalf("decisions = %+v, want b1's settled decision", doc.Decisions)
	}
	if len(doc.Findings) != 1 || !strings.Contains(doc.Findings[0], "b1:") || !strings.Contains(doc.Findings[0], "work done") {
		t.Fatalf("findings = %+v, want b1's recorded outcome", doc.Findings)
	}
	if len(doc.Remaining) != 0 {
		t.Fatalf("remaining = %+v, want none: b1 reached its terminal", doc.Remaining)
	}
	// The consumption snapshot the run.budget line rides reflects the same
	// ending: refused calls counted, the trip sticky.
	consumption, declared := coordinator.BudgetConsumption()
	if !declared || consumption.ToolCalls != 6 || consumption.MaxToolCalls != 2 || !consumption.Tripped || consumption.TripAxis != budgetAxisToolCalls {
		t.Fatalf("consumption = %+v declared=%v, want 6/2 tripped on tool_calls", consumption, declared)
	}
}
