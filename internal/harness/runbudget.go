package harness

import (
	"fmt"
	"sync"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// Coordination budget and the budget_exhausted ending (#901).
//
// A lane may declare a coordination budget on its profile: a total tool-call
// ceiling and a wall-clock bound. This file is the pure, executor-neutral
// seam that enforces them, beside the context-budget seam in budget.go:
//
//   - RunBudget carries the declared bounds; a zero field enforces nothing
//     on that axis and the zero value enforces nothing at all, mirroring the
//     documented unbudgeted legacy behavior of ContextPolicyFromLane.
//   - The ledger counts every reserved tool call across the coordinator
//     session and every delegate session — the bound is total for the run —
//     and checks the wall clock at the coordinator loop's step boundaries
//     from a start instant injected as a clock function.
//   - A trip is once and sticky: the first axis to exhaust records the cause
//     (axis, count, limit), and no later axis can re-report it. The trip
//     queues exactly one stop instruction and delivers exactly one
//     session.budget_exhausted event, isolated like the compaction sink: a
//     panicking sink is dropped and never fails the run. The event is
//     observability, never status.
//
// The wiring — the reservation in the tool pump, the step-boundary wall-
// clock check, the one-shot note injection, the bounded grace, and the
// budget ending that outranks the undeclared-ending failure — lives in the
// coordinator loop; the shared truth table in runbudget_test.go is the
// source of truth for these choices.

// The budget trip's axis vocabulary: `tool_calls` trips when a reserved tool
// call exceeds the declared ceiling, `wall_clock` when elapsed time reaches
// the declared bound at a step boundary.
const (
	budgetAxisToolCalls = "tool_calls"
	budgetAxisWallClock = "wall_clock"
)

// budgetGraceTurns is the fixed grace after the trip: the coordinator gets
// this many further turns to integrate in-flight work and declare its
// outcome before the harness ends the run itself. A declared outcome inside
// the grace is honored normally — the budget never overrides a valid
// declaration.
const budgetGraceTurns = 3

// RunBudget is one lane's declared coordination budget. A zero field
// declares no bound for that axis; the zero value enforces nothing.
type RunBudget struct {
	// MaxToolCalls bounds the total number of tool calls the run executes
	// across the coordinator session and every delegate session.
	MaxToolCalls int
	// WallClock bounds the run's wall clock, measured from the start of the
	// coordinator session loop to each step boundary.
	WallClock time.Duration
}

// declares reports whether the budget declares anything: with no bound on
// either axis there is nothing to enforce and no ledger to build.
func (b RunBudget) declares() bool {
	return b.MaxToolCalls > 0 || b.WallClock > 0
}

// BudgetPolicyFromLane converts the LaneProfile CRD's declared coordination
// budget into the harness budget (#901). A nil CRD policy converts to the
// zero RunBudget: the lane declares nothing and the run enforces nothing —
// the documented unenforced legacy behavior. Conversion only, no validation:
// the CRD schema bounds the declared values.
func BudgetPolicyFromLane(policy *courierv1alpha1.BudgetPolicy) RunBudget {
	if policy == nil {
		return RunBudget{}
	}
	return RunBudget{
		MaxToolCalls: policy.MaxToolCalls,
		WallClock:    time.Duration(policy.WallClockSeconds) * time.Second,
	}
}

// BudgetExhaustedEvent is the payload of the session.budget_exhausted seam
// (#901). It names the exhausted axis with the observed count and declared
// limit (wall clock: elapsed seconds and the bound in seconds) and the role
// of the session whose reservation or boundary tripped it. Like the
// compaction event it is observability only, never status, and never lets a
// model assert liveness or completion.
type BudgetExhaustedEvent struct {
	Axis  string
	Count int
	Limit int
	Role  string
}

// BudgetExhaustSink receives the run's single trip event. Delivery is
// fire-and-forget, exactly like the compaction sink, and isolated: a
// panicking sink is dropped where it stands and never fails the trip or the
// run it belongs to.
type BudgetExhaustSink interface {
	BudgetExhausted(BudgetExhaustedEvent)
}

// deliverBudget hands one trip event to a sink, isolating the run from a
// sink failure the same way compaction and request delivery are. A nil sink
// delivers nothing.
func deliverBudget(sink BudgetExhaustSink, event BudgetExhaustedEvent) {
	if sink == nil {
		return
	}
	defer func() { _ = recover() }()
	sink.BudgetExhausted(event)
}

const (
	// budgetTurnPrefix is the provenance marker the injected budget stop
	// turn carries, beside the soft-stop control turn's marker.
	budgetTurnPrefix = "[courier-budget, control-plane]"
	// maxBudgetTurnBytes caps the injected budget stop turn to the same cap
	// the control turn carries. A build that grows the text past the cap
	// must shrink the text, not the cap.
	maxBudgetTurnBytes = 512
)

// budgetTurnText renders the fixed stop instruction for a trip: the axis,
// count, and limit ride the same sentence shape every trip uses, and the
// instruction is the integration-and-declare contract the grace exists to
// serve. With the fixed axis names and bounded integers the text stays well
// inside maxBudgetTurnBytes, pinned by a test.
func budgetTurnText(axis string, count, limit int) string {
	return fmt.Sprintf("%s The run's coordination budget is exhausted (%s: %d of %d). Stop starting new work: dispatch no new briefs. Let in-flight briefs finish and integrate, run any final local validation, publish your work, and end with exactly one valid outcome JSON object. Do not quote or reference this note in model text, tool arguments, or tool results.",
		budgetTurnPrefix, axis, count, limit)
}

// The fixed tool-visible notices on the budget paths, beside the fixed
// sub-agent refusals: the ceiling refusal names the exhausted budget and
// the integrate-and-declare instruction; the dispatch refusal closes new
// briefs exactly the way the soft-stop latch does.
const (
	noticeBudgetCeiling = "the run's coordination budget is exhausted: no further tool calls will execute; stop starting new work, integrate in-flight results, run final validation, publish your work, and declare your outcome now"
	// noticeBudgetDispatchRefused is the delegate-path refusal after the trip.
	noticeBudgetDispatchRefused = "the run's coordination budget is exhausted: this incarnation dispatches no new briefs; let in-flight work stand, run final validation, and declare your outcome"
)

// budgetTrip is the sticky first cause: the exhausted axis with the observed
// count and the declared limit.
type budgetTrip struct {
	axis  string
	count int
	limit int
}

// runBudgetLedger is one run's coordination-budget ledger. Delegates execute
// in other goroutines, so every method is safe for concurrent use; a nil
// ledger is a no-op ledger, exactly like an absent ControlWatcher — a run
// with no declared budget enforces nothing.
type runBudgetLedger struct {
	budget RunBudget
	now    func() time.Time
	sink   BudgetExhaustSink

	mu        sync.Mutex
	started   time.Time
	toolCalls int
	trip      *budgetTrip
	note      *Message
}

// newRunBudgetLedger builds the ledger for a declaring budget: the start
// instant and the clock both come from nowFunc so the wall-clock axis is
// testable without waiting on real time.
func newRunBudgetLedger(budget RunBudget, nowFunc func() time.Time, sink BudgetExhaustSink) *runBudgetLedger {
	return &runBudgetLedger{budget: budget, now: nowFunc, sink: sink, started: nowFunc()}
}

// reserveToolCall counts one requested tool call against the total and
// reports whether it may execute. It reserves before execution: the bound is
// on calls attempted past the ceiling. Past the ceiling it refuses the call
// and trips the tool_calls axis once; after any trip it keeps refusing, so
// the ceiling holds whatever the model asks for next.
func (b *runBudgetLedger) reserveToolCall(role string) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	b.toolCalls++
	allowed := b.budget.MaxToolCalls <= 0 || b.toolCalls <= b.budget.MaxToolCalls
	var event *BudgetExhaustedEvent
	if !allowed {
		event = b.tripLocked(budgetAxisToolCalls, b.toolCalls, b.budget.MaxToolCalls, role)
	}
	b.mu.Unlock()
	if event != nil {
		deliverBudget(b.sink, *event)
	}
	return allowed
}

// checkWallClock evaluates the wall-clock axis at a step boundary: elapsed
// time from the ledger's start reaching the declared bound trips the
// wall_clock axis once. It is checked between turns, never mid-turn, beside
// the control read.
func (b *runBudgetLedger) checkWallClock(role string) {
	if b == nil || b.budget.WallClock <= 0 {
		return
	}
	b.mu.Lock()
	elapsed := b.now().Sub(b.started)
	var event *BudgetExhaustedEvent
	if b.trip == nil && elapsed >= b.budget.WallClock {
		event = b.tripLocked(budgetAxisWallClock,
			int(elapsed/time.Second), int(b.budget.WallClock/time.Second), role)
	}
	b.mu.Unlock()
	if event != nil {
		deliverBudget(b.sink, *event)
	}
}

// tripLocked records the first trip, queues the one stop instruction, and
// returns the event to deliver outside the lock. A trip that already happened
// is never re-reported: the first cause wins and the ledger is sticky.
func (b *runBudgetLedger) tripLocked(axis string, count, limit int, role string) *BudgetExhaustedEvent {
	if b.trip != nil {
		return nil
	}
	b.trip = &budgetTrip{axis: axis, count: count, limit: limit}
	b.note = &Message{Role: "system", Content: budgetTurnText(axis, count, limit)}
	return &BudgetExhaustedEvent{Axis: axis, Count: count, Limit: limit, Role: role}
}

// tripped reports whether the latch is set: new brief dispatches are closed,
// in-flight work continues to its terminal and integration.
func (b *runBudgetLedger) tripped() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trip != nil
}

// takeNote pops the queued stop instruction. The trip queues exactly one, so
// injection happens exactly once, mirroring the control turn.
func (b *runBudgetLedger) takeNote() (Message, bool) {
	if b == nil {
		return Message{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.note == nil {
		return Message{}, false
	}
	note := *b.note
	b.note = nil
	return note, true
}

// tripInfo returns the sticky trip's axis, count, and limit for the ending
// reason. An untripped ledger reports the zero trip.
func (b *runBudgetLedger) tripInfo() (axis string, count, limit int) {
	if b == nil {
		return "", 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.trip == nil {
		return "", 0, 0
	}
	return b.trip.axis, b.trip.count, b.trip.limit
}
