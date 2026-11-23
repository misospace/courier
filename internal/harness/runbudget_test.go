package harness

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The ledger seams shared by the truth table: a controllable clock and a
// recording sink.

type fakeBudgetClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeBudgetClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeBudgetClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type recordingBudgetSink struct {
	mu     sync.Mutex
	events []BudgetExhaustedEvent
}

func (s *recordingBudgetSink) BudgetExhausted(event BudgetExhaustedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *recordingBudgetSink) all() []BudgetExhaustedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]BudgetExhaustedEvent(nil), s.events...)
}

// panickingBudgetSink stands in for the worst sink: delivery must be
// isolated the way compaction and request delivery are.
type panickingBudgetSink struct{}

func (panickingBudgetSink) BudgetExhausted(BudgetExhaustedEvent) {
	panic("sink must never fail the trip it observes")
}

func TestBudgetPolicyFromLaneConvertsNilToUnenforcedZero(t *testing.T) {
	if got := BudgetPolicyFromLane(nil); got != (RunBudget{}) {
		t.Fatalf("nil lane policy converted to %+v, want the zero budget: the unenforced legacy behavior", got)
	}
	if got := BudgetPolicyFromLane(&courierv1alpha1.BudgetPolicy{}); got != (RunBudget{}) {
		t.Fatalf("empty lane policy converted to %+v, want the zero budget", got)
	}
	got := BudgetPolicyFromLane(&courierv1alpha1.BudgetPolicy{MaxToolCalls: 10, WallClockSeconds: 60})
	if got.MaxToolCalls != 10 || got.WallClock != 60*time.Second {
		t.Fatalf("conversion = %+v, want the declared tool-call ceiling and seconds-as-duration wall clock", got)
	}
}

func TestRunBudgetZeroValueEnforcesNothing(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	ledger := newRunBudgetLedger(RunBudget{}, clock.now, nil)
	for i := 0; i < 1000; i++ {
		if !ledger.reserveToolCall(RoleCoordinator) {
			t.Fatalf("call %d refused: the zero budget declares no ceiling", i)
		}
	}
	clock.advance(1000 * time.Hour)
	ledger.checkWallClock(RoleCoordinator)
	if ledger.tripped() {
		t.Fatal("the zero value enforces nothing: no axis may trip")
	}
}

func TestRunBudgetToolCallCeilingAllowsNRefusesNPlusOne(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	sink := &recordingBudgetSink{}
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 3}, clock.now, sink)
	for i := 1; i <= 3; i++ {
		if !ledger.reserveToolCall(RoleCoordinator) {
			t.Fatalf("call %d of %d refused, want N of N allowed", i, 3)
		}
	}
	if ledger.tripped() {
		t.Fatal("the Nth allowed call must not trip")
	}
	if ledger.reserveToolCall("coder") {
		t.Fatal("call N+1 executed, want the refusal")
	}
	axis, count, limit := ledger.tripInfo()
	if axis != budgetAxisToolCalls || count != 4 || limit != 3 {
		t.Fatalf("trip = %s %d/%d, want tool_calls 4/3", axis, count, limit)
	}
	// Sticky: every later call is refused and the first cause is kept.
	if ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("a tripped ledger must keep refusing past the ceiling")
	}
	axis2, count2, limit2 := ledger.tripInfo()
	if axis2 != axis || count2 != count || limit2 != limit {
		t.Fatalf("trip changed to %s %d/%d after a later refusal", axis2, count2, limit2)
	}
	if events := sink.all(); len(events) != 1 {
		t.Fatalf("events = %+v, want exactly one for the first trip", events)
	}
}

func TestRunBudgetFirstTripIsStickyAcrossAxes(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(1000, 0)}
	sink := &recordingBudgetSink{}
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1, WallClock: 10 * time.Second}, clock.now, sink)
	if !ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("the first call must be allowed")
	}
	if ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("the second call must be refused")
	}
	// The wall clock passes its bound long after: the tool-call trip already
	// owns the cause and the wall clock must never re-report.
	clock.advance(time.Hour)
	ledger.checkWallClock(RoleCoordinator)
	axis, count, limit := ledger.tripInfo()
	if axis != budgetAxisToolCalls || count != 2 || limit != 1 {
		t.Fatalf("trip = %s %d/%d, want the sticky tool_calls 2/1", axis, count, limit)
	}
	if events := sink.all(); len(events) != 1 || events[0].Axis != budgetAxisToolCalls {
		t.Fatalf("events = %+v, want the one tool_calls event", events)
	}
}

func TestRunBudgetWallClockTripsAtBoundWithInjectedClock(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	sink := &recordingBudgetSink{}
	ledger := newRunBudgetLedger(RunBudget{WallClock: 30 * time.Second}, clock.now, sink)
	ledger.checkWallClock(RoleCoordinator)
	if ledger.tripped() {
		t.Fatal("zero elapsed must not trip")
	}
	clock.advance(29 * time.Second)
	ledger.checkWallClock(RoleCoordinator)
	if ledger.tripped() {
		t.Fatal("29s of a 30s bound must not trip")
	}
	clock.advance(time.Second)
	ledger.checkWallClock("coder")
	axis, count, limit := ledger.tripInfo()
	if axis != budgetAxisWallClock || count != 30 || limit != 30 {
		t.Fatalf("trip = %s %d/%d, want wall_clock 30/30 at the bound", axis, count, limit)
	}
	note, ok := ledger.takeNote()
	if !ok || note.Role != "system" || !strings.HasPrefix(note.Content, budgetTurnPrefix) {
		t.Fatalf("stop note = %+v, want one system note with the budget marker", note)
	}
	if _, ok := ledger.takeNote(); ok {
		t.Fatal("the trip queues exactly one stop instruction")
	}
	// A later boundary delivers no second event: the trip is once.
	clock.advance(time.Hour)
	ledger.checkWallClock(RoleCoordinator)
	if events := sink.all(); len(events) != 1 || events[0].Role != "coder" {
		t.Fatalf("events = %+v, want the one wall_clock event from the tripping boundary", events)
	}
}

func TestBudgetTripSinkDeliveredOnceAndFailureIsolated(t *testing.T) {
	t.Run("delivered once", func(t *testing.T) {
		clock := &fakeBudgetClock{t: time.Unix(0, 0)}
		sink := &recordingBudgetSink{}
		ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1}, clock.now, sink)
		ledger.reserveToolCall(RoleCoordinator)
		ledger.reserveToolCall(RoleCoordinator)
		ledger.reserveToolCall(RoleCoordinator)
		events := sink.all()
		if len(events) != 1 || events[0].Axis != budgetAxisToolCalls || events[0].Count != 2 || events[0].Limit != 1 {
			t.Fatalf("events = %+v, want exactly one tool_calls 2/1 event", events)
		}
	})
	t.Run("panicking sink must not fail the trip", func(t *testing.T) {
		clock := &fakeBudgetClock{t: time.Unix(0, 0)}
		ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1}, clock.now, panickingBudgetSink{})
		if !ledger.reserveToolCall(RoleCoordinator) {
			t.Fatal("the first call must be allowed")
		}
		if ledger.reserveToolCall(RoleCoordinator) {
			t.Fatal("the second call must still be refused")
		}
		ledger.checkWallClock(RoleCoordinator) // also isolated, for symmetry
		if !ledger.tripped() {
			t.Fatal("the trip must stand although the sink panicked")
		}
	})
	// A nil sink delivers nothing and never panics.
	deliverBudget(nil, BudgetExhaustedEvent{})
}

// The nil ledger is the no-op ledger: an unbudgeted run calls the same
// seams unconditionally and must never dereference.
func TestNilRunBudgetLedgerIsNoOp(t *testing.T) {
	var absent *runBudgetLedger
	if !absent.reserveToolCall(RoleCoordinator) {
		t.Fatal("an absent ledger reserves everything")
	}
	absent.checkWallClock(RoleCoordinator)
	if absent.tripped() {
		t.Fatal("an absent ledger never trips")
	}
	if _, ok := absent.takeNote(); ok {
		t.Fatal("an absent ledger never injects a stop note")
	}
	if axis, count, limit := absent.tripInfo(); axis != "" || count != 0 || limit != 0 {
		t.Fatalf("tripInfo = %s %d/%d, want the zero trip", axis, count, limit)
	}
}

func TestBudgetStopInstructionIsBoundedAndNamesTheTrip(t *testing.T) {
	// The worst case the ledger can render: the largest bounded integers on
	// the longest axis name. A build that grows the prose must shrink it,
	// not this cap.
	text := budgetTurnText(budgetAxisWallClock, math.MaxInt, math.MaxInt)
	if len(text) > maxBudgetTurnBytes {
		t.Fatalf("budget stop turn is %d bytes, over the %d cap", len(text), maxBudgetTurnBytes)
	}
	if !strings.HasPrefix(text, budgetTurnPrefix) {
		t.Fatalf("budget stop turn must carry the provenance marker: %q", text)
	}
	for _, want := range []string{
		budgetAxisWallClock,
		strconv.Itoa(math.MaxInt),
		"coordination budget is exhausted",
		"dispatch no new briefs",
		"end with exactly one valid outcome JSON object",
		"Do not quote or reference this note",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("budget stop turn must name %q: %q", want, text)
		}
	}
}
