package harness

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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
	mu       sync.Mutex
	events   []BudgetExhaustedEvent
	warnings []BudgetWarningEvent
}

func (s *recordingBudgetSink) BudgetExhausted(event BudgetExhaustedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *recordingBudgetSink) BudgetNearLimit(event BudgetWarningEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.warnings = append(s.warnings, event)
}

func (s *recordingBudgetSink) all() []BudgetExhaustedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]BudgetExhaustedEvent(nil), s.events...)
}

func (s *recordingBudgetSink) allWarnings() []BudgetWarningEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]BudgetWarningEvent(nil), s.warnings...)
}

// panickingBudgetSink stands in for the worst sink: delivery must be
// isolated the way compaction and request delivery are.
type panickingBudgetSink struct{}

func (panickingBudgetSink) BudgetExhausted(BudgetExhaustedEvent) {
	panic("sink must never fail the trip it observes")
}

// panickingBudgetWarnSink is the same worst case for the warning seam.
type panickingBudgetWarnSink struct{}

func (panickingBudgetWarnSink) BudgetNearLimit(BudgetWarningEvent) {
	panic("sink must never fail the warning it observes")
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
	ledger := newRunBudgetLedger(RunBudget{}, clock.now, nil, nil)
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
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 3}, clock.now, sink, nil)
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
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1, WallClock: 10 * time.Second}, clock.now, sink, nil)
	ledger.begin()
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
	ledger := newRunBudgetLedger(RunBudget{WallClock: 30 * time.Second}, clock.now, sink, nil)
	ledger.begin()
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
		ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1}, clock.now, sink, nil)
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
		ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1}, clock.now, panickingBudgetSink{}, panickingBudgetWarnSink{})
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

// The wall clock measures from begin — the start of the coordinator session
// loop — never from ledger construction: startup time before the loop exists
// must not consume the declared bound.
func TestRunBudgetWallClockStartsAtBeginNotConstruction(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	sink := &recordingBudgetSink{}
	ledger := newRunBudgetLedger(RunBudget{WallClock: 30 * time.Second}, clock.now, sink, nil)
	clock.advance(100 * time.Second) // startup the bound never sees
	ledger.begin()
	ledger.checkWallClock(RoleCoordinator)
	if ledger.tripped() {
		t.Fatal("a loop that just began has consumed none of its bound")
	}
	clock.advance(29 * time.Second)
	ledger.checkWallClock(RoleCoordinator)
	if ledger.tripped() {
		t.Fatal("29s since begin is inside a 30s bound")
	}
	clock.advance(time.Second)
	ledger.checkWallClock(RoleCoordinator)
	axis, count, limit := ledger.tripInfo()
	if axis != budgetAxisWallClock || count != 30 || limit != 30 {
		t.Fatalf("trip = %s %d/%d, want wall_clock 30/30 measured from begin", axis, count, limit)
	}
}

// begin is once: re-entering the loop never re-stamps the clock.
func TestRunBudgetBeginIsOnce(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	ledger := newRunBudgetLedger(RunBudget{WallClock: 30 * time.Second}, clock.now, nil, nil)
	ledger.begin()
	clock.advance(20 * time.Second)
	ledger.begin()
	clock.advance(15 * time.Second)
	ledger.checkWallClock(RoleCoordinator)
	if !ledger.tripped() {
		t.Fatal("elapsed keeps running across a re-entered loop: begin never re-stamps")
	}
}

// Each axis warns once at budgetWarnPercent of its declared bound, and a
// boundary or call that trips never warns — the trip owns the message.
func TestRunBudgetNearLimitWarningsFireOncePerAxis(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	sink := &recordingBudgetSink{}
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 10, WallClock: 100 * time.Second}, clock.now, sink, sink)
	ledger.begin()
	for i := 0; i < 7; i++ {
		if !ledger.reserveToolCall(RoleCoordinator) {
			t.Fatal("calls below the ceiling are allowed")
		}
	}
	if warnings := sink.allWarnings(); len(warnings) != 0 {
		t.Fatalf("warnings = %+v, want none below the warning share", warnings)
	}
	if !ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("the eighth call is inside the ceiling")
	}
	warnings := sink.allWarnings()
	if len(warnings) != 1 {
		t.Fatalf("warnings = %+v, want the one tool_calls crossing", warnings)
	}
	if warnings[0].Axis != budgetAxisToolCalls || warnings[0].Count != 8 || warnings[0].Limit != 10 || warnings[0].Role != RoleCoordinator {
		t.Fatalf("warning = %+v, want tool_calls 8/10 at the warning share", warnings[0])
	}
	// The ninth and tenth allowed calls never re-warn.
	ledger.reserveToolCall(RoleCoordinator)
	ledger.reserveToolCall("coder")
	// The crossing call past the ceiling trips; the trip never re-warns.
	if ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("the eleventh call must be refused")
	}
	if warnings := sink.allWarnings(); len(warnings) != 1 || warnings[0].Axis != budgetAxisToolCalls {
		t.Fatalf("warnings = %+v, want still the one tool_calls warning", warnings)
	}
	if events := sink.all(); len(events) != 1 || events[0].Axis != budgetAxisToolCalls {
		t.Fatalf("events = %+v, want the one tool_calls trip", events)
	}

	// The wall clock warns once on its own fresh ledger: 80s of a 100s
	// bound warns, later boundaries do not, and the tripping boundary adds
	// the trip without a second warning. A ledger whose other axis has
	// already tripped warns nothing at all — tested by the same rule: any
	// trip suppresses all later warnings.
	clock2 := &fakeBudgetClock{t: time.Unix(0, 0)}
	wallSink := &recordingBudgetSink{}
	wall := newRunBudgetLedger(RunBudget{WallClock: 100 * time.Second}, clock2.now, wallSink, wallSink)
	wall.begin()
	clock2.advance(79 * time.Second)
	wall.checkWallClock(RoleCoordinator)
	if warnings := wallSink.allWarnings(); len(warnings) != 0 {
		t.Fatalf("warnings = %+v, want none below the warning share", warnings)
	}
	clock2.advance(time.Second)
	wall.checkWallClock(RoleCoordinator)
	warnings = wallSink.allWarnings()
	if len(warnings) != 1 || warnings[0].Axis != budgetAxisWallClock || warnings[0].Count != 80 || warnings[0].Limit != 100 {
		t.Fatalf("warnings = %+v, want the wall_clock 80/100 crossing", warnings)
	}
	clock2.advance(20 * time.Second)
	wall.checkWallClock(RoleCoordinator)
	if events := wallSink.all(); len(events) != 1 || events[0].Axis != budgetAxisWallClock || events[0].Count != 100 {
		t.Fatalf("events = %+v, want the wall_clock 100/100 trip", events)
	}
	if warnings := wallSink.allWarnings(); len(warnings) != 1 {
		t.Fatalf("warnings = %+v, want no warning from the tripping boundary or after a trip", warnings)
	}

	// A one-second-granularity bound still warns: the crossing compares
	// whole durations, not truncated seconds.
	clock3 := &fakeBudgetClock{t: time.Unix(0, 0)}
	tinySink := &recordingBudgetSink{}
	tiny := newRunBudgetLedger(RunBudget{WallClock: 4 * time.Second}, clock3.now, tinySink, tinySink)
	tiny.begin()
	clock3.advance(3200 * time.Millisecond)
	tiny.checkWallClock(RoleCoordinator)
	if warnings := tinySink.allWarnings(); len(warnings) != 1 || warnings[0].Axis != budgetAxisWallClock || warnings[0].Limit != 4 {
		t.Fatalf("warnings = %+v, want the wall_clock crossing of a 4-second bound", warnings)
	}
	clock3.advance(800 * time.Millisecond)
	tiny.checkWallClock(RoleCoordinator)
	if events := tinySink.all(); len(events) != 1 || events[0].Count != 4 {
		t.Fatalf("events = %+v, want the 4/4 trip", events)
	}
}

// Warning delivery is isolated exactly like the trip delivery, and a nil
// warn sink (an unbudgeted observation path) delivers nothing and panics
// nowhere.
func TestBudgetWarnSinkIsolatedAndNilSafe(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1, WallClock: 10 * time.Second}, clock.now, nil, panickingBudgetWarnSink{})
	ledger.begin()
	if !ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("the first call must be allowed despite the panicking warn sink")
	}
	clock.advance(9 * time.Second)
	ledger.checkWallClock(RoleCoordinator) // panics inside the sink, isolated
	if ledger.tripped() {
		t.Fatal("the warning must not trip anything")
	}
	if axis, _, _ := ledger.tripInfo(); axis != "" {
		t.Fatal("a warning is not a trip")
	}
	deliverBudgetWarning(nil, BudgetWarningEvent{})
}

// The consumption snapshot is the run.budget line's source: consumption
// against each declared bound, zero for each undeclared one, and the sticky
// trip state.
func TestRunBudgetConsumptionSnapshot(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 2, WallClock: 20 * time.Second}, clock.now, nil, nil)
	if got := ledger.consumption(); got.ToolCalls != 0 || got.ElapsedSeconds != 0 {
		t.Fatalf("fresh snapshot = %+v, want zero tallies", got)
	}
	ledger.reserveToolCall(RoleCoordinator)
	ledger.reserveToolCall(RoleCoordinator)
	if ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("the third call must be refused")
	}
	clock.advance(25 * time.Second)
	got := ledger.consumption()
	if got.ToolCalls != 3 || got.MaxToolCalls != 2 {
		t.Fatalf("tool tallies = %d/%d, want 3/2 (refused calls count toward consumption)", got.ToolCalls, got.MaxToolCalls)
	}
	if got.WallClockSeconds != 20 {
		t.Fatalf("wall clock bound = %d, want 20", got.WallClockSeconds)
	}
	if !got.Tripped || got.TripAxis != budgetAxisToolCalls {
		t.Fatalf("trip state = %+v, want the sticky tool_calls trip", got)
	}
	// An undeclared axis reports a zero limit, never an invented bound.
	partial := newRunBudgetLedger(RunBudget{MaxToolCalls: 5}, clock.now, nil, nil)
	if got := partial.consumption(); got.WallClockSeconds != 0 || got.ElapsedSeconds != 0 {
		t.Fatalf("undeclared-wall-clock snapshot = %+v, want zero wall-clock fields", got)
	}
	var absent *runBudgetLedger
	if got := absent.consumption(); got != (BudgetConsumption{}) {
		t.Fatalf("nil-ledger snapshot = %+v, want the zero snapshot", got)
	}
}

// The tripped ending carries the structured handoff the issue requires: the
// trip facts plus the decisions, findings, and unfinished work of the brief
// ledger — a parseable bounded document, never a bare reason string.
func TestBudgetHandoffReasonIsStructuredAndBounded(t *testing.T) {
	clock := &fakeBudgetClock{t: time.Unix(0, 0)}
	ledger := newRunBudgetLedger(RunBudget{MaxToolCalls: 1}, clock.now, nil, nil)
	ledger.begin()
	ledger.reserveToolCall(RoleCoordinator)
	if ledger.reserveToolCall(RoleCoordinator) {
		t.Fatal("the second call must trip")
	}
	clock.advance(45 * time.Second)
	briefs := NewBriefRegistry()
	if _, err := briefs.Register(Brief{
		ID: "b1", Role: "coder", Objective: "implement the ledger",
		Decisions:    []Decision{{Summary: "shape", Value: "pure seam beside budget.go"}},
		SuccessCheck: "tests pass",
	}); err != nil {
		t.Fatal(err)
	}
	briefs.RecordResult("b1", BriefResult{Summary: "ledger landed", Commit: "abc1234"})
	if _, err := briefs.Register(Brief{
		ID: "b2", Role: "reviewer", Objective: "review the wiring", SuccessCheck: "review posted",
	}); err != nil {
		t.Fatal(err)
	}
	briefs.MarkTerminal("b2")
	if _, err := briefs.Register(Brief{
		ID: "b3", Role: "coder", Objective: "obsolete rework", SuccessCheck: "done",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := briefs.Cancel("b3"); err != nil {
		t.Fatal(err)
	}

	reason := ledger.handoffReason(3, briefs)
	if !strings.HasPrefix(reason, "coordination budget exhausted: tool_calls 2/1; ") {
		t.Fatalf("reason must lead with the fixed trip line: %q", reason)
	}
	payload, found := strings.CutPrefix(reason, "coordination budget exhausted: tool_calls 2/1; "+budgetHandoffPrefix)
	if !found {
		t.Fatalf("reason must carry the structured handoff prefix %q: %q", budgetHandoffPrefix, reason)
	}
	var doc budgetHandoff
	if err := json.Unmarshal([]byte(payload), &doc); err != nil {
		t.Fatalf("handoff payload is not valid JSON: %v: %q", err, payload)
	}
	if doc.Axis != budgetAxisToolCalls || doc.Count != 2 || doc.Limit != 1 || doc.GraceTurns != 3 {
		t.Fatalf("handoff trip facts = %+v, want tool_calls 2/1 grace 3", doc)
	}
	if doc.ElapsedSeconds != 45 {
		t.Fatalf("handoff elapsed = %d, want 45", doc.ElapsedSeconds)
	}
	if len(doc.Decisions) != 1 || !strings.Contains(doc.Decisions[0], "b1: shape=pure seam beside budget.go") {
		t.Fatalf("decisions = %+v, want b1's settled decision", doc.Decisions)
	}
	if len(doc.Findings) != 1 || !strings.Contains(doc.Findings[0], "b1: integrated as abc1234") {
		t.Fatalf("findings = %+v, want b1's integration", doc.Findings)
	}
	if len(doc.Remaining) != 1 || !strings.Contains(doc.Remaining[0], "b2: review the wiring") {
		t.Fatalf("remaining = %+v, want b2's unfinished objective", doc.Remaining)
	}
	for _, item := range doc.Remaining {
		if strings.Contains(item, "b3") {
			t.Fatalf("remaining = %+v, want the deliberately cancelled b3 excluded", doc.Remaining)
		}
	}

	// The worst honest registry — many briefs with multi-kilobyte, rune-heavy
	// text — still renders inside the termination line's 1200-byte reason
	// bound, and the JSON survives whole: the handoff is bounded by
	// construction, never by silent truncation.
	padded := NewBriefRegistry()
	long := strings.Repeat("é日本語—", 400)
	for i := 0; i < 40; i++ {
		id := "brief-" + strconv.Itoa(i)
		if _, err := padded.Register(Brief{
			ID: id, Role: "coder", Objective: long,
			Decisions:    []Decision{{Summary: long, Value: long}},
			SuccessCheck: "done",
		}); err != nil {
			t.Fatal(err)
		}
		padded.RecordResult(id, BriefResult{Summary: long, Commit: "deadbeef"})
	}
	worst := ledger.handoffReason(math.MaxInt, padded)
	if len(worst) > 1200 {
		t.Fatalf("worst-case handoff reason is %d bytes, over the termination reason bound", len(worst))
	}
	worstPayload, found := strings.CutPrefix(worst, "coordination budget exhausted: tool_calls 2/1; "+budgetHandoffPrefix)
	if !found {
		t.Fatalf("worst-case reason lost its handoff prefix: %q", worst)
	}
	if err := json.Unmarshal([]byte(worstPayload), &doc); err != nil {
		t.Fatalf("worst-case handoff payload is not valid JSON: %v", err)
	}

	// An untripped ledger renders the trip line alone; the handoff never
	// invents a trip that did not happen.
	quiet := newRunBudgetLedger(RunBudget{}, clock.now, nil, nil)
	if got := quiet.handoffReason(0, NewBriefRegistry()); strings.Contains(got, budgetHandoffPrefix) {
		t.Fatalf("untripped reason = %q, want the trip line with no handoff document", got)
	}
}

func TestClipBudgetText(t *testing.T) {
	if got := clipBudgetText(" one\ttwo\nthree ", 100); got != "one two three" {
		t.Fatalf("flattened = %q, want whitespace collapsed to single spaces", got)
	}
	// The cut lands on a rune boundary and marks the truncation.
	got := clipBudgetText("日本語のテキスト", 7)
	if len(got) > 7 || !strings.HasSuffix(got, "…") {
		t.Fatalf("clipped = %q (%d bytes), want at most 7 bytes ending in the ellipsis", got, len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("clipped text split a multi-byte rune: %q", got)
	}
}

// The declared budget is advisory to the model through framing (#901): the
// section names exactly the axes the lane declared and marks the rest as
// unbounded, so the pinned text cannot imply a bound the runtime will not
// enforce. Nothing is declared, nothing is pinned.
func TestBudgetFramingSectionNamesOnlyDeclaredBounds(t *testing.T) {
	section := RunBudget{MaxToolCalls: 40, WallClock: 300 * time.Second}.framingSection()
	for _, want := range []string{"40 tool calls", "300 seconds", "contract data"} {
		if !strings.Contains(section, want) {
			t.Fatalf("framing must name %q: %q", want, section)
		}
	}
	// The grace promise the framing makes is the one the coordinator loop
	// keeps: a valid declaration inside the grace is honored (the run is
	// never doomed to budget_exhausted once tripped), and the text must not
	// claim otherwise.
	for _, want := range []string{"A valid declaration inside the short post-exhaustion grace is honored", "undeclared when that grace ends ends budget_exhausted"} {
		if !strings.Contains(section, want) {
			t.Fatalf("framing must state the grace contract %q: %q", want, section)
		}
	}
	if strings.Contains(section, "whether or not you declare") {
		t.Fatalf("framing must not promise a tripped run ends budget_exhausted regardless of its declaration: %q", section)
	}
	toolOnly := RunBudget{MaxToolCalls: 40}.framingSection()
	if strings.Contains(toolOnly, "300 seconds") || !strings.Contains(toolOnly, "no wall-clock bound") {
		t.Fatalf("tool-only framing = %q, want the wall clock named as unbounded", toolOnly)
	}
	if got := (RunBudget{}).framingSection(); got != "" {
		t.Fatalf("undeclared budget framed %q, want nothing", got)
	}
	var absent *runBudgetLedger
	if got := absent.framingSection(); got != "" {
		t.Fatalf("nil-ledger framing = %q, want nothing", got)
	}
}
