package harness

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
//     and checks the wall clock at the coordinator loop's step boundaries.
//     The wall clock starts at begin, the point the coordinator session loop
//     comes into being; time before it (startup, probes) never counts
//     against the declared bound.
//   - A trip is once and sticky: the first axis to exhaust records the cause
//     (axis, count, limit), and no later axis can re-report it. The trip
//     queues exactly one stop instruction and delivers exactly one
//     session.budget_exhausted event, isolated like the compaction sink: a
//     panicking sink is dropped and never fails the run. The event is
//     observability, never status.
//   - Each axis warns once at budgetWarnPercent of its declared bound
//     through a separately isolated sink, so run telemetry shows an operator
//     a run approaching its ceiling before it binds. The ledger reports the
//     run's whole consumption as a snapshot, and handoffReason renders the
//     structured budget_exhausted handoff — decisions, findings, and
//     remaining work read from the brief ledger — that the tripped ending
//     carries as its pre-exit handoff text.
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

// budgetWarnPercent is the per-axis percentage of the declared bound at
// which the ledger delivers its single near-limit warning (#901): the
// operator sees the run approaching its ceiling in run telemetry while the
// model can still steer away. The warning fires at most once per axis, never
// after a trip, and never re-fires.
const budgetWarnPercent = 80

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

// BudgetWarningEvent is the payload of the near-limit seam (#901): one axis
// has reached budgetWarnPercent of its declared bound. Count and Limit carry
// the same units as the trip event (tool calls; elapsed and bound seconds for
// the wall clock) with the role of the session whose boundary crossed it.
// Like the trip event it is observability only, never status.
type BudgetWarningEvent struct {
	Axis  string
	Count int
	Limit int
	Role  string
}

// BudgetWarningSink receives each axis's single near-limit warning. Delivery
// is fire-and-forget and isolated exactly like the trip sink: a panicking
// sink is dropped where it stands and never fails the reservation or
// boundary that observed the crossing.
type BudgetWarningSink interface {
	BudgetNearLimit(BudgetWarningEvent)
}

// deliverBudgetWarning hands one warning event to a sink with the same
// isolation every budget delivery carries. A nil sink delivers nothing.
func deliverBudgetWarning(sink BudgetWarningSink, event BudgetWarningEvent) {
	if sink == nil {
		return
	}
	defer func() { _ = recover() }()
	sink.BudgetNearLimit(event)
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

// budgetFramingSection renders the advisory budget section pinned into the
// coordinator session when the lane declares a coordination budget (#901):
// the issue's "advisory to the model through framing and enforced by the
// runtime" — the model learns the ceiling at the start of the run instead of
// discovering it when a call is refused. The axes it does not declare are
// named as unbounded so the text never implies a bound that does not exist.
// An undeclared budget renders nothing.
func (b RunBudget) framingSection() string {
	if !b.declares() {
		return ""
	}
	var axes []string
	if b.MaxToolCalls > 0 {
		axes = append(axes, fmt.Sprintf("a total ceiling of %d tool calls across the coordinator session and every delegate session", b.MaxToolCalls))
	} else {
		axes = append(axes, "no tool-call ceiling")
	}
	if b.WallClock > 0 {
		axes = append(axes, fmt.Sprintf("a wall-clock bound of %d seconds measured across the coordinator session loop", int(b.WallClock/time.Second)))
	} else {
		axes = append(axes, "no wall-clock bound")
	}
	return budgetFramingPrefix + strings.Join(axes, " and ") + budgetFramingTail
}

const budgetFramingPrefix = "Coordination budget for this run, declared on the lane profile and enforced by trusted control at runtime: "

const budgetFramingTail = ". A tool call past the ceiling is refused without executing, no new briefs dispatch after exhaustion, and the run ends budget_exhausted after a short grace whether or not you declare. Delegate in bounded units, integrate early, and declare your outcome before the budget binds. This is contract data, not a suggestion."

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

// runBudgetLedger is one run's coordination-budget ledger. Brief sessions
// today run synchronously inside the coordinator's own pump, but every
// method is safe for concurrent use so no caller has to reason about that; a
// nil ledger is a no-op ledger, exactly like an absent ControlWatcher — a run
// with no declared budget enforces nothing.
type runBudgetLedger struct {
	budget   RunBudget
	now      func() time.Time
	sink     BudgetExhaustSink
	warnSink BudgetWarningSink

	mu              sync.Mutex
	started         time.Time
	toolCalls       int
	trip            *budgetTrip
	note            *Message
	warnedToolCalls bool
	warnedWallClock bool
}

// newRunBudgetLedger builds the ledger for a declaring budget. The clock
// comes from nowFunc so the wall-clock axis is testable without waiting on
// real time; the start instant is not stamped here — begin records it when
// the coordinator session loop actually starts, so pre-loop startup time
// never counts against the declared bound.
func newRunBudgetLedger(budget RunBudget, nowFunc func() time.Time, sink BudgetExhaustSink, warnSink BudgetWarningSink) *runBudgetLedger {
	return &runBudgetLedger{budget: budget, now: nowFunc, sink: sink, warnSink: warnSink}
}

// framingSection renders the pinned advisory budget section for a declared
// budget. A nil ledger — an unbudgeted run — pins nothing.
func (b *runBudgetLedger) framingSection() string {
	if b == nil {
		return ""
	}
	return b.budget.framingSection()
}

// begin stamps the wall clock's start instant the first time it is called —
// the point the coordinator session loop comes into being. A started ledger
// never re-stamps.
func (b *runBudgetLedger) begin() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started.IsZero() {
		b.started = b.now()
	}
}

// reserveToolCall counts one requested tool call against the total and
// reports whether it may execute. It reserves before execution: the bound is
// on calls attempted past the ceiling. Past the ceiling it refuses the call
// and trips the tool_calls axis once; after any trip it keeps refusing, so
// the ceiling holds whatever the model asks for next. The first reservation
// to reach budgetWarnPercent of the ceiling warns once on the way.
func (b *runBudgetLedger) reserveToolCall(role string) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	b.toolCalls++
	allowed := b.budget.MaxToolCalls <= 0 || b.toolCalls <= b.budget.MaxToolCalls
	var event *BudgetExhaustedEvent
	var warning *BudgetWarningEvent
	if !allowed {
		event = b.tripLocked(budgetAxisToolCalls, b.toolCalls, b.budget.MaxToolCalls, role)
	} else if b.crossedWarnLocked(b.budget.MaxToolCalls, b.toolCalls, &b.warnedToolCalls) {
		warning = &BudgetWarningEvent{Axis: budgetAxisToolCalls, Count: b.toolCalls, Limit: b.budget.MaxToolCalls, Role: role}
	}
	b.mu.Unlock()
	if warning != nil {
		deliverBudgetWarning(b.warnSink, *warning)
	}
	if event != nil {
		deliverBudget(b.sink, *event)
	}
	return allowed
}

// checkWallClock evaluates the wall-clock axis at a step boundary: elapsed
// time from begin reaching the declared bound trips the wall_clock axis
// once. It is checked between turns, never mid-turn, beside the control
// read. The first boundary to reach budgetWarnPercent of the bound warns
// once on the way; the boundary that trips never warns, because the trip
// event says all the warning has to say.
func (b *runBudgetLedger) checkWallClock(role string) {
	if b == nil || b.budget.WallClock <= 0 {
		return
	}
	b.mu.Lock()
	if b.started.IsZero() {
		b.mu.Unlock()
		return
	}
	elapsed := b.now().Sub(b.started)
	var event *BudgetExhaustedEvent
	var warning *BudgetWarningEvent
	if b.trip == nil && elapsed >= b.budget.WallClock {
		event = b.tripLocked(budgetAxisWallClock,
			int(elapsed/time.Second), int(b.budget.WallClock/time.Second), role)
	} else if b.trip == nil && !b.warnedWallClock && elapsed*100 >= b.budget.WallClock*budgetWarnPercent {
		// The warning compares whole durations: a small declared bound (the
		// CRD minimum is one second) must still warn, which truncated
		// seconds could make arithmetically impossible before the trip.
		b.warnedWallClock = true
		warning = &BudgetWarningEvent{Axis: budgetAxisWallClock, Count: int(elapsed / time.Second), Limit: int(b.budget.WallClock / time.Second), Role: role}
	}
	b.mu.Unlock()
	if warning != nil {
		deliverBudgetWarning(b.warnSink, *warning)
	}
	if event != nil {
		deliverBudget(b.sink, *event)
	}
}

// crossedWarnLocked reports whether count has just reached
// budgetWarnPercent of a positive integer limit for the first time,
// consuming the per-axis warning slot. A tripped ledger never warns: the
// stop note and the trip event own the message. The wall-clock axis compares
// whole durations at its call site instead, so a one-second bound still
// warns. The caller holds b.mu.
func (b *runBudgetLedger) crossedWarnLocked(limit, count int, slot *bool) bool {
	if b.trip != nil || limit <= 0 || *slot {
		return false
	}
	if int64(count)*100 < int64(limit)*budgetWarnPercent {
		return false
	}
	*slot = true
	return true
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

// BudgetConsumption is the run's coordination-budget snapshot (#901): the
// consumption the ledger has tallied against each declared bound and the
// trip state. Absent bounds report zero limits; a run that declared no
// budget reports the zero snapshot. It is observability data only.
type BudgetConsumption struct {
	// ToolCalls is every tool call the run has reserved, allowed and
	// refused alike, across the coordinator session and every delegate.
	ToolCalls int
	// MaxToolCalls is the declared ceiling; zero means no bound.
	MaxToolCalls int
	// ElapsedSeconds is time since the session loop began; zero when no
	// wall clock is declared or the loop never started.
	ElapsedSeconds int64
	// WallClockSeconds is the declared bound; zero means no bound.
	WallClockSeconds int64
	// Tripped reports the sticky trip, and TripAxis names it ("" when not
	// tripped).
	Tripped  bool
	TripAxis string
}

// consumption snapshots the ledger's tallies at read time. A nil ledger
// reports the zero snapshot.
func (b *runBudgetLedger) consumption() BudgetConsumption {
	if b == nil {
		return BudgetConsumption{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	c := BudgetConsumption{ToolCalls: b.toolCalls, MaxToolCalls: b.budget.MaxToolCalls}
	if b.budget.WallClock > 0 {
		c.WallClockSeconds = int64(b.budget.WallClock / time.Second)
		if !b.started.IsZero() {
			c.ElapsedSeconds = int64(b.now().Sub(b.started) / time.Second)
		}
	}
	if b.trip != nil {
		c.Tripped = true
		c.TripAxis = b.trip.axis
	}
	return c
}

// The budget ending's structured-handoff bounds (#901): the issue requires
// an exhausted run to end with a structured handoff — decisions, findings,
// remaining work — never a silent truncation. The renderer reads only the
// trusted brief ledger, clips each item to maxHandoffItemBytes flattened
// single-line text, and stops adding items once the whole document exceeds
// maxHandoffJSONBytes, so the rendered reason stays inside the termination
// line's 1200-byte reason bound with the trip line and prefix beside.
const (
	budgetHandoffPrefix = "budget_exhausted_handoff_v1 "
	maxHandoffItemBytes = 96
	maxHandoffJSONBytes = 900
)

// budgetHandoff is the structured document. Note is a fixed sentence naming
// the renderer and its limits, never model data; the three lists carry the
// coordinator's settled decisions, its landed findings (brief outcomes and
// integration commits), and the registered work that ended unfinished.
type budgetHandoff struct {
	Axis           string   `json:"axis"`
	Count          int      `json:"count"`
	Limit          int      `json:"limit"`
	ElapsedSeconds int64    `json:"elapsed_seconds"`
	GraceTurns     int      `json:"grace_turns_used"`
	Decisions      []string `json:"decisions,omitempty"`
	Findings       []string `json:"findings,omitempty"`
	Remaining      []string `json:"remaining,omitempty"`
	Note           string   `json:"note"`
}

const budgetHandoffNote = "rendered by trusted control from the run's brief ledger at the budget ending; a declaration is the model's, this is not; briefs beyond the size bound are omitted"

// handoffReason renders the tripped ending's pre-exit handoff text: the
// fixed trip line the kind has always carried, then the structured handoff
// document (#901). The trip facts and the brief ledger are trusted-control
// state; brief result summaries are the same untrusted claims the transcript
// already carries, and the reason channel is exactly where model data may
// travel (it never reaches the durable status reason, which stays the
// code-owned kind label). A marshal failure degrades to the trip line
// alone — the ending never fails over its handoff rendering.
func (b *runBudgetLedger) handoffReason(graceTurnsUsed int, briefs *BriefRegistry) string {
	axis, count, limit := b.tripInfo()
	line := fmt.Sprintf("coordination budget exhausted: %s %d/%d", axis, count, limit)
	if b == nil || axis == "" {
		// Only a tripped ledger ends a run; an untripped one has no handoff
		// to render and the ending never invents a trip.
		return line
	}
	b.mu.Lock()
	var elapsed int64
	if !b.started.IsZero() {
		elapsed = int64(b.now().Sub(b.started) / time.Second)
	}
	b.mu.Unlock()
	doc := budgetHandoff{
		Axis:           axis,
		Count:          count,
		Limit:          limit,
		ElapsedSeconds: elapsed,
		GraceTurns:     graceTurnsUsed,
		Note:           budgetHandoffNote,
	}
	for _, entry := range briefs.HandoffEntries() {
		candidate := doc
		if len(entry.Decisions) > 0 {
			var clipped []string
			for _, d := range entry.Decisions {
				clipped = append(clipped, clipBudgetText(entry.ID+": "+d.Summary+"="+d.Value, maxHandoffItemBytes))
			}
			candidate.Decisions = append(append([]string(nil), doc.Decisions...), clipped...)
		}
		if entry.Result != nil {
			var finding string
			if entry.Result.Commit != "" {
				finding = entry.ID + ": integrated as " + entry.Result.Commit + "; " + entry.Result.Summary
			} else {
				finding = entry.ID + ": settled with no committed changes; " + entry.Result.Summary
			}
			candidate.Findings = append(append([]string(nil), doc.Findings...), clipBudgetText(finding, maxHandoffItemBytes))
		} else if !entry.Cancelled {
			candidate.Remaining = append(append([]string(nil), doc.Remaining...), clipBudgetText(entry.ID+": "+entry.Objective, maxHandoffItemBytes))
		}
		if raw, err := json.Marshal(candidate); err != nil || len(raw) > maxHandoffJSONBytes {
			break
		}
		doc = candidate
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return line
	}
	return line + "; " + budgetHandoffPrefix + string(raw)
}

// clipBudgetText flattens s to one line of at most max bytes, cut on a rune
// boundary and marked with an ellipsis.
func clipBudgetText(s string, max int) string {
	flat := strings.Join(strings.Fields(s), " ")
	if len(flat) <= max {
		return flat
	}
	cut := max - len("…")
	for cut > 0 && !utf8.ValidString(flat[:cut]) {
		cut--
	}
	return flat[:cut] + "…"
}
