package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// --- step-boundary compaction (#297) ---------------------------------------
//
// The pure policy and its truth table live in budget.go/budget_test.go. What
// these tests pin is the executor's application of it to a real transcript:
// where the decision runs, what a digest may never lose, that an unfitting
// compaction fails the run instead of truncating it, that the calibrated
// budget is used for the next decision, and that a compact-point session
// stays byte-identical to one with no policy.

// sameMessages reports whether two transcripts are byte-for-byte identical.
// Message carries a tool-call slice and is not comparable with ==, so the
// transcript's own fields are compared directly.
func sameMessages(a, b []Message) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Role != b[i].Role || a[i].Content != b[i].Content ||
			a[i].ToolCallID != b[i].ToolCallID || len(a[i].ToolCalls) != len(b[i].ToolCalls) {
			return false
		}
		for j := range a[i].ToolCalls {
			if a[i].ToolCalls[j] != b[i].ToolCalls[j] {
				return false
			}
		}
	}
	return true
}

// recordingSink captures the compaction telemetry the session emits.
type recordingSink struct {
	events []CompactEvent
}

func (s *recordingSink) SessionCompacted(event CompactEvent) {
	s.events = append(s.events, event)
}

// decodeRequest parses one recorded outbound chat request into the wire shape
// the gateway receives.
func decodeRequest(t *testing.T, rawBody string) wireRequest {
	t.Helper()
	var request wireRequest
	if err := json.Unmarshal([]byte(rawBody), &request); err != nil {
		t.Fatalf("outbound request is not the wire shape: %v", err)
	}
	return request
}

// digestFromRequest returns the compaction digest carried by one recorded
// outbound request. Reading the digest itself rather than the whole body is
// what makes a settled-state assertion specific: the pinned brief repeats the
// same settled text on every request, so a whole-body search would pass even
// with an empty digest.
func digestFromRequest(t *testing.T, rawBody string) string {
	t.Helper()
	for _, message := range decodeRequest(t, rawBody).Messages {
		if strings.Contains(message.Content, "[compacted:") {
			return message.Content
		}
	}
	return ""
}

// assertToolResultsDeclared enforces the invariant every OpenAI-compatible
// gateway enforces and a mock cannot: a tool result must answer a tool call
// that an assistant message in the same request declared. A digest inserted
// between them produces an orphaned result and a 400 from the provider.
func assertToolResultsDeclared(t *testing.T, rawBody string) {
	t.Helper()
	declared := map[string]bool{}
	for _, message := range decodeRequest(t, rawBody).Messages {
		for _, call := range message.ToolCalls {
			declared[call.ID] = true
		}
		if message.Role == "tool" && !declared[message.ToolCallID] {
			t.Fatalf("request carries a tool result for %q that no assistant message declares:\n%s", message.ToolCallID, rawBody)
		}
	}
}

// compactBudget resolves a coordinator budget with a declared limit of
// limit tokens and no output reservation, so tests state the numbers the
// transcript must cross.
func compactBudget(t *testing.T, limit int) ContextBudget {
	t.Helper()
	budget, err := ResolveContextBudget(ContextBudgetInput{
		Role:                   RoleCoordinator,
		DeclaredMaxInputTokens: limit,
		ProbedMaxInputTokens:   limit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return budget
}

// appendTurns grows the transcript by n assistant/tool turn pairs of a
// predictable cost. The pairs are appended in order so the last one is the
// session's most recent turn.
func appendTurns(session *Session, n int) {
	for i := 0; i < n; i++ {
		session.Append(Message{Role: "assistant", Content: strings.Repeat("a", 100*tokensPerChar)})
		session.Append(Message{Role: "tool", Content: strings.Repeat("t", 100*tokensPerChar), ToolCallID: "c"})
	}
}

// transcriptWithTurns builds a session whose only assistant message is the
// final one, so the most-recent-turn protection covers exactly that message
// and everything digested is strictly older work. Fifteen turn pairs reach
// ~3050 tokens, past the 3000-token compact point of a 4000-token budget.
func transcriptWithTurns(t *testing.T, budget ContextBudget, turns int) *Session {
	t.Helper()
	session := &Session{budget: budget, role: RoleCoordinator}
	session.Pin(Message{Role: "system", Content: "goal"})
	appendTurns(session, turns)
	return session
}

// TestCompactIsANoOpBelowTheCompactPoint pins the boundary: a transcript
// under the compact point is never touched, and never emits an event. A
// harness that compacted eagerly would rewrite turns the model still needs.
func TestCompactIsANoOpBelowTheCompactPoint(t *testing.T) {
	// Compact point is 75% of 4000 = 3000 tokens. Five turns of 101+26
	// tokens plus the pinned goal stay well under it.
	session := transcriptWithTurns(t, compactBudget(t, 4000), 5)
	before := append([]Message(nil), session.Messages...)
	sink := &recordingSink{}
	session.compactions = sink

	event, compacted, err := session.Compact(nil)
	if err != nil {
		t.Fatalf("Compact() error = %v, want no error below the compact point", err)
	}
	if compacted {
		t.Fatalf("Compact() compacted below the compact point: %+v", event)
	}
	if !sameMessages(session.Messages, before) {
		t.Fatalf("transcript changed below the compact point: %d messages, want %d", len(session.Messages), len(before))
	}
	if len(sink.events) != 0 {
		t.Fatalf("a no-op compaction emitted %d events, want 0", len(sink.events))
	}
}

// TestCompactAtCompactPointPreservesOrderAndEmits is the accepted case: the
// transcript shrinks, the digest sits where the first digested message was,
// every retained message keeps its bytes, and the telemetry carries the full
// accounting an operator reads.
func TestCompactAtCompactPointPreservesOrderAndEmits(t *testing.T) {
	// Compact point 75% of 4000 = 3000. The turns below total ~3800 tokens
	// before the pinned goal, so the boundary fires.
	session := transcriptWithTurns(t, compactBudget(t, 4000), 15)
	sink := &recordingSink{}
	session.compactions = sink
	original := append([]Message(nil), session.Messages...)
	var originalBefore int
	for _, message := range original {
		originalBefore += estimateMessageTokens(message)
	}
	lastTurn := original[len(original)-2:]

	event, compacted, err := session.Compact(nil)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if !compacted {
		t.Fatalf("Compact() did not fire at the compact point; before = %d", originalBefore)
	}
	if len(session.Messages) >= len(original) {
		t.Fatalf("transcript did not shrink: %d messages, want fewer than %d", len(session.Messages), len(original))
	}

	// Every retained message is exactly where it was: the pinned goal, then
	// the digest in place of the first digested turn, then the untouched
	// turns that followed it — in order, byte for byte.
	if !sameMessages(session.Messages[:1], original[:1]) {
		t.Fatalf("pinned goal changed or moved: %+v", session.Messages[0])
	}
	digest := session.Messages[1]
	if digest.Role != "system" || !strings.Contains(digest.Content, "compacted") {
		t.Fatalf("digest = %+v, want a system message naming what it compacted", digest)
	}
	retained := session.Messages[2:]
	if len(retained) >= len(original) || !sameMessages(retained, original[len(original)-len(retained):]) {
		t.Fatalf("retained tail is not the untouched tail of the original transcript: %d of %d", len(retained), len(original))
	}
	if !sameMessages(session.Messages[len(session.Messages)-2:], lastTurn) {
		t.Fatal("the most recent assistant turn and its tool result were changed or digested")
	}

	// The event carries the counts an operator reconciles against the
	// transcript the model actually sees: after is the exact cost of the
	// applied transcript, not the planner's upper-bound estimate.
	actual := 0
	for _, message := range session.Messages {
		actual += estimateMessageTokens(message)
	}
	if event.Role != RoleCoordinator {
		t.Fatalf("event role = %q, want %q", event.Role, RoleCoordinator)
	}
	if event.Model != "" {
		t.Fatalf("event model = %q, want the session's bound model", event.Model)
	}
	if event.Before != originalBefore {
		t.Fatalf("event before = %d, want %d", event.Before, originalBefore)
	}
	if event.After >= event.Before {
		t.Fatalf("event after = %d, want fewer than before = %d", event.After, event.Before)
	}
	if event.NetSaved != event.Before-event.After {
		t.Fatalf("event net = %d, want before-after = %d", event.NetSaved, event.Before-event.After)
	}
	if event.After != actual {
		t.Fatalf("event after = %d does not match the compacted transcript's %d tokens; the telemetry an operator reconciles must be exact", event.After, actual)
	}
	if !session.budget.FitsAfterCompaction(actual) {
		t.Fatalf("the compacted transcript (%d tokens) does not fit the effective limit", actual)
	}
	if len(sink.events) != 1 || sink.events[0] != event {
		t.Fatalf("sink events = %+v, want exactly the returned event", sink.events)
	}
}

// TestCompactPreservesSettledStateVerbatim pins the settled-state rule: the
// pinned brief survives, and the digest re-injects the brief's decisions and
// owned files byte-for-byte. A delegate that loses its settled decisions
// re-derives them, which is how compacted work contradicts the plan.
func TestCompactPreservesSettledStateVerbatim(t *testing.T) {
	budget := compactBudget(t, 4000)
	brief := Brief{
		ID:           "b1",
		Role:         "coder",
		Objective:    "Implement the thing",
		SuccessCheck: "tests pass",
		Decisions:    []Decision{{Summary: "storage", Value: "git, no PVC"}, {Summary: "merge", Value: "never, human gate"}},
		OwnedFiles:   []string{"internal/harness/session.go", "internal/harness/brief.go"},
	}
	session := &Session{budget: budget, role: brief.Role}
	session.Pin(Message{Role: "system", Content: briefText(brief) + "\n\n" + subagentFraming})
	session.Settle(settledStateBlock(brief))
	appendTurns(session, 15)
	pinned := session.Messages[:1]

	_, compacted, err := session.Compact(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !compacted {
		t.Fatal("compaction did not fire over a grown brief transcript")
	}
	if !sameMessages(session.Messages[:1], pinned) {
		t.Fatal("the pinned brief message was changed or digested")
	}
	digest := session.Messages[1]
	for _, fragment := range []string{
		"Settled decisions (apply exactly, do not re-derive):",
		"- storage: git, no PVC",
		"- merge: never, human gate",
		"Owned files: internal/harness/session.go, internal/harness/brief.go",
	} {
		if !strings.Contains(digest.Content, fragment) {
			t.Fatalf("digest dropped settled state %q:\n%s", fragment, digest.Content)
		}
	}
	if !strings.Contains(digest.Content, settledStateBlock(brief)) {
		t.Fatal("the digest does not carry the settled-state block verbatim")
	}
	// The most recent turn survives verbatim: the model must still be able to
	// read what it last said and the tool result that answered it.
	if session.Messages[len(session.Messages)-2].Role != "assistant" {
		t.Fatalf("retained tail = %+v, want the assistant turn last", session.Messages[len(session.Messages)-2])
	}
}

// TestCompactFailsClosedWhenProtectedStateCannotFit pins the fail-closed
// rule: when even digesting every unprotected message cannot satisfy the
// post-condition, compaction errors and the transcript is byte-for-byte
// unchanged. Truncating the protected tail to fit would silently delete the
// task the model is working on.
func TestCompactFailsClosedWhenProtectedStateCannotFit(t *testing.T) {
	// Compact point 75% of 200 = 150 tokens. The pinned goal alone exceeds
	// the effective limit, so no digest span can bring the prompt under it.
	budget := compactBudget(t, 200)
	session := transcriptWithTurns(t, budget, 6)
	before := append([]Message(nil), session.Messages...)

	_, compacted, err := session.Compact(nil)
	if err == nil {
		t.Fatalf("Compact() compacted = %v, want an error when protected state cannot fit", compacted)
	}
	if !errors.Is(err, ErrCompactionCannotFit) {
		t.Fatalf("Compact() error = %v, want ErrCompactionCannotFit", err)
	}
	if !sameMessages(session.Messages, before) {
		t.Fatalf("transcript changed after a failed compaction: %d messages, want %d unchanged", len(session.Messages), len(before))
	}
}

// TestCompactHysteresisDoesNotRefire pins the calibrated-budget rule: the
// boundary immediately after a compaction must not re-fire on the state it
// just produced, and a later boundary with a grown transcript must compact
// again. Using the original budget after a compaction re-digests the digest
// and fails closed even though the post-condition held.
func TestCompactHysteresisDoesNotRefire(t *testing.T) {
	session := transcriptWithTurns(t, compactBudget(t, 4000), 19)
	sink := &recordingSink{}
	session.compactions = sink
	original := session.budget

	_, compacted, err := session.Compact(nil)
	if err != nil || !compacted {
		t.Fatalf("first Compact() = %v, %v; want a compaction", compacted, err)
	}
	afterFirst := append([]Message(nil), session.Messages...)

	// The fixture only tells the calibrated budget from the original one if
	// the compacted transcript still sits above the original compact point:
	// that is exactly the state in which the original budget fires again.
	applied := 0
	for _, message := range session.Messages {
		applied += estimateMessageTokens(message)
	}
	if applied < original.CompactPoint {
		t.Fatalf("degenerate fixture: the compacted transcript is %d tokens, below the original compact point %d, so this test cannot distinguish the calibrated budget from the original", applied, original.CompactPoint)
	}
	if session.budget.CompactPoint <= original.CompactPoint {
		t.Fatalf("the calibrated compact point is %d, want above the original %d", session.budget.CompactPoint, original.CompactPoint)
	}
	if !original.ShouldCompact(applied) {
		t.Fatalf("the compacted transcript at %d tokens would not fire the original budget's compact point %d; the fixture cannot exercise hysteresis", applied, original.CompactPoint)
	}
	if session.budget.ShouldCompact(applied) {
		t.Fatalf("the calibrated budget fires at %d tokens against a compact point of %d; hysteresis did not take", applied, session.budget.CompactPoint)
	}

	// The next boundary, on the just-compacted transcript, is a no-op.
	_, compacted, err = session.Compact(nil)
	if err != nil {
		t.Fatalf("second Compact() error = %v", err)
	}
	if compacted {
		t.Fatal("the boundary right after a compaction re-fired on the just-compacted state")
	}
	if len(session.Messages) != len(afterFirst) {
		t.Fatalf("second Compact() rewrote the transcript: %d messages, want %d", len(session.Messages), len(afterFirst))
	}

	// Growth past the calibrated compact point compacts again.
	appendTurns(session, 15)
	_, compacted, err = session.Compact(nil)
	if err != nil {
		t.Fatalf("third Compact() error = %v", err)
	}
	if !compacted {
		t.Fatal("a grown transcript past the compact point must compact again")
	}
	if len(sink.events) != 2 {
		t.Fatalf("sink events = %d, want 2 (first compaction and the re-compaction)", len(sink.events))
	}
}

// TestCompactDoesNotDigestTheRecentTurn pins the protection rule on its own:
// with several digestable older turns, only the pinned head and the most
// recent turn survive as messages. Everything between them is digest, not
// loss — the transcript shrinks, and the retained tail is intact.
func TestCompactDoesNotDigestTheRecentTurn(t *testing.T) {
	session := transcriptWithTurns(t, compactBudget(t, 4000), 15)
	lastAssistant := session.Messages[len(session.Messages)-2:]
	lastTool := session.Messages[len(session.Messages)-1:]

	_, compacted, err := session.Compact(nil)
	if err != nil || !compacted {
		t.Fatalf("Compact() = %v, %v; want a compaction", compacted, err)
	}
	if !sameMessages(session.Messages[len(session.Messages)-2:], lastAssistant) {
		t.Fatalf("the most recent assistant turn was changed or digested: %+v", session.Messages[len(session.Messages)-2:])
	}
	if !sameMessages(session.Messages[len(session.Messages)-1:], lastTool) {
		t.Fatal("the tool result answering the most recent turn was changed or digested")
	}
}

// TestZeroBudgetNeverCompacts pins the no-policy rule: with no declared and
// verified context window the harness owns no policy, so an arbitrarily
// large transcript is left exactly as it is. Absence of a budget is not an
// unbounded context, and it never licenses a compaction.
func TestZeroBudgetNeverCompacts(t *testing.T) {
	session := &Session{}
	session.Pin(Message{Role: "system", Content: "goal"})
	appendTurns(session, 40)
	before := append([]Message(nil), session.Messages...)
	if _, compacted, err := session.Compact(nil); err != nil || compacted {
		t.Fatalf("Compact() = %v, %v; want a no-op under a zero budget", compacted, err)
	}
	if len(session.Messages) != len(before) {
		t.Fatalf("a zero budget rewrote a %d-message transcript", len(before))
	}
}

// TestNewCoordinatorFailsClosedOnAnUnresolvableBudget pins the startup seam:
// a role the deployment declared a budget for and could not resolve must
// fail the harness closed, not run on a context window nobody verified.
func TestNewCoordinatorFailsClosedOnAnUnresolvableBudget(t *testing.T) {
	gateway, _, cleanup := gatewayFor()
	defer cleanup()
	_, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		ContextBudgets: map[string]ContextBudgetInput{
			RoleCoordinator: {Role: RoleCoordinator}, // neither declared nor verified
		},
	})
	if !errors.Is(err, ErrNoContextCapacity) {
		t.Fatalf("NewCoordinator() error = %v, want ErrNoContextCapacity", err)
	}
}

// TestRunFailsClosedWhenCompactionCannotFit pins the coordinator's half of the
// fail-closed rule: a compaction that cannot fit is a trusted-control failure
// and ends the run as an infrastructure failure, never a silent truncation
// and never an undeclared ending.
func TestRunFailsClosedWhenCompactionCannotFit(t *testing.T) {
	gateway, mock, cleanup := gatewayFor()
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		ContextBudgets: map[string]ContextBudgetInput{
			// Effective limit far below the goal plus framing the pinned
			// system message carries: the protected tail can never fit.
			RoleCoordinator: {Role: RoleCoordinator, DeclaredMaxInputTokens: 10, ProbedMaxInputTokens: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err == nil {
		t.Fatalf("harness result = %+v, want an infrastructure failure", result)
	}
	if !errors.Is(result.Err, ErrCompactionCannotFit) {
		t.Fatalf("error = %v, want ErrCompactionCannotFit", result.Err)
	}
	if !strings.Contains(result.Err.Error(), "step-boundary compaction") {
		t.Fatalf("error = %v, want the harness's compaction failure named", result.Err)
	}
	if result.Outcome != "" {
		t.Fatalf("outcome = %q, want no declared outcome on an infrastructure failure", result.Outcome)
	}
	if mock.requestCount() != 0 {
		t.Fatalf("the harness sent %d model requests; it must not run after a failed compaction", mock.requestCount())
	}
}

// TestRunCompactsAcrossItsTurnsAndReportsTelemetry pins the whole loop for
// the coordinator's own session: a run whose transcript crosses the compact
// point compacts at a step boundary and the telemetry names the role and the
// bound model. The budget is small and the turns long because a real worker
// tool result is a few dozen tokens — only that combination puts a scripted
// run's growth across the compact point.
func TestRunCompactsAcrossItsTurnsAndReportsTelemetry(t *testing.T) {
	// Effective limit 1600, compact point 1200. The pinned goal plus
	// harness framing is ~400 tokens and each turn ~215, so the fourth
	// boundary crosses the point.
	turn := strings.Repeat("reasoning about the workspace. ", 25)
	script := make([]gatewayResponse, 0, 6)
	for i := 0; i < 5; i++ {
		script = append(script, gatewayResponse{body: sse(
			contentChunk(turn),
			toolCallChunk(0, "call-shell", toolShell, `{"command":"echo hi"}`),
			finishChunk("tool_calls"),
		)})
	}
	script = append(script, gatewayResponse{body: sse(
		contentChunk(`{"outcome":"blocked_external","missing":"a merge"}`), finishChunk("stop"))})

	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	gateway, _, cleanup := gatewayFor(script...)
	defer cleanup()
	sink := &recordingSink{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: snapshotProvider("snap-tip"),
		ContextBudgets: map[string]ContextBudgetInput{
			RoleCoordinator: {Role: RoleCoordinator, DeclaredMaxInputTokens: 1600, ProbedMaxInputTokens: 1600},
		},
		Compactions: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err != nil {
		t.Fatalf("harness result = %+v", result)
	}
	if len(sink.events) == 0 {
		t.Fatal("a run whose transcript crossed the compact point emitted no compaction event")
	}
	for _, event := range sink.events {
		if event.Role != RoleCoordinator {
			t.Fatalf("event role = %q, want %q", event.Role, RoleCoordinator)
		}
		if event.Model != "test/coordinator" {
			t.Fatalf("event model = %q, want the coordinator's bound model", event.Model)
		}
		if event.NetSaved <= 0 || event.After >= event.Before {
			t.Fatalf("event = %+v, want a positive reclaim", event)
		}
	}
}

// TestRunBelowCompactPointLeavesTheTranscriptAlone pins that a run that
// never crosses the compact point is untouched: no compaction, no event, and
// every request the gateway receives is byte-identical to the one it would
// have received with no budget configured at all. Comparing the recorded
// requests is the assertion that has teeth — a harness that rewrote the
// transcript while emitting nothing would pass a bare "no events" check.
func TestRunBelowCompactPointLeavesTheTranscriptAlone(t *testing.T) {
	// A compact point far above anything this short run reaches.
	script := make([]gatewayResponse, 0, 3)
	for i := 0; i < 2; i++ {
		script = append(script, gatewayResponse{body: sse(
			contentChunk("working"),
			toolCallChunk(0, "call-shell", toolShell, `{"command":"echo hi"}`),
			finishChunk("tool_calls"),
		)})
	}
	script = append(script, gatewayResponse{body: sse(
		contentChunk(`{"outcome":"no_change_needed","evidence":"already done"}`), finishChunk("stop"))})

	run := func(budgets map[string]ContextBudgetInput, sink *recordingSink) []string {
		t.Helper()
		worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
		defer cleanupWorker()
		worker.completeAll = true
		worker.snapshotTip = "snap-tip"
		gateway, mock, cleanup := gatewayFor(script...)
		defer cleanup()
		coordinator, err := NewCoordinator(CoordinatorConfig{
			Gateway:        gateway,
			Bindings:       testBindings(),
			Worker:         delegator,
			Snapshot:       snapshotProvider("snap-tip"),
			ContextBudgets: budgets,
			Compactions:    sink,
		})
		if err != nil {
			t.Fatal(err)
		}
		if result := coordinator.Run(context.Background(), testInvocation()); result.Err != nil {
			t.Fatalf("harness result = %+v", result)
		}
		bodies := make([]string, 0, len(mock.calls))
		for _, call := range mock.calls {
			bodies = append(bodies, call.rawBody)
		}
		return bodies
	}

	sink := &recordingSink{}
	budgeted := run(map[string]ContextBudgetInput{
		RoleCoordinator: {Role: RoleCoordinator, DeclaredMaxInputTokens: 100000, ProbedMaxInputTokens: 100000},
	}, sink)
	unbudgeted := run(nil, nil)

	if len(sink.events) != 0 {
		t.Fatalf("emitted %d compaction events below the compact point, want 0", len(sink.events))
	}
	if len(budgeted) == 0 || len(budgeted) != len(unbudgeted) {
		t.Fatalf("the two runs sent %d and %d requests; the comparison needs the same script", len(budgeted), len(unbudgeted))
	}
	for i := range budgeted {
		if budgeted[i] != unbudgeted[i] {
			t.Fatalf("request %d differs between a budgeted run and an unbudgeted one:\nwith budget: %s\nwithout:     %s", i, budgeted[i], unbudgeted[i])
		}
	}
}

// TestRunBriefFailsClosedOnUnfittableCompaction pins the delegate half of the
// fail-closed rule. A sub-agent whose protected brief and most recent turn
// cannot fit ends as an ordinary brief failure carrying a bounded,
// tool-visible explanation: the coordinator owns the brief's fate and can
// re-delegate under a fresh ID. What it must never do is continue on a
// truncated transcript as if the compaction had worked.
func TestRunBriefFailsClosedOnUnfittableCompaction(t *testing.T) {
	gateway, mock, cleanup := gatewayFor()
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		ContextBudgets: map[string]ContextBudgetInput{
			"coder": {Role: "coder", DeclaredMaxInputTokens: 10, ProbedMaxInputTokens: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	summary, completed, unavailable := coordinator.runBrief(context.Background(), Brief{
		ID: "b1", Role: "coder", Objective: "Implement the thing", SuccessCheck: "tests pass",
	})
	if completed || unavailable != nil {
		t.Fatalf("runBrief = %q, %v, %v; want an ordinary brief failure", summary, completed, unavailable)
	}
	if !strings.Contains(summary, "could not be compacted") {
		t.Fatalf("brief summary = %q, want the compaction failure named", summary)
	}
	if strings.Contains(summary, "\n") || len(summary) > 500 {
		t.Fatalf("brief summary is not bounded tool data: %q", summary)
	}
	if mock.requestCount() != 0 {
		t.Fatalf("the harness sent %d model requests; it must not run after a failed compaction", mock.requestCount())
	}
}

// TestRunBriefCompactsAndCarriesSettledState drives a real delegate session to
// a step-boundary compaction. The coder's brief is pinned, its settled
// decisions and owned files are re-injected verbatim into the digest, and the
// delegate still reaches its final result.
func TestRunBriefCompactsAndCarriesSettledState(t *testing.T) {
	turn := strings.Repeat("reading the repository. ", 40)
	gateway, mock, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			contentChunk(turn),
			toolCallChunk(0, "call-1", toolShell, `{"command":"ls"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			contentChunk(turn),
			toolCallChunk(0, "call-2", toolShell, `{"command":"ls -R"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk("the work is done"), finishChunk("stop"))},
	)
	defer cleanup()
	sink := &recordingSink{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		ContextBudgets: map[string]ContextBudgetInput{
			"coder": {Role: "coder", DeclaredMaxInputTokens: 800, ProbedMaxInputTokens: 800},
		},
		Compactions: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	brief := Brief{
		ID:           "b1",
		Role:         "coder",
		Objective:    "Implement the thing",
		SuccessCheck: "tests pass",
		Decisions:    []Decision{{Summary: "storage", Value: "git, no PVC"}},
		OwnedFiles:   []string{"internal/harness/session.go"},
	}
	summary, completed, unavailable := coordinator.runBrief(context.Background(), brief)
	if unavailable != nil || !completed {
		t.Fatalf("runBrief = %q, %v, %v; want a completed brief", summary, completed, unavailable)
	}
	if mock.requestCount() != 3 {
		t.Fatalf("the delegate sent %d requests, want 3", mock.requestCount())
	}
	if len(sink.events) == 0 {
		t.Fatal("a delegate transcript over the compact point emitted no compaction event")
	}
	for _, event := range sink.events {
		if event.Role != "coder" {
			t.Fatalf("event role = %q, want %q", event.Role, "coder")
		}
		if event.Model != "test/coder" {
			t.Fatalf("event model = %q, want the coder's bound model", event.Model)
		}
	}
	// The digest the delegate actually reads carries the settled state
	// verbatim, so a compacted brief still applies the decisions it was
	// given instead of re-deriving them. The assertion reads the digest
	// message itself: the pinned brief repeats the same text on every
	// request, so searching the whole body would pass with an empty digest.
	last := mock.calls[len(mock.calls)-1].rawBody
	digest := digestFromRequest(t, last)
	if digest == "" {
		t.Fatalf("the delegate's final request carries no compaction digest:\n%s", last)
	}
	for _, fragment := range []string{
		"Settled decisions (apply exactly",
		"storage: git, no PVC",
		"Owned files: internal/harness/session.go",
	} {
		if !strings.Contains(digest, fragment) {
			t.Fatalf("the compaction digest dropped settled state %q:\n%s", fragment, digest)
		}
	}
}

// TestRunCompactionNeverOrphansToolResults pins the digest's placement rule
// against the request a real gateway receives. A turn that requests several
// tools produces one assistant message followed by several results; a digest
// inserted between them leaves results whose tool calls nothing declares, and
// every OpenAI-compatible gateway rejects that request. A mock that decodes
// into the same Go structs cannot see the defect, so the assertion reads the
// outbound wire shape instead.
func TestRunCompactionNeverOrphansToolResults(t *testing.T) {
	turn := strings.Repeat("deciding what to run next. ", 25)
	script := make([]gatewayResponse, 0, 6)
	for i := 0; i < 5; i++ {
		script = append(script, gatewayResponse{body: sse(
			contentChunk(turn),
			toolCallChunk(0, fmt.Sprintf("call-%d-a", i), toolShell, `{"command":"echo hi"}`),
			toolCallChunk(1, fmt.Sprintf("call-%d-b", i), toolShell, `{"command":"echo ho"}`),
			finishChunk("tool_calls"),
		)})
	}
	script = append(script, gatewayResponse{body: sse(
		contentChunk(`{"outcome":"blocked_external","missing":"a merge"}`), finishChunk("stop"))})

	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	gateway, mock, cleanup := gatewayFor(script...)
	defer cleanup()
	sink := &recordingSink{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: snapshotProvider("snap-tip"),
		ContextBudgets: map[string]ContextBudgetInput{
			RoleCoordinator: {Role: RoleCoordinator, DeclaredMaxInputTokens: 1600, ProbedMaxInputTokens: 1600},
		},
		Compactions: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err != nil {
		t.Fatalf("harness result = %+v", result)
	}
	if len(sink.events) == 0 {
		t.Fatal("a run whose transcript crossed the compact point emitted no compaction event, so this test never exercised the digest's placement")
	}
	compacted := false
	for _, call := range mock.calls {
		if digestFromRequest(t, call.rawBody) != "" {
			compacted = true
		}
		assertToolResultsDeclared(t, call.rawBody)
	}
	if !compacted {
		t.Fatal("no outbound request carried a digest; the placement rule was never exercised")
	}
}

// TestCompactFailsRatherThanDigestTheRecentTurn is the load-bearing case for
// the recent-turn protection. This transcript is shaped so the only way to
// reclaim enough tokens is to digest the most recent turn: the older turns
// are far too small to clear the digest's own cost. The correct outcome is
// failure, because the turn the next model call must answer is exactly what a
// compaction may never replace. A harness that digests it here succeeds at
// reclaiming — and hands the model a task state it can no longer read.
func TestCompactFailsRatherThanDigestTheRecentTurn(t *testing.T) {
	// Compact point 975 of a 1300-token limit; the transcript below is
	// ~1040 tokens.
	session := &Session{budget: compactBudget(t, 1300), role: RoleCoordinator}
	session.Pin(Message{Role: "system", Content: "goal"})
	session.Append(Message{Role: "assistant", Content: strings.Repeat("a", 10*tokensPerChar)})
	session.Append(Message{Role: "tool", Content: strings.Repeat("t", 10*tokensPerChar), ToolCallID: "c1"})
	session.Append(Message{Role: "assistant", Content: strings.Repeat("a", 1000*tokensPerChar), ToolCalls: []ToolCall{{ID: "c2", Name: toolShell, Arguments: `{"command":"ls"}`}}})
	before := append([]Message(nil), session.Messages...)

	_, compacted, err := session.Compact(nil)
	if err == nil {
		t.Fatalf("Compact() compacted = %v, want a failure rather than digesting the most recent turn", compacted)
	}
	if !errors.Is(err, ErrCompactionCannotFit) {
		t.Fatalf("Compact() error = %v, want ErrCompactionCannotFit", err)
	}
	if !sameMessages(session.Messages, before) {
		t.Fatalf("the transcript changed after a compaction that could only succeed by digesting the most recent turn: %d messages, want %d", len(session.Messages), len(before))
	}
}

// --- tool-declaration accounting in the boundary estimate -------------------
//
// Gateway.StreamChat serializes ChatRequest.Tools into every provider
// request, so the declarations consume input capacity each turn even though
// they are not transcript. A boundary that estimated only messages could
// report a fitting post-condition for a request the effective limit cannot
// hold. These tests pin that Compact accounts for the declarations of the
// turn it guards.

// TestCompactCountsToolDeclarationsTowardTheCompactPoint is the false-pass
// counterexample: the same transcript is a no-op with no declarations and a
// compaction with a schema large enough to cross the compact point. Omitting
// the declarations from the account reports "no compaction needed" for a
// request that has already reached it.
func TestCompactCountsToolDeclarationsTowardTheCompactPoint(t *testing.T) {
	budget := compactBudget(t, 4000) // compact point 3000, no reservation
	tools := []ToolDef{{Name: "edit", Description: strings.Repeat("d", 1200)}}

	session := transcriptWithTurns(t, budget, 14)
	transcriptTokens := 0
	for _, message := range session.Messages {
		transcriptTokens += estimateMessageTokens(message)
	}
	toolTokens := estimateToolTokens(tools)
	if transcriptTokens >= budget.CompactPoint {
		t.Fatalf("test shape invalid: transcript alone (%d) already reaches the compact point %d", transcriptTokens, budget.CompactPoint)
	}
	if transcriptTokens+toolTokens < budget.CompactPoint {
		t.Fatalf("test shape invalid: transcript plus declarations (%d) does not reach the compact point %d", transcriptTokens+toolTokens, budget.CompactPoint)
	}

	sink := &recordingSink{}
	session.compactions = sink
	event, compacted, err := session.Compact(tools)
	if err != nil {
		t.Fatalf("Compact(tools) error = %v, want a compaction the transcript alone would not trigger", err)
	}
	if !compacted {
		t.Fatal("Compact(tools) was a no-op; the tool declarations were left out of the boundary account")
	}
	if event.Before != transcriptTokens+toolTokens {
		t.Fatalf("event.Before = %d, want transcript %d + declarations %d", event.Before, transcriptTokens, toolTokens)
	}
	if event.Before-event.After != event.NetSaved {
		t.Fatalf("event counts disagree: Before %d - After %d != NetSaved %d", event.Before, event.After, event.NetSaved)
	}
	if !budget.FitsAfterCompaction(event.After) {
		t.Fatalf("post-condition violated: After %d exceeds effective limit %d", event.After, budget.EffectiveLimit)
	}

	// The identical transcript with no declarations is the negative control:
	// the compaction fired because of the schema, not the messages.
	control := transcriptWithTurns(t, budget, 14)
	if _, compacted, err := control.Compact(nil); err != nil || compacted {
		t.Fatalf("Compact(nil) = %v, %v; want a no-op below the compact point", compacted, err)
	}
}

// TestCompactFailsClosedWhenDeclarationsCannotFit is the fail-closed half:
// declarations so large that the protected state can never fit alongside
// them end the run with the transcript unchanged, rather than reporting a fit
// over messages alone. The same state without the declarations compacts
// normally, so the failure is attributable to the schema and not the turn.
func TestCompactFailsClosedWhenDeclarationsCannotFit(t *testing.T) {
	budget := compactBudget(t, 4000)
	tools := []ToolDef{{Name: "huge", Description: strings.Repeat("d", 16000)}}
	if estimateToolTokens(tools) <= budget.EffectiveLimit {
		t.Fatalf("test shape invalid: declarations (%d) must alone exceed the effective limit %d", estimateToolTokens(tools), budget.EffectiveLimit)
	}

	session := transcriptWithTurns(t, budget, 15)
	before := append([]Message(nil), session.Messages...)
	sink := &recordingSink{}
	session.compactions = sink

	_, compacted, err := session.Compact(tools)
	if !errors.Is(err, ErrCompactionCannotFit) {
		t.Fatalf("Compact(tools) error = %v, want ErrCompactionCannotFit", err)
	}
	if compacted {
		t.Fatal("Compact(tools) compacted = true for a request that cannot fit with its declarations counted")
	}
	if !sameMessages(session.Messages, before) {
		t.Fatal("the transcript changed after a compaction that must fail closed")
	}
	if len(sink.events) != 0 {
		t.Fatalf("a failed compaction emitted %d telemetry events, want none", len(sink.events))
	}

	// The identical transcript without declarations compacts: the failure
	// belongs to the request shape, not to the turns.
	control := transcriptWithTurns(t, budget, 15)
	if _, compacted, err := control.Compact(nil); err != nil || !compacted {
		t.Fatalf("Compact(nil) = %v, %v; want the same shape to compact without declarations", compacted, err)
	}
}

// TestEstimateToolTokensMatchesTheWireShape pins the estimator's floor and
// that it costs the JSON encoding of the parameter schema, the bytes the
// request actually carries.
func TestEstimateToolTokensMatchesTheWireShape(t *testing.T) {
	if got := estimateToolTokens(nil); got != 0 {
		t.Fatalf("estimateToolTokens(nil) = %d, want 0", got)
	}
	if got, want := estimateToolTokens([]ToolDef{{}}), toolWireEnvelopeChars/tokensPerChar; got != want {
		t.Fatalf("estimateToolTokens(empty tool) = %d, want the %d-token wire envelope", got, want)
	}
	bare := estimateToolTokens([]ToolDef{{Name: "t", Description: "d"}})
	withSchema := estimateToolTokens([]ToolDef{{Name: "t", Description: "d", Parameters: map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}}})
	if withSchema <= bare {
		t.Fatalf("parameter schema cost nothing: with %d, bare %d", withSchema, bare)
	}
}
