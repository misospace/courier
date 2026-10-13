package executor

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

// testPayload mirrors the shape of every COURIER_TERMINATION producer's
// termination struct: a small set of fixed fields and a Reason string
// the line-fit loop is allowed to shrink. The executor and the control
// each define their own struct, but they share the same field shape —
// phase / result / exit_code / reason / outcome — so a single shared
// helper can drive both.
type testPayload struct {
	Phase    string `json:"phase"`
	Result   string `json:"result"`
	ExitCode int    `json:"exit_code"`
	Reason   string `json:"reason"`
	Outcome  string `json:"outcome,omitempty"`
}

func TestMarshalTerminationLine(t *testing.T) {
	line := MarshalTerminationLine(testPayload{Phase: "Verifying", Result: "success", ExitCode: 0, Reason: "ok"})
	if !strings.HasPrefix(line, TerminationLinePrefix) {
		t.Fatalf("line %q is missing the prefix %q", line, TerminationLinePrefix)
	}
	if !strings.HasSuffix(line, "\n") {
		t.Fatalf("line %q is missing the trailing newline", line)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(line, TerminationLinePrefix), "\n")
	var parsed testPayload
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("body is not valid JSON: %v: %q", err, body)
	}
	if parsed.Phase != "Verifying" || parsed.Result != "success" || parsed.ExitCode != 0 || parsed.Reason != "ok" {
		t.Fatalf("parsed payload = %+v", parsed)
	}
}

func TestMarshalTerminationLineFailsClosedOnBadPayload(t *testing.T) {
	// channels are not JSON-marshalable; MarshalTerminationLine must
	// return "" rather than panic or write a partial line that would
	// truncate the termination file.
	if got := MarshalTerminationLine(make(chan int)); got != "" {
		t.Fatalf("MarshalTerminationLine(bad payload) = %q, want empty", got)
	}
}

func TestFitTerminationLineFitsEscapeHeavyReason(t *testing.T) {
	// Eight-byte unit covering all six escaping characters: the two
	// quotes, two backslashes and newline double in length, while <, >
	// and & expand to six-byte \uXXXX escapes. 1024 units make exactly
	// 8192 raw bytes (8 KiB), which marshals well past the 4 KiB
	// budget; the line-fit loop must cut the reason until the line
	// fits, with phase / result / exit_code / outcome intact and a
	// non-empty reason.
	unit := `"` + `"` + `\` + `\` + "\n" + "<>&"
	if len(unit) != 8 {
		t.Fatalf("test unit is %d bytes, want 8", len(unit))
	}
	reason := strings.Repeat(unit, MaxTerminationLineBytes/len(unit))
	if len(reason) != MaxTerminationLineBytes {
		t.Fatalf("reason is %d bytes, want %d", len(reason), MaxTerminationLineBytes)
	}

	line := FitTerminationLine(&testPayload{
		Phase:    "NeedsHuman",
		Result:   "needs-human",
		ExitCode: 2,
		Outcome:  "needs_decision",
		Reason:   reason,
	})
	if len(line) > MaxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), MaxTerminationLineBytes)
	}

	body := strings.TrimSuffix(strings.TrimPrefix(line, TerminationLinePrefix), "\n")
	var parsed testPayload
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Phase != "NeedsHuman" {
		t.Fatalf("phase = %q, want %q", parsed.Phase, "NeedsHuman")
	}
	if parsed.Result != "needs-human" {
		t.Fatalf("result = %q, want %q", parsed.Result, "needs-human")
	}
	if parsed.ExitCode != 2 {
		t.Fatalf("exit_code = %d, want 2", parsed.ExitCode)
	}
	if parsed.Outcome != "needs_decision" {
		t.Fatalf("outcome = %q, want %q", parsed.Outcome, "needs_decision")
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the line-fit loop")
	}
	if !utf8.ValidString(parsed.Reason) {
		t.Fatalf("trimmed reason is not valid UTF-8: %q", parsed.Reason)
	}
	if !strings.HasSuffix(parsed.Reason, TerminationTruncationSuffix) {
		t.Fatalf("trimmed reason %q does not end with %q", parsed.Reason, TerminationTruncationSuffix)
	}
}

func TestFitTerminationLineLeavesShortReasonUntouched(t *testing.T) {
	payload := &testPayload{
		Phase:    "Verifying",
		Result:   "success",
		ExitCode: 0,
		Reason:   "short reason",
	}
	line := FitTerminationLine(payload)
	if !strings.Contains(line, `"reason":"short reason"`) {
		t.Fatalf("short reason was trimmed: %q", line)
	}
	if payload.Reason != "short reason" {
		t.Fatalf("payload reason was mutated to %q", payload.Reason)
	}
}

func TestFitTerminationLineKeepsUTF8ReasonOnShrink(t *testing.T) {
	// CJK is three UTF-8 bytes per rune and is not escaped by the JSON
	// marshaler, so a long run of it overruns the budget in raw bytes
	// and forces the line-fit loop to fire. The cut must land on a
	// rune boundary, leaving a valid-UTF-8, non-empty reason ending
	// with the truncation marker, and the line stays within the budget.
	reason := strings.Repeat("你", 1500)
	if len(reason) != 4500 {
		t.Fatalf("reason is %d bytes, want 4500", len(reason))
	}

	line := FitTerminationLine(&testPayload{
		Phase:    "Failed",
		Result:   "failure",
		ExitCode: 1,
		Reason:   reason,
	})
	if len(line) > MaxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), MaxTerminationLineBytes)
	}

	body := strings.TrimSuffix(strings.TrimPrefix(line, TerminationLinePrefix), "\n")
	var parsed testPayload
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the line-fit loop")
	}
	if !utf8.ValidString(parsed.Reason) {
		t.Fatalf("trimmed reason is not valid UTF-8: %q", parsed.Reason)
	}
	if !strings.HasSuffix(parsed.Reason, TerminationTruncationSuffix) {
		t.Fatalf("trimmed reason %q does not end with %q", parsed.Reason, TerminationTruncationSuffix)
	}
}

func TestFitTerminationLineFailsClosedOnNonPointer(t *testing.T) {
	// A non-pointer payload cannot have its Reason shrunk in place; the
	// helper must still produce a valid line rather than spin or
	// panic. The reason is left as-given; if the marshaled line already
	// fits, this is a no-op; if not, it falls back to the unmangled
	// marshal so the caller can still see the over-budget reason rather
	// than a half-built line.
	short := testPayload{Phase: "Verifying", Result: "success", ExitCode: 0, Reason: "ok"}
	line := FitTerminationLine(short)
	if !strings.Contains(line, `"reason":"ok"`) {
		t.Fatalf("non-pointer short reason was mangled: %q", line)
	}
}

func TestFitTerminationLineFailsClosedOnMissingReason(t *testing.T) {
	// A payload without a Reason field cannot be shrunk by the helper;
	// the unmangled MarshalTerminationLine is the safe fallback so the
	// producer still emits a handoff. The kubernetes budget may be
	// exceeded in this pathological case, but a non-empty line is
	// strictly more recoverable than nothing.
	type noReason struct {
		Phase string `json:"phase"`
	}
	line := FitTerminationLine(&noReason{Phase: "Verifying"})
	if !strings.Contains(line, `"phase":"Verifying"`) {
		t.Fatalf("missing-reason payload was mangled: %q", line)
	}
}

func TestShrinkTerminationReason(t *testing.T) {
	t.Run("closes a one-byte overflow", func(t *testing.T) {
		reason := strings.Repeat("a", 100)
		got := ShrinkTerminationReason(reason, 1)
		want := strings.Repeat("a", 100-len(TerminationTruncationSuffix)-1) + TerminationTruncationSuffix
		if got != want {
			t.Fatalf("ShrinkTerminationReason() = %q (%d bytes), want %q (%d bytes)", got, len(got), want, len(want))
		}
	})

	t.Run("reason at the marker floor is unchanged", func(t *testing.T) {
		reason := strings.Repeat("a", len(TerminationTruncationSuffix))
		got := ShrinkTerminationReason(reason, 1)
		if got != reason {
			t.Fatalf("ShrinkTerminationReason() = %q, want unchanged %q", got, reason)
		}
	})

	t.Run("content ending in the marker is not double-counted", func(t *testing.T) {
		reason := strings.Repeat("a", 50) + TerminationTruncationSuffix
		got := ShrinkTerminationReason(reason, 1)
		// The trailing marker is stripped before measuring, so the cut
		// applies to the 50-byte content and the marker is appended
		// exactly once.
		want := strings.Repeat("a", 50-len(TerminationTruncationSuffix)-1) + TerminationTruncationSuffix
		if got != want {
			t.Fatalf("ShrinkTerminationReason() = %q (%d bytes), want %q (%d bytes)", got, len(got), want, len(want))
		}
		if strings.HasSuffix(got, TerminationTruncationSuffix+TerminationTruncationSuffix) {
			t.Fatalf("ShrinkTerminationReason() double-appended the marker: %q", got)
		}
	})
}
