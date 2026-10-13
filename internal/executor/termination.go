package executor

import (
	"encoding/json"
	"reflect"
	"strings"
)

// The COURIER_TERMINATION handoff travels through the kubelet's pod
// termination-message budget (#188, #210). Two producers emit it today —
// the legacy courier-executor and the native courier-control — and the
// operator parses the prefix byte-for-byte, so neither the prefix nor the
// 4 KiB budget is allowed to drift between them. This file is the single
// place those shared constants and helpers live; both producers call into
// it instead of carrying their own copy.
const (
	// TerminationLinePrefix is the fixed leading string of every
	// COURIER_TERMINATION line. The operator's
	// parseCoordinatorTermination strips this prefix before unmarshaling,
	// so any change must land at both ends together.
	TerminationLinePrefix = "COURIER_TERMINATION "
	// MaxTerminationLineBytes is the Kubernetes pod termination-message
	// budget. The whole handoff line — prefix, JSON payload, trailing
	// newline — must stay at or under it, or the controller loses the
	// entire trusted handoff to truncation (#210).
	MaxTerminationLineBytes = 4096
	// TerminationTruncationSuffix is appended to a reason the line-fit
	// loop cut to fit the budget. It is short, ASCII, and ends the
	// reason on a non-rune-boundary so a downstream parser that reads
	// the reason sees an obvious "this was truncated" marker.
	TerminationTruncationSuffix = "... [truncated]"
)

// MarshalTerminationLine renders payload as the COURIER_TERMINATION
// handoff line: the fixed prefix, the marshaled JSON body, and a trailing
// newline. It is the single marshaling path used by every
// COURIER_TERMINATION producer so the operator's line-prefix parser and
// the controller's terminationMessage schema cannot drift between
// callers (#210). A marshal failure leaves no handoff line: writing an
// empty line would truncate the termination file and lose the trusted
// reason regardless.
func MarshalTerminationLine(payload any) string {
	body, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return TerminationLinePrefix + string(body) + "\n"
}

// FitTerminationLine renders the handoff line for payload, shrinking the
// payload's Reason field — UTF-8-safe, with the truncation marker —
// until the marshaled line fits MaxTerminationLineBytes. Phase / result /
// exit_code / outcome are never touched; reason always stays non-empty
// (it falls back to the marker floor when it cannot give more ground,
// and the loop stops at that floor so a fixed-fields overrun never
// spins). payload must be a non-nil pointer to a struct with a settable
// Reason string field. A non-pointer payload, a missing or non-string
// Reason field, or a marshal failure returns the unmangled
// MarshalTerminationLine so the worst case is "no fit," not "no
// handoff."
func FitTerminationLine(payload any) string {
	pv := reflect.ValueOf(payload)
	if pv.Kind() != reflect.Ptr || pv.IsNil() {
		return MarshalTerminationLine(payload)
	}
	pv = pv.Elem()
	rf := pv.FieldByName("Reason")
	if !rf.IsValid() || rf.Kind() != reflect.String || !rf.CanSet() {
		return MarshalTerminationLine(payload)
	}
	line := MarshalTerminationLine(payload)
	for len(line) > MaxTerminationLineBytes {
		reason := rf.String()
		shrunk := ShrinkTerminationReason(reason, len(line)-MaxTerminationLineBytes)
		if shrunk == reason {
			// The reason is at its floor and cannot give more ground; the
			// remaining overrun (if any) belongs to the fixed fields, which
			// FitTerminationLine never touches. Stop rather than spin.
			break
		}
		rf.SetString(shrunk)
		line = MarshalTerminationLine(payload)
	}
	return line
}

// ShrinkTerminationReason cuts raw reason bytes so a re-marshaled
// handoff line drops back within MaxTerminationLineBytes. overflow is
// how many bytes the current line exceeds the budget by. Each removed
// raw byte shrinks the escaped form by at least one byte (the JSON
// marshaler only ever grows non-ASCII or quoting characters), so
// removing overflow bytes plus the marker closes the gap; a UTF-8-safe
// cut and the marker keep the reason non-empty and the cut visible. It
// returns the reason unchanged when the reason is already at the marker
// floor, so the caller can stop without spinning. The trailing marker
// from a previous cut is stripped before measuring so the cut applies
// to the real content and the marker is appended exactly once, never
// double-appended.
func ShrinkTerminationReason(reason string, overflow int) string {
	const floor = len(TerminationTruncationSuffix)
	if len(reason) <= floor {
		return reason
	}
	base := reason
	if strings.HasSuffix(base, TerminationTruncationSuffix) {
		base = base[:len(base)-floor]
	}
	cut := overflow + floor
	limit := len(base) - cut
	if limit < 0 {
		limit = 0
	}
	// Never split a multi-byte rune: back off only over the UTF-8
	// continuation bytes at the cut point.
	for limit > 0 && base[limit]&0xC0 == 0x80 {
		limit--
	}
	return base[:limit] + TerminationTruncationSuffix
}
