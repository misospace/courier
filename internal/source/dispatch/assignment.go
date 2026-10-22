package dispatch

import (
	"strings"

	"github.com/misospace/courier/internal/source"
)

// The Kind vocabulary comes from the source.Assignment* constants in
// internal/source/source.go. Dispatch documents reasons as free text, not an
// enum, so Kind classification is heuristic by contract: the adapter
// classifies best-effort and core consumes only the normalized Kind.
const (
	// assignmentMaxReasons mirrors the CoderRun CRD cap on assignment
	// entries (MaxItems=16).
	assignmentMaxReasons = 16

	// assignmentMaxCodePoints mirrors the CoderRun CRD cap of 1024 Unicode
	// code points per assignment entry.
	assignmentMaxCodePoints = 1024
)

// newAssignment normalizes a Dispatch task's free-text reasons into a
// bounded source.Assignment. Reasons are untrusted task data: per entry,
// every rune below U+0020 except '\n' and '\t', plus U+007F, becomes a
// single space; the entry is trimmed, empty results are dropped, entries
// are capped at assignmentMaxCodePoints code points, and exact duplicates
// (after sanitization) collapse keeping the first occurrence. The kept
// list is then capped at assignmentMaxReasons entries. An offer whose
// reasons do not survive sanitization yields the zero Assignment.
func newAssignment(reasons []string) source.Assignment {
	kept := make([]string, 0, len(reasons))
	seen := make(map[string]struct{}, len(reasons))
	for _, reason := range reasons {
		entry := sanitizeReason(reason)
		if entry == "" {
			continue
		}
		if _, dup := seen[entry]; dup {
			continue
		}
		seen[entry] = struct{}{}
		kept = append(kept, entry)
		if len(kept) == assignmentMaxReasons {
			break
		}
	}
	if len(kept) == 0 {
		return source.Assignment{}
	}
	return source.Assignment{Kind: classifyAssignment(kept), Reasons: kept}
}

func sanitizeReason(reason string) string {
	var b strings.Builder
	for _, r := range reason {
		switch {
		case r == '\n' || r == '\t':
			b.WriteRune(r)
		case r < 0x20 || r == 0x7f:
			b.WriteByte(' ')
		default:
			b.WriteRune(r)
		}
	}
	entry := strings.TrimSpace(b.String())
	if entry == "" {
		return ""
	}
	if runes := []rune(entry); len(runes) > assignmentMaxCodePoints {
		// Re-trim after the cut so a boundary that lands inside whitespace
		// does not persist a trailing blank into the bounded entry.
		return strings.TrimSpace(string(runes[:assignmentMaxCodePoints]))
	}
	return entry
}

// classifyAssignment maps sanitized reasons to exactly one assignment kind.
// Matching is a case-insensitive substring test against the lowercased
// entries, in fixed priority order — merge-conflict, then ci-failure, then
// review-feedback: the first classification any entry triggers wins,
// priority over entry order. Any other non-empty assignment is other.
func classifyAssignment(reasons []string) string {
	entries := make([]string, len(reasons))
	for i, reason := range reasons {
		entries[i] = strings.ToLower(reason)
	}
	for _, entry := range entries {
		// "merge state change: dirty" is the GitHub mergeability marker a
		// conflicting PR produces without ever naming a conflict.
		if strings.Contains(entry, "conflict") || strings.Contains(entry, "merge state change: dirty") {
			return source.AssignmentMergeConflict
		}
	}
	for _, entry := range entries {
		if strings.Contains(entry, "check") && strings.Contains(entry, "fail") {
			return source.AssignmentCIFailure
		}
	}
	for _, entry := range entries {
		for _, marker := range []string{"review", "comment", "changes_requested", "changes requested"} {
			if strings.Contains(entry, marker) {
				return source.AssignmentReviewFeedback
			}
		}
	}
	return source.AssignmentOther
}
