package controller

import (
	"strings"
	"testing"
)

// TestFailedCheckLogsCoversOnlyFailedConfiguredMechanicalChecks pins which
// evidence a repair context may carry: the failed checks the lane configures
// as mechanical, and nothing else. A passed configured check, a failed check
// the lane never named, and a pending check are not evidence of a failure,
// and a failed check without published output contributes nothing.
func TestFailedCheckLogsCoversOnlyFailedConfiguredMechanicalChecks(t *testing.T) {
	observation := PRObservation{
		PR:   "42",
		Head: "abc123",
		Checks: []CheckObservation{
			{Name: "unit", State: CheckStateFailed, Output: "unit failed"},
			{Name: "lint", State: CheckStatePassed, Output: "lint passed"},
			{Name: "ai-review", State: CheckStateFailed, Output: "review failed"},
			{Name: "typecheck", State: CheckStatePending, Output: "typecheck pending"},
			{Name: "build", State: CheckStateFailed},
		},
	}
	logs := failedCheckLogs(observation, mechanicalSelector([]string{"unit", "lint", "typecheck", "build"}))
	if !strings.Contains(logs, "unit failed") {
		t.Fatalf("logs %q must carry the failed configured check's output", logs)
	}
	if !strings.Contains(logs, "--- unit ---") {
		t.Fatalf("logs %q must name the check the output belongs to", logs)
	}
	for _, forbidden := range []string{"lint passed", "review failed", "typecheck pending"} {
		if strings.Contains(logs, forbidden) {
			t.Fatalf("logs %q carry evidence of a check that is not a failed configured mechanical check: %q", logs, forbidden)
		}
	}
	if strings.Contains(logs, "build") {
		t.Fatalf("logs %q carry a block for a failed check that published no output", logs)
	}
}

// TestFailedCheckLogsBoundsEachCheckAndTheTotal pins the bounds of the log
// evidence one repair context carries: at most 2 KiB per check and at most
// 8 KiB in total, with every cut marked so the coordinator can tell a short
// log from a cut one.
func TestFailedCheckLogsBoundsEachCheckAndTheTotal(t *testing.T) {
	observation := PRObservation{
		PR:   "42",
		Head: "abc123",
	}
	for _, name := range []string{"one", "two", "three", "four", "five", "six", "seven"} {
		observation.Checks = append(observation.Checks, CheckObservation{
			Name:   name,
			State:  CheckStateFailed,
			Output: strings.Repeat(name, ciparkMaxCheckLogBytes),
		})
	}
	logs := failedCheckLogs(observation, mechanicalSelector([]string{"one", "two", "three", "four", "five", "six", "seven"}))
	if len(logs) > ciparkMaxRepairLogBytes {
		t.Fatalf("logs are %d bytes, want at most %d", len(logs), ciparkMaxRepairLogBytes)
	}
	if !strings.Contains(logs, ciparkTruncatedLog) {
		t.Fatalf("logs %q must mark the cut evidence", logs)
	}
	// Every rendered block stays inside the per-check bound.
	for _, block := range strings.Split(logs, "--- ") {
		if !strings.Contains(block, "---") {
			continue
		}
		if len(block) > ciparkMaxCheckLogBytes {
			t.Fatalf("block %q is %d bytes, want at most %d", block, len(block), ciparkMaxCheckLogBytes)
		}
	}
}

// TestFailedCheckLogsWithoutASelector pins that evidence needs a configured
// selection: with no configured mechanical check, there is no evidence to
// carry.
func TestFailedCheckLogsWithoutASelector(t *testing.T) {
	observation := PRObservation{
		Checks: []CheckObservation{{Name: "unit", State: CheckStateFailed, Output: "unit failed"}},
	}
	if logs := failedCheckLogs(observation, nil); logs != "" {
		t.Fatalf("logs %q, want no evidence without a configured selection", logs)
	}
}
