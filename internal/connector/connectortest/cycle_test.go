package connectortest

import "testing"

// TestJoinCountsOneLogicalFailure is the join's contract: one core
// observation and one connector observation for the same cycle join to one
// logical failure, never to two.
func TestJoinCountsOneLogicalFailure(t *testing.T) {
	cycle := Cycle{Connector: "dispatch", ID: "cycle-1"}
	joined := Join([]Observation{
		{Cycle: cycle, Side: SideCore, Reason: "source-error"},
		{Cycle: cycle, Side: SideConnector, Reason: "http-5xx"},
	})
	if joined != 1 {
		t.Fatalf("Join = %d, want 1 for one failure counted on both sides", joined)
	}
}

// TestJoinIgnoresOneSidedObservations proves a cycle observed on only one
// side joins to nothing: the two counters must both see a failure before the
// join reports one.
func TestJoinIgnoresOneSidedObservations(t *testing.T) {
	cycle := Cycle{Connector: "dispatch", ID: "cycle-1"}
	coreOnly := Join([]Observation{
		{Cycle: cycle, Side: SideCore, Reason: "source-error"},
		{Cycle: cycle, Side: SideCore, Reason: "source-error"},
	})
	if coreOnly != 0 {
		t.Fatalf("Join = %d, want 0 when only the core side observed the cycle", coreOnly)
	}
	connectorOnly := Join([]Observation{
		{Cycle: cycle, Side: SideConnector, Reason: "http-5xx"},
		{Cycle: cycle, Side: SideConnector, Reason: "http-5xx"},
	})
	if connectorOnly != 0 {
		t.Fatalf("Join = %d, want 0 when only the connector side observed the cycle", connectorOnly)
	}
}

// TestJoinCountsDistinctCycles proves two different cycles join to two
// logical failures, and that repeating observations for one cycle does not
// inflate the count.
func TestJoinCountsDistinctCycles(t *testing.T) {
	first := Cycle{Connector: "dispatch", ID: "cycle-1"}
	second := Cycle{Connector: "dispatch", ID: "cycle-2"}
	joined := Join([]Observation{
		{Cycle: first, Side: SideCore, Reason: "source-error"},
		{Cycle: first, Side: SideConnector, Reason: "http-5xx"},
		{Cycle: second, Side: SideCore, Reason: "source-error"},
		{Cycle: second, Side: SideConnector, Reason: "http-5xx"},
	})
	if joined != 2 {
		t.Fatalf("Join = %d, want 2 for two distinct cycles", joined)
	}
	// A repeated observation for one cycle is the same logical failure: the
	// join counts distinct keys, so it still reports one.
	repeated := Join([]Observation{
		{Cycle: first, Side: SideCore, Reason: "source-error"},
		{Cycle: first, Side: SideConnector, Reason: "http-5xx"},
		{Cycle: first, Side: SideConnector, Reason: "http-5xx"},
	})
	if repeated != 1 {
		t.Fatalf("Join = %d, want 1 for one cycle observed repeatedly", repeated)
	}
}

// TestJoinSeparatesConnectors proves the join key includes the connector: the
// same cycle id on two connectors is two logical failures, not one.
func TestJoinSeparatesConnectors(t *testing.T) {
	joined := Join([]Observation{
		{Cycle: Cycle{Connector: "dispatch", ID: "cycle-1"}, Side: SideCore, Reason: "source-error"},
		{Cycle: Cycle{Connector: "dispatch", ID: "cycle-1"}, Side: SideConnector, Reason: "http-5xx"},
		{Cycle: Cycle{Connector: "dispatch:cloud", ID: "cycle-1"}, Side: SideCore, Reason: "source-error"},
		{Cycle: Cycle{Connector: "dispatch:cloud", ID: "cycle-1"}, Side: SideConnector, Reason: "http-5xx"},
	})
	if joined != 2 {
		t.Fatalf("Join = %d, want 2 for one cycle on two connectors", joined)
	}
}

// TestJoinEmptyObservations proves the join of no observation is no failure.
func TestJoinEmptyObservations(t *testing.T) {
	if joined := Join(nil); joined != 0 {
		t.Fatalf("Join = %d, want 0 for no observations", joined)
	}
}
