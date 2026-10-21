// Package connectortest provides the reusable assertions of the connector
// diagnostic verification contract (docs/connector-diagnostics.md §11).
//
// It is test support only: no production package imports it, and it imports no
// production package. It exists so every connector package can inherit the
// same duplicate-counting assertion instead of re-deriving it per connector.
package connectortest

// Cycle identifies one discovery cycle for one connector. The cycle id is a
// test-scoped notion: it never becomes a metric label, because the envelope's
// labels are bounded and correlation identifiers ride the log half only.
type Cycle struct {
	Connector string
	ID        string
}

// Side distinguishes the core failure counter's event stream from the
// connector diagnostic envelope's event stream.
type Side string

const (
	SideCore      Side = "core"
	SideConnector Side = "connector"
)

// Observation is one failure-classifying observation from one side of the join.
type Observation struct {
	Cycle
	Side   Side
	Reason string
}

// Join returns the number of distinct (connector, cycle id) pairs observed on
// BOTH sides. One logical failure is counted once by the core counter and once
// by the connector envelope, so the join yields one joined failure, not two:
// a naive row-per-observation join would report 2 for the same failure.
//
// The join is over distinct keys, not over rows: an (connector, cycle id) pair
// seen several times on one side still contributes one joined failure, and a
// pair seen on only one side contributes none. A pair seen on both sides
// contributes exactly one, whatever the number of observations on either
// side — so the result counts logical failures, never events.
func Join(observations []Observation) int {
	type key struct {
		connector string
		id        string
	}
	core := map[key]struct{}{}
	connectorSide := map[key]struct{}{}
	for _, observation := range observations {
		k := key{connector: observation.Connector, id: observation.ID}
		switch observation.Side {
		case SideCore:
			core[k] = struct{}{}
		case SideConnector:
			connectorSide[k] = struct{}{}
		}
	}
	joined := 0
	for k := range core {
		if _, ok := connectorSide[k]; ok {
			joined++
		}
	}
	return joined
}
