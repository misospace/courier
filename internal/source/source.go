// Package source contains the source-agnostic lifecycle contract used by the
// operator. Concrete source implementations belong in sibling packages (for
// example, source/dispatch).
package source

import "context"

// WorkItem identifies work in a source. ID is intentionally opaque: sources
// are free to choose their identifier format, and the core must not know about
// a source's queue or API types.
type WorkItem struct {
	// ID is the source's opaque identifier used for claim and lifecycle calls.
	ID string
	// Mode, Repo, Ref, and Lane are the source-provided run identity.
	// They are deliberately plain Go values so the source package does not
	// depend on Kubernetes API types.
	Mode  string
	Repo  string
	Ref   int
	Lane  string
	Debug bool
}

// RunSpec is the source-neutral input from which the operator creates a
// CoderRun. A source adapter owns discovery; the operator owns the Kubernetes
// object and sets Source to its adapter name.
type RunSpec struct {
	Mode   string
	Source string
	// WorkItemID is copied from WorkItem.ID so it remains available for
	// lifecycle calls after the source item has been materialized as a run.
	WorkItemID string
	Repo       string
	Ref        int
	Lane       string
	Debug      bool
}

// Spec returns the immutable run data carried by a discovered work item.
func (w WorkItem) Spec(sourceName string) RunSpec {
	return RunSpec{
		Mode:       w.Mode,
		Source:     sourceName,
		WorkItemID: w.ID,
		Repo:       w.Repo,
		Ref:        w.Ref,
		Lane:       w.Lane,
		Debug:      w.Debug,
	}
}

// State is a source-agnostic work lifecycle state. These are the states the
// operator needs to publish back while a CoderRun is active.
type State string

const (
	StateInProgress State = "in-progress"
	StateInReview   State = "in-review"
	StateNeedsHuman State = "needs-human"
)

// Adapter is the source boundary used by the operator. Claim is separate from
// Transition because claiming commonly has compare-and-set semantics, while
// the lifecycle updates are ordinary state changes.
//
// No source-specific request, response, or status type belongs here. An
// adapter translates this contract to its own API.
type Adapter interface {
	// Discover returns source work that should become CoderRuns. It must not
	// claim items; claiming is a separate compare-and-set step.
	Discover(context.Context) ([]WorkItem, error)
	Claim(context.Context, WorkItem) error
	// Release rolls back a successful Claim when the operator cannot launch a
	// coordinator. It must be safe to retry.
	Release(context.Context, WorkItem) error
	Transition(context.Context, WorkItem, State) error
	// Resolve closes the source work after a run has reached its terminal
	// successful state. Needs-human is represented by Transition instead.
	Resolve(context.Context, WorkItem) error
}
