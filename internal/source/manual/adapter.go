// Package manual implements the source lifecycle for CoderRuns created
// directly through Kubernetes (for example, with kubectl during bootstrap).
package manual

import (
	"context"

	"github.com/misospace/courier/internal/source"
)

// Adapter deliberately has no external lifecycle to mutate. It lets a
// directly-created CoderRun use the same controller path as queue-backed work.
type Adapter struct{}

var _ source.Adapter = Adapter{}

func (Adapter) Discover(context.Context) ([]source.WorkItem, error) { return nil, nil }
func (Adapter) Claim(context.Context, source.WorkItem) error        { return nil }
func (Adapter) Release(context.Context, source.WorkItem) error      { return nil }
func (Adapter) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}
func (Adapter) Resolve(context.Context, source.WorkItem) error { return nil }
