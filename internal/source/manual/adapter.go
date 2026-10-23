// Package manual implements the source lifecycle for CoderRuns created
// directly through Kubernetes (for example, with kubectl during bootstrap).
package manual

import (
	"context"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

// connectorName is the bounded connector label value this adapter reports
// under.
const connectorName = "manual"

// Connector-declared reason vocabulary (§3). The manual adapter has no
// protocol exchange, so its vocabulary is minimal and closed.
const (
	reasonAvailable   = connector.AvailabilityAvailable
	reasonUnreachable = connector.AvailabilityUnreachable
)

// Adapter deliberately has no external lifecycle to mutate. It lets a
// directly-created CoderRun use the same controller path as queue-backed work.
// The zero value stays diagnostic-free; New attaches the optional connector
// diagnostic envelope.
type Adapter struct {
	diagnostics *connector.Emitter
	// discover stands in for the default no-op discovery so the availability
	// observation has something to observe. It is set only through the
	// test-only newWithDiscover constructor; New never sets it.
	discover func(context.Context) ([]source.WorkItem, error)
}

// New creates the manual adapter with the optional connector diagnostic
// envelope attached. A nil emitter leaves the adapter diagnostic-free.
// Capabilities are declared at construction (§5): only discovery and
// availability are supported; every other capability is explicitly
// unsupported so a dashboard never renders a fabricated panel.
func New(diagnostics *connector.Emitter) Adapter {
	a := Adapter{diagnostics: diagnostics}
	if diagnostics == nil {
		return a
	}
	diagnostics.RegisterConnector(connectorName, map[string][]string{
		connector.SignalAvailability: {reasonAvailable, reasonUnreachable},
	})
	diagnostics.Capability(connectorName, connector.CapabilityDiscovery, connector.CapabilitySupported)
	diagnostics.Capability(connectorName, connector.CapabilityDiscoveryExclusions, connector.CapabilityUnsupported)
	diagnostics.Capability(connectorName, connector.CapabilityLifecycleReport, connector.CapabilityUnsupported)
	diagnostics.Capability(connectorName, connector.CapabilityPreLaunchRevalidation, connector.CapabilityUnsupported)
	diagnostics.Capability(connectorName, connector.CapabilityCIParkLifecycle, connector.CapabilityUnsupported)
	diagnostics.Capability(connectorName, connector.CapabilityWorkIdentity, connector.CapabilityUnsupported)
	diagnostics.Capability(connectorName, connector.CapabilityAvailability, connector.CapabilitySupported)
	return a
}

var _ source.Adapter = Adapter{}

// newWithDiscover is a test-only constructor: it builds a New-configured
// adapter whose discovery stands in for the default no-op so the availability
// observation can be exercised against failure.
func newWithDiscover(diagnostics *connector.Emitter, discover func(context.Context) ([]source.WorkItem, error)) Adapter {
	a := New(diagnostics)
	a.discover = discover
	return a
}

func (a Adapter) Discover(ctx context.Context) ([]source.WorkItem, error) {
	var (
		items []source.WorkItem
		err   error
	)
	if a.discover != nil {
		items, err = a.discover(ctx)
	} else {
		items, err = nil, nil
	}
	if err != nil {
		a.diagnostics.Availability(connectorName, reasonUnreachable)
		return nil, err
	}
	a.diagnostics.Availability(connectorName, reasonAvailable)
	return items, nil
}
func (a Adapter) Claim(context.Context, source.WorkItem) error   { return nil }
func (a Adapter) Release(context.Context, source.WorkItem) error { return nil }
func (a Adapter) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}
func (a Adapter) Resolve(context.Context, source.WorkItem) error { return nil }
