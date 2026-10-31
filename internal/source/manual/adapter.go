// Package manual implements the source lifecycle for CoderRuns created
// directly through Kubernetes (for example, with kubectl during bootstrap).
package manual

import (
	"context"
	"errors"
	"strings"

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

// retryReasonStaleGeneration is the closed refusal-vocabulary value the
// adapter reports when a requeue presents a fence it cannot honor. It must
// stay equal to the retry vocabulary the controller settles
// status.retryRequest with (docs/explicit-retry.md), the same value the
// dispatch adapter reports for a fence its source no longer records.
const retryReasonStaleGeneration = "stale-generation"

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
var _ source.AttemptRequeuer = Adapter{}

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

// RequeueAttempt settles an explicit operator retry (#97) for a manually
// created run. It is a validated no-op source transition: the manual adapter
// has no external lifecycle to mutate, because the operator's kubectl act of
// creating the run is the source action itself, so there is no queue, claim,
// or hold to release and re-offer. What the call still owes is the fresh
// attempt identity the successor run materializes under, and the adapter mints
// it in its own namespace: "<WorkItemID>/retry/<requestID>", with the request
// ID the opaque retry request carried in req.Reason. The mint is
// deterministic in its inputs, which is the idempotency contract of
// source.AttemptRequeuer: a committed key replays this exact result, and the
// adapter's statelessness is exactly why the identity is minted at the call
// rather than observed later — there is no source state to observe.
// ControllerMaterializesSuccessor is set accordingly: no discovery pass will
// ever offer this identity, so the core creates the successor run itself when
// the request settles Requeued. That is the one documented exception to
// discovery-driven materialization (docs/explicit-retry.md, #609), bounded to
// source: manual.
//
// The validation fails closed. The work item identity, the idempotency key,
// and the request ID must all be non-blank: the identity names the attempt
// the retry replaces, the key is what a replayed request collapses onto, and
// the request ID is the source of the minted identity, without which two
// distinct retries of one work item would mint the same successor identity. A
// non-blank fence token is refused stale-generation, never honored: the
// manual adapter issues no claim receipts — its Claim never mints one — so a
// non-empty fence names a claim this source never recorded. An empty fence is
// the ordinary manual posture and is accepted.
func (a Adapter) RequeueAttempt(_ context.Context, item source.WorkItem, req source.AttemptRequeueRequest) (source.AttemptRequeueResult, error) {
	if strings.TrimSpace(item.ID) == "" {
		return source.AttemptRequeueResult{}, errors.New("manual adapter: attempt requeue requires a work item identity")
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return source.AttemptRequeueResult{}, errors.New("manual adapter: attempt requeue idempotency key required")
	}
	if strings.TrimSpace(req.Reason) == "" {
		return source.AttemptRequeueResult{}, errors.New("manual adapter: attempt requeue reason required")
	}
	if strings.TrimSpace(req.FenceToken) != "" {
		return source.AttemptRequeueResult{}, &source.RetryRefusalError{Reason: retryReasonStaleGeneration}
	}
	return source.AttemptRequeueResult{
		AttemptID:                       item.ID + "/retry/" + req.Reason,
		ControllerMaterializesSuccessor: true,
	}, nil
}
