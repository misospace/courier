// Package dispatch implements Courier's first-party Dispatch source adapter.
package dispatch

import (
	"context"
	"errors"
	"fmt"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

var (
	// ErrNilClient indicates that an adapter was constructed without an API
	// client. Keeping this as an error (rather than panicking) makes startup
	// configuration failures observable and keeps the adapter easy to fake.
	ErrNilClient = errors.New("dispatch source: nil client")
	// ErrEmptyWorkItemID indicates that the adapter cannot address the source
	// item. IDs are opaque to the core, but Dispatch still requires one.
	ErrEmptyWorkItemID = errors.New("dispatch source: empty work item ID")
	// ErrUnsupportedState indicates a state that this adapter cannot publish.
	ErrUnsupportedState = errors.New("dispatch source: unsupported state")
)

// Client is the deliberately narrow seam to the Dispatch API. The concrete
// HTTP client, authentication, endpoint paths, and request encoding are kept
// outside Courier's source interface and can be supplied by the deployment.
// A fake implementing these methods is sufficient for adapter tests.
type Client interface {
	Discover(context.Context) ([]source.WorkItem, error)
	Claim(context.Context, string) error
	Release(context.Context, string) error
	SetStatus(context.Context, string, string) error
	Resolve(context.Context, string) error
}

// ReporterClient is the optional Dispatch API capability used for lifecycle
// task reports.
type ReporterClient interface {
	Report(context.Context, string, source.Lifecycle) error
}

// PreLauncherClient is the optional Dispatch API capability used to revalidate
// claimed work immediately before coordinator launch.
type PreLauncherClient interface {
	PreLaunch(context.Context, string) error
}

// ObserverClient is the optional Dispatch API capability used for best-effort
// execution observations (misospace/courier#633): the run's current-state
// upserts travel to the execution-observation route, which is non-authoritative
// observability and must never stand in for a claim, report, or settlement
// call.
type ObserverClient interface {
	ReportExecution(context.Context, source.ExecutionObservation) error
}

// API is an alias retained as a descriptive name for callers that think of
// the injected dependency as an API rather than a client.
type API = Client

// Adapter translates Courier's generic source lifecycle into Dispatch API
// calls.
type Adapter struct {
	client        Client
	diagnostics   *connector.Emitter
	connectorName string
}

// Option configures an Adapter at construction.
type Option func(*Adapter)

// WithConnectorDiagnostics attaches the connector diagnostic envelope
// (docs/connector-diagnostics.md). A nil emitter leaves the adapter
// diagnostic-free, so existing call sites are unchanged. When the client is
// the concrete HTTP client the emitter also reaches its runtime emission
// points, the exclusion contract maximum is exported once, and the
// capability catalogue and initial availability are declared here — the one
// canonical construction site.
func WithConnectorDiagnostics(em *connector.Emitter) Option {
	return func(a *Adapter) {
		a.diagnostics = em
	}
}

// WithConnectorName sets the bounded connector identity this adapter reports
// under. Deployments that configure multiple Dispatch adapters must give each
// a distinct identity (the configured adapter name, e.g. "dispatch:<agent>"),
// so their metric series stay independent; a single legacy adapter keeps the
// default. An empty, over-long, or out-of-charset identity would silently
// merge or corrupt another connector's series, so it fails loudly at
// construction.
func WithConnectorName(name string) Option {
	return func(a *Adapter) {
		validateConnectorName(name)
		a.connectorName = name
	}
}

// DispatchAdapter is a descriptive alias for Adapter.
type DispatchAdapter = Adapter

var _ source.Adapter = (*Adapter)(nil)
var _ source.Reporter = (*Adapter)(nil)
var _ source.PreLauncher = (*Adapter)(nil)
var _ source.HandoutAcker = (*Adapter)(nil)
var _ source.WorkIdentifier = (*Adapter)(nil)
var _ source.CapacityRecoveryPreparer = (*Adapter)(nil)
var _ source.CapacityRecovery = (*Adapter)(nil)
var _ source.CIParkLifecycle = (*Adapter)(nil)
var _ source.CIParkLifecycleProbe = (*Adapter)(nil)
var _ source.AttemptRequeuer = (*Adapter)(nil)
var _ source.ExecutionObserver = (*Adapter)(nil)

// New creates a Dispatch adapter around an injected API client.
func New(client Client, opts ...Option) *Adapter {
	a := &Adapter{client: client, connectorName: defaultConnectorName}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	a.declareCapabilities()
	return a
}

// NewAdapter is an explicit constructor alias for callers that prefer the
// adapter's role in the name.
func NewAdapter(client Client, opts ...Option) *Adapter {
	return New(client, opts...)
}

// declareCapabilities publishes the capability catalogue (§5) and the
// connector's identity: the closed reason vocabulary is registered, the
// exclusion contract maximum is exported as data, and availability starts
// unknown until the first discovery attempt observes a state. Without an
// emitter this is a no-op.
func (a *Adapter) declareCapabilities() {
	em := a.diagnostics
	if em == nil {
		return
	}
	em.RegisterConnector(a.connectorName, reasonVocabulary())
	if hc, ok := a.client.(*HTTPClient); ok {
		hc.WithConnectorDiagnostics(em, a.connectorName)
		em.ExclusionLimit(a.connectorName, hc.prFixExclusionLimit)
		if hc.prFixExclusionsEnabled {
			em.Capability(a.connectorName, connector.CapabilityDiscoveryExclusions, connector.CapabilitySupported)
		} else {
			em.Capability(a.connectorName, connector.CapabilityDiscoveryExclusions, connector.CapabilityUnsupported)
		}
	} else {
		em.Capability(a.connectorName, connector.CapabilityDiscoveryExclusions, connector.CapabilityUnsupported)
	}
	em.Capability(a.connectorName, connector.CapabilityDiscovery, connector.CapabilitySupported)
	if _, ok := a.client.(ReporterClient); ok {
		em.Capability(a.connectorName, connector.CapabilityLifecycleReport, connector.CapabilitySupported)
	} else {
		em.Capability(a.connectorName, connector.CapabilityLifecycleReport, connector.CapabilityUnsupported)
	}
	if _, ok := a.client.(PreLauncherClient); ok {
		em.Capability(a.connectorName, connector.CapabilityPreLaunchRevalidation, connector.CapabilitySupported)
	} else {
		em.Capability(a.connectorName, connector.CapabilityPreLaunchRevalidation, connector.CapabilityUnsupported)
	}
	if _, ok := a.client.(ParkLifecycleClient); ok {
		em.Capability(a.connectorName, connector.CapabilityCIParkLifecycle, connector.CapabilitySupported)
	} else {
		em.Capability(a.connectorName, connector.CapabilityCIParkLifecycle, connector.CapabilityUnsupported)
	}
	if _, ok := a.client.(ObserverClient); ok {
		em.Capability(a.connectorName, connector.CapabilityExecutionObservations, connector.CapabilitySupported)
	} else {
		em.Capability(a.connectorName, connector.CapabilityExecutionObservations, connector.CapabilityUnsupported)
	}
	em.Capability(a.connectorName, connector.CapabilityWorkIdentity, connector.CapabilitySupported)
	em.Capability(a.connectorName, connector.CapabilityAvailability, connector.CapabilitySupported)
	em.Availability(a.connectorName, connector.AvailabilityUnknown)
}

// CIParkLifecycleUnsupported answers source.CIParkLifecycleProbe: whether
// the concrete client has latched the park lifecycle as unsupported — a bare
// 404 on a park-family route, permanent version-skew evidence for the
// process (internal/source/dispatch/cipark.go). Unknown until observed, and
// a client that is not the concrete HTTP client — every fake — reports
// not-known-unsupported, so the core parks exactly as it does today.
func (a *Adapter) CIParkLifecycleUnsupported() bool {
	probe, ok := a.client.(interface {
		ciParkLifecycleUnsupported() bool
	})
	if !ok {
		return false
	}
	return probe.ciParkLifecycleUnsupported()
}

// Discover returns Dispatch work that the operator can materialize as
// CoderRuns. Discovery does not claim anything.
// WorkIdentity implements source.WorkIdentifier.
func (a *Adapter) WorkIdentity(workItemID string) string {
	return WorkIdentity(workItemID)
}

func (a *Adapter) Discover(ctx context.Context) ([]source.WorkItem, error) {
	if err := a.validateClient(); err != nil {
		return nil, err
	}
	return a.client.Discover(ctx)
}

func (a *Adapter) DiscoverForCapacity(ctx context.Context, capacityID string, exclusions []source.WorkItem) ([]source.WorkItem, error) {
	if err := a.validateClient(); err != nil {
		return nil, err
	}
	if client, ok := a.client.(source.CapacityAwareDiscoverer); ok {
		return client.DiscoverForCapacity(ctx, capacityID, exclusions)
	}
	return a.DiscoverExcluding(ctx, exclusions)
}

// DiscoverExcluding uses the optional client capability, preserving compatibility
// with clients that only implement the original discovery contract.
func (a *Adapter) DiscoverExcluding(ctx context.Context, exclusions []source.WorkItem) ([]source.WorkItem, error) {
	if err := a.validateClient(); err != nil {
		return nil, err
	}
	if client, ok := a.client.(interface {
		DiscoverExcluding(context.Context, []source.WorkItem) ([]source.WorkItem, error)
	}); ok {
		return client.DiscoverExcluding(ctx, exclusions)
	}
	return a.client.Discover(ctx)
}

// Claim atomically claims item in Dispatch.
func (a *Adapter) Claim(ctx context.Context, item source.WorkItem) error {
	if err := a.validate(item); err != nil {
		return err
	}
	return a.client.Claim(ctx, item.ID)
}

// ClaimWithReceipt captures Dispatch's issued claim token as opaque durable context.
func (a *Adapter) ClaimWithReceipt(ctx context.Context, item source.WorkItem) (source.WorkItem, error) {
	if err := a.validate(item); err != nil {
		return source.WorkItem{}, err
	}
	claimer, ok := a.client.(interface {
		ClaimWithReceipt(context.Context, string, string) (string, error)
	})
	if !ok {
		return source.WorkItem{}, fmt.Errorf("dispatch source: client does not support claim receipts")
	}
	receipt, err := claimer.ClaimWithReceipt(ctx, item.ID, item.Context)
	if err != nil {
		return source.WorkItem{}, err
	}
	item.Context = receipt
	return item, nil
}

// Release returns a claimed item to Dispatch after coordinator launch fails.
// The source client is responsible for making this operation idempotent.
func (a *Adapter) Release(ctx context.Context, item source.WorkItem) error {
	if err := a.validate(item); err != nil {
		return err
	}
	return a.client.Release(ctx, item.ID)
}

// PreLaunch revalidates claimed work before a coordinator is started.
func (a *Adapter) PreLaunch(ctx context.Context, item source.WorkItem) error {
	if err := a.validate(item); err != nil {
		return err
	}
	checker, ok := a.client.(PreLauncherClient)
	if !ok {
		return nil
	}
	return checker.PreLaunch(ctx, item.ID)
}

// AckHandout acknowledges the stamped hand-out of a queue-backed attempt once
// the runner materializes the run that owns it (#456). Clients that do not
// implement the capability carry no stamped hand-outs, so the ack is a no-op;
// otherwise the call forwards to the client's generation-pinned, idempotent
// ack, whose refusals and failures the runner logs without blocking
// materialization.
func (a *Adapter) AckHandout(ctx context.Context, item source.WorkItem) error {
	if err := a.validate(item); err != nil {
		return err
	}
	acker, ok := a.client.(source.HandoutAcker)
	if !ok {
		return nil
	}
	return acker.AckHandout(ctx, item)
}

// Transition publishes a generic Courier lifecycle state to Dispatch.
func (a *Adapter) Transition(ctx context.Context, item source.WorkItem, state source.State) error {
	if err := a.validate(item); err != nil {
		return err
	}
	status, ok := dispatchStatus(state)
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnsupportedState, state)
	}
	return a.client.SetStatus(ctx, item.ID, status)
}

// InProgress marks item as actively being worked.
func (a *Adapter) InProgress(ctx context.Context, item source.WorkItem) error {
	return a.Transition(ctx, item, source.StateInProgress)
}

// InReview marks item as ready for human review.
func (a *Adapter) InReview(ctx context.Context, item source.WorkItem) error {
	return a.Transition(ctx, item, source.StateInReview)
}

// NeedsHuman marks item as requiring human intervention.
func (a *Adapter) NeedsHuman(ctx context.Context, item source.WorkItem) error {
	return a.Transition(ctx, item, source.StateNeedsHuman)
}

func (a *Adapter) PrepareCapacityRecovery(ctx context.Context, item source.WorkItem, _ source.CapacityRecoveryAttempt) (source.CapacityRecoveryProof, error) {
	if err := a.validate(item); err != nil {
		return source.CapacityRecoveryProof{}, err
	}
	preparer, ok := a.client.(interface {
		PrepareCapacityRecovery(context.Context, string, string) (source.CapacityRecoveryProof, error)
	})
	if !ok {
		return source.CapacityRecoveryProof{}, source.ErrUnsupportedCapacityRecovery
	}
	return preparer.PrepareCapacityRecovery(ctx, item.ID, item.Context)
}

func (a *Adapter) SettleCapacityExhaustion(ctx context.Context, item source.WorkItem, handoff source.CapacityHandoff) error {
	if err := a.validate(item); err != nil {
		return err
	}
	settler, ok := a.client.(interface {
		SettleCapacityExhaustion(context.Context, string, string, source.CapacityHandoff) error
	})
	if !ok {
		return source.ErrUnsupportedCapacityRecovery
	}
	return settler.SettleCapacityExhaustion(ctx, item.ID, item.Context, handoff)
}

// RequeueAttempt forwards one explicit operator retry (#97) to the Dispatch
// requeue route in the shape the fenced capacity-settlement methods use: the
// run's issued receipt and the request's run-scoped idempotency key travel
// unchanged, and the client owns the refusal classification — a coded source
// refusal arrives as a *source.RetryRefusalError, and a deployment that has not
// adopted the routes as the permanent source.ErrUnsupportedRetry. A client
// without the requeue capability cannot fence the transition at all, so the
// adapter reports that posture instead of approximating it with an ordinary
// lifecycle call, which the core forbids and would have to refuse anyway.
func (a *Adapter) RequeueAttempt(ctx context.Context, item source.WorkItem, req source.AttemptRequeueRequest) (source.AttemptRequeueResult, error) {
	if err := a.validate(item); err != nil {
		return source.AttemptRequeueResult{}, err
	}
	requeuer, ok := a.client.(RequeueClient)
	if !ok {
		return source.AttemptRequeueResult{}, source.ErrUnsupportedRetry
	}
	return requeuer.RequeueAttempt(ctx, item.ID, req)
}

// ReportExecution publishes one best-effort execution observation
// (misospace/courier#633). The observation is non-authoritative: its failure,
// rejection, or absence must never affect discovery, claim/admission, launch,
// status updates, publication, settlement, recovery, capacity release, or
// retention. A client without the observation seam cannot accept
// observations, so the adapter reports that posture instead of emulating the
// report with an ordinary lifecycle call, which the core forbids; callers
// degrade the feature only.
func (a *Adapter) ReportExecution(ctx context.Context, obs source.ExecutionObservation) error {
	if err := a.validateClient(); err != nil {
		return err
	}
	if obs.WorkItemID == "" {
		return ErrEmptyWorkItemID
	}
	observer, ok := a.client.(ObserverClient)
	if !ok {
		return source.ErrUnsupportedExecutionObservations
	}
	return observer.ReportExecution(ctx, obs)
}

// Report publishes lifecycle audit metadata to Dispatch.
func (a *Adapter) Report(ctx context.Context, item source.WorkItem, lifecycle source.Lifecycle) error {
	if err := a.validate(item); err != nil {
		return err
	}
	reporter, ok := a.client.(ReporterClient)
	if !ok {
		return errors.New("dispatch source: client does not support lifecycle reports")
	}
	return reporter.Report(ctx, item.ID, lifecycle)
}

// Resolve closes successfully completed work in Dispatch.
func (a *Adapter) Resolve(ctx context.Context, item source.WorkItem) error {
	if err := a.validate(item); err != nil {
		return err
	}
	return a.client.Resolve(ctx, item.ID)
}

// SetInProgress is a compatibility spelling for callers that use setter-style
// lifecycle methods.
func (a *Adapter) SetInProgress(ctx context.Context, item source.WorkItem) error {
	return a.InProgress(ctx, item)
}

// SetInReview is a compatibility spelling for callers that use setter-style
// lifecycle methods.
func (a *Adapter) SetInReview(ctx context.Context, item source.WorkItem) error {
	return a.InReview(ctx, item)
}

// SetNeedsHuman is a compatibility spelling for callers that use setter-style
// lifecycle methods.
func (a *Adapter) SetNeedsHuman(ctx context.Context, item source.WorkItem) error {
	return a.NeedsHuman(ctx, item)
}

func (a *Adapter) validate(item source.WorkItem) error {
	if err := a.validateClient(); err != nil {
		return err
	}
	if item.ID == "" {
		return ErrEmptyWorkItemID
	}
	return nil
}

func (a *Adapter) validateClient() error {
	if a == nil || a.client == nil {
		return ErrNilClient
	}
	return nil
}

func dispatchStatus(state source.State) (string, bool) {
	switch state {
	case source.StateInProgress:
		return "in-progress", true
	case source.StateInReview:
		return "in-review", true
	case source.StateNeedsHuman:
		return "needs-human", true
	default:
		return "", false
	}
}
