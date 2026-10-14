// Package dispatch implements Courier's first-party Dispatch source adapter.
package dispatch

import (
	"context"
	"errors"
	"fmt"

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

// API is an alias retained as a descriptive name for callers that think of
// the injected dependency as an API rather than a client.
type API = Client

// Adapter translates Courier's generic source lifecycle into Dispatch API
// calls.
type Adapter struct {
	client Client
}

// DispatchAdapter is a descriptive alias for Adapter.
type DispatchAdapter = Adapter

var _ source.Adapter = (*Adapter)(nil)
var _ source.Reporter = (*Adapter)(nil)
var _ source.PreLauncher = (*Adapter)(nil)
var _ source.WorkIdentifier = (*Adapter)(nil)
var _ source.CapacityRecoveryPreparer = (*Adapter)(nil)
var _ source.CapacityRecovery = (*Adapter)(nil)

// New creates a Dispatch adapter around an injected API client.
func New(client Client) *Adapter {
	return &Adapter{client: client}
}

// NewAdapter is an explicit constructor alias for callers that prefer the
// adapter's role in the name.
func NewAdapter(client Client) *Adapter {
	return New(client)
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
