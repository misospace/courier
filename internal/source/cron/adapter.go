package cron

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MaxConnectorNameLength bounds the configured connector label value, the
// same bound the Dispatch adapter applies to its composed identities.
const MaxConnectorNameLength = 64

// connectorName is the connector label the single-lane adapter reports
// under. Multi-lane wiring derives "cron:<lane>" per adapter.
const connectorName = "cron"

// Connector-declared reason vocabulary. Like the manual adapter, the cron
// adapter has no protocol exchange, so its vocabulary is minimal and closed.
const (
	reasonAvailable   = connector.AvailabilityAvailable
	reasonUnreachable = connector.AvailabilityUnreachable
)

// jobState pairs one compiled job with its occurrence cursor. The cursor is
// the next boundary not yet offered; it never rewinds, which is what makes
// re-materialization after a run is reaped impossible.
type jobState struct {
	job job
	// cursor is the next scheduled boundary this process may offer. Zero
	// until first use, when it is initialized to the first boundary strictly
	// after the current time: occurrences that fired before process start —
	// including every boundary that passed while the process was down — are
	// skipped, never caught up.
	cursor time.Time
}

// Adapter materializes scheduled work as source WorkItems. It has no
// external lifecycle to mutate — a materialized run settles against the
// world like every other source work item. The zero value stays
// diagnostic-free; New attaches the optional connector diagnostic envelope.
type Adapter struct {
	diagnostics *connector.Emitter
	// name is the bounded connector label this adapter reports under.
	name string
	// now is the adapter's time source, injectable so tests never sleep.
	// Nil falls back to time.Now.
	now func() time.Time
	// mu guards the per-job cursors across Discover calls.
	mu   sync.Mutex
	jobs []jobState
}

// Option customizes the adapter.
type Option func(*Adapter)

// WithConnectorName overrides the adapter's connector label. Multi-lane
// wiring passes "cron:<lane>" so independent adapters never share metric
// series. The value must fit MaxConnectorNameLength.
func WithConnectorName(name string) Option {
	return func(a *Adapter) { a.name = name }
}

// WithNow injects the adapter's time source. It is applied before the
// adapter initializes its cursors, so the injected clock decides the
// starting boundary.
func WithNow(now func() time.Time) Option {
	return func(a *Adapter) { a.now = now }
}

// New creates the adapter for the given validated schedules, with the
// optional connector diagnostic envelope attached (nil leaves the adapter
// diagnostic-free). Each schedule is compiled once here and every job's
// occurrence cursor starts at the first boundary strictly after
// construction time: the adapter offers no catch-up of occurrences missed
// before process start (docs/cron-schedules.md). Capabilities are declared
// at construction: only discovery and availability are supported; every
// other capability is explicitly unsupported so a dashboard never renders a
// fabricated panel.
func New(schedules []Schedule, diagnostics *connector.Emitter, options ...Option) *Adapter {
	a := &Adapter{
		diagnostics: diagnostics,
		name:        connectorName,
		jobs:        make([]jobState, 0, len(schedules)),
	}
	for _, option := range options {
		option(a)
	}
	for _, schedule := range schedules {
		compiled, err := compileJob(schedule)
		if err != nil {
			// Unreachable for schedules that came through Parse, which has
			// already compiled them; a hand-constructed schedule fails into
			// an empty, inert job set rather than a panic.
			log.FromContext(context.Background()).Error(err, "skipping invalid cron schedule", "name", schedule.Name)
			continue
		}
		a.jobs = append(a.jobs, jobState{job: compiled})
	}
	now := a.clock()
	for i := range a.jobs {
		a.jobs[i].cursor = a.jobs[i].job.schedule.next(now)
	}
	if diagnostics != nil {
		diagnostics.RegisterConnector(a.name, map[string][]string{
			connector.SignalAvailability: {reasonAvailable, reasonUnreachable},
		})
		diagnostics.Capability(a.name, connector.CapabilityDiscovery, connector.CapabilitySupported)
		diagnostics.Capability(a.name, connector.CapabilityDiscoveryExclusions, connector.CapabilityUnsupported)
		diagnostics.Capability(a.name, connector.CapabilityLifecycleReport, connector.CapabilityUnsupported)
		diagnostics.Capability(a.name, connector.CapabilityPreLaunchRevalidation, connector.CapabilityUnsupported)
		diagnostics.Capability(a.name, connector.CapabilityCIParkLifecycle, connector.CapabilityUnsupported)
		diagnostics.Capability(a.name, connector.CapabilityWorkIdentity, connector.CapabilityUnsupported)
		diagnostics.Capability(a.name, connector.CapabilityAvailability, connector.CapabilitySupported)
	}
	return a
}

var _ source.Adapter = (*Adapter)(nil)

func (a *Adapter) clock() time.Time {
	if a.now != nil {
		return a.now()
	}
	return time.Now()
}

// Discover offers one WorkItem per job whose latest due occurrence has not
// been offered by this process yet. The cursor semantics carry the settled
// missed-run policy: at most the latest boundary that passed since the last
// Discover is offered, earlier ones in the same gap are skipped, and
// boundaries that passed before adapter construction are never offered.
func (a *Adapter) Discover(ctx context.Context) ([]source.WorkItem, error) {
	items, err := a.discover(ctx)
	if err != nil {
		a.diagnostics.Availability(a.name, reasonUnreachable)
		return nil, err
	}
	a.diagnostics.Availability(a.name, reasonAvailable)
	return items, nil
}

func (a *Adapter) discover(ctx context.Context) ([]source.WorkItem, error) {
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	items := make([]source.WorkItem, 0, len(a.jobs))
	for i := range a.jobs {
		state := &a.jobs[i]
		if state.cursor.IsZero() {
			state.cursor = state.job.schedule.next(now)
			continue
		}
		var (
			latest  time.Time
			skipped int
		)
		for boundary := state.cursor; !boundary.After(now); boundary = state.job.schedule.next(boundary) {
			if !latest.IsZero() {
				skipped++
			}
			latest = boundary
		}
		if latest.IsZero() {
			continue
		}
		if skipped > 0 {
			log.FromContext(ctx).Info("skipping missed cron occurrences, offering only the latest",
				"job", state.job.name, "skipped", skipped,
				"latest", latest.Format(time.RFC3339))
		}
		state.cursor = state.job.schedule.next(latest)
		items = append(items, state.job.workItem(latest))
	}
	return items, nil
}

func (j job) workItem(boundary time.Time) source.WorkItem {
	return source.WorkItem{
		ID:      j.occurrenceID(boundary),
		Mode:    j.mode,
		Repo:    j.repo,
		Ref:     j.issue,
		Lane:    j.lane,
		Debug:   j.debug,
		Context: fmt.Sprintf("Scheduled occurrence of cron job %q.", j.name),
	}
}

func (a *Adapter) Claim(context.Context, source.WorkItem) error   { return nil }
func (a *Adapter) Release(context.Context, source.WorkItem) error { return nil }
func (a *Adapter) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}
func (a *Adapter) Resolve(context.Context, source.WorkItem) error { return nil }
