package harness

import (
	"sort"
	"sync"
)

// RequestClass names which of the run's two model consumers sent one observed
// model request (#738): the capability gate's cheap completion probe, or a
// session turn attempt, retries included.
type RequestClass string

const (
	// RequestProbe is the capability gate's completion probe for one bound
	// model: one per unique model, not per role (#735).
	RequestProbe RequestClass = "probe"
	// RequestWork is a session model turn attempt: the first attempt or one
	// of its in-process retries.
	RequestWork RequestClass = "work"
)

// RequestObservation is one observed model request: the run's record of a
// request the native harness actually sent. Role and Model name the binding
// that sent it — the role the request was attributed to and the model
// identifier it bound to — and they are fixed vocabulary, never credentials
// or provider response text. Attempt is the in-process retry index of a work
// request (0 on the first attempt and on probes). OK reports whether the
// request's stream terminalized without error.
//
// Usage is the provider's own report for the request when the provider sent
// one — nil is an explicit absence, never an invented zero.
// EstimatedPromptTokens is the harness's deterministic estimate of the
// request's prompt size — transcript plus tool declarations, the same
// account the compaction boundary uses — reported separately, never as
// provider-reported usage, and zero for probes.
type RequestObservation struct {
	Class                 RequestClass
	Role                  string
	Model                 string
	Attempt               int
	OK                    bool
	Usage                 *Usage
	EstimatedPromptTokens int
}

// ModelRequestSink receives one observation per model request the native
// harness sent, whether a capability probe or a session turn attempt
// (#738). It is observability only: delivery is fire-and-forget, exactly
// like the compaction sink, and a delivery is isolated so a failing sink
// cannot fail, delay, or change the request, the session, or the run.
type ModelRequestSink interface {
	ModelRequestObserved(observation RequestObservation)
}

// deliverRequest hands one observation to a request sink, isolating the run
// from a sink failure (#738): telemetry is observability only, so a sink
// that panics is dropped where it stands — the request it describes was
// already made, and the run it belongs to must not learn of the loss.
func deliverRequest(sink ModelRequestSink, observation RequestObservation) {
	if sink == nil {
		return
	}
	defer func() { _ = recover() }()
	sink.ModelRequestObserved(observation)
}

// cellKey is one role/model cell of the run's request table.
type cellKey struct {
	role  string
	model string
}

// cellTally is one cell's tallies.
type cellTally struct {
	requests int
	retries  int
	reported int
	missing  int
}

// ModelTelemetry tallies one run's model requests and applied compactions
// for the per-run model-telemetry summary (#738). It is the run's request
// sink: the capability probe and every session the coordinator opens
// deliver their observations to it, and its Summary reports the request
// counts by class and by role/model cell, the split between requests whose
// provider reported usage and those whose did not, the token sums the
// provider reported, the peaks, and the compaction account.
//
// It is observability only. Delivery never fails, delays, or changes the
// request, the session, or the run; every method is safe on a nil receiver
// so wiring a tally is optional and a nil tally is a no-op. A telemetry
// built with a next sink forwards every request observation it tallies to
// that sink — the run's structured-event adapter rides this seam; the
// forwarding order is the senders' order, which concurrent senders make
// deliberate rather than total.
type ModelTelemetry struct {
	mu    sync.Mutex
	next  ModelRequestSink
	total int

	probes, work, retries      int
	reported, missing          int
	input, output              int64
	cacheRead, cacheWrite      int64
	maxInput                   int64
	maxEstimated               int
	compactions, compactionNet int
	lastBefore, lastAfter      int
	cells                      map[cellKey]*cellTally
}

// NewModelTelemetry builds the run's model telemetry. next, when set,
// receives every request observation the telemetry tallies, in order — the
// run's structured-event adapter; a nil next tallies only.
func NewModelTelemetry(next ModelRequestSink) *ModelTelemetry {
	return &ModelTelemetry{
		next:  next,
		cells: make(map[cellKey]*cellTally),
	}
}

// ModelRequestObserved implements ModelRequestSink: it tallies the
// observation and, when the telemetry was built with one, forwards it to the
// chained sink. A nil receiver records nothing. The forward is isolated
// like every sink delivery: the tally must not inherit a downstream sink's
// failure.
func (t *ModelTelemetry) ModelRequestObserved(observation RequestObservation) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.tallyRequest(observation)
	next := t.next
	t.mu.Unlock()
	deliverRequest(next, observation)
}

// SessionCompacted tallies one applied step-boundary compaction for the
// run's summary: the count, the total reclaimed, and the before/after counts
// of the last compaction applied. It is the CompactionSink the coordinator
// chains beside the run's primary sink, and a nil receiver records nothing.
func (t *ModelTelemetry) SessionCompacted(event CompactEvent) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.compactions++
	t.compactionNet += event.NetSaved
	t.lastBefore, t.lastAfter = event.Before, event.After
}

// RoleModelRequests is one role/model cell of the run's model-telemetry
// summary: the cell's observed requests, split by usage outcome.
type RoleModelRequests struct {
	Role  string
	Model string
	// Requests is the cell's observed model requests, probe and work.
	Requests int
	// Retries is the cell's in-process work retries.
	Retries int
	// UsageReported and UsageMissing split the cell's usage outcomes: the
	// requests whose provider did and did not report usage.
	UsageReported int
	UsageMissing  int
}

// ModelTelemetrySummary is one run's model-telemetry snapshot (#738). It is
// a value: the caller keeps what it reads.
type ModelTelemetrySummary struct {
	// TotalRequests is every model request the run observed, probe and
	// work, retries included.
	TotalRequests int
	// ProbeRequests is the capability gate's completion probes, one per
	// unique bound model.
	ProbeRequests int
	// WorkRequests is the session turn attempts, retries included.
	WorkRequests int
	// Retries is the work requests that were in-process retries of a
	// failed attempt.
	Retries int
	// ByRoleModel splits the tallies per role and bound model, sorted by
	// role then model.
	ByRoleModel []RoleModelRequests
	// UsageReported is the requests whose provider reported usage.
	UsageReported int
	// UsageMissing is the requests whose provider reported none: an
	// explicit absence the summary keeps separate, never a zero.
	UsageMissing int
	// The token sums run over the requests whose provider reported usage
	// only. They are provider-reported counts, never estimates.
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	// MaxInputTokens is the largest input usage a single request's provider
	// reported.
	MaxInputTokens int64
	// MaxEstimatedPromptTokens is the largest harness-estimated prompt size
	// any observed request carried. It is the harness's deterministic
	// account, deliberately separate from the provider-reported maxima.
	MaxEstimatedPromptTokens int
	// Compactions is the applied step-boundary compactions the run's
	// sessions ran.
	Compactions int
	// CompactionsNetSaved is the total tokens those compactions reclaimed.
	CompactionsNetSaved int
	// LastCompactionBefore and LastCompactionAfter are the request prompt
	// sizes around the run's last applied compaction, zero when none ran.
	LastCompactionBefore int
	LastCompactionAfter  int
}

// Summary returns the run's model-telemetry snapshot as a value: the request
// counts, the usage outcomes, the token sums the provider reported, the
// peaks, the per-role/model table sorted by role then model, and the
// compaction account. A nil receiver returns the zero summary: a run with no
// telemetry tallied nothing and reports nothing.
func (t *ModelTelemetry) Summary() ModelTelemetrySummary {
	if t == nil {
		return ModelTelemetrySummary{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	summary := ModelTelemetrySummary{
		ProbeRequests:            t.probes,
		WorkRequests:             t.work,
		Retries:                  t.retries,
		TotalRequests:            t.total,
		UsageReported:            t.reported,
		UsageMissing:             t.missing,
		InputTokens:              t.input,
		OutputTokens:             t.output,
		CacheReadTokens:          t.cacheRead,
		CacheWriteTokens:         t.cacheWrite,
		MaxInputTokens:           t.maxInput,
		MaxEstimatedPromptTokens: t.maxEstimated,
		Compactions:              t.compactions,
		CompactionsNetSaved:      t.compactionNet,
		LastCompactionBefore:     t.lastBefore,
		LastCompactionAfter:      t.lastAfter,
		ByRoleModel:              make([]RoleModelRequests, 0, len(t.cells)),
	}
	for key, cell := range t.cells {
		summary.ByRoleModel = append(summary.ByRoleModel, RoleModelRequests{
			Role:          key.role,
			Model:         key.model,
			Requests:      cell.requests,
			Retries:       cell.retries,
			UsageReported: cell.reported,
			UsageMissing:  cell.missing,
		})
	}
	sort.Slice(summary.ByRoleModel, func(i, j int) bool {
		a, b := summary.ByRoleModel[i], summary.ByRoleModel[j]
		if a.Role != b.Role {
			return a.Role < b.Role
		}
		return a.Model < b.Model
	})
	return summary
}

// tallyRequest folds one observation into the run's tallies. The caller
// holds t.mu.
func (t *ModelTelemetry) tallyRequest(observation RequestObservation) {
	key := cellKey{role: observation.Role, model: observation.Model}
	cell := t.cells[key]
	if cell == nil {
		cell = &cellTally{}
		t.cells[key] = cell
	}
	t.total++
	cell.requests++
	switch observation.Class {
	case RequestProbe:
		t.probes++
	case RequestWork:
		t.work++
		if observation.Attempt > 0 {
			t.retries++
			cell.retries++
		}
	}
	if observation.Usage != nil {
		t.reported++
		cell.reported++
		t.input += observation.Usage.InputTokens
		t.output += observation.Usage.OutputTokens
		t.cacheRead += observation.Usage.CacheReadTokens
		t.cacheWrite += observation.Usage.CacheWriteTokens
		if observation.Usage.InputTokens > t.maxInput {
			t.maxInput = observation.Usage.InputTokens
		}
	} else {
		t.missing++
		cell.missing++
	}
	if observation.EstimatedPromptTokens > t.maxEstimated {
		t.maxEstimated = observation.EstimatedPromptTokens
	}
}

// SessionCompacted delivers each applied-compaction event to both the run's
// primary compaction sink and the run's model-telemetry tally (#738).
// Either half may be absent: the chain is how the coordinator gives the
// telemetry a compaction account without changing what the primary sink
// receives, and its absence leaves the primary sink exactly as it stood.
// Both deliveries are isolated: telemetry is observability, and a failing
// half is dropped, never propagated into the compaction that fed it.
type chainedCompactions struct {
	primary CompactionSink
	tally   *ModelTelemetry
}

// SessionCompacted implements CompactionSink.
func (c chainedCompactions) SessionCompacted(event CompactEvent) {
	if c.primary != nil {
		deliverCompaction(c.primary, event)
	}
	c.tally.SessionCompacted(event)
}

// deliverCompaction hands one applied-compaction event to a sink, isolating
// the run from a sink failure (#738) the same way request delivery is. A nil
// sink delivers nothing.
func deliverCompaction(sink CompactionSink, event CompactEvent) {
	if sink == nil {
		return
	}
	defer func() { _ = recover() }()
	sink.SessionCompacted(event)
}
