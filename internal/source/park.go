package source

import (
	"context"
	"errors"
)

// ErrUnsupportedCIPark means the source cannot represent the CI park/repair
// lifecycle at all. It is permanent for this adapter: the core records it as
// a diagnostic and never emulates the capability with ordinary
// Claim/Release/Transition calls.
var ErrUnsupportedCIPark = errors.New("source: CI park/repair lifecycle is unsupported")

// CIParkNotice tells the source that a run parked on pending CI for its
// published head (#475). A connector that manages PR-fix queues uses it to
// avoid racing a new fix attempt against an active Courier repair
// opportunity. PR is the run's pull request identity; Head is the exact
// parked head commit. IdempotencyKey deduplicates a retried report of the
// same park; an adapter without deduplication may ignore it.
type CIParkNotice struct {
	PR             string
	Head           string
	IdempotencyKey string
}

// CIRepairReservation is the fenced request for one bounded repair
// opportunity. FenceToken pins the reservation to one repair attempt of this
// run: it is opaque, derived by the core from the immutable run incarnation,
// and never from model, worker, or lane names. An adapter that cannot honor
// the reservation because the work was superseded — a newer generation
// claimed the work, or another worker was dispatched — must return
// ErrSuperseded and must never grant against a stale generation. PR is the
// pull request identity and Head is the parked head the repair would resume
// from; an adapter must re-read remote state and refuse the reservation if
// it no longer matches.
type CIRepairReservation struct {
	FenceToken     string
	IdempotencyKey string
	PR             string
	Head           string
}

// CIRepairGrant is the source-issued proof that exactly one repair
// opportunity is reserved for this run. FenceToken is the grant token the
// matching CIRepairOutcome must present back.
type CIRepairGrant struct {
	FenceToken string
}

// CIRepairResult is the outcome kind of one repair attempt.
type CIRepairResult string

const (
	// CIRepairRepushed: the coordinator fixed, revalidated, and pushed a new
	// head; CI observation starts over on that head.
	CIRepairRepushed CIRepairResult = "repushed"
	// CIRepairExhausted: the operator's retry/no-progress budget ran out
	// without a repairable failure; the run hands off.
	CIRepairExhausted CIRepairResult = "exhausted"
	// CIRepairUnsupported: recovery was not possible (unsupported checks,
	// unobservable PR, superseded head, or a safe handoff boundary); the run
	// hands off.
	CIRepairUnsupported CIRepairResult = "unsupported"
)

// CIRepairOutcome is the fenced, idempotent outcome of one repair attempt
// (#475). FenceToken must be the token issued by the CIRepairGrant this
// attempt reserved; an adapter must refuse an outcome carrying a token it no
// longer records with ErrSuperseded rather than mutating a newer owner. Head
// is the newly published head after a repush and is empty when nothing was
// pushed. Retries of the same IdempotencyKey must collapse into one
// transition and replay the originally committed result.
type CIRepairOutcome struct {
	FenceToken     string
	IdempotencyKey string
	PR             string
	Head           string
	Result         CIRepairResult
}

// CIParkLifecycle is the optional source capability for the CI
// park-and-resume loop (#475). ReportCIParked announces the park so a
// connector can defer its own PR-fix dispatch; ReserveCIRepair atomically
// reserves one repair opportunity or refuses with ErrSuperseded or
// ErrStaleWork; ReportCIRepair publishes the fenced, idempotent outcome of
// that attempt. A source that cannot provide claim/generation-fenced,
// idempotent transitions must not implement the interface, and must never
// emulate it with ordinary lifecycle calls: the core treats a missing
// implementation as unsupported.
type CIParkLifecycle interface {
	ReportCIParked(context.Context, WorkItem, CIParkNotice) error
	ReserveCIRepair(context.Context, WorkItem, CIRepairReservation) (CIRepairGrant, error)
	ReportCIRepair(context.Context, WorkItem, CIRepairOutcome) error
}
