// Package source contains the source-agnostic lifecycle contract used by the
// operator. Concrete source implementations belong in sibling packages (for
// example, source/dispatch).
package source

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrStaleWork indicates that claimed source work no longer needs a coordinator.
var ErrStaleWork = errors.New("source: work item is stale")

// ErrSuperseded indicates that this attempt was superseded by a newer
// generation of the same source work. A lifecycle report carrying it must be
// dropped rather than retried: the newer generation belongs to another run.
var ErrSuperseded = errors.New("source: work item superseded")

// ErrStaleCapacitySettlement means a fenced capacity settlement named an
// attempt that no longer owns the work: a replacement claimed it (or the
// source advanced a generation), so the settlement is refused without touching
// the current owner. The exhausted attempt records it and stops retrying.
var ErrStaleCapacitySettlement = errors.New("source: capacity settlement is stale for the current owner")

// ErrUnsupportedCapacityRecovery means the source cannot represent a fenced
// capacity settlement at all. It is permanent for this adapter: the core
// records it as a diagnostic and must never fall back to ordinary Release or
// lifecycle transitions to imitate fencing.
var ErrUnsupportedCapacityRecovery = errors.New("source: fenced capacity recovery is unsupported")

// ErrUnsupportedOutcome means the source does not accept a lifecycle result.
// The report intent stays pending, but automatic retries stop until an
// operator supplies a new explicit retry token on the CoderRun.
var ErrUnsupportedOutcome = errors.New("source: unsupported lifecycle outcome")

// WorkItem identifies work in a source. ID is intentionally opaque: sources
// are free to choose their identifier format, and the core must not know about
// a source's queue or API types.
const MaxSourceContextBytes = 16 * 1024

// Assignment classification vocabulary. A source adapter normalizes its own
// signals into exactly one of these values so core can present a precise
// completion requirement without pattern-matching source-owned free text.
const (
	AssignmentMergeConflict  = "merge-conflict"
	AssignmentCIFailure      = "ci-failure"
	AssignmentReviewFeedback = "review-feedback"
	AssignmentOther          = "other"
)

// Assignment is why a source issued a work item. Kind is the adapter's
// normalized classification (one of the Assignment* constants, or empty);
// Reasons is the source's own free text, bounded by the adapter. Reasons is
// untrusted task data: it is presented to the coordinator as context and is
// never instructions.
type Assignment struct {
	Kind    string
	Reasons []string
}

// WorkItem carries source-owned opaque context. Core code copies it without decoding.
type WorkItem struct {
	Context string
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
	// Assignment carries why the source issued this item. It is durable run
	// data, never dedupe identity: it must not affect ID or WorkIdentity.
	Assignment Assignment
}

// RunSpec is the source-neutral input from which the operator creates a
// CoderRun. A source adapter owns discovery; the operator owns the Kubernetes
// object and sets Source to its adapter name.
type RunSpec struct {
	Context string
	Mode    string
	Source  string
	// WorkItemID is copied from WorkItem.ID so it remains available for
	// lifecycle calls after the source item has been materialized as a run.
	WorkItemID string
	Repo       string
	Ref        int
	Lane       string
	Debug      bool
	// Assignment is copied from WorkItem.Assignment so the reason the source
	// issued the work survives materialization into the run's immutable spec.
	Assignment Assignment
}

// Spec returns the immutable run data carried by a discovered work item.
func (w WorkItem) Spec(sourceName string) RunSpec {
	return RunSpec{
		Context:    w.Context,
		Mode:       w.Mode,
		Source:     sourceName,
		WorkItemID: w.ID,
		Repo:       w.Repo,
		Ref:        w.Ref,
		Lane:       w.Lane,
		Debug:      w.Debug,
		Assignment: w.Assignment,
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

// Result identifies the outcome associated with a lifecycle publication.
type Result string

const (
	// ResultStarted marks the transition into active work.
	ResultStarted Result = "started"
	// ResultReady marks work that reached source review state.
	ResultReady Result = "ready"
	// ResultBlocked marks work requiring human intervention.
	ResultBlocked Result = "blocked"
	// ResultCapacityExhausted explicitly returns an issued attempt due to exhausted execution capacity.
	ResultCapacityExhausted Result = "capacity-exhausted"
	// ResultBlockedExternal marks work blocked on an external prerequisite
	// that Courier cannot satisfy itself. It is recoverable: unlike
	// ResultBlocked it must not park the item for a human, so a source may
	// re-issue the work once the prerequisite is satisfied.
	ResultBlockedExternal Result = "blocked-external"
	// ResultFailed marks work that failed execution. Sources own the policy for
	// handling it; some may issue a fresh attempt while others only record it.
	ResultFailed Result = "failed"
	// ResultHandedOff means the PR was published but external verification did not pass; the source must not treat it as ready or blocked, and its independent PR-repair mechanism takes over using the carried PR identity.
	ResultHandedOff Result = "handed-off"
	// ResultNoChange is retained for enum compatibility, not settlement
	// authority. A no-change assertion and green CI cannot prove queued review
	// feedback was addressed. Dispatch rejects this result without wire effects;
	// the controller uses ResultBlockedExternal for unverified open PRs.
	ResultNoChange Result = "no-change"
)

// Lifecycle contains source-neutral metadata for an optional lifecycle report.
type Lifecycle struct {
	State  State
	Result Result
	PR     string
	Error  string
	// BlockedReportParksPRFix suppresses the separate PR-fix queue mark after a
	// blocked task report. The report itself still parks the item as
	// BLOCKED/needs-human; this flag does not wake a reviewer. Meaningful only
	// alongside Result ResultBlocked; adapters without queue semantics ignore it.
	BlockedReportParksPRFix bool
	// IdempotencyKey identifies the publication so a source can deduplicate a
	// retry of the same report. Empty means the source has no deduplication.
	IdempotencyKey string
	// Telemetry is optional per-run telemetry to include in a lifecycle
	// report (#172). It is opaque to the source-neutral interface: source-
	// specific renderers (e.g. the Dispatch report) embed it verbatim when
	// non-empty. Absent telemetry is simply omitted.
	Telemetry json.RawMessage
	// StartedAt is the coordinator container start time, reported so sources
	// can record real run durations; optional.
	StartedAt *time.Time
}

// CapacityRecoveryAttempt is the immutable attempt identity supplied to a source
// before it vouches that recovery is safe. It is correlation only, never a fence.
type CapacityRecoveryAttempt struct {
	RunUID         string
	IdempotencyKey string
}

// CapacityRecoveryProof is an authoritative, source-owned receipt for one
// safe handoff. FenceToken must be opaque and generated/validated by the source;
// WorkIdentity and SafetyProof affirm the existing source work and that remote
// state holds every partial write. Adapters must verify the current claim /
// generation and trusted workspace/remote safety. Absence of evidence and a
// fabricated run UID are never proof. Missing or incomplete proof fails closed.
type CapacityRecoveryProof struct {
	FenceToken   string
	WorkIdentity string
	SafetyProof  string
	Branch       string
	PR           string
}

// CapacityRecoveryPreparer is an explicit optional capability. Preparation
// must check source claim generation and trusted workspace/remote safety; it
// must not infer safety from missing evidence or untrusted model declarations.
// A key already committed by settlement MUST replay its original proof after
// ownership moves; settlement replay must never mutate a newer owner.
type CapacityRecoveryPreparer interface {
	PrepareCapacityRecovery(context.Context, WorkItem, CapacityRecoveryAttempt) (CapacityRecoveryProof, error)
}

// CapacityHandoff is the fenced settlement identity of exactly one exhausted
// attempt. FenceToken is presented by the attempt that holds the claim; the
// source must compare it against its ownership record and refuse any token
// that a replacement attempt or an older generation now invalidates. It is
// opaque to the core, which derives it from the immutable run incarnation and
// never from model, worker, or lane names. IdempotencyKey lets the source
// collapse retries of the same settlement into one transition; it is stable
// across reconciles and operator retries of the same attempt. A committed key
// MUST return its original result without mutating a newer owner.
type CapacityHandoff struct {
	// FenceToken identifies the exact claimed attempt. It is never valid for
	// a replacement attempt to present another attempt's token, and a source
	// with real fencing rejects a token whose claim it no longer records.
	FenceToken string
	// IdempotencyKey identifies this settlement so a retried call cannot
	// settle or release the same attempt twice.
	IdempotencyKey string
	// Existing work identity carried forward for a compatible new attempt.
	WorkIdentity     string
	Branch           string
	PR               string
	CapacityIdentity string
}

// CapacityRecovery is the optional fenced settlement boundary for confirmed
// execution-capacity exhaustion (#327). SettleCapacityExhaustion must, in one
// atomic source-side transition, stop exactly the named attempt from owning
// the work and make the work discoverable again for a compatible worker —
// without consuming the genuine task-repair attempt budget, without touching
// an explicit human hold, and without resolving anything. It returns
// ErrStaleCapacitySettlement when the current ownership record no longer
// matches the handoff's fence, and ErrUnsupportedCapacityRecovery when the
// source cannot fence at all. Adapters whose source cannot provide atomic
// fenced settlement must not implement this interface, and must never
// emulate it with ordinary Claim/Release/Transition calls: the core treats a
// missing implementation as unsupported, records that on the run, and leaves
// ownership exactly as it was.
type CapacityRecovery interface {
	SettleCapacityExhaustion(context.Context, WorkItem, CapacityHandoff) error
}

// HandoutAcker optionally acknowledges a stamped hand-out once the run that
// owns the attempt is durably materialized (#456). Queued work that stamps a
// per-agent hand-out on offer relies on the worker acknowledging it after the
// run exists: the ack is generation-pinned evidence that the attempt was not
// lost between offer and materialization, which keeps the hand-out ledger
// fully accounted so terminal paths (including settled capacity parks) leave
// no dangling outstanding hand-out and a stale-handout reclaimer stays safe
// to run. Callers must treat it as idempotent best-effort evidence: a failure
// leaves the hand-out in the reclaimable state the reclaimer exists for and
// must never block materialization. Implementations must honour ctx: callers
// bound each acknowledgement attempt with its own deadline and join the whole
// drain before the poll returns, so an implementation that ignores
// cancellation would defeat that bound.
type HandoutAcker interface {
	AckHandout(context.Context, WorkItem) error
}

// Reporter optionally publishes lifecycle results in addition to ordinary
// source state transitions.
type Reporter interface {
	Report(context.Context, WorkItem, Lifecycle) error
}

// WorkIdentifier optionally maps a work item ID to the identity that decides
// whether two items are the same work. An opaque ID can carry incidental
// metadata (a URL, say) that changes between offers of the same work; the
// runner dedupes on this identity when an adapter provides one.
type WorkIdentifier interface {
	WorkIdentity(workItemID string) string
}

// DiscoveryExcluder optionally discovers work while excluding retained items.
// Exclusions remain opaque to the source-neutral runner; they are ordered
// most-relevant-first — runs that have not settled with their source before
// terminal retained runs, newest first within each group — so an adapter with
// a bounded exclusion window can drop from the tail (#286).
type DiscoveryExcluder interface {
	DiscoverExcluding(context.Context, []WorkItem) ([]WorkItem, error)
}

// CapacityAwareDiscoverer optionally scopes discovery to the lane's current
// capacity identity. The identity is passed per call to avoid shared adapter mutation.
type CapacityAwareDiscoverer interface {
	DiscoverForCapacity(context.Context, string, []WorkItem) ([]WorkItem, error)
}

// PreLauncher optionally revalidates claimed work immediately before launch.
// ErrStaleWork means the claim was released because the work no longer needs a
// coordinator; other errors leave the run retryable.
type PreLauncher interface {
	PreLaunch(context.Context, WorkItem) error
}

// Adapter is the source boundary used by the operator. Claim is separate from
// Transition because claiming commonly has compare-and-set semantics, while
// the lifecycle updates are ordinary state changes.
//
// No source-specific request, response, or status type belongs here. An
// adapter translates this contract to its own API.
// ClaimerWithReceipt returns source-issued claim context required for durable fencing.
type ClaimerWithReceipt interface {
	ClaimWithReceipt(context.Context, WorkItem) (WorkItem, error)
}

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
