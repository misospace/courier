// Package readapi is the read-model projection layer between the CoderRun /
// LaneProfile API objects and the operator-facing HTTP surface served in a
// later phase. It holds plain Go structs with JSON tags (never api/v1alpha1
// types, so no deepcopy machinery) and pure projection functions that shape
// cluster state into those structs.
//
// The package is a redaction boundary. Projection is an allowlist: a field
// reaches a response DTO only because this package explicitly copies it.
// Source receipts, source context, checkpoint plan and brief text, policy
// tokens, and pod environment never cross. Free-form text that may carry
// source or model content is bounded by boundTruncate before projection.
package readapi

import (
	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// APIVersion is the envelope apiVersion every projected response carries.
const APIVersion = "courier.misospace.dev/v1"

// Response kinds for the envelope. Internal constants only; no new API
// machinery is introduced.
const (
	KindOverview   = "Overview"
	KindLaneList   = "LaneHealthList"
	KindSourceList = "SourceHealthList"
	KindRunList    = "RunSummaryList"
	KindRunDetail  = "RunDetail"
)

// Availability is the shared state vocabulary for entities whose data can be
// absent, late, or inapplicable. Projections never encode "missing" as a zero
// value: an explicit state always accompanies data that may be missing.
type Availability string

const (
	// AvailabilityHealthy: present, and no negative evidence observed.
	AvailabilityHealthy Availability = "Healthy"
	// AvailabilityEmpty: the entity is known but carries no data (e.g. a
	// configured source or lane with zero runs).
	AvailabilityEmpty Availability = "Empty"
	// AvailabilityStale: data is present but older than the staleness window.
	AvailabilityStale Availability = "Stale"
	// AvailabilityUnavailable: negative availability evidence observed (e.g. a
	// provider or forge read could not complete).
	AvailabilityUnavailable Availability = "Unavailable"
	// AvailabilityUnsupported: the data cannot be projected for this entity
	// (e.g. a referenced LaneProfile that does not exist).
	AvailabilityUnsupported Availability = "Unsupported"
	// AvailabilityBlocked: an operator- or safety-owned hold is in force
	// (suspension, capacity cooldown, unsafe workspace).
	AvailabilityBlocked Availability = "Blocked"
	// AvailabilityUnknown: data may exist but its state cannot be determined
	// from the projected objects alone.
	AvailabilityUnknown Availability = "Unknown"
)

// Freshness is the companion state for observed-at timestamps: it must always
// be present where the timestamp itself may be absent, so a nil timestamp is
// never silently read as "now" or as zero.
type Freshness string

const (
	// FreshnessFresh: the observation is inside the staleness window.
	FreshnessFresh Freshness = "Fresh"
	// FreshnessStale: the observation is older than the staleness window.
	FreshnessStale Freshness = "Stale"
	// FreshnessUnknown: no observation exists.
	FreshnessUnknown Freshness = "Unknown"
)

// Reap-marker retention annotations, mirrored locally from the controller's
// reaper so this package does not depend on internal/controller. The values
// must match internal/controller/reap.go exactly; the read model only reads
// them, the controller remains their sole writer.
const (
	AnnotationDoneAt        = "courier.misospace.dev/done-at"
	AnnotationFailedAt      = "courier.misospace.dev/failed-at"
	AnnotationNeedsHumanAt  = "courier.misospace.dev/needs-human-at"
	AnnotationAlreadyDoneAt = "courier.misospace.dev/already-done-at"
)

// DefaultTextBound is the default rune bound applied by boundTruncate to any
// projected string that can carry source or model-adjacent text.
const DefaultTextBound = 200

// phase membership predicates, re-implemented locally (the readapi package
// must not import internal/controller or internal/source). The semantics
// mirror the controller: internal/controller/admission.go's
// phaseConsumesCapacity and internal/source/runner.go's PhaseReservesDiscovery.

// phaseSet is the closed CoderRun phase enum as projected.
var phaseSet = map[courierv1alpha1.Phase]bool{
	courierv1alpha1.PhasePending:         true,
	courierv1alpha1.PhaseClaimed:         true,
	courierv1alpha1.PhaseRunning:         true,
	courierv1alpha1.PhaseVerifying:       true,
	courierv1alpha1.PhaseAwaitingReview:  true,
	courierv1alpha1.PhaseHandedOff:       true,
	courierv1alpha1.PhaseNeedsHuman:      true,
	courierv1alpha1.PhaseDone:            true,
	courierv1alpha1.PhaseAlreadyDone:     true,
	courierv1alpha1.PhaseFailed:          true,
	courierv1alpha1.PhaseBlockedExternal: true,
}

// ValidPhase reports whether phase is a member of the CoderRun phase enum. An
// out-of-enum value is projected as unknown rather than interpreted.
func ValidPhase(phase courierv1alpha1.Phase) bool {
	return phaseSet[phase]
}

// phaseConsumesCapacity mirrors the operator's admission gate: Claimed and
// Running runs hold one execution-capacity slot on their lane.
func phaseConsumesCapacity(phase courierv1alpha1.Phase) bool {
	return phase == courierv1alpha1.PhaseClaimed || phase == courierv1alpha1.PhaseRunning
}

// phaseReservesDiscovery mirrors the source runner's capacity gate: empty-phase
// and Pending runs reserve discovery capacity alongside Claimed and Running.
func phaseReservesDiscovery(phase courierv1alpha1.Phase) bool {
	switch phase {
	case "", courierv1alpha1.PhasePending, courierv1alpha1.PhaseClaimed, courierv1alpha1.PhaseRunning:
		return true
	}
	return false
}

// phaseIsReapEligible mirrors the controller reaper's phase eligibility
// (internal/controller/reap.go): only Done, AlreadyDone, Failed, and
// NeedsHuman runs are ever reaped, subject to retention holds. AwaitingReview,
// HandedOff, and BlockedExternal are terminal but explicitly exempt, so
// eligibility must not be inferred from phaseIsTerminal.
func phaseIsReapEligible(phase courierv1alpha1.Phase) bool {
	switch phase {
	case courierv1alpha1.PhaseDone, courierv1alpha1.PhaseAlreadyDone,
		courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseNeedsHuman:
		return true
	}
	return false
}

// phaseIsTerminal mirrors the controller's terminal phases: the set on which
// Status.FinishedAt is (or would be) stamped, including AlreadyDone.
func phaseIsTerminal(phase courierv1alpha1.Phase) bool {
	switch phase {
	case courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseHandedOff,
		courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseDone,
		courierv1alpha1.PhaseAlreadyDone, courierv1alpha1.PhaseFailed,
		courierv1alpha1.PhaseBlockedExternal:
		return true
	}
	return false
}

// capacityExhaustionCategories mirrors isCapacityExhaustionRun in
// internal/controller/capacity_recovery.go: the terminal diagnostic categories
// that put a BlockedExternal run in the operator's capacity-recovery path.
var capacityExhaustionCategories = map[courierv1alpha1.TerminalDiagnosticCategory]bool{
	courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion:  true,
	courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable: true,
	courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable:    true,
	courierv1alpha1.TerminalDiagnosticCategoryModelTransportError: true,
}

// phaseIsCapacityExhaustion mirrors the controller's capacity-exhaustion gate:
// a BlockedExternal run whose terminal diagnostic names a failure the operator
// must settle before it releases ownership of the attempt's partial work.
func phaseIsCapacityExhaustion(run *courierv1alpha1.CoderRun) bool {
	return run != nil && run.Status.Phase == courierv1alpha1.PhaseBlockedExternal &&
		run.Status.TerminalDiagnostic != nil &&
		capacityExhaustionCategories[run.Status.TerminalDiagnostic.Category]
}

// CapacityRecovery condition reasons, mirrored from
// internal/controller/capacity_recovery.go so the read model can name the
// operator's own settlement states. The controller remains the sole writer.
const (
	capacityRecoverySettled     = "Settled"
	capacityRecoveryUnsupported = "Unsupported"
	capacityRecoveryStale       = "Stale"
	capacityRecoveryUnsafeWork  = "UnsafeWork"
)

// Timeline source-of-truth vocabularies. Source names the run-object field
// path that carries an entry's timestamp; Authority names who writes that
// field. Together they attribute every observation time the page shows, so an
// operator sees which recorded authority a stamp came from rather than being
// asked to trust the read model. Entries derive only from structured status on
// this one object: no log, transcript, or cluster-wide query is ever replayed.
const (
	TimelineSourceCreationTimestamp = "metadata.creationTimestamp"
	TimelineSourceAdmittedAt        = "status.admittedAt"
	TimelineSourceStartedAt         = "status.startedAt"
	TimelineSourcePhaseEnteredAt    = "status.phaseEnteredAt"
	TimelineSourceHeartbeatAt       = "status.heartbeat.at"
	TimelineSourceCIParkParkedAt    = "status.ciPark.parkedAt"
	TimelineSourceFinishedAt        = "status.finishedAt"
	TimelineSourceDiagnosticAt      = "status.terminalDiagnostic.observedAt"
	TimelineSourceNativeResultAt    = "status.nativeResult.at"
	// TimelineSourceRetention prefixes the reap-marker annotation that stamped
	// the entry, e.g. metadata.annotations/courier.misospace.dev/failed-at.
	TimelineSourceRetention = "metadata.annotations"
	// TimelineSourceCondition prefixes the allowlisted condition type that
	// stamped the entry, e.g. status.conditions[LifecycleReported].
	TimelineSourceCondition = "status.conditions"
)

// Timeline authorities: who writes the field a timeline entry reads.
const (
	// TimelineAuthorityOperator is the Courier controller: admission stamps,
	// phase transitions, CI park, terminal dispatch, retention markers, and
	// status conditions all land through its reconciles.
	TimelineAuthorityOperator = "operator"
	// TimelineAuthorityCoordinator is the run's own coordinator process, whose
	// heartbeat and terminal result reach the object only through the
	// operator's validated status path.
	TimelineAuthorityCoordinator = "coordinator"
	// TimelineAuthorityAPIServer is the API server's bookkeeping for the object
	// itself, namely its creation timestamp.
	TimelineAuthorityAPIServer = "apiserver"
)
