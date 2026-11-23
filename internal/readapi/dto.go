package readapi

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Envelope wraps every projected response so consumers can always see the
// apiVersion and kind of what they are reading. Payload carries the DTO for
// the endpoint.
type Envelope[T any] struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Payload    T      `json:"payload"`
}

// NewEnvelope wraps payload for serving.
func NewEnvelope[T any](kind string, payload T) Envelope[T] {
	return Envelope[T]{APIVersion: APIVersion, Kind: kind, Payload: payload}
}

// Pagination is the pagination block of a list response. Total is the count
// the projector could compute cheaply from the objects it was handed (the
// server filters a namespace-wide List in-process on every request — no
// index or field-selector machinery for v1 — so with filtering applied it is
// a floor, not a global count); More states whether another page exists.
// Truncated states whether the listing pass stopped at the request-cost cap
// before the namespace was exhausted, in which case Total and More describe
// only the served window.
type Pagination struct {
	Page      int  `json:"page"`
	PageSize  int  `json:"pageSize"`
	Total     int  `json:"total"`
	More      bool `json:"more"`
	Truncated bool `json:"truncated,omitempty"`
}

// Overview is the single-namespace snapshot served by GET overview.
// Truncated states that the namespace scan hit the server's fetch cap: the
// counts below then describe a bounded window, not the whole namespace.
type Overview struct {
	Namespace     string         `json:"namespace"`
	GeneratedAt   *metav1.Time   `json:"generatedAt,omitempty"`
	Freshness     Freshness      `json:"freshness"`
	Truncated     bool           `json:"truncated,omitempty"`
	Runs          RunsOverview   `json:"runs"`
	Lanes         LaneOverview   `json:"lanes"`
	Sources       SourceOverview `json:"sources"`
	ConditionRuns map[string]int `json:"conditionRuns,omitempty"`
}

// RunsOverview totals runs by phase across the namespace.
type RunsOverview struct {
	Total   int            `json:"total"`
	ByPhase map[string]int `json:"byPhase,omitempty"`
}

// LaneOverview summarizes the lane projections.
type LaneOverview struct {
	Count     int  `json:"count"`
	Saturated bool `json:"saturated"`
	Suspended bool `json:"suspended"`
	Cooldown  bool `json:"cooldown"`
	Unhealthy int  `json:"unhealthy"`
}

// SourceOverview summarizes the source projections.
type SourceOverview struct {
	Count       int `json:"count"`
	Healthy     int `json:"healthy"`
	Empty       int `json:"empty"`
	Stale       int `json:"stale"`
	Unavailable int `json:"unavailable"`
	Blocked     int `json:"blocked"`
}

// LaneHealth is one lane's projected state for GET lanes.
//
// Admitted counts runs holding an execution-capacity slot (Claimed + Running,
// the operator's admission gate); Reserved counts runs holding a discovery
// reservation (the source runner's gate): empty phase, Pending, Claimed, or
// Running. Reserved is therefore a superset of Admitted — an admitted run
// holds both gates — and a consumer must not add the two. Verifying and
// terminal phases hold neither. Occupied slots are never inferred from the
// count of retained CRs.
type LaneHealth struct {
	Name                  string          `json:"name"`
	Availability          Availability    `json:"availability"`
	Freshness             Freshness       `json:"freshness"`
	Notes                 []string        `json:"notes,omitempty"`
	ConfiguredConcurrency int             `json:"configuredConcurrency"`
	Admitted              int             `json:"admitted"`
	Reserved              int             `json:"reserved"`
	Saturated             bool            `json:"saturated"`
	Suspended             bool            `json:"suspended"`
	Cooldown              *CooldownHealth `json:"cooldown,omitempty"`
	PendingOldestSeconds  *int64          `json:"pendingOldestSeconds,omitempty"`
	// DerivedFrom documents how the counts above were derived, so a consumer
	// never mistakes them for a live scheduler view.
	DerivedFrom string `json:"derivedFrom"`
}

// CooldownHealth projects a lane's active capacity cooldown.
type CooldownHealth struct {
	CapacityID      string       `json:"capacityID"`
	Reason          string       `json:"reason"`
	ExhaustionCount int          `json:"exhaustionCount"`
	NextEligibleAt  *metav1.Time `json:"nextEligibleAt,omitempty"`
}

// SourceHealth is one source adapter's projected state for GET sources and
// overview aggregation. A configured source with zero runs appears with
// availability Empty. Freshness is derived from the source's run activity
// only: a source adapter's discovery state (its last successful poll) lives
// in process metrics, not cluster state, so the read model cannot surface it.
// An Empty source therefore reads freshness unknown — never a discovery
// failure, and never proof that discovery is healthy.
//
// LastSuccessAt and LastErrorAt carry the source's last recorded outcome from
// run terminal state: LastSuccessAt is the latest FinishedAt among the
// source's Done and AlreadyDone runs, and LastErrorAt is the latest
// FinishedAt among its Failed runs. A run parked in a waiting terminal phase
// (AwaitingReview, HandedOff, NeedsHuman, BlockedExternal) is neither a
// success nor an error and stamps neither field. Nil means no such run was
// projected (or none carried a recorded finish): a consumer must render the
// absence explicitly and never read it as "never failed" or "never
// succeeded".
type SourceHealth struct {
	Name                 string         `json:"name"`
	Availability         Availability   `json:"availability"`
	Freshness            Freshness      `json:"freshness"`
	Notes                []string       `json:"notes,omitempty"`
	ObservedAt           *metav1.Time   `json:"observedAt,omitempty"`
	LastSuccessAt        *metav1.Time   `json:"lastSuccessAt,omitempty"`
	LastErrorAt          *metav1.Time   `json:"lastErrorAt,omitempty"`
	TotalRuns            int            `json:"totalRuns"`
	RunsByPhase          map[string]int `json:"runsByPhase,omitempty"`
	OldestPendingSeconds *int64         `json:"oldestPendingSeconds,omitempty"`
}

// RunSummary is one run's projected row for GET runs (paginated list).
// Spec-side source-owned opaque identifiers — workItemID, sourceContext,
// sourceReceipt — are never projected: spec.workItemID doubles as a
// client-supplied contribution idempotency key (#510), so it can carry
// secret-shaped material and must never cross this surface. The run's name
// is the server-side correlation identity.
type RunSummary struct {
	Name           string       `json:"name"`
	Namespace      string       `json:"namespace"`
	Phase          string       `json:"phase"`
	Terminal       bool         `json:"terminal"`
	CreatedAt      *metav1.Time `json:"createdAt,omitempty"`
	AgeSeconds     *int64       `json:"ageSeconds,omitempty"`
	PhaseEnteredAt *metav1.Time `json:"phaseEnteredAt,omitempty"`
	Source         string       `json:"source"`
	Lane           string       `json:"lane"`
	// PRNumber is the parsed pull-request number; nil when Status.PR carries
	// no parseable number (e.g. a URL the projector cannot resolve). Absence
	// is explicit, never a silent zero.
	PRNumber *int   `json:"prNumber,omitempty"`
	Branch   string `json:"branch,omitempty"`
	HeadSHA  string `json:"headSHA,omitempty"`
	Restarts int    `json:"restarts"`
}

// RunDetail is the single-run response for GET runs/{name}: a RunSummary plus
// the allowlisted per-run state and bounded status-timestamp timeline. Every
// sub-block states its own presence and freshness so missing data is never read as a zero.
type RunDetail struct {
	RunSummary `json:",inline"`

	Coordinator       *CoordinatorDiagnostic `json:"coordinator,omitempty"`
	Heartbeat         HeartbeatHealth        `json:"heartbeat"`
	Checkpoint        CheckpointHealth       `json:"checkpoint"`
	Publication       PublicationHealth      `json:"publication"`
	CIPark            *CIParkHealth          `json:"ciPark,omitempty"`
	LifecycleReport   LifecycleReportHealth  `json:"lifecycleReport"`
	Admission         AdmissionHealth        `json:"admission"`
	Reap              ReapHealth             `json:"reap"`
	WorkspaceSafety   *WorkspaceSafetyView   `json:"workspaceSafety,omitempty"`
	NativeResultKind  string                 `json:"nativeResultKind,omitempty"`
	NativeResultAt    *metav1.Time           `json:"nativeResultAt,omitempty"`
	Conditions        []ConditionView        `json:"conditions,omitempty"`
	Timeline          []TimelineView         `json:"timeline,omitempty"`
	TimelineTruncated bool                   `json:"timelineTruncated,omitempty"`
	TerminalReason    string                 `json:"terminalReason,omitempty"`
}

// CoordinatorDiagnostic is the redacted projection of Status.TerminalDiagnostic.
// Image, node, container ID, pod UID, and pod environment are never projected.
type CoordinatorDiagnostic struct {
	Category   string             `json:"category"`
	ExitCode   *int32             `json:"exitCode,omitempty"`
	Outcome    string             `json:"outcome,omitempty"`
	Reason     string             `json:"reason,omitempty"`
	ObservedAt *metav1.Time       `json:"observedAt,omitempty"`
	Freshness  Freshness          `json:"freshness"`
	Container  *ContainerIdentity `json:"container,omitempty"`
}

// ContainerIdentity carries only the coordinator container's name, restart
// count, and bounded termination reason.
type ContainerIdentity struct {
	Name              string `json:"name,omitempty"`
	RestartCount      int32  `json:"restartCount"`
	TerminationReason string `json:"terminationReason,omitempty"`
}

// HeartbeatHealth states coordinator heartbeat presence and recency. UID and
// kind are dropped; only liveness evidence is projected.
type HeartbeatHealth struct {
	HasHeartbeat    bool         `json:"hasHeartbeat"`
	LastHeartbeatAt *metav1.Time `json:"lastHeartbeatAt,omitempty"`
	Freshness       Freshness    `json:"freshness"`
}

// CheckpointHealth states checkpoint presence only. Plan text and brief
// summaries are model output and are never projected. The API carries no
// checkpoint timestamp, so none is projected.
type CheckpointHealth struct {
	Present    bool `json:"present"`
	BriefCount int  `json:"briefCount"`
}

// PublicationHealth projects publication/verification state. Fingerprints are
// projected as presence booleans, never their values (they are compact
// identity strings a consumer does not need, and their preimage includes
// check names).
type PublicationHealth struct {
	LastCommit                string `json:"lastCommit,omitempty"`
	HasCheckFingerprint       bool   `json:"hasCheckFingerprint"`
	HasFailedCheckFingerprint bool   `json:"hasFailedCheckFingerprint"`
	DeclaredNoChange          bool   `json:"declaredNoChange"`
}

// CIParkHealth projects Status.CIPark.
type CIParkHealth struct {
	Head        string       `json:"head"`
	ParkedAt    *metav1.Time `json:"parkedAt,omitempty"`
	Attempts    int          `json:"attempts"`
	LastOutcome string       `json:"lastOutcome,omitempty"`
}

// LifecycleReportHealth projects the LifecycleReported condition and the retry
// tokens as booleans only: token values are policy state, never response data.
// The condition's reason is free text written along reconcile paths and is
// never projected.
type LifecycleReportHealth struct {
	ConditionPresent    bool         `json:"conditionPresent"`
	ConditionStatus     string       `json:"conditionStatus,omitempty"`
	ObservedAt          *metav1.Time `json:"observedAt,omitempty"`
	HasActiveRetryToken bool         `json:"hasActiveRetryToken"`
	HasLastRetryToken   bool         `json:"hasLastRetryToken"`
}

// AdmissionHealth projects admission evidence and the phase/reservation
// disagreement wedge: a run with an AdmittedAt stamp whose phase has fallen
// back to Pending. The state is flagged, never silently zeroed.
type AdmissionHealth struct {
	AdmittedAt   *metav1.Time `json:"admittedAt,omitempty"`
	Disagreement bool         `json:"disagreement"`
	Note         string       `json:"note,omitempty"`
}

// ReapHealth projects retention state: which reap marker is present (by
// annotation name), when it was stamped, the retention outcome that marker
// names, and whether the run's phase is one the reaper manages (Done,
// AlreadyDone, Failed, NeedsHuman; review-parked terminal phases are exempt).
type ReapHealth struct {
	MarkerAnnotation string       `json:"markerAnnotation,omitempty"`
	MarkerAt         *metav1.Time `json:"markerAt,omitempty"`
	Reason           string       `json:"reason,omitempty"`
	EligibleByPhase  bool         `json:"eligibleByPhase"`
	Holds            []string     `json:"holds,omitempty"`
}

// WorkspaceSafetyView projects the executor's workspace snapshot state and git
// identities. Only bounded, non-secret fields cross.
type WorkspaceSafetyView struct {
	State  string `json:"state,omitempty"`
	Branch string `json:"branch,omitempty"`
	Head   string `json:"head,omitempty"`
}

// ConditionView is one allowlisted status condition. Only the condition types
// in conditionAllowlist are projected, and only their type, status, and
// timestamp: a condition's reason is free text written along reconcile paths
// and is never projected.
type ConditionView struct {
	Type       string       `json:"type"`
	Status     string       `json:"status"`
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// TimelineView is one bounded entry in the run's status-derived event timeline.
type TimelineView struct {
	At     *metav1.Time `json:"at,omitempty"`
	Label  string       `json:"label"`
	Detail string       `json:"detail,omitempty"`
}

// RunSummaryList is the paginated GET runs response payload.
type RunSummaryList struct {
	Items      []RunSummary `json:"items"`
	Pagination Pagination   `json:"pagination"`
}

// LaneHealthList is the GET lanes response payload.
type LaneHealthList struct {
	Items      []LaneHealth `json:"items"`
	Pagination Pagination   `json:"pagination"`
}

// SourceHealthList is the source-health response payload.
type SourceHealthList struct {
	Items      []SourceHealth `json:"items"`
	Pagination Pagination     `json:"pagination"`
}
