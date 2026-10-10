package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Mode is the kind of work a CoderRun performs.
// +kubebuilder:validation:Enum=resolve-issue;fix-pr
type Mode string

const (
	// ModeResolveIssue: open a PR that resolves an issue and leave it mergeable.
	ModeResolveIssue Mode = "resolve-issue"
	// ModeFixPR: take over an existing PR and get it back to a healthy state.
	ModeFixPR Mode = "fix-pr"
)

// Phase is the lifecycle phase of a CoderRun.
// +kubebuilder:validation:Enum=Pending;Claimed;Running;Verifying;AwaitingReview;NeedsHuman;Done;Failed
type Phase string

const (
	PhasePending        Phase = "Pending"
	PhaseClaimed        Phase = "Claimed"
	PhaseRunning        Phase = "Running"
	PhaseVerifying      Phase = "Verifying"
	PhaseAwaitingReview Phase = "AwaitingReview"
	PhaseNeedsHuman     Phase = "NeedsHuman"
	PhaseDone           Phase = "Done"
	PhaseFailed         Phase = "Failed"
)

// CoderRunSpec is set once by a source adapter and is then immutable.
//
// There is deliberately no attempts field: each CoderRun is one immutable
// attempt at one goal. History is the sequence of runs linked by the branch/PR.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
type CoderRunSpec struct {
	// Mode is the kind of work: resolve-issue or fix-pr.
	Mode Mode `json:"mode"`

	// Source names the adapter that created this run
	// (dispatch, github-label, cron, cli, web).
	// +kubebuilder:validation:MinLength=1
	Source string `json:"source"`

	// SourceAgent identifies the source-side agent binding for lifecycle calls.
	// Empty preserves the source's legacy, unqualified identity.
	// +optional
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._-]*$`
	SourceAgent string `json:"sourceAgent,omitempty"`

	// WorkItemID is the opaque identifier assigned by the source adapter. It
	// is persisted so lifecycle calls never need to infer source identity from
	// the Kubernetes name or the numeric ref.
	// +kubebuilder:validation:MinLength=1
	WorkItemID string `json:"workItemID"`

	// Repo is the target repository, "owner/name".
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:Pattern=`^[^/]+/[^/]+$`
	Repo string `json:"repo"`

	// Ref is the issue number (resolve-issue) or PR number (fix-pr).
	// +kubebuilder:validation:Minimum=1
	Ref int `json:"ref"`

	// Lane names the LaneProfile that supplies the model ensemble and framing.
	// +kubebuilder:validation:MinLength=1
	Lane string `json:"lane"`

	// Debug, when true, raises the pod log level for this run only.
	// +optional
	Debug bool `json:"debug,omitempty"`
}

// Brief is a completed delegation unit recorded in the checkpoint.
type Brief struct {
	// ID identifies the brief within the run.
	ID string `json:"id"`
	// Summary is the objective/outcome of the brief; it also serves as the
	// recovery record when the checkpoint is lost and only git remains.
	Summary string `json:"summary"`
	// Commit is the pushed commit produced by the brief, if any.
	// +optional
	Commit string `json:"commit,omitempty"`
}

// Checkpoint is the compact, resumable run state. It holds only what the world
// (git, the PR, CI) cannot report; everything the world knows is re-read fresh
// on resume, and the world wins on conflict.
type Checkpoint struct {
	// Plan is the coordinator's working plan.
	// +optional
	Plan string `json:"plan,omitempty"`
	// CompletedBriefs are the delegation units finished so far, in order.
	// +optional
	CompletedBriefs []Brief `json:"completedBriefs,omitempty"`
}

// Heartbeat records the last successful coordinator activity, for liveness.
// Liveness keys on successful streaming or tool calls, never on wall-clock.
type Heartbeat struct {
	// At is when the last successful activity occurred.
	At metav1.Time `json:"at"`
	// Kind is the activity kind: "stream" or "tool".
	Kind string `json:"kind"`
	// CoordinatorPodUID fences this heartbeat to one control incarnation.
	// Only a heartbeat whose UID matches the current coordinator pod may
	// reset the consecutive-restart streak or count as fresh for that
	// incarnation; a previous incarnation's heartbeat is never evidence
	// about the replacement pod.
	// +kubebuilder:validation:MinLength=1
	CoordinatorPodUID string `json:"coordinatorPodUID"`
}

// ActiveOperation is one concurrently dispatched tool/subagent operation in
// the run's active-operation set. It is dispatch evidence for liveness
// suppression — not liveness itself, not a checkpoint — and only a heartbeat
// from the matching control incarnation is earned activity.
type ActiveOperation struct {
	// BriefID identifies the work unit the operation belongs to ("control"
	// for coordinator-level work). Brief IDs identify work units, not
	// transport attempts.
	BriefID string `json:"briefID"`
	// CoordinatorPodUID is the control incarnation that dispatched it.
	CoordinatorPodUID string `json:"coordinatorPodUID"`
	// WorkerPodUID is the worker pod executing it.
	WorkerPodUID string `json:"workerPodUID"`
	// DispatchedAt is when trusted control dispatched the operation. It is
	// diagnostic only: no liveness or timeout semantics attach to it, and
	// there is no operation-age limit.
	DispatchedAt metav1.Time `json:"dispatchedAt"`
}

// PublicationPolicy is the operator-resolved destination for one run. It is
// bound to the CoderRun UID and must not be reconstructed from mutable status.
type PublicationPolicy struct {
	// RunUID binds this policy to one CoderRun incarnation.
	RunUID string `json:"runUID"`
	// ProviderConfigRef identifies the administrator-configured forge provider.
	ProviderConfigRef string `json:"providerConfigRef"`
	// ProviderEndpoint is the canonical API base of the pinned registration.
	// The broker verifies its projection against it and refuses service on
	// mismatch, so selection and enforcement stay the same record across
	// broker replacement.
	// +optional
	ProviderEndpoint string `json:"providerEndpoint,omitempty"`
	// CredentialRefDigest is the SHA-256 identity of the projected broker
	// registration: name, type, endpoint, git endpoint template, and every
	// credential reference. The broker recomputes it from its own projection
	// and refuses service on mismatch. It never contains a credential value.
	// +optional
	CredentialRefDigest string `json:"credentialRefDigest,omitempty"`
	// BaseRepo is the provider-canonical repository receiving the pull request.
	BaseRepo string `json:"baseRepo"`
	// BaseRef is the exact base branch ref.
	BaseRef string `json:"baseRef"`
	// BaseOID is the base ref tip observed at admission.
	BaseOID string `json:"baseOID"`
	// WorkRepo is the provider-canonical repository that owns the writable ref.
	WorkRepo string `json:"workRepo"`
	// WorkRef is the exact publication branch ref.
	WorkRef string `json:"workRef"`
	// WorkInitiallyAbsent distinguishes an absent ref from one with a tip at admission.
	WorkInitiallyAbsent bool `json:"workInitiallyAbsent"`
	// WorkOID is the admission tip when the work ref already existed.
	// +optional
	WorkOID string `json:"workOID,omitempty"`
	// PRNumber is set for fix-pr runs only.
	// +optional
	PRNumber int `json:"prNumber,omitempty"`
	// HeadAnchorOID is the fix-pr head OID observed at admission. It is an
	// ownership anchor, not a frozen tip.
	// +optional
	HeadAnchorOID string `json:"headAnchorOID,omitempty"`
}

// CoderRunStatus is the observed state of a CoderRun.
type CoderRunStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// PublicationPolicy is the immutable destination resolved by the operator
	// before launch. Its RunUID must match this object's current UID.
	// +optional
	PublicationPolicy *PublicationPolicy `json:"publicationPolicy,omitempty"`

	// Branch is the observed work branch. Resolve-issue runs derive it from the
	// repository and issue; fix-pr runs adopt the branch attached to the PR.
	// +optional
	Branch string `json:"branch,omitempty"`

	// HeadRepo is the repository that owns the PR head branch for a fix-pr run.
	// It equals Spec.Repo for same-repository PRs and resolve-issue runs.
	// +optional
	HeadRepo string `json:"headRepo,omitempty"`

	// HeadSHA is the PR head commit observed when the run resolved its head,
	// recorded on the run for verification and resume. It is the observed
	// head, not an enforced pre-push gate.
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`

	// PR is the pull request opened by this run (number or URL).
	// +optional
	PR string `json:"pr,omitempty"`

	// AdmittedAt is when the run first left Pending past the lane
	// capacity/suspend gate. It is set exactly once and never overwritten: a
	// released claim that is re-admitted keeps its original admitted time.
	// +optional
	AdmittedAt *metav1.Time `json:"admittedAt,omitempty"`

	// StartedAt is when the coordinator container first reached Running.
	// Set once, never overwritten.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// FinishedAt is when the run's execution ended (the transition to a
	// terminal phase: AwaitingReview, NeedsHuman, Done, or Failed). It is set
	// exactly once and never overwritten; a later transition (e.g.
	// AwaitingReview -> Done) does not move it.
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// WaitDuration is the human-readable time between creation and StartedAt,
	// in Go duration format (e.g. "3m5s"). Written once when StartedAt is
	// recorded.
	// +optional
	WaitDuration string `json:"waitDuration,omitempty"`

	// RunDuration is the human-readable time between StartedAt and FinishedAt,
	// in Go duration format. Written once when FinishedAt is recorded.
	// +optional
	RunDuration string `json:"runDuration,omitempty"`

	// LastCommit is the last commit pushed to the work branch.
	// +optional
	LastCommit string `json:"lastCommit,omitempty"`

	// CheckFingerprint is the operator's compact identity of the last all-green
	// CI check observation while Verifying: the head commit plus the sorted
	// external check identifiers. A run leaves Verifying for AwaitingReview
	// only when two consecutive all-green observations carry the same
	// fingerprint; a pending or empty observation clears it, and a changed
	// check set resets it, so checks that are still registering cannot pass.
	// +optional
	CheckFingerprint string `json:"checkFingerprint,omitempty"`

	// Checkpoint is the compact resumable state written by the harness.
	// +optional
	Checkpoint *Checkpoint `json:"checkpoint,omitempty"`

	// Heartbeat is the last successful coordinator activity.
	// +optional
	Heartbeat *Heartbeat `json:"heartbeat,omitempty"`

	// ActiveOperations is the set of currently dispatched tool/subagent
	// operations, keyed by a harness-generated operation ID unique within the
	// run across retries and pod restarts. Entries persist before dispatch and
	// clear only on verified completion or cancellation; a valid entry
	// suppresses stale-heartbeat reaping of its control incarnation. The
	// trusted broker is the only writer.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	ActiveOperations map[string]ActiveOperation `json:"activeOperations,omitempty"`

	// Restarts counts infra relaunches (the crashloop backstop), not work
	// retries.
	// +optional
	Restarts int `json:"restarts,omitempty"`

	// Telemetry is the compact per-run summary tallied from the
	// coordinator's OpenCode event stream (#172). Best-effort: absent for
	// runs that ended before the event stream could be tallied.
	// +optional
	Telemetry *RunTelemetry `json:"telemetry,omitempty"`

	// Conditions is the standard condition set.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Repo",type=string,JSONPath=`.spec.repo`
// +kubebuilder:printcolumn:name="Ref",type=integer,JSONPath=`.spec.ref`
// +kubebuilder:printcolumn:name="Lane",type=string,JSONPath=`.spec.lane`
// +kubebuilder:printcolumn:name="Branch",type=string,JSONPath=`.status.branch`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Waited",type=string,JSONPath=`.status.waitDuration`
// +kubebuilder:printcolumn:name="Ran",type=string,JSONPath=`.status.runDuration`
// +kubebuilder:printcolumn:name="PR",type=string,JSONPath=`.status.pr`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CoderRun is one immutable attempt at one goal: resolve an issue or fix a PR.
type CoderRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CoderRunSpec   `json:"spec,omitempty"`
	Status CoderRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CoderRunList contains a list of CoderRun.
type CoderRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CoderRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CoderRun{}, &CoderRunList{})
}
