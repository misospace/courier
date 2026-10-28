package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Mode is the kind of work a CoderRun performs.
// +kubebuilder:validation:Enum=resolve-issue;fix-pr;contribute
type Mode string

const (
	// ModeResolveIssue: open a PR that resolves an issue and leave it mergeable.
	ModeResolveIssue Mode = "resolve-issue"
	// ModeFixPR: take over an existing PR and get it back to a healthy state.
	ModeFixPR Mode = "fix-pr"
	// ModeContribute: open a PR from the approved fork named by spec.workRepo
	// into the base repository spec.repo, for a goal carried in spec.sourceContext
	// and no source-owned work item (#595).
	ModeContribute Mode = "contribute"
)

// SoftStopAnnotation is the human ingress for an operator soft-stop of a
// Running run (#238). Its non-empty, bounded value is an opaque request ID;
// the controller alone translates it into the operator-owned
// CoderRun.status.controlRequest record. Clearing or changing the annotation
// clears or replaces the record.
const SoftStopAnnotation = "courier.misospace.dev/soft-stop"

// RetryAnnotation is the human ingress for an explicit, operator-authorized
// retry of a terminal CoderRun (#97/#606). Its non-empty, bounded value is an
// opaque request ID, and it mirrors the settled soft-stop ingress: the spec is
// immutable and source-owned, the status is controller-owned, and the annotation
// is the operator channel Kubernetes RBAC already gates. The controller alone
// translates it into the operator-owned CoderRun.status.retryRequest record.
// Each retry request is an explicit new annotation value: a value already served
// or settled is a no-op, and re-requesting requires a fresh value.
const RetryAnnotation = "courier.misospace.dev/retry"

// ControlRequestKind is the action one control request names. v1 closes the
// CRD enum to soft-stop: the only settled action is a cooperative stop.
// +kubebuilder:validation:Enum=soft-stop
type ControlRequestKind string

const (
	// ControlRequestSoftStop asks the named control incarnation to stop
	// dispatching new briefs, let in-flight work finish and integrate, and
	// exit cooperatively with a normal outcome.
	ControlRequestSoftStop ControlRequestKind = "soft-stop"
)

// ControlRequest is the operator-owned fenced control request stamped by the
// controller from the soft-stop annotation (#238). It is addressed to one
// control incarnation: targetPodUID is the live coordinator pod UID at stamp
// time, and a request whose incarnation is gone is void, never re-targeted.
type ControlRequest struct {
	// ID is the opaque request ID: the annotation value, verbatim.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ID string `json:"id"`
	// Kind is the requested action.
	Kind ControlRequestKind `json:"kind"`
	// TargetPodUID fences the request to one control incarnation.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	TargetPodUID string `json:"targetPodUID"`
	// RequestedAt is when the controller stamped this record.
	RequestedAt metav1.Time `json:"requestedAt"`
}

// ControlRequestAck is the harness-owned durable acknowledgement written
// through the broker's trusted status path after a matching control request
// was served and before the stop latch was set (#238). A request is never
// consumed without this record.
type ControlRequestAck struct {
	// ID is the acknowledged request ID: the serving record's ID, verbatim.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ID string `json:"id"`
	// At is when the harness wrote this acknowledgement.
	At metav1.Time `json:"at"`
}

// ConditionLifecycleReported is the status condition type the operator writes
// on a CoderRun to record the outcome of its terminal lifecycle report. It is
// read back wherever another component must know whether the report landed.
const ConditionLifecycleReported = "LifecycleReported"

// ConditionSoftStopObserved is the status condition type the operator writes
// on a CoderRun to surface a soft-stop control request the incarnation it
// named never acknowledged (#238). It is set with reason Unacknowledged on
// relaunch and clears on a fresh annotation or an acknowledgement.
const ConditionSoftStopObserved = "SoftStopObserved"

// ConditionReasonUnacknowledged is the reason carried by a
// ConditionSoftStopObserved condition for a request the named incarnation
// never acknowledged.
const ConditionReasonUnacknowledged = "Unacknowledged"

// ConditionCapacityRecovery is the status condition type the operator writes
// on a capacity-exhaustion run to record the outcome of its fenced recovery
// settlement with the source (#327). Status True with reason Settled means
// the exhausted attempt was released with source-enforced fencing; False
// with reason Unsupported, Stale, UnsafeWork, PendingWriters, or Retryable
// records why it has not been (or will not be, without an operator). It is
// written only for runs whose terminal diagnostic category is
// capacity-exhaustion.
const ConditionCapacityRecovery = "CapacityRecovery"

// ConditionPullRequestClosedUnmerged is the status condition type the operator
// writes on a settled AwaitingReview run whose exact recorded pull request was
// observed closed without merge (#277). Status True with reason ClosedUnmerged
// marks the run NeedsHuman so it is retained for a human instead of being held
// in AwaitingReview forever. A missing or unreadable pull request is never
// treated as closed.
const ConditionPullRequestClosedUnmerged = "PullRequestClosedUnmerged"

// ConditionReasonClosedUnmerged is the reason carried by a
// ConditionPullRequestClosedUnmerged condition when the recorded pull request
// was observed closed without merge (#277).
const ConditionReasonClosedUnmerged = "ClosedUnmerged"

// ConditionCIRepairInapplicable is the status condition type the operator
// writes on a Verifying run whose lane declares a mechanical-CI repair policy
// but whose observed head cannot be attributed to the run's own publication
// evidence (#550). A legacy coordinator publishes a pull request without the
// broker-verified commit record the park-and-resume loop requires, so the
// loop silently cannot engage on it. Status True with reason
// HeadUnattributable says so durably instead: the ordinary post-publication
// grading path decides the run, unchanged and fail-closed, and no head is
// ever fabricated to park on. The condition clears once the head becomes
// attributable or the lane stops declaring a policy.
const ConditionCIRepairInapplicable = "CIRepairInapplicable"

// ConditionReasonHeadUnattributable is the reason carried by a
// ConditionCIRepairInapplicable condition when the observed head matches no
// commit the run accounts for.
const ConditionReasonHeadUnattributable = "HeadUnattributable"

// ConditionRetryRequest is the status condition type the operator writes on a
// CoderRun to mirror its status.retryRequest record for observability (#97/#606).
// The condition status and reason track the record state one-for-one:
// Unknown/Requested, True/Requeued, True/Moot, and False/Refused. A Refused
// condition carries the bounded refusal reason in its message, matching
// status.retryRequest.reason and the run.retry event.
const ConditionRetryRequest = "RetryRequest"

// Condition reasons mirroring the four RetryRequest states exactly, so a
// reader can map the condition back to the record without decoding it.
const (
	ConditionReasonRetryRequested = "Requested"
	ConditionReasonRetryRequeued  = "Requeued"
	ConditionReasonRetryMoot      = "Moot"
	ConditionReasonRetryRefused   = "Refused"
)

// RetryRequestState is the durable state of one explicit retry request
// (#97/#606). Requested is durable intent persisted before any source call; the
// other three are settled. A settled record is immutable, and a Requeued record
// never reopens.
// +kubebuilder:validation:Enum=Requested;Requeued;Moot;Refused
type RetryRequestState string

const (
	RetryRequestStateRequested RetryRequestState = "Requested"
	RetryRequestStateRequeued  RetryRequestState = "Requeued"
	RetryRequestStateMoot      RetryRequestState = "Moot"
	RetryRequestStateRefused   RetryRequestState = "Refused"
)

// The bounded retry refusal vocabulary carried by status.retryRequest.reason and
// the run.retry event. It is a closed set: a refusal reason outside it is a
// controller bug, never free text. The first six are core (source-agnostic)
// refusals decided before the source call; the last three are source-state
// refusals the adapter returns after the Requested write. already-requeued has
// two roles: it is the single reason of the moot family, carried by a settled Moot
// record where the source reports the work already moved on through some other
// request or path — never for a key this request committed, since a committed key
// replays its committed success and settles Requeued — and it is the events-only
// reason for a fresh request arriving over an already-settled Requeued record:
// that announcement never rewrites the settled record.
const (
	RetryRefusalNotRetryablePhase        = "not-retryable-phase"
	RetryRefusalAutomaticRecoveryChannel = "automatic-recovery-channel"
	RetryRefusalActiveOwnerExists        = "active-owner-exists"
	RetryRefusalAlreadyRequeued          = "already-requeued"
	RetryRefusalUpstreamClosed           = "upstream-closed"
	RetryRefusalUnsupportedSource        = "unsupported-source"
	RetryRefusalStaleGeneration          = "stale-generation"
	RetryRefusalSourceRefused            = "source-refused"
	RetryRefusalHeldForHuman             = "held-for-human"
)

// RetryRequestMaxAttemptIDLength is the CRD maxLength on
// status.retryRequest.attemptID: the bound the source requeue contract's
// attempt identity must fit. The kubebuilder marker on RetryRequest.AttemptID
// carries the same value literally; keep them in sync.
const RetryRequestMaxAttemptIDLength = 256

// RetryRequest is the operator-owned durable record of one explicit retry
// request, translated by the controller alone from the RetryAnnotation (#97/#606).
// The operator never writes it and no harness path can forge it; the controller
// is its sole writer. It is replaceable across request lifetimes — a settled
// Refused/Moot record may be superseded by a fresh request from a new annotation
// value — but a settled record is never otherwise mutated, and a Requeued record
// is terminal for the predecessor's whole lifetime. Every field is serialized
// without omitempty where a stale sub-key must be cleared by a replacement
// record under a JSON merge patch (reason and settledAt), so a fresh Requested
// never inherits a predecessor's refusal reason or settled timestamp.
type RetryRequest struct {
	// ID is the opaque request ID: the annotation value, verbatim.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ID string `json:"id"`
	// RequestedAt is when the controller first stamped this request.
	RequestedAt metav1.Time `json:"requestedAt"`
	// State is the durable retry request state: Requested while the intent is
	// outstanding, Requeued once the source committed the requeue (including the
	// success a committed key replays after a lost response), Moot when the work
	// already moved on without this request's key, and Refused when the request
	// was declined.
	State RetryRequestState `json:"state"`
	// Reason is the bounded refusal/moot reason for a settled Refused/Moot
	// record, and empty while Requested or for a Requeued success. It is drawn
	// only from the closed RetryRefusal* vocabulary; the pattern admits the
	// empty value plus exactly that closed set, so arbitrary reason text can
	// never be persisted or served by the CRD.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^(|not-retryable-phase|automatic-recovery-channel|active-owner-exists|already-requeued|upstream-closed|unsupported-source|stale-generation|source-refused|held-for-human)$`
	Reason string `json:"reason"`
	// AttemptID is the fresh source attempt identity the adapter returned when
	// it requeued, if the source exposes one. Empty means the source requeued
	// without exposing a new identity; discovery observes the successor later.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	AttemptID string `json:"attemptID,omitempty"`
	// SettledAt is when the record reached Requeued, Moot, or Refused. It is
	// null while Requested, and always serialized so a replacement Requested
	// record clears a predecessor's settled timestamp.
	// +optional
	// +nullable
	SettledAt *metav1.Time `json:"settledAt"`
}

// Phase is the lifecycle phase of a CoderRun.
// +kubebuilder:validation:Enum=Pending;Claimed;Running;Verifying;AwaitingReview;HandedOff;NeedsHuman;Done;Failed;BlockedExternal
type Phase string

const (
	PhasePending        Phase = "Pending"
	PhaseClaimed        Phase = "Claimed"
	PhaseRunning        Phase = "Running"
	PhaseVerifying      Phase = "Verifying"
	PhaseAwaitingReview Phase = "AwaitingReview"
	// PhaseHandedOff marks stable, confirmed red checks on a published resolve-issue PR; it is explicitly non-successful, not AwaitingReview, NeedsHuman, or Failed.
	PhaseHandedOff  Phase = "HandedOff"
	PhaseNeedsHuman Phase = "NeedsHuman"
	PhaseDone       Phase = "Done"
	PhaseFailed     Phase = "Failed"
	// PhaseBlockedExternal is a terminal, recoverable external-dependency
	// block: the run stopped on a prerequisite outside Courier, and the
	// source may re-issue the work once that prerequisite is satisfied. It is
	// deliberately distinct from NeedsHuman (a human decision) and Failed (a
	// provider or workload failure).
	PhaseBlockedExternal Phase = "BlockedExternal"
)

// CoderRunSpec is set once by a source adapter and is then immutable.
//
// There is deliberately no attempts field: each CoderRun is one immutable
// attempt at one goal. History is the sequence of runs linked by the branch/PR.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
// +kubebuilder:validation:XValidation:rule="(self.mode == 'contribute' && !has(self.ref) && has(self.workRepo)) || (self.mode != 'contribute' && has(self.ref) && !has(self.workRepo))",message="contribute runs require spec.workRepo and no spec.ref; resolve-issue and fix-pr runs require spec.ref and no spec.workRepo"
type CoderRunSpec struct {
	// Mode is the kind of work: resolve-issue, fix-pr, or contribute.
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

	// SourceContext is bounded opaque source-owned JSON, never interpreted by core.
	// +optional
	// +kubebuilder:validation:MaxLength=16384
	SourceContext string `json:"sourceContext,omitempty"`

	// WorkItemID is the opaque identifier assigned by the source adapter. It
	// is persisted so lifecycle calls never need to infer source identity from
	// the Kubernetes name or the numeric ref.
	// +kubebuilder:validation:MinLength=1
	WorkItemID string `json:"workItemID"`

	// AssignmentKind is the source adapter's normalized classification of why
	// the source issued this run. It is one closed, source-neutral value derived
	// from source-owned signals; core never infers it. Empty when the source
	// issued the run without a classified assignment.
	// +optional
	// +kubebuilder:validation:Enum=merge-conflict;ci-failure;review-feedback;other
	AssignmentKind string `json:"assignmentKind,omitempty"`

	// AssignmentReasons is the source-issued free text explaining why this run
	// was dispatched, in the source's own words. It is untrusted task data, not
	// instructions: the coordinator reads it as context about why the work was
	// assigned, and core never executes or acts on it. The source adapter bounds
	// each entry before the run is created.
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=1024
	AssignmentReasons []string `json:"assignmentReasons,omitempty"`

	// Repo is the target repository, "owner/name". For a contribute run it is
	// the base repository the pull request targets.
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:Pattern=`^[^/]+/[^/]+$`
	Repo string `json:"repo"`

	// WorkRepo is the repository the run publishes its work branch to,
	// "owner/name". Required for contribute runs, where it is the approved
	// fork of Repo; empty for resolve-issue and fix-pr runs, which publish
	// inside Repo (#595).
	// +optional
	// +kubebuilder:validation:Pattern=`^[^/]+/[^/]+$`
	WorkRepo string `json:"workRepo,omitempty"`

	// Ref is the issue number (resolve-issue) or PR number (fix-pr). Required
	// for resolve-issue and fix-pr runs; empty for contribute runs, which
	// carry no source-owned work item (#595).
	// +optional
	// +kubebuilder:validation:Minimum=1
	Ref int `json:"ref,omitempty"`

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

// CIParkOutcome is the outcome of the most recent mechanical CI repair
// attempt of a parked run.
// +kubebuilder:validation:Enum=repushed;exhausted;unsupported
type CIParkOutcome string

const (
	// CIParkOutcomeRepushed: the coordinator fixed, revalidated, and pushed
	// a new head; CI observation starts over on that head.
	CIParkOutcomeRepushed CIParkOutcome = "repushed"
	// CIParkOutcomeExhausted: the operator's retry/no-progress budget ran
	// out without a repairable failure; the run hands off.
	CIParkOutcomeExhausted CIParkOutcome = "exhausted"
	// CIParkOutcomeUnsupported: recovery was not possible (unsupported
	// checks, unobservable PR, superseded head, or a safe handoff boundary);
	// the run hands off.
	CIParkOutcomeUnsupported CIParkOutcome = "unsupported"
)

// CIParkState is the durable park-and-resume record for a run whose
// published head is under mechanical CI observation (#475). It exists so
// parking survives pod deletion and controller restart: the exact head,
// when the park began, how many repair opportunities the run consumed,
// and the outcome of the most recent one. Operator-owned; the harness
// never writes it.
type CIParkState struct {
	// Head is the exact published head commit whose checks decide park,
	// resume, or handoff. A later observation of a different head never
	// triggers repair: the run fails closed to handoff instead.
	Head string `json:"head"`
	// ParkedAt is when the operator parked the run.
	ParkedAt metav1.Time `json:"parkedAt"`
	// Attempts counts consumed repair opportunities. The operator's
	// finite retry/no-progress budget is enforced against this count.
	// +kubebuilder:validation:Minimum=0
	Attempts int `json:"attempts"`
	// LastOutcome records the outcome of the most recent repair attempt;
	// empty until one occurred.
	// +optional
	LastOutcome CIParkOutcome `json:"lastOutcome,omitempty"`
}

// CIRepairContext is the bounded repair context handed to a resumed
// coordinator (#494). It records what the operator observed on the parked
// head, so the resumed attempt knows which checks failed and which head it
// repairs without re-reading the issue or any earlier transcript.
type CIRepairContext struct {
	// Head is the exact parked head whose checks failed. The repair belongs
	// to this head: an observation of any other head is not this run's work.
	// +kubebuilder:validation:MinLength=1
	Head string `json:"head"`
	// Attempt is the 1-based repair attempt this context belongs to.
	// +kubebuilder:validation:Minimum=1
	Attempt int `json:"attempt"`
	// FailedChecks are the names of the failed configured mechanical checks,
	// and only those: a check the lane never named is never evidence here.
	// +optional
	FailedChecks []string `json:"failedChecks,omitempty"`
	// Logs is the bounded log evidence of those checks.
	// +optional
	Logs string `json:"logs,omitempty"`
	// FenceToken is the token the source issued when it reserved this
	// repair opportunity. The outcome report must present it back, so it
	// travels with the attempt rather than being recomputed at the end.
	// Empty when the source cannot fence a repair.
	// +optional
	FenceToken string `json:"fenceToken,omitempty"`
	// MaxAttempts is the lane's repair budget, so a resumed coordinator
	// knows how many attempts remain.
	// +optional
	MaxAttempts int `json:"maxAttempts,omitempty"`
}

// TerminalDiagnosticCategory is the stable classification of a terminal run's
// cause, derived only from evidence the operator observed. Unknown is used
// whenever evidence is missing or ambiguous; a cause is never invented.
// +kubebuilder:validation:Enum=executor-crash;model-transport-error;continuation-exhaustion;verifier-rejection;kubernetes-failure;needs-decision;blocked-external;capacity-exhaustion;provider-unavailable;forge-unavailable;workload-failure;infrastructure-failure;unknown
type TerminalDiagnosticCategory string

const (
	// TerminalDiagnosticCategoryExecutorCrash: the executor bootstrap or
	// harness process failed before or around the model run.
	TerminalDiagnosticCategoryExecutorCrash TerminalDiagnosticCategory = "executor-crash"
	// TerminalDiagnosticCategoryModelTransportError: the model/agent child
	// process exited non-zero.
	TerminalDiagnosticCategoryModelTransportError TerminalDiagnosticCategory = "model-transport-error"
	// TerminalDiagnosticCategoryContinuationExhaustion: the bounded
	// continuation budget was exhausted or a no-progress loop was detected.
	TerminalDiagnosticCategoryContinuationExhaustion TerminalDiagnosticCategory = "continuation-exhaustion"
	// TerminalDiagnosticCategoryVerifierRejection: the verified world (git,
	// the PR, or authoritative linkage) disagreed with the declared or
	// expected outcome.
	TerminalDiagnosticCategoryVerifierRejection TerminalDiagnosticCategory = "verifier-rejection"
	// TerminalDiagnosticCategoryKubernetesFailure: a Kubernetes/container-level
	// failure (eviction, failed scheduling, OOMKill, or a lost pod).
	TerminalDiagnosticCategoryKubernetesFailure TerminalDiagnosticCategory = "kubernetes-failure"
	// TerminalDiagnosticCategoryNeedsDecision: the coordinator declared it
	// needed a human decision.
	TerminalDiagnosticCategoryNeedsDecision TerminalDiagnosticCategory = "needs-decision"
	// TerminalDiagnosticCategoryBlockedExternal: the coordinator declared it
	// was blocked on an external prerequisite.
	TerminalDiagnosticCategoryBlockedExternal TerminalDiagnosticCategory = "blocked-external"
	// TerminalDiagnosticCategoryCapacityExhaustion is a structured, affirmative
	// signal that execution capacity (not task correctness) was exhausted.
	TerminalDiagnosticCategoryCapacityExhaustion TerminalDiagnosticCategory = "capacity-exhaustion"
	// TerminalDiagnosticCategoryProviderUnavailable is a model-provider
	// availability handoff: the attempt was never served by the model
	// provider (no session id was produced and no structured
	// allowance-exhaustion signal was observed), so the work is released
	// recoverably for another capacity instead of failing permanently. It
	// never claims the quota itself is exhausted.
	TerminalDiagnosticCategoryProviderUnavailable TerminalDiagnosticCategory = "provider-unavailable"
	// TerminalDiagnosticCategoryForgeUnavailable is a forge-read
	// availability handoff: a forge read could not complete on a transient
	// availability condition, so the world was never observed and the work
	// is released recoverably for another capacity instead of failing
	// permanently. It never claims the world was read and found
	// contradictory.
	TerminalDiagnosticCategoryForgeUnavailable TerminalDiagnosticCategory = "forge-unavailable"
	// TerminalDiagnosticCategoryWorkloadFailure is the trusted native
	// coordinator's explicit workload-failure classification (#127): the
	// run's work failed — an undeclared ending, a declared changes with
	// nothing integrated, or another ending that is a verdict on the work.
	TerminalDiagnosticCategoryWorkloadFailure TerminalDiagnosticCategory = "workload-failure"
	// TerminalDiagnosticCategoryInfrastructureFailure is the trusted native
	// coordinator's explicit infrastructure-failure classification (#127):
	// the infrastructure failed around the work, and recovery is the
	// relaunch/resume case bounded by the crashloop counter, never a
	// terminal verdict on the work.
	TerminalDiagnosticCategoryInfrastructureFailure TerminalDiagnosticCategory = "infrastructure-failure"
	// TerminalDiagnosticCategoryUnknown: the evidence was missing or
	// ambiguous.
	TerminalDiagnosticCategoryUnknown TerminalDiagnosticCategory = "unknown"
)

// WorkspaceSafetyEvidence is the executor's trusted workspace snapshot.
type WorkspaceSafetyEvidence struct {
	State      string `json:"state,omitempty"`
	Branch     string `json:"branch,omitempty"`
	Head       string `json:"head,omitempty"`
	RemoteHead string `json:"remoteHead,omitempty"`
}

// TerminalDiagnostic is the durable, bounded, redacted record of how a run's
// coordinator ended. The operator captures it before pod cleanup so the cause
// survives pod deletion and controller restart. It is best-effort: absent when
// no evidence was available, and never fabricated from a missing pod.
type TerminalDiagnostic struct {
	// Category is the stable cause classification.
	Category TerminalDiagnosticCategory `json:"category"`
	// ExitCode is the observed coordinator container exit code, when the
	// container terminated and reported one.
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`
	// Reason is a short, redacted, length-bounded explanation.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Reason string `json:"reason,omitempty"`
	// Outcome is the executor-declared outcome kind, when a valid declaration
	// drove the ending (#169); empty when classified from the world alone.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	Outcome string `json:"outcome,omitempty"`
	// Container preserves the coordinator container identity and termination
	// metadata for kubectl-describe-equivalent inspection after the pod is
	// deleted. It is absent when no pod evidence was available.
	// +optional
	Container *TerminalContainerDiagnostic `json:"container,omitempty"`
	// ObservedAt is when the operator captured this diagnostic.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// TerminalContainerDiagnostic is the bounded pod/container identity retained
// for diagnostics after the pod is gone. No credential, token, prompt, or
// unbounded provider text is ever recorded here.
type TerminalContainerDiagnostic struct {
	// PodName is the coordinator pod name at capture.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	PodName string `json:"podName,omitempty"`
	// PodUID fences the retained identity to one pod incarnation.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	PodUID string `json:"podUID,omitempty"`
	// Namespace is the pod namespace at capture.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Namespace string `json:"namespace,omitempty"`
	// ContainerName is the coordinator container name.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ContainerName string `json:"containerName,omitempty"`
	// ContainerID is the runtime container id (for example
	// "containerd://...").
	// +optional
	// +kubebuilder:validation:MaxLength=256
	ContainerID string `json:"containerID,omitempty"`
	// Image is the coordinator container image.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Image string `json:"image,omitempty"`
	// NodeName is the node the coordinator pod ran on.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	NodeName string `json:"nodeName,omitempty"`
	// RestartCount is the container's restart count at capture.
	// +optional
	RestartCount int32 `json:"restartCount,omitempty"`
	// TerminationReason is the container's Terminated.Reason (for example
	// OOMKilled, Error, or Completed), when the runtime reported one.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	TerminationReason string `json:"terminationReason,omitempty"`
}

// NativeResultKind is the trusted native coordinator's explicit terminal
// classification (#127). It is set by trusted control at every ending, before
// process exit, and persists through the run's broker status path, so the
// operator classifies from that evidence plus its own world verification —
// never from the model's prose or the process exit code alone. The kinds
// mirror the operator's existing routes exactly; the one behavioral addition
// is the workload/infrastructure split: an infrastructure failure is the
// relaunch/resume case bounded by the crashloop counter (HARNESS.md §8), not
// a terminal verdict on the work.
// +kubebuilder:validation:Enum=published;no_change;needs_human;blocked_external;capacity_exhausted;workload_failed;infrastructure_failed
type NativeResultKind string

const (
	// NativeResultPublished: the coordinator declared changes and the broker
	// confirmed publication of the integrated head. It is still only a
	// handoff: the operator's own PR/head/check observation remains the
	// authority for Verifying.
	NativeResultPublished NativeResultKind = "published"
	// NativeResultNoChange: the coordinator declared no_change_needed with
	// evidence. It never resolves the source on the coordinator's word
	// alone; the operator settles it against the world exactly as the
	// declared exit-3 ending.
	NativeResultNoChange NativeResultKind = "no_change"
	// NativeResultNeedsHuman: a genuine human decision the run cannot make.
	NativeResultNeedsHuman NativeResultKind = "needs_human"
	// NativeResultBlockedExternal: a recoverable wait on an external
	// prerequisite; the source item stays active.
	NativeResultBlockedExternal NativeResultKind = "blocked_external"
	// NativeResultCapacityExhausted: confirmed execution-capacity exhaustion
	// (#320/#327); a recoverable wait on an external resource.
	NativeResultCapacityExhausted NativeResultKind = "capacity_exhausted"
	// NativeResultWorkloadFailed: the run's work failed — an undeclared
	// ending, a declared changes with nothing integrated, or another ending
	// that is a verdict on the work, never on the infrastructure.
	NativeResultWorkloadFailed NativeResultKind = "workload_failed"
	// NativeResultInfrastructureFailed: the infrastructure failed around the
	// work — a model gateway or stream failure, a status write that will not
	// land, a publication transport failure. This is the relaunch/resume
	// case bounded by the crashloop counter (HARNESS.md §8), never a terminal
	// verdict on the work.
	NativeResultInfrastructureFailed NativeResultKind = "infrastructure_failed"
)

// MaxNativeResultReasonBytes is the durable-reason bound shared by the
// NativeResult CRD validation, the broker's trusted status writer, and the
// control binary's pre-exit persistence (#127): one constant so the three
// ends of the status path cannot drift. The kubebuilder MaxLength on the
// Reason field below is this value's validated copy and must move with it.
const MaxNativeResultReasonBytes = 1024

// NativeResultReason returns the fixed, code-owned durable reason for a
// trusted native terminal result kind (#127). The durable reason is a stable
// operator label — never model output: the coordinator's declared evidence,
// question, or missing-dependency text stays on the termination handoff and
// the source report, which are the surfaces for it, while
// status.nativeResult.reason remains a fixed diagnostic trusted control can
// authoritatively stand behind. An unknown kind has no durable reason.
func NativeResultReason(kind NativeResultKind) string {
	switch kind {
	case NativeResultPublished:
		return "coordinator published through the broker"
	case NativeResultNoChange:
		return "coordinator declared no change needed"
	case NativeResultNeedsHuman:
		return "coordinator requested a human decision"
	case NativeResultBlockedExternal:
		return "coordinator blocked on an external prerequisite"
	case NativeResultCapacityExhausted:
		return "execution capacity was confirmed exhausted"
	case NativeResultWorkloadFailed:
		return "coordinator reported a workload failure"
	case NativeResultInfrastructureFailed:
		return "coordinator reported an infrastructure failure"
	default:
		return ""
	}
}

// NativeResult is the trusted native coordinator's structured terminal
// result (#127), written by trusted control through the run's broker status
// path before the control process exits. The broker fences it to the
// authenticated control incarnation, so a stale incarnation cannot record a
// result, and the operator consumes it only when it fences to the coordinator
// pod whose termination it is classifying. It supplements, never replaces,
// the operator's own world verification: a published result still only hands
// the run to Verifying.
type NativeResult struct {
	// Kind is the explicit terminal classification.
	Kind NativeResultKind `json:"kind"`
	// Reason is a fixed, code-owned operator label derived from Kind
	// (NativeResultReason), bounded by MaxNativeResultReasonBytes. It never
	// carries model output: the coordinator's declared evidence, question, or
	// missing-dependency text stays on the termination handoff and the source
	// report, never on durable status.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Reason string `json:"reason,omitempty"`
	// CoordinatorPodUID is the control incarnation that produced the result.
	// It is the fence: the broker refuses a result attributing itself to any
	// other incarnation, and the operator trusts a result only for the
	// coordinator pod whose termination carries the same UID.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	CoordinatorPodUID string `json:"coordinatorPodUID"`
	// At is when trusted control produced the result, before process exit.
	At metav1.Time `json:"at"`
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

// SourceIssueRef is the trusted source-side issue identity pinned at
// admission for resolve-issue runs. The repository is the provider-canonical
// form of spec.repo; the number is the issue number from the immutable spec.
// The model never selects it.
type SourceIssueRef struct {
	// Owner is the provider-canonical repository owner.
	// +kubebuilder:validation:MinLength=1
	Owner string `json:"owner"`
	// Name is the provider-canonical repository name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Number is the trusted source issue number.
	// +kubebuilder:validation:Minimum=1
	Number int `json:"number"`
}

// Criterion is one acceptance criterion pinned from the trusted source-issue
// snapshot at admission (#249/#280). ID is the ordinal assigned at extraction
// ("AC-1", "AC-2", …, in document order); Digest covers the normalized
// criterion text so an evidence row authored against edited text is refused.
type Criterion struct {
	// ID is the extraction ordinal ("AC-1", "AC-2", …).
	// +kubebuilder:validation:Pattern=`^AC-[1-9][0-9]*$`
	ID string `json:"id"`
	// Text is the normalized criterion text (whitespace-collapsed).
	// +kubebuilder:validation:MaxLength=512
	Text string `json:"text"`
	// Digest is the sha256 of the normalized text.
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest"`
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
	// SourceIssue pins the trusted source-issue identity for resolve-issue
	// runs, recorded beside the criterion pin so the policy is
	// self-describing. It is cross-checked by the broker binding against
	// the identity derived from the immutable spec. Nil for fix-pr runs and
	// for policies admitted before the criterion contract (#280).
	// +optional
	SourceIssue *SourceIssueRef `json:"sourceIssue,omitempty"`
	// AcceptanceCriteria is the revision-pinned acceptance-criterion set
	// extracted from the trusted source-issue body at admission. An empty
	// list is a legitimate pin — the issue carries no recognizable
	// Acceptance section — and is distinct from nil, which means no set was
	// pinned (fix-pr runs, policies admitted before #280). Later issue
	// edits never mutate a live run's pin.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	AcceptanceCriteria []Criterion `json:"acceptanceCriteria,omitempty"`
	// AcceptanceCriteriaDigest covers the trusted issue identity plus every
	// criterion digest in order. It is non-empty exactly when a set is
	// pinned; the broker binding recomputes it and refuses a mismatched pin.
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	AcceptanceCriteriaDigest string `json:"acceptanceCriteriaDigest,omitempty"`
}

// CoderRunStatus is the observed state of a CoderRun.
type CoderRunStatus struct {
	// SourceReceipt is sensitive source-issued claim fencing data.
	// +optional
	// +kubebuilder:validation:MaxLength=16384
	SourceReceipt string `json:"sourceReceipt,omitempty"`

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
	// terminal phase: AwaitingReview, HandedOff, NeedsHuman, Done, Failed, or
	// BlockedExternal). It is set
	// exactly once and never overwritten; a later transition (e.g.
	// AwaitingReview -> Done) does not move it.
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// PhaseEnteredAt is when the run most recently entered its current phase.
	// It is replaceable, not set-once: it is restamped on every phase change,
	// so phase-age observability can measure time in the current phase.
	// +optional
	PhaseEnteredAt *metav1.Time `json:"phaseEnteredAt,omitempty"`

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

	// FailedCheckFingerprint is the compact identity (head plus sorted check names) of the last fully-failed CI observation while Verifying. A resolve-issue run hands off only when two consecutive fully-failed observations carry the same fingerprint; a pending or empty observation clears it, a changed set resets it, and fix-pr does not use it.
	// +optional
	FailedCheckFingerprint string `json:"failedCheckFingerprint,omitempty"`

	// DeclaredNoChange records that the coordinator of this run exited with
	// the verified no_change_needed declaration. Only a fix-pr run has it set:
	// the run pushed nothing, so its terminal source report must state that no
	// change was pushed rather than claim a fix was pushed.
	// +optional
	DeclaredNoChange bool `json:"declaredNoChange,omitempty"`

	// LifecycleReportRetryToken records the last explicit retry token handled for an unsupported lifecycle result; it is an opaque, non-secret value limited to 128 Unicode code points.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	LifecycleReportRetryToken string `json:"lifecycleReportRetryToken,omitempty"`

	// LifecycleReportActiveRetryToken persists an explicit retry token while its report is transiently retrying.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	LifecycleReportActiveRetryToken string `json:"lifecycleReportActiveRetryToken,omitempty"`

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

	// ControlRequest is the operator-owned fenced control request (#238).
	// The controller translates the soft-stop annotation into this record
	// and alone writes it; the broker's trusted status listener accepts only
	// the harness-owned schema, so no other writer can forge a request. It
	// is replaceable, not set-once: a changed annotation replaces the record
	// and a cleared annotation clears it.
	// +optional
	ControlRequest *ControlRequest `json:"controlRequest,omitempty"`

	// ControlRequestAck is the harness-owned durable acknowledgement of the
	// control request the live incarnation served (#238). The harness writes
	// it through the broker's trusted status path, which fences it to a
	// matching live request.
	// +optional
	ControlRequestAck *ControlRequestAck `json:"controlRequestAck,omitempty"`

	// Restarts counts infra relaunches (the crashloop backstop), not work
	// retries.
	// +optional
	Restarts int `json:"restarts,omitempty"`

	// Telemetry is the compact per-run summary tallied from the
	// coordinator's OpenCode event stream (#172). Best-effort: absent for
	// runs that ended before the event stream could be tallied.
	// +optional
	Telemetry *RunTelemetry `json:"telemetry,omitempty"`

	// WorkspaceSafety is the bounded trusted snapshot reported by the executor
	// at terminal handoff. It is informational unless independently verified.
	// +optional
	WorkspaceSafety *WorkspaceSafetyEvidence `json:"workspaceSafety,omitempty"`

	// TerminalDiagnostic is the durable, bounded, redacted record of how the
	// run's coordinator ended, captured by the operator before pod cleanup so
	// the cause survives pod deletion and controller restart. Best-effort:
	// absent when no evidence was available, never fabricated.
	// +optional
	TerminalDiagnostic *TerminalDiagnostic `json:"terminalDiagnostic,omitempty"`

	// NativeResult is the trusted native coordinator's structured terminal
	// result (#127), persisted through the run's broker status path before
	// the control process exited. The trusted broker is the only writer; a
	// result is fenced to one control incarnation and a replacement
	// incarnation records its own. Absent for legacy executors and for runs
	// whose terminal write failed — the operator then classifies from the
	// kubelet termination handoff as before.
	// +optional
	NativeResult *NativeResult `json:"nativeResult,omitempty"`

	// CIPark is the durable park-and-resume record while the run waits on
	// CI in Verifying. Nil means the run is not parked (#475).
	// +optional
	CIPark *CIParkState `json:"cipark,omitempty"`

	// CIRepair is the bounded repair context the operator hands to a resumed
	// coordinator (#494). It is set only for a run that resumes a
	// mechanical-CI repair, and cleared once that repair attempt has been
	// observed, so a relaunch before that still finds the repair goal it
	// needs.
	// +optional
	CIRepair *CIRepairContext `json:"ciRepair,omitempty"`

	// CISettlement is the fenced outcome whose delivery to the source is
	// still outstanding (#475). It is recorded in the same status write that
	// settles the attempt — before the report is attempted — so a lost
	// response, a restart, or an unavailable source leaves durable retry
	// intent on the run instead of a deferral nobody will ever release. The
	// operator re-presents the record on later reconciles until the source
	// accepts it — same fence, same idempotency key, so the source replays
	// the committed transition — and clearing the record is the proof of
	// delivery.
	// +optional
	CISettlement *CISettlement `json:"ciSettlement,omitempty"`

	// CIParkDeferral is the park notice whose acceptance by the source is
	// still outstanding (#475). It is recorded in the same status write that
	// establishes the park — before the notice is attempted — so a lost
	// response leaves durable retry intent instead of a duplicate-worker
	// window nobody will ever close. The operator re-presents the notice on
	// later reconciles while the repair opportunity is active, and drops it
	// once that opportunity has ended: a late notice would defer the source
	// against nothing.
	// +optional
	CIParkDeferral *CIParkDeferral `json:"ciparkDeferral,omitempty"`

	// CIParkRelease is the fence-less deferral close whose delivery to the
	// source is still outstanding (#534). It is recorded in the same status
	// write that ends a run's park without a reservation — before the close
	// is attempted — so a lost response, a restart, or an unavailable source
	// leaves durable retry intent instead of a deferral nobody will ever
	// release. The operator re-presents the close on later reconciles with
	// the park notice's own idempotency key, and clearing the record is the
	// proof of delivery; a source with no such deferral refuses it moot —
	// the close is advisory and the record is dropped either way.
	// +optional
	CIParkRelease *CIParkRelease `json:"ciparkRelease,omitempty"`

	// RetryRequest is the operator-owned durable record of the explicit retry
	// request the RetryAnnotation named (#97/#606). The controller alone writes
	// it, translating the annotation into the Requested | Requeued | Moot |
	// Refused state machine; no operator or harness path can forge it. A run
	// carrying a Requested record is not reap-eligible until the record settles.
	// +optional
	RetryRequest *RetryRequest `json:"retryRequest,omitempty"`

	// Conditions is the standard condition set.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CISettlement is one fenced repair outcome the source has not accepted yet.
// It exists to outlive the reconcile that produced it: the fence travels
// with the record, so a retry presents exactly what the original report
// presented and the source's own idempotency does the collapsing.
type CISettlement struct {
	// FenceToken is the token the source issued when it reserved the repair
	// attempt this settlement answers.
	// +kubebuilder:validation:MinLength=1
	FenceToken string `json:"fenceToken"`
	// IdempotencyKey is the reservation key this outcome answers. New
	// settlements always record it before clearing the repair/park context so
	// a durable retry remains byte-identical even when those records are gone.
	// It is optional only for compatibility with settlement records written by
	// older controllers, which can still derive the key while a park remains.
	// +optional
	// +kubebuilder:validation:MinLength=1
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	// Result is the outcome kind being reported.
	// +kubebuilder:validation:Enum=repushed;exhausted;unsupported
	Result CIParkOutcome `json:"result"`
	// Head is the republished head when Result is repushed; empty otherwise.
	// +optional
	Head string `json:"head,omitempty"`
}

// CIParkDeferral is one park notice the source has not accepted yet. It
// exists to outlive the reconcile that produced it: the announced head and
// the idempotency key travel with the record, so a retry re-establishes the
// same deferral instead of double-booking it.
type CIParkDeferral struct {
	// Head is the exact parked head the notice announced.
	// +kubebuilder:validation:MinLength=1
	Head string `json:"head"`
	// IdempotencyKey deduplicates the notice at the source.
	// +kubebuilder:validation:MinLength=1
	IdempotencyKey string `json:"idempotencyKey"`
}

// CIParkRelease is one fence-less deferral close the source has not accepted
// yet (#534). It exists to outlive the reconcile that produced it: the
// parked head and the park notice's own idempotency key travel with the
// record, so a retry replays the same fence-less close instead of a second
// one.
type CIParkRelease struct {
	// Head is the exact parked head the released deferral announced.
	// +kubebuilder:validation:MinLength=1
	Head string `json:"head"`
	// IdempotencyKey is the park notice's own key, so the close is keyed by
	// the deferral the notice established and a retry replays one close.
	// +kubebuilder:validation:MinLength=1
	IdempotencyKey string `json:"idempotencyKey"`
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
