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
// +kubebuilder:validation:Enum=Pending;Claimed;Running;AwaitingReview;NeedsHuman;Done;Failed
type Phase string

const (
	PhasePending        Phase = "Pending"
	PhaseClaimed        Phase = "Claimed"
	PhaseRunning        Phase = "Running"
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
}

// CoderRunStatus is the observed state of a CoderRun.
type CoderRunStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Branch is the observed work branch. Resolve-issue runs derive it from the
	// repository and issue; fix-pr runs adopt the branch attached to the PR.
	// +optional
	Branch string `json:"branch,omitempty"`

	// PR is the pull request opened by this run (number or URL).
	// +optional
	PR string `json:"pr,omitempty"`

	// LastCommit is the last commit pushed to the work branch.
	// +optional
	LastCommit string `json:"lastCommit,omitempty"`

	// Checkpoint is the compact resumable state written by the harness.
	// +optional
	Checkpoint *Checkpoint `json:"checkpoint,omitempty"`

	// Heartbeat is the last successful coordinator activity.
	// +optional
	Heartbeat *Heartbeat `json:"heartbeat,omitempty"`

	// Restarts counts infra relaunches (the crashloop backstop), not work
	// retries.
	// +optional
	Restarts int `json:"restarts,omitempty"`

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
