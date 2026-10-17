package v1alpha1

import (
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SuspendAnnotation suspends a lane when set to "true" on its LaneProfile: the
// lane's source runner takes no new work and its Pending runs wait, while runs
// already admitted finish normally. It is an annotation so an operator can
// toggle it with kubectl without contending with the manifest's owner.
const SuspendAnnotation = "courier.misospace.dev/suspend"

// LaneProfileSpec defines a reusable model ensemble and its runtime framing.
// It is the seam that keeps Courier model-agnostic: roles may name cloud or
// local models in any mix, and nothing about a lane is baked into the core.
// When a field is added or removed here, mirror it in bootstrap.Profile
// (internal/bootstrap/config.go), which carries the portable spec for
// manager-bootstrapped profiles.
type LaneProfileSpec struct {
	// RuntimeImage selects the coordinator runtime and repository toolchain for
	// runs on this lane. Empty uses the deployment's default coordinator image;
	// a lane may select any compatible image without changing Courier core.
	// +optional
	RuntimeImage string `json:"runtimeImage,omitempty"`

	// Concurrency is the maximum number of Running CoderRuns admitted on this
	// lane. A local lane is typically 1; a cloud lane can be much higher.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Concurrency int `json:"concurrency"`

	// Roles maps a coordinator role (coordinator, coder, reviewer, ...) to a
	// model identifier understood by the configured model gateway.
	Roles map[string]string `json:"roles"`

	// Framing is free-text lane context injected into the coordinator prompt:
	// hardware reality, parallelism guidance, whether a load tool is available.
	// +optional
	Framing string `json:"framing,omitempty"`

	// CapacityGroup names the opaque execution-capacity pool this lane draws
	// from — a shared provider deployment, credential pool, or piece of local
	// hardware. It is intentionally a bare name: Courier core never learns
	// what the pool is. Lanes that share a group share its capacity cooldown;
	// a lane with no group is its own capacity identity. Empty uses the lane
	// name.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	CapacityGroup string `json:"capacityGroup,omitempty"`

	// Resources sizes the lane-sized pods Courier creates for runs on this
	// lane — the coordinator and the secure topology's control, worker, and
	// broker pods. Limits are used exactly as written and may be omitted;
	// requests keep what the lane sets, but CPU and memory requests are
	// floored to the built-in default wherever they are absent or
	// non-positive, so no lane is BestEffort. A lane that configures nothing
	// gets the default whole (see DefaultPodResources). Preflight probe pods
	// are the deliberate exception: they carry a small fixed sizing
	// (ProbePodResources) that does not follow the lane.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// CapacityCooldown is a durable pause on new work for one execution-capacity
// identity, recorded when a run on this lane ends with a confirmed
// capacity-exhaustion diagnostic (#327). It bounds how fast exhausted
// capacity re-claims work; it is not a run-duration limit, and it never gates
// runs already admitted or unrelated capacity identities.
type CapacityCooldown struct {
	// CapacityID is the capacity identity this cooldown belongs to: the
	// lane's CapacityGroup, or the lane name when the group is empty.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	CapacityID string `json:"capacityID"`
	// Reason is the operator-written explanation of why the cooldown exists.
	// It never carries executor freeform text.
	// +kubebuilder:validation:MaxLength=256
	Reason string `json:"reason"`
	// ExhaustionCount is the number of distinct confirmed exhaustion events
	// whose per-run markers fall in the active cooldown window. The count is
	// recomputed from retained CoderRuns, avoiding an unbounded lane history.
	// +kubebuilder:validation:Minimum=1
	ExhaustionCount int `json:"exhaustionCount"`
	// LastExhaustedRun is the most recent run observed while folding the
	// active window. Per-run CoderRun status markers provide deduplication.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	LastExhaustedRun string `json:"lastExhaustedRun,omitempty"`
	// NextEligibleAt is when discovery and admission for this capacity
	// identity may resume. It is the operator's bounded retry window, not a
	// provider-observed reset time (Courier never invents one).
	NextEligibleAt metav1.Time `json:"nextEligibleAt"`
}

// LaneProfileStatus is operator-written state about the lane itself.
type LaneProfileStatus struct {
	// CapacityCooldown, when set, pauses new work for its capacity identity
	// until NextEligibleAt (#327). Cooldowns for other capacity identities
	// never gate this lane.
	// +optional
	CapacityCooldown *CapacityCooldown `json:"capacityCooldown,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Concurrency",type=integer,JSONPath=`.spec.concurrency`
// +kubebuilder:printcolumn:name="Suspended",type=string,JSONPath=`.metadata.annotations.courier\.misospace\.dev/suspend`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LaneProfile is a reusable model ensemble + framing referenced by CoderRuns.
type LaneProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LaneProfileSpec `json:"spec,omitempty"`
	// Status is operator-written lane state (capacity cooldown). It lives on
	// the status subresource so spec writers (Helm, bootstrap) never clobber
	// it.
	// +optional
	Status LaneProfileStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LaneProfileList contains a list of LaneProfile.
type LaneProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LaneProfile `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LaneProfile{}, &LaneProfileList{})
}

// Suspended reports whether the lane is suspended by SuspendAnnotation.
func (l *LaneProfile) Suspended() bool {
	return l != nil && strings.EqualFold(strings.TrimSpace(l.Annotations[SuspendAnnotation]), "true")
}

// CapacityIdentity is the opaque execution-capacity identity this lane draws
// from: its CapacityGroup when set, otherwise the lane's own name. Two lanes
// naming the same group are the same capacity for cooldown purposes.
func (l *LaneProfile) CapacityIdentity() string {
	if l == nil {
		return ""
	}
	if group := strings.TrimSpace(l.Spec.CapacityGroup); group != "" {
		return group
	}
	return l.Name
}

// CooldownActive reports whether the lane's capacity identity is inside its
// cooldown window at now. A cooldown for a different capacity identity never
// gates this lane, so an outage in one pool cannot strand unrelated capacity.
func (l *LaneProfile) CooldownActive(now time.Time) bool {
	if l == nil {
		return false
	}
	cooldown := l.Status.CapacityCooldown
	return cooldown.ActiveFor(l.CapacityIdentity(), now)
}

// CapacityCooldownActive reports whether any lane with the target lane's
// capacity identity has a live cooldown. Only identity and deadline are read;
// cooldown status need not be fanned out to every profile in a shared group.
func CapacityCooldownActive(target *LaneProfile, lanes []LaneProfile, now time.Time) (*CapacityCooldown, bool) {
	if target == nil {
		return nil, false
	}
	identity := target.CapacityIdentity()
	for i := range lanes {
		cooldown := lanes[i].Status.CapacityCooldown
		if cooldown.ActiveFor(identity, now) {
			return cooldown, true
		}
	}
	return nil, false
}

// ActiveFor reports whether this cooldown is live for the given capacity
// identity at now. An empty CapacityID matches nothing: a cooldown is always
// written with the identity it belongs to.
func (c *CapacityCooldown) ActiveFor(capacityID string, now time.Time) bool {
	return c != nil && c.CapacityID != "" && c.CapacityID == capacityID && c.NextEligibleAt.Time.After(now)
}
