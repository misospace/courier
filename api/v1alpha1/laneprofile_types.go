package v1alpha1

import (
	"strings"

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
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Concurrency",type=integer,JSONPath=`.spec.concurrency`
// +kubebuilder:printcolumn:name="Suspended",type=string,JSONPath=`.metadata.annotations.courier\.misospace\.dev/suspend`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LaneProfile is a reusable model ensemble + framing referenced by CoderRuns.
type LaneProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LaneProfileSpec `json:"spec,omitempty"`
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
