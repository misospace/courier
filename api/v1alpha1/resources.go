package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// DefaultPodResources is the built-in sizing for the pods Courier creates when
// a lane configures none. It exists so no Courier-created pod is BestEffort:
// without requests the scheduler cannot bin-pack and a runaway build can take a
// node, and without limits one run can starve its neighbours. The maps are
// allocated on every call so a caller that mutates the result cannot change the
// default seen by anyone else.
func DefaultPodResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
}

// EffectiveResources returns the lane's configured pod resources, or the
// built-in default when the lane configures none. A lane that configures
// anything is used exactly as written: a partial config is never merged with
// or topped up from the default, because a lane's sizing is an operator
// decision, not a hint. A nil spec is treated as a lane that configures none.
// The result is a deep copy, so a caller that renders and mutates it cannot
// write through to the LaneProfile object — which may be the informer cache's
// copy, never the caller's to modify.
func (s *LaneProfileSpec) EffectiveResources() corev1.ResourceRequirements {
	if s == nil || (len(s.Resources.Requests) == 0 && len(s.Resources.Limits) == 0) {
		return DefaultPodResources()
	}
	if copied := s.Resources.DeepCopy(); copied != nil {
		return *copied
	}
	return DefaultPodResources()
}
