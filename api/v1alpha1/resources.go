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

// EffectivePodResources applies Courier's pod-sizing policy to a configured
// value: a caller that configured nothing gets the built-in default; a caller
// that configured either half keeps it, but CPU and memory *requests* are
// floored to the default wherever they are absent or non-positive, so every
// Courier-created pod carries finite CPU and memory requests (#356). Limits
// stay exactly as written — omitting them is a documented operator choice.
// The result is a deep copy, so a caller that renders and mutates it cannot
// write through to the object it read — which may be the informer cache's
// copy of a LaneProfile, never the caller's to modify.
func EffectivePodResources(in corev1.ResourceRequirements) corev1.ResourceRequirements {
	if len(in.Requests) == 0 && len(in.Limits) == 0 {
		return DefaultPodResources()
	}
	copied := in.DeepCopy()
	if copied == nil {
		return DefaultPodResources()
	}
	if copied.Requests == nil {
		copied.Requests = corev1.ResourceList{}
	}
	for name, floor := range DefaultPodResources().Requests {
		if configured, ok := copied.Requests[name]; !ok || configured.Sign() <= 0 {
			copied.Requests[name] = floor
		}
	}
	return *copied
}

// EffectiveResources returns the lane's configured pod resources under
// Courier's sizing policy (see EffectivePodResources): the configured value
// with finite CPU and memory requests guaranteed, or the built-in default
// when the lane configures none. A nil spec is a lane that configures none.
func (s *LaneProfileSpec) EffectiveResources() corev1.ResourceRequirements {
	if s == nil {
		return DefaultPodResources()
	}
	return EffectivePodResources(s.Resources)
}
