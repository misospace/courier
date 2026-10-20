package v1alpha1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// DefaultPodResources is the built-in sizing for the pods Courier creates when
// a lane configures none. It exists so no Courier-created pod is BestEffort:
// without requests the scheduler cannot bin-pack and a runaway build can take
// a node, and without limits one run can starve its neighbours. Maps are
// allocated on every call so a caller mutating the result cannot change the
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

// DefaultObserverResources is the built-in sizing for the workspace-observer
// sidecar and its seed init container that Courier attaches to coordinator
// pods. These are Courier-internal helpers — baseline capture and workspace
// observation — not lane workloads, so they carry fixed small sizing rather
// than the lane's: the lane sizes the work a model does, and sizing internal
// helpers from it would multiply a coordinator pod's scheduler reservation
// for processes that never grow with the lane. This closes the one unbounded
// container class a sized lane could still leave on a node: an observer with
// no limits can consume the whole node past every other container's limit.
// Maps are allocated on every call, as in DefaultPodResources.
func DefaultObserverResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("25m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
}

// ProbePodResources is the fixed sizing of a disposable preflight probe pod.
// It deliberately does not follow the lane's sizing: a probe is a
// seconds-lived `sh -c` network check, but preflight creates one per eligible
// node simultaneously, so reserving lane-scale *requests* for it can leave
// probes Pending on a cluster tight on requests and wedge preflight instead
// of proving isolation (#402). Requests stay finite so a probe is never
// BestEffort.
//
// Limits are fixed too, and deliberately generous rather than absent: a
// ResourceQuota that tracks limits.cpu or limits.memory rejects any container
// missing the matching limit, so a requests-only probe would be denied with
// Forbidden on such namespaces — a denial the create paths treat as an
// ordinary retryable error, wedging preflight instead of proving anything.
// The limits exist to satisfy that admission requirement with wide headroom
// over the probe image's real usage (a shell doing DNS and network checks
// peaks in the tens of MiB), so they cannot OOM-kill the probe and falsify
// the isolation result it exists to produce. The maps are allocated on every
// call so a caller that mutates the result cannot change the sizing seen by
// anyone else.
func ProbePodResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
}

// EffectivePodResources applies Courier's pod-sizing policy to a configured
// value: a caller that configured nothing gets the built-in default; a caller
// that configured either half keeps it, but CPU and memory *requests* are
// floored to the default wherever absent or non-positive, so every
// Courier-created pod carries finite CPU and memory requests (#356). Limits
// stay exactly as written — omitting them is a documented operator choice.
// Every other key passes through unchanged in both halves: the floor covers
// CPU and memory only, and this resolver neither validates nor rejects a
// key, so an extended resource's own requests/limits rules stay Kubernetes'.
// The result is a deep copy, so a caller that renders and mutates it cannot
// write through to the object it read — which may be the informer cache's
// copy of a LaneProfile, never the caller's to modify. This is render-layer
// defence in depth; the launch paths reject clearly-invalid sizing upstream
// with ValidatePodResources, so a validated lane never reaches the floor with
// a non-positive value at all.
func EffectivePodResources(in corev1.ResourceRequirements) corev1.ResourceRequirements {
	if len(in.Requests) == 0 && len(in.Limits) == 0 {
		return DefaultPodResources()
	}
	out := in.DeepCopy()
	if out.Requests == nil {
		out.Requests = corev1.ResourceList{}
	}
	for name, floor := range DefaultPodResources().Requests {
		if q, ok := out.Requests[name]; !ok || q.CmpInt64(0) <= 0 {
			out.Requests[name] = floor
		}
	}
	return *out
}

// ValidatePodResources rejects lane-configured CPU and memory sizing that
// cannot produce a runnable pod, so a misconfigured lane fails the run before
// launch instead of mid-provisioning:
//
//   - a negative quantity anywhere (request or limit) is nonsense;
//   - a zero CPU or memory limit means an instantly-killed or unschedulable
//     container;
//   - a zero CPU or memory request asks for no reservation. Omit the key to
//     take the built-in floor instead — a lane's sizing is an operator
//     decision, and silently rewriting a declared zero hides the
//     misconfiguration the operator meant to declare. The render-layer floor
//     still backs callers that bypass launch validation.
//
// Only cpu and memory are policed. Extended resources (e.g. GPU) stay
// exactly written with their own admission rules — a limits-only extended
// resource is valid and never floored (#403). An empty configuration is
// valid: it resolves to the built-in default.
func ValidatePodResources(in corev1.ResourceRequirements) error {
	for _, half := range []struct {
		what string
		list corev1.ResourceList
	}{{"requests", in.Requests}, {"limits", in.Limits}} {
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			q, ok := half.list[name]
			if !ok {
				continue
			}
			if q.CmpInt64(0) <= 0 {
				return fmt.Errorf("pod resources: %s %s must be positive; omit the key to take Courier's built-in default, got %s", half.what, name, q.String())
			}
		}
	}
	return nil
}

// EffectiveResources returns the lane's configured pod resources under
// Courier's sizing policy (see EffectivePodResources): the narrowed lane
// vocabulary with finite CPU and memory requests guaranteed, or the built-in
// default when the lane configures none. A nil spec is a lane that configures
// none. The result never aliases the LaneProfile's cached resource maps.
func (s *LaneProfileSpec) EffectiveResources() corev1.ResourceRequirements {
	if s == nil {
		return DefaultPodResources()
	}
	return EffectivePodResources(corev1.ResourceRequirements{
		Requests: s.Resources.Requests,
		Limits:   s.Resources.Limits,
	})
}

// ValidateResources rejects a lane-configured pod sizing that could never
// launch (see ValidatePodResources). Launch paths call it before rendering
// or creating any pod.
func (s *LaneProfileSpec) ValidateResources() error {
	if s == nil {
		return nil
	}
	return ValidatePodResources(corev1.ResourceRequirements{
		Requests: s.Resources.Requests,
		Limits:   s.Resources.Limits,
	})
}
