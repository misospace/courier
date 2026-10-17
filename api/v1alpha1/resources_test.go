package v1alpha1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestEffectiveResourcesDefaultsWhenLaneConfiguresNone pins the issue's whole
// point: a lane that says nothing about sizing still gets requests and limits,
// so its pods are not BestEffort.
func TestEffectiveResourcesDefaultsWhenLaneConfiguresNone(t *testing.T) {
	var spec *LaneProfileSpec
	got := spec.EffectiveResources()
	assertRequestsAndLimits(t, got, "250m", "512Mi", "2", "4Gi")
}

// TestEffectiveResourcesReturnsConfigVerbatim pins that an explicit lane
// config wins wholesale: no default is merged in, and no half is filled in.
func TestEffectiveResourcesReturnsConfigVerbatim(t *testing.T) {
	spec := &LaneProfileSpec{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("16Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("8"),
			corev1.ResourceMemory: resource.MustParse("32Gi"),
		},
	}}
	assertRequestsAndLimits(t, spec.EffectiveResources(), "4", "16Gi", "8", "32Gi")
}

// TestEffectiveResourcesKeepsPartialConfigUnpinned pins that a lane which
// sets only requests does not silently acquire default limits.
func TestEffectiveResourcesKeepsPartialConfigUnpinned(t *testing.T) {
	spec := &LaneProfileSpec{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		},
	}}
	got := spec.EffectiveResources()
	if len(got.Limits) != 0 {
		t.Fatalf("limits = %#v, want none: a partial lane config is used exactly as written", got.Limits)
	}
	assertRequests(t, got.Requests, "1", "2Gi")
}

// TestDefaultPodResourcesIsIndependentPerCall pins that a caller mutating one
// result cannot corrupt the default handed to the next lane.
func TestDefaultPodResourcesIsIndependentPerCall(t *testing.T) {
	first := DefaultPodResources()
	delete(first.Requests, corev1.ResourceCPU)
	delete(first.Limits, corev1.ResourceMemory)
	first.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("1Gi")

	second := DefaultPodResources()
	if _, ok := second.Requests[corev1.ResourceCPU]; !ok {
		t.Fatalf("requests = %#v, want the default cpu request intact", second.Requests)
	}
	if _, ok := second.Limits[corev1.ResourceMemory]; !ok {
		t.Fatalf("limits = %#v, want the default memory limit intact", second.Limits)
	}
	if _, ok := second.Requests[corev1.ResourceEphemeralStorage]; ok {
		t.Fatalf("requests = %#v, want no leakage from a mutated earlier result", second.Requests)
	}
	assertRequestsAndLimits(t, second, "250m", "512Mi", "2", "4Gi")
}

// TestEffectiveResourcesDoesNotAliasLaneConfig pins that the returned maps are
// a deep copy of the lane's: a caller that renders and mutates them cannot
// corrupt the LaneProfile object it read, which controller-runtime may have
// served from its informer cache.
func TestEffectiveResourcesDoesNotAliasLaneConfig(t *testing.T) {
	spec := &LaneProfileSpec{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
	}}
	got := spec.EffectiveResources()
	delete(got.Requests, corev1.ResourceCPU)
	delete(got.Limits, corev1.ResourceCPU)

	if cpu := spec.Resources.Requests.Cpu(); cpu == nil || cpu.String() != "1" {
		t.Fatalf("lane requests cpu = %v, want the configured 1 intact", spec.Resources.Requests.Cpu())
	}
	if cpu := spec.Resources.Limits.Cpu(); cpu == nil || cpu.String() != "2" {
		t.Fatalf("lane limits cpu = %v, want the configured 2 intact", spec.Resources.Limits.Cpu())
	}
}

func assertRequestsAndLimits(t *testing.T, got corev1.ResourceRequirements, cpuReq, memReq, cpuLim, memLim string) {
	t.Helper()
	assertRequests(t, got.Requests, cpuReq, memReq)
	assertRequests(t, got.Limits, cpuLim, memLim)
}

func assertRequests(t *testing.T, list corev1.ResourceList, cpu, memory string) {
	t.Helper()
	if len(list) == 0 {
		t.Fatalf("resource list is empty, want cpu %q and memory %q", cpu, memory)
	}
	if got := list.Cpu(); got == nil || got.String() != cpu {
		t.Fatalf("cpu = %v, want %q", list.Cpu(), cpu)
	}
	if got := list.Memory(); got == nil || got.String() != memory {
		t.Fatalf("memory = %v, want %q", list.Memory(), memory)
	}
}
