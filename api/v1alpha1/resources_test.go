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

// TestEffectiveResourcesReturnsConfigVerbatim pins that a fully configured
// lane wins wholesale: with finite CPU and memory requests present, the floor
// changes nothing and no limit is merged in.
func TestEffectiveResourcesReturnsConfigVerbatim(t *testing.T) {
	spec := &LaneProfileSpec{Resources: PodResources{
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
// sets only requests does not silently acquire default limits: omitting
// limits is a documented operator choice.
func TestEffectiveResourcesKeepsPartialConfigUnpinned(t *testing.T) {
	spec := &LaneProfileSpec{Resources: PodResources{
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

// TestEffectiveResourcesFloorsCPUMemoryRequests pins the #356 acceptance
// criterion the reviewer caught: requests-only-with-one-resource still lands
// on every pod with finite CPU *and* memory requests. The configured half of
// the requests is preserved; only the absent half is floored.
func TestEffectiveResourcesFloorsCPUMemoryRequests(t *testing.T) {
	spec := &LaneProfileSpec{Resources: PodResources{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"),
		},
	}}
	got := spec.EffectiveResources()
	assertRequests(t, got.Requests, "1", "512Mi")
	if cpu := got.Limits.Cpu(); cpu == nil || cpu.String() != "4" {
		t.Fatalf("limits cpu = %v, want the configured 4 intact", got.Limits.Cpu())
	}
	if len(got.Limits) != 1 {
		t.Fatalf("limits = %#v, want only the configured cpu limit", got.Limits)
	}
}

// TestEffectiveResourcesFloorsLimitsOnlyConfig pins that a lane setting only
// limits is not BestEffort on the requests side: the default requests appear
// while the configured limits stay exactly as written.
func TestEffectiveResourcesFloorsLimitsOnlyConfig(t *testing.T) {
	spec := &LaneProfileSpec{Resources: PodResources{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
	}}
	got := spec.EffectiveResources()
	assertRequests(t, got.Requests, "250m", "512Mi")
	if mem := got.Limits.Memory(); mem == nil || mem.String() != "8Gi" {
		t.Fatalf("limits memory = %v, want the configured 8Gi intact", got.Limits.Memory())
	}
	if len(got.Limits) != 1 {
		t.Fatalf("limits = %#v, want only the configured memory limit", got.Limits)
	}
}

// TestEffectiveResourcesFloorsNonPositiveRequests pins that "finite" means
// more than present: a zeroed or negative CPU/memory request is replaced by
// the floor rather than rendering a pod that reserves nothing.
func TestEffectiveResourcesFloorsNonPositiveRequests(t *testing.T) {
	spec := &LaneProfileSpec{Resources: PodResources{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("0"),
			corev1.ResourceMemory: resource.MustParse("-1"),
		},
	}}
	got := spec.EffectiveResources()
	assertRequests(t, got.Requests, "250m", "512Mi")
}

// TestEffectiveResourcesKeepsUnrelatedRequestKeys pins that the floor touches
// only CPU and memory: any other request key the lane configured survives.
func TestEffectiveResourcesKeepsUnrelatedRequestKeys(t *testing.T) {
	spec := &LaneProfileSpec{Resources: PodResources{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("1"),
			corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
		},
	}}
	got := spec.EffectiveResources()
	assertRequests(t, got.Requests, "1", "512Mi")
	if qty, ok := got.Requests[corev1.ResourceEphemeralStorage]; !ok || qty.String() != "10Gi" {
		t.Fatalf("ephemeral-storage = %v, want the configured 10Gi intact", got.Requests[corev1.ResourceEphemeralStorage])
	}
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
	spec := &LaneProfileSpec{Resources: PodResources{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
	}}
	got := spec.EffectiveResources()
	assertRequests(t, got.Requests, "1", "512Mi")
	delete(got.Requests, corev1.ResourceCPU)
	delete(got.Requests, corev1.ResourceMemory)
	delete(got.Limits, corev1.ResourceCPU)

	if cpu := spec.Resources.Requests.Cpu(); cpu == nil || cpu.String() != "1" {
		t.Fatalf("lane requests cpu = %v, want the configured 1 intact", spec.Resources.Requests.Cpu())
	}
	if len(spec.Resources.Requests) != 1 {
		t.Fatalf("lane requests = %#v, want only the configured cpu: the floored memory key must be the copy's, not the lane's", spec.Resources.Requests)
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
