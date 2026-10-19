package executor

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

func TestLegacyObserverPodIsolationContract(t *testing.T) {
	run := &courierv1alpha1.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: "observer-run", Namespace: "courier-system", UID: "run-uid"}, Spec: courierv1alpha1.CoderRunSpec{Mode: courierv1alpha1.ModeResolveIssue, Repo: "acme/widgets", Ref: 7, Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Branch: "courier/acme/widgets/issue-7"}}
	lane := &courierv1alpha1.LaneProfile{ObjectMeta: metav1.ObjectMeta{Name: "local"}, Spec: courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "model"}}}
	pod, err := BuildCoordinatorPod(run, lane, PodConfig{Image: "courier:test", GitRemoteURL: "https://github.com/%s.git", GitCredentialSecret: "git-auth"})
	if err != nil {
		t.Fatal(err)
	}
	if pod.Annotations[ObserverContractAnnotation] != ObserverContractVersion {
		t.Fatalf("observer annotation = %q", pod.Annotations[ObserverContractAnnotation])
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "workspace-observer-seed" || pod.Spec.InitContainers[0].Args[0] != "observe-seed" {
		t.Fatalf("seed init container = %#v", pod.Spec.InitContainers)
	}
	seed := pod.Spec.InitContainers[0]
	if hasMount(seed, observerTokenVolume) || hasEnv(seed.Env, "COURIER_TOKEN_FILE") {
		t.Fatal("seed init container can access the observer API token")
	}
	observer := containerNamed(t, pod.Spec.Containers, ObserverContainerName)
	if observer.Args[0] != "observe-workspace" {
		t.Fatalf("observer args = %#v", observer.Args)
	}
	if observer.SecurityContext == nil || observer.SecurityContext.RunAsUser == nil || *observer.SecurityContext.RunAsUser != ObserverUID || observer.SecurityContext.RunAsGroup == nil || *observer.SecurityContext.RunAsGroup != ObserverUID || observer.SecurityContext.ReadOnlyRootFilesystem == nil || !*observer.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("observer security context = %#v", observer.SecurityContext)
	}
	if observer.SecurityContext.AllowPrivilegeEscalation == nil || *observer.SecurityContext.AllowPrivilegeEscalation || observer.SecurityContext.Capabilities == nil || len(observer.SecurityContext.Capabilities.Drop) != 1 || observer.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("observer capabilities = %#v", observer.SecurityContext)
	}
	if !hasMount(observer, "workspace") || !mountReadOnly(observer, "workspace") {
		t.Fatal("observer workspace mount is not read-only")
	}
	if !hasMount(observer, observerStateVolume) || mountReadOnly(observer, observerStateVolume) {
		t.Fatal("observer state must be mounted read-write")
	}
	if !mountReadOnly(observer, observerTokenVolume) {
		t.Fatal("observer token must be read-only")
	}
	coordinator := containerNamed(t, pod.Spec.Containers, "coordinator")
	if hasMount(coordinator, observerStateVolume) || hasMount(coordinator, observerTokenVolume) {
		t.Fatal("coordinator can access protected observer state or token")
	}
	if hasEnv(coordinator.Env, "COURIER_TOKEN_FILE") || hasEnv(seed.Env, "COURIER_TOKEN_FILE") {
		t.Fatal("API token leaked outside observer")
	}
	if got := envValue(observer.Env, "COURIER_RUN_UID"); got != string(run.UID) {
		t.Fatalf("run UID env = %q", got)
	}
	if got := envValue(observer.Env, "COURIER_REPO_URL"); got != "https://github.com/acme/widgets.git" {
		t.Fatalf("remote env = %q", got)
	}
	if got := envValue(observer.Env, "COURIER_GIT_USERNAME"); got != "" {
		t.Fatalf("credential unexpectedly literal: %q", got)
	}
	if !envSecretRef(observer.Env, "COURIER_GIT_USERNAME", "git-auth", "username") || !envSecretRef(observer.Env, "COURIER_GIT_TOKEN", "git-auth", "token") {
		t.Fatal("observer credential secret refs missing")
	}
	if envSecretRef(observer.Env, "GITHUB_TOKEN", "git-auth", "token") {
		t.Fatal("observer received GitHub API credentials")
	}
	if observer.TerminationMessagePath != ObserverStatePath+"/termination" || observer.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Fatalf("termination path = %q", observer.TerminationMessagePath)
	}
	if seed.TerminationMessagePath != ObserverStatePath+"/seed-termination" || seed.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Fatalf("seed termination path = %q", seed.TerminationMessagePath)
	}
}

func hasMount(container corev1.Container, name string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}
func mountReadOnly(container corev1.Container, name string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name {
			return mount.ReadOnly
		}
	}
	return false
}
func hasEnv(envs []corev1.EnvVar, name string) bool {
	for _, env := range envs {
		if env.Name == name {
			return true
		}
	}
	return false
}
func envValue(envs []corev1.EnvVar, name string) string {
	for _, env := range envs {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}
func envSecretRef(envs []corev1.EnvVar, name, secret, key string) bool {
	for _, env := range envs {
		if env.Name == name && env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			return env.ValueFrom.SecretKeyRef.Name == secret && env.ValueFrom.SecretKeyRef.Key == key
		}
	}
	return false
}
func containerNamed(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, c := range containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("container %q missing", name)
	return corev1.Container{}
}

// TestLegacyObserverContainersCarryBoundedSizing pins the containment
// invariant for the two Courier-internal observer containers: the seed init
// container and the observer sidecar always carry finite cpu and memory
// requests and limits — an unbounded one of them can consume the node past
// every sized container's limit (the 82GiB starvation class) — and they carry
// the fixed small Courier-internal sizing, never the lane's, so a big lane
// does not triple the coordinator pod's scheduler reservation for helper
// processes and a lane that sizes nothing still does not leave them
// BestEffort.
func TestLegacyObserverContainersCarryBoundedSizing(t *testing.T) {
	newRun := func(name string) *courierv1alpha1.CoderRun {
		return &courierv1alpha1.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "courier-system", UID: "run-uid"}, Spec: courierv1alpha1.CoderRunSpec{Mode: courierv1alpha1.ModeResolveIssue, Repo: "acme/widgets", Ref: 7, Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Branch: "courier/acme/widgets/issue-7"}}
	}
	lanes := map[string]*courierv1alpha1.LaneProfile{
		"lane configures nothing": {ObjectMeta: metav1.ObjectMeta{Name: "local"}, Spec: courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "model"}}},
		"lane configures sizing": {ObjectMeta: metav1.ObjectMeta{Name: "local"}, Spec: courierv1alpha1.LaneProfileSpec{
			Roles: map[string]string{"coordinator": "model"},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m"), corev1.ResourceMemory: resource.MustParse("3Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("12Gi")},
			},
		}},
	}
	for name, lane := range lanes {
		pod, err := BuildCoordinatorPod(newRun("sized-observer-run"), lane, PodConfig{Image: "courier:test", GitRemoteURL: "https://github.com/%s.git", GitCredentialSecret: "git-auth"})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(pod.Spec.InitContainers) != 1 {
			t.Fatalf("%s: init containers = %#v", name, pod.Spec.InitContainers)
		}
		seed := pod.Spec.InitContainers[0]
		observer := containerNamed(t, pod.Spec.Containers, ObserverContainerName)
		for _, c := range []corev1.Container{seed, observer} {
			assertContainerCPUAndMemory(t, c.Resources.Requests, "25m", "64Mi")
			assertContainerCPUAndMemory(t, c.Resources.Limits, "1", "512Mi")
		}
	}
}
