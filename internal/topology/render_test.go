package topology

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/protocol"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	courier "github.com/misospace/courier/api/v1alpha1"
)

func testRun(name string) *courier.CoderRun {
	return &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("run-uid-" + name)},
		Spec: courier.CoderRunSpec{
			Mode:   courier.ModeResolveIssue,
			Source: "manual",
			Repo:   "misospace/courier",
			Ref:    1,
			Lane:   "default",
		},
	}
}

func credentialEnvs(pod *corev1.Pod) map[string]corev1.EnvVarSource {
	out := map[string]corev1.EnvVarSource{}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.ValueFrom != nil {
			out[env.Name] = *env.ValueFrom
		}
	}
	return out
}

func secretVolumes(pod *corev1.Pod) map[string]string {
	out := map[string]string{}
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			out[v.Name] = v.Secret.SecretName
		}
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.ServiceAccountToken != nil {
					out[v.Name] = "service-account-token"
				}
				if src.Secret != nil {
					out[v.Name] = src.Secret.Name
				}
			}
		}
	}
	return out
}

func TestWorkerPodHasNoIdentityMaterial(t *testing.T) {
	run := testRun("worker-clean")
	pub, _, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("worker pod must not automount a service account token")
	}
	if pod.Spec.ServiceAccountName != WorkerSAName(run.Name) {
		t.Fatalf("worker service account = %q", pod.Spec.ServiceAccountName)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("worker restart policy = %q", pod.Spec.RestartPolicy)
	}
	if vols := secretVolumes(pod); len(vols) != 0 {
		t.Fatalf("worker pod must mount no secret, token, or projected volume: %v", vols)
	}
	for _, v := range pod.Spec.Volumes {
		if v.EmptyDir == nil {
			t.Fatalf("worker volume %q must be an emptyDir", v.Name)
		}
	}
	envs := credentialEnvs(pod)
	for _, forbidden := range []string{EnvForgeAPIToken, EnvGitToken, EnvGitUsername, EnvBrokerTokenFile, EnvSigningKeyFile} {
		if _, ok := envs[forbidden]; ok {
			t.Fatalf("worker pod carries forbidden env %q", forbidden)
		}
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			t.Fatalf("worker env %q references a Secret", env.Name)
		}
	}
	if pod.Spec.Containers[0].Command[0] != "/usr/local/bin/courier-worker" {
		t.Fatalf("worker argv[0] = %v", pod.Spec.Containers[0].Command)
	}
	if pod.Labels["courier.misospace.dev/component"] != ComponentWorker {
		t.Fatalf("worker component label = %q", pod.Labels["courier.misospace.dev/component"])
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].UID != run.UID || pod.OwnerReferences[0].Controller == nil || !*pod.OwnerReferences[0].Controller {
		t.Fatal("worker pod must be controller-owned by the CoderRun")
	}
}

func TestWorkerPodCachePin(t *testing.T) {
	run := testRun("worker-cache")
	pub, _, _ := protocol.GenerateKey()
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, &CachePin{IP: "10.0.0.42", Port: 5120}, courier.DefaultPodResources(), nil)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, env := range pod.Spec.Containers[0].Env {
		found[env.Name] = env.Value
	}
	if found[EnvGoProxy] != "http://10.0.0.42:5120" {
		t.Fatalf("GOPROXY = %q", found[EnvGoProxy])
	}
	if found[EnvGoSumDB] != "off" {
		t.Fatalf("GOSUMDB = %q", found[EnvGoSumDB])
	}
}

// The worker carries the same per-pod toolchain-cache contract as the legacy
// coordinator pod: one emptyDir writable at the pinned cache path and
// read-only at the reference twin, with COURIER_TOOLCHAIN_DIR pointing at the
// read-only side. The mounts cannot shadow baked tool installations because
// tools install outside both cache paths.
func TestWorkerPodToolchainCacheContract(t *testing.T) {
	run := testRun("worker-toolchain")
	pub, _, _ := protocol.GenerateKey()
	pod, err := WorkerPod(run, "toolchain:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.Containers[0].Image != "toolchain:test" {
		t.Fatalf("worker image = %q, want the caller-resolved argument", pod.Spec.Containers[0].Image)
	}
	cache := 0
	reference := false
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		switch mount.MountPath {
		case executor.ToolchainCachePath:
			if mount.ReadOnly {
				t.Fatalf("toolchain cache mount at %s must be writable", mount.MountPath)
			}
			cache++
		case executor.ToolchainReferencePath:
			if !mount.ReadOnly {
				t.Fatal("toolchain reference mount must be read-only")
			}
			reference = true
		}
	}
	if cache != 1 || !reference {
		t.Fatalf("toolchain mounts: %d writable cache, reference present = %t", cache, reference)
	}
	for _, v := range pod.Spec.Volumes {
		if v.Name == "toolchain-cache" && v.EmptyDir == nil {
			t.Fatal("toolchain-cache volume must be an emptyDir (per-pod scratch cache)")
		}
	}
	env := map[string]string{}
	for _, e := range pod.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["COURIER_TOOLCHAIN_DIR"] != executor.ToolchainReferencePath {
		t.Fatalf("COURIER_TOOLCHAIN_DIR = %q", env["COURIER_TOOLCHAIN_DIR"])
	}
}

func TestControlPodIdentityContract(t *testing.T) {
	run := testRun("control-pod")
	pod, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa-uid",
		WorkerPodUID:          "worker-uid",
		ControlIncarnationUID: "incarnation-1",
		WorkerURL:             "http://r-worker.ns.svc.cluster.local:8080",
		BrokerURL:             "https://r-broker.ns.svc.cluster.local:8443",
		Resources:             courier.DefaultPodResources(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The operator's observation contract and the broker's authenticator both
	// key on the "coordinator" container and component label.
	if pod.Spec.Containers[0].Name != ControlContainerName {
		t.Fatalf("control container name = %q", pod.Spec.Containers[0].Name)
	}
	if pod.Labels["courier.misospace.dev/component"] != ComponentCoordinator {
		t.Fatalf("control component label = %q", pod.Labels["courier.misospace.dev/component"])
	}
	if pod.Spec.ServiceAccountName != ControlSAName(run.Name) {
		t.Fatalf("control service account = %q", pod.Spec.ServiceAccountName)
	}
	envs := map[string]corev1.EnvVar{}
	for _, env := range pod.Spec.Containers[0].Env {
		envs[env.Name] = env
	}
	for _, forbidden := range []string{EnvForgeAPIToken, EnvGitToken, EnvGitUsername} {
		if env, ok := envs[forbidden]; ok {
			t.Fatalf("control pod carries forge credential env %q = %+v", forbidden, env)
		}
	}
	if envs[EnvWorkerPodUID].Value != "worker-uid" || envs[EnvControlPodUID].Value != "incarnation-1" {
		t.Fatalf("control identity env = %+v %+v", envs[EnvWorkerPodUID], envs[EnvControlPodUID])
	}
	// The projected broker token must be pod-bound with the 600s audience.
	var tokenProjected bool
	for _, v := range pod.Spec.Volumes {
		if v.Projected == nil {
			continue
		}
		for _, src := range v.Projected.Sources {
			tok := src.ServiceAccountToken
			if tok == nil {
				continue
			}
			if tok.Audience != BrokerAudience || tok.ExpirationSeconds == nil || *tok.ExpirationSeconds != ControlTokenSeconds {
				t.Fatalf("control token projection = %+v", tok)
			}
			tokenProjected = true
		}
	}
	if !tokenProjected {
		t.Fatal("control pod must project a broker-audience token")
	}
	mounts := map[string]string{}
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		mounts[m.MountPath] = m.Name
	}
	if _, ok := mounts["/var/run/courier/signing"]; !ok {
		t.Fatal("control pod must mount the signing key")
	}
	if _, ok := mounts["/var/run/courier/broker"]; !ok {
		t.Fatal("control pod must mount the broker CA")
	}
	if envs["COURIER_TERMINATION_FILE"].Value != controlRuntimePath+"/termination" {
		t.Fatal("control termination file path drifts from the operator's contract")
	}
}

func TestBrokerPodProjectionContract(t *testing.T) {
	run := testRun("broker-pod")
	registration := registrationFixture()
	pod, err := BrokerPod(run, "harness:test", registration.Projection(), "sa-uid", courier.DefaultPodResources())
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.ServiceAccountName != BrokerSAName(run.Name) {
		t.Fatalf("broker service account = %q", pod.Spec.ServiceAccountName)
	}
	envs := map[string]corev1.EnvVar{}
	for _, env := range pod.Spec.Containers[0].Env {
		envs[env.Name] = env
	}
	if envs[EnvProviderName].Value != "github" || envs[EnvProviderType].Value != "github" {
		t.Fatalf("provider projection env = %+v", envs)
	}
	for _, name := range []string{EnvForgeAPIToken, EnvGitUsername, EnvGitToken} {
		ref := envs[name].ValueFrom.SecretKeyRef
		if ref == nil || ref.Name != CredentialsSecretName(run.Name) {
			t.Fatalf("env %q must resolve through the per-run credential copy", name)
		}
	}
	// Exactly the fixed key contract of the credential copy.
	if envs[EnvForgeAPIToken].ValueFrom.SecretKeyRef.Key != CredKeyForgeAPIToken ||
		envs[EnvGitToken].ValueFrom.SecretKeyRef.Key != CredKeyGitToken ||
		envs[EnvGitUsername].ValueFrom.SecretKeyRef.Key != CredKeyGitUsername {
		t.Fatal("broker credential env keys drift from the copy contract")
	}
	vols := secretVolumes(pod)
	if vols["tls"] != BrokerTLSSecretName(run.Name) || vols["policy"] != PolicySecretName(run.Name) {
		t.Fatalf("broker volumes = %v", vols)
	}
	if _, ok := vols["api-token"]; !ok {
		t.Fatal("broker must project its own API token")
	}
}

func registrationFixture() *forge.Registration {
	return &forge.Registration{
		Name:     "github",
		Type:     "github",
		Endpoint: "https://api.github.com/",
		Credentials: map[string]forge.CredentialRef{
			forge.PurposeForgeAPI: {SecretName: "src", Key: "token"},
		},
		Serves: []string{"*"},
	}
}

// configuredResources is a lane's explicit sizing: an operator decision the
// renderers must use exactly as written.
func configuredResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1500m"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		},
	}
}

// renderEveryPod builds the three lane-sized rendered pods with one sizing
// value, keyed by container name. Probe pods are deliberately absent: their
// sizing is fixed, not the lane's, and has its own tests below.
func renderEveryPod(t *testing.T, run *courier.CoderRun, resources corev1.ResourceRequirements) map[string]*corev1.Pod {
	t.Helper()
	pub, _, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	worker, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, resources, nil)
	if err != nil {
		t.Fatal(err)
	}
	control, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa-uid",
		WorkerPodUID:          "worker-uid",
		ControlIncarnationUID: "incarnation-1",
		WorkerURL:             "http://r-worker.ns.svc.cluster.local:8080",
		BrokerURL:             "https://r-broker.ns.svc.cluster.local:8443",
		Resources:             resources,
	})
	if err != nil {
		t.Fatal(err)
	}
	broker, err := BrokerPod(run, "harness:test", registrationFixture().Projection(), "sa-uid", resources)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*corev1.Pod{
		WorkerContainerName:  worker,
		ControlContainerName: control,
		BrokerContainerName:  broker,
	}
}

// TestConfiguredLaneResourcesReachEveryPod pins that the lane's sizing is the
// sizing of every lane-sized pod — worker, control, and broker — not just the
// coordinator's. Probe pods are the deliberate exception, tested separately.
func TestConfiguredLaneResourcesReachEveryPod(t *testing.T) {
	run := testRun("resources-configured")
	want := configuredResources()
	for container, pod := range renderEveryPod(t, run, want) {
		if got := pod.Spec.Containers[0].Resources; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s container resources = %+v, want %+v", container, got, want)
		}
	}
}

// TestNoRenderedPodIsBestEffort is the invariant of #356: a caller that
// configures no sizing still gets finite cpu and memory requests and limits,
// so no Courier-created pod lands in the BestEffort QoS class.
func TestNoRenderedPodIsBestEffort(t *testing.T) {
	run := testRun("resources-default")
	for container, pod := range renderEveryPod(t, run, corev1.ResourceRequirements{}) {
		resources := pod.Spec.Containers[0].Resources
		for _, list := range []struct {
			what string
			in   corev1.ResourceList
		}{{"requests", resources.Requests}, {"limits", resources.Limits}} {
			for _, key := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
				quantity, ok := list.in[key]
				if !ok || quantity.IsZero() {
					t.Fatalf("%s container %s carry no finite %s: %+v", container, list.what, key, resources)
				}
			}
		}
	}
}

// TestPartialLaneResourcesAreFloored pins the #356 floor at the render layer:
// a lane sizing only one of CPU/memory requests still renders pods with
// finite CPU *and* memory requests, while its configured values survive and
// limits it did not set stay absent.
func TestPartialLaneResourcesAreFloored(t *testing.T) {
	run := testRun("resources-partial")
	partial := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}
	for container, pod := range renderEveryPod(t, run, partial) {
		resources := pod.Spec.Containers[0].Resources
		if cpu := resources.Requests.Cpu(); cpu == nil || cpu.String() != "1" {
			t.Fatalf("%s container requests cpu = %v, want the configured 1 intact", container, resources.Requests.Cpu())
		}
		if mem := resources.Requests.Memory(); mem == nil || mem.String() != "512Mi" {
			t.Fatalf("%s container requests memory = %v, want the floored 512Mi", container, resources.Requests.Memory())
		}
		if len(resources.Limits) != 0 {
			t.Fatalf("%s container limits = %+v, want none: omitting limits is the lane's choice", container, resources.Limits)
		}
	}
}

// TestProbePodSizingIsFixed pins the #402 decision: a probe pod carries the
// fixed small probe sizing, not the lane's. Its requests stay finite so a
// probe is never BestEffort, and its limits stay fixed too — a ResourceQuota
// tracking limits.cpu or limits.memory rejects a container missing the
// matching limit, so a requests-only probe would be denied Forbidden on such
// namespaces and preflight would requeue on it forever.
func TestProbePodSizingIsFixed(t *testing.T) {
	run := testRun("resources-probe")
	pod := ProbePod(run, "node-a", "busybox:test", "true")
	got := pod.Spec.Containers[0].Resources
	if want := courier.ProbePodResources(); !reflect.DeepEqual(got, want) {
		t.Fatalf("probe container resources = %+v, want the fixed probe sizing %+v", got, want)
	}
	for _, key := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		quantity, ok := got.Requests[key]
		if !ok || quantity.IsZero() {
			t.Fatalf("probe container carries no finite %s request: %+v", key, got)
		}
		limit, ok := got.Limits[key]
		if !ok || limit.IsZero() {
			t.Fatalf("probe container carries no finite %s limit: a quota tracking limits.%s would reject it", key, key)
		}
		if quantity.Cmp(limit) > 0 {
			t.Fatalf("probe container %s request %v exceeds its limit %v; the API server would reject the pod", key, quantity, limit)
		}
	}
}

// TestProbePodSizingIgnoresTheLane pins the deliberate #402 exception: probe
// sizing is never the lane's or the built-in default, because preflight
// creates one probe per eligible node simultaneously and lane-scale
// reservations there can leave probes Pending on a request-tight cluster.
// The bound assertions, not just the inequality checks, are the regression
// guard: a probe regressing to the default *requests* would dodge an
// exact-equality check while reintroducing exactly the per-node reservation
// that wedged preflight; its limits are bounded below the default limits for
// the same reason — a probe charging 2 CPU/4Gi of limits against a quota is
// lane-scale by the back door.
func TestProbePodSizingIgnoresTheLane(t *testing.T) {
	run := testRun("resources-probe-lane")
	pod := ProbePod(run, "node-a", "busybox:test", "true")
	got := pod.Spec.Containers[0].Resources
	if reflect.DeepEqual(got, courier.DefaultPodResources()) {
		t.Fatal("probe container carries the built-in default sizing; probes must not reserve lane-scale capacity")
	}
	if reflect.DeepEqual(got, courier.EffectivePodResources(configuredResources())) {
		t.Fatal("probe container carries the lane's sizing; probes must not reserve lane-scale capacity")
	}
	floor := courier.DefaultPodResources().Requests
	if got.Requests.Cpu().Cmp(*floor.Cpu()) >= 0 {
		t.Fatalf("probe requests cpu = %v, want strictly below the default floor %v", got.Requests.Cpu(), floor.Cpu())
	}
	if got.Requests.Memory().Cmp(*floor.Memory()) >= 0 {
		t.Fatalf("probe requests memory = %v, want strictly below the default floor %v", got.Requests.Memory(), floor.Memory())
	}
	defaultLimits := courier.DefaultPodResources().Limits
	if got.Limits.Cpu().Cmp(*defaultLimits.Cpu()) >= 0 {
		t.Fatalf("probe limits cpu = %v, want strictly below the default limit %v", got.Limits.Cpu(), defaultLimits.Cpu())
	}
	if got.Limits.Memory().Cmp(*defaultLimits.Memory()) >= 0 {
		t.Fatalf("probe limits memory = %v, want strictly below the default limit %v", got.Limits.Memory(), defaultLimits.Memory())
	}
}

// TestBrokerProbePodMirrorsProbePodExceptItsIdentity pins the broker probe as
// the worker probe's exact twin except for the identity that selects the
// policy it exercises: the broker's component label (so the broker's
// NetworkPolicy — not the worker's — is what the pod's traffic is judged
// against) and the broker's service account. Everything else — the probe
// label, the no-token automount, the Never restart policy, the fixed probe
// sizing, the container shape — must match, and its name must stay
// deterministic and distinct from the worker probe's on the same node.
func TestBrokerProbePodMirrorsProbePodExceptItsIdentity(t *testing.T) {
	run := testRun("broker-probe")
	worker := ProbePod(run, "node-a", "busybox:test", "true")
	broker := BrokerProbePod(run, "node-a", "busybox:test", "true")

	if got := broker.Labels["courier.misospace.dev/component"]; got != ComponentBroker {
		t.Fatalf("broker probe component label = %q, want %q", got, ComponentBroker)
	}
	if worker.Labels["courier.misospace.dev/component"] != ComponentWorker {
		t.Fatalf("worker probe component label = %q, want %q", worker.Labels["courier.misospace.dev/component"], ComponentWorker)
	}
	if broker.Labels[LabelProbe] != "true" {
		t.Fatal("broker probe must carry the probe label")
	}
	if broker.Name == worker.Name {
		t.Fatalf("broker and worker probes for the same node share the name %q", broker.Name)
	}
	if again := BrokerProbePod(run, "node-a", "busybox:test", "true").Name; again != broker.Name {
		t.Fatalf("broker probe name is not deterministic: %q then %q", broker.Name, again)
	}
	if broker.Spec.ServiceAccountName != BrokerSAName(run.Name) {
		t.Fatalf("broker probe service account = %q, want %q", broker.Spec.ServiceAccountName, BrokerSAName(run.Name))
	}
	if broker.Spec.AutomountServiceAccountToken == nil || *broker.Spec.AutomountServiceAccountToken {
		t.Fatal("broker probe must not automount a service account token")
	}
	if broker.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("broker probe restart policy = %q", broker.Spec.RestartPolicy)
	}
	if broker.Spec.NodeName != worker.Spec.NodeName {
		t.Fatalf("broker probe node = %q, want the probe's node %q", broker.Spec.NodeName, worker.Spec.NodeName)
	}
	if want := courier.ProbePodResources(); !reflect.DeepEqual(broker.Spec.Containers[0].Resources, want) {
		t.Fatalf("broker probe container resources = %+v, want the fixed probe sizing %+v", broker.Spec.Containers[0].Resources, want)
	}
	if broker.Spec.Containers[0].Name != "probe" {
		t.Fatalf("broker probe container name = %q, want \"probe\"", broker.Spec.Containers[0].Name)
	}
	if broker.Spec.Containers[0].TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Fatalf("broker probe termination message policy = %q", broker.Spec.Containers[0].TerminationMessagePolicy)
	}
	if got, want := broker.Spec.Containers[0].Command, worker.Spec.Containers[0].Command; !reflect.DeepEqual(got, want) {
		t.Fatalf("broker probe command = %v, want the worker probe's %v", got, want)
	}
}

// TestRenderedPodsDoNotAliasLaneSizing pins that a rendered pod owns its sizing
// maps. The caller renders from a LaneProfile that controller-runtime may hold
// in its informer cache, so a pod spec that still referenced those maps could
// have its sizing mutated out from under the lane.
func TestRenderedPodsDoNotAliasLaneSizing(t *testing.T) {
	run := testRun("resources-alias")
	from := configuredResources()
	for container, pod := range renderEveryPod(t, run, from) {
		requests := pod.Spec.Containers[0].Resources.Requests
		requests[corev1.ResourceCPU] = resource.MustParse("64")
		if got := requests.Cpu().String(); got != "64" {
			t.Fatalf("%s container requests not writable, test proves nothing: cpu = %s", container, got)
		}
	}
	if got := from.Requests.Cpu().String(); got != "500m" {
		t.Fatalf("rendering aliased the caller's requests: cpu = %s, want 500m", got)
	}
}

// TestCoordinatorAndTopologyPodsAgreeOnTheDefault proves the two pod-rendering
// paths size an unconfigured lane identically, so one run is never half
// BestEffort.
func TestCoordinatorAndTopologyPodsAgreeOnTheDefault(t *testing.T) {
	run := testRun("resources-agree")
	lane := &courier.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: run.Spec.Lane, Namespace: run.Namespace},
		Spec:       courier.LaneProfileSpec{Roles: map[string]string{"coordinator": "test/coordinator"}},
	}
	coordinator, err := executor.BuildCoordinatorPod(run, lane, executor.PodConfig{
		Image:              "harness:test",
		WorkspacePath:      "/workspace",
		ServiceAccountName: "coordinator",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := courier.DefaultPodResources()
	if got := coordinator.Spec.Containers[0].Resources; !reflect.DeepEqual(got, want) {
		t.Fatalf("coordinator pod resources = %+v, want the built-in default %+v", got, want)
	}
	for container, pod := range renderEveryPod(t, run, lane.Spec.EffectiveResources()) {
		if got := pod.Spec.Containers[0].Resources; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s container resources = %+v, want the coordinator's %+v", container, got, want)
		}
	}
}

func TestBrokerRoleIsExactlyTheNamedGrants(t *testing.T) {
	run := testRun("rbac")
	role := BrokerRole(run)
	if len(role.Rules) != 4 {
		t.Fatalf("broker role rules = %+v", role.Rules)
	}
	for _, rule := range role.Rules {
		switch {
		case contains(rule.Resources, "coderruns"):
			if len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != run.Name {
				t.Fatalf("coderruns grant must be name-scoped: %+v", rule)
			}
		case contains(rule.Resources, "coderruns/status"):
			if len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != run.Name {
				t.Fatalf("status grant must be name-scoped: %+v", rule)
			}
		case contains(rule.Resources, "pods") && contains(rule.Verbs, "get"):
			// Named pod reads only: the three topology pods.
			want := []string{ControlPodName(run.Name), WorkerPodName(run.Name), BrokerPodName(run.Name)}
			if len(rule.ResourceNames) != 3 {
				t.Fatalf("pod get must be name-scoped: %+v", rule)
			}
			for _, name := range want {
				if !contains(rule.ResourceNames, name) {
					t.Fatalf("pod get missing named pod %q: %+v", name, rule.ResourceNames)
				}
			}
		case contains(rule.Resources, "pods") && contains(rule.Verbs, "list"):
			// The authenticator's exactly-one-live-control scan needs a
			// namespace list; RBAC cannot name-scope it. The run namespace
			// is dedicated, so it only ever sees this run's pods.
			if len(rule.ResourceNames) != 0 {
				t.Fatalf("pod list carries no resourceNames: %+v", rule)
			}
		default:
			t.Fatalf("unexpected rule resource: %+v", rule.Resources)
		}
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func TestServiceAccountsNeverAutomount(t *testing.T) {
	run := testRun("sas")
	for _, sa := range ServiceAccounts(run) {
		if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
			t.Fatalf("service account %s automounts a token", sa.Name)
		}
	}
}

func TestWorkerNetworkPolicyIsolation(t *testing.T) {
	run := testRun("netpol")
	policies := NetworkPolicies(run, nil, nil)
	byComponent := map[string]*networkingv1.NetworkPolicy{}
	for _, p := range policies {
		byComponent[p.Labels["courier.misospace.dev/component"]] = p
	}
	worker := byComponent[ComponentWorker]
	if worker == nil {
		t.Fatal("worker policy missing")
	}
	if len(worker.Spec.Ingress) != 1 || len(worker.Spec.Egress) != 0 {
		t.Fatalf("without a cache pin the worker must allow only control ingress and no egress: %+v", worker.Spec)
	}

	policies = NetworkPolicies(run, &CachePin{IP: "10.96.0.7", Port: 5120}, nil)
	for _, p := range policies {
		if p.Labels["courier.misospace.dev/component"] == ComponentWorker {
			worker = p
		}
	}
	if len(worker.Spec.Egress) != 1 {
		t.Fatalf("worker egress rules = %+v", worker.Spec.Egress)
	}
	if worker.Spec.Egress[0].To[0].IPBlock.CIDR != "10.96.0.7/32" {
		t.Fatalf("cache egress = %+v", worker.Spec.Egress[0].To)
	}

	// IPv6 cache pins use /128.
	policies = NetworkPolicies(run, &CachePin{IP: "fd00::7", Port: 5120}, nil)
	for _, p := range policies {
		if p.Labels["courier.misospace.dev/component"] == ComponentWorker {
			if p.Spec.Egress[0].To[0].IPBlock.CIDR != "fd00::7/128" {
				t.Fatalf("IPv6 cache egress = %+v", p.Spec.Egress[0].To)
			}
		}
	}
}

// The control pod serves nothing and only dials out, so its ingress exists
// to let the run's own pods reach it: the reply to a control-originated
// connection lands on the control pod's ephemeral port and is judged as fresh
// ingress, which a deny-all ingress drops on datapaths that do not treat it
// as an established reply (#711). Nothing outside the run can reach control.
func TestControlNetworkPolicyAdmitsTheRunsOwnPods(t *testing.T) {
	run := testRun("netpol-control")
	control := controlPolicy(t, run)
	if len(control.Spec.Ingress) != 1 {
		t.Fatalf("control ingress rules = %+v", control.Spec.Ingress)
	}
	rule := control.Spec.Ingress[0]
	if len(rule.From) != 1 {
		t.Fatalf("control ingress peers = %+v", rule.From)
	}
	peer := rule.From[0]
	if peer.PodSelector == nil {
		t.Fatalf("control ingress peer must select pods: %+v", peer)
	}
	if peer.NamespaceSelector != nil {
		t.Fatalf("control ingress peer must stay namespace-local: %+v", peer)
	}
	if got, want := peer.PodSelector.MatchLabels[LabelRun], RunLabelValue(run.Name); got != want {
		t.Fatalf("control ingress run label = %q; want %q", got, want)
	}
	if component, named := peer.PodSelector.MatchLabels["courier.misospace.dev/component"]; named {
		t.Fatalf("control ingress must admit the whole run, not component %q: %+v", component, peer.PodSelector.MatchLabels)
	}
	// The reply lands on the control pod's ephemeral port, so the rule may
	// not restrict ports.
	if len(rule.Ports) != 0 {
		t.Fatalf("control ingress ports = %+v", rule.Ports)
	}
	// Another run's pods carry a different run label, so they stay out.
	other := controlPolicy(t, testRun("other-run"))
	if peer.PodSelector.MatchLabels[LabelRun] == other.Spec.Ingress[0].From[0].PodSelector.MatchLabels[LabelRun] {
		t.Fatal("control ingress admits another run's pods")
	}
	// The egress the control depends on is unchanged: its broker, its
	// worker, and cluster DNS, each named within this run.
	egressPeers := map[string]bool{}
	for _, egress := range control.Spec.Egress {
		for _, to := range egress.To {
			if to.PodSelector == nil {
				continue
			}
			if to.PodSelector.MatchLabels["k8s-app"] == "kube-dns" {
				egressPeers["kube-dns"] = true
				continue
			}
			if got, want := to.PodSelector.MatchLabels[LabelRun], RunLabelValue(run.Name); got != want {
				t.Fatalf("control egress peer run label = %q; want %q", got, want)
			}
			egressPeers[to.PodSelector.MatchLabels["courier.misospace.dev/component"]] = true
		}
	}
	for _, component := range []string{ComponentBroker, ComponentWorker, "kube-dns"} {
		if !egressPeers[component] {
			t.Fatalf("control egress must reach the %s: %+v", component, control.Spec.Egress)
		}
	}
}

// controlPolicy returns the control pod's rendered NetworkPolicy.
func controlPolicy(t *testing.T, run *courier.CoderRun) *networkingv1.NetworkPolicy {
	t.Helper()
	for _, p := range NetworkPolicies(run, nil, nil) {
		if p.Labels["courier.misospace.dev/component"] == ComponentCoordinator {
			return p
		}
	}
	t.Fatal("control policy missing")
	return nil
}

func TestBrokerTLSIdentity(t *testing.T) {
	tls, err := GenerateBrokerTLS([]string{"r-broker.ns.svc.cluster.local", "r-broker.ns.svc", "r-broker"})
	if err != nil {
		t.Fatal(err)
	}
	caBlock, _ := pem.Decode(tls.CACertPEM)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !caCert.IsCA {
		t.Fatal("CA certificate is not a CA")
	}
	serverBlock, _ := pem.Decode(tls.ServerCertPEM)
	serverCert, err := x509.ParseCertificate(serverBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := serverCert.VerifyHostname("r-broker.ns.svc.cluster.local"); err != nil {
		t.Fatalf("server certificate hostname: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	if _, err := serverCert.Verify(x509.VerifyOptions{DNSName: "r-broker.ns.svc", Roots: roots}); err != nil {
		t.Fatalf("server certificate does not chain to the run CA: %v", err)
	}
	keyBlock, _ := pem.Decode(tls.ServerKeyPEM)
	if _, err := x509.ParseECPrivateKey(keyBlock.Bytes); err != nil {
		t.Fatalf("server key: %v", err)
	}
}

func TestResourceNamesStayUniqueAndStable(t *testing.T) {
	long := "this-run-name-is-far-too-long-for-kubernetes-object-names-" +
		"because-it-keeps-going-and-going-and-going-past-the-limit-abcde"
	first := ControlPodName(long)
	second := ControlPodName(long)
	if first != second {
		t.Fatal("derived names must be stable")
	}
	if len(first) > 253 {
		t.Fatalf("derived name exceeds 253 characters: %d", len(first))
	}
	if ControlPodName("short") != "short-control" {
		t.Fatalf("short name = %q", ControlPodName("short"))
	}
}

// TestProbePodNamesAreUniquePerNode pins the probe name's two jobs at once:
// two nodes whose names differ past the prefix the tag can keep, or that
// normalize to the same string, must never render the same probe pod — or one
// node's probe is created, collected, and deleted for the other. The name must
// also stay deterministic and DNS-valid, whatever the node looks like.
func TestProbePodNamesAreUniquePerNode(t *testing.T) {
	nodes := []string{
		// Share the prefix the tag can afford to keep.
		"worker-production-001",
		"worker-production-002",
		// Normalize to one string: the dot and the underscore both drop.
		"worker-prod.001",
		"worker-prod_001",
		"9lives",
		strings.Repeat("a", 253),
	}
	// A run name at the object-name limit leaves the probe suffix the same
	// budget it has for a short run.
	for _, runName := range []string{"probe-names", strings.Repeat("z", 253)} {
		run := testRun(runName)
		seen := map[string]string{}
		for _, node := range nodes {
			name := ProbePod(run, node, "busybox:test", "true").Name
			if again := ProbePod(run, node, "busybox:test", "true").Name; again != name {
				t.Errorf("probe pod name for node %q is not deterministic: %q then %q", node, name, again)
			}
			if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
				t.Errorf("probe pod name for node %q = %q: %v", node, name, errs)
			}
			if label, _, _ := strings.Cut(name, "."); len(label) > 63 {
				t.Errorf("probe pod name for node %q has a %d-character first label: %q", node, len(label), name)
			}
			if len(name) > 253 {
				t.Errorf("probe pod name for node %q is %d characters: %q", node, len(name), name)
			}
			if other, clash := seen[name]; clash {
				t.Errorf("nodes %q and %q render the same probe pod name %q", other, node, name)
			}
			seen[name] = node
		}
	}
	// Names that normalize away to nothing leave the digest as the whole tag.
	for _, node := range append([]string{"", "-", "---", strings.Repeat("A", 40)}, nodes...) {
		tag := nodeNameTag(node)
		if tag == "" {
			t.Errorf("node %q renders an empty probe tag", node)
		}
		if len(tag) > 16 {
			t.Errorf("probe tag for node %q is %d characters: %q", node, len(tag), tag)
		}
		if strings.HasPrefix(tag, "-") || strings.HasSuffix(tag, "-") {
			t.Errorf("probe tag for node %q starts or ends with a hyphen: %q", node, tag)
		}
		if errs := validation.IsDNS1123Label(tag); len(errs) > 0 {
			t.Errorf("probe tag for node %q = %q: %v", node, tag, errs)
		}
	}
}

// TestPodNamesAreValidDNSLabels pins the pod half of the naming bound: a
// pod's hostname is the first label of its name truncated to 63 characters,
// and in-cluster consumers read that hostname, so the first label of every
// rendered pod name is held to the DNS-label bound and the hostname the
// cluster reports matches the object name — for any supported CoderRun name.
func TestPodNamesAreValidDNSLabels(t *testing.T) {
	names := []string{
		"short",
		"run.test",
		"9lives",
		strings.Repeat("a", 60),
		strings.Repeat("a", 63) + "x",
		strings.Repeat("a", 63) + "y",
		strings.Repeat("z", 253),
		strings.Repeat("a", 100) + ".test",
		strings.Repeat("a.b", 84) + "c",
	}
	for _, name := range names {
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
			t.Fatalf("test input %q is not a supported CoderRun name: %v", name, errs)
		}
		for _, got := range []struct {
			kind string
			fn   func(string) string
		}{{"control", ControlPodName}, {"broker", BrokerPodName}, {"worker", WorkerPodName}} {
			pod := got.fn(name)
			if errs := validation.IsDNS1123Subdomain(pod); len(errs) > 0 {
				t.Errorf("%s pod name for run %q = %q: %v", got.kind, name, pod, errs)
			}
			label, _, _ := strings.Cut(pod, ".")
			if errs := validation.IsDNS1123Label(label); len(errs) > 0 {
				t.Errorf("%s pod name for run %q has first label %q: %v", got.kind, name, label, errs)
			}
			if len(label) > 63 {
				t.Errorf("%s pod name for run %q has a %d-character first label: %q", got.kind, name, len(label), pod)
			}
			if again := got.fn(name); again != pod {
				t.Errorf("%s pod name for run %q is not stable: %q then %q", got.kind, name, pod, again)
			}
		}
	}
	if ControlPodName("short") != "short-control" {
		t.Fatalf("valid short names must pass through unchanged: %q", ControlPodName("short"))
	}
	if ControlPodName(strings.Repeat("a", 63)+"x") == ControlPodName(strings.Repeat("a", 63)+"y") {
		t.Fatal("run names differing past the label bound collide")
	}
	for _, name := range []string{"short", "run.test", strings.Repeat("z", 253)} {
		pods := []string{ControlPodName(name), BrokerPodName(name), WorkerPodName(name)}
		if pods[0] == pods[1] || pods[1] == pods[2] || pods[0] == pods[2] {
			t.Fatalf("run %q pod names collide: %q", name, pods)
		}
	}
	run := testRun(strings.Repeat("z", 253))
	probe := ProbePod(run, "node-a", "busybox:test", "true").Name
	if label, _, _ := strings.Cut(probe, "."); len(label) > 63 {
		t.Errorf("probe pod name for a 253-character run has a %d-character first label: %q", len(label), probe)
	}
}

func TestServiceNamesAreValidDNSLabels(t *testing.T) {
	names := []string{
		"short",
		"run.test",
		"9lives",
		"12345",
		"9.lives.and.bones",
		strings.Repeat("a", 60),
		strings.Repeat("a", 59) + "x",
		strings.Repeat("a", 59) + "y",
		strings.Repeat("z", 253),
		strings.Repeat("a.b", 84) + "c",
		// Dotted names whose normalized prefix outgrows the label budget with
		// the truncation cut landing immediately after a dot: normalization
		// replaces dots with hyphens, so the cut must trim that trailing
		// hyphen rather than join the digest right after it. "-broker" and
		// "-worker" both leave a 47-character budget for a letter-leading
		// name and 46 after the digit-leading letter-prefix reserve, and
		// both of these names are 48 characters normalized, so the cut lands
		// immediately after a former dot in each.
		strings.Repeat("a.b", 16),
		strings.Repeat("1.", 23) + "12",
	}
	for _, name := range names {
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
			t.Fatalf("test input %q is not a supported CoderRun name: %v", name, errs)
		}
		for _, got := range []struct {
			kind string
			fn   func(string) string
		}{{"broker", BrokerServiceName}, {"worker", WorkerServiceName}} {
			svc := got.fn(name)
			if errs := validation.IsDNS1035Label(svc); len(errs) > 0 {
				t.Errorf("%s Service name for run %q = %q: %v", got.kind, name, svc, errs)
			}
			if again := got.fn(name); again != svc {
				t.Errorf("%s Service name for run %q is not stable: %q then %q", got.kind, name, svc, again)
			}
		}
	}
	if BrokerServiceName("short") != "short-broker" || WorkerServiceName("short") != "short-worker" {
		t.Fatalf("valid short names must pass through unchanged: %q %q",
			BrokerServiceName("short"), WorkerServiceName("short"))
	}
	if BrokerServiceName("run.test") == BrokerServiceName("run-test") {
		t.Fatal("dotted run name must not collide with its hyphenated form")
	}
	if BrokerServiceName("9lives") == WorkerServiceName("lives9") {
		t.Fatal("distinct run names must not collide")
	}
	sharedPrefixA := BrokerServiceName(strings.Repeat("a", 59) + "x")
	sharedPrefixB := BrokerServiceName(strings.Repeat("a", 59) + "y")
	if sharedPrefixA == sharedPrefixB {
		t.Fatalf("run names differing past the truncation point collide: %q", sharedPrefixA)
	}
	for _, name := range []string{"short", "run.test", "12345", strings.Repeat("a", 60)} {
		if BrokerServiceName(name) == WorkerServiceName(name) {
			t.Fatalf("run %q broker and worker Services collide: %q", name, BrokerServiceName(name))
		}
	}
	if BrokerServiceDNS("ns", "run.test") != BrokerServiceName("run.test")+".ns.svc.cluster.local" {
		t.Fatal("broker DNS must derive from the canonical Service name")
	}
	if WorkerServiceDNS("ns", "9lives") != WorkerServiceName("9lives")+".ns.svc.cluster.local" {
		t.Fatal("worker DNS must derive from the canonical Service name")
	}
}

func TestRenderedServicesCarryValidNames(t *testing.T) {
	for _, name := range []string{"short", "run.test", "9lives", strings.Repeat("a", 60), strings.Repeat("z", 253)} {
		run := testRun(name)
		broker := BrokerService(run)
		worker := WorkerService(run)
		if errs := validation.IsDNS1035Label(broker.Name); len(errs) > 0 {
			t.Errorf("rendered broker Service for run %q has invalid name %q: %v", name, broker.Name, errs)
		}
		if errs := validation.IsDNS1035Label(worker.Name); len(errs) > 0 {
			t.Errorf("rendered worker Service for run %q has invalid name %q: %v", name, worker.Name, errs)
		}
	}
}

func TestRunLabelValueStaysValidBoundedAndDistinct(t *testing.T) {
	names := []string{
		"short",
		"run.test",
		strings.Repeat("a", 60),
		// 64+ characters: valid as a DNS subdomain but over the 63-character
		// label-value cap.
		strings.Repeat("a", 63) + "x",
		strings.Repeat("a", 63) + "y",
		strings.Repeat("z", 253),
	}
	seen := map[string]string{}
	for _, name := range names {
		value := RunLabelValue(name)
		if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
			t.Errorf("run label value for run %q = %q: %v", name, value, errs)
		}
		if again := RunLabelValue(name); again != value {
			t.Errorf("run label value for run %q is not stable: %q then %q", name, value, again)
		}
		if other, clash := seen[value]; clash {
			t.Errorf("run label value %q collides between runs %q and %q", value, other, name)
		}
		seen[value] = name
	}
	if RunLabelValue("short") != "short" {
		t.Fatalf("valid short names must pass through unchanged: %q", RunLabelValue("short"))
	}
}

func TestRenderedSelectorsAgreeWithLabels(t *testing.T) {
	components := map[string]bool{ComponentCoordinator: true, ComponentBroker: true, ComponentWorker: true}
	for _, name := range []string{"short", "run.test", strings.Repeat("a", 63) + "x", strings.Repeat("z", 253)} {
		run := testRun(name)
		runLabel := Labels(run.Name, "")[LabelRun]
		if len(runLabel) > 63 {
			t.Fatalf("run %q renders a %d-character label value", name, len(runLabel))
		}
		check := func(kind string, got map[string]string) {
			t.Helper()
			if got["k8s-app"] == "kube-dns" {
				return // the DNS egress rule deliberately matches no run pod
			}
			if got[LabelRun] != runLabel {
				t.Errorf("run %q: %s selects run label %q; Labels writes %q", name, kind, got[LabelRun], runLabel)
			}
			if !components[got["courier.misospace.dev/component"]] {
				t.Errorf("run %q: %s selects unknown component %q", name, kind, got["courier.misospace.dev/component"])
			}
		}
		check("broker service selector", BrokerService(run).Spec.Selector)
		check("worker service selector", WorkerService(run).Spec.Selector)
		for _, policy := range NetworkPolicies(run, nil, nil) {
			check(policy.Name+" pod selector", policy.Spec.PodSelector.MatchLabels)
			for _, ingress := range policy.Spec.Ingress {
				for _, from := range ingress.From {
					if from.PodSelector == nil {
						continue
					}
					if policy.Labels["courier.misospace.dev/component"] == ComponentCoordinator {
						// The control's ingress admits the whole run, so
						// that peer names the run and no component. Every
						// other selector must name a component, or a
						// dropped component label would admit a pod the
						// policy never meant to admit.
						if got := from.PodSelector.MatchLabels; len(got) != 1 || got[LabelRun] != runLabel {
							t.Errorf("run %q: %s ingress peer must select the run's own pods: %+v", name, policy.Name, got)
						}
						continue
					}
					check(policy.Name+" ingress selector", from.PodSelector.MatchLabels)
				}
			}
			for _, egress := range policy.Spec.Egress {
				for _, to := range egress.To {
					if to.PodSelector != nil {
						check(policy.Name+" egress selector", to.PodSelector.MatchLabels)
					}
				}
			}
		}
	}
}

// skillMountFixture is one materialized skill bundle: the ConfigMap carrying
// its content and the key-to-path items its projection carries. The keys are
// the encoded ones the operator writes, and the paths are the bundle's own
// relative paths, so the mounted projection carries the bundle's layout.
func skillMountFixture(name, source string) SkillMount {
	return SkillMount{
		Name:          name,
		ConfigMapName: SkillContentConfigMapName("run", name),
		Source:        source,
		Items: []corev1.KeyToPath{
			{Key: "f0", Path: "SKILL.md"},
			{Key: "f1", Path: "assets/checklist.md"},
		},
	}
}

// skillConfigMapVolumeOf returns the pod volume projecting the given
// ConfigMap.
func skillConfigMapVolumeOf(pod *corev1.Pod, configMapName string) *corev1.Volume {
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].ConfigMap != nil && pod.Spec.Volumes[i].ConfigMap.Name == configMapName {
			return &pod.Spec.Volumes[i]
		}
	}
	return nil
}

// mountOf returns the container mount of one volume.
func mountOf(pod *corev1.Pod, volumeName string) (corev1.VolumeMount, bool) {
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == volumeName {
			return mount, true
		}
	}
	return corev1.VolumeMount{}, false
}

// TestControlPodRendersPinnedSkillMounts pins the control pod's half of the
// materialization contract: a pinned bundle is projected from its per-run
// content ConfigMap with the key-to-path items the operator wrote, and
// mounted read-only at the source path the deployment registered — so the pod
// reads the bundle exactly where the registration says it is, and never
// writes to it.
func TestControlPodRendersPinnedSkillMounts(t *testing.T) {
	run := testRun("control-skills")
	mount := skillMountFixture("repo-tour", "/opt/courier/skills/repo-tour")
	pod, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa-uid",
		WorkerPodUID:          "worker-uid",
		ControlIncarnationUID: "incarnation-1",
		WorkerURL:             "http://r-worker.ns.svc.cluster.local:8080",
		BrokerURL:             "https://r-broker.ns.svc.cluster.local:8443",
		Skills:                []SkillMount{mount},
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := skillConfigMapVolumeOf(pod, mount.ConfigMapName)
	if volume == nil {
		t.Fatalf("control pod carries no volume projecting %q", mount.ConfigMapName)
	}
	if volume.Secret != nil || volume.Projected != nil {
		t.Fatalf("control pod skill volume = %+v, want a ConfigMap projection", volume.VolumeSource)
	}
	if !reflect.DeepEqual(volume.ConfigMap.Items, mount.Items) {
		t.Fatalf("control pod skill volume items = %+v, want %+v", volume.ConfigMap.Items, mount.Items)
	}
	skillMount, ok := mountOf(pod, volume.Name)
	if !ok {
		t.Fatalf("control pod mounts no volume %q", volume.Name)
	}
	if skillMount.MountPath != mount.Source {
		t.Fatalf("control pod skill mount path = %q, want the registered source %q", skillMount.MountPath, mount.Source)
	}
	if !skillMount.ReadOnly {
		t.Fatal("control pod skill mount must be read-only")
	}
}

// TestWorkerPodRendersPinnedSkillMounts pins the worker pod's half: the
// bundle content is mounted read-only from a ConfigMap at the same registered
// source path, so a skill's supporting assets are readable where the worker's
// shell runs.
func TestWorkerPodRendersPinnedSkillMounts(t *testing.T) {
	run := testRun("worker-skills")
	pub, _, _ := protocol.GenerateKey()
	mount := skillMountFixture("repo-tour", "/opt/courier/skills/repo-tour")
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), []SkillMount{mount})
	if err != nil {
		t.Fatal(err)
	}
	volume := skillConfigMapVolumeOf(pod, mount.ConfigMapName)
	if volume == nil {
		t.Fatalf("worker pod carries no volume projecting %q", mount.ConfigMapName)
	}
	if volume.Secret != nil || volume.Projected != nil {
		t.Fatalf("worker pod skill volume = %+v, want a ConfigMap projection", volume.VolumeSource)
	}
	if !reflect.DeepEqual(volume.ConfigMap.Items, mount.Items) {
		t.Fatalf("worker pod skill volume items = %+v, want %+v", volume.ConfigMap.Items, mount.Items)
	}
	skillMount, ok := mountOf(pod, volume.Name)
	if !ok {
		t.Fatalf("worker pod mounts no volume %q", volume.Name)
	}
	if skillMount.MountPath != mount.Source {
		t.Fatalf("worker pod skill mount path = %q, want the registered source %q", skillMount.MountPath, mount.Source)
	}
	if !skillMount.ReadOnly {
		t.Fatal("worker pod skill mount must be read-only")
	}
}

// TestWorkerPodWithPinnedSkillMountsNoIdentityMaterial pins the worker's
// identity posture against a PINNED run: the bundle content is deployment
// approved skill material, not credential material, so the only volume it
// adds is a read-only ConfigMap. The worker still mounts no Secret, no
// projected volume, and no service account token — the invariant the
// skillless worker holds, unchanged by a pin.
func TestWorkerPodWithPinnedSkillMountsNoIdentityMaterial(t *testing.T) {
	run := testRun("worker-pinned-clean")
	pub, _, _ := protocol.GenerateKey()
	mount := skillMountFixture("repo-tour", "/opt/courier/skills/repo-tour")
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), []SkillMount{mount})
	if err != nil {
		t.Fatal(err)
	}
	if vols := secretVolumes(pod); len(vols) != 0 {
		t.Fatalf("a pinned worker must mount no secret, token, or projected volume: %v", vols)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("a pinned worker must not automount a service account token")
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			t.Fatalf("a pinned worker's env %q references a Secret", env.Name)
		}
	}
	var configMaps []string
	for _, volume := range pod.Spec.Volumes {
		switch {
		case volume.ConfigMap != nil:
			configMaps = append(configMaps, volume.ConfigMap.Name)
		case volume.EmptyDir == nil:
			t.Fatalf("a pinned worker volume %q must be an emptyDir or the skill ConfigMap", volume.Name)
		}
	}
	if len(configMaps) != 1 || configMaps[0] != mount.ConfigMapName {
		t.Fatalf("a pinned worker carries ConfigMap volumes %v, want only %q", configMaps, mount.ConfigMapName)
	}
	skillMount, ok := mountOf(pod, skillVolumeName(mount.Name))
	if !ok {
		t.Fatalf("a pinned worker mounts no volume %q", skillVolumeName(mount.Name))
	}
	if !skillMount.ReadOnly {
		t.Fatal("a pinned worker's skill mount must be read-only")
	}
}

// TestSkillContentConfigMapCarriesExactBytes pins the materialization
// contract of the content object: the bundle's bytes ride the ConfigMap's
// binary data, exactly — including a file that is not valid UTF-8, which the
// string form could not carry at all.
func TestSkillContentConfigMapCarriesExactBytes(t *testing.T) {
	run := testRun("skill-content-bytes")
	content := map[string][]byte{
		"f0": []byte("tour the repository first."),
		"f1": {0xff, 0xfe, 0x00, 0x80},
	}
	configMap := SkillContentConfigMap(run, "repo-tour", content)
	if configMap.Name != SkillContentConfigMapName(run.Name, "repo-tour") {
		t.Fatalf("content ConfigMap name = %q, want %q", configMap.Name, SkillContentConfigMapName(run.Name, "repo-tour"))
	}
	if len(configMap.Data) != 0 {
		t.Fatalf("content ConfigMap carries string data %v, want the bytes in binary data only", configMap.Data)
	}
	for key, want := range content {
		got, ok := configMap.BinaryData[key]
		if !ok {
			t.Fatalf("content ConfigMap carries no binary key %q", key)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("content ConfigMap key %q = %v, want %v", key, got, want)
		}
	}
}

// TestSkillBundlesConfigMapCarriesTheDocument pins the config object: the
// rendered document rides the ConfigMap's binary data under the documented
// key, so the control pod's mount carries exactly those bytes.
func TestSkillBundlesConfigMapCarriesTheDocument(t *testing.T) {
	run := testRun("skill-config-document")
	document := []byte("bundles:\n- name: repo-tour\n")
	configMap := SkillBundlesConfigMap(run, document)
	if configMap.Name != SkillBundlesConfigMapName(run.Name) {
		t.Fatalf("config ConfigMap name = %q, want %q", configMap.Name, SkillBundlesConfigMapName(run.Name))
	}
	if got := configMap.BinaryData[SkillBundlesConfigKey]; !bytes.Equal(got, document) {
		t.Fatalf("config ConfigMap document = %q, want %q", got, document)
	}
}

// TestControlPodRendersSkillConfigAtTheDocumentedPath pins where the bundle
// config lands: the control pod mounts the per-run config ConfigMap at the
// directory that places the file exactly at executor.SkillBundlesConfigPath,
// which is the path the pod's own loader reads. A mount one directory off
// would leave the loader without the config it fails closed on.
func TestControlPodRendersSkillConfigAtTheDocumentedPath(t *testing.T) {
	run := testRun("control-skill-config")
	pod, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa-uid",
		WorkerPodUID:          "worker-uid",
		ControlIncarnationUID: "incarnation-1",
		WorkerURL:             "http://r-worker.ns.svc.cluster.local:8080",
		BrokerURL:             "https://r-broker.ns.svc.cluster.local:8443",
		SkillBundlesConfigMap: SkillBundlesConfigMapName(run.Name),
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := skillConfigMapVolumeOf(pod, SkillBundlesConfigMapName(run.Name))
	if volume == nil {
		t.Fatalf("control pod carries no volume projecting the config ConfigMap %q", SkillBundlesConfigMapName(run.Name))
	}
	configMount, ok := mountOf(pod, volume.Name)
	if !ok {
		t.Fatalf("control pod mounts no config volume %q", volume.Name)
	}
	if configMount.MountPath != filepath.Dir(executor.SkillBundlesConfigPath) {
		t.Fatalf("config mount path = %q, want %q", configMount.MountPath, filepath.Dir(executor.SkillBundlesConfigPath))
	}
	if !configMount.ReadOnly {
		t.Fatal("config mount must be read-only")
	}
	if len(volume.ConfigMap.Items) != 1 || volume.ConfigMap.Items[0].Path != SkillBundlesConfigKey {
		t.Fatalf("config volume items = %+v, want the single %q file", volume.ConfigMap.Items, SkillBundlesConfigKey)
	}
	if got := filepath.Join(configMount.MountPath, SkillBundlesConfigKey); got != executor.SkillBundlesConfigPath {
		t.Fatalf("the config file lands at %q, want the documented %q", got, executor.SkillBundlesConfigPath)
	}
}

// TestSkilllessRunRendersNoSkillMaterial pins the absence contract: a run
// that pins no skill renders no skill volume, no skill mount, and no config
// mount, so a skillless pod never carries an empty bundle projection.
func TestSkilllessRunRendersNoSkillMaterial(t *testing.T) {
	run := testRun("skillless")
	pub, _, _ := protocol.GenerateKey()
	worker, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), nil)
	if err != nil {
		t.Fatal(err)
	}
	control, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa-uid",
		WorkerPodUID:          "worker-uid",
		ControlIncarnationUID: "incarnation-1",
		WorkerURL:             "http://r-worker.ns.svc.cluster.local:8080",
		BrokerURL:             "https://r-broker.ns.svc.cluster.local:8443",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range []*corev1.Pod{worker, control} {
		for _, volume := range pod.Spec.Volumes {
			if strings.HasPrefix(volume.Name, "skill-") {
				t.Fatalf("%s pod renders skill volume %q for a skillless run", pod.Name, volume.Name)
			}
		}
		for _, mount := range pod.Spec.Containers[0].VolumeMounts {
			if strings.HasPrefix(mount.Name, "skill-") {
				t.Fatalf("%s pod renders skill mount %q for a skillless run", pod.Name, mount.Name)
			}
			if mount.MountPath == filepath.Dir(executor.SkillBundlesConfigPath) {
				t.Fatalf("%s pod renders a bundle config mount for a skillless run", pod.Name)
			}
		}
	}
}

// TestWorkerPodGainsOnlyAReadOnlySkillMount pins the worker's posture against
// a pinned run: the skill mount is the only difference from a skillless
// worker. The volume it adds is a read-only ConfigMap — no Secret volume, no
// projected volume, no environment value, no service account, and no writable
// path comes with it — so the worker may read the bundle content and nothing
// else changes about what it holds.
func TestWorkerPodGainsOnlyAReadOnlySkillMount(t *testing.T) {
	run := testRun("worker-skill-posture")
	pub, _, _ := protocol.GenerateKey()
	base, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mount := skillMountFixture("repo-tour", "/opt/courier/skills/repo-tour")
	pinned, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), []SkillMount{mount})
	if err != nil {
		t.Fatal(err)
	}
	if len(pinned.Spec.Volumes) != len(base.Spec.Volumes)+1 {
		t.Fatalf("pinned worker carries %d volumes, want one more than the skillless worker's %d", len(pinned.Spec.Volumes), len(base.Spec.Volumes))
	}
	if len(pinned.Spec.Containers[0].VolumeMounts) != len(base.Spec.Containers[0].VolumeMounts)+1 {
		t.Fatalf("pinned worker carries %d mounts, want one more than the skillless worker's %d", len(pinned.Spec.Containers[0].VolumeMounts), len(base.Spec.Containers[0].VolumeMounts))
	}
	if !reflect.DeepEqual(pinned.Spec.Containers[0].Env, base.Spec.Containers[0].Env) {
		t.Fatalf("pinned worker env = %+v, want the skillless worker's %+v", pinned.Spec.Containers[0].Env, base.Spec.Containers[0].Env)
	}
	if pinned.Spec.ServiceAccountName != base.Spec.ServiceAccountName {
		t.Fatalf("pinned worker service account = %q, want %q", pinned.Spec.ServiceAccountName, base.Spec.ServiceAccountName)
	}
	if !reflect.DeepEqual(pinned.Spec.AutomountServiceAccountToken, base.Spec.AutomountServiceAccountToken) {
		t.Fatal("pinned worker automount changed")
	}
	baseMounts := map[string]corev1.VolumeMount{}
	for _, m := range base.Spec.Containers[0].VolumeMounts {
		baseMounts[m.Name+"\x00"+m.MountPath] = m
	}
	for _, m := range pinned.Spec.Containers[0].VolumeMounts {
		previous, ok := baseMounts[m.Name+"\x00"+m.MountPath]
		if !ok {
			if !m.ReadOnly {
				t.Fatalf("pinned worker gained a writable mount %q at %q", m.Name, m.MountPath)
			}
			continue
		}
		if !reflect.DeepEqual(m, previous) {
			t.Fatalf("pinned worker changed mount %q: %+v, want %+v", m.Name, m, previous)
		}
	}
	// The one volume the pin adds projects the bundle's ConfigMap.
	volume := skillConfigMapVolumeOf(pinned, mount.ConfigMapName)
	if volume == nil {
		t.Fatalf("a pinned worker carries no volume projecting %q", mount.ConfigMapName)
	}
	if volume.Secret != nil || volume.Projected != nil {
		t.Fatalf("the gained worker volume = %+v, want a ConfigMap projection", volume.VolumeSource)
	}
}

// TestSkillVolumeNamesAreUniqueAndLabelSafe pins that two pinned bundles
// never share a pod volume — their contents would shadow each other — and
// that the derived name is always a DNS label.
func TestSkillVolumeNamesAreUniqueAndLabelSafe(t *testing.T) {
	names := []string{"repo-tour", "simple-english", "a", strings.Repeat("a", 63)}
	seen := map[string]string{}
	for _, name := range names {
		volume := skillVolumeName(name)
		if errs := validation.IsDNS1123Label(volume); len(errs) > 0 {
			t.Errorf("skill volume name for %q = %q: %v", name, volume, errs)
		}
		if other, clash := seen[volume]; clash {
			t.Errorf("skills %q and %q render the same volume name %q", other, name, volume)
		}
		seen[volume] = name
		if again := skillVolumeName(name); again != volume {
			t.Errorf("skill volume name for %q is not stable: %q then %q", name, volume, again)
		}
	}
}

// The worker's workspace emptyDir mountpoint is root-owned while the pod runs
// as the runtime uid, so git's ownership gate refuses the repository every
// task runs in — the fixed unpack task answered "detected dubious ownership
// in repository at /workspace" with exit 128, twice into the terminal
// blocked-external ending. The scoped safe.directory exception rides the pod
// environment every task inherits: exactly the workspace path, never a
// wildcard (#779's scope rule, worker side).
func TestWorkerPodCarriesScopedWorkspaceSafeDirectory(t *testing.T) {
	run := testRun("worker-safe-directory")
	pub, _, _ := protocol.GenerateKey()
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), nil)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	var key, value string
	for _, e := range pod.Spec.Containers[0].Env {
		switch e.Name {
		case "GIT_CONFIG_COUNT":
			count++
			if e.Value != "1" {
				t.Fatalf("GIT_CONFIG_COUNT = %q; want exactly one scoped config entry", e.Value)
			}
		case "GIT_CONFIG_KEY_0":
			key = e.Value
		case "GIT_CONFIG_VALUE_0":
			value = e.Value
		}
	}
	if count != 1 {
		t.Fatalf("GIT_CONFIG_COUNT appears %d times; want exactly one", count)
	}
	if key != "safe.directory" || value != "/workspace" {
		t.Fatalf("GIT_CONFIG_KEY_0=%q GIT_CONFIG_VALUE_0=%q; want safe.directory scoped to the workspace path", key, value)
	}
}
