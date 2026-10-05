package topology

import (
	"testing"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func controlPodTestRun() *courier.CoderRun {
	return &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "runs", UID: "uid-run"},
		Spec: courier.CoderRunSpec{
			Mode: courier.ModeResolveIssue, Source: "manual", WorkItemID: "i",
			Repo: "acme/widgets", Ref: 7, Lane: "local",
		},
	}
}

func controlEnv(pod *corev1.Pod) map[string]string {
	values := map[string]string{}
	for _, env := range pod.Spec.Containers[0].Env {
		values[env.Name] = env.Value
	}
	return values
}

// The control pod is the only model-client surface: it must carry the run
// context and the gateway address, and the gateway key must arrive as a
// mounted Secret file, never as an env value.
func TestControlPodCarriesRunContextAndGateway(t *testing.T) {
	run := controlPodTestRun()
	pod, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		Run:                   executor.Invocation{Goal: "the goal", Model: "m", Roles: map[string]string{"coordinator": "m"}, Workspace: ControlWorkspacePath},
		GatewayURL:            "http://gateway:4000/v1",
		GatewayKeyMounted:     true,
		ControlSAUID:          "sa",
		WorkerPodUID:          "w",
		ControlIncarnationUID: "c",
		WorkerURL:             "http://worker:8080",
		BrokerURL:             "https://broker:8443",
		BrokerStatusURL:       "https://broker:8444",
	})
	if err != nil {
		t.Fatal(err)
	}
	env := controlEnv(pod)
	if env[EnvGatewayURL] != "http://gateway:4000/v1" {
		t.Fatalf("gateway URL env = %q", env[EnvGatewayURL])
	}
	if env[EnvGatewayKeyFile] == "" {
		t.Fatal("gateway key file env is missing")
	}
	if env["COURIER_GOAL"] != "the goal" {
		t.Fatalf("run context goal env = %q", env["COURIER_GOAL"])
	}
	mounted := false
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == "gateway-key" {
			mounted = true
			if !mount.ReadOnly {
				t.Fatal("the gateway key mount must be read-only")
			}
		}
	}
	if !mounted {
		t.Fatal("gateway key Secret is not mounted")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "gateway-key" && (volume.Secret == nil || volume.Secret.SecretName != GatewaySecretName(run.Name)) {
			t.Fatalf("gateway key volume = %+v", volume)
		}
	}
	// No forge or git credential may appear in control env.
	for _, forbidden := range []string{EnvForgeAPIToken, EnvGitToken, EnvGitUsername} {
		if _, ok := env[forbidden]; ok {
			t.Fatalf("control pod carries credential env %s", forbidden)
		}
	}
}

func TestControlPodWithoutGatewayOmitsGatewayMaterial(t *testing.T) {
	run := controlPodTestRun()
	pod, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa",
		WorkerPodUID:          "w",
		ControlIncarnationUID: "c",
		WorkerURL:             "http://worker:8080",
		BrokerURL:             "https://broker:8443",
		BrokerStatusURL:       "https://broker:8444",
	})
	if err != nil {
		t.Fatal(err)
	}
	env := controlEnv(pod)
	if _, ok := env[EnvGatewayURL]; ok {
		t.Fatal("gateway URL must be absent without gateway configuration")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "gateway-key" {
			t.Fatal("gateway key volume must be absent without gateway configuration")
		}
	}
}

func TestControlPodGatewayWithoutKeyIsValid(t *testing.T) {
	run := controlPodTestRun()
	pod, err := ControlPod(run, ControlInputs{
		Image: "harness:test", GatewayURL: "http://gateway:4000/v1",
		ControlSAUID: "sa", WorkerPodUID: "w", ControlIncarnationUID: "c",
		WorkerURL: "http://worker:8080", BrokerURL: "https://broker:8443", BrokerStatusURL: "https://broker:8444",
	})
	if err != nil {
		t.Fatal(err)
	}
	env := controlEnv(pod)
	if env[EnvGatewayURL] != "http://gateway:4000/v1" {
		t.Fatalf("gateway URL env = %q", env[EnvGatewayURL])
	}
	if _, ok := env[EnvGatewayKeyFile]; ok {
		t.Fatal("a gateway without a key Secret renders no key file env")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "gateway-key" {
			t.Fatal("no gateway key volume may render without the Secret")
		}
	}
}

func TestControlPodKeySecretWithoutURLIsRejected(t *testing.T) {
	run := controlPodTestRun()
	if _, err := ControlPod(run, ControlInputs{
		Image: "harness:test", GatewayKeyMounted: true,
		ControlSAUID: "sa", WorkerPodUID: "w", ControlIncarnationUID: "c",
		WorkerURL: "http://worker:8080", BrokerURL: "https://broker:8443", BrokerStatusURL: "https://broker:8444",
	}); err == nil {
		t.Fatal("a gateway key Secret without a gateway URL must fail rendering")
	}
}
