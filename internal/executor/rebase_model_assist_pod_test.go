package executor

import (
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The rebase model-assist configuration reaches generated coordinator pods
// as exactly the three environment variables the executor reads (#732),
// emitted only when the capability is enabled — absence is the meaningful
// fail-closed default — and the API key stays a SecretKeyRef registered in
// the shared credential enumeration the evidence intake re-scans.
func TestBuildCoordinatorPodCarriesRebaseModelAssistEnv(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "runs", UID: types.UID("uid-1")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeRebase, Repo: "acme/widgets", Ref: 42, Lane: "local",
		},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "runs"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test/model"}},
	}
	envByName := func(pod *corev1.Pod) map[string]corev1.EnvVar {
		values := map[string]corev1.EnvVar{}
		for _, env := range pod.Spec.Containers[0].Env {
			values[env.Name] = env
		}
		return values
	}
	const assistEnv = "COURIER_REBASE_MODEL_ASSIST"
	const gatewayURLEnv = "COURIER_MODEL_GATEWAY_URL"
	const gatewayKeyEnv = "COURIER_MODEL_GATEWAY_API_KEY"

	// Disabled: neither the assist flag nor any supporting variable is
	// emitted, because absence is the fail-closed default the executor
	// parses as off.
	disabled, err := BuildCoordinatorPod(run, lane, PodConfig{Image: "example/test"})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() disabled error = %v", err)
	}
	values := envByName(disabled)
	for _, name := range []string{assistEnv, gatewayURLEnv, gatewayKeyEnv} {
		if _, ok := values[name]; ok {
			t.Fatalf("a disabled configuration must not emit %s", name)
		}
	}

	// Enabled without a gateway URL is a configuration error, failed closed
	// in the builder exactly as at startup.
	if _, err := BuildCoordinatorPod(run, lane, PodConfig{Image: "example/test", RebaseModelAssist: true}); err == nil ||
		!strings.Contains(err.Error(), "COURIER_MODEL_GATEWAY_URL") {
		t.Fatalf("enabled without url err = %v, want the fail-closed configuration error", err)
	}
	if err := (PodConfig{Image: "example/test", WorkspacePath: "/workspace", RebaseModelAssist: true}).Validate(); err == nil ||
		!strings.Contains(err.Error(), "COURIER_MODEL_GATEWAY_URL") {
		t.Fatalf("Validate() enabled without url = %v, want the fail-closed configuration error", err)
	}

	// Enabled: the flag, the trimmed gateway URL, and the API key as a
	// SecretKeyRef under the configured key name.
	enabled, err := BuildCoordinatorPod(run, lane, PodConfig{
		Image:                 "example/test",
		RebaseModelAssist:     true,
		ModelGatewayURL:       "  http://gateway:4000/v1  ",
		ModelGatewayKeySecret: "gateway-key",
		ModelGatewayKeyName:   "api-key",
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() enabled error = %v", err)
	}
	values = envByName(enabled)
	if got := values[assistEnv]; got.Value != "enabled" || got.ValueFrom != nil {
		t.Fatalf("%s = %+v, want the literal enabled flag", assistEnv, got)
	}
	if got := values[gatewayURLEnv]; got.Value != "http://gateway:4000/v1" || got.ValueFrom != nil {
		t.Fatalf("%s = %+v, want the trimmed gateway URL", gatewayURLEnv, got)
	}
	key := values[gatewayKeyEnv]
	if key.Value != "" || key.ValueFrom == nil || key.ValueFrom.SecretKeyRef == nil ||
		key.ValueFrom.SecretKeyRef.Name != "gateway-key" || key.ValueFrom.SecretKeyRef.Key != "api-key" {
		t.Fatalf("%s = %+v, want the SecretKeyRef under the configured key name", gatewayKeyEnv, key)
	}
}
