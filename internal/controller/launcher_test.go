package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

func TestCoordinatorLauncherCreatesPodForClaimedRun(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  1,
			Lane: "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	var podPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	if podPod.Spec.Containers[0].Image != "example/opencode:test" {
		t.Fatalf("pod image = %q, want configured image", podPod.Spec.Containers[0].Image)
	}
	var updated courierv1alpha1.CoderRun
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1"}, &updated); err != nil {
		t.Fatalf("get updated run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("run phase = %q, want unchanged Claimed; phase transitions belong to the reconciler", updated.Status.Phase)
	}
}

func launcherScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return scheme
}

func TestCoordinatorLauncherWiresEvidencePerBuild(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  1,
			Lane: "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client:      fakeClient,
		Pod:         executor.PodConfig{Image: "example/opencode:test"},
		EvidenceURL: "http://courier-evidence.courier-system:8082",
		EvidenceKey: []byte("a-test-hmac-key-of-some-length"),
	}

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	var podPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	env := podEnvironmentByName(t, &podPod)
	if got, ok := env[executor.EnvEvidenceURL]; !ok || got.Value != "http://courier-evidence.courier-system:8082" {
		t.Fatalf("%s = %+v, want the configured intake URL", executor.EnvEvidenceURL, got)
	}
	nonceEnv, ok := env[executor.EnvEvidenceNonce]
	if !ok || nonceEnv.Value == "" {
		t.Fatalf("%s missing or empty in pod env", executor.EnvEvidenceNonce)
	}
	decodedNonce, err := hex.DecodeString(nonceEnv.Value)
	if err != nil {
		t.Fatalf("decode %s %q: %v", executor.EnvEvidenceNonce, nonceEnv.Value, err)
	}
	tokenEnv, ok := env[executor.EnvEvidenceToken]
	if !ok || tokenEnv.ValueFrom != nil {
		t.Fatalf("%s must be a literal value, not a ValueFrom source", executor.EnvEvidenceToken)
	}
	// The token is pinned to the nonce actually delivered to the pod, so a
	// reader of the pod can recompute exactly what the intake expects.
	if want := executor.EvidenceToken(launcher.EvidenceKey, "default", "run-1", "run-uid", decodedNonce); tokenEnv.Value != want {
		t.Fatalf("%s = %q, want the token derived from the delivered nonce", executor.EnvEvidenceToken, tokenEnv.Value)
	}
	podUID, ok := env[executor.EnvPodUID]
	if !ok || podUID.ValueFrom == nil || podUID.ValueFrom.FieldRef == nil || podUID.ValueFrom.FieldRef.FieldPath != "metadata.uid" {
		t.Fatalf("%s must be sourced from the fieldRef metadata.uid", executor.EnvPodUID)
	}
	if podPod.Spec.TerminationGracePeriodSeconds == nil || *podPod.Spec.TerminationGracePeriodSeconds != 45 {
		t.Fatalf("termination grace period = %v, want 45 for a capturing pod", podPod.Spec.TerminationGracePeriodSeconds)
	}

	// A second incarnation of the same run must receive a fresh nonce and a
	// fresh token: the intake slot is per-incarnation.
	if err := fakeClient.Delete(context.Background(), &podPod); err != nil {
		t.Fatalf("delete coordinator pod: %v", err)
	}
	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch() second incarnation error = %v", err)
	}
	var secondPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &secondPod); err != nil {
		t.Fatalf("get second coordinator pod: %v", err)
	}
	secondEnv := podEnvironmentByName(t, &secondPod)
	if secondEnv[executor.EnvEvidenceNonce].Value == nonceEnv.Value {
		t.Fatalf("second incarnation delivered the same nonce %q; nonces must be per-incarnation", nonceEnv.Value)
	}
	if secondEnv[executor.EnvEvidenceToken].Value == tokenEnv.Value {
		t.Fatalf("second incarnation delivered the same token; tokens must be per-incarnation")
	}
}

func TestCoordinatorLauncherOmitsEvidenceWhenUnconfigured(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  1,
			Lane: "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	var podPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	env := podEnvironmentByName(t, &podPod)
	for _, name := range []string{executor.EnvEvidenceURL, executor.EnvEvidenceToken, executor.EnvEvidenceNonce, executor.EnvPodUID} {
		if _, present := env[name]; present {
			t.Fatalf("%s present in pod env, want omitted when capture is unconfigured", name)
		}
	}
	if podPod.Spec.TerminationGracePeriodSeconds != nil {
		t.Fatalf("termination grace period = %d, want nil (kubelet default) when capture is unconfigured", *podPod.Spec.TerminationGracePeriodSeconds)
	}
}

func TestCoordinatorLauncherRejectsPartialEvidenceConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		evidenceURL string
		evidenceKey []byte
	}{
		{name: "url without key", evidenceURL: "http://courier-evidence.courier-system:8082"},
		{name: "key without url", evidenceKey: []byte("a-test-hmac-key-of-some-length")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
				Spec: courierv1alpha1.CoderRunSpec{
					Mode: courierv1alpha1.ModeResolveIssue,
					Repo: "acme/widgets",
					Ref:  1,
					Lane: "local",
				},
				Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
			}
			lane := &courierv1alpha1.LaneProfile{
				ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
				Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
			}
			fakeClient := fake.NewClientBuilder().
				WithScheme(launcherScheme(t)).
				WithStatusSubresource(&courierv1alpha1.CoderRun{}).
				WithRuntimeObjects(run, lane).
				Build()
			launcher := &CoordinatorLauncher{
				Client:      fakeClient,
				Pod:         executor.PodConfig{Image: "example/opencode:test"},
				EvidenceURL: tc.evidenceURL,
				EvidenceKey: tc.evidenceKey,
			}
			err := launcher.Launch(context.Background(), run)
			if !errors.Is(err, ErrEvidenceConfiguration) {
				t.Fatalf("Launch() error = %v, want ErrEvidenceConfiguration", err)
			}
			var podPod corev1.Pod
			if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); !apierrors.IsNotFound(err) {
				t.Fatalf("coordinator pod present after partial evidence configuration: %v", err)
			}
		})
	}
}

// podEnvironmentByName indexes the coordinator container's env by name so the
// evidence assertions can look values up without position bookkeeping.
func podEnvironmentByName(t *testing.T, pod *corev1.Pod) map[string]corev1.EnvVar {
	t.Helper()
	env := make(map[string]corev1.EnvVar, len(pod.Spec.Containers[0].Env))
	for _, value := range pod.Spec.Containers[0].Env {
		env[value.Name] = value
	}
	return env
}

func TestCoordinatorLauncherRejectsEmptyRunUIDWhenMinting(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: ""},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  1,
			Lane: "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client:      fakeClient,
		Pod:         executor.PodConfig{Image: "example/opencode:test"},
		EvidenceURL: "http://courier-evidence.courier-system:8082",
		EvidenceKey: []byte("a-test-hmac-key-of-some-length"),
	}
	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrEvidenceRunUIDRequired) {
		t.Fatalf("Launch() error = %v, want ErrEvidenceRunUIDRequired", err)
	}
	var podPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); !apierrors.IsNotFound(err) {
		t.Fatalf("coordinator pod present after empty run UID: %v", err)
	}
}

func TestCoordinatorLauncherTrimsSurroundingWhitespaceFromEvidenceURL(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  1,
			Lane: "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client:      fakeClient,
		Pod:         executor.PodConfig{Image: "example/opencode:test"},
		EvidenceURL: "  http://courier-evidence.courier-system:8082  ",
		EvidenceKey: []byte("a-test-hmac-key-of-some-length"),
	}
	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	var podPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	env := podEnvironmentByName(t, &podPod)
	if got, ok := env[executor.EnvEvidenceURL]; !ok || got.Value != "http://courier-evidence.courier-system:8082" {
		t.Fatalf("%s = %+v, want the intake URL trimmed of surrounding whitespace", executor.EnvEvidenceURL, got)
	}
}
