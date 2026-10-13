package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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

func TestCoordinatorLauncherRejectsMalformedEvidenceURL(t *testing.T) {
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
	// The lane is deliberately not loaded into the fake client. Launch validates
	// the intake URL before it reads the lane, so a malformed URL fails with the
	// URL rule no matter what. If that check ever regressed to after the lane
	// read, the absent lane would surface as a NotFound instead and the
	// assertions below would catch the ordering slip.
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run).
		Build()
	launcher := &CoordinatorLauncher{
		Client:      fakeClient,
		Pod:         executor.PodConfig{Image: "example/opencode:test"},
		EvidenceURL: "not-a-url",
		EvidenceKey: []byte("a-test-hmac-key-of-some-length"),
	}
	err := launcher.Launch(context.Background(), run)
	if err == nil {
		t.Fatal("Launch() = nil, want an error for a malformed intake URL")
	}
	if !strings.Contains(err.Error(), "evidence intake URL") {
		t.Fatalf("Launch() error = %q, want it to mention the intake URL shape rule", err)
	}
	var podPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); !apierrors.IsNotFound(err) {
		t.Fatalf("coordinator pod present after a malformed intake URL: %v", err)
	}
}

func TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodIsTerminating(t *testing.T) {
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
	// A pod that already exists in the API with DeletionTimestamp set:
	// the slot is held by a pod that is being torn down, so the relaunch
	// must not record Running. The launcher surfaces this collision as
	// ErrProvisioningInFlight so the reconciler can stay Claimed and
	// requeue, instead of treating AlreadyExists as a successful Launch.
	now := metav1.Now()
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-1-coordinator",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{"keep.test/deletion"},
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane, existing).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight; a colliding terminating pod must not be treated as a successful launch", err)
	}
}

func TestCoordinatorLauncherAcceptsExistingPodWhenLive(t *testing.T) {
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
	// A live existing pod (no DeletionTimestamp) that this run actually
	// owns — a previous Launch left it at the slot name. The resource
	// boundary is already idempotent, the relaunch is genuinely complete,
	// and the reconciler is free to record Running on the next pass
	// without a second charge. Ownership is verified by OwnerReference
	// (and LabelRun as the legacy fallback), so a foreign pod at the
	// same name is not silently adopted.
	controller := true
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-1-coordinator",
			Namespace: "default",
			Labels: map[string]string{
				executor.LabelRun: run.Name,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: courierv1alpha1.GroupVersion.String(),
				Kind:       "CoderRun",
				Name:       run.Name,
				UID:        run.UID,
				Controller: &controller,
			}},
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane, existing).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch() error = %v, want nil; a colliding live, run-owned pod keeps the existing idempotency", err)
	}
	var runOut courierv1alpha1.CoderRun
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1"}, &runOut); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if runOut.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("run phase = %q, want unchanged Claimed; phase transitions belong to the reconciler", runOut.Status.Phase)
	}
}

// TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodVanishesBetweenCreateAndGet
// pins the create-then-read race #217's review flagged: a colliding pod
// is deleted between the Create that returned AlreadyExists and the
// follow-up Get that returns NotFound. Treating that as success would
// write Running over a slot with no live coordinator.
func TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodVanishesBetweenCreateAndGet(t *testing.T) {
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
		WithInterceptorFuncs(interceptor.Funcs{
			// Create returns AlreadyExists as if a previous create of the
			// same name had landed, but the follow-up Get sees nothing.
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "pods"}, obj.GetName())
			},
		}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight; a slot whose pod vanished between create and read is not verifiably live", err)
	}
}

// TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodReadErrors
// covers the second half of #217's review: the follow-up Get returns a
// transient API error (anything other than success) rather than a real
// pod. Treating that as success would similarly write Running over a
// slot we have no proof is occupied by a live coordinator.
func TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodReadErrors(t *testing.T) {
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
	transient := errors.New("API server timeout: connection reset")
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "pods"}, obj.GetName())
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				// Fail only the pod re-read that verifies the collision;
				// the earlier LaneProfile Get must still succeed or Launch
				// returns the transient error before reaching the collision
				// branch.
				if _, ok := obj.(*corev1.Pod); ok {
					return transient
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight; a slot whose read returned a transient error is not verifiably live", err)
	}
}

// TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodIsForeign
// pins the ownership-check half of #217's review: a live pod sits at the
// deterministic slot name but is not owned by this run. Silently adopting
// it as "idempotently live" would bind the run to an arbitrary
// coordinator, so the launcher surfaces the collision instead.
func TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodIsForeign(t *testing.T) {
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
	// Live, no DeletionTimestamp, neither OwnerReference nor LabelRun
	// tying it to this run — a stale or unrelated pod sitting at the
	// deterministic slot name.
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-1-coordinator",
			Namespace: "default",
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane, existing).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight; an unrelated pod at the slot name must not be silently adopted", err)
	}
}
