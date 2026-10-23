package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
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
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbac scheme: %v", err)
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
	// The pod is run-owned so the DeletionTimestamp branch — not the
	// ownership check — is what rejects the collision.
	controller := true
	now := metav1.Now()
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-1-coordinator",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{"keep.test/deletion"},
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

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight; a colliding terminating pod must not be treated as a successful launch", err)
	}
}

// pinnedCriteriaPolicy builds a persisted policy whose criterion pin and
// base ref mirror admission's mapping of an extracted set, so launcher
// tests exercise the same shape the operator writes (#280).
func pinnedCriteriaPolicy(t *testing.T, body string) *courierv1alpha1.PublicationPolicy {
	t.Helper()
	set, err := broker.ExtractCriteria(body, broker.SourceIssue{Owner: "acme", Name: "widgets", Number: 1})
	if err != nil {
		t.Fatalf("extract criteria: %v", err)
	}
	policy := &courierv1alpha1.PublicationPolicy{
		SourceIssue: &courierv1alpha1.SourceIssueRef{Owner: "acme", Name: "widgets", Number: 1},
		BaseRef:     "main",
	}
	for _, criterion := range set.Criteria {
		policy.AcceptanceCriteria = append(policy.AcceptanceCriteria, courierv1alpha1.Criterion{
			ID: criterion.ID, Text: criterion.Text, Digest: criterion.Digest,
		})
	}
	policy.AcceptanceCriteriaDigest = set.Digest
	return policy
}

// The legacy coordinator pod receives the trusted COURIER_CRITERIA envelope
// projected from the persisted policy — the identical pinned set the broker
// binding re-verifies — while a run without a pin receives no envelope, and
// a corrupt pin fails the launch closed.
func TestCoordinatorLauncherProjectsCriteriaEnvelope(t *testing.T) {
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
	run.Status.PublicationPolicy = pinnedCriteriaPolicy(t, "## Acceptance\n- one\n- two\n")
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{Client: fakeClient, Pod: executor.PodConfig{Image: "example/opencode:test"}}

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	var pod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &pod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	env := podEnvironmentByName(t, &pod)
	criteria, ok := env["COURIER_CRITERIA"]
	if !ok || strings.TrimSpace(criteria.Value) == "" {
		t.Fatalf("COURIER_CRITERIA missing or empty in pod env: %+v", criteria)
	}
	var projected broker.CriterionSet
	if err := json.Unmarshal([]byte(criteria.Value), &projected); err != nil {
		t.Fatalf("COURIER_CRITERIA is not the broker criterion-set shape: %v", err)
	}
	if projected.Digest != run.Status.PublicationPolicy.AcceptanceCriteriaDigest ||
		projected.Issue != (broker.SourceIssue{Owner: "acme", Name: "widgets", Number: 1}) ||
		len(projected.Criteria) != 2 {
		t.Fatalf("projected envelope = %+v, want the persisted pin", projected)
	}
	// The envelope must survive a decode→validate round trip: it is the
	// exact comparison target the legacy boundary validates against.
	if _, err := broker.ValidateCriterionEvidence("", projected); err == nil {
		t.Fatal("an empty body against a non-empty pin must not validate")
	}

	// A run without a persisted pin launches without the envelope: its
	// absence is meaningful to the legacy boundary.
	unpinned := run.DeepCopy()
	unpinned.Status.PublicationPolicy = nil
	var existing corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &existing); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Delete(context.Background(), &existing); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(context.Background(), unpinned); err != nil {
		t.Fatalf("Launch() unpinned error = %v", err)
	}
	var bare corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &bare); err != nil {
		t.Fatal(err)
	}
	if _, ok := podEnvironmentByName(t, &bare)["COURIER_CRITERIA"]; ok {
		t.Fatal("a run without a pinned set must not receive COURIER_CRITERIA")
	}

	// A corrupt pin fails the launch closed rather than projecting an
	// envelope that cannot be trusted.
	corrupt := run.DeepCopy()
	corrupt.Status.PublicationPolicy.AcceptanceCriteria[0].Text = "tampered"
	if err := launcher.Launch(context.Background(), corrupt); err == nil {
		t.Fatal("a corrupt criterion pin must fail the launch")
	}

	// Orphaned pin fields are corruption, not a silent downgrade to the
	// pre-contract state: criteria left without an issue identity or set
	// digest must fail the launch instead of quietly omitting the envelope.
	orphaned := run.DeepCopy()
	orphaned.Status.PublicationPolicy.SourceIssue = nil
	orphaned.Status.PublicationPolicy.AcceptanceCriteriaDigest = ""
	if err := launcher.Launch(context.Background(), orphaned); err == nil {
		t.Fatal("orphaned criterion pin fields must fail the launch")
	}
}

// The legacy coordinator pod of a fix-pr run carries the pinned pull
// request's live target ref — a stacked base included — never the
// deployment default, because the run repairs a request that was opened
// against that base. A resolve-issue run keeps the deployment default: it
// works from the repository's default branch, not from a pull request.
func TestCoordinatorLauncherProjectsFixPRBaseRef(t *testing.T) {
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "default"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test-model"}},
	}
	newRun := func(name string, mode courierv1alpha1.Mode) *courierv1alpha1.CoderRun {
		return &courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")},
			Spec: courierv1alpha1.CoderRunSpec{
				Mode: mode,
				Repo: "acme/widgets",
				Ref:  7,
				Lane: "local",
			},
			Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
		}
	}
	fixRun := newRun("run-1", courierv1alpha1.ModeFixPR)
	issueRun := newRun("run-2", courierv1alpha1.ModeResolveIssue)
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(fixRun, issueRun, lane).
		Build()
	// The resolver answers with the pull request's live identity: a stacked
	// base the deployment default does not name, and the head the request
	// carries.
	resolutions := 0
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test", BaseBranch: "main"},
		PRHeadResolver: ExistingPRHeadResolverFunc(func(_ context.Context, run *courierv1alpha1.CoderRun) (HeadRef, error) {
			resolutions++
			return HeadRef{
				Repo:    "acme/widgets",
				Branch:  "courier/fix/acme-widgets/7",
				SHA:     "headsha",
				BaseRef: "courier/acme/widgets/issue-9",
			}, nil
		}),
	}

	// The fix-pr run's pod carries the pull request's live target ref, not
	// the deployment default the launcher is configured with.
	if err := launcher.Launch(context.Background(), fixRun); err != nil {
		t.Fatalf("Launch() fix-pr error = %v", err)
	}
	var fixPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &fixPod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	if base, ok := podEnvironmentByName(t, &fixPod)["COURIER_BASE"]; !ok || base.Value != "courier/acme/widgets/issue-9" {
		t.Fatalf("COURIER_BASE = %+v, want the pull request's live target ref courier/acme/widgets/issue-9, not the deployment default", base)
	}
	if resolutions != 1 {
		t.Fatalf("pull-request resolutions = %d, want 1; the launch must cost one pull-request read", resolutions)
	}

	// A resolve-issue run keeps the deployment default: no pull request
	// names its base, so the resolver is never consulted for it.
	if err := launcher.Launch(context.Background(), issueRun); err != nil {
		t.Fatalf("Launch() resolve-issue error = %v", err)
	}
	var issuePod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-2-coordinator"}, &issuePod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	if base, ok := podEnvironmentByName(t, &issuePod)["COURIER_BASE"]; !ok || base.Value != "main" {
		t.Fatalf("COURIER_BASE = %+v, want the launcher's configured base main", base)
	}
	if resolutions != 1 {
		t.Fatalf("pull-request resolutions = %d, want 1; a resolve-issue run resolves no base", resolutions)
	}
}

// An unverifiable base fails the launch closed. A missing resolver, a
// failed resolution, and a resolved pull request that names no base ref all
// leave the run's base unknown, and the deployment default must never
// stand in for it: a coordinator launched against the default would repair
// the pull request under a base it does not name.
func TestCoordinatorLauncherFailsClosedWhenFixPRBaseCannotBeVerified(t *testing.T) {
	resolutionErr := errors.New("the forge read failed")
	for _, tc := range []struct {
		name     string
		resolver ExistingPRHeadResolver
	}{
		{
			name:     "no resolver wired",
			resolver: nil,
		},
		{
			name: "resolution failed",
			resolver: ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (HeadRef, error) {
				return HeadRef{}, resolutionErr
			}),
		},
		{
			name: "resolved pull request names no base",
			resolver: ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (HeadRef, error) {
				return HeadRef{Repo: "acme/widgets", Branch: "courier/fix/acme-widgets/7", SHA: "headsha"}, nil
			}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
				Spec: courierv1alpha1.CoderRunSpec{
					Mode: courierv1alpha1.ModeFixPR,
					Repo: "acme/widgets",
					Ref:  7,
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
				Client:         fakeClient,
				Pod:            executor.PodConfig{Image: "example/opencode:test", BaseBranch: "main"},
				PRHeadResolver: tc.resolver,
			}

			err := launcher.Launch(context.Background(), run)
			var permanent *SecureNeedsHumanError
			if !errors.As(err, &permanent) {
				t.Fatalf("Launch() error = %v, want a *SecureNeedsHumanError the reconciler can terminalize", err)
			}
			if permanent.Reason != baseUnverifiedReason {
				t.Fatalf("reason = %q, want %q", permanent.Reason, baseUnverifiedReason)
			}
			// The rejection must terminalize the run: a class the launch
			// paths do not terminalize only cycles the claim.
			if launchRejection(err) == nil {
				t.Fatalf("reason %q is not a terminal launch rejection; the run would cycle claim and release instead of settling", permanent.Reason)
			}
			var pod corev1.Pod
			if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &pod); !apierrors.IsNotFound(err) {
				t.Fatalf("coordinator pod present after an unverifiable base: %v", err)
			}
		})
	}
}

// transientResolutionError is a forge read that never completed: the base
// was never observed, so the launch must be retried rather than settled.
type transientResolutionError struct{}

func (transientResolutionError) Error() string { return "the forge was temporarily unavailable" }

func (transientResolutionError) TransientAvailability() bool { return true }

// TestCoordinatorLauncherRetriesTransientFixPRBaseResolution proves the
// launcher's transient branch of the base contract: a read that never
// completed proves nothing about the pull request's base, so the launch
// fails with an ordinary retryable error and creates no pod, and the run is
// never settled on an unobserved base (#455).
func TestCoordinatorLauncherRetriesTransientFixPRBaseResolution(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeFixPR,
			Repo: "acme/widgets",
			Ref:  7,
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
		Pod:    executor.PodConfig{Image: "example/opencode:test", BaseBranch: "main"},
		PRHeadResolver: ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (HeadRef, error) {
			return HeadRef{}, transientResolutionError{}
		}),
	}

	err := launcher.Launch(context.Background(), run)
	if err == nil {
		t.Fatal("Launch() error = nil, want the transient read failure")
	}
	var permanent *SecureNeedsHumanError
	if errors.As(err, &permanent) {
		t.Fatalf("Launch() error = %v, want a retryable error: a read that never completed settles nothing", err)
	}
	if launchRejection(err) != nil {
		t.Fatalf("Launch() error = %v, want no terminal rejection: the run must be retried", err)
	}
	var pod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &pod); !apierrors.IsNotFound(err) {
		t.Fatalf("coordinator pod present after an unobserved base: %v", err)
	}
}

// TestCoordinatorLauncherClassifiesPermanentPodRejection pins the launcher's
// half of the permanent-rejection invariant: a coordinator pod spec the API
// server refuses outright, with the slot proven empty, is a permanent,
// terminalizable failure, because requeueing cannot change it. A Forbidden or
// quota denial stays an ordinary error — a grant or a quota can still be
// raised.
func TestCoordinatorLauncherClassifiesPermanentPodRejection(t *testing.T) {
	cases := map[string]struct {
		rejection     error
		wantPermanent bool
	}{
		"invalid": {
			rejection:     invalidPodRejection("coordinator"),
			wantPermanent: true,
		},
		"forbidden": {
			rejection:     apierrors.NewForbidden(corev1.Resource("pods"), "coordinator", errors.New("exceeded quota")),
			wantPermanent: false,
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
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
			watcher, ok := fakeClient.(client.WithWatch)
			if !ok {
				t.Fatal("the fake client does not support watch")
			}
			launcher.Client = interceptor.NewClient(watcher, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if obj.GetName() == "run-1-coordinator" {
						return test.rejection
					}
					return c.Create(ctx, obj, opts...)
				},
			})

			err := launcher.Launch(context.Background(), run)
			var permanent *SecureNeedsHumanError
			if test.wantPermanent {
				if !errors.As(err, &permanent) {
					t.Fatalf("Launch() error = %v, want a *SecureNeedsHumanError the reconciler can terminalize", err)
				}
				if permanent.Reason != "PodAdmission" {
					t.Fatalf("reason = %q, want PodAdmission", permanent.Reason)
				}
				if !strings.Contains(permanent.Detail, "rejected the coordinator pod run-1-coordinator") {
					t.Fatalf("detail = %q, want it to name the rejected coordinator pod", permanent.Detail)
				}
				return
			}
			if errors.As(err, &permanent) {
				t.Fatalf("Launch() error = %v, want an ordinary requeueable error", err)
			}
			if !errors.Is(err, test.rejection) {
				t.Fatalf("Launch() error = %v, want the unchanged create failure %v", err, test.rejection)
			}
		})
	}
}

// TestCoordinatorLauncherKeepsClaimOnPodAdmissionWithLiveSlot pins the other
// half of the same decision. The API server validates before its existence
// check, so a slot an earlier attempt of this still-Claimed run filled answers
// Invalid rather than AlreadyExists — and a live coordinator may be running
// there. The permanent class is therefore conditional on a proven-empty slot:
// with a writer in the slot the launcher stays Claimed (ErrProvisioningInFlight),
// because a terminalized NeedsHuman run is no longer observed, and the run
// adopts its own live pod once the lane is fixed.
func TestCoordinatorLauncherKeepsClaimOnPodAdmissionWithLiveSlot(t *testing.T) {
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
	// An earlier attempt of this run already created its coordinator, and the
	// lane has since been edited into a spec the API server refuses.
	controller := true
	live := &corev1.Pod{
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
		WithRuntimeObjects(run, lane, live).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}
	watcher, ok := fakeClient.(client.WithWatch)
	if !ok {
		t.Fatal("the fake client does not support watch")
	}
	launcher.Client = interceptor.NewClient(watcher, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetName() == "run-1-coordinator" {
				return invalidPodRejection("run-1-coordinator")
			}
			return c.Create(ctx, obj, opts...)
		},
	})

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight while this run's own coordinator occupies the slot; terminalizing would strand a live writer", err)
	}
	var permanent *SecureNeedsHumanError
	if errors.As(err, &permanent) {
		t.Fatalf("Launch() error = %v, want no terminalizable class while the slot is occupied", err)
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
	// without a second charge. Ownership is bound to the run's UID via
	// the controller OwnerReference, so neither a foreign pod nor a pod
	// left by a dead incarnation of the same name is adopted.
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
			// The pod create collides as if a previous create of the same
			// name had landed, but the follow-up Get sees nothing.
			// Scoped to pods: observer identity provisioning now precedes
			// the pod create, and provisioning its own resources is not
			// the race under test.
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Pod); ok {
					return apierrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "pods"}, obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
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

// TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodIsStaleIncarnation
// pins the UID-binding half of #217's review: a live pod left at the slot
// name by a previous CoderRun incarnation — same name, same LabelRun, and
// a controller OwnerReference of Kind CoderRun with the same name but the
// dead incarnation's UID. Ownership matching on name or label alone would
// adopt it and record Running over a coordinator belonging to a run that
// no longer exists, so ownership is bound to the run's UID.
func TestCoordinatorLauncherReportsProvisioningInFlightWhenExistingPodIsStaleIncarnation(t *testing.T) {
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
	// Live, no DeletionTimestamp, and every name-level identity marker
	// matching this run — only the owner UID differs (the previous
	// incarnation of run-1).
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
				UID:        types.UID("dead-incarnation-uid"),
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

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight; a pod owned by a dead run incarnation must not be adopted", err)
	}
}

// TestCoordinatorLauncherCollisionReadBypassesCachedClient pins the review
// finding that the collision verification must not run against the informer
// cache: the cached client still serves the pre-deletion view of the slot
// pod (live, run-owned), while the direct API reader shows the current
// state (terminating). A cached read would resolve the collision to a
// successful Launch and let the reconciler record Running over a pod being
// torn down, so the verification must go through the API reader.
func TestCoordinatorLauncherCollisionReadBypassesCachedClient(t *testing.T) {
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
	controller := true
	ownerRef := metav1.OwnerReference{
		APIVersion: courierv1alpha1.GroupVersion.String(),
		Kind:       "CoderRun",
		Name:       run.Name,
		UID:        run.UID,
		Controller: &controller,
	}
	// The stale cached view: the pod as it looked before the API server
	// stamped the deletion timestamp — live and run-owned.
	stale := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "run-1-coordinator",
			Namespace:       "default",
			Labels:          map[string]string{executor.LabelRun: run.Name},
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}
	// The current API-server view: the same pod carries DeletionTimestamp.
	now := metav1.Now()
	current := stale.DeepCopy()
	current.Finalizers = []string{"keep.test/deletion"}
	current.DeletionTimestamp = &now

	cached := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run, lane, stale).
		Build()
	apiReader := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithRuntimeObjects(current).
		Build()
	launcher := &CoordinatorLauncher{
		Client:    cached,
		APIReader: apiReader,
		Pod:       executor.PodConfig{Image: "example/opencode:test"},
	}

	err := launcher.Launch(context.Background(), run)
	if !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch() error = %v, want ErrProvisioningInFlight; the collision verification must read the API server's current state, not a cached pre-deletion view", err)
	}
}

func TestCoordinatorLauncherValidateRequiresAPIReader(t *testing.T) {
	unwired := &CoordinatorLauncher{}
	if err := unwired.Validate(); !errors.Is(err, ErrLauncherAPIReaderRequired) {
		t.Fatalf("Validate() error = %v, want ErrLauncherAPIReaderRequired", err)
	}
	wired := &CoordinatorLauncher{APIReader: fake.NewClientBuilder().WithScheme(launcherScheme(t)).Build()}
	if err := wired.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

// TestCoordinatorLauncherRejectsUnlaunchableLaneSizing pins the legacy launch
// path's counterpart of the secure path's LaneConfig rejection: a lane whose
// CPU/memory sizing could never launch fails the run NeedsHuman with a
// permanent, terminalizable error before the coordinator pod is created.
func TestCoordinatorLauncherRejectsUnlaunchableLaneSizing(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  1,
			Lane: "broken",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "default"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles: map[string]string{"coordinator": "test-model"},
			Resources: courierv1alpha1.PodResources{
				Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")},
			},
		},
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
	err := launcher.Launch(context.Background(), run)
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != "LaneConfig" {
		t.Fatalf("Launch() error = %v, want a *SecureNeedsHumanError with reason LaneConfig", err)
	}
	var podPod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &podPod); !apierrors.IsNotFound(err) {
		t.Fatalf("coordinator pod present after unlaunchable lane sizing: %v", err)
	}
}
