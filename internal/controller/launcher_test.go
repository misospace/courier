package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
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
