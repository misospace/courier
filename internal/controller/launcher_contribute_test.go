package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/adhoc"
)

// TestCoordinatorLauncherContributeRunRendersAdmittedGoal is the legacy (non-
// secure) launch path for a contribute run: the run is admitted through the
// adhoc source's own writer, so its durable context is a real envelope the
// adapter can decode, and Launch must hand the coordinator a goal rendered
// from that admitted objective rather than from the spec.
func TestCoordinatorLauncherContributeRunRendersAdmittedGoal(t *testing.T) {
	admitted := admitContributeRun(t, contributeTestClient(t))
	admitted.Status.Phase = courierv1alpha1.PhaseClaimed

	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(admitted, contributeLane()).
		Build()
	launcher := &CoordinatorLauncher{
		Client:  fakeClient,
		Pod:     executor.PodConfig{Image: "example/opencode:test"},
		Sources: NewSourceRegistry(map[string]source.Adapter{"adhoc": adhoc.New(nil, nil)}),
	}

	if err := launcher.Launch(context.Background(), admitted); err != nil {
		t.Fatalf("Launch() error = %v", err)
	}

	var pod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: admitted.Namespace, Name: admitted.Name + "-coordinator"}, &pod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	env := podEnvironmentByName(t, &pod)

	goal, ok := env["COURIER_GOAL"]
	if !ok {
		t.Fatal("COURIER_GOAL missing from the coordinator pod env")
	}
	for _, want := range []string{"implement X", "trunk"} {
		if !strings.Contains(goal.Value, want) {
			t.Fatalf("COURIER_GOAL = %q, want it to contain %q", goal.Value, want)
		}
	}
	if got := env["COURIER_MODE"]; got.Value != string(courierv1alpha1.ModeContribute) {
		t.Fatalf("COURIER_MODE = %q, want %q", got.Value, courierv1alpha1.ModeContribute)
	}
	if got := env["COURIER_REF"]; got.Value != "0" {
		t.Fatalf("COURIER_REF = %q, want 0 for a contribute run", got.Value)
	}
}

// TestCoordinatorLauncherContributeRunUndecodableContextFailsPermanent pins
// the fail-closed boundary of the same path: a contribute run whose durable
// context cannot be decoded is a permanent NeedsHuman verdict before any
// coordinator pod is created, and the rejection must be classified so the
// reconciler terminalizes it instead of churning claim and release.
func TestCoordinatorLauncherContributeRunUndecodableContextFailsPermanent(t *testing.T) {
	admitted := admitContributeRun(t, contributeTestClient(t))
	admitted.Spec.SourceContext = "{}"
	admitted.Status.Phase = courierv1alpha1.PhaseClaimed

	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(admitted, contributeLane()).
		Build()
	launcher := &CoordinatorLauncher{
		Client:  fakeClient,
		Pod:     executor.PodConfig{Image: "example/opencode:test"},
		Sources: NewSourceRegistry(map[string]source.Adapter{"adhoc": adhoc.New(nil, nil)}),
	}

	err := launcher.Launch(context.Background(), admitted)
	if err == nil {
		t.Fatal("Launch() error = nil, want a permanent NeedsHuman verdict for an undecodable source context")
	}
	var needsHuman *SecureNeedsHumanError
	if !errors.As(err, &needsHuman) || needsHuman.Reason != contributionContextReason {
		t.Fatalf("Launch() error = %v, want a *SecureNeedsHumanError with reason %q", err, contributionContextReason)
	}
	// The reconciler's own gate: without this classification the run would
	// release its claim and relaunch on every pass.
	if rejection := launchRejection(err); rejection == nil || rejection.Reason != contributionContextReason {
		t.Fatalf("launchRejection() = %+v, want the contribution-context verdict", rejection)
	}

	var pod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: admitted.Namespace, Name: admitted.Name + "-coordinator"}, &pod); !apierrors.IsNotFound(err) {
		t.Fatalf("coordinator pod present after an undecodable source context: %v", err)
	}
}

// TestCoordinatorLauncherContributeRunUnwiredRegistryStaysRetryable pins the
// other boundary of the path: a contribute run reaching Launch with no source
// registry wired at all is an operator wiring gap, not a source refusal. The
// failure must stay retryable — no NeedsHuman verdict, no rejection
// classification — because a later deployment can wire the registry.
func TestCoordinatorLauncherContributeRunUnwiredRegistryStaysRetryable(t *testing.T) {
	admitted := admitContributeRun(t, contributeTestClient(t))
	admitted.Status.Phase = courierv1alpha1.PhaseClaimed

	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(admitted, contributeLane()).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}

	err := launcher.Launch(context.Background(), admitted)
	if err == nil {
		t.Fatal("Launch() error = nil, want a failure for a contribute run with no source registry wired")
	}
	var needsHuman *SecureNeedsHumanError
	if errors.As(err, &needsHuman) {
		t.Fatalf("Launch() error = %v, want a retryable wiring failure, not a permanent NeedsHuman verdict", err)
	}
	if rejection := launchRejection(err); rejection != nil {
		t.Fatalf("launchRejection() = %+v, want nil: an unwired registry must stay retryable", rejection)
	}
}
