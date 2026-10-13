package executor

import (
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The trusted criterion envelope (#280) is part of the run-context
// environment contract: present exactly when a set is pinned, reconstructed
// by InvocationFromEnv so both a process executor and the native control pod
// see the identical operator-authored value.
func TestCriteriaEnvContract(t *testing.T) {
	pinned := Invocation{
		RunName: "run-1", Namespace: "runs", Mode: courierv1alpha1.ModeResolveIssue,
		Repo: "acme/widgets", Ref: 1, Goal: "g", Model: "m", Workspace: "/w",
		Criteria: `{"issue":{"owner":"acme","name":"widgets","number":1},"criteria":[],"digest":"sha256:abc"}`,
	}
	values := map[string]string{}
	for _, env := range RunContextEnvironment(pinned) {
		values[env.Name] = env.Value
	}
	if got := values["COURIER_CRITERIA"]; got != pinned.Criteria {
		t.Fatalf("COURIER_CRITERIA = %q, want the operator-authored envelope", got)
	}
	decoded, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Criteria != pinned.Criteria {
		t.Fatalf("round trip Criteria = %q, want %q", decoded.Criteria, pinned.Criteria)
	}

	// An unpinned invocation emits nothing: absence is meaningful.
	unpinned := pinned
	unpinned.Criteria = ""
	values = map[string]string{}
	for _, env := range RunContextEnvironment(unpinned) {
		values[env.Name] = env.Value
	}
	if _, ok := values["COURIER_CRITERIA"]; ok {
		t.Fatal("an unpinned invocation must not emit COURIER_CRITERIA")
	}
}

// The pod builder carries the per-run envelope from PodConfig into the
// coordinator container environment beside the run context.
func TestBuildCoordinatorPodCarriesCriteriaEnvelope(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "runs", UID: types.UID("uid-1")},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue, Repo: "acme/widgets", Ref: 1, Lane: "local",
		},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "runs"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "test/model"}},
	}
	envelope := `{"issue":{"owner":"acme","name":"widgets","number":1},"criteria":[],"digest":"sha256:abc"}`
	pod, err := BuildCoordinatorPod(run, lane, PodConfig{Image: "example/test", Criteria: envelope})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	found := false
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == "COURIER_CRITERIA" {
			found = true
			if env.Value != envelope || env.ValueFrom != nil {
				t.Fatalf("COURIER_CRITERIA = %+v, want the literal envelope", env)
			}
		}
	}
	if !found {
		t.Fatal("the coordinator container must carry COURIER_CRITERIA when a set is pinned")
	}

	bare, err := BuildCoordinatorPod(run, lane, PodConfig{Image: "example/test"})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() unpinned error = %v", err)
	}
	for _, env := range bare.Spec.Containers[0].Env {
		if env.Name == "COURIER_CRITERIA" {
			t.Fatal("an unpinned pod must not carry COURIER_CRITERIA")
		}
	}
}
