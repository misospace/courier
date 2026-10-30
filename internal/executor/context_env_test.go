package executor

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// contextEnvValues renders an Invocation's run-context block as the map
// InvocationFromEnv reads, so every test here drives the exact transport the
// operator's pod rendering and the control pod's reconstruction share.
func contextEnvValues(t *testing.T, inv Invocation) map[string]string {
	t.Helper()
	values := map[string]string{}
	for _, env := range RunContextEnvironment(inv) {
		values[env.Name] = env.Value
	}
	return values
}

// TestNewInvocationCarriesLaneContextPolicy pins the invocation contract for
// #636: the lane's declared context policy rides the invocation, deep-copied
// so a later mutation of the LaneProfile object cannot change what the run
// was admitted with, and stays nil when the lane declares none.
func TestNewInvocationCarriesLaneContextPolicy(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-ctx", Namespace: "courier-system"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  7,
		},
	}
	policy := &courierv1alpha1.ContextPolicy{
		CompactAtPercent:  60,
		OutputReservation: 4096,
		Roles: map[string]courierv1alpha1.ContextBudget{
			"coordinator": {MaxInputTokens: 128000},
			"coder":       {MaxInputTokens: 32000, CompactAtPercent: 50},
		},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "courier-system"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles:   map[string]string{"coordinator": "m1", "coder": "m2"},
			Framing: "lane framing",
			Context: policy,
		},
	}
	inv, err := NewInvocation(run, lane, "/workspace")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inv.Context, policy) {
		t.Fatalf("invocation context = %+v, want %+v", inv.Context, policy)
	}
	// Mutating the lane's policy after admission must not change the
	// invocation: the run carries its own pinned copy.
	policy.Roles["coordinator"] = courierv1alpha1.ContextBudget{MaxInputTokens: 1}
	if inv.Context.Roles["coordinator"].MaxInputTokens != 128000 {
		t.Fatalf("invocation context aliases the lane's policy: %+v", inv.Context)
	}

	bare, err := NewInvocation(run, &courierv1alpha1.LaneProfile{
		Spec: courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "m1"}},
	}, "/workspace")
	if err != nil {
		t.Fatal(err)
	}
	if bare.Context != nil {
		t.Fatalf("a lane with no context policy gave the invocation %+v, want nil", bare.Context)
	}
}

// TestRunContextEnvironmentCarriesContextPolicy pins that the declared
// policy travels as COURIER_CONTEXT_JSON and only when one is pinned: its
// absence is meaningful to the reconstruction boundary, never an empty
// stand-in.
func TestRunContextEnvironmentCarriesContextPolicy(t *testing.T) {
	inv := Invocation{
		RunName: "r", Namespace: "n", Ref: 1, Goal: "g", Model: "m", Workspace: "/w",
		Mode: courierv1alpha1.ModeResolveIssue,
		Context: &courierv1alpha1.ContextPolicy{
			CompactAtPercent:  60,
			OutputReservation: 4096,
			Roles:             map[string]courierv1alpha1.ContextBudget{"coordinator": {MaxInputTokens: 128000}},
		},
	}
	seen := map[string]string{}
	for _, env := range RunContextEnvironment(inv) {
		seen[env.Name] = env.Value
	}
	raw, ok := seen[EnvContextPolicy]
	if !ok {
		t.Fatalf("COURIER_CONTEXT_JSON missing from %v", seen)
	}
	var decoded courierv1alpha1.ContextPolicy
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("COURIER_CONTEXT_JSON is not a context policy: %v", err)
	}
	if !reflect.DeepEqual(decoded, *inv.Context) {
		t.Fatalf("COURIER_CONTEXT_JSON = %+v, want %+v", decoded, *inv.Context)
	}

	delete(seen, EnvContextPolicy)
	inv.Context = nil
	for _, env := range RunContextEnvironment(inv) {
		if env.Name == EnvContextPolicy {
			t.Fatal("a nil policy must not emit COURIER_CONTEXT_JSON")
		}
	}
}

// TestContextPolicyRoundTripsThroughEnv pins the restart contract (#636):
// the operator pins the policy into the pod env at admission, and a
// restarted control pod reconstructs the identical policy from that pinned
// environment — never a re-read of the live LaneProfile.
func TestContextPolicyRoundTripsThroughEnv(t *testing.T) {
	inv := Invocation{
		RunName: "r", Namespace: "n", Ref: 1, Goal: "g", Model: "m", Workspace: "/w",
		Mode: courierv1alpha1.ModeResolveIssue,
		Context: &courierv1alpha1.ContextPolicy{
			CompactAtPercent:  60,
			OutputReservation: 4096,
			Roles: map[string]courierv1alpha1.ContextBudget{
				"coordinator": {MaxInputTokens: 128000},
				"coder":       {OutputReservation: 2048},
			},
		},
	}
	values := contextEnvValues(t, inv)
	first, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	// A restart is a fresh reconstruction from the same pinned env.
	second, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("restart reconstruction diverged:\n got %+v\nwant %+v", second, first)
	}
	if !reflect.DeepEqual(first.Context, inv.Context) {
		t.Fatalf("reconstructed context = %+v, want %+v", first.Context, inv.Context)
	}
}

// TestInvocationFromEnvRejectsMalformedContextPolicy pins the fail-closed
// reconstruction: a malformed policy is a configuration failure the control
// pod must surface, never a session that silently runs unbudgeted.
func TestInvocationFromEnvRejectsMalformedContextPolicy(t *testing.T) {
	values := map[string]string{
		"COURIER_RUN_NAME": "r", "COURIER_RUN_NAMESPACE": "n", "COURIER_MODE": "resolve-issue",
		"COURIER_REF": "1", "COURIER_GOAL": "g", "COURIER_MODEL": "m", "COURIER_WORKSPACE": "/w",
		EnvContextPolicy: "{not a policy",
	}
	_, err := InvocationFromEnv(func(name string) string { return values[name] })
	if !errors.Is(err, ErrInvalidContextPolicy) {
		t.Fatalf("malformed policy error = %v, want ErrInvalidContextPolicy", err)
	}

	// The legacy environment keeps its shape: no policy, no error.
	delete(values, EnvContextPolicy)
	legacy, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Context != nil {
		t.Fatalf("legacy env reconstructed context %+v, want nil", legacy.Context)
	}
}
