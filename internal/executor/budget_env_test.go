package executor

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestNewInvocationCarriesLaneBudget pins the invocation contract for #901:
// the lane's declared coordination budget rides the invocation, deep-copied
// so a later mutation of the LaneProfile object cannot change what the run
// was admitted with, and stays nil when the lane declares none.
func TestNewInvocationCarriesLaneBudget(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-budget", Namespace: "courier-system"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  7,
		},
	}
	budget := &courierv1alpha1.BudgetPolicy{MaxToolCalls: 400, WallClockSeconds: 3600}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "courier-system"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles:  map[string]string{"coordinator": "m1"},
			Budget: budget,
		},
	}
	inv, err := NewInvocation(run, lane, "/workspace")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inv.Budget, budget) {
		t.Fatalf("invocation budget = %+v, want %+v", inv.Budget, budget)
	}
	budget.MaxToolCalls = 1
	if inv.Budget.MaxToolCalls != 400 {
		t.Fatalf("invocation budget aliases the lane's policy: %+v", inv.Budget)
	}

	bare, err := NewInvocation(run, &courierv1alpha1.LaneProfile{
		Spec: courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "m1"}},
	}, "/workspace")
	if err != nil {
		t.Fatal(err)
	}
	if bare.Budget != nil {
		t.Fatalf("a lane with no budget gave the invocation %+v, want nil", bare.Budget)
	}
}

// TestRunContextEnvironmentCarriesBudgetPolicy pins that the declared budget
// travels as COURIER_BUDGET_JSON and only when one is pinned: its absence is
// meaningful to the reconstruction boundary, never an empty stand-in.
func TestRunContextEnvironmentCarriesBudgetPolicy(t *testing.T) {
	inv := Invocation{
		RunName: "r", Namespace: "n", Ref: 1, Goal: "g", Model: "m", Workspace: "/w",
		Mode:   courierv1alpha1.ModeResolveIssue,
		Budget: &courierv1alpha1.BudgetPolicy{MaxToolCalls: 400, WallClockSeconds: 3600},
	}
	seen := map[string]string{}
	for _, env := range RunContextEnvironment(inv) {
		seen[env.Name] = env.Value
	}
	raw, ok := seen[EnvBudgetPolicy]
	if !ok {
		t.Fatalf("COURIER_BUDGET_JSON missing from %v", seen)
	}
	var decoded courierv1alpha1.BudgetPolicy
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("COURIER_BUDGET_JSON is not a budget policy: %v", err)
	}
	if !reflect.DeepEqual(decoded, *inv.Budget) {
		t.Fatalf("COURIER_BUDGET_JSON = %+v, want %+v", decoded, *inv.Budget)
	}

	inv.Budget = nil
	for _, env := range RunContextEnvironment(inv) {
		if env.Name == EnvBudgetPolicy {
			t.Fatal("a nil budget must not emit COURIER_BUDGET_JSON")
		}
	}
}

// TestBudgetPolicyRoundTripsThroughEnv pins the restart contract (#901): the
// operator pins the budget into the pod env at admission, and a restarted
// control pod reconstructs the identical budget from that pinned
// environment — never a re-read of the live LaneProfile.
func TestBudgetPolicyRoundTripsThroughEnv(t *testing.T) {
	inv := Invocation{
		RunName: "r", Namespace: "n", Ref: 1, Goal: "g", Model: "m", Workspace: "/w",
		Mode:   courierv1alpha1.ModeResolveIssue,
		Budget: &courierv1alpha1.BudgetPolicy{MaxToolCalls: 400, WallClockSeconds: 3600},
	}
	values := contextEnvValues(t, inv)
	first, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	second, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("restart reconstruction diverged:\n got %+v\nwant %+v", second, first)
	}
	if !reflect.DeepEqual(first.Budget, inv.Budget) {
		t.Fatalf("reconstructed budget = %+v, want %+v", first.Budget, inv.Budget)
	}
}

// TestInvocationFromEnvRejectsMalformedBudgetPolicy pins the fail-closed
// reconstruction: a malformed budget is a configuration failure the control
// pod must surface, never a session that silently runs unbudgeted.
func TestInvocationFromEnvRejectsMalformedBudgetPolicy(t *testing.T) {
	values := map[string]string{
		"COURIER_RUN_NAME": "r", "COURIER_RUN_NAMESPACE": "n", "COURIER_MODE": "resolve-issue",
		"COURIER_REF": "1", "COURIER_GOAL": "g", "COURIER_MODEL": "m", "COURIER_WORKSPACE": "/w",
		EnvBudgetPolicy: "{not a budget",
	}
	_, err := InvocationFromEnv(func(name string) string { return values[name] })
	if !errors.Is(err, ErrInvalidBudgetPolicy) {
		t.Fatalf("malformed budget error = %v, want ErrInvalidBudgetPolicy", err)
	}

	// The legacy environment keeps its shape: no budget, no error.
	delete(values, EnvBudgetPolicy)
	legacy, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Budget != nil {
		t.Fatalf("legacy env reconstructed budget %+v, want nil", legacy.Budget)
	}
}
