package executor

import (
	"reflect"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The native control pod reconstructs its Invocation from the same
// environment contract the operator renders; a mismatch would silently
// change the run's goal or role bindings.
func TestInvocationFromEnvRoundTripsRunContext(t *testing.T) {
	inv := Invocation{
		RunName:   "run-1",
		Namespace: "runs",
		Mode:      courierv1alpha1.ModeFixPR,
		Repo:      "acme/widgets",
		HeadRepo:  "acme/widgets-fork",
		HeadSHA:   "abc123",
		Ref:       9,
		Branch:    "courier/acme/widgets/issue-9",
		Goal:      "Take over PR #9.",
		Model:     "test/coordinator",
		Roles:     map[string]string{"coordinator": "m1", "coder": "m2"},
		Framing:   "local lane",
		Workspace: "/workspace",
		Debug:     true,
	}
	values := map[string]string{}
	for _, env := range RunContextEnvironment(inv) {
		values[env.Name] = env.Value
	}
	decoded, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, inv) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", decoded, inv)
	}
}

func TestInvocationFromEnvRejectsBrokenContext(t *testing.T) {
	tests := map[string]map[string]string{
		"missing goal": {
			"COURIER_RUN_NAME": "r", "COURIER_RUN_NAMESPACE": "n", "COURIER_MODE": "resolve-issue",
			"COURIER_REF": "1", "COURIER_MODEL": "m", "COURIER_WORKSPACE": "/w",
		},
		"bad mode": {
			"COURIER_RUN_NAME": "r", "COURIER_RUN_NAMESPACE": "n", "COURIER_MODE": "weird",
			"COURIER_REF": "1", "COURIER_GOAL": "g", "COURIER_MODEL": "m", "COURIER_WORKSPACE": "/w",
		},
		"bad ref": {
			"COURIER_RUN_NAME": "r", "COURIER_RUN_NAMESPACE": "n", "COURIER_MODE": "resolve-issue",
			"COURIER_REF": "0", "COURIER_GOAL": "g", "COURIER_MODEL": "m", "COURIER_WORKSPACE": "/w",
		},
		"broken roles json": {
			"COURIER_RUN_NAME": "r", "COURIER_RUN_NAMESPACE": "n", "COURIER_MODE": "resolve-issue",
			"COURIER_REF": "1", "COURIER_GOAL": "g", "COURIER_MODEL": "m", "COURIER_WORKSPACE": "/w",
			"COURIER_ROLES_JSON": "{not json",
		},
	}
	for name, values := range tests {
		if _, err := InvocationFromEnv(func(key string) string { return values[key] }); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

// The legacy environment keeps its shape: the run-context block must be the
// same values either way, so the env contract stays one contract.
func TestEnvironmentWithConfigCarriesRunContext(t *testing.T) {
	inv := Invocation{RunName: "r", Namespace: "n", Ref: 1, Goal: "g", Model: "m", Workspace: "/w", Mode: courierv1alpha1.ModeResolveIssue}
	full := EnvironmentWithConfig(inv, "opencode", "url", "base", "main", "bin", "json", "term", "agent")
	seen := map[string]string{}
	for _, env := range full {
		seen[env.Name] = env.Value
	}
	for _, env := range RunContextEnvironment(inv) {
		if seen[env.Name] != env.Value {
			t.Fatalf("env %s = %q, run-context block says %q", env.Name, seen[env.Name], env.Value)
		}
	}
	if seen["COURIER_EXECUTOR"] != "opencode" {
		t.Fatalf("executor name lost: %v", seen)
	}
}
