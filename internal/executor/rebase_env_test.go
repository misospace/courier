package executor

import (
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The rebase goal states the mechanical-only contract: the run documents the
// bounded execution path, never an open-ended model objective (#715).
func TestGoalCarriesRebaseContract(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeRebase,
			Ref:  42,
		},
	}
	goal, err := Goal(run)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Mechanically rebase PR #42",
		"trusted deterministic execution",
		"provably mechanical conflicts",
		"force-with-lease",
		"never merges",
		"never edits code semantics",
		"stops the run for a human decision",
		"never opens, retargets, or creates a pull request or branch",
	} {
		if !strings.Contains(goal, required) {
			t.Fatalf("goal missing %q:\n%s", required, goal)
		}
	}
}

// The environment reconstruction accepts rebase mode and keeps ref semantics
// identical to the other PR-repair modes.
func TestInvocationFromEnvAcceptsRebase(t *testing.T) {
	inv, err := InvocationFromEnv(mapGetter(map[string]string{
		"COURIER_RUN_NAME":      "run",
		"COURIER_RUN_NAMESPACE": "default",
		"COURIER_MODE":          "rebase",
		"COURIER_REPO":          "acme/widgets",
		"COURIER_HEAD_REPO":     "acme/widgets",
		"COURIER_HEAD_SHA":      "abc123",
		"COURIER_REF":           "42",
		"COURIER_BRANCH":        "pr-branch",
		"COURIER_MODEL":         "litellm/qwen3.8-27b",
		"COURIER_WORKSPACE":     "/workspaces/rebase-run",
		"COURIER_GOAL":          "goal",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if inv.Mode != courierv1alpha1.ModeRebase || inv.Ref != 42 || inv.Branch != "pr-branch" {
		t.Fatalf("invocation = %#v", inv)
	}
}

func TestInvocationFromEnvKeepsRebaseHeadIdentity(t *testing.T) {
	// The head identity is the lease input of the rebase publication; the
	// reconstruction must carry it verbatim (#715).
	inv, err := InvocationFromEnv(mapGetter(map[string]string{
		"COURIER_RUN_NAME":      "run",
		"COURIER_RUN_NAMESPACE": "default",
		"COURIER_MODE":          "rebase",
		"COURIER_REPO":          "acme/widgets",
		"COURIER_HEAD_REPO":     "acme/widgets",
		"COURIER_HEAD_SHA":      "abc123",
		"COURIER_REF":           "42",
		"COURIER_BRANCH":        "pr-branch",
		"COURIER_MODEL":         "litellm/qwen3.8-27b",
		"COURIER_GOAL":          "goal",
		"COURIER_WORKSPACE":     "/workspaces/rebase-run",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if inv.HeadSHA != "abc123" || inv.HeadRepo != "acme/widgets" {
		t.Fatalf("head identity = %q/%q, want abc123/acme/widgets", inv.HeadRepo, inv.HeadSHA)
	}
}

func mapGetter(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}
