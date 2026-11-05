package executor

import (
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		"never resolves a two-sided code conflict",
		"stops the run for a human decision",
		"never opens, retargets, or creates a pull request or branch",
	} {
		if !strings.Contains(goal, required) {
			t.Fatalf("goal missing %q:\n%s", required, goal)
		}
	}
}

// The assist-enabled rebase goal authorizes exactly the one bounded
// model-assisted attempt the engine makes, instead of forbidding the
// resolution the assist is for: the goal travels to the assist model as
// context, and a model told the work is forbidden can legitimately
// refuse it (#732). The assist-disabled rendering keeps the mechanical-only
// contract byte-for-byte, so callers without assist configuration — the
// secure launcher among them — are unchanged.
func TestGoalCarriesRebaseAssistContract(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeRebase,
			Ref:  42,
		},
	}
	assisted, err := GoalWithContributionAndRebaseAssist(run, ContributionExecution{}, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Mechanically rebase PR #42",
		"trusted deterministic execution",
		"force-with-lease",
		"provably mechanical conflicts (one-sided edits and comment-only overlaps)",
		"one model-assisted resolution attempt through the configured coordinator model",
		"validates the answer before use",
		"stops the run for a human decision with the conflict summary",
		"never merges",
		"never opens, retargets, or creates a pull request or branch",
	} {
		if !strings.Contains(assisted, required) {
			t.Fatalf("assisted goal missing %q:\n%s", required, assisted)
		}
	}
	for _, forbidden := range []string{
		"never resolves a two-sided code conflict",
		"never edits code semantics",
	} {
		if strings.Contains(assisted, forbidden) {
			t.Fatalf("assisted goal contradicts the authorized resolution with %q:\n%s", forbidden, assisted)
		}
	}

	mechanical, err := GoalWithContributionAndRebaseAssist(run, ContributionExecution{}, false)
	if err != nil {
		t.Fatal(err)
	}
	goal, err := Goal(run)
	if err != nil {
		t.Fatal(err)
	}
	if mechanical != goal {
		t.Fatalf("assist-disabled goal drifted from the mechanical-only goal:\n%q\nvs\n%q", mechanical, goal)
	}
}

// The invocation variant carries the same goal rendering split: the
// assist-enabled invocation's goal authorizes the model-assisted attempt,
// and the assist-disabled invocation's goal is identical to the plain one.
func TestNewInvocationCarriesRebaseAssistGoal(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeRebase,
			Ref:  42,
		},
	}
	lane := &courierv1alpha1.LaneProfile{
		Spec: courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "m1"}},
	}
	assisted, err := NewInvocationWithContributionAndRebaseAssist(run, lane, "/workspace", ContributionExecution{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(assisted.Goal, "one model-assisted resolution attempt") {
		t.Fatalf("assisted invocation goal does not authorize the attempt:\n%s", assisted.Goal)
	}
	if strings.Contains(assisted.Goal, "never resolves a two-sided code conflict") {
		t.Fatalf("assisted invocation goal still forbids the authorized resolution:\n%s", assisted.Goal)
	}
	mechanical, err := NewInvocationWithContributionAndRebaseAssist(run, lane, "/workspace", ContributionExecution{}, false)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := NewInvocation(run, lane, "/workspace")
	if err != nil {
		t.Fatal(err)
	}
	if mechanical.Goal != plain.Goal {
		t.Fatalf("assist-disabled invocation goal drifted from the plain goal:\n%q\nvs\n%q", mechanical.Goal, plain.Goal)
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
