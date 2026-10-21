package executor

import (
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// TestGoalWithoutRepairContextIsUnchanged pins that an ordinary run's goal
// carries no repair contract at all: Goal is called for every run, so the
// repair extension must leave a run without a repair context byte-identical
// to the ordinary goal.
func TestGoalWithoutRepairContextIsUnchanged(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Ref:  7,
		},
		Status: courierv1alpha1.CoderRunStatus{Branch: "courier/acme/widgets/issue-7"},
	}
	goal, err := Goal(run)
	if err != nil {
		t.Fatalf("Goal() error = %v", err)
	}
	for _, fragment := range []string{
		"Repair contract",
		"repair attempt",
		"Failed configured mechanical checks",
		"Bounded check output",
		"Durable checkpoint",
	} {
		if strings.Contains(goal, fragment) {
			t.Fatalf("ordinary goal carries repair text %q: %q", fragment, goal)
		}
	}

	// The same run with an empty repair context renders the ordinary goal
	// plus the contract: the extension is driven by the context's presence,
	// and it is appended to the ordinary goal rather than replacing it.
	withEmpty := run.DeepCopy()
	withEmpty.Status.CIRepair = &courierv1alpha1.CIRepairContext{}
	emptyGoal, err := Goal(withEmpty)
	if err != nil {
		t.Fatalf("Goal(empty repair context) error = %v", err)
	}
	if emptyGoal == goal {
		t.Fatalf("an empty repair context must still render the repair contract, got the ordinary goal: %q", emptyGoal)
	}
	if !strings.HasPrefix(emptyGoal, goal) {
		t.Fatalf("the repair contract must be appended to the ordinary goal, got %q", emptyGoal)
	}
}

// TestGoalCarriesBoundedRepairContract pins the repair contract a resumed
// coordinator receives (#494): the published head, the attempt, the failed
// configured mechanical checks and their bounded log evidence, the durable
// checkpoint, and the instruction to fix, revalidate, and push to the same
// run branch. It also pins the refusals — no second pull request, no
// re-planning, no waiting for CI — and the absence of any claim that an
// earlier conversation continues.
func TestGoalCarriesBoundedRepairContract(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Ref:  7,
		},
		Status: courierv1alpha1.CoderRunStatus{
			Branch: "courier/acme/widgets/issue-7",
			PR:     "42",
			CIRepair: &courierv1alpha1.CIRepairContext{
				Head:         "abc123",
				Attempt:      2,
				FailedChecks: []string{"unit", "lint"},
				Logs:         "--- unit ---\nboom\n--- lint ---\nstyle\n",
			},
			Checkpoint: &courierv1alpha1.Checkpoint{
				Plan: "1. repair the failing assertion",
				CompletedBriefs: []courierv1alpha1.Brief{
					{ID: "b1", Summary: "implement the fix", Commit: "def456"},
					{ID: "b2", Summary: "cover the regression", Commit: ""},
				},
			},
		},
	}
	goal, err := Goal(run)
	if err != nil {
		t.Fatalf("Goal() error = %v", err)
	}
	for _, fragment := range []string{
		"Repair contract",
		"bounded repair of already-published work",
		"abc123",
		"repair attempt 2",
		"repair budget",
		"Failed configured mechanical checks: unit, lint",
		"--- unit ---",
		"boom",
		"--- lint ---",
		"style",
		"Durable checkpoint",
		"only durable state of the earlier work",
		"1. repair the failing assertion",
		"- b1: implement the fix (commit: def456)",
		"- b2: cover the regression",
		"re-run the relevant local validation",
		"the same run branch (courier/acme/widgets/issue-7)",
		"declare the outcome exactly as an ordinary run does",
		"Do not: open a second pull request",
		"re-read or re-plan the original issue",
		"wait for CI or a review before pushing",
		"treat this repair as a fresh task",
		"no earlier conversation continues",
	} {
		if !strings.Contains(goal, fragment) {
			t.Fatalf("repair goal %q missing fragment %q", goal, fragment)
		}
	}
	for _, forbidden := range []string{
		"continue where",
		"resume the conversation",
		"as you did before",
	} {
		if strings.Contains(goal, forbidden) {
			t.Fatalf("repair goal %q claims a conversational resume: %q", goal, forbidden)
		}
	}
}

// TestGoalBoundsTheRepairEvidence pins the bounds of the repair contract: the
// failed-check evidence and the durable checkpoint are cut, and every cut is
// marked so the coordinator can tell a cut context from a complete one.
func TestGoalBoundsTheRepairEvidence(t *testing.T) {
	logs := strings.Repeat("x", repairLogMaxBytes*2)
	plan := strings.Repeat("p", repairPlanMaxBytes*2)
	summary := strings.Repeat("s", repairBriefMaxBytes*4)
	run := &courierv1alpha1.CoderRun{
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Ref:  7,
		},
		Status: courierv1alpha1.CoderRunStatus{
			CIRepair: &courierv1alpha1.CIRepairContext{
				Head:         "abc123",
				Attempt:      1,
				FailedChecks: []string{"unit"},
				Logs:         logs,
			},
			Checkpoint: &courierv1alpha1.Checkpoint{
				Plan: plan,
				CompletedBriefs: []courierv1alpha1.Brief{
					{ID: "b1", Summary: summary, Commit: "def456"},
				},
			},
		},
	}
	goal, err := Goal(run)
	if err != nil {
		t.Fatalf("Goal() error = %v", err)
	}
	if len(goal) > repairLogMaxBytes+repairCheckpointMaxBytes+4096 {
		t.Fatalf("repair goal is %d bytes, want the evidence and the checkpoint bounded", len(goal))
	}
	if !strings.Contains(goal, repairTruncated) {
		t.Fatalf("repair goal %q must mark the cut evidence", goal)
	}
	// The bounded evidence is bounded: the cut marker ends the evidence, so
	// the unbounded source text is not carried whole.
	if strings.Contains(goal, logs) {
		t.Fatalf("repair goal carries the whole unbounded log evidence")
	}
	if strings.Contains(goal, plan) {
		t.Fatalf("repair goal carries the whole unbounded plan")
	}
}

// TestGoalRendersAnEmptyCheckpoint pins that a repair without a checkpoint
// still renders the checkpoint section: the plan may be empty and no brief
// may have been completed, and the coordinator must be told that rather than
// left to guess.
func TestGoalRendersAnEmptyCheckpoint(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeFixPR,
			Ref:  9,
		},
		Status: courierv1alpha1.CoderRunStatus{
			CIRepair: &courierv1alpha1.CIRepairContext{
				Head:    "abc123",
				Attempt: 1,
			},
		},
	}
	goal, err := Goal(run)
	if err != nil {
		t.Fatalf("Goal() error = %v", err)
	}
	for _, fragment := range []string{
		"Durable checkpoint",
		"Plan:",
		"(empty)",
		"Completed briefs:",
		"(none recorded)",
		"(no output was published for the failed checks)",
		"(none recorded)",
	} {
		if !strings.Contains(goal, fragment) {
			t.Fatalf("repair goal %q missing fragment %q", goal, fragment)
		}
	}
}
