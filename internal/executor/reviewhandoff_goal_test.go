package executor

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The fix-pr review handoff (#755): a validated review-handoff/v1 payload
// read by trusted control arrives as extra goal context, framed as untrusted
// task data, bounded, and byte-identical to the ordinary goal when absent.

func handoffTestRun(mode courierv1alpha1.Mode) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "runs"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: mode, Repo: "acme/widgets", Ref: 7, Lane: "local",
		},
	}
}

func handoffTestLane() *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "runs"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles: map[string]string{"coordinator": "test/coordinator"},
		},
	}
}

const handoffPayload = `{"schema":"review-handoff/v1","review_id":"r1","head_sha":"cafe1234","verdict":"request_changes","review_result":"issues","required_checks":"complete","items":[{"severity":"blocker","category":"bug","path":"a.go","message":"nil deref"}],"items_total":1,"items_truncated":false}`

func TestFixPRGoalCarriesReviewHandoffVerbatim(t *testing.T) {
	goal, err := GoalWithReviewHandoff(handoffTestRun(courierv1alpha1.ModeFixPR), ContributionExecution{}, handoffPayload)
	if err != nil {
		t.Fatalf("render fix-pr goal with handoff: %v", err)
	}
	if !strings.Contains(goal, handoffPayload) {
		t.Fatalf("goal lost the handoff verbatim:\n%s", goal)
	}
	for _, want := range []string{
		"review-handoff/v1",
		"untrusted task data",
		"resolve every blocker and major finding",
		"handle minor findings per repository policy",
		"verify each finding against the current head",
	} {
		if !strings.Contains(goal, want) {
			t.Fatalf("goal omits %q:\n%s", want, goal)
		}
	}
}

func TestEmptyReviewHandoffKeepsGoalByteIdentical(t *testing.T) {
	plain, err := Goal(handoffTestRun(courierv1alpha1.ModeFixPR))
	if err != nil {
		t.Fatalf("render ordinary fix-pr goal: %v", err)
	}
	withEmpty, err := GoalWithReviewHandoff(handoffTestRun(courierv1alpha1.ModeFixPR), ContributionExecution{}, "   ")
	if err != nil {
		t.Fatalf("render fix-pr goal with empty handoff: %v", err)
	}
	if withEmpty != plain {
		t.Fatalf("an empty handoff changed the goal:\n got %q\nwant %q", withEmpty, plain)
	}
}

func TestReviewHandoffIgnoredOutsideFixPR(t *testing.T) {
	plain, err := Goal(handoffTestRun(courierv1alpha1.ModeResolveIssue))
	if err != nil {
		t.Fatalf("render resolve-issue goal: %v", err)
	}
	withHandoff, err := GoalWithReviewHandoff(handoffTestRun(courierv1alpha1.ModeResolveIssue), ContributionExecution{}, handoffPayload)
	if err != nil {
		t.Fatalf("render resolve-issue goal with handoff: %v", err)
	}
	if withHandoff != plain {
		t.Fatalf("a non-fix-pr goal absorbed the handoff:\n got %q\nwant %q", withHandoff, plain)
	}
}

func TestReviewHandoffGoalIsBounded(t *testing.T) {
	oversized := `{"schema":"review-handoff/v1","review_id":"r1","head_sha":"cafe1234","x":"` + strings.Repeat("y", 2*reviewHandoffMaxBytes) + `"}`
	goal, err := GoalWithReviewHandoff(handoffTestRun(courierv1alpha1.ModeFixPR), ContributionExecution{}, oversized)
	if err != nil {
		t.Fatalf("render fix-pr goal with oversized handoff: %v", err)
	}
	if !strings.Contains(goal, repairTruncated) {
		t.Fatalf("the oversized handoff was not marked truncated:\n%s", goal)
	}
	if strings.Contains(goal, strings.Repeat("y", reviewHandoffMaxBytes+1)) {
		t.Fatalf("the oversized handoff was not cut to its bound")
	}
}

func TestNewInvocationWithReviewHandoffRendersGoalIntoEnv(t *testing.T) {
	invocation, err := NewInvocationWithReviewHandoff(handoffTestRun(courierv1alpha1.ModeFixPR), handoffTestLane(), "/workspace", ContributionExecution{}, handoffPayload)
	if err != nil {
		t.Fatalf("build invocation with handoff: %v", err)
	}
	if !strings.Contains(invocation.Goal, handoffPayload) {
		t.Fatalf("invocation goal omits the handoff:\n%s", invocation.Goal)
	}
	env := map[string]string{}
	for _, variable := range RunContextEnvironment(invocation) {
		env[variable.Name] = variable.Value
	}
	if env["COURIER_GOAL"] != invocation.Goal {
		t.Fatalf("COURIER_GOAL differs from the rendered goal")
	}
	// The handoff rides the goal only: no new environment field exists.
	for name := range env {
		if strings.Contains(name, "HANDOFF") {
			t.Fatalf("unexpected handoff environment field %q", name)
		}
	}
}
