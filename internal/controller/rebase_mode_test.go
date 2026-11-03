package controller

import (
	"context"
	"errors"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A rebase run resolves the same live PR identity a fix-pr run does — head
// repository, branch, commit, and the live base ref — through the injected
// resolver, never a synthesized branch (#715).
func TestResolveRunBranchRebase(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{UID: "uid-1", Name: "run", Namespace: "default"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeRebase, Source: "dispatch", WorkItemID: "item", Repo: "acme/widgets", Ref: 42, Lane: "rebase-lane",
		},
	}
	resolver := ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (HeadRef, error) {
		return HeadRef{Repo: "acme/widgets", Branch: "pr-branch", SHA: "abc123", BaseRef: "main"}, nil
	})
	head, err := resolveRunBranch(context.Background(), run, resolver)
	if err != nil {
		t.Fatal(err)
	}
	want := HeadRef{Repo: "acme/widgets", Branch: "pr-branch", SHA: "abc123", BaseRef: "main"}
	if head != want {
		t.Fatalf("head = %#v, want %#v", head, want)
	}
}

// A rebase run fails closed without a resolver, exactly as fix-pr does: the
// launch must never synthesize a branch or guess a base for a mechanical
// replay.
func TestResolveRunBranchRebaseRequiresResolver(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{UID: "uid-1", Name: "run", Namespace: "default"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeRebase, Source: "dispatch", WorkItemID: "item", Repo: "acme/widgets", Ref: 42, Lane: "rebase-lane",
		},
	}
	if _, err := resolveRunBranch(context.Background(), run, nil); !errors.Is(err, ErrPRHeadResolverRequired) {
		t.Fatalf("err = %v, want %v", err, ErrPRHeadResolverRequired)
	}
}

// The rebase endings map onto the phases exactly like the fix-pr contract:
// a published or up-to-date attempt hands to world verification, and the
// report for a Failed attempt stays retryable by the source rather than
// parking a human for writer contention (#715).
func TestRebasePhaseAndReportClassification(t *testing.T) {
	if got := phaseForExit(courierv1alpha1.ModeRebase, 3, ""); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("declared no-change phase = %q, want Verifying", got)
	}
	if got := phaseForExit(courierv1alpha1.ModeRebase, 2, ""); got != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("semantic conflict phase = %q, want NeedsHuman", got)
	}
	if got := phaseForExit(courierv1alpha1.ModeRebase, 0, ""); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("published phase = %q, want Verifying", got)
	}
	state := stateForTerminalPhase(courierv1alpha1.PhaseFailed, courierv1alpha1.ModeRebase, terminalLifecycleIntent{})
	if state != "in-progress" {
		t.Fatalf("failed rebase state = %q, want in-progress (retryable by source)", state)
	}
	lifecycle := lifecycleForPhase(courierv1alpha1.PhaseFailed, courierv1alpha1.ModeRebase, state, "", nil, terminalLifecycleIntent{})
	if lifecycle.Result != "failed" {
		t.Fatalf("failed rebase report = %q, want failed", lifecycle.Result)
	}
	// A semantic conflict reports through NeedsHuman, the human-park path.
	blocked := lifecycleForPhase(courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.ModeRebase, "needs-human", "", nil, terminalLifecycleIntent{error: "semantic conflict in code.go"})
	if blocked.Result != "blocked" || blocked.Error != "semantic conflict in code.go" {
		t.Fatalf("semantic conflict report = %#v, want blocked with the conflict", blocked)
	}
}
