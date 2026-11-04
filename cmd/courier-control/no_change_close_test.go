package main

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
)

// TestNoChangeProposalForGatesModeAndKind proves the grounded close proposal
// (#720) is durable only for a resolve-issue no-change ending: the same
// proposal on another mode or another kind is dropped to nil.
func TestNoChangeProposalForGatesModeAndKind(t *testing.T) {
	proposal := &courierv1alpha1.NoChangeClose{
		Criteria: []courierv1alpha1.NoChangeCloseCriterion{{Criterion: "c", Path: "p", Excerpt: "an excerpt of at least twenty-four bytes long"}},
		Evidence: "done",
	}
	if got := noChangeProposalFor(courierv1alpha1.ModeResolveIssue, executor.HarnessResult{Kind: courierv1alpha1.NativeResultNoChange, NoChange: proposal}); got != proposal {
		t.Fatalf("resolve-issue no-change dropped the proposal: %#v", got)
	}
	cases := []struct {
		name string
		mode courierv1alpha1.Mode
		kind courierv1alpha1.NativeResultKind
	}{
		{"fix-pr keeps no proposal", courierv1alpha1.ModeFixPR, courierv1alpha1.NativeResultNoChange},
		{"contribute keeps no proposal", courierv1alpha1.ModeContribute, courierv1alpha1.NativeResultNoChange},
		{"resolve-issue published keeps no proposal", courierv1alpha1.ModeResolveIssue, courierv1alpha1.NativeResultPublished},
		{"resolve-issue needs-human keeps no proposal", courierv1alpha1.ModeResolveIssue, courierv1alpha1.NativeResultNeedsHuman},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := noChangeProposalFor(tc.mode, executor.HarnessResult{Kind: tc.kind, NoChange: proposal}); got != nil {
				t.Fatalf("%s carried a proposal: %#v", tc.name, got)
			}
		})
	}
}

// TestPersistTerminalResultStampsTheCloseProposal proves the trusted control
// process — never the parser — forges the fence and the instant: the
// proposal written through the broker carries the control incarnation's UID
// and the pre-exit clock, on the same patch as the terminal result.
func TestPersistTerminalResultStampsTheCloseProposal(t *testing.T) {
	stamp := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	previous := clockNow
	clockNow = func() time.Time { return stamp }
	defer func() { clockNow = previous }()

	identity := &controlIdentity{controlKubeUID: "control-kube-uid"}
	payload := terminationFor(executor.HarnessResult{
		Outcome: executor.OutcomeNoChangeNeeded,
		Kind:    courierv1alpha1.NativeResultNoChange,
		Reason:  `{"outcome":"no_change_needed","evidence":"already done"}`,
	})
	transport := &recordingTransport{}
	proposal := noChangeProposalFor(courierv1alpha1.ModeResolveIssue, executor.HarnessResult{
		Kind:     courierv1alpha1.NativeResultNoChange,
		NoChange: &courierv1alpha1.NoChangeClose{Criteria: []courierv1alpha1.NoChangeCloseCriterion{{Criterion: "c", Path: "docs/why.md", Excerpt: "an excerpt of at least twenty-four bytes long"}}, Evidence: "already done"},
	})
	persistTerminalResult(payload, proposal, harness.NewStatusReporter(transport, "control-kube-uid", "worker", 0), identity)

	if len(transport.patches) != 1 {
		t.Fatalf("patches = %d, want the single pre-exit write", len(transport.patches))
	}
	patch := transport.patches[0]
	if patch.TerminalResult == nil || patch.TerminalResult.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("terminal result = %#v", patch.TerminalResult)
	}
	if patch.NoChange == nil {
		t.Fatal("close proposal was not written beside the terminal result")
	}
	if patch.NoChange.CoordinatorPodUID != "control-kube-uid" || !patch.NoChange.At.Equal(&metav1.Time{Time: stamp}) {
		t.Fatalf("proposal fence = %#v, want the control incarnation UID and the pre-exit instant", patch.NoChange)
	}
	if patch.NoChange.Evidence != "already done" || len(patch.NoChange.Criteria) != 1 {
		t.Fatalf("proposal content = %#v", patch.NoChange)
	}
	// The caller's proposal must not be mutated by stamping.
	if proposal.CoordinatorPodUID != "" || !proposal.At.IsZero() {
		t.Fatalf("stamping mutated the caller's proposal: %#v", proposal)
	}
}
