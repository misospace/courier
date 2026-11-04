package harness

import (
	"context"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

// TestParseOutcomeCarriesGroundedCloseProposal proves a no_change_needed
// declaration's optional close block (#720) parses into the bounded typed
// proposal with the declared evidence, and that the proposal is parsed only
// for that outcome shape.
func TestParseOutcomeCarriesGroundedCloseProposal(t *testing.T) {
	text := `{"outcome":"no_change_needed","evidence":"already implemented upstream","close":{"criteria":[{"criterion":"the flag exists","path":"docs/flags.md","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`
	outcome, proposal, err := parseOutcome(text)
	if err != nil {
		t.Fatalf("parseOutcome: %v", err)
	}
	if outcome != executor.OutcomeNoChangeNeeded {
		t.Fatalf("outcome = %q", outcome)
	}
	if proposal == nil || len(proposal.Criteria) != 1 {
		t.Fatalf("proposal = %#v, want one grounded row", proposal)
	}
	row := proposal.Criteria[0]
	if row.Criterion != "the flag exists" || row.Path != "docs/flags.md" ||
		row.Excerpt != "an excerpt of at least twenty-four bytes long" {
		t.Fatalf("row = %#v", row)
	}
	if proposal.Evidence != "already implemented upstream" {
		t.Fatalf("evidence = %q, want the declared free-form summary", proposal.Evidence)
	}
	if proposal.CoordinatorPodUID != "" || !proposal.At.IsZero() {
		t.Fatalf("parser must not forge the fence or the time: %#v", proposal)
	}
}

// TestParseOutcomeDropsMalformedCloseKeepsEnding proves a bad evidence row
// never costs the run its valid ending: every malformed or over-bound close
// shape drops the proposal while the plain no_change_needed declaration
// still parses, and a changes declaration carrying close stays invalid.
func TestParseOutcomeDropsMalformedCloseKeepsEnding(t *testing.T) {
	longCriterion := strings.Repeat("c", courierv1alpha1.MaxNoChangeCriterionBytes+1)
	longEvidence := strings.Repeat("e", courierv1alpha1.MaxNoChangeEvidenceBytes+1)
	tooManyRows := `[{"criterion":"c","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}]`
	for i := 0; i < courierv1alpha1.MaxNoChangeCriteria; i++ {
		tooManyRows = strings.TrimSuffix(tooManyRows, "]") + `,{"criterion":"c","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}` + `]`
	}
	tests := []struct {
		name string
		text string
	}{
		{"close is not an object", `{"outcome":"no_change_needed","evidence":"done","close":"grounded by hope"}`},
		{"criteria empty", `{"outcome":"no_change_needed","evidence":"done","close":{"criteria":[]}}`},
		{"criteria over bound", `{"outcome":"no_change_needed","evidence":"done","close":{"criteria":` + tooManyRows + `}}`},
		{"criterion blank", `{"outcome":"no_change_needed","evidence":"done","close":{"criteria":[{"criterion":"  ","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`},
		{"criterion over bound", `{"outcome":"no_change_needed","evidence":"done","close":{"criteria":[{"criterion":"` + longCriterion + `","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`},
		{"path blank", `{"outcome":"no_change_needed","evidence":"done","close":{"criteria":[{"criterion":"c","path":"","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`},
		{"excerpt under bound", `{"outcome":"no_change_needed","evidence":"done","close":{"criteria":[{"criterion":"c","path":"p","excerpt":"short"}]}}`},
		{"excerpt over bound", `{"outcome":"no_change_needed","evidence":"done","close":{"criteria":[{"criterion":"c","path":"p","excerpt":"` + strings.Repeat("x", courierv1alpha1.MaxNoChangeExcerptBytes+1) + `"}]}}`},
		{"evidence over bound", `{"outcome":"no_change_needed","evidence":"` + longEvidence + `","close":{"criteria":[{"criterion":"c","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome, proposal, err := parseOutcome(tt.text)
			if err != nil {
				t.Fatalf("a malformed close must not cost the ending: %v", err)
			}
			if outcome != executor.OutcomeNoChangeNeeded {
				t.Fatalf("outcome = %q, want the plain no_change_needed ending", outcome)
			}
			if proposal != nil {
				t.Fatalf("malformed close produced a proposal: %#v", proposal)
			}
		})
	}

	if _, _, err := parseOutcome(`{"outcome":"changes","close":{"criteria":[{"criterion":"c","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`); err == nil {
		t.Fatal("a changes declaration carrying close must be invalid")
	}
}

// TestFinishCarriesCloseProposalToTheResult proves finish hands the parsed
// proposal to trusted control on the no-change route only when the parser
// produced one; the other endings never carry it.
func TestFinishCarriesCloseProposalToTheResult(t *testing.T) {
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: &Gateway{BaseURL: "http://gateway.invalid"},
		Bindings:  testBindings(),
		Publisher: &mockPublisher{},
		Status:    NewStatusReporter(&recordingStatusTransport{}, testControlUID, testWorkerUID, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, proposal, err := parseOutcome(`{"outcome":"no_change_needed","evidence":"done","close":{"criteria":[{"criterion":"c","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`)
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeNoChangeNeeded, "done", proposal)
	if result.Kind != courierv1alpha1.NativeResultNoChange || result.NoChange != proposal {
		t.Fatalf("result = %#v, want the no-change ending carrying the proposal", result)
	}
	plain := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeNoChangeNeeded, "done", nil)
	if plain.NoChange != nil {
		t.Fatalf("plain ending carried a proposal: %#v", plain.NoChange)
	}
}

// TestTerminalResultRidesTheProposal proves the reporter puts a carried
// proposal on the same fenced patch as the terminal result, and that a nil
// proposal leaves the patch exactly the legacy shape.
func TestTerminalResultRidesTheProposal(t *testing.T) {
	transport := &recordingStatusTransport{}
	reporter := testReporter(transport)
	proposal := &courierv1alpha1.NoChangeClose{
		Criteria:          []courierv1alpha1.NoChangeCloseCriterion{{Criterion: "c", Path: "p", Excerpt: "an excerpt of at least twenty-four bytes long"}},
		Evidence:          "done",
		CoordinatorPodUID: testControlUID,
		At:                metav1Now(),
	}
	result := courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultNoChange,
		Reason:            "concluded without change",
		CoordinatorPodUID: testControlUID,
		At:                metav1Now(),
	}
	if err := reporter.TerminalResult(context.Background(), result, proposal); err != nil {
		t.Fatal(err)
	}
	sent := transport.sent()
	if len(sent) != 1 || sent[0].NoChange == nil || sent[0].NoChange.Criteria[0].Path != "p" ||
		sent[0].TerminalResult == nil || sent[0].TerminalResult.Kind != courierv1alpha1.NativeResultNoChange {
		t.Fatalf("sent = %#v, want result and proposal on one patch", sent)
	}

	legacy := &recordingStatusTransport{}
	if err := testReporter(legacy).TerminalResult(context.Background(), result, nil); err != nil {
		t.Fatal(err)
	}
	if got := legacy.sent(); len(got) != 1 || got[0].NoChange != nil {
		t.Fatalf("legacy-shaped ending wrote %#v, want no noChange field", got)
	}
}
