package controller

import (
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// TestNativeResultKindOfRecognizesBudgetExhausted pins the #901 vocabulary:
// a handoff carrying budget_exhausted is a recognized trusted kind, and an
// unknown kind still classifies nothing.
func TestNativeResultKindOfRecognizesBudgetExhausted(t *testing.T) {
	if got := nativeResultKindOf(terminationMessage{ResultKind: "budget_exhausted"}); got != courierv1alpha1.NativeResultBudgetExhausted {
		t.Fatalf("nativeResultKindOf(budget_exhausted) = %q, want budget_exhausted", got)
	}
	if got := nativeResultKindOf(terminationMessage{ResultKind: " genuinely_exhausted "}); got != "" {
		t.Fatalf("an unknown kind classified as %q", got)
	}
}

// TestCategoryForNativeResultKindBudgetExhausted pins the #901 diagnostic
// mapping: budget exhaustion is a terminal verdict on the run's own
// coordination, so it classifies as a workload failure, not the recoverable
// capacity-exhaustion category.
func TestCategoryForNativeResultKindBudgetExhausted(t *testing.T) {
	if got := categoryForNativeResultKind(courierv1alpha1.NativeResultBudgetExhausted); got != courierv1alpha1.TerminalDiagnosticCategoryWorkloadFailure {
		t.Fatalf("budget_exhausted category = %q, want %q", got, courierv1alpha1.TerminalDiagnosticCategoryWorkloadFailure)
	}
	if got := categoryForNativeResultKind(courierv1alpha1.NativeResultCapacityExhausted); got != courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion {
		t.Fatalf("capacity_exhausted category = %q, want %q", got, courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion)
	}
}

// TestPhaseForNativeResultBudgetExhaustedIsTerminal pins the #901 phase
// mapping: budget exhaustion is not recoverable capacity exhaustion, so it
// rides the default terminal failure verdict rather than BlockedExternal.
func TestPhaseForNativeResultBudgetExhaustedIsTerminal(t *testing.T) {
	if got := phaseForNativeResult(courierv1alpha1.ModeResolveIssue, courierv1alpha1.NativeResultBudgetExhausted); got != courierv1alpha1.PhaseFailed {
		t.Fatalf("budget_exhausted phase = %q, want %q", got, courierv1alpha1.PhaseFailed)
	}
}
