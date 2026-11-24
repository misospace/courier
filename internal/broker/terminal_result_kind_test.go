package broker

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// TestValidTerminalResultAcceptsBudgetExhausted pins the #901 vocabulary:
// budget_exhausted is a trusted terminal kind the broker persists, while an
// unknown kind stays refused.
func TestValidTerminalResultAcceptsBudgetExhausted(t *testing.T) {
	exhausted := &courier.NativeResult{
		Kind:              courier.NativeResultBudgetExhausted,
		At:                metav1.Now(),
		CoordinatorPodUID: "pod-uid",
		Reason:            courier.NativeResultReason(courier.NativeResultBudgetExhausted),
	}
	if err := validTerminalResult(exhausted); err != nil {
		t.Fatalf("budget_exhausted refused: %v", err)
	}

	unknown := &courier.NativeResult{
		Kind:              courier.NativeResultKind("genuinely_done"),
		At:                metav1.Now(),
		CoordinatorPodUID: "pod-uid",
	}
	if err := validTerminalResult(unknown); err == nil {
		t.Fatal("an unknown terminal kind was accepted")
	}
}
