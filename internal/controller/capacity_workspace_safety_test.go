package controller

import (
	"context"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCapacityRecoveryRequiresTrustedWorkspaceSafety(t *testing.T) {
	for _, tc := range []struct {
		name      string
		evidence  *courierv1alpha1.WorkspaceSafetyEvidence
		secure    bool
		wantCalls int
	}{
		{name: "empty allows", evidence: &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"}, secure: true, wantCalls: 1},
		{name: "pushed matching remote head allows", evidence: &courierv1alpha1.WorkspaceSafetyEvidence{State: "pushed", Branch: "branch", Head: "sha", RemoteHead: "sha"}, secure: true, wantCalls: 1},
		{name: "dirty fails closed", evidence: &courierv1alpha1.WorkspaceSafetyEvidence{State: "dirty"}, secure: true},
		{name: "unknown fails closed", evidence: &courierv1alpha1.WorkspaceSafetyEvidence{State: "unknown"}, secure: true},
		{name: "legacy payload cannot prove safety", evidence: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
			run.UID, run.Spec.Source, run.Spec.WorkItemID = "uid", "fake", "work"
			run.Status.Branch, run.Status.WorkspaceSafety = "branch", tc.evidence
			run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
			run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: "Published"})
			c := capacityTestClient(t, admissionLane("pool-a", 1), run)
			s := &capacitySource{admissionSource: &admissionSource{}}
			r := &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), StatusWriter: fakeStatusWriter{client: c}}
			if tc.secure {
				r.Secure = &SecureControl{}
			}
			if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
				t.Fatal(err)
			}
			if s.prepareCalls != tc.wantCalls || len(s.settled) != tc.wantCalls {
				t.Fatalf("prepare=%d settle=%d, want %d", s.prepareCalls, len(s.settled), tc.wantCalls)
			}
		})
	}
}
