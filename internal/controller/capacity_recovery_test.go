package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
)

type capacitySource struct {
	*admissionSource
	settled    []source.CapacityHandoff
	prepareErr error
	settleErr  error
}

func (s *capacitySource) PrepareCapacityRecovery(_ context.Context, _ source.WorkItem, _ source.CapacityRecoveryAttempt) (source.CapacityRecoveryProof, error) {
	if s.prepareErr != nil {
		return source.CapacityRecoveryProof{}, s.prepareErr
	}
	return source.CapacityRecoveryProof{FenceToken: "source-fence", WorkIdentity: "work", SafetyProof: "remote-holds-partial-work", Branch: "branch", PR: "42"}, nil
}

func (s *capacitySource) SettleCapacityExhaustion(_ context.Context, _ source.WorkItem, handoff source.CapacityHandoff) error {
	s.settled = append(s.settled, handoff)
	return s.settleErr
}

func capacityTestClient(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courierv1alpha1.CoderRun{}, &courierv1alpha1.LaneProfile{}).WithRuntimeObjects(objects...).Build()
}

func TestCapacityRecoverySettlesIssueAndExistingPRFixWithFence(t *testing.T) {
	for _, mode := range []courierv1alpha1.Mode{courierv1alpha1.ModeResolveIssue, courierv1alpha1.ModeFixPR} {
		t.Run(string(mode), func(t *testing.T) {
			run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
			run.UID = "run-uid"
			run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = mode, "fake", "existing-work"
			run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
			run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: "Published"})
			lane := admissionLane("pool-a", 1)
			lane.Spec.CapacityGroup = "shared-pool"
			c := capacityTestClient(t, lane, run)
			s := &capacitySource{admissionSource: &admissionSource{}}
			r := &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), StatusWriter: fakeStatusWriter{client: c}, Now: func() time.Time { return time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC) }}
			got, err := r.Reconcile(context.Background(), admissionRequest(run.Name))
			if err != nil || got.RequeueAfter != 0 {
				t.Fatalf("Reconcile() = %#v, %v", got, err)
			}
			if len(s.settled) != 1 || s.settled[0].FenceToken != "source-fence" || s.settled[0].PR != "42" {
				t.Fatalf("settlement handoff = %#v", s.settled)
			}
			var stored courierv1alpha1.CoderRun
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &stored); err != nil {
				t.Fatal(err)
			}
			cond := apimeta.FindStatusCondition(stored.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
			if cond == nil || cond.Reason != capacityRecoverySettled || cond.Status != metav1.ConditionTrue {
				t.Fatalf("condition = %#v", cond)
			}
			var storedLane courierv1alpha1.LaneProfile
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(lane), &storedLane); err != nil {
				t.Fatal(err)
			}
			if !storedLane.CooldownActive(r.clock()) {
				t.Fatal("shared capacity cooldown was not persisted")
			}
		})
	}
}

func TestCapacityRecoveryRetriesWithSameKeyAfterReconcileReconstruction(t *testing.T) {
	run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
	run.UID, run.Spec.Source, run.Spec.WorkItemID = "run-uid", "fake", "work"
	run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
	run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: "Published"})
	c := capacityTestClient(t, admissionLane("pool-a", 1), run)
	s := &capacitySource{admissionSource: &admissionSource{}, settleErr: context.DeadlineExceeded}
	newReconciler := func() *CoderRunReconciler {
		return &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), StatusWriter: fakeStatusWriter{client: c}}
	}
	if _, err := newReconciler().Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatal(err)
	}
	s.settleErr = nil
	if _, err := newReconciler().Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatal(err)
	}
	if len(s.settled) != 2 || s.settled[0].IdempotencyKey == "" || s.settled[0].IdempotencyKey != s.settled[1].IdempotencyKey {
		t.Fatalf("settlement keys across reconciler restart = %#v", s.settled)
	}
}

func TestCapacityRecoveryFailsClosedForUnsupportedAndLiveWriters(t *testing.T) {
	for _, live := range []bool{false, true} {
		name := "unsupported"
		if live {
			name = "live-writer"
		}
		t.Run(name, func(t *testing.T) {
			run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
			run.UID, run.Spec.Source, run.Spec.WorkItemID = "run-uid", "fake", "work"
			run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
			run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: "Published"})
			objects := []runtime.Object{admissionLane("pool-a", 1), run}
			if live {
				objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: run.Namespace, Labels: map[string]string{executor.LabelRun: run.Name}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
			}
			c := capacityTestClient(t, objects...)
			s := &admissionSource{}
			r := &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), StatusWriter: fakeStatusWriter{client: c}}
			_, err := r.Reconcile(context.Background(), admissionRequest(run.Name))
			if err != nil {
				t.Fatal(err)
			}
			var stored courierv1alpha1.CoderRun
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &stored); err != nil {
				t.Fatal(err)
			}
			cond := apimeta.FindStatusCondition(stored.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
			want := capacityRecoveryUnsupported
			if live {
				want = capacityRecoveryPendingWriters
			}
			if cond == nil || cond.Reason != want {
				t.Fatalf("condition = %#v, want %s", cond, want)
			}
			if len(s.released) != 0 {
				t.Fatalf("unsafe ordinary releases: %#v", s.released)
			}
		})
	}
}
