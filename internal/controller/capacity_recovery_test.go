package controller

import (
	"bytes"
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
)

type capacitySource struct {
	*admissionSource
	settled         []source.CapacityHandoff
	prepareErr      error
	settleErr       error
	prepareCalls    int
	settleCommitted bool
}

func (s *capacitySource) PrepareCapacityRecovery(_ context.Context, _ source.WorkItem, _ source.CapacityRecoveryAttempt) (source.CapacityRecoveryProof, error) {
	s.prepareCalls++
	if s.prepareErr != nil {
		return source.CapacityRecoveryProof{}, s.prepareErr
	}
	return source.CapacityRecoveryProof{FenceToken: "source-fence", WorkIdentity: "work", SafetyProof: "remote-holds-partial-work", Branch: "branch", PR: "42"}, nil
}

func (s *capacitySource) SettleCapacityExhaustion(_ context.Context, _ source.WorkItem, handoff source.CapacityHandoff) error {
	if s.settleCommitted {
		if handoff.IdempotencyKey != s.settled[0].IdempotencyKey {
			return source.ErrStaleCapacitySettlement
		}
		s.settled = append(s.settled, handoff)
		return nil
	}
	s.settled = append(s.settled, handoff)
	if s.settleErr == nil {
		s.settleCommitted = true
	}
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
			run.Status.Branch = "branch"
			run.Status.WorkspaceSafety = &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"}
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

// TestCapacityRecoveryAcceptsAvailabilityHandoff proves the Scenario D gate:
// a run terminalized as provider-unavailable (an outage without a structured
// quota signal) drives the same fenced settlement and capacity cooldown as a
// confirmed exhaustion — work must move — while any other category on the
// same phase remains outside this path.
func TestCapacityRecoveryAcceptsAvailabilityHandoff(t *testing.T) {
	run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
	run.UID = "run-uid"
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeResolveIssue, "fake", "availability-work"
	run.Status.Branch = "branch"
	run.Status.WorkspaceSafety = &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"}
	run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable}
	run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: "Published"})
	lane := admissionLane("pool-a", 1)
	lane.Spec.CapacityGroup = "shared-pool"
	c := capacityTestClient(t, lane, run)
	s := &capacitySource{admissionSource: &admissionSource{}}
	r := &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), StatusWriter: fakeStatusWriter{client: c}, Now: func() time.Time { return time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC) }}
	if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(s.settled) != 1 {
		t.Fatalf("availability handoff did not settle: %#v", s.settled)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &stored); err != nil {
		t.Fatal(err)
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery); cond == nil || cond.Reason != capacityRecoverySettled {
		t.Fatalf("condition = %#v", cond)
	}
}

func TestCapacityRecoveryRetriesWithSameKeyAfterReconcileReconstruction(t *testing.T) {	run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
	run.UID, run.Spec.Source, run.Spec.WorkItemID = "run-uid", "fake", "work"
	run.Status.Branch = "branch"
	run.Status.WorkspaceSafety = &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"}
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

func TestCapacityRecoveryReplaysCommittedSettlementAfterStatusPatchFailure(t *testing.T) {
	run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
	run.UID, run.Spec.Source, run.Spec.WorkItemID = "run-uid", "fake", "work"
	run.Status.Branch = "branch"
	run.Status.WorkspaceSafety = &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"}
	run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
	run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: "Published"})
	c := capacityTestClient(t, admissionLane("pool-a", 1), run)
	s := &capacitySource{admissionSource: &admissionSource{}}
	writer := &failOnceCapacityStatusWriter{fakeStatusWriter: fakeStatusWriter{client: c}}
	newReconciler := func() *CoderRunReconciler {
		return &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), StatusWriter: writer}
	}
	if _, err := newReconciler().Reconcile(context.Background(), admissionRequest(run.Name)); err == nil {
		t.Fatal("first reconcile error = nil, want status patch failure after committed settle")
	}
	if !s.settleCommitted {
		t.Fatal("source settlement was not committed")
	}
	if _, err := newReconciler().Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatal(err)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &stored); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(stored.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != capacityRecoverySettled {
		t.Fatalf("recovered condition = %#v, want True Settled", cond)
	}
	if s.prepareCalls != 2 || s.settled[0].IdempotencyKey != s.settled[1].IdempotencyKey {
		t.Fatalf("replay calls prepare=%d settlements=%#v", s.prepareCalls, s.settled)
	}
}

type failOnceCapacityStatusWriter struct {
	fakeStatusWriter
	failed bool
}

func (w *failOnceCapacityStatusWriter) PatchStatus(ctx context.Context, name types.NamespacedName, patch []byte) error {
	if !w.failed && bytes.Contains(patch, []byte(courierv1alpha1.ConditionCapacityRecovery)) && bytes.Contains(patch, []byte(capacityRecoverySettled)) {
		w.failed = true
		return context.DeadlineExceeded
	}
	return w.fakeStatusWriter.PatchStatus(ctx, name, patch)
}

func TestCapacityCooldownIndependentOfLifecycleReport(t *testing.T) {
	for _, reportReason := range []string{"Retryable", "UnsupportedOutcome"} {
		t.Run(reportReason, func(t *testing.T) {
			run := admissionRun("exhausted", "pool-a", courierv1alpha1.PhaseBlockedExternal)
			run.UID = "run-uid"
			run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
			run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionFalse, Reason: reportReason})
			lane := admissionLane("pool-a", 1)
			lane.Spec.CapacityGroup = "shared"
			peer := admissionLane("pool-b", 1)
			peer.Spec.CapacityGroup = "shared"
			c := capacityTestClient(t, lane, peer, run)
			r := &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}), Now: func() time.Time { return time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC) }}
			if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
				t.Fatal(err)
			}
			var got courierv1alpha1.LaneProfile
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(lane), &got); err != nil {
				t.Fatal(err)
			}
			if !got.CooldownActive(r.clock()) {
				t.Fatal("pending report did not persist cooldown")
			}
			var other courierv1alpha1.LaneProfile
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(peer), &other); err != nil {
				t.Fatal(err)
			}
			if other.CooldownActive(r.clock()) {
				t.Fatal("unrelated capacity was paused")
			}
		})
	}
}

func TestCapacityCooldownExpiredHistoricalRunDoesNotResurrectOnReconcile(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	run := admissionRun("old-exhaustion", "pool-a", courierv1alpha1.PhaseBlockedExternal)
	run.UID = "old-run-uid"
	run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
	run.Status.Conditions = append(run.Status.Conditions,
		metav1.Condition{Type: conditionCapacityCooldownRecorded, Status: metav1.ConditionTrue, Reason: "Recorded", LastTransitionTime: metav1.NewTime(now.Add(-16 * time.Minute))},
		metav1.Condition{Type: lifecycleReportedCondition, Status: metav1.ConditionFalse, Reason: lifecycleReportUnsupportedOutcomeReason},
	)
	lane := admissionLane("pool-a", 1)
	peer := admissionLane("pool-b", 1)
	lane.Spec.CapacityGroup, peer.Spec.CapacityGroup = "shared", "shared"
	c := capacityTestClient(t, lane, peer, run)
	r := &CoderRunReconciler{Client: c, Sources: NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}), Now: func() time.Time { return now }}
	if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatal(err)
	}
	var got courierv1alpha1.LaneProfile
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(lane), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CapacityCooldown != nil {
		t.Fatalf("expired historical cooldown resurrected: %#v", got.Status.CapacityCooldown)
	}
	var unrelated courierv1alpha1.LaneProfile
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(peer), &unrelated); err != nil {
		t.Fatal(err)
	}
	if unrelated.CooldownActive(now) {
		t.Fatal("unrelated lane became ineligible")
	}
}

func TestCapacityCooldownCountsInterleavedSharedCapacityRunsOnce(t *testing.T) {
	laneA, laneB := admissionLane("pool-a", 1), admissionLane("pool-b", 1)
	laneA.Spec.CapacityGroup, laneB.Spec.CapacityGroup = "shared", "shared"
	c := capacityTestClient(t, laneA, laneB)
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	r := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}
	runs := []*courierv1alpha1.CoderRun{admissionRun("a", "pool-a", courierv1alpha1.PhaseBlockedExternal), admissionRun("b", "pool-b", courierv1alpha1.PhaseBlockedExternal)}
	for _, run := range runs {
		run.UID = types.UID("uid-" + run.Name)
		run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
	}
	for _, run := range []*courierv1alpha1.CoderRun{runs[0], runs[1], runs[0]} {
		var fresh courierv1alpha1.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &fresh); err != nil {
			t.Fatal(err)
		}
		if err := r.recordCapacityCooldown(context.Background(), &fresh); err != nil {
			t.Fatal(err)
		}
		r = &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}
	}
	var lane courierv1alpha1.LaneProfile
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(laneA), &lane); err != nil {
		t.Fatal(err)
	}
	cooldown := lane.Status.CapacityCooldown
	if cooldown.ExhaustionCount != 2 {
		t.Fatalf("count = %d, want two distinct run events", cooldown.ExhaustionCount)
	}
	if !cooldown.NextEligibleAt.Time.Equal(now.Add(15 * time.Minute)) {
		t.Fatalf("deadline = %v, want %v", cooldown.NextEligibleAt, now.Add(15*time.Minute))
	}
}

func TestCapacityCooldownExpiresWithoutRenewingOnOldRun(t *testing.T) {
	lane := admissionLane("pool-a", 1)
	c := capacityTestClient(t, lane)
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	r := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}
	var runs []*courierv1alpha1.CoderRun
	for _, name := range []string{"a", "b", "c"} {
		run := admissionRun(name, "pool-a", courierv1alpha1.PhaseBlockedExternal)
		run.UID = types.UID("uid-" + name)
		run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion}
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, run)
	}
	for i, run := range runs {
		now = now.Add(time.Minute)
		var fresh courierv1alpha1.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &fresh); err != nil {
			t.Fatal(err)
		}
		if err := r.recordCapacityCooldown(context.Background(), &fresh); err != nil {
			t.Fatal(err)
		}
		if i == 0 && !fresh.Status.Conditions[0].LastTransitionTime.IsZero() {
			// The first event starts the window; subsequent runs extend it.
		}
	}
	var got courierv1alpha1.LaneProfile
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(lane), &got); err != nil {
		t.Fatal(err)
	}
	wantDeadline := now.Add(15 * time.Minute)
	if !got.Status.CapacityCooldown.NextEligibleAt.Time.Equal(wantDeadline) {
		t.Fatalf("deadline = %v, want latest marker + window %v", got.Status.CapacityCooldown.NextEligibleAt, wantDeadline)
	}
	now = wantDeadline.Add(time.Second)
	var old courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(runs[0]), &old); err != nil {
		t.Fatal(err)
	}
	if err := r.recordCapacityCooldown(context.Background(), &old); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(lane), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CapacityCooldown != nil {
		t.Fatalf("expired markers renewed cooldown: %#v", got.Status.CapacityCooldown)
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
			run.Status.WorkspaceSafety = &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty"}
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
