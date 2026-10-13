package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// reapClock is pinned and nanosecond-free so RFC3339 annotation round-trips
// are exact and no test ever sleeps.
var reapClock = time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

// reapRun returns a Done run created two days ago, far older than any
// retention window, so a test that sees it deleted must have keyed on the
// durable marker and not on object age.
func reapRun(name string) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "local", courierv1alpha1.PhaseDone)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-48 * time.Hour))
	return run
}

func withDoneAt(run *courierv1alpha1.CoderRun, at time.Time) *courierv1alpha1.CoderRun {
	return withReapMarker(run, doneAtAnnotation, at)
}

func withReapMarker(run *courierv1alpha1.CoderRun, marker string, at time.Time) *courierv1alpha1.CoderRun {
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[marker] = at.UTC().Format(time.RFC3339)
	return run
}

// withLifecycleSettled marks a terminal run's source lifecycle report as
// positively settled (Published), the gate the Failed/NeedsHuman reaper
// requires before it will delete.
func withLifecycleSettled(run *courierv1alpha1.CoderRun) *courierv1alpha1.CoderRun {
	return withLifecycleReported(run, metav1.ConditionTrue, "Published")
}

func withLifecycleReported(run *courierv1alpha1.CoderRun, status metav1.ConditionStatus, reason string) *courierv1alpha1.CoderRun {
	run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{
		Type:               lifecycleReportedCondition,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: metav1.NewTime(reapClock),
	})
	return run
}

// terminalReapRun returns a settled Failed/NeedsHuman run created long ago with
// the given execution end, so a test that sees it deleted must key on the
// durable marker and the settled gate, not on object age.
func terminalReapRun(name string, phase courierv1alpha1.Phase, finishedAt time.Time) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "local", phase)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-72 * time.Hour))
	run.Status.FinishedAt = &metav1.Time{Time: finishedAt}
	return withLifecycleSettled(run)
}

func reapReconciler(c client.Client, src source.Adapter, now time.Time) *CoderRunReconciler {
	return &CoderRunReconciler{
		Client:        c,
		Sources:       NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter:  fakeStatusWriter{client: c},
		DoneRetention: 10 * time.Minute,
		Now:           func() time.Time { return now },
	}
}

// terminalReapReconciler pins short Failed/NeedsHuman windows and an explicit
// audit floor so the retention behavior is observable without sleeping.
func terminalReapReconciler(c client.Client, src source.Adapter, now time.Time, keep int) *CoderRunReconciler {
	return &CoderRunReconciler{
		Client:              c,
		Sources:             NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter:        fakeStatusWriter{client: c},
		DoneRetention:       10 * time.Minute,
		FailedRetention:     10 * time.Minute,
		NeedsHumanRetention: 10 * time.Minute,
		TerminalAuditKeep:   keep,
		Now:                 func() time.Time { return now },
	}
}

// failResolveSource fails only Resolve, the gate that must hold reap back.
type failResolveSource struct {
	source.Adapter
	err error
}

func (s failResolveSource) Resolve(context.Context, source.WorkItem) error { return s.err }

func TestReapFreshDoneStartsRetention(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, reapRun("fresh"))
	reconciler := reapReconciler(c, src, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("fresh"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(src.resolved) != 1 || src.resolved[0] != "fresh" {
		t.Fatalf("resolved IDs = %#v, want the source resolved before any reap state", src.resolved)
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want a full retention window on first Done observation", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("fresh"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got := stored.Annotations[doneAtAnnotation]; got != reapClock.UTC().Format(time.RFC3339) {
		t.Fatalf("done-at = %q, want %q", got, reapClock.UTC().Format(time.RFC3339))
	}
}

func TestReapValidMarkerSkipsResolveBeforeRetention(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, withDoneAt(reapRun("waiting"), reapClock.Add(-3*time.Minute)))
	reconciler := reapReconciler(c, src, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("waiting"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 7*time.Minute {
		t.Fatalf("RequeueAfter = %v, want the remaining 7m of the window", result.RequeueAfter)
	}
	if len(src.resolved) != 0 {
		t.Fatalf("resolved IDs = %#v, want none; a valid marker already proves the source resolved", src.resolved)
	}
	if err := c.Get(context.Background(), admissionKey("waiting"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get run: %v; a young Done run must not be deleted", err)
	}
}

func TestReapRestartSkipsResolveOnPersistedMarker(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, withDoneAt(reapRun("carried"), reapClock.Add(-3*time.Minute)))
	// A brand-new reconciler instance shares nothing with any predecessor
	// except the API server: the persisted marker alone must stand in for the
	// source resolve.
	reconciler := reapReconciler(c, src, reapClock.Add(2*time.Minute))

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("carried")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(src.resolved) != 0 {
		t.Fatalf("resolved IDs = %#v, want none after restart on a persisted marker", src.resolved)
	}
}

func TestReapDeleteSkipsResolve(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, withDoneAt(reapRun("spent"), reapClock.Add(-11*time.Minute)))
	reconciler := reapReconciler(c, src, reapClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("spent")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	err := c.Get(context.Background(), admissionKey("spent"), &courierv1alpha1.CoderRun{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get spent run = %v, want NotFound after deletion", err)
	}
	if len(src.resolved) != 0 {
		t.Fatalf("resolved IDs = %#v, want none; deletion must not re-resolve the source", src.resolved)
	}
	// Reconciling a deleted run is success, so a duplicate wake cannot error.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("spent")); err != nil {
		t.Fatalf("Reconcile() after deletion error = %v, want NotFound ignored", err)
	}
}

func TestReapRetentionSurvivesControllerRestart(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, reapRun("restart"))
	first := reapReconciler(c, src, reapClock)
	if _, err := first.Reconcile(context.Background(), admissionRequest("restart")); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}

	// A brand-new reconciler instance shares nothing with the first except the
	// API server: the persisted marker alone must carry the retention state.
	second := reapReconciler(c, src, reapClock.Add(11*time.Minute))
	if _, err := second.Reconcile(context.Background(), admissionRequest("restart")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	err := c.Get(context.Background(), admissionKey("restart"), &courierv1alpha1.CoderRun{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get restart run = %v, want NotFound; retention must outlive the process", err)
	}
}

func TestReapIgnoresObjectAge(t *testing.T) {
	src := &admissionSource{}
	// Created 48 hours ago, Done for one minute: object age must not reap it.
	run := withDoneAt(reapRun("aged"), reapClock.Add(-time.Minute))
	c := phaseClient(t, run)
	reconciler := reapReconciler(c, src, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("aged"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 9*time.Minute {
		t.Fatalf("RequeueAfter = %v, want the remaining 9m measured from the marker", result.RequeueAfter)
	}
	if err := c.Get(context.Background(), admissionKey("aged"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get run: %v; an old creationTimestamp must never shorten retention", err)
	}
}

func TestReapFailedStartsRetentionOnceReportSettled(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, terminalReapRun("failed", courierv1alpha1.PhaseFailed, reapClock.Add(-time.Minute)))
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("failed"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want a full retention window on first eligible Failed observation", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("failed"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got := stored.Annotations[failedAtAnnotation]; got != reapClock.UTC().Format(time.RFC3339) {
		t.Fatalf("failed-at = %q, want %q", got, reapClock.UTC().Format(time.RFC3339))
	}
	if _, ok := stored.Annotations[needsHumanAtAnnotation]; ok {
		t.Fatal("Failed run gained a needs-human-at marker")
	}
}

func TestReapFailedDeletesPastWindow(t *testing.T) {
	src := &admissionSource{}
	run := withReapMarker(
		terminalReapRun("failed", courierv1alpha1.PhaseFailed, reapClock.Add(-11*time.Minute)),
		failedAtAnnotation, reapClock.Add(-11*time.Minute))
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("failed")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("failed"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get failed run = %v, want NotFound after the window", err)
	}
}

func TestReapNeedsHumanUsesOwnWindow(t *testing.T) {
	src := &admissionSource{}
	// Still inside the window: retained, and requeued for the remainder.
	run := withReapMarker(
		terminalReapRun("needs", courierv1alpha1.PhaseNeedsHuman, reapClock.Add(-3*time.Minute)),
		needsHumanAtAnnotation, reapClock.Add(-3*time.Minute))
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("needs"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 7*time.Minute {
		t.Fatalf("RequeueAfter = %v, want the remaining 7m of the NeedsHuman window", result.RequeueAfter)
	}
	if err := c.Get(context.Background(), admissionKey("needs"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get needs run: %v; a young NeedsHuman run must not be deleted", err)
	}

	// Past the window: deleted.
	spent := withReapMarker(
		terminalReapRun("spent-needs", courierv1alpha1.PhaseNeedsHuman, reapClock.Add(-11*time.Minute)),
		needsHumanAtAnnotation, reapClock.Add(-11*time.Minute))
	c2 := phaseClient(t, spent)
	reconciler2 := terminalReapReconciler(c2, src, reapClock, -1)
	if _, err := reconciler2.Reconcile(context.Background(), admissionRequest("spent-needs")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if err := c2.Get(context.Background(), admissionKey("spent-needs"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get spent needs run = %v, want NotFound after the window", err)
	}
}

func TestReapNeedsHumanParksWithoutSettledReport(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("stuck", "local", courierv1alpha1.PhaseNeedsHuman)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-48 * time.Hour))
	run.Status.FinishedAt = &metav1.Time{Time: reapClock.Add(-48 * time.Hour)}
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("stuck"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0; a missing report condition parks without a timer", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("stuck"), &stored); err != nil {
		t.Fatalf("get run: %v; an unsettled NeedsHuman run must persist regardless of age", err)
	}
	if _, ok := stored.Annotations[needsHumanAtAnnotation]; ok {
		t.Fatal("NeedsHuman run gained a needs-human-at marker despite an unsettled report")
	}
}

func TestReapFailedHoldsWhileReportPending(t *testing.T) {
	src := &admissionSource{}
	run := terminalReapRun("pending", courierv1alpha1.PhaseFailed, reapClock.Add(-48*time.Hour))
	// Replace the settled condition with a pending one: the only durable
	// retry intent must hold the reap back.
	run.Status.Conditions = []metav1.Condition{{
		Type:               lifecycleReportedCondition,
		Status:             metav1.ConditionFalse,
		Reason:             lifecycleReportPendingReason,
		LastTransitionTime: metav1.NewTime(reapClock.Add(-48 * time.Hour)),
	}}
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("pending")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("pending"), &stored); err != nil {
		t.Fatalf("get run: %v; a report-pending run must not be reaped", err)
	}
	if _, ok := stored.Annotations[failedAtAnnotation]; ok {
		t.Fatal("failed-at marker written while the lifecycle report was pending")
	}
	if len(src.reports) == 0 {
		t.Fatal("report-pending run was not retried")
	}
}

func TestReapFailedIgnoresObjectAge(t *testing.T) {
	src := &admissionSource{}
	// Created 72 hours ago, eligible for one minute: object age must not reap.
	run := withReapMarker(
		terminalReapRun("aged", courierv1alpha1.PhaseFailed, reapClock.Add(-time.Minute)),
		failedAtAnnotation, reapClock.Add(-time.Minute))
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("aged"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 9*time.Minute {
		t.Fatalf("RequeueAfter = %v, want the remaining 9m measured from the marker", result.RequeueAfter)
	}
	if err := c.Get(context.Background(), admissionKey("aged"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get run: %v; an old creationTimestamp must never shorten retention", err)
	}
}

func TestReapFailedMalformedMarkerRestartsWindow(t *testing.T) {
	src := &admissionSource{}
	run := terminalReapRun("corrupt", courierv1alpha1.PhaseFailed, reapClock.Add(-48*time.Hour))
	run.Annotations = map[string]string{failedAtAnnotation: "not-a-timestamp"}
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("corrupt"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want a fresh retention window", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("corrupt"), &stored); err != nil {
		t.Fatalf("get run: %v; a malformed marker must fail safe, not delete", err)
	}
	if got := stored.Annotations[failedAtAnnotation]; got != reapClock.UTC().Format(time.RFC3339) {
		t.Fatalf("failed-at = %q, want the malformed value reset to %q", got, reapClock.UTC().Format(time.RFC3339))
	}
}

func TestReapFailedFutureMarkerWaitsBoundedWindow(t *testing.T) {
	src := &admissionSource{}
	run := withReapMarker(
		terminalReapRun("skewed", courierv1alpha1.PhaseFailed, reapClock.Add(-48*time.Hour)),
		failedAtAnnotation, reapClock.Add(time.Hour))
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("skewed"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want positive and bounded by the retention window", result.RequeueAfter)
	}
	if err := c.Get(context.Background(), admissionKey("skewed"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get run: %v; a future marker must never delete early", err)
	}
}

// TestReapAuditFloorKeepsMostRecentPerRepo pins the per-repository audit floor:
// with three same-repo terminal runs and a floor of two, the two most recent
// survive past their window and the oldest is reaped.
func TestReapAuditFloorKeepsMostRecentPerRepo(t *testing.T) {
	oldest := withReapMarker(
		terminalReapRun("oldest", courierv1alpha1.PhaseFailed, reapClock.Add(-30*time.Hour)),
		failedAtAnnotation, reapClock.Add(-30*time.Hour))
	middle := withReapMarker(
		terminalReapRun("middle", courierv1alpha1.PhaseNeedsHuman, reapClock.Add(-20*time.Hour)),
		needsHumanAtAnnotation, reapClock.Add(-20*time.Hour))
	newest := withReapMarker(
		terminalReapRun("newest", courierv1alpha1.PhaseFailed, reapClock.Add(-10*time.Hour)),
		failedAtAnnotation, reapClock.Add(-10*time.Hour))

	c := phaseClient(t, oldest, middle, newest)
	reconciler := terminalReapReconciler(c, &admissionSource{}, reapClock, 2)

	// The oldest has two newer terminal runs, so it is outside the floor.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("oldest")); err != nil {
		t.Fatalf("reconcile oldest: %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("oldest"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get oldest = %v, want NotFound once two newer runs cover the floor", err)
	}

	// The middle and newest are within the floor and must be retained.
	for _, name := range []string{"middle", "newest"} {
		result, err := reconciler.Reconcile(context.Background(), admissionRequest(name))
		if err != nil {
			t.Fatalf("reconcile %s: %v", name, err)
		}
		if result.RequeueAfter <= 0 {
			t.Fatalf("%s RequeueAfter = %v, want a bounded recheck while inside the audit floor", name, result.RequeueAfter)
		}
		if err := c.Get(context.Background(), admissionKey(name), &courierv1alpha1.CoderRun{}); err != nil {
			t.Fatalf("get %s: %v; the audit floor must retain it", name, err)
		}
	}
}

func TestReapLeavesUnreapedTerminalPhases(t *testing.T) {
	src := &admissionSource{}
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseHandedOff,
		courierv1alpha1.PhaseBlockedExternal,
	} {
		run := terminalReapRun("held-"+string(phase), phase, reapClock.Add(-48*time.Hour))
		c := phaseClient(t, run)
		reconciler := terminalReapReconciler(c, src, reapClock, -1)
		result, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name))
		if err != nil {
			t.Fatalf("reconcile %s: %v", phase, err)
		}
		if result.RequeueAfter != 0 {
			t.Fatalf("%s RequeueAfter = %v, want 0; this phase carries no reap timer", phase, result.RequeueAfter)
		}
		if err := c.Get(context.Background(), admissionKey(run.Name), &courierv1alpha1.CoderRun{}); err != nil {
			t.Fatalf("get %s: %v; %s must persist", phase, err, phase)
		}
	}
}

func TestReapHoldsWhenSourceResolveFails(t *testing.T) {
	resolveErr := errors.New("dispatch unavailable")
	c := phaseClient(t, reapRun("blocked"))
	reconciler := reapReconciler(c, failResolveSource{err: resolveErr}, reapClock)

	_, err := reconciler.Reconcile(context.Background(), admissionRequest("blocked"))
	if !errors.Is(err, resolveErr) {
		t.Fatalf("Reconcile() error = %v, want %v propagated", err, resolveErr)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("blocked"), &stored); err != nil {
		t.Fatalf("get run: %v; a failed source resolve must leave the run in place", err)
	}
	if _, ok := stored.Annotations[doneAtAnnotation]; ok {
		t.Fatal("done-at marker written despite failed source resolve; the clock must not start")
	}
}

func TestReapMalformedMarkerRestartsWindow(t *testing.T) {
	src := &admissionSource{}
	run := reapRun("corrupt")
	run.Annotations = map[string]string{doneAtAnnotation: "not-a-timestamp"}
	c := phaseClient(t, run)
	reconciler := reapReconciler(c, src, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("corrupt"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want a fresh retention window", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("corrupt"), &stored); err != nil {
		t.Fatalf("get run: %v; a malformed marker must fail safe, not delete", err)
	}
	if len(src.resolved) != 1 {
		t.Fatalf("resolved IDs = %#v, want one; a malformed marker proves nothing and must re-resolve", src.resolved)
	}
	if got := stored.Annotations[doneAtAnnotation]; got != reapClock.UTC().Format(time.RFC3339) {
		t.Fatalf("done-at = %q, want the malformed value reset to %q", got, reapClock.UTC().Format(time.RFC3339))
	}
}

func TestReapFutureMarkerWaitsBoundedWindow(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, withDoneAt(reapRun("skewed"), reapClock.Add(time.Hour)))
	reconciler := reapReconciler(c, src, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("skewed"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want positive and bounded by the retention window", result.RequeueAfter)
	}
	if err := c.Get(context.Background(), admissionKey("skewed"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get run: %v; a future marker must never delete early", err)
	}
}

func TestReapFailedSupersededReportReaps(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("superseded", "local", courierv1alpha1.PhaseFailed)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-72 * time.Hour))
	run.Status.FinishedAt = &metav1.Time{Time: reapClock.Add(-11 * time.Minute)}
	withLifecycleReported(run, metav1.ConditionTrue, "Superseded")
	withReapMarker(run, failedAtAnnotation, reapClock.Add(-11*time.Minute))
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("superseded")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("superseded"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get run = %v, want NotFound; a superseded report satisfies the settled gate", err)
	}
}

func TestReapFailedUnknownReportParks(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("unknown", "local", courierv1alpha1.PhaseFailed)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-72 * time.Hour))
	run.Status.FinishedAt = &metav1.Time{Time: reapClock.Add(-48 * time.Hour)}
	withLifecycleReported(run, metav1.ConditionUnknown, "Retryable")
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("unknown"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0; an Unknown report proves nothing and parks", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("unknown"), &stored); err != nil {
		t.Fatalf("get run: %v; an Unknown report must not be reaped", err)
	}
	if _, ok := stored.Annotations[failedAtAnnotation]; ok {
		t.Fatal("failed-at marker written despite an Unknown report")
	}
}

func TestReapAuditFloorIsPerRepo(t *testing.T) {
	src := &admissionSource{}
	candidate := withReapMarker(
		terminalReapRun("candidate", courierv1alpha1.PhaseFailed, reapClock.Add(-30*time.Hour)),
		failedAtAnnotation, reapClock.Add(-30*time.Hour))
	candidate.Spec.Repo = "acme/alpha"
	// A newer terminal run in another repository must not cover this run.
	other := withReapMarker(
		terminalReapRun("other", courierv1alpha1.PhaseFailed, reapClock.Add(-10*time.Hour)),
		failedAtAnnotation, reapClock.Add(-10*time.Hour))
	other.Spec.Repo = "acme/beta"

	c := phaseClient(t, candidate, other)
	reconciler := terminalReapReconciler(c, src, reapClock, 1)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("candidate"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %v, want a bounded recheck; the floor is per repository", result.RequeueAfter)
	}
	if err := c.Get(context.Background(), admissionKey("candidate"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get candidate: %v; another repo's newer run must not push it out of the floor", err)
	}
}

func TestReapAuditFloorTieBreaksDeterministically(t *testing.T) {
	src := &admissionSource{}
	mk := func(name string) *courierv1alpha1.CoderRun {
		run := terminalReapRun(name, courierv1alpha1.PhaseFailed, reapClock.Add(-30*time.Hour))
		return withReapMarker(run, failedAtAnnotation, reapClock.Add(-30*time.Hour))
	}
	c := phaseClient(t, mk("aaa"), mk("bbb"), mk("ccc"))
	reconciler := terminalReapReconciler(c, src, reapClock, 1)

	// Same terminal second for all three; the deterministic tiebreak keeps the
	// single greatest name and reaps the rest.
	for _, name := range []string{"aaa", "bbb"} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(name)); err != nil {
			t.Fatalf("reconcile %s: %v", name, err)
		}
		if err := c.Get(context.Background(), admissionKey(name), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
			t.Fatalf("get %s = %v, want NotFound; a tied run outside the floor must be reaped", name, err)
		}
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("ccc")); err != nil {
		t.Fatalf("reconcile ccc: %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("ccc"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get ccc: %v; the tiebreak winner must be retained", err)
	}
}

func TestReapFailedRetentionSurvivesRestart(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, terminalReapRun("restart", courierv1alpha1.PhaseFailed, reapClock.Add(-time.Minute)))

	first := terminalReapReconciler(c, src, reapClock, -1)
	if _, err := first.Reconcile(context.Background(), admissionRequest("restart")); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}

	// A brand-new reconciler instance shares nothing with the first except the
	// API server: the persisted marker alone must carry the retention state.
	second := terminalReapReconciler(c, src, reapClock.Add(11*time.Minute), -1)
	if _, err := second.Reconcile(context.Background(), admissionRequest("restart")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("restart"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get restart run = %v, want NotFound; retention must outlive the process", err)
	}
}
