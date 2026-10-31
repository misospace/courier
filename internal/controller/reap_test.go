package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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

func TestReapDonePermanentRefusalResolveReaps(t *testing.T) {
	c := phaseClient(t, reapRun("refused-resolve"))
	reconciler := reapReconciler(c, failResolveSource{err: source.ErrPermanentRefusal}, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("refused-resolve"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v; a permanent resolve refusal must settle, not retry", err)
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want a full retention window on the settled refusal", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("refused-resolve"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got := stored.Annotations[doneAtAnnotation]; got != reapClock.UTC().Format(time.RFC3339) {
		t.Fatalf("done-at = %q, want the settled refusal to start the clock at %q", got, reapClock.UTC().Format(time.RFC3339))
	}

	// Past the window the run reaps without re-resolving: the marker proves
	// the resolve settled, so the refusal is never re-presented.
	after := reapReconciler(c, failResolveSource{err: source.ErrPermanentRefusal}, reapClock.Add(11*time.Minute))
	if _, err := after.Reconcile(context.Background(), admissionRequest("refused-resolve")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("refused-resolve"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get run = %v, want NotFound; a permanently refused resolve must not hold the run past retention", err)
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

func TestReapFailedPermanentRefusalReportReaps(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("refused", "local", courierv1alpha1.PhaseFailed)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-72 * time.Hour))
	run.Status.FinishedAt = &metav1.Time{Time: reapClock.Add(-11 * time.Minute)}
	withLifecycleReported(run, metav1.ConditionTrue, lifecycleReportPermanentRefusalReason)
	withReapMarker(run, failedAtAnnotation, reapClock.Add(-11*time.Minute))
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("refused")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("refused"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get run = %v, want NotFound; a permanently refused report satisfies the settled gate", err)
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

// doneRun returns a Done run whose terminal time and retention marker sit at
// the given offsets, so a test can place a Done run anywhere on the audit
// floor's timeline.
func doneRun(name string, finishedAt, doneAt time.Time) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "local", courierv1alpha1.PhaseDone)
	run.CreationTimestamp = metav1.NewTime(finishedAt)
	run.Status.FinishedAt = &metav1.Time{Time: finishedAt}
	return withReapMarker(run, doneAtAnnotation, doneAt)
}

// TestReapAuditFloorSurvivesDoneBurstAndCleanup is the temporal regression for
// the audit floor's candidate set: five transient Done runs (exempt from the
// floor) and three never-reaped runs must not count as newer floor entries and
// drain the protected quota of the two settled outcomes the floor promises to
// keep. The Done runs are reaped afterwards, and the protected trail must
// survive that cleanup.
func TestReapAuditFloorSurvivesDoneBurstAndCleanup(t *testing.T) {
	protected := map[string]*courierv1alpha1.CoderRun{
		"protected-failed": withReapMarker(
			terminalReapRun("protected-failed", courierv1alpha1.PhaseFailed, reapClock.Add(-30*time.Hour)),
			failedAtAnnotation, reapClock.Add(-30*time.Hour)),
		"protected-needs": withReapMarker(
			terminalReapRun("protected-needs", courierv1alpha1.PhaseNeedsHuman, reapClock.Add(-29*time.Hour)),
			needsHumanAtAnnotation, reapClock.Add(-29*time.Hour)),
	}
	done := make([]*courierv1alpha1.CoderRun, 0, 5)
	objects := make([]runtime.Object, 0, len(protected)+5+3)
	for _, run := range protected {
		objects = append(objects, run)
	}
	for i := range 5 {
		// Five newer, already-spent Done runs: newer than every protected run,
		// past their own window, and not yet reconciled.
		run := doneRun(fmt.Sprintf("done-%d", i), reapClock.Add(-time.Minute), reapClock.Add(-11*time.Minute))
		done = append(done, run)
		objects = append(objects, run)
	}
	// Never-reaped runs are newer still, and must not consume the quota either.
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseAwaitingReview,
		courierv1alpha1.PhaseHandedOff,
		courierv1alpha1.PhaseBlockedExternal,
	} {
		objects = append(objects, terminalReapRun("held-"+string(phase), phase, reapClock.Add(-30*time.Second)))
	}

	c := phaseClient(t, objects...)
	reconciler := terminalReapReconciler(c, &admissionSource{}, reapClock, 2)

	// The whole floor is occupied by the two protected outcomes, so neither may
	// be reaped while eight newer runs of other phases exist.
	for name := range protected {
		result, err := reconciler.Reconcile(context.Background(), admissionRequest(name))
		if err != nil {
			t.Fatalf("reconcile %s: %v", name, err)
		}
		if result.RequeueAfter <= 0 {
			t.Fatalf("%s RequeueAfter = %v, want a bounded recheck inside the audit floor", name, result.RequeueAfter)
		}
		if err := c.Get(context.Background(), admissionKey(name), &courierv1alpha1.CoderRun{}); err != nil {
			t.Fatalf("get %s: %v; a Done burst must not drain the audit floor", name, err)
		}
	}

	// The Done runs are reaped on their own window; the protected trail must be
	// intact afterwards, not left drained by the burst that preceded it.
	for _, run := range done {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
			t.Fatalf("reconcile %s: %v", run.Name, err)
		}
		if err := c.Get(context.Background(), admissionKey(run.Name), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
			t.Fatalf("get %s = %v, want NotFound; Done keeps its own short window", run.Name, err)
		}
	}
	for name := range protected {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(name)); err != nil {
			t.Fatalf("re-reconcile %s: %v", name, err)
		}
		if err := c.Get(context.Background(), admissionKey(name), &courierv1alpha1.CoderRun{}); err != nil {
			t.Fatalf("get %s: %v; the protected audit trail must survive the Done cleanup", name, err)
		}
	}
}

// TestReapRequestCountsAcceptedDeleteWhileFinalizerHolds pins the reap
// counter's contract: the API server accepts the delete even while the secure
// topology finalizer still holds the object, so the counter records a request
// and the run stays visible on the oldest-terminal-age gauge until revocation
// completes. A second wake on the held object must not count it again.
func TestReapRequestCountsAcceptedDeleteWhileFinalizerHolds(t *testing.T) {
	run := withReapMarker(
		terminalReapRun("held", courierv1alpha1.PhaseFailed, reapClock.Add(-11*time.Minute)),
		failedAtAnnotation, reapClock.Add(-11*time.Minute))
	run.Finalizers = []string{secureTopologyFinalizer}

	registry := prometheus.NewRegistry()
	recorder := NewRunRecorder(registry)
	ageRegistry := prometheus.NewRegistry()
	collector := NewTerminalAgeCollector(nil)
	collector.now = func() time.Time { return reapClock }

	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, &admissionSource{}, reapClock, -1)
	reconciler.Metrics = recorder
	collector.client = c
	ageRegistry.MustRegister(collector)

	labels := map[string]string{"phase": "Failed"}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("held")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	m := gatherMetric(t, registry, "courier_coderuns_reap_requests_total", labels)
	if m.GetCounter() == nil || m.GetCounter().GetValue() != 1 {
		t.Fatalf("reap requests = %v, want 1 for an accepted delete", m.GetCounter())
	}
	var held courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("held"), &held); err != nil {
		t.Fatalf("get held run: %v; the finalizer must hold the object after the accepted delete", err)
	}
	if held.DeletionTimestamp.IsZero() {
		t.Fatal("held run has no DeletionTimestamp; the delete request never landed")
	}
	age, ok := gaugeValueIfPresent(t, ageRegistry, "courier_coderuns_oldest_terminal_age_seconds", labels)
	if !ok || age != (11*time.Minute).Seconds() {
		t.Fatalf("oldest terminal age = (%v, %v), want (%v, true); a held run is still retained", age, ok, (11 * time.Minute).Seconds())
	}

	// A duplicate wake on the held object counts nothing more.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("held")); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	m = gatherMetric(t, registry, "courier_coderuns_reap_requests_total", labels)
	if m.GetCounter().GetValue() != 1 {
		t.Fatalf("reap requests = %v after a duplicate wake, want 1", m.GetCounter().GetValue())
	}

	// Revocation completes: the finalizer is released and the object goes away,
	// which is when the age gauge drops the phase.
	held.Finalizers = nil
	if err := c.Update(context.Background(), &held); err != nil {
		t.Fatalf("release finalizer: %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("held"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get held run = %v, want NotFound once the finalizer is released", err)
	}
	if _, ok := gaugeValueIfPresent(t, ageRegistry, "courier_coderuns_oldest_terminal_age_seconds", labels); ok {
		t.Fatal("oldest terminal age still reports Failed after the object was removed")
	}
}

// reapRaceClient builds a fake client with a global resource-version counter —
// so an intervening write visibly bumps the RV the delete precondition pins —
// plus the given interceptor funcs, for the delete-boundary race regressions.
func reapRaceClient(t *testing.T, funcs interceptor.Funcs, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}, &courierv1alpha1.LaneProfile{}).
		WithRuntimeObjects(objects...).
		WithGlobalResourceVersionCounter().
		WithInterceptorFuncs(funcs).
		Build()
}

// reapRaceRun returns an expired-window Failed run, lifecycle-settled, exactly
// the shape the reaper deletes on an ordinary pass.
func reapRaceRun(name string) *courierv1alpha1.CoderRun {
	return withReapMarker(
		terminalReapRun(name, courierv1alpha1.PhaseFailed, reapClock.Add(-11*time.Minute)),
		failedAtAnnotation, reapClock.Add(-11*time.Minute))
}

// TestReapFailedHoldsWhenAnnotationAppearsBeforeDelete is the deterministic
// delete-boundary race regression: the operator's retry annotation lands after
// the reconcile's initial read but before the delete request, on an
// already-expired Failed run. The reaper must refuse to delete — the snapshot
// cannot see the annotation, so deleting would drop durable intent with no
// predecessor object on which the controller could persist the Requested
// record — and the next reconcile must translate the annotation into a settled
// retry request rather than lose it with the object.
func TestReapFailedHoldsWhenAnnotationAppearsBeforeDelete(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-2"}}

	reads := 0
	c := reapRaceClient(t, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			live, ok := obj.(*courierv1alpha1.CoderRun)
			if !ok || key != admissionKey("race-hold") {
				return nil
			}
			reads++
			if reads == 1 {
				// The reconcile's initial read: the annotation is not
				// there yet.
				return nil
			}
			// The snapshot was already taken: the operator's annotation
			// lands on the live object before the delete boundary.
			before := live.DeepCopy()
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}
			live.Annotations[courierv1alpha1.RetryAnnotation] = "req-race"
			return cl.Patch(ctx, live, client.MergeFrom(before))
		},
	}, reapRaceRun("race-hold"))
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("race-hold")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if src.calls != 0 {
		t.Fatalf("requeue calls = %d, want 0: the held reconcile never saw the annotation", src.calls)
	}
	var held courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("race-hold"), &held); err != nil {
		t.Fatalf("get run after the annotation landed: %v; the run must survive the delete boundary", err)
	}
	if held.Status.RetryRequest != nil {
		t.Fatalf("retryRequest = %+v on the held pass, want nil: the snapshot could not see the annotation", held.Status.RetryRequest)
	}

	// The annotation update re-arms the workqueue: the next reconcile sees it
	// and translates the request instead of losing it with the object. The
	// window is already spent, so the same dispatch reaps the predecessor once
	// the record settles — the intent was honored (the requeue committed under
	// the derived key), and a retry never extends the retention window.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("race-hold")); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if src.calls != 1 {
		t.Fatalf("requeue calls = %d, want 1: the annotation must be translated, not lost with the object", src.calls)
	}
	wantKey := "coderun/" + admissionNamespace + "/race-hold/" + retryKeyDigest("req-race")
	if len(src.requests) != 1 || src.requests[0].IdempotencyKey != wantKey {
		t.Fatalf("idempotency keys = %v, want [%s]", src.requests, wantKey)
	}
	if err := c.Get(context.Background(), admissionKey("race-hold"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get raced run = %v, want NotFound once the settled request released the expired window", err)
	}
}

// TestReapFailedReapsAfterAnnotationSettledIntoRecord pins the invariant the
// delete-boundary fence must preserve: an annotation the snapshot already
// carried was seen by syncRetry at the head of this very dispatch, and a value
// that settled into the record must not hold the reaper forever — a retry
// never extends a predecessor's retention beyond its ordinary window.
func TestReapFailedReapsAfterAnnotationSettledIntoRecord(t *testing.T) {
	src := &admissionSource{}
	run := reapRaceRun("race-settled")
	run.Annotations[courierv1alpha1.RetryAnnotation] = "req-1"
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock.Add(-2 * time.Hour)),
		State: courierv1alpha1.RetryRequestStateRequeued, AttemptID: "gen-1",
		SettledAt: &metav1.Time{Time: reapClock.Add(-time.Hour)},
	}
	c := phaseClient(t, run)
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("race-settled")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("race-settled"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get settled-retry run = %v, want NotFound; a settled annotation must not wedge the reaper", err)
	}
}

// TestReapFailedPreconditionDefeatsDeleteAfterAnnotation is the CAS half of
// the delete-boundary fence: the annotation lands between the fence's live
// re-read and the delete request itself, so no read the reconcile performs can
// see it first. The delete's resourceVersion precondition must defeat it — the
// intervening write bumps the object's resource version and the delete fails
// with a conflict the workqueue retries — and the retried reconcile must
// translate the annotation rather than lose it with the object.
func TestReapFailedPreconditionDefeatsDeleteAfterAnnotation(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-2"}}

	deletes := 0
	c := reapRaceClient(t, interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if run, ok := obj.(*courierv1alpha1.CoderRun); ok && run.Name == "race-cas" && deletes == 0 {
				deletes++
				// The operator's annotation lands after the fence's
				// re-read but before the delete request reaches the API
				// server. Later deletes (the retried reconcile) must pass
				// through untouched, or the test would defeat itself.
				live := &courierv1alpha1.CoderRun{}
				if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
					return err
				}
				before := live.DeepCopy()
				if live.Annotations == nil {
					live.Annotations = map[string]string{}
				}
				live.Annotations[courierv1alpha1.RetryAnnotation] = "req-race"
				if err := cl.Patch(ctx, live, client.MergeFrom(before)); err != nil {
					return err
				}
			}
			return cl.Delete(ctx, obj, opts...)
		},
	}, reapRaceRun("race-cas"))
	reconciler := terminalReapReconciler(c, src, reapClock, -1)

	_, err := reconciler.Reconcile(context.Background(), admissionRequest("race-cas"))
	if !apierrors.IsConflict(err) {
		t.Fatalf("Reconcile() error = %v, want a precondition conflict defeating the delete", err)
	}
	if src.calls != 0 {
		t.Fatalf("requeue calls = %d, want 0: the defeated reconcile never saw the annotation", src.calls)
	}
	var held courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("race-cas"), &held); err != nil {
		t.Fatalf("get run after the defeated delete: %v; the run must survive", err)
	}
	if held.Status.RetryRequest != nil {
		t.Fatalf("retryRequest = %+v on the defeated pass, want nil", held.Status.RetryRequest)
	}

	// The conflict re-arms the workqueue: the retried reconcile's snapshot now
	// carries the annotation and translates the request. The window is already
	// spent, so the same dispatch reaps the predecessor once the record
	// settles — the intent was honored, and the retention window is not
	// extended.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("race-cas")); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if src.calls != 1 {
		t.Fatalf("requeue calls = %d, want 1: the annotation must be translated, not lost with the object", src.calls)
	}
	wantKey := "coderun/" + admissionNamespace + "/race-cas/" + retryKeyDigest("req-race")
	if len(src.requests) != 1 || src.requests[0].IdempotencyKey != wantKey {
		t.Fatalf("idempotency keys = %v, want [%s]", src.requests, wantKey)
	}
	if err := c.Get(context.Background(), admissionKey("race-cas"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get raced run = %v, want NotFound once the settled request released the expired window", err)
	}
}
