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
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[doneAtAnnotation] = at.UTC().Format(time.RFC3339)
	return run
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

func TestReapDoneBeforeRetentionWaitsRemaining(t *testing.T) {
	src := &admissionSource{}
	c := phaseClient(t, withDoneAt(reapRun("young"), reapClock.Add(-3*time.Minute)))
	reconciler := reapReconciler(c, src, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("young"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 7*time.Minute {
		t.Fatalf("RequeueAfter = %v, want the remaining 7m of the window", result.RequeueAfter)
	}
	if err := c.Get(context.Background(), admissionKey("young"), &courierv1alpha1.CoderRun{}); err != nil {
		t.Fatalf("get run: %v; a young Done run must not be deleted", err)
	}
}

func TestReapDoneAfterRetentionDeletes(t *testing.T) {
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

func TestReapNeverTouchesNeedsHuman(t *testing.T) {
	src := &admissionSource{}
	run := admissionRun("stuck", "local", courierv1alpha1.PhaseNeedsHuman)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-48 * time.Hour))
	c := phaseClient(t, run)
	reconciler := reapReconciler(c, src, reapClock)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("stuck"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0; NeedsHuman carries no reap timer", result.RequeueAfter)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("stuck"), &stored); err != nil {
		t.Fatalf("get run: %v; NeedsHuman must persist regardless of age", err)
	}
	if _, ok := stored.Annotations[doneAtAnnotation]; ok {
		t.Fatal("NeedsHuman run gained a done-at marker; it must never enter the reap path")
	}
	if len(src.resolved) != 0 {
		t.Fatalf("resolved IDs = %#v, want none; NeedsHuman is not a resolved state", src.resolved)
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
