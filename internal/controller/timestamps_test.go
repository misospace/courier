package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// timestampClock is the pinned reconciler clock for these tests; each test
// advances it by hand to prove the set-once fields never move again.
var timestampClock = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func timestampReconciler(c client.Client, now time.Time) *CoderRunReconciler {
	return &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
		Now:          func() time.Time { return now },
	}
}

func TestAdmitSetsAdmittedAtOnce(t *testing.T) {
	now := timestampClock
	client := phaseClient(t,
		admissionLane("local", 1),
		admissionRun("run", "local", courierv1alpha1.PhasePending),
	)
	reconciler := timestampReconciler(client, now)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var run courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", run.Status.Phase)
	}
	if run.Status.AdmittedAt == nil {
		t.Fatal("AdmittedAt = nil, want the admission time")
	}
	admitted := run.Status.AdmittedAt.Time
	if !admitted.Equal(now) {
		t.Fatalf("AdmittedAt = %v, want %v", admitted, now)
	}

	// An advanced clock must not move a recorded timestamp.
	reconciler.Now = func() time.Time { return now.Add(time.Hour) }
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := client.Get(context.Background(), admissionKey("run"), &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.AdmittedAt == nil || !run.Status.AdmittedAt.Time.Equal(admitted) {
		t.Fatalf("AdmittedAt = %v after second reconcile, want unchanged %v", run.Status.AdmittedAt, admitted)
	}
}

func TestRunningRecordsStartedAtFromWorld(t *testing.T) {
	start := timestampClock.Add(-30 * time.Minute)
	creation := start.Add(-30 * time.Minute)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.CreationTimestamp = metav1.NewTime(creation)
	pod := runningCoordinatorPod(run)
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(start)
	client := phaseClient(t, run, pod)
	reconciler := timestampReconciler(client, timestampClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status.StartedAt == nil {
		t.Fatal("StartedAt = nil, want the container's running start")
	}
	if !got.Status.StartedAt.Time.Equal(start) {
		t.Fatalf("StartedAt = %v, want the container's start %v, not the reconciler clock", got.Status.StartedAt.Time, start)
	}
	if got.Status.WaitDuration != "30m0s" {
		t.Fatalf("WaitDuration = %q, want 30m0s (creation to start)", got.Status.WaitDuration)
	}

	reconciler.Now = func() time.Time { return timestampClock.Add(time.Hour) }
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !got.Status.StartedAt.Time.Equal(start) || got.Status.WaitDuration != "30m0s" {
		t.Fatalf("StartedAt/WaitDuration = %v/%q after second reconcile, want unchanged %v/30m0s", got.Status.StartedAt.Time, got.Status.WaitDuration, start)
	}
}

func TestFailedPodRecordsStartedAndFinished(t *testing.T) {
	start := timestampClock.Add(-30 * time.Minute)
	creation := start.Add(-5 * time.Minute)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.CreationTimestamp = metav1.NewTime(creation)
	pod := coordinatorPod(run, 17)
	pod.Status.ContainerStatuses[0].State.Terminated.StartedAt = metav1.NewTime(start)
	client := phaseClient(t, run, pod)
	reconciler := timestampReconciler(client, timestampClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if got.Status.StartedAt == nil || !got.Status.StartedAt.Time.Equal(start) {
		t.Fatalf("StartedAt = %v, want the terminated container's start %v, recorded in the same reconcile as the failure", got.Status.StartedAt, start)
	}
	if got.Status.FinishedAt == nil || !got.Status.FinishedAt.Time.Equal(timestampClock) {
		t.Fatalf("FinishedAt = %v, want %v", got.Status.FinishedAt, timestampClock)
	}
	if got.Status.RunDuration != "30m0s" {
		t.Fatalf("RunDuration = %q, want 30m0s (start to finish)", got.Status.RunDuration)
	}
	if got.Status.WaitDuration != "5m0s" {
		t.Fatalf("WaitDuration = %q, want 5m0s (creation to start)", got.Status.WaitDuration)
	}

	reconciler.Now = func() time.Time { return timestampClock.Add(time.Hour) }
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !got.Status.StartedAt.Time.Equal(start) || !got.Status.FinishedAt.Time.Equal(timestampClock) ||
		got.Status.RunDuration != "30m0s" || got.Status.WaitDuration != "5m0s" {
		t.Fatalf("timestamps moved on repeat: started %v, finished %v, run %q, wait %q",
			got.Status.StartedAt.Time, got.Status.FinishedAt.Time, got.Status.RunDuration, got.Status.WaitDuration)
	}
}

func TestFailedWithoutContainerStart(t *testing.T) {
	// A terminated coordinator whose state recorded no start time (a pod that
	// never got as far as one) has nothing to pair a RunDuration with: the
	// failure still records FinishedAt, and StartedAt and RunDuration stay
	// unset.
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.CreationTimestamp = metav1.NewTime(timestampClock.Add(-time.Hour))
	pod := coordinatorPod(run, 17) // Terminated with a zero StartedAt
	client := phaseClient(t, run, pod)
	reconciler := timestampReconciler(client, timestampClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if got.Status.FinishedAt == nil || !got.Status.FinishedAt.Time.Equal(timestampClock) {
		t.Fatalf("FinishedAt = %v, want %v", got.Status.FinishedAt, timestampClock)
	}
	if got.Status.StartedAt != nil {
		t.Fatalf("StartedAt = %v, want nil: no container start was ever observed", got.Status.StartedAt.Time)
	}
	if got.Status.RunDuration != "" {
		t.Fatalf("RunDuration = %q, want empty: it cannot be derived without a start", got.Status.RunDuration)
	}
}

func TestReAdmittedRunKeepsOriginalAdmittedAt(t *testing.T) {
	original := timestampClock.Add(-24 * time.Hour)
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	// A released claim keeps its original admission time in status; a
	// re-admission must not move it.
	run.Status.AdmittedAt = &metav1.Time{Time: original}
	client := phaseClient(t,
		admissionLane("local", 1),
		run,
	)
	reconciler := timestampReconciler(client, timestampClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", got.Status.Phase)
	}
	if got.Status.AdmittedAt == nil || !got.Status.AdmittedAt.Time.Equal(original) {
		t.Fatalf("AdmittedAt = %v, want the original admission time %v (re-admission never moves it)", got.Status.AdmittedAt, original)
	}
}

func TestAwaitingReviewRecordsFinishedAndRunDuration(t *testing.T) {
	start := timestampClock.Add(-30 * time.Minute)
	creation := start.Add(-5 * time.Minute)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.CreationTimestamp = metav1.NewTime(creation)
	pod := coordinatorPod(run, 3) // verified no_change_needed
	pod.Status.ContainerStatuses[0].State.Terminated.StartedAt = metav1.NewTime(start)
	client := phaseClient(t, run, pod)
	reconciler := timestampReconciler(client, timestampClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase = %q, want AwaitingReview", got.Status.Phase)
	}
	if got.Status.FinishedAt == nil || !got.Status.FinishedAt.Time.Equal(timestampClock) {
		t.Fatalf("FinishedAt = %v, want %v: a no-change declaration ends the run's execution", got.Status.FinishedAt, timestampClock)
	}
	if got.Status.RunDuration != "30m0s" {
		t.Fatalf("RunDuration = %q, want 30m0s (start to finish)", got.Status.RunDuration)
	}

	// A later reconcile must not move the set-once timestamp.
	reconciler.Now = func() time.Time { return timestampClock.Add(time.Hour) }
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !got.Status.FinishedAt.Time.Equal(timestampClock) || got.Status.RunDuration != "30m0s" {
		t.Fatalf("timestamps moved on repeat: finished %v, run %q", got.Status.FinishedAt.Time, got.Status.RunDuration)
	}
}

func TestDoneRecordsFinishedAndRunDuration(t *testing.T) {
	start := timestampClock.Add(-45 * time.Minute)
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Source = "test"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	run.Status.StartedAt = &metav1.Time{Time: start}
	client := phaseClient(t, run)
	reconciler := timestampReconciler(client, timestampClock)
	reconciler.Observer = fakeWorldObserver{observation: PRObservation{PR: "42", Merged: true}}

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status.Phase != courierv1alpha1.PhaseDone {
		t.Fatalf("phase = %q, want Done", got.Status.Phase)
	}
	if got.Status.FinishedAt == nil || !got.Status.FinishedAt.Time.Equal(timestampClock) {
		t.Fatalf("FinishedAt = %v, want %v", got.Status.FinishedAt, timestampClock)
	}
	if got.Status.RunDuration != "45m0s" {
		t.Fatalf("RunDuration = %q, want 45m0s (start to finish)", got.Status.RunDuration)
	}

	// A later reconcile (now in Done, taking the resolve/reap path) must not
	// move the set-once timestamp.
	reconciler.Now = func() time.Time { return timestampClock.Add(time.Hour) }
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !got.Status.FinishedAt.Time.Equal(timestampClock) || got.Status.RunDuration != "45m0s" {
		t.Fatalf("timestamps moved on repeat: finished %v, run %q", got.Status.FinishedAt.Time, got.Status.RunDuration)
	}
}

func TestNegativeWaitDurationClampedToZero(t *testing.T) {
	// A kubelet clock running behind the apiserver can report the container
	// start before the run was created; the derived wait must clamp to zero
	// rather than go negative.
	start := timestampClock.Add(-30 * time.Minute)
	creation := timestampClock.Add(-25 * time.Minute)
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.CreationTimestamp = metav1.NewTime(creation)
	pod := runningCoordinatorPod(run)
	pod.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(start)
	client := phaseClient(t, run, pod)
	reconciler := timestampReconciler(client, timestampClock)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var got courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status.StartedAt == nil || !got.Status.StartedAt.Time.Equal(start) {
		t.Fatalf("StartedAt = %v, want the observed container start %v", got.Status.StartedAt, start)
	}
	if got.Status.WaitDuration != "0s" {
		t.Fatalf("WaitDuration = %q, want 0s (negative wait clamped)", got.Status.WaitDuration)
	}
}
