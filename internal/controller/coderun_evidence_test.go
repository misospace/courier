package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
)

// testEvidenceSecret builds a labeled, run-owned evidence Secret in the run's
// namespace using the intake-consistent name and label value, so the intake's
// List/Delete find it by the same key the hook queries.
func testEvidenceSecret(run *courierv1alpha1.CoderRun) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      evidenceSecretName(run.Name, testNonce()),
			Namespace: run.Namespace,
			Labels:    map[string]string{EvidenceLabelKey: evidenceLabelValue(run.Name)},
		},
		Data: map[string][]byte{"manifest.json": []byte(`{}`)},
	}
}

// testOtherEvidenceSecret builds a Secret labeled for a different run, so the
// hook's per-run label selection must leave it untouched.
func testOtherEvidenceSecret(ns, otherName string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-" + evidenceSecretName(otherName, testNonce()),
			Namespace: ns,
			Labels:    map[string]string{EvidenceLabelKey: evidenceLabelValue(otherName)},
		},
		Data: map[string][]byte{"manifest.json": []byte(`{}`)},
	}
}

// evidenceReconciler wires a CoderRunReconciler to a shared fake client with a
// test source and the evidence intake attached, so the reconciler and the
// intake observe the same Secrets.
func evidenceReconciler(t *testing.T, intake *EvidenceIntake, c client.Client) *CoderRunReconciler {
	t.Helper()
	return &CoderRunReconciler{
		Client:       c,
		APIReader:    c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
		Evidence:     intake,
	}
}

func TestReconcileAwaitingReviewDeletesEvidence(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseAwaitingReview)
	secret := testEvidenceSecret(run)
	intake, c := newIntake(t, testKey, run, executor.PodConfig{}, secret)

	if _, err := evidenceReconciler(t, intake, c).Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	got := &corev1.Secret{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: secret.Name}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("evidence secret still present after AwaitingReview: get err = %v", err)
	}
}

func TestReconcileDoneDeletesEvidence(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseDone)
	secret := testEvidenceSecret(run)
	intake, c := newIntake(t, testKey, run, executor.PodConfig{}, secret)

	if _, err := evidenceReconciler(t, intake, c).Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	got := &corev1.Secret{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: secret.Name}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("evidence secret still present after Done: get err = %v", err)
	}
	// The Done run is retained on its first reap pass (marker set, requeued),
	// not deleted, so the evidence delete is observable rather than swallowed
	// by the run's own removal.
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if after.Status.Phase != courierv1alpha1.PhaseDone {
		t.Fatalf("run phase = %q, want Done (retained, not deleted)", after.Status.Phase)
	}
}

func TestReconcileFailedWithEvidenceSetsCondition(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseFailed)
	secret := testEvidenceSecret(run)
	intake, c := newIntake(t, testKey, run, executor.PodConfig{}, secret)
	r := evidenceReconciler(t, intake, c)

	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run: %v", err)
	}
	cond := apimeta.FindStatusCondition(after.Status.Conditions, EvidenceConditionType)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != EvidenceReasonCaptured {
		t.Fatalf("EvidenceCaptured = %#v, want True/%s with the run's generation", cond, EvidenceReasonCaptured)
	}
	if cond.ObservedGeneration != run.Generation {
		t.Fatalf("ObservedGeneration = %d, want the run's generation %d", cond.ObservedGeneration, run.Generation)
	}

	// A second reconcile of the same terminal run must not error and must keep
	// the condition: the intake's list is idempotent and the condition is
	// already in this exact state, so no redundant status write is forced.
	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run after second reconcile: %v", err)
	}
	if cond := apimeta.FindStatusCondition(after.Status.Conditions, EvidenceConditionType); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("EvidenceCaptured after re-reconcile = %#v, want still True", cond)
	}
}

func TestReconcileFailedWithoutEvidenceNoCondition(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseFailed)
	intake, c := newIntake(t, testKey, run, executor.PodConfig{})

	if _, err := evidenceReconciler(t, intake, c).Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if cond := apimeta.FindStatusCondition(after.Status.Conditions, EvidenceConditionType); cond != nil {
		t.Fatalf("EvidenceCaptured = %#v, want none when no Secret exists", cond)
	}
}

func TestReconcileNeedsHumanRetainsEvidenceAndSetsCondition(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseNeedsHuman)
	secret := testEvidenceSecret(run)
	intake, c := newIntake(t, testKey, run, executor.PodConfig{}, secret)

	if _, err := evidenceReconciler(t, intake, c).Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	// A dirty terminal run keeps its evidence; the hook only derives the
	// condition, never deletes.
	got := &corev1.Secret{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: secret.Name}, got); err != nil {
		t.Fatalf("evidence secret deleted on NeedsHuman: get err = %v", err)
	}
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if cond := apimeta.FindStatusCondition(after.Status.Conditions, EvidenceConditionType); cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != EvidenceReasonCaptured {
		t.Fatalf("EvidenceCaptured = %#v, want True/%s", cond, EvidenceReasonCaptured)
	}
}

func TestReconcileDeletesOnlyOwnEvidence(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseAwaitingReview)
	own := testEvidenceSecret(run)
	other := testOtherEvidenceSecret(run.Namespace, "other")
	intake, c := newIntake(t, testKey, run, executor.PodConfig{}, own, other)

	if _, err := evidenceReconciler(t, intake, c).Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	// The run's own evidence is gone...
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: own.Name}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("own evidence secret not deleted: get err = %v", err)
	}
	// ...while a different run's evidence (different label value) is untouched.
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: other.Name}, &corev1.Secret{}); err != nil {
		t.Fatalf("other run's evidence secret deleted: get err = %v", err)
	}
}

func TestReconcileNilEvidenceIsNoOp(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseFailed)
	secret := testEvidenceSecret(run)
	_, c := newIntake(t, testKey, run, executor.PodConfig{}, secret)
	// The intake is built but not attached to the reconciler: the hook must be
	// a no-op, so a stray Secret survives and no condition appears.
	r := &CoderRunReconciler{
		Client:       c,
		APIReader:    c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
	}

	if _, err := r.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() with nil Evidence error = %v", err)
	}
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if cond := apimeta.FindStatusCondition(after.Status.Conditions, EvidenceConditionType); cond != nil {
		t.Fatalf("EvidenceCaptured = %#v, want none with a nil intake", cond)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: secret.Name}, &corev1.Secret{}); err != nil {
		t.Fatalf("stray evidence secret touched with a nil intake: get err = %v", err)
	}
}

func TestReconcileRunningEvidenceNoOp(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	secret := testEvidenceSecret(run)
	pod := runningCoordinatorPod(run)
	intake, c := newIntake(t, testKey, run, executor.PodConfig{}, secret, pod)

	if _, err := evidenceReconciler(t, intake, c).Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	// Running carries no evidence action: the Secret is neither deleted nor
	// read into a condition.
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: secret.Name}, &corev1.Secret{}); err != nil {
		t.Fatalf("evidence secret deleted on Running: get err = %v", err)
	}
	var after courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("run"), &after); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if cond := apimeta.FindStatusCondition(after.Status.Conditions, EvidenceConditionType); cond != nil {
		t.Fatalf("EvidenceCaptured = %#v, want none on a Running run", cond)
	}
}
