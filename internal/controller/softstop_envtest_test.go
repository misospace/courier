package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/status"
)

// TestEnvtestSoftStopWireContract proves the #255 done-condition against a
// real API server: the stamped control request persists through the status
// subresource (the CRD carries the fields and the enum, nothing is pruned),
// the harness-owned acknowledgement round-trips through the merge-patch path,
// an explicit null clears the record (an omitted field never does), and the
// kind enum closes admission to soft-stop.
func TestEnvtestSoftStopWireContract(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	name := "softstop-wire"
	run := envtestRun(name, "envtest-lifecycle")
	run.Annotations = map[string]string{courierv1alpha1.SoftStopAnnotation: "wire-req-1"}
	run.Status.Phase = courierv1alpha1.PhaseRunning
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: run.Namespace,
			Name:      name + "-control",
			Labels: map[string]string{
				"courier.misospace.dev/coderrun":  name,
				"courier.misospace.dev/component": "coordinator",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "coordinator", Image: "courier-control:test"}},
		},
	}
	if err := envtestClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = envtestClient.Delete(ctx, pod)
		_ = envtestClient.Delete(ctx, run)
	})

	reconciler := &CoderRunReconciler{Client: envtestClient, Now: func() time.Time { return now }}
	pod.UID = types.UID("aaaa1111-2222-3333-4444-555555555555")
	if err := reconciler.syncSoftStop(ctx, run, []corev1.Pod{*pod}); err != nil {
		t.Fatalf("syncSoftStop: %v", err)
	}
	var stamped courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, &stamped); err != nil {
		t.Fatal(err)
	}
	record := stamped.Status.ControlRequest
	if record == nil || record.ID != "wire-req-1" || record.Kind != courierv1alpha1.ControlRequestSoftStop ||
		record.TargetPodUID != string(pod.UID) || !record.RequestedAt.Time.Equal(now) {
		t.Fatalf("persisted controlRequest = %+v, want the full fenced record", record)
	}

	// The harness-side acknowledgement travels the merge-patch status path.
	// The broker applies the same schema through its fenced writer.
	writer := status.NewHarnessWriter(status.KubePatchWriter{Client: envtestClient})
	if err := writer.Patch(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, status.HarnessPatch{
		ControlRequestAck: &courierv1alpha1.ControlRequestAck{ID: "wire-req-1", At: metav1.NewTime(now)},
	}); err != nil {
		t.Fatalf("harness ack patch: %v", err)
	}
	var acked courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, &acked); err != nil {
		t.Fatal(err)
	}
	if acked.Status.ControlRequestAck == nil || acked.Status.ControlRequestAck.ID != "wire-req-1" {
		t.Fatalf("persisted controlRequestAck = %+v, want the served acknowledgement", acked.Status.ControlRequestAck)
	}

	// Clearing the annotation clears the record; omission alone would not.
	withdrawn := acked.DeepCopy()
	withdrawn.Annotations = nil
	if err := reconciler.syncSoftStop(ctx, withdrawn, []corev1.Pod{*pod}); err != nil {
		t.Fatalf("syncSoftStop withdraw: %v", err)
	}
	var cleared courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, &cleared); err != nil {
		t.Fatal(err)
	}
	if cleared.Status.ControlRequest != nil || cleared.Status.ControlRequestAck != nil {
		t.Fatalf("after withdrawal: controlRequest = %+v, controlRequestAck = %+v, want both gone",
			cleared.Status.ControlRequest, cleared.Status.ControlRequestAck)
	}

	// The kind enum is CRD-closed: any other action is refused at admission.
	invalid := cleared.DeepCopy()
	invalid.Status.ControlRequest = &courierv1alpha1.ControlRequest{
		ID: "wire-req-2", Kind: courierv1alpha1.ControlRequestKind("reboot"),
		TargetPodUID: string(pod.UID), RequestedAt: metav1.NewTime(now),
	}
	err := envtestClient.Status().Update(ctx, invalid)
	if !apierrors.IsInvalid(err) && !apierrors.IsBadRequest(err) {
		t.Fatalf("non-soft-stop kind admitted with err = %v", err)
	}
}
