package broker

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// HarnessPatch is the closed set of status fields trusted harness control may
// write. It intentionally has no arbitrary map or raw status escape hatch.
type HarnessPatch struct {
	Checkpoint *courier.Checkpoint
	Heartbeat  *courier.Heartbeat
	LastCommit *string
}

// StatusIdentity is the authenticated run and control-pod incarnation supplied
// by the broker's authentication layer.
type StatusIdentity struct {
	RunUID                types.UID
	ControlPod            string
	ControlPodUID         types.UID
	ControlServiceAccount string
}

// StatusValidator re-checks operation-specific live-world preconditions. It is
// called with a fresh run and pod on every CAS attempt, including after conflict.
type StatusValidator func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error

// StatusWriter combines an uncached live reader with a client authorized for
// the named run's status subresource only.
type StatusWriter struct {
	reader    ctrlclient.Reader
	writer    ctrlclient.Client
	namespace string
	runName   string
}

func NewStatusWriter(reader ctrlclient.Reader, writer ctrlclient.Client, namespace, runName string) (*StatusWriter, error) {
	if reader == nil || writer == nil || namespace == "" || runName == "" {
		return nil, fmt.Errorf("broker status configuration is incomplete")
	}
	return &StatusWriter{reader: reader, writer: writer, namespace: namespace, runName: runName}, nil
}

// Write applies only HarnessPatch fields after validating the authenticated
// incarnation and current control pod. Status Update carries resourceVersion,
// so concurrent edits conflict instead of silently replacing map/list state.
func (w *StatusWriter) Write(ctx context.Context, identity StatusIdentity, patch HarnessPatch, validate StatusValidator) error {
	if w == nil || w.reader == nil || w.writer == nil {
		return fmt.Errorf("broker status writer is not configured")
	}
	if identity.RunUID == "" || identity.ControlPod == "" || identity.ControlPodUID == "" || identity.ControlServiceAccount == "" {
		return fmt.Errorf("authenticated status identity is incomplete")
	}
	if patch.Checkpoint == nil && patch.Heartbeat == nil && patch.LastCommit == nil {
		return fmt.Errorf("empty harness status patch")
	}
	if patch.LastCommit != nil && validate == nil {
		return fmt.Errorf("lastCommit requires live-world validation")
	}

	key := types.NamespacedName{Namespace: w.namespace, Name: w.runName}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		run := &courier.CoderRun{}
		if err := w.reader.Get(ctx, key, run); err != nil {
			return fmt.Errorf("read CoderRun for status: %w", err)
		}
		if run.UID != identity.RunUID || run.DeletionTimestamp != nil {
			return fmt.Errorf("authenticated CoderRun incarnation is no longer live")
		}
		pod := &corev1.Pod{}
		if err := w.reader.Get(ctx, types.NamespacedName{Namespace: w.namespace, Name: identity.ControlPod}, pod); err != nil {
			return fmt.Errorf("read current control pod: %w", err)
		}
		ownerName, ownerUID, ok := controllerRunOwner(pod)
		if pod.UID != identity.ControlPodUID || pod.DeletionTimestamp != nil || pod.Spec.ServiceAccountName != identity.ControlServiceAccount || !ok || ownerName != w.runName || ownerUID != identity.RunUID || pod.Labels["courier.misospace.dev/coderrun"] != w.runName || pod.Labels["courier.misospace.dev/component"] != "coordinator" {
			return fmt.Errorf("authenticated control pod is stale or is not owned by this CoderRun")
		}
		pods := &corev1.PodList{}
		selector := ctrlclient.MatchingLabels{"courier.misospace.dev/coderrun": w.runName, "courier.misospace.dev/component": "coordinator"}
		if err := w.reader.List(ctx, pods, selector, ctrlclient.InNamespace(w.namespace)); err != nil {
			return fmt.Errorf("list current control pods: %w", err)
		}
		current := 0
		for i := range pods.Items {
			candidate := &pods.Items[i]
			candidateOwner, candidateUID, owned := controllerRunOwner(candidate)
			if candidate.DeletionTimestamp == nil && candidate.Spec.ServiceAccountName == identity.ControlServiceAccount && owned && candidateOwner == w.runName && candidateUID == identity.RunUID {
				current++
				if candidate.UID != identity.ControlPodUID || candidate.Name != identity.ControlPod {
					return fmt.Errorf("authenticated control pod is not the current run incarnation")
				}
			}
		}
		if current != 1 {
			return fmt.Errorf("expected exactly one live control pod for CoderRun, found %d", current)
		}
		if validate != nil {
			if err := validate(ctx, run, pod, patch); err != nil {
				return fmt.Errorf("validate live status preconditions: %w", err)
			}
		}

		run = run.DeepCopy()
		if patch.Checkpoint != nil {
			run.Status.Checkpoint = patch.Checkpoint.DeepCopy()
		}
		if patch.Heartbeat != nil {
			heartbeat := *patch.Heartbeat
			run.Status.Heartbeat = &heartbeat
		}
		if patch.LastCommit != nil {
			run.Status.LastCommit = *patch.LastCommit
		}
		if err := w.writer.Status().Update(ctx, run); err != nil {
			if apierrors.IsConflict(err) {
				return err
			}
			return fmt.Errorf("update CoderRun status: %w", err)
		}
		return nil
	})
}
