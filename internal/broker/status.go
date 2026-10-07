package broker

import (
	"context"
	"errors"
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
// Active-operation changes are typed operations, not a map replacement: the
// writer applies add/clear against its fresh read under resourceVersion CAS,
// so clearing one entry can never drop another entry or another incarnation's
// state.
type HarnessPatch struct {
	Checkpoint *courier.Checkpoint `json:"checkpoint,omitempty"`
	Heartbeat  *courier.Heartbeat  `json:"heartbeat,omitempty"`
	LastCommit *string             `json:"lastCommit,omitempty"`

	// AddOperation registers one dispatched operation under its operation ID.
	// It is idempotent: re-adding the identical entry succeeds, and a
	// conflicting re-add is refused.
	AddOperation *OperationAddition `json:"addOperation,omitempty"`
	// ClearOperation removes exactly one operation entry by ID. Clearing an
	// absent ID succeeds: clears are retried idempotently.
	ClearOperation string `json:"clearOperation,omitempty"`
	// ReconcileOperations drops the entire active-operation set. A fresh
	// control process holds no in-flight operation in memory, only one live
	// control incarnation may exist (the authentication layer enforces it),
	// so every persisted entry at process start refers to process memory that
	// no longer exists. The broker refuses writes from any but the current
	// control incarnation, so this cannot drop a concurrent writer's state.
	ReconcileOperations bool `json:"reconcileOperations,omitempty"`
}

// OperationAddition carries the operation ID (the map key) and the entry.
type OperationAddition struct {
	OpID      string                  `json:"opID"`
	Operation courier.ActiveOperation `json:"operation"`
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
	if patch.Checkpoint == nil && patch.Heartbeat == nil && patch.LastCommit == nil &&
		patch.AddOperation == nil && patch.ClearOperation == "" && !patch.ReconcileOperations {
		return fmt.Errorf("empty harness status patch")
	}
	if patch.LastCommit != nil && validate == nil {
		return fmt.Errorf("lastCommit requires live-world validation")
	}
	// UID fencing: a heartbeat or operation entry is only ever written for the
	// authenticated current control incarnation. A stale incarnation cannot
	// authenticate at all, but the belt-and-suspenders check keeps a confused
	// or forging caller from attributing activity to another pod.
	if patch.Heartbeat != nil && patch.Heartbeat.CoordinatorPodUID != string(identity.ControlPodUID) {
		return fmt.Errorf("heartbeat UID does not match the authenticated control incarnation")
	}
	if patch.AddOperation != nil {
		if err := validOperationIdentity(identity, patch.AddOperation); err != nil {
			return err
		}
	}

	key := types.NamespacedName{Namespace: w.namespace, Name: w.runName}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		run := &courier.CoderRun{}
		if err := w.reader.Get(ctx, key, run); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("read CoderRun for status: %w", err)
			}
			return &StatusUnavailableError{Err: fmt.Errorf("read CoderRun for status: %w", err)}
		}
		if run.UID != identity.RunUID || run.DeletionTimestamp != nil {
			return fmt.Errorf("authenticated CoderRun incarnation is no longer live")
		}
		pod := &corev1.Pod{}
		if err := w.reader.Get(ctx, types.NamespacedName{Namespace: w.namespace, Name: identity.ControlPod}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("read current control pod: %w", err)
			}
			return &StatusUnavailableError{Err: fmt.Errorf("read current control pod: %w", err)}
		}
		ownerName, ownerUID, ok := controllerRunOwner(pod)
		if pod.UID != identity.ControlPodUID || pod.DeletionTimestamp != nil || pod.Spec.ServiceAccountName != identity.ControlServiceAccount || !ok || ownerName != w.runName || ownerUID != identity.RunUID || pod.Labels["courier.misospace.dev/coderrun"] != w.runName || pod.Labels["courier.misospace.dev/component"] != "coordinator" {
			return fmt.Errorf("authenticated control pod is stale or is not owned by this CoderRun")
		}
		pods := &corev1.PodList{}
		if err := w.reader.List(ctx, pods, ctrlclient.InNamespace(w.namespace)); err != nil {
			return &StatusUnavailableError{Err: fmt.Errorf("list current control pods: %w", err)}
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
				// A validator that reports its own backend failure (e.g. a
				// transient provider read) stays transient; anything else is
				// a definite live-world mismatch.
				var unavailable *StatusUnavailableError
				if errors.As(err, &unavailable) {
					return err
				}
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
		if patch.ReconcileOperations {
			run.Status.ActiveOperations = nil
		}
		if patch.AddOperation != nil {
			operations := run.Status.ActiveOperations
			if operations == nil {
				operations = make(map[string]courier.ActiveOperation, 1)
			}
			if existing, ok := operations[patch.AddOperation.OpID]; ok {
				if !sameOperationEvidence(existing, patch.AddOperation.Operation) {
					return fmt.Errorf("operation %s is already recorded with different evidence", patch.AddOperation.OpID)
				}
			} else {
				if len(operations) >= maxActiveOperations {
					return fmt.Errorf("active-operation set is full (%d entries)", maxActiveOperations)
				}
				operations[patch.AddOperation.OpID] = patch.AddOperation.Operation
			}
			run.Status.ActiveOperations = operations
		}
		if patch.ClearOperation != "" {
			delete(run.Status.ActiveOperations, patch.ClearOperation)
			if len(run.Status.ActiveOperations) == 0 {
				run.Status.ActiveOperations = nil
			}
		}
		if err := w.writer.Status().Update(ctx, run); err != nil {
			if apierrors.IsConflict(err) {
				return err
			}
			return &StatusUnavailableError{Err: fmt.Errorf("update CoderRun status: %w", err)}
		}
		return nil
	})
	if apierrors.IsConflict(err) {
		return &StatusConflictError{Err: err}
	}
	return err
}

// StatusUnavailableError reports a temporary failure of the status backend:
// an API-server read or write that failed without a definite answer, or a
// provider read a live-world validator depends on. Callers treat it as
// transient availability, never as a denial.
type StatusUnavailableError struct{ Err error }

func (e *StatusUnavailableError) Error() string { return e.Err.Error() }
func (e *StatusUnavailableError) Unwrap() error { return e.Err }

// StatusConflictError reports a status write that lost the
// resourceVersion CAS race past every retry. Callers treat it as
// transient: the write raced a concurrent status edit, and re-reading
// the world is always safe.
type StatusConflictError struct{ Err error }

func (e *StatusConflictError) Error() string { return e.Err.Error() }
func (e *StatusConflictError) Unwrap() error { return e.Err }

// sameOperationEvidence reports whether two records of one operation carry
// identical evidence. Timestamps compare as instants, not as struct values:
// a re-acknowledged write may carry a different clock representation of the
// same second, and refusing it would break the idempotent re-add that
// resolves an uncertain acknowledgment.
func sameOperationEvidence(a, b courier.ActiveOperation) bool {
	return a.BriefID == b.BriefID &&
		a.CoordinatorPodUID == b.CoordinatorPodUID &&
		a.WorkerPodUID == b.WorkerPodUID &&
		a.DispatchedAt.Equal(&b.DispatchedAt)
}

// maxActiveOperations bounds the per-run active-operation set. It is a fixed
// resource bound on delivered status state (the same class as the artifact
// bundle bounds), not a cap on model behavior: concurrent operations are
// single digits by construction.
const maxActiveOperations = 64

// validOperationIdentity checks one operation addition's structural identity:
// the entry must attribute itself to the authenticated control incarnation,
// carry a well-formed operation ID and brief ID, and name its worker pod.
func validOperationIdentity(identity StatusIdentity, addition *OperationAddition) error {
	if !validOperationID(addition.OpID) {
		return fmt.Errorf("operation ID is malformed")
	}
	if !validOperationID(addition.Operation.BriefID) {
		return fmt.Errorf("operation brief ID is malformed")
	}
	if addition.Operation.CoordinatorPodUID != string(identity.ControlPodUID) {
		return fmt.Errorf("operation UID does not match the authenticated control incarnation")
	}
	if addition.Operation.WorkerPodUID == "" {
		return fmt.Errorf("operation carries no worker pod UID")
	}
	return nil
}

// validOperationID reports whether the identifier is a bounded single path
// segment from the same conservative alphabet the harness mints operation and
// brief IDs from.
func validOperationID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}
