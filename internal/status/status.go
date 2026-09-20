// Package status contains the status-writing contract shared by the operator
// and the coordinator harness.
//
// Status is split deliberately: the operator owns lifecycle and observed-world
// fields, while the harness owns its checkpoint and activity heartbeat. Each
// writer emits a server-side-apply patch containing only its fields and uses a
// distinct field manager, so one writer cannot accidentally overwrite the
// other's status.
package status

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

const (
	// OperatorFieldManager owns lifecycle and observed-world status fields.
	OperatorFieldManager = "courier-operator"
	// HarnessFieldManager owns checkpoint and activity status fields.
	HarnessFieldManager = "courier-harness"

	// DefaultHeartbeatCadence bounds status writes while successful activity is
	// continuous. A caller can provide a shorter cadence for a deployment that
	// needs tighter liveness observation.
	DefaultHeartbeatCadence = 15 * time.Second
)

var (
	ErrNilPatcher = errors.New("status: nil patcher")
	ErrNilWriter  = errors.New("status: nil writer")
)

// PatchWriter is the narrow status transport used by both writers. The patch
// is a Kubernetes server-side-apply document and manager is its field manager.
// Keeping this interface small makes status behavior testable without an API
// server and leaves transport/authentication to controller-runtime.
type PatchWriter interface {
	PatchStatus(context.Context, types.NamespacedName, []byte, string) error
}

// KubePatchWriter adapts a controller-runtime client to PatchWriter. Status is
// patched through the status subresource using server-side apply.
type KubePatchWriter struct {
	Client client.Client
}

// PatchStatus applies a status-subresource patch with the requested field
// manager. The payload must identify a CoderRun and contain only the caller's
// owned status fields.
func (w KubePatchWriter) PatchStatus(ctx context.Context, name types.NamespacedName, patch []byte, manager string) error {
	if w.Client == nil {
		return ErrNilPatcher
	}
	if name.Name == "" {
		return fmt.Errorf("status: empty object name")
	}
	obj := &courierv1alpha1.CoderRun{}
	obj.Namespace = name.Namespace
	obj.Name = name.Name
	return w.Client.Status().Patch(ctx, obj,
		client.RawPatch(types.ApplyPatchType, patch),
		client.FieldOwner(manager),
	)
}

// OperatorPatch contains only fields owned by the operator. Empty values are
// omitted, which is appropriate for additive observed status and avoids
// clearing harness-owned fields or previously published operator fields.
// Branch is a pointer because a failed launch must be able to clear a stale
// resolved branch back to empty, which omitempty on a plain string cannot
// express.
type OperatorPatch struct {
	Phase      courierv1alpha1.Phase `json:"phase,omitempty"`
	Branch     *string               `json:"branch,omitempty"`
	PR         string                `json:"pr,omitempty"`
	LastCommit string                `json:"lastCommit,omitempty"`
	Restarts   int                   `json:"restarts,omitempty"`
	Conditions []metav1.Condition    `json:"conditions,omitempty"`
}

// HarnessPatch contains only fields owned by the coordinator harness.
type HarnessPatch struct {
	Checkpoint *courierv1alpha1.Checkpoint `json:"checkpoint,omitempty"`
	Heartbeat  *courierv1alpha1.Heartbeat  `json:"heartbeat,omitempty"`
}

type applyPatch struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Metadata   patchObject `json:"metadata"`
	Status     interface{} `json:"status"`
}

type patchObject struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// OperatorWriter writes only operator-owned status fields.
type OperatorWriter struct {
	patcher PatchWriter
}

// NewOperatorWriter constructs an operator status writer.
func NewOperatorWriter(patcher PatchWriter) *OperatorWriter {
	return &OperatorWriter{patcher: patcher}
}

// Patch applies operator-owned status fields with OperatorFieldManager.
func (w *OperatorWriter) Patch(ctx context.Context, name types.NamespacedName, fields OperatorPatch) error {
	if w == nil || w.patcher == nil {
		return ErrNilWriter
	}
	patch, err := marshalPatch(name, fields)
	if err != nil {
		return err
	}
	return w.patcher.PatchStatus(ctx, name, patch, OperatorFieldManager)
}

// HarnessWriter writes only harness-owned status fields.
type HarnessWriter struct {
	patcher PatchWriter
}

// NewHarnessWriter constructs a harness status writer.
func NewHarnessWriter(patcher PatchWriter) *HarnessWriter {
	return &HarnessWriter{patcher: patcher}
}

// Patch applies harness-owned status fields with HarnessFieldManager.
func (w *HarnessWriter) Patch(ctx context.Context, name types.NamespacedName, fields HarnessPatch) error {
	if w == nil || w.patcher == nil {
		return ErrNilWriter
	}
	patch, err := marshalPatch(name, fields)
	if err != nil {
		return err
	}
	return w.patcher.PatchStatus(ctx, name, patch, HarnessFieldManager)
}

// Checkpoint persists a checkpoint without touching the heartbeat or any
// operator-owned status field.
func (w *HarnessWriter) Checkpoint(ctx context.Context, name types.NamespacedName, checkpoint *courierv1alpha1.Checkpoint) error {
	return w.Patch(ctx, name, HarnessPatch{Checkpoint: checkpoint})
}

// Heartbeat persists a heartbeat without touching the checkpoint or any
// operator-owned status field.
func (w *HarnessWriter) Heartbeat(ctx context.Context, name types.NamespacedName, heartbeat *courierv1alpha1.Heartbeat) error {
	return w.Patch(ctx, name, HarnessPatch{Heartbeat: heartbeat})
}

func marshalPatch(name types.NamespacedName, fields interface{}) ([]byte, error) {
	if name.Name == "" {
		return nil, fmt.Errorf("status: empty object name")
	}
	return json.Marshal(applyPatch{
		APIVersion: courierv1alpha1.GroupVersion.String(),
		Kind:       "CoderRun",
		Metadata: patchObject{
			Name:      name.Name,
			Namespace: name.Namespace,
		},
		Status: fields,
	})
}

// Clock is the small time dependency needed by Heartbeat. Tests can provide a
// deterministic clock without sleeping.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Heartbeat records successful activity and coalesces writes to at most one
// status patch per cadence. The caller should invoke Record only after a
// successful model stream chunk or tool boundary; failed attempts must not
// keep a run alive.
type Heartbeat struct {
	writer  *HarnessWriter
	name    types.NamespacedName
	clock   Clock
	cadence time.Duration

	mu          sync.Mutex
	lastWritten time.Time
	pending     *courierv1alpha1.Heartbeat
}

// NewHeartbeat constructs a coalescing heartbeat reporter. A non-positive
// cadence uses DefaultHeartbeatCadence.
func NewHeartbeat(writer *HarnessWriter, name types.NamespacedName, clock Clock, cadence time.Duration) *Heartbeat {
	if clock == nil {
		clock = realClock{}
	}
	if cadence <= 0 {
		cadence = DefaultHeartbeatCadence
	}
	return &Heartbeat{
		writer:  writer,
		name:    name,
		clock:   clock,
		cadence: cadence,
	}
}

// Record records a successful activity kind. The first activity is written
// immediately. Further activity is coalesced until cadence has elapsed, at
// which point the newest activity is written. A failed write is not counted,
// so the next activity retries it.
func (h *Heartbeat) Record(ctx context.Context, kind string) error {
	if h == nil || h.writer == nil {
		return ErrNilWriter
	}
	now := h.clock.Now()
	heartbeat := &courierv1alpha1.Heartbeat{
		At:   metav1.NewTime(now),
		Kind: kind,
	}

	h.mu.Lock()
	h.pending = heartbeat
	if !h.lastWritten.IsZero() && now.Sub(h.lastWritten) < h.cadence {
		h.mu.Unlock()
		return nil
	}
	err := h.writer.Heartbeat(ctx, h.name, h.pending)
	if err == nil {
		h.lastWritten = now
		h.pending = nil
	}
	h.mu.Unlock()
	return err
}
