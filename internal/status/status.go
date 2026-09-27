// Package status contains the status-writing contract shared by the operator
// and the coordinator harness.
//
// Status is split deliberately: the operator owns lifecycle and observed-world
// fields, while the harness owns its checkpoint, activity heartbeat, and last
// brief commit. Each writer emits a narrow JSON merge patch against the status
// subresource containing only its own fields, so one writer cannot overwrite
// the other's status and a partial write never removes fields it omitted.
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

// DefaultHeartbeatCadence bounds status writes while successful activity is
// continuous. A caller can provide a shorter cadence for a deployment that
// needs tighter liveness observation.
const DefaultHeartbeatCadence = 15 * time.Second

var (
	ErrNilPatcher = errors.New("status: nil patcher")
	ErrNilWriter  = errors.New("status: nil writer")
)

// PatchWriter is the narrow status transport used by both writers. The patch
// is a JSON merge patch document applied to a CoderRun's status subresource;
// fields absent from the patch are left untouched. Keeping this interface
// small makes status behavior testable without an API server and leaves
// transport/authentication to controller-runtime.
type PatchWriter interface {
	PatchStatus(context.Context, types.NamespacedName, []byte) error
}

// KubePatchWriter adapts a controller-runtime client to PatchWriter. Status
// is patched through the status subresource with a JSON merge patch.
type KubePatchWriter struct {
	Client client.Client
}

// PatchStatus merges a status-subresource patch into the named CoderRun. The
// payload must contain only the caller's owned status fields under "status".
func (w KubePatchWriter) PatchStatus(ctx context.Context, name types.NamespacedName, patch []byte) error {
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
		client.RawPatch(types.MergePatchType, patch),
	)
}

// OperatorPatch contains only fields owned by the operator: lifecycle and
// observed-world fields. Empty values are omitted, which is appropriate for
// additive observed status and avoids clearing harness-owned fields or
// previously published operator fields. Branch, HeadRepo, HeadSHA,
// CheckFingerprint, and Restarts are pointers because the operator must be
// able to clear them back to their zero values — a failed launch discards a
// stale resolved branch and head identity, a pending or reshaped check
// observation discards stale green evidence, and a run that demonstrates
// liveness resets its consecutive-crashloop counter to 0 — which omitempty on
// a plain string or int cannot express: omitempty on a pointer omits only
// nil, so a pointer to zero is still emitted.
type OperatorPatch struct {
	Phase            courierv1alpha1.Phase `json:"phase,omitempty"`
	Branch           *string               `json:"branch,omitempty"`
	HeadRepo         *string               `json:"headRepo,omitempty"`
	HeadSHA          *string               `json:"headSHA,omitempty"`
	PR               string                `json:"pr,omitempty"`
	CheckFingerprint *string               `json:"checkFingerprint,omitempty"`
	Restarts         *int                  `json:"restarts,omitempty"`
	Conditions       []metav1.Condition    `json:"conditions,omitempty"`
}

// HarnessPatch contains only fields owned by the coordinator harness: its
// checkpoint, its activity heartbeat, and the last brief commit it published.
type HarnessPatch struct {
	Checkpoint *courierv1alpha1.Checkpoint `json:"checkpoint,omitempty"`
	Heartbeat  *courierv1alpha1.Heartbeat  `json:"heartbeat,omitempty"`
	LastCommit string                      `json:"lastCommit,omitempty"`
}

// OperatorWriter writes only operator-owned status fields.
type OperatorWriter struct {
	patcher PatchWriter
}

// NewOperatorWriter constructs an operator status writer.
func NewOperatorWriter(patcher PatchWriter) *OperatorWriter {
	return &OperatorWriter{patcher: patcher}
}

// Patch merges operator-owned status fields into the named run.
func (w *OperatorWriter) Patch(ctx context.Context, name types.NamespacedName, fields OperatorPatch) error {
	if w == nil || w.patcher == nil {
		return ErrNilWriter
	}
	patch, err := marshalPatch(name, fields)
	if err != nil {
		return err
	}
	return w.patcher.PatchStatus(ctx, name, patch)
}

// HarnessWriter writes only harness-owned status fields.
type HarnessWriter struct {
	patcher PatchWriter
}

// NewHarnessWriter constructs a harness status writer.
func NewHarnessWriter(patcher PatchWriter) *HarnessWriter {
	return &HarnessWriter{patcher: patcher}
}

// Patch merges harness-owned status fields into the named run.
func (w *HarnessWriter) Patch(ctx context.Context, name types.NamespacedName, fields HarnessPatch) error {
	if w == nil || w.patcher == nil {
		return ErrNilWriter
	}
	patch, err := marshalPatch(name, fields)
	if err != nil {
		return err
	}
	return w.patcher.PatchStatus(ctx, name, patch)
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

// mergePatch is the JSON merge patch document for a status subresource. Only
// the status fields are present: the object is identified by the patch
// request, so the body never carries identity that a merge could misapply.
type mergePatch struct {
	Status interface{} `json:"status"`
}

func marshalPatch(name types.NamespacedName, fields interface{}) ([]byte, error) {
	if name.Name == "" {
		return nil, fmt.Errorf("status: empty object name")
	}
	return json.Marshal(mergePatch{Status: fields})
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
