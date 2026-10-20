package harness

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/misospace/courier/internal/protocol"
)

// ErrAmbiguousDispatch reports that a dispatch's transport outcome is
// unknown: the worker may have accepted the task. Control does not
// automatically redeliver — it cancels the operation and reconciles until
// process termination is proven, and only then may new work start.
var ErrAmbiguousDispatch = errors.New("harness: dispatch transport failed ambiguously; cancel and reconcile before retrying")

// ErrTerminationUnverified marks a bound-expiry or cancellation whose
// reconciliation could not observe process termination: the cancellation
// may never have reached the worker, or the worker reported work that
// outlived the task. Callers may classify the deadline, but never claim
// the task stopped; the coordinator fences the worker until its termination
// is independently observed or the worker itself is replaced.
var ErrTerminationUnverified = errors.New("harness: operation termination could not be verified")

// terminationObserved reports whether a reconciled state proves nothing
// from the operation is still live: the worker definitively does not know
// the operation (observed absence), or the operation reached a terminal
// status and either carried no result (a cancelled or unknown observation,
// which is a definitive non-execution) or reported an empty process tree.
// Anything else — still running, or an unrecognized status — is not
// observed termination.
func terminationObserved(state protocol.ResultState) bool {
	switch state.Status {
	case "":
		return true
	case protocol.ResultCompleted, protocol.ResultFailed, protocol.ResultCancelled:
		return state.Result == nil || state.Result.LiveDescendants == 0
	default:
		return false
	}
}

// WorkerIdentity is the identity material every signed envelope binds: the
// run, the current control incarnation, and the worker pod incarnation.
type WorkerIdentity struct {
	RunUID        string
	ControlPodUID string
	WorkerPodUID  string
	Key           ed25519.PrivateKey
	// AdmissionWindow bounds envelope admission and replay only; it is never
	// an operation-duration limit. Zero uses the protocol default.
	AdmissionWindow time.Duration
}

// Delegator dispatches bounded work to the untrusted worker over the signed
// protocol (HARNESS.md §5). Everything a worker returns through it is
// untrusted. The delegator never executes a command locally: the worker is
// the only executor a model-controlled command ever reaches.
type Delegator struct {
	client    *protocol.Client
	identity  WorkerIdentity
	admission time.Duration
	now       func() time.Time
}

// NewDelegator wires the signed-protocol transport. Fails closed on
// incomplete identity configuration.
func NewDelegator(client *protocol.Client, identity WorkerIdentity) (*Delegator, error) {
	if client == nil || client.BaseURL == "" {
		return nil, errors.New("harness: worker protocol client is required")
	}
	if identity.RunUID == "" || identity.ControlPodUID == "" || identity.WorkerPodUID == "" {
		return nil, errors.New("harness: worker identity is incomplete")
	}
	if len(identity.Key) != ed25519.PrivateKeySize {
		return nil, errors.New("harness: worker signing key is unusable")
	}
	admission := identity.AdmissionWindow
	if admission == 0 {
		admission = 10 * time.Minute
	}
	return &Delegator{client: client, identity: identity, admission: admission, now: time.Now}, nil
}

// Dispatch delivers one task for a brief's fresh attempt and returns the
// worker's accepted status. The brief must be registered and untombstoned.
// An ambiguous transport failure surfaces as ErrAmbiguousDispatch: the
// caller reconciles with Cancel — it must not blindly redeliver, because the
// worker may have accepted the task.
func (d *Delegator) Dispatch(ctx context.Context, opID, briefID string, task protocol.Task) (string, error) {
	payload, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("harness: encode task: %w", err)
	}
	envelope := protocol.NewEnvelope(d.now(), d.admission, payload)
	envelope.Kind = protocol.KindDispatch
	envelope.RunUID = d.identity.RunUID
	envelope.ControlPodUID = d.identity.ControlPodUID
	envelope.WorkerPodUID = d.identity.WorkerPodUID
	envelope.BriefID = briefID
	envelope.OpID = opID
	return d.client.Dispatch(ctx, d.identity.Key, envelope, payload)
}

// Cancel delivers the signed cancellation for one attempt. The envelope
// carries the same opID as its dispatch with a strictly greater sequence, so
// the worker binds it to the original operation and acknowledges only after
// verified process termination (or that the operation never started). A
// cancel arriving before dispatch tombstones the ID at the worker: that
// dispatch, when it arrives, never runs.
func (d *Delegator) Cancel(ctx context.Context, opID, briefID string) error {
	envelope := protocol.NewEnvelope(d.now(), d.admission, nil)
	envelope.Kind = protocol.KindCancel
	envelope.Sequence = 1
	envelope.RunUID = d.identity.RunUID
	envelope.ControlPodUID = d.identity.ControlPodUID
	envelope.WorkerPodUID = d.identity.WorkerPodUID
	envelope.BriefID = briefID
	envelope.OpID = opID
	_, err := d.client.Cancel(ctx, d.identity.Key, envelope)
	return err
}

// Release tells the worker to drop one consumed result's payload after
// control has taken ownership of the terminal result. The envelope carries
// the same opID as its dispatch with a strictly greater sequence; the worker
// retains only the compact terminal status and keeps rejecting redelivery of
// the opID.
func (d *Delegator) Release(ctx context.Context, opID, briefID string) error {
	envelope := protocol.NewEnvelope(d.now(), d.admission, nil)
	envelope.Kind = protocol.KindRelease
	envelope.Sequence = 1
	envelope.RunUID = d.identity.RunUID
	envelope.ControlPodUID = d.identity.ControlPodUID
	envelope.WorkerPodUID = d.identity.WorkerPodUID
	envelope.BriefID = briefID
	envelope.OpID = opID
	_, err := d.client.Release(ctx, d.identity.Key, envelope)
	return err
}

// Result polls one operation's state. A missing operation is reported with
// Status "" — a replacement worker never saw the opID — which is observed
// worker absence, not completion.
func (d *Delegator) Result(ctx context.Context, opID string) (protocol.ResultState, error) {
	return d.client.Result(ctx, opID)
}

// ReconcileAmbiguous resolves an ambiguous dispatch: it cancels the
// operation — the honest worker either never runs it or verifies termination
// before acknowledging — and returns the observed state. This is the only
// sanctioned path after ErrAmbiguousDispatch; redelivery is not.
func (d *Delegator) ReconcileAmbiguous(ctx context.Context, opID, briefID string) (protocol.ResultState, error) {
	if err := d.Cancel(ctx, opID, briefID); err != nil {
		return protocol.ResultState{}, fmt.Errorf("harness: reconcile cancel for %s: %w", opID, err)
	}
	state, err := d.Result(ctx, opID)
	if err != nil {
		return protocol.ResultState{}, fmt.Errorf("harness: reconcile result for %s: %w", opID, err)
	}
	return state, nil
}
