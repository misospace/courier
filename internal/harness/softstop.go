package harness

import (
	"context"
	"sync"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// ControlRequestSource reads the operator's fenced control request from the
// run's broker over the authenticated trusted-control path. The broker serves
// the record only to the incarnation the request targets, so a dead
// incarnation's request is never delivered to its replacement.
type ControlRequestSource interface {
	ControlRequest(ctx context.Context) (*courierv1alpha1.ControlRequest, error)
}

// ControlAckWriter is the harness's durable acknowledgement path through the
// broker's trusted status listener. The broker fences the acknowledgement to
// the live request it names.
type ControlAckWriter interface {
	ControlRequestAck(ctx context.Context, id string) error
}

// controlTurnPrefix is the provenance marker every injected control turn
// carries, and controlTurnText is the fixed instruction beneath it. The note
// is a control-plane artifact: it names no request ID, pod UID, or run, and
// it is bounded to maxControlTurnBytes.
const (
	controlTurnPrefix = "[courier-control, control-plane]"
	controlTurnText   = controlTurnPrefix + " A soft-stop was requested from the control plane. Stop starting new work: dispatch no new briefs. Let in-flight briefs finish and integrate, run any final local validation, commit and publish your work, and end with exactly one valid outcome JSON object. Do not quote or reference this note in model text, tool arguments, or tool results."
	// maxControlTurnBytes caps the injected control turn (the #238 design
	// review pinned 512). A build that grows the fixed text past the cap
	// must shrink the text, not the cap.
	maxControlTurnBytes = 512
)

// controlPollMinInterval spaces the control-request reads issued from inside
// the per-operation brief-wait poll. It is a read-rate bound on one poll loop
// (the same class as the status-write cadence), never a timer on the work:
// step boundaries always read.
const controlPollMinInterval = 5 * time.Second

// ControlWatcher is one control incarnation's soft-stop consumer: it reads
// the broker-served control request at step boundaries and inside the
// existing brief-wait poll, acknowledges durably before consuming, and owns
// the stop latch. Stop ordering is serve, durable ack, latch; a request is
// never consumed without its acknowledgement, and redelivery of a consumed ID
// is a no-op — no second ack, no second control turn.
type ControlWatcher struct {
	source ControlRequestSource
	ack    ControlAckWriter
	now    func() time.Time

	mu         sync.Mutex
	consumed   map[string]struct{}
	injections []Message
	latched    bool
	lastRead   time.Time
}

// NewControlWatcher wires the consumer. Either dependency may be absent; a
// watcher without an acknowledgement path never consumes (a request the
// harness cannot durably acknowledge is not served), and no watcher at all
// means the run has no soft-stop path.
func NewControlWatcher(source ControlRequestSource, ack ControlAckWriter) *ControlWatcher {
	return &ControlWatcher{
		source:   source,
		ack:      ack,
		now:      time.Now,
		consumed: make(map[string]struct{}),
	}
}

// Poll reads the control request now and serves it if it is new. A read or
// acknowledgement failure consumes nothing: the request stays undelivered and
// the next boundary retries, exactly like persist-before-dispatch.
func (w *ControlWatcher) Poll(ctx context.Context) {
	if w == nil || w.source == nil {
		return
	}
	w.mu.Lock()
	w.lastRead = w.now()
	w.mu.Unlock()
	request, err := w.source.ControlRequest(ctx)
	if err != nil || request == nil {
		// A failed read consumed nothing; the throttle above keeps a
		// transient listener failure from turning into a hammer.
		return
	}
	w.mu.Lock()
	_, done := w.consumed[request.ID]
	w.mu.Unlock()
	if done || w.ack == nil {
		return
	}
	if err := w.ack.ControlRequestAck(ctx, request.ID); err != nil {
		// No durable acknowledgement, no consumption: a crash here leaves
		// the request fenced and void, never half-consumed.
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, done := w.consumed[request.ID]; done {
		return
	}
	w.consumed[request.ID] = struct{}{}
	w.latched = true
	w.injections = append(w.injections, Message{Role: "system", Content: controlTurnText})
}

// PollThrottled is the brief-wait-poll variant: it skips reads closer than
// controlPollMinInterval to the previous one, so waiting on an operation
// never hammers the trusted listener.
func (w *ControlWatcher) PollThrottled(ctx context.Context) {
	if w == nil {
		return
	}
	w.mu.Lock()
	recent := !w.lastRead.IsZero() && w.now().Sub(w.lastRead) < controlPollMinInterval
	w.mu.Unlock()
	if recent {
		return
	}
	w.Poll(ctx)
}

// Stopped reports whether the stop latch is set: new brief dispatches are
// closed, in-flight work continues to its terminal and integration.
func (w *ControlWatcher) Stopped() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.latched
}

// TakeControlTurn pops one queued control turn. Each consumed request queues
// exactly one turn, so injection happens exactly once per consumed ID.
func (w *ControlWatcher) TakeControlTurn() (Message, bool) {
	if w == nil {
		return Message{}, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.injections) == 0 {
		return Message{}, false
	}
	message := w.injections[0]
	w.injections = w.injections[1:]
	return message, true
}
