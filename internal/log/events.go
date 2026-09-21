// Package log emits Courier's structured run events: one JSON object per
// line on stdout, sized for Loki/VictoriaLogs-style ingestion. A deployment's
// normal log collection stack is the transport; this package owns only the
// line format, the run-scoped metadata, the debug gate, and redaction before
// anything is written.
//
// Every line is a self-contained JSON object with these stable fields:
//
//	time    string  RFC3339 UTC timestamp of the event
//	event   string  dotted event type (see the Event* constants)
//	run_id  string  CoderRun name; required on every event
//	repo    string  target repository, "owner/name"
//	ref     int     issue number (resolve-issue) or PR number (fix-pr)
//	mode    string  resolve-issue | fix-pr
//	brief   string  brief/subtask identifier, when applicable
//	role    string  model role (coordinator, coder, reviewer), when applicable
//	model   string  model binding, when applicable
//	status  string  outcome: ok, error, needs-human, or a phase name
//	detail  object  verbose diagnostic payload, debug runs only
//
// Fixed fields are always safe to emit; detail is gated on the emitter's
// level and redacted before serialization in both modes. The event classes
// the future harness owns (model.call, tool.call, tool.result, brief.start,
// brief.complete) are declared here as constants so richer emitters can
// adopt the same contract without changing this package; the current
// disposable runtime instruments only the lifecycle boundaries Courier
// itself observes (run.start, workspace.ready, executor.start, run.exit,
// phase.transition). Event emission is best-effort: callers log failures
// without failing the work that produced the event.
package log

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

// Event types. Dotted lowercase so Loki/VictoriaLogs field filters read
// naturally: {event="run.exit"}.
const (
	// EventRunStart reports that a coordinator run began.
	EventRunStart = "run.start"
	// EventRunExit reports the coordinator's terminal result.
	EventRunExit = "run.exit"
	// EventWorkspaceReady reports a prepared or adopted git workspace.
	EventWorkspaceReady = "workspace.ready"
	// EventExecutorStart reports an executor process invocation.
	EventExecutorStart = "executor.start"
	// EventModelCall reports one model invocation (future harness).
	EventModelCall = "model.call"
	// EventToolCall reports one tool invocation (future harness).
	EventToolCall = "tool.call"
	// EventToolResult reports one tool result (future harness).
	EventToolResult = "tool.result"
	// EventBriefStart reports the start of one delegation brief.
	EventBriefStart = "brief.start"
	// EventBriefComplete reports one completed delegation brief.
	EventBriefComplete = "brief.complete"
	// EventPhaseTransition reports a CoderRun lifecycle phase change.
	EventPhaseTransition = "phase.transition"
)

// Status values for the status field.
const (
	StatusOK         = "ok"
	StatusError      = "error"
	StatusNeedsHuman = "needs-human"
)

// ErrMissingRunID is returned when an event is emitted without run identity.
// Every event line must carry run_id, or a log store cannot filter to a run.
var ErrMissingRunID = errors.New("log: event requires a run_id")

// Level controls verbose event detail. It never weakens redaction.
type Level int

const (
	// LevelInfo emits lifecycle and state events without detail payloads.
	LevelInfo Level = iota
	// LevelDebug additionally includes each event's detail object.
	LevelDebug
)

// String returns the canonical level name used in COURIER_LOG_LEVEL.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	default:
		return "info"
	}
}

// ParseLevel maps a COURIER_LOG_LEVEL value to a Level. Unrecognized or
// empty values fall back to LevelInfo, so an unspecified run stays quiet.
func ParseLevel(value string) Level {
	if strings.EqualFold(strings.TrimSpace(value), "debug") {
		return LevelDebug
	}
	return LevelInfo
}

// Event is one structured run event. RunID is required; the remaining
// identity fields are filled from the run when the caller has them. Detail
// is verbose diagnostic data, included when the event marks itself Verbose
// (per-run debug) or the emitter runs at LevelDebug, and redacted in every
// case.
type Event struct {
	Type   string
	RunID  string
	Repo   string
	Ref    int
	Mode   string
	Brief  string
	Role   string
	Model  string
	Status string
	// Verbose marks event detail as belonging to a debug run. Because the
	// flag rides on the event, one process-wide emitter can serve runs of
	// mixed verbosity without a global switch: each caller decides per run.
	Verbose bool
	Detail  map[string]any
}

// eventLine is the serialized shape of an event. Field order is the wire
// contract; omitempty keeps optional fields out of lines that have no use
// for them.
type eventLine struct {
	Time   string          `json:"time"`
	Event  string          `json:"event"`
	RunID  string          `json:"run_id"`
	Repo   string          `json:"repo,omitempty"`
	Ref    int             `json:"ref,omitempty"`
	Mode   string          `json:"mode,omitempty"`
	Brief  string          `json:"brief,omitempty"`
	Role   string          `json:"role,omitempty"`
	Model  string          `json:"model,omitempty"`
	Status string          `json:"status,omitempty"`
	Detail json.RawMessage `json:"detail,omitempty"`
}

// Emitter writes structured run events as one JSON line per event. It is
// safe for concurrent use; lines are written atomically so a log stack never
// observes a partial event. The zero-value writer discards events.
type Emitter struct {
	w        io.Writer
	level    Level
	redactor *Redactor
	now      func() time.Time

	mu sync.Mutex
}

// NewEmitter constructs an emitter writing JSON lines to w at the given
// level. A nil w discards events. The emitter owns its Redactor; callers
// register the deployment's secret values on it via Redactor.
func NewEmitter(w io.Writer, level Level) *Emitter {
	if w == nil {
		w = io.Discard
	}
	return &Emitter{
		w:        w,
		level:    level,
		redactor: NewRedactor(),
		now:      time.Now,
	}
}

// Redactor returns the emitter's redactor. Register every secret value the
// process holds before emitting events that could carry it.
func (e *Emitter) Redactor() *Redactor {
	if e == nil {
		return NewRedactor()
	}
	return e.redactor
}

// Level returns the emitter's verbosity level.
func (e *Emitter) Level() Level {
	if e == nil {
		return LevelInfo
	}
	return e.level
}

// Emit serializes one event and writes it as a single JSON line. Events
// without RunID are rejected (ErrMissingRunID) rather than emitted: a line
// without run identity is unfilterable noise. Detail is included only at
// LevelDebug. Every string field passes through the redactor before
// serialization, and the finished line is redacted once more as defense in
// depth. A nil emitter discards events, so optional wiring needs no guard.
func (e *Emitter) Emit(event Event) error {
	if e == nil || e.w == nil {
		return nil
	}
	if strings.TrimSpace(event.RunID) == "" {
		return ErrMissingRunID
	}
	line := eventLine{
		Time:   e.now().UTC().Format(time.RFC3339),
		Event:  e.redactor.Redact(event.Type),
		RunID:  e.redactor.Redact(event.RunID),
		Repo:   e.redactor.Redact(event.Repo),
		Ref:    event.Ref,
		Mode:   e.redactor.Redact(event.Mode),
		Brief:  e.redactor.Redact(event.Brief),
		Role:   e.redactor.Redact(event.Role),
		Model:  e.redactor.Redact(event.Model),
		Status: e.redactor.Redact(event.Status),
	}
	if event.Detail != nil && (event.Verbose || e.level == LevelDebug) {
		// Serialize the detail as JSON, redact the JSON text, and keep the
		// result raw: redaction therefore also covers detail keys, and the
		// caller's map is never mutated.
		if raw, err := json.Marshal(event.Detail); err == nil {
			line.Detail = json.RawMessage(e.redactor.Redact(string(raw)))
		}
	}
	payload, err := json.Marshal(line)
	if err != nil {
		return err
	}
	payload = []byte(e.redactor.Redact(string(payload)))

	e.mu.Lock()
	defer e.mu.Unlock()
	_, err = e.w.Write(append(payload, '\n'))
	return err
}
