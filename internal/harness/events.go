// Package harness implements the native coordinator harness (HARNESS.md §5):
// the trusted control pod's model client. Every provider stream is normalized
// here, in trusted control code, into a fixed event vocabulary before
// anything else sees it; provider wire formats never reach the supervisor,
// the worker, or status. Model text is data: no event lets a model assert
// liveness, completion, or status.
//
// Delegation is typed: briefs carry a stable ID, settled decisions, owned
// files, non-goals, and an observable success check; results are untrusted
// and can never publish. The coordinator owns the run's terminal contract —
// plan, integrate, locally validate, publish — through trusted code paths only;
// external verification remains with the operator.
package harness

import "errors"

// EventKind is the fixed normalized event vocabulary. It is deliberately
// closed: provider wire shapes are parsed in the gateway client and nothing
// else, and no other event kind exists.
type EventKind string

const (
	// KindDelta is a model text chunk.
	KindDelta EventKind = "delta"
	// KindToolRequest is the model asking to run one trusted tool. The
	// request is data; only trusted control decides what executes.
	KindToolRequest EventKind = "tool-request"
	// KindToolResult reports a completed tool execution to the session. It is
	// produced by trusted control after verified termination, never by the
	// model or the worker.
	KindToolResult EventKind = "tool-result"
	// KindFinal closes a turn: the model stopped without requesting tools.
	KindFinal EventKind = "final"
	// KindError reports a failed model stream or transport. A failed stream
	// is never successful activity.
	KindError EventKind = "error"
)

// ToolCall is one normalized tool request: the model named a tool and
// supplied arguments. Arguments are raw JSON text — data to be validated by
// the trusted tool implementation, never executed or trusted as policy.
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolResult is one normalized tool result fed back into the session.
// Content is untrusted worker or broker output.
type ToolResult struct {
	ToolCallID string `json:"toolCallID"`
	Name       string `json:"name"`
	Content    string `json:"content"`
	IsError    bool   `json:"isError,omitempty"`
}

// Event is one normalized stream event. Exactly the fields named by Kind are
// meaningful; consumers must switch on Kind rather than guessing from fields.
type Event struct {
	Kind         EventKind
	Text         string     // KindDelta
	Tool         ToolCall   // KindToolRequest
	Result       ToolResult // KindToolResult
	FinishReason string     // KindFinal
	// Err is the redacted failure for KindError. It is safe to log or
	// surface: the gateway normalizer never echoes provider response bodies.
	Err error
}

// ErrStreamTruncated reports a stream that ended without a terminal event.
var ErrStreamTruncated = errors.New("harness: provider stream ended without a terminal event")

// The stream-overflow errors report a completion that crossed a transport
// resource bound. Their strings are fixed and redacted: they name the bound,
// never provider text or the size observed. A stream that crossed one is a
// hostile or broken gateway response, and the deltas already accepted remain
// valid data under a terminal error — never a tool request, because requests
// are emitted only after the whole stream is read.
var (
	// ErrStreamContentOverflow reports content deltas over the buffered
	// content bound.
	ErrStreamContentOverflow = errors.New("harness: provider stream exceeded the buffered content bound")

	// ErrStreamToolArgumentOverflow reports tool-call arguments over the
	// buffered tool-argument bound.
	ErrStreamToolArgumentOverflow = errors.New("harness: provider stream exceeded the buffered tool-argument bound")

	// ErrStreamToolCallOverflow reports more distinct tool-call indexes than
	// the buffered tool-call bound allows.
	ErrStreamToolCallOverflow = errors.New("harness: provider stream exceeded the buffered tool-call count bound")

	// ErrStreamEventOverflow reports more retained events than the buffered
	// event bound allows.
	ErrStreamEventOverflow = errors.New("harness: provider stream exceeded the buffered event bound")

	// ErrStreamToolIdentityOverflow reports more tool-call identity bytes than
	// the buffered tool-identity bound allows.
	ErrStreamToolIdentityOverflow = errors.New("harness: provider stream exceeded the buffered tool-identity bound")
)
