package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// RoleCoordinator is the one role a lane must bind: the coordinator owns the
// run's terminal contract. Every other role (coder, reviewer, …) is optional
// and only matters when the coordinator actually delegates.
const RoleCoordinator = "coordinator"

// Bindings is the LaneProfile role→model binding resolved at run start. The
// gateway endpoint and key are deployment configuration; the profile supplies
// only the per-role model identifiers.
type Bindings struct {
	roles map[string]string
}

// BindRoles builds the binding from a LaneProfile's Roles map. The
// coordinator role is required; a lane without it cannot run a coordinator.
// Other roles bind to whatever identifier the profile names and are probed
// at startup as optional capabilities.
func BindRoles(roles map[string]string) (Bindings, error) {
	out := Bindings{roles: make(map[string]string, len(roles))}
	for role, model := range roles {
		role = strings.TrimSpace(role)
		model = strings.TrimSpace(model)
		if role == "" || model == "" {
			return Bindings{}, fmt.Errorf("harness: role %q has an empty model binding", role)
		}
		out.roles[role] = model
	}
	if out.roles[RoleCoordinator] == "" {
		return Bindings{}, errors.New("harness: lane binds no coordinator model")
	}
	return out, nil
}

// Model returns the model identifier bound to a role.
func (b Bindings) Model(role string) string {
	return b.roles[role]
}

// Roles returns the bound roles sorted by name.
func (b Bindings) Roles() []string {
	out := make([]string, 0, len(b.roles))
	for role := range b.roles {
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

// ActivitySink receives earned-activity signals (HARNESS.md §6). A stream
// chunk from a role-bound model session is an earned stream heartbeat; a
// verified tool boundary is an earned tool heartbeat. Failed streams,
// retries, dispatch, cancellation, and claimed terminal results never call
// the sink — wiring it to status writes is #126's seam, not the model's.
type ActivitySink interface {
	StreamChunk()
	ToolBoundary()
}

// maxStreamRetries bounds in-process retries of transient gateway failures.
// This is an infrastructure retry, not a governor on model behavior: a
// persistently broken gateway exits as an infrastructure failure and the
// operator's crashloop backstop takes over, which is where stuckness belongs.
const maxStreamRetries = 3

// Session is one role-bound model conversation. It owns the normalized
// transcript and turns provider streams into events; every message it
// accepts or produces is data.
type Session struct {
	gateway  *Gateway
	binding  string
	Messages []Message
	activity ActivitySink
}

// NewSession opens a session for one role's bound model.
func NewSession(gateway *Gateway, binding Bindings, role string, activity ActivitySink) (*Session, error) {
	model := binding.Model(role)
	if model == "" {
		return nil, fmt.Errorf("harness: no model is bound to role %q", role)
	}
	return &Session{gateway: gateway, binding: model, activity: activity}, nil
}

// Append adds one message to the transcript.
func (s *Session) Append(message Message) {
	s.Messages = append(s.Messages, message)
}

// Turn performs one model turn: it sends the transcript with the given tool
// declarations and returns the normalized events. Transient gateway failures
// are retried in-process with the same transcript — a retried request is not
// activity and never reaches the activity sink. The returned events always
// end with a terminal event (final, tool requests, or error).
func (s *Session) Turn(ctx context.Context, tools []ToolDef) []Event {
	for attempt := 0; ; attempt++ {
		events, err := s.gateway.StreamChat(ctx, ChatRequest{Model: s.binding, Messages: s.Messages, Tools: tools})
		if err != nil {
			// A request-build failure is the harness's own bug, not the
			// gateway's; report it once without retry theater.
			return []Event{{Kind: KindError, Err: err}}
		}
		events = s.recordActivity(events)
		terminal := events[len(events)-1]
		if terminal.Kind != KindError {
			return events
		}
		var gatewayErr *GatewayError
		if !errors.As(terminal.Err, &gatewayErr) || !isTransientGatewayStatus(gatewayErr.Status) || attempt >= maxStreamRetries {
			return events
		}
	}
}

// recordActivity marks earned stream activity once per successful turn that
// produced content. Retries and errors never reach the sink.
func (s *Session) recordActivity(events []Event) []Event {
	if s.activity == nil {
		return events
	}
	for _, event := range events {
		if event.Kind == KindDelta {
			s.activity.StreamChunk()
			break
		}
	}
	return events
}

// ToolResultMessage renders a normalized tool result as the transcript
// message the next turn consumes.
func ToolResultMessage(result ToolResult) Message {
	return Message{Role: "tool", Content: result.Content, ToolCallID: result.ToolCallID}
}

// AssistantMessage renders the model's own turn — content plus any tool
// requests it made — as the transcript message that precedes the tool
// results.
func AssistantMessage(text string, calls []ToolCall) Message {
	return Message{Role: "assistant", Content: text, ToolCalls: calls}
}
