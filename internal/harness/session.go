package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
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

// ContextPolicy is the context budget a lane declared for its roles (#243).
// Declared values only: the compact fraction and output reservation apply to
// every role unless a per-role entry overrides them, and MaxInputTokens is the
// per-role ceiling the gateway read is measured against. The zero value
// declares nothing.
type ContextPolicy struct {
	CompactAtPercent  int
	OutputReservation int
	Roles             map[string]RoleContextPolicy
}

// RoleContextPolicy is one role's declared ceiling and overrides.
type RoleContextPolicy struct {
	MaxInputTokens    int
	CompactAtPercent  int
	OutputReservation int
}

// forRole builds one role's resolution input: the per-role ceiling with the
// global fraction and reservation as the base. ProbedMaxInputTokens stays
// zero — the verified limit comes from the gateway, never from the
// declaration.
func (p ContextPolicy) forRole(role string) ContextBudgetInput {
	declared := p.Roles[role]
	percent := p.CompactAtPercent
	if declared.CompactAtPercent != 0 {
		percent = declared.CompactAtPercent
	}
	reservation := p.OutputReservation
	if declared.OutputReservation != 0 {
		reservation = declared.OutputReservation
	}
	return ContextBudgetInput{
		Role:                   role,
		DeclaredMaxInputTokens: declared.MaxInputTokens,
		CompactAtPercent:       percent,
		OutputReservation:      reservation,
	}
}

// declaresRole answers whether the lane declared a usable context policy for
// this role — the predicate that makes a role required at the capability gate:
// a global fraction or reservation, or a positive per-role ceiling. It is
// deliberately broader than "the lane named this role": a global fraction
// covers every bound role without naming it. An all-zero per-role entry
// declares nothing — a config-shaped `roles: {coder: {}}` must not read as a
// declared budget — so an undeclared lane is merely degraded and never gated
// by a policy it never opted into.
func (p ContextPolicy) declaresRole(role string) bool {
	return p.CompactAtPercent != 0 || p.OutputReservation != 0 || p.Roles[role].MaxInputTokens > 0
}

// RoleBudget resolves one role's context budget from the lane's declaration
// plus the input limit the gateway verified for its bound model, so the
// capability probe and any other consumer cannot disagree on the effective
// limit.
//
// A role the lane declared nothing for and whose limit could not be verified
// resolves to a zero budget and no error — the same "the harness owns no
// policy for this role" convention the session loop reads, and never an
// implicit context window. Every other failure, including a declared ceiling
// the run cannot establish and an invalid declaration, is returned fail
// closed.
func (p ContextPolicy) RoleBudget(role string, probedMaxInputTokens int) (ContextBudget, error) {
	input := p.forRole(role)
	input.ProbedMaxInputTokens = probedMaxInputTokens
	budget, err := ResolveContextBudget(input)
	if err != nil {
		if errors.Is(err, ErrNoContextCapacity) && !p.declaresRole(role) {
			return ContextBudget{}, nil
		}
		return ContextBudget{}, err
	}
	return budget, nil
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

// retryBackoff spaces out in-process infra retries.
const retryBackoff = 250 * time.Millisecond

// maxInProcessRetryAfter bounds how long a single incarnation honors an
// authoritative Retry-After hint before handing the run back to the source.
// A longer hint is a resource-availability condition, not something to spin
// on in-process (#320).
const maxInProcessRetryAfter = 5 * time.Second

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
// are retried in-process with the same transcript; a retried or failed turn
// never reaches the activity sink — only a successfully terminalized stream
// (final or tool requests) earns its chunks as activity (HARNESS.md §6:
// retries are active, not alive). The returned events always end with a
// terminal event (final, tool requests, or error).
func (s *Session) Turn(ctx context.Context, tools []ToolDef) []Event {
	for attempt := 0; ; attempt++ {
		events, err := s.gateway.StreamChat(ctx, ChatRequest{Model: s.binding, Messages: s.Messages, Tools: tools})
		if err != nil {
			// A request-build failure is the harness's own bug, not the
			// gateway's; report it once without retry theater.
			return []Event{{Kind: KindError, Err: err}}
		}
		terminal := events[len(events)-1]
		if terminal.Kind != KindError {
			s.recordActivity(events)
			return events
		}
		if delay, retry := turnRetryDelay(terminal.Err); retry && attempt < maxStreamRetries {
			// Infra retry backoff: bounded, transport-class, not a model cap.
			// The delay honors an authoritative Retry-After hint when it is
			// short enough to wait out in-process.
			select {
			case <-ctx.Done():
				return events
			case <-time.After(delay):
			}
			continue
		}
		return events
	}
}

// turnRetryDelay reports whether a terminal stream error is worth retrying
// in-process and how long to wait first. Transport failures and retryable
// gateway statuses (5xx, request timeout, a plain 429) retry; a confirmed
// allowance exhaustion or other classified permanent failure does not. An
// authoritative Retry-After hint longer than maxInProcessRetryAfter is not
// waited on in-process — the run ends recoverable and the source re-offers.
func turnRetryDelay(err error) (time.Duration, bool) {
	var transportErr *GatewayTransportError
	if errors.As(err, &transportErr) {
		return retryBackoff, true
	}
	var gatewayErr *GatewayError
	if !errors.As(err, &gatewayErr) {
		return 0, false
	}
	classification := gatewayErr.classification()
	if !classification.Retryable || classification.RetryAfter > maxInProcessRetryAfter {
		return 0, false
	}
	if classification.RetryAfter > 0 {
		return classification.RetryAfter, true
	}
	return retryBackoff, true
}

// recordActivity marks earned stream activity once per successful turn that
// produced content. Failed attempts and retries never reach the sink.
func (s *Session) recordActivity(events []Event) {
	if s.activity == nil {
		return
	}
	for _, event := range events {
		if event.Kind == KindDelta {
			s.activity.StreamChunk()
			break
		}
	}
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
