package harness

import (
	"context"
	"encoding/json"
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
// a global fraction or reservation, a positive per-role ceiling, or a nonzero
// per-role override of either. It is deliberately broader than "the lane named
// this role": a global fraction covers every bound role without naming it, and
// an override is a declaration because forRole honors it. An all-zero
// per-role entry declares nothing — a config-shaped `roles: {coder: {}}` must
// not read as a declared budget — so an undeclared lane is merely degraded and
// never gated by a policy it never opted into.
func (p ContextPolicy) declaresRole(role string) bool {
	declared := p.Roles[role]
	return p.CompactAtPercent != 0 || p.OutputReservation != 0 ||
		declared.MaxInputTokens > 0 || declared.CompactAtPercent != 0 || declared.OutputReservation != 0
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

// CompactionSink receives one telemetry event per applied step-boundary
// compaction (#243). It is observability only: emission is void, never
// changes the transcript, and never affects the run's outcome.
type CompactionSink interface {
	SessionCompacted(CompactEvent)
}

// Session is one role-bound model conversation. It owns the normalized
// transcript and turns provider streams into events; every message it
// accepts or produces is data.
//
// budget is the resolved context policy for this session's role (budget.go).
// A zero budget owns no policy: no compact point, no digest, ever — absence
// of a declared and verified context window means the harness manages no
// context capacity, which is never the same as capacity being unbounded.
// compactions reports applied compactions to the sink.
type Session struct {
	gateway  *Gateway
	binding  string
	Messages []Message
	activity ActivitySink

	// role is the session's lane role, named in compaction telemetry so an
	// operator can tell a compacted delegate from a compacted coordinator.
	role string

	budget      ContextBudget
	compactions CompactionSink

	// pinned is the number of leading messages compaction must never digest:
	// the run's objective plus the harness's own framing, and the brief plus
	// the sub-agent framing in a delegate session. They are the task, not
	// the work log; a digest that lost them would describe work against a
	// task the model can no longer read.
	pinned int
	// settled is the settled-state block re-injected verbatim into every
	// digest: the brief's decisions and owned files, in the exact bytes
	// briefText renders them, so a delegate re-reads settled state rather
	// than a paraphrase of it. Empty for the coordinator session, which
	// settles no brief.
	settled string
}

// SessionOption configures a session at construction. Options keep
// NewSession's existing call shape: a caller that owns no context policy
// simply passes none.
type SessionOption func(*Session)

// WithContextBudget installs the session's resolved context policy. The
// caller resolves it once per role; the session never derives one.
func WithContextBudget(budget ContextBudget) SessionOption {
	return func(s *Session) { s.budget = budget }
}

// WithCompactionSink installs the applied-compaction telemetry sink.
func WithCompactionSink(sink CompactionSink) SessionOption {
	return func(s *Session) { s.compactions = sink }
}

// NewSession opens a session for one role's bound model.
func NewSession(gateway *Gateway, binding Bindings, role string, activity ActivitySink, options ...SessionOption) (*Session, error) {
	model := binding.Model(role)
	if model == "" {
		return nil, fmt.Errorf("harness: no model is bound to role %q", role)
	}
	session := &Session{gateway: gateway, binding: model, activity: activity, role: role}
	for _, option := range options {
		option(session)
	}
	return session, nil
}

// Append adds one message to the transcript.
func (s *Session) Append(message Message) {
	s.Messages = append(s.Messages, message)
}

// Pin appends a message compaction must never digest. Pinned messages are
// the leading part of the transcript: they carry the run's objective or the
// delegate's brief together with the harness's own framing, and losing them
// to a digest would leave a model working against a task it can no longer
// read. Every other message is ordinary transcript and may be digested.
// Pin before the first turn: the pinned set is the transcript's leading
// prefix, so pinning later would pin the transcript itself, digest included.
func (s *Session) Pin(message Message) {
	s.Messages = append(s.Messages, message)
	s.pinned = len(s.Messages)
}

// Settle records the session's settled state — the brief's decisions and
// owned files, rendered verbatim by the caller — for verbatim re-injection
// into every digest. Settled state is not conversation: it is what the
// delegate was told to apply exactly, and a compaction must never reduce it
// to a summary. The coordinator session settles no brief and carries none.
func (s *Session) Settle(text string) {
	s.settled = text
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

// tokensPerChar is the estimate's characters-per-token ratio. Tokenization is
// provider- and model-specific and no deployment can be trusted to expose it
// as a stable capability, so the harness estimates deterministically instead:
// four characters per token is the conventional English-text midpoint. The
// estimate only has to be reproducible and the right order of magnitude,
// because it selects a compaction point, never asserts a provider's limit.
const tokensPerChar = 4

// estimateMessageTokens is the deterministic, model-agnostic prompt cost of
// one transcript message: its role, content, tool_call_id, and every
// requested tool's name and arguments. Every message costs at least one
// token, so an empty message never looks free. The digest is estimated with
// this same function, so a digest's cost and the transcript it replaces are
// measured on one scale.
func estimateMessageTokens(message Message) int {
	chars := len(message.Role) + len(message.Content) + len(message.ToolCallID)
	for _, call := range message.ToolCalls {
		chars += len(call.ID) + len(call.Name) + len(call.Arguments)
	}
	tokens := chars / tokensPerChar
	if tokens < 1 {
		return 1
	}
	return tokens
}

// estimateToolTokens is the deterministic prompt cost of the tool
// declarations sent with a turn request. Gateway.StreamChat serializes the
// declarations into the same provider request as the transcript, so a
// boundary that accounted only for messages could report a fitting
// post-condition for a request the model's input limit cannot hold. Every
// declaration costs at least one token, and parameters are estimated over
// their JSON encoding, the bytes the wire actually carries.
func estimateToolTokens(tools []ToolDef) int {
	total := 0
	for _, tool := range tools {
		chars := len(tool.Name) + len(tool.Description)
		if len(tool.Parameters) > 0 {
			if encoded, err := json.Marshal(tool.Parameters); err == nil {
				chars += len(encoded)
			}
		}
		tokens := chars / tokensPerChar
		if tokens < 1 {
			tokens = 1
		}
		total += tokens
	}
	return total
}

// messageTokens estimates each transcript message's prompt cost, oldest
// first — the shape PlanCompaction consumes.
func messageTokens(messages []Message) []int {
	out := make([]int, len(messages))
	for i, message := range messages {
		out[i] = estimateMessageTokens(message)
	}
	return out
}

// Compact applies one step-boundary compaction decision to the transcript
// (#243). It is called between turns and never mid-turn: the next turn is
// the first thing that could cross the effective limit, and a decision made
// after a turn has been sent would be reacting to a request already in
// flight.
//
// The pure policy lives in PlanCompaction; this is its application to a real
// transcript. Protected is exactly what the policy refuses to digest: the
// pinned objective or brief, and the most recent turn. Everything else is
// ordinary transcript and is replaced, oldest first, by one digest placed
// where the first digested message stood — so every retained message keeps
// its position and bytes relative to the digest, and the digest never splits
// an assistant from the tool results answering it. The digest carries the
// settled-state block verbatim, so a compacted delegate still reads the
// decisions it was told to apply exactly.
//
// The tools argument must be the declarations the next Turn will send: the
// boundary accounts for the full request the provider will receive —
// transcript plus serialized tool declarations — so a fit claimed over
// messages alone is never reported for a request the input limit cannot
// hold.
//
// On success the session adopts the plan's calibrated budget, so the next
// boundary does not re-fire on the state this compaction just produced. On
// ErrCompactionCannotFit the transcript is returned byte-for-byte unchanged
// and the caller fails the run: dropping protected state to fit would be a
// silent truncation of the task.
func (s *Session) Compact(tools []ToolDef) (CompactEvent, bool, error) {
	if s.budget.EffectiveLimit <= 0 {
		return CompactEvent{}, false, nil
	}
	protected := s.protectedMessages()
	tokens := messageTokens(s.Messages)
	// The digest's cost is estimated over a digest of every non-protected
	// message, the widest span a plan could ever select. The real digest
	// covers a prefix of those messages and is therefore smaller, so both
	// acceptance conditions the planner checks hold conservatively for it:
	// the post-condition (prompt + output reservation within the effective
	// limit) is met with room to spare, and the reclaim check (After <
	// Before) is satisfied by more than the plan assumed.
	digestTokens := estimateMessageTokens(compactDigest(countDigestable(protected), s.settled, messagesAt(s.Messages, digestibleIndexes(protected))))
	plan, err := PlanCompaction(CompactionInput{
		MessageTokens: tokens,
		Protected:     protected,
		DigestTokens:  digestTokens,
		FixedTokens:   estimateToolTokens(tools),
		Budget:        s.budget,
	})
	if err != nil {
		return CompactEvent{}, false, err
	}
	if !plan.Compacted {
		return CompactEvent{}, false, nil
	}
	// The planner stops as soon as the post-condition holds, which can land
	// between an assistant that requested tools and the results answering
	// them. An orphaned tool result is a request no OpenAI-compatible gateway
	// accepts, so the span grows forward over the rest of the group it starts
	// in. Growing the span only ever digests more, so the applied
	// compaction reclaims at least what the plan verified.
	span := digestedSpan(s.Messages, plan.Digested, protected)
	digest := compactDigest(len(span), s.settled, messagesAt(s.Messages, span))
	digestedTokens := 0
	for _, i := range span {
		digestedTokens += tokens[i]
	}
	// Account for what actually lands rather than for what the plan selected,
	// so the counts an operator reads match the transcript the model sees. The
	// planner measured its digest over the widest span it could pick, so this
	// figure is never below the real one — and it re-checks the post-condition
	// on it rather than trusting the planner's arithmetic through the change.
	after := plan.Before - digestedTokens + estimateMessageTokens(digest)
	if !s.budget.FitsAfterCompaction(after) {
		return CompactEvent{}, false, fmt.Errorf("%w: the compacted transcript is %d tokens against an effective limit of %d with a reservation of %d",
			ErrCompactionCannotFit, after, s.budget.EffectiveLimit, s.budget.OutputReservation)
	}
	s.Messages = applyCompaction(s.Messages, span, digest)
	// Hysteresis: the next decision runs against the post-compaction
	// transcript, so it must run against the plan's calibrated budget.
	s.budget = plan.BudgetAfter
	event := CompactEvent{Role: s.role, Model: s.binding, Before: plan.Before, After: after, NetSaved: plan.Before - after, DigestedTokens: digestedTokens}
	if s.compactions != nil {
		// Observability only: a sink cannot fail a run or a transcript.
		s.compactions.SessionCompacted(event)
	}
	return event, true, nil
}

// protectedMessages marks the messages a compaction must never digest: every
// pinned message, and the most recent turn — the last assistant message and
// everything after it, which is the turn the next model call must answer.
func (s *Session) protectedMessages() []bool {
	protected := make([]bool, len(s.Messages))
	for i := range s.Messages {
		protected[i] = i < s.pinned
	}
	for i := len(s.Messages) - 1; i >= 0; i-- {
		if s.Messages[i].Role == "assistant" {
			for j := i; j < len(s.Messages); j++ {
				protected[j] = true
			}
			break
		}
	}
	return protected
}

// digestedSpan returns the plan's digested indexes grown forward over the
// tool results answering the last digested assistant message. The transcript
// is a conversation in which every tool result answers the assistant message
// immediately before it, so a span that ends on an assistant and leaves its
// results behind produces a request whose tool calls no assistant declares —
// rejected as malformed by every OpenAI-compatible gateway. Extension stops
// at the first protected index or the first non-tool message; the protected
// tail is never grown into, so the span only ever digests what the policy
// already allowed.
func digestedSpan(messages []Message, digested []int, protected []bool) []int {
	if len(digested) == 0 {
		return digested
	}
	span := append([]int(nil), digested...)
	for i := span[len(span)-1] + 1; i < len(messages); i++ {
		if messages[i].Role != "tool" || protectedAt(protected, i) {
			break
		}
		span = append(span, i)
	}
	return span
}

// applyCompaction replaces the digested messages with the single digest
// message, keeping every retained message's content and relative order.
func applyCompaction(messages []Message, digested []int, digest Message) []Message {
	if len(digested) == 0 {
		return messages
	}
	digestedAt := make(map[int]struct{}, len(digested))
	for _, i := range digested {
		digestedAt[i] = struct{}{}
	}
	out := make([]Message, 0, len(messages)-len(digested)+1)
	replaced := false
	for i, message := range messages {
		if _, ok := digestedAt[i]; ok {
			if !replaced {
				out = append(out, digest)
				replaced = true
			}
			continue
		}
		out = append(out, message)
	}
	return out
}

// digestLineChars bounds one digested message's rendered line: the role plus
// the head of the content, collapsed to a single line. A digest is a
// directory of what was compacted, not a substitute for it — the value it
// preserves is the shape of the work, and any bound works only if it is
// applied before a digest can cost more than the span it replaces.
const digestLineChars = 96

// maxDigestLines bounds how many digested messages one digest names. Past
// that the digest says how many it did not name: an unbounded digest would
// grow with the transcript it replaces and stop reclaiming anything, which
// is the degenerate plan budget.go fails closed on.
const maxDigestLines = 20

// compactDigest renders the harness-composed digest that replaces a span of
// digested messages: a header naming how many earlier messages were
// compacted, the settled-state block verbatim when the session has one, and
// one bounded line per digested message. It is deterministic — the same
// transcript and settled state always render the same digest — because the
// planner's token accounting and the message the model actually reads must
// describe the same bytes.
func compactDigest(count int, settled string, digested []Message) Message {
	var b strings.Builder
	fmt.Fprintf(&b, "[compacted: %d earlier messages were summarized to save context; their tool results and file contents are no longer available]", count)
	if settled != "" {
		b.WriteString("\n" + settled)
	}
	named := digested
	elided := 0
	if len(named) > maxDigestLines {
		elided = len(named) - maxDigestLines
		named = named[:maxDigestLines]
	}
	for _, message := range named {
		b.WriteString("\n- " + digestLine(message))
	}
	if elided > 0 {
		fmt.Fprintf(&b, "\n- ... and %d more messages", elided)
	}
	return Message{Role: "system", Content: b.String()}
}

// digestLine renders one digested message as a single bounded line: its role
// and the head of its content, with newlines and runs of whitespace collapsed
// so one message is always one line.
func digestLine(message Message) string {
	text := strings.Join(strings.Fields(message.Content), " ")
	if runes := []rune(text); len(runes) > digestLineChars {
		text = string(runes[:digestLineChars]) + "..."
	}
	if text == "" {
		return message.Role + ": (no text)"
	}
	return message.Role + ": " + text
}

// digestibleIndexes lists the non-protected message indexes, oldest first.
func digestibleIndexes(protected []bool) []int {
	out := make([]int, 0, len(protected))
	for i, p := range protected {
		if !p {
			out = append(out, i)
		}
	}
	return out
}

// countDigestable counts the non-protected messages.
func countDigestable(protected []bool) int {
	n := 0
	for _, p := range protected {
		if !p {
			n++
		}
	}
	return n
}

// messagesAt returns the messages at the given indexes, in that order.
func messagesAt(messages []Message, indexes []int) []Message {
	out := make([]Message, 0, len(indexes))
	for _, i := range indexes {
		if i >= 0 && i < len(messages) {
			out = append(out, messages[i])
		}
	}
	return out
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
