package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// AnthropicTransport is the native Anthropic Messages transport (#716): the
// Messages API wire format — top-level system, content blocks, native
// tool-use and tool-result blocks, thinking blocks with their signatures, and
// Anthropic stop reasons — normalized into the same trusted event vocabulary
// the OpenAI-compatible transport emits. Nothing here routes through OpenAI
// translation: reasoning/thinking blocks are preserved as provider-native
// continuation data and never flattened into assistant text, dropped, or
// leaked into user-visible transcript content.
//
// The endpoint is the same deployment-configured gateway the OpenAI
// transport uses; requests post to BaseURL + "/messages", the surface
// LiteLLM's anthropic-format consumers already use.
type AnthropicTransport struct {
	// BaseURL is the API root, for example "http://litellm:4000/v1".
	// Requests post to BaseURL + "/messages" and read BaseURL + "/model/info"
	// for the verified input limit when the gateway exposes it.
	BaseURL string
	// APIKey is sent as the x-api-key header — the authentication the
	// Messages API and its gateway surfaces expect. Empty means the gateway
	// accepts unauthenticated callers.
	APIKey string
	HTTP   *http.Client
	// ExhaustionSignals names the structured provider signals that mean a
	// model allowance is spent rather than a transient rate limit (#320),
	// with the same provider-neutral defaults and deployment extensions as
	// the OpenAI transport.
	ExhaustionSignals ExhaustionSignals
}

// anthropicMaxTokensFallback is the output bound a turn sends when the lane
// declares no output reservation. The Messages API requires max_tokens on
// every request; this is a request-shape requirement, not a capability claim
// — the lane's declared reservation overrides it through ChatRequest.MaxTokens
// whenever one is declared.
const anthropicMaxTokensFallback = 32768

// anthropicVersion is the Messages API version header every request carries.
const anthropicVersion = "2023-06-01"

// anthropicSystemSeparator joins the transcript's consecutive leading system
// messages inside the top-level system field. It is stable and plain — the
// pinned system material (goal, harness framing, lane framing, skills index)
// must re-read as one coherent block, never reordered or re-rendered.
const anthropicSystemSeparator = "\n\n"

func (a *AnthropicTransport) client() *http.Client {
	if a.HTTP != nil {
		return a.HTTP
	}
	return http.DefaultClient
}

func (a *AnthropicTransport) signals() ExhaustionSignals {
	if a.ExhaustionSignals.Empty() {
		return DefaultExhaustionSignals()
	}
	return a.ExhaustionSignals
}

// ModelInputLimit reads the input limit the gateway advertises for one bound
// model. The /model/info read is gateway deployment metadata, not part of
// either wire protocol; a gateway without it returns an explicit error and
// the role's declared ceiling stays `configured` — never a silently invented
// limit (#716).
func (a *AnthropicTransport) ModelInputLimit(ctx context.Context, model string) (int, error) {
	return modelInfoInputLimit(ctx, a.client(), a.BaseURL, a.APIKey, model, a.signals())
}

// StreamChat performs one streaming Messages turn and returns the normalized
// event sequence under the same ordering and terminal guarantees as every
// transport: deltas first, then assembled tool requests, then exactly one
// terminal event (final, tool requests, or error). Thinking deltas are
// accumulated into the turn's provider-native blocks and never become
// KindDelta events — reasoning is not assistant text. The turn's terminal
// event carries Blocks: the assistant content blocks verbatim, signatures
// included, for the transport to re-render on the next request.
func (a *AnthropicTransport) StreamChat(ctx context.Context, req ChatRequest) ([]Event, error) {
	wire, err := anthropicWireRequest(req)
	if err != nil {
		return nil, fmt.Errorf("harness: encode Messages request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(a.BaseURL, "/")+"/messages", bytes.NewReader(wire))
	if err != nil {
		return nil, fmt.Errorf("harness: build Messages request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	if a.APIKey != "" {
		httpReq.Header.Set("x-api-key", a.APIKey)
	}
	resp, err := a.client().Do(httpReq)
	if err != nil {
		return []Event{{Kind: KindError, Err: &GatewayTransportError{Err: err}}}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// The same bounded-read redaction contract as every transport: the
		// provider body is classified, never retained or echoed.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProviderErrorBody))
		return []Event{{Kind: KindError, Err: gatewayStatusError(resp.StatusCode, resp.Header, body, a.signals())}}, nil
	}
	return normalizeAnthropicStream(resp.Body, a.signals())
}

// anthropicWireRequest renders the normalized transcript into the Messages
// API request shape. The transcript's normalized roles map as: leading
// system messages become the top-level system field; a mid-transcript system
// message — the compaction digest, the only way one exists — becomes a user
// text block, because the Messages API has no mid-stream system role and the
// digest is framing the model reads as transcript state; tool results group
// into one user message of tool_result blocks answering the assistant turn
// before them; an assistant turn with provider-native blocks re-renders them
// verbatim, thinking signatures included, and one without falls back to
// text and tool_use blocks built from the normalized fields.
func anthropicWireRequest(req ChatRequest) ([]byte, error) {
	wire := anthropicWireRequestShape{
		Model:     req.Model,
		MaxTokens: req.MaxTokens,
		Stream:    true,
		// The Messages API requires a non-null messages array (#724): an
		// all-system transcript renders no wire messages, and a nil slice
		// marshals "messages": null, which litellm's /v1/messages
		// normalizer 500s on every first turn. Initialize non-nil so the
		// field can never marshal as null.
		Messages: []anthropicWireMessage{},
	}
	if wire.MaxTokens <= 0 {
		wire.MaxTokens = anthropicMaxTokensFallback
	}
	var systemParts []string
	leadingSystem := true
	for _, message := range req.Messages {
		// Every consecutive leading system message — a production session
		// pins several: the goal plus harness framing, optional lane framing,
		// the skills index (#716 review) — belongs in the top-level system
		// field, in order, joined by the stable separator. None of them may
		// be dropped or duplicated into messages. The first non-system
		// message ends the collection: any later system message is
		// mid-transcript framing (the compaction digest), which the Messages
		// API has no role for and which renders as a user text block.
		if message.Role == "system" && leadingSystem {
			// An empty pinned message contributes nothing to the system
			// field — the pre-#724 join behavior — and can never carry the
			// task turn, so it never becomes the promoted message either.
			if message.Content != "" {
				systemParts = append(systemParts, message.Content)
			}
			continue
		}
		if message.Role != "system" {
			leadingSystem = false
		}
		switch {
		case message.Role == "system":
			wire.Messages = append(wire.Messages, anthropicWireMessage{
				Role:    "user",
				Content: []json.RawMessage{anthropicTextBlock(message.Content)},
			})
		case message.Role == "assistant":
			blocks, err := anthropicAssistantBlocks(message)
			if err != nil {
				return nil, err
			}
			wire.Messages = append(wire.Messages, anthropicWireMessage{Role: "assistant", Content: blocks})
		case message.Role == "tool":
			// Tool results answer the assistant turn immediately before them
			// and travel grouped as one user message of tool_result blocks —
			// the shape the Messages API requires. The transcript guarantees
			// the ordering; a tool result with nothing before it is a broken
			// transcript, not a renderable one.
			if len(wire.Messages) == 0 || wire.Messages[len(wire.Messages)-1].Role != "user" || !wire.Messages[len(wire.Messages)-1].toolResults {
				wire.Messages = append(wire.Messages, anthropicWireMessage{Role: "user", toolResults: true})
			}
			grouped := &wire.Messages[len(wire.Messages)-1]
			result := map[string]any{
				"type":        "tool_result",
				"tool_use_id": message.ToolCallID,
			}
			if message.Content != "" {
				result["content"] = []map[string]any{{"type": "text", "text": message.Content}}
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				return nil, fmt.Errorf("harness: encode tool result: %w", err)
			}
			grouped.Content = append(grouped.Content, encoded)
		default:
			wire.Messages = append(wire.Messages, anthropicWireMessage{
				Role:    message.Role,
				Content: []json.RawMessage{anthropicTextBlock(message.Content)},
			})
		}
	}
	// An all-system transcript — the pinned coordinator session: goal plus
	// harness framing, optional lane framing, skills index, all
	// Role "system" — renders zero wire messages, but the Messages API
	// requires at least one message and the model must be handed the task
	// as a turn (#724). The final pinned system message is promoted to the
	// user turn verbatim, nothing invented, and everything before it stays
	// in the top-level system field.
	if len(systemParts) > 0 && len(wire.Messages) == 0 {
		promoted := systemParts[len(systemParts)-1]
		systemParts = systemParts[:len(systemParts)-1]
		wire.Messages = append(wire.Messages, anthropicWireMessage{
			Role:    "user",
			Content: []json.RawMessage{anthropicTextBlock(promoted)},
		})
	}
	if len(systemParts) > 0 {
		wire.System = strings.Join(systemParts, anthropicSystemSeparator)
	}
	for _, tool := range req.Tools {
		schema := tool.Parameters
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		wire.Tools = append(wire.Tools, anthropicWireTool{Name: tool.Name, Description: tool.Description, InputSchema: schema})
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func anthropicTextBlock(text string) json.RawMessage {
	encoded, _ := json.Marshal(map[string]any{"type": "text", "text": text})
	return encoded
}

// anthropicAssistantBlocks renders one assistant transcript message into its
// content blocks. Provider-native blocks ride verbatim when the turn carried
// them — thinking signatures included; the fallback builds text and tool_use
// blocks from the normalized fields for transcripts a transport did not
// produce. Arguments that are not valid JSON cannot render as tool_use
// input and fail closed rather than smuggling a string where the wire wants
// an object.
func anthropicAssistantBlocks(message Message) ([]json.RawMessage, error) {
	if len(message.Blocks) > 0 {
		return message.Blocks, nil
	}
	var blocks []json.RawMessage
	if message.Content != "" {
		blocks = append(blocks, anthropicTextBlock(message.Content))
	}
	for _, call := range message.ToolCalls {
		var input json.RawMessage
		if json.Valid([]byte(call.Arguments)) {
			input = json.RawMessage(call.Arguments)
		} else {
			encoded, err := json.Marshal(call.Arguments)
			if err != nil {
				return nil, fmt.Errorf("harness: encode tool request arguments: %w", err)
			}
			// A non-JSON argument string cannot satisfy the wire's object
			// shape; wrapping keeps the data visible to the tool instead of
			// dropping it, and the tool implementation rejects what it
			// cannot parse.
			input = encoded
		}
		encoded, err := json.Marshal(map[string]any{
			"type":  "tool_use",
			"id":    call.ID,
			"name":  call.Name,
			"input": input,
		})
		if err != nil {
			return nil, fmt.Errorf("harness: encode tool request: %w", err)
		}
		blocks = append(blocks, encoded)
	}
	if len(blocks) == 0 {
		return nil, errors.New("harness: assistant turn has no content for the Messages wire")
	}
	return blocks, nil
}

type anthropicWireRequestShape struct {
	Model     string                 `json:"model"`
	MaxTokens int                    `json:"max_tokens"`
	Stream    bool                   `json:"stream"`
	System    string                 `json:"system,omitempty"`
	Messages  []anthropicWireMessage `json:"messages"`
	Tools     []anthropicWireTool    `json:"tools,omitempty"`
}

type anthropicWireMessage struct {
	Role string `json:"role"`
	// Content is the message's content blocks. A plain string would be
	// accepted for text-only turns, but tool_use, tool_result, and thinking
	// all require the block shape, so every message renders blocks.
	Content []json.RawMessage `json:"content"`
	// toolResults marks a grouped tool-result user message; it is a render
	// aid, never marshaled.
	toolResults bool `json:"-"`
}

type anthropicWireTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"input_schema"`
}

// anthropicStreamEvent is one SSE data payload. Every Messages stream event
// carries its type in the payload; the SSE event: line is redundant and
// skipped.
type anthropicStreamEvent struct {
	Type         string                     `json:"type"`
	Message      *anthropicMessageStart     `json:"message,omitempty"`
	Index        *int                       `json:"index,omitempty"`
	ContentBlock *anthropicContentBlockHead `json:"content_block,omitempty"`
	Delta        *anthropicStreamDelta      `json:"delta,omitempty"`
	// Usage is the message_delta's cumulative output count for the turn,
	// raw because a report telemetry cannot read as exact whole-token
	// counts is an absence, never a turn failure (#738).
	Usage json.RawMessage     `json:"usage,omitempty"`
	Error *anthropicErrorBody `json:"error,omitempty"`
}

// anthropicDeltaUsage is the output count a Messages message_delta carries,
// decoded leniently. The count is cumulative within the message, not a
// per-delta increment: each delta's value replaces the previous one.
type anthropicDeltaUsage struct {
	OutputTokens json.Number `json:"output_tokens"`
}

// anthropicMessageStart is the message's opening envelope. Its usage is the
// only place the Messages stream names the input and prompt-cache counts
// (#738). The usage object is raw because a report telemetry cannot read as
// exact whole-token counts is an absence, never a turn failure; a provider
// that did not report usage omits it, and an absent report is an explicit
// absence the terminal records as nil, never a zero.
type anthropicMessageStart struct {
	Usage json.RawMessage `json:"usage"`
}

// decodeMessageStartUsage decodes the opening envelope's usage leniently
// (#738): a provider or proxy that reports a fractional or otherwise
// non-integer count has not reported an exact token count, so the report is
// treated as absent — the turn is never failed for telemetry's sake, and no
// fractional count the provider did not report is invented. The envelope's
// mandatory total input count — input_tokens, the only place the stream
// names the input accepted — must be present and exact before an exact
// report is decoded: a present object missing it names no exact usage at
// all, and an absent field is never an invented zero (#738). The
// prompt-cache counts are optional and take their documented default of
// zero when the provider omits them. The last return reports whether an
// exact report was decoded.
func decodeMessageStartUsage(raw json.RawMessage) (input, cacheRead, cacheWrite int64, ok bool) {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, 0, 0, false
	}
	var wire struct {
		InputTokens              json.Number `json:"input_tokens"`
		CacheReadInputTokens     json.Number `json:"cache_read_input_tokens"`
		CacheCreationInputTokens json.Number `json:"cache_creation_input_tokens"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return 0, 0, 0, false
	}
	input, inputOK := wholeCount(wire.InputTokens)
	cacheRead, readOK := optionalWholeCount(wire.CacheReadInputTokens)
	cacheWrite, writeOK := optionalWholeCount(wire.CacheCreationInputTokens)
	if !inputOK || !readOK || !writeOK {
		return 0, 0, 0, false
	}
	return input, cacheRead, cacheWrite, true
}

// decodeDeltaUsage decodes the message_delta's cumulative output count
// leniently (#738), under the same exactness rule as the opening envelope's:
// the delta's mandatory total output count — output_tokens, the only place
// the stream names the output produced — must be present and exact before
// an exact report is decoded, and an absent field is never an invented
// zero (#738).
func decodeDeltaUsage(raw json.RawMessage) (output int64, ok bool) {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, false
	}
	var wire anthropicDeltaUsage
	if err := json.Unmarshal(raw, &wire); err != nil {
		return 0, false
	}
	return wholeCount(wire.OutputTokens)
}

type anthropicContentBlockHead struct {
	Type  string          `json:"type"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	Data  string          `json:"data,omitempty"`
}

type anthropicStreamDelta struct {
	Type        string `json:"type,omitempty"`
	Text        string `json:"text,omitempty"`
	Thinking    string `json:"thinking,omitempty"`
	Signature   string `json:"signature,omitempty"`
	PartialJSON string `json:"partial_json,omitempty"`
	StopReason  string `json:"stop_reason,omitempty"`
}

type anthropicErrorBody struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// anthropicBlock is one content block under assembly. Text and thinking
// accumulate deltas; tool_use accumulates input-JSON fragments the way the
// OpenAI transport assembles argument fragments; redacted_thinking arrives
// whole at content_block_start.
type anthropicBlock struct {
	kind      string
	text      strings.Builder
	thinking  strings.Builder
	signature strings.Builder
	toolCall  ToolCall
	args      []byte
	redacted  string
	started   bool
}

// finalize renders the block's provider-native JSON, verbatim in content and
// shape. Thinking keeps its signature beside it — the pair is the provider's
// continuation contract — and redacted thinking passes through the opaque
// data the provider sent.
func (b *anthropicBlock) finalize() (json.RawMessage, error) {
	switch b.kind {
	case "text":
		return json.Marshal(map[string]any{"type": "text", "text": b.text.String()})
	case "thinking":
		return json.Marshal(map[string]any{"type": "thinking", "thinking": b.thinking.String(), "signature": b.signature.String()})
	case "redacted_thinking":
		return json.Marshal(map[string]any{"type": "redacted_thinking", "data": b.redacted})
	case "tool_use":
		input := json.RawMessage(b.args)
		if !json.Valid(input) {
			return nil, errors.New("harness: incomplete tool request in provider stream")
		}
		return json.Marshal(map[string]any{"type": "tool_use", "id": b.toolCall.ID, "name": b.toolCall.Name, "input": input})
	default:
		return nil, fmt.Errorf("harness: unexpected content block type %q", b.kind)
	}
}

// anthropicStopReason maps the Messages API stop reasons onto the normalized
// finish vocabulary. tool_use is not a final: the turn continues with tool
// requests. pause_turn has no normalized meaning — continuing it would need
// provider-native resume state the transcript model does not carry — so it
// fails closed instead of masquerading as a completed answer.
func anthropicStopReason(reason string) (finish string, toolUse bool, err error) {
	switch reason {
	case "tool_use":
		return "", true, nil
	case "end_turn", "stop_sequence":
		return "stop", false, nil
	case "max_tokens":
		return "length", false, nil
	case "refusal":
		return "content_filter", false, nil
	case "":
		return "", false, ErrStreamTruncated
	default:
		return "", false, fmt.Errorf("harness: unhandled Anthropic stop reason %q", reason)
	}
}

// anthropicStreamErrorClass maps the Messages API in-stream error types onto
// the neutral availability classes. The type string is the only thing
// retained — the message is provider-controlled body text and is never
// echoed, exactly like every other transport path.
func anthropicStreamErrorClass(errType string, signals ExhaustionSignals) *GatewayError {
	classification := classifyStreamSignal("", errType, signals)
	gerr := &GatewayError{Availability: classification.Availability, ProviderCode: classification.Code}
	if gerr.Availability != ProviderUnknown {
		return gerr
	}
	switch errType {
	case "rate_limit_error":
		return &GatewayError{Availability: ProviderTransientRateLimit, Retryable: true, ProviderCode: errType}
	case "overloaded_error", "api_error", "timeout_error":
		return &GatewayError{Availability: ProviderUnavailable, Retryable: true, ProviderCode: errType}
	case "authentication_error", "permission_error":
		return &GatewayError{Availability: ProviderAuthenticationFailed, ProviderCode: errType}
	case "invalid_request_error", "not_found_error":
		return &GatewayError{Availability: ProviderRequestRejected, ProviderCode: errType}
	default:
		return &GatewayError{Availability: ProviderUnknown, ProviderCode: errType}
	}
}

// normalizeAnthropicStream reads a Messages SSE body and produces the
// normalized event sequence, under the same transport resource bounds and the
// same atomic tool-request emission as the OpenAI normalizer: fragments are
// assembled during the scan, validated whole, and emitted only after the
// stream terminalizes.
func normalizeAnthropicStream(body io.Reader, signals ExhaustionSignals) ([]Event, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxStreamLineBytes)

	var events []Event
	blocksByIndex := map[int]*anthropicBlock{}
	var order []int
	var blocks []json.RawMessage
	var contentBytes, argumentBytes, identityBytes int
	var stopReason string
	var sawMessageStop bool
	// usage is the provider's report for the turn, assembled from the
	// message's opening envelope (input and prompt-cache counts) and the
	// cumulative output count the message_delta events carry (#738).
	// usageSeen distinguishes "the provider reported usage" from a nil
	// report: absence is recorded as absence, never a zero.
	// usageUnusable marks a usage object the provider sent that is not an
	// exact whole-token count: a report telemetry cannot honor is no
	// report, so it voids the turn's whole account — the partial counts
	// already decoded are discarded with it — and no later exact object
	// resurrects it (#738).
	var usage *Usage
	usageSeen := false
	usageUnusable := false

	terminal := func(err error) ([]Event, error) {
		event := Event{Kind: KindError, Err: err}
		if usageSeen {
			event.Usage = usage
		}
		events = append(events, event)
		return events, nil
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") {
			continue
		}
		payload, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		var event anthropicStreamEvent
		if err := json.Unmarshal([]byte(strings.TrimSpace(payload)), &event); err != nil {
			return terminal(errors.New("harness: undecodable stream chunk"))
		}
		switch event.Type {
		case "ping":
			continue
		case "message_start":
			// The opening envelope's usage is the only place the stream
			// names the input and prompt-cache counts (#738); the event
			// itself carries no other payload. A present usage object that
			// is not an exact whole-token count is not a report: it voids
			// the turn's whole account, never a turn failure, and never a
			// partial one. An exact envelope fills in the input-side counts
			// without discarding what the turn already recorded exactly —
			// an output count an earlier delta named stays, so no exact
			// count the provider reported is ever lost to a zero.
			if event.Message != nil {
				if input, cacheRead, cacheWrite, ok := decodeMessageStartUsage(event.Message.Usage); ok {
					if !usageUnusable {
						if usage == nil {
							usage = &Usage{}
						}
						usage.InputTokens = input
						usage.CacheReadTokens = cacheRead
						usage.CacheWriteTokens = cacheWrite
						usageSeen = true
					}
				} else if usageReported(event.Message.Usage) {
					usageUnusable = true
					usage, usageSeen = nil, false
				}
			}
			continue
		case "content_block_start":
			if event.Index == nil || event.ContentBlock == nil {
				return terminal(errors.New("harness: undecodable stream chunk"))
			}
			if len(blocksByIndex) >= maxStreamToolCallCount {
				return terminal(ErrStreamToolCallOverflow)
			}
			block := &anthropicBlock{kind: event.ContentBlock.Type, started: true}
			switch block.kind {
			case "tool_use":
				block.toolCall = ToolCall{ID: event.ContentBlock.ID, Name: event.ContentBlock.Name}
				identity := len(block.toolCall.ID) + len(block.toolCall.Name)
				if identityBytes+identity > maxStreamToolIdentityBytes {
					return terminal(ErrStreamToolIdentityOverflow)
				}
				identityBytes += identity
			case "redacted_thinking":
				block.redacted = event.ContentBlock.Data
			case "text", "thinking":
			default:
				return terminal(fmt.Errorf("harness: unexpected content block type %q", block.kind))
			}
			blocksByIndex[*event.Index] = block
			order = append(order, *event.Index)
		case "content_block_delta":
			if event.Index == nil || event.Delta == nil {
				return terminal(errors.New("harness: undecodable stream chunk"))
			}
			block, ok := blocksByIndex[*event.Index]
			if !ok {
				return terminal(errors.New("harness: undecodable stream chunk"))
			}
			switch event.Delta.Type {
			case "text_delta":
				if contentBytes+len(event.Delta.Text) > maxStreamContentBytes {
					return terminal(ErrStreamContentOverflow)
				}
				contentBytes += len(event.Delta.Text)
				block.text.WriteString(event.Delta.Text)
				next, kept := appendBoundedEvent(events, Event{Kind: KindDelta, Text: event.Delta.Text})
				if !kept {
					return terminal(ErrStreamEventOverflow)
				}
				events = next
			case "thinking_delta":
				// Reasoning is payload the turn retains, not assistant text:
				// it counts against the content bound and never becomes a
				// KindDelta event, so it can never leak into the transcript
				// as ordinary model output.
				if contentBytes+len(event.Delta.Thinking) > maxStreamContentBytes {
					return terminal(ErrStreamContentOverflow)
				}
				contentBytes += len(event.Delta.Thinking)
				block.thinking.WriteString(event.Delta.Thinking)
			case "signature_delta":
				block.signature.WriteString(event.Delta.Signature)
			case "input_json_delta":
				if argumentBytes+len(event.Delta.PartialJSON) > maxStreamToolArgumentBytes {
					return terminal(ErrStreamToolArgumentOverflow)
				}
				argumentBytes += len(event.Delta.PartialJSON)
				block.args = append(block.args, event.Delta.PartialJSON...)
			default:
				return terminal(fmt.Errorf("harness: unexpected stream delta type %q", event.Delta.Type))
			}
		case "content_block_stop":
			if event.Index == nil {
				return terminal(errors.New("harness: undecodable stream chunk"))
			}
			block, ok := blocksByIndex[*event.Index]
			if !ok || !block.started {
				return terminal(errors.New("harness: undecodable stream chunk"))
			}
			block.started = false
		case "message_delta":
			if event.Delta != nil && event.Delta.StopReason != "" {
				stopReason = event.Delta.StopReason
			}
			// The Messages stream's output count arrives here, cumulative
			// within the message: each value replaces the previous one, so
			// the last one is the turn's (#738). A present count that is
			// not an exact whole-token count is not a report: it voids
			// the turn's whole account, discarding the partial counts
			// already decoded beside it — a partial zero-valued report is
			// worse than no report at all — and never fails the turn. An
			// absent usage object changes nothing: a report assembled
			// from what the provider exactly named stands.
			if output, ok := decodeDeltaUsage(event.Usage); ok {
				if !usageUnusable {
					if usage == nil {
						usage = &Usage{}
					}
					usage.OutputTokens = output
					usageSeen = true
				}
			} else if usageReported(event.Usage) {
				usageUnusable = true
				usage, usageSeen = nil, false
			}
		case "message_stop":
			sawMessageStop = true
		case "error":
			// The error object is provider-controlled body data; only the
			// classified type reaches the trusted error string.
			errType := ""
			if event.Error != nil {
				errType = event.Error.Type
			}
			return terminal(anthropicStreamErrorClass(errType, signals))
		default:
			// An unknown payload type is a wire shape this normalizer does
			// not represent. Fail closed.
			return terminal(fmt.Errorf("harness: unexpected stream event type %q", event.Type))
		}
	}
	if err := scanner.Err(); err != nil {
		return terminal(&GatewayTransportError{Err: err})
	}
	if !sawMessageStop {
		// A stream that ended without message_stop never terminalized: no
		// content deserves to be treated as a completed answer.
		return terminal(ErrStreamTruncated)
	}
	finish, toolUse, err := anthropicStopReason(stopReason)
	if err != nil {
		return terminal(err)
	}
	// Assemble the assistant turn's provider-native blocks in stream order.
	for _, index := range order {
		block := blocksByIndex[index]
		encoded, err := block.finalize()
		if err != nil {
			return terminal(err)
		}
		blocks = append(blocks, encoded)
	}
	switch {
	case toolUse:
		var requests []ToolCall
		for _, encoded := range blocks {
			var block struct {
				Type  string          `json:"type"`
				ID    string          `json:"id"`
				Name  string          `json:"name"`
				Input json.RawMessage `json:"input"`
			}
			if err := json.Unmarshal(encoded, &block); err != nil {
				return terminal(errors.New("harness: incomplete tool request in provider stream"))
			}
			if block.Type != "tool_use" {
				continue
			}
			if block.ID == "" || block.Name == "" || len(block.Input) == 0 {
				return terminal(errors.New("harness: incomplete tool request in provider stream"))
			}
			requests = append(requests, ToolCall{ID: block.ID, Name: block.Name, Arguments: string(block.Input)})
		}
		if len(requests) == 0 {
			return terminal(errors.New("harness: tool_use finish without any tool request"))
		}
		if len(events)+len(requests) > maxStreamEvents {
			return terminal(ErrStreamEventOverflow)
		}
		for _, request := range requests {
			events = append(events, Event{Kind: KindToolRequest, Tool: request})
		}
		// The assistant turn's continuation blocks ride the last terminal
		// event: the consumer takes the blocks from the turn's terminal and
		// re-renders them verbatim on the next request.
		events[len(events)-1].Blocks = blocks
	default:
		next, kept := appendBoundedEvent(events, Event{Kind: KindFinal, FinishReason: finish, Blocks: blocks})
		if !kept {
			return terminal(ErrStreamEventOverflow)
		}
		events = next
	}
	// The terminal event is the last one in both success branches: the last
	// assembled tool request or the final. It carries the provider's usage
	// report for the turn when the provider sent one (#738); a stream the
	// provider counted is the request the run's telemetry accounts for.
	if usageSeen {
		events[len(events)-1].Usage = usage
	}
	return events, nil
}
