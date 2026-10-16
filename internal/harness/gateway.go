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
	"sort"
	"strconv"
	"strings"
	"time"
)

// Gateway is the LiteLLM-compatible model gateway client: the harness's only
// model transport. The endpoint and key are deployment/model configuration,
// never a LaneProfile concern; a lane supplies only per-role model
// identifiers. The OpenAI-compatible wire format is parsed here and nowhere
// else.
type Gateway struct {
	// BaseURL is the OpenAI-compatible API root, for example
	// "http://litellm:4000/v1". Requests post to BaseURL + "/chat/completions".
	BaseURL string
	// APIKey is the bearer token sent with every request. Empty means the
	// gateway accepts unauthenticated callers.
	APIKey string
	HTTP   *http.Client
	// ExhaustionSignals names the structured provider signals that mean a
	// model allowance is spent rather than a transient rate limit (#320). An
	// empty value falls back to the provider-neutral defaults; deployments
	// extend the defaults through configuration.
	ExhaustionSignals ExhaustionSignals
}

func (g *Gateway) client() *http.Client {
	if g.HTTP != nil {
		return g.HTTP
	}
	return http.DefaultClient
}

// signals returns the configured exhaustion signals, falling back to the
// provider-neutral defaults when the deployment configures none.
func (g *Gateway) signals() ExhaustionSignals {
	if g.ExhaustionSignals.Empty() {
		return DefaultExhaustionSignals()
	}
	return g.ExhaustionSignals
}

// Message is one conversation message in the normalized transcript. Role is
// the OpenAI-compatible role vocabulary; tool results ride in Role "tool"
// with ToolCallID set, and an assistant's own tool requests ride in
// ToolCalls. Messages are never marshaled directly: toWireMessages renders
// them in the OpenAI-compatible request shape.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content,omitempty"`
	// ToolCallID links a Role "tool" message to the request it answers.
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"-"`
}

// wireMessage is the OpenAI-compatible request shape of one transcript
// message. Assistant tool requests serialize as tool_calls with function
// objects — the shape every OpenAI-compatible gateway accepts — and tool
// results carry tool_call_id.
type wireMessage struct {
	Role       string                  `json:"role"`
	Content    string                  `json:"content,omitempty"`
	ToolCallID string                  `json:"tool_call_id,omitempty"`
	ToolCalls  []wireAssistantToolCall `json:"tool_calls,omitempty"`
}

type wireAssistantToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func toWireMessages(messages []Message) []wireMessage {
	out := make([]wireMessage, 0, len(messages))
	for _, message := range messages {
		wire := wireMessage{Role: message.Role, Content: message.Content, ToolCallID: message.ToolCallID}
		for _, call := range message.ToolCalls {
			entry := wireAssistantToolCall{ID: call.ID, Type: "function"}
			entry.Function.Name = call.Name
			entry.Function.Arguments = call.Arguments
			wire.ToolCalls = append(wire.ToolCalls, entry)
		}
		out = append(out, wire)
	}
	return out
}

// ToolDef declares one tool the model may request. Only trusted control
// implements tools; the declaration is the model's entire view of them.
type ToolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

// ChatRequest is one model turn. Model is the role-bound model identifier
// from the LaneProfile, passed through to the gateway verbatim: the gateway
// owns provider routing and Courier stays model-agnostic.
type ChatRequest struct {
	Model    string
	Messages []Message
	Tools    []ToolDef
}

type wireRequest struct {
	Model    string        `json:"model"`
	Messages []wireMessage `json:"messages"`
	Tools    []wireTool    `json:"tools,omitempty"`
	Stream   bool          `json:"stream"`
}

type wireTool struct {
	Type     string  `json:"type"`
	Function ToolDef `json:"function"`
}

// StreamChat performs one streaming chat completion and returns the
// normalized event sequence. Events arrive in a fixed order: every delta
// first, then assembled tool requests, then — when the turn ended without
// requesting tools — exactly one terminal event (final or error). A
// tool_calls finish ends the sequence with the assembled tool requests; the
// trusted tool results start the next turn. A non-2xx response or an
// unreadable stream is a KindError, never a partial success.
func (g *Gateway) StreamChat(ctx context.Context, req ChatRequest) ([]Event, error) {
	body, err := json.Marshal(wireRequest{
		Model:    req.Model,
		Messages: toWireMessages(req.Messages),
		Tools:    toolsToWire(req.Tools),
		Stream:   true,
	})
	if err != nil {
		return nil, fmt.Errorf("harness: encode chat request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(g.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("harness: build chat request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if g.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+g.APIKey)
	}
	resp, err := g.client().Do(httpReq)
	if err != nil {
		return []Event{{Kind: KindError, Err: &GatewayTransportError{Err: err}}}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Read only a bounded prefix of the error body to classify the
		// failure. The body is never retained or echoed: a provider error
		// may quote credentials, endpoints, or model-influenced text, and the
		// capability contract requires a safe reason instead.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProviderErrorBody))
		return []Event{{Kind: KindError, Err: gatewayStatusError(resp.StatusCode, resp.Header, body, g.signals())}}, nil
	}
	return normalizeStream(resp.Body, g.signals())
}

// wireModelInfo is the OpenAI/LiteLLM-compatible model-metadata envelope. An
// entry advertises its input limit under model_info, with the entry-level field
// as the fallback for a gateway that does not nest it.
type wireModelInfo struct {
	Data []wireModelEntry `json:"data"`
}

type wireModelEntry struct {
	ModelName      string `json:"model_name"`
	ID             string `json:"id"`
	MaxInputTokens int    `json:"max_input_tokens"`
	ModelInfo      struct {
		MaxInputTokens int `json:"max_input_tokens"`
	} `json:"model_info"`
}

func (e wireModelEntry) inputLimit() int {
	if e.ModelInfo.MaxInputTokens > 0 {
		return e.ModelInfo.MaxInputTokens
	}
	return e.MaxInputTokens
}

// maxModelInfoBody bounds the model-metadata read. It is a transport resource
// bound, not a work limit: a gateway lists every model it serves, and a body
// larger than this is broken or hostile.
const maxModelInfoBody = 1 << 20

// ModelInputLimit reads the input limit the gateway advertises for one bound
// model. The read is what lets a lane's declared budget be verified rather
// than merely declared (#243, #296): a ceiling the gateway could not confirm
// stays `configured`, never healthy. Failures follow the same redaction
// contract as every other gateway path — a bounded body prefix is read only to
// classify the failure and is never retained or echoed — and an unknown model,
// an unusable limit, or an unreadable envelope is an explicit error, never a
// silent zero a caller could mistake for a small window. The success path
// reads under maxModelInfoBody, so a hostile 200 cannot amplify memory at
// startup.
func (g *Gateway) ModelInputLimit(ctx context.Context, model string) (int, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(g.BaseURL, "/")+"/model/info", nil)
	if err != nil {
		return 0, fmt.Errorf("harness: build model info request: %w", err)
	}
	httpReq.Header.Set("Accept", "application/json")
	if g.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+g.APIKey)
	}
	resp, err := g.client().Do(httpReq)
	if err != nil {
		return 0, &GatewayTransportError{Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProviderErrorBody))
		return 0, gatewayStatusError(resp.StatusCode, resp.Header, body, g.signals())
	}
	var report wireModelInfo
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxModelInfoBody)).Decode(&report); err != nil {
		return 0, errors.New("harness: gateway model info is undecodable")
	}
	limit, named := 0, false
	for _, entry := range report.Data {
		if entry.ModelName != model && entry.ID != model {
			continue
		}
		limit, named = entry.inputLimit(), true
		break
	}
	if !named {
		return 0, fmt.Errorf("harness: gateway model info names no entry for model %q", model)
	}
	if limit <= 0 {
		return 0, fmt.Errorf("harness: gateway advertises no input limit for model %q", model)
	}
	return limit, nil
}

// gatewayStatusError maps a gateway HTTP status to a redacted, classified,
// actionable error. Provider response bodies are never echoed.
func gatewayStatusError(status int, header http.Header, body []byte, signals ExhaustionSignals) *GatewayError {
	classification := ClassifyProviderError(status, header, body, signals)
	return &GatewayError{
		Status:       status,
		Availability: classification.Availability,
		ProviderCode: classification.Code,
		RetryAfter:   classification.RetryAfter,
		Retryable:    classification.Retryable,
	}
}

// GatewayTransportError is a transport-class gateway failure: dial, reset,
// EOF, or a mid-stream read error. Retrying and re-probing can succeed, so
// both the session retry loop and the probe classifier treat it as transient.
type GatewayTransportError struct{ Err error }

func (e *GatewayTransportError) Error() string {
	return fmt.Sprintf("harness: gateway transport failed: %v", e.Err)
}

func (e *GatewayTransportError) Unwrap() error { return e.Err }

// GatewayError is a redacted, classified gateway failure. It carries the
// status, a fixed availability class, and at most a short allowlisted provider
// signal — never a provider response body.
type GatewayError struct {
	Status int
	// Availability is the provider-availability class. Empty means the error
	// was built without classification (a legacy literal); callers fall back
	// to status-based retryability.
	Availability ProviderAvailability
	// ProviderCode is the bounded structured provider error code or type when
	// recognized; empty otherwise.
	ProviderCode string
	// RetryAfter is the parsed authoritative reset/retry hint, zero when
	// absent or unknown.
	RetryAfter time.Duration
	// Retryable reports whether an immediate in-process retry can succeed.
	// Only meaningful when Availability is set.
	Retryable bool
}

func (e *GatewayError) Error() string {
	if e.Status == 0 && e.Availability != "" {
		return fmt.Sprintf("harness: gateway reported a stream error (%s)", e.Availability)
	}
	return fmt.Sprintf("harness: gateway returned status %d (%s)", e.Status, httpStatusCategory(e.Status))
}

// classification returns the error's classification, defaulting to a
// status-derived one for an error built without explicit classification.
func (e *GatewayError) classification() ProviderClassification {
	if e.Availability == "" {
		return ClassifyProviderError(e.Status, nil, nil, DefaultExhaustionSignals())
	}
	return ProviderClassification{
		Availability: e.Availability,
		Code:         e.ProviderCode,
		RetryAfter:   e.RetryAfter,
		Retryable:    e.Retryable,
	}
}

func httpStatusCategory(status int) string {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return "authentication or authorization rejected"
	case status == http.StatusNotFound:
		return "model or route not found"
	case status == http.StatusBadRequest:
		return "request rejected"
	case status == http.StatusTooManyRequests:
		return "rate limited"
	case status >= 500:
		return "gateway unavailable"
	default:
		return "unexpected response"
	}
}

// streamErrorClassification classifies a provider error object that arrived
// inside a 200 stream. It carries no HTTP status, so only a recognized
// structured signal is placed; everything else is unknown.
func streamErrorClassification(wire *wireError, signals ExhaustionSignals) error {
	classification := classifyStreamSignal(wireCodeString(wire.Code), wire.Type, signals)
	return &GatewayError{
		Status:       0,
		Availability: classification.Availability,
		ProviderCode: classification.Code,
		RetryAfter:   classification.RetryAfter,
		Retryable:    classification.Retryable,
	}
}

// wireCodeString renders a decoded JSON error code (string or number) as a
// string, or empty for any other shape.
func wireCodeString(code any) string {
	switch value := code.(type) {
	case string:
		return value
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case json.Number:
		return value.String()
	default:
		return ""
	}
}

func toolsToWire(tools []ToolDef) []wireTool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]wireTool, 0, len(tools))
	for _, t := range tools {
		out = append(out, wireTool{Type: "function", Function: t})
	}
	return out
}

// maxStreamLineBytes bounds one SSE line. It is a transport resource bound,
// not a work or model-behavior limit: a single SSE line larger than this is
// a hostile or broken gateway response.
const maxStreamLineBytes = 1 << 20

type wireChunk struct {
	Choices []wireChoice `json:"choices"`
	Error   *wireError   `json:"error"`
}

type wireChoice struct {
	Index        int       `json:"index"`
	Delta        wireDelta `json:"delta"`
	FinishReason string    `json:"finish_reason"`
}

type wireDelta struct {
	Content   string         `json:"content"`
	ToolCalls []wireToolCall `json:"tool_calls"`
}

type wireToolCall struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type wireError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    any    `json:"code"`
}

// normalizeStream reads an SSE body and produces the normalized event
// sequence. Tool-call fragments are assembled here — consumers never see a
// partial tool request — and the terminal event closes the sequence.
func normalizeStream(body io.Reader, signals ExhaustionSignals) ([]Event, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxStreamLineBytes)

	var events []Event
	// Tool-call fragments are accumulated per index and emitted, in index
	// order, once the stream closes the turn.
	fragments := map[int]*ToolCall{}
	var finish string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") {
			continue
		}
		payload, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		payload = strings.TrimSpace(payload)
		if payload == "[DONE]" {
			break
		}
		var chunk wireChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return streamError(events, fmt.Errorf("harness: undecodable stream chunk"))
		}
		if chunk.Error != nil {
			// The error object is provider-controlled body data; its type and
			// message never reach the trusted error string, which can flow
			// into capability diagnostics and termination reasons. Only a
			// recognized structured code/type is retained as a classification.
			return streamError(events, streamErrorClassification(chunk.Error, signals))
		}
		for _, choice := range chunk.Choices {
			if choice.Index != 0 {
				// Requests carry n=1 implicitly; any other choice index is a
				// wire shape this normalizer does not represent. Fail closed.
				return streamError(events, fmt.Errorf("harness: unexpected stream choice index %d", choice.Index))
			}
			if choice.Delta.Content != "" {
				events = append(events, Event{Kind: KindDelta, Text: choice.Delta.Content})
			}
			for _, call := range choice.Delta.ToolCalls {
				fragment, ok := fragments[call.Index]
				if !ok {
					fragment = &ToolCall{}
					fragments[call.Index] = fragment
				}
				if call.ID != "" {
					fragment.ID = call.ID
				}
				if call.Function.Name != "" {
					fragment.Name = call.Function.Name
				}
				fragment.Arguments += call.Function.Arguments
			}
			if choice.FinishReason != "" {
				finish = choice.FinishReason
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return streamError(events, &GatewayTransportError{Err: err})
	}
	if finish == "" {
		// A turn with no finish reason never terminalized: no content
		// deserves to be treated as a completed answer.
		return streamError(events, ErrStreamTruncated)
	}
	switch finish {
	case "tool_calls":
		if len(fragments) == 0 {
			return streamError(events, fmt.Errorf("harness: tool_calls finish without any tool request"))
		}
		for _, index := range sortedFragmentIndexes(fragments) {
			call := fragments[index]
			if call.Name == "" || call.ID == "" {
				return streamError(events, fmt.Errorf("harness: incomplete tool request in provider stream"))
			}
			events = append(events, Event{Kind: KindToolRequest, Tool: *call})
		}
	default:
		if len(fragments) > 0 {
			return streamError(events, fmt.Errorf("harness: incomplete tool request in provider stream"))
		}
		events = append(events, Event{Kind: KindFinal, FinishReason: finish})
	}
	return events, nil
}

// streamError appends exactly one terminal error event to whatever events
// already arrived. Deltas already emitted remain valid data; the turn failed
// and is reported as such, never masked as a final.
func streamError(events []Event, err error) ([]Event, error) {
	return append(events, Event{Kind: KindError, Err: err}), nil
}

func sortedFragmentIndexes(fragments map[int]*ToolCall) []int {
	indexes := make([]int, 0, len(fragments))
	for index := range fragments {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	return indexes
}
