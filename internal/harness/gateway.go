package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
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
}

func (g *Gateway) client() *http.Client {
	if g.HTTP != nil {
		return g.HTTP
	}
	return http.DefaultClient
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
		return []Event{{Kind: KindError, Err: gatewayStatusError(resp.StatusCode)}}, nil
	}
	return normalizeStream(resp.Body)
}

// gatewayStatusError maps a gateway HTTP status to a redacted, actionable
// error. Provider response bodies are never echoed: they may quote upstream
// endpoints, key material, or model-influenced text, and the capability
// contract requires a safe reason instead.
func gatewayStatusError(status int) error {
	return &GatewayError{Status: status}
}

// GatewayTransportError is a transport-class gateway failure: dial, reset,
// EOF, or a mid-stream read error. Retrying and re-probing can succeed, so
// both the session retry loop and the probe classifier treat it as transient.
type GatewayTransportError struct{ Err error }

func (e *GatewayTransportError) Error() string {
	return fmt.Sprintf("harness: gateway transport failed: %v", e.Err)
}

func (e *GatewayTransportError) Unwrap() error { return e.Err }

// GatewayError is a redacted gateway failure. It carries only the status and
// a fixed category — never a provider response body.
type GatewayError struct {
	Status int
}

func (e *GatewayError) Error() string {
	return fmt.Sprintf("harness: gateway returned status %d (%s)", e.Status, httpStatusCategory(e.Status))
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

// isTransientGatewayStatus reports whether a gateway status is worth
// retrying in-process: gateway-side failures (5xx) and the two explicitly
// retryable client statuses (request timeout, rate limit). Every other 4xx
// is a configuration or request-shape problem that retrying cannot fix.
func isTransientGatewayStatus(status int) bool {
	return status >= 500 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests
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
func normalizeStream(body io.Reader) ([]Event, error) {
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
			return streamError(events, fmt.Errorf("harness: gateway reported stream error (%s)", strings.TrimSpace(chunk.Error.Type)))
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
