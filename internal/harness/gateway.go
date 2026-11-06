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
	"sync/atomic"
	"time"
)

// Gateway is the OpenAI-compatible model transport (#716): one of the two
// wire formats behind the ModelTransport boundary, and the default for lanes
// that declare none. The endpoint and key are deployment/model configuration,
// never a LaneProfile concern; a lane supplies only per-role model
// identifiers. The OpenAI-compatible wire format is parsed here and nowhere
// else; the Anthropic Messages wire format lives in the Anthropic transport.
type Gateway struct {
	// BaseURL is the OpenAI-compatible API root, for example
	// "http://litellm:4000/v1". Requests post to BaseURL + "/chat/completions".
	BaseURL string
	// APIKey is the bearer token sent with every request. Empty means the
	// gateway accepts unauthenticated callers.
	APIKey string
	// HTTP is the caller-supplied HTTP client. Nil means the transport uses
	// the harness-owned default, whose first-byte stall bound (#754) fails a
	// gateway that never produces a response header as a transport error; a
	// supplied client belongs to whoever supplied it and is never modified.
	HTTP *http.Client
	// ExhaustionSignals names the structured provider signals that mean a
	// model allowance is spent rather than a transient rate limit (#320). An
	// empty value falls back to the provider-neutral defaults; deployments
	// extend the defaults through configuration.
	ExhaustionSignals ExhaustionSignals
	// usageUnsupported remembers a gateway that rejected the request because
	// of the usage request's own field (#738): after one such rejection,
	// every request this transport sends omits stream_options and the
	// provider's usage stays an explicit absence. It is observability state:
	// setting it never changes a request beyond dropping the telemetry
	// field, so the run behaves exactly as it would without the usage
	// request.
	usageUnsupported atomic.Bool
}

func (g *Gateway) client() *http.Client {
	if g.HTTP != nil {
		return g.HTTP
	}
	// The harness-owned default carries the first-byte stall bound (#754):
	// http.DefaultClient has no timeouts at all, so a gateway that accepted
	// the connection and stalled would hold the turn on the bare run context.
	return ownedModelClient
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
// ToolCalls. Messages are never marshaled directly: each transport renders
// them in its own wire shape.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content,omitempty"`
	// ToolCallID links a Role "tool" message to the request it answers.
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"-"`
	// Blocks is the assistant turn's provider-native content blocks, opaque
	// above the transport (#716): the transport that produced them re-renders
	// them verbatim on the next request — Anthropic thinking blocks with
	// their signatures foremost, so continuation keeps provider semantics —
	// and every other transport ignores them. Nothing above the transport
	// may parse, log, render, or flatten these bytes into ordinary text.
	Blocks []json.RawMessage `json:"-"`
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
	// MaxTokens is the output ceiling the caller reserves for the turn, from
	// the lane's declared output reservation when one is declared. Wire
	// formats that require an explicit output bound — the Anthropic Messages
	// API — send it; formats that treat it as optional, like the OpenAI
	// chat-completions request, ignore it exactly as today.
	MaxTokens int
}

// wireStreamOptions is the streaming option the OpenAI-compatible request
// carries so the gateway reports the turn's usage on the stream (#738): the
// telemetry that accounts for the run's model spend reads the count the
// provider actually sent. It is a request-shape requirement of that
// telemetry contract, not a capability claim: a gateway that ignores it
// simply reports no usage, and the absence is recorded, never invented.
type wireStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type wireRequest struct {
	Model         string             `json:"model"`
	Messages      []wireMessage      `json:"messages"`
	Tools         []wireTool         `json:"tools,omitempty"`
	Stream        bool               `json:"stream"`
	StreamOptions *wireStreamOptions `json:"stream_options,omitempty"`
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
//
// Every streamed turn asks the gateway for its usage report with
// stream_options.include_usage (#738). A gateway that does not know the
// field must not be turned away by it: telemetry is observability only, so
// a request-shape rejection that names the field is answered by one retry
// without it, the transport remembers, and every later request on it omits
// the field — the usage is then an explicit absence, and the run proceeds
// exactly as it would have without the telemetry request. The rejected
// attempt itself is bounded by the same memory: the field is dropped after
// the first rejection, so only the requests in flight around that first
// rejection can repeat it. The rejected request the provider already
// received is not left unnamed: it rides the turn's terminal event on
// RejectedRequests, so the run's request observers record every provider
// request the turn sent and the per-run request counts stay a total.
func (g *Gateway) StreamChat(ctx context.Context, req ChatRequest) ([]Event, error) {
	includeUsage := !g.usageUnsupported.Load()
	resp, sent, err := g.post(ctx, req, includeUsage)
	if err != nil {
		if !sent {
			// The request was never built: not a provider request, reported
			// as the harness's own failure.
			return nil, err
		}
		return []Event{{Kind: KindError, Err: err}}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Read only a bounded prefix of the error body to classify the
		// failure. The body is never retained or echoed: a provider error
		// may quote credentials, endpoints, or model-influenced text, and the
		// capability contract requires a safe reason instead.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProviderErrorBody))
		// A gateway that rejects the request shape because of the usage
		// request (#738) must not fail the turn for telemetry's sake: the
		// rejection is recognized from the bounded error prefix naming the
		// field, the field is dropped for the rest of this transport's life,
		// and the turn retries once without it. Any other rejection behaves
		// exactly as before.
		if includeUsage && rejectedUsageRequest(body) {
			g.usageUnsupported.Store(true)
			resp, sent, err = g.post(ctx, req, false)
			if err != nil {
				if !sent {
					return nil, err
				}
				return []Event{{Kind: KindError, Err: err, RejectedRequests: 1}}, nil
			}
			defer resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProviderErrorBody))
				return []Event{{Kind: KindError, Err: gatewayStatusError(resp.StatusCode, resp.Header, body, g.signals()), RejectedRequests: 1}}, nil
			}
			events, err := normalizeStream(resp.Body, g.signals())
			// The request the provider rejected is a provider request the
			// run's accounting must name (#738): it rides the turn's
			// terminal event, whatever the retry's outcome, and the
			// observers record it as its own non-OK request with an
			// explicit usage absence.
			if len(events) > 0 {
				events[len(events)-1].RejectedRequests = 1
			}
			return events, err
		}
		return []Event{{Kind: KindError, Err: gatewayStatusError(resp.StatusCode, resp.Header, body, g.signals())}}, nil
	}
	return normalizeStream(resp.Body, g.signals())
}

// post sends one chat-completions request and reports whether it was sent:
// a request that failed to build never reached the provider, and the caller
// reports it as the harness's own failure rather than a provider response
// (#738). includeUsage decides whether the request carries
// stream_options.include_usage. A sent-and-failed transport error returns
// sent=true with a GatewayTransportError.
func (g *Gateway) post(ctx context.Context, req ChatRequest, includeUsage bool) (*http.Response, bool, error) {
	body, err := json.Marshal(wireRequest{
		Model:    req.Model,
		Messages: toWireMessages(req.Messages),
		Tools:    toolsToWire(req.Tools),
		Stream:   true,
		// The provider's usage report rides the terminal event when the
		// gateway sends one (#738); a gateway that sends none records an
		// explicit absence.
		StreamOptions: wireStreamOptionsFor(includeUsage),
	})
	if err != nil {
		return nil, false, fmt.Errorf("harness: encode chat request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(g.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("harness: build chat request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if g.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+g.APIKey)
	}
	resp, err := g.client().Do(httpReq)
	if err != nil {
		return nil, true, &GatewayTransportError{Err: err}
	}
	return resp, true, nil
}

// wireStreamOptionsFor renders the request's stream_options field: the usage
// request when telemetry asks for one, absent otherwise.
func wireStreamOptionsFor(includeUsage bool) *wireStreamOptions {
	if !includeUsage {
		return nil
	}
	return &wireStreamOptions{IncludeUsage: true}
}

// rejectedUsageRequest reports whether a non-2xx response is the gateway
// rejecting the request because of the usage request itself (#738): the
// bounded error prefix names the field. The status class is not a gate —
// gateways reject unknown fields with 4xx and, less conventionally, with
// 5xx — because a false match costs only the usage (an explicit absence for
// the rest of the transport's life), while a missed one would cost the run.
// The prefix is inspected and never retained or echoed. A gateway may name
// other rejected arguments in the same body; the check matches the field
// name only, because that is the argument this transport added. A gateway
// that rejects the field only inside an otherwise-acceptable 200 stream
// cannot be helped here: that rejection stays a stream error, as it would
// without the usage request's retry seam.
func rejectedUsageRequest(body []byte) bool {
	return bytes.Contains(body, []byte("stream_options"))
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
// silent zero a caller could mistake for a small window — trailing data after
// the envelope included. The success path
// reads under maxModelInfoBody, so a hostile 200 cannot amplify memory at
// startup.
func (g *Gateway) ModelInputLimit(ctx context.Context, model string) (int, error) {
	return modelInfoInputLimit(ctx, g.client(), g.BaseURL, g.APIKey, model, g.signals())
}

// modelInfoInputLimit is the shared /model/info read behind every transport's
// verified input limit. The endpoint is gateway deployment metadata, not part
// of either wire protocol, so both transports may use it where it exists; a
// transport whose gateway lacks it returns the explicit error and the role's
// declared ceiling stays `configured` (#716 — the endpoint is never a
// universal protocol requirement, and a missing limit is never invented).
func modelInfoInputLimit(ctx context.Context, client *http.Client, baseURL, apiKey, model string, signals ExhaustionSignals) (int, error) {
	if client == nil {
		client = ownedModelClient
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(baseURL, "/")+"/model/info", nil)
	if err != nil {
		return 0, fmt.Errorf("harness: build model info request: %w", err)
	}
	httpReq.Header.Set("Accept", "application/json")
	if apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return 0, &GatewayTransportError{Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProviderErrorBody))
		return 0, gatewayStatusError(resp.StatusCode, resp.Header, body, signals)
	}
	var report wireModelInfo
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxModelInfoBody))
	if err := decoder.Decode(&report); err != nil {
		return 0, errors.New("harness: gateway model info is undecodable")
	}
	// Trailing bytes — a second JSON value or any non-whitespace garbage —
	// make the envelope unusable, not merely larger: a decoder that silently
	// accepted them would treat a malformed or hostile body as a trusted one.
	if decoder.More() {
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

// maxStreamContentBytes bounds the total content-delta bytes one completion
// may hold. It is a transport resource bound, not a work or model-behavior
// limit: a per-line bound says nothing about a stream of many individually
// valid lines, and one that exceeds this is a hostile or broken gateway
// response.
const maxStreamContentBytes = 8 << 20

// maxStreamToolArgumentBytes bounds the tool-argument bytes one completion
// may hold across every fragment. It is a transport resource bound, not a
// work or model-behavior limit: it says what a single turn may cost the
// control pod to assemble, never how much work the model may do.
const maxStreamToolArgumentBytes = 4 << 20

// maxStreamToolCallCount bounds the distinct tool-call indexes one completion
// may hold. It is a transport resource bound, not a work or model-behavior
// limit: it bounds the fragment table a stream can grow, not the tool calls
// a model is allowed to make.
const maxStreamToolCallCount = 128

// maxStreamEvents bounds the events one completion may retain. It is a
// transport resource bound, not a work, duration, or model-behavior limit: an
// Event costs its caller a slot whatever it carries, so a payload-byte bound
// does not bound retention on its own — at this cap the retained events cost
// roughly 11 MB even when every one of them carries a single byte.
const maxStreamEvents = 1 << 16

// maxStreamToolIdentityBytes bounds the tool-call identity bytes one
// completion may send. It is a transport resource bound, not a work,
// duration, or model-behavior limit: identity fields are overwritten per
// fragment rather than accumulated, so a budget charged only against what a
// call retains would let a stream send unbounded identity bytes for free.
const maxStreamToolIdentityBytes = 64 << 10

type wireChunk struct {
	Choices []wireChoice `json:"choices"`
	Error   *wireError   `json:"error"`
	// Usage is the provider's report for the whole turn, on a usage chunk
	// whose choices are empty: the gateway's answer to the request's
	// stream_options.include_usage. It is raw because a report telemetry
	// cannot read as exact whole-token counts is an absence, never a turn
	// failure (#738).
	Usage json.RawMessage `json:"usage,omitempty"`
}

// wireUsage is the provider's token accounting for one completion, in the
// shape the OpenAI-compatible stream carries: the input accepted (with the
// prompt-cache subset the provider served from its cache, reported
// alongside) and the output produced. Prompt and completion counts are the
// report's mandatory totals — a present usage object missing either is not
// an exact report — while the cache subset is optional and defaults to zero
// when the provider omits it. Counts decode as JSON numbers, and a value
// that is not a whole number fails the count, not the turn.
type wireUsage struct {
	PromptTokens        json.Number `json:"prompt_tokens"`
	CompletionTokens    json.Number `json:"completion_tokens"`
	PromptTokensDetails struct {
		Cached json.Number `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

// decodeUsage decodes the provider's usage report leniently (#738): a
// provider or proxy that reports a fractional or otherwise non-integer
// count has not reported an exact token count, so the report is treated as
// absent — the turn is never failed for telemetry's sake, and no
// fractional count the provider did not report is invented. The report's
// mandatory total counts — prompt_tokens and completion_tokens, the input
// accepted and the output produced — must be present and exact before an
// exact report is decoded: a present object missing either names no exact
// usage at all, and an absent field is never an invented zero (#738). The
// prompt-cache subset is optional and takes its documented default of zero
// when the provider omits it. The second return reports whether an exact
// report was decoded.
func decodeUsage(raw json.RawMessage) (*Usage, bool) {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false
	}
	var wire wireUsage
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, false
	}
	prompt, promptOK := wholeCount(wire.PromptTokens)
	completion, completionOK := wholeCount(wire.CompletionTokens)
	cached, cachedOK := optionalWholeCount(wire.PromptTokensDetails.Cached)
	if !promptOK || !completionOK || !cachedOK {
		return nil, false
	}
	return &Usage{
		InputTokens:     prompt,
		OutputTokens:    completion,
		CacheReadTokens: cached,
	}, true
}

// usageReported reports whether a raw usage object is present: non-empty and
// not JSON null. Both transports share it as the difference between "the
// provider sent a usage object telemetry cannot honor" and "the provider
// sent none" (#738): the former voids the turn's whole report — a report
// that cannot be honored is no report, and a partial zero-valued one is
// worse than none — while the latter leaves whatever the provider exactly
// named standing.
func usageReported(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

// wholeCount reads one provider-reported count as an exact integer token
// count. The field must be present: an absent number is no report at all,
// and a zero the provider did not state is not an exact one (#738) — a
// usage object missing a required count voids the report exactly as a
// fractional one does. A fractional or malformed value is likewise not a
// report.
func wholeCount(number json.Number) (int64, bool) {
	if number == "" {
		return 0, false
	}
	value, err := number.Int64()
	if err != nil {
		return 0, false
	}
	return value, true
}

// optionalWholeCount reads one provider-reported count whose presence the
// transport does not require: the prompt-cache subsets some providers omit
// entirely. An absent field takes its documented transport default — the
// provider named no cache activity, so zero — while a present value must
// still be an exact whole-token count, and absence never stands in for a
// total the provider did not report.
func optionalWholeCount(number json.Number) (int64, bool) {
	if number == "" {
		return 0, true
	}
	return wholeCount(number)
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

// toolFragment is one tool call still under assembly: identity fields arrive
// whole, arguments arrive in fragments appended to args and become a string
// only when the request is emitted.
type toolFragment struct {
	call ToolCall
	args []byte
}

// normalizeStream reads an SSE body and produces the normalized event
// sequence. Tool-call fragments are assembled here — consumers never see a
// partial tool request — and the terminal event closes the sequence. The
// per-line, payload, and event bounds are transport resource bounds: each is
// checked before the bytes, entry, or event it would retain is kept, so
// crossing one ends the turn with a redacted error and never with an
// executable partial tool request: requests are assembled during the scan
// loop and emitted atomically after it — every fragment's completeness and
// the whole set's event capacity are checked before the first request is
// retained.
func normalizeStream(body io.Reader, signals ExhaustionSignals) ([]Event, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxStreamLineBytes)

	var events []Event
	// Tool-call fragments are accumulated per index and emitted, in index
	// order, once the stream closes the turn.
	fragments := map[int]*toolFragment{}
	var contentBytes, argumentBytes, identityBytes int
	var finish string
	// usage is the provider's report for the turn, from the usage chunk the
	// gateway sends in answer to stream_options.include_usage (#738). Nil is
	// an explicit absence: the gateway sent no usage and the terminal event
	// records that, never an invented zero.
	var usage *Usage

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
			return streamError(events, usage, fmt.Errorf("harness: undecodable stream chunk"))
		}
		if chunk.Error != nil {
			// The error object is provider-controlled body data; its type and
			// message never reach the trusted error string, which can flow
			// into capability diagnostics and termination reasons. Only a
			// recognized structured code/type is retained as a classification.
			return streamError(events, usage, streamErrorClassification(chunk.Error, signals))
		}
		if chunk.Usage != nil {
			// The usage chunk carries no deltas and no tool fragments, only
			// the provider's count for the turn (#738). The choices loop
			// below sees nothing in it. A report telemetry cannot read as
			// exact whole-token counts is an absence, never a turn failure:
			// the run must proceed exactly as it would have without the
			// usage request. An exact report replaces an earlier one — the
			// gateway reports the turn's usage once, on a trailing chunk —
			// and a present report that is not exact voids the report
			// already assembled (#738): a report that cannot be honored is
			// no report, on this transport exactly as on the Anthropic one.
			if decoded, ok := decodeUsage(chunk.Usage); ok {
				usage = decoded
			} else if usageReported(chunk.Usage) {
				usage = nil
			}
		}
		for _, choice := range chunk.Choices {
			if choice.Index != 0 {
				// Requests carry n=1 implicitly; any other choice index is a
				// wire shape this normalizer does not represent. Fail closed.
				return streamError(events, usage, fmt.Errorf("harness: unexpected stream choice index %d", choice.Index))
			}
			if choice.Delta.Content != "" {
				if contentBytes+len(choice.Delta.Content) > maxStreamContentBytes {
					return streamError(events, usage, ErrStreamContentOverflow)
				}
				contentBytes += len(choice.Delta.Content)
				next, kept := appendBoundedEvent(events, Event{Kind: KindDelta, Text: choice.Delta.Content})
				if !kept {
					return streamError(events, usage, ErrStreamEventOverflow)
				}
				events = next
			}
			for _, call := range choice.Delta.ToolCalls {
				fragment, ok := fragments[call.Index]
				if !ok {
					if len(fragments) >= maxStreamToolCallCount {
						return streamError(events, usage, ErrStreamToolCallOverflow)
					}
					fragment = &toolFragment{}
					fragments[call.Index] = fragment
				}
				// Identity is charged as received, not as retained: a stream may
				// overwrite these fields, but every byte it sends costs the
				// control pod to receive and parse. Empty fields are free, so
				// this charges exactly the non-empty ID and Name.
				identity := len(call.ID) + len(call.Function.Name)
				if identityBytes+identity > maxStreamToolIdentityBytes {
					return streamError(events, usage, ErrStreamToolIdentityOverflow)
				}
				identityBytes += identity
				if call.ID != "" {
					fragment.call.ID = call.ID
				}
				if call.Function.Name != "" {
					fragment.call.Name = call.Function.Name
				}
				if call.Function.Arguments != "" {
					if argumentBytes+len(call.Function.Arguments) > maxStreamToolArgumentBytes {
						return streamError(events, usage, ErrStreamToolArgumentOverflow)
					}
					argumentBytes += len(call.Function.Arguments)
					fragment.args = append(fragment.args, call.Function.Arguments...)
				}
			}
			if choice.FinishReason != "" {
				finish = choice.FinishReason
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return streamError(events, usage, &GatewayTransportError{Err: err})
	}
	if finish == "" {
		// A turn with no finish reason never terminalized: no content
		// deserves to be treated as a completed answer.
		return streamError(events, usage, ErrStreamTruncated)
	}
	switch finish {
	case "tool_calls":
		if len(fragments) == 0 {
			return streamError(events, usage, fmt.Errorf("harness: tool_calls finish without any tool request"))
		}
		// Requests are emitted atomically. Every fragment's completeness and
		// the whole set's event capacity are checked before the first request
		// is retained, so no rejection path can leave one executable request
		// beside the terminal error.
		indexes := sortedFragmentIndexes(fragments)
		for _, index := range indexes {
			fragment := fragments[index]
			if fragment.call.Name == "" || fragment.call.ID == "" {
				return streamError(events, usage, fmt.Errorf("harness: incomplete tool request in provider stream"))
			}
		}
		if len(events)+len(indexes) > maxStreamEvents {
			return streamError(events, usage, ErrStreamEventOverflow)
		}
		for _, index := range indexes {
			fragment := fragments[index]
			call := fragment.call
			call.Arguments = string(fragment.args)
			events = append(events, Event{Kind: KindToolRequest, Tool: call})
		}
	default:
		if len(fragments) > 0 {
			return streamError(events, usage, fmt.Errorf("harness: incomplete tool request in provider stream"))
		}
		next, kept := appendBoundedEvent(events, Event{Kind: KindFinal, FinishReason: finish})
		if !kept {
			return streamError(events, usage, ErrStreamEventOverflow)
		}
		events = next
	}
	// The terminal event is the last one in both success branches: the last
	// assembled tool request or the final. It carries the provider's usage
	// report for the turn when the gateway sent one (#738); a stream the
	// provider counted is the request the run's telemetry accounts for.
	if usage != nil {
		events[len(events)-1].Usage = usage
	}
	return events, nil
}

// appendBoundedEvent appends one retained event unless the per-response event
// bound is already reached, in which case it appends nothing and reports
// false. Only the events a turn keeps as data go through it: the terminal
// error streamError appends is deliberately exempt, because a turn that fails
// at the cap must still be reportable.
func appendBoundedEvent(events []Event, event Event) ([]Event, bool) {
	if len(events) >= maxStreamEvents {
		return events, false
	}
	return append(events, event), true
}

// streamError appends exactly one terminal error event to whatever events
// already arrived. Deltas already emitted remain valid data; the turn failed
// and is reported as such, never masked as a final. The usage the provider
// reported before the failure rides the error event when it did (#738): a
// stream the provider already counted is not free. This one append is
// exempt from maxStreamEvents on purpose: it is the report, not data.
func streamError(events []Event, usage *Usage, err error) ([]Event, error) {
	event := Event{Kind: KindError, Err: err}
	if usage != nil {
		event.Usage = usage
	}
	return append(events, event), nil
}

func sortedFragmentIndexes(fragments map[int]*toolFragment) []int {
	indexes := make([]int, 0, len(fragments))
	for index := range fragments {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	return indexes
}
