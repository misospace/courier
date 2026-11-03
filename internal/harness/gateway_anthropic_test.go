package harness

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// anthropicSSE renders one SSE body from data payloads, one line each, the
// shape the Messages stream uses.
func anthropicSSE(payloads ...string) string {
	var b strings.Builder
	for _, payload := range payloads {
		b.WriteString("event: placeholder\n")
		b.WriteString("data: " + payload + "\n\n")
	}
	return b.String()
}

func anthropicPayload(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// anthropicFor builds a transport against a stub gateway streaming body, and
// returns the transport with the captured request bodies.
func anthropicFor(t *testing.T, body string) (*AnthropicTransport, *[][]byte, func()) {
	t.Helper()
	var requests [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := readAllLimited(r.Body, maxStreamContentBytes)
		requests = append(requests, payload)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(body))
	}))
	transport := &AnthropicTransport{BaseURL: server.URL, APIKey: "test-key"}
	return transport, &requests, server.Close
}

func readAllLimited(body interface{ Read([]byte) (int, error) }, limit int64) ([]byte, error) {
	return readAllBounded(body, limit)
}

func readAllBounded(body interface{ Read([]byte) (int, error) }, limit int64) ([]byte, error) {
	out := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	var total int64
	for {
		n, err := body.Read(buf)
		total += int64(n)
		if total > limit {
			return nil, errors.New("body over bound")
		}
		out = append(out, buf[:n]...)
		if err != nil {
			return out, nil
		}
	}
}

// A full tool-use turn: thinking with its signature, streaming text, a tool
// request assembled from input-JSON fragments, and a tool_use stop. Thinking
// never appears as a delta; the turn's blocks carry it verbatim, signature
// included.
func TestAnthropicStreamNormalizesThinkingTextAndToolUse(t *testing.T) {
	transport, _, cleanup := anthropicFor(t, anthropicSSE(
		anthropicPayload(t, map[string]any{"type": "message_start", "message": map[string]any{"id": "msg_1"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "thinking", "thinking": ""}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "thinking_delta", "thinking": "ponder"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "signature_delta", "signature": "sig-1"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 1, "content_block": map[string]any{"type": "text", "text": ""}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 1, "delta": map[string]any{"type": "text_delta", "text": "run"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 1, "delta": map[string]any{"type": "text_delta", "text": "ning"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 1}),
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 2, "content_block": map[string]any{"type": "tool_use", "id": "toolu-1", "name": "shell"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 2, "delta": map[string]any{"type": "input_json_delta", "partial_json": `{"command":"ls`}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 2, "delta": map[string]any{"type": "input_json_delta", "partial_json": ` -la"}`}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 2}),
		anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "tool_use"}}),
		anthropicPayload(t, map[string]any{"type": "message_stop"}),
	))
	defer cleanup()
	events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "MiniMax-M3.1-Flash", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	var calls []ToolCall
	var terminal Event
	for _, event := range events {
		switch event.Kind {
		case KindDelta:
			text.WriteString(event.Text)
		case KindToolRequest:
			calls = append(calls, event.Tool)
			terminal = event
		default:
			t.Fatalf("unexpected event kind %q", event.Kind)
		}
	}
	if text.String() != "running" {
		t.Fatalf("delta text = %q; thinking must never surface as assistant text", text.String())
	}
	if len(calls) != 1 || calls[0].ID != "toolu-1" || calls[0].Name != "shell" || calls[0].Arguments != `{"command":"ls -la"}` {
		t.Fatalf("assembled tool call = %+v", calls)
	}
	if len(terminal.Blocks) != 3 {
		t.Fatalf("terminal blocks = %d; want thinking, text, tool_use", len(terminal.Blocks))
	}
	var thinking struct {
		Type      string `json:"type"`
		Thinking  string `json:"thinking"`
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(terminal.Blocks[0], &thinking); err != nil {
		t.Fatal(err)
	}
	if thinking.Type != "thinking" || thinking.Thinking != "ponder" || thinking.Signature != "sig-1" {
		t.Fatalf("thinking block = %+v; signature must ride beside the reasoning", thinking)
	}
	var toolUse struct {
		Type  string          `json:"type"`
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(terminal.Blocks[2], &toolUse); err != nil {
		t.Fatal(err)
	}
	if toolUse.Type != "tool_use" || toolUse.ID != "toolu-1" || string(toolUse.Input) != `{"command":"ls -la"}` {
		t.Fatalf("tool_use block = %+v", toolUse)
	}
}

// The continuation round-trip: the next request re-renders the assistant
// turn's provider-native blocks verbatim — thinking signature included —
// groups the tool results into one user message of tool_result blocks, keeps
// the leading system material top-level, and carries the output reservation
// as max_tokens.
func TestAnthropicContinuationRerendersBlocksVerbatim(t *testing.T) {
	first := anthropicSSE(
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "thinking"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "thinking_delta", "thinking": "ponder"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "signature_delta", "signature": "sig-1"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 1, "content_block": map[string]any{"type": "tool_use", "id": "toolu-1", "name": "shell"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 1, "delta": map[string]any{"type": "input_json_delta", "partial_json": `{"command":"ls"}`}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 1}),
		anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "tool_use"}}),
		anthropicPayload(t, map[string]any{"type": "message_stop"}),
	)
	second := anthropicSSE(
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": "done"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
		anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn"}}),
		anthropicPayload(t, map[string]any{"type": "message_stop"}),
	)
	responses := []string{first, second}
	var requests [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := readAllLimited(r.Body, maxStreamContentBytes)
		requests = append(requests, payload)
		_, _ = w.Write([]byte(responses[len(requests)-1]))
	}))
	defer server.Close()
	transport := &AnthropicTransport{BaseURL: server.URL}

	turnOne, err := transport.StreamChat(context.Background(), ChatRequest{
		Model:     "MiniMax-M3.1-Flash",
		MaxTokens: 1234,
		Messages:  []Message{{Role: "system", Content: "objective"}, {Role: "user", Content: "go"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal := turnOne[len(turnOne)-1]
	transcript := []Message{
		{Role: "system", Content: "objective"},
		{Role: "user", Content: "go"},
		AssistantMessageWithBlocks("", []ToolCall{terminal.Tool}, terminal.Blocks),
		ToolResultMessage(ToolResult{ToolCallID: terminal.Tool.ID, Name: "shell", Content: "file listing"}),
	}
	turnTwo, err := transport.StreamChat(context.Background(), ChatRequest{Model: "MiniMax-M3.1-Flash", MaxTokens: 1234, Messages: transcript})
	if err != nil {
		t.Fatal(err)
	}
	if last := turnTwo[len(turnTwo)-1]; last.Kind != KindFinal || last.FinishReason != "stop" {
		t.Fatalf("second turn terminal = %+v", last)
	}
	if len(requests) != 2 {
		t.Fatalf("captured %d requests", len(requests))
	}
	var wire struct {
		Model     string `json:"model"`
		MaxTokens int    `json:"max_tokens"`
		System    string `json:"system"`
		Messages  []struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(requests[1], &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Model != "MiniMax-M3.1-Flash" || wire.MaxTokens != 1234 {
		t.Fatalf("request header = model %q max_tokens %d", wire.Model, wire.MaxTokens)
	}
	if wire.System != "objective" {
		t.Fatalf("system = %q; leading system material must stay top-level", wire.System)
	}
	// The leading system message is top-level system, so the wire carries
	// three messages: user, assistant, grouped tool results.
	if len(wire.Messages) != 3 {
		t.Fatalf("messages = %d; want user, assistant, grouped tool results", len(wire.Messages))
	}
	if wire.Messages[0].Role != "user" || wire.Messages[2].Role != "user" {
		t.Fatalf("roles = %q/%q", wire.Messages[0].Role, wire.Messages[2].Role)
	}
	// The assistant message's blocks re-render verbatim: the thinking block
	// keeps its signature, the tool_use keeps its input object.
	assistant := wire.Messages[1]
	if assistant.Role != "assistant" || len(assistant.Content) != 2 {
		t.Fatalf("assistant message = %+v", assistant)
	}
	thinkingJSON, _ := json.Marshal(assistant.Content[0])
	var thinking map[string]any
	if err := json.Unmarshal(thinkingJSON, &thinking); err != nil {
		t.Fatal(err)
	}
	if thinking["type"] != "thinking" || thinking["thinking"] != "ponder" || thinking["signature"] != "sig-1" {
		t.Fatalf("re-rendered thinking = %s", thinkingJSON)
	}
	toolUseJSON, _ := json.Marshal(assistant.Content[1])
	var toolUse map[string]any
	if err := json.Unmarshal(toolUseJSON, &toolUse); err != nil {
		t.Fatal(err)
	}
	if toolUse["type"] != "tool_use" || toolUse["id"] != "toolu-1" {
		t.Fatalf("re-rendered tool_use = %s", toolUseJSON)
	}
	// The tool results group into one user message of tool_result blocks.
	if len(wire.Messages[2].Content) != 1 {
		t.Fatalf("tool-result message blocks = %d", len(wire.Messages[2].Content))
	}
	resultJSON, _ := json.Marshal(wire.Messages[2].Content[0])
	var result map[string]any
	if err := json.Unmarshal(resultJSON, &result); err != nil {
		t.Fatal(err)
	}
	if result["type"] != "tool_result" || result["tool_use_id"] != "toolu-1" {
		t.Fatalf("tool_result block = %s", resultJSON)
	}
}

// A final turn without tools: text deltas plus a terminal final whose blocks
// still carry the thinking verbatim.
func TestAnthropicStreamFinalCarriesBlocks(t *testing.T) {
	transport, _, cleanup := anthropicFor(t, anthropicSSE(
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "thinking"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "thinking_delta", "thinking": "hidden"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
		anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 1, "content_block": map[string]any{"type": "text"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 1, "delta": map[string]any{"type": "text_delta", "text": "answer"}}),
		anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 1}),
		anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn"}}),
		anthropicPayload(t, map[string]any{"type": "message_stop"}),
	))
	defer cleanup()
	events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindFinal || last.FinishReason != "stop" {
		t.Fatalf("terminal = %+v", last)
	}
	if len(last.Blocks) != 2 {
		t.Fatalf("final blocks = %d", len(last.Blocks))
	}
	if !strings.Contains(string(last.Blocks[0]), `"hidden"`) {
		t.Fatalf("thinking block = %s", last.Blocks[0])
	}
}

// In-stream and HTTP failures normalize into the same safe classes, never
// echoing provider bodies.
func TestAnthropicErrorClassification(t *testing.T) {
	t.Run("http 429 is transient and retryable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`))
		}))
		defer server.Close()
		transport := &AnthropicTransport{BaseURL: server.URL}
		events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		last := events[len(events)-1]
		if last.Kind != KindError {
			t.Fatalf("terminal = %+v", last)
		}
		var gatewayErr *GatewayError
		if !errors.As(last.Err, &gatewayErr) || !gatewayErr.Retryable {
			t.Fatalf("error = %v; want retryable gateway error", last.Err)
		}
	})
	t.Run("in-stream overloaded error is retryable and redacted", func(t *testing.T) {
		transport, _, cleanup := anthropicFor(t, anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "error", "error": map[string]any{"type": "overloaded_error", "message": "secret endpoint detail"}}),
		))
		defer cleanup()
		events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		last := events[len(events)-1]
		var gatewayErr *GatewayError
		if !errors.As(last.Err, &gatewayErr) || !gatewayErr.Retryable || gatewayErr.Availability != ProviderUnavailable {
			t.Fatalf("error = %v; want retryable unavailable", last.Err)
		}
		if strings.Contains(last.Err.Error(), "secret endpoint detail") {
			t.Fatalf("provider body leaked: %v", last.Err)
		}
	})
}

// Fail-closed wire handling: a stream without message_stop, an unhandled
// stop reason, and unknown event types are errors, never partial successes.
func TestAnthropicStreamFailsClosed(t *testing.T) {
	t.Run("no message_stop is truncated", func(t *testing.T) {
		transport, _, cleanup := anthropicFor(t, anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": "partial"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
		))
		defer cleanup()
		events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		if last := events[len(events)-1]; last.Kind != KindError || !errors.Is(last.Err, ErrStreamTruncated) {
			t.Fatalf("terminal = %+v", last)
		}
	})
	t.Run("pause_turn has no normalized meaning", func(t *testing.T) {
		transport, _, cleanup := anthropicFor(t, anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "pause_turn"}}),
			anthropicPayload(t, map[string]any{"type": "message_stop"}),
		))
		defer cleanup()
		events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		if last := events[len(events)-1]; last.Kind != KindError {
			t.Fatalf("terminal = %+v; a pause must never read as a completed turn", last)
		}
	})
	t.Run("unknown event type fails closed", func(t *testing.T) {
		transport, _, cleanup := anthropicFor(t, anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "carrier_pigeon"}),
		))
		defer cleanup()
		events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		if last := events[len(events)-1]; last.Kind != KindError {
			t.Fatalf("terminal = %+v", last)
		}
	})
	t.Run("tool_use without a valid input object is rejected", func(t *testing.T) {
		transport, _, cleanup := anthropicFor(t, anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "tool_use", "id": "t1", "name": "shell"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
			anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "tool_use"}}),
			anthropicPayload(t, map[string]any{"type": "message_stop"}),
		))
		defer cleanup()
		events, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		if last := events[len(events)-1]; last.Kind != KindError {
			t.Fatalf("terminal = %+v", last)
		}
	})
}

// The declared transport selects the implementation explicitly and fails
// closed on anything else; one build runs both lanes (acceptance 1 and 8).
func TestTransportSelection(t *testing.T) {
	if _, err := ParseWire("grpc"); err == nil {
		t.Fatal("unknown transport must fail closed")
	}
	if wire, err := ParseWire(""); err != nil || wire != WireOpenAI {
		t.Fatalf("empty transport = %q, %v; want the openai default", wire, err)
	}
	if _, err := NewModelTransport("anthropic", "", "", nil, ExhaustionSignals{}); err == nil {
		t.Fatal("a transport without an endpoint must fail closed")
	}
	anthropic, err := NewModelTransport("anthropic", "http://gw/v1", "", nil, ExhaustionSignals{})
	if err != nil {
		t.Fatal(err)
	}
	openai, err := NewModelTransport("openai", "http://gw/v1", "", nil, ExhaustionSignals{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := anthropic.(*AnthropicTransport); !ok {
		t.Fatalf("anthropic lane built %T", anthropic)
	}
	if _, ok := openai.(*Gateway); !ok {
		t.Fatalf("openai lane built %T", openai)
	}
}

// The Messages API requires max_tokens on every request: the caller's
// declared output reservation overrides, and an undeclared lane sends the
// documented request-shape floor.
func TestAnthropicMaxTokens(t *testing.T) {
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := readAllLimited(r.Body, maxStreamContentBytes)
		var body map[string]any
		_ = json.Unmarshal(payload, &body)
		bodies = append(bodies, body)
		_, _ = w.Write([]byte(anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn"}}),
			anthropicPayload(t, map[string]any{"type": "message_stop"}),
		)))
	}))
	defer server.Close()
	transport := &AnthropicTransport{BaseURL: server.URL}
	if _, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m", MaxTokens: 512, Messages: []Message{{Role: "user", Content: "hi"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}}); err != nil {
		t.Fatal(err)
	}
	if got := bodies[0]["max_tokens"].(float64); got != 512 {
		t.Fatalf("declared max_tokens = %v", got)
	}
	if got := bodies[1]["max_tokens"].(float64); got != anthropicMaxTokensFallback {
		t.Fatalf("fallback max_tokens = %v; want the documented request-shape floor", got)
	}
}

// Regression for the #716 review's merge blocker: a production session pins
// several leading system messages — goal plus harness framing, lane framing,
// skills index — and every one of them must reach the top-level system field
// in order, joined by the stable separator, none dropped, none duplicated
// into messages. The first non-system message ends the collection, and a
// mid-transcript system message (the compaction digest) still renders as a
// user block instead of system material.
func TestAnthropicLeadingSystemMessagesAllPreserved(t *testing.T) {
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := readAllLimited(r.Body, maxStreamContentBytes)
		bodies = append(bodies, payload)
		_, _ = w.Write([]byte(anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": "ok"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
			anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn"}}),
			anthropicPayload(t, map[string]any{"type": "message_stop"}),
		)))
	}))
	defer server.Close()
	transport := &AnthropicTransport{BaseURL: server.URL}
	transcript := []Message{
		{Role: "system", Content: "goal: land the fix"},
		{Role: "system", Content: "harness framing: bounded steps"},
		{Role: "system", Content: "skills index: shell, git"},
		{Role: "user", Content: "go"},
		{Role: "system", Content: "[compacted: digest]"},
		{Role: "user", Content: "continue"},
	}
	if _, err := transport.StreamChat(context.Background(), ChatRequest{Model: "MiniMax-M3.1-Flash", Messages: transcript}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 {
		t.Fatalf("captured %d requests", len(bodies))
	}
	var wire struct {
		System   string `json:"system"`
		Messages []struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(bodies[0], &wire); err != nil {
		t.Fatal(err)
	}
	want := "goal: land the fix\n\nharness framing: bounded steps\n\nskills index: shell, git"
	if wire.System != want {
		t.Fatalf("system = %q; want all three pinned messages in order joined by the separator", wire.System)
	}
	for i, message := range wire.Messages {
		if message.Role == "system" {
			t.Fatalf("message %d rendered as system role; the wire has no such role", i)
		}
		for _, block := range message.Content {
			blockJSON, _ := json.Marshal(block)
			for _, pinned := range []string{"goal: land the fix", "harness framing: bounded steps", "skills index: shell, git"} {
				if strings.Contains(string(blockJSON), pinned) {
					t.Fatalf("pinned system content %q duplicated into messages at %d: %s", pinned, i, blockJSON)
				}
			}
		}
	}
	if len(wire.Messages) != 3 {
		t.Fatalf("messages = %d; want user, digest user block, user", len(wire.Messages))
	}
	digestJSON, _ := json.Marshal(wire.Messages[1].Content[0])
	var digest map[string]any
	if err := json.Unmarshal(digestJSON, &digest); err != nil {
		t.Fatal(err)
	}
	if digest["type"] != "text" || digest["text"] != "[compacted: digest]" {
		t.Fatalf("mid-transcript system message = %s; want the compaction-digest user block", digestJSON)
	}
}

// Regression for #724: the pinned coordinator transcript is all system role,
// which rendered zero wire messages and marshaled "messages": null — a shape
// litellm's /v1/messages normalizer 500s on every first turn. The all-system
// transcript must produce a non-null messages array of exactly one user turn
// carrying the final pinned system message verbatim, with everything before
// it joined in the top-level system field.
func TestAnthropicAllSystemTranscriptPromotesFinalSystemToUserTurn(t *testing.T) {
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := readAllLimited(r.Body, maxStreamContentBytes)
		bodies = append(bodies, payload)
		_, _ = w.Write([]byte(anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": "ok"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
			anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn"}}),
			anthropicPayload(t, map[string]any{"type": "message_stop"}),
		)))
	}))
	defer server.Close()
	transport := &AnthropicTransport{BaseURL: server.URL}
	transcript := []Message{
		{Role: "system", Content: "goal: land the fix"},
		{Role: "system", Content: "harness framing: bounded steps"},
		{Role: "system", Content: "skills index: shell, git"},
	}
	if _, err := transport.StreamChat(context.Background(), ChatRequest{Model: "MiniMax-M3.1-Flash", Messages: transcript}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 {
		t.Fatalf("captured %d requests", len(bodies))
	}
	if strings.Contains(string(bodies[0]), `"messages":null`) {
		t.Fatalf("wire marshaled a null messages array: %s", bodies[0])
	}
	var wire struct {
		System   string `json:"system"`
		Messages []struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(bodies[0], &wire); err != nil {
		t.Fatal(err)
	}
	if want := "goal: land the fix\n\nharness framing: bounded steps"; wire.System != want {
		t.Fatalf("system = %q; want the first two pinned messages joined by the separator", wire.System)
	}
	if len(wire.Messages) != 1 {
		t.Fatalf("messages = %d entries; want exactly one user turn", len(wire.Messages))
	}
	if wire.Messages[0].Role != "user" {
		t.Fatalf("promoted turn role = %q; want user", wire.Messages[0].Role)
	}
	if len(wire.Messages[0].Content) != 1 {
		t.Fatalf("promoted turn carries %d blocks; want one text block", len(wire.Messages[0].Content))
	}
	blockJSON, _ := json.Marshal(wire.Messages[0].Content[0])
	var block map[string]any
	if err := json.Unmarshal(blockJSON, &block); err != nil {
		t.Fatal(err)
	}
	if block["type"] != "text" || block["text"] != "skills index: shell, git" {
		t.Fatalf("promoted turn block = %s; want the final pinned system message verbatim", blockJSON)
	}
}

// The single-pinned-message edge of #724: with one system message and
// nothing else, there is nothing left for the system field — the whole
// pinned message becomes the user turn and the system field is omitted.
func TestAnthropicSingleSystemMessageBecomesTheUserTurn(t *testing.T) {
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := readAllLimited(r.Body, maxStreamContentBytes)
		bodies = append(bodies, payload)
		_, _ = w.Write([]byte(anthropicSSE(
			anthropicPayload(t, map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": "ok"}}),
			anthropicPayload(t, map[string]any{"type": "content_block_stop", "index": 0}),
			anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn"}}),
			anthropicPayload(t, map[string]any{"type": "message_stop"}),
		)))
	}))
	defer server.Close()
	transport := &AnthropicTransport{BaseURL: server.URL}
	if _, err := transport.StreamChat(context.Background(), ChatRequest{Model: "MiniMax-M3.1-Flash", Messages: []Message{
		{Role: "system", Content: "goal: land the fix"},
	}}); err != nil {
		t.Fatal(err)
	}
	var wire struct {
		System   *string `json:"system"`
		Messages []struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(bodies[0], &wire); err != nil {
		t.Fatal(err)
	}
	if wire.System != nil {
		t.Fatalf("system = %q; want the field omitted for a single pinned message", *wire.System)
	}
	if len(wire.Messages) != 1 || wire.Messages[0].Role != "user" {
		t.Fatalf("messages = %+v; want exactly one user turn", wire.Messages)
	}
	blockJSON, _ := json.Marshal(wire.Messages[0].Content[0])
	if !strings.Contains(string(blockJSON), "goal: land the fix") {
		t.Fatalf("promoted turn block = %s; want the pinned message verbatim", blockJSON)
	}
}
