package harness

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

// A LaneProfile binding names its model "provider/model". The provider half is
// client-side routing — the OpenCode transport resolves it locally — so the
// gateway-visible id is the model half, and a gateway that lists bare names
// serves the same binding (#812).
func TestGatewayModelIDDerivesGatewayName(t *testing.T) {
	tests := []struct {
		name  string
		model string
		want  string
	}{
		{name: "the provider half is dropped", model: "litellm/qwen3.8-flash-next", want: "qwen3.8-flash-next"},
		{name: "only the first segment is the provider", model: "litellm/deep/nested", want: "deep/nested"},
		{name: "a bare name is served verbatim", model: "qwen3.8-flash-next", want: "qwen3.8-flash-next"},
		{name: "an empty leading half is kept", model: "/model", want: "/model"},
		{name: "an empty trailing half is kept", model: "litellm/", want: "litellm/"},
		{name: "the empty binding is the empty binding", model: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := gatewayModelID(tt.model); got != tt.want {
				t.Fatalf("gatewayModelID(%q) = %q, want %q", tt.model, got, tt.want)
			}
		})
	}
}

// The OpenAI-compatible request carries the model half, while the harness's
// own accounting keeps the configured binding — the binding identity a run
// reports is never rewritten to a wire detail.
func TestGatewayChatRequestSendsModelHalf(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(gatewayResponse{body: sse(finishChunk("stop"))})
	defer cleanup()

	events, err := gateway.StreamChat(context.Background(), ChatRequest{
		Model:    "litellm/qwen3.8-flash-next",
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("StreamChat: %v", err)
	}
	if len(events) == 0 || events[len(events)-1].Kind != KindFinal {
		t.Fatalf("events = %+v, want a final terminal", events)
	}
	calls := mocker.calls
	if len(calls) != 1 {
		t.Fatalf("gateway requests = %d, want 1", len(calls))
	}
	if calls[0].model != "qwen3.8-flash-next" {
		t.Fatalf("wire model = %q, want the model half of the bound name", calls[0].model)
	}
}

// The Messages request carries the same model half, so a prefixed binding
// behaves identically on both native transports.
func TestAnthropicChatRequestSendsModelHalf(t *testing.T) {
	transport, requests, cleanup := anthropicFor(t, anthropicSSE(
		anthropicPayload(t, map[string]any{"type": "message_start", "message": map[string]any{"id": "msg_1"}}),
		anthropicPayload(t, map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn"}}),
		anthropicPayload(t, map[string]any{"type": "message_stop"}),
	))
	defer cleanup()

	if _, err := transport.StreamChat(context.Background(), ChatRequest{
		Model:    "litellm-anthropic/MiniMax-M3.1-Flash",
		Messages: []Message{{Role: "user", Content: "hi"}},
	}); err != nil {
		t.Fatalf("StreamChat: %v", err)
	}
	if len(*requests) != 1 {
		t.Fatalf("gateway requests = %d, want 1", len(*requests))
	}
	if body := string((*requests)[0]); !strings.Contains(body, `"model":"MiniMax-M3.1-Flash"`) {
		t.Fatalf("wire body carries no stripped model id: %s", body)
	}
}

// A rejected request names the exact gateway model id it was answered for: the
// id is harness configuration, never provider body text, so a startup probe's
// 403 reads as which id the gateway refused (#812). The provider body stays
// redacted.
func TestGatewayStatusErrorNamesRejectedModel(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{
		status:  http.StatusForbidden,
		errBody: `{"error":{"message":"The requested model is not available for API key","type":"key_model_access_denied"}}`,
	})
	defer cleanup()

	events, err := gateway.StreamChat(context.Background(), ChatRequest{
		Model:    "litellm/qwen3.8-flash-next",
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("StreamChat: %v", err)
	}
	terminal := events[len(events)-1]
	if terminal.Kind != KindError {
		t.Fatalf("terminal = %+v, want an error", terminal)
	}
	var gatewayErr *GatewayError
	if !errors.As(terminal.Err, &gatewayErr) {
		t.Fatalf("err = %v (%T), want a classified gateway error", terminal.Err, terminal.Err)
	}
	if gatewayErr.Model != "qwen3.8-flash-next" {
		t.Fatalf("error model = %q, want the gateway-visible id", gatewayErr.Model)
	}
	reason := terminal.Err.Error()
	if !strings.Contains(reason, `for model "qwen3.8-flash-next"`) {
		t.Fatalf("reason = %q, want the rejected model id named", reason)
	}
	for _, secret := range []string{"API key", "key_model_access_denied", "not available"} {
		if strings.Contains(reason, secret) {
			t.Fatalf("reason %q leaked provider body text", reason)
		}
	}
}

// A gateway that lists bare names answers the model-metadata read for a
// prefixed binding: the probe verifies the deployment's budget instead of
// failing on the routing half (#812).
func TestModelInputLimitMatchesModelHalf(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(
		`{"data":[{"model_name":"glm-5.3-flash-hyper","model_info":{"max_input_tokens":131072}}]}`))
	defer cleanup()

	limit, err := gateway.ModelInputLimit(context.Background(), "litellm/glm-5.3-flash-hyper")
	if err != nil {
		t.Fatalf("ModelInputLimit: %v", err)
	}
	if limit != 131072 {
		t.Fatalf("limit = %d, want the gateway's advertised limit", limit)
	}
}

// A gateway that really lists the full bound name still matches verbatim: the
// stripped half is an additional accepted spelling, never a replacement.
func TestModelInputLimitStillMatchesFullBinding(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(
		`{"data":[{"model_name":"litellm/qwen3.8-flash-next","model_info":{"max_input_tokens":262144}}]}`))
	defer cleanup()

	limit, err := gateway.ModelInputLimit(context.Background(), "litellm/qwen3.8-flash-next")
	if err != nil {
		t.Fatalf("ModelInputLimit: %v", err)
	}
	if limit != 262144 {
		t.Fatalf("limit = %d, want the gateway's advertised limit", limit)
	}
}

// When the gateway lists the same model under both spellings, the verified
// limit is the entry the request wire actually addresses — the gateway-visible
// half — never whichever entry the gateway happened to list first.
func TestModelInputLimitPrefersGatewayVisibleEntry(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "full spelling listed first",
			body: `{"data":[{"model_name":"litellm/qwen3.8-flash-next","model_info":{"max_input_tokens":262144}},{"model_name":"qwen3.8-flash-next","model_info":{"max_input_tokens":131072}}]}`,
			want: 131072,
		},
		{
			name: "bare spelling listed first",
			body: `{"data":[{"model_name":"qwen3.8-flash-next","model_info":{"max_input_tokens":131072}},{"model_name":"litellm/qwen3.8-flash-next","model_info":{"max_input_tokens":262144}}]}`,
			want: 131072,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(tt.body))
			defer cleanup()

			limit, err := gateway.ModelInputLimit(context.Background(), "litellm/qwen3.8-flash-next")
			if err != nil {
				t.Fatalf("ModelInputLimit: %v", err)
			}
			if limit != tt.want {
				t.Fatalf("limit = %d, want the gateway-visible entry's limit %d", limit, tt.want)
			}
		})
	}
}

// A model-metadata read the gateway refuses names the gateway-visible id it
// was answered for, and keeps the provider body redacted.
func TestModelInputLimitStatusErrorNamesGatewayModel(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"message":"model not available for API key","type":"key_model_access_denied"}}`))
	})
	defer cleanup()

	_, err := gateway.ModelInputLimit(context.Background(), "litellm/qwen3.8-flash-next")
	var gatewayErr *GatewayError
	if !errors.As(err, &gatewayErr) {
		t.Fatalf("err = %v (%T), want a classified gateway error", err, err)
	}
	if gatewayErr.Status != http.StatusForbidden || gatewayErr.Model != "qwen3.8-flash-next" {
		t.Fatalf("error = %+v, want a 403 naming the gateway-visible id", *gatewayErr)
	}
	if !strings.Contains(err.Error(), `for model "qwen3.8-flash-next"`) {
		t.Fatalf("reason = %q, want the rejected model id named", err)
	}
	if strings.Contains(err.Error(), "API key") {
		t.Fatalf("reason %q leaked provider body text", err)
	}
}

// A missing entry names the binding as configured: the operator's LaneProfile
// is what the message has to identify.
func TestModelInputLimitUnknownModelNamesBinding(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(
		`{"data":[{"model_name":"other/model","max_input_tokens":1024}]}`))
	defer cleanup()

	_, err := gateway.ModelInputLimit(context.Background(), "litellm/qwen3.8-flash-next")
	if err == nil || !strings.Contains(err.Error(), `"litellm/qwen3.8-flash-next"`) {
		t.Fatalf("err = %v, want the configured binding named", err)
	}
}
