package harness

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// --- OpenAI-compatible stream usage report (#738) --------------------------
//
// The gateway asks every streamed turn for the provider's usage report and
// rides it on the turn's terminal event. These tests pin the request shape
// (stream_options.include_usage), that a reported usage lands on the final or
// tool terminal, that a stream the provider did not count records an explicit
// absence, and that a usage the provider reported before an in-stream failure
// is not discarded as free.

// TestGatewayStreamRequestsUsageAndCarriesItOnTheFinal pins the happy path:
// the request asks for usage, and the provider's trailing usage report lands
// on the turn's final terminal.
func TestGatewayStreamRequestsUsageAndCarriesItOnTheFinal(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(gatewayResponse{body: sse(
		contentChunk("answer"),
		finishChunk("stop"),
		usageChunk(120, 30, 80), // prompt, completion, cached
	)})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	// The request must ask for the usage report.
	if len(mocker.calls) != 1 {
		t.Fatalf("gateway calls = %d, want 1", len(mocker.calls))
	}
	request := decodeRequest(t, mocker.calls[0].rawBody)
	if request.StreamOptions == nil || !request.StreamOptions.IncludeUsage {
		t.Fatalf("request stream_options = %+v, want include_usage true", request.StreamOptions)
	}
	// The provider's report lands on the final terminal.
	last := events[len(events)-1]
	if last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if last.Usage == nil {
		t.Fatal("the final terminal carries no usage, but the provider reported some")
	}
	if last.Usage.InputTokens != 120 || last.Usage.OutputTokens != 30 || last.Usage.CacheReadTokens != 80 {
		t.Fatalf("terminal usage = %+v, want the provider's report", last.Usage)
	}
}

// TestGatewayStreamWithoutUsageRecordsNil pins the explicit-absence rule: a
// stream the provider did not count carries a nil usage on its terminal, never
// an invented zero.
func TestGatewayStreamWithoutUsageRecordsNil(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(
		contentChunk("answer"),
		finishChunk("stop"),
	)})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if last.Usage != nil {
		t.Fatalf("terminal usage = %+v, want nil (the provider reported none)", last.Usage)
	}
}

// TestGatewayStreamCarriesUsageOnAnInStreamError pins that a usage the
// provider reported before an in-stream failure is not discarded: the error
// terminal rides it, because a stream the provider already counted is not
// free.
func TestGatewayStreamCarriesUsageOnAnInStreamError(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(
		contentChunk("partial"),
		usageChunk(90, 4, 0),
		errorChunk("boom", "provider_error"),
	)})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error", last)
	}
	if last.Usage == nil {
		t.Fatal("the error terminal carries no usage, but the provider reported some before the failure")
	}
	if last.Usage.InputTokens != 90 || last.Usage.OutputTokens != 4 {
		t.Fatalf("error-terminal usage = %+v, want the provider's pre-failure report", last.Usage)
	}
}

// TestGatewayStreamRetriesWithoutUsageWhenRejected pins the compatibility
// seam (#738): a gateway that rejects the request shape because of the
// usage request's own field gets one retry without the field, the turn
// succeeds, the transport remembers, and every later request on it omits
// stream_options. Telemetry must not turn a healthy deployment away.
func TestGatewayStreamRetriesWithoutUsageWhenRejected(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusBadRequest, errBody: `{"error":{"message":"unrecognized request argument supplied: stream_options","type":"invalid_request_error"}}`},
		gatewayResponse{body: sse(contentChunk("answer"), finishChunk("stop"))},
		gatewayResponse{body: sse(contentChunk("again"), finishChunk("stop"))},
	)
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final after the retry without the field", last)
	}
	if got := mocker.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2 (rejected attempt + retry without the field)", got)
	}
	if first := decodeRequest(t, mocker.calls[0].rawBody); first.StreamOptions == nil {
		t.Fatal("the first request carried no usage request; it must ask before it can be rejected")
	}
	if retry := decodeRequest(t, mocker.calls[1].rawBody); retry.StreamOptions != nil {
		t.Fatalf("retry stream_options = %+v, want the field dropped", retry.StreamOptions)
	}
	if last.Usage != nil {
		t.Fatalf("terminal usage = %+v, want nil: the accepted request asked for no usage", last.Usage)
	}
	// The transport remembers: the next turn does not ask again.
	if _, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}}); err != nil {
		t.Fatal(err)
	}
	if got := mocker.requestCount(); got != 3 {
		t.Fatalf("gateway requests = %d, want 3", got)
	}
	if later := decodeRequest(t, mocker.calls[2].rawBody); later.StreamOptions != nil {
		t.Fatalf("later request stream_options = %+v, want the field remembered as unsupported", later.StreamOptions)
	}
}

// TestGatewayStreamOtherRejectionsAreNotRetried pins that a request-shape
// rejection that does not name the usage field behaves exactly as before:
// one attempt, one error, no retry theater for telemetry's sake.
func TestGatewayStreamOtherRejectionsAreNotRetried(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusBadRequest, errBody: `{"error":{"message":"unrecognized request argument supplied: messages","type":"invalid_request_error"}}`},
	)
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error", last)
	}
	if got := mocker.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1 (a rejection that does not name the field is not retried)", got)
	}
}

// TestGatewayStreamCarriesUsageOnTheToolTerminal pins the tool-request
// terminal: the usage the provider reported rides the last assembled tool
// request, the event the session records.
func TestGatewayStreamCarriesUsageOnTheToolTerminal(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(
		toolCallChunk(0, "call-1", "shell", "{}"),
		finishChunk("tool_calls"),
		usageChunk(70, 9, 0),
	)})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindToolRequest {
		t.Fatalf("terminal event = %+v, want tool request", last)
	}
	if last.Usage == nil || last.Usage.InputTokens != 70 || last.Usage.OutputTokens != 9 {
		t.Fatalf("terminal usage = %+v, want the provider's report on the tool terminal", last.Usage)
	}
}

// TestGatewayStreamLaterUsageChunkWins pins the documented contract: the
// gateway reports the turn's usage once, on a trailing chunk, and a later
// chunk replaces an earlier one.
func TestGatewayStreamLaterUsageChunkWins(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(
		contentChunk("answer"),
		finishChunk("stop"),
		usageChunk(10, 1, 0),
		usageChunk(120, 30, 0),
	)})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindFinal || last.Usage == nil || last.Usage.InputTokens != 120 || last.Usage.OutputTokens != 30 {
		t.Fatalf("terminal = %+v usage = %+v, want final with the later report", last, last.Usage)
	}
}

// errorChunk renders one OpenAI in-stream error object. The message is
// provider-controlled body data the normalizer redacts.
func errorChunk(message, typ string) string {
	return `{"error":{"message":` + jsonString(message) + `,"type":` + jsonString(typ) + `}}`
}

func jsonString(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}

// TestGatewayStreamRetriesWithoutUsageWhenAnyStatusNamesTheField pins that
// the fallback is not a status-class gate: a gateway that rejects unknown
// fields with 5xx gets the same one retry without the field as one that
// answers 400.
func TestGatewayStreamRetriesWithoutUsageWhenAnyStatusNamesTheField(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusBadGateway, errBody: `{"error":{"message":"unsupported request argument stream_options","type":"api_error"}}`},
		gatewayResponse{body: sse(contentChunk("answer"), finishChunk("stop"))},
	)
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final after the retry without the field", last)
	}
	if got := mocker.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2", got)
	}
	if retry := decodeRequest(t, mocker.calls[1].rawBody); retry.StreamOptions != nil {
		t.Fatalf("retry stream_options = %+v, want the field dropped", retry.StreamOptions)
	}
}

// TestGatewayStreamUnreadableUsageIsAnAbsenceNotAFailure pins the tolerance
// rule (#738): a provider or proxy that reports a fractional token count has
// not reported an exact count, so the report is an absence and the turn
// succeeds — telemetry must never fail a stream the provider counted.
func TestGatewayStreamUnreadableUsageIsAnAbsenceNotAFailure(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(
		contentChunk("answer"),
		finishChunk("stop"),
		`{"usage":{"prompt_tokens":120.5,"completion_tokens":30,"prompt_tokens_details":{"cached_tokens":80}}}`,
	)})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final (a fractional usage report must not fail the turn)", last)
	}
	if last.Usage != nil {
		t.Fatalf("terminal usage = %+v, want nil (a fractional count is not an exact report)", last.Usage)
	}
}
