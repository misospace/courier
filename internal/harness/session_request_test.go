package harness

import (
	"context"
	"net/http"
	"strconv"
	"testing"
)

// --- session model-request observations (#738) -----------------------------
//
// The session's request sink receives one observation per model request the
// session's turns send, because the gateway terminalizes every failed attempt
// (a 5xx, a dropped connection, an in-stream error) as a KindError event and
// only a request-build failure — the harness's own bug, not a sent request —
// is left unobserved. A transport-level fallback is observed too: the one
// request a usage-field rejection forces before the attempt's own is recorded
// first, under the attempt's own index. These tests pin that the session
// reports every request it made, with the attempt index, the harness's
// estimated prompt size, and the provider's usage from the terminal when it
// reported one.

// recordingRequestSink captures the request observations a session delivers.
type recordingRequestSink struct {
	observations []RequestObservation
}

func (s *recordingRequestSink) ModelRequestObserved(observation RequestObservation) {
	s.observations = append(s.observations, observation)
}

// TestSessionRequestSinkRecordsEveryAttempt pins that a turn whose early
// attempts fail and then succeeds reports one observation per attempt it sent:
// the failed attempts as non-OK with no usage, the successful attempt as OK
// with the provider's usage, each with its attempt index and the shared
// prompt-size estimate.
func TestSessionRequestSinkRecordsEveryAttempt(t *testing.T) {
	shrinkRetryDelays(t)
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusServiceUnavailable},
		gatewayResponse{status: http.StatusBadGateway},
		// The usage chunk trails the finish chunk, the ordering the
		// OpenAI-compatible stream uses.
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"), usageChunk(40, 12, 6))},
	)
	defer cleanup()
	sink := &recordingRequestSink{}
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil, WithRequestSink(sink))
	if err != nil {
		t.Fatal(err)
	}
	// A transcript the estimate can measure: the prompt size is the transcript
	// plus tool declarations, the same account the compaction boundary uses.
	session.Append(Message{Role: "user", Content: "resolve the issue"})
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if got := mocker.requestCount(); got != 3 {
		t.Fatalf("gateway requests = %d, want 3", got)
	}
	if len(sink.observations) != 3 {
		t.Fatalf("observations = %d, want 3 (one per attempt, retries included)", len(sink.observations))
	}
	for i, obs := range sink.observations {
		if obs.Class != RequestWork {
			t.Fatalf("observation %d class = %s, want work", i, obs.Class)
		}
		if obs.Role != RoleCoordinator || obs.Model != "test/coordinator" {
			t.Fatalf("observation %d attribution = %s/%s, want the session's role/binding", i, obs.Role, obs.Model)
		}
		if obs.Attempt != i {
			t.Fatalf("observation %d attempt = %d, want %d", i, obs.Attempt, i)
		}
		if i < 2 {
			if obs.OK {
				t.Fatalf("observation %d (a failed attempt) is OK; it must not be", i)
			}
			if obs.Usage != nil {
				t.Fatalf("observation %d (a failed attempt) carried usage %v; a non-2xx reports none", i, obs.Usage)
			}
		} else {
			if !obs.OK {
				t.Fatalf("observation %d (the successful attempt) is not OK", i)
			}
			if obs.Usage == nil || obs.Usage.InputTokens != 40 || obs.Usage.OutputTokens != 12 || obs.Usage.CacheReadTokens != 6 {
				t.Fatalf("observation %d usage = %+v, want the provider's report", i, obs.Usage)
			}
		}
	}
	// The prompt-size estimate is the same for every attempt: it is measured
	// once per call, before any retry.
	if sink.observations[0].EstimatedPromptTokens == 0 {
		t.Fatal("the estimate is zero; a session with a transcript must report a nonzero prompt size")
	}
	if sink.observations[0].EstimatedPromptTokens != sink.observations[2].EstimatedPromptTokens {
		t.Fatalf("estimate changed between attempts: %d vs %d", sink.observations[0].EstimatedPromptTokens, sink.observations[2].EstimatedPromptTokens)
	}
}

// TestSessionRequestSinkRecordsTheRejectedUsageRequest pins the total
// request accounting across the usage-field seam (#738): the request the
// provider rejected is part of the run's model spend, so the session records
// it — first, in the order the provider saw it, as a non-OK request with an
// explicit usage absence and the attempt's shared estimate, under the
// attempt's own index, because the fallback retry is the transport's, not a
// new session attempt.
func TestSessionRequestSinkRecordsTheRejectedUsageRequest(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusBadRequest, errBody: `{"error":{"message":"unrecognized request argument supplied: stream_options","type":"invalid_request_error"}}`},
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))},
	)
	defer cleanup()
	sink := &recordingRequestSink{}
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil, WithRequestSink(sink))
	if err != nil {
		t.Fatal(err)
	}
	session.Append(Message{Role: "user", Content: "resolve the issue"})
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if got := mocker.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2 (the rejected ask and the retry)", got)
	}
	if len(sink.observations) != 2 {
		t.Fatalf("observations = %d, want 2 (one per provider request the turn sent)", len(sink.observations))
	}
	for i, obs := range sink.observations {
		if obs.Class != RequestWork {
			t.Fatalf("observation %d class = %s, want work", i, obs.Class)
		}
		if obs.Role != RoleCoordinator || obs.Model != "test/coordinator" {
			t.Fatalf("observation %d attribution = %s/%s, want the session's role/binding", i, obs.Role, obs.Model)
		}
		if obs.Attempt != 0 {
			t.Fatalf("observation %d attempt = %d, want 0 (the fallback retry is the transport's, not a new attempt)", i, obs.Attempt)
		}
		if obs.EstimatedPromptTokens == 0 {
			t.Fatalf("observation %d estimate = 0, want the transcript's shared nonzero estimate", i)
		}
		if obs.Usage != nil {
			t.Fatalf("observation %d carried usage %v; the seam's requests report none", i, obs.Usage)
		}
	}
	if obs := sink.observations[0]; obs.OK {
		t.Fatalf("observation 0 (the rejected request) is OK; it must not be: %+v", obs)
	}
	if obs := sink.observations[1]; !obs.OK {
		t.Fatalf("observation 1 (the accepted retry) is not OK: %+v", obs)
	}
	if sink.observations[0].EstimatedPromptTokens != sink.observations[1].EstimatedPromptTokens {
		t.Fatalf("estimate changed across the seam: %d vs %d", sink.observations[0].EstimatedPromptTokens, sink.observations[1].EstimatedPromptTokens)
	}
}

// TestSessionRequestSinkReportsFailedAttemptsWhenRetriesExhaust pins that a
// turn whose every attempt fails reports all of them — the retries are part
// of the run's model spend, and their absence of usage is an explicit
// absence, not a zero.
func TestSessionRequestSinkReportsFailedAttemptsWhenRetriesExhaust(t *testing.T) {
	shrinkRetryDelays(t)
	script := make([]gatewayResponse, maxStreamRetries+1)
	for i := range script {
		script[i] = gatewayResponse{status: http.StatusServiceUnavailable}
	}
	gateway, _, cleanup := gatewayFor(script...)
	defer cleanup()
	sink := &recordingRequestSink{}
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil, WithRequestSink(sink))
	if err != nil {
		t.Fatal(err)
	}
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error after exhausting retries", last)
	}
	// maxStreamRetries+1 attempts: the initial one plus maxStreamRetries retries.
	if len(sink.observations) != maxStreamRetries+1 {
		t.Fatalf("observations = %d, want %d (every attempt, retries included)", len(sink.observations), maxStreamRetries+1)
	}
	for i, obs := range sink.observations {
		if obs.OK {
			t.Fatalf("observation %d (every attempt failed) is OK; it must not be", i)
		}
		if obs.Usage != nil {
			t.Fatalf("observation %d carried usage; a 5xx reports none", i)
		}
	}
}

// TestSessionRequestSinkFailureCannotChangeTheTurn pins the best-effort
// guarantee (#738): a request sink that panics is isolated, and the turn
// returns exactly the events it would have returned without telemetry.
func TestSessionRequestSinkFailureCannotChangeTheTurn(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))})
	defer cleanup()
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil, WithRequestSink(panickingSink{}))
	if err != nil {
		t.Fatal(err)
	}
	session.Append(Message{Role: "user", Content: "resolve the issue"})
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final (a failing sink must not change the turn)", last)
	}
	if got := mocker.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
}

// panickingSink is a request sink whose delivery always fails hard.
type panickingSink struct{}

func (panickingSink) ModelRequestObserved(RequestObservation) {
	panic("sink is broken")
}

// usageChunk renders one OpenAI usage chunk: the provider's own report that
// the stream terminal carries when the gateway includes one.
func usageChunk(prompt, completion, cached int) string {
	return `{"usage":{"prompt_tokens":` + strconv.Itoa(prompt) + `,"completion_tokens":` + strconv.Itoa(completion) + `,"prompt_tokens_details":{"cached_tokens":` + strconv.Itoa(cached) + `}}}`
}
