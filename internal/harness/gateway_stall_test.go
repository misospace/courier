package harness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestGatewayFirstByteStallEndsTurnAsTransportError pins the first-byte stall
// guard (#754): a gateway that accepts the connection and never writes a
// response header fails the turn as a transport error within the bound,
// instead of holding the turn until the run's context ends.
func TestGatewayFirstByteStallEndsTurnAsTransportError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Hold the accepted connection without ever writing a response
		// header. Hijacking keeps the server out of the picture: the
		// connection stays open, silent, and the client's problem.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		time.Sleep(5 * time.Second)
	})}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()

	gateway := &Gateway{
		BaseURL: "http://" + listener.Addr().String(),
		HTTP:    &http.Client{Transport: stallGuardedTransport(250 * time.Millisecond)},
	}
	start := time.Now()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed >= 5*time.Second {
		t.Fatalf("turn ran %v without ending at the stall bound", elapsed)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want exactly the terminal error", len(events))
	}
	last := events[len(events)-1]
	if last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error", last)
	}
	var transportErr *GatewayTransportError
	if !errors.As(last.Err, &transportErr) {
		t.Fatalf("terminal error = %v, want GatewayTransportError (a stalled header is transport-class)", last.Err)
	}
}

// TestStallGuardDoesNotBoundGeneration pins the bound's shape (#754): it
// bounds the wait for the response header only. A healthy streaming turn
// whose model generation trickles — silent gaps far longer than the stall
// bound once headers have arrived — still completes.
func TestStallGuardDoesNotBoundGeneration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for _, delta := range []string{"a", "b", "c"} {
			fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":%q}}]}\n\n", delta)
			flusher.Flush()
			// Silent mid-stream gaps longer than the stall bound: the
			// model is thinking, not stalled.
			time.Sleep(700 * time.Millisecond)
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	})}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()

	gateway := &Gateway{
		BaseURL: "http://" + listener.Addr().String(),
		HTTP:    &http.Client{Transport: stallGuardedTransport(250 * time.Millisecond)},
	}
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final (a slow stream is not a stalled one)", last)
	}
	var text strings.Builder
	for _, event := range events {
		if event.Kind == KindDelta {
			text.WriteString(event.Text)
		}
	}
	if text.String() != "abc" {
		t.Fatalf("deltas = %q, want %q", text.String(), "abc")
	}
}

// TestOwnedModelClientCarriesStallGuard pins that the harness-owned default
// client behind both wire transports carries the first-byte stall bound
// (#754): production constructs every transport without a supplied client, so
// the owned one is the one every session turn and the rebase assist turn run
// on.
func TestOwnedModelClientCarriesStallGuard(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transport http.RoundTripper
	}{
		{"openai", (&Gateway{}).client().Transport},
		{"anthropic", (&AnthropicTransport{}).client().Transport},
	} {
		transport, ok := tc.transport.(*http.Transport)
		if !ok {
			t.Fatalf("%s: owned transport = %T, want *http.Transport", tc.name, tc.transport)
		}
		if transport.ResponseHeaderTimeout != modelStallBound {
			t.Fatalf("%s: ResponseHeaderTimeout = %v, want %v", tc.name, transport.ResponseHeaderTimeout, modelStallBound)
		}
	}
}
