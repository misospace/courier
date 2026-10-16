package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	couriermetrics "github.com/misospace/courier/internal/mcp/metrics"
)

func TestRunValidatesOptions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	tests := []struct {
		name    string
		opts    options
		wantErr string
	}{
		{name: "metrics URL is required", wantErr: "-metrics-url is required"},
		{name: "unknown backend", opts: options{metricsURL: "http://vllm:8000/metrics", backend: "sglang"}, wantErr: "unknown backend mapping"},
		{name: "non-HTTP scheme", opts: options{metricsURL: "ftp://vllm:8000/metrics"}, wantErr: "must be http or https"},
		{name: "zero scrape timeout", opts: options{metricsURL: "http://vllm:8000/metrics", backend: "vllm"}, wantErr: "-scrape-timeout must be positive (got 0s)"},
		{name: "negative scrape timeout", opts: options{metricsURL: "http://vllm:8000/metrics", backend: "vllm", scrapeTimeout: -time.Second}, wantErr: "-scrape-timeout must be positive (got -1s)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(t.Context(), listener, tt.opts, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run() error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// rpcCall POSTs one JSON-RPC request. The server runs stateless, so a client
// needs no initialize handshake to list or call the tool.
func rpcCall(t *testing.T, base, method, params string) json.RawMessage {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":` + params + `}`
	req, err := http.NewRequest(http.MethodPost, base, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: HTTP %s", method, resp.Status)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("%s: decode response: %v", method, err)
	}
	if len(envelope.Error) > 0 {
		t.Fatalf("%s: JSON-RPC error: %s", method, envelope.Error)
	}
	if len(envelope.Result) == 0 {
		t.Fatalf("%s: response has no result", method)
	}
	return envelope.Result
}

func TestRunServesLoadToolOverStreamableHTTP(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(vllmMetricsText))
	}))
	defer endpoint.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, listener, options{
			metricsURL:    endpoint.URL,
			backend:       couriermetrics.BackendAuto,
			scrapeTimeout: 2 * time.Second,
		}, io.Discard)
	}()

	base := "http://" + listener.Addr().String()
	var listed struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(rpcCall(t, base, "tools/list", "{}"), &listed); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}
	if len(listed.Tools) != 1 || listed.Tools[0].Name != couriermetrics.ToolName {
		t.Fatalf("tools = %+v, want exactly %q", listed.Tools, couriermetrics.ToolName)
	}

	var call struct {
		IsError           bool           `json:"isError"`
		StructuredContent map[string]any `json:"structuredContent"`
	}
	callParams := `{"name":"` + couriermetrics.ToolName + `","arguments":{}}`
	if err := json.Unmarshal(rpcCall(t, base, "tools/call", callParams), &call); err != nil {
		t.Fatalf("decode tools/call: %v", err)
	}
	if call.IsError {
		t.Fatalf("tools/call returned a tool error: %+v", call)
	}
	if call.StructuredContent["available"] != true || call.StructuredContent["waiting"] != float64(3) {
		t.Fatalf("structuredContent = %+v, want available with waiting=3", call.StructuredContent)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not shut down on context cancel")
	}
}

const vllmMetricsText = `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 1.0
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting 3.0
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc 0.72
`

// TestNewHTTPServerBoundsWritesAroundScrape pins the review contract that the
// write budget is derived from the scrape timeout: it must stay finite yet
// outlive any valid -scrape-timeout so it cannot cut off a bounded tool call.
func TestNewHTTPServerBoundsWritesAroundScrape(t *testing.T) {
	for _, scrape := range []time.Duration{time.Second, 30 * time.Second, 45 * time.Second, 2 * time.Minute} {
		srv := newHTTPServer(http.NotFoundHandler(), scrape)
		if srv.WriteTimeout <= scrape {
			t.Errorf("scrape %s: WriteTimeout %s does not outlive the scrape budget", scrape, srv.WriteTimeout)
		}
		if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.IdleTimeout <= 0 {
			t.Errorf("scrape %s: inbound budgets must all be finite: %+v", scrape, srv)
		}
	}
}

// awaitForceClosed blocks until the server closes conn, proven by EOF or a
// connection error such as reset. The read deadline is a test guard, not
// evidence of closure: hitting it fails the test, because it means the server
// left the connection open past the deadline instead of force-closing it.
func awaitForceClosed(t *testing.T, conn net.Conn, within time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(within))
	for {
		_, err := conn.Read(make([]byte, 1))
		if err == nil {
			continue // drain any response bytes written before the close
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("connection still open after %s; want server-initiated EOF or reset", within)
		}
		return // e.g. connection reset: the server closed the socket
	}
}

// TestRunShutdownBoundsIncompleteRequest opens a real TCP connection, sends
// only a partial HTTP request, cancels run, and asserts that run returns
// within the configured shutdown grace and the connection is force-closed.
func TestRunShutdownBoundsIncompleteRequest(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(vllmMetricsText))
	}))
	defer endpoint.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	const grace = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, listener, options{
			metricsURL:    endpoint.URL,
			backend:       couriermetrics.BackendAuto,
			scrapeTimeout: 2 * time.Second,
			shutdownGrace: grace,
		}, io.Discard)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	// Send a partial request: a method line and a Host header, but no
	// terminating blank line. The server cannot finish reading the header
	// block, so the connection stays open without consuming a request.
	if _, err := conn.Write([]byte("POST / HTTP/1.1\r\nHost: x\r\n")); err != nil {
		cancel()
		t.Fatalf("partial write: %v", err)
	}
	// Give the server a moment to register the partial-read connection in
	// its active-conn count before we cancel, so Shutdown has something to
	// wait on.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	cancel()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("run returned %v on shutdown", err)
		}
		// The server must honor the grace budget and close: allow generous
		// slack for goroutine scheduling but reject a hung shutdown.
		if elapsed >= 5*time.Second {
			t.Fatalf("run took %s to return after cancel, want bounded by grace %s + force-close", elapsed, grace)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return within 5s of cancel")
	}

	// The connection must be force-closed once the grace budget expires:
	// the read side must observe EOF or a connection reset; the local read
	// deadline expiring first fails the test.
	awaitForceClosed(t, conn, 2*time.Second)
	conn.Close()
}

// TestRunShutdownBoundedByIncompleteBody exercises a slightly different stuck
// state: the request headers are complete, but the body Content-Length is
// much larger than what the client sends. Shutdown must still force-close
// the connection once the grace budget is exhausted.
func TestRunShutdownBoundedByIncompleteBody(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(vllmMetricsText))
	}))
	defer endpoint.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	const grace = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, listener, options{
			metricsURL:    endpoint.URL,
			backend:       couriermetrics.BackendAuto,
			scrapeTimeout: 2 * time.Second,
			shutdownGrace: grace,
		}, io.Discard)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	// Complete headers but advertise a 1 MiB body we will not deliver. The
	// server will block reading the body and must be force-closed on shutdown.
	header := "POST / HTTP/1.1\r\n" +
		"Host: x\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: 1048576\r\n\r\n"
	if _, err := conn.Write([]byte(header)); err != nil {
		cancel()
		t.Fatalf("write headers: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	cancel()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("run returned %v on shutdown", err)
		}
		if elapsed >= 5*time.Second {
			t.Fatalf("run took %s to return after cancel, want bounded by grace %s + force-close", elapsed, grace)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return within 5s of cancel")
	}

	// The server may have started an error response before the force-close;
	// after draining it the connection must show EOF or a reset, not merely
	// a local deadline hit.
	awaitForceClosed(t, conn, 2*time.Second)
	conn.Close()
}

func TestRunRedactsQueryCredentials(t *testing.T) {
	const secret = "startup-log-secret-5f2c9"

	dead := httptest.NewServer(http.NotFoundHandler())
	transportURL := dead.URL
	dead.Close() // nothing listens there anymore

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, listener, options{
			metricsURL: "http://alice:hunter2@" + strings.TrimPrefix(transportURL, "http://") +
				"/metrics?token=" + secret + "&z=1#" + secret,
			backend:       couriermetrics.BackendAuto,
			scrapeTimeout: 2 * time.Second,
		}, &logs)
	}()

	var call struct {
		IsError bool `json:"isError"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		StructuredContent map[string]any `json:"structuredContent"`
	}
	if err := json.Unmarshal(rpcCall(t, "http://"+listener.Addr().String(), "tools/call",
		`{"name":"`+couriermetrics.ToolName+`","arguments":{}}`), &call); err != nil {
		t.Fatalf("decode tools/call: %v", err)
	}
	if call.IsError {
		t.Fatalf("tools/call returned a tool error: %+v", call)
	}
	if call.StructuredContent["available"] != false {
		t.Fatalf("structuredContent = %+v, want unavailable", call.StructuredContent)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not shut down on context cancel")
	}

	detail := fmt.Sprint(call.StructuredContent["detail"])
	if detail == "" && len(call.Content) > 0 {
		t.Fatalf("no structured detail available; content = %+v", call.Content)
	}
	for _, surface := range append([]string{logs.String(), detail}, contentTexts(call.Content)...) {
		for _, leaked := range []string{secret, "hunter2", "alice"} {
			if strings.Contains(surface, leaked) {
				t.Fatalf("diagnostic surface %q leaks %q", surface, leaked)
			}
		}
	}
	if !strings.Contains(logs.String(), transportURL+"/metrics") {
		t.Fatalf("startup log %q should keep the host and path for debugging", logs.String())
	}
}

func contentTexts(content []struct {
	Text string `json:"text"`
}) []string {
	texts := make([]string, 0, len(content))
	for _, part := range content {
		texts = append(texts, part.Text)
	}
	return texts
}
