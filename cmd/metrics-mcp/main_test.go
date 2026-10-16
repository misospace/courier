package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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

// A query-authenticated metrics URL must not leak its secret into the
// startup log or into the model-visible unavailable reason when the
// endpoint fails. A closed server forces a transport error, which is the
// failure path that renders the URL through redactError.
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
