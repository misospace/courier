package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// connectMCP runs server over an in-memory MCP transport and connects a
// client session, exercising the real initialize handshake.
func connectMCP(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx := t.Context()
	go func() { _ = server.Run(ctx, serverTransport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestMCPServerListsAndInvokesLoadTool(t *testing.T) {
	endpoint := serve(t, http.StatusOK, vllmMetrics)
	session := connectMCP(t, NewServer(endpoint.URL, NewScraper(endpoint.Client(), mustMapping(t, BackendAuto))))

	listed, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	if len(listed.Tools) != 1 {
		t.Fatalf("tools = %d, want exactly one read-only tool", len(listed.Tools))
	}
	tool := listed.Tools[0]
	if tool.Name != ToolName {
		t.Fatalf("tool name = %q, want %q", tool.Name, ToolName)
	}
	if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
		t.Fatalf("tool %q must advertise read-only", tool.Name)
	}
	var schema struct {
		Required []string `json:"required"`
	}
	raw, err := json.Marshal(tool.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("input schema %s: %v", raw, err)
	}
	if len(schema.Required) != 0 {
		t.Fatalf("tool %q must take no required arguments, got %v", tool.Name, schema.Required)
	}

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: ToolName})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	if result.IsError {
		t.Fatalf("tools/call returned a tool error: %+v", result)
	}
	var load Load
	raw, err = json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatalf("structured content: %v", err)
	}
	if err := json.Unmarshal(raw, &load); err != nil {
		t.Fatalf("structured content %s is not a Load: %v", raw, err)
	}
	if !load.Available || load.Waiting == nil || *load.Waiting != 3.0 || load.Backpressured == nil || !*load.Backpressured {
		t.Fatalf("load = %+v, want waiting=3 with backpressure", load)
	}
}

// Non-finite gauges ride a valid scrape: the call must still return a normal
// result, since serialization would otherwise fail on NaN/±Inf.
func TestMCPServerHandlesNonFiniteGauges(t *testing.T) {
	nonFinite := `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 2.0
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting NaN
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc +Inf
`
	endpoint := serve(t, http.StatusOK, nonFinite)
	session := connectMCP(t, NewServer(endpoint.URL, NewScraper(endpoint.Client(), mustMapping(t, BackendAuto))))

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: ToolName})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	if result.IsError {
		t.Fatalf("non-finite gauges must degrade in the payload, not error: %+v", result)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatalf("structured content: %v", err)
	}
	if strings.Contains(string(raw), "NaN") || strings.Contains(string(raw), "Inf") {
		t.Fatalf("structured content %s carries a non-finite value", raw)
	}
	var load Load
	if err := json.Unmarshal(raw, &load); err != nil {
		t.Fatalf("structured content %s is not a Load: %v", raw, err)
	}
	if !load.Available || load.Running == nil || *load.Running != 2.0 || load.Waiting != nil || load.Backpressured != nil {
		t.Fatalf("load = %+v, want available with only the usable running gauge", load)
	}
}

// A degraded scrape is still a successful tool call: degradation rides in the
// payload so the coordinator gets an answer, never a failure.
func TestMCPServerDegradesThroughProtocol(t *testing.T) {
	endpoint := httptest.NewServer(http.NotFoundHandler())
	endpoint.Close() // down before we ever call
	session := connectMCP(t, NewServer(endpoint.URL, NewScraper(&http.Client{Timeout: time.Second}, mustMapping(t, BackendAuto))))

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: ToolName})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	if result.IsError {
		t.Fatal("degraded load must be a normal result, not a tool error")
	}
	var load Load
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &load); err != nil {
		t.Fatalf("structured content %s is not a Load: %v", raw, err)
	}
	if load.Available || load.Detail == "" {
		t.Fatalf("load = %+v, want unavailable with a detail", load)
	}
}
