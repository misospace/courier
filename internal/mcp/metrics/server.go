package metrics

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// ToolName is the single tool this server exposes. Coordinator MCP
	// clients prefix it with this server's name (e.g. metrics_model_load).
	ToolName = "model_load"
	// Version is the MCP implementation version.
	Version = "0.1.0"
)

// NewServer builds the MCP server exposing the single read-only load tool.
// The tool takes no arguments and performs only the configured scrape; every
// outcome, including scrape failure, is a normal result with an honest Load
// payload rather than an error, so a degraded backend never blocks a run.
func NewServer(metricsURL string, scraper *Scraper) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "courier-metrics-mcp", Version: Version}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name: ToolName,
		Description: "Report current load on the configured model server: running and queued request counts, " +
			"whether the backend is backpressured, and KV cache usage when reported. " +
			"Backpressure reflects queued requests, not merely running ones. " +
			"When available is false, treat load as unknown and decide as if the tool were absent.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, Load, error) {
		return nil, scraper.Scrape(ctx, metricsURL), nil
	})
	return server
}

// HTTPHandler serves server over streamable HTTP, stateless and JSON-only:
// one POST, one bounded scrape, no sessions to hold open.
func HTTPHandler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
}
