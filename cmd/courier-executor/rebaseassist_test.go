package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/rebase"
)

// sseGateway is the OpenAI-compatible SSE test double, in the exact stream
// shape harness.Gateway's normalizer parses: data lines of JSON chunks, a
// trailing usage chunk, and a [DONE] sentinel. It records every request
// body so tests can assert the model and prompt the adapter actually sent.
type sseGateway struct {
	mu     sync.Mutex
	body   string
	last   string
	auth   string
	status int
}

func (g *sseGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.auth = r.Header.Get("Authorization")
	g.last = readAll(r)
	if g.status != 0 && g.status != http.StatusOK {
		w.WriteHeader(g.status)
		fmt.Fprint(w, `{"error":{"message":"rejected"}}`)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, g.body)
}

func readAll(r *http.Request) string {
	var sb strings.Builder
	// The request body is small in tests; one bounded read suffices.
	buf := make([]byte, 1<<20)
	n, _ := r.Body.Read(buf)
	sb.Write(buf[:n])
	return sb.String()
}

// stream builds the SSE body for a turn: content deltas, a stop finish, and
// the usage chunk the transport's include_usage request asks for.
func stream(usage string, deltas ...string) string {
	var sb strings.Builder
	for _, delta := range deltas {
		fmt.Fprintf(&sb, "data: %s\n\n", fmt.Sprintf(`{"choices":[{"index":0,"delta":{"content":%q},"finish_reason":null}]}`, delta))
	}
	sb.WriteString("data: " + `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n")
	if usage != "" {
		sb.WriteString("data: " + fmt.Sprintf(`{"choices":[],"usage":%s}`, usage) + "\n\n")
	}
	sb.WriteString("data: [DONE]\n\n")
	return sb.String()
}

func newAssistGateway(t *testing.T, sse *sseGateway) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(sse)
	t.Cleanup(server.Close)
	return server
}

func testConfig(url string) config {
	return config{
		RebaseAssist:       "true",
		ModelGatewayURL:    url,
		ModelGatewayAPIKey: "gateway-secret-key",
		Model:              "litellm/qwen3.8-27b",
	}
}

func assistRequest() rebase.AssistRequest {
	return rebase.AssistRequest{
		Goal:     "replay the pull request onto its live base",
		Base:     "main",
		Branch:   "pr-branch",
		PRDiff:   "diff --git a/code.go b/code.go",
		BaseDiff: "diff --git a/code.go b/code.go",
		Conflicts: []rebase.ConflictFile{
			{Path: "code.go", Content: "<<<<<<< HEAD\nx := 3\n=======\nx := 2\n>>>>>>> pr-branch\n"},
		},
	}
}

// A resolution answer parses into files, and the turn's request carries the
// configured model and the engine's prompt; the usage chunk's counts surface
// in the resolution.
func TestGatewayResolverResolvesFiles(t *testing.T) {
	answer := `Sure — here is the resolution.
{"files":[{"path":"code.go","content":"package main\n\nfunc main() {\n\tx := 5\n\t_ = x\n}\n"}]}`
	sse := &sseGateway{body: stream(`{"prompt_tokens":110,"completion_tokens":37}`, answer)}
	server := newAssistGateway(t, sse)
	resolver, err := newRebaseAssistResolver(testConfig(server.URL))
	if err != nil || resolver == nil {
		t.Fatalf("resolver = %#v (%v), want built", resolver, err)
	}
	resolution, err := resolver.Resolve(context.Background(), assistRequest())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := resolution.Files["code.go"]; !strings.Contains(got, "x := 5") {
		t.Fatalf("resolved content = %q, want the model's file", got)
	}
	if resolution.InputTokens != 110 || resolution.OutputTokens != 37 {
		t.Fatalf("usage = %d/%d, want the provider's report", resolution.InputTokens, resolution.OutputTokens)
	}
	if resolution.Model != "litellm/qwen3.8-27b" {
		t.Fatalf("model = %q, want the configured coordinator model", resolution.Model)
	}
	// The request the adapter sent: the configured model, one user message
	// carrying the engine's prompt, and the gateway key in the header alone.
	sse.mu.Lock()
	defer sse.mu.Unlock()
	if !strings.Contains(sse.last, `"model":"litellm/qwen3.8-27b"`) {
		t.Fatalf("request model missing: %s", sse.last)
	}
	if !strings.Contains(sse.last, "x := 3") || !strings.Contains(sse.last, "refusal") {
		t.Fatalf("request prompt missing the conflict evidence: %s", sse.last)
	}
	if sse.auth != "Bearer gateway-secret-key" {
		t.Fatalf("authorization = %q, want the configured gateway key", sse.auth)
	}
}

// A refusal answer maps to a resolution whose Refusal is set and whose file
// map is empty: the engine treats the decline as the whole answer.
func TestGatewayResolverRefusal(t *testing.T) {
	sse := &sseGateway{body: stream("", `{"refusal":"the conflict needs a product decision"}`)}
	server := newAssistGateway(t, sse)
	resolver, _ := newRebaseAssistResolver(testConfig(server.URL))
	resolution, err := resolver.Resolve(context.Background(), assistRequest())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolution.Refusal == "" || len(resolution.Files) != 0 {
		t.Fatalf("resolution = %#v, want the refusal and no files", resolution)
	}
}

// A gateway stream error — a non-2xx response, here — surfaces as the
// resolver error the engine classifies as an unavailable resolver, never as
// a resolution.
func TestGatewayResolverStreamError(t *testing.T) {
	sse := &sseGateway{status: http.StatusServiceUnavailable}
	server := newAssistGateway(t, sse)
	resolver, _ := newRebaseAssistResolver(testConfig(server.URL))
	_, err := resolver.Resolve(context.Background(), assistRequest())
	if err == nil {
		t.Fatal("Resolve succeeded on an error stream")
	}
	var gatewayErr *harness.GatewayError
	if !errors.As(err, &gatewayErr) {
		t.Fatalf("err = %v, want a classified gateway error", err)
	}
}

// A response holding no JSON object at all is a failed turn, not an empty
// resolution: the engine must never see a silent no-op answer as data.
func TestGatewayResolverNoJSON(t *testing.T) {
	sse := &sseGateway{body: stream("", "I cannot help with that request.")}
	server := newAssistGateway(t, sse)
	resolver, _ := newRebaseAssistResolver(testConfig(server.URL))
	if _, err := resolver.Resolve(context.Background(), assistRequest()); err == nil {
		t.Fatal("Resolve accepted a prose-only answer")
	}
}

// Env parsing: enabled with a URL builds; enabled without a URL is a
// configuration error; anything else is disabled and builds nil.
func TestNewRebaseAssistResolverEnv(t *testing.T) {
	cfg := testConfig("http://gateway:4000/v1")
	resolver, err := newRebaseAssistResolver(cfg)
	if err != nil || resolver == nil {
		t.Fatalf("enabled+url = (%#v, %v), want built", resolver, err)
	}
	for _, value := range []string{"1", "ENABLED", "True"} {
		cfg.RebaseAssist = value
		if resolver, err = newRebaseAssistResolver(cfg); err != nil || resolver == nil {
			t.Fatalf("enabled value %q = (%#v, %v), want built", value, resolver, err)
		}
	}
	cfg.ModelGatewayURL = ""
	if _, err := newRebaseAssistResolver(cfg); err == nil || !strings.Contains(err.Error(), "COURIER_MODEL_GATEWAY_URL") {
		t.Fatalf("enabled without url err = %v, want a configuration error", err)
	}
	for _, value := range []string{"", "0", "false", "off", "yes-but"} {
		cfg.RebaseAssist = value
		cfg.ModelGatewayURL = "http://gateway:4000/v1"
		if resolver, err = newRebaseAssistResolver(cfg); err != nil || resolver != nil {
			t.Fatalf("disabled value %q = (%#v, %v), want nil", value, resolver, err)
		}
	}
}
