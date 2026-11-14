// Package e2e holds Courier's end-to-end regression harness: fixtures that
// replay the integrated native control path end to end against production
// packages, with bounded local fakes only at the boundaries the production
// system cannot reach from a test (the model gateway, the worker protocol,
// the source adapter, and the forge's own API).
//
// The package is test-only: every file in it ends in _test.go, so the
// production build never carries it.
//
// Fixture A (fixture_a_test.go) replays the whole native path:
//
//	admission -> source claim -> native capability gate -> a real role-bound
//	model turn with earned tool activity -> authenticated status progress ->
//	fenced publication -> the operator observes the terminal result and
//	settles the source.
//
// Every seam is a production package. The fakes are:
//
//   - the model gateway (a scripted local SSE server),
//   - the worker protocol (a local signed-protocol listener),
//   - the source adapter (a recording adapter),
//   - the forge API (internal/forge/forgetest's local-git double),
//   - the Kubernetes TokenReview backend (the token minting a real cluster
//     performs).
//
// No inference is paid for, no third-party pull request is opened, and no
// cloud model window is needed.
package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

var (
	// envtestClient is the real API server's client. Every operator-side
	// read and write in this package goes through it.
	envtestClient client.Client
	// envtestConfig is the API server's REST configuration, used to build
	// the production clientset the secure path's access reviews need.
	envtestConfig *rest.Config
	// envtestServer is the running API server.
	envtestServer *envtest.Environment
)

// TestMain starts the real API server the fixtures reconcile against. The
// suite skips entirely when KUBEBUILDER_ASSETS is empty, so a checkout
// without envtest assets still builds and vets.
func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}

	envtestServer = &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../charts/courier/crd-manifests"},
		ErrorIfCRDPathMissing: true,
	}
	config, err := envtestServer.Start()
	if err != nil {
		panic(err)
	}
	envtestConfig = config
	envtestClient, err = client.New(config, client.Options{Scheme: e2eScheme()})
	if err != nil {
		_ = envtestServer.Stop()
		panic(err)
	}
	exitCode := m.Run()
	if err := envtestServer.Stop(); err != nil && exitCode == 0 {
		exitCode = 1
	}
	os.Exit(exitCode)
}

// e2eScheme is the scheme the fixtures read and write with: the core
// Kubernetes kinds the secure topology provisions plus Courier's own API.
func e2eScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		rbacv1.AddToScheme,
		networkingv1.AddToScheme,
		courierv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			panic(err)
		}
	}
	return scheme
}

// gitFixture is the fixture's local git world: one bare repository plays the
// forge's base and work repository (a resolve-issue run publishes into the
// base repository itself), and one working clone holds the commits the
// fixture proposes.
type gitFixture struct {
	// Root is the fixture's temporary directory.
	Root string
	// Bare is the bare repository the forge double reads refs from and the
	// broker pushes into.
	Bare string
	// Source is the working repository the fixture commits into.
	Source string
	// BaseOID is the base commit every proposal descends from.
	BaseOID string
}

// newGitFixture builds the local git world: a bare repository with a main
// branch holding one base commit, and a working clone that tracks it.
func newGitFixture(t *testing.T) *gitFixture {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "repo.git")
	source := filepath.Join(root, "source")
	runGit(t, "", "init", "--bare", "--quiet", bare)
	runGit(t, "", "init", "--quiet", source)
	runGit(t, source, "config", "user.email", "courier@example.invalid")
	runGit(t, source, "config", "user.name", "courier")
	writeFile(t, filepath.Join(source, "README.md"), "courier fixture\n")
	runGit(t, source, "add", "README.md")
	runGit(t, source, "commit", "--quiet", "-m", "base")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "push", "--quiet", bare, "HEAD:refs/heads/main")
	base := gitOutput(t, source, "rev-parse", "HEAD")
	return &gitFixture{Root: root, Bare: bare, Source: source, BaseOID: base}
}

// Propose commits one change on top of the fixture's base commit in the
// working clone and returns the commit's OID. The commit is not pushed: the
// broker publishes it.
func (g *gitFixture) Propose(t *testing.T, name, content string) string {
	t.Helper()
	writeFile(t, filepath.Join(g.Source, name), content)
	runGit(t, g.Source, "add", name)
	runGit(t, g.Source, "commit", "--quiet", "-m", "propose "+name)
	return gitOutput(t, g.Source, "rev-parse", "HEAD")
}

// Bundle writes a git bundle holding the fixture's whole history and
// returns its path, so the broker can import the proposal into its own
// private object store.
func (g *gitFixture) Bundle(t *testing.T) string {
	t.Helper()
	path := filepath.Join(g.Root, "proposal.bundle")
	runGit(t, g.Source, "bundle", "create", path, "--all")
	return path
}

// runGit runs one git command in a directory and fails the test when it
// does not succeed.
func runGit(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}

// gitOutput runs one git command and returns its trimmed standard output.
func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return strings.TrimSpace(string(runGit(t, dir, args...)))
}

// writeFile writes a file the fixture owns.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// modelRequest is one chat request the scripted gateway received.
type modelRequest struct {
	// Model is the model identifier the request named.
	Model string
	// Tools names the tools the request offered.
	Tools []string
}

// modelFault is one scripted failure answer: the non-2xx response a provider
// returns when it throttles or rejects a request. The harness's own
// classification and retry policy — never the fixture — decide what such an
// answer means.
type modelFault struct {
	// status is the HTTP status the gateway answers with.
	status int
	// headers are the response headers, a Retry-After hint among them.
	headers map[string]string
	// body is the response body.
	body string
}

// modelResponse is one scripted answer: either a streamed body or a fault.
type modelResponse struct {
	// sse is the streamed body of a successful answer.
	sse string
	// fault, when set, is the failure the gateway answers with instead of a
	// streamed body.
	fault *modelFault
}

// scriptedModel is the local model gateway: it speaks the exact SSE shapes
// the production harness gateway decodes, counts every chat request it
// received, and refuses to answer beyond its script. The count is what makes
// a regression that hammers the model detectable.
type scriptedModel struct {
	mu       sync.Mutex
	script   []modelResponse
	seen     []modelRequest
	server   *httptest.Server
	exceeded int
	// log is the fixture's shared timeline.
	log *eventLog
}

// newScriptedModelWith starts the local gateway with the given responses,
// each served in order. A response may be a streamed body or a fault, so a
// fixture can inject a provider failure at an exact point of the run.
func newScriptedModelWith(t *testing.T, log *eventLog, responses []modelResponse) *scriptedModel {
	t.Helper()
	model := &scriptedModel{script: append([]modelResponse(nil), responses...), log: log}
	model.server = httptest.NewServer(model)
	t.Cleanup(model.server.Close)
	return model
}

// BaseURL is the gateway's OpenAI-compatible API root.
func (m *scriptedModel) BaseURL() string {
	return m.server.URL + "/v1"
}

// Requests reports how many chat requests the gateway received.
func (m *scriptedModel) Requests() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.seen)
}

// RequestLog returns the recorded chat requests in the order they arrived.
func (m *scriptedModel) RequestLog() []modelRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]modelRequest, len(m.seen))
	copy(out, m.seen)
	return out
}

// Exceeded reports how many requests the gateway refused because the script
// was exhausted: every one of them is a request the harness made beyond the
// bound the fixture set.
func (m *scriptedModel) Exceeded() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.exceeded
}

// ServeHTTP answers the gateway's two production routes: the chat
// completion and the model metadata read.
func (m *scriptedModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat/completions"):
		m.serveChat(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/model/info"):
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[{"model_name":"test/coordinator","id":"test/coordinator","max_input_tokens":200000}]}`)
	default:
		http.NotFound(w, r)
	}
}

// serveChat records the request and answers it with the next scripted
// response. A request beyond the script is a hard failure: the fixture
// asked for a bounded number of model turns, and more means a regression.
func (m *scriptedModel) serveChat(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Model string `json:"model"`
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "unreadable request", http.StatusBadRequest)
		return
	}
	names := make([]string, 0, len(request.Tools))
	for _, tool := range request.Tools {
		names = append(names, tool.Function.Name)
	}
	m.mu.Lock()
	m.seen = append(m.seen, modelRequest{Model: request.Model, Tools: names})
	if len(m.script) == 0 {
		m.exceeded++
		m.mu.Unlock()
		http.Error(w, "the scripted gateway received an unscripted request", http.StatusInternalServerError)
		return
	}
	response := m.script[0]
	m.script = m.script[1:]
	m.mu.Unlock()

	if m.log != nil {
		m.log.Append("model-request")
	}
	if response.fault != nil {
		for name, value := range response.fault.headers {
			w.Header().Set(name, value)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.fault.status)
		fmt.Fprint(w, response.fault.body)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, response.sse)
}

// sse renders the SSE body of one OpenAI-compatible response from its data
// chunks.
func sse(chunks ...string) string {
	var b strings.Builder
	for _, chunk := range chunks {
		b.WriteString("data: " + chunk + "\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

// contentChunk renders one streamed content delta.
func contentChunk(text string) string {
	return fmt.Sprintf(`{"choices":[{"index":0,"delta":{"content":%q},"finish_reason":null}]}`, text)
}

// toolCallChunk renders one streamed tool call.
func toolCallChunk(index int, id, name, arguments string) string {
	return fmt.Sprintf(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":%d,"id":%q,"type":"function","function":{"name":%q,"arguments":%q}}]},"finish_reason":null}]}`, index, id, name, arguments)
}

// finishChunk renders the turn's terminal chunk.
func finishChunk(reason string) string {
	return fmt.Sprintf(`{"choices":[{"index":0,"delta":{},"finish_reason":%q}]}`, reason)
}
