package harness

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/protocol"
)

// --- scripted model gateway ----------------------------------------------

type chatRecord struct {
	model     string
	rawBody   string
	toolNames []string
}

type gatewayResponse struct {
	status int // 0 means 200 with an SSE body
	body   string
	// errBody overrides the default provider error body on a non-2xx
	// response; header adds response headers (for example Retry-After).
	errBody string
	header  http.Header
}

type mockGateway struct {
	mu     sync.Mutex
	script []gatewayResponse
	calls  []chatRecord
}

func (m *mockGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "unreadable", http.StatusBadRequest)
		return
	}
	var req wireRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var toolNames []string
	for _, tool := range req.Tools {
		toolNames = append(toolNames, tool.Function.Name)
	}
	m.calls = append(m.calls, chatRecord{model: req.Model, rawBody: string(raw), toolNames: toolNames})
	if len(m.script) == 0 {
		http.Error(w, "unexpected call", http.StatusInternalServerError)
		return
	}
	response := m.script[0]
	m.script = m.script[1:]
	if response.status != 0 {
		// The body content is never echoed into harness errors; make it
		// recognizable so redaction tests can assert it stays out.
		for key, values := range response.header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.status)
		body := response.errBody
		if body == "" {
			body = `{"error":{"message":"super-secret-provider-detail","type":"provider_error"}}`
		}
		fmt.Fprint(w, body)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, response.body)
}

func (m *mockGateway) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func gatewayFor(script ...gatewayResponse) (*Gateway, *mockGateway, func()) {
	mocker := &mockGateway{script: script}
	server := httptest.NewServer(mocker)
	gateway := &Gateway{BaseURL: server.URL}
	gateway.HTTP = server.Client()
	return gateway, mocker, server.Close
}

func sse(chunks ...string) string {
	var b strings.Builder
	for _, chunk := range chunks {
		b.WriteString("data: " + chunk + "\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func contentChunk(text string) string {
	return fmt.Sprintf(`{"choices":[{"index":0,"delta":{"content":%q},"finish_reason":null}]}`, text)
}

func toolCallChunk(index int, id, name, arguments string) string {
	return fmt.Sprintf(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":%d,"id":%q,"type":"function","function":{"name":%q,"arguments":%q}}]},"finish_reason":null}]}`, index, id, name, arguments)
}

func finishChunk(reason string) string {
	return fmt.Sprintf(`{"choices":[{"index":0,"delta":{},"finish_reason":%q}]}`, reason)
}

// --- mock worker ----------------------------------------------------------

// workerSignedRequest mirrors the protocol package's signed request shape.
type workerSignedRequest struct {
	Envelope  protocol.Envelope `json:"envelope"`
	Signature []byte            `json:"signature"`
	Payload   []byte            `json:"payload,omitempty"`
}

type dispatchRecord struct {
	envelope protocol.Envelope
	task     protocol.Task
}

type mockWorker struct {
	mu          sync.Mutex
	key         ed25519.PublicKey
	dispatches  []dispatchRecord
	cancels     []protocol.Envelope
	snapshots   int
	completeAll bool
	// dispatchPlan scripts per-dispatch status codes (0 means 200). A 5xx
	// stands in for an ambiguous transport outcome.
	dispatchPlan []int
	// dispatchRequests counts every dispatch request that arrived, recorded
	// or not: after an ambiguous failure the question is whether the worker
	// saw a second attempt.
	dispatchRequests int
	// resultFailures makes that many result polls fail with 503 before the
	// scripted state is served.
	resultFailures int
	results        map[string]protocol.ResultState
	// snapshotTip is echoed by the scripted unpack task; packArtifact is the
	// bundle the scripted pack task returns (empty artifact = no changes).
	snapshotTip  string
	packArtifact []byte
	snapshotTips []string
	// bootstrapResults are consumed in order by the scripted bootstrap task;
	// with none scripted, a bootstrap task completes with an empty capture.
	bootstrapResults []protocol.Result
	// hangResults makes every result poll block until its request context
	// ends, standing in for a worker whose operation never terminates.
	hangResults bool
	// hangStage makes only the result polls of one bootstrap stage's
	// operations ("bootstrap" or "capture") block until their request
	// context ends, standing in for a worker whose bootstrap task or whose
	// environment-capture task — but not both — never terminates.
	hangStage string
	// bootstrapOps and captureOps record the op IDs dispatched for each
	// bootstrap stage, so hangStage can hang exactly one of them.
	bootstrapOps map[string]bool
	captureOps   map[string]bool
}

func isWorkerBootstrapTask(task protocol.Task) bool {
	return len(task.Command) == 3 && task.Command[0] == "sh" && task.Command[1] == "-c" &&
		strings.Contains(task.Command[2], "mise run bootstrap")
}

func isWorkerEnvCaptureTask(task protocol.Task) bool {
	return len(task.Command) == 3 && task.Command[0] == "sh" && task.Command[1] == "-c" &&
		strings.Contains(task.Command[2], "mise x -- env -0")
}

func isWorkerUnpackTask(task protocol.Task) bool {
	return len(task.Command) == 3 && task.Command[0] == "sh" && task.Command[1] == "-c" &&
		strings.Contains(task.Command[2], "refs/courier/snapshot/work:refs/heads/work")
}

func isWorkerPackTask(task protocol.Task) bool {
	return len(task.Command) == 3 && task.Command[0] == "sh" && task.Command[1] == "-c" &&
		strings.Contains(task.Command[2], "refs/courier/briefs/")
}

func (m *mockWorker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks":
		m.handleDispatch(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks/cancel":
		m.handleCancel(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tasks/") && strings.HasSuffix(r.URL.Path, "/result"):
		m.handleResult(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/snapshot":
		m.mu.Lock()
		m.snapshots++
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (m *mockWorker) handleDispatch(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatchRequests++
	if len(m.dispatchPlan) > 0 {
		status := m.dispatchPlan[0]
		m.dispatchPlan = m.dispatchPlan[1:]
		if status != 0 {
			w.WriteHeader(status)
			fmt.Fprint(w, "worker is overloaded")
			return
		}
	}
	var req workerSignedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := req.Envelope.Verify(m.key, req.Signature); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var task protocol.Task
	if err := json.Unmarshal(req.Payload, &task); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	m.dispatches = append(m.dispatches, dispatchRecord{envelope: req.Envelope, task: task})
	if m.results == nil {
		m.results = map[string]protocol.ResultState{}
	}
	// The fixed unpack and pack tasks are answered from their scripted
	// fields: the unpack echoes the tip, the pack returns the bundle.
	switch {
	case isWorkerUnpackTask(task):
		tip := m.snapshotTip
		if len(m.snapshotTips) > 0 {
			tip = m.snapshotTips[0]
			m.snapshotTips = m.snapshotTips[1:]
		}
		m.results[req.Envelope.OpID] = protocol.ResultState{
			Status: protocol.ResultCompleted,
			Result: &protocol.Result{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: tip},
		}
	case isWorkerPackTask(task):
		m.results[req.Envelope.OpID] = protocol.ResultState{
			Status: protocol.ResultCompleted,
			Result: &protocol.Result{Status: protocol.ResultCompleted, ExitCode: 0, Artifact: m.packArtifact},
		}
	case isWorkerBootstrapTask(task), isWorkerEnvCaptureTask(task):
		if isWorkerBootstrapTask(task) {
			if m.bootstrapOps == nil {
				m.bootstrapOps = map[string]bool{}
			}
			m.bootstrapOps[req.Envelope.OpID] = true
		} else {
			if m.captureOps == nil {
				m.captureOps = map[string]bool{}
			}
			m.captureOps[req.Envelope.OpID] = true
		}
		result := protocol.Result{
			Status:   protocol.ResultCompleted,
			ExitCode: 0,
			// A default capture the devenv integrity rules accept: one
			// NUL-terminated record, so a test that only cares about
			// dispatch shape gets an applied bootstrap.
			StdoutTail: "PATH=/mock/shims\x00",
		}
		if len(m.bootstrapResults) > 0 {
			result = m.bootstrapResults[0]
			m.bootstrapResults = m.bootstrapResults[1:]
		}
		status := result.Status
		if status == "" {
			status = protocol.ResultCompleted
		}
		m.results[req.Envelope.OpID] = protocol.ResultState{Status: status, Result: &result}
	case m.completeAll:
		m.results[req.Envelope.OpID] = protocol.ResultState{
			Status: protocol.ResultCompleted,
			Result: &protocol.Result{
				Status:     protocol.ResultCompleted,
				ExitCode:   0,
				StdoutTail: "I published PR #999",
			},
		}
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"running"}`)
}

func (m *mockWorker) handleCancel(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var req workerSignedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := req.Envelope.Verify(m.key, req.Signature); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	m.cancels = append(m.cancels, req.Envelope)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"cancelled"}`)
}

func (m *mockWorker) handleResult(w http.ResponseWriter, r *http.Request) {
	if m.hangResults {
		<-r.Context().Done()
		return
	}
	if m.hangStage != "" {
		m.mu.Lock()
		opID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), "/result")
		hang := (m.hangStage == "bootstrap" && m.bootstrapOps[opID]) ||
			(m.hangStage == "capture" && m.captureOps[opID])
		m.mu.Unlock()
		if hang {
			<-r.Context().Done()
			return
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resultFailures > 0 {
		m.resultFailures--
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "poll failed transiently")
		return
	}
	opID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), "/result")
	state, ok := m.results[opID]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(state); err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
	}
}

func (m *mockWorker) dispatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.dispatches)
}

func (m *mockWorker) cancelCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.cancels)
}

// --- recording activity sink ----------------------------------------------

type recordingActivity struct {
	mu    sync.Mutex
	steps []string
}

func (a *recordingActivity) StreamChunk() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.steps = append(a.steps, "stream")
}

func (a *recordingActivity) ToolBoundary() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.steps = append(a.steps, "tool")
}

func (a *recordingActivity) count(kind string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	total := 0
	for _, step := range a.steps {
		if step == kind {
			total++
		}
	}
	return total
}

// --- test helpers ----------------------------------------------------------

func testBindings() Bindings {
	bindings, err := BindRoles(map[string]string{"coordinator": "test/coordinator", "coder": "test/coder"})
	if err != nil {
		panic(err)
	}
	return bindings
}

func testInvocation() executor.Invocation {
	return executor.Invocation{
		RunName: "run-1",
		Mode:    courier.ModeResolveIssue,
		Ref:     7,
		Goal:    "Resolve the issue.",
		Model:   "test/coordinator",
		Roles:   map[string]string{"coordinator": "test/coordinator"},
	}
}

func delegateCall(id, role string) ToolCall {
	arguments := fmt.Sprintf(`{"id":%q,"role":%q,"objective":"Do the work","successCheck":"tests pass"}`, id, role)
	return ToolCall{ID: "call-" + id, Name: toolDelegate, Arguments: arguments}
}

func shellCall(command string) ToolCall {
	return ToolCall{ID: "call-shell", Name: toolShell, Arguments: fmt.Sprintf(`{"command":%q}`, command)}
}

func testWorkerAndDelegator(t *testing.T) (*mockWorker, *Delegator, func()) {
	t.Helper()
	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	worker := &mockWorker{key: pub, results: map[string]protocol.ResultState{}}
	server := httptest.NewServer(worker)
	delegator, err := NewDelegator(&protocol.Client{BaseURL: server.URL}, WorkerIdentity{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: "worker", Key: priv,
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker, delegator, server.Close
}

type mockPublisher struct {
	mu         sync.Mutex
	integrated []IntegrationRequest
	publishes  int
	commits    int

	integrateErr error
	publishErr   error
}

func (p *mockPublisher) Integrate(_ context.Context, req IntegrationRequest) (Integration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.integrateErr != nil {
		return Integration{}, p.integrateErr
	}
	p.commits++
	p.integrated = append(p.integrated, req)
	return Integration{Commit: fmt.Sprintf("commit-%d", p.commits), Changed: true}, nil
}

func (p *mockPublisher) Publish(_ context.Context) (Publication, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.publishErr != nil {
		return Publication{}, p.publishErr
	}
	p.publishes++
	return Publication{OID: fmt.Sprintf("remote-%d", p.publishes)}, nil
}

func (p *mockPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.publishes
}

// snapshotProvider returns a snapshot seam stub that always serves the same
// tip; the mock worker's unpack task echoes it back.
func snapshotProvider(tip string) SnapshotProvider {
	return func(context.Context) (Snapshot, error) {
		return Snapshot{Data: []byte("snapshot"), Tip: tip}, nil
	}
}

func sequenceSnapshotProvider(tips ...string) SnapshotProvider {
	var next atomic.Int32
	return func(context.Context) (Snapshot, error) {
		index := int(next.Add(1) - 1)
		if index >= len(tips) {
			index = len(tips) - 1
		}
		if index < 0 {
			return Snapshot{}, errors.New("sequence snapshot provider requires at least one tip")
		}
		tip := tips[index]
		return Snapshot{Data: []byte("snapshot-" + tip), Tip: tip}, nil
	}
}

// --- stream normalization --------------------------------------------------

func TestStreamNormalizesChunksIntoFixedVocabulary(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(
		contentChunk("think"),
		contentChunk("ing"),
		toolCallChunk(0, "call-1", "shell", `{"command":"ls`),
		toolCallChunk(0, "", "", ` -la"}`), // continuation fragment
		finishChunk("tool_calls"),
	)})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	var calls []ToolCall
	terminal := false
	for _, event := range events {
		switch event.Kind {
		case KindDelta:
			text.WriteString(event.Text)
		case KindToolRequest:
			calls = append(calls, event.Tool)
		case KindFinal:
			terminal = true
		default:
			t.Fatalf("unexpected event kind %q", event.Kind)
		}
	}
	if text.String() != "thinking" {
		t.Fatalf("delta text = %q", text.String())
	}
	if terminal {
		t.Fatal("a tool_calls finish must not carry a final event")
	}
	if len(calls) != 1 || calls[0].Name != "shell" || calls[0].Arguments != `{"command":"ls -la"}` {
		t.Fatalf("assembled tool call = %+v", calls)
	}
}

func TestStreamTruncatedIsAnError(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(contentChunk("partial"))})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindError || !errors.Is(last.Err, ErrStreamTruncated) {
		t.Fatalf("terminal event = %+v, want stream-truncated error", last)
	}
}

func TestStreamErrorChunkIsRedacted(t *testing.T) {
	// Both halves of the provider error object are provider-controlled body
	// data; neither may appear in the trusted error string.
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: "data: " + `{"error":{"message":"secret-key sk-abc","type":"sentinel-provider-type-x9"}}` + "\n\n"})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindError {
		t.Fatalf("terminal event = %+v", last)
	}
	for _, leaked := range []string{"secret-key", "sk-abc", "sentinel-provider-type-x9"} {
		if strings.Contains(last.Err.Error(), leaked) {
			t.Fatalf("error echoed provider payload %q: %v", leaked, last.Err)
		}
	}
}

func TestGatewayStatusErrorIsRedacted(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{status: http.StatusForbidden})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindError {
		t.Fatalf("terminal event = %+v", last)
	}
	if !strings.Contains(last.Err.Error(), "403") {
		t.Fatalf("error must carry the status: %v", last.Err)
	}
	if strings.Contains(last.Err.Error(), "super-secret-provider-detail") {
		t.Fatalf("error echoed the provider body: %v", last.Err)
	}
}

// --- acceptance: failed retry vs successful chunk --------------------------

func TestFailedStreamRetriesThenSuccessfulChunkEarnsActivity(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusServiceUnavailable},
		gatewayResponse{status: http.StatusBadGateway},
		gatewayResponse{body: sse(contentChunk("working"), finishChunk("stop"))},
	)
	defer cleanup()
	activity := &recordingActivity{}
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, activity)
	if err != nil {
		t.Fatal(err)
	}
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if got := mocker.requestCount(); got != 3 {
		t.Fatalf("gateway requests = %d, want 3 (two transient failures retried)", got)
	}
	if activity.count("stream") != 1 {
		t.Fatalf("stream activity = %d, want exactly one from the successful chunk", activity.count("stream"))
	}
	if activity.count("tool") != 0 {
		t.Fatal("a stream turn must not earn tool activity")
	}
}

func TestFailedStreamNeverEarnsActivityAndExhaustsRetries(t *testing.T) {
	script := make([]gatewayResponse, maxStreamRetries+1)
	for i := range script {
		script[i] = gatewayResponse{status: http.StatusServiceUnavailable}
	}
	gateway, mocker, cleanup := gatewayFor(script...)
	defer cleanup()
	activity := &recordingActivity{}
	session, _ := NewSession(gateway, testBindings(), RoleCoordinator, activity)
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error", last)
	}
	if activity.count("stream") != 0 || activity.count("tool") != 0 {
		t.Fatal("failed model streams must never earn activity")
	}
	if got := mocker.requestCount(); got != maxStreamRetries+1 {
		t.Fatalf("gateway requests = %d, want %d", got, maxStreamRetries+1)
	}
}

func TestPermanentGatewayStatusIsNotRetried(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(gatewayResponse{status: http.StatusNotFound})
	defer cleanup()
	session, _ := NewSession(gateway, testBindings(), RoleCoordinator, nil)
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v", last)
	}
	if got := mocker.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1 (client-class rejection is not transient)", got)
	}
}

// --- acceptance: duplicate brief -------------------------------------------

func TestDuplicateBriefIsRejectedWithoutWorkerContact(t *testing.T) {
	delegateOK := gatewayResponse{body: sse(
		contentChunk("delegating"),
		toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		finishChunk("tool_calls"),
	)}
	gateway, _, cleanup := gatewayFor(
		delegateOK,
		// The first brief really executes: its subagent runs one shell task.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo work"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk("work done"), finishChunk("stop"))},
		delegateOK, // duplicate brief ID
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.packArtifact = []byte("bundle-bytes")
	publisher := &mockPublisher{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Publisher: publisher,
		Snapshot:  snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	// The first brief ran its full chain — unpack, shell, pack — exactly
	// once; the duplicate was refused at the ledger before any second
	// worker contact.
	if got := worker.dispatchCount(); got != 3 {
		t.Fatalf("worker dispatches = %d, want 3 (unpack, shell, pack)", got)
	}
	if got := worker.dispatchRequests; got != 3 {
		t.Fatalf("worker dispatch requests = %d, want 3", got)
	}
	briefs := coordinator.Briefs().BriefIDs()
	if len(briefs) != 1 || briefs[0] != "b1" {
		t.Fatalf("brief ledger = %v", briefs)
	}
}

// --- acceptance: cancellation ----------------------------------------------

func TestCancelledBriefIsTombstonedForNewWork(t *testing.T) {
	delegateOK := gatewayResponse{body: sse(
		contentChunk("delegating"),
		toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		finishChunk("tool_calls"),
	)}
	cancelCall := gatewayResponse{body: sse(
		contentChunk("cancelling"),
		toolCallChunk(0, "call-2", toolCancelBrief, `{"id":"b1"}`),
		finishChunk("tool_calls"),
	)}
	gateway, _, cleanup := gatewayFor(
		delegateOK,
		gatewayResponse{body: sse(contentChunk("work done"), finishChunk("stop"))},
		cancelCall,
		delegateOK, // tombstoned work unit: rejected
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{Gateway: gateway, Bindings: testBindings()})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	if !coordinator.Briefs().Cancelled("b1") {
		t.Fatal("brief b1 must be tombstoned")
	}
}

func TestRegistryTombstoneRejectsReRegistration(t *testing.T) {
	registry := NewBriefRegistry()
	if _, err := registry.Register(Brief{ID: "b1", Role: "coder", Objective: "o", SuccessCheck: "s"}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(Brief{ID: "b1", Role: "coder", Objective: "o", SuccessCheck: "s"}); !errors.Is(err, ErrDuplicateBrief) {
		t.Fatalf("duplicate register error = %v", err)
	}
	if ok := registry.Cancelled("b1"); ok {
		t.Fatal("an active brief is not cancelled")
	}
	if _, err := registry.Cancel("b1"); err != nil {
		t.Fatal(err)
	}
	if !registry.Cancelled("b1") {
		t.Fatal("cancellation must tombstone the brief")
	}
	if _, err := registry.Cancel("missing"); !errors.Is(err, ErrBriefUnknown) {
		t.Fatalf("cancel of unknown brief error = %v", err)
	}
}

func TestDelegatorCancelBindsSameOpIDWithGreaterSequence(t *testing.T) {
	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	worker := &mockWorker{key: pub, results: map[string]protocol.ResultState{}}
	server := httptest.NewServer(worker)
	defer server.Close()
	delegator, err := NewDelegator(&protocol.Client{BaseURL: server.URL}, WorkerIdentity{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: "worker", Key: priv,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := delegator.Dispatch(ctx, "op-1", "brief-1", protocol.Task{Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	if err := delegator.Cancel(ctx, "op-1", "brief-1"); err != nil {
		t.Fatal(err)
	}
	if worker.cancelCount() != 1 {
		t.Fatalf("worker cancels = %d", worker.cancelCount())
	}
	cancel := worker.cancels[0]
	if cancel.OpID != "op-1" || cancel.Sequence != 1 || cancel.Kind != protocol.KindCancel {
		t.Fatalf("cancel envelope = %+v", cancel)
	}
	if cancel.RunUID != "run" || cancel.ControlPodUID != "control" || cancel.WorkerPodUID != "worker" {
		t.Fatalf("cancel envelope lost identity binding: %+v", cancel)
	}
}

// --- acceptance: ambiguous dispatch reconciles, never redelivers -----------

func TestAmbiguousDispatchReconcilesWithoutRedelivery(t *testing.T) {
	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	worker := &mockWorker{
		key:         pub,
		snapshotTip: "snap-tip",
		// The unpack dispatch succeeds; the shell dispatch is a
		// transport-class failure (5xx, not a definite 4xx rejection): the
		// worker may or may not have accepted.
		dispatchPlan: []int{0, http.StatusServiceUnavailable},
	}
	server := httptest.NewServer(worker)
	defer server.Close()
	delegator, err := NewDelegator(&protocol.Client{BaseURL: server.URL}, WorkerIdentity{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: "worker", Key: priv,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Drive the shell path directly: the outcome is a tool error carrying
	// the reconciled observation, and the shell was dispatched exactly once
	// (the unpack ran first) — no automatic redelivery — plus the
	// reconciling cancel.
	result := coordinator.runShell(context.Background(), shellCall("echo hi"), "b1", ToolResult{ToolCallID: "c1", Name: toolShell})
	if !result.IsError {
		t.Fatalf("shell result after ambiguous dispatch = %+v, want tool error", result)
	}
	if got := worker.dispatchRequests; got != 2 {
		t.Fatalf("worker dispatch requests = %d, want 2 (unpack + shell, no redelivery)", got)
	}
	if got := worker.cancelCount(); got != 1 {
		t.Fatalf("worker cancels = %d, want the reconciling cancel", got)
	}
}

// --- acceptance: unavailable forge ------------------------------------------

func capabilityBroker(report map[string]any, status int) *BrokerProbeClient {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 0 {
			w.WriteHeader(status)
			fmt.Fprint(w, "internal: token sk-abc leaked")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(report); err != nil {
			panic(err)
		}
	}))
	return &BrokerProbeClient{BaseURL: server.URL, TokenSource: NewStaticBrokerTokenSource([]byte("tok")), HTTP: server.Client()}
}

func TestCapabilityHealthNamesUnavailableProvider(t *testing.T) {
	deps := ProbeDeps{
		Broker: capabilityBroker(map[string]any{
			"provider": map[string]any{"name": "acme-forge", "type": "github", "endpoint": "https://api.acme.example/"},
			"operations": []map[string]any{
				{"name": "read-work-item", "available": false, "detail": "credential rejected"},
				{"name": "create-pull-request", "available": true},
				{"name": "update-pull-request", "available": true},
				{"name": "comment", "available": true},
			},
			"git": map[string]any{"endpoint": "https://git.acme.example/%s.git", "ready": true},
		}, 0),
		Mode: "resolve-issue",
	}
	caps := ProbeCapabilities(context.Background(), deps)
	forgeCap, ok := findCapability(caps, "forge-operations")
	if !ok {
		t.Fatalf("capability table = %+v", caps)
	}
	if forgeCap.State != StateUnavailable || !forgeCap.Required {
		t.Fatalf("forge capability = %+v", forgeCap)
	}
	if !strings.Contains(forgeCap.Detail, "acme-forge") || !strings.Contains(forgeCap.Detail, "read-work-item") {
		t.Fatalf("forge capability must name the provider and operation: %+v", forgeCap)
	}
	if _, err := Gate(caps); err == nil {
		t.Fatal("the gate must fail closed on a required unavailable forge capability")
	} else if !strings.Contains(err.Error(), "acme-forge") {
		t.Fatalf("gate error must name the provider: %v", err)
	}
}

func TestCapabilityHealthBrokerUnreachableUsesSafeReason(t *testing.T) {
	deps := ProbeDeps{
		Broker: capabilityBroker(nil, http.StatusServiceUnavailable),
		Mode:   "resolve-issue",
	}
	caps := ProbeCapabilities(context.Background(), deps)
	forgeCap, _ := findCapability(caps, "forge-operations")
	if forgeCap.State != StateUnavailable {
		t.Fatalf("forge capability = %+v", forgeCap)
	}
	if strings.Contains(forgeCap.Detail, "sk-abc") {
		t.Fatalf("unavailable detail echoed the provider body: %+v", forgeCap)
	}
}

func TestOptionalRoleDegradedDoesNotGate(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{status: http.StatusServiceUnavailable})
	defer cleanup()
	deps := ProbeDeps{
		Gateway:  gateway,
		Bindings: testBindings(),
		Broker: capabilityBroker(map[string]any{
			"provider": map[string]any{"name": "acme-forge"},
			"operations": []map[string]any{
				{"name": "read-work-item", "available": true},
				{"name": "create-pull-request", "available": true},
				{"name": "update-pull-request", "available": true},
				{"name": "comment", "available": true},
			},
			"git": map[string]any{"ready": true},
		}, 0),
		ModelProbe: func(_ context.Context, model string) error {
			if model == "test/coder" {
				return errors.New("gateway unavailable")
			}
			return nil
		},
		PublicationPresent: true,
		SnapshotPresent:    true,
	}
	caps := ProbeCapabilities(context.Background(), deps)
	if _, err := Gate(caps); err != nil {
		t.Fatalf("gate must pass with only optional capabilities down: %v", err)
	}
	coderCap, ok := findCapability(caps, "model-binding:coder")
	if !ok || coderCap.State != StateUnavailable || coderCap.Required {
		t.Fatalf("optional degraded capability = %+v, want named and optional", coderCap)
	}
	coordCap, ok := findCapability(caps, "model-binding:coordinator")
	if !ok || coordCap.State != StateHealthy || !coordCap.Required {
		t.Fatalf("coordinator binding = %+v, want healthy and required", coordCap)
	}
}

func findCapability(caps []Capability, name string) (Capability, bool) {
	for _, capability := range caps {
		if capability.Name == name {
			return capability, true
		}
	}
	return Capability{}, false
}

// --- coordinator: subagents route through the worker and cannot publish ----

func TestSubagentShellRoutesThroughWorkerAndOnlyControlPublishes(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.packArtifact = []byte("bundle-bytes")

	gateway, _, cleanup := gatewayFor(
		// Coordinator: delegate to coder.
		gatewayResponse{body: sse(
			contentChunk("delegating"),
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
			finishChunk("tool_calls"),
		)},
		// Coder sub-agent: run shell on the worker.
		gatewayResponse{body: sse(
			contentChunk("working"),
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo I published PR #999"}`),
			finishChunk("tool_calls"),
		)},
		// Coder sub-agent: done.
		gatewayResponse{body: sse(contentChunk("work done"), finishChunk("stop"))},
		// Coordinator: declare changes.
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	defer cleanup()

	publisher := &mockPublisher{}
	activity := &recordingActivity{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: publisher,
		Activity:  activity,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeChanges || result.Err != nil {
		t.Fatalf("harness result = %+v, want declared changes", result)
	}
	// The brief ran unpack, shell, and pack in order, all through the
	// signed worker protocol — never locally — with control verifying each
	// termination.
	if got := worker.dispatchCount(); got != 3 {
		t.Fatalf("worker dispatches = %d, want 3 (unpack, shell, pack)", got)
	}
	unpack := worker.dispatches[0]
	if !isWorkerUnpackTask(unpack.task) || unpack.envelope.BriefID != "b1" {
		t.Fatalf("unpack dispatch = %+v", unpack)
	}
	record := worker.dispatches[1]
	if strings.Join(record.task.Command, " ") != "sh -c echo I published PR #999" {
		t.Fatalf("worker task command = %v", record.task.Command)
	}
	if record.envelope.BriefID != "b1" {
		t.Fatalf("dispatch brief binding = %q", record.envelope.BriefID)
	}
	if record.envelope.Sequence != 0 || record.envelope.Kind != protocol.KindDispatch {
		t.Fatalf("dispatch envelope = %+v", record.envelope)
	}
	if pack := worker.dispatches[2]; !isWorkerPackTask(pack.task) || pack.envelope.BriefID != "b1" {
		t.Fatalf("pack dispatch = %+v", pack)
	}
	// The worker's claim of publication is untrusted data: integration and
	// publication happened exactly once from trusted control's own chain,
	// bound to the dispatched snapshot tip — plus the idempotent finish
	// confirmation.
	if len(publisher.integrated) != 1 {
		t.Fatalf("integrations = %+v, want exactly 1", publisher.integrated)
	}
	if publisher.integrated[0].BriefID != "b1" || publisher.integrated[0].DispatchedTip != "snap-tip" {
		t.Fatalf("integration request = %+v", publisher.integrated[0])
	}
	if got := publisher.count(); got != 2 {
		t.Fatalf("publisher calls = %d, want 2 (per-brief publication + finish confirmation)", got)
	}
	// Every verified boundary earned activity; the model streams earned
	// stream activity.
	if activity.count("tool") != 3 {
		t.Fatalf("tool activity = %d, want 3 verified boundaries (unpack, shell, pack)", activity.count("tool"))
	}
	if activity.count("stream") < 1 {
		t.Fatal("successful model streams must earn stream activity")
	}
}

func TestWorkerPublicationClaimWithoutDeclaredChangesNeverPublishes(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	// No packArtifact: the brief produced no committed work, so nothing
	// integrates and nothing publishes — whoever claims otherwise.
	gateway, _, cleanup := gatewayFor(
		// Coordinator: delegate.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Work","successCheck":"ok"}`),
			finishChunk("tool_calls"),
		)},
		// Coder: shell.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"git push"}`),
			finishChunk("tool_calls"),
		)},
		// Coder: claims publication in its summary.
		gatewayResponse{body: sse(contentChunk("I published the PR already"), finishChunk("stop"))},
		// Coordinator: blocked, not changes.
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"review"}`), finishChunk("stop"))},
	)
	defer cleanup()
	publisher := &mockPublisher{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway: gateway, Bindings: testBindings(), Worker: delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	if publisher.count() != 0 {
		t.Fatal("a worker or sub-agent publication claim must never trigger the publisher")
	}
}

func TestUndeclaredEndingFailsIncomplete(t *testing.T) {
	gateway, _, cleanup := gatewayFor(
		gatewayResponse{body: sse(contentChunk("I think I am done?"), finishChunk("stop"))},
		// Clarification retry, still not a declaration.
		gatewayResponse{body: sse(contentChunk("no really"), finishChunk("stop"))},
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{Gateway: gateway, Bindings: testBindings()})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err != nil || result.Outcome != "" {
		t.Fatalf("undeclared ending must not carry an outcome or infra error: %+v", result)
	}
	if !strings.Contains(result.Reason, "undeclared") {
		t.Fatalf("reason = %q", result.Reason)
	}
}

// --- delegator against the real honest worker listener ----------------------

func TestDelegatorEndToEndWithRealWorker(t *testing.T) {
	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	uid := fmt.Sprintf("worker-%d", time.Now().UnixNano())
	realWorker, err := protocol.NewWorker(protocol.WorkerConfig{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: uid, PublicKey: pub,
		WorkspaceDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(realWorker.Handler())
	defer server.Close()
	delegator, err := NewDelegator(&protocol.Client{BaseURL: server.URL}, WorkerIdentity{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: uid, Key: priv,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	opID := "shell.b1.test"
	if _, err := delegator.Dispatch(ctx, opID, "b1", protocol.Task{Command: []string{"sh", "-c", "printf ok"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, err := delegator.Result(ctx, opID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Status == protocol.ResultCompleted {
			if state.Result == nil || state.Result.StdoutTail != "ok" {
				t.Fatalf("worker result = %+v", state.Result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker task did not complete")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// A same-ID redispatch is a definite conflict: no redelivery path.
	if _, err := delegator.Dispatch(ctx, opID, "b1", protocol.Task{Command: []string{"true"}}); err == nil {
		t.Fatal("redispatch of a live opID must be rejected")
	}
}

// --- review-round regressions ----------------------------------------------

// The outbound transcript must serialize assistant tool requests in the
// OpenAI-compatible tool_calls shape; a wrong shape loses the
// request/result linkage at real gateways and is invisible to a mock that
// decodes into the same Go structs.
func TestOutboundToolCallsUseOpenAIWireShape(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", "shell", `{"command":"ls"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	// Drive one turn by hand so the transcript contains an assistant turn
	// with tool requests followed by a tool result.
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil)
	if err != nil {
		t.Fatal(err)
	}
	events := session.Turn(context.Background(), nil)
	var calls []ToolCall
	for _, event := range events {
		if event.Kind == KindToolRequest {
			calls = append(calls, event.Tool)
		}
	}
	session.Append(AssistantMessage("working", calls))
	session.Append(ToolResultMessage(ToolResult{ToolCallID: "call-1", Name: "shell", Content: "out"}))
	session.Turn(context.Background(), nil)

	if len(mocker.calls) != 2 {
		t.Fatalf("gateway requests = %d, want 2", len(mocker.calls))
	}
	body := mocker.calls[1].rawBody
	if !strings.Contains(body, `"tool_calls":[{"id":"call-1","type":"function","function":{"name":"shell"`) {
		t.Fatalf("second request does not carry the OpenAI tool_calls shape: %s", body)
	}
	if !strings.Contains(body, `"tool_call_id":"call-1"`) {
		t.Fatalf("tool result message lost its tool_call_id linkage: %s", body)
	}
	if strings.Contains(body, `"toolCalls"`) {
		t.Fatalf("request leaks the internal toolCalls field: %s", body)
	}
}

// The coordinator declares delegation tools; subagent sessions must not.
func TestToolDeclarationsMatchSessionContext(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	gateway, mocker, cleanup := gatewayFor(
		// Coordinator: delegate.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Work","successCheck":"ok"}`),
			finishChunk("tool_calls"),
		)},
		// Coder: shell.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo hi"}`),
			finishChunk("tool_calls"),
		)},
		// Coder: done.
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))},
		// Coordinator: undeclared ending is fine; the assertions below are
		// about declarations.
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway: gateway, Bindings: testBindings(), Worker: delegator,
		Snapshot: snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.Run(context.Background(), testInvocation())

	coordinatorTools := mocker.calls[0].toolNames
	if !contains(coordinatorTools, toolDelegate) || !contains(coordinatorTools, toolCancelBrief) || !contains(coordinatorTools, toolShell) {
		t.Fatalf("coordinator toolset = %v, want delegate, cancel_brief, shell", coordinatorTools)
	}
	subagentTools := mocker.calls[1].toolNames
	if contains(subagentTools, toolDelegate) || contains(subagentTools, toolCancelBrief) {
		t.Fatalf("subagent toolset leaks privileged tools: %v", subagentTools)
	}
	// The subagent receives the bounded delegate framing, never the
	// coordinator's terminal-ownership framing.
	subagentBody := mocker.calls[1].rawBody
	if !strings.Contains(subagentBody, "bounded delegate") {
		t.Fatal("the subagent session does not carry the delegate framing")
	}
	if strings.Contains(subagentBody, "You are the trusted coordinator") {
		t.Fatal("the subagent session carries the coordinator framing")
	}
	coordinatorBody := mocker.calls[0].rawBody
	if !strings.Contains(coordinatorBody, "You are the trusted coordinator") {
		t.Fatal("the coordinator session lost its trusted framing")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// A prompt-injected subagent emitting privileged tool names gets refusals,
// never nested delegation or cross-brief tombstones.
func TestSubagentCannotDelegateOrCancel(t *testing.T) {
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.briefs.Register(Brief{ID: "b1", Role: "coder", Objective: "o", SuccessCheck: "s"}); err != nil {
		t.Fatal(err)
	}
	delegateResult := coordinator.runTool(context.Background(), delegateCall("b2", "coder"), "b1")
	if !delegateResult.IsError || !strings.Contains(delegateResult.Content, "not available to sub-agents") {
		t.Fatalf("subagent delegate result = %+v", delegateResult)
	}
	cancelResult := coordinator.runTool(context.Background(), ToolCall{ID: "c2", Name: toolCancelBrief, Arguments: `{"id":"b1"}`}, "b1")
	if !cancelResult.IsError || !strings.Contains(cancelResult.Content, "not available to sub-agents") {
		t.Fatalf("subagent cancel result = %+v", cancelResult)
	}
	if coordinator.Briefs().Cancelled("b1") {
		t.Fatal("a subagent must not be able to tombstone a brief")
	}
	if len(coordinator.Briefs().BriefIDs()) != 1 {
		t.Fatal("a subagent must not be able to register a brief")
	}
}

// A stream that emitted deltas and then failed is a failed stream: it earns
// no activity, and its transient retry does not either.
func TestFailedStreamWithPartialDeltasEarnsNoActivity(t *testing.T) {
	gateway, _, cleanup := gatewayFor(
		gatewayResponse{body: sse(contentChunk("partial thought"), contentChunk(" more"), contentChunk(" still more"))},
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))},
	)
	defer cleanup()
	activity := &recordingActivity{}
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, activity)
	if err != nil {
		t.Fatal(err)
	}
	// The first stream truncates (no finish reason): deltas arrived but the
	// turn failed.
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error", last)
	}
	if activity.count("stream") != 0 {
		t.Fatalf("a failed stream with partial deltas earned activity %d times", activity.count("stream"))
	}
	events = session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if activity.count("stream") != 1 {
		t.Fatalf("the successful retry earned %d stream events, want 1", activity.count("stream"))
	}
}

// A result poll that fails transiently must not abandon a live operation:
// control reconciles through cancellation before giving up.
func TestResultPollFailureReconciles(t *testing.T) {
	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	worker := &mockWorker{
		key:         pub,
		completeAll: true,
		// The failure is scripted for the shell's own poll; the snapshot
		// prepare is skipped below so the unpack dispatch never happens.
		resultFailures: 1,
		results:        map[string]protocol.ResultState{},
	}
	server := httptest.NewServer(worker)
	defer server.Close()
	delegator, err := NewDelegator(&protocol.Client{BaseURL: server.URL}, WorkerIdentity{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: "worker", Key: priv,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: snapshotProvider("snap-tip"),
		Activity: &recordingActivity{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.dispatchAndAwait(context.Background(), "shell.b1.test", "b1", protocol.Task{Command: []string{"true"}})
	if err != nil {
		t.Fatalf("dispatchAndAwait after a failed poll = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("reconciled result = %+v", result)
	}
	if got := worker.cancelCount(); got != 1 {
		t.Fatalf("reconciling cancels = %d, want 1", got)
	}
}

// A transient snapshot upload failure must not be cached for the pod's life:
// the next task retries the upload and proceeds.
func TestSnapshotTransientFailureIsRetried(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	gateway, _, cleanup := gatewayFor(
		// Coordinator: shell; snapshot upload fails once, then succeeds.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolShell, `{"command":"echo one"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo two"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	attempts := 0
	worker.snapshotTip = "snap-tip"
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: func(context.Context) (Snapshot, error) {
			attempts++
			if attempts == 1 {
				return Snapshot{}, errors.New("snapshot render failed transiently")
			}
			return Snapshot{Data: []byte("snapshot"), Tip: "snap-tip"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	// The first shell failed at the snapshot and dispatched nothing; the
	// second retried the upload, ran the unpack, and dispatched its task —
	// the failure was not cached.
	if worker.dispatchCount() != 2 {
		t.Fatalf("worker dispatches = %d, want 2 (unpack + the retried task)", worker.dispatchCount())
	}
	if worker.snapshots != 1 {
		t.Fatalf("snapshot uploads = %d, want 1", worker.snapshots)
	}
}

// Oversized artifacts are bounded at the transcript boundary; bulk artifact
// handling stays with trusted validation (#125).
func TestOversizedArtifactIsNotInlined(t *testing.T) {
	result := workerResultToTool(protocol.Result{
		Status:   protocol.ResultCompleted,
		ExitCode: 0,
		Artifact: make([]byte, maxInlineArtifactBytes+1),
	}, ToolResult{ToolCallID: "c", Name: toolShell})
	if result.IsError {
		t.Fatal("a successful task with a large artifact is not an error")
	}
	if strings.Contains(result.Content, "not inlined") {
		return
	}
	t.Fatal("oversized artifact was inlined into the transcript")
}

// --- human-review regressions ----------------------------------------------

// A prompt-injected subagent emitting the forge tool name gets a refusal,
// never a broker call through trusted control.
func TestSubagentCannotCallForge(t *testing.T) {
	forge := &countingForge{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Forge:    forge,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Subagent context (briefID set): refused even when the forge seam is
	// configured; the coordinator context (briefID empty) goes through.
	result := coordinator.runTool(context.Background(), ToolCall{ID: "c1", Name: toolForge, Arguments: `{"operation":"read-work-item"}`}, "b1")
	if !result.IsError || !strings.Contains(result.Content, "not available to sub-agents") {
		t.Fatalf("subagent forge result = %+v", result)
	}
	if forge.calls != 0 {
		t.Fatal("a subagent forge request must never reach the forge seam")
	}
	coordinatorResult := coordinator.runTool(context.Background(), ToolCall{ID: "c2", Name: toolForge, Arguments: `{"operation":"read-work-item"}`}, "")
	if coordinatorResult.IsError {
		t.Fatalf("coordinator forge result = %+v", coordinatorResult)
	}
	if forge.calls != 1 {
		t.Fatalf("forge calls = %d, want 1", forge.calls)
	}
}

type countingForge struct{ calls int }

func (f *countingForge) Call(_ context.Context, _, _ string) (string, error) {
	f.calls++
	return "{}", nil
}

// The delegate toolset never offers forge reads, delegation, or
// cancellation, even when the forge seam is configured.
func TestSubagentToolsetExcludesForge(t *testing.T) {
	forge := &countingForge{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Forge:    forge,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinatorToolset := coordinator.coordinatorTools()
	if !containsTool(coordinatorToolset, toolForge) {
		t.Fatal("the coordinator toolset must offer forge reads when wired")
	}
	subagentToolset := coordinator.subagentTools()
	for _, forbidden := range []string{toolForge, toolDelegate, toolCancelBrief} {
		if containsTool(subagentToolset, forbidden) {
			t.Fatalf("subagent toolset leaks %q", forbidden)
		}
	}
}

func containsTool(tools []ToolDef, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

// A real transport failure (connection refused) is retried by the session
// loop and classified retryable by the probe machinery.
func TestTransportFailureIsRetriedAndClassifiedRetryable(t *testing.T) {
	var connections atomic.Int32
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connections.Add(1)
		// Close mid-response: a transport-class failure, not a status.
		panic(http.ErrAbortHandler)
	})}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()

	gateway := &Gateway{BaseURL: "http://" + listener.Addr().String()}
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil)
	if err != nil {
		t.Fatal(err)
	}
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error", last)
	}
	var transportErr *GatewayTransportError
	if !errors.As(lastErr(events), &transportErr) {
		t.Fatalf("terminal error = %v, want GatewayTransportError", lastErr(events))
	}
	if !isProbeRetryable(lastErr(events)) {
		t.Fatal("a transport failure must classify as retryable for probes")
	}
	if got := connections.Load(); got < 2 {
		t.Fatalf("connections = %d, want the transport failure retried", got)
	}
}

func lastErr(events []Event) error {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == KindError {
			return events[i].Err
		}
	}
	return nil
}

// The delegate's actual result is retained under its brief ID and is what
// publication planning consumes — never a fabricated objective.
func TestBriefResultRetention(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"ORIGINAL OBJECTIVE","successCheck":"ok"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo hi"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk("the actual delegate summary"), finishChunk("stop"))},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	defer cleanup()
	worker.snapshotTip = "snap-tip"
	worker.packArtifact = []byte("bundle-bytes")
	publisher := &mockPublisher{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeChanges {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	if len(mocker.calls) != 4 || !strings.Contains(mocker.calls[0].rawBody, "run relevant local validation against a fresh snapshot of the integrated and published head yourself") || !strings.Contains(mocker.calls[0].rawBody, "fix local validation failures before declaring completion") || !strings.Contains(mocker.calls[0].rawBody, "without waiting for external verification") {
		t.Fatalf("coordinator prompt does not carry the executor-neutral handoff contract: calls=%d prompt=%q", len(mocker.calls), mocker.calls[0].rawBody)
	}
	if len(publisher.integrated) != 1 {
		t.Fatalf("integrations = %+v", publisher.integrated)
	}
	if publisher.integrated[0].BriefID != "b1" || publisher.integrated[0].DispatchedTip != "snap-tip" {
		t.Fatalf("integration request = %+v", publisher.integrated[0])
	}
	retained, ok := coordinator.Briefs().Result("b1")
	if !ok {
		t.Fatal("brief b1 recorded no result")
	}
	if !strings.Contains(retained.Summary, "the actual delegate summary") {
		t.Fatalf("retained result = %+v, want the delegate's actual summary", retained)
	}
	if retained.Commit != "commit-1" {
		t.Fatalf("retained commit = %q, want the integration commit", retained.Commit)
	}
	if publisher.count() != 2 {
		t.Fatalf("publisher calls = %d, want 2 (per-brief + finish)", publisher.count())
	}
}

func TestCoordinatorShellRefreshesPublishedIntegrationSnapshot(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTips = []string{"brief-base", "published-head", "published-head"}
	worker.snapshotTip = "published-head"
	worker.packArtifact = []byte("bundle-bytes")
	gateway, _, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "delegate", toolDelegate, `{"id":"b1","role":"coder","objective":"Implement","successCheck":"tests pass"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			toolCallChunk(0, "brief-shell", toolShell, `{"command":"echo implement"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk("implemented"), finishChunk("stop"))},
		gatewayResponse{body: sse(
			toolCallChunk(0, "validate", toolShell, `{"command":"go test ./..."}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	defer cleanup()
	publisher := &mockPublisher{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway: gateway, Bindings: testBindings(), Worker: delegator,
		Snapshot:  sequenceSnapshotProvider("brief-base", "published-head", "published-head"),
		Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeChanges || result.Err != nil {
		t.Fatalf("harness result = %+v, want published changes", result)
	}
	if len(worker.dispatches) != 5 {
		t.Fatalf("worker dispatches = %d, want brief unpack/shell/pack and control unpack/validation", len(worker.dispatches))
	}
	if !isWorkerUnpackTask(worker.dispatches[0].task) || worker.dispatches[0].envelope.BriefID != "b1" {
		t.Fatalf("brief unpack = %+v", worker.dispatches[0])
	}
	if !isWorkerPackTask(worker.dispatches[2].task) || worker.dispatches[2].envelope.BriefID != "b1" {
		t.Fatalf("brief pack = %+v", worker.dispatches[2])
	}
	if !isWorkerUnpackTask(worker.dispatches[3].task) || worker.dispatches[3].envelope.BriefID != "control" {
		t.Fatalf("control refresh = %+v", worker.dispatches[3])
	}
	if got := strings.Join(worker.dispatches[4].task.Command, " "); got != "sh -c go test ./..." || worker.dispatches[4].envelope.BriefID != "control" {
		t.Fatalf("validation dispatch = %+v, want validation on control snapshot", worker.dispatches[4])
	}
	if worker.snapshots != 2 {
		t.Fatalf("snapshot uploads = %d, want brief base and a fresh published head", worker.snapshots)
	}
	if len(publisher.integrated) != 1 || publisher.integrated[0].DispatchedTip != "brief-base" {
		t.Fatalf("integration requests = %+v, want artifact anchored to brief-base", publisher.integrated)
	}
}

// Verified tool completions are emitted as KindToolResult events — the §5
// vocabulary is real, not declared-but-dead.
func TestToolResultEventsAreEmitted(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	gateway, _, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Work","successCheck":"ok"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo hi"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Drive the coordinator loop; the pump emits tool-result events for the
	// shell execution and the coordinator turn.
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil)
	if err != nil {
		t.Fatal(err)
	}
	turn := coordinator.pump(context.Background(), session, "", coordinator.coordinatorTools())
	if turn.streamErr != nil || turn.final {
		t.Fatalf("first turn = %+v", turn)
	}
	if len(turn.toolResults) != 1 || turn.toolResults[0].Kind != KindToolResult {
		t.Fatalf("tool-result events = %+v, want one emitted KindToolResult", turn.toolResults)
	}
	if turn.toolResults[0].Result.Content == "" {
		t.Fatal("the emitted tool result carries no content")
	}
}

// An unbound role must not burn the stable brief ID: the binding is
// validated before registration, so the model can re-delegate the same work
// unit with a corrected request.
func TestUnboundRoleDoesNotBurnBriefID(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	gateway, _, cleanup := gatewayFor(
		// Coordinator delegates to a role no model is bound to.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"reviewer","objective":"Work","successCheck":"ok"}`),
			finishChunk("tool_calls"),
		)},
		// Coordinator re-delegates the SAME ID with a bound role.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolDelegate, `{"id":"b1","role":"coder","objective":"Work","successCheck":"ok"}`),
			finishChunk("tool_calls"),
		)},
		// Coder runs shell.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-3", toolShell, `{"command":"echo hi"}`),
			finishChunk("tool_calls"),
		)},
		// Coder finishes.
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(), // coordinator + coder bound; reviewer is not
		Worker:   delegator,
		Snapshot: snapshotProvider("snap-tip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	briefs := coordinator.Briefs().BriefIDs()
	if len(briefs) != 1 || briefs[0] != "b1" {
		t.Fatalf("brief ledger = %v, want exactly the re-delegated b1", briefs)
	}
	if worker.dispatchCount() != 2 {
		t.Fatalf("worker dispatches = %d, want 2 (the corrected delegation's unpack and shell)", worker.dispatchCount())
	}
}

// --- acceptance: projected broker token rotates -----------------------------

// authRecorder is a broker stand-in that records the Authorization header of
// every request it serves, so a test can prove which token bytes reached the
// wire rather than which bytes a client happens to hold.
type authRecorder struct {
	mu     sync.Mutex
	auths  []string
	server *httptest.Server
}

func newAuthRecorder(t *testing.T) *authRecorder {
	t.Helper()
	rec := &authRecorder{}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.auths = append(rec.auths, r.Header.Get("Authorization"))
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/capabilities" {
			fmt.Fprint(w, `{"provider":{"name":"acme-forge","type":"github"},"operations":[],"git":{"endpoint":"https://git.acme.example/x.git","ready":true}}`)
			return
		}
		fmt.Fprint(w, `{"id":"req-1"}`)
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func (r *authRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.auths...)
}

// projectToken lays out a projected volume the way kubelet does: a
// timestamped payload directory, a `..data` symlink pointing at the current
// payload, and a stable `token` symlink through `..data`. It returns the token
// path — the path control reads on every request.
func projectToken(t *testing.T, root, payload, token string) string {
	t.Helper()
	dir := filepath.Join(root, payload)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "token"), token)
	swapProjectedData(t, root, payload)
	path := filepath.Join(root, "token")
	if err := os.Symlink(filepath.Join("..data", "token"), path); err != nil {
		t.Fatal(err)
	}
	return path
}

// rotateProjectedToken performs kubelet's rotation: a new payload directory
// and an atomic `..data` symlink swap over the old one. The stable `token`
// path keeps its name and identity, so a per-request read resolves the new
// bytes with no cache to invalidate.
func rotateProjectedToken(t *testing.T, root, payload, token string) {
	t.Helper()
	dir := filepath.Join(root, payload)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "token"), token)
	swapProjectedData(t, root, payload)
}

func swapProjectedData(t *testing.T, root, payload string) {
	t.Helper()
	next := filepath.Join(root, "..data_tmp")
	if err := os.Symlink(payload, next); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(next, filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
}

// TestBrokerTokenSourcePicksUpRotation is the #383 regression: control reads
// the projected broker token once at startup today, so kubelet's rotation
// replaces the mounted file while every broker client keeps authenticating
// with the bytes that have already expired. The same client and the same token
// source must present the replacement token on the very next request, with no
// identity rebuild and no run restart.
func TestBrokerTokenSourcePicksUpRotation(t *testing.T) {
	rec := newAuthRecorder(t)
	root := t.TempDir()
	// The projection writes the token verbatim; the presented bearer value
	// must still be trimmed, because the broker rejects one that is not.
	path := projectToken(t, root, "..2026_10_17_00_00_00", "token-one\n")
	source := NewFileBrokerTokenSource(path)
	client := &BrokerClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}
	probe := &BrokerProbeClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}

	if _, err := client.ControlRequest(context.Background()); err != nil {
		t.Fatalf("pre-rotation control read = %v, want the recorded request", err)
	}
	if _, err := probe.Capabilities(context.Background()); err != nil {
		t.Fatalf("pre-rotation capability read = %v, want the recorded request", err)
	}

	rotateProjectedToken(t, root, "..2026_10_17_00_10_00", "token-two\n")

	if _, err := client.ControlRequest(context.Background()); err != nil {
		t.Fatalf("post-rotation control read = %v, want the recorded request", err)
	}
	if _, err := probe.Capabilities(context.Background()); err != nil {
		t.Fatalf("post-rotation capability read = %v, want the recorded request", err)
	}

	want := []string{"Bearer token-one", "Bearer token-one", "Bearer token-two", "Bearer token-two"}
	got := rec.seen()
	if len(got) != len(want) {
		t.Fatalf("recorded authorizations = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("recorded authorizations = %q, want %q", got, want)
		}
	}
}

// TestBrokerTokenSourceFailsClosed pins the other half of #383: a projection
// control cannot read must produce a retryable local failure with a fixed
// diagnostic, no request on the wire, and no token bytes anywhere in the
// error. A run mid-rotation must wait for the new file, never send an
// unauthenticated or stale-authenticated request.
func TestBrokerTokenSourceFailsClosed(t *testing.T) {
	const secret = "sk-super-secret-token-one"
	rec := newAuthRecorder(t)
	root := t.TempDir()
	// Every case starts from a projection that has just served one
	// authenticated read, so the failure path runs on a client whose token
	// was demonstrably live a moment ago — a mid-rotation projection, not a
	// misconfigured test.
	healthy := filepath.Join(root, "healthy")
	writeFile(t, healthy, secret+"\n")
	empty := filepath.Join(root, "empty")
	writeFile(t, empty, "")
	blank := filepath.Join(root, "blank")
	writeFile(t, blank, "  \n\t ")
	cases := []struct {
		name   string
		source BrokerTokenSource
	}{
		{name: "missing file", source: NewFileBrokerTokenSource(filepath.Join(root, "absent"))},
		{name: "empty file", source: NewFileBrokerTokenSource(empty)},
		{name: "whitespace only", source: NewFileBrokerTokenSource(blank)},
		{name: "no source wired", source: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warm := &BrokerClient{BaseURL: rec.server.URL, TokenSource: NewFileBrokerTokenSource(healthy), HTTP: rec.server.Client()}
			if _, err := warm.ControlRequest(context.Background()); err != nil {
				t.Fatalf("warm-up read = %v, want the recorded request", err)
			}
			before := len(rec.seen())
			client := &BrokerClient{BaseURL: rec.server.URL, TokenSource: tc.source, HTTP: rec.server.Client()}
			_, err := client.ControlRequest(context.Background())
			if err == nil {
				t.Fatalf("control read with %s = nil, want a fail-closed error", tc.name)
			}
			var callErr *BrokerCallError
			if !errors.As(err, &callErr) || callErr.Category != "token is unavailable" || !callErr.Retryable {
				t.Fatalf("control read error = %v, want a retryable token-unavailable BrokerCallError", err)
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), root) {
				t.Fatalf("error text carries credential or path detail: %v", err)
			}
			probe := &BrokerProbeClient{BaseURL: rec.server.URL, TokenSource: tc.source, HTTP: rec.server.Client()}
			if _, err := probe.Capabilities(context.Background()); err == nil || err.Error() != "broker token is unavailable" {
				t.Fatalf("capability read error = %v, want the fixed probe diagnostic", err)
			}
			if after := len(rec.seen()); after != before {
				t.Fatalf("requests reaching the broker = %d, want %d: no request may be issued without a fresh token", after, before)
			}
		})
	}
}

// --- acceptance: a broker 401 during the kubelet token-swap window ----------

// funcTokenSource is a scripted BrokerTokenSource: every read consults f, so
// a test decides exactly which bytes each request — and each 401 re-read —
// sees.
type funcTokenSource func() ([]byte, error)

func (f funcTokenSource) BrokerToken() ([]byte, error) { return f() }

// swapBrokerStatus serves a broker stand-in that answers any route, records
// every Authorization header, and answers refusedStatus to every bearer except
// accepted — the apiserver's answer to a token it no longer honors (401) or
// a credential it will not authorize (403).
func swapBrokerStatus(t *testing.T, accepted string, refusedStatus int) *authRecorder {
	t.Helper()
	rec := &authRecorder{}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.auths = append(rec.auths, r.Header.Get("Authorization"))
		rec.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+accepted {
			w.WriteHeader(refusedStatus)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"req-1"}`)
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func swapBroker(t *testing.T, accepted string) *authRecorder {
	t.Helper()
	return swapBrokerStatus(t, accepted, http.StatusUnauthorized)
}

// noLeakedCredential fails the test when any text in the error chain carries
// a token byte string or one of the forbidden values: a rotation diagnostic
// describes the window, never the bytes or the paths behind them.
func noLeakedCredential(t *testing.T, err error, forbidden ...string) {
	t.Helper()
	if err == nil {
		return
	}
	if text := err.Error(); strings.Contains(text, "token-") {
		t.Fatalf("error text carries credential bytes: %v", err)
	}
	for _, secret := range forbidden {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error text carries forbidden detail: %v", err)
		}
	}
}

// TestBroker401DuringTokenSwapRetriesWithFreshToken is the #406 regression:
// a broker call landing between the apiserver refusing the on-disk token and
// kubelet swapping the projection earned a 401 that classified as a definite
// denial, blocking a healthy run behind a human. When the refused request's
// own credential has already been replaced on disk, the refusal is the swap
// window: retryable, and the next attempt carries the fresh bytes.
func TestBroker401DuringTokenSwapRetriesWithFreshToken(t *testing.T) {
	rec := swapBroker(t, "token-two")
	reads := 0
	source := funcTokenSource(func() ([]byte, error) {
		reads++
		if reads == 1 {
			return []byte("token-one"), nil
		}
		return []byte("token-two"), nil
	})
	client := &BrokerClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}
	ctx := context.Background()
	request, err := retried(ctx, func(ctx context.Context) (*courier.ControlRequest, error) {
		return client.ControlRequest(ctx)
	})
	noLeakedCredential(t, err)
	if err != nil || request == nil {
		t.Fatalf("control read across the swap window = (%v, %v), want success once the fresh token is mounted", request, err)
	}
	want := []string{"Bearer token-one", "Bearer token-two"}
	if got := rec.seen(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("recorded authorizations = %q, want %q", got, want)
	}
}

// TestBroker401WithoutRotationStaysADenial pins the other half of #406: the
// same 401 is what a misconfigured or cross-run caller earns, so it must stay
// a definite denial — a blocked publication, never an infrastructure blip —
// whenever the projected credential is unchanged since the refused request
// read it.
func TestBroker401WithoutRotationStaysADenial(t *testing.T) {
	rec := swapBroker(t, "never-accepted")
	root := t.TempDir()
	client := &BrokerClient{BaseURL: rec.server.URL, TokenSource: NewStaticBrokerTokenSource([]byte("token-one")), HTTP: rec.server.Client()}
	_, err := client.ControlRequest(context.Background())
	var callErr *BrokerCallError
	if !errors.As(err, &callErr) || callErr.StatusCode != http.StatusUnauthorized || callErr.Retryable {
		t.Fatalf("control read error = %v, want a non-retryable 401 BrokerCallError", err)
	}
	engine, buildErr := NewPublicationEngine(client, NewIntegrator(filepath.Join(root, "integration"), nil))
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	classified := engine.classify(err, "the publication")
	noLeakedCredential(t, classified, root, "token-one")
	var blocked *PublicationBlockedError
	var transient *TransientPublicationError
	if !errors.As(classified, &blocked) || errors.As(classified, &transient) {
		t.Fatalf("classified = %v, want a blocked publication", classified)
	}
}

// TestBroker401SpinAcrossRoundsStaysTransient pins the bounded-spin property:
// a credential the broker refuses while the projection keeps producing new
// bytes exhausts the bounded retry rounds and surfaces as an infrastructure
// failure that relaunching reconciles — never a blocked publication, and
// never an unbounded loop.
func TestBroker401SpinAcrossRoundsStaysTransient(t *testing.T) {
	rec := swapBroker(t, "never-accepted")
	reads := 0
	source := funcTokenSource(func() ([]byte, error) {
		reads++
		return []byte(fmt.Sprintf("token-%d", reads)), nil
	})
	client := &BrokerClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}
	err := retriedCall(context.Background(), func(ctx context.Context) error {
		return client.PostStatus(ctx, StatusPatch{})
	})
	noLeakedCredential(t, err)
	if !isBrokerRetryable(err) {
		t.Fatalf("post-swap-window error = %v, want a retryable classification", err)
	}
	// One send and one 401 re-read per attempt, bounded by the retry rounds.
	if reads != 2*(publishRetryRounds+1) {
		t.Fatalf("token reads = %d, want %d: the window must retry bounded, not spin", reads, 2*(publishRetryRounds+1))
	}
	engine, buildErr := NewPublicationEngine(client, NewIntegrator(filepath.Join(t.TempDir(), "integration"), nil))
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	classified := engine.classify(err, "the publication")
	noLeakedCredential(t, classified)
	var transient *TransientPublicationError
	var blocked *PublicationBlockedError
	if !errors.As(classified, &transient) || errors.As(classified, &blocked) {
		t.Fatalf("classified = %v, want a transient publication failure", classified)
	}
}

// TestTrustedStatusSurvivesTokenSwapWindow proves the trusted-status path
// inherits the per-request distinction: a status write landing in the swap
// window retries with the fresh bytes instead of persisting a failure that
// only a human could clear.
func TestTrustedStatusSurvivesTokenSwapWindow(t *testing.T) {
	rec := swapBroker(t, "token-two")
	reads := 0
	source := funcTokenSource(func() ([]byte, error) {
		reads++
		if reads == 1 {
			return []byte("token-one"), nil
		}
		return []byte("token-two"), nil
	})
	reporter := NewStatusReporter(&BrokerClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}, "control-uid", "worker-uid", 0)
	if err := reporter.BeginOperation(context.Background(), "op-1", "brief-1"); err != nil {
		noLeakedCredential(t, err)
		t.Fatalf("status write across the swap window = %v, want success once the fresh token is mounted", err)
	}
	want := []string{"Bearer token-one", "Bearer token-two"}
	if got := rec.seen(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("recorded authorizations = %q, want %q", got, want)
	}
}

// TestBroker401WithFailingRereadStaysADenial pins the loudness guard: when
// the re-read that would prove rotation fails — the projection is unreadable
// exactly when its bytes are being questioned — there is no evidence the
// request was rotated under, and the 401 stays a definite denial.
func TestBroker401WithFailingRereadStaysADenial(t *testing.T) {
	rec := swapBroker(t, "never-accepted")
	root := t.TempDir()
	reads := 0
	source := funcTokenSource(func() ([]byte, error) {
		reads++
		if reads == 1 {
			return []byte("token-one"), nil
		}
		return nil, errors.New("harness: broker token file is unreadable")
	})
	client := &BrokerClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}
	_, err := client.ControlRequest(context.Background())
	noLeakedCredential(t, err)
	var callErr *BrokerCallError
	if !errors.As(err, &callErr) || callErr.StatusCode != http.StatusUnauthorized || callErr.Retryable {
		t.Fatalf("control read error = %v, want a non-retryable 401 BrokerCallError", err)
	}
	if reads != 2 {
		t.Fatalf("token reads = %d, want 2 (the request and its one re-read)", reads)
	}
	engine, buildErr := NewPublicationEngine(client, NewIntegrator(filepath.Join(root, "integration"), nil))
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	classified := engine.classify(err, "the publication")
	noLeakedCredential(t, classified, root)
	var blocked *PublicationBlockedError
	var transient *TransientPublicationError
	if !errors.As(classified, &blocked) || errors.As(classified, &transient) {
		t.Fatalf("classified = %v, want a blocked publication", classified)
	}
}

// TestBroker403IsADenialEvenWithFreshBytes pins the 401-only guard: the
// rotation-window reclassification is scoped to the status a rotated-out
// credential earns. A 403 is an authorization refusal of whatever bytes the
// request carried, so it is never retried — and the projection is not even
// re-read, because its answer could not change the verdict.
func TestBroker403IsADenialEvenWithFreshBytes(t *testing.T) {
	rec := swapBrokerStatus(t, "never-accepted", http.StatusForbidden)
	reads := 0
	source := funcTokenSource(func() ([]byte, error) {
		reads++
		return []byte(fmt.Sprintf("token-%d", reads)), nil
	})
	client := &BrokerClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}
	_, err := client.ControlRequest(context.Background())
	noLeakedCredential(t, err)
	var callErr *BrokerCallError
	if !errors.As(err, &callErr) || callErr.StatusCode != http.StatusForbidden || callErr.Retryable {
		t.Fatalf("control read error = %v, want a non-retryable 403 BrokerCallError", err)
	}
	if reads != 1 {
		t.Fatalf("token reads = %d, want 1: a 403 must not consult the projection", reads)
	}
}

// TestProbe401DuringTokenSwapIsRetryable proves the launch gate follows the
// same per-request rule: a capabilities read refused with 401 retries when
// the projection has already replaced the refused bytes, so a swap window at
// launch reports unavailable capabilities rather than NeedsHuman — while a
// same-bytes 401 stays non-retryable and fails the gate loudly.
func TestProbe401DuringTokenSwapIsRetryable(t *testing.T) {
	cases := []struct {
		name      string
		accepted  string
		refused   int
		rotate    bool
		reads     int
		retryable bool
	}{
		{name: "rotation window", accepted: "token-two", refused: http.StatusUnauthorized, rotate: true, reads: 2, retryable: true},
		{name: "unchanged credential", accepted: "never-accepted", refused: http.StatusUnauthorized, rotate: false, reads: 2, retryable: false},
		{name: "authorization refusal", accepted: "never-accepted", refused: http.StatusForbidden, rotate: true, reads: 1, retryable: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := swapBrokerStatus(t, tc.accepted, tc.refused)
			reads := 0
			source := funcTokenSource(func() ([]byte, error) {
				reads++
				if tc.rotate && reads > 1 {
					return []byte("token-two"), nil
				}
				return []byte("token-one"), nil
			})
			probe := &BrokerProbeClient{BaseURL: rec.server.URL, TokenSource: source, HTTP: rec.server.Client()}
			_, err := probe.Capabilities(context.Background())
			noLeakedCredential(t, err)
			var probeErr *BrokerProbeError
			if !errors.As(err, &probeErr) || probeErr.Msg == "" {
				t.Fatalf("capability read error = %v, want a fixed probe diagnostic", err)
			}
			if probeErr.Retryable != tc.retryable {
				t.Fatalf("capability read retryable = %v, want %v", probeErr.Retryable, tc.retryable)
			}
			if reads != tc.reads {
				t.Fatalf("token reads = %d, want %d", reads, tc.reads)
			}
		})
	}
}

// --- broker transport reuse (#384) -----------------------------------------

// countingTLSServer is an httptest TLS server that counts the connections it
// opens and closes, and hands back its certificate as a CA PEM so a default
// broker client can trust it.
func countingTLSServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *atomic.Int64, *atomic.Int64, []byte) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	var opened, closed atomic.Int64
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			opened.Add(1)
		case http.StateClosed:
			closed.Add(1)
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	return server, &opened, &closed, caPEM
}

// waitForClosed polls until the server has observed want connection closes.
func waitForClosed(t *testing.T, closed *atomic.Int64, want int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if closed.Load() >= want {
			return
		}
	}
	t.Fatalf("server closed %d connections, want %d", closed.Load(), want)
}

func TestBrokerClientReusesOneTransportAcrossRequests(t *testing.T) {
	server, opened, closed, caPEM := countingTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/trusted/v1/status" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	client := &BrokerClient{BaseURL: server.URL, TokenSource: NewStaticBrokerTokenSource([]byte("tok")), CA: caPEM}
	for i := 1; i <= 5; i++ {
		if err := client.PostStatus(context.Background(), StatusPatch{}); err != nil {
			t.Fatalf("status post %d: %v", i, err)
		}
	}
	if got := opened.Load(); got != 1 {
		t.Fatalf("five sequential status posts opened %d TLS connections, want 1", got)
	}
	built, err := client.transport()
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := built.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client built transport of type %T, want *http.Transport", built.Transport)
	}
	if transport.IdleConnTimeout <= 0 || transport.MaxIdleConns <= 0 || transport.MaxIdleConnsPerHost <= 0 {
		t.Fatalf("idle bounds = timeout %d, max %d, max-per-host %d, want all finite",
			transport.IdleConnTimeout, transport.MaxIdleConns, transport.MaxIdleConnsPerHost)
	}
	client.CloseIdleConnections()
	waitForClosed(t, closed, 1)
}

func TestBrokerClientConcurrentRequestsShareOneTransport(t *testing.T) {
	server, opened, _, caPEM := countingTLSServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	client := &BrokerClient{BaseURL: server.URL, TokenSource: NewStaticBrokerTokenSource([]byte("tok")), CA: caPEM}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := client.PostStatus(context.Background(), StatusPatch{}); err != nil {
				t.Errorf("concurrent status post: %v", err)
			}
		}()
	}
	group.Wait()
	if got := opened.Load(); got == 0 || got > 8 {
		t.Fatalf("eight concurrent status posts opened %d TLS connections, want between 1 and 8", got)
	}
	first, err := client.transport()
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.transport()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("the client must retain one built transport, not build a new one per request")
	}
}

func TestBrokerProbeClientReusesOneTransportAcrossProbes(t *testing.T) {
	server, opened, closed, caPEM := countingTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/capabilities" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"provider":{"name":"acme-forge","type":"github"},"git":{"endpoint":"https://git.acme.example/acme.git","ready":true}}`)
	})
	client := &BrokerProbeClient{BaseURL: server.URL, TokenSource: NewStaticBrokerTokenSource([]byte("tok")), CA: caPEM}
	for i := 1; i <= 3; i++ {
		report, err := client.Capabilities(context.Background())
		if err != nil || report.Provider.Name != "acme-forge" {
			t.Fatalf("probe %d = %+v, err %v", i, report, err)
		}
	}
	if got := opened.Load(); got != 1 {
		t.Fatalf("three sequential capability probes opened %d TLS connections, want 1", got)
	}
	client.CloseIdleConnections()
	waitForClosed(t, closed, 1)
}

func TestSuppliedHTTPClientKeepsOwnership(t *testing.T) {
	server, _, closed, _ := countingTLSServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	supplied := server.Client()
	client := &BrokerClient{BaseURL: server.URL, TokenSource: NewStaticBrokerTokenSource([]byte("tok")), HTTP: supplied}
	if got, err := client.transport(); err != nil || got != supplied {
		t.Fatalf("transport() = %v, %v, want the supplied client itself", got, err)
	}
	if err := client.PostStatus(context.Background(), StatusPatch{}); err != nil {
		t.Fatal(err)
	}
	client.CloseIdleConnections()
	time.Sleep(200 * time.Millisecond)
	if got := closed.Load(); got != 0 {
		t.Fatalf("CloseIdleConnections closed %d connections on a supplied client, want 0: the supplier owns the pool", got)
	}
}
