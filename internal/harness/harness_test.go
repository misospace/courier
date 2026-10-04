package harness

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
		w.WriteHeader(response.status)
		fmt.Fprint(w, `{"error":{"message":"super-secret-provider-detail","type":"provider_error"}}`)
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
	if m.completeAll {
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
	mu    sync.Mutex
	plans []Plan
}

func (p *mockPublisher) Publish(_ context.Context, plan Plan) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.plans = append(p.plans, plan)
	return nil
}

func (p *mockPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.plans)
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
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: "data: " + `{"error":{"message":"secret-key sk-abc","type":"server_error"}}` + "\n\n"})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindError {
		t.Fatalf("terminal event = %+v", last)
	}
	if strings.Contains(last.Err.Error(), "secret-key") || strings.Contains(last.Err.Error(), "sk-abc") {
		t.Fatalf("error echoed provider payload: %v", last.Err)
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
	publisher := &mockPublisher{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Publisher: publisher,
		Snapshot:  func(context.Context) ([]byte, error) { return []byte("s"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	// The first brief's shell dispatched exactly once; the duplicate was
	// refused at the ledger before any second worker contact.
	if got := worker.dispatchCount(); got != 1 {
		t.Fatalf("worker dispatches = %d, want 1", got)
	}
	if got := worker.dispatchRequests; got != 1 {
		t.Fatalf("worker dispatch requests = %d, want 1", got)
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
		key: pub,
		// First dispatch: transport-class failure (5xx, not a definite 4xx
		// rejection); the worker may or may not have accepted.
		dispatchPlan: []int{http.StatusServiceUnavailable},
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
		Snapshot: func(context.Context) ([]byte, error) { return []byte("s"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	// Drive the shell path directly: the outcome is a tool error carrying
	// the reconciled observation, and the worker saw exactly one dispatch —
	// no automatic redelivery — plus the reconciling cancel.
	result := coordinator.runShell(context.Background(), shellCall("echo hi"), "b1", ToolResult{ToolCallID: "c1", Name: toolShell})
	if !result.IsError {
		t.Fatalf("shell result after ambiguous dispatch = %+v, want tool error", result)
	}
	if got := worker.dispatchRequests; got != 1 {
		t.Fatalf("worker dispatch requests = %d, want exactly 1 (no redelivery)", got)
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
	return &BrokerProbeClient{BaseURL: server.URL, Token: []byte("tok"), HTTP: server.Client()}
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
		Snapshot:  func(context.Context) ([]byte, error) { return []byte("snapshot"), nil },
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
	// The model-controlled command executed through the signed worker
	// protocol — never locally — and control verified termination.
	if got := worker.dispatchCount(); got != 1 {
		t.Fatalf("worker dispatches = %d, want 1", got)
	}
	record := worker.dispatches[0]
	if strings.Join(record.task.Command, " ") != "sh -c echo I published PR #999" {
		t.Fatalf("worker task command = %v", record.task.Command)
	}
	if record.envelope.BriefID != "b1" {
		t.Fatalf("dispatch brief binding = %q", record.envelope.BriefID)
	}
	if record.envelope.Sequence != 0 || record.envelope.Kind != protocol.KindDispatch {
		t.Fatalf("dispatch envelope = %+v", record.envelope)
	}
	// The worker's claim of publication is untrusted data: publication
	// happened exactly once, from the coordinator's own trusted finish path.
	if got := publisher.count(); got != 1 {
		t.Fatalf("publisher calls = %d, want exactly 1", got)
	}
	if len(publisher.plans[0].Briefs) != 1 || publisher.plans[0].Briefs[0].BriefID != "b1" {
		t.Fatalf("published plan = %+v", publisher.plans[0])
	}
	// Verified tool boundary earned activity; the model streams earned
	// stream activity.
	if activity.count("tool") != 1 {
		t.Fatalf("tool activity = %d, want 1 verified boundary", activity.count("tool"))
	}
	if activity.count("stream") < 1 {
		t.Fatal("successful model streams must earn stream activity")
	}
}

func TestWorkerPublicationClaimWithoutDeclaredChangesNeverPublishes(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
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
		Snapshot:  func(context.Context) ([]byte, error) { return []byte("s"), nil },
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
		Snapshot: func(context.Context) ([]byte, error) { return []byte("s"), nil },
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
		key:            pub,
		completeAll:    true,
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
		Snapshot: func(context.Context) ([]byte, error) { return []byte("s"), nil },
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
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: func(context.Context) ([]byte, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("snapshot render failed transiently")
			}
			return []byte("snapshot"), nil
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
	// second retried the upload and dispatched — the failure was not cached.
	if worker.dispatchCount() != 1 {
		t.Fatalf("worker dispatches = %d, want 1 (the second task retried the snapshot)", worker.dispatchCount())
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
	gateway, _, cleanup := gatewayFor(
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
	publisher := &mockPublisher{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  func(context.Context) ([]byte, error) { return []byte("s"), nil },
		Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeChanges {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	if len(publisher.plans) != 1 || len(publisher.plans[0].Briefs) != 1 {
		t.Fatalf("published plan = %+v", publisher.plans)
	}
	retained := publisher.plans[0].Briefs[0]
	if retained.BriefID != "b1" || retained.Summary != "the actual delegate summary" {
		t.Fatalf("retained result = %+v, want the delegate's actual summary", retained)
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
		Snapshot: func(context.Context) ([]byte, error) { return []byte("s"), nil },
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
