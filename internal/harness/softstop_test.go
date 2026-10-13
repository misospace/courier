package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- watcher seams ----------------------------------------------------------

type stubControlSource struct {
	mu       sync.Mutex
	requests []*courierv1alpha1.ControlRequest
	errs     []error
	calls    int
}

func (s *stubControlSource) ControlRequest(context.Context) (*courierv1alpha1.ControlRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.calls
	s.calls++
	if i < len(s.errs) && s.errs[i] != nil {
		return nil, s.errs[i]
	}
	if i < len(s.requests) {
		return s.requests[i], nil
	}
	return nil, nil
}

func (s *stubControlSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type stubAckWriter struct {
	mu       sync.Mutex
	ids      []string
	failures map[string]int
}

func (s *stubAckWriter) ControlRequestAck(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures != nil && s.failures[id] > 0 {
		s.failures[id]--
		return errors.New("status path is down")
	}
	s.ids = append(s.ids, id)
	return nil
}

func (s *stubAckWriter) ackedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ids...)
}

func controlRequest(id string) *courierv1alpha1.ControlRequest {
	return &courierv1alpha1.ControlRequest{
		ID: id, Kind: courierv1alpha1.ControlRequestSoftStop,
		TargetPodUID: "pod-uid-live", RequestedAt: metav1.NewTime(time.Now()),
	}
}

// --- watcher semantics --------------------------------------------------------

func TestControlTurnIsBoundedMarkedNote(t *testing.T) {
	if len(controlTurnText) > maxControlTurnBytes {
		t.Fatalf("control turn is %d bytes, over the %d cap", len(controlTurnText), maxControlTurnBytes)
	}
	if !strings.HasPrefix(controlTurnText, controlTurnPrefix) {
		t.Fatalf("control turn must carry the provenance marker: %q", controlTurnText)
	}
	// The note carries no dynamic identity: it is the same fixed string for
	// every request, so it cannot leak the request ID, a pod UID, or a run.
	for _, forbidden := range []string{"softstop-", "pod-uid", "req-"} {
		if strings.Contains(controlTurnText, forbidden) {
			t.Fatalf("control turn must not carry %q-style identity", forbidden)
		}
	}
}

func TestControlWatcherAckThenLatchThenInject(t *testing.T) {
	source := &stubControlSource{requests: []*courierv1alpha1.ControlRequest{controlRequest("req-1")}}
	ack := &stubAckWriter{}
	watcher := NewControlWatcher(source, ack)
	if watcher.Stopped() {
		t.Fatal("watcher starts unstopped")
	}
	watcher.Poll(context.Background())
	if !watcher.Stopped() {
		t.Fatal("a served request must flip the latch after its ack succeeds")
	}
	if got := ack.ackedIDs(); len(got) != 1 || got[0] != "req-1" {
		t.Fatalf("acked ids = %v, want exactly one ack of req-1", got)
	}
	// Redelivery of the consumed ID is a no-op: no second ack, no second
	// control turn.
	source.requests = append(source.requests, controlRequest("req-1"))
	watcher.Poll(context.Background())
	if got := ack.ackedIDs(); len(got) != 1 {
		t.Fatalf("redelivery acked again: %v", got)
	}
	note, ok := watcher.TakeControlTurn()
	if !ok || note.Role != "system" || !strings.HasPrefix(note.Content, controlTurnPrefix) {
		t.Fatalf("control turn = %+v, want one system note with the marker", note)
	}
	if _, ok := watcher.TakeControlTurn(); ok {
		t.Fatal("exactly one control turn per consumed request ID")
	}
}

func TestControlWatcherFailedAckConsumesNothing(t *testing.T) {
	source := &stubControlSource{requests: []*courierv1alpha1.ControlRequest{controlRequest("req-1"), controlRequest("req-1")}}
	ack := &stubAckWriter{failures: map[string]int{"req-1": 1}}
	watcher := NewControlWatcher(source, ack)
	watcher.Poll(context.Background())
	if watcher.Stopped() {
		t.Fatal("a request whose ack write failed must not consume or latch")
	}
	if _, ok := watcher.TakeControlTurn(); ok {
		t.Fatal("a request whose ack write failed must not queue a control turn")
	}
	// The next boundary retries and succeeds.
	watcher.Poll(context.Background())
	if !watcher.Stopped() {
		t.Fatal("the retry must consume once the ack succeeds")
	}
	if got := ack.ackedIDs(); len(got) != 1 || got[0] != "req-1" {
		t.Fatalf("acked ids = %v, want one successful ack of req-1", got)
	}
}

func TestControlWatcherWithoutAckPathNeverConsumes(t *testing.T) {
	source := &stubControlSource{requests: []*courierv1alpha1.ControlRequest{controlRequest("req-1")}}
	watcher := NewControlWatcher(source, nil)
	watcher.Poll(context.Background())
	if watcher.Stopped() {
		t.Fatal("a watcher with no durable acknowledgement path must never consume")
	}
}

func TestControlWatcherThrottleSpacesReads(t *testing.T) {
	source := &stubControlSource{}
	watcher := NewControlWatcher(source, &stubAckWriter{})
	now := time.Now()
	watcher.now = func() time.Time { return now }
	watcher.PollThrottled(context.Background())
	watcher.PollThrottled(context.Background())
	if got := source.readCount(); got != 1 {
		t.Fatalf("reads within the throttle window = %d, want 1", got)
	}
	now = now.Add(controlPollMinInterval + time.Second)
	watcher.PollThrottled(context.Background())
	if got := source.readCount(); got != 2 {
		t.Fatalf("reads after the throttle window = %d, want 2", got)
	}
}

// --- brief ledger predicate ---------------------------------------------------

func TestHasInFlightTracksTerminalStates(t *testing.T) {
	registry := NewBriefRegistry()
	if registry.HasInFlight() {
		t.Fatal("empty ledger has nothing in flight")
	}
	if _, err := registry.Register(Brief{ID: "b1", Role: "coder", Objective: "o", SuccessCheck: "s"}); err != nil {
		t.Fatal(err)
	}
	if !registry.HasInFlight() {
		t.Fatal("a registered brief is in flight")
	}
	if _, err := registry.Register(Brief{ID: "b2", Role: "coder", Objective: "o", SuccessCheck: "s"}); err != nil {
		t.Fatal(err)
	}
	registry.RecordResult("b1", BriefResult{Summary: "done"})
	if !registry.HasInFlight() {
		t.Fatal("b2 is still in flight")
	}
	// A failed brief ends its work unit without a checkpoint result; the
	// soft-stop exit must not wait on it forever.
	registry.MarkTerminal("b2")
	if registry.HasInFlight() {
		t.Fatal("a terminal brief is no longer in flight")
	}
	if got := registry.CompletedBriefs(); len(got) != 1 || got[0].ID != "b1" {
		t.Fatalf("completed briefs = %+v, want only the recorded result", got)
	}
}

// --- coordinator soft-stop paths ----------------------------------------------

type stubPublisher struct {
	mu        sync.Mutex
	publishes int
	integrate int
	result    Publication
	err       error
}

func (p *stubPublisher) Integrate(context.Context, IntegrationRequest) (Integration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.integrate++
	return Integration{}, errors.New("not used in soft-stop tests")
}

func (p *stubPublisher) Publish(context.Context) (Publication, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishes++
	return p.result, p.err
}

func (p *stubPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.publishes
}

func TestSoftStopLatchBlocksNewBriefsOnly(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{body: sse(toolCallChunk(0, "c1", toolDelegate, `{"id":"b1","role":"coder","objective":"o","successCheck":"s"}`), finishChunk("tool_calls"))},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	defer cleanup()
	watcher := NewControlWatcher(&stubControlSource{requests: []*courierv1alpha1.ControlRequest{controlRequest("req-1")}}, &stubAckWriter{})
	watcher.Poll(context.Background()) // the request landed before the first turn
	publisher := &stubPublisher{result: Publication{OID: "abc123"}}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway: gateway, Bindings: testBindings(),
		Control: watcher, Publisher: publisher,
		WorkRefBeyondAnchor: func(context.Context) (bool, error) { return true, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != "changes" {
		t.Fatalf("outcome = %v, want changes from the world-derived finish: %+v", result.Outcome, result)
	}
	// The delegate tool result the model saw must name the latch, and the
	// transcript's first request must carry the injected control note.
	var sawRefusal, sawNote bool
	mocker.mu.Lock()
	for _, call := range mocker.calls {
		if strings.Contains(call.rawBody, "soft stop is in effect") {
			sawRefusal = true
		}
		if strings.Contains(call.rawBody, controlTurnPrefix) {
			sawNote = true
		}
	}
	mocker.mu.Unlock()
	if !sawRefusal {
		t.Fatal("the latch must refuse new briefs with a model-visible reason")
	}
	if !sawNote {
		t.Fatal("the consumed request must inject its control turn into the coordinator transcript")
	}
	// The declared "changes" is ignored: the world-derived publish ran once.
	if publisher.count() != 1 {
		t.Fatalf("publishes = %d, want the one world-derived publication", publisher.count())
	}
}

func TestSoftStopFinishDerivesOutcomeFromWorld(t *testing.T) {
	t.Run("beyond-anchor-publishes-changes", func(t *testing.T) {
		publisher := &stubPublisher{result: Publication{OID: "abc123"}}
		coordinator, err := NewCoordinator(CoordinatorConfig{
			Gateway: &Gateway{BaseURL: "http://unused.invalid"}, Bindings: testBindings(),
			Publisher:           publisher,
			WorkRefBeyondAnchor: func(context.Context) (bool, error) { return true, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		result := coordinator.finishSoftStop(context.Background())
		if result.Err != nil || result.Outcome != "changes" {
			t.Fatalf("result = %+v, want changes", result)
		}
		if publisher.count() != 1 {
			t.Fatalf("publishes = %d, want 1", publisher.count())
		}
	})
	t.Run("at-anchor-is-no-change", func(t *testing.T) {
		publisher := &stubPublisher{}
		coordinator, err := NewCoordinator(CoordinatorConfig{
			Gateway: &Gateway{BaseURL: "http://unused.invalid"}, Bindings: testBindings(),
			Publisher:           publisher,
			WorkRefBeyondAnchor: func(context.Context) (bool, error) { return false, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		result := coordinator.finishSoftStop(context.Background())
		if result.Err != nil || result.Outcome != "no_change_needed" {
			t.Fatalf("result = %+v, want no_change_needed", result)
		}
		if publisher.count() != 0 {
			t.Fatal("a tip at the anchor is ordinary pre-existing state, never published")
		}
	})
	t.Run("blocked-promotes-to-blocked-external", func(t *testing.T) {
		publisher := &stubPublisher{err: &PublicationBlockedError{Reason: "the live work tip is foreign"}}
		coordinator, err := NewCoordinator(CoordinatorConfig{
			Gateway: &Gateway{BaseURL: "http://unused.invalid"}, Bindings: testBindings(),
			Publisher:           publisher,
			WorkRefBeyondAnchor: func(context.Context) (bool, error) { return true, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		result := coordinator.finishSoftStop(context.Background())
		if result.Err != nil || result.Outcome != "blocked_external" {
			t.Fatalf("result = %+v, want blocked_external", result)
		}
	})
	t.Run("failed-observation-is-infrastructure", func(t *testing.T) {
		coordinator, err := NewCoordinator(CoordinatorConfig{
			Gateway: &Gateway{BaseURL: "http://unused.invalid"}, Bindings: testBindings(),
			Publisher:           &stubPublisher{},
			WorkRefBeyondAnchor: func(context.Context) (bool, error) { return false, errors.New("broker unreachable") },
		})
		if err != nil {
			t.Fatal(err)
		}
		result := coordinator.finishSoftStop(context.Background())
		if result.Err == nil {
			t.Fatalf("result = %+v, want an infrastructure error", result)
		}
	})
	t.Run("cancellation-takes-precedence", func(t *testing.T) {
		coordinator, err := NewCoordinator(CoordinatorConfig{
			Gateway: &Gateway{BaseURL: "http://unused.invalid"}, Bindings: testBindings(),
			Publisher:           &stubPublisher{},
			WorkRefBeyondAnchor: func(context.Context) (bool, error) { return true, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result := coordinator.finishSoftStop(ctx)
		if result.Err == nil || result.Outcome != "" {
			t.Fatalf("result = %+v, want the SIGTERM path to end as an error, never a soft-stop outcome", result)
		}
	})
}

// A soft stop acknowledged at a recovered incarnation's first step boundary
// arrives before any snapshot, brief, or shell has prepared the publication
// engine. The finish must still confirm the world's own already-published
// work and end as changes — never refuse the publish and relaunch. This runs
// the real engine against the real broker: a stub publisher cannot see the
// state dependency.
func TestSoftStopEarlyFinishConfirmsRecoveredWork(t *testing.T) {
	ctx := context.Background()
	f := newBrokerFixture(t)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	bundle := f.workerBundle(tip, "b1", "c.txt", "work\n", engine)
	integration, err := engine.Integrate(ctx, f.request("b1", tip, bundle))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	published := integration.Commit

	// The recovered incarnation: control-side state is gone, the world is
	// not, and nothing has prepared the engine yet.
	recovered := f.engineFor()
	beyond, err := recovered.WorkRefBeyondAnchor(ctx)
	if err != nil || !beyond {
		t.Fatalf("work ref beyond anchor = %v (err %v), want the recovered publication", beyond, err)
	}
	gateway, _, cleanup := gatewayFor(
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	defer cleanup()
	watcher := NewControlWatcher(&stubControlSource{requests: []*courierv1alpha1.ControlRequest{controlRequest("req-1")}}, &stubAckWriter{})
	watcher.Poll(ctx) // acknowledged before the first turn, engine unprepared
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway: gateway, Bindings: testBindings(),
		Control: watcher, Publisher: recovered,
		WorkRefBeyondAnchor: recovered.WorkRefBeyondAnchor,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(ctx, testInvocation())
	if result.Err != nil || result.Outcome != "changes" {
		t.Fatalf("result = %+v, want changes from the world's confirmed work", result)
	}
	// The confirmation is idempotent: the remote holds exactly the one
	// publication, with no duplicate push and no relaunch.
	remote, _, err := f.remoteWorkTip()
	if err != nil || remote != published {
		t.Fatalf("remote tip = %s (err %v), want exactly %s", remote, err, published)
	}
	if commits := gitRev(t, f.workPath, "rev-list", "--count", f.baseOID+".."+remote); commits != "1" {
		t.Fatalf("remote commits beyond base = %s, want exactly 1 (no duplicates)", commits)
	}
}

// --- broker client read route ---------------------------------------------------

func TestBrokerClientControlRequest(t *testing.T) {
	record := controlRequest("req-9")
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/trusted/v1/control-request" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "nope", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, string(body))
	}))
	defer server.Close()
	client := &BrokerClient{BaseURL: server.URL, Token: []byte("tok"), HTTP: server.Client()}
	got, err := client.ControlRequest(context.Background())
	if err != nil || got == nil || got.ID != "req-9" || got.TargetPodUID != "pod-uid-live" {
		t.Fatalf("read = %+v, err = %v, want the record", got, err)
	}
}

func TestBrokerClientControlRequestAbsentIsNotError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	client := &BrokerClient{BaseURL: server.URL, Token: []byte("tok"), HTTP: server.Client()}
	got, err := client.ControlRequest(context.Background())
	if err != nil || got != nil {
		t.Fatalf("read = %+v, err = %v, want (nil, nil): a request addressed elsewhere is an ordinary absence", got, err)
	}
}

// --- status reporter ack shape --------------------------------------------------

type recordingTransport struct {
	mu      sync.Mutex
	patches []StatusPatch
}

func (t *recordingTransport) PostStatus(_ context.Context, patch StatusPatch) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.patches = append(t.patches, patch)
	return nil
}

func TestStatusReporterControlRequestAckPatchShape(t *testing.T) {
	transport := &recordingTransport{}
	reporter := NewStatusReporter(transport, "control-uid", "worker-uid", 0)
	if err := reporter.ControlRequestAck(context.Background(), "req-1"); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.patches) != 1 {
		t.Fatalf("patches = %d, want 1", len(transport.patches))
	}
	ack := transport.patches[0].ControlRequestAck
	if ack == nil || ack.ID != "req-1" || ack.At.IsZero() {
		t.Fatalf("ack patch = %+v, want id req-1 with a timestamp", transport.patches[0])
	}
	if transport.patches[0].Checkpoint != nil || transport.patches[0].Heartbeat != nil || transport.patches[0].AddOperation != nil {
		t.Fatal("the ack must travel alone: it is the only field of its patch")
	}
	raw, err := json.Marshal(transport.patches[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"controlRequestAck"`) {
		t.Fatalf("wire shape = %s, want the broker's controlRequestAck field name", raw)
	}
}
