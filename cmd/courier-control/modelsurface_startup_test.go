package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
	courierlog "github.com/misospace/courier/internal/log"
)

// The model-surface startup-order regressions (#730 review): the digest is
// computed from the production assembly and durably persisted through the
// trusted status path before the capability gate can issue its first model
// request — the gate's model probes included — and a run whose provenance
// never lands issues zero model requests.

// startupSequencer records the ordered startup events across the two
// boundaries the model-surface contract governs: the trusted status path and
// the model transport. One global order is the point — the regressions
// assert on where the durable surface record sits relative to the first
// model request, not on either sink alone.
type startupSequencer struct {
	mu     sync.Mutex
	events []string
}

func (s *startupSequencer) record(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *startupSequencer) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

// first returns the index of the first recorded event with exactly this
// name, or -1 when none was recorded.
func (s *startupSequencer) first(event string) int {
	events := s.snapshot()
	for i, candidate := range events {
		if candidate == event {
			return i
		}
	}
	return -1
}

// firstModel returns the index of the first recorded model request, or -1
// when no model request was recorded.
func (s *startupSequencer) firstModel() int {
	events := s.snapshot()
	for i, event := range events {
		if strings.HasPrefix(event, "model:") {
			return i
		}
	}
	return -1
}

// sequencedStatusTransport adapts the sequencer onto the trusted status
// path, naming each patch kind it serves, and serves a scripted error queue
// one error per write until empty.
type sequencedStatusTransport struct {
	seq     *startupSequencer
	mu      sync.Mutex
	errs    []error
	patches []harness.StatusPatch
}

func (t *sequencedStatusTransport) PostStatus(_ context.Context, patch harness.StatusPatch) error {
	t.mu.Lock()
	name := "status:patch"
	switch {
	case patch.ModelSurface != nil:
		name = "status:modelSurface"
	case patch.TerminalResult != nil:
		name = "status:terminalResult"
	case patch.AddOperation != nil:
		name = "status:addOperation"
	}
	t.patches = append(t.patches, patch)
	var err error
	if len(t.errs) > 0 {
		err = t.errs[0]
		t.errs = t.errs[1:]
	}
	t.mu.Unlock()
	t.seq.record(name)
	return err
}

// sequencedGateway adapts the sequencer onto the model transport boundary: a
// chat turn and a metadata read are both model requests the contract
// governs, and neither is ever served.
type sequencedGateway struct {
	seq *startupSequencer
}

func (g *sequencedGateway) StreamChat(context.Context, harness.ChatRequest) ([]harness.Event, error) {
	g.seq.record("model:chat")
	return nil, errors.New("sequenced gateway serves no model")
}

func (g *sequencedGateway) ModelInputLimit(context.Context, string) (int, error) {
	g.seq.record("model:metadata")
	return 0, errors.New("sequenced gateway serves no metadata")
}

// startupTestDeps builds the pre-model startup deps for the ordering
// regressions: an identity whose broker and worker endpoints are
// unreachable, a sequenced status path and model transport, and a skillless
// invocation — no pins, so skill loading performs no I/O and the assembled
// toolset is the skillless one. The broker and worker probes fail retryable,
// so the gate exercises every re-probe round before it fails closed.
func startupTestDeps(t *testing.T, seq *startupSequencer, transport harness.StatusTransport, gateway harness.ModelTransport) modelWorkDeps {
	t.Helper()
	bindings, err := harness.BindRoles(map[string]string{harness.RoleCoordinator: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	invocation := executor.Invocation{RunName: "run-1", Mode: courierv1alpha1.ModeResolveIssue}
	identity := &controlIdentity{
		runUID:         "run-uid",
		controlUID:     "control-uid",
		controlKubeUID: "control-kube-uid",
		workerUID:      "worker-uid",
		workerURL:      "http://worker.invalid",
		brokerURL:      "https://broker.invalid",
		statusURL:      "https://broker.invalid/status",
	}
	return modelWorkDeps{
		ctx:          context.Background(),
		identity:     identity,
		reporter:     harness.NewStatusReporter(transport, identity.controlKubeUID, identity.workerUID, 0),
		bindings:     bindings,
		invocation:   invocation,
		gateway:      gateway,
		emitter:      courierlog.NewEmitter(io.Discard, courierlog.LevelInfo),
		tally:        harness.NewModelTelemetry(modelRequestSink{emitter: courierlog.NewEmitter(io.Discard, courierlog.LevelInfo), stderr: &bytes.Buffer{}, invocation: invocation}),
		budgetInputs: make(map[string]harness.ContextBudgetInput),
		probeTimeout: 5 * time.Second,
	}
}

// TestSurfacePersistFailureIssuesZeroModelRequests is the #730 regression:
// a persistence failure of status.modelSurface fails closed before the
// capability gate runs, so the run issues zero model requests — the gate's
// probes included — and probe spend can never exist without the durable
// surface identity meant to explain it.
func TestSurfacePersistFailureIssuesZeroModelRequests(t *testing.T) {
	seq := &startupSequencer{}
	transport := &sequencedStatusTransport{
		seq:  seq,
		errs: []error{errors.New("broker refuses the surface record")},
	}
	gateway := &sequencedGateway{seq: seq}

	work, gateFail, err := prepareModelWork(startupTestDeps(t, seq, transport, gateway))
	if err == nil || !strings.Contains(err.Error(), "persist model-surface provenance") {
		t.Fatalf("err = %v, want the model-surface persistence failure", err)
	}
	if work != nil || gateFail != nil {
		t.Fatalf("work = %+v, gateFail = %+v, want both nil on a persistence failure", work, gateFail)
	}
	if got := seq.first("status:modelSurface"); got != 0 {
		t.Fatalf("events = %v, want the surface write attempted first, at index %d", seq.snapshot(), got)
	}
	if got := seq.firstModel(); got >= 0 {
		t.Fatalf("events = %v, want zero model requests after the surface persistence failed, got the first at index %d", seq.snapshot(), got)
	}
}

// TestSurfacePersistPrecedesModelRequests pins the order the contract
// requires: the durable surface record lands before the capability gate
// issues its first model request, so even a gate that will not pass spends
// its probe budget against an already-persisted identity (#730 review).
func TestSurfacePersistPrecedesModelRequests(t *testing.T) {
	shrinkProbeRetryDelay(t)
	seq := &startupSequencer{}
	transport := &sequencedStatusTransport{seq: seq}
	gateway := &sequencedGateway{seq: seq}

	work, gateFail, err := prepareModelWork(startupTestDeps(t, seq, transport, gateway))
	if err != nil {
		t.Fatalf("prepareModelWork() error = %v", err)
	}
	if gateFail == nil {
		t.Fatal("gateFail = nil, want the gate to fail closed on the unreachable dependencies")
	}
	if work == nil || work.surface == nil || work.surface.Digest == "" {
		t.Fatalf("work = %+v, want the assembled surface", work)
	}
	surfaceAt := seq.first("status:modelSurface")
	if surfaceAt < 0 {
		t.Fatalf("events = %v, want the surface write recorded", seq.snapshot())
	}
	modelAt := seq.firstModel()
	if modelAt < 0 {
		t.Fatalf("events = %v, want the gate's model probes recorded", seq.snapshot())
	}
	if modelAt < surfaceAt {
		t.Fatalf("events = %v, the first model request preceded the durable surface record", seq.snapshot())
	}
}

// shrinkProbeRetryDelay shrinks the gate's re-probe wait for the duration of
// one test: the regressions drive the gate through every round, and the
// production delay is tuned for a broker pod that is still starting.
func shrinkProbeRetryDelay(t *testing.T) {
	t.Helper()
	original := probeRetryDelay
	probeRetryDelay = time.Millisecond
	t.Cleanup(func() { probeRetryDelay = original })
}
