package harness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/devenv"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/protocol"
)

// contractWorld builds the real-git fixture behind the #414 contract-state
// tests: an upstream repository whose base and work commits optionally carry
// the development-bootstrap contract files, the snapshot bundle the broker
// would render from them, and a control integration tree seeded from it.
type contractWorld struct {
	t          *testing.T
	integrator *Integrator
	work       string
	base       string
	bundle     []byte
}

func newContractWorld(t *testing.T, baseFiles, headFiles map[string]string, headDeletes ...string) *contractWorld {
	t.Helper()
	ctx := context.Background()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--quiet", "--initial-branch", "main")
	gitCmd(t, upstream, "config", "gc.auto", "0")
	writeFile(t, filepath.Join(upstream, "a.txt"), "a\n")
	if len(baseFiles) > 0 || len(headFiles) > 0 {
		if err := os.MkdirAll(filepath.Join(upstream, filepath.Dir(devenv.ConfigPath)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for path, content := range baseFiles {
		writeFile(t, filepath.Join(upstream, path), content)
	}
	gitCmd(t, upstream, "add", "-A")
	gitCommit(t, upstream, "base")
	base := gitRev(t, upstream, "rev-parse", "HEAD")
	writeFile(t, filepath.Join(upstream, "b.txt"), "b\n")
	for path, content := range headFiles {
		writeFile(t, filepath.Join(upstream, path), content)
	}
	for _, path := range headDeletes {
		gitCmd(t, upstream, "rm", "--quiet", path)
	}
	gitCmd(t, upstream, "add", "-A")
	gitCommit(t, upstream, "work")
	work := gitRev(t, upstream, "rev-parse", "HEAD")
	gitCmd(t, upstream, "update-ref", "refs/courier/snapshot/work", work)
	gitCmd(t, upstream, "update-ref", "refs/courier/snapshot/base", base)
	bundle := gitBundle(t, upstream, "refs/courier/snapshot/work", "refs/courier/snapshot/base")
	integrator := NewIntegrator(filepath.Join(t.TempDir(), "integration"), nil)
	if _, err := integrator.Seed(ctx, bundle); err != nil {
		t.Fatalf("seed integration tree: %v", err)
	}
	gitCmd(t, integrator.Dir, "config", "gc.auto", "0")
	gitCmd(t, integrator.Dir, "config", "maintenance.auto", "false")
	return &contractWorld{t: t, integrator: integrator, work: work, base: base, bundle: bundle}
}

func contractConfig(t *testing.T) string {
	t.Helper()
	return "# mise config for " + t.Name() + "\n[tools]\ngo = \"1.24\"\n"
}

func TestIntegratorContractStateApproved(t *testing.T) {
	world := newContractWorld(t,
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
	)
	ctx := context.Background()
	state := world.integrator.ContractState(ctx, world.work)
	if state != devenv.StateApproved {
		t.Fatalf("contract state = %q, want approved", state)
	}
	snap, err := world.integrator.Render(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ContractState != devenv.StateApproved {
		t.Fatalf("rendered snapshot contract state = %q, want approved", snap.ContractState)
	}
}

func TestIntegratorContractStateModifiedAtHead(t *testing.T) {
	world := newContractWorld(t,
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
		map[string]string{devenv.ConfigPath: contractConfig(t) + "extra = true\n", devenv.LockPath: "lock@base\n"},
	)
	ctx := context.Background()
	state := world.integrator.ContractState(ctx, world.work)
	if state != devenv.StateModified {
		t.Fatalf("contract state = %q, want modified", state)
	}
	snap, err := world.integrator.Render(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ContractState != devenv.StateModified {
		t.Fatalf("rendered snapshot contract state = %q, want modified", snap.ContractState)
	}
}

func TestIntegratorContractStateModifiedByLock(t *testing.T) {
	// The lock file is part of the contract: a head that drifts only in the
	// lock is a modified contract, never an approved one.
	world := newContractWorld(t,
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@head\n"},
	)
	state := world.integrator.ContractState(context.Background(), world.work)
	if state != devenv.StateModified {
		t.Fatalf("contract state = %q, want modified", state)
	}
}

func TestIntegratorContractStateContractRemovedAtHeadIsAbsent(t *testing.T) {
	// devenv.Compare classifies a missing head contract as absent: without a
	// head contract there is nothing to bootstrap, whatever the base
	// approved.
	world := newContractWorld(t,
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
		nil,
		devenv.ConfigPath, devenv.LockPath,
	)
	state := world.integrator.ContractState(context.Background(), world.work)
	if state != devenv.StateAbsent {
		t.Fatalf("contract state = %q, want absent", state)
	}
}

func TestIntegratorContractStateAbsent(t *testing.T) {
	world := newContractWorld(t, nil, nil)
	ctx := context.Background()
	state := world.integrator.ContractState(ctx, world.work)
	if state != devenv.StateAbsent {
		t.Fatalf("contract state = %q, want absent", state)
	}
	snap, err := world.integrator.Render(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ContractState != devenv.StateAbsent {
		t.Fatalf("rendered snapshot contract state = %q, want absent", snap.ContractState)
	}
}

// TestIntegratorContractStateUnreadableSkipsBootstrap simulates a git read
// that fails outside the missing-path class: a shimmed git answers
// `cat-file blob` with exit code 3. The state must not read as "absent"
// (an unreadable world may never pass as contract-free) and must not fail
// the snapshot either — unprovable content is skipped, and the workspace
// simply runs unbootstrapped until git reads again.
func TestIntegratorContractStateUnreadableSkipsBootstrap(t *testing.T) {
	world := newContractWorld(t,
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
	)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shimDir := t.TempDir()
	shim := "#!/bin/sh\n" +
		`if [ "$1" = "cat-file" ] && [ "$2" = "blob" ]; then` + "\n" +
		"  echo \"simulated contract read failure\" >&2\n" +
		"  exit 3\n" +
		"fi\n" +
		"exec " + realGit + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(shimDir, "git"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shimDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx := context.Background()
	state := world.integrator.ContractState(ctx, world.work)
	if state != devenv.StateModified {
		t.Fatalf("contract state = %q, want modified", state)
	}
	snap, err := world.integrator.Render(ctx)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if snap.ContractState != devenv.StateModified {
		t.Fatalf("rendered snapshot contract state = %q, want modified", snap.ContractState)
	}
}

// TestSeedSnapshotCarriesContractState drives PublicationEngine.seed through
// a stub broker: the seed snapshot dispatches too, so it must carry the
// contract state the same way a rendered snapshot does — the first dispatch
// of an incarnation is the one that must bootstrap the workspace.
func TestSeedSnapshotCarriesContractState(t *testing.T) {
	world := newContractWorld(t,
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
		map[string]string{devenv.ConfigPath: contractConfig(t), devenv.LockPath: "lock@base\n"},
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != brokerPathSnapshot {
			http.NotFound(w, r)
			return
		}
		w.Header().Set(headerSnapshotTip, world.work)
		w.Header().Set(headerSnapshotBaseTip, world.base)
		w.Header().Set(headerSnapshotWorkRef, "true")
		w.Header().Set(headerSnapshotAtAnchor, "false")
		_, _ = w.Write(world.bundle)
	}))
	defer server.Close()
	tree := NewIntegrator(filepath.Join(t.TempDir(), "integration"), nil)
	client := &BrokerClient{BaseURL: server.URL, TokenSource: NewStaticBrokerTokenSource([]byte("token")), HTTP: server.Client()}
	engine, err := NewPublicationEngine(client, tree)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
	if snap.Tip != world.work || snap.ContractState != devenv.StateApproved {
		t.Fatalf("seed snapshot = tip %s state %q, want tip %s approved", snap.Tip, snap.ContractState, world.work)
	}
	rendered, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatalf("rendered snapshot: %v", err)
	}
	if rendered.ContractState != devenv.StateApproved {
		t.Fatalf("rendered snapshot contract state = %q, want approved", rendered.ContractState)
	}
}

// --- coordinator flow ------------------------------------------------------

func contractSnapshotProvider(tip string, state devenv.State) SnapshotProvider {
	return func(context.Context) (Snapshot, error) {
		return Snapshot{Data: []byte("bundle-bytes"), Tip: tip, BaseTip: "base-tip", ContractState: state}, nil
	}
}

// bootstrapCapture renders worker stdout in the shape the fixed bootstrap
// task produces: a report, the env marker, then NUL-terminated KEY=VALUE
// records.
func bootstrapCapture(entries ...string) string {
	return "bootstrap report\n" + devenv.EnvMarker + "\n" + strings.Join(entries, "\x00") + "\x00"
}

func delegateResponse(id string) gatewayResponse {
	return gatewayResponse{body: sse(
		toolCallChunk(0, "call-1", toolDelegate, `{"id":"`+id+`","role":"coder","objective":"Do the work","successCheck":"tests pass"}`),
		finishChunk("tool_calls"),
	)}
}

func TestCoordinatorBootstrapsApprovedContractAndAttachesEnv(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.bootstrapResults = []protocol.Result{
		{ExitCode: 0, StdoutTail: bootstrapCapture("BOOTSTRAP_HOME=/opt/env", "BOOTSTRAP_PATH=/opt/bin")},
	}

	gateway, gatewayMock, cleanup := gatewayFor(
		delegateResponse("b1"),
		// Coder sub-agent: run shell on the worker.
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-2", toolShell, `{"command":"echo working"}`),
			finishChunk("tool_calls"),
		)},
		// Coder sub-agent: done.
		gatewayResponse{body: sse(contentChunk("work done"), finishChunk("stop"))},
		// Coordinator: declare changes.
		gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
	)
	_ = gateway
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:   gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  contractSnapshotProvider("snap-tip", devenv.StateApproved),
		Publisher: &mockPublisher{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeChanges || result.Err != nil {
		t.Fatalf("harness result = %+v, want declared changes", result)
	}
	if got := worker.dispatchCount(); got != 4 {
		t.Fatalf("worker dispatches = %d, want 4 (unpack, bootstrap, shell, pack)", got)
	}
	if !isWorkerBootstrapTask(worker.dispatches[1].task) || worker.dispatches[1].envelope.BriefID != "b1" {
		t.Fatalf("bootstrap dispatch = %+v", worker.dispatches[1])
	}
	shell := worker.dispatches[2]
	if strings.Join(shell.task.Command, " ") != "sh -c echo working" {
		t.Fatalf("shell dispatch = %+v", shell)
	}
	want := []string{"BOOTSTRAP_HOME=/opt/env", "BOOTSTRAP_PATH=/opt/bin"}
	if strings.Join(shell.task.Env, "\n") != strings.Join(want, "\n") {
		t.Fatalf("shell task env = %v, want the applied bootstrap delta %v", shell.task.Env, want)
	}
	if len(worker.dispatches[3].task.Env) != 0 {
		t.Fatalf("pack task env = %v, want no environment on a fixed control task", worker.dispatches[3].task.Env)
	}
	if gatewayMock.requestCount() == 0 {
		t.Fatal("test precondition lost: the gateway was never used")
	}
}

func TestCoordinatorBlockedExternalWhenBootstrapContractFails(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.bootstrapResults = []protocol.Result{
		{ExitCode: 2, StderrTail: "fatal: mise failed\nsecond line"},
	}

	gateway, gatewayMock, cleanup := gatewayFor(
		// Coordinator: delegate — and never get another turn, because the
		// bootstrap blocker ends the run inside this turn's tool boundary.
		delegateResponse("b1"),
	)
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: contractSnapshotProvider("snap-tip", devenv.StateApproved),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%+v), want blocked_external", result.Outcome, result)
	}
	if !strings.HasPrefix(result.Reason, "the approved bootstrap contract failed with exit code 2") {
		t.Fatalf("reason = %q, want the fixed blocker reason with the exit code", result.Reason)
	}
	// Worker output is untrusted and never enters a durable reason: the
	// failure's stderr must not appear in the run's termination handoff.
	if strings.Contains(result.Reason, "fatal: mise failed second line") {
		t.Fatalf("reason = %q, must not carry untrusted worker output", result.Reason)
	}
	if strings.Contains(result.Reason, "\n") {
		t.Fatalf("reason = %q, must be single-line sanitized", result.Reason)
	}
	// One gateway request: the run ended at the delegation turn's own tool
	// boundary, before any further model request.
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1", got)
	}
	// The brief was aborted without reaching its pack chain: only the
	// unpack and the failed bootstrap were dispatched.
	if got := worker.dispatchCount(); got != 2 {
		t.Fatalf("worker dispatches = %d, want 2 (unpack, bootstrap)", got)
	}
}

func TestCoordinatorBlockedExternalOnMalformedBootstrapCapture(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	worker.bootstrapResults = []protocol.Result{
		{ExitCode: 0, StdoutTail: "the script printed no environment marker"},
	}

	gateway, _, cleanup := gatewayFor(delegateResponse("b1"))
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: contractSnapshotProvider("snap-tip", devenv.StateApproved),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%+v), want blocked_external", result.Outcome, result)
	}
	if !strings.Contains(result.Reason, "bootstrap environment capture malformed") {
		t.Fatalf("reason = %q, want the malformed-capture blocker", result.Reason)
	}
}

func TestCoordinatorBlockedExternalOnOversizedBootstrapEnv(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	entries := make([]string, 0, 80)
	for i := range 80 {
		entries = append(entries, "VARIABLE"+pad(i)+"="+strings.Repeat("x", 1024))
	}
	worker.bootstrapResults = []protocol.Result{
		{ExitCode: 0, StdoutTail: bootstrapCapture(entries...)},
	}

	gateway, _, cleanup := gatewayFor(delegateResponse("b1"))
	defer cleanup()

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: contractSnapshotProvider("snap-tip", devenv.StateApproved),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%+v), want blocked_external", result.Outcome, result)
	}
	if !strings.Contains(result.Reason, "bootstrap environment exceeded the 64 KiB dispatch bound") {
		t.Fatalf("reason = %q, want the oversized-delta blocker", result.Reason)
	}
}

func TestCoordinatorSkipsBootstrapWithoutAnApprovedContract(t *testing.T) {
	for _, state := range []devenv.State{devenv.StateModified, devenv.StateAbsent} {
		t.Run(string(state), func(t *testing.T) {
			worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
			defer cleanupWorker()
			worker.completeAll = true
			worker.snapshotTip = "snap-tip"

			gateway, _, cleanup := gatewayFor(
				delegateResponse("b1"),
				gatewayResponse{body: sse(
					toolCallChunk(0, "call-2", toolShell, `{"command":"echo working"}`),
					finishChunk("tool_calls"),
				)},
				gatewayResponse{body: sse(contentChunk("work done"), finishChunk("stop"))},
				gatewayResponse{body: sse(contentChunk(`{"outcome":"changes"}`), finishChunk("stop"))},
			)
			defer cleanup()

			coordinator, err := NewCoordinator(CoordinatorConfig{
				Gateway:   gateway,
				Bindings:  testBindings(),
				Worker:    delegator,
				Snapshot:  contractSnapshotProvider("snap-tip", state),
				Publisher: &mockPublisher{},
			})
			if err != nil {
				t.Fatal(err)
			}
			result := coordinator.Run(context.Background(), testInvocation())
			if result.Outcome != executor.OutcomeChanges || result.Err != nil {
				t.Fatalf("harness result = %+v, want declared changes", result)
			}
			if got := worker.dispatchCount(); got != 3 {
				t.Fatalf("worker dispatches = %d, want 3 (unpack, shell, pack) with no bootstrap", got)
			}
			for _, record := range worker.dispatches {
				if isWorkerBootstrapTask(record.task) {
					t.Fatalf("a %s contract must dispatch no bootstrap task, got %+v", state, record.task)
				}
			}
			if len(worker.dispatches[1].task.Env) != 0 {
				t.Fatalf("shell task env = %v, want no environment when nothing was bootstrapped", worker.dispatches[1].task.Env)
			}
		})
	}
}

// TestRunBriefAbortsOnRecordedBootstrapBlocker pins the brief-level guard:
// a blocker recorded before the brief ran aborts it after its first turn,
// without further model requests and without integration.
func TestRunBriefAbortsOnRecordedBootstrapBlocker(t *testing.T) {
	gateway, gatewayMock, cleanup := gatewayFor(
		gatewayResponse{body: sse(contentChunk("about to work"), finishChunk("stop"))},
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{Gateway: gateway, Bindings: testBindings()})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.recordBootstrapBlocker(&bootstrapBlocker{reason: "the approved bootstrap contract failed"})
	summary, completed, unavailable := coordinator.runBrief(context.Background(), Brief{ID: "b1", Role: "coder", Objective: "work", SuccessCheck: "ok"})
	if completed || unavailable != nil {
		t.Fatalf("runBrief = (%q, %v, %v), want an aborted brief", summary, completed, unavailable)
	}
	if summary != "workspace bootstrap failed; the brief is aborted" {
		t.Fatalf("summary = %q, want the fixed abort text", summary)
	}
	if got := gatewayMock.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1 (the aborted turn only)", got)
	}
}

// TestBootstrapLivenessBoundIsABlocker pins the blocker classification of
// bootstrapWorkspace: the bootstrap task's own liveness bound expiring is a
// terminal environment blocker, while a parent cancellation is an ordinary
// error and never becomes one.
func TestBootstrapLivenessBoundIsABlocker(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.hangResults = true

	gateway, _, cleanup := gatewayFor()
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{Gateway: gateway, Bindings: testBindings(), Worker: delegator})
	if err != nil {
		t.Fatal(err)
	}

	restored := bootstrapBound
	bootstrapBound = 100 * time.Millisecond
	defer func() { bootstrapBound = restored }()

	err = coordinator.bootstrapWorkspace(context.Background(), "b1")
	var blocker *bootstrapBlocker
	if !errors.As(err, &blocker) || !strings.Contains(err.Error(), "exceeded its liveness bound") {
		t.Fatalf("bound expiry error = %v, want the liveness-bound blocker", err)
	}

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	err = coordinator.bootstrapWorkspace(parent, "b1")
	if errors.As(err, &blocker) {
		t.Fatalf("parent cancellation error = %v, want an ordinary error", err)
	}
}
