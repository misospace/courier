package harness

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/executor"
	couriergit "github.com/misospace/courier/internal/git"
)

// The §5 pipeline must work with the control process's cwd outside any git
// repository — the rendered control pod's working directory is the private
// workspace, which is not itself a repository. A regression here made every
// honest artifact fail as "malformed" while the repo's own test runs masked
// it, because the package directory is a git checkout.
func TestArtifactPipelineRunsOutsideARepository(t *testing.T) {
	// Build the world first (while the cwd is whatever go test uses), then
	// move the cwd to a plain directory for the pipeline under test.
	world := newArtifactWorld(t, nil)
	plain := t.TempDir()
	t.Chdir(plain)
	if entries, err := os.ReadDir(plain); err != nil || len(entries) != 0 {
		t.Fatalf("the regression target must be a non-repository directory: %v %v", entries, err)
	}
	bundle := world.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "worker work\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "worker commit")
	})
	integration, err := world.integrate("b1", bundle, "implemented the feature")
	if err != nil {
		t.Fatalf("integrate from a repoless cwd: %v", err)
	}
	if !integration.Changed {
		t.Fatal("the honest artifact must integrate")
	}
}

// A hostile artifact carrying a submodule pointer and a .gitmodules file is
// inert tree data: the gitlink path counts against the scope, and no
// submodule initialization or fetch is ever attempted.
func TestArtifactGitlinkIsInertTreeData(t *testing.T) {
	w := newArtifactWorld(t, nil)
	bundle := w.simulateWorker("b1", func(dir string) {
		if err := os.MkdirAll(filepath.Join(dir, "vendor", "lib"), 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(dir, ".gitmodules"), "[submodule \"lib\"]\n\tpath = vendor/lib\n\turl = "+dir+"\n")
		// A gitlink entry pointing at an arbitrary (nonexistent) commit: the
		// pointer is data; nothing is fetched or initialized.
		gitCmd(t, dir, "update-index", "--add", "--cacheinfo",
			"160000,0123456789012345678901234567890123456789,vendor/lib")
		gitCmd(t, dir, "add", ".gitmodules")
		gitCommit(t, dir, "hostile submodule")
	})
	integration, err := w.integrate("b1", bundle, "adds a dependency")
	if err != nil {
		t.Fatalf("integrate gitlink artifact: %v", err)
	}
	if !integration.Changed {
		t.Fatal("the gitlink artifact must integrate as inert tree data")
	}
	// The gitlink is in the committed tree with its declared path.
	tree := gitRev(t, w.integrator.Dir, "ls-tree", "HEAD", "vendor/lib")
	if !strings.Contains(tree, "commit 0123456789012345678901234567890123456789") {
		t.Fatalf("gitlink entry = %q, want an inert commit pointer", tree)
	}
	// No submodule checkout materialized.
	if _, err := os.Stat(filepath.Join(w.integrator.Dir, "vendor", "lib", ".git")); !os.IsNotExist(err) {
		t.Fatal("the gitlink must never be initialized or fetched")
	}
}

// A worker that lies about the unpacked tip fails the prepare: the workspace
// does not hold the snapshot control dispatched, and nothing is dispatched
// against it.
func TestWorkerLyingTipFailsPrepare(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "0000000000000000000000000000000000000001"
	gateway, _, cleanup := gatewayFor(
		gatewayResponse{body: sse(
			toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Work","successCheck":"ok"}`),
			finishChunk("tool_calls"),
		)},
		gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
	)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  gateway,
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: snapshotProvider("0000000000000000000000000000000000000002"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
	}
	// Only the unpack was dispatched; no brief work ran against a workspace
	// that lied about its content. The brief is registered but carries no
	// result: the work unit is spent, and corrected work re-delegates under
	// a fresh ID.
	if got := worker.dispatchCount(); got != 1 {
		t.Fatalf("worker dispatches = %d, want 1 (the unpack only)", got)
	}
	if briefs := coordinator.Briefs().BriefIDs(); len(briefs) != 1 || briefs[0] != "b1" {
		t.Fatalf("brief ledger = %v, want the registered b1", briefs)
	}
	if _, ok := coordinator.Briefs().Result("b1"); ok {
		t.Fatal("a failed prepare must not record a brief result")
	}
}

// Diff paths with spaces, quotes, and non-ASCII bytes survive the NUL-framed
// changed-path enumeration and the scope check at their real names.
func TestArtifactChangedPathsWithSpecialNames(t *testing.T) {
	scope, err := NewPathScope([]string{"docs"})
	if err != nil {
		t.Fatal(err)
	}
	w := newArtifactWorld(t, scope)
	name := "docs/Étoile \"quoted\" file.txt"
	bundle := w.simulateWorker("b1", func(dir string) {
		if err := os.MkdirAll(filepath.Join(dir, "docs"), 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(dir, name), "content\n")
		gitCmd(t, dir, "add", "-A")
		gitCommit(t, dir, "special names")
	})
	integration, err := w.integrate("b1", bundle, "docs work")
	if err != nil {
		t.Fatalf("integrate special-named path: %v", err)
	}
	if !integration.Changed {
		t.Fatal("the special-named path must integrate")
	}
	if _, err := os.Lstat(filepath.Join(w.integrator.Dir, name)); err != nil {
		t.Fatalf("special-named path not materialized: %v", err)
	}
}

// The seed snapshot's ref advertisement is exact: broker snapshots carry
// exactly the two snapshot refs, rendered snapshots exactly the work ref,
// and anything else is refused.
func TestSeedSnapshotRefSetIsExact(t *testing.T) {
	ctx := context.Background()
	dir, _ := seedHarnessRepo(t, t.TempDir())
	workTip := gitRev(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "update-ref", "refs/courier/snapshot/work", workTip)
	gitCmd(t, dir, "update-ref", "refs/courier/snapshot/base", workTip)
	gitCmd(t, dir, "update-ref", "refs/heads/sneaky", workTip)

	bundlePath := filepath.Join(t.TempDir(), "s.bundle")
	if err := couriergit.BundleCreate(ctx, dir, bundlePath,
		"refs/courier/snapshot/work", "refs/courier/snapshot/base", "refs/heads/sneaky"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	integrator := NewIntegrator(filepath.Join(t.TempDir(), "integration"), nil)
	if _, err := integrator.Seed(ctx, data); err == nil {
		t.Fatal("a snapshot bundle advertising extra refs must be refused")
	}
}

// A brief whose sub-agent session fails or truncates after committing
// partial work never enters the artifact chain: no pack task runs,
// Publisher.Integrate and Publish stay untouched, and no completed
// BriefResult is recorded. Only a terminal final result authorizes
// integration and publication (§7's cadence is per completed unit).
func TestFailedBriefNeverEntersTheArtifactChain(t *testing.T) {
	t.Run("stream failure after partial work", func(t *testing.T) {
		worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
		defer cleanupWorker()
		worker.completeAll = true
		worker.snapshotTip = "snap-tip"
		gateway, _, cleanup := gatewayFor(
			// Coordinator: delegate.
			gatewayResponse{body: sse(
				toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Work","successCheck":"ok"}`),
				finishChunk("tool_calls"),
			)},
			// Coder: one shell step that leaves committed state on the worker.
			gatewayResponse{body: sse(
				toolCallChunk(0, "call-2", toolShell, `{"command":"git commit partial work"}`),
				finishChunk("tool_calls"),
			)},
			// Coder: the next stream fails permanently — the brief never
			// reaches a final result.
			gatewayResponse{status: http.StatusBadRequest},
			// Coordinator: gives up.
			gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
		)
		defer cleanup()
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
		if result.Outcome != executor.OutcomeBlockedExternal {
			t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
		}
		// Unpack and the partial shell ran; the pack task never did.
		if got := worker.dispatchCount(); got != 2 {
			t.Fatalf("worker dispatches = %d, want 2 (unpack + partial shell, no pack)", got)
		}
		if len(publisher.integrated) != 0 {
			t.Fatalf("integrations = %+v, want none for a failed brief", publisher.integrated)
		}
		if publisher.count() != 0 {
			t.Fatalf("publishes = %d, want none for a failed brief", publisher.count())
		}
		if _, ok := coordinator.Briefs().Result("b1"); ok {
			t.Fatal("a failed brief must not record a completed result")
		}
	})

	t.Run("truncated stream after partial work", func(t *testing.T) {
		worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
		defer cleanupWorker()
		worker.completeAll = true
		worker.snapshotTip = "snap-tip"
		gateway, _, cleanup := gatewayFor(
			gatewayResponse{body: sse(
				toolCallChunk(0, "call-1", toolDelegate, `{"id":"b1","role":"coder","objective":"Work","successCheck":"ok"}`),
				finishChunk("tool_calls"),
			)},
			// Coder: shell, then a stream that emits deltas and never
			// terminalizes — no final, no tool requests.
			gatewayResponse{body: sse(
				toolCallChunk(0, "call-2", toolShell, `{"command":"git commit partial work"}`),
				finishChunk("tool_calls"),
			)},
			gatewayResponse{body: sse(contentChunk("partial thought"))},
			gatewayResponse{body: sse(contentChunk(`{"outcome":"blocked_external","missing":"x"}`), finishChunk("stop"))},
		)
		defer cleanup()
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
		if result := coordinator.Run(context.Background(), testInvocation()); result.Outcome != executor.OutcomeBlockedExternal {
			t.Fatalf("outcome = %q (%v)", result.Outcome, result.Err)
		}
		if got := worker.dispatchCount(); got != 2 {
			t.Fatalf("worker dispatches = %d, want 2 (unpack + partial shell, no pack)", got)
		}
		if len(publisher.integrated) != 0 || publisher.count() != 0 {
			t.Fatalf("integration/publication ran for a truncated brief: %+v / %d", publisher.integrated, publisher.count())
		}
	})
}
