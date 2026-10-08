package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	couriergit "github.com/misospace/courier/internal/git"
)

// artifactWorld builds the real-git fixture behind the §5 worker-boundary
// tests: an upstream repository with a base and a work commit, the snapshot
// bundle the broker would render from them, and a control integration tree
// seeded from it. The returned tip is the dispatched base tip.
type artifactWorld struct {
	t          *testing.T
	integrator *Integrator
	tip        string // dispatched snapshot tip
	upstream   string
	base       string
	work       string
}

func newArtifactWorld(t *testing.T, scope PathScope) *artifactWorld {
	t.Helper()
	ctx := context.Background()
	upstream := t.TempDir()
	gitCmd(t, upstream, "init", "--quiet", "--initial-branch", "main")
	gitCmd(t, upstream, "config", "gc.auto", "0")
	writeFile(t, filepath.Join(upstream, "a.txt"), "a\n")
	gitCmd(t, upstream, "add", "a.txt")
	gitCommit(t, upstream, "base")
	base := gitRev(t, upstream, "rev-parse", "HEAD")
	writeFile(t, filepath.Join(upstream, "b.txt"), "b\n")
	gitCmd(t, upstream, "add", "b.txt")
	gitCommit(t, upstream, "work")
	work := gitRev(t, upstream, "rev-parse", "HEAD")

	// The broker's seed snapshot: fixed snapshot refs over the observed tips.
	gitCmd(t, upstream, "update-ref", "refs/courier/snapshot/work", work)
	gitCmd(t, upstream, "update-ref", "refs/courier/snapshot/base", base)
	bundle := gitBundle(t, upstream, "refs/courier/snapshot/work", "refs/courier/snapshot/base")

	integrator := NewIntegrator(filepath.Join(t.TempDir(), "integration"), scope)
	tip, err := integrator.Seed(ctx, bundle)
	if err != nil {
		t.Fatalf("seed integration tree: %v", err)
	}
	if tip != work {
		t.Fatalf("seeded tip = %s, want the snapshot work tip %s", tip, work)
	}
	return &artifactWorld{t: t, integrator: integrator, tip: tip, upstream: upstream, base: base, work: work}
}

// simulateWorker unpacks the snapshot like the worker's fixed task does,
// applies the mutation as the model's shell would, commits, and returns the
// bundle the fixed pack task would render for the brief.
func (w *artifactWorld) simulateWorker(briefID string, mutate func(dir string)) []byte {
	w.t.Helper()
	ctx := context.Background()
	workspace := w.t.TempDir()
	gitCmd(w.t, workspace, "init", "--quiet", "--initial-branch=integration", ".")
	gitCmd(w.t, workspace, "config", "gc.auto", "0")
	gitCmd(w.t, workspace, "fetch", "--quiet", "--no-tags", w.upstream,
		"+refs/courier/snapshot/work:refs/heads/work")
	gitCmd(w.t, workspace, "symbolic-ref", "HEAD", "refs/heads/work")
	gitCmd(w.t, workspace, "reset", "--quiet", "--hard", "refs/heads/work")
	if mutate != nil {
		mutate(workspace)
	}
	ref := ResultRefPrefix + briefID
	gitCmd(w.t, workspace, "update-ref", ref, "HEAD")
	bundlePath := filepath.Join(workspace, "artifact."+briefID+".bundle")
	if err := couriergit.BundleCreate(ctx, workspace, bundlePath, ref); err != nil {
		w.t.Fatalf("worker bundle render: %v", err)
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		w.t.Fatal(err)
	}
	return data
}

func (w *artifactWorld) integrate(briefID string, bundle []byte, summary string) (Integration, error) {
	w.t.Helper()
	return w.integrator.Integrate(context.Background(), IntegrationRequest{
		BriefID:       briefID,
		DispatchedTip: w.tip,
		Bundle:        bundle,
		Brief:         Brief{ID: briefID, Role: "coder", Objective: "do work", SuccessCheck: "tests pass"},
		Summary:       summary,
	})
}

func requireRejected(t *testing.T, err error, category string) {
	t.Helper()
	if err == nil {
		t.Fatalf("artifact was accepted, want rejection %q", category)
	}
	var rejected *ArtifactRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("error = %v, want an ArtifactRejectedError", err)
	}
	if rejected.Category != category {
		t.Fatalf("rejection = %q, want %q", rejected.Category, category)
	}
}

// The honest-bundle happy path: one integration commit per brief, the
// worker's own commits left behind, the bundle's ref never applied.
func TestArtifactHappyPathIntegratesOneCommit(t *testing.T) {
	w := newArtifactWorld(t, nil)
	bundle := w.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "worker work\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "worker commit")
	})
	integration, err := w.integrate("b1", bundle, "implemented the feature")
	if err != nil {
		t.Fatalf("integrate: %v", err)
	}
	if !integration.Changed || integration.Commit == "" {
		t.Fatalf("integration = %+v, want one changed commit", integration)
	}
	// Exactly one commit beyond the dispatched tip, and it is control's own
	// commit, not the worker's head.
	count := gitRev(t, w.integrator.Dir, "rev-list", "--count", w.tip+".."+integration.Commit)
	if count != "1" {
		t.Fatalf("commits beyond tip = %s, want 1", count)
	}
	if _, err := os.Stat(filepath.Join(w.integrator.Dir, "c.txt")); err != nil {
		t.Fatalf("integrated file missing from the private tree: %v", err)
	}
	// Refless import: the bundle's ref never entered the private tree.
	refs := gitRev(t, w.integrator.Dir, "for-each-ref", "--format=%(refname)")
	if strings.Contains(refs, ResultRefPrefix) {
		t.Fatalf("bundle ref leaked into the private tree: %s", refs)
	}
	// The worker's commit objects may be present but are not on the branch.
	branch := gitRev(t, w.integrator.Dir, "rev-parse", "refs/heads/integration")
	if branch != integration.Commit {
		t.Fatalf("integration branch = %s, want the integration commit", branch)
	}
}

// A second brief integrates on top of the first: the snapshot it was built
// from carried the first brief's integration.
func TestArtifactSecondBriefStacksOnTheFirst(t *testing.T) {
	w := newArtifactWorld(t, nil)
	first := w.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "one\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "first")
	})
	if _, err := w.integrate("b1", first, "first"); err != nil {
		t.Fatalf("first integrate: %v", err)
	}
	// The next snapshot is rendered from the integrated tree.
	snapshot, err := w.integrator.Render(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tip == w.tip {
		t.Fatal("the rendered snapshot must carry the integrated work")
	}
	// The worker unpacks that snapshot for the second brief: the work ref
	// must materialize the integrated tree.
	workspace := w.t.TempDir()
	gitCmd(t, workspace, "init", "--quiet", "--initial-branch=integration", ".")
	gitCmd(t, workspace, "config", "gc.auto", "0")
	bundlePath := filepath.Join(workspace, "snapshot.pack")
	if err := os.WriteFile(bundlePath, snapshot.Data, 0o600); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, workspace, "fetch", "--quiet", "--no-tags", bundlePath,
		"+refs/courier/snapshot/work:refs/heads/work")
	gitCmd(t, workspace, "symbolic-ref", "HEAD", "refs/heads/work")
	gitCmd(t, workspace, "reset", "--quiet", "--hard", "refs/heads/work")
	if _, err := os.Stat(filepath.Join(workspace, "c.txt")); err != nil {
		t.Fatalf("the snapshot lost the first brief's work: %v", err)
	}
}

func TestArtifactRejectsForeignBase(t *testing.T) {
	w := newArtifactWorld(t, nil)
	// A bundle built on a base that is not the dispatched snapshot tip.
	other := t.TempDir()
	gitCmd(t, other, "init", "--quiet", "--initial-branch", "main")
	writeFile(t, filepath.Join(other, "unrelated.txt"), "x\n")
	gitCmd(t, other, "add", "-A")
	gitCommit(t, other, "other root")
	gitCmd(t, other, "update-ref", ResultRefPrefix+"b1", "HEAD")
	bundle := gitBundle(t, other, ResultRefPrefix+"b1")
	_, err := w.integrate("b1", bundle, "claims success")
	requireRejected(t, err, rejectAncestry)
}

func TestArtifactRejectsUnrelatedOrphanHistory(t *testing.T) {
	w := newArtifactWorld(t, nil)
	// The worker bundles an orphan commit that shares no history with the
	// snapshot: same repository, disjoint root.
	workspace := w.t.TempDir()
	gitCmd(t, workspace, "init", "--quiet", "--initial-branch=main", ".")
	writeFile(t, filepath.Join(workspace, "orphan.txt"), "orphan\n")
	gitCmd(t, workspace, "add", "-A")
	gitCommit(t, workspace, "orphan root")
	gitCmd(t, workspace, "update-ref", ResultRefPrefix+"b1", "HEAD")
	bundle := gitBundle(t, workspace, ResultRefPrefix+"b1")
	// The bundle cannot even verify: its closure lacks the dispatched tip's
	// prerequisite world and the ancestry check fails on the imported head.
	_, err := w.integrate("b1", bundle, "summary")
	if err == nil {
		t.Fatal("orphan history accepted")
	}
	var rejected *ArtifactRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("error = %v, want an ArtifactRejectedError", err)
	}
}

func TestArtifactRejectsTamperedBundle(t *testing.T) {
	w := newArtifactWorld(t, nil)
	bundle := w.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "work\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "work")
	})
	// Flip one byte in the pack data: git's own hash checks must fail.
	tampered := append([]byte(nil), bundle...)
	i := bytes.Index(tampered, []byte("PACK"))
	if i < 0 {
		t.Fatal("bundle carries no pack")
	}
	tampered[len(tampered)-10] ^= 0xff
	_, err := w.integrate("b1", tampered, "summary")
	if err == nil {
		t.Fatal("tampered bundle accepted")
	}
	// Either the verify or the unpack stage catches the corruption.
	var rejected *ArtifactRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("error = %v, want an ArtifactRejectedError", err)
	}
}

func TestArtifactRejectsWrongRefSets(t *testing.T) {
	w := newArtifactWorld(t, nil)
	// Two refs: the expected one plus an extra.
	workspace := w.simulateWorkerWorkspace("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "work\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "work")
	})
	gitCmd(t, workspace, "update-ref", ResultRefPrefix+"b1", "HEAD")
	gitCmd(t, workspace, "update-ref", "refs/courier/sneaky/extra", "HEAD")
	bundle := gitBundle(t, workspace, ResultRefPrefix+"b1", "refs/courier/sneaky/extra")
	_, err := w.integrate("b1", bundle, "summary")
	requireRejected(t, err, rejectRefSet)

	// One ref, but the wrong name.
	gitCmd(t, workspace, "update-ref", "refs/heads/elsewhere", "HEAD")
	gitCmd(t, workspace, "update-ref", "-d", "refs/courier/sneaky/extra")
	bundle = gitBundle(t, workspace, "refs/heads/elsewhere")
	_, err = w.integrate("b1", bundle, "summary")
	requireRejected(t, err, rejectRefSet)

	// A ref outside the refs/courier/ namespace with the brief's content.
	gitCmd(t, workspace, "update-ref", "-d", "refs/heads/elsewhere")
	gitCmd(t, workspace, "update-ref", "refs/heads/b1", "HEAD")
	bundle = gitBundle(t, workspace, "refs/heads/b1")
	_, err = w.integrate("b1", bundle, "summary")
	requireRejected(t, err, rejectRefSet)
}

// A prerequisite-bearing bundle: the worker excludes the dispatched tip, so
// the bundle declares a prerequisite. It verifies against the private tree
// (which holds the tip) but cannot unpack in isolation — it is rejected as
// not self-contained.
func TestArtifactRejectsPrerequisiteBundle(t *testing.T) {
	w := newArtifactWorld(t, nil)
	workspace := w.simulateWorkerWorkspace("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "work\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "work")
	})
	ref := ResultRefPrefix + "b1"
	gitCmd(t, workspace, "update-ref", ref, "HEAD")
	bundlePath := filepath.Join(t.TempDir(), "prereq.bundle")
	gitCmd(t, workspace, "bundle", "create", bundlePath, ref, "^"+w.tip)
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.integrate("b1", data, "summary")
	requireRejected(t, err, rejectMalformed)
}

func TestArtifactRejectsOversizedBlob(t *testing.T) {
	w := newArtifactWorld(t, nil)
	big := make([]byte, MaxArtifactBlobBytes+1)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	bundle := w.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "big.bin"), string(big))
		gitCmd(t, dir, "add", "big.bin")
		gitCommit(t, dir, "big")
	})
	_, err := w.integrate("b1", bundle, "summary")
	requireRejected(t, err, rejectBlob)
}

func TestArtifactRejectsOversizedBundleFile(t *testing.T) {
	w := newArtifactWorld(t, nil)
	big := make([]byte, MaxArtifactBundleBytes+1024)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	// The bundle-size bound holds before git sees the artifact: hand it the
	// oversized bytes directly.
	_, err := w.integrate("b1", big, "summary")
	requireRejected(t, err, rejectBundleSize)
}

func TestArtifactRejectsObjectCountBound(t *testing.T) {
	w := newArtifactWorld(t, nil)
	bundle := w.simulateWorker("b1", func(dir string) {
		// One commit with more objects than the bound allows: 1-byte files
		// keep the unpacked total under its own bound so the count is what
		// trips. The content is unique so git cannot deduplicate.
		if err := os.MkdirAll(filepath.Join(dir, "many"), 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < MaxArtifactObjects+10; i++ {
			writeFile(t, filepath.Join(dir, "many", pad(i)+".txt"), "x"+pad(i))
		}
		gitCmd(t, dir, "add", "-A")
		gitCommit(t, dir, "many objects")
	})
	_, err := w.integrate("b1", bundle, "summary")
	requireRejected(t, err, rejectObjects)
}

func TestArtifactRejectsUnpackedSizeBound(t *testing.T) {
	w := newArtifactWorld(t, nil)
	// Many small unique blobs: the count stays under its bound while the
	// total unpacked size exceeds 64 MiB. The content must differ per blob,
	// or git would deduplicate them into one object.
	blob := strings.Repeat("x", 1023) + "\n"
	bundle := w.simulateWorker("b1", func(dir string) {
		if err := os.MkdirAll(filepath.Join(dir, "bulk"), 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 66000; i++ {
			writeFile(t, filepath.Join(dir, "bulk", pad(i)+".bin"), blob+pad(i))
		}
		gitCmd(t, dir, "add", "-A")
		gitCommit(t, dir, "bulk")
	})
	_, err := w.integrate("b1", bundle, "summary")
	requireRejected(t, err, rejectUnpacked)
}

func TestArtifactPathScopeEnforced(t *testing.T) {
	scope, err := NewPathScope([]string{"allowed"})
	if err != nil {
		t.Fatal(err)
	}
	w := newArtifactWorld(t, scope)
	// Out-of-scope change: rejected with the fixed category.
	bundle := w.simulateWorker("b1", func(dir string) {
		if err := os.MkdirAll(filepath.Join(dir, "allowed"), 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(dir, "allowed", "ok.txt"), "in scope\n")
		writeFile(t, filepath.Join(dir, "forbidden.txt"), "out of scope\n")
		gitCmd(t, dir, "add", "-A")
		gitCommit(t, dir, "mixed")
	})
	_, err = w.integrate("b1", bundle, "summary")
	requireRejected(t, err, rejectPathScope)

	// In-scope change: accepted.
	w2 := newArtifactWorld(t, scope)
	bundle2 := w2.simulateWorker("b1", func(dir string) {
		if err := os.MkdirAll(filepath.Join(dir, "allowed", "nested"), 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(dir, "allowed", "nested", "ok.txt"), "in scope\n")
		gitCmd(t, dir, "add", "-A")
		gitCommit(t, dir, "scoped")
	})
	integration, err := w2.integrate("b1", bundle2, "summary")
	if err != nil {
		t.Fatalf("in-scope artifact rejected: %v", err)
	}
	if !integration.Changed {
		t.Fatal("in-scope artifact must integrate")
	}
}

func TestArtifactSymlinkCountsAtItsOwnPath(t *testing.T) {
	scope, err := NewPathScope([]string{"linked"})
	if err != nil {
		t.Fatal(err)
	}
	w := newArtifactWorld(t, scope)
	bundle := w.simulateWorker("b1", func(dir string) {
		if err := os.MkdirAll(filepath.Join(dir, "linked"), 0o755); err != nil {
			t.Fatal(err)
		}
		// A symlink whose target leaves the scope: the changed path is the
		// symlink's own path, which is in scope.
		if err := os.Symlink("../../../elsewhere", filepath.Join(dir, "linked", "link")); err != nil {
			t.Fatal(err)
		}
		gitCmd(t, dir, "add", "-A")
		gitCommit(t, dir, "symlink")
	})
	if _, err := w.integrate("b1", bundle, "summary"); err != nil {
		t.Fatalf("in-scope symlink rejected: %v", err)
	}
	// The materialized tree carries the symlink entry itself.
	info, err := os.Lstat(filepath.Join(w.integrator.Dir, "linked", "link"))
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("symlink not materialized as a symlink entry: %v", err)
	}
}

func TestArtifactDeletionIntegrates(t *testing.T) {
	w := newArtifactWorld(t, nil)
	bundle := w.simulateWorker("b1", func(dir string) {
		gitCmd(t, dir, "rm", "--quiet", "a.txt")
		gitCommit(t, dir, "remove a")
	})
	integration, err := w.integrate("b1", bundle, "removed the file")
	if err != nil {
		t.Fatalf("integrate deletion: %v", err)
	}
	if !integration.Changed {
		t.Fatal("a deletion is work")
	}
	if _, err := os.Stat(filepath.Join(w.integrator.Dir, "a.txt")); !os.IsNotExist(err) {
		t.Fatal("the deleted file survived integration")
	}
}

func TestArtifactEmptyDiffIntegratesNothing(t *testing.T) {
	w := newArtifactWorld(t, nil)
	// An honest research brief: no committed work beyond the dispatched tip.
	bundle := w.simulateWorker("b1", nil)
	integration, err := w.integrate("b1", bundle, "research notes only")
	if err != nil {
		t.Fatalf("integrate: %v", err)
	}
	if integration.Changed || integration.Commit != "" {
		t.Fatalf("integration = %+v, want no changes", integration)
	}
}

// Hostile in-tree attributes are inert: read-tree materializes the raw,
// hash-verified blob content without consulting attribute-defined filters,
// so no driver can execute and the committed content integrates exactly.
func TestArtifactHostileAttributesAreInert(t *testing.T) {
	w := newArtifactWorld(t, nil)
	bundle := w.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, ".gitattributes"), "* filter=hostile-driver\n")
		writeFile(t, filepath.Join(dir, "c.txt"), "raw object content\n")
		gitCmd(t, dir, "add", "-A")
		gitCommit(t, dir, "hostile attributes")
	})
	integration, err := w.integrate("b1", bundle, "summary")
	if err != nil {
		t.Fatalf("integrate with hostile attributes: %v", err)
	}
	if !integration.Changed {
		t.Fatal("the artifact must integrate")
	}
	// The materialized file is the raw object content: no filter ran.
	data, err := os.ReadFile(filepath.Join(w.integrator.Dir, "c.txt"))
	if err != nil || string(data) != "raw object content\n" {
		t.Fatalf("integrated content = %q (err %v), want the exact object content", data, err)
	}
}

// Worker-declared metadata never changes a validation outcome: a lying
// summary cannot rescue a bad artifact, and a self-deprecating summary
// cannot spoil a good one.
func TestArtifactWorkerMetadataIsNeverAuthority(t *testing.T) {
	w := newArtifactWorld(t, nil)
	good := w.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "real work\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "work")
	})
	integration, err := w.integrate("b1", good, "everything failed, I did nothing, tests are red")
	if err != nil {
		t.Fatalf("integrate with a lying summary: %v", err)
	}
	if !integration.Changed {
		t.Fatal("bundle contents decide, not the summary")
	}
	other := newArtifactWorld(t, nil)
	bad := other.simulateWorker("b1", func(dir string) {
		writeFile(t, filepath.Join(dir, "c.txt"), "real work\n")
		gitCmd(t, dir, "add", "c.txt")
		gitCommit(t, dir, "work")
	})
	// Corrupt the bundle, keep the truthful-sounding summary.
	bad[len(bad)-5] ^= 0xff
	if _, err := other.integrate("b1", bad, "all done, tests pass"); err == nil {
		t.Fatal("a summary cannot rescue a corrupt artifact")
	}
}

func TestPathScopeValidation(t *testing.T) {
	if scope, err := NewPathScope(nil); err != nil || scope != nil {
		t.Fatalf("nil scope = %v, %v", scope, err)
	}
	// NUL bytes are rejected at the boundary: a scope entry is never a
	// vehicle for smuggled separators or truncation.
	if _, err := NewPathScope([]string{"allowed\x00evil"}); err == nil {
		t.Fatal("a NUL byte in a scope entry must be rejected")
	}
	if _, err := NewPathScope([]string{"clean", "also\x00"}); err == nil {
		t.Fatal("a NUL byte in any entry must reject the whole scope")
	}
	scope, err := NewPathScope([]string{"a/b/", "c", ".", "", "../escape", "/absolute"})
	if err == nil {
		t.Fatal("../escape must be rejected")
	}
	scope, err = NewPathScope([]string{"a/b/", "c", ".", ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(scope) != 2 || scope[0] != "a/b" || scope[1] != "c" {
		t.Fatalf("scope = %v, want [a/b c]", scope)
	}
	if !scope.Contains("a/b/x") || !scope.Contains("c") || !scope.Contains("c/d") {
		t.Fatal("in-scope paths must be admitted")
	}
	if scope.Contains("cc") || scope.Contains("a") || scope.Contains("d") {
		t.Fatal("prefix confusion admitted an out-of-scope path")
	}
	if scope.Contains("../a/b/x") || scope.Contains("/a/b") {
		t.Fatal("escaping paths must never be admitted")
	}
}

// --- shared real-git helpers ------------------------------------------------

// gitCommit creates a fixture commit with an explicit identity: the
// hardened runner replaces the child environment, so no ambient git
// identity exists on CI runners and none may leak from the developer's
// machine either.
func gitCommit(t *testing.T, dir, message string) {
	t.Helper()
	if _, err := couriergit.Hardened(context.Background(), dir,
		"-c", "user.name=courier-test", "-c", "user.email=courier-test@example.invalid",
		"commit", "--no-verify", "-m", message); err != nil {
		t.Fatalf("git commit: %v", err)
	}
}

func gitCmd(t *testing.T, dir string, args ...string) {
	t.Helper()
	out, err := couriergit.Hardened(context.Background(), dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v (%s)", strings.Join(args, " "), err, out)
	}
}

func gitRev(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := couriergit.Hardened(context.Background(), dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}

// gitBundle renders a full-closure bundle for the given refs from dir.
func gitBundle(t *testing.T, dir string, refs ...string) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "out.bundle")
	if err := couriergit.BundleCreate(context.Background(), dir, path, refs...); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// simulateWorkerWorkspace unpacks the snapshot like the worker's fixed task
// and returns the workspace for further (untrusted) mutation.
func (w *artifactWorld) simulateWorkerWorkspace(briefID string, mutate func(dir string)) string {
	w.t.Helper()
	workspace := w.t.TempDir()
	gitCmd(w.t, workspace, "init", "--quiet", "--initial-branch=integration", ".")
	gitCmd(w.t, workspace, "config", "gc.auto", "0")
	gitCmd(w.t, workspace, "fetch", "--quiet", "--no-tags", w.upstream,
		"+refs/courier/snapshot/work:refs/heads/work")
	gitCmd(w.t, workspace, "symbolic-ref", "HEAD", "refs/heads/work")
	gitCmd(w.t, workspace, "reset", "--quiet", "--hard", "refs/heads/work")
	if mutate != nil {
		mutate(workspace)
	}
	return workspace
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func pad(value int) string {
	return fmt.Sprintf("%07d", value)
}
