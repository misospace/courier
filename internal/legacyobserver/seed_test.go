package legacyobserver

import (
	"context"
	"path/filepath"
	"testing"
)

// remoteWithBranches builds a bare remote carrying baseTip on baseBranch and
// (when workTip is non-empty) a work branch diverged from it.
func remoteWithBranches(t *testing.T, root, name string, withBase, withWork bool) (string, string, string) {
	t.Helper()
	remote := filepath.Join(root, name+".git")
	gitTest(t, root, "init", "--bare", "--quiet", remote)
	seed := filepath.Join(root, name+"-seed")
	gitTest(t, root, "clone", "--quiet", remote, seed)
	gitTest(t, seed, "config", "user.name", "test")
	gitTest(t, seed, "config", "user.email", "test@example.invalid")
	writeTestFile(t, seed, "tracked.txt", "baseline\n")
	gitTest(t, seed, "add", ".")
	gitTest(t, seed, "commit", "-q", "-m", "baseline")
	gitTest(t, seed, "branch", "-M", "main")
	var base, work string
	if withBase {
		gitTest(t, seed, "push", "-q", "origin", "main")
		base = gitText(t, seed, "rev-parse", "HEAD")
	}
	if withWork {
		gitTest(t, seed, "checkout", "-q", "-b", "work")
		writeTestFile(t, seed, "work.txt", "head\n")
		gitTest(t, seed, "add", ".")
		gitTest(t, seed, "commit", "-q", "-m", "work")
		gitTest(t, seed, "push", "-q", "origin", "work")
		work = gitText(t, seed, "rev-parse", "HEAD")
	}
	return remote, base, work
}

func TestSeedForkWithoutBaseBranch(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	// Upstream owns the base branch only; the fork carries the PR head and
	// deliberately lacks the base branch entirely.
	upstream, _, _ := remoteWithBranches(t, root, "upstream", true, false)
	fork, _, workTip := remoteWithBranches(t, root, "fork", false, true)
	scratch := filepath.Join(root, "seed-scratch")
	tip, err := Seed(ctx, fork, upstream, "main", "work", scratch, "", "")
	if err != nil {
		t.Fatalf("Seed with fork head and upstream base: %v", err)
	}
	if tip != workTip {
		t.Fatalf("baseline = %q, want fork work tip %q", tip, workTip)
	}
}

func TestSeedForkMissingWorkFallsBackToUpstreamBase(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	upstream, baseTip, _ := remoteWithBranches(t, root, "upstream", true, false)
	fork, _, _ := remoteWithBranches(t, root, "fork-empty", false, false)
	scratch := filepath.Join(root, "seed-scratch")
	tip, err := Seed(ctx, fork, upstream, "main", "work", scratch, "", "")
	if err != nil {
		t.Fatalf("Seed fallback to upstream base: %v", err)
	}
	if tip == "" {
		t.Fatal("empty baseline")
	}
	// The baseline must be the upstream base tip, not anything from the fork.
	if tip != baseTip {
		t.Fatalf("baseline = %q, want upstream base %q", tip, baseTip)
	}
}

func TestSeedSameRepositoryCases(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote, base, work := remoteWithBranches(t, root, "mono", true, true)
	tip, err := Seed(ctx, remote, "", "main", "work", filepath.Join(root, "s1"), "", "")
	if err != nil {
		t.Fatalf("Seed same-repository existing branch: %v", err)
	}
	if tip != work {
		t.Fatalf("baseline = %q, want work tip %q", tip, work)
	}
	tip, err = Seed(ctx, remote, "", "main", "nope", filepath.Join(root, "s2"), "", "")
	if err != nil {
		t.Fatalf("Seed same-repository base fallback: %v", err)
	}
	if tip != base {
		t.Fatalf("fallback baseline = %q, want base tip %q", tip, base)
	}
}
