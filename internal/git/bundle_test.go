package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// seedRepo creates a repository with one committed file and returns its
// directory and head OID.
func seedRepo(t *testing.T, dir, file, content string) (string, string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "init", "--quiet", "--initial-branch", "main")
	if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", file)
	git(t, dir, "commit", "--quiet", "-m", "seed "+file)
	out, err := gitOutput(dir, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return dir, strings.TrimSpace(string(out))
}

func commitFile(t *testing.T, dir, file, content, message string) string {
	t.Helper()
	if parent := filepath.Dir(file); parent != "." {
		if err := os.MkdirAll(filepath.Join(dir, parent), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", file)
	git(t, dir, "commit", "--quiet", "-m", message)
	out, err := gitOutput(dir, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

func TestBundleCreateHeadsVerifyInventory(t *testing.T) {
	ctx := context.Background()
	dir, _ := seedRepo(t, t.TempDir(), "a.txt", "a")
	work := commitFile(t, dir, "b.txt", "b", "work")
	bundle := filepath.Join(t.TempDir(), "w.bundle")
	if err := BundleCreate(ctx, dir, bundle, "HEAD"); err != nil {
		t.Fatal(err)
	}

	heads, err := BundleHeads(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != 1 || heads[0].Name != "HEAD" || heads[0].OID != work {
		t.Fatalf("heads = %+v, want the work commit", heads)
	}

	// A self-contained bundle verifies against any repository: no
	// prerequisites to resolve.
	fresh := t.TempDir()
	git(t, fresh, "init", "--quiet", "--initial-branch", "main")
	if err := VerifyBundle(ctx, fresh, bundle); err != nil {
		t.Fatalf("self-contained bundle verify: %v", err)
	}

	// The inventory covers exactly the delivered object set: two commits,
	// two trees, two blobs.
	quarantine := t.TempDir()
	if err := Unbundle(ctx, fresh, quarantine, bundle); err != nil {
		t.Fatal(err)
	}
	inventory, err := InventoryObjects(ctx, fresh, quarantine)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Count != 6 || inventory.TotalBytes <= 0 || inventory.MaxBlobBytes != 1 {
		t.Fatalf("inventory = %+v, want 6 objects, 1-byte blobs, positive total", inventory)
	}
}

func TestReflessFetchAndAncestry(t *testing.T) {
	ctx := context.Background()
	dir, base := seedRepo(t, t.TempDir(), "a.txt", "a")
	work := commitFile(t, dir, "b.txt", "b", "work")
	bundle := filepath.Join(t.TempDir(), "w.bundle")
	if err := BundleCreate(ctx, dir, bundle, "HEAD"); err != nil {
		t.Fatal(err)
	}

	fresh := t.TempDir()
	git(t, fresh, "init", "--quiet", "--initial-branch", "main")
	git(t, fresh, "fetch", "--quiet", "--", dir, "+refs/heads/main:refs/heads/base")
	git(t, fresh, "reset", "--quiet", "--hard", "refs/heads/base")
	// The production import path: fetch by the bundle's advertised ref
	// (never by a remote-claimed OID) and resolve the result locally.
	if err := FetchBundleIntoFetchHead(ctx, fresh, bundle, "HEAD"); err != nil {
		t.Fatalf("refless fetch: %v", err)
	}
	fetched, err := ResolveCommit(ctx, fresh, "FETCH_HEAD^{commit}")
	if err != nil {
		t.Fatal(err)
	}
	if fetched != work {
		t.Fatalf("FETCH_HEAD resolved to %s, want the work commit %s", fetched, work)
	}
	ok, err := IsAncestor(ctx, fresh, base, work)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("the fetched work head must descend from the base")
	}
	ok, err = IsAncestor(ctx, fresh, work, base)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("the base is not a descendant of the work head")
	}
	// Refless: the fetch left no ref behind (FETCH_HEAD is a file, not a
	// ref).
	refs, err := gitOutput(fresh, "for-each-ref", "--format=%(refname)")
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range strings.Split(strings.TrimSpace(string(refs)), "\n") {
		if strings.HasPrefix(ref, "refs/courier/") {
			t.Fatalf("bundle ref %q leaked into the tree", ref)
		}
	}
}

func TestChangedPathsListsDeletesAndTypeChanges(t *testing.T) {
	ctx := context.Background()
	dir, base := seedRepo(t, t.TempDir(), "a.txt", "a")
	git(t, dir, "rm", "--quiet", "a.txt")
	if err := os.MkdirAll(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nested", "c.txt"), []byte("c"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../a-target", filepath.Join(dir, "b.txt")); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "--quiet", "-m", "delete, add, symlink")
	head, err := gitOutput(dir, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	paths, err := ChangedPaths(ctx, dir, base, strings.TrimSpace(string(head)))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"a.txt": true, "nested/c.txt": true, "b.txt": true}
	if len(paths) != len(want) {
		t.Fatalf("paths = %v, want %v", paths, want)
	}
	for _, path := range paths {
		if !want[path] {
			t.Fatalf("unexpected changed path %q in %v", path, want)
		}
	}
}

func TestReadTreeIntegrationCommitLifecycle(t *testing.T) {
	ctx := context.Background()
	dir, base := seedRepo(t, t.TempDir(), "a.txt", "a")
	work := commitFile(t, dir, "b.txt", "b", "work")

	// Materialize the base into a fresh worktree, then read the work tree
	// over it and commit exactly one integration commit.
	tree := t.TempDir()
	git(t, tree, "init", "--quiet", "--initial-branch", "integration")
	git(t, tree, "fetch", "--quiet", "--", dir, "+refs/heads/main:refs/remotes/origin/main")
	git(t, tree, "reset", "--quiet", "--hard", base)
	if err := ReadTreeInto(ctx, tree, work); err != nil {
		t.Fatal(err)
	}
	quiet, err := DiffIndexQuiet(ctx, tree)
	if err != nil {
		t.Fatal(err)
	}
	if quiet {
		t.Fatal("the work tree must differ from the base commit")
	}
	commit, err := CommitIndexed(ctx, tree, "integrate b\n\nObjective: test")
	if err != nil {
		t.Fatal(err)
	}
	if commit == work {
		t.Fatal("the integration commit is control's own commit, not the worker's")
	}
	// One commit beyond the base, with the worker's exact tree content.
	count, err := gitOutput(tree, "rev-list", "--count", base+".."+commit)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(count)) != "1" {
		t.Fatalf("integration commits = %s, want exactly 1", count)
	}
	content, err := gitOutput(tree, "diff", "--name-only", work, commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(content) != 0 {
		t.Fatalf("integrated tree differs from the worker tree: %s", content)
	}
	// A second read of the same tree is quiet: no empty integration commit.
	if err := ReadTreeInto(ctx, tree, work); err != nil {
		t.Fatal(err)
	}
	quiet, err = DiffIndexQuiet(ctx, tree)
	if err != nil {
		t.Fatal(err)
	}
	if !quiet {
		t.Fatal("re-reading the integrated tree must be a no-op")
	}
}

func TestMergeCommitClassifiesConflicts(t *testing.T) {
	ctx := context.Background()
	dir, base := seedRepo(t, t.TempDir(), "a.txt", "base")
	git(t, dir, "checkout", "--quiet", "-b", "other")
	conflict := commitFile(t, dir, "a.txt", "other", "other change")
	git(t, dir, "checkout", "--quiet", "main")
	commitFile(t, dir, "a.txt", "main", "main change")

	err := MergeCommit(ctx, dir, conflict)
	var conflictErr *MergeConflictError
	if !asConflict(err, &conflictErr) {
		t.Fatalf("merge error = %v, want MergeConflictError", err)
	}
	if len(conflictErr.Paths) != 1 || conflictErr.Paths[0] != "a.txt" {
		t.Fatalf("conflict paths = %v", conflictErr.Paths)
	}
	git(t, dir, "merge", "--abort")
	// A clean merge of an ancestor is a no-op success.
	if err := MergeCommit(ctx, dir, base); err != nil {
		t.Fatalf("merging an ancestor: %v", err)
	}
}

func asConflict(err error, target **MergeConflictError) bool {
	if e, ok := err.(*MergeConflictError); ok {
		*target = e
		return true
	}
	return false
}

func TestFetchRefRejectsInvalidEndpoints(t *testing.T) {
	ctx := context.Background()
	dir, _ := seedRepo(t, t.TempDir(), "a.txt", "a")
	target := t.TempDir()
	git(t, target, "init", "--quiet", "--initial-branch", "main")
	if err := FetchRef(ctx, target, "ext::sh -c touch", "refs/heads/main", "refs/heads/main", "", ""); err == nil {
		t.Fatal("ext transport endpoints must be rejected")
	}
	if err := FetchRef(ctx, target, dir, "refs/heads/main", "refs/heads/fetched", "", ""); err != nil {
		t.Fatalf("local fetch: %v", err)
	}
}
