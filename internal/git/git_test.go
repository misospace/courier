package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/forge"
)

// TestMain isolates git from the developer's global and system config and pins
// a committer identity for the whole package: Prepare runs git merges inside
// workspaces it clones itself, TrustDirectory writes the global config, and
// settings such as commit.gpgsign must not leak in from a workstation.
func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "courier-git-test-")
	if err != nil {
		panic(err)
	}
	for key, value := range map[string]string{
		"GIT_CONFIG_GLOBAL":   filepath.Join(home, "gitconfig"),
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME":     "Courier Test",
		"GIT_AUTHOR_EMAIL":    "courier-test@example.invalid",
		"GIT_COMMITTER_NAME":  "Courier Test",
		"GIT_COMMITTER_EMAIL": "courier-test@example.invalid",
	} {
		if err := os.Setenv(key, value); err != nil {
			panic(err)
		}
	}
	code := m.Run()
	_ = os.RemoveAll(home)
	os.Exit(code)
}

func TestPrepareAdoptsOrphanAndSyncsBaseBeforeWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	// Create the orphaned work branch from the first base revision.
	orphan := filepath.Join(root, "orphan")
	git(t, root, "clone", remote, orphan)
	git(t, orphan, "config", "user.name", "Courier Test")
	git(t, orphan, "config", "user.email", "courier-test@example.invalid")
	git(t, orphan, "checkout", "-b", "courier/resolve-issue/acme-widget/10", "origin/main")
	writeFile(t, filepath.Join(orphan, "work.txt"), "completed brief\n")
	commit(t, orphan, "work: previous brief")
	git(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widget/10")

	// Move main after the work branch was created. Adoption must merge this
	// revision before returning control to the coordinator.
	writeFile(t, filepath.Join(source, "base.txt"), "new base\n")
	commit(t, source, "base: second revision")
	git(t, source, "push", "origin", "main")

	workspaceDir := filepath.Join(root, "workspace")
	workspace, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: workspaceDir,
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/10",
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !workspace.Adopted {
		t.Fatal("expected existing branch to be adopted")
	}
	if _, err := os.Stat(filepath.Join(workspaceDir, "base.txt")); err != nil {
		t.Fatalf("adopted branch was not synchronized to base: %v", err)
	}
	if _, err := gitOutput(workspaceDir, "merge-base", "--is-ancestor", "origin/main", "HEAD"); err != nil {
		t.Fatalf("base is not an ancestor after adoption: %v", err)
	}
	git(t, workspaceDir, "config", "user.name", "Courier Test")
	git(t, workspaceDir, "config", "user.email", "courier-test@example.invalid")

	writeFile(t, filepath.Join(workspaceDir, "second.txt"), "another completed brief\n")
	sha, err := workspace.CommitBrief(ctx, Brief{
		ID:        "brief-2",
		Objective: "Add the second work unit",
		Outcome:   "The coordinator can resume from this commit",
	})
	if err != nil {
		t.Fatalf("commit brief: %v", err)
	}
	if len(sha) != 40 {
		t.Fatalf("commit SHA = %q, want a full SHA", sha)
	}
	message, err := gitOutput(workspaceDir, "show", "-s", "--format=%B", "HEAD")
	if err != nil {
		t.Fatalf("read commit message: %v", err)
	}
	if !strings.Contains(string(message), "Add the second work unit") || !strings.Contains(string(message), "The coordinator can resume from this commit") {
		t.Fatalf("commit message is not substantive: %q", message)
	}
	if err := workspace.Push(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	verify := filepath.Join(root, "verify")
	git(t, root, "clone", remote, verify)
	git(t, verify, "checkout", "courier/resolve-issue/acme-widget/10")
	if _, err := os.Stat(filepath.Join(verify, "second.txt")); err != nil {
		t.Fatalf("pushed brief commit is missing from remote branch: %v", err)
	}
}

// SyncToBase must merge the upstream base, not the fork's: a fork PR's origin
// is the fork, whose base branch can be arbitrarily stale or divergent.
func TestPrepareSyncsBaseFromUpstreamWhenOriginIsFork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	upstream := filepath.Join(root, "upstream.git")
	fork := filepath.Join(root, "fork.git")
	source := filepath.Join(root, "source")
	initBare(t, upstream)
	initBare(t, fork)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", upstream)
	git(t, source, "push", "-u", "origin", "main")

	// Fork the upstream, then create the PR head branch on the fork. The
	// fork-seed clone may have no checked-out branch (an unborn HEAD when the
	// fork's default branch name is guessed), so push the explicit refspec.
	git(t, root, "clone", upstream, filepath.Join(root, "forkseed"))
	git(t, filepath.Join(root, "forkseed"), "remote", "add", "fork", fork)
	git(t, filepath.Join(root, "forkseed"), "push", "fork", "origin/main:refs/heads/main")
	forkWork := filepath.Join(root, "forkwork")
	git(t, root, "clone", fork, forkWork)
	git(t, forkWork, "config", "user.name", "Courier Test")
	git(t, forkWork, "config", "user.email", "courier-test@example.invalid")
	git(t, forkWork, "checkout", "-b", "fix/pr-12", "origin/main")
	writeFile(t, filepath.Join(forkWork, "work.txt"), "fork work\n")
	commit(t, forkWork, "work: fork brief")
	git(t, forkWork, "push", "origin", "HEAD:refs/heads/fix/pr-12")

	// Both bases move after the work branch was created: upstream main gains
	// the commit that must be merged, fork main gains one that must not be.
	writeFile(t, filepath.Join(source, "upstream-base.txt"), "new upstream base\n")
	commit(t, source, "base: upstream second revision")
	git(t, source, "push", "origin", "main")
	writeFile(t, filepath.Join(forkWork, "fork-main.txt"), "stale fork main\n")
	git(t, forkWork, "checkout", "-B", "main", "origin/main")
	commit(t, forkWork, "base: divergent fork main")
	git(t, forkWork, "push", "origin", "main")

	workspace, err := Prepare(ctx, PrepareOptions{
		RemoteURL:     fork,
		BaseRemoteURL: upstream,
		Directory:     filepath.Join(root, "workspace"),
		Base:          "main",
		Branch:        "fix/pr-12",
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !workspace.Adopted {
		t.Fatal("expected existing fork branch to be adopted")
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "upstream-base.txt")); err != nil {
		t.Fatalf("upstream base was not merged into the fork work branch: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "fork-main.txt")); !os.IsNotExist(err) {
		t.Fatalf("fork's own main was merged instead of the upstream base")
	}
	if _, err := gitOutput(workspace.Directory, "merge-base", "--is-ancestor", "upstream/main", "HEAD"); err != nil {
		t.Fatalf("upstream main is not an ancestor after adoption: %v", err)
	}
}

func TestPrepareHandsConflictedBaseSyncToCoordinator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	const branch = "courier/fix-pr/acme-widget/12"
	work := filepath.Join(root, "work")
	git(t, root, "clone", remote, work)
	git(t, work, "config", "user.name", "Courier Test")
	git(t, work, "config", "user.email", "courier-test@example.invalid")
	git(t, work, "checkout", "-b", branch, "origin/main")
	writeFile(t, filepath.Join(work, "README.md"), "branch edit\n")
	commit(t, work, "work: edit readme")
	git(t, work, "push", "origin", "HEAD:refs/heads/"+branch)

	writeFile(t, filepath.Join(source, "README.md"), "base edit\n")
	commit(t, source, "base: edit readme")
	git(t, source, "push", "origin", "main")

	workspaceDir := filepath.Join(root, "workspace")
	workspace, err := Prepare(ctx, PrepareOptions{RemoteURL: remote, Directory: workspaceDir, Base: "main", Branch: branch})
	if err != nil {
		t.Fatalf("prepare on a conflicting branch must hand off, not fail: %v", err)
	}
	if !workspace.Adopted {
		t.Fatal("conflicting branch should still be adopted")
	}
	if workspace.Conflict == nil || workspace.Conflict.Base != "origin/main" || !reflect.DeepEqual(workspace.Conflict.Paths, []string{"README.md"}) {
		t.Fatalf("conflict = %#v, want README.md against origin/main", workspace.Conflict)
	}
	if current, err := workspace.CurrentBranch(ctx); err != nil || current != branch {
		t.Fatalf("current branch = %q, %v; want the PR branch kept checked out", current, err)
	}
	if pending, err := workspace.MergeInProgress(ctx); err != nil || !pending {
		t.Fatalf("merge in progress = %v, %v; want the merge left for the coordinator", pending, err)
	}
	if merged, err := workspace.ContainsBase(ctx); err != nil || merged {
		t.Fatalf("contains base = %v, %v; want false until the merge is committed", merged, err)
	}

	writeFile(t, filepath.Join(workspaceDir, "README.md"), "branch edit\nbase edit\n")
	git(t, workspaceDir, "add", "README.md")
	git(t, workspaceDir, "commit", "--no-edit")
	if pending, err := workspace.MergeInProgress(ctx); err != nil || pending {
		t.Fatalf("merge in progress after commit = %v, %v", pending, err)
	}
	if merged, err := workspace.ContainsBase(ctx); err != nil || !merged {
		t.Fatalf("contains base after resolution = %v, %v", merged, err)
	}
}

func TestSyncToBaseNonConflictFailureFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	workspace, err := Prepare(ctx, PrepareOptions{RemoteURL: remote, Directory: filepath.Join(root, "workspace"), Base: "main", Branch: "courier/resolve-issue/acme-widget/3"})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	workspace.Base = "no-such-base"
	err = workspace.SyncToBase(ctx)
	if err == nil {
		t.Fatal("sync to a missing base succeeded")
	}
	var conflict *MergeConflictError
	if errors.As(err, &conflict) {
		t.Fatalf("a non-conflict failure was reported as a conflict: %v", err)
	}
}

// A preparation destination that already holds a checkout is rebuilt from
// the remote rather than reused: an earlier attempt can leave one behind
// carrying a local-only merge of the wrong base, an uncommitted edit, and an
// untracked file, and none of it may reach the run.
func TestPrepareRebuildsWorkspaceOverLeftoverCheckout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	const branch = "courier/resolve-issue/acme-widget/14"
	work := filepath.Join(root, "work")
	git(t, root, "clone", remote, work)
	git(t, work, "config", "user.name", "Courier Test")
	git(t, work, "config", "user.email", "courier-test@example.invalid")
	git(t, work, "checkout", "-b", branch, "origin/main")
	writeFile(t, filepath.Join(work, "work.txt"), "completed brief\n")
	commit(t, work, "work: previous brief")
	git(t, work, "push", "origin", "HEAD:refs/heads/"+branch)
	publishedHead := strings.TrimSpace(string(mustGit(t, work, "rev-parse", branch)))

	// The destination holds a checkout left behind by an earlier attempt: a
	// local-only merge of a base the run must not have, an uncommitted edit
	// to a tracked file, and an untracked file.
	workspaceDir := filepath.Join(root, "workspace")
	git(t, root, "clone", remote, workspaceDir)
	git(t, workspaceDir, "config", "user.name", "Courier Test")
	git(t, workspaceDir, "config", "user.email", "courier-test@example.invalid")
	git(t, workspaceDir, "checkout", "-B", branch, "origin/"+branch)
	git(t, workspaceDir, "checkout", "-b", "stale-base", "origin/main")
	writeFile(t, filepath.Join(workspaceDir, "stale-base.txt"), "stale base\n")
	commit(t, workspaceDir, "stale: base the run must not merge")
	git(t, workspaceDir, "checkout", branch)
	git(t, workspaceDir, "merge", "--no-edit", "stale-base")
	staleMerge := strings.TrimSpace(string(mustGit(t, workspaceDir, "rev-parse", "HEAD")))
	parents := strings.Fields(string(mustGit(t, workspaceDir, "rev-list", "--parents", "-n", "1", "HEAD")))
	if len(parents) != 3 {
		t.Fatalf("workspace HEAD = %q is not a merge commit: the fixture must reproduce the incident's local-only merge", staleMerge)
	}
	writeFile(t, filepath.Join(workspaceDir, "README.md"), "edited by an earlier attempt\n")
	writeFile(t, filepath.Join(workspaceDir, "untracked.txt"), "scratch\n")

	workspace, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: workspaceDir,
		Base:      "main",
		Branch:    branch,
	})
	if err != nil {
		t.Fatalf("prepare over a leftover checkout: %v", err)
	}
	if !workspace.Adopted {
		t.Fatal("expected the published branch to be adopted")
	}
	head := strings.TrimSpace(string(mustGit(t, workspaceDir, "rev-parse", "HEAD")))
	if head != publishedHead {
		t.Fatalf("workspace HEAD = %q, want the published head %q: preparation must rebuild the branch from the published head", head, publishedHead)
	}
	merges := strings.TrimSpace(string(mustGit(t, workspaceDir, "log", "--merges", "--oneline")))
	if merges != "" {
		t.Fatalf("workspace history holds merge commits %q, want none: the stale merge must not survive preparation", merges)
	}
	if _, err := gitOutput(workspaceDir, "merge-base", "--is-ancestor", staleMerge, "HEAD"); err == nil {
		t.Fatalf("the stale local-only merge %q survived preparation", staleMerge)
	}
	if _, err := gitOutput(workspaceDir, "merge-base", "--is-ancestor", "origin/main", "HEAD"); err != nil {
		t.Fatalf("base is not an ancestor after preparation: %v", err)
	}
	if got := strings.TrimSpace(string(mustRead(t, filepath.Join(workspaceDir, "README.md")))); got != "base one" {
		t.Fatalf("tracked file content = %q, want the published content: the uncommitted edit must not survive preparation", got)
	}
	if _, err := os.Stat(filepath.Join(workspaceDir, "untracked.txt")); !os.IsNotExist(err) {
		t.Fatalf("the untracked file survived preparation (stat error %v)", err)
	}
	url := strings.TrimSpace(string(mustGit(t, workspaceDir, "remote", "get-url", "origin")))
	if url != remote {
		t.Fatalf("work remote url = %q, want the configured remote %q", url, remote)
	}
}

// The clear step refuses paths that could wipe a system directory: the
// filesystem root, and a top-level path that is not a mounted volume root.
func TestClearableDirectoryRefusesSystemPaths(t *testing.T) {
	t.Parallel()
	clearable, err := clearableDirectory("/")
	if err != nil || clearable {
		t.Fatalf("clearableDirectory(/) = (%v, %v), want (false, nil)", clearable, err)
	}
	clearable, err = clearableDirectory("/courier-no-such-workspace")
	if err == nil && clearable {
		t.Fatal("clearableDirectory(/courier-no-such-workspace) = true, want a top-level path refused")
	}
	clearable, err = clearableDirectory(t.TempDir())
	if err != nil || !clearable {
		t.Fatalf("clearableDirectory(temp dir) = (%v, %v), want (true, nil)", clearable, err)
	}
	// /proc is a mounted volume root in every Linux environment: the pod
	// mounts its workspace volume the same way, at a single-segment path.
	clearable, err = clearableDirectory("/proc")
	if err != nil || !clearable {
		t.Fatalf("clearableDirectory(/proc) = (%v, %v), want a mount root allowed", clearable, err)
	}
}

// The rebuild removes a symlink inside the workspace but never follows it: a
// link pointing outside the workspace leaves its target alone.
func TestPrepareRebuildDoesNotFollowSymlinks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	outside := filepath.Join(root, "outside.txt")
	writeFile(t, outside, "outside the workspace\n")

	workspaceDir := filepath.Join(root, "workspace")
	git(t, root, "clone", remote, workspaceDir)
	git(t, workspaceDir, "config", "user.name", "Courier Test")
	git(t, workspaceDir, "config", "user.email", "courier-test@example.invalid")
	link := filepath.Join(workspaceDir, "link.txt")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	if _, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: workspaceDir,
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/18",
	}); err != nil {
		t.Fatalf("prepare over a leftover checkout: %v", err)
	}
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Fatalf("the symlink survived preparation (stat error %v)", err)
	}
	if got := strings.TrimSpace(string(mustRead(t, outside))); got != "outside the workspace" {
		t.Fatalf("the symlink target was followed and altered: %q", got)
	}
}

// A symlink standing at the workspace path is refused: clearing through it
// would empty the checkout the link points at, a directory outside the
// configured workspace, so preparation fails closed and leaves the target
// untouched.
func TestPrepareRefusesSymlinkedWorkspacePath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	// The destination path is a symlink to a git checkout elsewhere.
	target := filepath.Join(root, "elsewhere")
	git(t, root, "clone", remote, target)
	git(t, target, "checkout", "main")
	headBefore := strings.TrimSpace(string(mustGit(t, target, "rev-parse", "HEAD")))
	link := filepath.Join(root, "workspace")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	_, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: link,
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/19",
	})
	if err == nil {
		t.Fatal("preparing into a symlinked workspace path succeeded, want refusal")
	}
	if !strings.Contains(err.Error(), link) {
		t.Fatalf("error %q does not name the refused path %q", err, link)
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error %q does not state that the path is a symlink", err)
	}
	if got := strings.TrimSpace(string(mustGit(t, target, "rev-parse", "HEAD"))); got != headBefore {
		t.Fatalf("the linked checkout moved from %q to %q: preparation must not touch it", headBefore, got)
	}
	if _, statErr := os.Stat(filepath.Join(target, "README.md")); statErr != nil {
		t.Fatalf("preparation cleared the linked checkout: %v", statErr)
	}
}

// A workspace path carrying a null byte is refused before any filesystem
// interpretation: no lookup, clone, or clear touches anything.
func TestPrepareRefusesNullByteWorkspacePath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	directory := filepath.Join(root, "workspace") + "\x00trailing"
	_, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: directory,
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/20",
	})
	if err == nil {
		t.Fatal("preparing into a path with a null byte succeeded, want refusal")
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 2 {
		t.Fatalf("the refused preparation created entries in %q: %v", root, entries)
	}
}

// A destination that holds files but no git checkout is refused: preparation
// never deletes anything it did not create, so it fails closed and names the
// path it refused.
func TestPrepareRefusesNonCheckoutDestination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	workspaceDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(workspaceDir, "leftover.txt"), "not a checkout\n")

	_, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: workspaceDir,
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/15",
	})
	if err == nil {
		t.Fatal("preparing into a non-checkout directory succeeded")
	}
	if !strings.Contains(err.Error(), workspaceDir) {
		t.Fatalf("error %q does not name the refused path %q", err, workspaceDir)
	}
	if !strings.Contains(err.Error(), "not a git checkout") {
		t.Fatalf("error %q does not state that the directory holds no git checkout", err)
	}
	if _, statErr := os.Stat(filepath.Join(workspaceDir, "leftover.txt")); statErr != nil {
		t.Fatalf("preparation removed a file it did not create: %v", statErr)
	}
}

// A leftover checkout can belong to another repository.  The rebuild leaves
// the workspace bound to the configured remote and holding that remote's
// published head: the other repository's content is gone.
func TestPrepareRebuildsWorkspaceBoundToConfiguredRemote(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	other := filepath.Join(root, "other.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initBare(t, other)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")
	writeFile(t, filepath.Join(source, "other-repo.txt"), "other repository\n")
	commit(t, source, "other: repository content")
	git(t, source, "remote", "add", "other", other)
	git(t, source, "push", "other", "main")

	const branch = "courier/resolve-issue/acme-widget/16"
	work := filepath.Join(root, "work")
	git(t, root, "clone", remote, work)
	git(t, work, "config", "user.name", "Courier Test")
	git(t, work, "config", "user.email", "courier-test@example.invalid")
	git(t, work, "checkout", "-b", branch, "origin/main")
	writeFile(t, filepath.Join(work, "work.txt"), "completed brief\n")
	commit(t, work, "work: previous brief")
	git(t, work, "push", "origin", "HEAD:refs/heads/"+branch)
	publishedHead := strings.TrimSpace(string(mustGit(t, work, "rev-parse", branch)))

	// The leftover checkout belongs to the other repository.
	workspaceDir := filepath.Join(root, "workspace")
	git(t, root, "clone", other, workspaceDir)
	git(t, workspaceDir, "config", "user.name", "Courier Test")
	git(t, workspaceDir, "config", "user.email", "courier-test@example.invalid")
	git(t, workspaceDir, "checkout", "main")

	workspace, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: workspaceDir,
		Base:      "main",
		Branch:    branch,
	})
	if err != nil {
		t.Fatalf("prepare over a leftover checkout: %v", err)
	}
	if !workspace.Adopted {
		t.Fatal("expected the published branch to be adopted")
	}
	url := strings.TrimSpace(string(mustGit(t, workspaceDir, "remote", "get-url", "origin")))
	if url != remote {
		t.Fatalf("work remote url = %q, want the configured remote %q", url, remote)
	}
	head := strings.TrimSpace(string(mustGit(t, workspaceDir, "rev-parse", "HEAD")))
	if head != publishedHead {
		t.Fatalf("workspace HEAD = %q, want the published head %q", head, publishedHead)
	}
	if _, err := os.Stat(filepath.Join(workspaceDir, "other-repo.txt")); !os.IsNotExist(err) {
		t.Fatalf("the other repository's content survived preparation (stat error %v)", err)
	}
}

func TestPrepareCreatesBranchFromBase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "base.txt"), "base\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	workspace, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: filepath.Join(root, "workspace"),
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/11",
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if workspace.Adopted {
		t.Fatal("new branch was unexpectedly marked adopted")
	}
	if got := strings.TrimSpace(string(mustRead(t, filepath.Join(workspace.Directory, "base.txt")))); got != "base" {
		t.Fatalf("new branch was not based on main: %q", got)
	}
}

func TestRemoteBranchExists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	exists, err := RemoteBranchExists(ctx, remote, "main")
	if err != nil {
		t.Fatalf("RemoteBranchExists(main): %v", err)
	}
	if !exists {
		t.Fatal("expected main to be advertised by the remote")
	}

	exists, err = RemoteBranchExists(ctx, remote, "courier/resolve-issue/acme-widget/10")
	if err != nil {
		t.Fatalf("RemoteBranchExists(missing): %v", err)
	}
	if exists {
		t.Fatal("expected a missing branch to report false")
	}

	if _, err := RemoteBranchExists(ctx, remote, "-not-a-branch"); err == nil {
		t.Fatal("RemoteBranchExists accepted an invalid ref")
	}
}

func TestRemoteBranchHead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")
	shaOutput, err := exec.Command("git", "-C", source, "rev-parse", "main").Output()
	if err != nil {
		t.Fatalf("read local main sha: %v", err)
	}
	want := strings.TrimSpace(string(shaOutput))

	head, exists, err := RemoteBranchHead(ctx, remote, "main")
	if err != nil {
		t.Fatalf("RemoteBranchHead(main): %v", err)
	}
	if !exists || head != want {
		t.Fatalf("RemoteBranchHead(main) = (%q, %v), want (%q, true)", head, exists, want)
	}

	head, exists, err = RemoteBranchHead(ctx, remote, "courier/resolve-issue/acme-widget/10")
	if err != nil {
		t.Fatalf("RemoteBranchHead(missing): %v", err)
	}
	if exists || head != "" {
		t.Fatalf("RemoteBranchHead(missing) = (%q, %v), want empty and false", head, exists)
	}

	if _, _, err := RemoteBranchHead(ctx, remote, "-not-a-branch"); err == nil {
		t.Fatal("RemoteBranchHead accepted an invalid ref")
	}
}

func TestTrustDirectoryAddsOnlyConfiguredPath(t *testing.T) {
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(t.TempDir(), "gitconfig"))
	directory := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := TrustDirectory(context.Background(), directory); err != nil {
		t.Fatalf("TrustDirectory: %v", err)
	}
	output, err := gitOutput(directory, "config", "--global", "--get-all", "safe.directory")
	if err != nil {
		t.Fatalf("read safe.directory: %v", err)
	}
	if got := strings.TrimSpace(string(output)); got != directory {
		t.Fatalf("safe.directory = %q, want %q", got, directory)
	}
	if err := TrustDirectory(context.Background(), "*"); err == nil {
		t.Fatal("TrustDirectory accepted a wildcard path")
	}
}

func TestWorkStateDistinguishesNoWorkDirtyWorkAndCommits(t *testing.T) {
	root := t.TempDir()
	initRepo(t, root)
	writeFile(t, filepath.Join(root, "base.txt"), "base\n")
	commit(t, root, "base: initial")
	workspace := &Workspace{Directory: root}
	start, err := workspace.Head(context.Background())
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	state, err := workspace.WorkState(context.Background(), start)
	if err != nil {
		t.Fatalf("WorkState(no work): %v", err)
	}
	if state != WorkStateNone {
		t.Fatalf("WorkState(no work) = %q, want %q", state, WorkStateNone)
	}

	writeFile(t, filepath.Join(root, "partial.txt"), "partial\n")
	state, err = workspace.WorkState(context.Background(), start)
	if err != nil {
		t.Fatalf("WorkState(dirty): %v", err)
	}
	if state != WorkStateDirty {
		t.Fatalf("WorkState(dirty) = %q, want %q", state, WorkStateDirty)
	}

	commit(t, root, "work: complete")
	state, err = workspace.WorkState(context.Background(), start)
	if err != nil {
		t.Fatalf("WorkState(committed): %v", err)
	}
	if state != WorkStateCommitted {
		t.Fatalf("WorkState(committed) = %q, want %q", state, WorkStateCommitted)
	}

	writeFile(t, filepath.Join(root, "scratch.txt"), "temporary\n")
	state, err = workspace.WorkState(context.Background(), start)
	if err != nil || state != WorkStateCommitted {
		t.Fatalf("WorkState(commit with scratch) = %q, %v; want committed", state, err)
	}
	writeFile(t, filepath.Join(root, "base.txt"), "edited\n")
	state, err = workspace.WorkState(context.Background(), start)
	if err != nil || state != WorkStateDirty {
		t.Fatalf("WorkState(commit with tracked edit) = %q, %v; want dirty", state, err)
	}
	commit(t, root, "work: tracked edit")
	writeFile(t, filepath.Join(root, "base.txt"), "staged\n")
	git(t, root, "add", "base.txt")
	state, err = workspace.WorkState(context.Background(), start)
	if err != nil || state != WorkStateDirty {
		t.Fatalf("WorkState(commit with staged edit) = %q, %v; want dirty", state, err)
	}
}

func TestStatusPorcelainReturnsChangePathsNotRawLines(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	initRepo(t, root)
	writeFile(t, filepath.Join(root, "tracked.txt"), "base\n")
	writeFile(t, filepath.Join(root, "moved.txt"), "move me\n")
	commit(t, root, "base: initial")
	workspace := &Workspace{Directory: root}

	// A modified tracked file, an untracked file, and a rename.
	writeFile(t, filepath.Join(root, "tracked.txt"), "edited\n")
	writeFile(t, filepath.Join(root, "untracked.txt"), "new\n")
	git(t, root, "mv", "moved.txt", "renamed.txt")

	paths, err := workspace.StatusPorcelain(ctx)
	if err != nil {
		t.Fatalf("StatusPorcelain: %v", err)
	}
	slices.Sort(paths)
	want := []string{"renamed.txt", "tracked.txt", "untracked.txt"}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("StatusPorcelain() = %v, want %v (paths, not porcelain lines; a rename reports the new name, not the origin)", paths, want)
	}
}

func TestCurrentBranchDistinguishesAttachedAndDetached(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	initRepo(t, root)
	writeFile(t, filepath.Join(root, "base.txt"), "base\n")
	commit(t, root, "base: initial")
	git(t, root, "checkout", "-b", "feature/work")

	workspace := &Workspace{Directory: root}
	got, err := workspace.CurrentBranch(ctx)
	if err != nil {
		t.Fatalf("CurrentBranch(attached): %v", err)
	}
	if got != "feature/work" {
		t.Fatalf("CurrentBranch(attached) = %q, want %q", got, "feature/work")
	}

	git(t, root, "checkout", "--detach", "HEAD")
	got, err = workspace.CurrentBranch(ctx)
	if err != nil {
		t.Fatalf("CurrentBranch(detached): %v", err)
	}
	if got != "" {
		t.Fatalf("CurrentBranch(detached) = %q, want empty", got)
	}

	if _, err := (&Workspace{}).CurrentBranch(ctx); err == nil {
		t.Fatal("CurrentBranch with an empty directory succeeded, want an error")
	}
}

func TestCommitsOnBranchSince(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	initRepo(t, root)
	writeFile(t, filepath.Join(root, "base.txt"), "base\n")
	commit(t, root, "base: initial")
	git(t, root, "checkout", "-b", "feature/work")

	workspace := &Workspace{Directory: root}
	start, err := workspace.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	// A commit made while on the branch advances the branch ref.
	writeFile(t, filepath.Join(root, "work.txt"), "work\n")
	commit(t, root, "work: on branch")
	count, err := workspace.CommitsOnBranchSince(ctx, "feature/work", start)
	if err != nil {
		t.Fatalf("CommitsOnBranchSince(advanced branch): %v", err)
	}
	if count < 1 {
		t.Fatalf("CommitsOnBranchSince(advanced branch) = %d, want >= 1", count)
	}

	// A commit made from a detached HEAD does not advance the branch ref, so
	// the count measured from the branch's own tip stays at zero.
	tip, err := workspace.Head(ctx)
	if err != nil {
		t.Fatalf("Head(branch tip): %v", err)
	}
	git(t, root, "checkout", "--detach")
	writeFile(t, filepath.Join(root, "detached.txt"), "detached\n")
	commit(t, root, "work: on detached head")
	count, err = workspace.CommitsOnBranchSince(ctx, "feature/work", tip)
	if err != nil {
		t.Fatalf("CommitsOnBranchSince(after detach): %v", err)
	}
	if count != 0 {
		t.Fatalf("CommitsOnBranchSince(after detach) = %d, want 0 (branch ref did not advance)", count)
	}

	// A branch name that does not exist reports zero without an error.
	count, err = workspace.CommitsOnBranchSince(ctx, "feature/missing", tip)
	if err != nil {
		t.Fatalf("CommitsOnBranchSince(missing branch): %v", err)
	}
	if count != 0 {
		t.Fatalf("CommitsOnBranchSince(missing branch) = %d, want 0", count)
	}

	if _, err := (&Workspace{}).CommitsOnBranchSince(ctx, "feature/work", tip); err == nil {
		t.Fatal("CommitsOnBranchSince with an empty directory succeeded, want an error")
	}
}

func TestBranchNameRejectsMissingRef(t *testing.T) {
	if _, err := BranchName("acme/widget", 0, "resolve-issue"); err == nil {
		t.Fatal("BranchName accepted a missing ref")
	}
	first, err := BranchName("acme/widget", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("BranchName: %v", err)
	}
	second, err := BranchName("acme/widget", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("BranchName second call: %v", err)
	}
	if first != second {
		t.Fatalf("BranchName is not deterministic: %q != %q", first, second)
	}
}

func TestBranchNamePreservesRepositoryIdentityWhenSlugging(t *testing.T) {
	dotted, err := BranchName("acme/foo.bar", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("dotted BranchName: %v", err)
	}
	dashed, err := BranchName("acme/foo-bar", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("dashed BranchName: %v", err)
	}
	if dotted == dashed {
		t.Fatalf("distinct repositories collided on branch %q", dotted)
	}
}

func initBare(t *testing.T, directory string) {
	t.Helper()
	git(t, filepath.Dir(directory), "init", "--bare", directory)
}

func initRepo(t *testing.T, directory string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, directory, "init")
	git(t, directory, "config", "user.name", "Courier Test")
	git(t, directory, "config", "user.email", "courier-test@example.invalid")
}

func commit(t *testing.T, directory, message string) {
	t.Helper()
	git(t, directory, "add", "--all", "--", ".")
	git(t, directory, "commit", "-m", message)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func git(t *testing.T, directory string, args ...string) {
	t.Helper()
	if _, err := gitOutput(directory, args...); err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
}

func gitOutput(directory string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = directory
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func TestBlobAtReadsRevisionScopedContentAndClassifiesAbsence(t *testing.T) {
	repo := t.TempDir()
	initRepo(t, repo)
	writeFile(t, filepath.Join(repo, "tracked.txt"), "base\n")
	git(t, repo, "add", "--all", "--", ".")
	git(t, repo, "commit", "-m", "base")
	base, err := HeadOID(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(repo, ".mise"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(repo, ".mise", "config.toml"), "[tools]\ngo = \"1.27\"\n")
	git(t, repo, "add", "--all", "--", ".")
	git(t, repo, "commit", "-m", "contract")

	content, ok, err := BlobAt(context.Background(), repo, "HEAD", ".mise/config.toml")
	if err != nil || !ok {
		t.Fatalf("BlobAt HEAD = (%q, %v, %v)", content, ok, err)
	}
	if string(content) != "[tools]\ngo = \"1.27\"\n" {
		t.Fatalf("BlobAt content = %q", content)
	}
	if _, ok, _ := BlobAt(context.Background(), repo, base, ".mise/config.toml"); ok {
		t.Fatal("BlobAt reported a contract path at a revision without one")
	}
	if _, ok, _ := BlobAt(context.Background(), repo, "HEAD", "missing.txt"); ok {
		t.Fatal("BlobAt reported a path the revision does not carry")
	}
	if _, _, err := BlobAt(context.Background(), repo, "", ".mise/config.toml"); err == nil {
		t.Fatal("BlobAt accepted an empty revision")
	}
}

// writeFakeGit installs an executable stand-in for the git binary on PATH and
// returns the directory holding it, so a test can drive run()'s process-level
// outcomes — a process that never starts, one that runs and exits, and one
// killed by its context — without reaching the network.
func writeFakeGit(t *testing.T, script string) string {
	t.Helper()
	directory := t.TempDir()
	executable := filepath.Join(directory, "git")
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	return directory
}

// TestCommandErrorTransientAvailabilityClassification pins the process-level
// evidence rule behind CommandError.TransientAvailability: a git process that
// started and was killed by its context deadline left the world unobserved
// and classifies transient through forge.IsTransientAvailability, while a
// process that ran and exited under its own control is git's own verdict and
// never does. A process that never started failed inside the local executor
// — a missing git binary, for instance — which is evidence about this
// machine's configuration, not about any remote, so it fails closed. The
// classification reads the process state, never git's stderr text.
func TestCommandErrorTransientAvailabilityClassification(t *testing.T) {
	t.Run("killed by the context deadline", func(t *testing.T) {
		t.Setenv("PATH", writeFakeGit(t, "#!/bin/sh\nexec /bin/sleep 30\n"))
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := run(ctx, "", "sleep")
		if err == nil {
			t.Fatal("expected the killed git process to fail")
		}
		if !forge.IsTransientAvailability(err) {
			t.Fatalf("a git process killed by its context deadline must classify transient: %v", err)
		}
	})
	t.Run("ran and exited", func(t *testing.T) {
		t.Setenv("PATH", writeFakeGit(t, "#!/bin/sh\nexit 3\n"))
		_, err := run(context.Background(), "", "status")
		if err == nil {
			t.Fatal("expected the failing git process to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("a git process that ran and exited carries its own verdict, not a transient availability failure: %v", err)
		}
	})
	t.Run("never started", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		_, err := run(context.Background(), "", "status")
		if err == nil {
			t.Fatal("expected the missing git binary to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("a git process that never started is a permanent local executor failure, not a transient availability failure: %v", err)
		}
	})
}

// TestRemoteBranchHeadClassifiesEndpointEvidence pins the network-level
// evidence rule behind the git-side remote-publication read: a git process
// that ran and failed is classified by the remote's own endpoint, never by
// git's stderr text. An endpoint that answers with 429 or 5xx, and an
// endpoint the probe cannot complete against, classify transient
// availability; an endpoint that answered with any other status carries
// git's verdict and keeps the fail-closed behavior, as does a remote with no
// probeable network endpoint.
func TestRemoteBranchHeadClassifiesEndpointEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	statusServer := func(status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		}))
	}
	resetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(resetServer.Close)

	cases := []struct {
		name      string
		remoteURL string
		transient bool
	}{
		{"endpoint answers 500", statusServer(http.StatusInternalServerError).URL, true},
		{"endpoint answers 429", statusServer(http.StatusTooManyRequests).URL, true},
		{"endpoint drops the connection", resetServer.URL, true},
		{"endpoint answers 404", statusServer(http.StatusNotFound).URL, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := RemoteBranchHead(ctx, tc.remoteURL, "main")
			if err == nil {
				t.Fatal("expected the remote read to fail")
			}
			if got := forge.IsTransientAvailability(err); got != tc.transient {
				t.Fatalf("RemoteBranchHead(%s) transient availability = %v, want %v: %v", tc.remoteURL, got, tc.transient, err)
			}
		})
	}
	t.Run("local path carries no network endpoint", func(t *testing.T) {
		_, _, err := RemoteBranchHead(ctx, t.TempDir(), "main")
		if err == nil {
			t.Fatal("expected the remote read against a non-repository path to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("a remote with no probeable network endpoint must keep the fail-closed classification: %v", err)
		}
	})
}

// TestHTTPEndpointProbeDoesNotFollowRedirects pins the probe's shape: it is
// exactly one request to the endpoint git itself would read, never a
// redirect chain to another host. A redirect answer proves the endpoint
// answered, so it classifies available and leaves git's verdict standing.
func TestHTTPEndpointProbeDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var hits, escapeHatches int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escapeHatches++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(target.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Redirect(w, r, target.URL+"/escape", http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	parsed, err := url.Parse(redirector.URL)
	if err != nil {
		t.Fatalf("parse probe URL: %v", err)
	}
	if !httpEndpointAvailable(context.Background(), parsed) {
		t.Fatal("a redirect answer proves the endpoint answered, so the probe must classify it available")
	}
	if hits != 1 {
		t.Fatalf("probe request count = %d, want exactly 1", hits)
	}
	if escapeHatches != 0 {
		t.Fatalf("the probe followed the redirect to another host %d time(s)", escapeHatches)
	}
}

// TestRemoteBranchHeadNeverStartedFailsClosed pins the boundary between a
// failed remote read and a local executor failure: when the git process
// never started — a missing binary on PATH, for instance — the read carried
// no transport evidence at all, so it must fail closed to the
// verifier-rejection ending even when the remote's endpoint is genuinely
// unreachable, rather than masquerading as forge unavailability.
func TestRemoteBranchHeadNeverStartedFailsClosed(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	endpoint.Close()
	t.Setenv("PATH", t.TempDir())
	_, _, err := RemoteBranchHead(context.Background(), endpoint.URL, "main")
	if err == nil {
		t.Fatal("expected the remote read to fail")
	}
	if forge.IsTransientAvailability(err) {
		t.Fatalf("a remote read whose git process never started is a local executor failure, not transient availability: %v", err)
	}
}

// TestScpLikeHostExtractsGitScpLikeForms pins the scp-like host extraction
// behind the ssh probe: git accepts the scp-like ssh form both with and
// without a user — user@host:path and host:path — so a userless host must
// still produce the dial target, while anything git itself would not treat
// as a remote host (a local path, a slash or space before the separator, a
// bracketed IPv6 literal) yields the empty string and fails closed.
func TestScpLikeHostExtractsGitScpLikeForms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		remoteURL string
		host      string
	}{
		{"user at host path", "git@github.com:owner/repo.git", "github.com"},
		{"userless host path", "github.com:owner/repo.git", "github.com"},
		{"userless bare host", "host:repo", "host"},
		{"at in the path is not a user", "host:pa@th", "host"},
		{"absolute local path", "/tmp/checkout:thing", ""},
		{"relative local path", "dir/file:thing", ""},
		{"space in the host", "my host:repo", ""},
		{"backslash in the host", `dir\file:thing`, ""},
		{"empty user", "@github.com:repo", ""},
		{"empty host", "user@:repo", ""},
		{"bracketed ipv6 literal", "[::1]:repo", ""},
		{"unbracketed ipv6 literal", "::1:repo", ""},
		{"ipv6 literal splits as git splits", "user@fe80::1:repo", "fe80"},
		{"no colon", "github.com/owner/repo.git", ""},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := scpLikeHost(tc.remoteURL); got != tc.host {
				t.Fatalf("scpLikeHost(%q) = %q, want %q", tc.remoteURL, got, tc.host)
			}
		})
	}
}

// TestRemoteBranchHeadClassifiesSSHEndpointEvidence pins the ssh dial probe
// for scheme-carrying ssh remotes: an endpoint the dial cannot complete
// against classifies transient availability, an endpoint the dial reaches
// keeps git's own fail-closed verdict.
func TestRemoteBranchHeadClassifiesSSHEndpointEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	refused := func(t *testing.T) string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve a port: %v", err)
		}
		addr := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatalf("release the reserved port: %v", err)
		}
		return "ssh://" + addr + "/owner/repo.git"
	}
	listening := func(t *testing.T) string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_ = conn.Close()
			}
		}()
		return "ssh://" + listener.Addr().String() + "/owner/repo.git"
	}

	t.Run("dial cannot complete", func(t *testing.T) {
		t.Parallel()
		_, _, err := RemoteBranchHead(ctx, refused(t), "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if !forge.IsTransientAvailability(err) {
			t.Fatalf("an ssh endpoint the dial cannot reach must classify transient: %v", err)
		}
	})
	t.Run("dial reaches the endpoint", func(t *testing.T) {
		t.Parallel()
		_, _, err := RemoteBranchHead(ctx, listening(t), "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("an ssh endpoint the dial reached must keep the fail-closed classification: %v", err)
		}
	})
}
