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
	// The ssh probe resolves through the ssh command the environment and
	// git's configuration select, so a developer's own GIT_SSH_COMMAND,
	// GIT_SSH, or core.sshCommand must not redirect it under test; the tests
	// that exercise that selection set it themselves. Unset, not empty: git
	// runs a present-but-blank value as the command itself, and the probe
	// must see the environment git would see.
	for _, key := range []string{"GIT_SSH_COMMAND", "GIT_SSH"} {
		if err := os.Unsetenv(key); err != nil {
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

// TestLocalGitFailureNeverClassifiesAsTransientAvailability pins the
// boundary the transient-availability marker must not cross: the marker
// belongs to the remote-publication read alone, so no local git command
// carries it.
//
// A git process that started and was killed by its context deadline — an
// OOM-kill, a pod eviction, or the run's own budget — says nothing about
// any forge, and the run must never end blocked_external /
// forge-unavailable on its account. A process that ran and exited under its
// own control is git's own verdict and never transient. A process that never
// started failed inside the local executor — a missing git binary, for
// instance — which is evidence about this machine's configuration, not about
// any remote, so it fails closed. The classification reads the process
// state, never git's stderr text.
func TestLocalGitFailureNeverClassifiesAsTransientAvailability(t *testing.T) {
	t.Run("killed by the context deadline", func(t *testing.T) {
		t.Setenv("PATH", writeFakeGit(t, "#!/bin/sh\nexec /bin/sleep 30\n"))
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := run(ctx, "", "sleep")
		if err == nil {
			t.Fatal("expected the killed git process to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("a local git command killed by its context deadline must not classify as transient availability: %v", err)
		}

		// The workspace head read is a local read of this machine: it
		// never reaches a forge, so a forge must never be blamed for
		// it.
		headCtx, headCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer headCancel()
		workspace := &Workspace{Directory: t.TempDir()}
		_, err = workspace.Head(headCtx)
		if err == nil {
			t.Fatal("expected the killed workspace head read to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("a local workspace head read killed by its context deadline must not classify as transient availability: %v", err)
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

// TestRemoteBranchHeadCutShortIsTransientAvailability pins the other side of
// that boundary: a remote read whose git process was cut short — killed by
// its deadline, its cancellation, or a signal — never observed the remote,
// so it is transport evidence on its own and classifies transient without any
// endpoint probe. The remote's own verdict cannot stand here: there is none.
func TestRemoteBranchHeadCutShortIsTransientAvailability(t *testing.T) {
	t.Setenv("PATH", writeFakeGit(t, "#!/bin/sh\ncase \"$1\" in\ncheck-ref-format) exit 0 ;;\n*) exec /bin/sleep 30 ;;\nesac\n"))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := RemoteBranchHead(ctx, "https://example.invalid/owner/repo.git", "main")
	if err == nil {
		t.Fatal("expected the cut-short remote read to fail")
	}
	if !forge.IsTransientAvailability(err) {
		t.Fatalf("a remote read cut short before git answered must classify transient: %v", err)
	}
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

// TestEndpointProbeYieldsNoEvidenceWhenCallerContextEnded pins the probe's
// evidence rule: the probe's answer is the evidence, and a probe the
// caller's own cancellation cut short observed nothing at all.
//
// Each helper therefore reports the same value for an endpoint that answered
// as for a probe that never got to ask — no evidence that the endpoint was
// unavailable — so a completed git verdict is never reclassified on the
// strength of the caller's own cancellation, deadline, or pod shutdown.
func TestEndpointProbeYieldsNoEvidenceWhenCallerContextEnded(t *testing.T) {
	t.Parallel()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("http", func(t *testing.T) {
		t.Parallel()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)
		parsed, err := url.Parse(server.URL)
		if err != nil {
			t.Fatalf("parse probe URL: %v", err)
		}
		answered := httpEndpointAvailable(context.Background(), parsed)
		if !answered {
			t.Fatal("an endpoint that answered must report no evidence of unavailability")
		}
		if got := httpEndpointAvailable(canceled, parsed); got != answered {
			t.Fatalf("httpEndpointAvailable(canceled caller) = %v, want %v: a probe the caller's cancellation cut short observed nothing", got, answered)
		}
	})

	t.Run("ssh", func(t *testing.T) {
		t.Parallel()
		address := accepting(t, func(conn net.Conn) {
			_, _ = conn.Write([]byte("SSH-2.0-probe\r\n"))
			_ = conn.Close()
		})
		answered := sshEndpointAvailable(context.Background(), address)
		if !answered {
			t.Fatal("an ssh endpoint that answered its banner must report no evidence of unavailability")
		}
		if got := sshEndpointAvailable(canceled, address); got != answered {
			t.Fatalf("sshEndpointAvailable(canceled caller) = %v, want %v: a probe the caller's cancellation cut short observed nothing", got, answered)
		}
	})

	t.Run("git protocol", func(t *testing.T) {
		t.Parallel()
		address := accepting(t, func(conn net.Conn) {
			buf := make([]byte, 128)
			_, _ = conn.Read(buf)
			_, _ = conn.Write([]byte("ERR access denied or repository not exported\n"))
			_ = conn.Close()
		})
		answered := gitEndpointAvailable(context.Background(), address, "127.0.0.1")
		if !answered {
			t.Fatal("a git:// endpoint that answered the probe request must report no evidence of unavailability")
		}
		if got := gitEndpointAvailable(canceled, address, "127.0.0.1"); got != answered {
			t.Fatalf("gitEndpointAvailable(canceled caller) = %v, want %v: a probe the caller's cancellation cut short observed nothing", got, answered)
		}
	})
}

// TestEndpointProbeYieldsNoEvidenceWhenCallerDeadlineEndsTheHandshake pins
// the deadline half of the same rule: a handshake the caller's own deadline
// ended observed nothing, so it is not evidence that the endpoint was
// unavailable. The endpoint here accepts the connection and stays silent, so
// the caller's deadline is the only thing that can end the wait.
func TestEndpointProbeYieldsNoEvidenceWhenCallerDeadlineEndsTheHandshake(t *testing.T) {
	t.Parallel()
	address := accepting(t, func(conn net.Conn) {
		<-time.After(5 * time.Second)
		_ = conn.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if !sshEndpointAvailable(ctx, address) {
		t.Fatal("a handshake the caller's deadline ended must report no evidence of unavailability")
	}
}

// TestEndpointProbeYieldsNoEvidenceWhenCallerCancelsMidHandshake pins the
// handshake half of the caller's-context rule: the caller's context can end
// after the connection is established but before the peer answers, and such
// a probe observed nothing at all — it must not be read as evidence that the
// endpoint was unavailable.
func TestEndpointProbeYieldsNoEvidenceWhenCallerCancelsMidHandshake(t *testing.T) {
	t.Parallel()
	silent := func(conn net.Conn) {
		<-time.After(5 * time.Second)
		_ = conn.Close()
	}

	t.Run("ssh", func(t *testing.T) {
		t.Parallel()
		address := accepting(t, silent)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// The connection is established first; the cancellation lands
		// while the handshake still waits for the peer's first byte.
		time.AfterFunc(100*time.Millisecond, cancel)
		if !sshEndpointAvailable(ctx, address) {
			t.Fatal("a handshake the caller's cancellation cut short must report no evidence of unavailability")
		}
	})

	t.Run("git protocol", func(t *testing.T) {
		t.Parallel()
		address := accepting(t, silent)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		time.AfterFunc(100*time.Millisecond, cancel)
		if !gitEndpointAvailable(ctx, address, "127.0.0.1") {
			t.Fatal("a git:// handshake the caller's cancellation cut short must report no evidence of unavailability")
		}
	})
}

// TestRemoteBranchHeadKeepsGitVerdictWhenProbeIsCutShort pins the rule that
// keeps a completed git verdict standing: the endpoint probe must not read
// the caller's own cancellation as evidence that the endpoint was
// unavailable.
//
// The remote here answers git with a completed non-zero verdict, and the
// probe's request is cut short by the caller's context ending mid-probe. The
// probe observed nothing, so the read stays a fail-closed verdict rather
// than becoming transient availability.
func TestRemoteBranchHeadKeepsGitVerdictWhenProbeIsCutShort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The caller's context ends while the probe is in flight, so
		// the probe's request fails with the context error and the
		// probe never observed the endpoint.
		cancel()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("PATH", writeFakeGit(t, "#!/bin/sh\ncase \"$1\" in\ncheck-ref-format) exit 0 ;;\nls-remote) exit 128 ;;\n*) exit 1 ;;\nesac\n"))
	_, _, err := RemoteBranchHead(ctx, server.URL, "main")
	if err == nil {
		t.Fatal("expected the remote read to fail")
	}
	if forge.IsTransientAvailability(err) {
		t.Fatalf("a completed git verdict must not be reclassified when the caller's context ends during the probe: %v", err)
	}
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
//
// The stand-in git removes itself once the ref has been validated, so the
// remote read itself cannot start a process: the local executor failed
// before any remote was contacted.
func TestRemoteBranchHeadNeverStartedFailsClosed(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	endpoint.Close()
	t.Setenv("PATH", writeFakeGit(t, "#!/bin/sh\ncase \"$1\" in\ncheck-ref-format) /bin/rm -f \"$0\" ;;\nesac\nexit 0\n"))
	_, _, err := RemoteBranchHead(context.Background(), endpoint.URL, "main")
	if err == nil {
		t.Fatal("expected the remote read to fail")
	}
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		t.Fatalf("expected a git command error, got %T: %v", err, err)
	}
	if commandErr.started {
		t.Fatalf("the remote read's git process started; the fixture must make it fail before the process starts: %v", err)
	}
	if forge.IsTransientAvailability(err) {
		t.Fatalf("a remote read whose git process never started is a local executor failure, not transient availability: %v", err)
	}
}

// TestScpLikeTargetExtractsGitScpLikeDestinations pins the ssh destination
// extraction behind the ssh probe: git accepts the scp-like ssh form both
// with and without a user — user@host:path and host:path — and hands that
// part to ssh as the destination, so the probe must extract exactly what
// ssh would match its configuration against, user included, while anything
// git itself would not treat as a remote host (a local path, a slash or
// space before the separator, a bracketed IPv6 literal) yields the empty
// string and fails closed.
func TestScpLikeTargetExtractsGitScpLikeDestinations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		remoteURL string
		target    string
	}{
		{"user at host path", "git@github.com:owner/repo.git", "git@github.com"},
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
		{"ipv6 literal splits as git splits", "user@fe80::1:repo", "user@fe80"},
		{"no colon", "github.com/owner/repo.git", ""},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := scpLikeTarget(tc.remoteURL); got != tc.target {
				t.Fatalf("scpLikeTarget(%q) = %q, want %q", tc.remoteURL, got, tc.target)
			}
		})
	}
}

// TestRemoteBranchHeadClassifiesSSHEndpointEvidence pins the ssh handshake
// probe for scheme-carrying ssh remotes: an endpoint the dial cannot complete
// against, or one that closes the connection before its identification
// banner, classifies transient availability — the handshake went unobserved;
// an endpoint that answers keeps git's own fail-closed verdict.
//
// The probe resolves the effective endpoint through ssh before dialing, so
// these cases need the ssh binary: without it the probe resolves nothing and
// yields no evidence at all.
func TestRemoteBranchHeadClassifiesSSHEndpointEvidence(t *testing.T) {
	t.Parallel()
	requireSSH(t)
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
	closing := func(t *testing.T) string {
		t.Helper()
		return "ssh://" + accepting(t, func(conn net.Conn) { _ = conn.Close() }) + "/owner/repo.git"
	}
	answering := func(t *testing.T) string {
		t.Helper()
		return "ssh://" + accepting(t, func(conn net.Conn) {
			_, _ = conn.Write([]byte("SSH-2.0-probe\r\n"))
			_ = conn.Close()
		}) + "/owner/repo.git"
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
	t.Run("connection lost before an answer", func(t *testing.T) {
		t.Parallel()
		_, _, err := RemoteBranchHead(ctx, closing(t), "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if !forge.IsTransientAvailability(err) {
			t.Fatalf("an ssh endpoint that closes before its banner must classify transient: %v", err)
		}
	})
	t.Run("endpoint answers", func(t *testing.T) {
		t.Parallel()
		_, _, err := RemoteBranchHead(ctx, answering(t), "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("an ssh endpoint that answered its banner must keep the fail-closed classification: %v", err)
		}
	})
}

// TestRemoteBranchHeadClassifiesGitProtocolEndpointEvidence pins the git://
// handshake probe: the git protocol makes the client speak first, so a
// daemon that answers the probe request keeps git's own fail-closed verdict,
// while one that closes or is unreachable before any answer classifies
// transient availability.
func TestRemoteBranchHeadClassifiesGitProtocolEndpointEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("endpoint answers", func(t *testing.T) {
		t.Parallel()
		addr := accepting(t, func(conn net.Conn) {
			buf := make([]byte, 128)
			_, _ = conn.Read(buf)
			_, _ = conn.Write([]byte("ERR access denied or repository not exported\n"))
			_ = conn.Close()
		})
		_, _, err := RemoteBranchHead(ctx, "git://"+addr+"/owner/repo.git", "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("a git:// endpoint that answered the probe request must keep the fail-closed classification: %v", err)
		}
	})
	t.Run("connection lost before an answer", func(t *testing.T) {
		t.Parallel()
		addr := accepting(t, func(conn net.Conn) { _ = conn.Close() })
		_, _, err := RemoteBranchHead(ctx, "git://"+addr+"/owner/repo.git", "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if !forge.IsTransientAvailability(err) {
			t.Fatalf("a git:// endpoint that closes before answering must classify transient: %v", err)
		}
	})
}

// accepting serves one-shot TCP connections on an ephemeral loopback
// listener, handling each accepted connection with handle, and returns the
// listener's address.
func accepting(t *testing.T, handle func(net.Conn)) string {
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
			handle(conn)
		}
	}()
	return listener.Addr().String()
}

// requireSSH skips the test when the machine has no ssh binary: the ssh
// probe resolves the effective endpoint through ssh, and without it the
// probe resolves no endpoint and yields no evidence, so the behavior under
// test cannot be observed.
func requireSSH(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("the ssh endpoint probe resolves the effective endpoint through ssh; this machine has no ssh binary")
	}
}

// useSSHUserConfig makes the real ssh binary read an isolated test
// configuration without relying on HOME. OpenSSH does not consistently use
// the HOME environment variable to locate ~/.ssh/config across platforms
// (notably the Linux CI runner), so a PATH wrapper injects -F explicitly
// while preserving the production call shape: sshEffectiveAddress still
// executes "ssh -G ..." and the real OpenSSH parser resolves the endpoint.
func useSSHUserConfig(t *testing.T, config string) {
	t.Helper()
	realSSH, err := exec.LookPath("ssh")
	if err != nil {
		t.Skip("the ssh endpoint probe resolves the effective endpoint through ssh; this machine has no ssh binary")
	}

	configPath := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write ssh test config: %v", err)
	}

	wrapperDir := t.TempDir()
	wrapper := filepath.Join(wrapperDir, "ssh")
	script := "#!/bin/sh\nexec \"$COURIER_TEST_REAL_SSH\" -F \"$COURIER_TEST_SSH_CONFIG\" \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatalf("write ssh config wrapper: %v", err)
	}

	t.Setenv("COURIER_TEST_REAL_SSH", realSSH)
	t.Setenv("COURIER_TEST_SSH_CONFIG", configPath)
	t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// writeFakeSSH installs an executable stand-in for the ssh binary on PATH
// and returns the directory holding it, so a test can pin the arguments the
// probe hands ssh and the output it reads back without a real ssh client.
func writeFakeSSH(t *testing.T, script string) string {
	t.Helper()
	directory := t.TempDir()
	executable := filepath.Join(directory, "ssh")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"+script), 0o700); err != nil {
		t.Fatalf("write fake ssh: %v", err)
	}
	return directory
}

// TestSSHEffectiveAddressResolvesTheConfiguredEndpoint pins the resolution
// the ssh probe's evidence rests on: git hands the destination to ssh, which
// applies the user's ssh configuration before connecting, so the probe must
// dial the endpoint ssh resolves to — not the literal URL host at port 22 —
// and an unconfigured destination must keep today's literal behavior.
func TestSSHEffectiveAddressResolvesTheConfiguredEndpoint(t *testing.T) {
	requireSSH(t)
	address := accepting(t, func(conn net.Conn) { _ = conn.Close() })
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	useSSHUserConfig(t, fmt.Sprintf(
		"Host forge584.invalid\n HostName %s\n Port %s\n", host, port))

	t.Run("the configured HostName and Port are the endpoint", func(t *testing.T) {
		endpoint, ok := sshEffectiveAddress(context.Background(), "git@forge584.invalid", "")
		if !ok {
			t.Fatal("a destination with a configured HostName and Port must resolve an effective endpoint")
		}
		if endpoint != address {
			t.Fatalf("sshEffectiveAddress = %q, want the configured endpoint %q", endpoint, address)
		}
	})
	t.Run("the url port overrides the configured port as it does for git", func(t *testing.T) {
		endpoint, ok := sshEffectiveAddress(context.Background(), "git@forge584.invalid", "8022")
		if !ok {
			t.Fatal("a destination with a configured HostName and an explicit url port must resolve an effective endpoint")
		}
		if want := net.JoinHostPort(host, "8022"); endpoint != want {
			t.Fatalf("sshEffectiveAddress = %q, want the url port over the configured host %q", endpoint, want)
		}
	})
	t.Run("an unconfigured destination keeps the literal host at port 22", func(t *testing.T) {
		endpoint, ok := sshEffectiveAddress(context.Background(), "plain584.example", "")
		if !ok {
			t.Fatal("an unconfigured destination must still resolve to its literal endpoint")
		}
		if want := "plain584.example:22"; endpoint != want {
			t.Fatalf("sshEffectiveAddress = %q, want %q", endpoint, want)
		}
	})
}

// TestSSHEffectiveAddressYieldsNoEndpointWhenUnresolvable pins the
// fail-closed half of the resolution: when the effective endpoint cannot be
// resolved there is no endpoint to probe, and the probe must yield no
// endpoint rather than guess one.
func TestSSHEffectiveAddressYieldsNoEndpointWhenUnresolvable(t *testing.T) {
	ctx := context.Background()

	t.Run("no ssh binary", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		if _, ok := sshEffectiveAddress(ctx, "forge584.invalid", ""); ok {
			t.Fatal("without an ssh binary the effective endpoint cannot be resolved; the probe must yield no endpoint, not guess one")
		}
	})
	t.Run("a destination starting with a dash never reaches ssh as an option", func(t *testing.T) {
		if _, ok := sshEffectiveAddress(ctx, "-oProxyCommand=touch-pwned", ""); ok {
			t.Fatal("a destination that would reach ssh as an option resolves no endpoint")
		}
	})
	t.Run("a non-numeric url port resolves no endpoint", func(t *testing.T) {
		if _, ok := sshEffectiveAddress(ctx, "forge584.invalid", "not-a-port"); ok {
			t.Fatal("a port git could not hand to ssh resolves no endpoint")
		}
	})
	t.Run("ssh rejects its configuration", func(t *testing.T) {
		requireSSH(t)
		useSSHUserConfig(t, "Host forge584.invalid\n\tNoSuchKeyword yes\n")
		if _, ok := sshEffectiveAddress(ctx, "forge584.invalid", ""); ok {
			t.Fatal("ssh rejecting the configuration means the effective endpoint cannot be resolved: the probe must yield no endpoint")
		}
	})
}

// TestSSHEffectiveAddressAsksSSHForTheGitDestination pins the exchange with
// ssh through a stand-in: the probe hands ssh exactly the destination and
// explicit port git would, and only a usable hostname and port in ssh's
// answer produce an endpoint.
func TestSSHEffectiveAddressAsksSSHForTheGitDestination(t *testing.T) {
	ctx := context.Background()
	stubSSH := func(t *testing.T, output string) string {
		t.Helper()
		argsFile := filepath.Join(t.TempDir(), "args")
		t.Setenv("COURIER_SSH_STUB_ARGS", argsFile)
		t.Setenv("PATH", withGit(t, writeFakeSSH(t, fmt.Sprintf(`printf '%%s\n' "$@" > "$COURIER_SSH_STUB_ARGS"
%s`, output))))
		return argsFile
	}

	t.Run("the destination and the url port reach ssh as git hands them", func(t *testing.T) {
		argsFile := stubSSH(t, "echo \"hostname forge.internal\"\necho \"port 2222\"\n")
		endpoint, ok := sshEffectiveAddress(ctx, "git@forge584.invalid", "2222")
		if !ok {
			t.Fatal("a usable hostname and port must produce an effective endpoint")
		}
		if want := "forge.internal:2222"; endpoint != want {
			t.Fatalf("sshEffectiveAddress = %q, want %q", endpoint, want)
		}
		args, err := os.ReadFile(argsFile)
		if err != nil {
			t.Fatalf("read ssh stub args: %v", err)
		}
		if want := "-G\n-p\n2222\ngit@forge584.invalid\n"; string(args) != want {
			t.Fatalf("ssh received %q, want %q", args, want)
		}
	})
	t.Run("a url without a port hands ssh no port override", func(t *testing.T) {
		argsFile := stubSSH(t, "echo \"hostname forge.internal\"\necho \"port 22\"\n")
		if _, ok := sshEffectiveAddress(ctx, "git@forge584.invalid", ""); !ok {
			t.Fatal("a usable hostname and port must produce an effective endpoint")
		}
		args, err := os.ReadFile(argsFile)
		if err != nil {
			t.Fatalf("read ssh stub args: %v", err)
		}
		if want := "-G\ngit@forge584.invalid\n"; string(args) != want {
			t.Fatalf("ssh received %q, want %q", args, want)
		}
	})
	t.Run("output without a usable port yields no endpoint", func(t *testing.T) {
		stubSSH(t, "echo \"hostname forge.internal\"\necho \"ciphers aes256-ctr\"\n")
		if _, ok := sshEffectiveAddress(ctx, "git@forge584.invalid", ""); ok {
			t.Fatal("output ssh did not answer with a port is not an effective endpoint")
		}
	})
	t.Run("output with an out-of-range port yields no endpoint", func(t *testing.T) {
		stubSSH(t, "echo \"hostname forge.internal\"\necho \"port 70000\"\n")
		if _, ok := sshEffectiveAddress(ctx, "git@forge584.invalid", ""); ok {
			t.Fatal("a port outside the TCP range is not an effective endpoint")
		}
	})
}

// stubSSHBinary installs an executable stand-in with the given binary name
// (a name with separators creates the directories beneath the returned one)
// that records its arguments and prints a fixed `ssh -G` answer, returning
// the directory holding it and the file the arguments land in. PATH is set
// to the stand-in directory plus the real git binary's: the probe resolves
// core.sshCommand through git itself, so the stubbed environment must leave
// git resolvable exactly as production does.
func stubSSHBinary(t *testing.T, name, output string) (string, string) {
	t.Helper()
	argsFile := filepath.Join(t.TempDir(), "args")
	t.Setenv("COURIER_SSH_STUB_ARGS", argsFile)
	directory := t.TempDir()
	executable := filepath.Join(directory, name)
	if parent := filepath.Dir(executable); parent != directory {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			t.Fatalf("create fake binary directory: %v", err)
		}
	}
	script := fmt.Sprintf("printf '%%s\\n' \"$@\" > \"$COURIER_SSH_STUB_ARGS\"\n%s", output)
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"+script), 0o700); err != nil {
		t.Fatalf("write fake %s: %v", name, err)
	}
	t.Setenv("PATH", withGit(t, directory))
	return directory, argsFile
}

// withGit prepends the directory holding the real git binary to a PATH
// value, so a test that replaces PATH with a stub directory still resolves
// git the way production does.
func withGit(t *testing.T, directory string) string {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		return directory
	}
	return directory + string(os.PathListSeparator) + filepath.Dir(gitPath)
}

// readStubArgs reads the arguments a stubSSHBinary stand-in recorded.
func readStubArgs(t *testing.T, argsFile string) []string {
	t.Helper()
	data, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read ssh stub args: %v", err)
	}
	return strings.Split(strings.TrimRight(string(data), "\n"), "\n")
}

// TestSSHEffectiveAddressResolvesThroughTheSSHCommandGitInvokes pins the
// probe's runtime dependency on git's own ssh selection: the git process
// inherits the environment, so it may invoke a different ssh binary or extra
// options through GIT_SSH_COMMAND or GIT_SSH, and a probe that resolved
// through plain ssh could dial an endpoint that particular git process would
// never dial — the wrong-target class of error #584 removed from the URL,
// arriving here through the environment instead.
func TestSSHEffectiveAddressResolvesThroughTheSSHCommandGitInvokes(t *testing.T) {
	ctx := context.Background()
	const answer = "echo \"hostname forge.internal\"\necho \"port 2222\"\n"
	target := "git@forge584.invalid"

	t.Run("a plain GIT_SSH_COMMAND resolves through its first token", func(t *testing.T) {
		_, argsFile := stubSSHBinary(t, "custom-ssh", answer)
		t.Setenv("GIT_SSH_COMMAND", "custom-ssh")
		endpoint, ok := sshEffectiveAddress(ctx, target, "")
		if !ok {
			t.Fatal("GIT_SSH_COMMAND's binary must resolve the effective endpoint")
		}
		if endpoint != "forge.internal:2222" {
			t.Fatalf("sshEffectiveAddress = %q, want %q", endpoint, "forge.internal:2222")
		}
		if args := readStubArgs(t, argsFile); !slices.Equal(args, []string{"-G", target}) {
			t.Fatalf("the resolved command received %q, want [\"-G\" %q]", args, target)
		}
	})
	t.Run("the command's own options pass through and the url port lands after them", func(t *testing.T) {
		_, argsFile := stubSSHBinary(t, "custom-ssh", answer)
		t.Setenv("GIT_SSH_COMMAND", "custom-ssh -o Port=1111 -F /etc/alt/config")
		if _, ok := sshEffectiveAddress(ctx, target, "2222"); !ok {
			t.Fatal("the command's options must not stop the resolution")
		}
		// Git places its own -p after the GIT_SSH_COMMAND options — the same
		// position in both processes — so ssh's own option precedence
		// resolves any overlap identically for git and the probe.
		want := []string{"-o", "Port=1111", "-F", "/etc/alt/config", "-G", "-p", "2222", target}
		if args := readStubArgs(t, argsFile); !slices.Equal(args, want) {
			t.Fatalf("the resolved command received %q, want %q", args, want)
		}
	})
	t.Run("GIT_SSH is exec'd directly as one path, whitespace and all", func(t *testing.T) {
		directory, argsFile := stubSSHBinary(t, "alt ssh", answer)
		t.Setenv("GIT_SSH", filepath.Join(directory, "alt ssh"))
		endpoint, ok := sshEffectiveAddress(ctx, target, "")
		if !ok {
			t.Fatal("GIT_SSH's path must resolve the effective endpoint")
		}
		if endpoint != "forge.internal:2222" {
			t.Fatalf("sshEffectiveAddress = %q, want %q", endpoint, "forge.internal:2222")
		}
		if args := readStubArgs(t, argsFile); !slices.Equal(args, []string{"-G", target}) {
			t.Fatalf("GIT_SSH was split like a shell command line: %q", args)
		}
	})
	t.Run("GIT_SSH_COMMAND wins over GIT_SSH as git's own precedence does", func(t *testing.T) {
		directory, _ := stubSSHBinary(t, "custom-ssh", answer)
		t.Setenv("GIT_SSH_COMMAND", "custom-ssh")
		t.Setenv("GIT_SSH", filepath.Join(directory, "never-invoked"))
		if _, ok := sshEffectiveAddress(ctx, target, ""); !ok {
			t.Fatal("GIT_SSH_COMMAND must take precedence over GIT_SSH")
		}
	})
	t.Run("quoted segments group their characters into one token", func(t *testing.T) {
		directory, argsFile := stubSSHBinary(t, filepath.Join("my ssh", "ssh"), answer)
		t.Setenv("GIT_SSH_COMMAND", fmt.Sprintf("%q -o %q", filepath.Join(directory, "my ssh", "ssh"), "Port=1111"))
		if _, ok := sshEffectiveAddress(ctx, target, ""); !ok {
			t.Fatal("a quoted binary path must resolve the effective endpoint")
		}
		want := []string{"-o", "Port=1111", "-G", target}
		if args := readStubArgs(t, argsFile); !slices.Equal(args, want) {
			t.Fatalf("the resolved command received %q, want %q", args, want)
		}
	})
}

// TestSSHProbeCommandYieldsNothingItCannotFaithfullyEmulate pins the
// fail-closed half of the environment selection: git runs GIT_SSH_COMMAND
// through the shell, and a line whose meaning depends on shell features the
// probe does not execute — substitution, expansion, control operators —
// must resolve no endpoint and run nothing, never an approximation of
// itself.
func TestSSHProbeCommandYieldsNothingItCannotFaithfullyEmulate(t *testing.T) {
	ctx := context.Background()
	target := "git@forge584.invalid"

	t.Run("a command substitution resolves nothing and executes nothing", func(t *testing.T) {
		sentinel := filepath.Join(t.TempDir(), "sentinel")
		_, _ = stubSSHBinary(t, "ssh", "touch \"$COURIER_PROBE_SENTINEL\"\n")
		t.Setenv("COURIER_PROBE_SENTINEL", sentinel)
		t.Setenv("GIT_SSH_COMMAND", "ssh $(touch "+sentinel+")")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("a command line the shell would expand resolves no endpoint")
		}
		if _, err := os.Lstat(sentinel); !os.IsNotExist(err) {
			t.Fatal("an unemulatable GIT_SSH_COMMAND must never execute, not even once")
		}
	})
	t.Run("an unquoted control operator resolves nothing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		t.Setenv("GIT_SSH_COMMAND", "ssh forge584.invalid; touch /tmp/pwned")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("a command line with a control operator resolves no endpoint")
		}
	})
	t.Run("a first token starting with a dash resolves nothing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		t.Setenv("GIT_SSH_COMMAND", "-oProxyCommand=touch-pwned")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("a first token that would reach the shell as an option resolves no endpoint")
		}
	})
	t.Run("a GIT_SSH starting with a dash resolves nothing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		t.Setenv("GIT_SSH", "-oProxyCommand=touch-pwned")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("GIT_SSH is a path, and a path that reaches exec as an option resolves no endpoint")
		}
	})
	t.Run("an unterminated quote resolves nothing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		t.Setenv("GIT_SSH_COMMAND", "\"ssh")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("a command line whose quoting never closes resolves no endpoint")
		}
	})
	t.Run("a set-but-blank GIT_SSH_COMMAND resolves nothing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		t.Setenv("GIT_SSH_COMMAND", "")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("git runs a blank GIT_SSH_COMMAND as the command itself and dials nothing: the probe must yield no evidence")
		}
	})
	t.Run("a set-but-blank GIT_SSH resolves nothing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		t.Setenv("GIT_SSH", "")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("a blank GIT_SSH is not a path git could exec: the probe must yield no evidence")
		}
	})
	t.Run("a command separator resolves nothing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		t.Setenv("GIT_SSH_COMMAND", "ssh\ngit@forge584.invalid")
		if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
			t.Fatal("a newline makes the line two commands, not one; the probe resolves no endpoint for it")
		}
	})
}

// useCoreSSHCommand sets core.sshCommand in the isolated global
// configuration TestMain pins (GIT_CONFIG_GLOBAL) and removes it when the
// test finishes, so a later test never inherits it.
func useCoreSSHCommand(t *testing.T, command string) {
	t.Helper()
	git(t, "", "config", "--global", "core.sshCommand", command)
	t.Cleanup(func() { git(t, "", "config", "--global", "--unset", "core.sshCommand") })
}

// TestSSHProbeCommandResolvesCoreSSHCommand pins the configuration half of
// git's ssh selection: core.sshCommand ranks between GIT_SSH_COMMAND and
// GIT_SSH, and the remote-publication read runs outside any repository, so
// the probe reads it from the same system and global scopes that read
// applies.
func TestSSHProbeCommandResolvesCoreSSHCommand(t *testing.T) {
	ctx := context.Background()
	const answer = "echo \"hostname forge.internal\"\necho \"port 2222\"\n"
	target := "git@forge584.invalid"

	t.Run("core.sshCommand is the resolution when the variables are unset", func(t *testing.T) {
		_, argsFile := stubSSHBinary(t, "configured-ssh", answer)
		useCoreSSHCommand(t, "configured-ssh -o Port=1111")
		if _, ok := sshEffectiveAddress(ctx, target, ""); !ok {
			t.Fatal("core.sshCommand must resolve the effective endpoint when no variable is set")
		}
		want := []string{"-o", "Port=1111", "-G", target}
		if args := readStubArgs(t, argsFile); !slices.Equal(args, want) {
			t.Fatalf("the resolved command received %q, want %q", args, want)
		}
	})
	t.Run("GIT_SSH_COMMAND wins over core.sshCommand as git's precedence does", func(t *testing.T) {
		_, argsFile := stubSSHBinary(t, "custom-ssh", answer)
		useCoreSSHCommand(t, "never-invoked")
		t.Setenv("GIT_SSH_COMMAND", "custom-ssh")
		if _, ok := sshEffectiveAddress(ctx, target, ""); !ok {
			t.Fatal("GIT_SSH_COMMAND must take precedence over core.sshCommand")
		}
		if args := readStubArgs(t, argsFile); !slices.Equal(args, []string{"-G", target}) {
			t.Fatalf("GIT_SSH_COMMAND did not drive the resolution: %q", args)
		}
	})
	t.Run("core.sshCommand wins over GIT_SSH as git's precedence does", func(t *testing.T) {
		directory, _ := stubSSHBinary(t, "configured-ssh", answer)
		useCoreSSHCommand(t, "configured-ssh")
		t.Setenv("GIT_SSH", filepath.Join(directory, "never-invoked"))
		if _, ok := sshEffectiveAddress(ctx, target, ""); !ok {
			t.Fatal("core.sshCommand must take precedence over GIT_SSH")
		}
	})
}

// TestSSHEffectiveEndpointYieldsNoEndpointBehindAProxy pins the fail-closed
// proxy rule: a proxycommand or proxyjump routes ssh's connection through
// another host, so the resolved host and port is not the network path ssh's
// traffic actually traverses, and a direct dial of it is not evidence about
// the remote. `ssh -G` prints those lines only when they are set, so their
// absence never disturbs the plain resolution.
func TestSSHEffectiveEndpointYieldsNoEndpointBehindAProxy(t *testing.T) {
	ctx := context.Background()
	target := "git@forge584.invalid"
	for name, output := range map[string]string{
		"a proxycommand": "echo \"hostname forge.internal\"\necho \"port 2222\"\necho \"proxycommand nc %h %p\"\n",
		"a proxyjump":    "echo \"hostname forge.internal\"\necho \"port 2222\"\necho \"proxyjump jump584.invalid\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			stubSSHBinary(t, "ssh", output)
			if _, ok := sshEffectiveAddress(ctx, target, ""); ok {
				t.Fatal("a proxied connection must resolve no endpoint")
			}
		})
	}
	t.Run("an unset proxy keeps the resolution", func(t *testing.T) {
		stubSSHBinary(t, "ssh", "echo \"hostname forge.internal\"\necho \"port 2222\"\necho \"proxyusecommand no\"\n")
		if _, ok := sshEffectiveAddress(ctx, target, ""); !ok {
			t.Fatal("a dump without a proxy configuration must resolve normally")
		}
	})
}

// TestSplitShellTokens pins the tokenizer the ssh-command selection rests
// on: it must reproduce a shell's word splitting exactly as far as it goes
// and refuse everything beyond it.
func TestSplitShellTokens(t *testing.T) {
	for name, tc := range map[string]struct {
		command string
		tokens  []string
		ok      bool
	}{
		"a plain command":                {command: "ssh", tokens: []string{"ssh"}, ok: true},
		"blank-separated options":        {command: "ssh -o Port=2222", tokens: []string{"ssh", "-o", "Port=2222"}, ok: true},
		"surrounding blanks":             {command: "  ssh\t-x  ", tokens: []string{"ssh", "-x"}, ok: true},
		"quoted whitespace":              {command: `"/usr/bin/my ssh" -o "Port=2222"`, tokens: []string{"/usr/bin/my ssh", "-o", "Port=2222"}, ok: true},
		"single quotes are literal":      {command: `ssh '$HOME'`, tokens: []string{"ssh", "$HOME"}, ok: true},
		"adjacent parts concatenate":     {command: `"/usr/"local ssh`, tokens: []string{"/usr/local", "ssh"}, ok: true},
		"an empty value has no tokens":   {command: "", tokens: nil, ok: true},
		"a blank value has no tokens":    {command: "  \t ", tokens: nil, ok: true},
		"a newline is a separator":       {command: "ssh\nfoo", ok: false},
		"a carriage return is refused":   {command: "ssh\rfoo", ok: false},
		"command substitution":           {command: "ssh $(x)", ok: false},
		"parameter expansion":            {command: "ssh $HOME", ok: false},
		"backtick substitution":          {command: "ssh `x`", ok: false},
		"backslash":                      {command: `ssh \x`, ok: false},
		"control operator":               {command: "ssh a && ssh b", ok: false},
		"semicolon":                      {command: "ssh a;b", ok: false},
		"redirection":                    {command: "ssh a >b", ok: false},
		"pipe":                           {command: "ssh a | b", ok: false},
		"subshell":                       {command: "(ssh)", ok: false},
		"a glob":                         {command: "ssh *", ok: false},
		"a tilde":                        {command: "ssh ~", ok: false},
		"a comment character":            {command: "ssh #x", ok: false},
		"unterminated double quote":      {command: `"ssh`, ok: false},
		"unterminated single quote":      {command: "'ssh", ok: false},
		"expansion inside double quotes": {command: `ssh "$HOME"`, ok: false},
		"backslash inside double quotes": {command: `"a\ b"`, ok: false},
	} {
		t.Run(name, func(t *testing.T) {
			tokens, ok := splitShellTokens(tc.command)
			if ok != tc.ok {
				t.Fatalf("splitShellTokens(%q) ok = %v, want %v", tc.command, ok, tc.ok)
			}
			if !slices.Equal(tokens, tc.tokens) {
				t.Fatalf("splitShellTokens(%q) = %q, want %q", tc.command, tokens, tc.tokens)
			}
		})
	}
}

// TestRemoteEndpointAvailableDialsTheResolvedEndpoint pins the regression
// from issue #584: a remote whose ssh configuration redirects the connection
// must be probed at the configured endpoint. The alias below resolves to
// nothing on its own — a probe of the literal host at port 22 could only
// ever answer "unreachable" — so an available answer proves the probe dialed
// where ssh would, and a refused configured endpoint is evidence about the
// endpoint git would actually reach.
func TestRemoteEndpointAvailableDialsTheResolvedEndpoint(t *testing.T) {
	requireSSH(t)
	ctx := context.Background()
	answering := accepting(t, func(conn net.Conn) {
		_, _ = conn.Write([]byte("SSH-2.0-probe\r\n"))
		_ = conn.Close()
	})
	answerHost, answerPort, err := net.SplitHostPort(answering)
	if err != nil {
		t.Fatalf("split answering address: %v", err)
	}
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	refusedAddress := refused.Addr().String()
	if err := refused.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	refusedHost, refusedPort, err := net.SplitHostPort(refusedAddress)
	if err != nil {
		t.Fatalf("split refused address: %v", err)
	}
	useSSHUserConfig(t, fmt.Sprintf(
		"Host answer584.invalid\n HostName %s\n Port %s\nHost refused584.invalid\n HostName %s\n Port %s\n",
		answerHost, answerPort, refusedHost, refusedPort))

	t.Run("the ssh url form probes the configured endpoint", func(t *testing.T) {
		if !remoteEndpointAvailable(ctx, "ssh://answer584.invalid/owner/repo.git") {
			t.Fatal("an answering configured endpoint must report no evidence of unavailability")
		}
	})
	t.Run("the scp-like form resolves through the configuration too", func(t *testing.T) {
		if !remoteEndpointAvailable(ctx, "git@answer584.invalid:owner/repo.git") {
			t.Fatal("the scp-like destination must be resolved through the ssh configuration like the url form")
		}
	})
	t.Run("an unreachable configured endpoint is evidence", func(t *testing.T) {
		if remoteEndpointAvailable(ctx, "ssh://refused584.invalid/owner/repo.git") {
			t.Fatal("a configured endpoint that refuses the dial is evidence of unavailability")
		}
	})
}

// TestRemoteEndpointAvailableYieldsNoEvidenceWhenEndpointCannotResolve pins
// the consequence of an unresolvable effective endpoint at the probe
// boundary: with no ssh binary there is no resolution, so even a remote
// whose literal host would refuse any dial yields no evidence — git's own
// verdict stands.
func TestRemoteEndpointAvailableYieldsNoEvidenceWhenEndpointCannotResolve(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	ctx := context.Background()
	if !remoteEndpointAvailable(ctx, "ssh://127.0.0.1:1/owner/repo.git") {
		t.Fatal("without an ssh binary the effective endpoint cannot be resolved: the probe must yield no evidence")
	}
	if !remoteEndpointAvailable(ctx, "git@127.0.0.1:owner/repo.git") {
		t.Fatal("the scp-like form must yield no evidence too when the effective endpoint cannot be resolved")
	}
}

// TestRemoteBranchHeadKeepsGitVerdictWhenConfigurationRedirectsTheProbe pins
// the classification-level consequence of issue #584: an ssh remote whose
// configuration redirects it to an endpoint that answered keeps git's own
// completed verdict. The old probe dialed the literal URL host at port 22 —
// a host the ssh configuration says nobody dials — answered "unavailable",
// and flipped a completed git verdict into transient availability for a
// forge that was never shown to be unavailable.
func TestRemoteBranchHeadKeepsGitVerdictWhenConfigurationRedirectsTheProbe(t *testing.T) {
	requireSSH(t)
	ctx := context.Background()
	answering := accepting(t, func(conn net.Conn) {
		_, _ = conn.Write([]byte("SSH-2.0-probe\r\n"))
		_ = conn.Close()
	})
	answerHost, answerPort, err := net.SplitHostPort(answering)
	if err != nil {
		t.Fatalf("split answering address: %v", err)
	}
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	refusedAddress := refused.Addr().String()
	if err := refused.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	refusedHost, refusedPort, err := net.SplitHostPort(refusedAddress)
	if err != nil {
		t.Fatalf("split refused address: %v", err)
	}
	useSSHUserConfig(t, fmt.Sprintf(
		"Host answer584.invalid\n HostName %s\n Port %s\nHost refused584.invalid\n HostName %s\n Port %s\n",
		answerHost, answerPort, refusedHost, refusedPort))

	t.Run("a configured endpoint that answered keeps git's verdict", func(t *testing.T) {
		_, _, err := RemoteBranchHead(ctx, "ssh://answer584.invalid/owner/repo.git", "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if forge.IsTransientAvailability(err) {
			t.Fatalf("the probe resolved the endpoint git itself would dial and it answered, so git's completed verdict must stand: %v", err)
		}
	})
	t.Run("a configured endpoint that is unreachable is evidence", func(t *testing.T) {
		_, _, err := RemoteBranchHead(ctx, "ssh://refused584.invalid/owner/repo.git", "main")
		if err == nil {
			t.Fatal("expected the remote read to fail")
		}
		if !forge.IsTransientAvailability(err) {
			t.Fatalf("a failed read whose configured endpoint could not be reached is transient availability: %v", err)
		}
	})
}
