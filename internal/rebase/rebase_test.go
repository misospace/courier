package rebase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/git"
)

// TestMain isolates git from the developer's global and system config and
// pins the identity the scripted rebase continue commits with, exactly as
// internal/git's suite does.
func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "courier-rebase-test-")
	if err != nil {
		panic(err)
	}
	for key, value := range map[string]string{
		"GIT_CONFIG_GLOBAL":   filepath.Join(home, "gitconfig"),
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME":     "Courier Rebase Test",
		"GIT_AUTHOR_EMAIL":    "courier-rebase-test@example.invalid",
		"GIT_COMMITTER_NAME":  "Courier Rebase Test",
		"GIT_COMMITTER_EMAIL": "courier-rebase-test@example.invalid",
	} {
		if err := os.Setenv(key, value); err != nil {
			panic(err)
		}
	}
	code := m.Run()
	_ = os.RemoveAll(home)
	os.Exit(code)
}

func runGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = directory
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out))
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fixture builds a base repository with a moved main and a PR branch off the
// original main. mainEdits and prEdits apply after the shared first commit,
// so the PR branch is one base move behind and the edits decide whether the
// replay conflicts.
type fixture struct {
	origin    string // the remote repository (also the base repo)
	workspace string // the prepared clone
	base      string // the base branch tip's ref name ("main")
	branch    string // the PR head branch
	head      string // the remote head OID observed at admission
}

func newFixture(t *testing.T, mainEdits, prEdits map[string]string) fixture {
	t.Helper()
	origin := t.TempDir()
	runGit(t, origin, "init", "--initial-branch=main")
	runGit(t, origin, "config", "user.name", "Courier Rebase Test")
	runGit(t, origin, "config", "user.email", "courier-rebase-test@example.invalid")
	writeFile(t, filepath.Join(origin, "shared.txt"), "one\ntwo\nthree\n")
	writeFile(t, filepath.Join(origin, "code.go"), "package main\n\nfunc main() {\n\tx := 1\n\t_ = x\n}\n")
	// notes.md exists in the shared base so both-side edits of it are content
	// conflicts, not add/add conflicts: the mechanical rules classify the
	// former, and an add/add carries no base content to prove anything with.
	writeFile(t, filepath.Join(origin, "notes.md"), "intro\n# header\nbody\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "base")
	runGit(t, origin, "checkout", "-b", "pr-branch")
	for path, content := range prEdits {
		writeFile(t, filepath.Join(origin, path), content)
	}
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "pr change")
	runGit(t, origin, "checkout", "main")
	for path, content := range mainEdits {
		writeFile(t, filepath.Join(origin, path), content)
	}
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "main move")
	// writer-branch gives the contention test a branch to move the PR ref to.
	runGit(t, origin, "branch", "writer-branch")

	head := runGit(t, origin, "rev-parse", "pr-branch")
	workspace := filepath.Join(t.TempDir(), "ws")
	if _, err := git.Prepare(context.Background(), git.PrepareOptions{
		RemoteURL: origin,
		Directory: workspace,
		Base:      "main",
		Branch:    "pr-branch",
		// The rebase execution path adopts the branch verbatim: preparation
		// must never start the merge fix-pr would (#715).
		SkipBaseSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	return fixture{origin: origin, workspace: workspace, base: "main", branch: "pr-branch", head: head}
}

func execute(t *testing.T, f fixture, expectedHead string) Result {
	t.Helper()
	result, err := Execute(context.Background(), Options{
		Repo:         "acme/widgets",
		Base:         f.base,
		Branch:       f.branch,
		ExpectedHead: expectedHead,
		Workspace:    f.workspace,
		RemoteURL:    f.origin,
		CheckTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return result
}

func remoteHead(t *testing.T, f fixture) string {
	t.Helper()
	head, exists, err := git.RemoteBranchHead(context.Background(), f.origin, f.branch)
	if err != nil || !exists {
		t.Fatalf("remote head: %v (exists=%v)", err, exists)
	}
	return head
}

// reclone prepares a fresh workspace from the fixture's origin, exactly as a
// fresh pod's preparation would.
func reclone(t *testing.T, f fixture) fixture {
	t.Helper()
	workspace := filepath.Join(t.TempDir(), "ws")
	if _, err := git.Prepare(context.Background(), git.PrepareOptions{
		RemoteURL: f.origin, Directory: workspace, Base: f.base, Branch: f.branch, SkipBaseSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	f.workspace = workspace
	return f
}

// A clean replay publishes the rebased head to the existing PR branch, and
// the workspace preparation that preceded it started no merge.
func TestMechanicalRebasePublishesThroughLease(t *testing.T) {
	f := newFixture(t,
		map[string]string{"other.txt": "main content\n"},
		map[string]string{"pr.txt": "pr content\n"},
	)
	ws := &git.Workspace{Directory: f.workspace, Base: f.base}
	if merged, err := ws.MergeInProgress(context.Background()); err != nil || merged {
		t.Fatalf("merge in progress = %v (%v); preparation must never merge for rebase", merged, err)
	}
	if contained, err := ws.ContainsBase(context.Background()); err != nil || contained {
		t.Fatalf("contains base = %v (%v); the adopted branch must stay unsynced", contained, err)
	}
	result := execute(t, f, f.head)
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if result.Head == "" || result.Head == f.head {
		t.Fatalf("head = %q, want a new rebased head", result.Head)
	}
	if result.LeaseHead != f.head {
		t.Fatalf("lease head = %q, want the admission head %q", result.LeaseHead, f.head)
	}
	if got := remoteHead(t, f); got != result.Head {
		t.Fatalf("remote head = %q, want the published head", got)
	}
	ws = &git.Workspace{Directory: f.workspace, Base: f.base}
	contained, err := ws.ContainsBase(context.Background())
	if err != nil || !contained {
		t.Fatalf("rebased branch contains base = %v (%v)", contained, err)
	}
	// The remote never gained a ref: publication went to the one existing branch.
	exists, err := git.RemoteBranchExists(context.Background(), f.origin, "never-created")
	if err != nil || exists {
		t.Fatalf("unexpected branch creation check result %v (%v)", exists, err)
	}
}

// A comment-only overlap where the PR side extends the main side is
// mechanical: the superset keeps both edits and the replay publishes.
func TestMechanicalCommentSubsetConflictResolves(t *testing.T) {
	f := newFixture(t,
		map[string]string{"notes.md": "intro\n# header (main)\nbody\n"},
		map[string]string{"notes.md": "intro\n# header (main)\n# pr addition\nbody\n"},
	)
	result := execute(t, f, f.head)
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	data, err := os.ReadFile(filepath.Join(f.workspace, "notes.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "# header (main)") || !strings.Contains(string(data), "# pr addition") {
		t.Fatalf("resolved file lost an edit: %s", data)
	}
	if strings.Contains(string(data), "<<<<<<<") {
		t.Fatalf("resolved file still carries conflict markers: %s", data)
	}
}

// A two-sided code edit is never resolved: the attempt stops for a human
// with the conflict summary, publishes nothing, and leaves the remote
// branch exactly where it was.
func TestSemanticConflictStopsForHuman(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	result := execute(t, f, f.head)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if result.Conflict == nil || len(result.Conflict.Files) != 1 || result.Conflict.Files[0] != "code.go" {
		t.Fatalf("conflict = %#v, want code.go named for the human", result.Conflict)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched admission head", got)
	}
	// The workspace was restored: a relaunch starts from the admission head.
	ws := &git.Workspace{Directory: f.workspace}
	if head, err := ws.Head(context.Background()); err != nil || head != f.head {
		t.Fatalf("workspace head = %q (%v), want the restored branch", head, err)
	}
}

// Mixed conflicts are all-or-nothing: one non-mechanical hunk declines the
// whole replay even when another file resolved mechanically.
func TestSemanticConflictIsAllOrNothing(t *testing.T) {
	f := newFixture(t,
		map[string]string{
			"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n",
			"docs.md": "a\n# main note\nb\n",
		},
		map[string]string{
			"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n",
			"docs.md": "a\n# main note\n# pr note\nb\n",
		},
	)
	result := execute(t, f, f.head)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched admission head", got)
	}
}

// A PR branch that moved after admission is declined before anything runs:
// one attempt per episode, no publication, no mutation.
func TestMovedRemoteHeadDeclines(t *testing.T) {
	f := newFixture(t,
		map[string]string{"other.txt": "main content\n"},
		map[string]string{"pr.txt": "pr content\n"},
	)
	runGit(t, f.origin, "update-ref", "refs/heads/pr-branch", runGit(t, f.origin, "rev-parse", "main"))
	result := execute(t, f, f.head)
	if result.Outcome != OutcomeRemoteHeadMoved {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != runGit(t, f.origin, "rev-parse", "main") {
		t.Fatalf("remote head moved by the engine: %q", got)
	}
}

// A writer that moves the PR branch during the attempt meets the leased
// push: git refuses, nothing is published, and the ending names the writer.
func TestConflictingActiveWriterIsRefusedByLease(t *testing.T) {
	f := newFixture(t,
		map[string]string{"other.txt": "main content\n"},
		map[string]string{"pr.txt": "pr content\n"},
	)
	hooks := filepath.Join(f.workspace, ".git", "hooks")
	if err := os.MkdirAll(hooks, 0o755); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" + fmt.Sprintf("git -C %q update-ref refs/heads/pr-branch %q\n", f.origin, "refs/heads/writer-branch") + "exit 0\n"
	if err := os.WriteFile(filepath.Join(hooks, "pre-push"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	result := execute(t, f, f.head)
	if result.Outcome != OutcomeRemoteHeadMoved {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != runGit(t, f.origin, "rev-parse", "writer-branch") {
		t.Fatalf("remote head = %q, want the writer's own push to stand", got)
	}
}

// A restart after a published attempt sees the moved head and declines
// against the stale admission lease; against the live head the same branch
// is already current, so the attempt claims nothing and pushes nothing.
func TestRestartSeesWorldNotMemory(t *testing.T) {
	f := newFixture(t,
		map[string]string{"other.txt": "main content\n"},
		map[string]string{"pr.txt": "pr content\n"},
	)
	first := execute(t, f, f.head)
	if first.Outcome != OutcomePublished {
		t.Fatalf("first outcome = %q (%s)", first.Outcome, first.Reason)
	}
	published := remoteHead(t, f)
	// Relaunch against the stale admission head: declined, remote untouched.
	stale := execute(t, f, f.head)
	if stale.Outcome != OutcomeRemoteHeadMoved {
		t.Fatalf("stale restart outcome = %q (%s)", stale.Outcome, stale.Reason)
	}
	if got := remoteHead(t, f); got != published {
		t.Fatalf("restart mutated the remote: %q", got)
	}
	// A fresh pod re-prepares the workspace and re-reads the live head: the
	// published branch already contains the base, so the attempt claims
	// nothing and pushes nothing.
	second := execute(t, reclone(t, f), published)
	if second.Outcome != OutcomeUpToDate {
		t.Fatalf("up-to-date restart outcome = %q (%s)", second.Outcome, second.Reason)
	}
	if got := remoteHead(t, f); got != published {
		t.Fatalf("up-to-date attempt pushed: %q", got)
	}
}

// A branch that already contains the base tip is up to date: nothing is
// pushed and the ending says so.
func TestUpToDateBranchPushesNothing(t *testing.T) {
	f := newFixture(t,
		map[string]string{"other.txt": "main content\n"},
		map[string]string{"pr.txt": "pr content\n"},
	)
	// Someone resolves the conflict by merging on the PR branch before the
	// rebase attempt starts.
	runGit(t, f.origin, "checkout", "pr-branch")
	runGit(t, f.origin, "merge", "--no-edit", "main")
	head := runGit(t, f.origin, "rev-parse", "pr-branch")
	result := execute(t, reclone(t, f), head)
	if result.Outcome != OutcomeUpToDate {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != head {
		t.Fatalf("remote head = %q, want the untouched merge head", got)
	}
}

// A bounded Go build broken in a PR-touched path is a semantic verdict on
// the replay: stop for a human, publish nothing.
func TestBoundedCheckFailureInPRTouchedPathStops(t *testing.T) {
	f := newFixture(t,
		map[string]string{"other.txt": "main content\n"},
		map[string]string{
			"go.mod":    "module example.com/prbreak\n\ngo 1.21\n",
			"broken.go": "package main\n\nfunc broken() {\n",
		},
	)
	result := execute(t, f, f.head)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched admission head", got)
	}
}

// A bounded Go build failure on paths the PR never touched predates the
// rebase and does not block the mechanical publication.
func TestBoundedCheckFailureOutsidePRPathsPublishes(t *testing.T) {
	// The base module itself is broken; the PR touches only a text file.
	origin := t.TempDir()
	runGit(t, origin, "init", "--initial-branch=main")
	runGit(t, origin, "config", "user.name", "Courier Rebase Test")
	runGit(t, origin, "config", "user.email", "courier-rebase-test@example.invalid")
	writeFile(t, filepath.Join(origin, "go.mod"), "module example.com/broken\n\ngo 1.21\n")
	writeFile(t, filepath.Join(origin, "broken.go"), "package main\n\nfunc broken() {\n")
	writeFile(t, filepath.Join(origin, "notes.md"), "intro\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "broken base")
	runGit(t, origin, "checkout", "-b", "pr-branch")
	writeFile(t, filepath.Join(origin, "notes.md"), "intro\npr\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "pr note")
	runGit(t, origin, "checkout", "main")
	writeFile(t, filepath.Join(origin, "main.txt"), "main\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "main move")
	head := runGit(t, origin, "rev-parse", "pr-branch")
	workspace := filepath.Join(t.TempDir(), "ws")
	if _, err := git.Prepare(context.Background(), git.PrepareOptions{
		RemoteURL: origin, Directory: workspace, Base: "main", Branch: "pr-branch", SkipBaseSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	f := fixture{origin: origin, workspace: workspace, base: "main", branch: "pr-branch", head: head}
	result := execute(t, f, head)
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
}

// The leased push refuses an empty lease outright: an unconditional force
// push is never an accident away.
func TestPushForceWithLeaseRequiresLease(t *testing.T) {
	w := &git.Workspace{Directory: t.TempDir(), Branch: "pr-branch"}
	if err := w.PushForceWithLease(context.Background(), ""); err == nil {
		t.Fatal("empty lease was accepted")
	} else if !strings.Contains(err.Error(), "expected remote head") {
		t.Fatalf("err = %v", err)
	}
}

// A missing remote branch is never recreated: the publication only ever
// targets the existing PR branch.
func TestVanishedBranchDeclines(t *testing.T) {
	f := newFixture(t,
		map[string]string{"other.txt": "main content\n"},
		map[string]string{"pr.txt": "pr content\n"},
	)
	// Delete the branch in the origin, then restore the writer-branch under
	// the PR name so the admission head no longer matches anything.
	runGit(t, f.origin, "update-ref", "-d", "refs/heads/pr-branch")
	result := execute(t, f, f.head)
	if result.Outcome != OutcomeRemoteHeadMoved {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if _, exists, _ := git.RemoteBranchHead(context.Background(), f.origin, f.branch); exists {
		t.Fatal("vanished branch was recreated")
	}
}

// validateOptions fails closed on a call that cannot lease its publication.
func TestValidateOptionsRequiresLeaseInputs(t *testing.T) {
	_, err := Execute(context.Background(), Options{Workspace: t.TempDir(), RemoteURL: "origin"})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("err = %v, want missing-input failure", err)
	}
	if !errors.Is(err, err) {
		t.Fatal("unreachable")
	}
}
