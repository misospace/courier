package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/forge"
)

const rebasePreflightBranch = "courier/resolve-issue/acme-widgets/7"

// fixPRMergeStateFixtureJSON is the single-pull-request fixture the #887
// pre-flight reads: the pinned open request on the run branch whose live head
// SHA is the remote head the replay leases to, with the merge answer the test
// is scripting.
func fixPRMergeStateFixtureJSON(headSHA string, mergeable bool, mergeableState string) string {
	return fmt.Sprintf(`[{"number":7,"state":"open","mergeable":%t,"mergeable_state":%q,"title":"Work","base":{"ref":"main","repo":{"full_name":"acme/widgets"}},"head":{"ref":%q,"sha":%q,"repo":{"full_name":"acme/widgets"}}}]`,
		mergeable, mergeableState, rebasePreflightBranch, headSHA)
}

// writeFixPRNoChangeCoordinator writes the fake coordinator that records its
// own run and declares no_change_needed. When prState names the fake forge
// state file it first lets the forge recompute the pull request clean, so the
// final no-change verification passes. It returns the fake binary path.
func writeFixPRNoChangeCoordinator(t *testing.T, root, prState, cleanFixture string) string {
	t.Helper()
	binary := filepath.Join(root, "opencode")
	script := "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf 'coordinator ran\\n' > \"$COURIER_SCRATCH_DIR/coordinator-ran\"\n"
	if prState != "" {
		script += writePullRequestScript(prState, cleanFixture)
	}
	script += "printf '{\"outcome\":\"no_change_needed\",\"evidence\":\"the branch already carries the review work\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n"
	writeExecutable(t, binary, script)
	return binary
}

// setFixPREnv points a fix-pr run at remote for pull request #7 of
// acme/widgets and returns the workspace directory the run will use.
func setFixPREnv(t *testing.T, root, remote, githubBase, openCodeBinary string) string {
	t.Helper()
	workspace := setResolveIssueEnv(t, root, remote, githubBase, openCodeBinary)
	t.Setenv("COURIER_MODE", "fix-pr")
	t.Setenv("COURIER_HEAD_REPO", "acme/widgets")
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "test-token")
	return workspace
}

// remoteWithMechanicallyConflictingBranch is a bare remote whose PR branch and
// a later main commit edit notes.md in the comment-subset shape the rebase
// engine's mechanical rules resolve.
func remoteWithMechanicallyConflictingBranch(t *testing.T, root string) string {
	t.Helper()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "notes.md"), "intro\n# header\nbody\n")
	commit(t, source, "base: shared notes")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")
	runGit(t, source, "checkout", "-b", rebasePreflightBranch)
	write(t, filepath.Join(source, "notes.md"), "intro\n# header (main)\n# pr addition\nbody\n")
	commit(t, source, "work: pr notes")
	runGit(t, source, "push", "origin", rebasePreflightBranch)
	runGit(t, source, "checkout", "main")
	write(t, filepath.Join(source, "notes.md"), "intro\n# header (main)\nbody\n")
	commit(t, source, "base: main notes")
	runGit(t, source, "push", "origin", "main")
	return remote
}

// TestRunFixPRConflictingBranchRebasesMechanicallyBeforeCoordinator pins the
// #887 pre-flight: a fix-pr run whose pull request the forge reports dirty
// replays the branch onto the live base with the mechanical engine and
// publishes it under the lease before the coordinator starts, keeping both
// comment edits and never producing a merge commit.
func TestRunFixPRConflictingBranchRebasesMechanicallyBeforeCoordinator(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithMechanicallyConflictingBranch(t, root)
	preRunHead := remoteHeadSHA(t, remote, rebasePreflightBranch)
	mainHead := remoteHeadSHA(t, remote, "main")

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fixPRMergeStateFixtureJSON(preRunHead, false, "dirty"))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, prState,
		fixPRMergeStateFixtureJSON(preRunHead, true, "clean"))
	setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNoChangeNeeded {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNoChangeNeeded, output.String(), errorsOut.String())
	}
	stdout := output.String()
	if !strings.Contains(stdout, `"event":"rebase.preflight"`) {
		t.Fatalf("pre-flight event missing: %q", stdout)
	}
	if !strings.Contains(stdout, `"status":"published"`) {
		t.Fatalf("pre-flight event must report a published replay: %q", stdout)
	}

	newHead := remoteHeadSHA(t, remote, rebasePreflightBranch)
	if newHead == preRunHead {
		t.Fatalf("remote branch head = %q, want the replayed head: the conflicting branch was never published", newHead)
	}
	if _, code := gitResult(t, remote, "merge-base", "--is-ancestor", mainHead, newHead); code != 0 {
		t.Fatalf("main tip %q is not an ancestor of the rebased head %q: the replay did not land on the live base", mainHead, newHead)
	}
	merges, code := gitResult(t, remote, "log", "--merges", "--oneline", newHead)
	if code != 0 {
		t.Fatalf("git log --merges on %q exited %d, want 0: %q", newHead, code, merges)
	}
	if merges != "" {
		t.Fatalf("rebased branch history holds merge commits %q, want none: the pre-flight replays and never merges", merges)
	}
	notes, _ := gitResult(t, remote, "show", newHead+":notes.md")
	if !strings.Contains(notes, "# header (main)") || !strings.Contains(notes, "# pr addition") {
		t.Fatalf("rebased notes.md lost an edit: %q", notes)
	}
	if strings.Contains(notes, "<<<<<<<") {
		t.Fatalf("rebased notes.md still carries conflict markers: %q", notes)
	}
	if _, err := os.Stat(filepath.Join(root, "scratch", "coordinator-ran")); err != nil {
		t.Fatalf("coordinator did not run after the replay: marker missing (stat error %v)", err)
	}
}

// TestRunFixPRConflictingBranchSemanticConflictStopsForHuman pins the #887
// decline: a conflict the mechanical rules reject ends the run for a human
// with the file named, publishes nothing, and never starts the coordinator.
func TestRunFixPRConflictingBranchSemanticConflictStopsForHuman(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithConflictingBranch(t, root)
	preRunHead := remoteHeadSHA(t, remote, rebasePreflightBranch)

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fixPRMergeStateFixtureJSON(preRunHead, false, "dirty"))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, "", "")
	setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNeedsHuman, output.String(), errorsOut.String())
	}
	stdout := output.String()
	for _, fragment := range []string{`"phase":"NeedsHuman"`, `"outcome":"needs_decision"`, `"category":"needs-decision"`, "README.md"} {
		if !strings.Contains(stdout, fragment) {
			t.Fatalf("output missing %q: %q", fragment, stdout)
		}
	}
	if head := remoteHeadSHA(t, remote, rebasePreflightBranch); head != preRunHead {
		t.Fatalf("remote branch head = %q, want the untouched %q: a declined replay must leave no half-rebased branch", head, preRunHead)
	}
	if _, err := os.Stat(filepath.Join(root, "scratch", "coordinator-ran")); !os.IsNotExist(err) {
		t.Fatalf("coordinator ran after a semantic conflict: marker exists (stat error %v)", err)
	}
}

// TestRunFixPRConflictingBranchHeadMismatchRefusesReplay pins the #887 head
// guard on the run's own forge read: a dirty answer whose head ref is not the
// run's branch ends the run for a human before any replay, push, or
// coordinator start, because a force-with-lease may rewrite only the head the
// request itself names.
func TestRunFixPRConflictingBranchHeadMismatchRefusesReplay(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithConflictingBranch(t, root)
	preRunHead := remoteHeadSHA(t, remote, rebasePreflightBranch)

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fmt.Sprintf(`[{"number":7,"state":"open","mergeable":false,"mergeable_state":"dirty","title":"Work","base":{"ref":"main","repo":{"full_name":"acme/widgets"}},"head":{"ref":"feature","sha":%q,"repo":{"full_name":"acme/widgets"}}}]`, preRunHead))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, "", "")
	setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNeedsHuman, output.String(), errorsOut.String())
	}
	stdout := output.String()
	for _, fragment := range []string{`"phase":"NeedsHuman"`, "refuses to replay or publish another head", rebasePreflightBranch} {
		if !strings.Contains(stdout, fragment) {
			t.Fatalf("output missing %q: %q", fragment, stdout)
		}
	}
	if head := remoteHeadSHA(t, remote, rebasePreflightBranch); head != preRunHead {
		t.Fatalf("remote branch head = %q, want the untouched %q: a refused replay must publish nothing", head, preRunHead)
	}
	if _, err := os.Stat(filepath.Join(root, "scratch", "coordinator-ran")); !os.IsNotExist(err) {
		t.Fatalf("coordinator ran after a head-identity refusal: marker exists (stat error %v)", err)
	}
}

// TestRunFixPRConflictingBranchWriterMovedHeadDeclinesReplayToMidMerge pins
// the #887 declined-replay ending: when the branch no longer sits at the head
// the lease names, the replay declines before touching anything and publishes
// nothing, and the run continues the ordinary lifecycle — which hands the
// still-conflicting branch to the coordinator mid-merge rather than letting
// the declined replay decide the run's ending.
func TestRunFixPRConflictingBranchWriterMovedHeadDeclinesReplayToMidMerge(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithConflictingBranch(t, root)
	prHead := remoteHeadSHA(t, remote, rebasePreflightBranch)
	staleHead := remoteHeadSHA(t, remote, "main")

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fixPRMergeStateFixtureJSON(staleHead, false, "dirty"))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, "", "")
	setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNeedsHuman, output.String(), errorsOut.String())
	}
	stdout := output.String()
	if !strings.Contains(stdout, `"event":"rebase.preflight"`) || !strings.Contains(stdout, `"status":"remote-head-moved"`) {
		t.Fatalf("output missing the declined-replay pre-flight event: %q", stdout)
	}
	if !strings.Contains(stdout, "still unresolved; conflicts remain in README.md") {
		t.Fatalf("the declined replay must leave the run to the ordinary mid-merge handoff: %q", stdout)
	}
	if head := remoteHeadSHA(t, remote, rebasePreflightBranch); head != prHead {
		t.Fatalf("remote branch head = %q, want the untouched %q: a declined replay publishes nothing", head, prHead)
	}
	if _, err := os.Stat(filepath.Join(root, "scratch", "coordinator-ran")); err != nil {
		t.Fatalf("coordinator did not run after the declined replay: marker missing (stat error %v)", err)
	}
}

// TestRunFixPRCleanBranchProducesNoRebaseAndNoPush pins the #887 no-op: a
// forge answer that is not a conflict leaves the branch alone — no pre-flight
// event, no extra push, and the run branch commits unrewritten in the
// workspace.
func TestRunFixPRCleanBranchProducesNoRebaseAndNoPush(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	preRunHead := remoteHeadSHA(t, remote, rebasePreflightBranch)

	source := filepath.Join(root, "source")
	write(t, filepath.Join(source, "docs.txt"), "main moved on\n")
	commit(t, source, "base: unrelated docs")
	runGit(t, source, "push", "origin", "main")

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fixPRMergeStateFixtureJSON(preRunHead, true, "clean"))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, "", "")
	workspace := setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNoChangeNeeded {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNoChangeNeeded, output.String(), errorsOut.String())
	}
	if strings.Contains(output.String(), `"event":"rebase.preflight"`) {
		t.Fatalf("clean pull request triggered a replay: %q", output.String())
	}
	if head := remoteHeadSHA(t, remote, rebasePreflightBranch); head != preRunHead {
		t.Fatalf("remote branch head = %q, want the untouched %q: a clean branch must not be pushed", head, preRunHead)
	}
	if _, code := gitResult(t, workspace, "merge-base", "--is-ancestor", preRunHead, "HEAD"); code != 0 {
		t.Fatalf("pre-run head %q is not an ancestor of the workspace HEAD: a clean run rewrote the branch commits", preRunHead)
	}
}

// TestRunFixPRBlockedAnswerProducesNoRebaseAndNoPush pins the #945 review
// fix: a forge mergeable no whose state names a blockage by required checks
// or reviews is not a conflict report. Even though the boolean says the
// branch cannot merge, the pre-flight replays nothing — no event, no push —
// and the run takes the ordinary path.
func TestRunFixPRBlockedAnswerProducesNoRebaseAndNoPush(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	preRunHead := remoteHeadSHA(t, remote, rebasePreflightBranch)

	source := filepath.Join(root, "source")
	write(t, filepath.Join(source, "docs.txt"), "main moved on\n")
	commit(t, source, "base: unrelated docs")
	runGit(t, source, "push", "origin", "main")

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fixPRMergeStateFixtureJSON(preRunHead, false, "blocked"))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, prState,
		fixPRMergeStateFixtureJSON(preRunHead, true, "clean"))
	setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNoChangeNeeded {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNoChangeNeeded, output.String(), errorsOut.String())
	}
	if strings.Contains(output.String(), `"event":"rebase.preflight"`) {
		t.Fatalf("a blocked answer triggered a replay: %q", output.String())
	}
	if head := remoteHeadSHA(t, remote, rebasePreflightBranch); head != preRunHead {
		t.Fatalf("remote branch head = %q, want the untouched %q: a blocked answer must never be force-pushed", head, preRunHead)
	}
	if _, err := os.Stat(filepath.Join(root, "scratch", "coordinator-ran")); err != nil {
		t.Fatalf("coordinator did not run on the ordinary path: marker missing (stat error %v)", err)
	}
}

// TestRunFixPRBlockedAnswerOnConflictingBranchNeverReplays pins the same
// invariant where a mechanical replay would have succeeded: the git history
// conflicts, but the forge answer names blockage rather than conflict, so
// the pre-flight keeps its hands off the branch and the run falls through to
// the ordinary mid-merge handoff with the remote untouched.
func TestRunFixPRBlockedAnswerOnConflictingBranchNeverReplays(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithMechanicallyConflictingBranch(t, root)
	preRunHead := remoteHeadSHA(t, remote, rebasePreflightBranch)

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fixPRMergeStateFixtureJSON(preRunHead, false, "blocked"))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, "", "")
	setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNeedsHuman, output.String(), errorsOut.String())
	}
	stdout := output.String()
	if strings.Contains(stdout, `"event":"rebase.preflight"`) {
		t.Fatalf("a blocked answer triggered a replay even though the branch conflicts: %q", stdout)
	}
	if !strings.Contains(stdout, "still unresolved; conflicts remain in notes.md") {
		t.Fatalf("the run must fall through to the ordinary mid-merge handoff: %q", stdout)
	}
	if head := remoteHeadSHA(t, remote, rebasePreflightBranch); head != preRunHead {
		t.Fatalf("remote branch head = %q, want the untouched %q: a blocked answer must never be force-pushed", head, preRunHead)
	}
	if _, err := os.Stat(filepath.Join(root, "scratch", "coordinator-ran")); err != nil {
		t.Fatalf("coordinator did not run on the ordinary mid-merge path: marker missing (stat error %v)", err)
	}
}

// TestRunFixPRConflictingBranchWithoutLeasableHeadSkipsReplay pins the
// no-lease ending: a dirty answer whose head commit neither the admission
// pin nor the forge answer names cannot carry a force-with-lease, so the
// pre-flight reports the replay skipped with its reason and the run
// continues on the ordinary path without publishing.
func TestRunFixPRConflictingBranchWithoutLeasableHeadSkipsReplay(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithMechanicallyConflictingBranch(t, root)
	preRunHead := remoteHeadSHA(t, remote, rebasePreflightBranch)

	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, fixPRMergeStateFixtureJSON("", false, "dirty"))

	fakeOpenCode := writeFixPRNoChangeCoordinator(t, root, "", "")
	setFixPREnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stdout=%q stderr=%q", code, exitNeedsHuman, output.String(), errorsOut.String())
	}
	stdout := output.String()
	if !strings.Contains(stdout, `"event":"rebase.preflight"`) || !strings.Contains(stdout, `"status":"skipped"`) {
		t.Fatalf("output missing the skipped-replay pre-flight event: %q", stdout)
	}
	if !strings.Contains(stdout, "named a head commit to lease the replay to") {
		t.Fatalf("the skipped event must carry its reason: %q", stdout)
	}
	if head := remoteHeadSHA(t, remote, rebasePreflightBranch); head != preRunHead {
		t.Fatalf("remote branch head = %q, want the untouched %q: an unleased replay must never publish", head, preRunHead)
	}
	if _, err := os.Stat(filepath.Join(root, "scratch", "coordinator-ran")); err != nil {
		t.Fatalf("coordinator did not run on the ordinary path: marker missing (stat error %v)", err)
	}
}

// TestFixPRBranchConflictsWithBase pins which forge answers open the
// force-push pre-flight: only the authoritative mergeable no whose state
// names GitHub's conflict value. A nil or positive answer, and any state
// that names another reason — blockage, drift, a still-computing or absent
// answer — closes it.
func TestFixPRBranchConflictsWithBase(t *testing.T) {
	yes, no := true, false
	for _, test := range []struct {
		name string
		pr   forge.PullRequest
		want bool
	}{
		{"dirty false conflicts", forge.PullRequest{Mergeable: &no, MergeableState: "dirty"}, true},
		{"blocked false is not a conflict", forge.PullRequest{Mergeable: &no, MergeableState: "blocked"}, false},
		{"behind false is not a conflict", forge.PullRequest{Mergeable: &no, MergeableState: "behind"}, false},
		{"contradictory clean false is not a conflict", forge.PullRequest{Mergeable: &no, MergeableState: "clean"}, false},
		{"false without a state is not a conflict", forge.PullRequest{Mergeable: &no}, false},
		{"pending answer is not a conflict", forge.PullRequest{MergeableState: "dirty"}, false},
		{"mergeable true is not a conflict", forge.PullRequest{Mergeable: &yes, MergeableState: "clean"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := fixPRBranchConflictsWithBase(test.pr); got != test.want {
				t.Fatalf("fixPRBranchConflictsWithBase() = %v, want %v", got, test.want)
			}
		})
	}
}

// TestConflictingFixPRHeadReason pins which forge head identity a leased
// force-with-lease replay may target: only the exact head ref and repository
// the run was admitted against.
func TestConflictingFixPRHeadReason(t *testing.T) {
	const branch = "courier/resolve-issue/acme-widgets/7"
	for _, test := range []struct {
		name string
		pr   forge.PullRequest
		cfg  config
		want string
	}{
		{
			name: "matching same-repository head replays",
			pr:   forge.PullRequest{HeadRef: branch, HeadRepo: "acme/widgets"},
			cfg:  config{Repo: "acme/widgets", Branch: branch},
			want: "",
		},
		{
			name: "missing head ref has no lease target",
			pr:   forge.PullRequest{HeadRef: "", HeadRepo: "acme/widgets"},
			cfg:  config{Repo: "acme/widgets", Branch: branch},
			want: "the forge answer carries no head repository or ref",
		},
		{
			name: "missing head repository has no lease target",
			pr:   forge.PullRequest{HeadRef: branch, HeadRepo: ""},
			cfg:  config{Repo: "acme/widgets", Branch: branch},
			want: "the forge answer carries no head repository or ref",
		},
		{
			name: "head ref is not the run branch",
			pr:   forge.PullRequest{HeadRef: "courier/resolve-issue/acme-widgets/8", HeadRepo: "acme/widgets"},
			cfg:  config{Repo: "acme/widgets", Branch: branch},
			want: `its head ref "courier/resolve-issue/acme-widgets/8" is not the run's branch "courier/resolve-issue/acme-widgets/7"`,
		},
		{
			name: "head repository is not the run repository",
			pr:   forge.PullRequest{HeadRef: branch, HeadRepo: "acme/fork"},
			cfg:  config{Repo: "acme/widgets", Branch: branch},
			want: `its head repository "acme/fork" is not the run's "acme/widgets"`,
		},
		{
			name: "fork head repository matches the pinned head repo",
			pr:   forge.PullRequest{HeadRef: branch, HeadRepo: "acme/fork"},
			cfg:  config{Repo: "acme/widgets", HeadRepo: "acme/fork", Branch: branch},
			want: "",
		},
		{
			name: "fork head repository differs from the pinned head repo",
			pr:   forge.PullRequest{HeadRef: branch, HeadRepo: "acme/other"},
			cfg:  config{Repo: "acme/widgets", HeadRepo: "acme/fork", Branch: branch},
			want: `its head repository "acme/other" is not the run's "acme/fork"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := conflictingFixPRHeadReason(test.pr, test.cfg); got != test.want {
				t.Fatalf("conflictingFixPRHeadReason() = %q, want %q", got, test.want)
			}
		})
	}
}
