package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/git"
	courierlog "github.com/misospace/courier/internal/log"
)

func TestMain(m *testing.M) {
	// Tests supply their own run configuration; never inherit a live pod's
	// termination path or other COURIER_* settings.
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "COURIER_") {
			_ = os.Unsetenv(name)
		}
	}
	// Run writes safe.directory into git's global config, so point that at a
	// throwaway file and keep workstation settings such as commit.gpgsign out.
	home, err := os.MkdirTemp("", "courier-executor-test-")
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

func TestRunPreparesOrphanBranchAndInvokesOpenCodeWithExactContext(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	orphan := filepath.Join(root, "orphan")
	runGit(t, root, "clone", remote, orphan)
	configureGit(t, orphan)
	runGit(t, orphan, "checkout", "-b", "courier/resolve-issue/acme-widgets/7", "origin/main")
	write(t, filepath.Join(orphan, "work.txt"), "previous brief\n")
	commit(t, orphan, "work: previous brief")
	runGit(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")

	write(t, filepath.Join(source, "base.txt"), "moved base\n")
	commit(t, source, "base: moved")
	runGit(t, source, "push", "origin", "main")

	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch dir")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nif [ -e \"$COURIER_SCRATCH_DIR/outcome.json\" ]; then exit 9; fi\nprintf 'scratch dir: %s\\n' \"$COURIER_SCRATCH_DIR\"\nprintf 'opencode argv: %s\\n' \"$*\"\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\n")
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(scratchDirectory, "outcome.json"), `{"outcome":"no_change_needed","evidence":"stale"}`)
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_OPENCODE_AGENT", "architect")
	t.Setenv("COURIER_FRAMING", "capacity is elastic; fan out freely")
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != 0 {
		t.Fatalf("run exit code = %d, stderr=%q, stdout=%q", code, errorsOut.String(), output.String())
	}
	if _, err := os.Stat(filepath.Join(workspace, "base.txt")); err != nil {
		t.Fatalf("adopted branch was not synchronized to base: %v", err)
	}
	if !strings.Contains(output.String(), "Open a PR to address issue #7.") || !strings.Contains(output.String(), "any-model/name") || !strings.Contains(output.String(), "capacity is elastic; fan out freely") {
		t.Fatalf("OpenCode did not receive exact goal/model/framing: %q", output.String())
	}
	wantOutcomePath := filepath.Join(scratchDirectory, "outcome.json")
	if !strings.Contains(output.String(), "scratch dir: "+scratchDirectory) || !strings.Contains(output.String(), "Open a PR to address issue #7. Declare the outcome at "+wantOutcomePath) {
		t.Fatalf("child scratch path and actual goal declaration path disagree: %q", output.String())
	}
	if !strings.Contains(output.String(), "--agent architect") {
		t.Fatalf("OpenCode did not receive configured agent: %q", output.String())
	}
	if !strings.Contains(output.String(), `COURIER_TERMINATION {"phase":"Verifying","result":"success","exit_code":0`) {
		t.Fatalf("missing stable success termination: %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"changes"`) {
		t.Fatalf("runtime did not read the child's changes declaration at %q: %q", wantOutcomePath, output.String())
	}
	terminationOutput := string(mustRead(t, termination))
	if !strings.Contains(terminationOutput, `"phase":"Verifying"`) {
		t.Fatalf("termination file = %q", terminationOutput)
	}
}

func TestRunResolveIssueWithExistingOpenPullRequestReportsForReview(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	// A binary that fails loudly if it is ever executed: reaching it would mean
	// the run adopted the branch despite the open PR.
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\nexit 99\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"courier/resolve-issue/acme-widgets/7"}}]`)
	}))
	defer prServer.Close()

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 (report for review); stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}
	if !strings.Contains(output.String(), "PR #12") {
		t.Fatalf("termination should name the PR number: %q", output.String())
	}
	if strings.Contains(output.String(), `"phase":"NeedsHuman"`) || strings.Contains(output.String(), "refusing adoption") {
		t.Fatalf("open PR should be reported for review, not handed to a human: %q", output.String())
	}
	for _, event := range parseEvents(t, &output) {
		if name, _ := event["event"].(string); name == "workspace.ready" || name == "executor.start" {
			t.Fatalf("run reached %s despite an existing PR: %v", name, event)
		}
	}
}

func TestRunResolveIssueRefusesBranchWithClosedPullRequest(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	// A binary that fails loudly if it is ever executed: reaching it would mean
	// the run adopted the branch despite the existing PR.
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\nexit 99\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"number":12,"state":"closed","head":{"ref":"courier/resolve-issue/acme-widgets/7"}}]`)
	}))
	defer prServer.Close()

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("missing NeedsHuman termination: %q", output.String())
	}
	if !strings.Contains(output.String(), "refusing adoption") {
		t.Fatalf("termination should refuse adoption: %q", output.String())
	}
	if !strings.Contains(output.String(), "PR #12") {
		t.Fatalf("termination should name the PR number: %q", output.String())
	}
	for _, event := range parseEvents(t, &output) {
		if name, _ := event["event"].(string); name == "workspace.ready" || name == "executor.start" {
			t.Fatalf("run reached %s despite an existing PR: %v", name, event)
		}
	}
}

func TestRunResolveIssueWithMixedPullRequestsReportsOpenForReview(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	// A binary that fails loudly if it is ever executed: reaching it would mean
	// the run adopted the branch despite the open PR.
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\nexit 99\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"number":11,"state":"closed","head":{"ref":"courier/resolve-issue/acme-widgets/7"}},{"number":12,"state":"open","head":{"ref":"courier/resolve-issue/acme-widgets/7"}}]`)
	}))
	defer prServer.Close()

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 (report for review); stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}
	if !strings.Contains(output.String(), "PR #12") {
		t.Fatalf("termination should name the open PR number: %q", output.String())
	}
	for _, event := range parseEvents(t, &output) {
		if name, _ := event["event"].(string); name == "workspace.ready" || name == "executor.start" {
			t.Fatalf("run reached %s despite an existing PR: %v", name, event)
		}
	}
}

func TestRunResolveIssueAdoptsBranchWithoutPullRequest(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'opencode argv: %s\\n' \"$*\"\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\n")

	const githubToken = "github-api-token"
	const gitToken = "git-token"
	var authorization string
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()

	workspace := setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_GIT_TOKEN", gitToken)
	t.Setenv("GITHUB_TOKEN", githubToken)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("orphan adoption should proceed to success: %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode argv") {
		t.Fatalf("opencode was not invoked after orphan adoption: %q", output.String())
	}
	if _, err := os.Stat(filepath.Join(workspace, "work.txt")); err != nil {
		t.Fatalf("existing branch was not adopted: %v", err)
	}
	if authorization != "Bearer github-api-token" {
		t.Fatalf("GitHub authorization = %q, want narrow GitHub token", authorization)
	}
}

func runConflictedAdoption(t *testing.T, openCodeScript string) (int, string) {
	t.Helper()
	root := t.TempDir()
	remote := remoteWithConflictingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'opencode argv: %s\\n' \"$*\"\n"+openCodeScript)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	return code, output.String() + errorsOut.String()
}

func TestRunConflictedAdoptionReachesCoordinatorAndResolves(t *testing.T) {
	code, output := runConflictedAdoption(t, "printf 'branch edit\\nbase edit\\n' > README.md\ngit add README.md\ngit commit --no-edit >/dev/null\n")
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 after the coordinator commits the merge; output=%q", code, output)
	}
	if !strings.Contains(output, "opencode argv") {
		t.Fatalf("a conflicting branch must reach the coordinator, not fail during setup: %q", output)
	}
	if !strings.Contains(output, "stopped on conflicts in README.md") {
		t.Fatalf("the coordinator goal should name the conflicted path: %q", output)
	}
	if !strings.Contains(output, `"phase":"Verifying"`) {
		t.Fatalf("a resolved and committed merge should verify: %q", output)
	}
}

func TestRunUnresolvedConflictBecomesNeedsHuman(t *testing.T) {
	code, output := runConflictedAdoption(t, "exit 0\n")
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; output=%q", code, exitNeedsHuman, output)
	}
	if !strings.Contains(output, "still unresolved; conflicts remain in README.md") {
		t.Fatalf("unresolved-merge reason = %q", output)
	}
}

func TestRunAbandonedConflictMergeBecomesNeedsHuman(t *testing.T) {
	code, output := runConflictedAdoption(t, "git merge --abort\nprintf 'other\\n' > other.txt\ngit add other.txt\ngit commit -m 'test: unrelated work' >/dev/null\n")
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; output=%q", code, exitNeedsHuman, output)
	}
	if !strings.Contains(output, "does not contain origin/main") {
		t.Fatalf("abandoned-merge reason = %q", output)
	}
}

func TestRunExitZeroWithoutLocalWorkBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("no-op phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "without declaring an outcome and produced no commit or workspace changes") {
		t.Fatalf("no-op reason = %q", output.String())
	}
}

// TestRunTrackedCourierDataSurvivesGitAddAll verifies executor setup and
// cleanup preserve tracked .courier content on an adopted work branch.
func TestRunTrackedCourierDataSurvivesGitAddAll(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	orphan := filepath.Join(root, "orphan")
	if err := os.MkdirAll(filepath.Join(orphan, ".courier"), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(orphan, ".courier", "tracked.txt"), "tracked user data\n")
	commit(t, orphan, "work: stale declaration")
	runGit(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'working tree should preserve tracked data\\n' >> .courier/tracked.txt\ngit add -A\ngit commit -m 'test: preserve courier data' >/dev/null\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	workspace := setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitSuccess, errorsOut.String(), output.String())
	}
	if got := string(mustRead(t, filepath.Join(workspace, ".courier", "tracked.txt"))); got != "tracked user data\nworking tree should preserve tracked data\n" {
		t.Fatalf("tracked courier data = %q, want preserved modifications", got)
	}
}

func TestRunExitZeroWithDirtyWorkBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'partial\\n' > partial.txt\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("dirty-work phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "uncommitted workspace changes") {
		t.Fatalf("dirty-work reason = %q", output.String())
	}
}

func TestRunCommittedWorkOnWrongBranchBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\ngit checkout -b fix/elsewhere\nprintf 'elsewhere\\n' > elsewhere.txt\ngit add --all -- .\ngit commit -m 'test: work on wrong branch' >/dev/null\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("wrong-branch phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "not on the run branch") {
		t.Fatalf("wrong-branch reason = %q", output.String())
	}
	if !strings.Contains(output.String(), "fix/elsewhere") {
		t.Fatalf("wrong-branch reason should name the wrong branch: %q", output.String())
	}
}

func TestRunDetachedHeadCommitBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\ngit checkout --detach\nprintf 'detached\\n' > detached.txt\ngit add --all -- .\ngit commit -m 'test: work on detached head' >/dev/null\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("detached-HEAD phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "detached HEAD") {
		t.Fatalf("detached-HEAD reason = %q", output.String())
	}
	if !strings.Contains(output.String(), "not on the run branch") {
		t.Fatalf("detached-HEAD reason should name the run branch: %q", output.String())
	}
}

func TestRunDeclaredChangesVerifiedReachesVerifying(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) || !strings.Contains(output.String(), `"outcome":"changes"`) {
		t.Fatalf("declared-changes verification = %q", output.String())
	}
	if !strings.Contains(output.String(), `"event":"outcome.declared"`) {
		t.Fatalf("missing outcome.declared event: %q", output.String())
	}
}

// TestRunCommittedDeclarationWithWorkStaysVerifying confirms a declaration in
// per-run scratch does not become workspace work when the coordinator commits
// its real changes.
func TestRunCommittedDeclarationWithWorkStaysVerifying(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("committed declaration must not falsify the work state: %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"changes"`) {
		t.Fatalf("declared-changes verification = %q", output.String())
	}
}

func TestRunDeclaredNoChangeNeededExitsThreeAndComments(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"no_change_needed\",\"evidence\":\"already fixed in v2\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")

	var mu sync.Mutex
	var comments []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/comments") {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			comments = append(comments, string(body))
			mu.Unlock()
			_, _ = w.Write([]byte(`{"id":1}`))
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer api.Close()
	setResolveIssueEnv(t, root, remote, api.URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "test-token")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNoChangeNeeded {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNoChangeNeeded, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NoChangeNeeded"`) {
		t.Fatalf("no-change-needed phase = %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"no_change_needed"`) {
		t.Fatalf("termination missing outcome: %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode declared no change needed: already fixed in v2") {
		t.Fatalf("no-change-needed reason = %q", output.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(comments) != 1 {
		t.Fatalf("comments = %d, want 1", len(comments))
	}
	if !strings.Contains(comments[0], "already addressed") || !strings.Contains(comments[0], "already fixed in v2") {
		t.Fatalf("comment = %q", comments[0])
	}
}

func TestRunDeclaredNeedsDecisionReachesNeedsHumanAndComments(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"needs_decision\",\"question\":\"Which payment provider should the run use?\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")

	var mu sync.Mutex
	var comments []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/comments") {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			comments = append(comments, string(body))
			mu.Unlock()
			_, _ = w.Write([]byte(`{"id":1}`))
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer api.Close()
	setResolveIssueEnv(t, root, remote, api.URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "test-token")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("needs-decision phase = %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"needs_decision"`) {
		t.Fatalf("termination missing outcome: %q", output.String())
	}
	if !strings.Contains(output.String(), "coordinator needs a decision: Which payment provider should the run use?") {
		t.Fatalf("needs-decision reason = %q", output.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(comments) != 1 {
		t.Fatalf("comments = %d, want 1", len(comments))
	}
	if !strings.Contains(comments[0], "needs a decision") || !strings.Contains(comments[0], "Which payment provider") {
		t.Fatalf("comment = %q", comments[0])
	}
}

// TestRunOutcomeCommentIsRedactedBeforeForge registers a fake secret via the
// environment (the name shape is what RegisterEnvironment picks up), has the
// coordinator embed it in a needs_decision question, and proves the comment
// posted to the forge is scrubbed while the run still ends NeedsHuman.
func TestRunOutcomeCommentIsRedactedBeforeForge(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, fmt.Sprintf(`#!/bin/sh
case "$1" in mcp) exit 0;; esac
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"needs_decision","question":"Which payment provider should the run use? The staging key is %s."}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`, testForgeSecret))

	var mu sync.Mutex
	var comments []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/comments") {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			comments = append(comments, string(body))
			mu.Unlock()
			_, _ = w.Write([]byte(`{"id":1}`))
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer api.Close()
	setResolveIssueEnv(t, root, remote, api.URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_FORGE_SECRET", testForgeSecret)
	t.Setenv("GITHUB_TOKEN", "test-token")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("needs-decision phase = %q", output.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(comments) != 1 {
		t.Fatalf("comments = %d, want 1", len(comments))
	}
	if strings.Contains(comments[0], testForgeSecret) {
		t.Fatal("comment contains a fake credential (constant: testForgeSecret)")
	}
	if !strings.Contains(comments[0], courierlog.RedactedPlaceholder) {
		t.Fatalf("comment credential was not replaced by the redaction placeholder: %q", comments[0])
	}
	if !strings.Contains(comments[0], "Which payment provider") {
		t.Fatalf("comment lost the question around the redaction: %q", comments[0])
	}
}

// TestRunMissingRefOutcomeCommentEmitsErrorEvent proves a terminal outcome that
// wants to comment without a positive reference is not skipped silently: an
// outcome.comment error event names the missing reference, and the run's
// ending is unchanged.
func TestRunMissingRefOutcomeCommentEmitsErrorEvent(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"needs_decision\",\"question\":\"Which payment provider should the run use?\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	// No COURIER_REF: the run has no issue/PR to comment on.
	t.Setenv("COURIER_LOG_LEVEL", "debug")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("needs-decision phase = %q", output.String())
	}
	events := parseEvents(t, &output)
	comment := findEvent(t, events, "outcome.comment")
	if comment["status"] != "error" {
		t.Fatalf("outcome.comment status = %v, want error", comment["status"])
	}
	detail, ok := comment["detail"].(map[string]any)
	if !ok {
		t.Fatal("outcome.comment must carry a detail object")
	}
	if reason, _ := detail["reason"].(string); !strings.Contains(reason, "reference") {
		t.Fatalf("outcome.comment reason does not name the missing reference: %q", reason)
	}
}

func TestRunDeclaredBlockedExternalReachesNeedsHumanAndComments(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	missing := "secrets/prod-keys; ask the platform team to provision access before retry with token " + testForgeSecret
	t.Setenv("COURIER_TEST_MISSING", missing)
	t.Setenv("COURIER_FORGE_SECRET", testForgeSecret)
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '{"outcome":"blocked_external","missing":"%s"}' "$COURIER_TEST_MISSING" > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`)
	var comments []string
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/comments") {
			body, _ := io.ReadAll(r.Body)
			comments = append(comments, string(body))
			_, _ = w.Write([]byte(`{"id":1}`))
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("GITHUB_TOKEN", "test-token")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) || !strings.Contains(output.String(), `"outcome":"blocked_external"`) {
		t.Fatalf("blocked-external termination = %q", output.String())
	}
	if !strings.Contains(output.String(), "coordinator is blocked on an external prerequisite: secrets/prod-keys; ask the platform team to provision access before retry with token "+courierlog.RedactedPlaceholder) {
		t.Fatalf("blocked-external reason did not preserve the redacted explanation: %q", output.String())
	}
	if strings.Contains(output.String(), testForgeSecret) {
		t.Fatal("termination output contains a fake credential (constant: testForgeSecret)")
	}
	if len(comments) != 1 || !strings.Contains(comments[0], "secrets/prod-keys; ask the platform team to provision access before retry with token "+courierlog.RedactedPlaceholder) {
		t.Fatalf("blocked-external comment = %#v, want full redacted missing explanation", comments)
	}
	if strings.Contains(comments[0], testForgeSecret) {
		t.Fatal("blocked-external comment contains a fake credential (constant: testForgeSecret)")
	}
}
func TestRunDeclaredChangesButDirtyBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'partial\\n' > partial.txt\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("declared-changes-dirty phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode declared changes but left uncommitted workspace changes") {
		t.Fatalf("declared-changes-dirty reason = %q", output.String())
	}
}

func TestRunDeclaredChangesButNoCommitBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("declared-changes-none phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode declared changes but produced no commit") {
		t.Fatalf("declared-changes-none reason = %q", output.String())
	}
}

func TestRunDeclaredNoChangeNeededButCommittedBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"no_change_needed\",\"evidence\":\"thought it was done\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("no-change-needed-committed phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode declared no change needed but the workspace has new commits or uncommitted changes") {
		t.Fatalf("no-change-needed-committed reason = %q", output.String())
	}
}

func TestRunInvalidOutcomeDeclarationBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf 'not json' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("invalid-declaration phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode wrote an invalid outcome declaration") {
		t.Fatalf("invalid-declaration reason = %q", output.String())
	}
}

// TestRunCommittedWorkStillOnRunBranchStaysVerifying is the case a HEAD-based
// check gets wrong: the commit IS on the run branch, only HEAD moved off it
// afterwards. The run branch ref still contains the work, so the run stays
// Verifying.
func TestRunCommittedWorkStillOnRunBranchStaysVerifying(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\ngit checkout -b review/after\nexit 0\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("work on the run branch must stay Verifying: %q", output.String())
	}
}

func TestRunCommitWithUntrackedScratchReachesVerifying(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\ngit add completed.txt\ngit commit -m 'test: completed work' >/dev/null\nprintf 'scratch\\n' > scratch.txt\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	workspace := setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output, errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}
	if _, err := os.Stat(filepath.Join(workspace, "scratch.txt")); err != nil {
		t.Fatalf("untracked scratch missing: %v", err)
	}
}

func TestTruncateTerminationReasonRespectsByteLimitAndUTF8(t *testing.T) {
	long := strings.Repeat("a", maxTerminationReasonBytes-len(terminationTruncationSuffix)-1) + "é" + strings.Repeat("b", 20)
	got := truncateTerminationReason(long)
	if len(got) > maxTerminationReasonBytes {
		t.Fatalf("truncated reason is %d bytes, want <= %d", len(got), maxTerminationReasonBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncated reason is not valid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, terminationTruncationSuffix) {
		t.Fatalf("truncated reason = %q, want truncation suffix", got)
	}
	short := "short reason"
	if got := truncateTerminationReason(short); got != short {
		t.Fatalf("short reason = %q, want unchanged %q", got, short)
	}
}

func TestRunChildExitTwoBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nexit 2\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d (the child's code must not pass through); stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("child-exit-2 phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode exited with status 2") {
		t.Fatalf("child-exit-2 reason = %q", output.String())
	}
}

// TestRunChildExitThreeBecomesFailed is the symmetric guard for the
// no_change_needed exit: a child that exits 3 without a declaration must fail
// the run with the standard failure code, never end it with the no-change-
// needed termination.
func TestRunChildExitThreeBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nexit 3\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("child-exit-3 phase = %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode exited with status 3") {
		t.Fatalf("child-exit-3 reason = %q", output.String())
	}
	if strings.Contains(output.String(), `"phase":"NoChangeNeeded"`) {
		t.Fatalf("child exit 3 must never end the run with the no_change_needed exit: %q", output.String())
	}
}

func TestReadConfigDefaultsScratchDirectoryOutsideWorkspace(t *testing.T) {
	values := map[string]string{
		"COURIER_REPO_URL":  "https://git.example/acme/widgets.git",
		"COURIER_BRANCH":    "feature/7",
		"COURIER_GOAL":      "write to /var/tmp/courier-scratch/outcome.json",
		"COURIER_MODEL":     "model",
		"COURIER_WORKSPACE": "/workspace",
	}
	cfg, err := readConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("readConfig() error = %v", err)
	}
	if cfg.ScratchDirectory != defaultScratchDirectory {
		t.Fatalf("scratch directory = %q, want %q", cfg.ScratchDirectory, defaultScratchDirectory)
	}
}

// TestRunUncommittedWorkResumesAndSucceeds proves a recoverable dirty exit
// resumes the same session with an uncommitted state message, and a second
// turn that commits the work lands the run in Verifying.
func TestRunUncommittedWorkResumesAndSucceeds(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
SESSION=
for a in "$@"; do
  case "$a" in --session) SESSION=1;; esac
done
if [ -n "$SESSION" ]; then
  git add --all -- .
  git commit -m 'test: committed the partial work' >/dev/null
  exit 0
fi
printf '{"type":"text","sessionID":"ses_resume1","part":{"type":"text","text":"partial work"}}\n'
printf 'partial\n' > partial.txt
exit 0
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 1 {
		t.Fatalf("executor.continuation count = %d, want 1", n)
	}
	detail := eventDetail(t, findEvent(t, events, "executor.continuation"))
	if detail["kind"] != "uncommitted" {
		t.Fatalf("continuation kind = %v, want uncommitted", detail["kind"])
	}
	if detail["continuation"] != float64(1) {
		t.Fatalf("continuation number = %v, want 1", detail["continuation"])
	}

	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	if !strings.Contains(calls[1], "--session ses_resume1") {
		t.Fatalf("resumed call did not carry --session ses_resume1: %q", calls[1])
	}
	if !strings.Contains(calls[1], "uncommitted changes in") || !strings.Contains(calls[1], "partial.txt") {
		t.Fatalf("resumed call lost the uncommitted state message: %q", calls[1])
	}
}

// TestRunNoWorkResumesAndSucceeds proves a recoverable no-work exit resumes
// the same session with a no-work state message, and a second turn that commits
// lands the run in Verifying.
func TestRunNoWorkResumesAndSucceeds(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
SESSION=
for a in "$@"; do
  case "$a" in --session) SESSION=1;; esac
done
if [ -n "$SESSION" ]; then
  printf 'done\n' > done.txt
  git add --all -- .
  git commit -m 'test: produced the work' >/dev/null
  exit 0
fi
printf '{"type":"text","sessionID":"ses_nowork1","part":{"type":"text","text":"nothing yet"}}\n'
exit 0
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 1 {
		t.Fatalf("executor.continuation count = %d, want 1", n)
	}
	detail := eventDetail(t, findEvent(t, events, "executor.continuation"))
	if detail["kind"] != "no-work" {
		t.Fatalf("continuation kind = %v, want no-work", detail["kind"])
	}
	if detail["continuation"] != float64(1) {
		t.Fatalf("continuation number = %v, want 1", detail["continuation"])
	}

	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	if !strings.Contains(calls[1], "--session ses_nowork1") {
		t.Fatalf("resumed call did not carry --session ses_nowork1: %q", calls[1])
	}
	if !strings.Contains(calls[1], "without producing a commit or workspace changes") {
		t.Fatalf("resumed call lost the no-work state message: %q", calls[1])
	}
}

// TestRunOffBranchResumesAndSucceeds proves a recoverable off-branch exit
// resumes the same session with an off-branch state message, and a second turn
// that moves the commit onto the run branch lands the run in Verifying.
func TestRunOffBranchResumesAndSucceeds(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
SESSION=
for a in "$@"; do
  case "$a" in --session) SESSION=1;; esac
done
if [ -n "$SESSION" ]; then
  git branch -f "$COURIER_BRANCH" HEAD
  exit 0
fi
git checkout -b side/branch
printf 'side\n' > side.txt
git add --all -- .
git commit -m 'test: work on a side branch' >/dev/null
printf '{"type":"text","sessionID":"ses_offbranch1","part":{"type":"text","text":"committed off branch"}}\n'
exit 0
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 1 {
		t.Fatalf("executor.continuation count = %d, want 1", n)
	}
	detail := eventDetail(t, findEvent(t, events, "executor.continuation"))
	if detail["kind"] != "off-branch" {
		t.Fatalf("continuation kind = %v, want off-branch", detail["kind"])
	}
	if detail["continuation"] != float64(1) {
		t.Fatalf("continuation number = %v, want 1", detail["continuation"])
	}

	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	if !strings.Contains(calls[1], "--session ses_offbranch1") {
		t.Fatalf("resumed call did not carry --session ses_offbranch1: %q", calls[1])
	}
	if !strings.Contains(calls[1], "not on the run branch") {
		t.Fatalf("resumed call lost the off-branch state message: %q", calls[1])
	}
}

// TestRunLoopGuardStopsOnRepeatedState proves a run that leaves the identical
// dirty state on every turn resumes once and then the loop guard stops it,
// naming the repeated uncommitted state in the reason.
func TestRunLoopGuardStopsOnRepeatedState(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
printf '{"type":"text","sessionID":"ses_loop1","part":{"type":"text","text":"same text"}}\n'
printf 'partial\n' > partial.txt
exit 0
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), "looping:") {
		t.Fatalf("reason = %q, want a looping: reason", output.String())
	}
	if !strings.Contains(output.String(), "uncommitted [") {
		t.Fatalf("reason = %q, want the uncommitted summary in the looping history", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 1 {
		t.Fatalf("executor.continuation count = %d, want 1 (first classification resumes once, second identical loops)", n)
	}
}

// TestRunLoopGuardStopsAtContinuationCap proves a run whose state keeps
// changing (so the no-progress guard never fires) is still stopped once it
// exhausts the continuation budget, reporting the budget and each numbered
// continuation.
func TestRunLoopGuardStopsAtContinuationCap(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
N=$(cat "$COURIER_FAKE_STATE/count" 2>/dev/null || echo 0)
N=$((N+1))
echo "$N" > "$COURIER_FAKE_STATE/count"
printf '{"type":"text","sessionID":"ses_cap1","part":{"type":"text","text":"unique text turn %s"}}\n' "$N"
exit 0
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_CONTINUATIONS", "2")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), "looping after 2 continuations:") {
		t.Fatalf("reason = %q, want the continuation-cap reason", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 2 {
		t.Fatalf("executor.continuation count = %d, want 2", n)
	}
	if got := eventDetail(t, findEvent(t, events, "executor.continuation"))["continuation"]; got != float64(1) {
		t.Fatalf("first continuation number = %v, want 1", got)
	}
	var sawTwo bool
	for _, event := range events {
		if event["event"] != "executor.continuation" {
			continue
		}
		if d, ok := event["detail"].(map[string]any); ok && d["continuation"] == float64(2) {
			sawTwo = true
		}
	}
	if !sawTwo {
		t.Fatalf("no continuation numbered 2 among events: %q", output.String())
	}
}

// TestRunCrashResumesAndSucceeds proves a crashing turn (non-zero, non-2)
// that captured a session id is resumed with a crash state message, and a
// second turn that commits lands the run in Verifying.
func TestRunCrashResumesAndSucceeds(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
SESSION=
for a in "$@"; do
  case "$a" in --session) SESSION=1;; esac
done
if [ -n "$SESSION" ]; then
  printf 'recovered\n' > recovered.txt
  git add --all -- .
  git commit -m 'test: recovered after crash' >/dev/null
  exit 0
fi
printf '{"type":"text","sessionID":"ses_crash1","part":{"type":"text","text":"about to crash"}}\n'
exit 1
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 1 {
		t.Fatalf("executor.continuation count = %d, want 1", n)
	}
	detail := eventDetail(t, findEvent(t, events, "executor.continuation"))
	if detail["kind"] != "crash" {
		t.Fatalf("continuation kind = %v, want crash", detail["kind"])
	}
	if detail["code"] != float64(1) {
		t.Fatalf("continuation code = %v, want 1", detail["code"])
	}
	if detail["continuation"] != float64(1) {
		t.Fatalf("continuation number = %v, want 1", detail["continuation"])
	}

	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	if !strings.Contains(calls[1], "--session ses_crash1") {
		t.Fatalf("resumed call did not carry --session ses_crash1: %q", calls[1])
	}
}

// TestRunCrashFailsAfterBudget proves a run that keeps crashing exhausts the
// continuation budget and then fails with the child's exit code, reporting the
// number of continuations.
func TestRunCrashFailsAfterBudget(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
printf '{"type":"text","sessionID":"ses_crashfail","part":{"type":"text","text":"crashing again"}}\n'
exit 1
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_CONTINUATIONS", "2")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != 1 {
		t.Fatalf("run exit code = %d, want the child's code 1; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("missing Failed termination: %q", output.String())
	}
	if !strings.Contains(output.String(), "opencode exited with status 1 after 2 continuations") {
		t.Fatalf("failure reason = %q, want the budget-exhausted reason", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 2 {
		t.Fatalf("executor.continuation count = %d, want 2", n)
	}
	for _, event := range events {
		if event["event"] != "executor.continuation" {
			continue
		}
		if d := eventDetail(t, event); d["kind"] != "crash" {
			t.Fatalf("continuation kind = %v, want crash", d["kind"])
		}
	}
}

// TestRunNoWorkResumeCarriesCapabilitySuffix proves a resumed no-work state
// message ends with the capability-unavailable suffix when the preflight
// finds a configured capability down.
func TestRunNoWorkResumeCarriesCapabilitySuffix(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) printf 'metrics failed\n    connection refused\n'; exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
SESSION=
for a in "$@"; do
  case "$a" in --session) SESSION=1;; esac
done
if [ -n "$SESSION" ]; then
  printf 'done\n' > done.txt
  git add --all -- .
  git commit -m 'test: produced the work' >/dev/null
  exit 0
fi
printf '{"type":"text","sessionID":"ses_cap1","part":{"type":"text","text":"nothing yet"}}\n'
exit 0
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}

	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	want := "needs_decision or blocked_external if a human is needed.; configured capability unavailable: metrics"
	if !strings.Contains(calls[1], want) {
		t.Fatalf("resumed no-work state message = %q, want it to end with %q", calls[1], want)
	}
}

// TestRunCrashWithoutSessionFailsImmediately proves a child that exits with a
// crash code before printing any sessionID fails with the standard failure
// code and emits no continuation event: there is no session to resume.
func TestRunCrashWithoutSessionFailsImmediately(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nexit 3\n")
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("missing Failed termination: %q", output.String())
	}
	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 0 {
		t.Fatalf("executor.continuation count = %d, want 0 (no session to resume)", n)
	}
}

func TestReadConfigGitHubTokenPrecedenceAndFallback(t *testing.T) {
	base := map[string]string{
		"COURIER_REPO_URL": "https://git.example/acme/widgets.git",
		"COURIER_BRANCH":   "feature/7",
		"COURIER_GOAL":     "goal",
		"COURIER_MODEL":    "model",
	}
	for _, test := range []struct {
		name       string
		github     string
		git        string
		wantGitHub string
	}{
		{name: "GitHub token wins", github: "github-token", git: "git-token", wantGitHub: "github-token"},
		{name: "Git token fallback", git: "git-token", wantGitHub: "git-token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := make(map[string]string, len(base)+2)
			for key, value := range base {
				values[key] = value
			}
			values["GITHUB_TOKEN"] = test.github
			values["COURIER_GIT_TOKEN"] = test.git
			cfg, err := readConfig(func(name string) string { return values[name] })
			if err != nil {
				t.Fatalf("readConfig() error = %v", err)
			}
			if cfg.GitHubToken != test.wantGitHub {
				t.Fatalf("GitHub token = %q, want %q", cfg.GitHubToken, test.wantGitHub)
			}
		})
	}
}

// TestReadConfigContinuationDefaults proves the continuation budget and resume
// backoff parse their env vars and fall back to defaults on bad values.
func TestReadConfigContinuationDefaults(t *testing.T) {
	base := map[string]string{
		"COURIER_REPO_URL": "https://git.example/acme/widgets.git",
		"COURIER_BRANCH":   "feature/7",
		"COURIER_GOAL":     "goal",
		"COURIER_MODEL":    "model",
	}
	for _, test := range []struct {
		name        string
		maxCont     string
		backoff     string
		wantMax     int
		wantBackoff time.Duration
	}{
		{name: "defaults when unset", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "valid max continuations", maxCont: "5", wantMax: 5, wantBackoff: 5 * time.Second},
		{name: "one max continuation", maxCont: "1", wantMax: 1, wantBackoff: 5 * time.Second},
		{name: "unparseable max continuations", maxCont: "abc", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "zero max continuations", maxCont: "0", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "negative max continuations", maxCont: "-1", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "valid resume backoff", backoff: "2.5", wantMax: 3, wantBackoff: 2500 * time.Millisecond},
		{name: "unparseable resume backoff", backoff: "abc", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "negative resume backoff", backoff: "-1", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "non-finite resume backoff", backoff: "Inf", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "NaN resume backoff", backoff: "NaN", wantMax: 3, wantBackoff: 5 * time.Second},
		{name: "zero resume backoff", backoff: "0", wantMax: 3, wantBackoff: 0},
		{name: "oversized resume backoff clamped", backoff: "1e300", wantMax: 3, wantBackoff: 3600 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := make(map[string]string, len(base)+2)
			for key, value := range base {
				values[key] = value
			}
			if test.maxCont != "" {
				values["COURIER_MAX_CONTINUATIONS"] = test.maxCont
			}
			if test.backoff != "" {
				values["COURIER_RESUME_BACKOFF_SECONDS"] = test.backoff
			}
			cfg, err := readConfig(func(name string) string { return values[name] })
			if err != nil {
				t.Fatalf("readConfig() error = %v", err)
			}
			if cfg.MaxContinuations != test.wantMax {
				t.Fatalf("MaxContinuations = %d, want %d", cfg.MaxContinuations, test.wantMax)
			}
			if cfg.ResumeBackoff != test.wantBackoff {
				t.Fatalf("ResumeBackoff = %v, want %v", cfg.ResumeBackoff, test.wantBackoff)
			}
		})
	}
}

func TestContinuationFingerprintIsUnambiguousAndStable(t *testing.T) {
	first, err := continuationFingerprint(git.WorkStateDirty, "branch|one", "head", []string{"a,b"}, "last|text")
	if err != nil {
		t.Fatalf("continuationFingerprint() error = %v", err)
	}
	second, err := continuationFingerprint(git.WorkStateDirty, "branch|one", "head", []string{"a", "b"}, "last|text")
	if err != nil {
		t.Fatalf("continuationFingerprint() error = %v", err)
	}
	if first == second {
		t.Fatal("distinct dirty-path lists produced the same fingerprint")
	}

	third, err := continuationFingerprint(git.WorkStateDirty, "branch", "head|a", []string{"b"}, "last|text")
	if err != nil {
		t.Fatalf("continuationFingerprint() error = %v", err)
	}
	fourth, err := continuationFingerprint(git.WorkStateDirty, "branch|head", "a", []string{"b"}, "last|text")
	if err != nil {
		t.Fatalf("continuationFingerprint() error = %v", err)
	}
	if third == fourth {
		t.Fatal("pipe-bearing branch and head fields produced the same fingerprint")
	}

	fifth, err := continuationFingerprint(git.WorkStateDirty, "branch", "head", []string{"a|b"}, "c")
	if err != nil {
		t.Fatalf("continuationFingerprint() error = %v", err)
	}
	sixth, err := continuationFingerprint(git.WorkStateDirty, "branch", "head", []string{"a"}, "b|c")
	if err != nil {
		t.Fatalf("continuationFingerprint() error = %v", err)
	}
	if fifth == sixth {
		t.Fatal("pipe-bearing dirty path and last-text fields produced the same fingerprint")
	}

	repeated, err := continuationFingerprint(git.WorkStateDirty, "branch|one", "head", []string{"a,b"}, "last|text")
	if err != nil {
		t.Fatalf("continuationFingerprint() error = %v", err)
	}
	if first != repeated {
		t.Fatalf("identical state fingerprint changed: %q != %q", first, repeated)
	}
	if len(first) != 64 {
		t.Fatalf("fingerprint length = %d, want 64 hex characters", len(first))
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Fatalf("fingerprint is not hexadecimal: %v", err)
	}
}

func TestReadConfigReadsHeadIdentity(t *testing.T) {
	values := map[string]string{
		"COURIER_REPO_URL":      "https://git.example/octocat/widgets.git",
		"COURIER_BASE_REPO_URL": "https://git.example/acme/widgets.git",
		"COURIER_BRANCH":        "fix/pr-12",
		"COURIER_GOAL":          "goal",
		"COURIER_MODEL":         "model",
		"COURIER_REPO":          "acme/widgets",
		"COURIER_HEAD_REPO":     "octocat/widgets",
		"COURIER_HEAD_SHA":      "abc123",
	}
	cfg, err := readConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("readConfig() error = %v", err)
	}
	if cfg.HeadRepo != "octocat/widgets" {
		t.Fatalf("HeadRepo = %q, want octocat/widgets", cfg.HeadRepo)
	}
	if cfg.HeadSHA != "abc123" {
		t.Fatalf("HeadSHA = %q, want abc123", cfg.HeadSHA)
	}
	if cfg.Repo != "acme/widgets" {
		t.Fatalf("Repo = %q, want the base repo acme/widgets", cfg.Repo)
	}
	if cfg.BaseRemoteURL != "https://git.example/acme/widgets.git" {
		t.Fatalf("BaseRemoteURL = %q, want the base repo for base sync", cfg.BaseRemoteURL)
	}
}

func TestReadConfigHeadIdentityOptional(t *testing.T) {
	values := map[string]string{
		"COURIER_REPO_URL": "https://git.example/acme/widgets.git",
		"COURIER_BRANCH":   "courier/acme/widgets/issue-7",
		"COURIER_GOAL":     "goal",
		"COURIER_MODEL":    "model",
		"COURIER_REPO":     "acme/widgets",
	}
	cfg, err := readConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("readConfig() error = %v, want head identity optional", err)
	}
	if cfg.HeadRepo != "" || cfg.HeadSHA != "" {
		t.Fatalf("head identity = %q/%q, want empty when unset", cfg.HeadRepo, cfg.HeadSHA)
	}
}

func TestReadConfigFixPRRequiresHeadRepo(t *testing.T) {
	values := map[string]string{
		"COURIER_REPO_URL": "https://git.example/acme/widgets.git",
		"COURIER_BRANCH":   "fix/pr-12",
		"COURIER_GOAL":     "goal",
		"COURIER_MODEL":    "model",
		"COURIER_MODE":     "fix-pr",
		"COURIER_REPO":     "acme/widgets",
		// COURIER_HEAD_REPO is deliberately missing.
	}
	_, err := readConfig(func(name string) string { return values[name] })
	if err == nil || !strings.Contains(err.Error(), "COURIER_HEAD_REPO") {
		t.Fatalf("readConfig() error = %v, want missing COURIER_HEAD_REPO for fix-pr", err)
	}
}

func TestRunResolveIssueFailsWhenGitHubErrors(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\nexit 99\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"message":"internal error"}`)
	}))
	defer prServer.Close()

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != 1 {
		t.Fatalf("run exit code = %d, want 1 (Failed); stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("expected Failed termination: %q", output.String())
	}
	if strings.Contains(output.String(), "opencode argv") || code == 99 {
		t.Fatalf("opencode was invoked despite a GitHub API error: %q", output.String())
	}
}

func TestGuardFixPRHead(t *testing.T) {
	// A bare remote standing in for the fork that owns the PR head: main plus
	// one published work branch. A branch name that was never pushed is the
	// missing-head case.
	remote := gitRemoteWithBranch(t, t.TempDir(), "fix/existing")

	for _, test := range []struct {
		name     string
		mode     string
		repo     string
		headRepo string
		branch   string
		want     int
		wantOut  []string
	}{
		{
			name:     "fork head branch missing is NeedsHuman",
			mode:     "fix-pr",
			repo:     "acme/widgets",
			headRepo: "octocat/widgets",
			branch:   "fix/missing",
			want:     exitNeedsHuman,
			wantOut:  []string{`"phase":"NeedsHuman"`, "octocat/widgets", "fix/missing"},
		},
		{
			name:     "fork head branch present proceeds",
			mode:     "fix-pr",
			repo:     "acme/widgets",
			headRepo: "octocat/widgets",
			branch:   "fix/existing",
			want:     0,
		},
		{
			name:     "same-repo fix-pr is a no-op",
			mode:     "fix-pr",
			repo:     "acme/widgets",
			headRepo: "acme/widgets",
			branch:   "fix/missing",
			want:     0,
		},
		{
			name:     "resolve-issue is unaffected",
			mode:     "resolve-issue",
			repo:     "acme/widgets",
			headRepo: "octocat/widgets",
			branch:   "fix/missing",
			want:     0,
		},
		{
			name:     "head repo differing only by case is a no-op",
			mode:     "fix-pr",
			repo:     "acme/widgets",
			headRepo: "Acme/widgets",
			branch:   "fix/missing",
			want:     0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			var errorsOut bytes.Buffer
			report := newReporter(&output, &errorsOut, config{
				RemoteURL: remote,
				Mode:      test.mode,
				Repo:      test.repo,
				HeadRepo:  test.headRepo,
				Branch:    test.branch,
			})
			if code := report.guardFixPRHead(context.Background()); code != test.want {
				t.Fatalf("guardFixPRHead exit code = %d, want %d; stdout=%q stderr=%q", code, test.want, output.String(), errorsOut.String())
			}
			for _, fragment := range test.wantOut {
				if !strings.Contains(output.String(), fragment) {
					t.Fatalf("output missing %q: %q", fragment, output.String())
				}
			}
		})
	}
}

// gitRemoteWithBranch builds a bare remote with main and the named branch
// published, the state a fork head carries when the PR head is reachable.
func gitRemoteWithBranch(t *testing.T, root, branch string) string {
	t.Helper()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")
	runGit(t, source, "branch", branch)
	runGit(t, source, "push", "origin", branch)
	return remote
}

func TestAskpassNeverLogsOrTakesCredentialsAsArguments(t *testing.T) {
	var output bytes.Buffer
	if code := askpass([]string{"Password for https://git.example/repo.git:"}, "user", "secret-token", &output); code != 0 {
		t.Fatalf("askpass exit code = %d", code)
	}
	if output.String() != "secret-token" {
		t.Fatalf("askpass output = %q", output.String())
	}
	if strings.Contains(strings.Join([]string{"Password for https://git.example/repo.git:"}, " "), "secret-token") {
		t.Fatal("credential appeared in askpass arguments")
	}
}

func TestReadConfigRequiresRemoteAndStatusBranch(t *testing.T) {
	_, err := readConfig(func(name string) string {
		if name == "COURIER_GOAL" || name == "COURIER_MODEL" || name == "COURIER_BRANCH" {
			return "set"
		}
		return ""
	})
	if err == nil || !strings.Contains(err.Error(), "COURIER_REPO_URL") {
		t.Fatalf("readConfig error = %v, want missing remote URL", err)
	}
}

// fake event-test secrets. Assertions never print these values or any output
// line that could contain them; failures name the constant instead.
const (
	testGitToken    = "git-push-token-f4e3d2c1"
	testGitHubTokn  = "gh-api-token-b1b2b3b4"
	testURLToken    = "url-userinfo-token-aa11bb22"
	testForgeSecret = "forge-decision-secret-7c9d2e1f"
)

func TestRunEmitsRunScopedEventsWithoutDetail(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'opencode ran\\n'\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\n")

	// The branch exists on the remote and is orphaned: the adoption guard
	// must see no pull requests and let the run proceed.
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_GIT_TOKEN", testGitToken)
	t.Setenv("GITHUB_TOKEN", testGitHubTokn)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q", code, errorsOut.String())
	}

	events := parseEvents(t, &output)
	wantTypes := map[string]bool{
		"run.start": false, "workspace.ready": false, "executor.start": false, "run.exit": false,
	}
	for _, event := range events {
		eventType, _ := event["event"].(string)
		if _, tracked := wantTypes[eventType]; !tracked {
			t.Fatalf("unexpected event type %q", eventType)
		}
		wantTypes[eventType] = true
		if event["run_id"] != "coderrun-it-7" {
			t.Fatalf("event %s is missing the run scope", eventType)
		}
		if event["repo"] != "acme/widgets" || event["ref"] != float64(7) || event["mode"] != "resolve-issue" {
			t.Fatalf("event %s is missing repo/ref/mode metadata", eventType)
		}
		if _, ok := event["detail"]; ok {
			t.Fatalf("info-level event %s must not carry detail", eventType)
		}
	}
	for eventType, seen := range wantTypes {
		if !seen {
			t.Fatalf("missing %s event", eventType)
		}
	}
	exit := findEvent(t, events, "run.exit")
	if exit["status"] != "ok" {
		t.Fatalf("run.exit status = %v, want ok", exit["status"])
	}
	// The registered credentials are never used on this path and must never
	// surface anywhere in the run's output.
	for _, leaked := range []string{testGitToken, testGitHubTokn} {
		if strings.Contains(output.String(), leaked) || strings.Contains(errorsOut.String(), leaked) {
			t.Fatalf("output contains a fake credential (constant: %s)", secretConstantName(leaked))
		}
	}
}

func TestRunDebugLevelEmitsDetailAndStillRedacts(t *testing.T) {
	root := t.TempDir()
	// The remote embeds an unregistered credential in userinfo position, so
	// the clone failure forces every exit path through redaction.
	t.Setenv("COURIER_REPO_URL", "file://"+testURLToken+"@example.invalid/nonexistent.git")
	t.Setenv("COURIER_WORKSPACE", filepath.Join(root, "workspace"))
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "goal")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_LOG_LEVEL", "debug")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != 1 {
		t.Fatalf("run exit code = %d, want 1", code)
	}

	events := parseEvents(t, &output)
	exit := findEvent(t, events, "run.exit")
	if exit["status"] != "error" {
		t.Fatalf("run.exit status = %v, want error", exit["status"])
	}
	detail, ok := exit["detail"].(map[string]any)
	if !ok {
		t.Fatal("debug-level run.exit must carry a detail object")
	}
	if _, ok := detail["exit_code"]; !ok {
		t.Fatal("run.exit detail must carry the exit code")
	}
	reason, _ := detail["reason"].(string)
	if !strings.Contains(reason, "[REDACTED]") {
		t.Fatal("run.exit reason was not redacted")
	}
	if strings.Contains(output.String(), testURLToken) || strings.Contains(errorsOut.String(), testURLToken) {
		t.Fatal("output contains the fake URL credential (constant: testURLToken)")
	}
}

func TestRunConfigFailureKeepsRunScope(t *testing.T) {
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_MODE", "resolve-issue")
	// COURIER_REPO_URL is deliberately missing.

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != 1 {
		t.Fatalf("run exit code = %d, want 1", code)
	}
	exit := findEvent(t, parseEvents(t, &output), "run.exit")
	if exit["run_id"] != "coderrun-it-7" || exit["status"] != "error" {
		t.Fatalf("run.exit event lost run scope or status: run_id=%v status=%v", exit["run_id"], exit["status"])
	}
}

// TestChildOutputIsRedactedBeforeStreams replaces OpenCode with a script that
// prints registered credentials to both streams — one of them fragmented
// across separate shell writes — and proves the redacting transport removed
// every value while keeping ordinary output, stream separation, and the exit
// code intact. Failure messages name constants, never values.
func TestChildOutputIsRedactedBeforeStreams(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()

	fragment1 := testGitToken[:9]
	fragment2 := testGitToken[9:]
	script := fmt.Sprintf(`#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'stdout git token '
printf '%s'
printf '%s delivered\n'
printf 'stdout plain line\n'
printf 'stderr bearer ' >&2
printf '%s\n' >&2
printf 'stderr partial without newline ' >&2
printf 'completed\n' > completed.txt
git add --all -- .
git commit -m 'test: completed work' >/dev/null
`, fragment1, fragment2, testGitHubTokn)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, script)

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_GIT_TOKEN", testGitToken)
	t.Setenv("GITHUB_TOKEN", testGitHubTokn)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0", code)
	}

	stdoutText := output.String()
	stderrText := errorsOut.String()
	for _, leaked := range []string{testGitToken, fragment1, fragment2} {
		if strings.Contains(stdoutText, leaked) {
			t.Fatalf("stdout contains a fake credential fragment (constant: %s)", secretConstantName(testGitToken))
		}
	}
	if !strings.Contains(stdoutText, "stdout git token") || !strings.Contains(stdoutText, "delivered") || !strings.Contains(stdoutText, "stdout plain line") {
		t.Fatal("stdout lost ordinary output around the redaction")
	}
	if !strings.Contains(stdoutText, courierlog.RedactedPlaceholder) {
		t.Fatal("stdout credential was not replaced by the redaction placeholder")
	}
	if strings.Contains(stderrText, testGitHubTokn) {
		t.Fatal("stderr contains a fake credential (constant: testGitHubTokn)")
	}
	if !strings.Contains(stderrText, "stderr bearer") || !strings.Contains(stderrText, "stderr partial without newline") {
		t.Fatal("stderr lost ordinary output, including the flushed final partial line")
	}
	if !strings.Contains(stderrText, courierlog.RedactedPlaceholder) {
		t.Fatal("stderr credential was not replaced by the redaction placeholder")
	}
}

// TestChildOutputRedactedOnFailureExit keeps failure-exit semantics and
// redaction on a failing child: the run exits with the standard failure code
// (the child's code rides in the termination reason) and the child's final
// output is flushed through the redactor before the termination handoff.
func TestChildOutputRedactedOnFailureExit(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, fmt.Sprintf(`#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'failing run token %s\n'
exit 3
`, testGitToken))

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_GIT_TOKEN", testGitToken)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d (the child's code must not pass through)", code, exitFailed)
	}
	if !strings.Contains(output.String(), "opencode exited with status 3") {
		t.Fatalf("failure reason should keep the child's exit code: %q", output.String())
	}
	if strings.Contains(output.String(), testGitToken) {
		t.Fatal("stdout contains a fake credential on the failure path (constant: testGitToken)")
	}
	if !strings.Contains(output.String(), "failing run token") || !strings.Contains(output.String(), courierlog.RedactedPlaceholder) {
		t.Fatal("failure-path stdout lost ordinary output or the redaction placeholder")
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatal("failure path lost the termination handoff")
	}
}

// TestRunSurfacesUnavailableCapabilityBeforeWork proves the bounded preflight
// names an unreachable configured server to the model, emits a structured
// capability.status diagnostic, and augments the no-work terminal reason,
// while a registered credential stays out of every stream.
func TestRunSurfacesUnavailableCapabilityBeforeWork(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in\n  mcp) printf '%s\\n' '✗ github failed'; printf '%s\\n' '    SSE error: Non-200 status code (400)'; exit 0;;\nesac\nprintf 'opencode argv: %s\\n' \"$*\"\nexit 0\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_MODE", "resolve-issue")

	const capabilityProbeToken = "cap-probe-token-zz99"
	t.Setenv("GITHUB_TOKEN", capabilityProbeToken)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if strings.Contains(output.String(), "✗") {
		t.Fatal("run stdout contains a raw probe glyph; probe output must stay out of the run's streams")
	}
	if strings.Contains(errorsOut.String(), "✗") {
		t.Fatal("run stderr contains raw probe output")
	}

	events := parseEvents(t, &output)
	status := findEvent(t, events, "capability.status")
	if status["status"] != "error" {
		t.Fatalf("capability.status = %v, want error", status["status"])
	}
	detail, ok := status["detail"].(map[string]any)
	if !ok {
		t.Fatal("capability.status must carry a detail object")
	}
	servers, ok := detail["servers"].([]any)
	if !ok || len(servers) == 0 {
		t.Fatal("capability.status detail must carry the server list")
	}
	first, ok := servers[0].(map[string]any)
	if !ok {
		t.Fatal("first capability entry is not an object")
	}
	if first["name"] != "github" || first["available"] != false {
		t.Fatalf("first capability = name %v available %v, want github unavailable", first["name"], first["available"])
	}
	if reason, _ := first["reason"].(string); !strings.Contains(reason, "Non-200") {
		t.Fatal("first capability reason lost the connection error text")
	}

	start := strings.Index(output.String(), "opencode argv:")
	if start < 0 {
		t.Fatal("run output is missing the opencode argv echo")
	}
	argvText := output.String()[start:]
	if !strings.Contains(argvText, "UNAVAILABLE") || !strings.Contains(argvText, "github (SSE error") {
		t.Fatal("the model was not told the configured server is unavailable")
	}
	if strings.Contains(argvText, capabilityProbeToken) {
		t.Fatal("opencode argv output contains a fake credential (constant: capabilityProbeToken)")
	}
	if strings.Contains(output.String(), capabilityProbeToken) || strings.Contains(errorsOut.String(), capabilityProbeToken) {
		t.Fatal("output contains a fake credential (constant: capabilityProbeToken)")
	}
	if !strings.Contains(output.String(), "without declaring an outcome and produced no commit") || !strings.Contains(output.String(), "configured capability unavailable: github") {
		t.Fatalf("terminal reason lost the no-work sentence or the unavailable capability: %q", output.String())
	}
}

// TestRunHealthyCapabilityLeavesRunBehaviorUnchanged proves a preflight that
// finds every configured server available adds no framing, reports an ok
// capability.status, and changes nothing about the run's outcome.
func TestRunHealthyCapabilityLeavesRunBehaviorUnchanged(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in\n  mcp) printf '%s\\n' '✓ github connected'; exit 0;;\nesac\nprintf 'opencode argv: %s\\n' \"$*\"\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_FRAMING", "keep scope tight")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}

	events := parseEvents(t, &output)
	status := findEvent(t, events, "capability.status")
	if status["status"] != "ok" {
		t.Fatalf("capability.status = %v, want ok", status["status"])
	}
	detail, ok := status["detail"].(map[string]any)
	if !ok {
		t.Fatal("capability.status must carry a detail object")
	}
	servers, ok := detail["servers"].([]any)
	if !ok || len(servers) == 0 {
		t.Fatal("capability.status detail must carry the server list")
	}
	first, ok := servers[0].(map[string]any)
	if !ok || first["name"] != "github" || first["available"] != true {
		t.Fatalf("first capability = %v, want github available", first)
	}

	start := strings.Index(output.String(), "opencode argv:")
	if start < 0 {
		t.Fatal("run output is missing the opencode argv echo")
	}
	argvText := output.String()[start:]
	if !strings.Contains(argvText, "keep scope tight") {
		t.Fatal("the model did not receive the configured framing")
	}
	if strings.Contains(argvText, "UNAVAILABLE") {
		t.Fatal("healthy preflight must not add an unavailable-capability note")
	}

	exit := findEvent(t, events, "run.exit")
	if exit["status"] != "ok" {
		t.Fatalf("run.exit status = %v, want ok", exit["status"])
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("healthy run lost the Verifying termination: %q", output.String())
	}
}

func TestPreflightCapabilitiesIgnoresStderrNoise(t *testing.T) {
	// A binary that has no `mcp` subcommand prints an error to stderr and exits
	// non-zero. The probe parses stdout only, so it must report no capabilities
	// rather than inventing one from the stderr text.
	dir := t.TempDir()
	fake := filepath.Join(dir, "opencode")
	writeExecutable(t, fake, "#!/bin/sh\nprintf '%s\\n' 'error: unknown command \"mcp\"' >&2\nexit 1\n")
	caps := preflightCapabilities(context.Background(), executor.Command{Binary: fake, Args: []string{"mcp", "list"}}, dir)
	if len(caps) != 0 {
		t.Fatalf("preflightCapabilities() = %#v, want none from stderr-only noise", caps)
	}
}

func TestPreflightCapabilitiesParsesStdoutStatus(t *testing.T) {
	// A healthy server and a failed server printed on stdout (as `opencode mcp
	// list` does) are parsed regardless of the process exit status.
	dir := t.TempDir()
	fake := filepath.Join(dir, "opencode")
	writeExecutable(t, fake, "#!/bin/sh\nprintf '%s\\n' '\u2713 github connected'\nprintf '%s\\n' '\u2717 metrics failed'\nprintf '%s\\n' '    connection refused'\nexit 3\n")
	caps := preflightCapabilities(context.Background(), executor.Command{Binary: fake, Args: []string{"mcp", "list"}}, dir)
	want := []executor.MCPCapability{
		{Name: "github", Available: true},
		{Name: "metrics", Available: false, Reason: "connection refused"},
	}
	if !reflect.DeepEqual(caps, want) {
		t.Fatalf("preflightCapabilities() = %#v, want %#v", caps, want)
	}
}

// Guards a null byte in the binary path: exec must reject it without a panic, a hang, or invented capabilities.
func TestPreflightCapabilitiesHandlesInvalidBinaryPath(t *testing.T) {
	caps := preflightCapabilities(context.Background(), executor.Command{Binary: "opencode\x00", Args: []string{"mcp", "list"}}, "")
	if len(caps) != 0 {
		t.Fatalf("preflightCapabilities() = %#v, want none from a null-byte binary path", caps)
	}
}

// Guards a symlinked binary: the probe must resolve the link and parse the target's stdout.
func TestPreflightCapabilitiesFollowsSymlinkedBinary(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real-opencode")
	writeExecutable(t, real, "#!/bin/sh\ncase \"$1\" in mcp) printf '%s\\n' '✓ github connected'; exit 0;; esac\n")
	link := filepath.Join(dir, "opencode")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("os.Symlink() error = %v", err)
	}
	caps := preflightCapabilities(context.Background(), executor.Command{Binary: link, Args: []string{"mcp", "list"}}, dir)
	want := []executor.MCPCapability{{Name: "github", Available: true}}
	if !reflect.DeepEqual(caps, want) {
		t.Fatalf("preflightCapabilities() = %#v, want %#v", caps, want)
	}
}

// parseEvents decodes the JSON event lines among the run's output. Event
// lines are the only JSON objects with an "event" field. Failures report
// line indexes, never line contents.
func parseEvents(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	var events []map[string]any
	for i, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", i+1, err)
		}
		if _, ok := event["event"]; ok {
			events = append(events, event)
		}
	}
	return events
}

func findEvent(t *testing.T, events []map[string]any, eventType string) map[string]any {
	t.Helper()
	for _, event := range events {
		if event["event"] == eventType {
			return event
		}
	}
	t.Fatalf("no %s event among %d events", eventType, len(events))
	return nil
}

// countEvents reports how many events of a type are present, for assertions
// that a continuation fired exactly N times.
func countEvents(t *testing.T, events []map[string]any, eventType string) int {
	t.Helper()
	n := 0
	for _, event := range events {
		if event["event"] == eventType {
			n++
		}
	}
	return n
}

// eventDetail returns the decoded detail object of an event, failing the test
// when the event carries no detail (e.g. emitted at a non-debug level).
func eventDetail(t *testing.T, event map[string]any) map[string]any {
	t.Helper()
	detail, ok := event["detail"].(map[string]any)
	if !ok {
		t.Fatalf("event %v carries no detail object", event["event"])
	}
	return detail
}

func secretConstantName(value string) string {
	switch value {
	case testGitToken:
		return "testGitToken"
	case testGitHubTokn:
		return "testGitHubTokn"
	default:
		return "unknown-fake-credential"
	}
}

// remoteWithExistingBranch builds a bare remote whose base and a work branch
// are already published, the state a resolve-issue run must inspect before it
// decides whether the branch is safe to adopt.
func remoteWithExistingBranch(t *testing.T, root string) string {
	t.Helper()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	orphan := filepath.Join(root, "orphan")
	runGit(t, root, "clone", remote, orphan)
	configureGit(t, orphan)
	runGit(t, orphan, "checkout", "-b", "courier/resolve-issue/acme-widgets/7", "origin/main")
	write(t, filepath.Join(orphan, "work.txt"), "previous brief\n")
	commit(t, orphan, "work: previous brief")
	runGit(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")
	return remote
}

// remoteWithConflictingBranch is remoteWithExistingBranch where the work
// branch and a later base commit edit the same line, so adoption's base sync
// stops on a conflict in README.md.
func remoteWithConflictingBranch(t *testing.T, root string) string {
	t.Helper()
	remote := remoteWithExistingBranch(t, root)
	orphan := filepath.Join(root, "orphan")
	write(t, filepath.Join(orphan, "README.md"), "branch edit\n")
	commit(t, orphan, "work: edit readme")
	runGit(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")
	source := filepath.Join(root, "source")
	write(t, filepath.Join(source, "README.md"), "base edit\n")
	commit(t, source, "base: edit readme")
	runGit(t, source, "push", "origin", "main")
	return remote
}

// setResolveIssueEnv points a run at remote in resolve-issue mode against a
// fake GitHub API. It returns the workspace directory the run will use.
func setResolveIssueEnv(t *testing.T, root, remote, githubBase, openCodeBinary string) string {
	t.Helper()
	workspace := filepath.Join(root, "workspace")
	scratch := filepath.Join(root, "scratch")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratch)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_GITHUB_API_BASE", githubBase)
	t.Setenv("COURIER_OPENCODE_BINARY", openCodeBinary)
	t.Setenv("COURIER_TERMINATION_FILE", filepath.Join(root, "termination"))
	return workspace
}

// setContinuationEnv configures a resolve-issue run for the resume and loop
// tests: zero resume backoff so a crash resume never sleeps, debug event detail
// so the executor.continuation payload is present, and a fake-script state dir
// OUTSIDE the workspace (exported as COURIER_FAKE_STATE) so the script's argv
// log and counters cannot pollute the workspace's WorkState.
func setContinuationEnv(t *testing.T, root, remote, githubBase, openCodeBinary string) string {
	t.Helper()
	stateDir := filepath.Join(root, "fakestate")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	workspace := setResolveIssueEnv(t, root, remote, githubBase, openCodeBinary)
	// Run identity is required or the emitter drops every event line.
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_LOG_LEVEL", "debug")
	t.Setenv("COURIER_FAKE_STATE", stateDir)
	return workspace
}

// readArgvLog returns the argv lines the fake script recorded, one per
// invocation, in order. The first line is the initial call; any later line is
// a resume.
func readArgvLog(t *testing.T, stateDir string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(stateDir, "argv.log"))
	if err != nil {
		t.Fatalf("read fake-script argv log: %v", err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

func configureGit(t *testing.T, directory string) {
	t.Helper()
	runGit(t, directory, "config", "user.name", "Courier Test")
	runGit(t, directory, "config", "user.email", "courier-test@example.invalid")
}

func commit(t *testing.T, directory, message string) {
	t.Helper()
	runGit(t, directory, "add", "--all", "--", ".")
	runGit(t, directory, "commit", "-m", message)
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
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

func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = directory
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v (%s)", strings.Join(args, " "), err, output)
	}
}

// TestRunContinuationDetailIsRedacted proves the opaque fingerprint does not
// expose a registered secret captured in the child's raw output, while the
// no-progress termination history still carries its redaction placeholder.
func TestRunContinuationDetailIsRedacted(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
printf '{"type":"text","sessionID":"ses_redact1","part":{"type":"text","text":"echo sekrit-token-value-42 done"}}\n'
printf 'partial\n' > partial.txt
exit 0
`)
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer prServer.Close()
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	const secretValue = "sekrit-token-value-42"
	// Name shaped like a credential so newReporter's RegisterEnvironment
	// picks it up and registers the value with the run's redactor.
	t.Setenv("COURIER_FAKE_TOKEN", secretValue)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if strings.Contains(output.String(), secretValue) {
		t.Fatal("run stdout leaked the raw secret")
	}
	if !strings.Contains(output.String(), "looping:") {
		t.Fatalf("reason = %q, want a looping: reason", output.String())
	}

	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 1 {
		t.Fatalf("executor.continuation count = %d, want 1 (first classification resumes once, second identical loops)", n)
	}
	cont := findEvent(t, events, "executor.continuation")
	detail := eventDetail(t, cont)
	fingerprint, ok := detail["fingerprint"].(string)
	if !ok {
		t.Fatal("executor.continuation detail must carry a fingerprint string")
	}
	if strings.Contains(fingerprint, secretValue) {
		t.Fatal("fingerprint detail leaked the raw secret")
	}
	if len(fingerprint) != 64 {
		t.Fatalf("fingerprint length = %d, want 64 hex characters", len(fingerprint))
	}
	if _, err := hex.DecodeString(fingerprint); err != nil {
		t.Fatalf("fingerprint is not hexadecimal: %v", err)
	}
	if strings.Contains(fingerprint, "[REDACTED]") || strings.Contains(fingerprint, "echo") || strings.Contains(fingerprint, "done") {
		t.Fatalf("fingerprint exposed raw state text: %q", fingerprint)
	}

	start := strings.Index(output.String(), "COURIER_TERMINATION")
	if start < 0 {
		t.Fatal("missing the COURIER_TERMINATION line")
	}
	terminationLine := output.String()[start:]
	if !strings.Contains(terminationLine, `"phase":"NeedsHuman"`) {
		t.Fatalf("termination line = %q, want the NeedsHuman phase", terminationLine)
	}
	if strings.Contains(terminationLine, secretValue) {
		t.Fatal("termination reason leaked the raw secret")
	}
	if !strings.Contains(terminationLine, "[REDACTED]") {
		t.Fatal("termination reason lost the redaction placeholder")
	}
	if !strings.Contains(terminationLine, "last message") {
		t.Fatal("termination reason lost the state history")
	}
}

// TestRunEmitsTelemetry proves the run tallies the coordinator's OpenCode
// event stream: a run.summary event carries the rich summary next to the
// terminal handoff, and the COURIER_TERMINATION line carries the compact form.
func TestRunEmitsTelemetry(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	orphan := filepath.Join(root, "orphan")
	runGit(t, root, "clone", remote, orphan)
	configureGit(t, orphan)
	runGit(t, orphan, "checkout", "-b", "courier/resolve-issue/acme-widgets/7", "origin/main")
	write(t, filepath.Join(orphan, "work.txt"), "previous brief\n")
	commit(t, orphan, "work: previous brief")
	runGit(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")

	write(t, filepath.Join(source, "base.txt"), "moved base\n")
	commit(t, source, "base: moved")
	runGit(t, source, "push", "origin", "main")

	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch dir")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
if [ -e "$COURIER_SCRATCH_DIR/outcome.json" ]; then exit 9; fi
printf 'scratch dir: %s\n' "$COURIER_SCRATCH_DIR"
printf 'opencode argv: %s\n' "$*"
printf 'completed\n' > completed.txt
git add --all -- .
git commit -m 'test: completed work' >/dev/null
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
printf '{"sessionID":"ses_exec","part":{"type":"step-finish","tokens":{"input":100,"output":20,"reasoning":0,"cache":{"read":500,"write":10}}}}\n'
printf '{"sessionID":"ses_exec","part":{"type":"tool","tool":"bash","state":{"status":"completed","time":{"start":1000,"end":2000}}}}\n'
printf '{"sessionID":"ses_exec","part":{"type":"tool","tool":"task","state":{"status":"completed","time":{"start":2000,"end":7000},"input":{"subagent_type":"coder-local"},"metadata":{"model":{"modelID":"litellm/x"},"sessionId":"ses_sub"}}}}\n'
`)
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(scratchDirectory, "outcome.json"), `{"outcome":"no_change_needed","evidence":"stale"}`)
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_OPENCODE_AGENT", "architect")
	t.Setenv("COURIER_FRAMING", "capacity is elastic; fan out freely")
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	// Run identity is required or the emitter drops the run.summary event.
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != 0 {
		t.Fatalf("run exit code = %d, stderr=%q, stdout=%q", code, errorsOut.String(), output.String())
	}
	if _, err := os.Stat(filepath.Join(workspace, "base.txt")); err != nil {
		t.Fatalf("adopted branch was not synchronized to base: %v", err)
	}
	if !strings.Contains(output.String(), "Open a PR to address issue #7.") || !strings.Contains(output.String(), "any-model/name") || !strings.Contains(output.String(), "capacity is elastic; fan out freely") {
		t.Fatalf("OpenCode did not receive exact goal/model/framing: %q", output.String())
	}
	wantOutcomePath := filepath.Join(scratchDirectory, "outcome.json")
	if !strings.Contains(output.String(), "scratch dir: "+scratchDirectory) || !strings.Contains(output.String(), "Open a PR to address issue #7. Declare the outcome at "+wantOutcomePath) {
		t.Fatalf("child scratch path and actual goal declaration path disagree: %q", output.String())
	}
	if !strings.Contains(output.String(), "--agent architect") {
		t.Fatalf("OpenCode did not receive configured agent: %q", output.String())
	}
	if !strings.Contains(output.String(), `COURIER_TERMINATION {"phase":"Verifying","result":"success","exit_code":0`) {
		t.Fatalf("missing stable success termination: %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"changes"`) {
		t.Fatalf("runtime did not read the child's changes declaration at %q: %q", wantOutcomePath, output.String())
	}
	terminationOutput := string(mustRead(t, termination))
	if !strings.Contains(terminationOutput, `"phase":"Verifying"`) {
		t.Fatalf("termination file = %q", terminationOutput)
	}

	summary := findEvent(t, parseEvents(t, &output), "run.summary")
	detail := eventDetail(t, summary)
	tallied, ok := detail["summary"].(map[string]any)
	if !ok {
		t.Fatalf("run.summary detail carries no summary object: %v", detail)
	}
	if tallied["model_calls"] != float64(1) {
		t.Fatalf("summary model_calls = %v, want 1", tallied["model_calls"])
	}
	if tallied["tool_calls"] != float64(2) {
		t.Fatalf("summary tool_calls = %v, want 2", tallied["tool_calls"])
	}
	subagents, ok := tallied["subagents"].([]any)
	if !ok || len(subagents) != 1 {
		t.Fatalf("summary subagents = %v, want one entry", tallied["subagents"])
	}
	first, ok := subagents[0].(map[string]any)
	if !ok {
		t.Fatalf("first subagent entry is not an object: %v", subagents[0])
	}
	if first["calls"] != float64(1) {
		t.Fatalf("subagent calls = %v, want 1", first["calls"])
	}

	start := strings.Index(output.String(), "COURIER_TERMINATION ")
	if start < 0 {
		t.Fatal("missing the COURIER_TERMINATION line")
	}
	terminationLine := output.String()[start:]
	if !strings.Contains(terminationLine, `"summary":{"modelCalls":1,"toolCalls":2`) {
		t.Fatalf("termination line missing the compact telemetry: %q", terminationLine)
	}
}
