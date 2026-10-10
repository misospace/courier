package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/evidence"
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

// TestEmitTerminationStaysWithinTerminationBudget proves the handoff line never
// exceeds the 4 KiB pod termination-message budget. A 1024-byte reason built
// from every JSON-escaping character (which expands under escaping) plus a
// full 24-subagent summary is shrunk by dropping subagent entries
// lowest-runtime first — and, if still over, the whole summary — while
// phase/result/exit_code/outcome and the reason always survive.
func TestEmitTerminationStaysWithinTerminationBudget(t *testing.T) {
	// Eight-byte unit covering all six escaping characters: the two quotes,
	// two backslashes and newline double in length, while <, > and & expand to
	// six-byte \uXXXX escapes. 128 units make exactly 1024 raw bytes.
	unit := `"` + `"` + `\` + `\` + "\n" + "<>&"
	if len(unit) != 8 {
		t.Fatalf("test unit is %d bytes, want 8", len(unit))
	}
	reason := strings.Repeat(unit, maxTerminationReasonBytes/len(unit))
	if len(reason) != maxTerminationReasonBytes {
		t.Fatalf("reason is %d bytes, want %d", len(reason), maxTerminationReasonBytes)
	}

	// The maximum subagent count, each with a long agent and model name, so
	// the unshrunk line is well over the budget and the summary must give ground.
	const numSubagents = 24
	subagents := make([]courierv1alpha1.SubagentTally, 0, numSubagents)
	for i := 0; i < numSubagents; i++ {
		subagents = append(subagents, courierv1alpha1.SubagentTally{
			Agent:         "agent-" + strings.Repeat("x", 64),
			Model:         "model-" + strings.Repeat("y", 32),
			Calls:         1,
			RuntimeMillis: int64((numSubagents - i) * 10),
		})
	}
	summary := &courierv1alpha1.RunTelemetry{
		ModelCalls: 1,
		ToolCalls:  numSubagents,
		Sessions:   1,
		Subagents:  subagents,
	}

	var out bytes.Buffer
	emitTermination(&out, config{}, termination{
		Phase:    "Failed",
		Result:   "failure",
		ExitCode: exitFailed,
		Outcome:  "changes",
		Reason:   reason,
		Summary:  summary,
	})

	line := out.String()
	if len(line) > maxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), maxTerminationLineBytes)
	}

	var parsed struct {
		Phase    string `json:"phase"`
		Result   string `json:"result"`
		ExitCode int    `json:"exit_code"`
		Outcome  string `json:"outcome"`
		Reason   string `json:"reason"`
	}
	body := strings.TrimPrefix(line, terminationLinePrefix)
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Phase != "Failed" {
		t.Fatalf("phase = %q, want %q", parsed.Phase, "Failed")
	}
	if parsed.Result != "failure" {
		t.Fatalf("result = %q, want %q", parsed.Result, "failure")
	}
	if parsed.ExitCode != exitFailed {
		t.Fatalf("exit_code = %d, want %d", parsed.ExitCode, exitFailed)
	}
	if parsed.Outcome != "changes" {
		t.Fatalf("outcome = %q, want %q", parsed.Outcome, "changes")
	}
	if parsed.Reason == "" {
		t.Fatal("reason was dropped from the termination line")
	}
}

// TestEmitTerminationTrimsEscapeHeavyReasonWithoutSummary is the worst case the
// summary-only shrinking cannot cover: a 1024-byte reason of a single
// JSON-escaping character (& -> \u0026, six bytes each) with no summary. The
// raw-byte truncation in terminate leaves the reason at its 1024-byte bound,
// but the escaped payload pushes the line to 6144 bytes. With the summary
// already empty, the line can only fit by cutting the reason itself; the
// handoff must stay within budget with phase/result/exit_code/outcome intact
// and a non-empty reason.
func TestEmitTerminationTrimsEscapeHeavyReasonWithoutSummary(t *testing.T) {
	reason := strings.Repeat("&", maxTerminationReasonBytes)
	if len(reason) != maxTerminationReasonBytes {
		t.Fatalf("reason is %d bytes, want %d", len(reason), maxTerminationReasonBytes)
	}

	var out bytes.Buffer
	emitTermination(&out, config{}, termination{
		Phase:    "Failed",
		Result:   "failure",
		ExitCode: exitFailed,
		Outcome:  "changes",
		Reason:   reason,
	})

	line := out.String()
	if len(line) > maxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), maxTerminationLineBytes)
	}

	var parsed struct {
		Phase    string `json:"phase"`
		Result   string `json:"result"`
		ExitCode int    `json:"exit_code"`
		Outcome  string `json:"outcome"`
		Reason   string `json:"reason"`
	}
	body := strings.TrimPrefix(line, terminationLinePrefix)
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Phase != "Failed" {
		t.Fatalf("phase = %q, want %q", parsed.Phase, "Failed")
	}
	if parsed.Result != "failure" {
		t.Fatalf("result = %q, want %q", parsed.Result, "failure")
	}
	if parsed.ExitCode != exitFailed {
		t.Fatalf("exit_code = %d, want %d", parsed.ExitCode, exitFailed)
	}
	if parsed.Outcome != "changes" {
		t.Fatalf("outcome = %q, want %q", parsed.Outcome, "changes")
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the last-resort trim")
	}
}

// TestEmitTerminationFitsEscapeHeavyReasonWithFullSummary is the combined worst
// case: a 1024-byte &-only reason (marshalling to 6144 bytes) alongside a full
// 24-subagent summary. The summary gives ground first and is fully dropped,
// then the reason is cut as a last resort; the line must still fit the budget
// with phase/result/exit_code/outcome intact and a non-empty reason.
func TestEmitTerminationFitsEscapeHeavyReasonWithFullSummary(t *testing.T) {
	reason := strings.Repeat("&", maxTerminationReasonBytes)
	if len(reason) != maxTerminationReasonBytes {
		t.Fatalf("reason is %d bytes, want %d", len(reason), maxTerminationReasonBytes)
	}

	const numSubagents = 24
	subagents := make([]courierv1alpha1.SubagentTally, 0, numSubagents)
	for i := 0; i < numSubagents; i++ {
		subagents = append(subagents, courierv1alpha1.SubagentTally{
			Agent:         "agent-" + strings.Repeat("x", 64),
			Model:         "model-" + strings.Repeat("y", 32),
			Calls:         1,
			RuntimeMillis: int64((numSubagents - i) * 10),
		})
	}
	summary := &courierv1alpha1.RunTelemetry{
		ModelCalls: 1,
		ToolCalls:  numSubagents,
		Sessions:   1,
		Subagents:  subagents,
	}

	var out bytes.Buffer
	emitTermination(&out, config{}, termination{
		Phase:    "Failed",
		Result:   "failure",
		ExitCode: exitFailed,
		Outcome:  "changes",
		Reason:   reason,
		Summary:  summary,
	})

	line := out.String()
	if len(line) > maxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), maxTerminationLineBytes)
	}

	var parsed struct {
		Phase    string `json:"phase"`
		Result   string `json:"result"`
		ExitCode int    `json:"exit_code"`
		Outcome  string `json:"outcome"`
		Reason   string `json:"reason"`
	}
	body := strings.TrimPrefix(line, terminationLinePrefix)
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Phase != "Failed" {
		t.Fatalf("phase = %q, want %q", parsed.Phase, "Failed")
	}
	if parsed.Result != "failure" {
		t.Fatalf("result = %q, want %q", parsed.Result, "failure")
	}
	if parsed.ExitCode != exitFailed {
		t.Fatalf("exit_code = %d, want %d", parsed.ExitCode, exitFailed)
	}
	if parsed.Outcome != "changes" {
		t.Fatalf("outcome = %q, want %q", parsed.Outcome, "changes")
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the last-resort trim")
	}
}

// TestEmitTerminationDropsLowestRuntimeSubagentsFirst pins the drop ORDER when
// a full summary overruns the budget with an ordinary (non-escaping) reason:
// subagent entries are dropped lowest-runtime first, so the survivors are a
// prefix of the input list (built descending-runtime, the survivors are the
// highest-runtime). Scalar summary fields survive, and the line stays within
// the budget.
func TestEmitTerminationDropsLowestRuntimeSubagentsFirst(t *testing.T) {
	// 24 subagents, highest-runtime first, with long names so the unshrunk
	// line is well over the 4 KiB budget and the summary must give ground.
	const numSubagents = 24
	subagents := make([]courierv1alpha1.SubagentTally, 0, numSubagents)
	for i := 0; i < numSubagents; i++ {
		subagents = append(subagents, courierv1alpha1.SubagentTally{
			Agent:         "agent-" + strings.Repeat("x", 80),
			Model:         "model-" + strings.Repeat("y", 48),
			Calls:         1,
			RuntimeMillis: int64((numSubagents - i) * 10),
		})
	}
	summary := &courierv1alpha1.RunTelemetry{
		ModelCalls: 7,
		ToolCalls:  numSubagents,
		Sessions:   2,
		Subagents:  subagents,
	}
	// A plain ASCII reason: no escaping, so only the summary gives ground.
	reason := strings.Repeat("r", 300)

	var out bytes.Buffer
	emitTermination(&out, config{}, termination{
		Phase:    "Failed",
		Result:   "failure",
		ExitCode: exitFailed,
		Reason:   reason,
		Summary:  summary,
	})

	line := out.String()
	if len(line) > maxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), maxTerminationLineBytes)
	}

	var parsed struct {
		Reason  string `json:"reason"`
		Summary *struct {
			ModelCalls int                             `json:"modelCalls"`
			ToolCalls  int                             `json:"toolCalls"`
			Sessions   int                             `json:"sessions"`
			Subagents  []courierv1alpha1.SubagentTally `json:"subagents"`
		} `json:"summary"`
	}
	body := strings.TrimPrefix(line, terminationLinePrefix)
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Summary == nil {
		t.Fatal("summary was dropped entirely; some subagents should survive")
	}
	if got := len(parsed.Summary.Subagents); got == 0 {
		t.Fatal("no subagents survived the shrink")
	} else if got == numSubagents {
		t.Fatalf("all %d subagents survived; the summary should have given ground", numSubagents)
	}
	// The survivors are a prefix of the input list: built descending-runtime,
	// the kept entries are the highest-runtime.
	for i, kept := range parsed.Summary.Subagents {
		if kept != subagents[i] {
			t.Fatalf("survivor %d = %+v, want the input's %d-th entry %+v (drops are lowest-runtime first)", i, kept, i, subagents[i])
		}
	}
	// Scalar fields survive the shrink.
	if parsed.Summary.ModelCalls != 7 {
		t.Fatalf("modelCalls = %d, want 7", parsed.Summary.ModelCalls)
	}
	if parsed.Summary.ToolCalls != numSubagents {
		t.Fatalf("toolCalls = %d, want %d", parsed.Summary.ToolCalls, numSubagents)
	}
	if parsed.Summary.Sessions != 2 {
		t.Fatalf("sessions = %d, want 2", parsed.Summary.Sessions)
	}
	if parsed.Reason != reason {
		t.Fatalf("reason was altered; only the summary should give ground: %q", parsed.Reason)
	}
}

// TestEmitTerminationKeepsUTF8ReasonOnLastResortShrink is the UTF-8 boundary
// case for the last-resort reason cut: a long multi-byte (CJK) reason with no
// summary overruns the budget, so the reason is cut; the cut must land on a
// rune boundary, leaving a valid-UTF-8, non-empty reason ending with the
// truncation marker, and the line stays within the budget.
func TestEmitTerminationKeepsUTF8ReasonOnLastResortShrink(t *testing.T) {
	// CJK is three UTF-8 bytes per rune and is not escaped by the JSON
	// marshaler, so a long run of it overruns the budget in raw bytes and
	// forces the last-resort reason cut to fire.
	reason := strings.Repeat("你", 1500)
	if len(reason) != 4500 {
		t.Fatalf("reason is %d bytes, want 4500", len(reason))
	}

	var out bytes.Buffer
	emitTermination(&out, config{}, termination{
		Phase:    "Failed",
		Result:   "failure",
		ExitCode: exitFailed,
		Reason:   reason,
	})

	line := out.String()
	if len(line) > maxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), maxTerminationLineBytes)
	}

	var parsed struct {
		Reason string `json:"reason"`
	}
	body := strings.TrimPrefix(line, terminationLinePrefix)
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the last-resort trim")
	}
	if !utf8.ValidString(parsed.Reason) {
		t.Fatalf("trimmed reason is not valid UTF-8 (the cut split a rune): %q", parsed.Reason)
	}
	if !strings.HasSuffix(parsed.Reason, terminationTruncationSuffix) {
		t.Fatalf("trimmed reason = %q, want the truncation marker", parsed.Reason)
	}
}

// TestShrinkTerminationReason pins the last-resort reason cut: it removes
// exactly the overflow (plus the marker) bytes, leaves a reason at the marker
// floor unchanged, and does not double-count a marker the content already ends
// with.
func TestShrinkTerminationReason(t *testing.T) {
	t.Run("closes a one-byte overflow", func(t *testing.T) {
		reason := strings.Repeat("a", 100)
		got := shrinkTerminationReason(reason, 1)
		want := strings.Repeat("a", 84) + terminationTruncationSuffix
		if got != want {
			t.Fatalf("shrinkTerminationReason() = %q (%d bytes), want %q (%d bytes)", got, len(got), want, len(want))
		}
	})

	t.Run("reason at the marker floor is unchanged", func(t *testing.T) {
		reason := strings.Repeat("a", len(terminationTruncationSuffix))
		got := shrinkTerminationReason(reason, 1)
		if got != reason {
			t.Fatalf("shrinkTerminationReason() = %q, want unchanged %q", got, reason)
		}
	})

	t.Run("content ending in the marker is not double-counted", func(t *testing.T) {
		reason := strings.Repeat("a", 50) + terminationTruncationSuffix
		got := shrinkTerminationReason(reason, 1)
		// The trailing marker is stripped before measuring, so the cut applies
		// to the 50-byte content and the marker is appended exactly once.
		want := strings.Repeat("a", 34) + terminationTruncationSuffix
		if got != want {
			t.Fatalf("shrinkTerminationReason() = %q (%d bytes), want %q (%d bytes)", got, len(got), want, len(want))
		}
		if strings.HasSuffix(got, terminationTruncationSuffix+terminationTruncationSuffix) {
			t.Fatalf("shrinkTerminationReason() double-appended the marker: %q", got)
		}
	})
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

// The failure-evidence tests below pin the gate's silence and capture
// contract (#198): a clean, remote-held worktree captures nothing and emits
// nothing; a dirty or remote-lacking terminal is preserved. The intake is a
// fake httptest endpoint that records each delivery.

// testEvidenceToken is the per-incarnation bearer token the run presents to
// the intake; it is not a real credential.
const testEvidenceToken = "evidence-intake-token-0f8e"

// intakeRecord is one POST the fake evidence intake received.
type intakeRecord struct {
	authorization string
	manifest      evidence.Manifest
	archiveFiles  map[string]string
}

// newEvidenceIntake stands up the fake intake endpoint and returns it plus a
// snapshot function returning every delivery so far (its length is the intake
// count, so absence is asserted by an empty snapshot). Each delivery records
// the Authorization header, the decoded manifest, and the archive tar's
// members by name.
func newEvidenceIntake(t *testing.T) (*httptest.Server, func() []intakeRecord) {
	t.Helper()
	var mu sync.Mutex
	var records []intakeRecord
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		rec := intakeRecord{
			authorization: r.Header.Get("Authorization"),
			archiveFiles:  map[string]string{},
		}
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, "multipart: "+err.Error(), http.StatusBadRequest)
			return
		}
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "multipart part: "+err.Error(), http.StatusBadRequest)
				return
			}
			data, err := io.ReadAll(part)
			if err != nil {
				http.Error(w, "multipart part read: "+err.Error(), http.StatusBadRequest)
				return
			}
			switch part.FormName() {
			case "manifest":
				if err := json.Unmarshal(data, &rec.manifest); err != nil {
					http.Error(w, "manifest: "+err.Error(), http.StatusBadRequest)
					return
				}
			case "archive":
				files, err := readTarGZ(data)
				if err != nil {
					http.Error(w, "archive: "+err.Error(), http.StatusBadRequest)
					return
				}
				rec.archiveFiles = files
			}
		}
		mu.Lock()
		records = append(records, rec)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, func() []intakeRecord {
		mu.Lock()
		defer mu.Unlock()
		out := make([]intakeRecord, len(records))
		copy(out, records)
		return out
	}
}

// readTarGZ expands a gzip'd tar into its member name to content.
func readTarGZ(data []byte) (map[string]string, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	files := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		files[hdr.Name] = string(content)
	}
	return files, nil
}

// remoteWithBranchAtMainTip builds a bare remote whose base and a run branch
// at the base tip (no work yet) are published: the state a relaunched
// resolve-issue run adopts before it finds nothing to recover.
func remoteWithBranchAtMainTip(t *testing.T, root string) string {
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
	runGit(t, source, "branch", "courier/resolve-issue/acme-widgets/7", "main")
	runGit(t, source, "push", "origin", "courier/resolve-issue/acme-widgets/7")
	return remote
}

// fakeGitHubAPI is the shared fake GitHub endpoint: it answers the pull
// request list with no pulls (so adoption proceeds) and accepts outcome
// comments.
func fakeGitHubAPI(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/comments") {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(`{"id":1}`))
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(server.Close)
	return server
}

// armEvidence points a run's evidence capture at the intake with the
// per-incarnation token, the condition under which the gate may deliver.
func armEvidence(t *testing.T, intakeURL string) {
	t.Helper()
	t.Setenv("COURIER_EVIDENCE_URL", intakeURL)
	t.Setenv("COURIER_EVIDENCE_TOKEN", testEvidenceToken)
}

// noEvidenceCapture asserts the intake saw no delivery and the run emitted no
// evidence.capture event: the gate found the world clean.
func noEvidenceCapture(t *testing.T, snapshot func() []intakeRecord, out *bytes.Buffer) {
	t.Helper()
	if records := snapshot(); len(records) != 0 {
		t.Fatalf("intake received %d deliveries, want 0 for a clean world", len(records))
	}
	if n := countEvents(t, parseEvents(t, out), "evidence.capture"); n != 0 {
		t.Fatalf("evidence.capture events = %d, want 0 for a clean world", n)
	}
}

// TestEvidenceGateCleanRemoteConfirmed proves the gate stays silent for a
// clean, remote-held worktree: the run commits and pushes its work, so the
// terminal is confirmed on the remote, and although capture is armed the gate
// finds nothing unrecoverable, so it delivers nothing and emits no event.
func TestEvidenceGateCleanRemoteConfirmed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'completed\n' > completed.txt
git add --all -- . && git commit -m done && git push origin "$COURIER_BRANCH" >/dev/null 2>&1 || true
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("run should end verifying: %q", output.String())
	}
	noEvidenceCapture(t, snapshot, &output)
}

// TestEvidenceRelaunchedClean proves a relaunch that adopts a run branch and
// finds nothing to recover is silent: the worktree is clean and holds no
// commits, so the gate's WorkState is none and the run ends NoChangeNeeded
// without capturing or emitting, even with capture armed.
func TestEvidenceRelaunchedClean(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithBranchAtMainTip(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"no_change_needed","evidence":"already verified"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNoChangeNeeded {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNoChangeNeeded, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NoChangeNeeded"`) {
		t.Fatalf("run should end no-change-needed: %q", output.String())
	}
	noEvidenceCapture(t, snapshot, &output)
}

// TestEvidenceDirtyTerminalCaptures proves a dirty terminal is preserved: the
// run leaves an uncommitted file and declares changes, so the gate finds
// unrecoverable state, captures the worktree, delivers one authenticated
// bundle, and emits a single evidence.capture event. A second run of the same
// scenario with capture unconfigured must produce an identical terminal
// handoff, proving the capture never changes the run's ending.
func TestEvidenceDirtyTerminalCaptures(t *testing.T) {
	runScenario := func(t *testing.T, armed bool) (int, string, func() []intakeRecord) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		var snapshot func() []intakeRecord
		if armed {
			intake, snap := newEvidenceIntake(t)
			snapshot = snap
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			armEvidence(t, intake.URL)
		} else {
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String(), snapshot
	}

	code, armedOut, snapshot := runScenario(t, true)
	if code != exitFailed {
		t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
	}
	if !strings.Contains(armedOut, `"phase":"Failed"`) {
		t.Fatalf("armed run should end failed: %q", armedOut)
	}

	t.Run("armed captures and delivers", func(t *testing.T) {
		records := snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want 1 for a dirty terminal", len(records))
		}
		rec := records[0]
		if rec.authorization != "Bearer "+testEvidenceToken {
			t.Fatalf("authorization = %q, want Bearer <testEvidenceToken>", rec.authorization)
		}
		if rec.manifest.Trigger != "terminal" {
			t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
		}
		if rec.manifest.Run.Name != "coderrun-it-7" {
			t.Fatalf("manifest run name = %q, want coderrun-it-7", rec.manifest.Run.Name)
		}
		if rec.manifest.Workspace.Branch == "" {
			t.Fatal("manifest workspace branch is empty")
		}
		if !isCommitSHA(rec.manifest.Workspace.StartSHA) {
			t.Fatalf("manifest start sha = %q, want a 40-hex commit", rec.manifest.Workspace.StartSHA)
		}
		if rec.manifest.Totals.Files < 1 {
			t.Fatalf("manifest totals files = %d, want at least 1", rec.manifest.Totals.Files)
		}
		content, ok := rec.archiveFiles["evidence-target.txt"]
		if !ok {
			t.Fatalf("archive members = %v, want evidence-target.txt", rec.archiveFiles)
		}
		if content != "uncovered work\n" {
			t.Fatalf("archive content = %q, want the uncommitted file's content", content)
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		capture := findEvent(t, events, "evidence.capture")
		detail := eventDetail(t, capture)
		if detail["trigger"] != "terminal" {
			t.Fatalf("event trigger = %v, want terminal", detail["trigger"])
		}
		if detail["outcome"] != "delivered" {
			t.Fatalf("event outcome = %v, want delivered", detail["outcome"])
		}
	})

	t.Run("unconfigured leaves the ending unchanged", func(t *testing.T) {
		disarmedCode, disarmedOut, _ := runScenario(t, false)
		if disarmedCode != exitFailed {
			t.Fatalf("disarmed run exit code = %d, want %d", disarmedCode, exitFailed)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, disarmedOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with capture unconfigured:\n armed=%v\n disarmed=%v", want, got)
		}
	})
}

// terminationLineJSON decodes the COURIER_TERMINATION handoff line of an
// output into a map, dropping the summary so a byte-identical comparison
// ignores telemetry the scenario does not record.
func terminationLineJSON(t *testing.T, out string) map[string]any {
	t.Helper()
	start := strings.Index(out, "COURIER_TERMINATION ")
	if start < 0 {
		t.Fatal("missing the COURIER_TERMINATION line")
	}
	// Take only the single handoff line: a run.exit event is written after it, so
	// slicing the rest of the output would leave it concatenated and the JSON
	// unparseable.
	line := out[start+len("COURIER_TERMINATION "):]
	if idx := strings.IndexByte(line, '\n'); idx >= 0 {
		line = line[:idx]
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("COURIER_TERMINATION line is not valid JSON: %v (%s)", err, line)
	}
	delete(m, "summary")
	return m
}

// isCommitSHA reports whether v is a 40-character hexadecimal commit.
func isCommitSHA(v string) bool {
	if len(v) != 40 {
		return false
	}
	for _, r := range v {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

// remoteWithBaseOnly builds a bare remote whose only published ref is the
// base: the state a resolve-issue run clones into when the run branch does
// not yet exist on the remote, so the gate's bounded fetch of that branch
// fails and the local commit is left unconfirmed.
func remoteWithBaseOnly(t *testing.T, root string) string {
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
	return remote
}

// TestEvidenceLocalCommitsNotOnRemoteCaptures proves a local commit the
// remote run branch does not hold is preserved: the run commits work but does
// not push, the remote has no run branch so the gate's bounded fetch fails,
// and the run declares changes for a Verifying ending. The gate finds
// unrecoverable state, captures the local commit, delivers one authenticated
// bundle, and emits a single evidence.capture event. A second run of the same
// scenario with capture unconfigured must produce the same Verifying handoff,
// proving the capture never changes the run's ending.
func TestEvidenceLocalCommitsNotOnRemoteCaptures(t *testing.T) {
	runScenario := func(t *testing.T, armed bool) (int, string, func() []intakeRecord) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithBaseOnly(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'local work\n' > local-work.txt
git add --all -- .
git commit -m 'work: local commit' >/dev/null
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		var snapshot func() []intakeRecord
		if armed {
			intake, snap := newEvidenceIntake(t)
			snapshot = snap
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			armEvidence(t, intake.URL)
		} else {
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String(), snapshot
	}

	code, armedOut, snapshot := runScenario(t, true)
	if code != exitSuccess {
		t.Fatalf("armed run exit code = %d, want %d (Verifying)", code, exitSuccess)
	}
	if !strings.Contains(armedOut, `"phase":"Verifying"`) {
		t.Fatalf("armed run should end verifying: %q", armedOut)
	}

	t.Run("armed captures and delivers", func(t *testing.T) {
		records := snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want 1 for a local commit the remote lacks", len(records))
		}
		rec := records[0]
		if rec.authorization != "Bearer "+testEvidenceToken {
			t.Fatalf("authorization = %q, want Bearer <testEvidenceToken>", rec.authorization)
		}
		if rec.manifest.Trigger != "terminal" {
			t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
		}
		if rec.manifest.Run.Name != "coderrun-it-7" {
			t.Fatalf("manifest run name = %q, want coderrun-it-7", rec.manifest.Run.Name)
		}
		commitEntries := 0
		for name := range rec.archiveFiles {
			if strings.HasPrefix(name, "commits/") {
				commitEntries++
			}
		}
		if commitEntries != 1 {
			t.Fatalf("archive members = %v, want exactly one commits/ entry", rec.archiveFiles)
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		capture := findEvent(t, events, "evidence.capture")
		detail := eventDetail(t, capture)
		if detail["trigger"] != "terminal" {
			t.Fatalf("event trigger = %v, want terminal", detail["trigger"])
		}
		if detail["outcome"] != "delivered" {
			t.Fatalf("event outcome = %v, want delivered", detail["outcome"])
		}
	})

	t.Run("unconfigured leaves the ending unchanged", func(t *testing.T) {
		disarmedCode, disarmedOut, _ := runScenario(t, false)
		if disarmedCode != exitSuccess {
			t.Fatalf("disarmed run exit code = %d, want %d (Verifying)", disarmedCode, exitSuccess)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, disarmedOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with capture unconfigured:\n armed=%v\n disarmed=%v", want, got)
		}
	})
}

// TestEvidenceDeliveryAttempts proves the delivery budget on a failing dirty
// terminal: with capture armed the run makes at most two POST attempts at
// the intake. An intake that never accepts records exactly two attempts and a
// "lost" outcome, and its terminal handoff stays identical to the same run
// with capture unconfigured, so an undeliverable bundle never changes the
// ending. An intake that rejects the first attempt and accepts the second
// records two attempts and a "delivered" outcome.
func TestEvidenceDeliveryAttempts(t *testing.T) {
	// newStatusIntake is newEvidenceIntake with a per-call status code, so a
	// subtest can fail the first attempt or all of them.
	newStatusIntake := func(t *testing.T, statusFor func(call int) int) (*httptest.Server, func() []intakeRecord) {
		t.Helper()
		var mu sync.Mutex
		var records []intakeRecord
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			rec := intakeRecord{
				authorization: r.Header.Get("Authorization"),
				archiveFiles:  map[string]string{},
			}
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, "multipart: "+err.Error(), http.StatusBadRequest)
				return
			}
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					http.Error(w, "multipart part: "+err.Error(), http.StatusBadRequest)
					return
				}
				data, err := io.ReadAll(part)
				if err != nil {
					http.Error(w, "multipart part read: "+err.Error(), http.StatusBadRequest)
					return
				}
				switch part.FormName() {
				case "manifest":
					if err := json.Unmarshal(data, &rec.manifest); err != nil {
						http.Error(w, "manifest: "+err.Error(), http.StatusBadRequest)
						return
					}
				case "archive":
					files, err := readTarGZ(data)
					if err != nil {
						http.Error(w, "archive: "+err.Error(), http.StatusBadRequest)
						return
					}
					rec.archiveFiles = files
				}
			}
			mu.Lock()
			status := statusFor(len(records) + 1)
			records = append(records, rec)
			mu.Unlock()
			w.WriteHeader(status)
		}))
		t.Cleanup(server.Close)
		return server, func() []intakeRecord {
			mu.Lock()
			defer mu.Unlock()
			out := make([]intakeRecord, len(records))
			copy(out, records)
			return out
		}
	}

	// runDirty plays the TestEvidenceDirtyTerminalCaptures scenario — the
	// fake leaves an uncommitted file and declares changes, so the run ends
	// Failed with a dirty worktree. An empty evidenceURL leaves capture
	// unconfigured, the baseline for the handoff comparison.
	runDirty := func(t *testing.T, evidenceURL string) (int, string) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
		t.Setenv("COURIER_REF", "7")
		t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
		t.Setenv("GITHUB_TOKEN", "github-api-token")
		if evidenceURL == "" {
			t.Setenv("COURIER_EVIDENCE_URL", "")
			t.Setenv("COURIER_EVIDENCE_TOKEN", "")
		} else {
			armEvidence(t, evidenceURL)
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String()
	}

	t.Run("exhausted", func(t *testing.T) {
		intake, snapshot := newStatusIntake(t, func(int) int { return http.StatusInternalServerError })
		code, armedOut := runDirty(t, intake.URL)
		if code != exitFailed {
			t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
		}
		if !strings.Contains(armedOut, `"phase":"Failed"`) {
			t.Fatalf("armed run should end failed: %q", armedOut)
		}
		if records := snapshot(); len(records) != 2 {
			t.Fatalf("intake received %d deliveries, want 2 (the delivery budget)", len(records))
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		if n := countEvents(t, events, "evidence.capture"); n != 1 {
			t.Fatalf("evidence.capture events = %d, want 1", n)
		}
		detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
		if detail["outcome"] != "lost" {
			t.Fatalf("event outcome = %v, want lost", detail["outcome"])
		}
		baselineCode, baselineOut := runDirty(t, "")
		if baselineCode != exitFailed {
			t.Fatalf("baseline exit code = %d, want %d", baselineCode, exitFailed)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, baselineOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with an undeliverable bundle:\n armed=%v\n baseline=%v", want, got)
		}
	})

	t.Run("retry_succeeds", func(t *testing.T) {
		intake, snapshot := newStatusIntake(t, func(call int) int {
			if call == 1 {
				return http.StatusInternalServerError
			}
			return http.StatusOK
		})
		code, armedOut := runDirty(t, intake.URL)
		if code != exitFailed {
			t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
		}
		if records := snapshot(); len(records) != 2 {
			t.Fatalf("intake received %d deliveries, want 2 (one rejected, one accepted)", len(records))
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		if n := countEvents(t, events, "evidence.capture"); n != 1 {
			t.Fatalf("evidence.capture events = %d, want 1", n)
		}
		detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
		if detail["outcome"] != "delivered" {
			t.Fatalf("event outcome = %v, want delivered", detail["outcome"])
		}
	})
}

// TestEvidenceDisabledWithoutEnv proves capture is all-or-nothing: with only
// one of the two variables present the failing dirty terminal is exactly as
// silent as with none — no POST reaches the intake, no evidence.capture
// event is emitted, and the terminal handoff equals the evidence-unset
// baseline, so partial configuration behaves like absence.
func TestEvidenceDisabledWithoutEnv(t *testing.T) {
	// runPartial plays the same dirty terminal as TestEvidenceDeliveryAttempts
	// under an arbitrary evidence configuration: an empty argument leaves the
	// variable unset for that run.
	runPartial := func(t *testing.T, evidenceURL, evidenceToken string) (int, string) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
		t.Setenv("COURIER_REF", "7")
		t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
		t.Setenv("GITHUB_TOKEN", "github-api-token")
		t.Setenv("COURIER_EVIDENCE_URL", evidenceURL)
		t.Setenv("COURIER_EVIDENCE_TOKEN", evidenceToken)
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String()
	}

	for _, tc := range []struct {
		name   string
		urlSet bool
		token  string
	}{
		{name: "url_without_token", urlSet: true},
		{name: "token_without_url", token: testEvidenceToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The subtest's intake records any POST the present variable
			// might still provoke; in the absent-URL variant it stands by
			// unused.
			intake, snapshot := newEvidenceIntake(t)
			var url string
			if tc.urlSet {
				url = intake.URL
			}
			code, out := runPartial(t, url, tc.token)
			if code != exitFailed {
				t.Fatalf("run exit code = %d, want %d", code, exitFailed)
			}
			if !strings.Contains(out, `"phase":"Failed"`) {
				t.Fatalf("run should end failed: %q", out)
			}
			noEvidenceCapture(t, snapshot, bytes.NewBufferString(out))
			baselineCode, baselineOut := runPartial(t, "", "")
			if baselineCode != exitFailed {
				t.Fatalf("baseline exit code = %d, want %d", baselineCode, exitFailed)
			}
			noEvidenceCapture(t, snapshot, bytes.NewBufferString(baselineOut))
			want := terminationLineJSON(t, out)
			got := terminationLineJSON(t, baselineOut)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("terminal handoff changed with partial capture configuration:\n partial=%v\n baseline=%v", want, got)
			}
		})
	}
}

// TestEvidenceFailedWorldReadDegradesToManifest proves a failed world read
// degrades the capture to the manifest alone: the fake coordinator declares
// changes and then strips every permission from the workspace's .git tree, so
// the run's post-exit WorkState read fails with the state unknown. The gate
// still finds the moment unrecoverable, but the capture's own git reads fail
// as well, so the run delivers exactly one manifest-only bundle — a "manifest"
// part with no "archive" part, trigger "terminal" — and records a "degraded"
// outcome. The terminal handoff must equal the same run with capture
// unconfigured, so a failed world read ends the run the same way with or
// without the capture.
func TestEvidenceFailedWorldReadDegradesToManifest(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission stripping does not apply to root")
	}
	// delivery is one POST the part-recording intake received: the form-part
	// names in arrival order plus the decoded manifest, so a manifest-only
	// delivery is told apart from one that carries an archive.
	type delivery struct {
		parts    []string
		manifest evidence.Manifest
	}
	newPartsIntake := func(t *testing.T) (*httptest.Server, func() []delivery) {
		t.Helper()
		var mu sync.Mutex
		var records []delivery
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			rec := delivery{parts: []string{}}
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, "multipart: "+err.Error(), http.StatusBadRequest)
				return
			}
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					http.Error(w, "multipart part: "+err.Error(), http.StatusBadRequest)
					return
				}
				rec.parts = append(rec.parts, part.FormName())
				if part.FormName() != "manifest" {
					if _, err := io.Copy(io.Discard, part); err != nil {
						http.Error(w, "multipart part read: "+err.Error(), http.StatusBadRequest)
						return
					}
					continue
				}
				data, err := io.ReadAll(part)
				if err != nil {
					http.Error(w, "multipart part read: "+err.Error(), http.StatusBadRequest)
					return
				}
				if err := json.Unmarshal(data, &rec.manifest); err != nil {
					http.Error(w, "manifest: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
			mu.Lock()
			records = append(records, rec)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)
		return server, func() []delivery {
			mu.Lock()
			defer mu.Unlock()
			out := make([]delivery, len(records))
			for i := range records {
				out[i] = records[i]
				out[i].parts = append([]string(nil), records[i].parts...)
			}
			return out
		}
	}
	// restoreTree puts a stripped .git tree back to directories 0o755 and
	// files 0o644 (u+rwX). The recursive strip leaves .git itself unreadable,
	// so each directory's mode is restored before its entries are read and the
	// repair holds no matter how far the strip got; the temp dir cannot be
	// cleaned up while its .git is still unreadable.
	// filepath.Walk reads each directory before calling its callback, so a
	// stripped directory is never repaired by the callback; restore chmods
	// before reading and recurses by hand.
	var restoreTree func(dir string)
	restoreTree = func(dir string) {
		_ = os.Chmod(dir, 0o755)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			switch {
			case entry.Type()&os.ModeSymlink != 0:
			case entry.IsDir():
				restoreTree(path)
			default:
				_ = os.Chmod(path, 0o644)
			}
		}
	}
	// runScenario plays the failed-world-read scenario: the script writes the
	// outcome declaration to the scratch dir (outside the workspace) first,
	// then strips the .git tree, then exits 0. An armed run points the capture
	// at the part-recording intake; a disarmed one leaves capture
	// unconfigured, the baseline for the handoff comparison.
	runScenario := func(t *testing.T, armed bool) (int, string, func() []delivery) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
chmod -R a-rwx "$COURIER_WORKSPACE/.git"
exit 0
`)
		var snapshot func() []delivery
		if armed {
			intake, snap := newPartsIntake(t)
			snapshot = snap
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			armEvidence(t, intake.URL)
		} else {
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			t.Setenv("COURIER_EVIDENCE_URL", "")
			t.Setenv("COURIER_EVIDENCE_TOKEN", "")
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		// Restore explicitly before the assertions and the temp dir cleanup:
		// defers run last, and a stripped .git would otherwise survive to them.
		gitDir := filepath.Join(root, "workspace", ".git")
		restoreTree(gitDir)
		t.Cleanup(func() { restoreTree(gitDir) })
		return code, output.String(), snapshot
	}

	code, armedOut, snapshot := runScenario(t, true)
	if code != exitFailed {
		t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
	}
	if !strings.Contains(armedOut, `"phase":"Failed"`) {
		t.Fatalf("armed run should end failed: %q", armedOut)
	}

	t.Run("capture degrades to the manifest alone", func(t *testing.T) {
		records := snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want 1 for a failed world read", len(records))
		}
		rec := records[0]
		if len(rec.parts) != 1 || rec.parts[0] != "manifest" {
			t.Fatalf("delivery parts = %v, want the manifest alone (no archive part)", rec.parts)
		}
		if rec.manifest.Trigger != "terminal" {
			t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		if n := countEvents(t, events, "evidence.capture"); n != 1 {
			t.Fatalf("evidence.capture events = %d, want 1", n)
		}
		detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
		if detail["trigger"] != "terminal" {
			t.Fatalf("event trigger = %v, want terminal", detail["trigger"])
		}
		if detail["outcome"] != "degraded" {
			t.Fatalf("event outcome = %v, want degraded", detail["outcome"])
		}
	})

	t.Run("unconfigured leaves the ending unchanged", func(t *testing.T) {
		disarmedCode, disarmedOut, _ := runScenario(t, false)
		if disarmedCode != exitFailed {
			t.Fatalf("disarmed run exit code = %d, want %d", disarmedCode, exitFailed)
		}
		if !strings.Contains(disarmedOut, `"phase":"Failed"`) {
			t.Fatalf("disarmed run should end failed: %q", disarmedOut)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, disarmedOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with capture unconfigured:\n armed=%v\n disarmed=%v", want, got)
		}
	})
}

// TestEvidenceDeadlineStopsAttempts proves the operation deadline bounds the
// delivery loop: with evidenceOperationDeadline shrunk to a second and an
// intake that sleeps past it before answering, the first POST attempt is
// aborted by the deadline and the loop's pre-attempt context check stops a
// second attempt — the intake saw exactly one request start, the run records
// a "lost" outcome, and the terminal handoff is identical to the same run
// with capture unconfigured, so a stalled intake never changes the ending.
func TestEvidenceDeadlineStopsAttempts(t *testing.T) {
	prev := evidenceOperationDeadline
	evidenceOperationDeadline = time.Second
	t.Cleanup(func() { evidenceOperationDeadline = prev })

	// runDirty plays the TestEvidenceDirtyTerminalCaptures scenario — the
	// fake leaves an uncommitted file and declares changes, so the run ends
	// Failed with a dirty worktree. An empty URL leaves capture unconfigured,
	// the baseline for the handoff comparison.
	runDirty := func(t *testing.T, evidenceURL string) (int, string) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
		t.Setenv("COURIER_REF", "7")
		t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
		t.Setenv("GITHUB_TOKEN", "github-api-token")
		if evidenceURL == "" {
			t.Setenv("COURIER_EVIDENCE_URL", "")
			t.Setenv("COURIER_EVIDENCE_TOKEN", "")
		} else {
			armEvidence(t, evidenceURL)
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String()
	}

	// The intake records each request start, then sleeps past the shrunk
	// deadline before answering the first one, so the client aborts the
	// in-flight attempt and the delivery loop's pre-attempt context check
	// must stop the second one.
	var mu sync.Mutex
	var starts int
	intake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		mu.Lock()
		first := starts == 0
		starts++
		mu.Unlock()
		if first {
			time.Sleep(5 * time.Second)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(intake.Close)

	code, armedOut := runDirty(t, intake.URL)
	if code != exitFailed {
		t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
	}
	if !strings.Contains(armedOut, `"phase":"Failed"`) {
		t.Fatalf("armed run should end failed: %q", armedOut)
	}
	mu.Lock()
	startsCount := starts
	mu.Unlock()
	if startsCount != 1 {
		t.Fatalf("intake saw %d request starts, want 1 (the deadline must stop the second attempt)", startsCount)
	}
	events := parseEvents(t, bytes.NewBufferString(armedOut))
	if n := countEvents(t, events, "evidence.capture"); n != 1 {
		t.Fatalf("evidence.capture events = %d, want 1", n)
	}
	detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
	if detail["outcome"] != "lost" {
		t.Fatalf("event outcome = %v, want lost", detail["outcome"])
	}
	baselineCode, baselineOut := runDirty(t, "")
	if baselineCode != exitFailed {
		t.Fatalf("baseline exit code = %d, want %d", baselineCode, exitFailed)
	}
	want := terminationLineJSON(t, armedOut)
	got := terminationLineJSON(t, baselineOut)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("terminal handoff changed with a stalled intake:\n armed=%v\n baseline=%v", want, got)
	}
}

// TestEvidenceSIGTERMKillsProcessGroupAndCaptures proves the pod signal
// paths end to end against a real process, the way the kernel delivers them.
// Both scenarios share one harness: a run whose model child is a shell that
// spawns a long-lived grandchild (sleep) in its own process group and blocks
// on it.
//
// sigterm — the kubelet's SIGTERM cancels the run's NotifyContext, the
// child's Cancel SIGKILLs the whole process group (-pid) so the grandchild
// dies with it, and the run ends Failed (1) with a single
// cancellation-triggered evidence delivery that preserves the uncommitted
// work and a Failed termination file.
//
// sigkill — a SIGKILL to the executor's main process leaves the run no
// cancellation path at all, the second half of issue #198's signal
// done-condition: its own process group (the model child and the grandchild)
// survives the kill. That is the documented loss — the capture is lost with
// the process (the intake receives no request), the termination file is never
// written, and the process state is a death by SIGKILL. The test kills the
// surviving grandchild as cleanup.
//
// The scenarios are subprocess-based rather than in-process because the
// signal must reach a real process: an in-process run() cannot receive a
// SIGTERM or SIGKILL the way the executor binary does. The signals are sent
// with the shell kill builtin rather than syscall.Kill; it delivers the
// identical POSIX signal.
//
// The model child uses a direct-child `sleep 300 &` plus `wait` (not a
// subshell): a subshell would orphan the sleep, let `wait` return, and let
// the run finish before the signal, defeating the point.
func TestEvidenceSIGTERMKillsProcessGroupAndCaptures(t *testing.T) {
	t.Run("sigterm", func(t *testing.T) {
		res := signalKillScenario(t, "TERM", 60*time.Second)

		if res.processState == nil {
			t.Fatalf("no process state after the executor exited")
		}
		if code := res.processState.ExitCode(); code != 1 {
			t.Fatalf("executor exit code = %d, want 1 (Failed); stdout=%q stderr=%q", code, res.out, res.errOut)
		}

		// The intake received exactly one delivery: the bearer token is the one
		// we configured, the trigger is the cancellation path, and the archive
		// preserves the uncommitted file.
		records := res.snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want exactly 1; stdout=%q stderr=%q", len(records), res.out, res.errOut)
		}
		rec := records[0]
		if rec.authorization != "Bearer ev-token-1" {
			t.Fatalf("authorization = %q, want Bearer ev-token-1", rec.authorization)
		}
		if rec.manifest.Trigger != "cancellation" {
			t.Fatalf("manifest trigger = %q, want cancellation", rec.manifest.Trigger)
		}
		if _, ok := rec.archiveFiles["partial.txt"]; !ok {
			t.Fatalf("archive members = %v, want partial.txt", rec.archiveFiles)
		}

		// The group SIGKILL must have reaped the grandchild: probe it with
		// signal 0 for up to ~15s; if it survives, that is a failure.
		deadline := time.Now().Add(15 * time.Second)
		for processRunning(t, res.grandchildPid) {
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if processRunning(t, res.grandchildPid) {
			bestEffortSignal("KILL", res.grandchildPid)
			t.Errorf("process-group kill failed: grandchild %d survived the SIGTERM cascade", res.grandchildPid)
		}

		// The cancellation path ends Failed in the termination file.
		term := mustRead(t, res.termination)
		if !strings.Contains(string(term), `"phase":"Failed"`) {
			t.Fatalf("termination file %q does not contain %q", term, `"phase":"Failed"`)
		}
	})

	t.Run("sigkill", func(t *testing.T) {
		res := signalKillScenario(t, "KILL", 10*time.Second)

		if res.processState == nil {
			t.Fatalf("no process state after the executor exited")
		}
		// The kernel delivered the signal, not the run: the process state is
		// a death by SIGKILL, with no exit code of its own.
		if res.processState.Success() || res.processState.ExitCode() != -1 || !strings.Contains(res.processState.String(), "killed") {
			t.Fatalf("executor process state = %q (exit code %d), want a death by SIGKILL; stdout=%q stderr=%q",
				res.processState.String(), res.processState.ExitCode(), res.out, res.errOut)
		}

		// The run never ran its cancellation path, so the capture is lost
		// with the process: the intake received no request at all.
		if records := res.snapshot(); len(records) != 0 {
			t.Fatalf("intake received %d deliveries, want 0 (the capture is lost with the process); stdout=%q stderr=%q", len(records), res.out, res.errOut)
		}

		// The run never reached its terminal handoff: the termination file is
		// unwritten (or empty if the handoff began but never completed).
		term, err := os.ReadFile(res.termination)
		if err == nil && len(term) != 0 {
			t.Fatalf("termination file holds %d bytes, want absent or empty: %q", len(term), term)
		}

		// The model child runs in its own process group, which a SIGKILL to
		// the main process cannot reach: the grandchild is expected alive
		// right after the kill. That survival is the documented loss; the
		// test kills it as cleanup and re-kills any survivor at the end.
		if !processRunning(t, res.grandchildPid) {
			t.Fatalf("grandchild %d is not alive right after the SIGKILL; want it alive (the documented loss)", res.grandchildPid)
		}
		bestEffortSignal("KILL", res.grandchildPid)
		t.Cleanup(func() { bestEffortSignal("KILL", res.grandchildPid) })
	})
}

// signalKillResult carries one real-process signal scenario's outcome for
// the per-signal assertions.
type signalKillResult struct {
	out           string
	errOut        string
	processState  *os.ProcessState
	snapshot      func() []intakeRecord
	grandchildPid int
	termination   string
}

// signalKillScenario runs one real-process signal scenario: it builds the
// executor into the scratch temp dir, stages a run whose model child spawns a
// long-lived grandchild (sleep) in its own process group and blocks on it,
// starts the executor as a subprocess, waits for the grandchild pid file and
// the dirty work to prove the run reached the model child and is blocked,
// sends the named signal to the executor's main process, and waits at most
// waitLimit for it to exit. Anything that outlives the scenario is killed in
// a cleanup.
func signalKillScenario(t *testing.T, sig string, waitLimit time.Duration) signalKillResult {
	t.Helper()
	root := t.TempDir()

	// 1. Build the executor once, into the scratch temp dir (never into the
	//    repo). Skip when the toolchain is unavailable or the build fails.
	execBin := filepath.Join(root, "executor")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", execBin, "./cmd/courier-executor")
	_, thisFile, _, _ := runtime.Caller(0)
	build.Dir = filepath.Join(filepath.Dir(thisFile), "..", "..")
	build.Env = withGoBin(os.Environ())
	var buildOut bytes.Buffer
	build.Stdout = &buildOut
	build.Stderr = &buildOut
	if err := build.Run(); err != nil {
		t.Skipf("cannot build the executor for a subprocess test, skipping: %v (%s)", err, buildOut.String())
	}

	// 2. Scenario layout, all under the scratch temp dir.
	remote := remoteWithExistingBranch(t, root)
	workspace := filepath.Join(root, "workspace")
	scratch := filepath.Join(root, "scratch")
	termination := filepath.Join(root, "termination")
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered\n' > partial.txt
mkdir -p "$COURIER_SCRATCH_DIR"
sleep 300 &
echo "$!" > "$COURIER_SCRATCH_DIR/grandchild.pid"
wait
`)

	// The live intake the subprocess POSTs to: records the delivery count, the
	// Authorization header, the decoded manifest, and the archive members.
	intake, snapshot := newEvidenceIntake(t)

	// 3. The subprocess environment: the test process environment (PATH,
	//    GIT_CONFIG_GLOBAL, ...) minus any live COURIER_/GITHUB settings, plus
	//    exactly the values setResolveIssueEnv writes, the evidence endpoint,
	//    and a per-incarnation token.
	env := subprocessEnv(os.Environ(), map[string]string{
		"COURIER_REPO_URL":         remote,
		"COURIER_WORKSPACE":        workspace,
		"COURIER_SCRATCH_DIR":      scratch,
		"COURIER_BASE":             "main",
		"COURIER_BRANCH":           "courier/resolve-issue/acme-widgets/7",
		"COURIER_GOAL":             "Open a PR to address issue #7.",
		"COURIER_MODEL":            "any-model/name",
		"COURIER_MODE":             "resolve-issue",
		"COURIER_REPO":             "acme/widgets",
		"COURIER_GITHUB_API_BASE":  fakeGitHubAPI(t).URL,
		"COURIER_OPENCODE_BINARY":  fakeOpenCode,
		"COURIER_TERMINATION_FILE": termination,
		"COURIER_REF":              "7",
		"COURIER_RUN_NAME":         "coderrun-it-7",
		"GITHUB_TOKEN":             "github-api-token",
		"COURIER_EVIDENCE_URL":     intake.URL,
		"COURIER_EVIDENCE_TOKEN":   "ev-token-1",
	})

	// 4. Start the executor as a real subprocess and drain its streams into
	//    bounded buffers. Do not Wait yet.
	var out, errOut bytes.Buffer
	cmd := exec.Command(execBin)
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("start executor subprocess: %v", err)
	}

	var grandchildPid int
	t.Cleanup(func() {
		// Reap anything that outlives the test; killing an already-dead
		// process is a harmless no-op.
		_ = cmd.Process.Kill()
		if grandchildPid > 0 {
			bestEffortSignal("KILL", grandchildPid)
		}
	})

	// 5. Poll up to ~20s for the grandchild to appear and the dirty work to be
	//    written, proving the run reached the model-child launch and is blocked.
	partialPath := filepath.Join(workspace, "partial.txt")
	gcPidPath := filepath.Join(scratch, "grandchild.pid")
	end := time.Now().Add(20 * time.Second)
	for {
		_, pidErr := os.Stat(gcPidPath)
		_, partErr := os.Stat(partialPath)
		if pidErr == nil && partErr == nil {
			break
		}
		if time.Now().After(end) {
			_ = cmd.Process.Kill()
			t.Fatalf("did not observe the grandchild pid and dirty work within 20s; stdout=%q stderr=%q", out.String(), errOut.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	rawPid := mustRead(t, gcPidPath)
	if _, err := fmt.Sscanf(string(rawPid), "%d", &grandchildPid); err != nil || grandchildPid <= 0 {
		t.Fatalf("grandchild.pid %q is not a positive pid: %v", rawPid, err)
	}

	// 6. Send the named signal to the executor's main process, the way the
	//    kubelet (TERM) or the kernel (KILL) would. Any group kill that
	//    follows is the executor's own doing (what the sigterm scenario
	//    tests).
	if err := sendSignal(t, sig, cmd.Process.Pid); err != nil {
		t.Fatalf("send %s to the executor: %v", sig, err)
	}

	// 7. Wait for the subprocess to finish, with the scenario's cap.
	done := make(chan struct{}, 1)
	go func() {
		_ = cmd.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(waitLimit):
		_ = cmd.Process.Kill()
		t.Fatalf("executor did not exit within %s of %s; stdout=%q stderr=%q", waitLimit, sig, out.String(), errOut.String())
	}
	return signalKillResult{
		out:           out.String(),
		errOut:        errOut.String(),
		processState:  cmd.ProcessState,
		snapshot:      snapshot,
		grandchildPid: grandchildPid,
		termination:   termination,
	}
}

// withGoBin returns env with the scratch Go toolchain's bin directory placed
// at the front of PATH so `go` resolves for the build step even when the test
// process was started without it.
func withGoBin(env []string) []string {
	const bin = "/var/tmp/courier-scratch/go/bin"
	out := make([]string, 0, len(env)+1)
	path := ""
	for _, kv := range env {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if name == "PATH" {
			path = value
			continue
		}
		out = append(out, kv)
	}
	out = append(out, "PATH="+bin+string(os.PathListSeparator)+path)
	return out
}

// subprocessEnv returns the environment for the executor subprocess: the
// caller's environment with any live COURIER_* and GITHUB_TOKEN settings
// removed, and the provided key/value pairs appended. The test's own run
// configuration (mirroring setResolveIssueEnv) is the only source of the
// subprocess's COURIER_* values, so a live pod's settings cannot leak in.
func subprocessEnv(base []string, set map[string]string) []string {
	out := make([]string, 0, len(base)+len(set))
	for _, kv := range base {
		name, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if strings.HasPrefix(name, "COURIER_") || name == "GITHUB_TOKEN" {
			continue
		}
		out = append(out, kv)
	}
	for name, value := range set {
		out = append(out, name+"="+value)
	}
	return out
}

// sendSignal sends a named POSIX signal (e.g. "TERM", "KILL") to pid via the
// shell kill builtin and returns an error if it cannot be delivered. The test
// process is the executor's parent and shares the grandchild's owner, so
// it reaches the same targets syscall.Kill would.
func sendSignal(t *testing.T, sig string, pid int) error {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("kill -%s %d", sig, pid))
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kill -%s %d: %v (%s)", sig, pid, err, buf.String())
	}
	return nil
}

// bestEffortSignal sends a named signal to pid, ignoring a delivery error. It
// is used only for cleanup, where the target may already be gone.
func bestEffortSignal(sig string, pid int) {
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("kill -%s %d", sig, pid))
	_ = cmd.Run()
}

// processRunning reports whether pid names a genuinely running process: its
// /proc/<pid>/stat entry exists and its state is not a zombie. A zombie is dead
// (killed) even though its pid lingers until it is reaped, and a signal-0
// probe cannot tell a zombie from a live process.
func processRunning(t *testing.T, pid int) bool {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	s := string(b)
	idx := len(s) - 1
	for idx > 0 && s[idx] != ')' {
		idx--
	}
	// The comm sits in the first parentheses; the last ")" in the line closes it.
	// After it the fields are: state ppid pgrp ...
	f := strings.Fields(s[idx+2:])
	return len(f) > 0 && f[0] != "Z"
}

// TestEvidenceCrashCapturePrecedesResume proves the crash path's capture
// ordering (issue #198's done-condition 4): a run whose first turn crashes
// dirty — the model child reports a session id, leaves an uncommitted file,
// and exits 1 — resumes once, and the resumed turn declares changes against
// the same dirty workspace, which classify resolves as a Failed terminal.
// The crash-triggered capture is synchronous: it completes before the
// resume decision, so in the run's stdout event order the evidence.capture
// (crash) event line appears before the second executor.start
// (resumed:true) — pinning capture-before-resume, and the same-bearer pair
// of deliveries pins same-incarnation duplicate replacement. The resume
// backoff is zero: the order, not the timing, is what is pinned.
func TestEvidenceCrashCapturePrecedesResume(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	// The scratch marker tells the fake which turn it is on: turn 1 (no
	// marker) creates the marker, reports the session id, leaves the dirty
	// work, and crashes; turn 2 (marker present) leaves the work dirty
	// still and declares changes, which classify resolves as a Failed
	// terminal because the workspace is still dirty.
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
if [ -e "$COURIER_SCRATCH_DIR/crash-marker" ]; then
  mkdir -p "$COURIER_SCRATCH_DIR"
  printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
  exit 0
fi
mkdir -p "$COURIER_SCRATCH_DIR"
touch "$COURIER_SCRATCH_DIR/crash-marker"
printf '{"sessionID":"s1"}\n'
printf 'dirty\n' > partial.txt
exit 1
`)
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	// Zero backoff for speed: the order, not the timing, is what is pinned.
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	// Debug so the executor.start and executor.continuation payloads (the
	// resumed flag and the crash kind) are present for the order assertion.
	t.Setenv("COURIER_LOG_LEVEL", "debug")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("run should end failed: %q", output.String())
	}

	// Exactly two deliveries, both authenticated with the same bearer: the
	// first is the crash capture, the second the resumed turn's terminal
	// capture.
	records := snapshot()
	if len(records) != 2 {
		t.Fatalf("intake received %d deliveries, want 2 (crash, then terminal); stdout=%q", len(records), output.String())
	}
	for i, rec := range records {
		if rec.authorization != "Bearer "+testEvidenceToken {
			t.Fatalf("delivery %d authorization = %q, want Bearer <testEvidenceToken>", i, rec.authorization)
		}
	}
	if records[0].manifest.Trigger != "crash" {
		t.Fatalf("first manifest trigger = %q, want crash", records[0].manifest.Trigger)
	}
	if records[1].manifest.Trigger != "terminal" {
		t.Fatalf("second manifest trigger = %q, want terminal", records[1].manifest.Trigger)
	}

	events := parseEvents(t, &output)
	continuationIdx := -1
	var captureIdxs, startIdxs []int
	for i, ev := range events {
		switch ev["event"] {
		case "executor.continuation":
			if detail, ok := ev["detail"].(map[string]any); ok && detail["kind"] == "crash" && continuationIdx < 0 {
				continuationIdx = i
			}
		case "executor.start":
			startIdxs = append(startIdxs, i)
		case "evidence.capture":
			captureIdxs = append(captureIdxs, i)
		}
	}
	if continuationIdx < 0 {
		t.Fatalf("no executor.continuation event of kind crash; stdout=%q", output.String())
	}
	if len(startIdxs) != 2 {
		t.Fatalf("executor.start events = %d, want 2 (initial, then resumed); stdout=%q", len(startIdxs), output.String())
	}
	if len(captureIdxs) != 2 {
		t.Fatalf("evidence.capture events = %d, want 2 (crash, then terminal); stdout=%q", len(captureIdxs), output.String())
	}
	resumedStart := startIdxs[1]
	resumedDetail, _ := events[resumedStart]["detail"].(map[string]any)
	if resumedDetail["resumed"] != true {
		t.Fatalf("second executor.start resumed = %v, want true", events[resumedStart]["detail"])
	}
	// Inspection and capture happen before the resume decision completes: the
	// crash continuation, then its capture, then the resumed turn.
	if !(continuationIdx < captureIdxs[0] && captureIdxs[0] < resumedStart) {
		t.Fatalf("event order continuation=%d, first capture=%d, resumed start=%d; want continuation < capture < resumed start", continuationIdx, captureIdxs[0], resumedStart)
	}
	for i, idx := range captureIdxs {
		detail := eventDetail(t, events[idx])
		want := "crash"
		if i == 1 {
			want = "terminal"
		}
		if detail["trigger"] != want {
			t.Fatalf("evidence.capture %d trigger = %v, want %s", i, detail["trigger"], want)
		}
		if detail["outcome"] != "delivered" {
			t.Fatalf("evidence.capture %d outcome = %v, want delivered", i, detail["outcome"])
		}
	}
}

// TestEvidenceDeadlineExpiredCaptureDeliversManifestOnly pins DESIGN.md's
// degradation guarantee at the operation deadline: when the 20-second capture
// bound itself expires before the snapshot lands, the manifest alone must
// still reach the intake inside a fresh short window, recorded as a degraded
// outcome. The operation deadline is shrunk to a millisecond so the capture's
// first git call always loses the race; the terminal ending is unchanged.
func TestEvidenceDeadlineExpiredCaptureDeliversManifestOnly(t *testing.T) {
	prev := evidenceOperationDeadline
	evidenceOperationDeadline = time.Millisecond
	t.Cleanup(func() { evidenceOperationDeadline = prev })

	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitFailed {
		t.Fatalf("exit code = %d, want %d", code, exitFailed)
	}

	records := snapshot()
	if len(records) != 1 {
		t.Fatalf("intake received %d deliveries, want the manifest-only degraded bundle", len(records))
	}
	rec := records[0]
	if rec.manifest.Trigger != "terminal" {
		t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
	}
	if len(rec.archiveFiles) != 0 {
		t.Fatalf("degraded delivery carried archive members %v, want the manifest alone", rec.archiveFiles)
	}
	events := parseEvents(t, bytes.NewBufferString(output.String()))
	detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
	if detail["outcome"] != "degraded" {
		t.Fatalf("event outcome = %v, want degraded", detail["outcome"])
	}
}
