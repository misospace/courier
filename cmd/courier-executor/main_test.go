package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/misospace/courier/internal/broker"
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

// The shared termination-line constants live in internal/executor so the
// executor and the native control producer cannot drift (#210). The
// local aliases here keep the existing test bodies readable without
// re-declaring the values.
const (
	terminationTruncationSuffix = executor.TerminationTruncationSuffix
	terminationLinePrefix       = executor.TerminationLinePrefix
	maxTerminationLineBytes     = executor.MaxTerminationLineBytes
)

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
		_, _ = io.WriteString(w, pullRequestFixtureJSON(12, "open", false, "Closes #7."))
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
		_, _ = io.WriteString(w, pullRequestFixtureJSON(12, "closed", false, "Closes #7."))
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
		_, _ = io.WriteString(w, "["+pullRequestFixtureObjectJSON(11, "closed", false, "Closes #7.")+","+pullRequestFixtureObjectJSON(12, "open", false, "Closes #7.")+"]")
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

// TestRunResolveIssueAdoptionGuardFailClosedWorlds pins the #343 rule at the
// adoption guard: only an open, non-draft PR whose full identity and base ref
// match the run may be reported for review. A draft, a PR re-targeted to
// another base, a same-owner fork claiming the branch selector, and an
// ambiguous set of open PRs all terminate NeedsHuman with the precise state
// in the reason, before Prepare and before any executor turn.
func TestRunResolveIssueAdoptionGuardFailClosedWorlds(t *testing.T) {
	cases := []struct {
		name         string
		pullRequests string
		wantReason   string
	}{
		{
			name:         "draft PR is not reviewable",
			pullRequests: pullRequestFixtureJSON(12, "open", true, "Closes #7."),
			wantReason:   "resolve branch already has draft PR #12",
		},
		{
			name:         "wrong-base PR settles nothing",
			pullRequests: "[" + pullRequestFixtureObjectJSONWithBase(12, "open", false, "release-9", "Closes #7.") + "]",
			wantReason:   "targets a base other than the pinned base",
		},
		{
			name:         "foreign head is never adopted",
			pullRequests: "[" + pullRequestFixtureObjectJSONWithHeadRepo(12, "open", false, "fork/widgets", "Closes #7.") + "]",
			wantReason:   "from a different repository",
		},
		{
			name:         "ambiguous open PRs settle nothing",
			pullRequests: "[" + pullRequestFixtureObjectJSON(12, "open", false, "Closes #7.") + "," + pullRequestFixtureObjectJSON(13, "open", false, "Closes #7.") + "]",
			wantReason:   "more than one open pull request",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			remote := remoteWithExistingBranch(t, root)

			// A binary that fails loudly if it is ever executed: reaching it
			// would mean the run adopted the branch despite the existing PR.
			fakeOpenCode := filepath.Join(root, "opencode")
			writeExecutable(t, fakeOpenCode, "#!/bin/sh\nexit 99\n")

			prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.pullRequests)
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
			if !strings.Contains(output.String(), tc.wantReason) {
				t.Fatalf("termination reason should name the actual state %q: %q", tc.wantReason, output.String())
			}
			if strings.Contains(output.String(), `"phase":"Verifying"`) {
				t.Fatalf("fail-closed world must not be reported for review: %q", output.String())
			}
			for _, event := range parseEvents(t, &output) {
				if name, _ := event["event"].(string); name == "workspace.ready" || name == "executor.start" {
					t.Fatalf("run reached %s despite a fail-closed existing PR: %v", name, event)
				}
			}
		})
	}
}

// TestRunResolveIssueAdoptionGuardRejectsUnidentifiedPullRequest proves the
// guard reads through the identity-checked forge capability rather than a
// raw head listing: a record claiming the branch ref without base- or
// head-repository identity can no longer be reported for review, and the
// unreadable world fails closed instead of adopting.
func TestRunResolveIssueAdoptionGuardRejectsUnidentifiedPullRequest(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

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
	if code != 1 {
		t.Fatalf("run exit code = %d, want 1 (Failed); stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("missing Failed termination: %q", output.String())
	}
	if !strings.Contains(output.String(), "missing the base repository identity") {
		t.Fatalf("termination should name the identity-check failure: %q", output.String())
	}
	for _, event := range parseEvents(t, &output) {
		if name, _ := event["event"].(string); name == "workspace.ready" || name == "executor.start" {
			t.Fatalf("run reached %s despite an unverifiable PR record: %v", name, event)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	if got := terminationLineJSON(t, output.String())["category"]; got != terminalCategoryNeedsDecision {
		t.Fatalf("termination category = %v, want %q", got, terminalCategoryNeedsDecision)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	if got := terminationLineJSON(t, output.String())["category"]; got != terminalCategoryVerifierRejection {
		t.Fatalf("termination category = %v, want %q", got, terminalCategoryVerifierRejection)
	}
}

func TestRunInvalidOutcomeDeclarationBecomesFailed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf 'not json' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
// with. The cut itself is the shared executor.ShrinkTerminationReason helper
// that every COURIER_TERMINATION producer uses for the reason fit, so this
// test pins the shared behavior, not a private copy (#210).
func TestShrinkTerminationReason(t *testing.T) {
	t.Run("closes a one-byte overflow", func(t *testing.T) {
		reason := strings.Repeat("a", 100)
		got := executor.ShrinkTerminationReason(reason, 1)
		want := strings.Repeat("a", 84) + terminationTruncationSuffix
		if got != want {
			t.Fatalf("shrinkTerminationReason() = %q (%d bytes), want %q (%d bytes)", got, len(got), want, len(want))
		}
	})

	t.Run("reason at the marker floor is unchanged", func(t *testing.T) {
		reason := strings.Repeat("a", len(terminationTruncationSuffix))
		got := executor.ShrinkTerminationReason(reason, 1)
		if got != reason {
			t.Fatalf("shrinkTerminationReason() = %q, want unchanged %q", got, reason)
		}
	})

	t.Run("content ending in the marker is not double-counted", func(t *testing.T) {
		reason := strings.Repeat("a", 50) + terminationTruncationSuffix
		got := executor.ShrinkTerminationReason(reason, 1)
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
	prServer := fakeLinkedPRServer(t)
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
	if got := terminationLineJSON(t, output.String())["category"]; got != terminalCategoryModelTransportError {
		t.Fatalf("termination category = %v, want %q", got, terminalCategoryModelTransportError)
	}
}

// TestRunChildExitThreeHandsOffRecoverably is the symmetric guard for the
// no_change_needed exit under the availability policy (#327 Scenario D): a
// child that exits 3 without a declaration or a session burns its availability
// budget and ends on the recoverable handoff, never with the no-change-needed
// termination and never as a permanent Failed.
func TestRunChildExitThreeHandsOffRecoverably(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nexit 3\n")
	prServer := fakeLinkedPRServer(t)
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_AVAILABILITY_RETRIES", "1")
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (recoverable handoff); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"capacity_exhausted"`) || !strings.Contains(output.String(), `"category":"provider-unavailable"`) {
		t.Fatalf("child-exit-3 handoff = %q", output.String())
	}
	if !strings.Contains(output.String(), "last exit 3") {
		t.Fatalf("child-exit-3 reason should keep the child's exit code: %q", output.String())
	}
	if strings.Contains(output.String(), `"phase":"NoChangeNeeded"`) {
		t.Fatalf("child exit 3 must never end the run with the no_change_needed exit: %q", output.String())
	}
	if strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("an unavailable provider must not permanently fail the work: %q", output.String())
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
	prServer, prState := fakeForgePullRequestServer(t)
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
  git push origin "$COURIER_BRANCH" >/dev/null 2>&1
`+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The resumed turn opened the pull request."))+`  exit 0
fi
printf '{"type":"text","sessionID":"ses_resume1","part":{"type":"text","text":"partial work"}}\n'
printf 'partial\n' > partial.txt
exit 0
`)
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
	prServer, prState := fakeForgePullRequestServer(t)
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
  git push origin "$COURIER_BRANCH" >/dev/null 2>&1
`+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The resumed turn opened the pull request."))+`  exit 0
fi
printf '{"type":"text","sessionID":"ses_nowork1","part":{"type":"text","text":"nothing yet"}}\n'
exit 0
`)
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
	prServer, prState := fakeForgePullRequestServer(t)
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
SESSION=
for a in "$@"; do
  case "$a" in --session) SESSION=1;; esac
done
if [ -n "$SESSION" ]; then
  git branch -f "$COURIER_BRANCH" HEAD
  git push origin "HEAD:refs/heads/$COURIER_BRANCH" >/dev/null 2>&1
`+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The resumed turn opened the pull request."))+`  exit 0
fi
git checkout -b side/branch
printf 'side\n' > side.txt
git add --all -- .
git commit -m 'test: work on a side branch' >/dev/null
printf '{"type":"text","sessionID":"ses_offbranch1","part":{"type":"text","text":"committed off branch"}}\n'
exit 0
`)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer := fakeLinkedPRServer(t)
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
	prServer, prState := fakeForgePullRequestServer(t)
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
  git push origin "$COURIER_BRANCH" >/dev/null 2>&1
`+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The resumed turn opened the pull request."))+`  exit 0
fi
printf '{"type":"text","sessionID":"ses_crash1","part":{"type":"text","text":"about to crash"}}\n'
exit 1
`)
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

// TestRunCrashHandsOffRecoverablyAfterBudget proves a run that keeps
// crashing consumes the continuation budget and then ends on the recoverable
// availability handoff — reporting the number of continuations — instead of
// terminalizing as Failed and stranding the claim (#327).
func TestRunCrashHandsOffRecoverablyAfterBudget(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
printf '{"type":"text","sessionID":"ses_crashfail","part":{"type":"text","text":"crashing again"}}\n'
exit 1
`)
	prServer := fakeLinkedPRServer(t)
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_CONTINUATIONS", "2")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (recoverable handoff); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	out := output.String()
	if !strings.Contains(out, `"outcome":"capacity_exhausted"`) || !strings.Contains(out, `"category":"provider-unavailable"`) {
		t.Fatalf("missing recoverable availability handoff: %q", out)
	}
	if strings.Contains(out, `"phase":"Failed"`) {
		t.Fatalf("a crashed child must not strand the work as Failed: %q", out)
	}
	if !strings.Contains(out, "opencode exited with status 1 after 2 continuations") {
		t.Fatalf("handoff reason = %q, want the budget-exhausted detail", out)
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
	prServer, prState := fakeForgePullRequestServer(t)
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
  git push origin "$COURIER_BRANCH" >/dev/null 2>&1
`+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The resumed turn opened the pull request."))+`  exit 0
fi
printf '{"type":"text","sessionID":"ses_cap1","part":{"type":"text","text":"nothing yet"}}\n'
exit 0
`)
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

// TestRunCrashWithoutSessionRetriesThenHandsOffRecoverably proves a provider
// outage (#327 Scenario D): a child that exits with a crash code before any
// sessionID burns the dedicated availability budget with backoff events, then
// ends on the recoverable capacity-handoff channel — exit 2 with the
// capacity_exhausted outcome and the honest provider-unavailable category —
// instead of terminalizing as Failed. Work released this way is claimable by
// another capacity; it is never a task verdict, and the task-repair
// continuation budget is untouched.
func TestRunCrashWithoutSessionRetriesThenHandsOffRecoverably(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nexit 3\n")
	prServer := fakeLinkedPRServer(t)
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_AVAILABILITY_RETRIES", "2")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (recoverable handoff); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	out := output.String()
	if !strings.Contains(out, `"outcome":"capacity_exhausted"`) || !strings.Contains(out, `"category":"provider-unavailable"`) {
		t.Fatalf("missing recoverable availability handoff: %q", out)
	}
	if strings.Contains(out, `"phase":"Failed"`) {
		t.Fatalf("outage must not terminalize as Failed: %q", out)
	}
	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 2 {
		t.Fatalf("executor.continuation count = %d, want 2 availability retries", n)
	}
	for _, event := range events {
		if event["event"] != "executor.continuation" {
			continue
		}
		if d := eventDetail(t, event); d["kind"] != "availability-retry" {
			t.Fatalf("continuation kind = %v, want availability-retry", d["kind"])
		}
	}
}

// TestRunAvailabilityBudgetDoesNotConsumeTaskContinuations proves the two
// budgets are separate: repeated no-session crashes (an outage) must not
// drain the task-repair continuation budget, so a provider that recovers
// mid-run still gets its full repair allowance for real work states.
func TestRunAvailabilityBudgetDoesNotConsumeTaskContinuations(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
if [ "$(wc -l < "$COURIER_FAKE_STATE/argv.log" | tr -d ' ')" -le 2 ]; then exit 3; fi
printf '{"type":"text","sessionID":"ses_attempt_%s","part":{"type":"text","text":"back"}}\n' "$(wc -l < "$COURIER_FAKE_STATE/argv.log" | tr -d ' ')"
exit 1
`)
	prServer := fakeLinkedPRServer(t)
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_AVAILABILITY_RETRIES", "5")
	t.Setenv("COURIER_MAX_CONTINUATIONS", "1")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (recoverable handoff after the task budget); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	out := output.String()
	if !strings.Contains(out, "after 1 continuations") || !strings.Contains(out, `"category":"provider-unavailable"`) {
		t.Fatalf("terminal handoff missing the task-budget detail: %q", out)
	}
	// Budget separation: the two no-session crashes rode the availability
	// budget; only the post-recovery crash consumed a task continuation.
	events := parseEvents(t, &output)
	var crash, avail int
	for _, event := range events {
		if event["event"] != "executor.continuation" {
			continue
		}
		switch kind := eventDetail(t, event)["kind"]; kind {
		case "crash":
			crash++
		case "availability-retry":
			avail++
		}
	}
	if crash != 1 || avail != 2 {
		t.Fatalf("crash continuations = %d (want 1), availability retries = %d (want 2)", crash, avail)
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

	// The branch exists on the remote and is orphaned until the model
	// commits and opens the run-branch pull request (#247): the adoption
	// guard must see no pull requests and let the run proceed, and the
	// pre-handoff observation must see the opened PR.
	prServer, prState := fakeForgePullRequestServer(t)

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'opencode ran\\n'\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\ngit push origin \"$COURIER_BRANCH\" >/dev/null 2>&1\n"+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The completed turn opened the pull request.")))

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
	if got := terminationLineJSON(t, output.String())["category"]; got != terminalCategoryExecutorCrash {
		t.Fatalf("termination category = %v, want %q", got, terminalCategoryExecutorCrash)
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

	prServer, prState := fakeForgePullRequestServer(t)

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
git push origin "$COURIER_BRANCH" >/dev/null 2>&1
`, fragment1, fragment2, testGitHubTokn) + writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The completed turn opened the pull request."))
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
// redaction on an abnormally ending child under the availability policy: the
// run ends on the recoverable handoff (the child's code rides in the handoff
// reason) and the child's final output is flushed through the redactor before
// the termination handoff.
func TestChildOutputRedactedOnFailureExit(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	prServer := fakeLinkedPRServer(t)

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
	t.Setenv("COURIER_MAX_AVAILABILITY_RETRIES", "1")
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (recoverable availability handoff); stderr=%q", code, exitNeedsHuman, errorsOut.String())
	}
	if !strings.Contains(output.String(), "last exit 3") {
		t.Fatalf("handoff reason should keep the child's exit code: %q", output.String())
	}
	if strings.Contains(output.String(), testGitToken) {
		t.Fatal("stdout contains a fake credential on the failure path (constant: testGitToken)")
	}
	if !strings.Contains(output.String(), "failing run token") || !strings.Contains(output.String(), courierlog.RedactedPlaceholder) {
		t.Fatal("failure-path stdout lost ordinary output or the redaction placeholder")
	}
	if !strings.Contains(output.String(), `"category":"provider-unavailable"`) {
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

	prServer := fakeLinkedPRServer(t)

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

	prServer := fakeLinkedPRServer(t)

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

// fakeLinkedPRServer stands up the minimal httptest fixture for resolve-issue
// tests that never reach the Verifying handoff (loop-guard, crash, and
// no-work endings) or that run without a trusted source identity, where the
// pre-handoff forge observation is skipped. It answers every request with
// no pull requests: adoption therefore proceeds, and a run that ends
// Verifying would find a confirmed missing PR (#247) rather than a permitted
// one. Tests that end Verifying script the run-branch pull request with
// fakeForgePullRequestServer instead.
func fakeLinkedPRServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/comments") {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, `{"id":1}`)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(server.Close)
	return server
}

// fakeForgePullRequestServer stands up the stateful GitHub pulls-list
// fixture for the pre-handoff observation tests (#247, #248). Every
// request re-reads the state file: while it is absent the run branch
// carries no pull request; when it holds a JSON array the server answers
// the head query with it; the literal "500" makes the forge read fail.
// Posted issue comments are appended to comments.jsonl beside the state
// file (see readForgeComments) and a comment-fail marker file beside it
// makes every comment return HTTP 500 (#282).
// Tests script the world by writing the file before the run, and a fake
// OpenCode script can "open", "ready", or break the pull request mid-run
// by rewriting it, so the executor's live observations see a forge that
// changes across continuations exactly as a real one would.
func fakeForgePullRequestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pulls.json")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/comments") {
			body, _ := io.ReadAll(r.Body)
			// The "comment-fail" marker file (written by a test before
			// the run) makes every comment a forge failure, so a test
			// can pin that a comment failure never changes the run's
			// ending (#282). Otherwise the comment is appended to
			// comments.jsonl for readForgeComments.
			if _, err := os.Stat(filepath.Join(dir, "comment-fail")); err == nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			f, err := os.OpenFile(filepath.Join(dir, "comments.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = f.Write(append(body, '\n'))
			_ = f.Close()
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":1}`)
			return
		}
		data, err := os.ReadFile(statePath)
		w.Header().Set("Content-Type", "application/json")
		if err != nil || len(bytes.TrimSpace(data)) == 0 {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		if string(bytes.TrimSpace(data)) == "500" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(server.Close)
	return server, statePath
}

// pullRequestFixtureObjectJSON renders one pull request on the run branch
// in the shape the identity-checked forge read requires: full base- and
// head-repository identity naming acme/widgets, matching the run's pinned
// repository and branch. Bodies must avoid apostrophes so the fixture
// survives single-quoted shell embedding in the fake OpenCode scripts.
func pullRequestFixtureObjectJSON(number int, state string, draft bool, body string) string {
	return fmt.Sprintf(`{"number":%d,"state":%q,"draft":%t,"title":"Work","base":{"ref":"main","repo":{"full_name":"acme/widgets"}},"head":{"ref":"courier/resolve-issue/acme-widgets/7","sha":"deadbeef","repo":{"full_name":"acme/widgets"}},"body":%q}`, number, state, draft, body)
}

// pullRequestFixtureJSON wraps a single fixture object in the array the
// pulls-list endpoint returns.
func pullRequestFixtureJSON(number int, state string, draft bool, body string) string {
	return "[" + pullRequestFixtureObjectJSON(number, state, draft, body) + "]"
}

// pullRequestFixtureObjectJSONWithBase renders one pull request whose head
// identity matches the run but whose BASE ref differs from the pinned base,
// the wrong-base world the executor must fail closed on (#247).
func pullRequestFixtureObjectJSONWithBase(number int, state string, draft bool, baseRef, body string) string {
	return fmt.Sprintf(`{"number":%d,"state":%q,"draft":%t,"title":"Work","base":{"ref":%q,"repo":{"full_name":"acme/widgets"}},"head":{"ref":"courier/resolve-issue/acme-widgets/7","sha":"deadbeef","repo":{"full_name":"acme/widgets"}},"body":%q}`, number, state, draft, baseRef, body)
}

// pullRequestFixtureObjectJSONWithHeadRepo renders one pull request whose
// head REPOSITORY differs from the run's own — a same-owner fork claiming
// the branch selector — the foreign world the executor must fail closed on
// rather than adopt or delete toward a missing-PR claim (#247).
func pullRequestFixtureObjectJSONWithHeadRepo(number int, state string, draft bool, headRepo, body string) string {
	return fmt.Sprintf(`{"number":%d,"state":%q,"draft":%t,"title":"Work","base":{"ref":"main","repo":{"full_name":"acme/widgets"}},"head":{"ref":"courier/resolve-issue/acme-widgets/7","sha":"deadbeef","repo":{"full_name":%q}},"body":%q}`, number, state, draft, headRepo, body)
}

// writePullRequestScript is the shell line a fake OpenCode script uses to
// stand in for the model opening or readying the run-branch pull request
// through the forge capability: it writes the fixture JSON to the state
// file the fake forge server answers from.
func writePullRequestScript(statePath, fixture string) string {
	return fmt.Sprintf("printf '%%s' '%s' > '%s'\n", fixture, statePath)
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
	prServer := fakeLinkedPRServer(t)
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

// TestRunResolveIssuePublishedPRWithVagueBodyNeedsHuman exercises the legacy
// handoff's authoritative-linkage check (#248) on a published PR whose body
// uses the original-dogfood's vague "Addresses" form. The model declared
// changes; the live PR body disagrees with the trusted source identity, so
// the run ends as NeedsHuman with the precise rejection and a changes
// outcome tag. A forge read failure is not a run failure, so the
// continuation path stays scoped to a real body mismatch.
func TestRunResolveIssuePublishedPRWithVagueBodyNeedsHuman(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\ngit push origin \"$COURIER_BRANCH\" >/dev/null 2>&1\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Match either the /pulls (head query) or /pulls/{n} (post-create read).
		if strings.HasSuffix(r.URL.Path, "/pulls") && r.URL.RawQuery != "" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, pullRequestFixtureJSON(12, "open", false, "Addresses #7. Just bundling changes."))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer prServer.Close()

	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_GITHUB_API_BASE", prServer.URL)
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_RUN_NAME", "coderrun-linkage-needs-human")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("missing NeedsHuman termination: %q", output.String())
	}
	// The reason must name the precise failure mode so a human can
	// settle the run. The trusted source identity is part of the
	// reason; the body's vague form ("Addresses #7") is reported
	// as a rejected closing reference even when the validator did
	// not find a supported keyword, so the test asserts the
	// expected source identity and the linkage failure marker.
	if !strings.Contains(output.String(), "acme/widgets#7") {
		t.Fatalf("NeedsHuman reason should name the trusted source identity: %q", output.String())
	}
	if !strings.Contains(output.String(), "trusted source-issue linkage") {
		t.Fatalf("NeedsHuman reason should describe the linkage failure: %q", output.String())
	}
}

// TestRunResolveIssuePublishedPRWithCorrectLinkageVerifying proves the
// positive path: a body that names the trusted source issue with a
// supported closing keyword passes the legacy linkage check and the run
// ends as Verifying.
func TestRunResolveIssuePublishedPRWithCorrectLinkageVerifying(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\ngit push origin \"$COURIER_BRANCH\" >/dev/null 2>&1\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pulls") && r.URL.RawQuery != "" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, pullRequestFixtureJSON(12, "open", false, "Closes #7. This PR resolves the icon assets gap."))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer prServer.Close()

	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_GITHUB_API_BASE", prServer.URL)
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_RUN_NAME", "coderrun-pos")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"changes"`) {
		t.Fatalf("Verifying termination should carry the declared changes outcome: %q", output.String())
	}
	if strings.Contains(output.String(), "linkage-repair") || strings.Contains(output.String(), "NeedsHuman") {
		t.Fatalf("correct linkage should not surface any linkage note: %q", output.String())
	}
}

// TestRunResolveIssueUndeclaredVerifyingTriggersLinkageRepair exercises
// the undeclared-Verifying path's continuation. The model finishes
// without writing an outcome declaration; the legacy executor sees
// committed work, looks up the live PR, finds a body with the wrong
// source-issue number, and feeds a precise repair message to the
// captured session under the bounded continuation budget.
func TestRunResolveIssueUndeclaredVerifyingTriggersLinkageRepair(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	stateDir := filepath.Join(root, "fakestate")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A two-turn script: the first turn emits a session ID and a
	// "not done yet" text, so the executor offers a continuation.
	// The second turn declares changes, so the run ends as
	// Verifying if the body is fixed; otherwise the linkage check
	// keeps firing until the budget is exhausted. We arrange the
	// body to remain wrong across both turns so the run ends as
	// NeedsHuman — this proves the undeclared path applies the
	// same linkage contract as the declared path.
	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	argvLog := filepath.Join(stateDir, "argv.log")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\necho \"$*\" >> \""+argvLog+"\"\n# Reuse the resume session id so a captured session exists.\necho '{\"sessionID\":\"ses_linkage\"}'\nprintf 'not done yet\\n'\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\ngit push origin \"$COURIER_BRANCH\" >/dev/null 2>&1\n# Never declare an outcome; the legacy path must keep the run\n# on the undeclared-Verifying track and the linkage check must\n# keep firing.\nexit 0\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pulls") && r.URL.RawQuery != "" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, pullRequestFixtureJSON(12, "open", false, "Closes #999. Wrong issue number."))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer prServer.Close()

	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_GITHUB_API_BASE", prServer.URL)
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_MAX_CONTINUATIONS", "2")
	t.Setenv("COURIER_FAKE_STATE", stateDir)
	t.Setenv("COURIER_RUN_NAME", "coderrun-linkage-7")
	t.Setenv("COURIER_LOG_LEVEL", "debug")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman after exhausting linkage repair budget); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	// The undeclared path's repair flow must have run at least
	// one linkage-repair continuation before the budget is
	// exhausted. The instruction is logged as the verbose
	// continuation detail so the operator can audit what the
	// model was told to fix.
	if !strings.Contains(output.String(), `"kind":"linkage-repair"`) {
		t.Fatalf("expected a linkage-repair continuation in the event log: %q", output.String())
	}
	if !strings.Contains(output.String(), "acme/widgets#7") {
		t.Fatalf("repair message should name the trusted source identity: %q", output.String())
	}
	if !strings.Contains(output.String(), `"pr":12`) {
		t.Fatalf("repair message should name the existing PR number: %q", output.String())
	}
	// The repair message must name the supported closing keywords
	// and the trusted source identity so the model can repair the
	// body without re-deriving the contract.
	if !strings.Contains(output.String(), "Closes") || !strings.Contains(output.String(), "Fixes") {
		t.Fatalf("repair message should name the supported closing keywords: %q", output.String())
	}
}

// TestRunResolveIssueLinkageReadFailureFailsClosed proves the executor
// is fail-closed on a forge read failure (#248 follow-up): an unverifiable
// PR body cannot quietly become a Verifying success. The run has no
// captured session (the model exited 0), so the bounded retry has nothing
// to resume; the run escalates to NeedsHuman with the precise forge
// error as the reason, and the linkage.read.failed event carries the
// original error. The declared-changes outcome tag is preserved.
func TestRunResolveIssueLinkageReadFailureFailsClosed(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")

	// A GitHub server that always 500s: the legacy executor cannot
	// prove the PR body satisfies authoritative source-issue linkage
	// when the forge is unreachable, so the run fails closed instead
	// of becoming a Verifying success on an unverifiable world.
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer prServer.Close()

	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_GITHUB_API_BASE", prServer.URL)
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_RUN_NAME", "coderrun-linkage-read-error")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman on fail-closed forge read failure); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman handoff on forge read failure: %q", output.String())
	}
	if !strings.Contains(output.String(), `"linkage.read.failed"`) {
		t.Fatalf("expected the linkage read failure to be logged: %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"changes"`) {
		t.Fatalf("declared outcome tag should be preserved on fail-closed NeedsHuman: %q", output.String())
	}
	if !strings.Contains(output.String(), "acme/widgets#7") {
		t.Fatalf("NeedsHuman reason should name the trusted source identity: %q", output.String())
	}
	if !strings.Contains(output.String(), "500") {
		t.Fatalf("NeedsHuman reason should name the forge read error: %q", output.String())
	}
}

// TestRunResolveIssueLinkageReadFailureRetriesInsideBudget exercises
// the bounded retry/fail-closed mechanism (#248 follow-up, #247) on the
// declared-changes path: a forge read failure that is recoverable
// inside the captured session's continuation budget must route through a
// re-verify continuation exactly the way the undeclared-Verifying path
// does. #247 requires that a transient read failure is never dressed up
// as a confirmed missing pull request: the continuation is a factual
// pr-unverified turn that says the read failed, not a missing-PR create
// prompt. The fake's first turn declares changes; the second turn never
// declares an outcome, so the run ends after the read-failure retry fires
// at least once and the budget is then exhausted.
func TestRunResolveIssueLinkageReadFailureRetriesInsideBudget(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	stateDir := filepath.Join(root, "fakestate")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	argvLog := filepath.Join(stateDir, "argv.log")
	// Two turns: turn 1 declares changes against committed work; turn
	// 2 (after the linkage-repair continuation fires) does not declare an
	// outcome and exits 0, so the run is classified as undeclared mid-
	// budget. The shared helper's retry path must run at least once on
	// turn 1's classified Verifying candidate.
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\necho \"$*\" >> \""+argvLog+"\"\necho '{\"sessionID\":\"ses_forge_retry\"}'\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\nif [ -e \"$COURIER_SCRATCH_DIR/turn1\" ]; then\n  exit 0\nfi\ntouch \"$COURIER_SCRATCH_DIR/turn1\"\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")

	// A GitHub server that always 500s: the linkage check cannot read
	// the PR body, so the executor's shared repair/budget helper must
	// route the declared-changes Verifying candidate through the same
	// repair continuation the undeclared path uses.
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer prServer.Close()

	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_GITHUB_API_BASE", prServer.URL)
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_MAX_CONTINUATIONS", "2")
	t.Setenv("COURIER_FAKE_STATE", stateDir)
	t.Setenv("COURIER_RUN_NAME", "coderrun-linkage-forge-retry")
	t.Setenv("COURIER_LOG_LEVEL", "debug")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	// After the linkage-repair continuation fires, the next turn does
	// not declare an outcome. The undeclared path then re-reads the
	// forge — which is still 500-ing — and only one of the two budget
	// slots is consumed by that retry. The run ends undeclared-Verifying
	// (success) only if the path mistakenly fell through; the correct
	// fail-closed path needs a captured session that re-declares. The
	// fake does not re-declare, so the run ends NeedsHuman after the
	// budget is exhausted by re-invoking the helper on the undeclared
	// path. The code is therefore exitNeedsHuman, and the captured-
	// session budget has been consumed at least once by a linkage-
	// repair continuation.
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on forge read failure); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"kind":"pr-unverified"`) {
		t.Fatalf("expected the shared helper to fire a factual pr-unverified continuation: %q", output.String())
	}
	// #247: the read failure must not be represented as a confirmed
	// missing pull request.
	if strings.Contains(output.String(), "no pull request for that head") {
		t.Fatalf("a failed read must not claim the pull request is missing: %q", output.String())
	}
	if !strings.Contains(output.String(), `"linkage.read.failed"`) {
		t.Fatalf("expected the linkage read failure event: %q", output.String())
	}
	if !strings.Contains(output.String(), "acme/widgets#7") {
		t.Fatalf("re-verify message should name the trusted source identity: %q", output.String())
	}
}

// TestRunResolveIssueDeclaredChangesLinkageFailureUsesBoundedRepair
// proves the declared-changes Verifying candidate participates in the
// same bounded continuation/repair mechanism as the undeclared path
// (#248 follow-up): when the model has declared changes and the live
// PR body disagrees with the trusted source issue, the legacy executor
// must ask the captured session to repair the body, escalate to
// NeedsHuman only after the bounded budget is exhausted, and preserve
// the declared changes outcome tag on the terminal handoff.
func TestRunResolveIssueDeclaredChangesLinkageFailureUsesBoundedRepair(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	stateDir := filepath.Join(root, "fakestate")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	argvLog := filepath.Join(stateDir, "argv.log")
	// Both turns declare changes against committed work; the body
	// never gets corrected, so the bounded budget is exhausted and
	// the run escalates to NeedsHuman, not to a forged Verifying
	// success. The model never repairs the body because the fake
	// only declares changes — the linkage check on each turn will
	// fire again, consume one continuation, and so on.
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\necho \"$*\" >> \""+argvLog+"\"\necho '{\"sessionID\":\"ses_decl_repair\"}'\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\ngit push origin \"$COURIER_BRANCH\" >/dev/null 2>&1\nprintf '{\"outcome\":\"changes\"}' > \"$COURIER_SCRATCH_DIR/outcome.json\"\nexit 0\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pulls") && r.URL.RawQuery != "" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, pullRequestFixtureJSON(12, "open", false, "Addresses #7. Just bundling changes."))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer prServer.Close()

	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_GITHUB_API_BASE", prServer.URL)
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_MAX_CONTINUATIONS", "1")
	t.Setenv("COURIER_FAKE_STATE", stateDir)
	t.Setenv("COURIER_RUN_NAME", "coderrun-decl-linkage-repair")
	t.Setenv("COURIER_LOG_LEVEL", "debug")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (declared NeedsHuman after bounded repair budget); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman termination: %q", output.String())
	}
	if !strings.Contains(output.String(), `"outcome":"changes"`) {
		t.Fatalf("NeedsHuman should preserve the declared changes outcome tag: %q", output.String())
	}
	if !strings.Contains(output.String(), `"kind":"linkage-repair"`) {
		t.Fatalf("expected the declared-changes linkage failure to use the bounded repair continuation: %q", output.String())
	}
	if !strings.Contains(output.String(), "acme/widgets#7") {
		t.Fatalf("repair message and exhausted-budget reason should name the trusted source identity: %q", output.String())
	}
	if !strings.Contains(output.String(), `"pr":12`) {
		t.Fatalf("linkage-repair continuation should record the offending PR number: %q", output.String())
	}
}

// TestRunResolveIssueUndeclaredLinkageReadFailureFailsClosed proves the
// undeclared-Verifying path is fail-closed on a forge read failure (#248
// follow-up): a transient GitHub outage during the authoritative
// linkage check cannot quietly become a Verifying success even when
// the coordinator never declared an outcome. The fake leaves the work
// committed without writing an outcome declaration, so the run takes
// the undeclared-Verifying route; the GitHub server answers every
// pull request query with HTTP 500, so the shared repair helper
// exhausts its bounded budget across the captured session's
// continuations and the run ends NeedsHuman with the precise forge
// error as the reason.
func TestRunResolveIssueUndeclaredLinkageReadFailureFailsClosed(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	stateDir := filepath.Join(root, "fakestate")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeOpenCode := filepath.Join(root, "opencode")
	scratchDirectory := filepath.Join(root, "scratch")
	if err := os.MkdirAll(scratchDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	argvLog := filepath.Join(stateDir, "argv.log")
	// Two turns: turn 1 prints text + session id, commits work, and
	// intentionally never declares an outcome so the run takes the
	// undeclared-Verifying route. Turn 2 does the same so the
	// shared linkage-repair helper exhausts the bounded budget
	// across continuations and the run escalates to NeedsHuman.
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\necho \"$*\" >> \""+argvLog+"\"\nmkdir -p \"$COURIER_SCRATCH_DIR\"\nif [ ! -e \"$COURIER_SCRATCH_DIR/undecl_turn1\" ]; then\n  touch \"$COURIER_SCRATCH_DIR/undecl_turn1\"\n  echo '{\"sessionID\":\"ses_undeclared_forge\"}'\n  printf 'partial work\\n'\n  printf 'completed\\n' > completed.txt\n  git add --all -- .\n  git commit -m 'test: completed work' >/dev/null\n  exit 0\nfi\nprintf 'still no outcome\\n'\nexit 0\n")

	// A GitHub server that always 500s: the undeclared-Verifying
	// path's shared linkage check sees a forge read error on every
	// iteration. The bounded budget plus a captured session are
	// required to fail closed without becoming a Verifying success.
	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer prServer.Close()

	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_SCRATCH_DIR", scratchDirectory)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7. Declare the outcome at "+filepath.Join(defaultScratchDirectory, defaultOutcomeFilename)+".")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_GITHUB_API_BASE", prServer.URL)
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	t.Setenv("COURIER_MAX_CONTINUATIONS", "1")
	t.Setenv("COURIER_FAKE_STATE", stateDir)
	t.Setenv("COURIER_RUN_NAME", "coderrun-undeclared-forge-read-fails")
	t.Setenv("COURIER_LOG_LEVEL", "debug")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman on fail-closed undeclared forge read failure); stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman handoff on undeclared forge read failure: %q", output.String())
	}
	if strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("undeclared-Verifying must not be handed off as Verifying on a forge read error: %q", output.String())
	}
	if !strings.Contains(output.String(), `"kind":"pr-unverified"`) {
		t.Fatalf("expected the undeclared path to fire a factual pr-unverified continuation before exhausting the budget: %q", output.String())
	}
	if !strings.Contains(output.String(), `"linkage.read.failed"`) {
		t.Fatalf("expected the linkage read failure event: %q", output.String())
	}
	if !strings.Contains(output.String(), "acme/widgets#7") {
		t.Fatalf("NeedsHuman reason should name the trusted source identity: %q", output.String())
	}
}

// The #247 tests below pin the post-turn, pre-handoff forge observation for
// legacy resolve-issue runs: a confirmed missing or draft pull request
// resumes the captured session with a factual state message inside #170's
// shared budget, while an ambiguous, closed-only, or unpublished world and a
// session that cannot be resumed fail closed. A transient forge read is
// never dressed up as a confirmed missing pull request.

// runResolveIssue247 runs a scripted resolve-issue scenario against the
// stateful fake forge and a bare-base remote (the run branch is absent
// from the remote at adoption time, so the adoption guard is skipped).
// prePullRequests optionally seeds the pull-request state served before
// the run starts. The returned values are the exit code, the run output,
// and the fake-script argv log.
func runResolveIssue247(t *testing.T, prePullRequests, script string) (int, string, []string) {
	t.Helper()
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, prState := fakeForgePullRequestServer(t)
	if prePullRequests != "" {
		write(t, prState, prePullRequests)
	}
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, script)
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	return code, output.String(), readArgvLog(t, filepath.Join(root, "fakestate"))
}

// script247 renders the shared fake OpenCode body for the #247 tests: log
// argv, announce the session, commit per-turn work, push it when push is
// set, run extraShell (which may reference $COURIER_FAKE_PR_STATE), then
// declare changes. Every turn carries the same session id so a captured
// session exists for resumes.
func script247(session string, push bool, extraShell string) string {
	pushLine := ""
	if push {
		pushLine = "git push origin \"$COURIER_BRANCH\" >/dev/null 2>&1 || true\n"
	}
	return `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
N=$(cat "$COURIER_FAKE_STATE/count" 2>/dev/null || echo 0)
N=$((N+1))
echo "$N" > "$COURIER_FAKE_STATE/count"
printf '{"type":"text","sessionID":"` + session + `","part":{"type":"text","text":"turn %s of the work"}}\n' "$N"
printf 'work\n' > "work-$N.txt"
git add --all -- .
git commit -m "test: work turn $N" >/dev/null 2>&1 || true
` + pushLine + extraShell + `mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`
}

// script247OnSecondTurn runs turn 1's work, and on every later turn also
// runs resumeShell — the seam that stands in for the model acting on a
// pr-missing or pr-draft continuation through the forge capability.
func script247OnSecondTurn(session, resumeShell string) string {
	return script247(session, true, `if [ "$N" -ge 2 ]; then
`+resumeShell+`fi
`)
}

// stalePushScript renders the fake OpenCode body that leaves the run with a
// published branch ref that does NOT carry its newest commit: it pushes one
// commit and then commits again without pushing, the stale-remote world the
// sha publication proof must fail closed on (#247).
func stalePushScript() string {
	return `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$*" >> "$COURIER_FAKE_STATE/argv.log"
printf 'pushed work\n' > work-pushed.txt
git add --all -- .
git commit -m 'test: pushed commit' >/dev/null
git push origin "$COURIER_BRANCH" >/dev/null 2>&1
printf 'never pushed\n' > work-local.txt
git add --all -- .
git commit -m 'test: commit that never pushed' >/dev/null
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`
}

// TestRunResolveIssueMissingPullRequestResumesSessionToOpenIt proves the
// core #247 ending: a resolve-issue run whose published branch carries no
// pull request does not hand off as Verifying. It resumes the same
// OpenCode session with a factual message naming the pinned repository,
// branch, and trusted source issue; the resumed turn opens the intended
// PR through the forge capability and re-declares, and the verified
// open PR then yields the ordinary Verifying handoff.
func TestRunResolveIssueMissingPullRequestResumesSessionToOpenIt(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, prState := fakeForgePullRequestServer(t)
	fakeOpenCode := filepath.Join(root, "opencode")
	// The forge shows no pull request until the resumed turn "opens"
	// one through it, standing in for the model acting on the
	// pr-missing continuation.
	writeExecutable(t, fakeOpenCode, script247OnSecondTurn("ses_missing1",
		writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The session opened the intended pull request."))))
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	out := output.String()
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stdout=%q state=%q", code, out, mustReadState(prState))
	}
	if !strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", out)
	}
	if !strings.Contains(out, `"kind":"pr-missing"`) {
		t.Fatalf("expected a pr-missing continuation: %q", out)
	}
	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	if !strings.Contains(calls[1], "--session ses_missing1") {
		t.Fatalf("resume did not carry the same session: %q", calls[1])
	}
	for _, want := range []string{
		"Courier refused the handoff",
		`is published to acme/widgets`,
		`"courier/resolve-issue/acme-widgets/7"`,
		"acme/widgets#7",
		"declare blocked_external",
	} {
		if !strings.Contains(calls[1], want) {
			t.Fatalf("missing-PR state message lost %q: %q", want, calls[1])
		}
	}
}

// TestRunResolveIssueMissingPullRequestWithoutSessionFailsClosed proves
// #247's fail-closed rule for a missing same-session ID: with no captured
// session to resume, a confirmed missing pull request never becomes a
// Verifying handoff and never triggers a blind retry.
func TestRunResolveIssueMissingPullRequestWithoutSessionFailsClosed(t *testing.T) {
	sessionless := `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$*" >> "$COURIER_FAKE_STATE/argv.log"
printf 'work\n' > work.txt
git add --all -- .
git commit -m 'test: committed work without a session' >/dev/null
git push origin "$COURIER_BRANCH" >/dev/null 2>&1 || true
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`
	code, out, _ := runResolveIssue247(t, "", sessionless)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed without a session); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "never opened a pull request") {
		t.Fatalf("reason should name the missing pull request: %q", out)
	}
	if n := strings.Count(out, `"event":"executor.continuation"`); n != 0 {
		t.Fatalf("executor.continuation count = %d, want 0 (nothing to resume)", n)
	}
}

// TestRunResolveIssueMissingPullRequestBudgetExhaustsToNeedsHuman proves
// the missing-PR continuation shares #170's max-attempt budget and
// produces a bounded terminal reason on true exhaustion: two resumable
// turns are offered, then the run escalates to NeedsHuman.
func TestRunResolveIssueMissingPullRequestBudgetExhaustsToNeedsHuman(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, _ := fakeForgePullRequestServer(t)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, script247("ses_missing_budget", true, ""))
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_CONTINUATIONS", "2")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stdout=%q", code, exitNeedsHuman, output.String())
	}
	if !strings.Contains(output.String(), "never opened a pull request") {
		t.Fatalf("terminal reason should name the missing pull request: %q", output.String())
	}
	events := parseEvents(t, &output)
	if n := countEvents(t, events, "executor.continuation"); n != 2 {
		t.Fatalf("executor.continuation count = %d, want 2 (shared bounded budget)", n)
	}
	for _, event := range events {
		if event["event"] != "executor.continuation" {
			continue
		}
		if kind := eventDetail(t, event)["kind"]; kind != "pr-missing" {
			t.Fatalf("continuation kind = %v, want pr-missing", kind)
		}
	}
}

// TestRunResolveIssueDraftPullRequestResumesSessionToReadyIt proves the
// draft ending of #247: a confirmed draft PR on the published head
// resumes the same session with a message asking for that exact PR to be
// marked ready — not for a duplicate — and the readied PR then yields the
// ordinary Verifying handoff.
func TestRunResolveIssueDraftPullRequestResumesSessionToReadyIt(t *testing.T) {
	draft := pullRequestFixtureJSON(12, "open", true, "Closes #7. Work in progress.")
	readied := pullRequestFixtureJSON(12, "open", false, "Closes #7. Work in progress.")
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, draft)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, script247OnSecondTurn("ses_draft1",
		writePullRequestScript(prState, readied)))
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stdout=%q", code, output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", output.String())
	}
	if !strings.Contains(output.String(), `"kind":"pr-draft"`) {
		t.Fatalf("expected a pr-draft continuation: %q", output.String())
	}
	if !strings.Contains(output.String(), `"pr":12`) {
		t.Fatalf("pr-draft continuation should record the draft PR number: %q", output.String())
	}
	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	if !strings.Contains(calls[1], "--session ses_draft1") {
		t.Fatalf("resume did not carry the same session: %q", calls[1])
	}
	for _, want := range []string{
		"pull request #12",
		"ready for review",
		"do not create a duplicate pull request",
	} {
		if !strings.Contains(calls[1], want) {
			t.Fatalf("draft state message lost %q: %q", want, calls[1])
		}
	}
}

// TestRunResolveIssueDraftPullRequestBudgetExhaustsToNeedsHuman proves a
// PR that stays draft past the shared budget terminates with the bounded
// draft reason rather than a Verifying handoff.
func TestRunResolveIssueDraftPullRequestBudgetExhaustsToNeedsHuman(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, prState := fakeForgePullRequestServer(t)
	write(t, prState, pullRequestFixtureJSON(12, "open", true, "Closes #7. Still a draft."))
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, script247("ses_draft_budget", true, ""))
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_CONTINUATIONS", "1")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stdout=%q", code, exitNeedsHuman, output.String())
	}
	if !strings.Contains(output.String(), "remained a draft") {
		t.Fatalf("terminal reason should name the draft PR: %q", output.String())
	}
	if strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("a draft PR must not be handed off as Verifying: %q", output.String())
	}
}

// TestRunResolveIssueUnpublishedHeadFailsClosedWithoutCreatePrompt proves
// #247's remote-publication guard: a head confirmed absent from the
// remote is an unpublished run, not a branch missing a pull request. The
// run fails closed and is never asked to open a pull request over
// unpublished work.
func TestRunResolveIssueUnpublishedHeadFailsClosedWithoutCreatePrompt(t *testing.T) {
	noPush := `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$*" >> "$COURIER_FAKE_STATE/argv.log"
printf 'work\n' > work.txt
git add --all -- .
git commit -m 'test: committed but never pushed' >/dev/null
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`
	code, out, _ := runResolveIssue247(t, "", noPush)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on unpublished head); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "is not published on the remote") {
		t.Fatalf("reason should name the unpublished head: %q", out)
	}
	if strings.Contains(out, "Open the pull request") {
		t.Fatalf("an unpublished head must never be asked to open a pull request: %q", out)
	}
}

// TestRunResolveIssueAmbiguousOpenPullRequestsFailClosed proves #247's
// no-adoption rule: when the forge reports more than one open pull
// request on the verified head, the executor refuses to pick one. The
// run terminates immediately, without a retry continuation or a create
// prompt.
func TestRunResolveIssueAmbiguousOpenPullRequestsFailClosed(t *testing.T) {
	ambiguous := "[" + pullRequestFixtureObjectJSON(12, "open", false, "Closes #7. First.") + "," + pullRequestFixtureObjectJSON(13, "open", false, "Closes #7. Second.") + "]"
	code, out, _ := runResolveIssue247(t, ambiguous, script247("ses_ambiguous", true, ""))
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on ambiguity); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "more than one open pull request") {
		t.Fatalf("reason should name the ambiguity: %q", out)
	}
	if n := strings.Count(out, `"event":"executor.continuation"`); n != 0 {
		t.Fatalf("executor.continuation count = %d, want 0 (ambiguous world is fail-closed)", n)
	}
}

// TestRunResolveIssueClosedOnlyPullRequestFailsClosed proves #247 keeps
// the adoption guard's refusal mid-run: a head whose only pull requests
// are closed or merged is never reopened, adopted, or duplicated by a
// continuation.
func TestRunResolveIssueClosedOnlyPullRequestFailsClosed(t *testing.T) {
	closed := pullRequestFixtureJSON(11, "closed", false, "Closes #7. Abandoned.")
	code, out, _ := runResolveIssue247(t, closed, script247("ses_closed", true, ""))
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on closed-only PRs); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "only closed or merged pull requests") {
		t.Fatalf("reason should name the closed-only state: %q", out)
	}
	if strings.Contains(out, `"kind":"pr-missing"`) {
		t.Fatalf("a closed PR must not be treated as a missing one: %q", out)
	}
}

// TestRunResolveIssueTransientForgeReadRetriesFactuallyThenVerifies
// proves the #247 transient rule: a failed live observation is never
// represented as a confirmed missing pull request. The captured session
// gets a factual re-verify turn, and when the next observation succeeds
// the run completes its Verifying handoff.
func TestRunResolveIssueTransientForgeReadRetriesFactuallyThenVerifies(t *testing.T) {
	prJSON := pullRequestFixtureJSON(12, "open", false, "Closes #7. The PR was there all along.")
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, prState := fakeForgePullRequestServer(t)
	fakeOpenCode := filepath.Join(root, "opencode")
	// Turn 1: the forge read fails (the state file says 500). Turn 2:
	// the state file holds the real PR the transient read missed.
	write(t, prState, "500")
	writeExecutable(t, fakeOpenCode, script247OnSecondTurn("ses_transient1",
		writePullRequestScript(prState, prJSON)))
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 (transient read retried, then verified); stdout=%q", code, output.String())
	}
	if !strings.Contains(output.String(), `"kind":"pr-unverified"`) {
		t.Fatalf("expected a factual pr-unverified continuation: %q", output.String())
	}
	if strings.Contains(output.String(), "no pull request for that head") {
		t.Fatalf("a transient read failure must not claim the pull request is missing: %q", output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination after the retry: %q", output.String())
	}
	calls := readArgvLog(t, filepath.Join(root, "fakestate"))
	if len(calls) != 2 {
		t.Fatalf("fake script invoked %d times, want 2: %q", len(calls), calls)
	}
	if !strings.Contains(calls[1], "this is not proof that the pull request is missing") {
		t.Fatalf("re-verify message should disclaim a missing-PR claim: %q", calls[1])
	}
}

// TestRunResolveIssueStaleRemoteBranchFailsClosedWithoutCreatePrompt proves
// the #247 publication guard is a sha proof, not a ref-existence proof: the
// branch ref exists on the remote but at an older commit, so the run's newest
// commits never pushed. The run fails closed as an incomplete publication —
// it is never asked to open a pull request over work that is not on the
// remote, and the reason names both commits.
func TestRunResolveIssueStaleRemoteBranchFailsClosedWithoutCreatePrompt(t *testing.T) {
	code, out, _ := runResolveIssue247(t, "", stalePushScript())
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on stale remote head); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "never pushed") {
		t.Fatalf("reason should name the unpushed commits: %q", out)
	}
	if strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("a stale remote head must not be handed off as Verifying: %q", out)
	}
	if strings.Contains(out, "Open the pull request") {
		t.Fatalf("unpushed commits must never earn a create-PR prompt: %q", out)
	}
}

// TestRunResolveIssueOpenPullRequestBehindUnpushedCommitsFailsClosed proves
// the publication check covers the already-open route: an open, correctly
// linked PR on a stale remote ref must not carry the Verifying handoff over
// local commits that never pushed.
func TestRunResolveIssueOpenPullRequestBehindUnpushedCommitsFailsClosed(t *testing.T) {
	linked := pullRequestFixtureJSON(12, "open", false, "Closes #7. The PR exists but is behind.")
	code, out, _ := runResolveIssue247(t, linked, stalePushScript())
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on unpushed commits behind an open PR); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "never pushed") {
		t.Fatalf("reason should name the unpushed commits: %q", out)
	}
	if strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("an open PR on a stale ref must not justify Verifying: %q", out)
	}
}

// TestRunResolveIssueDraftPullRequestBehindUnpushedCommitsFailsClosed proves
// the publication proof covers the draft route too: a draft PR on a remote
// ref that does not carry the run's current commit must not earn a
// mark-it-ready continuation over work that never pushed; the run fails
// closed on the stale ref instead.
func TestRunResolveIssueDraftPullRequestBehindUnpushedCommitsFailsClosed(t *testing.T) {
	draft := pullRequestFixtureJSON(12, "open", true, "Closes #7. Draft, but the ref is stale.")
	code, out, _ := runResolveIssue247(t, draft, stalePushScript())
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on stale ref behind a draft); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "never pushed") {
		t.Fatalf("reason should name the unpushed commits: %q", out)
	}
	if strings.Contains(out, `"kind":"pr-draft"`) {
		t.Fatalf("a stale ref must not earn a ready-the-draft continuation: %q", out)
	}
}

// TestRunResolveIssuePublicationReadFailureRetriesAsUnverified proves the
// git half of the observation obeys #247's transient rule: a publication
// read that cannot complete is unverifiable, never a confirmed missing PR.
// The captured session gets the factual pr-unverified turn, and exhaustion
// escalates with the could-not-verify reason.
func TestRunResolveIssuePublicationReadFailureRetriesAsUnverified(t *testing.T) {
	// The fake opens, readies, or breaks nothing here: after the first
	// push the remote itself disappears (simulated by removing the bare
	// repo), so the forge read succeeds with no PR while the remote-head
	// read fails outright.
	script := `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
N=$(cat "$COURIER_FAKE_STATE/count" 2>/dev/null || echo 0)
N=$((N+1))
echo "$N" > "$COURIER_FAKE_STATE/count"
printf '{"type":"text","sessionID":"ses_pub_read","part":{"type":"text","text":"turn %s"}}\n' "$N"
printf 'work\n' > "work-$N.txt"
git add --all -- .
git commit -m "test: work turn $N" >/dev/null 2>&1 || true
git push origin "$COURIER_BRANCH" >/dev/null 2>&1 || true
if [ "$N" -ge 1 ]; then rm -rf "$COURIER_REPO_URL"; fi
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, _ := fakeForgePullRequestServer(t)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, script)
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_MAX_CONTINUATIONS", "1")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	out := output.String()
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (unverifiable publication read fails closed); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, `"kind":"pr-unverified"`) {
		t.Fatalf("expected the factual pr-unverified continuation on a failed publication read: %q", out)
	}
	if !strings.Contains(out, "could not be verified") {
		t.Fatalf("terminal reason should name the unverifiable observation: %q", out)
	}
	if strings.Contains(out, "no pull request for that head") {
		t.Fatalf("a failed publication read must never claim the pull request is missing: %q", out)
	}
	if strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("an unverifiable publication read must not end Verifying: %q", out)
	}
}

// TestRunResolveIssueWrongBasePullRequestFailsClosed proves #247's base
// check: an open PR carries the verified head but targets a base other than
// the pinned one. The executor neither adopts it, prompts to ready or repair
// it, nor creates a parallel pull request; the run fails closed.
func TestRunResolveIssueWrongBasePullRequestFailsClosed(t *testing.T) {
	wrongBase := "[" + pullRequestFixtureObjectJSONWithBase(12, "open", false, "release-9", "Closes #7. Targeting the wrong base.") + "]"
	code, out, _ := runResolveIssue247(t, wrongBase, script247("ses_wrongbase", true, ""))
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on wrong base); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "targets a base other than the pinned base") {
		t.Fatalf("reason should name the base mismatch: %q", out)
	}
	if n := strings.Count(out, `"event":"executor.continuation"`); n != 0 {
		t.Fatalf("executor.continuation count = %d, want 0 (wrong-base world is fail-closed)", n)
	}
}

// TestRunResolveIssueForeignHeadPullRequestFailsClosed proves #247's foreign
// rule end to end: a same-owner fork claiming the run branch's selector must
// fail the observation closed, not be adopted and not be deleted toward a
// "missing" claim that prompts a duplicate creation.
func TestRunResolveIssueForeignHeadPullRequestFailsClosed(t *testing.T) {
	foreign := "[" + pullRequestFixtureObjectJSONWithHeadRepo(44, "open", false, "acme/widgets-fork", "Closes #7. Not this run's head.") + "]"
	code, out, _ := runResolveIssue247(t, foreign, script247("ses_foreign", true, ""))
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (fail-closed on foreign head); stdout=%q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, "from a different repository") {
		t.Fatalf("reason should name the foreign head: %q", out)
	}
	if strings.Contains(out, `"kind":"pr-missing"`) {
		t.Fatalf("a foreign claim must not be turned into a missing-PR continuation: %q", out)
	}
	if n := strings.Count(out, `"event":"executor.continuation"`); n != 0 {
		t.Fatalf("executor.continuation count = %d, want 0 (foreign world is fail-closed)", n)
	}
}

// mustReadState reads the fake forge state file for failure messages,
// returning a marker when it never materialized.
func mustReadState(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "<no state>"
	}
	return string(data)
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
	prServer, prState := fakeForgePullRequestServer(t)
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'completed\n' > completed.txt
git add --all -- . && git commit -m done && git push origin "$COURIER_BRANCH" >/dev/null 2>&1 || true
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The pushed turn opened the pull request.")))
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
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
// and the run declares changes. The pre-handoff observation (#247) then
// confirms the head was never published and fails the handoff closed to
// NeedsHuman. The gate finds the same unrecoverable state, captures the
// local commit, delivers one authenticated bundle, and emits a single
// evidence.capture event. A second run of the same scenario with capture
// unconfigured must produce the same handoff, proving the capture never
// changes the run's ending.
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
	if code != exitNeedsHuman {
		t.Fatalf("armed run exit code = %d, want %d (unpublished head fails closed, #247)", code, exitNeedsHuman)
	}
	if !strings.Contains(armedOut, `"phase":"NeedsHuman"`) {
		t.Fatalf("armed run should end needs-human: %q", armedOut)
	}
	if !strings.Contains(armedOut, "is not published on the remote") {
		t.Fatalf("armed run should name the unpublished head: %q", armedOut)
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
		if disarmedCode != exitNeedsHuman {
			t.Fatalf("disarmed run exit code = %d, want %d (unpublished head fails closed, #247)", disarmedCode, exitNeedsHuman)
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

// TestLinkageRepairMessage pins the exact shape of the bounded repair
// instruction the legacy executor feeds to the captured session (#248):
// the trusted source identity, the supported closing keywords, the
// offending PR number when the check observed one, and the continuation
// number. The missing-PR instruction moved to prMissingMessage (#247),
// which carries the published-branch facts, so this helper only repairs
// a published PR body.
func TestLinkageRepairMessage(t *testing.T) {
	source := broker.SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502}

	outcome := &handoffCheckOutcome{pr: &prLinkageObservation{Number: 12, State: "open", Body: "Addresses #502"}}
	want := "Courier refused the handoff: PR #12's body does not carry an authoritative closing reference to misospace/miso-gallery#502. Update the PR body through the forge capability so it carries a supported closing keyword (Closes, Fixes, or Resolves) followed by #502, or the cross-repo form misospace/miso-gallery#502 when the trusted source repository differs from the PR base. Then re-declare the same outcome. (continuation 2 of 3; 1 remaining)"
	if got := linkageRepairMessage(source, outcome, 2, 3); got != want {
		t.Fatalf("linkageRepairMessage = %q, want %q", got, want)
	}
}

// TestCombinedBodyRepairMessageNamesBothContracts pins the
// combined-repair continuation shape (#282): a body that violates
// both linkage (#248) and the criterion-evidence contract (#249)
// earns a single bounded continuation that names both defects so one
// turn can fix both, instead of two sequential repairs that would
// burn the shared #170 budget in turn. The linkage defect is named
// first because the closing reference is structurally prior — a body
// without one does not address a specific issue for criteria to
// evaluate — and the criterion-evidence defect is named second so a
// single repair pass lands both contracts.
func TestCombinedBodyRepairMessageNamesBothContracts(t *testing.T) {
	source := broker.SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502}
	linkErr := &broker.LinkageError{Reason: "publication denied: pull request body names different source issue number admitted (expected misospace/miso-gallery#502, found Closes #999)"}
	critErr := &broker.CriteriaError{Reason: "publication denied: criterion evidence missing for AC-2"}
	pinned := &broker.CriterionSet{
		Issue: source,
		Criteria: []broker.Criterion{
			{ID: "AC-1", Text: "first criterion", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			{ID: "AC-2", Text: "second criterion", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		},
		Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	outcome := &handoffCheckOutcome{
		pr:          &prLinkageObservation{Number: 12, State: "open"},
		err:         linkErr,
		criteria:    &broker.CriteriaReport{SetDigest: pinned.Digest},
		criteriaErr: critErr,
		pinnedSet:   pinned,
	}
	msg := combinedBodyRepairMessage(source, outcome, 2, 3)
	// Both defects must be present in one message.
	if !strings.Contains(msg, "Closes, Fixes, or Resolves") {
		t.Fatalf("combinedBodyRepairMessage must name the linkage contract: %q", msg)
	}
	if !strings.Contains(msg, "courier-criteria:v1") {
		t.Fatalf("combinedBodyRepairMessage must name the criterion-evidence contract: %q", msg)
	}
	if !strings.Contains(msg, "criterion evidence missing for AC-2") {
		t.Fatalf("combinedBodyRepairMessage must quote the criterion validator's reason: %q", msg)
	}
	if !strings.Contains(msg, pinned.Digest) {
		t.Fatalf("combinedBodyRepairMessage must name the pinned set digest: %q", msg)
	}
	// The single bounded repair is one continuation slot, not two.
	// The combined message concatenates the two repair parts and
	// each part names the continuation number and the remaining
	// budget; both must agree.
	if got := strings.Count(msg, "(continuation 2 of 3; 1 remaining)"); got != 2 {
		t.Fatalf("combinedBodyRepairMessage should name (continuation 2 of 3; 1 remaining) twice (one from each part) since the shared budget consumes one slot; got %d in: %q", got, msg)
	}
	if strings.Contains(msg, "(continuation 3 of 3") {
		t.Fatalf("combinedBodyRepairMessage should not name a different continuation number: %q", msg)
	}
	// Linkage first, criteria second — structurally prior ordering.
	linkIdx := strings.Index(msg, "Closes, Fixes, or Resolves")
	critIdx := strings.Index(msg, "courier-criteria:v1")
	if linkIdx == -1 || critIdx == -1 || linkIdx > critIdx {
		t.Fatalf("combinedBodyRepairMessage should name linkage first, then criteria: linkIdx=%d critIdx=%d in: %q", linkIdx, critIdx, msg)
	}
}

// TestCombinedBodyExhaustedReasonNamesBothContracts pins the
// human-readable NeedsHuman reason for a run whose budget ran out
// before the model repaired both body contracts (#282): the reason
// names every defect the validator surfaced so the operator can
// settle the run without re-reading the body, and the trusted source
// identity once. The two halves are joined with " and " so the
// operator reads the failure as a single combined defect.
func TestCombinedBodyExhaustedReasonNamesBothContracts(t *testing.T) {
	var output, errorsOut bytes.Buffer
	report := newReporter(&output, &errorsOut, config{
		Mode: "resolve-issue",
		Repo: "misospace/miso-gallery",
		Ref:  502,
	})
	linkErr := &broker.LinkageError{Reason: "publication denied: pull request body names different source issue number admitted (expected misospace/miso-gallery#502, found Closes #999)"}
	critErr := &broker.CriteriaError{Reason: "publication denied: criterion evidence missing for AC-2"}
	outcome := &handoffCheckOutcome{
		pr:          &prLinkageObservation{Number: 12, State: "open"},
		err:         linkErr,
		criteriaErr: critErr,
	}
	got := report.combinedBodyExhaustedReason(outcome)
	for _, want := range []string{
		"incomplete handoff:",
		"linkage:",
		"criterion evidence",
		" and ",
		"misospace/miso-gallery#502",
		"different source issue number",
		"criterion evidence missing for AC-2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("combinedBodyExhaustedReason missing %q in: %q", want, got)
		}
	}
	// The trusted source identity must appear at least once in a
	// parenthetical trailing the reason so the operator reads a
	// single combined defect with one anchoring source issue; the
	// linkage validator's reason may also name it inline, so the
	// minimum contract is "one occurrence" (the trailing form),
	// not "exactly one" (the validator's own text could echo it).
	if !strings.Contains(got, "(trusted source issue misospace/miso-gallery#502)") {
		t.Fatalf("combinedBodyExhaustedReason must name the trusted source issue in a trailing parenthetical: %q", got)
	}
}

// pinCriteriaEnvelopeForMisoGallery extracts the pinned criterion set the
// controller admission stage would produce from the real miso-gallery
// issue body, serializes it in the COURIER_CRITERIA envelope shape the
// legacy executor consumes (#280), and returns the rendered JSON so a
// run-level test can set it as the run's COURIER_CRITERIA value. The
// envelope is rooted in the broker's internal package; this helper is the
// test-local mirror so the executor's tests never depend on the
// controller's projection logic.
func pinCriteriaEnvelopeForMisoGallery(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "internal", "broker", "testdata", "miso-gallery-502-issue.md"))
	if err != nil {
		t.Fatalf("read broker testdata fixture: %v", err)
	}
	set, err := broker.ExtractCriteria(string(body), broker.SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502})
	if err != nil {
		t.Fatalf("extract criteria from broker testdata fixture: %v", err)
	}
	if set.IsEmpty() {
		t.Fatal("expected a non-empty pinned set for miso-gallery #502")
	}
	rendered, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("marshal pinned criterion envelope: %v", err)
	}
	return string(rendered)
}

// TestCriteriaRepairMessagePinsValidatorVocabulary pins the exact shape of
// the criterion-repair continuation (#282, #249): the offending PR number,
// the trusted source issue identity, the broker-defined rejection reason
// (drawn verbatim from the validator so any change to the validator's
// vocabulary surfaces in both paths), the pinned set digest and the
// verbatim pinned criterion texts so the model can verify what it must
// satisfy, and the grammar the body must satisfy — the
// "courier-criteria:v1" header, the "AC-n sha256:... status - reference"
// row form, the closed status vocabulary, and the per-status shape rules.
// The continuation number, total, and remaining slots are named so the
// model sees the remaining budget (#282).
func TestCriteriaRepairMessagePinsValidatorVocabulary(t *testing.T) {
	source := broker.SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502}
	ce := &broker.CriteriaError{
		Missing:         []string{"AC-2"},
		CoverageClaim:   broker.CoverageFull,
		CoverageDerived: broker.CoveragePartial,
	}
	// Reflect the validator's describeCriteriaFailure composition: missing
	// fields followed by the coverage lie. Mirror the validator's
	// describeCriteriaFailure ordering so the test pins the actual broker
	// vocabulary, not a re-derivation.
	ce.Reason = "publication denied: criterion evidence missing for AC-2; coverage claimed full but the records derive partial"
	pinned := &broker.CriterionSet{
		Issue: source,
		Criteria: []broker.Criterion{
			{ID: "AC-1", Text: "image ships three tracked PNGs", Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"},
			{ID: "AC-2", Text: "icon endpoints return 200", Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222"},
			{ID: "AC-3", Text: "assets path is included in build", Digest: "sha256:3333333333333333333333333333333333333333333333333333333333333333"},
		},
		Digest: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
	}
	outcome := &handoffCheckOutcome{
		pr:          &prLinkageObservation{Number: 12, State: "open"},
		criteria:    &broker.CriteriaReport{SetDigest: pinned.Digest},
		criteriaErr: ce,
		pinnedSet:   pinned,
	}
	msg := criteriaRepairMessage(source, outcome, 3, 3)
	for _, want := range []string{
		"PR #12",
		"misospace/miso-gallery#502",
		ce.Reason,
		"courier-criteria:v1",
		"- AC-<n> sha256:<64hex>: <status> - <reference>",
		"implementation-evidence",
		"regression-check",
		"manual-verification",
		"not-implemented",
		"regression-check and implementation-evidence",
		"file path or check URL",
		"Update the PR body through the forge capability",
		"(continuation 3 of 3; 0 remaining)",
		pinned.Digest,
		"AC-1 sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"AC-2 sha256:2222222222222222222222222222222222222222222222222222222222222222",
		"AC-3 sha256:3333333333333333333333333333333333333333333333333333333333333333",
		"image ships three tracked PNGs",
		"icon endpoints return 200",
		"assets path is included in build",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("criteriaRepairMessage missing %q in: %q", want, msg)
		}
	}
}

// TestCriteriaRepairMessageFallsBackToReportDigestOnUncarriedSet pins
// the criterion-repair message's digest-source contract (#282): when
// the outcome carries no pinned set (e.g. an older checkCriterionEvidence
// path that produced a report but did not attach the set), the
// repair path must still surface the validator-observed set digest
// rather than falling back to the generic "the admitted set" string,
// so an operator can read the contract the body failed against.
func TestCriteriaRepairMessageFallsBackToReportDigestOnUncarriedSet(t *testing.T) {
	source := broker.SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502}
	ce := &broker.CriteriaError{Reason: "publication denied: criterion-evidence block missing"}
	reportDigest := "sha256:5555555555555555555555555555555555555555555555555555555555555555"
	outcome := &handoffCheckOutcome{
		pr:          &prLinkageObservation{Number: 12, State: "open"},
		criteria:    &broker.CriteriaReport{SetDigest: reportDigest},
		criteriaErr: ce,
	}
	msg := criteriaRepairMessage(source, outcome, 1, 3)
	if !strings.Contains(msg, reportDigest) {
		t.Fatalf("criteriaRepairMessage should carry the report's set digest when no pinned set is attached: %q", msg)
	}
	if strings.Contains(msg, "the admitted set") {
		t.Fatalf("criteriaRepairMessage should not fall back to generic phrase when the report carries a digest: %q", msg)
	}
}

// TestCriteriaExhaustedReasonPinsIncompleteHandoffMarker pins the precise
// NeedsHuman reason the legacy executor records when the criteria-repair
// budget is exhausted (#282): the "incomplete handoff: criterion
// evidence" prefix, the broker-defined violation reason (when the
// validator saw something the broker test table covers), the trusted
// source identity, and the stable fallback the helper returns when no
// structured error is set.
func TestCriteriaExhaustedReasonPinsIncompleteHandoffMarker(t *testing.T) {
	var output, errorsOut bytes.Buffer
	report := newReporter(&output, &errorsOut, config{
		Mode: "resolve-issue",
		Repo: "misospace/miso-gallery",
		Ref:  502,
	})

	// Validator-seen path: the broker carries the violation reason
	// verbatim so the operator can read what failed.
	withReason := &handoffCheckOutcome{
		pr:          &prLinkageObservation{Number: 12, State: "open"},
		criteriaErr: &broker.CriteriaError{Reason: "publication denied: criterion evidence missing for AC-2"},
	}
	got := report.criteriaExhaustedReason(withReason)
	if !strings.HasPrefix(got, "incomplete handoff: criterion evidence") {
		t.Fatalf("criteriaExhaustedReason prefix = %q, want %q", got, "incomplete handoff: criterion evidence")
	}
	if !strings.Contains(got, "missing for AC-2") {
		t.Fatalf("criteriaExhaustedReason should carry the broker reason: %q", got)
	}
	if !strings.Contains(got, "misospace/miso-gallery#502") {
		t.Fatalf("criteriaExhaustedReason should name the trusted source identity: %q", got)
	}

	// Stable fallback path: nothing the validator saw, the helper
	// returns the documented stable string while still naming the
	// trusted source identity.
	fallback := report.criteriaExhaustedReason(&handoffCheckOutcome{pr: &prLinkageObservation{Number: 12, State: "open"}})
	if !strings.HasPrefix(fallback, "incomplete handoff: criterion evidence block did not satisfy the pinned set") {
		t.Fatalf("fallback prefix = %q, want the stable string", fallback)
	}
	if !strings.Contains(fallback, "misospace/miso-gallery#502") {
		t.Fatalf("fallback should name the trusted source identity: %q", fallback)
	}
}

// TestPullRequestStateMessages pins the exact #247 continuation
// instructions: the missing-PR message names the pinned repository, the
// run branch, and the trusted source issue and forbids foreign adoption;
// the draft message names the exact PR number and asks only for that PR
// to be marked ready; the unverified message reports a failed read as a
// failed read and never claims the pull request is missing.
func TestPullRequestStateMessages(t *testing.T) {
	var output, errorsOut bytes.Buffer
	report := newReporter(&output, &errorsOut, config{
		Mode:   "resolve-issue",
		Repo:   "misospace/miso-gallery",
		Branch: "courier/resolve-issue/miso-gallery/502",
		Base:   "main",
		Ref:    502,
	})
	source := report.sourceIssue()

	missing := report.prMissingMessage(source, 1)
	for _, want := range []string{
		`the run branch "courier/resolve-issue/miso-gallery/502" is published to misospace/miso-gallery`,
		"head misospace/miso-gallery:courier/resolve-issue/miso-gallery/502 against base main",
		"authoritative closing reference to misospace/miso-gallery#502",
		"Do not adopt a pull request whose head belongs to another repository",
		"declare blocked_external naming the block",
		"(continuation 1)",
	} {
		if !strings.Contains(missing, want) {
			t.Fatalf("missing-PR message lost %q: %q", want, missing)
		}
	}

	draft := report.prDraftMessage(source, 44, 2)
	for _, want := range []string{
		"pull request #44 for the run branch",
		"Mark pull request #44 ready for review through the forge capability",
		"do not create a duplicate pull request",
		"do not change its base or head",
		"(continuation 2)",
	} {
		if !strings.Contains(draft, want) {
			t.Fatalf("draft message lost %q: %q", want, draft)
		}
	}

	unverified := report.prUnverifiedMessage(source, errors.New("GitHub API returned HTTP 500"), 3)
	for _, want := range []string{
		"could not verify the pull-request state",
		"GitHub API returned HTTP 500",
		"this is not proof that the pull request is missing",
		"open, non-draft pull request",
		"(continuation 3)",
	} {
		if !strings.Contains(unverified, want) {
			t.Fatalf("unverified message lost %q: %q", want, unverified)
		}
	}
}

// TestObserveRunBranchPullRequestIdentity pins the identity rules of the
// executor's pre-handoff forge read (#247): only a pull request whose
// full base- and head-repository identity matches the run's pinned
// repository and branch counts as the run's own; a foreign head is never
// adopted; a response missing either identity is a read failure, never a
// classified world; and two open PRs on the verified head are ambiguous
// rather than a silent pick.
func TestObserveRunBranchPullRequestIdentity(t *testing.T) {
	newObserved := func(body string) runBranchPRObservation {
		t.Helper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		}))
		t.Cleanup(server.Close)
		var output, errorsOut bytes.Buffer
		report := newReporter(&output, &errorsOut, config{
			Mode:          "resolve-issue",
			Repo:          "acme/widgets",
			Branch:        "courier/resolve-issue/acme-widgets/7",
			Ref:           7,
			Base:          "main",
			GitHubAPIBase: server.URL,
		})
		observation, err := report.observeRunBranchPullRequest(context.Background())
		if err != nil {
			t.Fatalf("observeRunBranchPullRequest() error = %v", err)
		}
		return observation
	}

	if got := newObserved(pullRequestFixtureJSON(12, "open", false, "Closes #7.")); got.kind != prStateOpen || got.open == nil || got.open.Number != 12 {
		t.Fatalf("own-head open PR classified as %+v, want prStateOpen #12", got)
	}
	if got := newObserved(pullRequestFixtureJSON(12, "open", true, "Closes #7.")); got.kind != prStateDraft || got.draft == nil {
		t.Fatalf("draft PR classified as %+v, want prStateDraft", got)
	}
	foreign := `[{"number":9,"state":"open","draft":false,"base":{"ref":"main","repo":{"full_name":"acme/widgets"}},"head":{"ref":"courier/resolve-issue/acme-widgets/7","repo":{"full_name":"mallory/widgets"}},"body":"Closes #7."}]`
	if got := newObserved(foreign); got.kind != prStateForeignHead {
		t.Fatalf("foreign-head PR classified as %+v, want prStateForeignHead (fail-closed, never adopted, never deleted toward a missing claim)", got)
	}
	wrongBase := `[{"number":10,"state":"open","draft":false,"base":{"ref":"release-9","repo":{"full_name":"acme/widgets"}},"head":{"ref":"courier/resolve-issue/acme-widgets/7","sha":"deadbeef","repo":{"full_name":"acme/widgets"}},"body":"Closes #7."}]`
	if got := newObserved(wrongBase); got.kind != prStateWrongBase {
		t.Fatalf("wrong-base PR classified as %+v, want prStateWrongBase (fail-closed, never adopted)", got)
	}
	missingIdentity := `[{"number":9,"state":"open","draft":false,"body":"Closes #7."}]`
	foreignServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, missingIdentity)
	}))
	t.Cleanup(foreignServer.Close)
	var output, errorsOut bytes.Buffer
	report := newReporter(&output, &errorsOut, config{
		Mode:          "resolve-issue",
		Repo:          "acme/widgets",
		Branch:        "courier/resolve-issue/acme-widgets/7",
		Ref:           7,
		Base:          "main",
		GitHubAPIBase: foreignServer.URL,
	})
	if _, err := report.observeRunBranchPullRequest(context.Background()); err == nil {
		t.Fatal("a response without repository identity must fail the read, not classify a world")
	}
	ambiguous := "[" + pullRequestFixtureObjectJSON(12, "open", false, "Closes #7.") + "," + pullRequestFixtureObjectJSON(13, "open", false, "Closes #7.") + "]"
	if got := newObserved(ambiguous); got.kind != prStateAmbiguous {
		t.Fatalf("two open PRs on the verified head classified as %+v, want prStateAmbiguous", got)
	}
}

// TestLinkageExhaustedReason pins the exact NeedsHuman reason the legacy
// executor records when the bounded repair budget is exhausted (#248):
// the trusted source identity and, when the validator saw any, the
// model's attempted references as rendered by
// broker.FormatLinkageObservations.
func TestLinkageExhaustedReason(t *testing.T) {
	var output, errorsOut bytes.Buffer
	report := newReporter(&output, &errorsOut, config{
		Mode: "resolve-issue",
		Repo: "misospace/miso-gallery",
		Ref:  502,
	})
	source := report.sourceIssue()

	linkErr := broker.ValidateLinkage("Closes #999. Wrong issue number.", source)

	wrongNumber, ok := linkErr.(*broker.LinkageError)
	if !ok {
		t.Fatalf("ValidateLinkage = %T (%v), want *broker.LinkageError", linkErr, linkErr)
	}
	missingRef := &broker.LinkageError{Reason: broker.ErrLinkageMissing.Error()}

	cases := []struct {
		name    string
		outcome *handoffCheckOutcome
		want    string
	}{
		{
			name:    "missing PR",
			outcome: &handoffCheckOutcome{missing: true},
			want:    "the model never opened a pull request for the run branch, so the handoff was refused; the run needs a human to open one with the trusted source-issue linkage to misospace/miso-gallery#502",
		},
		{
			name:    "wrong issue number reports the model's attempted reference",
			outcome: &handoffCheckOutcome{pr: &prLinkageObservation{Number: 12, State: "open"}, err: wrongNumber},
			want:    "the published pull request body did not satisfy the trusted source-issue linkage to misospace/miso-gallery#502 after the bounded repair budget (model wrote: Closes #999): publication denied: pull request body names a different source issue number than admitted (expected misospace/miso-gallery#502, found Closes #999)",
		},
		{
			name:    "no supported reference names the missing-reference rejection",
			outcome: &handoffCheckOutcome{pr: &prLinkageObservation{Number: 12, State: "open"}, err: missingRef},
			want:    "the published pull request body did not satisfy the trusted source-issue linkage to misospace/miso-gallery#502 after the bounded repair budget: " + broker.ErrLinkageMissing.Error(),
		},
		{
			name:    "no structured error keeps the generic handoff marker",
			outcome: &handoffCheckOutcome{pr: &prLinkageObservation{Number: 12, State: "open"}},
			want:    "the published pull request body did not satisfy the trusted source-issue linkage to misospace/miso-gallery#502; the run needs a human",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := report.linkageExhaustedReason(tc.outcome); got != tc.want {
				t.Fatalf("linkageExhaustedReason = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLinkageForgeFailureReason pins the exact NeedsHuman reason for a
// forge read failure (#248, #247): the trusted source identity, the
// open non-draft contract the read could not confirm, the forge's error
// text verbatim, and the human handoff marker.
func TestLinkageForgeFailureReason(t *testing.T) {
	var output, errorsOut bytes.Buffer
	report := newReporter(&output, &errorsOut, config{
		Mode: "resolve-issue",
		Repo: "misospace/miso-gallery",
		Ref:  502,
	})
	forgeErr := errors.New("GitHub request: GET https://api.example.com/repos/misospace/miso-gallery/pulls?per_page=100: 500 Internal Server Error")
	want := "the live pull-request state could not be verified to confirm an open, non-draft pull request with authoritative source-issue linkage to misospace/miso-gallery#502: " +
		forgeErr.Error() + "; the run needs a human"
	if got := report.linkageForgeFailureReason(forgeErr); got != want {
		t.Fatalf("linkageForgeFailureReason = %q, want %q", got, want)
	}
}

// TestPreHandoffFailClosedReasons pins the exact NeedsHuman reasons for
// the worlds the #247 observation must not paper over with a retry or a
// creation: an unpublished head (ref absent or stale), an ambiguous set of
// open pull requests, a head whose only pull requests are closed or merged,
// a foreign record claiming the head selector, and an open pull request on
// a base other than the pinned one.
func TestPreHandoffFailClosedReasons(t *testing.T) {
	var output, errorsOut bytes.Buffer
	report := newReporter(&output, &errorsOut, config{
		Mode:   "resolve-issue",
		Repo:   "misospace/miso-gallery",
		Branch: "courier/resolve-issue/miso-gallery/502",
		Ref:    502,
		Base:   "main",
	})
	cases := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "unpublished head",
			got:  report.unpublishedBranchReason(remotePublicationState{localHead: "5566aabb"}),
			want: `the run branch "courier/resolve-issue/miso-gallery/502" is not published on the remote, so the handoff was refused; the executor will not ask for a pull request over unpublished work; the run needs a human`,
		},
		{
			name: "stale remote head",
			got:  report.unpublishedBranchReason(remotePublicationState{localHead: "5566aabb", remoteHead: "1122ccdd"}),
			want: `the run branch "courier/resolve-issue/miso-gallery/502" on the remote is at 1122ccdd while the run's current commit is 5566aabb, so the remote does not carry the run's pushed work; the executor will not hand off as Verifying or ask for a pull request over commits that never pushed; the run needs a human`,
		},
		{
			name: "foreign head claim",
			got:  report.foreignHeadPRReason(),
			want: `the forge reported a pull request claiming the head selector misospace/miso-gallery:courier/resolve-issue/miso-gallery/502 from a different repository, so the observation cannot settle this run's own pull-request state; the executor refuses to adopt a foreign head or open another pull request; the run needs a human`,
		},
		{
			name: "open PR on a wrong base",
			got:  report.wrongBasePRReason(),
			want: `an open pull request on the run branch "courier/resolve-issue/miso-gallery/502" of misospace/miso-gallery targets a base other than the pinned base "main"; the executor refuses to adopt it, re-target it, or create a parallel pull request; the run needs a human to settle the base`,
		},
		{
			name: "ambiguous open PRs",
			got:  report.prAmbiguousReason(),
			want: `the forge reports more than one open pull request for the run branch "courier/resolve-issue/miso-gallery/502" on misospace/miso-gallery; the executor refuses to adopt one or create another; the run needs a human to settle which pull request belongs to this run`,
		},
		{
			name: "closed-only PRs",
			got:  report.closedOnlyPRReason(),
			want: `the run branch "courier/resolve-issue/miso-gallery/502" on misospace/miso-gallery has only closed or merged pull requests and no open one; the executor refuses adoption, reopening, or duplicate creation; the run needs a human`,
		},
		{
			name: "draft never readied",
			got:  report.draftExhaustedReason(44),
			want: `pull request #44 for the run branch "courier/resolve-issue/miso-gallery/502" remained a draft after the bounded continuation budget, so the handoff was refused; the run needs a human to mark it ready for review (trusted source issue misospace/miso-gallery#502)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s reason = %q, want %q", tc.name, tc.got, tc.want)
			}
		})
	}
}

// TestInstallGitIdentityPrecedence pins the standalone executor's identity
// contract (#317): a configured or exported nonempty GIT_* value always wins,
// unset or blank names receive the shared default, and restore returns the
// environment to exactly its prior state.
func TestInstallGitIdentityPrecedence(t *testing.T) {
	names := []string{"GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL"}
	for _, name := range names {
		value, set := os.LookupEnv(name)
		if set {
			t.Cleanup(func() { _ = os.Setenv(name, value) })
		} else {
			t.Cleanup(func() { _ = os.Unsetenv(name) })
		}
	}
	if err := os.Setenv("GIT_AUTHOR_NAME", "Deploy Bot"); err != nil {
		t.Fatal(err)
	}
	if err := os.Unsetenv("GIT_AUTHOR_EMAIL"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("GIT_COMMITTER_NAME", "   "); err != nil {
		t.Fatal(err)
	}
	if err := os.Unsetenv("GIT_COMMITTER_EMAIL"); err != nil {
		t.Fatal(err)
	}
	restore := installGitIdentity()
	if got := os.Getenv("GIT_AUTHOR_NAME"); got != "Deploy Bot" {
		t.Fatalf("GIT_AUTHOR_NAME = %q, want the configured value to be preserved", got)
	}
	if got := os.Getenv("GIT_AUTHOR_EMAIL"); got != executor.DefaultGitAuthorEmail {
		t.Fatalf("GIT_AUTHOR_EMAIL = %q, want the shared default", got)
	}
	if got := os.Getenv("GIT_COMMITTER_NAME"); got != executor.DefaultGitAuthorName {
		t.Fatalf("GIT_COMMITTER_NAME = %q, want the shared default for a blank value", got)
	}
	if got := os.Getenv("GIT_COMMITTER_EMAIL"); got != executor.DefaultGitAuthorEmail {
		t.Fatalf("GIT_COMMITTER_EMAIL = %q, want the shared default", got)
	}
	restore()
	if got := os.Getenv("GIT_AUTHOR_NAME"); got != "Deploy Bot" {
		t.Fatalf("after restore GIT_AUTHOR_NAME = %q, want the prior value back", got)
	}
	if _, set := os.LookupEnv("GIT_AUTHOR_EMAIL"); set {
		t.Fatal("after restore GIT_AUTHOR_EMAIL is set, want it returned to unset")
	}
	if got := os.Getenv("GIT_COMMITTER_NAME"); got != "   " {
		t.Fatalf("after restore GIT_COMMITTER_NAME = %q, want the prior blank value back", got)
	}
	if _, set := os.LookupEnv("GIT_COMMITTER_EMAIL"); set {
		t.Fatal("after restore GIT_COMMITTER_EMAIL is set, want it returned to unset")
	}
}

// The criterion-evidence contract tests below generalize the legacy
// pre-handoff plan (#282, #249): both linkage and criteria share one
// mechanism — the run loop calls applyHandoffPlan once per turn and
// reacts to whichever body contract failed. The run behavior must agree
// across both bodies: a vacuous pass for an empty/unpinned set, a
// bounded "criteria-repair" continuation when a set was pinned and the
// evidence block disagrees with it, an "incomplete handoff: criterion
// evidence" NeedsHuman reason when the bounded budget is exhausted, and
// a Verifying handoff with a criteria.coverage event-detail when the
// body satisfies both contracts and a set was pinned.

// withClosingReference prepends a supported closing-keyword reference
// to a body so the linkage check passes before the criterion check
// runs. The legacy executor enforces the body contracts in linkage-
// first order — a head whose linkage has already failed cannot earn a
// criteria-repair continuation — and the deficient/partial fixtures
// intentionally omit the closing keyword. The honest-partial fixture
// already carries it; the others need this shim.
func withClosingReference(body, source string) string {
	return "Closes " + source + "\n\n" + body
}

// readBrokerFixture reads one of the broker testdata markdown fixtures
// the legacy executor's criterion-evidence contract consumes. It is the
// test-local mirror of broker.loadFixture so the executor's tests do
// not depend on the broker's internals.
func readBrokerFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "broker", "testdata", name))
	if err != nil {
		t.Fatalf("read broker testdata fixture %s: %v", name, err)
	}
	return string(data)
}

// setMisoGalleryRepoConfig swaps the run's COURIER_REPO, COURIER_BRANCH,
// and COURIER_REF onto the miso-gallery identity so the pinned-set
// fixture (which names misospace/miso-gallery#502) is compared against
// a body whose trusted source identity matches. setContinuationEnv sets
// acme/widgets defaults, so this helper resets the run-level identity
// for the criterion tests.
func setMisoGalleryRepoConfig(t *testing.T) {
	t.Helper()
	t.Setenv("COURIER_REPO", "misospace/miso-gallery")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/miso-gallery/502")
	t.Setenv("COURIER_REF", "502")
}

// misoGalleryPRFixtureJSON renders the run-branch pull request in the
// identity-checked shape the executor's pre-handoff observation
// requires (#247, #248): full base- and head-repository identity naming
// misospace/miso-gallery (the trusted source identity the criterion set
// is pinned against). The default pullRequestFixtureJSON names
// acme/widgets and would mismatched as a foreign head, so the
// criterion tests need the miso-gallery mirror.
func misoGalleryPRFixtureJSON(number int, state string, draft bool, body string) string {
	return fmt.Sprintf(`[{"number":%d,"state":%q,"draft":%t,"title":"Work","base":{"ref":"main","repo":{"full_name":"misospace/miso-gallery"}},"head":{"ref":"courier/resolve-issue/miso-gallery/502","sha":"deadbeef","repo":{"full_name":"misospace/miso-gallery"}},"body":%q}]`, number, state, draft, body)
}

// runCriteriaEndToEnd stands up the run scaffolding shared by the
// criterion evidence end-to-end tests: an HTTP forge that scripts the
// run-branch PR body via writePullRequestScript across turns, a fake
// OpenCode script, and the resolved COURIER_CRITERIA envelope to the
// miso-gallery pinned set. prBody is the initial PR body the forge
// returns; script is the full fake OpenCode script that decides what
// happens after the first turn (typically echoing the same deficient
// body so the budget exhausts); criteriaEnvelope is the JSON for the
// pinned criterion set (empty string for no-pinning tests). The
// helpers set continuations=1, run name, debug log level, zero resume
// backoff, and the fake state dir so the run loop can retarget the PR
// body mid-run if a test chooses to.
func runCriteriaEndToEnd(t *testing.T, prBody, script, criteriaEnvelope string) (int, string, []string) {
	t.Helper()
	code, out, argv, _ := runCriteriaEndToEndTracked(t, prBody, script, criteriaEnvelope, false)
	return code, out, argv
}

// runCriteriaEndToEndTracked is runCriteriaEndToEnd with the forge
// comment traffic surfaced: it also returns the bodies of every issue
// comment the run posted, and failComments makes the forge reject every
// comment with HTTP 500 so a test can pin that a comment failure never
// changes the run's ending (#282, step 4). The run's trusted identity is
// the miso-gallery one the pinned-set fixture is bound to; a test that
// needs an identity trustedSourceIssue cannot bind uses
// runCriteriaEndToEndTrackedAs.
func runCriteriaEndToEndTracked(t *testing.T, prBody, script, criteriaEnvelope string, failComments bool) (int, string, []string, []string) {
	t.Helper()
	return runCriteriaEndToEndTrackedAs(t, prBody, script, criteriaEnvelope, failComments, "misospace/miso-gallery", "502")
}

// runCriteriaEndToEndTrackedAs is runCriteriaEndToEndTracked with the
// run's COURIER_REPO/COURIER_REF identity supplied by the caller, applied
// after the harness's own defaults so every other input — the run branch,
// the fixture PR head, the pinned envelope — stays exactly what the shared
// fixture describes. It exists so a test can pin the #282 case where the
// identity the pinned envelope must bind to is itself malformed, without
// standing up a second forge fixture.
func runCriteriaEndToEndTrackedAs(t *testing.T, prBody, script, criteriaEnvelope string, failComments bool, repo, ref string) (int, string, []string, []string) {
	t.Helper()
	return runCriteriaEndToEndPullRequestRecord(t, misoGalleryPRFixtureJSON(12, "open", false, prBody), script, criteriaEnvelope, failComments, repo, ref)
}

// runCriteriaEndToEndPullRequestRecord is runCriteriaEndToEndTrackedAs with
// the raw pull-request record the fake forge serves supplied by the caller,
// so a test can stand up a world other than the default open non-draft PR —
// in particular the missing-PR world (an empty record) whose own bounded
// #247 continuation must not be spent on an operator-side envelope fault
// (#282). Every other input stays exactly what the shared fixture
// describes.
func runCriteriaEndToEndPullRequestRecord(t *testing.T, pullsJSON, script, criteriaEnvelope string, failComments bool, repo, ref string) (int, string, []string, []string) {
	t.Helper()
	root := t.TempDir()
	remote := remoteWithBaseOnly(t, root)
	prServer, prState := fakeForgePullRequestServer(t)
	if failComments {
		write(t, filepath.Join(filepath.Dir(prState), "comment-fail"), "")
	}
	stateDir := filepath.Join(root, "fakestate")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// An empty record leaves the fake forge with no pull request at all:
	// the handler answers the pulls list with [] and the run observes the
	// missing-PR world (#247).
	if pullsJSON != "" {
		write(t, prState, pullsJSON)
	}
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, script)
	setContinuationEnv(t, root, remote, prServer.URL, fakeOpenCode)
	setMisoGalleryRepoConfig(t)
	t.Setenv("COURIER_REPO", repo)
	t.Setenv("COURIER_REF", ref)
	if criteriaEnvelope != "" {
		t.Setenv("COURIER_CRITERIA", criteriaEnvelope)
	}
	t.Setenv("COURIER_MAX_CONTINUATIONS", "1")
	t.Setenv("COURIER_FAKE_STATE", stateDir)
	t.Setenv("COURIER_LOG_LEVEL", "debug")
	t.Setenv("COURIER_RUN_NAME", "coderrun-criteria-it-"+strings.ReplaceAll(t.Name(), "/", "-"))
	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	return code, output.String(), readArgvLog(t, stateDir), readForgeComments(t, prState)
}

// readForgeComments returns the bodies of the issue comments the run
// posted to the fake forge (#282), decoded from the comments.jsonl file
// the fakeForgePullRequestServer fixture appends to. A forge with no
// posted comments yields nil.
func readForgeComments(t *testing.T, statePath string) []string {
	t.Helper()
	path := filepath.Join(filepath.Dir(statePath), "comments.jsonl")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("read forge comments: %v", err)
	}
	var bodies []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var payload struct {
			Body string `json:"body"`
		}
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			t.Fatalf("decode forge comment %q: %v", line, err)
		}
		bodies = append(bodies, payload.Body)
	}
	return bodies
}

// scriptCriteriaDeficientTurns is the fake OpenCode body for the
// criteria-repair exhaustion tests: every turn commits and pushes
// without repairing the body, so the bounded budget exhausts and the
// run ends NeedsHuman. The first turn declares changes; later turns do
// not, so the verifier-rejection verdict settles the run. Written as a
// raw string with literal newlines so every shell command is exactly
// what gets written — the inlined helper must avoid Go string-escape
// confusion around the printf format strings.
func scriptCriteriaDeficientTurns(sessionID string) string {
	script := `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
N=$(cat "$COURIER_FAKE_STATE/count" 2>/dev/null || echo 0)
N=$((N+1)); echo "$N" > "$COURIER_FAKE_STATE/count"
printf '{"sessionID":"` + sessionID + `","part":{"type":"text","text":"turn %s"}}\n' "$N"
printf 'work\n' > "work-$N.txt"
git add --all -- .
git commit -m "test: turn $N" >/dev/null 2>&1 || true
git push origin "$COURIER_BRANCH" >/dev/null 2>&1 || true
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`
	return script
}

// scriptCriteriaHonestPartialTurns is the fake OpenCode body for the
// honest-partial positive path: a single turn opens a PR whose body
// already carries the honest-partial evidence block, declares
// changes, and exits 0; the criteria check then passes and the run
// ends Verifying with the criteria.coverage=partial event detail. No
// continuation is fired.
func scriptCriteriaHonestPartialTurns(sessionID string) string {
	script := `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf '%s\n' "$(printf '%s' "$*" | tr '\n' ' ')" >> "$COURIER_FAKE_STATE/argv.log"
printf '{"sessionID":"` + sessionID + `","part":{"type":"text","text":"honest partial run"}}\n'
printf 'work\n' > work.txt
git add --all -- .
git commit -m 'test: opened the PR with an honest partial evidence block' >/dev/null
git push origin "$COURIER_BRANCH" >/dev/null 2>&1 || true
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
exit 0
`
	return script
}

// TestRunResolveIssueCriteriaDeficientBodyTriggersRepair proves the
// criterion-evidence contract enters the shared pre-handoff plan on
// the same path as linkage (#282): a deficient PR body (the initial
// miso-gallery#505 handoff with an authoritative closing reference
// prepended so linkage passes the body through to the criterion check)
// fires a criteria-repair continuation under the bounded budget; when
// the model never repairs the body, the run ends NeedsHuman with the
// "incomplete handoff: criterion evidence" reason and the declared
// "changes" outcome tag preserved on the terminal handoff.
func TestRunResolveIssueCriteriaDeficientBodyTriggersRepair(t *testing.T) {
	source := "misospace/miso-gallery#502"
	// deficient body + a Closes #502 prefix so linkage accepts the
	// body before the criterion check sees a no-block body.
	body := withClosingReference(readBrokerFixture(t, "miso-gallery-505-pr-deficient.md"), "#502")
	envelope := pinCriteriaEnvelopeForMisoGallery(t)
	code, out, _ := runCriteriaEndToEnd(t, body, scriptCriteriaDeficientTurns("ses_criteria_deficient"), envelope)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman after criteria-repair budget exhausts); stderr/criteria detail: %q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman phase: %q", out)
	}
	if !strings.Contains(out, `"category":"verifier-rejection"`) {
		t.Fatalf("expected verifier-rejection category: %q", out)
	}
	if !strings.Contains(out, `"outcome":"changes"`) {
		t.Fatalf("declared changes outcome tag should be preserved: %q", out)
	}
	if !strings.Contains(out, `"kind":"criteria-repair"`) {
		t.Fatalf("expected a criteria-repair continuation in the event log: %q", out)
	}
	if !strings.Contains(out, "incomplete handoff: criterion evidence") {
		t.Fatalf("exhausted-budget reason should mention incomplete criterion handoff: %q", out)
	}
	if !strings.Contains(out, source) {
		t.Fatalf("exhausted-budget reason should name the trusted source identity: %q", out)
	}
}

// TestRunResolveIssueCriteriaPartialBodyTriggersRepair is the partial
// case of the criteria-repair continuation: an evidence block that
// names AC-1 and AC-3 but omits AC-2 while claiming coverage=full
// passes linkage (the body still says Closes #502.) and fails the
// criterion check with a CriteriaError carrying Missing=[AC-2] and a
// coverage claim/derivation disagreement. The bounded budget exhausts
// with one criteria-repair continuation; the run ends NeedsHuman with
// the declared outcome tag preserved and the "incomplete handoff"
// reason carrying the broker-defined violation.
//
// The continuation is asserted against the pinned contract it renders,
// not just the validator's verdict: the instruction must carry the
// operator-pinned set digest and every pinned criterion row — ID,
// digest, and verbatim text — so a model can satisfy the exact pinned
// set instead of re-deriving it (#282). Asserting only "AC-2" would pass
// on the broker-defined reason alone, which never proves the pinned
// criteria reached the message.
func TestRunResolveIssueCriteriaPartialBodyTriggersRepair(t *testing.T) {
	body := readBrokerFixture(t, "miso-gallery-505-pr-partial.md")
	envelope := pinCriteriaEnvelopeForMisoGallery(t)
	var pinned broker.CriterionSet
	if err := json.Unmarshal([]byte(envelope), &pinned); err != nil {
		t.Fatalf("decode pinned criterion envelope: %v", err)
	}
	if len(pinned.Criteria) < 2 {
		t.Fatalf("pinned fixture set carries %d criteria, want the full miso-gallery #502 set", len(pinned.Criteria))
	}
	code, out, _ := runCriteriaEndToEnd(t, body, scriptCriteriaDeficientTurns("ses_criteria_partial"), envelope)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman after criteria-repair budget exhausts); detail: %q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, `"kind":"criteria-repair"`) {
		t.Fatalf("expected a criteria-repair continuation: %q", out)
	}
	if n := strings.Count(out, `"kind":"criteria-repair"`); n != 1 {
		t.Fatalf("criteria-repair continuation count = %d, want 1 (single slot consumed before exhaustion): %q", n, out)
	}
	if !strings.Contains(out, "incomplete handoff: criterion evidence") {
		t.Fatalf("NeedsHuman reason should mention incomplete criterion handoff: %q", out)
	}
	if !strings.Contains(out, "AC-2") {
		t.Fatalf("broker-defined reason should name the missing criterion: %q", out)
	}
	// The pinned set digest is the trust anchor the repair instruction
	// quotes; it appears nowhere else in the run's telemetry, so its
	// presence proves the envelope's contract reached the message.
	if !strings.Contains(out, "pinned set "+pinned.Digest) {
		t.Fatalf("repair instruction must name the operator-pinned set digest %s: %q", pinned.Digest, out)
	}
	// Every pinned criterion is reproduced verbatim, text included, in
	// the row shape a body must carry (see pinnedSetRepairSnippet).
	for _, c := range pinned.Criteria {
		row := fmt.Sprintf("%s sha256:%s — %s", c.ID, strings.TrimPrefix(c.Digest, "sha256:"), strings.TrimSpace(c.Text))
		if !strings.Contains(out, row) {
			t.Fatalf("repair instruction must quote pinned criterion %q: %q", row, out)
		}
	}
	if !strings.Contains(out, `"outcome":"changes"`) {
		t.Fatalf("declared changes outcome tag should be preserved on criteria-exhausted NeedsHuman: %q", out)
	}
}

// TestRunResolveIssueCriteriaHonestPartialHandsOffVerifyingWithCoverage
// pins the positive path of the criterion-evidence contract at the
// legacy executor's pre-handoff plan (#249, #282): a body that names
// the trusted source issue and carries an honest partial evidence
// block passes both the linkage and criterion checks on the first
// turn; the run ends Verifying with the run.exit event detail carrying
// "criteria.coverage":"partial" and the per-status record counts so an
// operator audits the run's honesty against the linked criteria
// without the legacy handoff falsely claiming fully verified
// completion outside the PR body.
func TestRunResolveIssueCriteriaHonestPartialHandsOffVerifyingWithCoverage(t *testing.T) {
	body := readBrokerFixture(t, "miso-gallery-505-pr-honest-partial.md")
	envelope := pinCriteriaEnvelopeForMisoGallery(t)
	code, out, _, comments := runCriteriaEndToEndTracked(t, body, scriptCriteriaHonestPartialTurns("ses_criteria_honest_partial"), envelope, false)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 (Verifying after honest partial evidence); detail: %q", code, out)
	}
	if !strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", out)
	}
	if !strings.Contains(out, `"outcome":"changes"`) {
		t.Fatalf("Verifying termination should carry the declared changes outcome: %q", out)
	}
	if !strings.Contains(out, `"criteria.coverage":"partial"`) {
		t.Fatalf("run.exit event detail should carry the criteria.coverage=partial audit field: %q", out)
	}
	// Per-status record counts must ride the run.exit detail so
	// the operator audits the run's honesty against the linked
	// criteria (#249, #282). The honest-partial fixture carries a
	// regression-check row, a manual-verification row, and another
	// regression-check row, so both statuses surface in the
	// emitted counts.
	if !strings.Contains(out, `"criteria.status_counts"`) {
		t.Fatalf("run.exit event detail should carry criteria.status_counts: %q", out)
	}
	for _, status := range []string{"regression-check", "manual-verification"} {
		if !strings.Contains(out, status) {
			t.Fatalf("criteria.status_counts should surface %q: %q", status, out)
		}
	}
	// The criteria pass must not fire any continuation — the body
	// satisfies both contracts on the first turn.
	for _, kind := range []string{`"kind":"criteria-repair"`, `"kind":"linkage-repair"`, `"kind":"pr-missing"`, `"kind":"pr-draft"`, `"kind":"pr-unverified"`} {
		if strings.Contains(out, kind) {
			t.Fatalf("criteria pass must not fire continuation kind %s: %q", kind, out)
		}
	}
	// #282 step 4: the derived coverage and per-status counts reach the
	// run's source issue as an outcome comment as well as the run.exit
	// telemetry — the honest partial label must be visible outside the
	// PR body, and it must agree with the event detail.
	if len(comments) != 1 {
		t.Fatalf("passing pinned handoff must post exactly one coverage comment, got %d: %q", len(comments), comments)
	}
	for _, want := range []string{"criterion-evidence", "`partial`", "regression-check: 2", "manual-verification: 1"} {
		if !strings.Contains(comments[0], want) {
			t.Fatalf("coverage comment missing %q: %q", want, comments[0])
		}
	}
	if strings.Contains(comments[0], "`full`") {
		t.Fatalf("honest partial comment must not claim full: %q", comments[0])
	}
}

// TestRunResolveIssueEmptyPinnedSetSkipsCriteriaCheck pins the
// vacuous-pass branch of the criterion-evidence contract (#249, #282):
// when an issue has no acceptance section (or its admission produced
// an empty pinned set), the criterion check is a vacuous pass and the
// body faces only the linkage contract. A body that would otherwise
// fail criteria — here the deficient fixture with a Closes #502 prefix
// — still hands off Verifying because the criteria check is skipped
// entirely. No "criteria-repair" event is emitted and no
// "criteria.coverage" rides the run.exit event detail.
func TestRunResolveIssueEmptyPinnedSetSkipsCriteriaCheck(t *testing.T) {
	emptyEnvelope := `{"issue":{"owner":"misospace","name":"miso-gallery","number":502},"criteria":[],"digest":"sha256:empty-pin"}`
	body := withClosingReference(readBrokerFixture(t, "miso-gallery-505-pr-deficient.md"), "#502")
	code, out, _, comments := runCriteriaEndToEndTracked(t, body, scriptCriteriaHonestPartialTurns("ses_criteria_empty_set"), emptyEnvelope, false)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 (empty pinned set is vacuous; linkage holds); detail: %q", code, out)
	}
	if !strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", out)
	}
	if strings.Contains(out, `"kind":"criteria-repair"`) {
		t.Fatalf("empty pinned set must not fire criteria-repair continuation: %q", out)
	}
	if strings.Contains(out, `"criteria.coverage"`) {
		t.Fatalf("empty pinned set must not surface criteria.coverage on run.exit: %q", out)
	}
	// The coverage comment is a pinned-handoff surface: an empty pinned
	// set must post no outcome comment at all (#282).
	if len(comments) != 0 {
		t.Fatalf("empty pinned set must post no coverage comment, got %q", comments)
	}
}

// TestRunResolveIssueUnpinnedRunSkipsCriteriaCheck pins the
// absent-envelope branch of the criterion-evidence contract (#249,
// #282): with no COURIER_CRITERIA env var, the criterion check is
// skipped entirely and the body faces only the linkage contract.
// The deficient body (with a Closes #502 prefix so linkage passes)
// hands off Verifying with no criteria-repair event and no
// criteria.coverage in the run.exit detail.
func TestRunResolveIssueUnpinnedRunSkipsCriteriaCheck(t *testing.T) {
	body := withClosingReference(readBrokerFixture(t, "miso-gallery-505-pr-deficient.md"), "#502")
	code, out, _, comments := runCriteriaEndToEndTracked(t, body, scriptCriteriaHonestPartialTurns("ses_criteria_unpinned"), "", false)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 (no COURIER_CRITERIA at all; linkage holds); detail: %q", code, out)
	}
	if !strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("missing Verifying termination: %q", out)
	}
	if strings.Contains(out, `"kind":"criteria-repair"`) {
		t.Fatalf("absent COURIER_CRITERIA must not fire criteria-repair continuation: %q", out)
	}
	if strings.Contains(out, `"criteria.coverage"`) {
		t.Fatalf("absent COURIER_CRITERIA must not surface criteria.coverage on run.exit: %q", out)
	}
	// An unpinned run has no criterion contract to report: no coverage
	// comment may reach the source issue (#282).
	if len(comments) != 0 {
		t.Fatalf("absent COURIER_CRITERIA must post no coverage comment, got %q", comments)
	}
}

// TestRunResolveIssueCombinedBodyFiresSingleBodyRepair pins the
// combined-repair contract on the legacy pre-handoff plan (#282): a
// body that fails BOTH linkage (#248) and the criterion-evidence
// contract (#249) earns a single bounded continuation that names
// both defects, not two sequential repairs. The fixture
// (miso-gallery-505-pr-deficient.md) uses "Addresses #502" — a
// non-closing reference, so the linkage validator rejects it — and
// carries no criterion-evidence block, so the criterion validator
// also rejects it. With MaxContinuations=1, the run ends NeedsHuman
// after one combined body-repair continuation; the run.exit reason
// names both halves of the failure, and the executor never fires
// a separate "criteria-repair" or "linkage-repair" kind on the
// shared budget.
func TestRunResolveIssueCombinedBodyFiresSingleBodyRepair(t *testing.T) {
	source := "misospace/miso-gallery#502"
	// The deficient fixture has "Addresses #502" (not a closing
	// keyword) and no criterion block, so it fails both contracts
	// against the same live body the forge read returned.
	body := readBrokerFixture(t, "miso-gallery-505-pr-deficient.md")
	envelope := pinCriteriaEnvelopeForMisoGallery(t)
	code, out, _ := runCriteriaEndToEnd(t, body, scriptCriteriaDeficientTurns("ses_combined_body"), envelope)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman after combined-repair budget exhausts); detail: %q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman phase: %q", out)
	}
	if !strings.Contains(out, `"category":"verifier-rejection"`) {
		t.Fatalf("expected verifier-rejection category: %q", out)
	}
	if !strings.Contains(out, `"outcome":"changes"`) {
		t.Fatalf("declared changes outcome tag should be preserved: %q", out)
	}
	// The combined repair is a single bounded continuation that
	// names both defects: the body-repair kind for the shared
	// budget slot, with linkage and criteria vocabulary both
	// present in the same continuation message.
	if n := strings.Count(out, `"kind":"body-repair"`); n != 1 {
		t.Fatalf("expected exactly one body-repair continuation (combined); got %d in: %q", n, out)
	}
	// The combined repair must not split into separate
	// single-contract continuations.
	if strings.Contains(out, `"kind":"linkage-repair"`) {
		t.Fatalf("combined body-repair must not split into a separate linkage-repair event: %q", out)
	}
	if strings.Contains(out, `"kind":"criteria-repair"`) {
		t.Fatalf("combined body-repair must not split into a separate criteria-repair event: %q", out)
	}
	// The combined repair message quotes the linkage contract's
	// closing-keyword vocabulary and the criterion contract's
	// block header so one turn can fix both.
	if !strings.Contains(out, "Closes, Fixes, or Resolves") {
		t.Fatalf("combined repair must name the linkage contract: %q", out)
	}
	if !strings.Contains(out, "courier-criteria:v1") {
		t.Fatalf("combined repair must name the criterion contract: %q", out)
	}
	// The exhausted reason names both halves: linkage plus
	// criterion evidence, joined with " and ", and the trusted
	// source identity anchors the trailing parenthetical.
	if !strings.Contains(out, "incomplete handoff: linkage:") {
		t.Fatalf("exhausted reason must name the linkage half: %q", out)
	}
	if !strings.Contains(out, "criterion evidence") {
		t.Fatalf("exhausted reason must name the criterion half: %q", out)
	}
	if !strings.Contains(out, source) {
		t.Fatalf("exhausted reason must name the trusted source identity: %q", out)
	}
}

// TestPinnedCriterionSetBindsEnvelopeToTrustedSource pins the trust
// binding of the pinned criterion envelope (#282): an operator-authored
// COURIER_CRITERIA envelope that names a source issue other than the
// run's trusted COURIER_REPO/COURIER_REF identity must fail closed with
// criteriaEnvelopeIdentityErr — before the pinned-empty shortcut, so a
// foreign envelope cannot slip through as a vacuous pass — and the
// error must name both identities so the operator can tell which
// admission input was wrong. The matching case still decodes and
// validates normally (block missing on a matching non-empty set is a
// repairable CriteriaError, not an identity failure), and the repository
// identity is compared case-insensitively.
func TestPinnedCriterionSetBindsEnvelopeToTrustedSource(t *testing.T) {
	var output, errorsOut bytes.Buffer
	obs := &prLinkageObservation{Number: 12, State: "open", Body: "Closes #502"}
	needle := strings.Repeat("a", 64)
	emptyDigest := strings.Repeat("b", 64)
	cases := []struct{ name, envelope string }{
		{"foreign repo", `{"issue":{"owner":"misospace","name":"other-repo","number":502},"criteria":[{"id":"AC-1","text":"x","digest":"sha256:` + needle + `"}],"digest":"sha256:` + emptyDigest + `"}`},
		{"foreign number", `{"issue":{"owner":"misospace","name":"miso-gallery","number":503},"criteria":[{"id":"AC-1","text":"x","digest":"sha256:` + needle + `"}],"digest":"sha256:` + emptyDigest + `"}`},
		{"zero issue with empty set", `{"issue":{},"criteria":[],"digest":"sha256:` + emptyDigest + `"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := newReporter(&output, &errorsOut, config{
				Mode:     "resolve-issue",
				Repo:     "misospace/miso-gallery",
				Ref:      502,
				Criteria: tc.envelope,
			})
			set, err := report.pinnedCriterionSet()
			if !errors.Is(err, criteriaEnvelopeIdentityErr) {
				t.Fatalf("pinnedCriterionSet err = %v, want criteriaEnvelopeIdentityErr", err)
			}
			if set != nil {
				t.Fatalf("identity mismatch must not return a pinned set: %+v", set)
			}
			if !strings.Contains(err.Error(), "misospace/miso-gallery#502") {
				t.Fatalf("error must name the trusted source identity: %q", err.Error())
			}
		})
	}
	// A pinned envelope with no verifiable trusted identity at all (here
	// a fix-pr mode run) fails closed the same way: an unbound contract
	// is not enforced silently (#282).
	report := newReporter(&output, &errorsOut, config{
		Mode:     "fix-pr",
		Repo:     "misospace/miso-gallery",
		Ref:      502,
		Criteria: `{"issue":{"owner":"misospace","name":"miso-gallery","number":502},"criteria":[{"id":"AC-1","text":"x","digest":"sha256:` + needle + `"}],"digest":"sha256:` + emptyDigest + `"}`,
	})
	if set, err := report.pinnedCriterionSet(); !errors.Is(err, criteriaEnvelopeIdentityErr) || set != nil {
		t.Fatalf("pinned envelope without a trusted identity must fail closed, got set %+v err %v", set, err)
	}
	// Case-insensitive repository identity with an empty criteria list:
	// the identity binds, the set is empty, and the vacuous pass stands.
	report = newReporter(&output, &errorsOut, config{
		Mode:     "resolve-issue",
		Repo:     "misospace/miso-gallery",
		Ref:      502,
		Criteria: `{"issue":{"owner":"MisoSpace","name":"MISO-GALLERY","number":502},"criteria":[],"digest":"sha256:` + emptyDigest + `"}`,
	})
	set, err := report.pinnedCriterionSet()
	if err != nil || set == nil || !set.IsEmpty() {
		t.Fatalf("case-insensitive matching empty set must decode and bind, got set %+v err %v", set, err)
	}
	if outcome, err := report.checkCriterionEvidence(set, obs); err != nil || outcome != nil {
		t.Fatalf("case-insensitive matching empty set must vacuous-pass, got outcome %+v err %v", outcome, err)
	}
	// Matching non-empty envelope against a body with no evidence block:
	// the identity binds, the validator fires, and the failure is the
	// repairable CriteriaError shape, not the identity sentinel.
	report = newReporter(&output, &errorsOut, config{
		Mode:     "resolve-issue",
		Repo:     "misospace/miso-gallery",
		Ref:      502,
		Criteria: `{"issue":{"owner":"misospace","name":"miso-gallery","number":502},"criteria":[{"id":"AC-1","text":"x","digest":"sha256:` + needle + `"}],"digest":"sha256:` + emptyDigest + `"}`,
	})
	set, err = report.pinnedCriterionSet()
	if err != nil {
		t.Fatalf("matching envelope must decode, got err %v", err)
	}
	outcome, err := report.checkCriterionEvidence(set, obs)
	if err != nil {
		t.Fatalf("matching envelope must reach the validator, got err %v", err)
	}
	if outcome == nil || outcome.criteriaErr == nil {
		t.Fatalf("matching envelope with block-less body must produce a criteriaErr outcome, got %+v", outcome)
	}
}

// TestHandoffCoverageCommentRendersDerivedCoverage pins the outcome
// comment body the executor posts on a passing pinned handoff (#282,
// step 4): the derived coverage is named verbatim, per-status counts
// render in the broker vocabulary order and omit absent statuses, and
// the comment never claims fully verified completion.
func TestHandoffCoverageCommentRendersDerivedCoverage(t *testing.T) {
	got := handoffCoverageComment("run-9", broker.CoveragePartial, map[broker.CriterionStatus]int{
		broker.StatusManualVerification: 1,
		broker.StatusRegressionCheck:    2,
		broker.StatusNotImplemented:     0,
	})
	for _, want := range []string{"run-9", "`partial`", "regression-check: 2", "manual-verification: 1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("coverage comment missing %q: %q", want, got)
		}
	}
	if strings.Contains(got, "not-implemented") {
		t.Fatalf("zero-count statuses must be omitted: %q", got)
	}
	if strings.Contains(got, "full") {
		t.Fatalf("partial handoff comment must not claim full: %q", got)
	}
	if strings.Index(got, "regression-check") > strings.Index(got, "manual-verification") {
		t.Fatalf("counts must render in the broker vocabulary order: %q", got)
	}
	plain := handoffCoverageComment("run-9", broker.CoverageFull, nil)
	if !strings.Contains(plain, "`full`") || strings.Contains(plain, "Per-status") {
		t.Fatalf("full handoff without counts: %q", plain)
	}
}

// TestRunResolveIssueForeignCriterionEnvelopeFailsClosedWithoutRepair
// pins the envelope identity binding end to end (#282): a syntactically
// valid COURIER_CRITERIA envelope naming a different source issue than
// the run's trusted COURIER_REPO/COURIER_REF must terminate the run
// NeedsHuman (verifier-rejection) immediately — the linkage.read.failed
// event surfaces with kind "criteria-identity", the reason names the
// policy mismatch — and must never spend a bounded continuation, since
// no PR-body edit can fix a wrong pinned envelope.
func TestRunResolveIssueForeignCriterionEnvelopeFailsClosedWithoutRepair(t *testing.T) {
	body := withClosingReference(readBrokerFixture(t, "miso-gallery-505-pr-deficient.md"), "#502")
	foreign := `{"issue":{"owner":"misospace","name":"miso-gallery","number":503},"criteria":[{"id":"AC-1","text":"pinned for the wrong issue","digest":"sha256:` + strings.Repeat("a", 64) + `"}],"digest":"sha256:` + strings.Repeat("b", 64) + `"}`
	code, out, _ := runCriteriaEndToEnd(t, body, scriptCriteriaHonestPartialTurns("ses_foreign_envelope"), foreign)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman on envelope identity mismatch); detail: %q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman phase: %q", out)
	}
	if !strings.Contains(out, `"category":"verifier-rejection"`) {
		t.Fatalf("expected verifier-rejection category: %q", out)
	}
	if !strings.Contains(out, `"kind":"criteria-identity"`) {
		t.Fatalf("linkage.read.failed event must carry kind criteria-identity: %q", out)
	}
	if !strings.Contains(out, "policy mismatch") {
		t.Fatalf("reason must name the policy mismatch: %q", out)
	}
	// The identity mismatch is not model-repairable: no continuation of
	// any body-repair kind may fire, and the run must not claim
	// Verifying.
	for _, kind := range []string{`"kind":"criteria-repair"`, `"kind":"body-repair"`, `"kind":"linkage-repair"`} {
		if strings.Contains(out, kind) {
			t.Fatalf("identity mismatch must not fire continuation kind %s: %q", kind, out)
		}
	}
	if strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("identity mismatch must not hand off Verifying: %q", out)
	}
}

// TestRunResolveIssueUnbindableIdentityWithPinnedEnvelopeFailsClosed
// pins the other half of the envelope trust binding (#282): a
// resolve-issue run that carries a well-formed COURIER_CRITERIA envelope
// but no trusted source identity to bind it to (COURIER_REPO naming no
// owner/repo, so trustedSourceIssue is not ok) must not skip both body
// contracts and hand off as Verifying. checkHandoff fails the run closed
// on the same criteriaEnvelopeIdentityErr the bound check raises, so the
// run terminates NeedsHuman (verifier-rejection) with the
// linkage.read.failed event at kind "criteria-identity", spending no
// continuation of any kind and posting no coverage comment.
func TestRunResolveIssueUnbindableIdentityWithPinnedEnvelopeFailsClosed(t *testing.T) {
	body := withClosingReference(readBrokerFixture(t, "miso-gallery-505-pr-deficient.md"), "#502")
	envelope := pinCriteriaEnvelopeForMisoGallery(t)
	// COURIER_REPO names no owner/repo pair: the run has no trusted source
	// identity, so the pinned envelope cannot be bound to anything.
	code, out, _, comments := runCriteriaEndToEndTrackedAs(t, body, scriptCriteriaHonestPartialTurns("ses_unbindable_identity"), envelope, false, "miso-gallery", "502")
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman on an unbindable pinned envelope); detail: %q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman phase: %q", out)
	}
	if !strings.Contains(out, `"category":"verifier-rejection"`) {
		t.Fatalf("expected verifier-rejection category: %q", out)
	}
	if !strings.Contains(out, `"kind":"criteria-identity"`) {
		t.Fatalf("linkage.read.failed event must carry kind criteria-identity: %q", out)
	}
	if !strings.Contains(out, "policy mismatch") {
		t.Fatalf("reason must name the policy mismatch: %q", out)
	}
	if !strings.Contains(out, "cannot be bound") {
		t.Fatalf("reason must say the pinned envelope cannot be bound: %q", out)
	}
	// Zero continuations of any kind: the budget is untouched, and the
	// run neither claims Verifying nor reports coverage it never earned.
	if n := strings.Count(out, `"event":"executor.continuation"`); n != 0 {
		t.Fatalf("continuation count = %d, want 0 (an unbound envelope fails closed on sight): %q", n, out)
	}
	if strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("an unbindable pinned envelope must not hand off Verifying: %q", out)
	}
	if len(comments) != 0 {
		t.Fatalf("a failed-closed handoff must post no coverage comment, got %q", comments)
	}
}

// TestRunResolveIssueUndecodableCriterionEnvelopeFailsClosed pins the
// decode-failure ordering of the pre-handoff plan (#282): the pinned
// envelope is read once from the pod environment at process start, so a
// COURIER_CRITERIA value that does not decode can never become valid on a
// later observation. The run must therefore fail closed on sight — the
// linkage.read.failed event still surfaces with kind "criteria", but the
// run spends no continuation of the shared #170 budget and is never told
// the PR body is unverified, because the cause is the admission-time
// projection, not the body.
func TestRunResolveIssueUndecodableCriterionEnvelopeFailsClosed(t *testing.T) {
	body := withClosingReference(readBrokerFixture(t, "miso-gallery-505-pr-deficient.md"), "#502")
	undecodable := `{"issue":{"owner":"misospace","name":"miso-gallery","number":502},"criteria":[{"id":"AC-1"`
	code, out, _, comments := runCriteriaEndToEndTracked(t, body, scriptCriteriaHonestPartialTurns("ses_undecodable_envelope"), undecodable, false)
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d (NeedsHuman on an undecodable pinned envelope); detail: %q", code, exitNeedsHuman, out)
	}
	if !strings.Contains(out, `"phase":"NeedsHuman"`) {
		t.Fatalf("expected NeedsHuman phase: %q", out)
	}
	if !strings.Contains(out, `"category":"verifier-rejection"`) {
		t.Fatalf("expected verifier-rejection category: %q", out)
	}
	if !strings.Contains(out, `"kind":"criteria"`) {
		t.Fatalf("linkage.read.failed event must carry kind criteria: %q", out)
	}
	if strings.Contains(out, `"kind":"criteria-identity"`) {
		t.Fatalf("a decode failure must not be reported as an identity mismatch: %q", out)
	}
	if strings.Contains(out, `"kind":"linkage"`) {
		t.Fatalf("a decode failure must not be reported as a linkage read failure: %q", out)
	}
	// The reason names the decode fault, not a body defect, and the
	// decode error itself rides along.
	if !strings.Contains(out, "could not be decoded") {
		t.Fatalf("reason must name the undecodable envelope: %q", out)
	}
	if !strings.Contains(out, "decode COURIER_CRITERIA envelope") {
		t.Fatalf("reason must carry the decoder's own error: %q", out)
	}
	// The decode fault is not retryable: the captured session is never
	// asked to repair a PR body that the envelope, not the body, broke.
	if strings.Contains(out, "could not verify the pull-request state") {
		t.Fatalf("an undecodable envelope must not hand the model the PR-unverified repair message: %q", out)
	}
	if n := strings.Count(out, `"event":"executor.continuation"`); n != 0 {
		t.Fatalf("continuation count = %d, want 0 (an undecodable envelope spends no #170 slot): %q", n, out)
	}
	if strings.Contains(out, `"phase":"Verifying"`) {
		t.Fatalf("an undecodable pinned envelope must not hand off Verifying: %q", out)
	}
	if len(comments) != 0 {
		t.Fatalf("a failed-closed handoff must post no coverage comment, got %q", comments)
	}
}

// TestRunResolveIssueUndecodableCriterionEnvelopeSettlesBeforeObservation
// pins the *ordering* of the admission-time envelope check (#282): a
// COURIER_CRITERIA envelope that does not decode must fail closed on sight
// whatever the forge world looks like, including the worlds whose own
// bounded #247 outcome is a continuation. Classifying the observation
// first let a missing or draft PR spend continuation 1 of kind pr-missing
// / pr-draft on an admission-time fault no turn can repair, and only reach
// the decoder when the world happened to carry an open PR — the decode
// fault was invisible exactly where a slot could be wasted.
func TestRunResolveIssueUndecodableCriterionEnvelopeSettlesBeforeObservation(t *testing.T) {
	undecodable := `{"issue":{"owner":"misospace","name":"miso-gallery","number":502},"criteria":[{"id":"AC-1`
	body := withClosingReference(readBrokerFixture(t, "miso-gallery-505-pr-deficient.md"), "#502")
	worlds := []struct {
		name     string
		pulls    string
		session  string
		notFired string
	}{
		{"missing pull request", "", "ses_undecodable_missing_pr", `"kind":"pr-missing"`},
		{"draft pull request", misoGalleryPRFixtureJSON(12, "open", true, body), "ses_undecodable_draft_pr", `"kind":"pr-draft"`},
		{"closed pull request", misoGalleryPRFixtureJSON(12, "closed", false, body), "ses_undecodable_closed_pr", "only closed or merged"},
		// The forge answers the pulls list with HTTP 500: the observation
		// itself is unreadable. The envelope fault is settled first, so it
		// is never reported as a retryable linkage read failure.
		{"unreadable forge", "500", "ses_undecodable_forge_500", "could not verify the pull-request state"},
	}
	for _, world := range worlds {
		t.Run(world.name, func(t *testing.T) {
			code, out, _, comments := runCriteriaEndToEndPullRequestRecord(t, world.pulls,
				scriptCriteriaHonestPartialTurns(world.session), undecodable, false, "misospace/miso-gallery", "502")
			if code != exitNeedsHuman {
				t.Fatalf("run exit code = %d, want %d (NeedsHuman on an undecodable pinned envelope); detail: %q", code, exitNeedsHuman, out)
			}
			if !strings.Contains(out, `"phase":"NeedsHuman"`) || !strings.Contains(out, `"category":"verifier-rejection"`) {
				t.Fatalf("expected a fail-closed NeedsHuman verdict: %q", out)
			}
			// The envelope fault, not the observation, settles the run.
			if !strings.Contains(out, `"kind":"criteria"`) {
				t.Fatalf("linkage.read.failed event must carry kind criteria: %q", out)
			}
			if strings.Contains(out, `"kind":"criteria-identity"`) || strings.Contains(out, `"kind":"linkage"`) {
				t.Fatalf("a decode failure must not be reported as another read class: %q", out)
			}
			if !strings.Contains(out, "could not be decoded") {
				t.Fatalf("reason must name the undecodable envelope: %q", out)
			}
			// The observation's own bounded outcome never fires, so no
			// #170 slot is spent on the admission-time fault.
			if strings.Contains(out, world.notFired) {
				t.Fatalf("the envelope fault must settle the run before %s: %q", world.notFired, out)
			}
			if n := strings.Count(out, `"event":"executor.continuation"`); n != 0 {
				t.Fatalf("continuation count = %d, want 0 (an undecodable envelope spends no #170 slot): %q", n, out)
			}
			if strings.Contains(out, `"phase":"Verifying"`) {
				t.Fatalf("an undecodable pinned envelope must not hand off Verifying: %q", out)
			}
			if len(comments) != 0 {
				t.Fatalf("a failed-closed handoff must post no coverage comment, got %q", comments)
			}
		})
	}
}

// TestRunResolveIssueCoverageCommentFailureDoesNotBlockHandoff pins the
// best-effort semantics of the handoff coverage comment (#282, step 4):
// when the forge rejects the comment, the run records the
// outcome.comment error and still hands off Verifying with the same
// telemetry.
func TestRunResolveIssueCoverageCommentFailureDoesNotBlockHandoff(t *testing.T) {
	body := readBrokerFixture(t, "miso-gallery-505-pr-honest-partial.md")
	envelope := pinCriteriaEnvelopeForMisoGallery(t)
	code, out, _, comments := runCriteriaEndToEndTracked(t, body, scriptCriteriaHonestPartialTurns("ses_coverage_comment_fail"), envelope, true)
	if code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0 (comment failure must not block the handoff); detail: %q", code, out)
	}
	if len(comments) != 0 {
		t.Fatalf("no comment may land when the forge rejects them: %q", comments)
	}
	if !strings.Contains(out, `"outcome.comment"`) {
		t.Fatalf("failed comment must surface an outcome.comment event: %q", out)
	}
	if !strings.Contains(out, `"phase":"Verifying"`) || !strings.Contains(out, `"criteria.coverage":"partial"`) {
		t.Fatalf("handoff and telemetry must stand despite the comment failure: %q", out)
	}
}
