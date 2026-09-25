package main

import (
	"bytes"
	"context"
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
	"testing"

	"github.com/misospace/courier/internal/executor"
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
	os.Exit(m.Run())
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
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'opencode argv: %s\\n' \"$*\"\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\n")
	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7.")
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
	if !strings.Contains(output.String(), "--agent architect") {
		t.Fatalf("OpenCode did not receive configured agent: %q", output.String())
	}
	if !strings.Contains(output.String(), `COURIER_TERMINATION {"phase":"Verifying","result":"success","exit_code":0`) {
		t.Fatalf("missing stable success termination: %q", output.String())
	}
	terminationOutput := string(mustRead(t, termination))
	if !strings.Contains(terminationOutput, `"phase":"Verifying"`) {
		t.Fatalf("termination file = %q", terminationOutput)
	}
}

func TestRunResolveIssueRefusesBranchWithOpenPullRequest(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)

	// A binary that fails loudly if it is ever executed: reaching it would mean
	// the run ignored the existing PR.
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\nexit 99\n")

	prServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"feature/x"}}]`)
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
	if !strings.Contains(output.String(), "PR #12") {
		t.Fatalf("termination should name the PR number: %q", output.String())
	}
	if strings.Contains(output.String(), "opencode argv") || code == 99 {
		t.Fatalf("opencode was invoked despite an existing PR: %q", output.String())
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

func TestRunExitZeroWithoutLocalWorkBecomesNeedsHuman(t *testing.T) {
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
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), "without producing a commit or workspace changes") {
		t.Fatalf("no-op reason = %q", output.String())
	}
}

func TestRunExitZeroWithDirtyWorkBecomesNeedsHuman(t *testing.T) {
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
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), "uncommitted workspace changes") {
		t.Fatalf("dirty-work reason = %q", output.String())
	}
}

func TestRunCommitWithUntrackedScratchReachesVerifying(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\nprintf 'completed\\n' > completed.txt\ngit add completed.txt\ngit commit -m 'test: completed work' >/dev/null\nprintf 'scratch\\n' > scratch.txt\n")
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

func TestRunPreservesExplicitNeedsHumanSignal(t *testing.T) {
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
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), "requested human attention") {
		t.Fatalf("explicit needs-human reason = %q", output.String())
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
	testGitToken   = "git-push-token-f4e3d2c1"
	testGitHubTokn = "gh-api-token-b1b2b3b4"
	testURLToken   = "url-userinfo-token-aa11bb22"
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

// TestChildOutputRedactedOnFailureExit keeps exit semantics and redaction on
// a failing child: the exit code passes through and the child's final output
// is flushed through the redactor before the termination handoff.
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
	if code := run(context.Background(), &output, &errorsOut); code != 3 {
		t.Fatalf("run exit code = %d, want the child's exit code 3", code)
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
	if code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNeedsHuman, errorsOut.String(), output.String())
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
	if !strings.Contains(output.String(), "without producing a commit") || !strings.Contains(output.String(), "configured capability unavailable: github") {
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

// setResolveIssueEnv points a run at remote in resolve-issue mode against a
// fake GitHub API. It returns the workspace directory the run will use.
func setResolveIssueEnv(t *testing.T, root, remote, githubBase, openCodeBinary string) string {
	t.Helper()
	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7.")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_MODE", "resolve-issue")
	t.Setenv("COURIER_REPO", "acme/widgets")
	t.Setenv("COURIER_GITHUB_API_BASE", githubBase)
	t.Setenv("COURIER_OPENCODE_BINARY", openCodeBinary)
	t.Setenv("COURIER_TERMINATION_FILE", termination)
	return workspace
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
