package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/devenv"
	"github.com/misospace/courier/internal/executor"
	couriergit "github.com/misospace/courier/internal/git"
)

// bootstrapTestRepo initializes a fresh local repository on main with one
// empty initial commit, reusing the suite's git helpers.
func bootstrapTestRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	runGit(t, dir, "init", "-b", "main")
	configureGit(t, dir)
	write(t, filepath.Join(dir, "README.md"), "seed\n")
	commit(t, dir, "base: initial")
	return dir
}

// bootstrapContract writes the development bootstrap contract files into the
// repository; an empty lock means the lock file is absent.
func bootstrapContract(t *testing.T, dir, config, lock string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, ".mise"), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, devenv.ConfigPath), config)
	if lock != "" {
		write(t, filepath.Join(dir, devenv.LockPath), lock)
	}
}

// bootstrapHead returns the repository's current HEAD OID.
func bootstrapHead(t *testing.T, dir string) string {
	t.Helper()
	out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").CombinedOutput()
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v (%s)", err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestBootstrapContractStateAbsentWithoutContract(t *testing.T) {
	dir := bootstrapTestRepo(t)
	head := bootstrapHead(t, dir)
	state, err := bootstrapContractState(context.Background(), dir, "main", head)
	if err != nil {
		t.Fatalf("bootstrapContractState() error = %v", err)
	}
	if state != devenv.StateAbsent {
		t.Fatalf("bootstrapContractState() = %q, want %q", state, devenv.StateAbsent)
	}
}

func TestBootstrapContractStateApprovedIdenticalBaseAndHead(t *testing.T) {
	dir := bootstrapTestRepo(t)
	bootstrapContract(t, dir, "[tools]\n", "lock-v1\n")
	commit(t, dir, "base: contract")
	base := bootstrapHead(t, dir)
	write(t, filepath.Join(dir, "README.md"), "unrelated work\n")
	commit(t, dir, "work: unrelated")
	head := bootstrapHead(t, dir)
	state, err := bootstrapContractState(context.Background(), dir, base, head)
	if err != nil {
		t.Fatalf("bootstrapContractState() error = %v", err)
	}
	if state != devenv.StateApproved {
		t.Fatalf("bootstrapContractState() = %q, want %q", state, devenv.StateApproved)
	}
}

func TestBootstrapContractStateModifiedConfigChanged(t *testing.T) {
	dir := bootstrapTestRepo(t)
	bootstrapContract(t, dir, "[tools]\n", "lock-v1\n")
	commit(t, dir, "base: contract")
	base := bootstrapHead(t, dir)
	bootstrapContract(t, dir, "[tools]\nnode = '22'\n", "lock-v1\n")
	commit(t, dir, "work: contract changed")
	head := bootstrapHead(t, dir)
	state, err := bootstrapContractState(context.Background(), dir, base, head)
	if err != nil {
		t.Fatalf("bootstrapContractState() error = %v", err)
	}
	if state != devenv.StateModified {
		t.Fatalf("bootstrapContractState() = %q, want %q", state, devenv.StateModified)
	}
}

func TestBootstrapContractStateModifiedLockChanged(t *testing.T) {
	dir := bootstrapTestRepo(t)
	bootstrapContract(t, dir, "[tools]\n", "lock-v1\n")
	commit(t, dir, "base: contract")
	base := bootstrapHead(t, dir)
	bootstrapContract(t, dir, "[tools]\n", "lock-v2\n")
	commit(t, dir, "work: lock changed")
	head := bootstrapHead(t, dir)
	state, err := bootstrapContractState(context.Background(), dir, base, head)
	if err != nil {
		t.Fatalf("bootstrapContractState() error = %v", err)
	}
	if state != devenv.StateModified {
		t.Fatalf("bootstrapContractState() = %q, want %q", state, devenv.StateModified)
	}
}

func TestBootstrapContractStateModifiedContractOnlyAtHead(t *testing.T) {
	dir := bootstrapTestRepo(t)
	base := bootstrapHead(t, dir)
	bootstrapContract(t, dir, "[tools]\n", "lock-v1\n")
	commit(t, dir, "work: contract added")
	head := bootstrapHead(t, dir)
	state, err := bootstrapContractState(context.Background(), dir, base, head)
	if err != nil {
		t.Fatalf("bootstrapContractState() error = %v", err)
	}
	if state != devenv.StateModified {
		t.Fatalf("bootstrapContractState() = %q, want %q", state, devenv.StateModified)
	}
}

func TestBootstrapContractStateUnresolvableBaseReturnsError(t *testing.T) {
	dir := bootstrapTestRepo(t)
	bootstrapContract(t, dir, "[tools]\n", "lock-v1\n")
	commit(t, dir, "work: contract")
	head := bootstrapHead(t, dir)
	state, err := bootstrapContractState(context.Background(), dir, "no-such-revision", head)
	if err == nil {
		t.Fatal("bootstrapContractState() error = nil, want unresolvable base error")
	}
	if state != "" {
		t.Fatalf("bootstrapContractState() state = %q on error, want the meaningless zero State", state)
	}
}

func TestBootstrapBlockerTerminationShape(t *testing.T) {
	result := devenv.Result{Status: "failed", Reason: "bootstrap task exited with status 3", ExitCode: 3}
	term := bootstrapBlockerTermination(result)
	if term.Phase != "NeedsHuman" {
		t.Errorf("Phase = %q, want NeedsHuman", term.Phase)
	}
	if term.Result != "needs-human" {
		t.Errorf("Result = %q, want needs-human", term.Result)
	}
	if term.ExitCode != exitNeedsHuman {
		t.Errorf("ExitCode = %d, want %d", term.ExitCode, exitNeedsHuman)
	}
	if term.Outcome != outcomeBlockedExternal {
		t.Errorf("Outcome = %q, want %q", term.Outcome, outcomeBlockedExternal)
	}
	if term.Category != terminalCategoryBlockedExternal {
		t.Errorf("Category = %q, want %q", term.Category, terminalCategoryBlockedExternal)
	}
	if want := "workspace bootstrap failed: bootstrap task exited with status 3"; term.Reason != want {
		t.Errorf("Reason = %q, want %q", term.Reason, want)
	}
}

func TestApplyBootstrapEnvLeavesProtectedKeysUntouched(t *testing.T) {
	t.Setenv("PATH", "/original/bin")
	t.Setenv("COURIER_MODEL", "original")
	t.Setenv("GIT_CONFIG_COUNT", "0")
	t.Setenv("IFS", "original")
	t.Setenv("LD_PRELOAD", "original")
	applyBootstrapEnv([]string{
		"PATH=/minted/bin",
		"COURIER_MODEL=pwned",
		"GIT_CONFIG_COUNT=9",
		"IFS=pwned",
		"LD_PRELOAD=/pwned.so",
		"MYTOOL_VERSION=1.2.3",
	})
	for name, want := range map[string]string{
		"PATH":             "/minted/bin",
		"COURIER_MODEL":    "original",
		"GIT_CONFIG_COUNT": "0",
		"IFS":              "original",
		"LD_PRELOAD":       "original",
		"MYTOOL_VERSION":   "1.2.3",
	} {
		if got := os.Getenv(name); got != want {
			t.Errorf("os.Getenv(%q) = %q, want %q", name, got, want)
		}
	}
}

// remoteWithApprovedContract builds the resolve-issue fixture remote whose
// base (main) and run branch both carry the identical contract, so the
// checked-out workspace classifies approved and the bootstrap executes.
func remoteWithApprovedContract(t *testing.T, root string) string {
	t.Helper()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	bootstrapContract(t, source, "[tasks]\nbootstrap = 'echo bootstrapped'\n", "lock\n")
	commit(t, source, "base: contract")
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

// TestRunBlocksExternalWhenApprovedBootstrapFails drives run() end to end
// with an approved contract and a mise that fails deterministically on every
// host: the failure is a blocked-external NeedsHuman handoff, and the model
// process never launches — no retry loop, no scavenger hunt (#414).
func TestRunBlocksExternalWhenApprovedBootstrapFails(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithApprovedContract(t, root)

	// A failing mise shadows whatever the host provides, so the approved
	// contract's deterministic failure does not depend on the host's PATH.
	failingBin := filepath.Join(root, "failing-bin")
	if err := os.MkdirAll(failingBin, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(failingBin, "mise"), "#!/bin/sh\nexit 3\n")
	t.Setenv("PATH", failingBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	prServer, _ := fakeForgePullRequestServer(t)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ntouch never-launched\n")

	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_GIT_TOKEN", testGitToken)
	t.Setenv("GITHUB_TOKEN", testGitHubTokn)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q", code, exitNeedsHuman, errorsOut.String())
	}
	if _, err := os.Stat(filepath.Join(root, "workspace", "never-launched")); !os.IsNotExist(err) {
		t.Fatal("the model process launched before the bootstrap blocker ended the run")
	}
	raw, err := os.ReadFile(filepath.Join(root, "termination"))
	if err != nil {
		t.Fatalf("read termination file: %v", err)
	}
	line := strings.TrimPrefix(strings.TrimSpace(string(raw)), executor.TerminationLinePrefix)
	var message struct {
		Phase    string `json:"phase"`
		Result   string `json:"result"`
		ExitCode int    `json:"exit_code"`
		Outcome  string `json:"outcome"`
		Category string `json:"category"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(line), &message); err != nil {
		t.Fatalf("termination line = %q: %v", line, err)
	}
	if message.Phase != "NeedsHuman" || message.Result != "needs-human" || message.ExitCode != exitNeedsHuman ||
		message.Outcome != outcomeBlockedExternal || message.Category != terminalCategoryBlockedExternal {
		t.Fatalf("termination = %+v, want the blocked-external handoff", message)
	}
	if !strings.Contains(message.Reason, "workspace bootstrap failed") {
		t.Fatalf("termination reason = %q, want the bootstrap failure", message.Reason)
	}
	events := parseEvents(t, &output)
	bootstrap := findEvent(t, events, "workspace.bootstrap")
	detail, _ := bootstrap["detail"].(map[string]any)
	if detail["status"] != "failed" {
		t.Fatalf("workspace.bootstrap status = %v, want failed", detail["status"])
	}
	if detail["exit_code"] != float64(3) {
		t.Fatalf("workspace.bootstrap exit_code = %v, want 3", detail["exit_code"])
	}
}

// TestRunApprovesContractAgainstUpstreamOnFork drives run() end to end in
// the fork topology: the run branch carries the base-approved contract, the
// fork's own main does not, and the approved contract therefore still runs —
// the base comparison must resolve through the base remote, never the bare
// branch name (#414 review).
func TestRunApprovesContractAgainstUpstreamOnFork(t *testing.T) {
	root := t.TempDir()
	upstream := filepath.Join(root, "upstream.git")
	fork := filepath.Join(root, "fork.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", upstream)
	runGit(t, root, "init", "--bare", fork)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "fork", fork)
	runGit(t, source, "push", "fork", "HEAD:refs/heads/main")
	bootstrapContract(t, source, "[tasks]\nbootstrap = 'echo bootstrapped'\n", "lock\n")
	commit(t, source, "base: contract")
	runGit(t, source, "remote", "add", "origin", upstream)
	runGit(t, source, "push", "origin", "main")
	runGit(t, source, "checkout", "-b", "courier/resolve-issue/acme-widgets/7")
	write(t, filepath.Join(source, "work.txt"), "previous brief\n")
	commit(t, source, "work: previous brief")
	runGit(t, source, "push", "fork", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")

	prServer, _ := fakeForgePullRequestServer(t)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ntouch never-launched\n")

	failingBin := filepath.Join(root, "failing-bin")
	if err := os.MkdirAll(failingBin, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(failingBin, "mise"), "#!/bin/sh\nexit 3\n")
	t.Setenv("PATH", failingBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	setResolveIssueEnv(t, root, fork, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_BASE_REPO_URL", upstream)
	t.Setenv("COURIER_GIT_TOKEN", testGitToken)
	t.Setenv("GITHUB_TOKEN", testGitHubTokn)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNeedsHuman {
		t.Fatalf("run exit code = %d, want %d; stderr=%q", code, exitNeedsHuman, errorsOut.String())
	}
	raw, err := os.ReadFile(filepath.Join(root, "termination"))
	if err != nil {
		t.Fatalf("read termination file: %v", err)
	}
	if !strings.Contains(string(raw), outcomeBlockedExternal) {
		t.Fatalf("termination = %q, want the approved contract's blocked-external handoff", string(raw))
	}
}

// TestRunSkipsModifiedContractAndProceeds drives run() end to end with a
// contract the run branch modified away from its base: the untrusted
// modification is skipped, never executed, and the model work proceeds
// normally (#414).
func TestRunSkipsModifiedContractAndProceeds(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithApprovedContract(t, root)

	orphan := filepath.Join(root, "orphan")
	bootstrapContract(t, orphan, "[tasks]\nbootstrap = 'echo model-authored'\n", "lock\n")
	commit(t, orphan, "work: modify contract")
	runGit(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")

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
		t.Fatalf("run exit code = %d, want %d; stderr=%q", code, exitSuccess, errorsOut.String())
	}
	events := parseEvents(t, &output)
	bootstrap := findEvent(t, events, "workspace.bootstrap")
	detail, _ := bootstrap["detail"].(map[string]any)
	if detail["status"] != "skipped" {
		t.Fatalf("workspace.bootstrap status = %v, want skipped", detail["status"])
	}
	if reason, _ := detail["reason"].(string); !strings.Contains(reason, "differs") {
		t.Fatalf("workspace.bootstrap reason = %q, want the modified-contract skip", reason)
	}
	for _, event := range events {
		if event["event"] == "executor.start" {
			return
		}
	}
	t.Fatal("the run did not proceed to the model process after skipping the modified contract")
}

// TestBaseRevisionRefAnchorsBaseApprovalToForkTopology pins the fork-topology
// anchor fix: the contract's base comparison must resolve through the base
// remote's ref, never the bare branch name, which on a fork workspace is the
// fork's own branch (#414 review).
func TestBaseRevisionRefAnchorsBaseApprovalToForkTopology(t *testing.T) {
	repo := bootstrapTestRepo(t)
	fork := &couriergit.Workspace{
		Directory:     repo,
		RemoteURL:     "https://fork.example/fork.git",
		BaseRemoteURL: "https://upstream.example/upstream.git",
		Base:          "main",
	}
	if got := fork.BaseRevisionRef(); got != "upstream/main" {
		t.Fatalf("BaseRevisionRef() = %q, want upstream/main", got)
	}
	sameRepo := &couriergit.Workspace{Directory: repo, RemoteURL: "https://upstream.example/upstream.git", Base: "main"}
	if got := sameRepo.BaseRevisionRef(); got != "origin/main" {
		t.Fatalf("BaseRevisionRef() = %q, want origin/main", got)
	}
}
