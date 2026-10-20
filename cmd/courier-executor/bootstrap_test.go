package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/devenv"
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

// contractFixtureConfig is the parseable contract fixture: a
// [tasks.bootstrap] task referencing a script file the repository carries at
// both compared revisions, so the classification can pin it.
const contractFixtureConfig = "[tasks.bootstrap]\nrun = 'hack/bootstrap.sh'\n"

// remoteWithApprovedContract builds the resolve-issue fixture remote whose
// base (main) and run branch both carry the identical contract, so the
// checked-out workspace classifies approved — and, under the legacy gate-off
// (#414 review), is skipped without executing anything.
func remoteWithApprovedContract(t *testing.T, root string) string {
	t.Helper()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	if err := os.MkdirAll(filepath.Join(source, "hack"), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(source, "hack", "bootstrap.sh"), "#!/bin/sh\necho bootstrapped\n")
	bootstrapContract(t, source, contractFixtureConfig, "lock\n")
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

// approvedSkipReason is the fixed skip reason the legacy executor emits when
// the contract classifies approved (#414 review): the credential-bearing
// legacy executor never executes repository contract code.
const approvedSkipReason = "bootstrap contract approved but not executed: the legacy executor is credential-bearing; the approved bootstrap runs only in the secure worker topology"

// TestRunSkipsApprovedContractWithoutExecuting drives run() end to end with
// an approved contract: the legacy executor is credential-bearing and must
// never execute repository contract code, so the run proceeds
// unbootstrapped — the scripted mise stand-in is never invoked — and the
// receipt names exactly that (#414 review).
func TestRunSkipsApprovedContractWithoutExecuting(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithApprovedContract(t, root)

	// A mise stand-in shadows whatever the host provides and leaves a marker
	// if anything ever invokes it: the whole point is that nothing does.
	miseBin := filepath.Join(root, "mise-bin")
	if err := os.MkdirAll(miseBin, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(miseBin, "mise"), "#!/bin/sh\ntouch "+filepath.Join(root, "mise-invoked")+"\nexit 3\n")
	t.Setenv("PATH", miseBin+string(os.PathListSeparator)+os.Getenv("PATH"))

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
	if _, err := os.Stat(filepath.Join(root, "mise-invoked")); !os.IsNotExist(err) {
		t.Fatal("the legacy executor executed the approved contract despite being credential-bearing")
	}
	events := parseEvents(t, &output)
	bootstrap := findEvent(t, events, "workspace.bootstrap")
	detail, _ := bootstrap["detail"].(map[string]any)
	if detail["status"] != "skipped" {
		t.Fatalf("workspace.bootstrap status = %v, want skipped", detail["status"])
	}
	if reason, _ := detail["reason"].(string); reason != approvedSkipReason {
		t.Fatalf("workspace.bootstrap reason = %q, want the fixed approved-skip reason %q", reason, approvedSkipReason)
	}
}

// TestRunSkipsApprovedContractOnFork drives run() end to end in the fork
// topology: the run branch carries the base-approved contract, the fork's
// own main does not, and the receipt must carry the approved-skip reason —
// the base comparison must resolve through the base remote, never the bare
// branch name, or this run would report the modified-skip reason instead
// (#414 review).
func TestRunSkipsApprovedContractOnFork(t *testing.T) {
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
	if err := os.MkdirAll(filepath.Join(source, "hack"), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(source, "hack", "bootstrap.sh"), "#!/bin/sh\necho bootstrapped\n")
	bootstrapContract(t, source, contractFixtureConfig, "lock\n")
	commit(t, source, "base: contract")
	runGit(t, source, "remote", "add", "origin", upstream)
	runGit(t, source, "push", "origin", "main")
	runGit(t, source, "checkout", "-b", "courier/resolve-issue/acme-widgets/7")
	write(t, filepath.Join(source, "work.txt"), "previous brief\n")
	commit(t, source, "work: previous brief")
	runGit(t, source, "push", "fork", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")

	prServer, prState := fakeForgePullRequestServer(t)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf 'opencode ran\\n'\nprintf 'completed\\n' > completed.txt\ngit add --all -- .\ngit commit -m 'test: completed work' >/dev/null\ngit push origin \"$COURIER_BRANCH\" >/dev/null 2>&1\n"+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The completed turn opened the pull request.")))

	miseBin := filepath.Join(root, "mise-bin")
	if err := os.MkdirAll(miseBin, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(miseBin, "mise"), "#!/bin/sh\ntouch "+filepath.Join(root, "mise-invoked")+"\nexit 3\n")
	t.Setenv("PATH", miseBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	setResolveIssueEnv(t, root, fork, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_BASE_REPO_URL", upstream)
	t.Setenv("COURIER_GIT_TOKEN", testGitToken)
	t.Setenv("GITHUB_TOKEN", testGitHubTokn)
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("COURIER_REF", "7")

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want %d; stderr=%q", code, exitSuccess, errorsOut.String())
	}
	if _, err := os.Stat(filepath.Join(root, "mise-invoked")); !os.IsNotExist(err) {
		t.Fatal("the legacy executor executed the approved contract despite being credential-bearing")
	}
	events := parseEvents(t, &output)
	bootstrap := findEvent(t, events, "workspace.bootstrap")
	detail, _ := bootstrap["detail"].(map[string]any)
	if reason, _ := detail["reason"].(string); reason != approvedSkipReason {
		t.Fatalf("workspace.bootstrap reason = %q, want the approved-skip reason %q", reason, approvedSkipReason)
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
	if reason, _ := detail["reason"].(string); !strings.Contains(reason, "not provably identical") {
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
