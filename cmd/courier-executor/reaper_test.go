//go:build linux

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	// reaperHelperEnv names the helper mode a re-executed copy of this test
	// binary should run; absent, the helper returns immediately. It cannot
	// start with COURIER_: TestMain unsets every COURIER_* variable so no live
	// pod's configuration leaks into a run.
	reaperHelperEnv  = "TEST_REAPER_HELPER_MODE"
	reaperModeFull   = "full"
	reaperModePIDOne = "pid1"
	// Every wait in these tests is bounded: adoption and collection are the
	// claims under test, and an unproven claim is a failure, not a hang.
	reaperAdoptLimit = 5 * time.Second
	reaperReapLimit  = 5 * time.Second
	reaperPoll       = 20 * time.Millisecond
)

// runReaperHelper re-executes this test binary as the reaper's helper and
// returns everything it printed.
func runReaperHelper(t *testing.T, mode string, attr *syscall.SysProcAttr) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestAdoptedChildReaperHelper", "-test.count=1")
	cmd.Env = append(os.Environ(), reaperHelperEnv+"="+mode)
	cmd.SysProcAttr = attr
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

// errnoName renders the errno behind err, so a skipped environment restriction
// says which restriction it was.
func errnoName(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	return "no errno in " + err.Error()
}

// TestAdoptedChildReaperKeepsSupervisedStatusesAndReapsOrphans proves the two
// halves of #347 that pull against each other: a supervised child keeps the
// exit status os/exec produced for it, while the descendants it orphans are
// collected instead of piling up as zombies.
func TestAdoptedChildReaperKeepsSupervisedStatusesAndReapsOrphans(t *testing.T) {
	out, err := runReaperHelper(t, reaperModeFull, nil)
	if err != nil {
		t.Fatalf("reaper helper failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "reaped=") {
		t.Fatalf("reaper helper printed no reaped=<n> tally:\n%s", out)
	}
}

// TestAdoptedChildReaperAsPIDOne runs the same helper as PID 1 of a fresh PID
// namespace, which is the production shape: the kernel reparents orphans to
// PID 1 on its own, with no subreaper flag involved. An environment that
// forbids the namespace is skipped, never failed.
func TestAdoptedChildReaperAsPIDOne(t *testing.T) {
	attr := &syscall.SysProcAttr{
		Cloneflags:   syscall.CLONE_NEWPID | syscall.CLONE_NEWUSER,
		Unshareflags: syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Getuid(), Size: 1},
		},
		GidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Getgid(), Size: 1},
		},
		GidMappingsEnableSetgroups: false,
	}
	out, err := runReaperHelper(t, reaperModePIDOne, attr)
	if err != nil && (errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EINVAL)) {
		t.Skipf("cannot run the helper as PID 1 of a new PID namespace: %s (%v)", errnoName(err), err)
	}
	if err != nil {
		t.Fatalf("reaper helper as PID 1 failed: %v\n%s", err, out)
	}
}

// TestAdoptedChildReaperHelper is the reaper's contract, run in a process of
// its own so the subreaper flag and the adopted orphans are really its own.
func TestAdoptedChildReaperHelper(t *testing.T) {
	mode := os.Getenv(reaperHelperEnv)
	if mode == "" {
		return
	}
	if mode == reaperModePIDOne && os.Getpid() != 1 {
		t.Fatalf("helper is pid %d, not 1: the PID namespace was not obtained", os.Getpid())
	}
	reaper := installAdoptedChildReaper()
	dir := t.TempDir()

	reaperKeepsExitStatus(t, reaper, filepath.Join(dir, "exiting.pid"))
	reaperKeepsSignalStatus(t, reaper)
	if mode == reaperModeFull {
		reaperSurvivesChurn(t, reaper)
	}
	// Last of the claims about descendants, and deliberately so: it leaves a
	// zombie behind on purpose, which the churn check above would rightly call
	// a leak.
	unwatched := reaperLeavesAnotherWaitersChildAlone(t, filepath.Join(dir, "unwatched.pid"))
	live := reaperLeavesLiveOrphanAlone(t, reaper, filepath.Join(dir, "live.pid"))

	// Nothing outlives the helper: the live orphan is killed here and the
	// reaper is what collects it, so the test leaves no process of its own.
	if err := syscall.Kill(live, syscall.SIGKILL); err != nil {
		t.Fatalf("kill live orphan %d: %v", live, err)
	}
	before := reaper.reaped.Load()
	waitForReaped(t, reaper, before+1)
	// The orphan the reaper declined to collect is this process's own to take,
	// once it has proved it stays a zombie.
	waitForCollectedByWait(t, unwatched)
	waitForNoDescendants(t)
	fmt.Printf("reaped=%d\n", reaper.reaped.Load())
}

// waitForCollectedByWait collects, as an ordinary parent would, a descendant
// the reaper deliberately left alone.
func waitForCollectedByWait(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(reaperReapLimit)
	for {
		var status syscall.WaitStatus
		if collected, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil); err != nil || collected > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant %d was never collected by an ordinary wait", pid)
		}
		time.Sleep(reaperPoll)
	}
}

// reaperChild builds a supervised command the way the run builds its model
// child: in its own process group. That is what makes the descendants it leaves
// behind recognizable as its own once it is gone, and so collectable.
func reaperChild(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

// reaperLeavesAnotherWaitersChildAlone is the claim the whole design exists to
// make, tested without a race: a child this process starts the way internal/git
// and the evidence capture start theirs — outside the funnel, in no process
// group of its own — is not collectable, so its exit status survives for the
// waiter that owns it, and the grandchild it orphans stays put rather than being
// taken from that waiter too. A reaper that collected either would replace a real
// exit status with ECHILD and change a run's ending.
func reaperLeavesAnotherWaitersChildAlone(t *testing.T, pidPath string) int {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", `sleep 300 & echo $! > "$1"; exit 9`, "sh", pidPath)
	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("unfunnelled child error = %v, want exit status 9", err)
	}
	if code := exitErr.ExitCode(); code != 9 {
		t.Fatalf("unfunnelled child exit code = %d, want 9: the reaper stole it (%v)", code, err)
	}

	orphan := readPID(t, pidPath)
	t.Cleanup(func() { bestEffortSignal("KILL", orphan) })
	if state := waitForAdoption(t, orphan); state == "Z" {
		t.Fatalf("orphan %d of an unfunnelled child is a zombie while alive", orphan)
	}

	// Die and stay dead: this orphan's group is the reaper's own, which no
	// supervised child led, so the reaper must leave the zombie exactly where
	// it is. The helper collects it itself at the end.
	if err := syscall.Kill(orphan, syscall.SIGKILL); err != nil {
		t.Fatalf("kill orphan %d: %v", orphan, err)
	}
	deadline := time.Now().Add(reaperReapLimit)
	for {
		state, _, present := reaperProcStat(orphan)
		if state == "Z" {
			return orphan
		}
		if !present {
			t.Fatalf("orphan %d of an unfunnelled child was collected: the reaper took a status another waiter owns", orphan)
		}
		if time.Now().After(deadline) {
			t.Fatalf("orphan %d of an unfunnelled child never reached the zombie state it must keep (state %q)", orphan, state)
		}
		time.Sleep(reaperPoll)
	}
}

// reaperKeepsExitStatus runs a supervised child that orphans a child of its
// own and exits 7: the exit status must survive the reaper, and the orphan
// must be adopted and then collected.
func reaperKeepsExitStatus(t *testing.T, reaper *adoptedChildReaper, pidPath string) {
	t.Helper()
	cmd := reaperChild("/bin/sh", "-c", `sleep 300 & echo $! > "$1"; exit 7`, "sh", pidPath)
	err := reaper.run(cmd)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("supervised child error = %v, want exit status 7", err)
	}
	if code := exitErr.ExitCode(); code != 7 {
		t.Fatalf("supervised child exit code = %d, want 7: the reaper stole it (%v)", code, err)
	}

	orphan := readPID(t, pidPath)
	t.Cleanup(func() { bestEffortSignal("KILL", orphan) })
	if state := waitForAdoption(t, orphan); state == "Z" {
		t.Fatalf("adopted orphan %d is a zombie while alive", orphan)
	}

	before := reaper.reaped.Load()
	if err := syscall.Kill(orphan, syscall.SIGKILL); err != nil {
		t.Fatalf("kill adopted orphan %d: %v", orphan, err)
	}
	deadline := time.Now().Add(reaperReapLimit)
	for {
		_, _, present := reaperProcStat(orphan)
		collected := reaper.reaped.Load() > before
		if !present && collected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("adopted orphan %d was not collected within %s: /proc entry present=%v, reaped %d -> %d",
				orphan, reaperReapLimit, present, before, reaper.reaped.Load())
		}
		time.Sleep(reaperPoll)
	}
}

// reaperKeepsSignalStatus runs a supervised child that kills itself: a reaped
// signal status is the same status os/exec reports, not a substitute one.
func reaperKeepsSignalStatus(t *testing.T, reaper *adoptedChildReaper) {
	t.Helper()
	err := reaper.run(reaperChild("/bin/sh", "-c", "kill -9 $$"))
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("self-signalled child error = %v, want a death by signal", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("self-signalled child status = %v, want death by SIGKILL", exitErr.Sys())
	}
	if !strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("self-signalled child error = %v, want the os/exec signal text", err)
	}
}

// reaperSurvivesChurn runs the shape a real run produces over hours: a child
// that forks a short-lived grandchild and exits, twenty-five times over. Every
// one of those grandchildren is adopted, and none of them may be left behind.
func reaperSurvivesChurn(t *testing.T, reaper *adoptedChildReaper) {
	t.Helper()
	const turns = 25
	before := reaper.reaped.Load()
	for turn := 0; turn < turns; turn++ {
		if err := reaper.run(reaperChild("/bin/sh", "-c", "sleep 0.05 & exit 0")); err != nil {
			t.Fatalf("churn turn %d: %v", turn, err)
		}
	}
	waitForReaped(t, reaper, before+turns)
	waitForNoZombieDescendants(t)
}

// reaperLeavesLiveOrphanAlone runs a supervised child that leaves a long-lived
// orphan behind and returns its pid. A reaper collects; it never signals, so
// the orphan must still be running a second later.
func reaperLeavesLiveOrphanAlone(t *testing.T, reaper *adoptedChildReaper, pidPath string) int {
	t.Helper()
	cmd := reaperChild("/bin/sh", "-c", `sleep 300 & echo $! > "$1"; exit 0`, "sh", pidPath)
	if err := reaper.run(cmd); err != nil {
		t.Fatalf("supervised child that leaves a live orphan: %v", err)
	}
	pid := readPID(t, pidPath)
	t.Cleanup(func() { bestEffortSignal("KILL", pid) })
	if state := waitForAdoption(t, pid); state == "Z" {
		t.Fatalf("live orphan %d is a zombie while alive", pid)
	}
	until := time.Now().Add(1 * time.Second)
	for time.Now().Before(until) {
		state, _, present := reaperProcStat(pid)
		if !present {
			t.Fatalf("live orphan %d is gone: the reaper must never signal an adopted descendant", pid)
		}
		if state == "Z" {
			t.Fatalf("live orphan %d became a zombie while it was still running", pid)
		}
		time.Sleep(reaperPoll)
	}
	return pid
}

// waitForAdoption waits until pid's parent is this process, which is the kernel
// handing an orphan over, and returns the state observed at that moment.
func waitForAdoption(t *testing.T, pid int) string {
	t.Helper()
	deadline := time.Now().Add(reaperAdoptLimit)
	for {
		state, ppid, present := reaperProcStat(pid)
		if present && ppid == os.Getpid() {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d was not adopted by pid %d within %s (present=%v state=%q ppid=%d)",
				pid, os.Getpid(), reaperAdoptLimit, present, state, ppid)
		}
		time.Sleep(reaperPoll)
	}
}

// waitForReaped waits until the reaper has collected at least want adopted
// descendants.
func waitForReaped(t *testing.T, reaper *adoptedChildReaper, want uint64) {
	t.Helper()
	deadline := time.Now().Add(reaperReapLimit)
	for reaper.reaped.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("reaper collected %d adopted descendants, want %d within %s", reaper.reaped.Load(), want, reaperReapLimit)
		}
		time.Sleep(reaperPoll)
	}
}

// waitForNoZombieDescendants waits until no descendant of this process is a
// zombie: the bug #347 reports, after hours of churn.
func waitForNoZombieDescendants(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(reaperReapLimit)
	for {
		pids, err := descendantPIDs()
		if err != nil {
			t.Fatalf("enumerate descendants: %v", err)
		}
		zombies := zombieStates(pids)
		if len(zombies) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendants %v are still zombies %s after the turns that created them exited", zombies, reaperReapLimit)
		}
		time.Sleep(reaperPoll)
	}
}

// waitForNoDescendants waits until this process has no descendants at all.
func waitForNoDescendants(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(reaperReapLimit)
	for {
		pids, err := descendantPIDs()
		if err != nil {
			t.Fatalf("enumerate descendants: %v", err)
		}
		if len(pids) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendants %v outlived the helper", pids)
		}
		time.Sleep(reaperPoll)
	}
}

// zombieStates returns the pids whose recorded state is Z.
func zombieStates(pids []int) []int {
	var zombies []int
	for _, pid := range pids {
		if state, _, present := reaperProcStat(pid); present && state == "Z" {
			zombies = append(zombies, pid)
		}
	}
	return zombies
}

// reaperProcStat returns the state and parent pid recorded for pid. The comm
// field can contain spaces and parentheses, so parsing starts after its
// closing parenthesis; a pid with no entry is absent, not an error.
func reaperProcStat(pid int) (state string, ppid int, present bool) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", 0, false
	}
	text := string(data)
	end := strings.LastIndex(text, ")")
	if end < 0 {
		return "", 0, false
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 2 {
		return "", 0, false
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", 0, false
	}
	return fields[0], parent, true
}

// readPID reads the single pid a helper child wrote.
func readPID(t *testing.T, path string) int {
	t.Helper()
	raw := mustRead(t, path)
	pid := 0
	if _, err := fmt.Sscanf(string(raw), "%d", &pid); err != nil || pid <= 0 {
		t.Fatalf("%s holds %q, which is not a positive pid: %v", path, raw, err)
	}
	return pid
}

// TestExecutorReapsAdoptedOrphansAndStillReportsExit runs the built executor
// end to end with a model child that leaves an orphan behind, and checks the
// two halves of #347 against the real binary: the run's ending is the ending
// the same run produces without an orphan (the no-work Failed ending), and the
// orphan is never retained as a zombie.
func TestExecutorReapsAdoptedOrphansAndStillReportsExit(t *testing.T) {
	root := t.TempDir()

	// Build into the scratch temp dir, never into the repo.
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

	remote := remoteWithExistingBranch(t, root)
	workspace := filepath.Join(root, "workspace")
	scratch := filepath.Join(root, "scratch")
	termination := filepath.Join(root, "termination")
	fakeOpenCode := filepath.Join(root, "opencode")
	// The first model child leaves two long-lived orphans and crashes; the kernel
	// hands both to the executor, and the run continues into a second turn that
	// holds itself open for a few seconds and exits 0 without touching the
	// workspace — the no-work ending the same run produces with no orphan at
	// all. The second turn is what makes the collection observable while the
	// executor is still running. Both orphans' output is detached: an orphan
	// holding the model's stdout would instead hold the run's pipe open long
	// past its exit, and this test is about descendants, not that separate bound.
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in
mcp) exit 0;;
esac
mkdir -p "$COURIER_SCRATCH_DIR"
if [ ! -f "$COURIER_SCRATCH_DIR/turned" ]; then
  : > "$COURIER_SCRATCH_DIR/turned"
  sleep 300 >/dev/null 2>&1 &
  echo "$!" > "$COURIER_SCRATCH_DIR/doomed.pid"
  sleep 300 >/dev/null 2>&1 &
  echo "$!" > "$COURIER_SCRATCH_DIR/live.pid"
  exit 1
fi
sleep 4
exit 0
`)
	prServer := fakeLinkedPRServer(t)
	env := subprocessEnv(os.Environ(), map[string]string{
		"COURIER_REPO_URL":               remote,
		"COURIER_WORKSPACE":              workspace,
		"COURIER_SCRATCH_DIR":            scratch,
		"COURIER_BASE":                   "main",
		"COURIER_BRANCH":                 "courier/resolve-issue/acme-widgets/7",
		"COURIER_GOAL":                   "Open a PR to address issue #7.",
		"COURIER_MODEL":                  "any-model/name",
		"COURIER_MODE":                   "resolve-issue",
		"COURIER_REPO":                   "acme/widgets",
		"COURIER_GITHUB_API_BASE":        prServer.URL,
		"COURIER_OPENCODE_BINARY":        fakeOpenCode,
		"COURIER_TERMINATION_FILE":       termination,
		"COURIER_REF":                    "7",
		"COURIER_RUN_NAME":               "coderrun-it-7",
		"COURIER_RESUME_BACKOFF_SECONDS": "0",
	})

	var out, errOut bytes.Buffer
	cmd := exec.Command(execBin)
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("start executor subprocess: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	doomedPath := filepath.Join(scratch, "doomed.pid")
	livePath := filepath.Join(scratch, "live.pid")
	doomedPid := waitForOrphanPID(t, cmd, &out, &errOut, doomedPath)
	livePid := waitForOrphanPID(t, cmd, &out, &errOut, livePath)
	t.Cleanup(func() {
		bestEffortSignal("KILL", doomedPid)
		bestEffortSignal("KILL", livePid)
	})
	// The kernel has to hand the orphans to the executor before anything can
	// be said about what it does with them.
	waitForAdoptedBy(t, cmd, &out, &errOut, doomedPid)
	waitForAdoptedBy(t, cmd, &out, &errOut, livePid)
	// The doomed orphan dies while the executor is still working through the
	// run's second turn, and a /proc entry that outlives that window is a
	// retained descendant: the bug #347 reports.
	if err := syscall.Kill(doomedPid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the doomed orphan %d: %v", doomedPid, err)
	}
	waitForCollected(t, cmd, &out, &errOut, doomedPid, 3*time.Second)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	// The run must end on its own; the zombie probe runs on the way out.
	deadline := time.Now().Add(90 * time.Second)
	for {
		if state, _, present := reaperProcStat(livePid); !present || state == "Z" {
			t.Fatalf("the executor's live orphan %d is present=%v state=%q; it must keep running and never become a zombie; stdout=%q stderr=%q",
				livePid, present, state, out.String(), errOut.String())
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("the executor did not exit within 90s of its orphan appearing; stdout=%q stderr=%q", out.String(), errOut.String())
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("executor exited 0, want the no-work Failed exit %d; stdout=%q stderr=%q", exitFailed, out.String(), errOut.String())
			}
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != exitFailed {
				t.Fatalf("executor exit = %v, want exit code %d; stdout=%q stderr=%q", err, exitFailed, out.String(), errOut.String())
			}
			finishOrphanAssertions(t, livePid)
			assertNoWorkFailedEnding(t, out.String(), mustRead(t, termination))
			return
		case <-time.After(reaperPoll):
		}
	}
}

// waitForOrphanPID waits for the model child to report the pid of the orphan
// it left behind, or fails with the run's streams attached.
func waitForOrphanPID(t *testing.T, cmd *exec.Cmd, out, errOut *bytes.Buffer, orphanPath string) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		raw, err := os.ReadFile(orphanPath)
		if err == nil {
			pid := 0
			if _, err := fmt.Sscanf(string(raw), "%d", &pid); err != nil || pid <= 0 {
				t.Fatalf("orphan pid file holds %q, which is not a positive pid: %v", raw, err)
			}
			return pid
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("the model child reported no orphan pid within 30s; stdout=%q stderr=%q", out.String(), errOut.String())
		}
		time.Sleep(reaperPoll)
	}
}

// waitForAdoptedBy waits until the kernel has reparented pid to the running
// executor. Without adoption there is nothing to reap and nothing to prove, so
// a run that never reaches it fails here.
func waitForAdoptedBy(t *testing.T, cmd *exec.Cmd, out, errOut *bytes.Buffer, pid int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, ppid, present := reaperProcStat(pid)
		if present && ppid == cmd.Process.Pid {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("orphan %d was not adopted by the executor %d within 15s (present=%v ppid=%d); stdout=%q stderr=%q",
				pid, cmd.Process.Pid, present, ppid, out.String(), errOut.String())
		}
		time.Sleep(reaperPoll)
	}
}

// waitForCollected waits until pid has no /proc entry at all. An entry that
// survives the bound is a descendant nobody collected, which is the retained
// zombie the reaper exists to prevent. The bound is short on purpose: the run's
// second model child is still going, so a survivor here is the executor's
// doing, not the host's.
func waitForCollected(t *testing.T, cmd *exec.Cmd, out, errOut *bytes.Buffer, pid int, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		state, _, present := reaperProcStat(pid)
		if !present {
			return
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("adopted orphan %d was still on the host (state %q) %s after it died, mid-run; stdout=%q stderr=%q",
				pid, state, bound, out.String(), errOut.String())
		}
		time.Sleep(reaperPoll)
	}
}

// finishOrphanAssertions checks what the executor left behind for the live
// orphan. The orphan was running when the executor started exiting, and a
// reaper never signals one, so the orphan is expected to outlive the executor:
// what must not survive is a zombie, which is the retained descendant #347 is
// about. Once the executor is gone the orphan belongs to whatever adopts
// orphans here — this test process, which every in-process run test turns into
// a subreaper — so the cleanup can collect it.
func finishOrphanAssertions(t *testing.T, orphanPid int) {
	t.Helper()
	state, ppid, present := reaperProcStat(orphanPid)
	if present && state == "Z" {
		t.Fatalf("orphan %d is a zombie after the executor exited: a retained descendant", orphanPid)
	}
	if !present {
		return
	}
	bestEffortSignal("KILL", orphanPid)
	if ppid != os.Getpid() {
		return
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var status syscall.WaitStatus
		if reaped, err := syscall.Wait4(orphanPid, &status, syscall.WNOHANG, nil); err == nil && reaped > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("orphan %d was not collectable after the executor exited", orphanPid)
		}
		time.Sleep(reaperPoll)
	}
}

// assertNoWorkFailedEnding pins the ending TestRunExitZeroWithoutLocalWorkBecomesFailed
// pins for the same no-work run: an orphan left behind must not change it.
func assertNoWorkFailedEnding(t *testing.T, stdout string, termination []byte) {
	t.Helper()
	if !strings.Contains(stdout, `"phase":"Failed"`) {
		t.Fatalf("stdout does not hold %q: %q", `"phase":"Failed"`, stdout)
	}
	if !strings.Contains(stdout, "without declaring an outcome and produced no commit or workspace changes") {
		t.Fatalf("stdout does not hold the no-work reason: %q", stdout)
	}
	if !strings.Contains(string(termination), `"phase":"Failed"`) {
		t.Fatalf("termination file does not hold %q: %q", `"phase":"Failed"`, termination)
	}
}

// TestEveryExecutorCommandRunsThroughTheReaper guards the invariant that makes
// the reaper safe: a child started outside the funnel could have its exit
// status collected out from under os/exec's Wait, and the run would end on a
// status nobody asked for.
func TestEveryExecutorCommandRunsThroughTheReaper(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	source, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "main.go"))
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(source)
	spawns := strings.Count(text, "exec.Command(") + strings.Count(text, "exec.CommandContext(")
	funneled := strings.Count(text, "childReaper().run(")
	if spawns != funneled {
		t.Fatalf("main.go starts %d commands and runs %d of them through the reaper: a command started outside the reaper would have its exit status stolen", spawns, funneled)
	}
	if spawns == 0 {
		t.Fatalf("main.go starts no command at all: this guard is no longer measuring anything")
	}
}
