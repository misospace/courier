//go:build linux

package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The containment tests re-execute this test binary as a helper where the
// process shape under test — PID 1 of a fresh PID namespace, a subreaper of
// its own — must belong to the helper alone. The env-guarded helper test
// below returns immediately when the variable is absent, so a plain test run
// never runs the helper's body in process.
const containmentHelperEnv = "TEST_WORKER_CONTAINMENT_HELPER"

// containmentHelperPIDOne is the production shape: the helper is PID 1 of a
// fresh PID namespace, so the kernel reparents orphans to it on its own, with
// no subreaper flag involved.
const containmentHelperPIDOne = "pid1"

// containmentHelperSubreaper is the shape a host that forbids user namespaces
// can still exercise: the helper is an ordinary process with a living
// ancestor, and the child-subreaper flag hands it the orphans its own
// descendants leave behind.
const containmentHelperSubreaper = "subreaper"

// containmentAdoptLimit bounds every wait in the helper and its parent: an
// unproven claim is a failure, not a hang.
const containmentAdoptLimit = 5 * time.Second

// containmentChurnLimit bounds the churn's wait: a slow host must not turn a
// proven collection into a flake, but the wait still ends.
const containmentChurnLimit = 30 * time.Second

// containmentChurnTurns is how many task trees the churn case creates: the
// shape a real run produces over hours.
const containmentChurnTurns = 25

// containmentPoll is the interval every bounded wait in the containment tests
// polls at.
const containmentPoll = 20 * time.Millisecond

// containmentHelperStartErr marks a helper that never started: the
// environment refused the process shape, not the contract the helper checks.
var containmentHelperStartErr = errors.New("containment helper did not start")

// runContainmentHelper re-executes this test binary as the containment
// helper in the given mode and returns everything it printed.
func runContainmentHelper(t *testing.T, mode string, attr *syscall.SysProcAttr) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestWorkerContainmentHelper", "-test.count=1")
	cmd.Env = append(os.Environ(), containmentHelperEnv+"="+mode)
	cmd.SysProcAttr = attr
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	// A helper that leaves a descendant holding its output would hold the
	// parent's pipe open long past its own exit, so the wait is bounded: a
	// helper that wedges fails the test instead of hanging it.
	cmd.WaitDelay = 30 * time.Second
	// Arming is process-wide and sticky, so a drain pass may already be
	// running in this binary from an earlier test: the helper goes through
	// the same funnel every task child does — its pid is registered before
	// the start is released and released only after Wait — so no pass can
	// reap the status cmd.Wait owes.
	if err := containment.startTaskChild(cmd); err != nil {
		return "", fmt.Errorf("%w: %w", containmentHelperStartErr, err)
	}
	defer containment.releaseTaskChild(cmd.Process.Pid)
	err := cmd.Wait()
	return out.String(), err
}

// TestWorkerReaperKeepsTaskStatusesAsPIDOne runs the reaper's own claims as
// PID 1 of a fresh PID namespace, which is the production shape: the kernel
// reparents orphans to PID 1 on its own, with no subreaper flag involved. The
// collection claims live in TestWorkerReaperCollectsAdoptedChildren instead,
// because a procfs instance belongs to the pid namespace it was mounted in and
// a test-created PID namespace leaves /proc bound to the outer one: there the
// worker's own child enumeration cannot name what its wait collects. An
// environment that forbids the namespace is skipped, never failed, and the
// skip names the restriction it was.
func TestWorkerReaperKeepsTaskStatusesAsPIDOne(t *testing.T) {
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
	out, err := runContainmentHelper(t, containmentHelperPIDOne, attr)
	if err != nil && errors.Is(err, containmentHelperStartErr) &&
		(errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EINVAL)) {
		t.Skipf("environment forbids creating a user+PID namespace (%s); the living-ancestor adoption path is covered by TestWorkerReaperCollectsAdoptedChildren",
			errnoName(err))
	}
	if err != nil {
		t.Fatalf("containment helper as PID 1 failed: %v\n%s", err, out)
	}
	// The helper's own exit status is the claim: it fails loudly when a
	// supervised task leader's exit or signal status is not the one os/exec
	// produced.
}

// TestWorkerReaperCollectsAdoptedChildren runs the same reaper contract as an
// ordinary process with a living ancestor, which is the shape a host that
// forbids user namespaces can still exercise: the child-subreaper flag hands
// the helper the orphans its own task trees leave behind, and the reaper
// collects them. The contract is asserted on every host, so the worker's
// reaping behaviour never rests on a namespace the environment may refuse.
func TestWorkerReaperCollectsAdoptedChildren(t *testing.T) {
	out, err := runContainmentHelper(t, containmentHelperSubreaper, nil)
	if err != nil {
		t.Fatalf("containment helper as an adopted-child subreaper failed: %v\n%s", err, out)
	}
	reaped, ok := containmentReapedTally(out)
	if !ok {
		t.Fatalf("containment helper printed no reaped=<n> tally:\n%s", out)
	}
	if reaped < containmentChurnTurns {
		t.Fatalf("the reaper collected %d adopted descendants, want at least the %d the churn alone creates:\n%s",
			reaped, containmentChurnTurns, out)
	}
}

// containmentReapedTally reads the reaped=<n> tally the helper printed.
func containmentReapedTally(out string) (uint64, bool) {
	for _, line := range strings.Split(out, "\n") {
		rest, found := strings.CutPrefix(line, "reaped=")
		if !found {
			continue
		}
		count, err := strconv.ParseUint(strings.TrimSpace(rest), 10, 64)
		if err != nil {
			continue
		}
		return count, true
	}
	return 0, false
}

// errnoName renders the errno behind err, so a skipped environment
// restriction says which restriction it was.
func errnoName(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	return "no errno in " + err.Error()
}

// TestWorkerContainmentHelper is the adopted-child reaper's contract, run in
// a process of its own so the subreaper flag and the adopted orphans are
// really its own. The mode in the environment selects the shape the helper
// runs in: PID 1 of a fresh PID namespace, or an ordinary process with a
// living ancestor, which is the shape a host that forbids user namespaces can
// still exercise and the shape in which the worker's own process accounting
// closes.
func TestWorkerContainmentHelper(t *testing.T) {
	mode := os.Getenv(containmentHelperEnv)
	if mode == "" {
		return
	}
	switch mode {
	case containmentHelperPIDOne:
		if os.Getpid() != 1 {
			t.Fatalf("helper is pid %d, not 1: the PID namespace was not obtained", os.Getpid())
		}
	case containmentHelperSubreaper:
		if os.Getpid() == 1 {
			t.Fatalf("helper is pid 1: the living-ancestor path must run as an ordinary process")
		}
	default:
		t.Fatalf("unknown containment helper mode %q", mode)
	}
	if err := ArmAdoptedChildReaper(); err != nil {
		t.Fatalf("arm adopted-child reaper: %v", err)
	}
	// The funnel's own claims hold in either shape: a supervised task leader
	// keeps the exit status os/exec produced for it, and it keeps the signal
	// status os/exec reports.
	helperKeepsExitStatus(t)
	helperKeepsSignalStatus(t)
	// The collection claims belong to the living-ancestor mode alone. The
	// worker reads its children through the procfs instance, and a procfs
	// instance belongs to the pid namespace it was mounted in: in the
	// PID-namespace mode that is the outer namespace, so the pids the worker
	// enumerates are not the pids its own wait can name and its reaper cannot
	// collect anything there. The living-ancestor mode shares one namespace
	// between the process and the procfs — the pod's own namespace
	// relationship — and there the reaper's bookkeeping closes.
	if mode == containmentHelperSubreaper {
		helperReapsUnsupervisedChild(t)
		helperReapsOrphanedTree(t)
		helperSurvivesChurn(t)
		helperLeavesLiveChildAlone(t)
	}
	fmt.Printf("reaped=%d\n", containment.reaped.Load())
}

// helperKeepsExitStatus is the first half of the contract: a supervised task
// leader keeps the exit status os/exec produced for it, even while the reaper
// is collecting orphans around it. The child goes through the funnel, so the
// status is the funnel's to protect.
func helperKeepsExitStatus(t *testing.T) {
	t.Helper()
	supervised := exec.Command("sh", "-c", "exit 7")
	if err := containment.startTaskChild(supervised); err != nil {
		t.Fatalf("start supervised child: %v", err)
	}
	waitErr := supervised.Wait()
	containment.releaseTaskChild(supervised.Process.Pid)
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != 7 {
		t.Fatalf("supervised child's status = %v, want exit 7 owned by os/exec", waitErr)
	}
}

// helperKeepsSignalStatus is the signal half of the same contract: a task
// leader that dies by a signal keeps the signal status os/exec reports, not a
// substitute the reaper produced on its way past.
func helperKeepsSignalStatus(t *testing.T) {
	t.Helper()
	killed := exec.Command("sh", "-c", "kill -9 $$")
	if err := containment.startTaskChild(killed); err != nil {
		t.Fatalf("start self-signalled child: %v", err)
	}
	waitErr := killed.Wait()
	containment.releaseTaskChild(killed.Process.Pid)
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("self-signalled child's status = %v, want a death by signal", waitErr)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("self-signalled child's status = %v, want death by SIGKILL", exitErr.Sys())
	}
}

// helperReapsUnsupervisedChild is the reaper's own claim: a child nobody
// waits for becomes a zombie adopted child, and the reaper — not the test —
// is what collects it. A zombie's /proc entry stays until someone reaps it,
// so the entry's disappearance is the proof. The child is deliberately
// started outside the funnel: its exit status belongs to the reaper, and the
// test asserts none for it.
func helperReapsUnsupervisedChild(t *testing.T) {
	t.Helper()
	orphan := exec.Command("sh", "-c", "exit 0")
	if err := orphan.Start(); err != nil {
		t.Fatalf("start unsupervised child: %v", err)
	}
	waitForGone(t, orphan.Process.Pid, "zombie child")
}

// helperReapsOrphanedTree is the deeper shape: a task tree whose parent
// exits normally leaves its descendant orphaned, the kernel hands that
// descendant to the helper, and the reaper collects it too instead of leaving
// it behind for the pod's lifetime. The parent's own status is the funnel's
// to keep, so the parent goes through the funnel as well.
func helperReapsOrphanedTree(t *testing.T) {
	t.Helper()
	before := containment.reaped.Load()
	dir := t.TempDir()
	// The descendant outlives the adoption bound, so the adoption is
	// observed on a live process rather than raced against its death.
	tree := exec.Command("sh", "-c", "sleep 30 & echo $! > tree-child.txt; exit 0")
	tree.Dir = dir
	if err := containment.startTaskChild(tree); err != nil {
		t.Fatalf("start task tree: %v", err)
	}
	treePID := tree.Process.Pid
	waitErr := tree.Wait()
	containment.releaseTaskChild(treePID)
	if waitErr != nil {
		t.Fatalf("task tree's status = %v, want the exit 0 its own shell produced", waitErr)
	}
	grandchild := waitForTreeChildPID(t, filepath.Join(dir, "tree-child.txt"))
	t.Cleanup(func() { _ = syscall.Kill(grandchild, syscall.SIGKILL) })
	// The kernel hands the orphan over: the descendant's parent becomes the
	// helper's own pid, which is the adoption the child-subreaper flag buys
	// when the helper is not PID 1.
	waitForAdoption(t, grandchild)
	// The descendant dies only once its adoption is proven, and the reaper
	// is what collects it: the entry's disappearance is the descendant's
	// proof, and the counter's movement is the reaper's own claim beside it.
	if err := syscall.Kill(grandchild, syscall.SIGKILL); err != nil {
		t.Fatalf("kill orphaned grandchild %d: %v", grandchild, err)
	}
	waitForGone(t, grandchild, "orphaned grandchild")
	waitForReapedCount(t, before+1, containmentAdoptLimit)
}

// helperSurvivesChurn is the shape a real run produces over hours: a task
// that forks a short-lived descendant and exits, turn after turn. Every one of
// those descendants is adopted by the helper and collected, and none of them
// may be left behind as a zombie.
func helperSurvivesChurn(t *testing.T) {
	t.Helper()
	before := containment.reaped.Load()
	for turn := 0; turn < containmentChurnTurns; turn++ {
		child := exec.Command("sh", "-c", "sleep 0.05 & exit 0")
		if err := containment.startTaskChild(child); err != nil {
			t.Fatalf("start churn turn %d: %v", turn, err)
		}
		waitErr := child.Wait()
		containment.releaseTaskChild(child.Process.Pid)
		if waitErr != nil {
			t.Fatalf("churn turn %d status = %v, want the exit 0 its own shell produced", turn, waitErr)
		}
	}
	waitForReapedCount(t, before+containmentChurnTurns, containmentChurnLimit)
	waitForNoZombieChildren(t)
}

// helperLeavesLiveChildAlone is the live-child half of the contract: a
// descendant that is still running is never signalled by the reaper. Reaping
// is the whole job.
func helperLeavesLiveChildAlone(t *testing.T) {
	t.Helper()
	live := exec.Command("sh", "-c", "exec sleep 30")
	if err := live.Start(); err != nil {
		t.Fatalf("start live child: %v", err)
	}
	livePID := live.Process.Pid
	// The child holds the helper's output descriptors, so a failed
	// assertion must not leave it behind: the cleanup kills it whatever the
	// assertion says.
	t.Cleanup(func() { _ = syscall.Kill(livePID, syscall.SIGKILL) })
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !descendantLive(livePID) {
			t.Fatalf("live child %d was disturbed by the reaper", livePID)
		}
		time.Sleep(containmentPoll)
	}
	// Nothing outlives the helper: the live child is killed here and the
	// reaper is what collects it, so the test leaves no process of its own.
	if err := syscall.Kill(livePID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill live child %d: %v", livePID, err)
	}
	waitForGone(t, livePID, "killed live child")
}

// waitForGone asserts that a pid's /proc entry has disappeared — reaped,
// not merely stopped: a zombie keeps its entry. The read is the worker's own
// process reader, and a read that fails for any reason other than a
// confirmed disappearance is not evidence of absence: it fails the assertion
// closed.
func waitForGone(t *testing.T, pid int, what string) {
	t.Helper()
	deadline := time.Now().Add(containmentAdoptLimit)
	for {
		_, err := processStat(pid)
		if err != nil {
			if errors.Is(err, errProcessGone) {
				return
			}
			t.Fatalf("stat of %s %d is unreadable: %v", what, pid, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %d was never reaped", what, pid)
		}
		time.Sleep(containmentPoll)
	}
}

// waitForAdoption waits until the kernel has handed pid to this process,
// which is the adoption the child-subreaper flag buys when the helper is not
// PID 1. The stat is the worker's own reader, so a read that fails for any
// reason other than a confirmed disappearance fails the wait closed.
func waitForAdoption(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(containmentAdoptLimit)
	for {
		info, err := processStat(pid)
		if err != nil {
			if errors.Is(err, errProcessGone) {
				t.Fatalf("descendant %d was gone before it was adopted", pid)
			}
			t.Fatalf("stat of descendant %d is unreadable: %v", pid, err)
		}
		if info.ppid == os.Getpid() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant %d was not adopted by pid %d within %s (its parent is %d)",
				pid, os.Getpid(), containmentAdoptLimit, info.ppid)
		}
		time.Sleep(containmentPoll)
	}
}

// waitForTreeChildPID reads the pid a task tree reported for its descendant.
// A pid that never arrives is a failure, not a hang.
func waitForTreeChildPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(containmentAdoptLimit)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			if pid, convErr := strconv.Atoi(strings.TrimSpace(string(data))); convErr == nil && pid > 0 {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("task tree never reported its descendant's pid in %s", path)
		}
		time.Sleep(containmentPoll)
	}
}

// waitForReapedCount waits until the reaper has collected at least want
// adopted descendants.
func waitForReapedCount(t *testing.T, want uint64, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for containment.reaped.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("the reaper collected %d adopted descendants, want %d within %s",
				containment.reaped.Load(), want, limit)
		}
		time.Sleep(containmentPoll)
	}
}

// waitForNoZombieChildren waits until no child of this process is a zombie:
// the accumulation the reaper exists to prevent. The listing is the worker's
// own enumeration, and a stat that cannot be read for any reason other than
// a confirmed disappearance fails the wait closed.
func waitForNoZombieChildren(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(containmentChurnLimit)
	for {
		pids, err := directChildPIDs()
		if err != nil {
			t.Fatalf("enumerate the helper's children: %v", err)
		}
		var zombies []int
		for _, pid := range pids {
			info, statErr := processStat(pid)
			if statErr != nil {
				if errors.Is(statErr, errProcessGone) {
					continue
				}
				t.Fatalf("stat of child %d is unreadable: %v", pid, statErr)
			}
			if info.state == "Z" {
				zombies = append(zombies, pid)
			}
		}
		if len(zombies) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("children %v are still zombies %s after the turns that created them exited",
				zombies, containmentChurnLimit)
		}
		time.Sleep(containmentPoll)
	}
}

// TestWorkerSweepTerminatesCompletedTaskEscapee pins containment for
// successful tasks: a completed task may leave a daemonised descendant
// behind, and the sweep runs at every grouped task's termination — the
// escapee is killed and collected at the completion itself, so control never
// clears its suppression on a completion the worker never proved (#399).
func TestWorkerSweepTerminatesCompletedTaskEscapee(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required to leave the task's process group")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	// The outer shell sleeps briefly so the daemon's pidfile is on disk
	// before the task terminates; the daemon redirects its own output and
	// loops, which is exactly the untrusted mutation a completed task must
	// not leave behind.
	task, _ := json.Marshal(Task{Command: []string{"sh", "-c", `setsid sh -c 'echo $$ > daemon.txt; while :; do sleep 0.2; done' >/dev/null 2>&1 & sleep 0.5`}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	daemon := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "daemon.txt"))
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, err := client.Result(ctx, "op-1")
		if err == nil && state.Status == ResultCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never completed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The task is completed; the daemon outlived it and was adopted. The
	// sweep kills it at the termination and collects what it killed — the
	// /proc entry of a zombie stays until someone reaps it, so its
	// disappearance is the proof of both halves.
	deadline = time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat("/proc/" + strconv.Itoa(daemon)); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("adopted daemon %d outlived the completed task's containment", daemon)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestWorkerSweepSparesLiveTaskTrees pins the blast radius of the pod-wide
// sweep: cancelling one task must never touch the tree of another task that
// is still running. The registered task leader and everything under it are
// excluded from adoption, so the survivor completes with the exit code its
// own work produced, not the sweep's.
func TestWorkerSweepSparesLiveTaskTrees(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required to leave the task's process group")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	survivorPayload, _ := json.Marshal(Task{Command: []string{"sh", "-c", "sleep 3; echo done"}})
	survivorEnv := dispatchEnvelope(worker.clock())
	survivorEnv.OpID = "op-survivor"
	survivorEnv.PayloadDigest = PayloadDigestOf(survivorPayload)
	survivorEnv.Nonce = "survivor-dispatch-nonce-1"
	if _, err := client.Dispatch(ctx, priv, survivorEnv, survivorPayload); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	escapeePayload, _ := json.Marshal(Task{Command: []string{"sh", "-c", `setsid sh -c 'echo $$ > escaped.txt; while :; do sleep 0.2; done' >/dev/null 2>&1 & wait`}})
	escapeeEnv := dispatchEnvelope(worker.clock())
	escapeeEnv.OpID = "op-escapee"
	escapeeEnv.PayloadDigest = PayloadDigestOf(escapeePayload)
	escapeeEnv.Nonce = "escapee-dispatch-nonce-1"
	if _, err := client.Dispatch(ctx, priv, escapeeEnv, escapeePayload); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	escapee := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "escaped.txt"))
	if !descendantLive(escapee) {
		t.Fatalf("escapee %d was gone before cancellation", escapee)
	}

	cancelEnv := escapeeEnv
	cancelEnv.Kind = KindCancel
	cancelEnv.Sequence = 1
	cancelEnv.PayloadDigest = CancelPayloadDigest
	cancelEnv.Nonce = "escapee-cancel-nonce-1"
	status, err := client.Cancel(ctx, priv, cancelEnv)
	if err != nil || status != ResultCancelled {
		t.Fatalf("Cancel = %q, %v", status, err)
	}
	if descendantLive(escapee) {
		t.Fatalf("escapee %d outlived acknowledged cancellation", escapee)
	}

	// The survivor's own tree was never adopted — its leader is registered
	// for the whole of its run — so its completion is the exit code its own
	// work produced.
	deadline := time.Now().Add(15 * time.Second)
	for {
		state, err := client.Result(ctx, "op-survivor")
		if err != nil {
			t.Fatalf("Result: %v", err)
		}
		if state.Status == ResultCompleted {
			if state.Result == nil || state.Result.ExitCode != 0 || state.Result.LiveDescendants != 0 {
				t.Fatalf("survivor result = %+v", state.Result)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("survivor task never completed after the escapee's cancellation")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWorkerCancelRefusesWhenEnumerationFails pins the conservative half of
// the containment contract: without a process listing the sweep cannot prove
// anything about the adopted escapees, so the cancellation stays unverified
// and the escapee keeps running — never acknowledged as stopped. #399 keeps
// #372's refusal contract for the cases containment cannot prove.
func TestWorkerCancelRefusesWhenEnumerationFails(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required to leave the task's process group")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	task, _ := json.Marshal(Task{Command: []string{"sh", "-c", `setsid sh -c 'echo $$ > escaped.txt; exec sleep 300' & wait`}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	escapee := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "escaped.txt"))
	if !descendantLive(escapee) {
		t.Fatalf("escapee %d was gone before cancellation", escapee)
	}
	hook := func() error { return errors.New("enumeration unavailable") }
	processEnumerationHook.Store(&hook)
	defer processEnumerationHook.Store(nil)

	cancelEnv := env
	cancelEnv.Kind = KindCancel
	cancelEnv.Sequence = 1
	cancelEnv.PayloadDigest = CancelPayloadDigest
	cancelEnv.Nonce = "cancel-nonce-value-0001"
	status, err := client.Cancel(ctx, priv, cancelEnv)
	if err == nil {
		t.Fatalf("cancellation was acknowledged with status %q while its escapees were unprovable", status)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("Cancel error = %v, want an unverified-termination rejection", err)
	}
	state, err := client.Result(ctx, "op-1")
	if err != nil || state.Result == nil || state.Result.LiveDescendants == 0 {
		t.Fatalf("unverified cancellation must still report the live tree: %+v, %v", state.Result, err)
	}
	if !descendantLive(escapee) {
		t.Fatalf("escapee %d is gone, so the refusal was not about surviving work", escapee)
	}
}

// TestWorkerSweepFailsClosedOnUnreadableStat pins the fail-closed half of
// the snapshot contract: a per-process stat that fails for any reason other
// than a confirmed disappearance is not evidence of absence. While a live
// escapee remains, making its stat unreadable must fail the whole sweep
// proof — the cancellation stays unverified exactly as when the enumeration
// itself fails — and must never collapse into the sweep claiming a clean pod
// because it silently dropped the one process it could not read.
func TestWorkerSweepFailsClosedOnUnreadableStat(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required to leave the task's process group")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	task, _ := json.Marshal(Task{Command: []string{"sh", "-c", `setsid sh -c 'echo $$ > unreadable.txt; exec sleep 300' & wait`}})
	env := dispatchEnvelope(worker.clock())
	env.OpID = "op-unreadable"
	env.PayloadDigest = PayloadDigestOf(task)
	env.Nonce = "unreadable-dispatch-nonce-1"
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	escapee := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "unreadable.txt"))
	if !descendantLive(escapee) {
		t.Fatalf("escapee %d was gone before cancellation", escapee)
	}
	// The hook reports exactly the live escapee's stat as unreadable while
	// every other process — the sweep's own view of the rest of the pod
	// included — stays readable. That is the partial view the sweep must
	// refuse to treat as proof.
	hook := func(pid int) error {
		if pid == escapee {
			return errors.New("injected unreadable stat")
		}
		return nil
	}
	processStatFailureHook.Store(&hook)
	defer processStatFailureHook.Store(nil)

	cancelEnv := env
	cancelEnv.Kind = KindCancel
	cancelEnv.Sequence = 1
	cancelEnv.PayloadDigest = CancelPayloadDigest
	cancelEnv.Nonce = "unreadable-cancel-nonce-1"
	status, err := client.Cancel(ctx, priv, cancelEnv)
	if err == nil {
		t.Fatalf("cancellation was acknowledged with status %q while a live process was unreadable", status)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("Cancel error = %v, want an unverified-termination rejection", err)
	}
	state, err := client.Result(ctx, "op-unreadable")
	if err != nil || state.Result == nil || state.Result.LiveDescendants == 0 {
		t.Fatalf("unverified cancellation must still report the live tree: %+v, %v", state.Result, err)
	}
	if !descendantLive(escapee) {
		t.Fatalf("escapee %d is gone, so the refusal was not about surviving work", escapee)
	}
}

// TestTaskPinIdentity pins the identity guarantee the containment kills
// depend on: a supervised task holds a pidfd for its leader, and a signal
// sent through that descriptor reaches the process it was opened for and
// never a process that later reused the number. After the leader has been
// reaped, the pin answers ESRCH — the proof that a recycled pid can never
// receive the old task's group signal, its sweep kill, or a stale release
// of its supervised entry.
func TestTaskPinIdentity(t *testing.T) {
	if containment.taskPin(os.Getpid()) != -1 {
		t.Fatal("taskPin answered for a pid that was never registered")
	}
	child := exec.Command("sh", "-c", "exec sleep 30")
	if err := containment.startTaskChild(child); err != nil {
		t.Fatalf("start task child: %v", err)
	}
	pid := child.Process.Pid
	pin := containment.taskPin(pid)
	if pin < 0 {
		containment.releaseTaskChild(pid)
		_ = child.Wait()
		t.Fatal("a supervised task started without a pidfd pin")
	}
	if err := pidfdSignal(pin, 0); err != nil {
		containment.releaseTaskChild(pid)
		_ = child.Wait()
		t.Fatalf("signal through the live pin: %v", err)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		containment.releaseTaskChild(pid)
		_ = child.Wait()
		t.Fatalf("kill task child: %v", err)
	}
	// Wait reaps the leader completely: the number is now free for the
	// kernel to hand out, and only the still-open pin keeps this signal
	// path pointed at the process it was opened for.
	if err := child.Wait(); err == nil {
		containment.releaseTaskChild(pid)
		t.Fatal("task child exited without an error after SIGKILL")
	}
	if err := pidfdSignal(pin, 0); !errors.Is(err, syscall.ESRCH) {
		containment.releaseTaskChild(pid)
		t.Fatalf("signal through the pin of a reaped process = %v, want ESRCH", err)
	}
	// The pin also keeps the number itself out of circulation: fresh
	// children cannot be allocated a pid the kernel still pins.
	for i := 0; i < 32; i++ {
		probe := exec.Command("true")
		// Through the funnel: the reaper may already be armed in this
		// binary, and an unregistered child's status is exactly what a
		// drain pass is entitled to collect.
		if err := containment.startTaskChild(probe); err != nil {
			containment.releaseTaskChild(pid)
			t.Fatalf("start probe: %v", err)
		}
		probePID := probe.Process.Pid
		err := probe.Wait()
		containment.releaseTaskChild(probePID)
		if err != nil {
			containment.releaseTaskChild(pid)
			t.Fatalf("wait probe: %v", err)
		}
		if probePID == pid {
			containment.releaseTaskChild(pid)
			t.Fatalf("a fresh child took the pinned pid %d while its pin was open", pid)
		}
	}
	containment.releaseTaskChild(pid)
	if containment.taskPin(pid) != -1 {
		t.Fatal("release left a supervised entry or its pin behind")
	}
}

// TestWorkerPinRefusalFailsClosed pins the fail-closed half of the pin
// contract: a registration that cannot open the pidfd never leaves an
// unpinned supervised entry behind — tolerance for a refused open was the
// recycled-pid race this pin exists to close. The funnel kills the child it
// just started and proves its tree dead, so the failure result reports a
// verified stop, and no supervised entry survives the run.
func TestWorkerPinRefusalFailsClosed(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	hook := func(int) (int, error) { return -1, syscall.ENOSYS }
	pidfdOpenHook.Store(&hook)
	defer pidfdOpenHook.Store(nil)

	task, _ := json.Marshal(Task{Command: []string{"sh", "-c", "exec sleep 300"}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		state, err := client.Result(ctx, "op-1")
		if err == nil && state.Result != nil {
			if state.Result.Status != ResultFailed || state.Result.ExitCode != -1 {
				t.Fatalf("pin-failure result = %+v, want a failed start", state.Result)
			}
			if state.Result.LiveDescendants != 0 {
				t.Fatalf("pin-failure teardown did not prove the tree dead: %+v", state.Result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never failed after the pin was refused")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// An unpinned entry cannot exist: the refused registration never stored
	// one, and the funnel's teardown left nothing supervised behind.
	if len(containment.supervised) != 0 {
		t.Fatalf("a failed registration left supervised entries behind: %v", containment.supervised)
	}
}

// TestWorkerPinFailureSweepsEscapee pins the sweep half of the funnel's
// teardown: a task child that started, daemonised an escapee into a session
// of its own, and then failed its pin registration must not leave that
// escapee running behind a clean report. The escaped pid is written from
// inside the new session, so by the time registration fails the group signal
// can no longer reach the escapee — only the sweep can.
func TestWorkerPinFailureSweepsEscapee(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required to leave the task's process group")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	// The hook holds registration open until the escapee has reported its
	// pid from inside its own session, so the teardown below really races a
	// live escapee rather than a child killed before it forked anything.
	// The error is deliberately not ENOSYS: every pin refusal now fails
	// closed the same way.
	hook := func(int) (int, error) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(worker.config.WorkspaceDir, "escaped.txt")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		return -1, errors.New("too many open files")
	}
	pidfdOpenHook.Store(&hook)
	defer pidfdOpenHook.Store(nil)

	task, _ := json.Marshal(Task{Command: []string{"sh", "-c", `setsid sh -c 'echo $$ > escaped.txt; while :; do sleep 0.2; done' >/dev/null 2>&1 & sleep 5`}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	escapee := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "escaped.txt"))
	deadline := time.Now().Add(30 * time.Second)
	for {
		state, err := client.Result(ctx, "op-1")
		if err == nil && state.Result != nil {
			if state.Result.Status != ResultFailed || state.Result.ExitCode != -1 {
				t.Fatalf("pin-failure result = %+v, want a failed start", state.Result)
			}
			if state.Result.LiveDescendants != 0 {
				t.Fatalf("funnel teardown did not prove the escapee dead: %+v", state.Result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never failed after the pin was refused")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The escapee was killed by the sweep and collected: a zombie keeps its
	// /proc entry until someone reaps it, so the disappearance proves both
	// halves — nothing live survives the call, and nothing was left unreaped.
	deadline = time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat("/proc/" + strconv.Itoa(escapee)); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("escapee %d outlived the funnel's containment proof", escapee)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
