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

// containmentAdoptLimit bounds every wait in the helper and its parent: an
// unproven claim is a failure, not a hang.
const containmentAdoptLimit = 5 * time.Second

// runContainmentHelper re-executes this test binary as the containment
// helper and returns everything it printed.
func runContainmentHelper(t *testing.T, attr *syscall.SysProcAttr) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestWorkerContainmentHelper", "-test.count=1")
	cmd.Env = append(os.Environ(), containmentHelperEnv+"=pid1")
	cmd.SysProcAttr = attr
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	// Arming is process-wide and sticky, so a drain pass may already be
	// running in this binary from an earlier test: the helper goes through
	// the same funnel every task child does — its pid is registered before
	// the start is released and released only after Wait — so no pass can
	// reap the status cmd.Wait owes.
	if err := containment.startTaskChild(cmd); err != nil {
		return "", err
	}
	defer containment.releaseTaskChild(cmd.Process.Pid)
	err := cmd.Wait()
	return out.String(), err
}

// TestWorkerReaperCollectsAsPIDOne runs the reaper's contract as PID 1 of a
// fresh PID namespace, which is the production shape: the kernel reparents
// orphans to PID 1 on its own, with no subreaper flag involved. An
// environment that forbids the namespace is skipped, never failed.
func TestWorkerReaperCollectsAsPIDOne(t *testing.T) {
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
	out, err := runContainmentHelper(t, attr)
	if err != nil && (errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EINVAL)) {
		t.Skipf("cannot run the helper as PID 1 of a new PID namespace: %v", err)
	}
	if err != nil {
		t.Fatalf("containment helper as PID 1 failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "reaped=") {
		t.Fatalf("containment helper printed no reaped=<n> tally:\n%s", out)
	}
}

// TestWorkerContainmentHelper is the adopted-child reaper's contract, run in
// a process of its own so the subreaper flag and the adopted orphans are
// really its own.
func TestWorkerContainmentHelper(t *testing.T) {
	if os.Getenv(containmentHelperEnv) == "" {
		return
	}
	if os.Getpid() != 1 {
		t.Fatalf("helper is pid %d, not 1: the PID namespace was not obtained", os.Getpid())
	}
	if err := ArmAdoptedChildReaper(); err != nil {
		t.Fatalf("arm adopted-child reaper: %v", err)
	}

	// A supervised task leader keeps the exit status os/exec produced for it,
	// even while the reaper is collecting orphans around it.
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

	// A child nobody waits for becomes a zombie adopted child, and the
	// reaper — not the test — is what collects it. A zombie's /proc entry
	// stays until someone reaps it, so the entry's disappearance is the
	// proof.
	orphan := exec.Command("sh", "-c", "exit 0")
	if err := orphan.Start(); err != nil {
		t.Fatalf("start unsupervised child: %v", err)
	}
	waitForGone(t, orphan.Process.Pid, "zombie child")

	// A deeper tree converges the same way: the parent is collected first,
	// its orphaned descendant is adopted in turn, and the reaper collects it
	// too instead of leaving it behind for the pod's lifetime.
	dir := t.TempDir()
	tree := exec.Command("sh", "-c", "sleep 1 & echo $! > tree-child.txt; wait")
	tree.Dir = dir
	if err := tree.Start(); err != nil {
		t.Fatalf("start unsupervised tree: %v", err)
	}
	treePID := tree.Process.Pid
	if err := syscall.Kill(treePID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill unsupervised tree: %v", err)
	}
	grandchild := 0
	deadline := time.Now().Add(containmentAdoptLimit)
	for grandchild == 0 {
		if time.Now().After(deadline) {
			t.Fatal("orphaned grandchild never reported its pid")
		}
		if data, err := os.ReadFile(filepath.Join(dir, "tree-child.txt")); err == nil {
			grandchild, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
		time.Sleep(20 * time.Millisecond)
	}
	deadline = time.Now().Add(containmentAdoptLimit)
	for {
		_, treeGone := os.Stat("/proc/" + strconv.Itoa(treePID))
		_, childGone := os.Stat("/proc/" + strconv.Itoa(grandchild))
		if os.IsNotExist(treeGone) && os.IsNotExist(childGone) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("adopted tree %d/%d was never fully reaped", treePID, grandchild)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The live-orphan half of the contract: a live adopted descendant is
	// never signalled by the reaper. Reaping is the whole job.
	live := exec.Command("sh", "-c", "exec sleep 30")
	if err := live.Start(); err != nil {
		t.Fatalf("start live orphan: %v", err)
	}
	livePID := live.Process.Pid
	time.Sleep(300 * time.Millisecond)
	if !descendantLive(livePID) {
		t.Fatalf("live orphan %d was disturbed by the reaper", livePID)
	}
	// Nothing outlives the helper: the live orphan is killed here and the
	// reaper is what collects it, so the test leaves no process of its own.
	if err := syscall.Kill(livePID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill live orphan %d: %v", livePID, err)
	}
	waitForGone(t, livePID, "killed live orphan")
	if containment.reaped.Load() < 3 {
		t.Fatalf("reaper collected %d orphans, want at least 3", containment.reaped.Load())
	}
	fmt.Printf("reaped=%d\n", containment.reaped.Load())
}

// waitForGone asserts that a pid's /proc entry has disappeared — reaped,
// not merely stopped: a zombie keeps its entry.
func waitForGone(t *testing.T, pid int, what string) {
	t.Helper()
	deadline := time.Now().Add(containmentAdoptLimit)
	for {
		if _, err := os.Stat("/proc/" + strconv.Itoa(pid)); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %d was never reaped", what, pid)
		}
		time.Sleep(20 * time.Millisecond)
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
