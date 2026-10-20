package protocol

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// The worker executes untrusted tasks. #372 gave every task its own process
// group and made cancellation kill and prove that group; two classes of
// process sit outside any group contract, and #399 closes both without
// granting the worker pod a single privilege it does not already have:
//
//   - A descendant that makes a session of its own (setsid, a daemonising
//     tool) leaves the group, and no group signal reaches it. The worker is
//     PID 1 of its pod, so the kernel hands it every orphaned descendant its
//     tasks leave behind; the subreaper flag keeps that true for any shape
//     where it is not PID 1. Once a task's ancestors are gone, every escapee
//     it left is a direct child of the worker — enumerable and killable
//     regardless of the group or session it moved to.
//   - An adopted orphan that dies is reaped by nobody: PID 1 does not reap
//     on its own, so every cancelled task would leave a zombie behind for
//     the pod's lifetime. The adopted-child reaper collects them.
//
// One ambiguity is stated rather than hidden: a process whose intermediate
// ancestor died while its task still runs is reparented past the live task —
// the kernel offers nothing else — and is therefore indistinguishable from a
// dead task's escapee. The sweep kills it too: containment of untrusted
// processes wins over a descendant whose supervisor is already gone.

// taskContainment is the process-wide containment state. It is process-level,
// not per-Worker, because the kernel facts it tracks — this process's
// children and their adoption — belong to the process, not to one Worker
// instance: a test binary hosting two listeners, or any future embedding,
// must still never reap a child whose exit status os/exec owns.
type taskContainment struct {
	// gate is held across a task child's Start and its pid registration, and
	// across every adoption pass. Because a task's pid is therefore either
	// registered before a pass enumerates children or does not exist yet, a
	// pass can never kill or reap a task between its fork and its
	// registration.
	gate sync.Mutex
	// mu guards supervised.
	mu sync.Mutex
	// supervised holds the pids whose exit status os/exec owns: the task
	// leaders whose Wait is still owed, each mapped to a pidfd pinning the
	// process. The pin keeps the numeric pid unrecyclable for exactly as
	// long as the registration lives, so a late release can never delete a
	// different task's entry and no new process or process group can take
	// a supervised leader's number. Everything else this process could see
	// as a zombie child is an adopted orphan, and collecting it is the
	// reaper's whole job.
	supervised map[int]int
	// reaped counts the adopted descendants collected. It is the observable
	// the tests use; nothing reads it in the run itself.
	reaped atomic.Uint64
}

var containment = &taskContainment{supervised: make(map[int]int)}

// startTaskChild is the single funnel for a task child: Start, register with
// its identity pin, return. Registration happens under the gate, so no
// adoption pass can see the process's children without this task's pid in it.
// A registration that cannot pin terminates the child it just started rather
// than run a task whose containment the worker cannot prove.
func (c *taskContainment) startTaskChild(cmd *exec.Cmd) error {
	c.gate.Lock()
	defer c.gate.Unlock()
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := c.registerSupervised(cmd.Process.Pid); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	return nil
}

// releaseTaskChild hands the pid back once Wait has taken its status. The
// entry is removed before the pin closes, and while the pin was held no new
// process could take this pid, so the delete can only remove this task's
// entry. Closing the pin is what makes the number available again.
func (c *taskContainment) releaseTaskChild(pid int) {
	c.mu.Lock()
	pin, ok := c.supervised[pid]
	delete(c.supervised, pid)
	c.mu.Unlock()
	if ok && pin >= 0 {
		_ = syscall.Close(pin)
	}
}

// registerSupervised records a pid whose exit status os/exec owns and pins
// it with a pidfd, so the number stays this task's for the whole life of
// the registration. The pin is what makes the containment kills identity-safe,
// so a refused open (other than a platform without pidfds) fails the
// registration rather than leave an unpinned supervised entry: an unpinned
// registration would reopen exactly the recycled-pid window the pin closes.
// Only the funnel calls it in the run.
func (c *taskContainment) registerSupervised(pid int) error {
	pin := -1
	fd, err := openPidfd(pid)
	switch {
	case err == nil:
		pin = fd
	case errors.Is(err, syscall.ENOSYS):
		// A platform without pidfds has no /proc to enumerate either, so
		// the containment proof fails closed at the snapshot regardless.
	default:
		return fmt.Errorf("protocol: could not pin supervised pid %d: %w", pid, err)
	}
	c.mu.Lock()
	c.supervised[pid] = pin
	c.mu.Unlock()
	return nil
}

// taskPin returns the containment pidfd pinning a supervised pid, or -1
// when the pid is not registered or was registered without a pin. Holding
// the pin across a process-group cleanup is what keeps the group number
// from being reused by a concurrent task while the cleanup still signals it.
func (c *taskContainment) taskPin(pid int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pin, ok := c.supervised[pid]; ok {
		return pin
	}
	return -1
}

func (c *taskContainment) supervises(pid int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.supervised[pid]
	return ok
}

// drain collects every zombie child that is not a supervised task leader,
// repeating while a pass still reaps something because each reap can expose
// further zombies. Reaping only: no signal, no kill, no process-group
// operation — a live orphan keeps running exactly as long as its dead parent
// left it running, and termination is the sweep's business, not the drain's.
func (c *taskContainment) drain() {
	c.gate.Lock()
	defer c.gate.Unlock()
	for {
		pids, err := directChildPIDs()
		if err != nil {
			return
		}
		collected := false
		for _, pid := range pids {
			if c.supervises(pid) {
				continue
			}
			var status syscall.WaitStatus
			// WNOHANG: a live orphan is not this pass's business, and ECHILD
			// (already collected) and EINTR (a signal beat us to it) are not
			// failures worth reporting.
			reaped, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
			if err != nil || reaped <= 0 {
				continue
			}
			c.reaped.Add(1)
			collected = true
		}
		if !collected {
			return
		}
	}
}

// sweepAdoptedTrees terminates and collects everything the worker has
// adopted, and returns how many live processes were still unaccounted for
// when the deadline expired. A pass kills the whole adopted tree at once —
// the closure of each adopted root over the parent map — so a deep escapee
// tree does not need one pass per generation. What survives the bound keeps
// the termination unverified; an unproven stop must never read as verified.
func (c *taskContainment) sweepAdoptedTrees(deadline time.Time) (int, error) {
	// processEnumerationHook, when non-nil, fails the enumeration; tests
	// install it to pin the conservative contract. It is nil in production,
	// and it is atomic because a task's goroutine reads it long after the
	// test goroutine that installed it has moved on.
	if hook := processEnumerationHook.Load(); hook != nil {
		if err := (*hook)(); err != nil {
			return 0, err
		}
	}
	self := os.Getpid()
	for {
		c.gate.Lock()
		snapshot, err := processSnapshot()
		if err != nil {
			c.gate.Unlock()
			return 0, err
		}
		c.reapAdoptedZombies(snapshot, self)
		live := c.adoptedLiveProcesses(snapshot, self)
		if len(live) > 0 && time.Now().Before(deadline) {
			for _, entry := range live {
				// The signal goes through the pidfd the snapshot opened for
				// this process, so it can never reach a process that reused
				// the number: the open pins the target, and a target that
				// died since the snapshot answers ESRCH, which is exactly a
				// containment success for this entry. Anything else is a
				// failed containment and fails the proof closed.
				if err := pidfdSignal(entry.fd, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
					closeProcFds(snapshot)
					c.gate.Unlock()
					return 0, err
				}
			}
		}
		closeProcFds(snapshot)
		c.gate.Unlock()
		if len(live) == 0 {
			return 0, nil
		}
		if !time.Now().Before(deadline) {
			return len(live), nil
		}
		time.Sleep(taskTreeVerifyPoll)
	}
}

// closeProcFds releases every pidfd a snapshot opened. A pin must never
// outlive the pass that needed the identity it guarantees.
func closeProcFds(snapshot map[int]procInfo) {
	for _, info := range snapshot {
		if info.fd >= 0 {
			_ = syscall.Close(info.fd)
		}
	}
}

// reapAdoptedZombies collects the zombie children the worker has adopted. A
// zombie has already exited and holds no descriptors, so it mutates nothing
// even before it is reaped — but it is never reaped by anyone else, and it
// is exactly the accumulation #399 closes.
func (c *taskContainment) reapAdoptedZombies(snapshot map[int]procInfo, self int) {
	for pid, info := range snapshot {
		if info.ppid != self || pid == self || c.supervises(pid) {
			continue
		}
		if info.state != "Z" && info.state != "X" {
			continue
		}
		var status syscall.WaitStatus
		if reaped, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil); err == nil && reaped > 0 {
			c.reaped.Add(1)
		}
	}
}

// adoptedLiveProcesses lists the live processes the worker has adopted:
// direct children that are not supervised task leaders, plus every
// descendant of theirs. A zombie holds no descriptors and writes nothing,
// so it is not a surviving descendant; it was collected above, and its
// descendants were reparented to this process the moment it exited.
func (c *taskContainment) adoptedLiveProcesses(snapshot map[int]procInfo, self int) []procInfo {
	children := make(map[int][]int, len(snapshot))
	for pid, info := range snapshot {
		children[info.ppid] = append(children[info.ppid], pid)
	}
	queue := make([]int, 0, 8)
	for _, pid := range children[self] {
		if pid == self || c.supervises(pid) {
			continue
		}
		queue = append(queue, pid)
	}
	live := make([]procInfo, 0, 4)
	seen := make(map[int]struct{}, len(queue))
	for len(queue) > 0 {
		pid := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if _, dup := seen[pid]; dup {
			continue
		}
		seen[pid] = struct{}{}
		info, ok := snapshot[pid]
		if !ok {
			continue
		}
		if info.state != "Z" && info.state != "X" {
			live = append(live, info)
		}
		queue = append(queue, children[pid]...)
	}
	return live
}

// directChildPIDs lists every process forked by any of this process's
// threads. Go's forkExec runs on whichever thread the calling goroutine
// holds, so an adopted orphan can be listed under any tid, not only the
// thread-group leader. An unreadable per-thread file is skipped (a thread
// can exit between reading the directory and reading its file), but a pass
// in which not one thread file could be read is an error rather than a
// silent empty listing — an empty answer that cannot be distinguished from
// an unreadable one would let the drain claim a clean pod it never saw.
func directChildPIDs() ([]int, error) {
	const taskDir = "/proc/self/task"
	tasks, err := os.ReadDir(taskDir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", taskDir, err)
	}
	var pids []int
	read := 0
	for _, task := range tasks {
		if _, err := strconv.Atoi(task.Name()); err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(taskDir, task.Name(), "children"))
		if err != nil {
			continue
		}
		read++
		for _, field := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(field)
			if err != nil {
				continue
			}
			pids = append(pids, pid)
		}
	}
	if read == 0 {
		return nil, fmt.Errorf("no %s/<tid>/children file could be read", taskDir)
	}
	return pids, nil
}

// procInfo is one process's run state, parent, and process group, read from
// a single /proc pass, together with the pidfd that pins the process the
// entry describes. The snapshot entry and the descriptor are the same
// identity: the entry was read after the pin was opened, and the pin keeps
// the number from being recycled while the descriptor is held.
type procInfo struct {
	state string
	ppid  int
	pgid  int
	fd    int
}

// processSnapshot pins and then reads every process in one /proc walk.
// Opening the pidfd before the read is what makes the entry authoritative:
// a pid that dies after the open can never be reused while the descriptor
// is held, so the stat read that follows describes exactly the process the
// descriptor will signal. A process already gone at the open, or gone by
// the time its stat is read, contributes nothing — a disappearance is not a
// survivor. Anything else — a refused or failing stat read, a malformed
// stat, a pidfd the kernel refused for a live process — fails the whole
// snapshot: an unreadable process is not evidence of absence, and a proof
// of a clean pod must not be built from a partial view. The caller owns
// every descriptor in the map and must release them with closeProcFds.
func processSnapshot() (map[int]procInfo, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	snapshot := make(map[int]procInfo, 64)
	for _, entry := range entries {
		pid, convErr := strconv.Atoi(entry.Name())
		if convErr != nil {
			continue
		}
		fd, openErr := openPidfd(pid)
		if openErr != nil {
			// ESRCH: the process was already reaped, and a reaped pid has
			// nothing left to prove. Any other refusal means a live process
			// the sweep cannot see or safely signal, which is exactly the
			// partial view that must not read as clean.
			if errors.Is(openErr, syscall.ESRCH) {
				continue
			}
			closeProcFds(snapshot)
			return nil, fmt.Errorf("pid %d could not be pinned: %w", pid, openErr)
		}
		info, statErr := processStat(pid)
		if statErr != nil {
			_ = syscall.Close(fd)
			if errors.Is(statErr, errProcessGone) {
				continue
			}
			closeProcFds(snapshot)
			return nil, statErr
		}
		info.fd = fd
		snapshot[pid] = info
	}
	return snapshot, nil
}

// errProcessGone marks the one per-process read failure that is evidence:
// the process is confirmed absent, so nothing it could have been is left to
// account for. Every other failure is not evidence of absence and fails the
// proof closed.
var errProcessGone = errors.New("process is gone")

// processStatFailureHook, when non-nil, reports a pid's stat as unreadable
// (a failure that is not a confirmed disappearance). Tests install it to pin
// the fail-closed contract on a partial /proc view. It is nil in production.
var processStatFailureHook atomic.Pointer[func(pid int) error]

// processEnumerationHook, when non-nil, fails the sweep's process
// enumeration. Tests install it to pin the conservative contract: without a
// process listing nothing about the escapees can be proven, and unproven
// termination must never read as verified. It is nil in production.
var processEnumerationHook atomic.Pointer[func() error]

var (
	armOnce sync.Once
	armErr  error
)

// ArmAdoptedChildReaper arms this process as an adopted-child reaper: it
// proves descendants can be enumerated, becomes a child subreaper so
// orphaned descendants are reparented to it even when it is not PID 1, and
// drains zombie children on every SIGCHLD. It is idempotent — the process
// shape it changes is the process's, not one Worker's.
//
// The returned error is something to log, never something to fail on: as PID
// 1 of its pod the worker adopts orphans by definition, and a worker that
// could not arm would only accumulate zombies — restarting it over that
// would trade a slow leak for an outage. An enumeration failure is the one
// case that disarms entirely: a reaper that adopts and cannot collect leaves
// more zombies behind than one that never armed at all.
func ArmAdoptedChildReaper() error {
	armOnce.Do(func() {
		if _, err := directChildPIDs(); err != nil {
			armErr = fmt.Errorf("protocol: adopted descendants cannot be enumerated, reaping disabled: %w", err)
			return
		}
		if err := becomeChildSubreaper(); err != nil {
			armErr = fmt.Errorf("protocol: could not become an adopted-child subreaper: %w", err)
		}
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGCHLD)
		go func() {
			for range ch {
				// Standard signals coalesce, so a delivered SIGCHLD may
				// stand for several dead descendants; each pass
				// re-enumerates rather than trusting the count of
				// deliveries.
				containment.drain()
			}
		}()
		containment.drain()
	})
	return armErr
}

// errSubreaperUnsupported is returned on platforms without the prctl call.
// The worker runs on Linux only; the stub exists so the package builds
// wherever the rest of it builds.
var errSubreaperUnsupported = errors.New("protocol: child-subreaper adoption is unavailable on this platform")
