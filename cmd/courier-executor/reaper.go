//go:build linux

package main

import (
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

	"golang.org/x/sys/unix"
)

// adoptedChildReaper reaps the descendants the executor has adopted but never
// supervised. The executor is PID 1 of the coordinator pod — the pod sets
// Command directly, so there is no shell above it — and the kernel reparents
// every orphaned descendant (npm, node, esbuild, a stuck tail) to PID 1. PID 1
// never reaps on its own, so without this the pod accumulates zombies for the
// length of the run (#347).
//
// Reaping is the whole job: a live orphan keeps running exactly as long as its
// dead parent left it running. Nothing here ever signals, kills, or touches a
// process group, and a supervised child is never reaped here — os/exec's Wait
// is what preserves a supervised child's exit status.
type adoptedChildReaper struct {
	// gate is held across a child's Start and its pid registration, and across
	// every drain pass. Because a supervised pid is therefore either registered
	// before the pass enumerates the descendants or does not exist yet, a drain
	// pass can never reap a supervised child between its fork and its
	// registration.
	gate sync.Mutex
	// mu guards supervised and groups.
	mu sync.Mutex
	// supervised holds the pids whose exit status belongs to os/exec.
	supervised map[int]struct{}
	// groups holds the process groups a supervised child leads. Only a child
	// spawned with its own process group gives the descendants it leaves
	// behind a marker the kernel keeps after its parent dies, and that marker
	// is what separates the orphans worth collecting from the children this
	// process spawns itself.
	groups map[int]struct{}
	// reaped counts the adopted descendants this reaper collected. It is the
	// observable the tests use; nothing reads it in the run itself.
	reaped atomic.Uint64
	// armed records that this instance may adopt and collect. Only main arms.
	armed bool
}

var (
	reaperMu      sync.Mutex
	processReaper = &adoptedChildReaper{supervised: make(map[int]struct{}), groups: make(map[int]struct{})}
)

// installAdoptedChildReaper arms the process-wide reaper and returns it. Only
// main arms it: an in-process caller — `go test` runs this executor in process —
// would turn the test binary into a subreaper that adopts every orphan its
// subprocesses leave and then, correctly, declines to collect most of them.
// Every other caller goes through childReaper, which is a funnel and nothing
// more until the reaper is armed.
func installAdoptedChildReaper() *adoptedChildReaper {
	reaperMu.Lock()
	defer reaperMu.Unlock()
	if processReaper.armed {
		return processReaper
	}
	processReaper.armed = true
	armAdoptedChildReaper(processReaper)
	return processReaper
}

// childReaper returns the reaper every supervised child goes through, whether or
// not it has been armed.
func childReaper() *adoptedChildReaper {
	reaperMu.Lock()
	defer reaperMu.Unlock()
	return processReaper
}

// armAdoptedChildReaper gives r the ability to collect. Neither failure below is
// fatal: the run must never fail because a reaper could not be armed.
func armAdoptedChildReaper(r *adoptedChildReaper) {
	// Prove descendants can be enumerated before adopting anything: a reaper
	// that adopts and cannot collect leaves more zombies behind than one that
	// never armed at all.
	if _, err := descendantPIDs(); err != nil {
		fmt.Fprintln(os.Stderr, "courier: adopted descendants cannot be enumerated, reaping disabled:", err)
		return
	}
	// PID 1 adopts orphans by definition. PR_SET_CHILD_SUBREAPER gives the
	// same adoption when the executor is not PID 1, so the reaper holds in a
	// test binary and in any future embedding of this code.
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		fmt.Fprintln(os.Stderr, "courier: could not become an adopted-child subreaper:", err)
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGCHLD)
	go func() {
		for range ch {
			// Standard signals coalesce, so a delivered SIGCHLD may stand for
			// several dead descendants. Each pass re-enumerates rather than
			// trusting the count of deliveries.
			r.drain()
		}
	}()
}

// descendantPIDs lists every process forked by any of this process's threads.
// Go's forkExec runs on whichever thread the calling goroutine holds, so an
// adopted orphan can be listed under any tid, not only the thread-group
// leader. An unreadable per-thread file is skipped (a thread can exit between
// reading the directory and reading its file); failing to read the directory
// itself is an error.
func descendantPIDs() ([]int, error) {
	const taskDir = "/proc/self/task"
	tasks, err := os.ReadDir(taskDir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", taskDir, err)
	}
	var pids []int
	for _, task := range tasks {
		if _, err := strconv.Atoi(task.Name()); err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(taskDir, task.Name(), "children"))
		if err != nil {
			continue
		}
		for _, field := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(field)
			if err != nil {
				continue
			}
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

// drain collects every dead adopted descendant of a supervised child, repeating
// while a pass still reaps something because each reap can expose further
// zombies. Reaping only: no signal, no kill, no process-group operation.
func (r *adoptedChildReaper) drain() {
	r.gate.Lock()
	defer r.gate.Unlock()
	for {
		pids, err := descendantPIDs()
		if err != nil {
			return
		}
		collected := false
		for _, pid := range pids {
			if r.supervises(pid) {
				continue
			}
			// An adopted orphan is an ordinary child of this process, so the
			// kernel offers nothing that tells it apart from the children this
			// process spawns itself — the git calls in internal/git, the
			// capture, the askpass — whose exit statuses os/exec owns and which
			// no registry here knows about. Reaping one of those would replace
			// a real git status with ECHILD and silently change a run's ending,
			// so a descendant is collectable only when it sits in a process
			// group a supervised child led. That covers the model child's tool
			// tree, which is what leaks. It does not cover a descendant that
			// made a group or session of its own — a setsid, a detached spawn —
			// or the git and ssh children a killed run orphans into the
			// executor's own group: those are adopted, and left to be.
			group, ok := procProcessGroup(pid)
			if !ok || !r.watches(group) {
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
			r.reaped.Add(1)
			collected = true
		}
		if !collected {
			return
		}
	}
}

// procProcessGroup returns the process group recorded for pid. The comm field
// can contain spaces and parentheses, so parsing starts after its closing one.
func procProcessGroup(pid int) (int, bool) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	text := string(data)
	end := strings.LastIndex(text, ")")
	if end < 0 {
		return 0, false
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 3 {
		return 0, false
	}
	group, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0, false
	}
	return group, true
}

// supervises reports whether pid is a child whose exit status os/exec owns.
func (r *adoptedChildReaper) supervises(pid int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.supervised[pid]
	return ok
}

// watches reports whether group is one a supervised child led. The set outlives
// the child itself: the orphans worth collecting only exist once it exited.
func (r *adoptedChildReaper) watches(group int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.groups[group]
	return ok
}

// run is the single funnel for a supervised child: Start, register, Wait,
// unregister. Registration happens under the gate so no drain pass can reap the
// child out from under Wait, and unregistration happens only after Wait has
// taken the status.
func (r *adoptedChildReaper) run(cmd *exec.Cmd) error {
	r.gate.Lock()
	if err := cmd.Start(); err != nil {
		r.gate.Unlock()
		return err
	}
	pid := cmd.Process.Pid
	r.mu.Lock()
	r.supervised[pid] = struct{}{}
	if cmd.SysProcAttr != nil && cmd.SysProcAttr.Setpgid {
		r.groups[pid] = struct{}{}
	}
	r.mu.Unlock()
	r.gate.Unlock()

	err := cmd.Wait()

	r.mu.Lock()
	delete(r.supervised, pid)
	r.mu.Unlock()
	return err
}
