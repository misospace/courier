//go:build unix

package protocol

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
)

// closeFd releases a pidfd or an openat-walk descriptor. It is the
// containment namespace's syscall.Close seam: Windows's Close takes a
// syscall.Handle rather than an int descriptor, and the containment code is
// written against the POSIX ABI.
func closeFd(fd int) { _ = syscall.Close(fd) }

// reapNoHang reaps pid's zombie without blocking, answering whether the
// status was collected right now. WNOHANG: a live orphan is not this pass's
// business, and ECHILD (already collected) and EINTR (a signal beat us to
// it) are not failures worth reporting.
func reapNoHang(pid int) bool {
	var status syscall.WaitStatus
	reaped, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
	return err == nil && reaped > 0
}

// watchChildSignals starts the drain of zombie children on every SIGCHLD.
// Standard signals coalesce, so a delivered SIGCHLD may stand for several
// dead descendants; each pass of the loop re-enumerates rather than trusting
// the count of deliveries.
func watchChildSignals() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGCHLD)
	go func() {
		for range ch {
			containment.drain()
		}
	}()
}

// signalProcessGroup delivers sig to the whole process group led by pid. A
// group that is already gone counts as success: there is nothing left to
// terminate, and reporting it as a failure would mask a completed cleanup.
func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	err := syscall.Kill(-pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

// probeProcessGroup is the signal-0 liveness probe the empty-group proof
// falls back to: a group that answers ESRCH has no live member left.
func probeProcessGroup(pgid int) error {
	return syscall.Kill(-pgid, syscall.Signal(0))
}

// taskSysProcAttr gives the task child a process group of its own, which is
// what makes a group signal reach the task's descendants.
func taskSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// walkOpenFlags composes the confinement walk's per-component open flags.
// O_NOFOLLOW is the walk's whole boundary: no component — parent or final —
// may be a symlink at open time. For the final component, O_NONBLOCK keeps a
// swapped-in FIFO from blocking the open; the regular-file check on the
// descriptor rejects it right after.
func walkOpenFlags(parent bool) int {
	flags := syscall.O_NOFOLLOW | syscall.O_CLOEXEC | syscall.O_RDONLY
	if parent {
		flags |= syscall.O_DIRECTORY
	} else {
		flags |= syscall.O_NONBLOCK
	}
	return flags
}
