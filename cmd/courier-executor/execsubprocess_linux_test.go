//go:build linux

package main

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// waitProcessExitEvent waits until pid is no longer a running process and
// reports how it waited. The wait is event-driven: a pidfd becomes readable
// when the process exits, so the wait ends on the kernel's event rather than
// on a poll interval.
//
// The watched process is not a child of this test process (it is a descendant
// of the executor subprocess), so the kernel may refuse the pidfd (EPERM) in
// a restrictive environment; the tight poll stands in then. The bound is a
// failure ceiling, never the expected wait.
func waitProcessExitEvent(t *testing.T, pid int, bound time.Duration) (bool, string) {
	t.Helper()
	if !processRunning(t, pid) {
		return true, "already exited"
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		t.Logf("pidfd for pid %d refused (%v); waiting on the process state instead", pid, err)
		return waitProcessGoneByPolling(t, pid, bound), "process state poll"
	}
	defer unix.Close(fd)
	exited, timedOut := waitPidfdExit(fd, bound)
	if timedOut {
		return !processRunning(t, pid), "pidfd (bound reached)"
	}
	if !exited {
		return waitProcessGoneByPolling(t, pid, bound), "process state poll"
	}
	// The kernel reported the exit; a zombie is dead too.
	return !processRunning(t, pid), "pidfd"
}

// waitPidfdExit waits for the pidfd to report the process's exit, and reports
// whether the wait ran out of time.
func waitPidfdExit(fd int, bound time.Duration) (exited bool, timedOut bool) {
	polls := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	deadline := time.Now().Add(bound)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return false, true
		}
		timeout := int(left.Milliseconds()) + 1
		n, err := unix.Poll(polls, timeout)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return false, false
		}
		if n > 0 {
			return true, false
		}
	}
}
