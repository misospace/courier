//go:build linux

package protocol

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// openPidfd opens a pidfd for pid. While the returned descriptor stays
// open, the kernel will not recycle that numeric pid for another process,
// and a signal sent through the descriptor reaches the process it was
// opened for or, once that process is reaped, fails with ESRCH — never a
// process that reused the number. Opening one's own descendant needs no
// privilege.
func openPidfd(pid int) (int, error) {
	return unix.PidfdOpen(pid, 0)
}

// pidfdSignal delivers sig to exactly the process the pidfd refers to.
func pidfdSignal(pidfd int, sig syscall.Signal) error {
	return unix.PidfdSendSignal(pidfd, sig, nil, 0)
}
