//go:build windows

package protocol

import (
	"errors"
	"syscall"
)

// The worker's containment proof is written against Linux process semantics:
// /proc, pidfs, prctl, POSIX wait, and process groups. Windows offers none of
// the ABI those proofs rest on. These stand-ins exist for the build graph
// only and fail closed, mirroring the pidfd, openat, and subreaper pairs: a
// platform that can prove nothing answers as unproven, never as cleanly
// verified.

var errProcessGroupUnsupported = errors.New("protocol: process-group control is unavailable on this platform")

func closeFd(int) {}

// reapNoHang has nothing to reap into: without POSIX wait a zombie status
// stays an os/exec concern.
func reapNoHang(int) bool { return false }

// watchChildSignals has no child-death signal to watch.
func watchChildSignals() {}

func signalProcessGroup(int, syscall.Signal) error { return errProcessGroupUnsupported }

func probeProcessGroup(int) error { return errProcessGroupUnsupported }

// taskSysProcAttr leaves the task child in the default group: group signals
// already fail closed, so claiming a group this platform cannot signal or
// verify would lie.
func taskSysProcAttr() *syscall.SysProcAttr { return nil }

func walkOpenFlags(bool) int { return syscall.O_RDONLY }
