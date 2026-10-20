//go:build !linux

package protocol

import "syscall"

// openPidfd and pidfdSignal exist for the build graph only: on any platform
// without pidfds there is no /proc to enumerate either, so the containment
// proof fails closed before either call matters.
func openPidfd(int) (int, error) { return -1, syscall.ENOSYS }

func pidfdSignal(int, syscall.Signal) error { return syscall.ENOSYS }
