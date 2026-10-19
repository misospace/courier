//go:build linux

package protocol

import "golang.org/x/sys/unix"

// becomeChildSubreaper asks the kernel to reparent this process's orphaned
// descendants to it instead of to the nearest init or subreaper. It needs no
// capability; as PID 1 the adoption happens by definition and the flag only
// makes the same behaviour hold for any other shape.
func becomeChildSubreaper() error {
	return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
}
