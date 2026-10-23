//go:build linux

package protocol

import "syscall"

// openat is the platform boundary of readArtifact's confinement walk: each
// call opens a single path component from the caller's directory descriptor
// under the caller's full flag set (O_NOFOLLOW among them). Linux's
// syscall.Openat is the only openat the walk's confinement semantics are
// trusted on, so no other platform routes through it.
func openat(dirfd int, name string, flags int) (int, error) {
	return syscall.Openat(dirfd, name, flags, 0)
}
