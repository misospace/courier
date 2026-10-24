//go:build unix

package evidence

import (
	"os"
	"syscall"
)

// captureOpenFlags composes readCapped's open flags. O_NOFOLLOW is the
// confinement of the open itself — a path swapped for a symlink between the
// classification and the open fails with ELOOP instead of following the new
// target — and O_NONBLOCK keeps the open from waiting for a peer that may
// never come. Both are POSIX ABI, so this composition is what the capture
// trusts.
func captureOpenFlags() (int, error) {
	return os.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_NONBLOCK, nil
}
