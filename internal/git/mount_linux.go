//go:build linux

package git

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// isMountRoot reports whether directory is a mount point: a directory whose
// device differs from its parent's.  The pod's workspace volume is mounted at
// a single-segment path, and that mount root is the directory the clear step
// has to empty.
func isMountRoot(directory string) (bool, error) {
	var info, parent unix.Stat_t
	if err := unix.Stat(directory, &info); err != nil {
		return false, err
	}
	if err := unix.Stat(filepath.Dir(directory), &parent); err != nil {
		return false, err
	}
	return info.Dev != parent.Dev, nil
}
