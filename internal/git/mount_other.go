//go:build !linux

package git

// isMountRoot reports whether directory is a mount point.  Only Linux answers
// that here, so every other platform stays conservative: a single-segment
// path is refused rather than cleared, and a path with two or more segments
// does not need the answer.
func isMountRoot(string) (bool, error) {
	return false, nil
}
