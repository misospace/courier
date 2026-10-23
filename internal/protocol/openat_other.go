//go:build !linux

package protocol

// openat exists for the build graph only: readArtifact's confinement walk is
// written against Linux openat O_NOFOLLOW semantics, and an artifact read on
// any platform the walk is not trusted on fails closed instead of open.
func openat(int, string, int) (int, error) {
	return -1, errOpenatUnsupported
}
