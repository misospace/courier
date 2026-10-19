//go:build !linux

package protocol

// becomeChildSubreaper exists for the build graph only: on any platform
// without it, directChildPIDs has already failed (there is no /proc to
// enumerate), so ArmAdoptedChildReaper never reaches this call.
func becomeChildSubreaper() error {
	return errSubreaperUnsupported
}
