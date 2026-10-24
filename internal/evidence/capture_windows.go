//go:build windows

package evidence

import "errors"

// captureOpenFlags exists for the build graph only: the capture's read is
// written against POSIX open ABI (O_NOFOLLOW, O_NONBLOCK), and an open
// without the flags that bound it is not one the evidence contract trusts,
// so the read fails closed instead of running unrestricted.
var errCaptureOpenUnsupported = errors.New("evidence: capture open flags are unavailable on this platform")

func captureOpenFlags() (int, error) {
	return 0, errCaptureOpenUnsupported
}
