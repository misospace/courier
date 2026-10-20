package forge

import "errors"

// IsTransientAvailability reports whether err is a transient availability
// failure of a forge read. A transient availability error means the read
// could not complete at all — the forge answered with a rate limit or a
// server error, or the transport failed before a response existed — so the
// world was never observed. Callers must treat such an error as
// unverifiable, never as a verified contradiction: a read that did not
// happen proves nothing about whether a pull request exists or what its
// body says. Provider implementations mark their own errors by
// implementing TransientAvailability() bool; the predicate traverses
// wrapped errors so boundary wrappers carrying the original typed error
// through still classify.
func IsTransientAvailability(err error) bool {
	var transient interface{ TransientAvailability() bool }
	return errors.As(err, &transient) && transient.TransientAvailability()
}
