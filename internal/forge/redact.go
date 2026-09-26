package forge

import "regexp"

// redactedPlaceholder replaces any secret-like token in a diagnostic.
const redactedPlaceholder = "[REDACTED]"

var (
	// userinfoPattern matches the whole "user:pass" userinfo (user and
	// password) of a "scheme://user:pass@host" URL.
	userinfoPattern = regexp.MustCompile(`://([^/@]+)@`)
	// bearerPattern matches a case-insensitive "bearer <token>" pair.
	bearerPattern = regexp.MustCompile(`(?i)bearer\s+([A-Za-z0-9\-._~+/]{8,})`)
)

// RedactDetail returns a diagnostic safe to surface to the core. It removes
// any occurrence of a secret-like token so an unavailability reason cannot
// leak a credential. It is intentionally minimal: it is a last-line guard,
// not a general secrets scanner (the broker, not this package, holds real
// credentials).
//
// It redacts: the whole userinfo (user and password) of any
// "scheme://user:pass@host" URL, and any value that looks like an
// Authorization bearer token.
func RedactDetail(detail string) string {
	detail = userinfoPattern.ReplaceAllString(detail, "://"+redactedPlaceholder+"@")
	return bearerPattern.ReplaceAllString(detail, "bearer "+redactedPlaceholder)
}
