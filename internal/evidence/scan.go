package evidence

import (
	"sort"

	"github.com/misospace/courier/internal/log"
)

// CredentialRef is the portable form of one pod-builder credential mapping:
// the env var name the executor receives and the Secret key that supplies it.
// It is the single source of truth shared with the pod builder (which
// translates it to corev1.EnvVar{ValueFrom.SecretKeyRef}) and the intake
// (which re-scans against the same set). The evidence package MUST NOT import
// Kubernetes types; this plain struct is that shared contract.
type CredentialRef struct {
	EnvName    string
	SecretName string
	SecretKey  string
}

// Scanner decides fail-closed whether a string contains a credential. It
// delegates to internal/log's Redactor, so registration keys on environment
// variable name shape and the defensive pattern table always applies.
type Scanner struct {
	red *log.Redactor
}

// NewScanner returns a Scanner with no registered credentials; only the
// defensive pattern table applies.
func NewScanner() *Scanner {
	return &Scanner{red: log.NewRedactor()}
}

// RegisterCredentials registers resolved credential values so the scan withholds
// them. values maps CredentialRef.EnvName -> resolved secret value; only values
// whose ENV NAME is secret-shaped (internal/log's rule) are registered — a value
// reached through a non-secret-shaped env name (e.g. COURIER_GIT_USERNAME) is
// deliberately NOT registered, and the Secret KEY is never consulted.
func (s *Scanner) RegisterCredentials(values map[string]string) {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	environ := make([]string, 0, len(values))
	for _, name := range names {
		environ = append(environ, name+"="+values[name])
	}
	s.red.RegisterEnvironment(environ)
}

// Matched reports whether v contains a registered credential or a
// pattern-table credential shape. Fail closed: any match is a match.
func (s *Scanner) Matched(v string) bool {
	return s.red.Redact(v) != v
}

// RedactMetadata returns log.RedactedPlaceholder when v matches, else v
// unchanged. Used for metadata-only strings (paths, symlink targets): a
// matching name is substituted with the placeholder in full — metadata is not
// evidence.
func (s *Scanner) RedactMetadata(v string) string {
	if s.Matched(v) {
		return log.RedactedPlaceholder
	}
	return v
}
