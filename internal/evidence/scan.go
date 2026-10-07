package evidence

import (
	"sort"
	"strings"

	"github.com/misospace/courier/internal/log"
)

// CredentialRef is the portable, Kubernetes-free form of one pod-builder
// credential mapping: the env var name the executor receives and the Secret
// key that supplies it. It is the credential contract INTENDED to be shared
// with the pod builder (which will translate it to
// corev1.EnvVar{ValueFrom.SecretKeyRef}) and the intake (which re-scans
// against the same set); those wirings land in issue #199/#228 and issue
// #200 respectively and do not exist on this head. The evidence package MUST
// NOT import Kubernetes types; this plain struct is that portable contract.
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
	// literals retains non-empty resolved values under secret-shaped env
	// names as-is (no trimming), including the 1-7 byte values the shared
	// Redactor's length guard would silently drop, so the evidence scan
	// fails closed for short credentials.
	literals []string
}

// NewScanner returns a Scanner with no registered credentials; only the
// defensive pattern table applies.
func NewScanner() *Scanner {
	return &Scanner{red: log.NewRedactor()}
}

// RegisterCredentials registers resolved credential values so the scan withholds
// them. values maps CredentialRef.EnvName -> resolved secret value. Values whose
// ENV NAME is secret-shaped (internal/log's rule) are registered with the shared
// Redactor — a value reached through a non-secret-shaped env name (e.g.
// COURIER_GIT_USERNAME) is deliberately NOT registered, and the Secret KEY is
// never consulted. In addition, every non-empty value under a secret-shaped name
// is retained as-is (no trimming) so short 1-7 byte values the Redactor's length
// guard would drop still match, keeping the evidence scan fail closed.
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

	// The shared Redactor drops values shorter than its length guard, which
	// is a log-volume heuristic, not a safety boundary. For durable evidence
	// that is a fail-open hole, so retain every non-empty value whose env
	// name is secret-shaped, as-is, regardless of length. A value already
	// retained by an earlier call is not appended again.
	seen := make(map[string]bool, len(values)+len(s.literals))
	for _, existing := range s.literals {
		seen[existing] = true
	}
	for _, name := range names {
		value := values[name]
		if value == "" || !isSecretEnvName(name) || seen[value] {
			continue
		}
		seen[value] = true
		s.literals = append(s.literals, value)
	}
}

// isSecretEnvName reports whether an environment variable name is shaped like
// a credential. It mirrors internal/log's private isSecretEnvName (kept in
// sync by a sync test) because internal/log's copy is unexported and must not
// change; the evidence package needs the same shape check to retain short
// values the shared Redactor's length guard would drop.
func isSecretEnvName(name string) bool {
	name = strings.ToUpper(strings.TrimSpace(name))
	for _, suffix := range []string{
		"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASS",
		"API_KEY", "APIKEY", "KEY", "CREDENTIAL", "CREDENTIALS",
	} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// Matched reports whether v contains a registered credential, a retained
// short literal, or a pattern-table credential shape. Fail closed: any match
// is a match.
func (s *Scanner) Matched(v string) bool {
	if s.red.Redact(v) != v {
		return true
	}
	for _, lit := range s.literals {
		if strings.Contains(v, lit) {
			return true
		}
	}
	return false
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
