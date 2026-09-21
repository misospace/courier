package log

import (
	"regexp"
	"sort"
	"strings"
	"sync"
)

// RedactedPlaceholder replaces every redacted secret. It appears only inside
// serialized event values, so it is safe for Loki/VictoriaLogs queries to
// treat as "a secret was here".
const RedactedPlaceholder = "[REDACTED]"

// minRegisteredSecretLength keeps a literal registration from mangling every
// line it appears in. Real bearer tokens and API keys are longer; a shorter
// "secret" is either a red herring or too collision-prone to substitute on.
const minRegisteredSecretLength = 8

// Redactor removes secrets from strings before they are serialized or
// emitted. Two mechanisms complement each other:
//
//   - Explicit registration. Callers register the literal secret values they
//     actually hold (git tokens, forge tokens, provider keys, environment
//     values passed through RegisterEnvironment). Registered values are the
//     primary mechanism: they are matched exactly and removed completely,
//     regardless of the shape they appear in.
//   - Defensive patterns. Common credential shapes — URL userinfo,
//     authorization header values, credential query parameters, and a table
//     of well-known public token formats — are redacted even when
//     unregistered. The pattern table is best-effort defense in depth for
//     shapes a caller forgot to register; it is not a complete substitute
//     for registration and it deliberately knows nothing about any specific
//     deployment.
//
// A Redactor is safe for concurrent use. Redact never mutates its input and
// never reports which secret it removed.
type Redactor struct {
	mu      sync.Mutex
	secrets []string
	sorted  []string
}

// NewRedactor constructs an empty Redactor.
func NewRedactor() *Redactor { return &Redactor{} }

// Register adds one literal secret value. Empty and short values are ignored
// (see minRegisteredSecretLength): registering a short literal would corrupt
// unrelated output that happens to contain it. Registering the same value
// twice is a no-op.
func (r *Redactor) Register(secret string) {
	secret = strings.TrimSpace(secret)
	if len(secret) < minRegisteredSecretLength {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.secrets {
		if existing == secret {
			return
		}
	}
	r.secrets = append(r.secrets, secret)
	r.sorted = append([]string(nil), r.secrets...)
	// Longest first: when one registered secret is a prefix of another,
	// replacing the shorter one first would leave the remainder of the
	// longer secret visible in the output.
	sort.Slice(r.sorted, func(i, j int) bool { return len(r.sorted[i]) > len(r.sorted[j]) })
}

// RegisterEnvironment registers the values of secret-bearing environment
// variables from a list of "NAME=VALUE" entries (os.Environ's shape). Names
// are matched by shape, not by provider: anything that names a token, key,
// secret, password, or credential is registered. Deployment-provided
// provider keys (model API keys, forge tokens) therefore need no per-provider
// configuration here.
func (r *Redactor) RegisterEnvironment(environ []string) {
	for _, entry := range environ {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || value == "" {
			continue
		}
		if isSecretEnvName(name) {
			r.Register(value)
		}
	}
}

// isSecretEnvName reports whether an environment variable name is shaped like
// a credential. The check is deliberately broad: registering a value that
// turns out to be harmless only costs a substitution, while missing a token
// costs retained credentials in the log store.
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

// redactionRule pairs one defensive pattern with its replacement.
type redactionRule struct {
	pattern     *regexp.Regexp
	replacement string
}

// The userinfo rules keep the scheme and redact the whole userinfo component,
// whichever position the credential occupies: some forges take the token as
// the password, others as the username. Usernames use a conservative
// RFC-style charset so a query-string "user@" in a pathless URL is not
// mistaken for credentials; the password side stays broad because it must
// absorb arbitrary tokens. Character classes exclude quotes and whitespace
// so a rule can never reach across JSON string boundaries.
var redactionRules = []redactionRule{
	{
		// scheme://user:password@host
		pattern:     regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[a-zA-Z0-9._%~-]*:[^\s/@:"'<>]+@`),
		replacement: `$1` + RedactedPlaceholder + `@`,
	},
	{
		// scheme://token@host (credential in the username position)
		pattern:     regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[a-zA-Z0-9._%~-]+@`),
		replacement: `$1` + RedactedPlaceholder + `@`,
	},
	{
		// Authorization-style fields: "Authorization: Bearer t",
		// "proxy-authorization":"Basic t", shell forms with '='. Both
		// surrounding quotes and the scheme's trailing space are captured
		// and restored so JSON stays valid. The credential needs 8+
		// charset characters: long enough for real tokens, short enough
		// that scheme words and "none"/"null" sentinels never match, which
		// also keeps re-redacting already-redacted text a no-op.
		pattern:     regexp.MustCompile(`(?i)\b((?:proxy-)?authorization)("?)\s*([:=])\s*("?)((?:bearer|basic|token|digest)[ \t]+)?[a-z0-9._~+/=-]{8,}`),
		replacement: `$1$2$3$4$5` + RedactedPlaceholder,
	},
	{
		// Credential query parameters: ?token=…, &api_key=…, ?key=….
		// The delimiter preceding the parameter name keeps lookalike names
		// (sort_key, cache_token_ttl) untouched.
		pattern:     regexp.MustCompile(`(?i)([?&](?:access_token|api_key|apikey|token|secret|password|passwd|key|credentials?|authorization|sig|signature)=)[^\s&"']+`),
		replacement: `$1` + RedactedPlaceholder,
	},
	{
		// Well-known public token formats, unregistered. This table covers
		// the common stable prefixes (forge, cloud-provider, and generic
		// API-key shapes) as best-effort defense; it is intentionally not
		// exhaustive and registration remains the primary mechanism.
		pattern: regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{16,}` +
			`|github_pat_[A-Za-z0-9_]{22,}` +
			`|sk-ant-[A-Za-z0-9_-]{30,}` +
			`|sk-[A-Za-z0-9_-]{20,}` +
			`|AKIA[0-9A-Z]{16}` +
			`|ASIA[0-9A-Z]{16}` +
			`|xox[baprse]-[A-Za-z0-9-]{10,}` +
			`|glpat-[A-Za-z0-9_-]{15,}` +
			`|gsk_[A-Za-z0-9]{20,}` +
			`|shpat_[A-Za-z0-9]{20,}` +
			`|dop_v1_[a-f0-9]{32,})`),
		replacement: RedactedPlaceholder,
	},
}

// Redact returns value with every registered secret and every
// pattern-matched credential replaced by RedactedPlaceholder. Redaction is
// idempotent: redacting already-redacted text returns it unchanged. Normal
// strings — prose, URLs without credentials, ordinary query parameters,
// email addresses — pass through unmodified.
func (r *Redactor) Redact(value string) string {
	if value == "" {
		return value
	}
	r.mu.Lock()
	sorted := r.sorted
	r.mu.Unlock()
	for _, secret := range sorted {
		if strings.Contains(value, secret) {
			value = strings.ReplaceAll(value, secret, RedactedPlaceholder)
		}
	}
	for _, rule := range redactionRules {
		value = rule.pattern.ReplaceAllString(value, rule.replacement)
	}
	return value
}
