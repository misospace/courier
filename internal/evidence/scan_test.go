package evidence

import (
	"testing"

	"github.com/misospace/courier/internal/log"
)

func TestMatched(t *testing.T) {
	const registered = "s3cr3t-registered-value"

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"registered value", "prefix " + registered + " suffix", true},
		{"pattern table shape", "token=ghp_" + "abcdefghij0123456789", true},
		{"empty string", "", false},
		{"ordinary text", "this is a normal log line about retries", false},
		{"arbitrary bytes as string", "\xff\xfe not a secret \x00", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScanner()
			s.RegisterCredentials(map[string]string{"COURIER_GIT_TOKEN": registered})
			original := tc.in
			if got := s.Matched(tc.in); got != tc.want {
				t.Errorf("Matched(%q) = %v, want %v", tc.in, got, tc.want)
			}
			if tc.in != original {
				t.Errorf("Matched mutated its input: %q -> %q", original, tc.in)
			}
		})
	}
}

func TestRedactMetadata(t *testing.T) {
	const registered = "s3cr3t-registered-value"

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"registered value", "dir/" + registered + "/file.go", "[REDACTED]"},
		{"pattern table shape", "ghp_" + "abcdefghij0123456789", "[REDACTED]"},
		{"empty string", "", ""},
		{"ordinary path", "internal/evidence/scan.go", "internal/evidence/scan.go"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScanner()
			s.RegisterCredentials(map[string]string{"COURIER_GIT_TOKEN": registered})
			original := tc.in
			if got := s.RedactMetadata(tc.in); got != tc.want {
				t.Errorf("RedactMetadata(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if tc.in != original {
				t.Errorf("RedactMetadata mutated its input: %q -> %q", original, tc.in)
			}
		})
	}
}

func TestRegisterCredentialsSecretShapedEnvName(t *testing.T) {
	const value = "credential-value-registered"

	for _, envName := range []string{"COURIER_GIT_TOKEN", "GITHUB_TOKEN", "AWS_SECRET", "DB_PASSWORD", "MODEL_API_KEY"} {
		t.Run(envName, func(t *testing.T) {
			s := NewScanner()
			s.RegisterCredentials(map[string]string{envName: value})
			if !s.Matched(value) {
				t.Errorf("value under secret-shaped env name %s not registered", envName)
			}
		})
	}
}

func TestRegisterCredentialsNonSecretShapedEnvNameNotRegistered(t *testing.T) {
	const value = "username-value-not-a-secret"

	for _, envName := range []string{"COURIER_GIT_USERNAME", "GIT_EMAIL", "BUILD_MODE"} {
		t.Run(envName, func(t *testing.T) {
			s := NewScanner()
			s.RegisterCredentials(map[string]string{envName: value})
			if s.Matched(value) {
				t.Errorf("value under non-secret-shaped env name %s was registered", envName)
			}
		})
	}
}

func TestRegisterCredentialsKeysOnEnvNameNotSecretKey(t *testing.T) {
	const value = "token-shaped-key-bad-env-name"

	ref := CredentialRef{
		EnvName:    "COURIER_GIT_USERNAME",
		SecretName: "runner-creds",
		SecretKey:  "token",
	}
	s := NewScanner()
	s.RegisterCredentials(map[string]string{ref.EnvName: value})
	if s.Matched(value) {
		t.Errorf("value with Secret key %q but env name %q was registered; shape must key on env name only",
			ref.SecretKey, ref.EnvName)
	}
}

func TestShortSecretFailClosed(t *testing.T) {
	// 7-byte value: the shared Redactor's length guard (8) silently drops it,
	// but the evidence scan must still withhold it — fail closed for durable
	// evidence, where a dropped short value is a leak, not a red herring.
	s := NewScanner()
	s.RegisterCredentials(map[string]string{"DB_PASSWORD": "abc1234"})

	if !s.Matched("password: abc1234") {
		t.Errorf("Matched = false, want true for a 7-byte secret-shaped value")
	}
	if got := s.RedactMetadata("secrets/abc1234.env"); got != "[REDACTED]" {
		t.Errorf("RedactMetadata = %q, want %q", got, "[REDACTED]")
	}
}

func TestOneByteSecretShapedFailClosed(t *testing.T) {
	// 1-byte value under a secret-shaped name. Fail closed by design: any
	// non-empty value resolved under a secret-shaped name is withheld, no
	// matter how short.
	s := NewScanner()
	s.RegisterCredentials(map[string]string{"X_SECRET": "x"})

	if !s.Matched("prefix-x-suffix") {
		t.Errorf("Matched = false, want true for a 1-byte secret-shaped value")
	}
}

func TestEmptyValueNotRetained(t *testing.T) {
	// An empty value under a secret-shaped name is retained by neither the
	// Redactor nor the literal fallback, so a scanner registered with only
	// empty values matches nothing new.
	s := NewScanner()
	s.RegisterCredentials(map[string]string{"DB_PASSWORD": "", "GITHUB_TOKEN": ""})

	if s.Matched("hello world, nothing to see here") {
		t.Errorf("scanner registered with only empty values matched benign content")
	}
}

func TestNonSecretShapedShortValueNotMatched(t *testing.T) {
	// A short value under a non-secret-shaped name is neither registered nor
	// retained, so it must not match.
	s := NewScanner()
	s.RegisterCredentials(map[string]string{"COURIER_GIT_USERNAME": "bob"})

	if s.Matched("hello bob") {
		t.Errorf("short value under a non-secret-shaped name should not match")
	}
}

func TestIsSecretEnvNameSyncWithLog(t *testing.T) {
	// Sync test between the local predicate and internal/log's observable
	// behavior: registering an 8+ byte value under a name and checking whether
	// the Redactor redacts it reveals whether internal/log treats that name as
	// secret-shaped. The two implementations must agree.
	const value = "aaaabbbbcccc1111" // 16 bytes, >= internal/log's length guard

	secretShaped := []string{
		"COURIER_GIT_TOKEN", "GITHUB_TOKEN", "DB_PASSWORD", "MY_SECRET",
		"FOO_PASSWD", "BAR_PASS", "MY_API_KEY", "MY_APIKEY",
		"AZURE_CREDENTIAL", "AWS_CREDENTIALS", "SIGNING_KEY",
	}
	nonSecretShaped := []string{
		"COURIER_GIT_USERNAME", "PATH", "MODEL_NAME", "HOME", "RUN_NAMESPACE",
		"GITHUB_REPO", "COURIER_INSTANCE", "LANG", "WORKDIR",
	}

	for _, name := range append(append([]string{}, secretShaped...), nonSecretShaped...) {
		t.Run(name, func(t *testing.T) {
			r := log.NewRedactor()
			r.RegisterEnvironment([]string{name + "=" + value})
			observable := r.Redact(value) != value

			if got := isSecretEnvName(name); got != observable {
				t.Errorf("isSecretEnvName(%q) = %v, internal/log observable = %v", name, got, observable)
			}
		})
	}
}
