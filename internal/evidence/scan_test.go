package evidence

import "testing"

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
