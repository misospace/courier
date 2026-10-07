package executor

import (
	"strings"
	"testing"
)

func TestValidateEvidenceURL(t *testing.T) {
	for _, test := range []struct {
		name    string
		url     string
		wantErr string
	}{
		{name: "http", url: "http://courier-evidence.courier-system:8082", wantErr: ""},
		{name: "https with path", url: "https://intake.example.com/evidence", wantErr: ""},
		{name: "surrounding whitespace", url: "  http://intake:8082  ", wantErr: ""},
		{name: "empty is disabled", url: "", wantErr: ""},
		{name: "no scheme", url: "not-a-url", wantErr: "absolute http or https URL with a host"},
		{name: "host without scheme", url: "intake.courier-system:8082", wantErr: "absolute http or https URL with a host"},
		{name: "other scheme", url: "ftp://intake:8082", wantErr: "absolute http or https URL with a host"},
		{name: "no host", url: "http://", wantErr: "absolute http or https URL with a host"},
		{name: "empty authority", url: "http:///evidence", wantErr: "absolute http or https URL with a host"},
		{name: "userinfo", url: "http://user:pass@intake:8082", wantErr: "must not contain userinfo"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateEvidenceURL(test.url)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateEvidenceURL(%q) = %v, want nil", test.url, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateEvidenceURL(%q) = nil, want an error", test.url)
			}
			if !strings.HasPrefix(err.Error(), "executor: evidence intake URL ") {
				t.Fatalf("ValidateEvidenceURL(%q) error = %q, want the intake-URL prefix", test.url, err)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateEvidenceURL(%q) error = %q, want it to name the violated rule %q", test.url, err, test.wantErr)
			}
			if strings.Contains(err.Error(), "user:pass") {
				t.Fatalf("ValidateEvidenceURL(%q) error = %q, must not echo the value", test.url, err)
			}
		})
	}
}

func TestPodConfigValidateChecksEvidenceURLShape(t *testing.T) {
	base := func() PodConfig {
		return PodConfig{Image: "registry.example/courier-opencode:test", WorkspacePath: "/workspace"}
	}
	for _, test := range []struct {
		name    string
		url     string
		token   string
		nonce   string
		wantErr bool
	}{
		{
			name:    "malformed url with token and nonce",
			url:     "not-a-url",
			token:   "a-token",
			nonce:   "0123456789abcdef",
			wantErr: true,
		},
		{
			name:    "userinfo url with token and nonce",
			url:     "http://user:pass@intake:8082",
			token:   "a-token",
			nonce:   "0123456789abcdef",
			wantErr: true,
		},
		{
			name:  "valid url with token and nonce",
			url:   "http://courier-evidence.courier-system:8082",
			token: "a-token",
			nonce: "0123456789abcdef",
		},
		{name: "all empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := base()
			config.EvidenceURL = test.url
			config.EvidenceToken = test.token
			config.EvidenceNonce = test.nonce
			err := config.Validate()
			if test.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want an intake-URL shape error")
			}
			if test.wantErr && err != nil && !strings.Contains(err.Error(), "evidence intake URL") {
				t.Fatalf("Validate() error = %q, want it to mention the intake URL shape rule", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want no error", err)
			}
		})
	}
}
