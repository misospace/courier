package main

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRunArgsFailsClosed covers the binary's flag validation: an incomplete
// or unknown configuration is a construction error, never a half-listening
// console.
func TestRunArgsFailsClosed(t *testing.T) {
	dir := t.TempDir()
	sessionKeyFile := filepath.Join(dir, "session-key")
	tokensFile := filepath.Join(dir, "tokens")
	if err := os.WriteFile(sessionKeyFile, []byte("key-material\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokensFile, []byte("token-material\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "token mode without secrets",
			args: []string{"--auth-mode=token"},
			want: "requires --session-key-file and --operator-tokens-file",
		},
		{
			name: "token mode with a missing session key file",
			args: []string{"--session-key-file=" + filepath.Join(dir, "absent"), "--operator-tokens-file=" + tokensFile},
			want: "read",
		},
		{
			name: "token mode with a missing tokens file",
			args: []string{"--session-key-file=" + sessionKeyFile, "--operator-tokens-file=" + filepath.Join(dir, "absent")},
			want: "read",
		},
		{
			name: "header mode without a trusted header",
			args: []string{"--auth-mode=header"},
			want: "requires --trusted-header",
		},
		{
			name: "unknown auth mode",
			args: []string{"--auth-mode=cookie"},
			want: `unknown --auth-mode "cookie"`,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := runArgs(testCase.args)
			if err == nil {
				t.Fatalf("runArgs(%q) = nil, want an error naming %q", testCase.args, testCase.want)
			}
			if !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("runArgs(%q) error = %v, want it to name %q", testCase.args, err, testCase.want)
			}
		})
	}
}

// TestContributionFlags covers contribution flag pairing and file loading.
func TestContributionFlags(t *testing.T) {
	dir := t.TempDir()
	sessionKeyFile := filepath.Join(dir, "session-key")
	tokensFile := filepath.Join(dir, "tokens")
	contributionTokenFile := filepath.Join(dir, "contribute-token")
	for path, value := range map[string]string{sessionKeyFile: "key", tokensFile: "operator", contributionTokenFile: " admission-token\n"} {
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	base := []string{"--session-key-file=" + sessionKeyFile, "--operator-tokens-file=" + tokensFile}
	for _, args := range [][]string{{"--contribute-url=http://example.test"}, {"--contribute-token-file=" + contributionTokenFile}} {
		_, _, err := parseFlags(append(append([]string{}, base...), args...))
		if err == nil || !strings.Contains(err.Error(), "--contribute-url and --contribute-token-file") {
			t.Fatalf("pair args %v error = %v", args, err)
		}
	}
	_, cfg, err := parseFlags(append(append([]string{}, base...), "--contribute-url=https://example.test/", "--contribute-token-file="+contributionTokenFile, "--contribute-timeout=3s"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ContributeURL != "https://example.test/" || string(cfg.ContributeToken) != "admission-token" || cfg.ContributeTimeout != 3*time.Second {
		t.Fatalf("contribution config = %#v", cfg)
	}
	for _, timeout := range []string{"0s", "30s", "31s"} {
		_, _, err := parseFlags(append(append([]string{}, base...), "--contribute-timeout="+timeout))
		if err == nil || !strings.Contains(err.Error(), "less than 30s") {
			t.Errorf("timeout %s error = %v", timeout, err)
		}
	}
}

// TestHTTPServerTimeouts pins the bounded-listener posture of the console's
// http.Server: every listener timeout is set, and the write bound outlives
// the default backend probe a page render may take.
func TestHTTPServerTimeouts(t *testing.T) {
	srv := newHTTPServer(defaultBind, http.NewServeMux())
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"ReadHeaderTimeout", srv.ReadHeaderTimeout},
		{"ReadTimeout", srv.ReadTimeout},
		{"WriteTimeout", srv.WriteTimeout},
		{"IdleTimeout", srv.IdleTimeout},
	} {
		if tc.timeout <= 0 {
			t.Errorf("%s = %v, want a bounded timeout", tc.name, tc.timeout)
		}
	}
	if srv.WriteTimeout <= defaultBackendTimeout {
		t.Errorf("WriteTimeout %v must outlive the %v default backend probe", srv.WriteTimeout, defaultBackendTimeout)
	}
}

// TestParseFlagsCookiePosture pins the binary's cookie posture plumbing:
// the default mints Secure session cookies, and --cookie-secure=false is
// the only opt-out. The console listener is plaintext HTTP in every
// supported deployment, so the posture must not depend on the listener.
func TestParseFlagsCookiePosture(t *testing.T) {
	dir := t.TempDir()
	sessionKeyFile := filepath.Join(dir, "session-key")
	tokensFile := filepath.Join(dir, "tokens")
	if err := os.WriteFile(sessionKeyFile, []byte("key-material\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokensFile, []byte("token-material\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--session-key-file=" + sessionKeyFile, "--operator-tokens-file=" + tokensFile}

	bind, cfg, err := parseFlags(base)
	if err != nil {
		t.Fatal(err)
	}
	if bind != defaultBind {
		t.Fatalf("default bind = %q, want %q", bind, defaultBind)
	}
	if cfg.InsecureCookies {
		t.Fatal("default cookie posture is insecure")
	}

	bind, cfg, err = parseFlags(append(base, "--cookie-secure=false", "--bind=:9099"))
	if err != nil {
		t.Fatal(err)
	}
	if bind != ":9099" {
		t.Fatalf("bind = %q, want :9099", bind)
	}
	if !cfg.InsecureCookies {
		t.Fatal("--cookie-secure=false did not opt out of the Secure posture")
	}
}
