package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
