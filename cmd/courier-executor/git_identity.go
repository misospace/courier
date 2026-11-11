package main

import (
	"github.com/misospace/courier/internal/executor"
	"io"
	"os"
	"strings"
)

// installGitIdentity supplies the default git identity for any GIT_* variable
// the process does not already carry. A deployment-provided value — a shell
// export or a key of the executor environment Secret — always wins; only
// unset or blank names fall back, so the standalone path overrides without an
// operator (or upstream) hard-coding a particular account (#317).
func installGitIdentity() func() {
	defaults := map[string]string{
		executor.EnvGitAuthorName:     executor.DefaultGitAuthorName,
		executor.EnvGitAuthorEmail:    executor.DefaultGitAuthorEmail,
		executor.EnvGitCommitterName:  executor.DefaultGitAuthorName,
		executor.EnvGitCommitterEmail: executor.DefaultGitAuthorEmail,
	}
	type previousValue struct {
		value string
		set   bool
	}
	previous := make(map[string]previousValue, len(defaults))
	for name, fallback := range defaults {
		value, set := os.LookupEnv(name)
		previous[name] = previousValue{value: value, set: set}
		if !set || strings.TrimSpace(value) == "" {
			_ = os.Setenv(name, fallback)
		}
	}
	return func() {
		for name, value := range previous {
			if !value.set {
				_ = os.Unsetenv(name)
				continue
			}
			_ = os.Setenv(name, value.value)
		}
	}
}

func installAskpass(executable string) func() {
	type environment struct {
		value string
		set   bool
	}
	previous := map[string]environment{}
	for _, name := range []string{"GIT_ASKPASS", "GIT_TERMINAL_PROMPT", "COURIER_ASKPASS"} {
		value, set := os.LookupEnv(name)
		previous[name] = environment{value: value, set: set}
	}
	_ = os.Setenv("GIT_ASKPASS", executable)
	_ = os.Setenv("GIT_TERMINAL_PROMPT", "0")
	_ = os.Setenv("COURIER_ASKPASS", "1")
	return func() {
		for name, value := range previous {
			if !value.set {
				_ = os.Unsetenv(name)
				continue
			}
			_ = os.Setenv(name, value.value)
		}
	}
}

func askpass(args []string, username, token string, stdout io.Writer) int {
	prompt := strings.ToLower(strings.Join(args, " "))
	value := token
	if strings.Contains(prompt, "username") {
		value = username
	}
	_, _ = io.WriteString(stdout, value)
	return 0
}
