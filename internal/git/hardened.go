package git

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
)

// hardenedEnv is the hostile-repository git environment shared by the broker
// and trusted control integration (HARNESS.md §5): no system or global config
// trust, no terminal prompts, no optional locks, and external transport
// helpers disabled. Protocol ext transport is disabled through the config
// count so every hardened invocation refuses `ext::` URLs even if a hostile
// repository or bundle names one.
func hardenedEnv() []string {
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.DevNull,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=protocol.ext.allow",
		"GIT_CONFIG_VALUE_0=never",
		"GIT_OPTIONAL_LOCKS=0",
	}
}

// Hardened runs one git command with the hostile-repository environment and
// returns its stdout. Git's stderr is never propagated: it can echo hostile
// repository, bundle, or transport content, and callers classify failures
// with fixed categories. Arguments must be assembled entirely from trusted
// values; no request text or model-controlled string may reach this boundary.
func Hardened(ctx context.Context, directory string, args ...string) ([]byte, error) {
	stdout, _, err := hardened(ctx, directory, nil, args)
	return stdout, err
}

// HardenedWithEnv runs one hardened git command with additional environment
// variables (used for object-directory quarantine during bundle inspection).
func HardenedWithEnv(ctx context.Context, directory string, extra []string, args ...string) ([]byte, error) {
	stdout, _, err := hardened(ctx, directory, extra, args)
	return stdout, err
}

// HardenedExit runs one hardened git command and preserves the process exit
// code on failure so callers can distinguish git's conventional exit 1
// (ancestry negative, no diff) from real failures.
func HardenedExit(ctx context.Context, directory string, args ...string) error {
	_, _, err := hardened(ctx, directory, nil, args)
	return err
}

func hardened(ctx context.Context, directory string, extra []string, args []string) ([]byte, string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if directory != "" {
		cmd.Dir = directory
	}
	cmd.Env = append(hardenedEnv(), extra...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// Stderr is dropped: it routinely carries repository, bundle, or
		// transport content that must never enter trusted error strings.
		return nil, strings.TrimSpace(stderr.String()), &CommandError{Args: append([]string(nil), args...), Err: err}
	}
	return stdout.Bytes(), strings.TrimSpace(stderr.String()), nil
}
