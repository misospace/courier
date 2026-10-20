package git

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
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
//
// This package does not sanitize. A caller holding text that originated
// outside the trust boundary — the courier-executor probing whether a path
// a pull-request body cited exists at the published head (#367), which
// extracts a restricted charset from the body and roots it against the
// commit's top-level entries before building the argv here — owns that
// sanitization, and this comment records that the exception is the
// caller's to justify, not a license to pass arbitrary text.
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	if directory != "" {
		cmd.Dir = directory
	}
	cmd.Env = append(hardenedEnv(), extra...)
	var stdout, stderr boundedBuffer
	// Output past the cap is dropped, never buffered, and the command is
	// stopped: an untrusted repository must not be able to make the
	// operator hold its output without limit.
	stdout.limit = maxStdoutBytes
	stdout.onOverflow = cancel
	stderr.limit = maxStderrBytes
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stdout.overflow {
			return nil, "", errOutputTooLarge
		}
		// Stderr is dropped: it routinely carries repository, bundle, or
		// transport content that must never enter trusted error strings.
		return nil, strings.TrimSpace(stderr.String()), &CommandError{Args: append([]string(nil), args...), Err: err}
	}
	if stdout.overflow {
		return nil, "", errOutputTooLarge
	}
	return stdout.Bytes(), strings.TrimSpace(stderr.String()), nil
}

// hardenedLines runs one hardened git command with stdout piped and hands
// each output line to scan. When scan reports stop, the git process is
// killed at once and scan's error is returned: the remaining output is
// neither read nor held.
func hardenedLines(ctx context.Context, directory string, extra []string, args []string, scan func(line string) (stop bool, err error)) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	if directory != "" {
		cmd.Dir = directory
	}
	cmd.Env = append(hardenedEnv(), extra...)
	var stderr boundedBuffer
	stderr.limit = maxStderrBytes
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("git: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return &CommandError{Args: append([]string(nil), args...), Err: err}
	}
	scanner := bufio.NewScanner(stdout)
	// A hostile line must not be able to exhaust memory on its own.
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanLineBytes)
	var scanErr error
	for scanner.Scan() {
		stop, err := scan(scanner.Text())
		if err != nil {
			scanErr = err
			break
		}
		if stop {
			scanErr = errors.New("git: output scan stopped")
			break
		}
	}
	if scanErr == nil {
		// The scan did not reach the end of the output, so it failed on the
		// output itself: an over-long line, for instance.
		if err := scanner.Err(); err != nil {
			scanErr = fmt.Errorf("git: output scan failed: %w", err)
		}
	}
	if scanErr != nil {
		// Stop git at once: the rest of the output is not needed and must
		// not be read. The child is stopped before the wait even when the
		// failure came from the scan, or it would block on a pipe nobody
		// drains.
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}
	waitErr := cmd.Wait()
	if scanErr != nil {
		return scanErr
	}
	if waitErr != nil {
		return &CommandError{Args: append([]string(nil), args...), Err: waitErr}
	}
	return nil
}

// The capture bounds of the hardened command boundary. They bound what one
// git command can make the operator hold.
const (
	// maxStdoutBytes caps the stdout a hardened command may produce. Past
	// the cap the command fails instead of being buffered without limit.
	maxStdoutBytes = 32 << 20
	// maxStderrBytes caps the stderr that is retained. Git's stderr is
	// never propagated to callers, so the excess is discarded.
	maxStderrBytes = 1 << 20
	// maxScanLineBytes caps one line of streamed command output.
	maxScanLineBytes = 1 << 20
)

// errOutputTooLarge reports that a command produced more output than the
// capture bound allows. It carries no untrusted content.
var errOutputTooLarge = errors.New("git: output exceeded the capture limit")

// boundedBuffer holds at most limit bytes and discards the rest. It never
// grows past the limit, so a hostile command cannot exhaust memory through
// its output.
type boundedBuffer struct {
	buffer     bytes.Buffer
	limit      int
	overflow   bool
	onOverflow func()
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.overflow {
		return len(p), nil
	}
	if b.buffer.Len()+len(p) > b.limit {
		if room := b.limit - b.buffer.Len(); room > 0 {
			b.buffer.Write(p[:room])
		}
		b.overflow = true
		if b.onOverflow != nil {
			b.onOverflow()
		}
		return len(p), nil
	}
	return b.buffer.Write(p)
}

// Bytes returns the retained bytes.
func (b *boundedBuffer) Bytes() []byte { return b.buffer.Bytes() }

// String returns the retained bytes as a string.
func (b *boundedBuffer) String() string { return b.buffer.String() }
