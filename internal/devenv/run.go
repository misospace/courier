package devenv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"time"
)

// DefaultBound is the liveness bound on each bootstrap command (#414): a
// stuck bootstrap produces a structured failure, never an unbounded wait.
// It bounds stuckness, never a run's duration.
const DefaultBound = 15 * time.Minute

// maxCaptureBytes bounds the in-memory `mise x -- env -0` capture. The
// environment is bounded data; anything larger is not an environment and
// fails the run instead of being truncated.
const maxCaptureBytes = 1 << 20

// terminationGrace bounds the wait after a bootstrap command's own process is
// gone while a descendant still holds its output descriptors. It bounds
// cleanup, never a run's duration.
const terminationGrace = 10 * time.Second

// Options configures one Run.
type Options struct {
	// Dir is the working directory — the workspace the contract was checked
	// out into.
	Dir string
	// Bound bounds each bootstrap command; 0 means DefaultBound. It bounds
	// stuckness (liveness), never a run's duration.
	Bound time.Duration
	// Env is the base environment for child processes — the executor's own
	// environment. The captured delta is computed against it.
	Env []string
	// Stdout and Stderr are streamed to unbuffered as they arrive; callers
	// pass redacting writers. Nil discards.
	Stdout io.Writer
	Stderr io.Writer
}

// Result is the structured bootstrap receipt (#414). EnvDelta carries
// KEY=VALUE pairs to apply over the base environment on "applied"; EnvKeys
// lists their sorted names only, never values — no secrets in receipts.
type Result struct {
	// Status is "applied", "skipped", or "failed". Run itself never returns
	// "skipped": callers skip on Compare's verdict before executing.
	Status string
	// Reason is empty when applied and a bounded human-readable explanation
	// otherwise.
	Reason string
	// Elapsed is the total wall time across both commands.
	Elapsed time.Duration
	// ExitCode is the failing child's exit code; -1 when it never exited.
	ExitCode int
	// EnvDelta is set only on "applied".
	EnvDelta []string
	// EnvKeys is the sorted names of EnvDelta, set only on "applied".
	EnvKeys []string
}

// Run executes the approved bootstrap contract in o.Dir: `mise run bootstrap`,
// then `mise x -- env -0` to capture the deterministic activated environment.
// miseBinary is the mise executable (absolute, or looked up on PATH by the
// caller — Run does not LookPath; an empty miseBinary is a structured
// failure). Commands run as direct argv, never through a shell. Each command
// leads its own process group and the whole group is killed on context
// cancellation or bound expiry (setpgid + Cancel kill(-pid, SIGKILL) and a
// WaitDelay cleanup, the pattern of internal/protocol/worker.go runTask).
//
// Deterministic failures — mise missing, nonzero exit, timeout, cancellation,
// malformed or oversized environment capture — return a Result with Status
// "failed" and a nil error, so callers treat them like any other contract
// outcome. Context cancellation still yields a failed Result (its Reason
// mentions the cancellation), not a Go error. Only truly unexpected internal
// errors return a non-nil error.
func Run(ctx context.Context, miseBinary string, o Options) (Result, error) {
	started := time.Now()
	if miseBinary == "" {
		return Result{
			Status:   "failed",
			Reason:   "bootstrap contract requires mise; no mise executable provided",
			ExitCode: -1,
		}, nil
	}
	bound := o.Bound
	if bound <= 0 {
		bound = DefaultBound
	}

	result, err := runBootstrap(ctx, miseBinary, o, bound)
	if err != nil || result.Status != "applied" {
		result.Elapsed = time.Since(started)
		return result, err
	}

	result2, err := runEnvCapture(ctx, miseBinary, o, bound, result)
	result2.Elapsed = time.Since(started)
	return result2, err
}

// runBootstrap executes `mise run bootstrap`, streaming its output to the
// caller's writers as it arrives and classifying the wait into the receipt.
func runBootstrap(ctx context.Context, miseBinary string, o Options, bound time.Duration) (Result, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	if err := cmdCtx.Err(); err != nil {
		return boundedFailure(err, bound, "bootstrap task"), nil
	}
	cmd := newCommand(cmdCtx, miseBinary, []string{"run", "bootstrap"}, o)
	cmd.Stdout = o.Stdout
	cmd.Stderr = o.Stderr
	if err := cmd.Start(); err != nil {
		return Result{
			Status:   "failed",
			Reason:   fmt.Sprintf("bootstrap task failed to start: %v", err),
			ExitCode: -1,
		}, nil
	}
	waitErr := cmd.Wait()
	if err := cmdCtx.Err(); err != nil {
		result := boundedFailure(err, bound, "bootstrap task")
		result.ExitCode = exitCodeOf(cmd)
		return result, nil
	}
	if waitErr == nil {
		return Result{Status: "applied", ExitCode: 0}, nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return Result{
			Status:   "failed",
			Reason:   fmt.Sprintf("bootstrap task failed with exit %d", exitErr.ExitCode()),
			ExitCode: exitErr.ExitCode(),
		}, nil
	}
	return Result{
		Status:   "failed",
		Reason:   fmt.Sprintf("bootstrap task failed: %v", waitErr),
		ExitCode: exitCodeOf(cmd),
	}, nil
}

// runEnvCapture executes `mise x -- env -0`, captures stdout in memory under
// the byte bound, and turns the records into the filtered delta over the base
// environment.
func runEnvCapture(ctx context.Context, miseBinary string, o Options, bound time.Duration, applied Result) (Result, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	if err := cmdCtx.Err(); err != nil {
		return boundedFailure(err, bound, "environment capture"), nil
	}
	capture := &boundedBuffer{max: maxCaptureBytes}
	cmd := newCommand(cmdCtx, miseBinary, []string{"x", "--", "env", "-0"}, o)
	cmd.Stdout = capture
	cmd.Stderr = o.Stderr
	if err := cmd.Start(); err != nil {
		return Result{
			Status:   "failed",
			Reason:   fmt.Sprintf("environment capture failed to start: %v", err),
			ExitCode: -1,
		}, nil
	}
	waitErr := cmd.Wait()
	if err := cmdCtx.Err(); err != nil {
		result := boundedFailure(err, bound, "environment capture")
		result.ExitCode = exitCodeOf(cmd)
		return result, nil
	}
	if capture.overflow {
		return Result{
			Status:   "failed",
			Reason:   "environment capture exceeded 1 MiB",
			ExitCode: exitCodeOf(cmd),
		}, nil
	}
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			return Result{
				Status:   "failed",
				Reason:   fmt.Sprintf("environment capture failed with exit %d", exitErr.ExitCode()),
				ExitCode: exitErr.ExitCode(),
			}, nil
		}
		return Result{
			Status:   "failed",
			Reason:   fmt.Sprintf("environment capture failed: %v", waitErr),
			ExitCode: exitCodeOf(cmd),
		}, nil
	}

	records, err := splitNUL(capture.buf.String())
	if err != nil {
		return Result{
			Status:   "failed",
			Reason:   err.Error(),
			ExitCode: exitCodeOf(cmd),
		}, nil
	}
	delta, err := envDelta(records, baseEnvMap(o.Env))
	if err != nil {
		return Result{
			Status:   "failed",
			Reason:   "malformed environment capture",
			ExitCode: exitCodeOf(cmd),
		}, nil
	}
	keys := make([]string, 0, len(delta))
	for _, kv := range delta {
		name, _, _ := strings.Cut(kv, "=")
		keys = append(keys, name)
	}
	slices.Sort(keys)
	applied.EnvDelta = delta
	applied.EnvKeys = keys
	return applied, nil
}

// newCommand builds one bootstrap command in its own process group, so the
// group signal below reaches the task's descendants and not just the direct
// child — mise's real work runs in them.
func newCommand(ctx context.Context, miseBinary string, argv []string, o Options) *exec.Cmd {
	cmd := exec.CommandContext(ctx, miseBinary, argv...)
	cmd.Dir = o.Dir
	cmd.Env = o.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = terminationGrace
	return cmd
}

// boundedFailure turns a bound expiry or a cancellation into the receipt's
// deterministic failure, naming which happened.
func boundedFailure(err error, bound time.Duration, what string) Result {
	if errors.Is(err, context.DeadlineExceeded) {
		return Result{
			Status:   "failed",
			Reason:   fmt.Sprintf("%s exceeded its %s bound", what, bound),
			ExitCode: -1,
		}
	}
	return Result{
		Status:   "failed",
		Reason:   fmt.Sprintf("%s was cancelled", what),
		ExitCode: -1,
	}
}

// exitCodeOf reports the child's exit code when it reached an exit, and -1
// when it never did — a killed process has no exit code to report.
func exitCodeOf(cmd *exec.Cmd) int {
	if cmd.ProcessState == nil {
		return -1
	}
	return cmd.ProcessState.ExitCode()
}

// boundedBuffer drains its input without blocking the child, retaining at
// most max bytes and flagging the overflow instead of truncating silently.
type boundedBuffer struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.buf.Len(); room < len(p) {
		if room > 0 {
			b.buf.Write(p[:room])
		}
		b.overflow = true
		return len(p), nil
	}
	b.buf.Write(p)
	return len(p), nil
}

// baseEnvMap parses a base environment into name→value. Entries without '='
// are not environments and are ignored: the delta comparison only ever needs
// well-formed pairs.
func baseEnvMap(env []string) map[string]string {
	base := make(map[string]string, len(env))
	for _, kv := range env {
		if name, value, ok := strings.Cut(kv, "="); ok {
			base[name] = value
		}
	}
	return base
}

// splitNUL validates and splits a NUL-separated environment capture. An
// `env -0` capture always terminates every record with a NUL byte and always
// carries at least one record, so an empty capture or one whose final record
// lacks its terminator is a truncated capture: it fails closed rather than
// silently yielding a corrupt partial environment.
func splitNUL(capture string) ([]string, error) {
	if capture == "" {
		return nil, errors.New("empty environment capture")
	}
	if !strings.HasSuffix(capture, "\x00") {
		return nil, errors.New("truncated environment capture")
	}
	return strings.Split(capture, "\x00"), nil
}

// envDelta filters NUL-separated KEY=VALUE records into the delta over base:
// records that equal the base value are not a change, protected keys are
// dropped before they can reach a delta, and any malformed record fails the
// whole capture rather than being partially applied.
func envDelta(records []string, base map[string]string) ([]string, error) {
	delta := make([]string, 0, len(records))
	for _, record := range records {
		if record == "" {
			continue
		}
		name, value, ok := strings.Cut(record, "=")
		if !ok || name == "" || hasControl(name) {
			return nil, errors.New("malformed environment capture")
		}
		if ProtectedKeys(name) {
			continue
		}
		if baseValue, inBase := base[name]; inBase && baseValue == value {
			continue
		}
		delta = append(delta, record)
	}
	return delta, nil
}

// hasControl reports whether a name carries control characters. NUL and '='
// cannot survive the record split by construction; anything else that is not
// a printable name byte is rejected so an environment name stays a name.
func hasControl(name string) bool {
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
