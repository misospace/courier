package executor

import (
	"context"
	"fmt"
	"strings"
)

const (
	// OpenCodeExitSuccess is the normal headless OpenCode completion status.
	OpenCodeExitSuccess = 0
	// OpenCodeExitNeedsHuman is the shim's explicit opt-out status. A future
	// harness may replace this with a structured self-declaration.
	OpenCodeExitNeedsHuman = 2
)

// OpenCode is the temporary headless executor. It intentionally only wraps
// one non-interactive invocation; checkpointing and resume belong to the
// custom harness that will replace it.
type OpenCode struct {
	Binary          string
	Format          string
	BootstrapBinary string
}

// DefaultOpenCode returns the default executable configuration.
func DefaultOpenCode() OpenCode {
	return OpenCode{Binary: "opencode", Format: "json", BootstrapBinary: "/usr/local/bin/courier-executor"}
}

func (o OpenCode) Name() string { return "opencode" }

// Command returns an argument-vector command suitable for exec.Command. The
// goal and lane framing are one prompt, while model selection is explicit.
func (o OpenCode) Command(inv Invocation) Command {
	binary := strings.TrimSpace(o.Binary)
	if binary == "" {
		binary = DefaultOpenCode().Binary
	}
	args := []string{"run", "--model", inv.Model}
	if format := strings.TrimSpace(o.Format); format != "" {
		args = append(args, "--format", format)
	}
	args = append(args, prompt(inv))
	return Command{Binary: binary, Args: args}
}

// BootstrapCommand returns the command used in a coordinator Pod. The
// bootstrap is responsible for preparing git and then invoking Command in the
// workspace; keeping that setup out of a Pod shell avoids shell interpolation.
func (o OpenCode) BootstrapCommand(inv Invocation, binary string) Command {
	if strings.TrimSpace(binary) == "" {
		binary = strings.TrimSpace(o.BootstrapBinary)
	}
	if strings.TrimSpace(binary) == "" {
		binary = DefaultOpenCode().BootstrapBinary
	}
	return Command{Binary: binary}
}

// Result maps every process termination to an explicit terminal phase. A
// context cancellation is an infrastructure failure, not a needs-human
// declaration; the operator can decide whether to relaunch it.
func (o OpenCode) Result(exitCode int, err error) Outcome {
	if err != nil {
		return Outcome{Phase: TerminalFailed, Reason: "opencode process failed", Err: err}
	}
	switch exitCode {
	case OpenCodeExitSuccess:
		return Outcome{Phase: TerminalVerifying, Reason: "opencode completed"}
	case OpenCodeExitNeedsHuman:
		return Outcome{Phase: TerminalNeedsHuman, Reason: "opencode requested human attention"}
	default:
		return Outcome{Phase: TerminalFailed, Reason: fmt.Sprintf("opencode exited with status %d", exitCode)}
	}
}

// RunResult is a small process-runner seam used by callers that want to keep
// context cancellation semantics explicit without coupling the executor to an
// exec.Cmd implementation.
func (o OpenCode) RunResult(ctx context.Context, exitCode int, err error) Outcome {
	if ctx.Err() != nil {
		return Outcome{Phase: TerminalFailed, Reason: "opencode context ended", Err: ctx.Err()}
	}
	return o.Result(exitCode, err)
}

func prompt(inv Invocation) string {
	goal := strings.TrimSpace(inv.Goal)
	framing := strings.TrimSpace(inv.Framing)
	if framing == "" {
		return goal
	}
	return goal + "\n\nLane framing:\n" + framing
}
