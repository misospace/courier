// Package executor contains the small contract between the operator and a
// coordinator runtime.  The first runtime is a temporary OpenCode shim; the
// contract deliberately does not describe a resumable harness.
package executor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// TerminalPhase is the phase an executor reports when its process exits.
// Keeping this type separate from Kubernetes API objects makes the executor
// usable by a process runner and keeps the terminal mapping explicit.
type TerminalPhase string

const (
	TerminalVerifying  TerminalPhase = TerminalPhase(courierv1alpha1.PhaseVerifying)
	TerminalNeedsHuman TerminalPhase = TerminalPhase(courierv1alpha1.PhaseNeedsHuman)
	TerminalFailed     TerminalPhase = TerminalPhase(courierv1alpha1.PhaseFailed)
)

// Outcome is the only result an executor returns after its process exits.
// An exit code of zero is a successful handoff to external verification; the
// temporary shim uses exit code 2 for a deliberate needs-human result and
// treats all other failures as infrastructure/runtime failures.
type Outcome struct {
	Phase  TerminalPhase
	Reason string
	Err    error
}

// Invocation is the run-specific context passed to an executor.
type Invocation struct {
	RunName   string
	Namespace string
	Mode      courierv1alpha1.Mode
	Repo      string
	Ref       int
	Branch    string
	Goal      string
	Model     string
	Framing   string
	Workspace string
	Debug     bool
}

// Command is a process command with no shell interpolation.
type Command struct {
	Binary string
	Args   []string
}

// Executor is the minimal runtime contract used by the coordinator Pod
// builder. Implementations are responsible for constructing a command and
// mapping its process exit to a terminal outcome.
type Executor interface {
	Name() string
	Command(Invocation) Command
	Result(exitCode int, err error) Outcome
}

// Bootstrapper is implemented by runtimes that need a small executable
// bootstrap before their actual model process. It is optional so other
// forge/model-agnostic executors can continue to provide their own command.
type Bootstrapper interface {
	BootstrapCommand(Invocation, string) Command
}

var (
	ErrNilRun             = errors.New("executor: run is required")
	ErrNilLane            = errors.New("executor: lane profile is required")
	ErrInvalidMode        = errors.New("executor: unsupported run mode")
	ErrInvalidReference   = errors.New("executor: reference must be positive")
	ErrMissingModel       = errors.New("executor: coordinator model is required")
	ErrMissingRunName     = errors.New("executor: run name is required")
	ErrMissingNamespace   = errors.New("executor: run namespace is required")
	ErrMissingWorkspace   = errors.New("executor: workspace path is required")
	ErrMissingCommandName = errors.New("executor: command name is required")
)

// Goal returns the concise coordinator goal for a run. Lane framing is passed
// separately so it remains context rather than becoming a hard-coded prompt
// scaffold.
func Goal(run *courierv1alpha1.CoderRun) (string, error) {
	if run == nil {
		return "", ErrNilRun
	}
	if run.Spec.Ref < 1 {
		return "", ErrInvalidReference
	}
	switch run.Spec.Mode {
	case courierv1alpha1.ModeResolveIssue:
		return fmt.Sprintf("Open a PR to address issue #%d. Make sure CI is green and it's ready for review, and delegate as much as possible to keep your context clean.", run.Spec.Ref), nil
	case courierv1alpha1.ModeFixPR:
		return fmt.Sprintf("Take over PR #%d. Check why it's blocked (changes requested, conflicts, etc.) and get it back to a healthy state based on the feedback.", run.Spec.Ref), nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidMode, run.Spec.Mode)
	}
}

// NewInvocation assembles the run and lane data passed to an executor.
func NewInvocation(run *courierv1alpha1.CoderRun, lane *courierv1alpha1.LaneProfile, workspace string) (Invocation, error) {
	if run == nil {
		return Invocation{}, ErrNilRun
	}
	if lane == nil {
		return Invocation{}, ErrNilLane
	}
	if strings.TrimSpace(run.Name) == "" {
		return Invocation{}, ErrMissingRunName
	}
	if strings.TrimSpace(run.Namespace) == "" {
		return Invocation{}, ErrMissingNamespace
	}
	if strings.TrimSpace(workspace) == "" {
		return Invocation{}, ErrMissingWorkspace
	}
	goal, err := Goal(run)
	if err != nil {
		return Invocation{}, err
	}
	model := strings.TrimSpace(lane.Spec.Roles["coordinator"])
	if model == "" {
		return Invocation{}, ErrMissingModel
	}
	return Invocation{
		RunName:   run.Name,
		Namespace: run.Namespace,
		Mode:      run.Spec.Mode,
		Repo:      run.Spec.Repo,
		Ref:       run.Spec.Ref,
		Branch:    run.Status.Branch,
		Goal:      goal,
		Model:     model,
		Framing:   lane.Spec.Framing,
		Workspace: workspace,
		Debug:     run.Spec.Debug,
	}, nil
}

// Environment returns the run context a coordinator container receives. The
// values are kept in environment variables rather than shell-expanded command
// strings so repository names, framing, and goals cannot become shell syntax.
func Environment(inv Invocation, executorName string) []EnvVar {
	return EnvironmentWithConfig(inv, executorName, "", "", "", "", "")
}

// EnvironmentWithConfig extends the run context with the deployment-specific
// git and bootstrap settings needed by the executable shim. Secrets are wired
// separately by the Pod builder as SecretKeyRef values.
func EnvironmentWithConfig(inv Invocation, executorName, remoteURL, baseBranch, opencodeBinary, opencodeFormat, terminationFile string) []EnvVar {
	level := "info"
	if inv.Debug {
		level = "debug"
	}
	values := []EnvVar{
		{Name: "COURIER_EXECUTOR", Value: executorName},
		{Name: "COURIER_RUN_NAME", Value: inv.RunName},
		{Name: "COURIER_RUN_NAMESPACE", Value: inv.Namespace},
		{Name: "COURIER_MODE", Value: string(inv.Mode)},
		{Name: "COURIER_REPO", Value: inv.Repo},
		{Name: "COURIER_REF", Value: strconv.Itoa(inv.Ref)},
		{Name: "COURIER_BRANCH", Value: inv.Branch},
		{Name: "COURIER_GOAL", Value: inv.Goal},
		{Name: "COURIER_MODEL", Value: inv.Model},
		{Name: "COURIER_FRAMING", Value: inv.Framing},
		{Name: "COURIER_WORKSPACE", Value: inv.Workspace},
		{Name: "COURIER_LOG_LEVEL", Value: level},
		{Name: "COURIER_REPO_URL", Value: remoteURL},
		{Name: "COURIER_BASE", Value: baseBranch},
		{Name: "COURIER_OPENCODE_BINARY", Value: opencodeBinary},
		{Name: "COURIER_OPENCODE_FORMAT", Value: opencodeFormat},
		{Name: "COURIER_TERMINATION_FILE", Value: terminationFile},
		// The coordinator commits completed work in the cloned repository. A
		// fresh clone has no git identity, so provide a stable non-secret
		// identity without requiring mutable image configuration.
		{Name: "GIT_AUTHOR_NAME", Value: "Courier"},
		{Name: "GIT_AUTHOR_EMAIL", Value: "courier@localhost"},
		{Name: "GIT_COMMITTER_NAME", Value: "Courier"},
		{Name: "GIT_COMMITTER_EMAIL", Value: "courier@localhost"},
	}
	return values
}

// EnvVar is kept local to the executor package so command construction tests
// do not need to know about Kubernetes container types.
type EnvVar struct {
	Name  string
	Value string
}
