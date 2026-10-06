// Package executor contains the small contract between the operator and a
// coordinator runtime.  The first runtime is a temporary OpenCode shim; the
// contract deliberately does not describe a resumable harness.
package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
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

// Outcome describes the process-level result an executor reports. A runtime
// must combine a zero exit with local workspace evidence before handing work
// to external verification; process success alone is not evidence that useful
// work was produced. The temporary shim uses exit code 2 for a deliberate
// needs-human result and treats all other failures as infrastructure/runtime
// failures.
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
	HeadRepo  string
	HeadSHA   string
	Goal      string
	Model     string
	Roles     map[string]string
	Framing   string
	Session   string
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

// DeclaredOutcome is the terminal declaration a coordinator makes about the
// work itself. The vocabulary matches the legacy outcome contract; the
// operator maps it to phases through the existing exit-code contract.
type DeclaredOutcome string

const (
	// OutcomeChanges: work was committed and pushed to the run branch.
	OutcomeChanges DeclaredOutcome = "changes"
	// OutcomeNoChangeNeeded: the work was already done, with evidence.
	OutcomeNoChangeNeeded DeclaredOutcome = "no_change_needed"
	// OutcomeNeedsDecision: a human decision the run cannot make is required.
	OutcomeNeedsDecision DeclaredOutcome = "needs_decision"
	// OutcomeBlockedExternal: something outside the run is missing.
	OutcomeBlockedExternal DeclaredOutcome = "blocked_external"
)

// HarnessResult is what a native harness reports when its execution ends.
// Outcome carries the coordinator's own declaration; Err carries an
// infrastructure failure (pod, gateway, status path) that the operator
// relaunches under the crashloop backstop — it is never a verdict on the work.
type HarnessResult struct {
	Outcome DeclaredOutcome
	Reason  string
	Err     error
}

// Harness is the native secure-mode runtime seam (#124): the trusted control
// pod's in-process coordinator. It consumes the same Invocation context a
// process executor receives and reports the same terminal vocabulary. Unlike
// Executor, a Harness owns the model client and delegation itself; it must
// never execute model-controlled commands locally — every model-requested
// command is dispatched to the untrusted worker over the signed protocol, and
// publication is reachable only from the harness's own trusted control path,
// never from a delegated brief.
type Harness interface {
	Name() string
	Run(context.Context, Invocation) HarnessResult
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

// forgeContract routes every forge read/write through Courier's configured forge
// capability rather than a forge-specific CLI, so the same goal works on any
// forge. (#90)
const forgeContract = "Route every forge read and write through the configured forge capability, not a forge-specific CLI."

// completionContract states that delegation covers bounded work, never the run's
// terminal contract: the coordinator integrates and verifies delegated work,
// pushes, and opens or updates the PR itself. A local commit or pushed branch
// with no required PR is not completion. (#90)
const completionContract = "Delegate implementation, research, and review to sub-agents, but you own completion: integrate and verify their work, push the branch, and open or update the pull request yourself — never stop at a local commit or branch when a pull request is required."

// publicationContract names the run branch as the only place work may be
// published, so a coordinator or delegate never opens or pushes work from a
// different branch than the run's. (#134)
const publicationContract = "Publish only to the run branch: commit on, push, and open or update the pull request from that single branch, and never create or publish work from any other branch."

// outcomeContract tells the coordinator to declare how the run ended in the
// supplied per-run scratch file, keeping the handoff outside the target repo.
const outcomeContract = "You own this run's completion end to end, and you exercise it by declaring an outcome: write JSON to %s — {\"outcome\":\"changes\"} once your work is committed and pushed to the run branch; {\"outcome\":\"no_change_needed\",\"evidence\":\"...\"} when the work is already done, citing the evidence; {\"outcome\":\"needs_decision\",\"question\":\"...\"} only for a decision you cannot make yourself; {\"outcome\":\"blocked_external\",\"missing\":\"...\"} when something outside this run is missing. A run that ends without a valid declaration fails as incomplete."

const outcomeFilename = "outcome.json"

// Goal returns the concise coordinator goal for a run. Lane framing is passed
// separately so it remains context rather than becoming a hard-coded prompt
// scaffold. The outcome declaration path matches the per-run scratch mount.
func Goal(run *courierv1alpha1.CoderRun) (string, error) {
	if run == nil {
		return "", ErrNilRun
	}
	if run.Spec.Ref < 1 {
		return "", ErrInvalidReference
	}
	outcomeFile := filepath.Join("/var/tmp/courier-scratch", outcomeFilename)
	branch := strings.TrimSpace(run.Status.Branch)
	publication := ""
	if branch != "" {
		publication = " " + publicationContract + " The run branch is " + branch + "."
	}
	switch run.Spec.Mode {
	case courierv1alpha1.ModeResolveIssue:
		return fmt.Sprintf("Open a PR to address issue #%d and drive it to a review-ready state with CI green. %s %s%s %s", run.Spec.Ref, forgeContract, completionContract, publication, fmt.Sprintf(outcomeContract, outcomeFile)), nil
	case courierv1alpha1.ModeFixPR:
		return fmt.Sprintf("Take over PR #%d. Inspect the current pull request state, CI/checks, and review feedback to determine what's blocking it, then return it to a review-ready state. %s %s%s %s", run.Spec.Ref, forgeContract, completionContract, publication, fmt.Sprintf(outcomeContract, outcomeFile)), nil
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
	roles := make(map[string]string, len(lane.Spec.Roles))
	for role, roleModel := range lane.Spec.Roles {
		roles[role] = roleModel
	}
	return Invocation{
		RunName:   run.Name,
		Namespace: run.Namespace,
		Mode:      run.Spec.Mode,
		Repo:      run.Spec.Repo,
		Ref:       run.Spec.Ref,
		Branch:    run.Status.Branch,
		HeadRepo:  run.Status.HeadRepo,
		HeadSHA:   run.Status.HeadSHA,
		Goal:      goal,
		Model:     model,
		Roles:     roles,
		Framing:   lane.Spec.Framing,
		Workspace: workspace,
		Debug:     run.Spec.Debug,
	}, nil
}

// RunContextEnvironment returns the run-context subset of the coordinator
// environment contract: the values both a process executor and the native
// control pod need to reconstruct an Invocation. Secrets, runtime-specific
// values, and remote URLs are wired separately by the Pod builder.
func RunContextEnvironment(inv Invocation) []EnvVar {
	level := "info"
	if inv.Debug {
		level = "debug"
	}
	rolesJSON, err := json.Marshal(inv.Roles)
	if err != nil {
		rolesJSON = []byte("{}")
	}
	return []EnvVar{
		{Name: "COURIER_RUN_NAME", Value: inv.RunName},
		{Name: "COURIER_RUN_NAMESPACE", Value: inv.Namespace},
		{Name: "COURIER_MODE", Value: string(inv.Mode)},
		{Name: "COURIER_REPO", Value: inv.Repo},
		{Name: "COURIER_HEAD_REPO", Value: inv.HeadRepo},
		{Name: "COURIER_HEAD_SHA", Value: inv.HeadSHA},
		{Name: "COURIER_REF", Value: strconv.Itoa(inv.Ref)},
		{Name: "COURIER_BRANCH", Value: inv.Branch},
		{Name: "COURIER_GOAL", Value: inv.Goal},
		{Name: "COURIER_MODEL", Value: inv.Model},
		{Name: "COURIER_ROLES_JSON", Value: string(rolesJSON)},
		{Name: "COURIER_FRAMING", Value: inv.Framing},
		{Name: "COURIER_WORKSPACE", Value: inv.Workspace},
		{Name: "COURIER_LOG_LEVEL", Value: level},
	}
}

// Environment returns the run context a coordinator container receives. The
// values are kept in environment variables rather than shell-expanded command
// strings so repository names, framing, and goals cannot become shell syntax.
func Environment(inv Invocation, executorName string) []EnvVar {
	return EnvironmentWithConfig(inv, executorName, "", "", "", "", "", "", "", "", "")
}

// EnvironmentWithConfig extends the run context with the deployment-specific
// git and bootstrap settings needed by the executable shim. Secrets are wired
// separately by the Pod builder as SecretKeyRef values. remoteURL points at
// the head repository for a fix-pr run; baseRemoteURL always points at the
// repository that owns the base branch, so a fork workspace can sync against
// upstream while pushing to the fork. evidenceURL and evidenceNonce are set
// only when failure-evidence capture is enabled, and each empty argument
// omits its own environment name.
func EnvironmentWithConfig(inv Invocation, executorName, remoteURL, baseRemoteURL, baseBranch, opencodeBinary, opencodeFormat, terminationFile, opencodeAgent, evidenceURL, evidenceNonce string) []EnvVar {
	values := []EnvVar{{Name: "COURIER_EXECUTOR", Value: executorName}}
	values = append(values, RunContextEnvironment(inv)...)
	values = append(values,
		EnvVar{Name: "COURIER_REPO_URL", Value: remoteURL},
		EnvVar{Name: "COURIER_BASE_REPO_URL", Value: baseRemoteURL},
		EnvVar{Name: "COURIER_BASE", Value: baseBranch},
		EnvVar{Name: "COURIER_OPENCODE_BINARY", Value: opencodeBinary},
		EnvVar{Name: "COURIER_OPENCODE_FORMAT", Value: opencodeFormat},
		EnvVar{Name: "COURIER_OPENCODE_AGENT", Value: opencodeAgent},
		EnvVar{Name: "COURIER_TERMINATION_FILE", Value: terminationFile},
		// The coordinator commits completed work in the cloned repository. A
		// fresh clone has no git identity, so provide a stable non-secret
		// identity without requiring mutable image configuration.
		EnvVar{Name: "GIT_AUTHOR_NAME", Value: "Courier"},
		EnvVar{Name: "GIT_AUTHOR_EMAIL", Value: "courier@localhost"},
		EnvVar{Name: "GIT_COMMITTER_NAME", Value: "Courier"},
		EnvVar{Name: "GIT_COMMITTER_EMAIL", Value: "courier@localhost"},
	)
	if strings.TrimSpace(evidenceURL) != "" {
		values = append(values, EnvVar{Name: EnvEvidenceURL, Value: evidenceURL})
	}
	if strings.TrimSpace(evidenceNonce) != "" {
		values = append(values, EnvVar{Name: EnvEvidenceNonce, Value: evidenceNonce})
	}
	return values
}

// EnvVar is kept local to the executor package so command construction tests
// do not need to know about Kubernetes container types.
type EnvVar struct {
	Name  string
	Value string
}

// InvocationFromEnv reconstructs an Invocation from the run-context
// environment contract written by RunContextEnvironment. It is the adapter
// seam the native harness control binary uses: the operator renders one
// Invocation per run, and the trusted control pod reconstructs the same
// values from its environment. get is the environment accessor, so tests can
// supply a map.
func InvocationFromEnv(get func(string) string) (Invocation, error) {
	inv := Invocation{
		RunName:   get("COURIER_RUN_NAME"),
		Namespace: get("COURIER_RUN_NAMESPACE"),
		Repo:      get("COURIER_REPO"),
		HeadRepo:  get("COURIER_HEAD_REPO"),
		HeadSHA:   get("COURIER_HEAD_SHA"),
		Branch:    get("COURIER_BRANCH"),
		Goal:      get("COURIER_GOAL"),
		Model:     get("COURIER_MODEL"),
		Framing:   get("COURIER_FRAMING"),
		Workspace: get("COURIER_WORKSPACE"),
		Debug:     get("COURIER_LOG_LEVEL") == "debug",
	}
	inv.Mode = courierv1alpha1.Mode(get("COURIER_MODE"))
	switch inv.Mode {
	case courierv1alpha1.ModeResolveIssue, courierv1alpha1.ModeFixPR:
	default:
		return Invocation{}, fmt.Errorf("%w: %q", ErrInvalidMode, inv.Mode)
	}
	ref, err := strconv.Atoi(get("COURIER_REF"))
	if err != nil || ref < 1 {
		return Invocation{}, ErrInvalidReference
	}
	inv.Ref = ref
	rolesJSON := get("COURIER_ROLES_JSON")
	if strings.TrimSpace(rolesJSON) != "" {
		if err := json.Unmarshal([]byte(rolesJSON), &inv.Roles); err != nil {
			return Invocation{}, fmt.Errorf("executor: decode COURIER_ROLES_JSON: %w", err)
		}
	}
	switch {
	case inv.RunName == "":
		return Invocation{}, ErrMissingRunName
	case inv.Namespace == "":
		return Invocation{}, ErrMissingNamespace
	case inv.Workspace == "":
		return Invocation{}, ErrMissingWorkspace
	case inv.Goal == "":
		return Invocation{}, errors.New("executor: goal is required")
	case inv.Model == "":
		return Invocation{}, ErrMissingModel
	}
	return inv, nil
}
