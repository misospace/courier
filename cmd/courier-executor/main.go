// Command courier-executor is the executable bootstrap for the temporary
// OpenCode runtime. It prepares the ephemeral git workspace, then delegates
// the actual model run to headless OpenCode.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/github"
)

const (
	exitSuccess    = 0
	exitNeedsHuman = 2
	defaultBase    = "main"
	defaultWork    = "/workspace"
	defaultFormat  = "json"
)

type config struct {
	RemoteURL       string
	Directory       string
	Base            string
	Branch          string
	Repo            string
	Mode            string
	Goal            string
	Model           string
	Framing         string
	GitHubAPIBase   string
	OpenCodeBinary  string
	OpenCodeFormat  string
	TerminationFile string
	GitUsername     string
	GitToken        string
	GitHubToken     string
}

type termination struct {
	Phase    string `json:"phase"`
	Result   string `json:"result"`
	ExitCode int    `json:"exit_code"`
	Reason   string `json:"reason"`
}

func main() {
	if os.Getenv("COURIER_ASKPASS") == "1" {
		os.Exit(askpass(os.Args[1:], os.Getenv("COURIER_GIT_USERNAME"), os.Getenv("COURIER_GIT_TOKEN"), os.Stdout))
	}
	os.Exit(run(context.Background(), os.Stdout, os.Stderr))
}

func readConfig(getenv func(string) string) (config, error) {
	remoteURL := strings.TrimSpace(getenv("COURIER_REPO_URL"))
	if remoteURL == "" {
		remoteURL = strings.TrimSpace(getenv("COURIER_REMOTE_URL"))
	}
	cfg := config{
		RemoteURL:       remoteURL,
		Directory:       strings.TrimSpace(getenv("COURIER_WORKSPACE")),
		Base:            strings.TrimSpace(getenv("COURIER_BASE")),
		Branch:          strings.TrimSpace(getenv("COURIER_BRANCH")),
		Repo:            strings.TrimSpace(getenv("COURIER_REPO")),
		Goal:            strings.TrimSpace(getenv("COURIER_GOAL")),
		Model:           strings.TrimSpace(getenv("COURIER_MODEL")),
		Framing:         getenv("COURIER_FRAMING"),
		Mode:            strings.TrimSpace(getenv("COURIER_MODE")),
		OpenCodeBinary:  strings.TrimSpace(getenv("COURIER_OPENCODE_BINARY")),
		OpenCodeFormat:  strings.TrimSpace(getenv("COURIER_OPENCODE_FORMAT")),
		TerminationFile: strings.TrimSpace(getenv("COURIER_TERMINATION_FILE")),
		GitUsername:     getenv("COURIER_GIT_USERNAME"),
		GitToken:        getenv("COURIER_GIT_TOKEN"),
		GitHubToken:     getenv("GITHUB_TOKEN"),
		GitHubAPIBase:   strings.TrimSpace(getenv("COURIER_GITHUB_API_BASE")),
	}
	if strings.TrimSpace(cfg.GitHubToken) == "" {
		cfg.GitHubToken = cfg.GitToken
	}
	if cfg.Directory == "" {
		cfg.Directory = defaultWork
	}
	if cfg.Base == "" {
		cfg.Base = defaultBase
	}
	if cfg.OpenCodeBinary == "" {
		cfg.OpenCodeBinary = "opencode"
	}
	if cfg.OpenCodeFormat == "" {
		cfg.OpenCodeFormat = defaultFormat
	}
	if cfg.GitHubAPIBase == "" {
		cfg.GitHubAPIBase = "https://api.github.com/"
	}
	for _, required := range []struct {
		name  string
		value string
	}{
		{name: "COURIER_REPO_URL", value: cfg.RemoteURL},
		{name: "COURIER_BRANCH", value: cfg.Branch},
		{name: "COURIER_GOAL", value: cfg.Goal},
		{name: "COURIER_MODEL", value: cfg.Model},
	} {
		if required.value == "" {
			return config{}, fmt.Errorf("%s is required", required.name)
		}
	}
	if parsed, err := url.Parse(cfg.RemoteURL); err == nil && parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return config{}, errors.New("COURIER_REPO_URL must not contain credentials")
		}
	}
	return cfg, nil
}

func run(ctx context.Context, stdout, stderr io.Writer) int {
	cfg, err := readConfig(os.Getenv)
	if err != nil {
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: err.Error()})
		return 1
	}

	askpassPath, err := os.Executable()
	if err != nil {
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "resolve executable: " + err.Error()})
		return 1
	}
	restoreIdentity := installGitIdentity()
	defer restoreIdentity()
	restoreEnv := installAskpass(askpassPath)
	defer restoreEnv()

	if code := guardAdoption(ctx, cfg, stdout); code != 0 {
		return code
	}

	workspace, err := git.Prepare(ctx, git.PrepareOptions{
		RemoteURL: cfg.RemoteURL,
		Directory: cfg.Directory,
		Base:      cfg.Base,
		Branch:    cfg.Branch,
	})
	if err != nil {
		reason := redactCredentials(err.Error(), cfg)
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: reason})
		return 1
	}

	runtime := executor.OpenCode{Binary: cfg.OpenCodeBinary, Format: cfg.OpenCodeFormat}
	command := runtime.Command(executor.Invocation{
		Goal:      cfg.Goal,
		Model:     cfg.Model,
		Framing:   cfg.Framing,
		Workspace: workspace.Directory,
	})
	process := exec.CommandContext(ctx, command.Binary, command.Args...)
	process.Dir = workspace.Directory
	process.Stdout = stdout
	process.Stderr = stderr
	if err := process.Run(); err != nil {
		code := processExitCode(err)
		if code < 0 {
			code = 1
		}
		if code == exitNeedsHuman {
			emitTermination(stdout, cfg, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: code, Reason: "opencode requested human attention"})
			return code
		}
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: code, Reason: redactCredentials(fmt.Sprintf("opencode exited with status %d", code), cfg)})
		return code
	}

	emitTermination(stdout, cfg, termination{Phase: "Verifying", Result: "success", ExitCode: exitSuccess, Reason: "opencode completed"})
	return exitSuccess
}

// guardAdoption enforces the resolve-issue invariant that a deterministic
// branch may be adopted only when it is orphaned. If the branch already
// exists on the remote, no pull request may exist for that head; if one
// does, a human decides, and the run stops before Prepare or OpenCode.
// It returns 0 when the run may proceed.
func guardAdoption(ctx context.Context, cfg config, stdout io.Writer) int {
	if cfg.Mode != "resolve-issue" {
		return 0
	}
	exists, err := git.RemoteBranchExists(ctx, cfg.RemoteURL, cfg.Branch)
	if err != nil {
		reason := redactCredentials(err.Error(), cfg)
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: reason})
		return 1
	}
	if !exists {
		return 0
	}
	owner, name, ok := splitOwnerRepo(cfg.Repo)
	if !ok {
		reason := "resolve branch already exists on the remote and COURIER_REPO does not name an owner/repo; refusing adoption"
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: reason})
		return 1
	}
	client, err := github.NewClient(cfg.GitHubAPIBase, cfg.GitHubToken)
	if err != nil {
		reason := redactCredentials(err.Error(), cfg)
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: reason})
		return 1
	}
	pulls, err := client.PullRequestsForHead(ctx, owner, name, cfg.Branch)
	if err != nil {
		reason := redactCredentials(err.Error(), cfg)
		emitTermination(stdout, cfg, termination{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: reason})
		return 1
	}
	for _, pull := range pulls {
		reason := fmt.Sprintf("resolve branch already has PR #%d (%s); refusing adoption", pull.Number, pull.State)
		emitTermination(stdout, cfg, termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: exitNeedsHuman, Reason: reason})
		return exitNeedsHuman
	}
	return 0
}

// splitOwnerRepo splits an owner/name repository identity on the final "/".
func splitOwnerRepo(value string) (owner, name string, ok bool) {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "/")
	if idx <= 0 || idx == len(value)-1 {
		return "", "", false
	}
	return value[:idx], value[idx+1:], true
}

func installGitIdentity() func() {
	defaults := map[string]string{
		"GIT_AUTHOR_NAME":     "Courier",
		"GIT_AUTHOR_EMAIL":    "courier@localhost",
		"GIT_COMMITTER_NAME":  "Courier",
		"GIT_COMMITTER_EMAIL": "courier@localhost",
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

func processExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
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

func emitTermination(stdout io.Writer, cfg config, result termination) {
	payload, err := json.Marshal(result)
	if err != nil {
		return
	}
	line := "COURIER_TERMINATION " + string(payload) + "\n"
	_, _ = io.WriteString(stdout, line)
	if cfg.TerminationFile == "" {
		return
	}
	// The file is an optional local handoff for an operator or sidecar. It is
	// never used for credentials and is replaced atomically enough for a single
	// writer in the ephemeral workspace.
	_ = os.WriteFile(cfg.TerminationFile, []byte(line), 0o600)
}

func redactCredentials(value string, cfg config) string {
	value = redact(value, cfg.GitToken)
	return redact(value, cfg.GitHubToken)
}

func redact(value, secret string) string {
	if secret == "" {
		return value
	}
	return strings.ReplaceAll(value, secret, "[REDACTED]")
}
