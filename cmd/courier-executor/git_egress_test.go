package main

import (
	"testing"

	"github.com/misospace/courier/internal/executor"
)

func gitEgressEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func validGitEgressEnv() map[string]string {
	return map[string]string{
		executor.EnvGitEgressAddress:     "127.0.0.1:43184",
		executor.EnvInstallationOwner:    "acme",
		executor.EnvInstallationEndpoint: "https://api.github.com/",
		executor.EnvInstallationSecret:   "run-creds",
		executor.EnvAPITokenFile:         "/var/run/courier-git-credentials/token",
		"POD_NAMESPACE":                  "courier-runs",
		"COURIER_REPO":                   "acme/widgets",
		"COURIER_HEAD_REPO":              "acme/widgets-fork",
		"COURIER_REPO_URL":               "https://github.com/acme/widgets-fork.git",
		"COURIER_BASE_REPO_URL":          "https://github.com/acme/widgets.git",
		"COURIER_BRANCH":                 "courier/run",
	}
}

func TestDecideGitEgress(t *testing.T) {
	config, err := decideGitEgress(gitEgressEnv(validGitEgressEnv()))
	if err != nil {
		t.Fatalf("valid guard config rejected: %v", err)
	}
	if config.workRef != "refs/heads/courier/run" {
		t.Fatalf("workRef = %q", config.workRef)
	}
	remotes := gitEgressRemotes(config)
	if len(remotes) != 2 || !remotes[0].Push || remotes[0].Identity != "acme/widgets-fork" || remotes[1].Push {
		t.Fatalf("remotes = %+v, want pushable head and read-only base", remotes)
	}

	env := validGitEgressEnv()
	env["COURIER_BASE_REPO_URL"] = ""
	config, err = decideGitEgress(gitEgressEnv(env))
	if err != nil {
		t.Fatalf("same-origin base remote rejected: %v", err)
	}
	if config.baseRemote != config.workRemote {
		t.Fatalf("empty base remote = %q, want the work remote", config.baseRemote)
	}

	env = validGitEgressEnv()
	env["COURIER_HEAD_REPO"] = ""
	config, err = decideGitEgress(gitEgressEnv(env))
	if err != nil {
		t.Fatalf("same-repository guard config rejected: %v", err)
	}
	remotes = gitEgressRemotes(config)
	if len(remotes) != 1 || !remotes[0].Push || remotes[0].Identity != "acme/widgets" {
		t.Fatalf("same-repository remotes = %+v, want one merged pushable remote", remotes)
	}
	if config.workIdentity != "acme/widgets" {
		t.Fatalf("work identity = %q, want the base identity", config.workIdentity)
	}

	for _, drop := range []string{
		executor.EnvGitEgressAddress, executor.EnvInstallationOwner, executor.EnvInstallationEndpoint,
		executor.EnvInstallationSecret, executor.EnvAPITokenFile, "POD_NAMESPACE",
		"COURIER_REPO", "COURIER_REPO_URL", "COURIER_BRANCH",
	} {
		env = validGitEgressEnv()
		env[drop] = ""
		if _, err := decideGitEgress(gitEgressEnv(env)); err == nil {
			t.Fatalf("dropping %s accepted", drop)
		}
	}
}
