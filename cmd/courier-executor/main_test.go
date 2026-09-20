package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunPreparesOrphanBranchAndInvokesOpenCodeWithExactContext(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	orphan := filepath.Join(root, "orphan")
	runGit(t, root, "clone", remote, orphan)
	configureGit(t, orphan)
	runGit(t, orphan, "checkout", "-b", "courier/resolve-issue/acme-widgets/7", "origin/main")
	write(t, filepath.Join(orphan, "work.txt"), "previous brief\n")
	commit(t, orphan, "work: previous brief")
	runGit(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widgets/7")

	write(t, filepath.Join(source, "base.txt"), "moved base\n")
	commit(t, source, "base: moved")
	runGit(t, source, "push", "origin", "main")

	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, "#!/bin/sh\nprintf 'opencode argv: %s\\n' \"$*\"\n")
	workspace := filepath.Join(root, "workspace")
	termination := filepath.Join(root, "termination")
	t.Setenv("COURIER_REPO_URL", remote)
	t.Setenv("COURIER_WORKSPACE", workspace)
	t.Setenv("COURIER_BASE", "main")
	t.Setenv("COURIER_BRANCH", "courier/resolve-issue/acme-widgets/7")
	t.Setenv("COURIER_GOAL", "Open a PR to address issue #7.")
	t.Setenv("COURIER_MODEL", "any-model/name")
	t.Setenv("COURIER_FRAMING", "capacity is elastic; fan out freely")
	t.Setenv("COURIER_OPENCODE_BINARY", fakeOpenCode)
	t.Setenv("COURIER_TERMINATION_FILE", termination)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != 0 {
		t.Fatalf("run exit code = %d, stderr=%q, stdout=%q", code, errorsOut.String(), output.String())
	}
	if _, err := os.Stat(filepath.Join(workspace, "base.txt")); err != nil {
		t.Fatalf("adopted branch was not synchronized to base: %v", err)
	}
	if !strings.Contains(output.String(), "Open a PR to address issue #7.") || !strings.Contains(output.String(), "any-model/name") || !strings.Contains(output.String(), "capacity is elastic; fan out freely") {
		t.Fatalf("OpenCode did not receive exact goal/model/framing: %q", output.String())
	}
	if !strings.Contains(output.String(), `COURIER_TERMINATION {"phase":"AwaitingReview","result":"success","exit_code":0`) {
		t.Fatalf("missing stable success termination: %q", output.String())
	}
	terminationOutput := string(mustRead(t, termination))
	if !strings.Contains(terminationOutput, `"phase":"AwaitingReview"`) {
		t.Fatalf("termination file = %q", terminationOutput)
	}
}

func TestAskpassNeverLogsOrTakesCredentialsAsArguments(t *testing.T) {
	var output bytes.Buffer
	if code := askpass([]string{"Password for https://git.example/repo.git:"}, "user", "secret-token", &output); code != 0 {
		t.Fatalf("askpass exit code = %d", code)
	}
	if output.String() != "secret-token" {
		t.Fatalf("askpass output = %q", output.String())
	}
	if strings.Contains(strings.Join([]string{"Password for https://git.example/repo.git:"}, " "), "secret-token") {
		t.Fatal("credential appeared in askpass arguments")
	}
}

func TestReadConfigRequiresRemoteAndStatusBranch(t *testing.T) {
	_, err := readConfig(func(name string) string {
		if name == "COURIER_GOAL" || name == "COURIER_MODEL" || name == "COURIER_BRANCH" {
			return "set"
		}
		return ""
	})
	if err == nil || !strings.Contains(err.Error(), "COURIER_REPO_URL") {
		t.Fatalf("readConfig error = %v, want missing remote URL", err)
	}
}

func configureGit(t *testing.T, directory string) {
	t.Helper()
	runGit(t, directory, "config", "user.name", "Courier Test")
	runGit(t, directory, "config", "user.email", "courier-test@example.invalid")
}

func commit(t *testing.T, directory, message string) {
	t.Helper()
	runGit(t, directory, "add", "--all", "--", ".")
	runGit(t, directory, "commit", "-m", message)
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = directory
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v (%s)", strings.Join(args, " "), err, output)
	}
}
