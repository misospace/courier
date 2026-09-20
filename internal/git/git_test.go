package git

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareAdoptsOrphanAndSyncsBaseBeforeWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "README.md"), "base one\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	// Create the orphaned work branch from the first base revision.
	orphan := filepath.Join(root, "orphan")
	git(t, root, "clone", remote, orphan)
	git(t, orphan, "checkout", "-b", "courier/resolve-issue/acme-widget/10", "origin/main")
	writeFile(t, filepath.Join(orphan, "work.txt"), "completed brief\n")
	commit(t, orphan, "work: previous brief")
	git(t, orphan, "push", "origin", "HEAD:refs/heads/courier/resolve-issue/acme-widget/10")

	// Move main after the work branch was created. Adoption must merge this
	// revision before returning control to the coordinator.
	writeFile(t, filepath.Join(source, "base.txt"), "new base\n")
	commit(t, source, "base: second revision")
	git(t, source, "push", "origin", "main")

	workspaceDir := filepath.Join(root, "workspace")
	workspace, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: workspaceDir,
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/10",
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !workspace.Adopted {
		t.Fatal("expected existing branch to be adopted")
	}
	if _, err := os.Stat(filepath.Join(workspaceDir, "base.txt")); err != nil {
		t.Fatalf("adopted branch was not synchronized to base: %v", err)
	}
	if _, err := gitOutput(workspaceDir, "merge-base", "--is-ancestor", "origin/main", "HEAD"); err != nil {
		t.Fatalf("base is not an ancestor after adoption: %v", err)
	}
	git(t, workspaceDir, "config", "user.name", "Courier Test")
	git(t, workspaceDir, "config", "user.email", "courier-test@example.invalid")

	writeFile(t, filepath.Join(workspaceDir, "second.txt"), "another completed brief\n")
	sha, err := workspace.CommitBrief(ctx, Brief{
		ID:        "brief-2",
		Objective: "Add the second work unit",
		Outcome:   "The coordinator can resume from this commit",
	})
	if err != nil {
		t.Fatalf("commit brief: %v", err)
	}
	if len(sha) != 40 {
		t.Fatalf("commit SHA = %q, want a full SHA", sha)
	}
	message, err := gitOutput(workspaceDir, "show", "-s", "--format=%B", "HEAD")
	if err != nil {
		t.Fatalf("read commit message: %v", err)
	}
	if !strings.Contains(string(message), "Add the second work unit") || !strings.Contains(string(message), "The coordinator can resume from this commit") {
		t.Fatalf("commit message is not substantive: %q", message)
	}
	if err := workspace.Push(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	verify := filepath.Join(root, "verify")
	git(t, root, "clone", remote, verify)
	git(t, verify, "checkout", "courier/resolve-issue/acme-widget/10")
	if _, err := os.Stat(filepath.Join(verify, "second.txt")); err != nil {
		t.Fatalf("pushed brief commit is missing from remote branch: %v", err)
	}
}

func TestPrepareCreatesBranchFromBase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "base.txt"), "base\n")
	commit(t, source, "base: initial")
	git(t, source, "branch", "-M", "main")
	git(t, source, "remote", "add", "origin", remote)
	git(t, source, "push", "-u", "origin", "main")

	workspace, err := Prepare(ctx, PrepareOptions{
		RemoteURL: remote,
		Directory: filepath.Join(root, "workspace"),
		Base:      "main",
		Branch:    "courier/resolve-issue/acme-widget/11",
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if workspace.Adopted {
		t.Fatal("new branch was unexpectedly marked adopted")
	}
	if got := strings.TrimSpace(string(mustRead(t, filepath.Join(workspace.Directory, "base.txt")))); got != "base" {
		t.Fatalf("new branch was not based on main: %q", got)
	}
}

func TestBranchNameRejectsMissingRef(t *testing.T) {
	if _, err := BranchName("acme/widget", 0, "resolve-issue"); err == nil {
		t.Fatal("BranchName accepted a missing ref")
	}
	first, err := BranchName("acme/widget", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("BranchName: %v", err)
	}
	second, err := BranchName("acme/widget", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("BranchName second call: %v", err)
	}
	if first != second {
		t.Fatalf("BranchName is not deterministic: %q != %q", first, second)
	}
}

func TestBranchNamePreservesRepositoryIdentityWhenSlugging(t *testing.T) {
	dotted, err := BranchName("acme/foo.bar", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("dotted BranchName: %v", err)
	}
	dashed, err := BranchName("acme/foo-bar", 12, "resolve-issue")
	if err != nil {
		t.Fatalf("dashed BranchName: %v", err)
	}
	if dotted == dashed {
		t.Fatalf("distinct repositories collided on branch %q", dotted)
	}
}

func initBare(t *testing.T, directory string) {
	t.Helper()
	git(t, filepath.Dir(directory), "init", "--bare", directory)
}

func initRepo(t *testing.T, directory string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, directory, "init")
	git(t, directory, "config", "user.name", "Courier Test")
	git(t, directory, "config", "user.email", "courier-test@example.invalid")
}

func commit(t *testing.T, directory, message string) {
	t.Helper()
	git(t, directory, "add", "--all", "--", ".")
	git(t, directory, "commit", "-m", message)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
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

func git(t *testing.T, directory string, args ...string) {
	t.Helper()
	if _, err := gitOutput(directory, args...); err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
}

func gitOutput(directory string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = directory
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
