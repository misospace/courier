package legacyobserver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestObserveRemoteBranchStates(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	gitTest(t, root, "init", "--bare", remote)
	seed := filepath.Join(root, "seed")
	gitTest(t, root, "clone", remote, seed)
	gitTest(t, seed, "config", "user.name", "test")
	gitTest(t, seed, "config", "user.email", "test@example.invalid")
	writeTestFile(t, seed, "tracked.txt", "baseline\n")
	gitTest(t, seed, "add", ".")
	gitTest(t, seed, "commit", "-m", "baseline")
	gitTest(t, seed, "branch", "-M", "main")
	gitTest(t, seed, "push", "origin", "main")
	baseline := gitText(t, seed, "rev-parse", "HEAD")
	gitTest(t, seed, "checkout", "-b", "work")
	gitTest(t, seed, "push", "origin", "work")

	observe := func(name string) executorEvidence {
		t.Helper()
		workspace := filepath.Join(root, name)
		gitTest(t, root, "clone", "--branch", "work", remote, workspace)
		scratch := filepath.Join(root, "scratch-"+name)
		ev, err := Observe(ctx, Options{Workspace: workspace, Scratch: scratch, RemoteURL: remote, Branch: "work", BaseBranch: "main", Baseline: baseline})
		if err != nil {
			t.Fatalf("Observe: %v", err)
		}
		return executorEvidence{state: ev.State, head: ev.Head, remote: ev.RemoteHead}
	}
	if got := observe("clean"); got.state != "pushed" {
		t.Fatalf("clean state = %#v", got)
	}

	workspace := filepath.Join(root, "dirty")
	gitTest(t, root, "clone", "--branch", "work", remote, workspace)
	writeTestFile(t, workspace, "untracked", "surprise")
	ev, err := Observe(ctx, Options{Workspace: workspace, Scratch: filepath.Join(root, "scratch-dirty"), RemoteURL: remote, Branch: "work", BaseBranch: "main", Baseline: baseline})
	if err != nil || ev.State != "dirty" {
		t.Fatalf("untracked state = %#v, err %v", ev, err)
	}

	local := filepath.Join(root, "local")
	gitTest(t, root, "clone", "--branch", "work", remote, local)
	gitTest(t, local, "config", "user.name", "test")
	gitTest(t, local, "config", "user.email", "test@example.invalid")
	writeTestFile(t, local, "tracked.txt", "local\n")
	gitTest(t, local, "commit", "-am", "unpushed")
	ev, err = Observe(ctx, Options{Workspace: local, Scratch: filepath.Join(root, "scratch-local"), RemoteURL: remote, Branch: "work", BaseBranch: "main", Baseline: baseline})
	if err != nil || ev.State != "unknown" {
		t.Fatalf("unpushed state = %#v, err %v", ev, err)
	}
}

type executorEvidence struct{ state, head, remote string }

func TestWorkspaceHeadRejectsMalformedAndSymlinkMetadata(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(string)
	}{
		{"malformed", func(d string) {
			if err := os.WriteFile(filepath.Join(d, ".git", "HEAD"), []byte("garbage\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink-ref", func(d string) {
			if err := os.WriteFile(filepath.Join(d, ".git", "HEAD"), []byte("ref: refs/heads/work\\n"), 0600); err != nil {
				t.Fatal(err)
			}
			ref := filepath.Join(d, ".git", "refs", "heads", "work")
			if err := os.Symlink("../../config", ref); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := t.TempDir()
			gitTest(t, d, "init", "--quiet")
			test.mutate(d)
			if _, _, err := workspaceHead(d); err == nil {
				t.Fatal("expected unsafe metadata error")
			}
		})
	}
}

func TestObserveNeverExecutesWorkspaceHooksOrConfig(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	gitTest(t, root, "init", "--bare", remote)
	seed := filepath.Join(root, "seed")
	gitTest(t, root, "clone", remote, seed)
	gitTest(t, seed, "config", "user.name", "test")
	gitTest(t, seed, "config", "user.email", "test@example.invalid")
	writeTestFile(t, seed, "f", "x")
	gitTest(t, seed, "add", ".")
	gitTest(t, seed, "commit", "-m", "base")
	gitTest(t, seed, "branch", "-M", "main")
	gitTest(t, seed, "push", "origin", "main")
	baseline := gitText(t, seed, "rev-parse", "HEAD")
	workspace := filepath.Join(root, "workspace")
	gitTest(t, root, "clone", "--branch", "main", remote, workspace)
	marker := filepath.Join(root, "executed")
	if err := os.WriteFile(filepath.Join(workspace, ".git", "config"), []byte("[core]\n\thooksPath = "+root+"/hooks\n[alias]\n\tstatus = !touch "+marker+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "hooks"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "hooks", "pre-fetch"), []byte("#!/bin/sh\ntouch "+marker+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	_, err := Observe(ctx, Options{Workspace: workspace, Scratch: filepath.Join(root, "scratch"), RemoteURL: remote, Branch: "main", BaseBranch: "main", Baseline: baseline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("workspace config executed: %v", err)
	}
}

func writeTestFile(t *testing.T, dir, name, value string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}
func gitTest(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
}
func gitText(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}
