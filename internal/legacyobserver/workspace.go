package legacyobserver

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/misospace/courier/internal/executor"
)

// Options identifies the model workspace and the observer-owned scratch clone.
// RemoteURL is operator-trusted; no workspace git configuration is consulted.
type Options struct {
	Workspace  string
	Scratch    string
	RemoteURL  string
	Branch     string
	BaseBranch string
	Baseline   string
	Username   string
	Token      string
}

// Seed fetches trusted refs into a fresh bare repository and returns the work
// branch tip, or the base tip when no work branch exists yet.
func Seed(ctx context.Context, remoteURL, baseBranch, workBranch, scratch, username, token string) (string, error) {
	if !validBranch(baseBranch) || !validBranch(workBranch) || baseBranch == workBranch {
		return "", errors.New("invalid observer branch")
	}
	if remoteURL == "" || scratch == "" {
		return "", errors.New("missing observer remote or scratch")
	}
	if err := os.Mkdir(scratch, 0700); err != nil {
		return "", err
	}
	if err := runGit(ctx, scratch, nil, "init", "--bare", "--quiet", "."); err != nil {
		return "", errors.New("observer repository initialization failed")
	}
	auth, cleanup, err := askpass(scratch, username, token)
	if err != nil {
		return "", err
	}
	defer cleanup()
	if err := fetch(ctx, scratch, remoteURL, "refs/heads/"+baseBranch, "refs/observer/base", auth); err != nil {
		return "", errors.New("observer base fetch failed")
	}
	base, err := refOID(ctx, scratch, "refs/observer/base")
	if err != nil {
		return "", errors.New("observer base ref unavailable")
	}
	remoteWork, err := remoteBranchExists(ctx, scratch, remoteURL, workBranch, auth)
	if err != nil {
		return "", errors.New("observer work branch lookup failed")
	}
	if remoteWork {
		if err := fetch(ctx, scratch, remoteURL, "refs/heads/"+workBranch, "refs/observer/work", auth); err != nil {
			return "", errors.New("observer work fetch failed")
		}
		if tip, err := refOID(ctx, scratch, "refs/observer/work"); err == nil {
			return tip, nil
		}
		return "", errors.New("observer work ref unavailable")
	}
	return base, nil
}

// Observe independently fetches the current branch and compares the complete
// filesystem (except the .git directory) to its tree.
func Observe(ctx context.Context, o Options) (executor.WorkspaceSafetyEvidence, error) {
	ev := executor.WorkspaceSafetyEvidence{State: "unknown"}
	if !validBranch(o.Branch) || !validBranch(o.BaseBranch) || o.RemoteURL == "" || o.Baseline == "" {
		return ev, nil
	}
	head, branch, err := workspaceHead(o.Workspace)
	if err != nil {
		return ev, nil
	}
	ev.Branch, ev.Head = branch, head
	if branch != o.Branch {
		return ev, nil
	}
	if !validOID(o.Baseline) {
		return ev, nil
	}
	if err := os.Mkdir(o.Scratch, 0700); err != nil {
		return ev, nil
	}
	if err := runGit(ctx, o.Scratch, nil, "init", "--bare", "--quiet", "."); err != nil {
		return ev, nil
	}
	auth, cleanup, err := askpass(o.Scratch, o.Username, o.Token)
	if err != nil {
		return ev, nil
	}
	defer cleanup()
	exists, err := remoteBranchExists(ctx, o.Scratch, o.RemoteURL, o.Branch, auth)
	if err != nil {
		return ev, nil
	}
	if !exists {
		if head != o.Baseline {
			ev.State = "dirty"
			return ev, nil
		}
		if err := fetch(ctx, o.Scratch, o.RemoteURL, "refs/heads/"+o.BaseBranch, "refs/observer/base", auth); err != nil {
			return ev, nil
		}
		base, err := refOID(ctx, o.Scratch, "refs/observer/base")
		if err != nil || base != o.Baseline {
			return ev, nil
		}
		if treeMatches(ctx, o.Workspace, o.Scratch, o.Baseline) {
			ev.State = "empty"
		} else {
			ev.State = "dirty"
		}
		return ev, nil
	}
	if err := fetch(ctx, o.Scratch, o.RemoteURL, "refs/heads/"+o.Branch, "refs/observer/work", auth); err != nil {
		return ev, nil
	}
	tip, err := refOID(ctx, o.Scratch, "refs/observer/work")
	if err != nil {
		return ev, nil
	}
	ev.RemoteHead = tip
	if head != tip {
		return ev, nil
	}
	if treeMatches(ctx, o.Workspace, o.Scratch, tip) {
		ev.State = "pushed"
	} else {
		ev.State = "dirty"
	}
	return ev, nil
}

func validBranch(s string) bool {
	return s != "" && !strings.HasPrefix(s, "-") && !strings.ContainsAny(s, "~^:?*[\\\x00\t\n") && !strings.Contains(s, "..") && !strings.Contains(s, "//") && !strings.HasSuffix(s, ".")
}
func validOID(s string) bool {
	if len(s) != 40 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func runGit(ctx context.Context, dir string, extra []string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.DevNull, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0", "GIT_CONFIG_COUNT=6", "GIT_CONFIG_KEY_0=core.hooksPath", "GIT_CONFIG_VALUE_0=/dev/null", "GIT_CONFIG_KEY_1=core.fsmonitor", "GIT_CONFIG_VALUE_1=false", "GIT_CONFIG_KEY_2=core.replaceRefs", "GIT_CONFIG_VALUE_2=false", "GIT_CONFIG_KEY_3=protocol.ext.allow", "GIT_CONFIG_VALUE_3=never", "GIT_CONFIG_KEY_4=protocol.file.allow", "GIT_CONFIG_VALUE_4=always", "GIT_CONFIG_KEY_5=core.pager", "GIT_CONFIG_VALUE_5=cat"}
	cmd.Env = append(cmd.Env, extra...)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	return cmd.Run()
}
func fetch(ctx context.Context, repo, url, src, dst string, env []string) error {
	return runGit(ctx, repo, env, "-c", "fetch.fsckObjects=true", "fetch", "--no-tags", "--no-recurse-submodules", url, "+"+src+":"+dst)
}
func remoteBranchExists(ctx context.Context, repo, url, branch string, auth []string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", "ls-remote", "--heads", url, "refs/heads/"+branch)
	cmd.Dir = repo
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.DevNull, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_COUNT=6", "GIT_CONFIG_KEY_0=core.hooksPath", "GIT_CONFIG_VALUE_0=/dev/null", "GIT_CONFIG_KEY_1=core.fsmonitor", "GIT_CONFIG_VALUE_1=false", "GIT_CONFIG_KEY_2=core.replaceRefs", "GIT_CONFIG_VALUE_2=false", "GIT_CONFIG_KEY_3=protocol.ext.allow", "GIT_CONFIG_VALUE_3=never", "GIT_CONFIG_KEY_4=protocol.file.allow", "GIT_CONFIG_VALUE_4=always", "GIT_CONFIG_KEY_5=core.pager", "GIT_CONFIG_VALUE_5=cat"}, auth...)
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	return len(bytes.TrimSpace(out)) != 0, nil
}

func refOID(ctx context.Context, repo, ref string) (string, error) {
	b, err := execGitOutput(ctx, repo, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(b))
	if !validOID(s) {
		return "", errors.New("invalid object id")
	}
	return s, nil
}
func execGitOutput(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.DevNull, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_COUNT=6", "GIT_CONFIG_KEY_0=core.hooksPath", "GIT_CONFIG_VALUE_0=/dev/null", "GIT_CONFIG_KEY_1=core.fsmonitor", "GIT_CONFIG_VALUE_1=false", "GIT_CONFIG_KEY_2=core.replaceRefs", "GIT_CONFIG_VALUE_2=false", "GIT_CONFIG_KEY_3=protocol.ext.allow", "GIT_CONFIG_VALUE_3=never", "GIT_CONFIG_KEY_4=protocol.file.allow", "GIT_CONFIG_VALUE_4=always", "GIT_CONFIG_KEY_5=core.pager", "GIT_CONFIG_VALUE_5=cat"}
	return cmd.Output()
}

func askpass(dir, username, token string) ([]string, func(), error) {
	if username == "" && token == "" {
		return nil, func() {}, nil
	}
	name := filepath.Join(dir, "askpass")
	body := "#!/bin/sh\ncase \"$1\" in *sername*) printf '%s\\n' \"$COURIER_OBSERVER_USERNAME\";; *) printf '%s\\n' \"$COURIER_OBSERVER_TOKEN\";; esac\n"
	if runtime.GOOS == "windows" {
		return nil, nil, errors.New("askpass unsupported")
	}
	if err := os.WriteFile(name, []byte(body), 0700); err != nil {
		return nil, nil, err
	}
	return []string{"GIT_ASKPASS=" + name, "GIT_ASKPASS_REQUIRE=force", "COURIER_OBSERVER_USERNAME=" + username, "COURIER_OBSERVER_TOKEN=" + token}, func() { _ = os.Remove(name) }, nil
}

func workspaceHead(workspace string) (string, string, error) {
	root, err := filepath.Abs(workspace)
	if err != nil {
		return "", "", err
	}
	gitdir := filepath.Join(root, ".git")
	st, err := os.Lstat(gitdir)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return "", "", errors.New("invalid workspace git directory")
	}
	headBytes, err := safeMetadata(gitdir, "HEAD")
	if err != nil {
		return "", "", err
	}
	head := strings.TrimSpace(string(headBytes))
	if strings.HasPrefix(head, "ref: ") {
		ref := strings.TrimPrefix(head, "ref: ")
		if !strings.HasPrefix(ref, "refs/heads/") || strings.Contains(ref, "..") {
			return "", "", errors.New("invalid workspace HEAD ref")
		}
		branch := strings.TrimPrefix(ref, "refs/heads/")
		if !validBranch(branch) {
			return "", "", errors.New("invalid workspace branch")
		}
		b, err := safeRef(gitdir, ref)
		if err != nil {
			return "", "", err
		}
		oid := strings.TrimSpace(string(b))
		if !validOID(oid) {
			return "", "", errors.New("invalid workspace ref")
		}
		return oid, branch, nil
	}
	if !validOID(head) {
		return "", "", errors.New("invalid workspace HEAD")
	}
	return head, "", nil
}
func safeMetadata(gitdir, rel string) ([]byte, error) {
	full := filepath.Join(gitdir, filepath.FromSlash(rel))
	cur := gitdir
	for _, part := range strings.Split(filepath.FromSlash(rel), string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		st, e := os.Lstat(cur)
		if e != nil || st.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe git metadata")
		}
		if cur != full && !st.IsDir() {
			return nil, errors.New("unsafe git metadata")
		}
	}
	st, e := os.Lstat(full)
	if e != nil || !st.Mode().IsRegular() {
		return nil, errors.New("unsafe git metadata")
	}
	return os.ReadFile(full)
}
func safeRef(gitdir, ref string) ([]byte, error) {
	if b, e := safeMetadata(gitdir, ref); e == nil {
		return b, nil
	}
	b, e := safeMetadata(gitdir, "packed-refs")
	if e != nil {
		return nil, e
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == ref && validOID(fields[0]) {
			return []byte(fields[0]), nil
		}
	}
	return nil, errors.New("workspace ref unavailable")
}

type treeEntry struct {
	mode, oid string
	path      []byte
}

func treeMatches(ctx context.Context, workspace, bare, commit string) bool {
	if !validOID(commit) {
		return false
	}
	b, err := execGitOutput(ctx, bare, "ls-tree", "-r", "-z", "--full-tree", commit)
	if err != nil {
		return false
	}
	entries := map[string]treeEntry{}
	for _, rec := range bytes.Split(b, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			return false
		}
		fields := strings.Fields(string(rec[:tab]))
		if len(fields) != 3 || fields[1] != "blob" || !validOID(fields[2]) {
			return false
		}
		p := rec[tab+1:]
		if len(p) == 0 || p[0] == '/' || bytes.Contains(p, []byte("../")) || bytes.Equal(p, []byte("..")) {
			return false
		}
		entries[string(p)] = treeEntry{fields[0], fields[2], append([]byte(nil), p...)}
	}
	root, err := filepath.Abs(workspace)
	if err != nil {
		return false
	}
	st, err := os.Lstat(root)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return false
	}
	seen := map[string]bool{}
	err = filepath.WalkDir(root, func(name string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if name == root {
			return nil
		}
		rel, e := filepath.Rel(root, name)
		if e != nil {
			return e
		}
		rel = filepath.ToSlash(rel)
		if rel == ".git" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return errors.New("invalid git metadata")
		}
		if strings.HasPrefix(rel, ".git/") {
			return errors.New("nested git metadata")
		}
		if d.IsDir() {
			return nil
		}
		entry, ok := entries[rel]
		if !ok {
			return errors.New("untracked path")
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		switch {
		case info.Mode().IsRegular():
			data, e := os.ReadFile(name)
			if e != nil {
				return e
			}
			h := sha1.Sum(append([]byte(fmt.Sprintf("blob %d\x00", len(data))), data...))
			if hex.EncodeToString(h[:]) != entry.oid {
				return errors.New("blob mismatch")
			}
			mode := "100644"
			if info.Mode().Perm()&0111 != 0 {
				mode = "100755"
			}
			if entry.mode != mode {
				return errors.New("mode mismatch")
			}
		case info.Mode()&os.ModeSymlink != 0:
			target, e := os.Readlink(name)
			if e != nil {
				return e
			}
			h := sha1.Sum([]byte(fmt.Sprintf("blob %d\x00%s", len(target), target)))
			if entry.mode != "120000" || hex.EncodeToString(h[:]) != entry.oid {
				return errors.New("symlink mismatch")
			}
			if target == "" || filepath.IsAbs(target) {
				return errors.New("unsafe symlink")
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(name), target))
			if resolved == root || !within(root, resolved) {
				return errors.New("escaping symlink")
			}
			if fi, e := os.Stat(name); e == nil && fi.IsDir() {
				return errors.New("directory symlink")
			}
		default:
			return errors.New("special file")
		}
		seen[rel] = true
		return nil
	})
	if err != nil {
		return false
	}
	return len(seen) == len(entries)
}
func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
