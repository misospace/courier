// Package git contains the small set of git operations used by a coordinator
// workspace.  It intentionally knows nothing about a forge: a remote is just
// a git remote URL and pushing is done with git's normal refspecs.
package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const defaultRemote = "origin"

// PrepareOptions describes a coordinator checkout.
//
// Base and Branch are required deliberately.  In particular, callers must not
// silently fall back to an issue number (or another shared branch) when the
// deterministic branch is unavailable.
type PrepareOptions struct {
	RemoteURL string
	Directory string
	Base      string
	Branch    string
	// RemoteName is normally left empty for origin.  It is configurable so the
	// package remains useful with repositories that use another remote name.
	RemoteName string
}

// Workspace is a checked-out coordinator workspace.
type Workspace struct {
	Directory  string
	RemoteURL  string
	RemoteName string
	Base       string
	Branch     string
	Adopted    bool
}

// Brief describes one completed delegation unit.  Objective and Outcome are
// both included in the commit message so the log remains useful if a status
// checkpoint is lost.
type Brief struct {
	ID string
	// Summary is the compact handoff text used by callers that already have a
	// completed brief summary.  Objective and Outcome are the more structured
	// form; at least Summary or Objective must be supplied.
	Summary   string
	Objective string
	Outcome   string
}

// CommandError preserves git's stderr and exit code for callers that need to
// distinguish a missing ref from an actual command failure.
type CommandError struct {
	Args   []string
	Err    error
	Stderr string
}

func (e *CommandError) Error() string {
	if e.Stderr == "" {
		return fmt.Sprintf("git %s: %v", strings.Join(e.Args, " "), e.Err)
	}
	return fmt.Sprintf("git %s: %v: %s", strings.Join(e.Args, " "), e.Err, e.Stderr)
}

func (e *CommandError) Unwrap() error { return e.Err }

// ExitCode returns git's process exit code, or -1 if git did not start.
func (e *CommandError) ExitCode() int {
	var exitErr *exec.ExitError
	if !errors.As(e.Err, &exitErr) {
		return -1
	}
	return exitErr.ExitCode()
}

func run(ctx context.Context, directory string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if directory != "" {
		cmd.Dir = directory
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, &CommandError{Args: append([]string(nil), args...), Err: err, Stderr: strings.TrimSpace(stderr.String())}
	}
	return stdout.Bytes(), nil
}

// Clone clones remoteURL into destination.  Arguments are passed directly to
// git; no shell is involved, so URLs and paths cannot become shell syntax.
func Clone(ctx context.Context, remoteURL, destination string) error {
	if strings.TrimSpace(remoteURL) == "" {
		return errors.New("git clone: remote URL is required")
	}
	if strings.TrimSpace(destination) == "" {
		return errors.New("git clone: destination is required")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return fmt.Errorf("create clone parent: %w", err)
	}
	_, err := run(ctx, "", "clone", "--", remoteURL, destination)
	return err
}

// RemoteBranchExists reports whether remoteURL advertises the branch, without
// requiring a local clone. It is the forge-agnostic half of the "adopt only
// when orphaned" guard: deciding what an existing remote branch means is the
// caller's job.
func RemoteBranchExists(ctx context.Context, remoteURL, branch string) (bool, error) {
	if strings.TrimSpace(remoteURL) == "" {
		return false, errors.New("remote URL is required")
	}
	if err := validateRef(branch, "branch"); err != nil {
		return false, err
	}
	out, err := run(ctx, "", "ls-remote", "--heads", "--", remoteURL, "refs/heads/"+branch)
	if err != nil {
		return false, err
	}
	return len(bytes.TrimSpace(out)) > 0, nil
}

// Prepare clones a repository and checks out its deterministic work branch.
// If the branch already exists on the remote, it is adopted and synchronized
// with Base before returning.  New branches are created from the fetched Base.
func Prepare(ctx context.Context, options PrepareOptions) (*Workspace, error) {
	if strings.TrimSpace(options.RemoteURL) == "" {
		return nil, errors.New("prepare: remote URL is required")
	}
	if strings.TrimSpace(options.Directory) == "" {
		return nil, errors.New("prepare: directory is required")
	}
	if err := validateRef(options.Base, "base"); err != nil {
		return nil, err
	}
	if err := validateRef(options.Branch, "branch"); err != nil {
		return nil, err
	}
	remoteName := options.RemoteName
	if remoteName == "" {
		remoteName = defaultRemote
	}
	if err := validateRef(remoteName, "remote name"); err != nil {
		return nil, err
	}

	if err := Clone(ctx, options.RemoteURL, options.Directory); err != nil {
		return nil, err
	}
	workspace := &Workspace{
		Directory:  options.Directory,
		RemoteURL:  options.RemoteURL,
		RemoteName: remoteName,
		Base:       options.Base,
		Branch:     options.Branch,
	}
	if err := workspace.fetch(ctx); err != nil {
		return nil, err
	}

	exists, err := workspace.remoteBranchExists(ctx)
	if err != nil {
		return nil, err
	}
	if exists {
		if err := workspace.Adopt(ctx); err != nil {
			return nil, err
		}
		return workspace, nil
	}

	if _, err := run(ctx, workspace.Directory, "checkout", "-b", workspace.Branch, workspace.remoteRef(workspace.Base)); err != nil {
		return nil, err
	}
	return workspace, nil
}

// Adopt checks out an existing remote work branch and synchronizes it with the
// configured base before returning.  It is separate from Prepare so resume
// code can make the adoption step explicit, while Prepare remains the normal
// clone-and-prepare entry point.
func (w *Workspace) Adopt(ctx context.Context) error {
	if err := validateRef(w.Base, "base"); err != nil {
		return err
	}
	if err := validateRef(w.Branch, "branch"); err != nil {
		return err
	}
	if err := w.fetch(ctx); err != nil {
		return err
	}
	exists, err := w.remoteBranchExists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("adopt: remote branch %q does not exist", w.Branch)
	}
	if _, err := run(ctx, w.Directory, "checkout", "-B", w.Branch, w.remoteRef(w.Branch)); err != nil {
		return err
	}
	// This order is intentional: adoption always synchronizes to base before
	// a coordinator can inspect or change the worktree.
	if err := w.SyncToBase(ctx); err != nil {
		return err
	}
	w.Adopted = true
	return nil
}

func (w *Workspace) remoteRef(ref string) string {
	return w.RemoteName + "/" + ref
}

func (w *Workspace) fetch(ctx context.Context) error {
	_, err := run(ctx, w.Directory, "fetch", "--prune", w.RemoteName)
	return err
}

func (w *Workspace) remoteBranchExists(ctx context.Context) (bool, error) {
	_, err := run(ctx, w.Directory, "show-ref", "--verify", "--quiet", "refs/remotes/"+w.remoteRef(w.Branch))
	if err == nil {
		return true, nil
	}
	var commandErr *CommandError
	if errors.As(err, &commandErr) && commandErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// SyncToBase fetches Base and merges it into the current work branch.  A merge
// preserves the remote branch's ancestry, allowing a normal push after
// adoption; callers never need an unadvertised force-push.
func (w *Workspace) SyncToBase(ctx context.Context) error {
	if err := validateRef(w.Base, "base"); err != nil {
		return err
	}
	if err := w.fetch(ctx); err != nil {
		return err
	}
	_, err := run(ctx, w.Directory, "merge", "--no-edit", w.remoteRef(w.Base))
	return err
}

// SyncToBase synchronizes a checked-out work branch with a remote base branch.
func SyncToBase(ctx context.Context, directory, remoteName, base string) error {
	if remoteName == "" {
		remoteName = defaultRemote
	}
	return (&Workspace{Directory: directory, RemoteName: remoteName, Base: base}).SyncToBase(ctx)
}

// CommitMessage builds the durable handoff message for a completed brief.
func CommitMessage(brief Brief) (string, error) {
	summary := strings.TrimSpace(brief.Summary)
	objective := strings.TrimSpace(brief.Objective)
	outcome := strings.TrimSpace(brief.Outcome)
	if summary == "" {
		summary = objective
	}
	if summary == "" {
		return "", errors.New("commit brief: objective is required")
	}
	var body []string
	if objective != "" && objective != summary {
		body = append(body, "Objective: "+objective)
	}
	if outcome != "" {
		body = append(body, "Outcome: "+outcome)
	}
	if len(body) == 0 {
		return summary, nil
	}
	return summary + "\n\n" + strings.Join(body, "\n"), nil
}

// CommitBrief stages all workspace changes and creates exactly one commit for
// the completed brief.  The returned SHA is the commit that should be pushed
// and recorded in the run checkpoint.
func (w *Workspace) CommitBrief(ctx context.Context, brief Brief) (string, error) {
	message, err := CommitMessage(brief)
	if err != nil {
		return "", err
	}
	if _, err := run(ctx, w.Directory, "add", "--all", "--", "."); err != nil {
		return "", err
	}
	if _, err := run(ctx, w.Directory, "diff", "--cached", "--quiet"); err == nil {
		return "", errors.New("commit brief: no changes to commit")
	} else {
		var commandErr *CommandError
		if !errors.As(err, &commandErr) || commandErr.ExitCode() != 1 {
			return "", err
		}
	}
	if _, err := run(ctx, w.Directory, "commit", "-m", message); err != nil {
		return "", err
	}
	sha, err := run(ctx, w.Directory, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(sha)), nil
}

// CommitBrief stages and commits one completed brief in directory.
func CommitBrief(ctx context.Context, directory string, brief Brief) (string, error) {
	return (&Workspace{Directory: directory}).CommitBrief(ctx, brief)
}

// Push publishes the current work branch using an explicit refspec.
func (w *Workspace) Push(ctx context.Context) error {
	if err := validateRef(w.Branch, "branch"); err != nil {
		return err
	}
	_, err := run(ctx, w.Directory, "push", w.RemoteName, "HEAD:refs/heads/"+w.Branch)
	return err
}

// Push publishes branch from directory to remoteName.
func Push(ctx context.Context, directory, remoteName, branch string) error {
	if remoteName == "" {
		remoteName = defaultRemote
	}
	return (&Workspace{Directory: directory, RemoteName: remoteName, Branch: branch}).Push(ctx)
}

// BranchName returns the deterministic work-branch name for a run.  A missing
// or non-positive ref is an error rather than a fallback, preventing unrelated
// runs from colliding on a shared branch.
func BranchName(repo string, ref int, mode string) (string, error) {
	if strings.TrimSpace(repo) == "" {
		return "", errors.New("branch name: repository is required")
	}
	if ref <= 0 {
		return "", errors.New("branch name: ref must be positive")
	}
	if strings.TrimSpace(mode) == "" {
		return "", errors.New("branch name: mode is required")
	}
	repoSlug := slug(repo)
	// Keep the readable legacy name for ordinary owner/name identities, but
	// retain a stable digest whenever slugging discards repository characters.
	// Without this, e.g. acme/foo.bar and acme/foo-bar resolve to one branch.
	canonical := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(repo, "/", "-")))
	if repoSlug != canonical {
		digest := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(repo))))
		repoSlug += "-" + hex.EncodeToString(digest[:])[:8]
	}
	branch := "courier/" + slug(mode) + "/" + repoSlug + "/" + strconv.Itoa(ref)
	if err := validateRef(branch, "branch"); err != nil {
		return "", err
	}
	return branch, nil
}

// WorkBranch is an alias that names the purpose of BranchName at call sites.
func WorkBranch(repo string, ref int, mode string) (string, error) {
	return BranchName(repo, ref, mode)
}

func slug(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	result := strings.Trim(b.String(), "-")
	if result == "" {
		return "repo"
	}
	return result
}

func validateRef(ref, label string) error {
	if strings.TrimSpace(ref) == "" {
		return fmt.Errorf("%s is required", label)
	}
	if strings.HasPrefix(ref, "-") {
		return fmt.Errorf("invalid %s %q", label, ref)
	}
	// check-ref-format is itself argumentized and catches all of git's ref
	// rules, including control characters and ambiguous sequences.
	if _, err := run(context.Background(), "", "check-ref-format", "--branch", ref); err != nil {
		return fmt.Errorf("invalid %s %q: %w", label, ref, err)
	}
	return nil
}
