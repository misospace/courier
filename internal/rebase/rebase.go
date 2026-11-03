// Package rebase implements the trusted, bounded mechanical rebase execution
// path (#715). It exists so a rebase CoderRun never rides the ordinary PR-fix
// path: there is no model session, no merge-based workspace preparation, and
// no open-ended goal. A rebase is prepared by adopting the existing PR branch
// verbatim, replayed onto the pull request's live base by deterministic code,
// and published to the existing PR branch only through a force-with-lease tied
// to the remote head this attempt observed.
//
// Hard walls (misospace/courier#715, misospace/dispatch#1327):
//
//   - Only mechanical conflict resolution may land. Every conflicted hunk is
//     classified by closed rules; a hunk outside the rules stops the attempt
//     for a human with the conflict summary — it is never "resolved" by
//     picking a side.
//   - The push is an explicit compare-and-set: a concurrent writer that moved
//     the PR branch between the attempt's read and its push fails the attempt
//     instead of being overwritten.
//   - One attempt per conflicting episode: the engine runs once per execution
//     and never retries itself. Reissue policy belongs to the source.
//   - Publication goes only to the existing PR branch. The engine never
//     creates a ref, never merges, and never edits code semantics.
package rebase

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/misospace/courier/internal/git"
)

// Outcome is the terminal classification of one rebase attempt. It maps onto
// the executor's termination contract and from there onto the source report;
// it is never a claim of success the world has not verified.
type Outcome string

const (
	// OutcomePublished: the rebased head passed the bounded checks and was
	// pushed to the existing PR branch under the lease. It is still only a
	// handoff to the operator's own verification (CI, mergeable state).
	OutcomePublished Outcome = "published"
	// OutcomeUpToDate: the branch already contains the base tip, so there was
	// nothing to rebase and nothing to push.
	OutcomeUpToDate Outcome = "up-to-date"
	// OutcomeSemanticConflict: a conflicted hunk outside the mechanical rules
	// stopped the attempt. A human must decide; the remote branch is
	// untouched.
	OutcomeSemanticConflict Outcome = "semantic-conflict"
	// OutcomeRemoteHeadMoved: the PR branch was not at the expected head, or
	// moved before this attempt's leased push could land. The attempt ends
	// without publishing; the source's own attempt policy governs any reissue.
	OutcomeRemoteHeadMoved Outcome = "remote-head-moved"
	// OutcomeFailed: the attempt failed for a reason that is neither a
	// semantic conflict nor writer contention (git or check infrastructure).
	OutcomeFailed Outcome = "failed"
)

// DefaultCheckTimeout bounds one bounded repository check invocation. A check
// that exceeds it fails the check, never the lease.
const DefaultCheckTimeout = 10 * time.Minute

// maxRebaseRounds bounds how many conflict rounds (one per replayed commit
// needing resolution) one attempt may resolve, so a pathological branch can
// never turn mechanical resolution into an unbounded loop.
const maxRebaseRounds = 256

// maxConflictFileBytes bounds the size of a conflicted file the parser will
// classify. Anything larger is declined as non-mechanical: a mechanical
// resolution must be verifiable by reading the whole conflict.
const maxConflictFileBytes = 4 << 20

// Options describe one rebase attempt. Repo, Base, and Branch are the PR
// identity admission resolved; ExpectedHead is the remote head OID observed
// then, and is the lease the publication pushes under.
type Options struct {
	// Repo is the base repository identity ("owner/name"); reason text only.
	Repo string
	// Base is the pull request's live base ref, resolved from the PR itself.
	Base string
	// Branch is the existing PR head branch. It is never created.
	Branch string
	// ExpectedHead is the remote head OID observed at admission. Publication
	// is leased to this exact OID.
	ExpectedHead string
	// Workspace is the prepared checkout directory (cloned with
	// git.PrepareOptions.SkipBaseSync).
	Workspace string
	// RemoteURL is the work-branch remote URL the workspace was cloned from,
	// used for the pre-publication remote identity reads.
	RemoteURL string
	// RemoteName is the work-branch remote; empty means origin.
	RemoteName string
	// BaseRemoteURL names the repository that owns Base when it is not the
	// work-branch remote (fork topology). Empty means Base lives on
	// RemoteName.
	BaseRemoteURL string
	// CheckTimeout bounds each bounded repository check invocation; zero uses
	// DefaultCheckTimeout.
	CheckTimeout time.Duration
}

// ConflictReport is the human decision evidence a semantic-conflict outcome
// carries: which files conflicted and why they are not mechanical.
type ConflictReport struct {
	// Base is the base ref the rebase stopped on.
	Base string `json:"base"`
	// Files are the conflicted paths that are not mechanically resolvable.
	Files []string `json:"files"`
	// Reason is a bounded, human-readable summary of why each file needs a
	// decision.
	Reason string `json:"reason"`
}

// Result is the terminal record of one attempt.
type Result struct {
	Outcome Outcome
	// Reason is a bounded human-readable summary for the run's termination
	// diagnostic and the source report.
	Reason string
	// Head is the published remote head after OutcomePublished.
	Head string
	// Conflict is set for OutcomeSemanticConflict.
	Conflict *ConflictReport
	// LeaseHead is the remote head this attempt read at start and leased its
	// publication to.
	LeaseHead string
}

// Execute runs one rebase attempt. It is deterministic, bounded, and
// side-effect-free on the remote until the final leased push. An error return
// means the attempt could not be classified at all (infrastructure); a
// classified ending — including failure — comes back as a Result.
func Execute(ctx context.Context, opts Options) (Result, error) {
	if err := validateOptions(opts); err != nil {
		return Result{}, err
	}
	w := &git.Workspace{
		Directory:     opts.Workspace,
		RemoteURL:     strings.TrimSpace(opts.RemoteURL),
		RemoteName:    remoteName(opts.RemoteName),
		BaseRemoteURL: opts.BaseRemoteURL,
		Base:          opts.Base,
		Branch:        opts.Branch,
	}
	result := Result{}

	// The lease head is re-read fresh at execution start: it is the value this
	// attempt will hold the push to, not a projection of admission state.
	head, exists, err := git.RemoteBranchHead(ctx, w.RemoteURL, opts.Branch)
	if err != nil {
		return classifyInfra(ctx, w, "read the PR branch's remote head", err)
	}
	if !exists {
		return Result{Outcome: OutcomeRemoteHeadMoved,
			Reason: fmt.Sprintf("PR branch %q no longer exists on the remote; refusing to recreate it", opts.Branch),
		}, nil
	}
	result.LeaseHead = head
	if opts.ExpectedHead != "" && head != opts.ExpectedHead {
		return Result{Outcome: OutcomeRemoteHeadMoved,
			Reason: fmt.Sprintf("PR branch %q is at %s, not the expected head %s observed at admission; a writer moved it, so this attempt declines without publishing", opts.Branch, short(head), short(opts.ExpectedHead)),
		}, nil
	}

	// Refresh the work remote so the lease head and the base tip are the live
	// ones. Prepare already fetched; this re-fetch bounds the window between
	// clone and decision but never between push and lease — the lease does.
	if err := w.Fetch(ctx); err != nil {
		return classifyInfra(ctx, w, "fetch the work remote", err)
	}
	if w.BaseRemoteURL != "" && w.BaseRemoteURL != w.RemoteURL {
		if err := w.EnsureBaseRemote(ctx); err != nil {
			return classifyInfra(ctx, w, "configure the base remote", err)
		}
		if err := w.FetchBase(ctx); err != nil {
			return classifyInfra(ctx, w, "fetch the base remote", err)
		}
	}

	origHead, err := w.Head(ctx)
	if err != nil {
		return classifyInfra(ctx, w, "record the pre-rebase head", err)
	}
	// The workspace must be the branch the lease names: a checkout that
	// diverges from the remote head this attempt leased would replay stale
	// commits and publish them over the newer ones. The executor prepares a
	// fresh workspace immediately before this call, so a mismatch is a
	// workspace preparation fault and never an input the engine papers over.
	if origHead != head {
		return Result{Outcome: OutcomeFailed,
			Reason: fmt.Sprintf("workspace head %s is not the leased PR branch head %s; refusing to replay a stale checkout", short(origHead), short(head)),
		}, nil
	}
	// PR-touched files: the diff the replayed commits own. Used to attribute
	// bounded-check failures; never to widen the mechanical rules.
	touched, err := w.DiffPaths(ctx, w.BaseRevisionRef(), origHead)
	if err != nil {
		return classifyInfra(ctx, w, "list the PR's changed paths", err)
	}

	// Already current: the branch contains the base tip. Nothing to do, and
	// explicitly nothing to push.
	current, err := w.ContainsBase(ctx)
	if err != nil {
		return classifyInfra(ctx, w, "compare the branch with the base tip", err)
	}
	if current {
		result.Outcome = OutcomeUpToDate
		result.Reason = fmt.Sprintf("PR branch %q already contains base %q; nothing to rebase or push", opts.Branch, opts.Base)
		return result, nil
	}

	if err := w.Rebase(ctx, w.BaseRevisionRef()); err != nil {
		var conflictErr *git.RebaseConflictError
		if !errors.As(err, &conflictErr) {
			if abortErr := w.RebaseAbort(ctx); abortErr != nil {
				return classifyInfra(ctx, w, "abort the failed rebase", abortErr)
			}
			return classifyInfra(ctx, w, "rebase onto "+opts.Base, err)
		}
		// The replay stopped: classify every hunk mechanically or stop for a
		// human. Either way the remote is untouched until publication.
		if resolveErr := resolveConflicts(ctx, w, conflictErr.Paths); resolveErr != nil {
			var semantic *semanticError
			if errors.As(resolveErr, &semantic) {
				// Abort restores the branch exactly; the episode stays at one
				// attempt, and the conflict summary names the decision.
				if abortErr := w.RebaseAbort(ctx); abortErr != nil {
					return classifyInfra(ctx, w, "abort the stopped rebase", abortErr)
				}
				return Result{Outcome: OutcomeSemanticConflict,
					Reason:   semantic.reason,
					Conflict: &ConflictReport{Base: opts.Base, Files: semantic.files, Reason: semantic.reason},
				}, nil
			}
			if abortErr := w.RebaseAbort(ctx); abortErr != nil {
				return classifyInfra(ctx, w, "abort the failed resolution", abortErr)
			}
			return classifyInfra(ctx, w, "resolve the rebase conflicts", resolveErr)
		}
	}

	if err := verifyRebasedHistory(ctx, w, opts, origHead); err != nil {
		return classifyInfra(ctx, w, "verify the rebased branch", err)
	}

	// Bounded repository checks before publication. A failure attributed to a
	// PR-touched path is a semantic verdict on the replay: stop for a human,
	// publish nothing. A failure attributed only elsewhere predates the
	// rebase and does not block mechanical publication.
	if reason, stop := checkRebasedTree(ctx, w, opts, touched); stop {
		return Result{Outcome: OutcomeSemanticConflict,
			Reason:   reason,
			Conflict: &ConflictReport{Base: opts.Base, Files: []string{}, Reason: reason},
		}, nil
	}

	newHead, err := w.Head(ctx)
	if err != nil {
		return classifyInfra(ctx, w, "read the rebased head", err)
	}
	// Publish only to the branch that still exists at the leased OID. The
	// existence check doubles as a creation guard: a vanished PR branch is
	// never silently recreated.
	live, exists, err := git.RemoteBranchHead(ctx, w.RemoteURL, opts.Branch)
	if err != nil {
		return classifyInfra(ctx, w, "verify the PR branch before publication", err)
	}
	if !exists || live != result.LeaseHead {
		return Result{Outcome: OutcomeRemoteHeadMoved,
			Reason: fmt.Sprintf("PR branch %q moved to %s after this attempt read it at %s; the leased publication declines rather than discarding the writer", opts.Branch, short(live), short(result.LeaseHead)),
		}, nil
	}
	if err := w.PushForceWithLease(ctx, result.LeaseHead); err != nil {
		// A lease refusal is writer contention, not an infrastructure fault.
		if isLeaseRefusal(err) {
			return Result{Outcome: OutcomeRemoteHeadMoved,
				Reason: fmt.Sprintf("the leased push of PR branch %q was refused because the remote head moved during the attempt; nothing was published", opts.Branch),
			}, nil
		}
		return classifyInfra(ctx, w, "publish the rebased head", err)
	}
	result.Outcome = OutcomePublished
	result.Head = newHead
	result.Reason = fmt.Sprintf("rebased PR branch %q onto %q and published %s through the leased push", opts.Branch, opts.Base, short(newHead))
	return result, nil
}

func validateOptions(opts Options) error {
	var missing []string
	if strings.TrimSpace(opts.Base) == "" {
		missing = append(missing, "base ref")
	}
	if strings.TrimSpace(opts.Branch) == "" {
		missing = append(missing, "PR head branch")
	}
	if strings.TrimSpace(opts.Workspace) == "" {
		missing = append(missing, "workspace directory")
	}
	if strings.TrimSpace(opts.RemoteURL) == "" {
		missing = append(missing, "remote URL")
	}
	if strings.TrimSpace(opts.ExpectedHead) == "" {
		missing = append(missing, "expected remote head")
	}
	if len(missing) > 0 {
		return fmt.Errorf("rebase: missing %s", strings.Join(missing, ", "))
	}
	return nil
}

func remoteName(name string) string {
	if strings.TrimSpace(name) == "" {
		return "origin"
	}
	return strings.TrimSpace(name)
}

func short(oid string) string {
	if len(oid) > 12 {
		return oid[:12]
	}
	return oid
}

// classifyInfra converts an unexpected git failure into a failed outcome with
// a bounded reason. The workspace is never pushed from here.
func classifyInfra(ctx context.Context, w *git.Workspace, doing string, err error) (Result, error) {
	// A failed attempt must not leave the workspace mid-rebase; the branch may
	// be adopted again by a relaunch.
	if inProgress, inErr := w.RebaseInProgress(ctx); inErr == nil && inProgress {
		_ = w.RebaseAbort(ctx)
	}
	return Result{Outcome: OutcomeFailed, Reason: fmt.Sprintf("could not %s: %v", doing, err)}, nil
}

// isLeaseRefusal reports whether a push failure is git's stale-info lease
// refusal rather than a transport or authentication fault.
func isLeaseRefusal(err error) bool {
	var commandErr *git.CommandError
	if !errors.As(err, &commandErr) {
		return false
	}
	msg := strings.ToLower(commandErr.Stderr)
	return strings.Contains(msg, "stale info") ||
		strings.Contains(msg, "cannot lock ref") ||
		strings.Contains(msg, "fetch first") ||
		strings.Contains(msg, "behind its remote counterpart")
}

// verifyRebasedHistory enforces the invariants a mechanical rebase must hold
// before any check or publication: the rebase fully completed, the tree is
// clean, the branch identity is unchanged, the base tip is contained, and no
// replayed commit was lost.
func verifyRebasedHistory(ctx context.Context, w *git.Workspace, opts Options, origHead string) error {
	inProgress, err := w.RebaseInProgress(ctx)
	if err != nil {
		return err
	}
	if inProgress {
		return errors.New("the rebase did not complete")
	}
	dirty, err := w.StatusPorcelain(ctx)
	if err != nil {
		return err
	}
	if len(dirty) > 0 {
		return fmt.Errorf("the rebased tree is not clean (%d path(s) pending)", len(dirty))
	}
	branch, err := w.CurrentBranch(ctx)
	if err != nil {
		return err
	}
	if branch != opts.Branch {
		return fmt.Errorf("the workspace is on %q, not the PR branch %q", branch, opts.Branch)
	}
	contained, err := w.ContainsBase(ctx)
	if err != nil {
		return err
	}
	if !contained {
		return fmt.Errorf("the rebased branch does not contain base %q", opts.Base)
	}
	before, err := w.CommitCount(ctx, w.BaseRevisionRef(), origHead)
	if err != nil {
		return err
	}
	after, err := w.CommitCount(ctx, w.BaseRevisionRef(), "HEAD")
	if err != nil {
		return err
	}
	if before != after {
		return fmt.Errorf("the rebase changed the PR's commit count from %d to %d", before, after)
	}
	return nil
}

// checkRebasedTree runs the bounded repository checks. stop is true when the
// attempt must not publish; reason is the bounded evidence. The only check
// this release defines is a bounded Go build, and only when the repository is
// a Go module; repositories without a recognized toolchain pass with no
// check, which is documented behavior.
func checkRebasedTree(ctx context.Context, w *git.Workspace, opts Options, touched []string) (reason string, stop bool) {
	if _, err := os.Stat(filepath.Join(w.Directory, "go.mod")); err != nil {
		return "", false
	}
	timeout := opts.CheckTimeout
	if timeout <= 0 {
		timeout = DefaultCheckTimeout
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(checkCtx, "go", "build", "./...")
	cmd.Dir = w.Directory
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOWORK=off")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return "", false
	}
	failed := normalizePaths(buildFailurePaths(stdout.String()+stderr.String()), w.Directory)
	prFailed := intersect(failed, touched)
	if len(prFailed) == 0 {
		return fmt.Sprintf("the bounded Go build fails on paths the PR never touched (%s); the failure predates the rebase, so the mechanical publication proceeds",
			truncateJoin(failed, 5)), false
	}
	return fmt.Sprintf("the bounded Go build fails in PR-touched path(s) %s after the rebase; a mechanical replay must not publish a build it broke",
		truncateJoin(prFailed, 5)), true
}

// buildFailurePaths extracts the file paths a Go build named in its errors,
// bounded to the first maxBuildErrorPaths distinct entries.
func buildFailurePaths(output string) []string {
	const maxBuildErrorPaths = 64
	seen := map[string]struct{}{}
	var paths []string
	scanner := bufio.NewScanner(strings.NewReader(output))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() && len(paths) < maxBuildErrorPaths {
		line := strings.TrimSpace(scanner.Text())
		idx := strings.Index(line, ".go:")
		if idx <= 0 {
			continue
		}
		path := line[:idx+3]
		if strings.ContainsAny(path, " \t") || strings.HasPrefix(path, "-") {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return paths
}

// normalizePaths makes extracted check paths comparable with the diff paths:
// a leading "./" is dropped and an absolute path inside the workspace is made
// workspace-relative. A path that resolves outside the workspace is dropped —
// it can never be a PR-touched path of this replay.
func normalizePaths(paths []string, directory string) []string {
	var out []string
	for _, path := range paths {
		clean := strings.TrimPrefix(path, "./")
		if filepath.IsAbs(clean) {
			rel, err := filepath.Rel(directory, clean)
			if err != nil || strings.HasPrefix(rel, "..") {
				continue
			}
			clean = rel
		}
		out = append(out, clean)
	}
	return out
}

func intersect(candidates, touched []string) []string {
	set := make(map[string]struct{}, len(touched))
	for _, t := range touched {
		set[t] = struct{}{}
	}
	var out []string
	for _, c := range candidates {
		if _, ok := set[c]; ok {
			out = append(out, c)
		}
	}
	return out
}

func truncateJoin(values []string, max int) string {
	if len(values) > max {
		return strings.Join(values[:max], ", ") + fmt.Sprintf(" (and %d more)", len(values)-max)
	}
	return strings.Join(values, ", ")
}
