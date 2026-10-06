package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// BundleRef is one ref advertised by a git bundle.
type BundleRef struct {
	OID  string
	Name string
}

// VerifyBundle checks a bundle against repoDir: every prerequisite must be
// present and the object closure must pass git's own hash checks. The bundle
// path must be an absolute regular file.
func VerifyBundle(ctx context.Context, repoDir, bundlePath string) error {
	path, err := localBundlePath(bundlePath)
	if err != nil {
		return err
	}
	if _, err := Hardened(ctx, repoDir, "bundle", "verify", "--quiet", path); err != nil {
		return fmt.Errorf("git bundle verify: %w", err)
	}
	return nil
}

// BundleHeads lists the refs a bundle advertises. Ref names are returned
// exactly as the bundle declares them; callers enforce the expected ref set.
func BundleHeads(ctx context.Context, bundlePath string) ([]BundleRef, error) {
	path, err := localBundlePath(bundlePath)
	if err != nil {
		return nil, err
	}
	out, err := Hardened(ctx, "", "bundle", "list-heads", path)
	if err != nil {
		return nil, fmt.Errorf("git bundle list-heads: %w", err)
	}
	var heads []BundleRef
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || validOID(fields[0]) != nil || fields[1] == "" {
			return nil, errors.New("git bundle: malformed ref advertisement")
		}
		heads = append(heads, BundleRef{OID: strings.ToLower(fields[0]), Name: fields[1]})
	}
	if len(heads) == 0 {
		return nil, errors.New("git bundle: no refs advertised")
	}
	return heads, nil
}

// Unbundle unpacks every object the bundle carries into objectDir. The
// directory must be empty or dedicated: objects beyond any result ref's
// closure land there too, because bounds apply to the whole delivered set.
// Prerequisites are resolved against repoDir.
func Unbundle(ctx context.Context, repoDir, objectDir, bundlePath string) error {
	path, err := localBundlePath(bundlePath)
	if err != nil {
		return err
	}
	env := []string{"GIT_OBJECT_DIRECTORY=" + objectDir}
	if _, err := HardenedWithEnv(ctx, repoDir, env, "bundle", "unbundle", path); err != nil {
		return fmt.Errorf("git bundle unbundle: %w", err)
	}
	return nil
}

// ObjectInventory is the whole object set of one object directory.
type ObjectInventory struct {
	Count        int
	TotalBytes   int64
	MaxBlobBytes int64
}

// InventoryObjects enumerates every object in objectDir (alternates are never
// configured there) and applies git's hash checks while reading them. It runs
// inside repoDir because cat-file requires a repository even with an explicit
// object directory; GIT_OBJECT_DIRECTORY still fully isolates the listing to
// the quarantine. The caller applies the §5 bounds to the result.
func InventoryObjects(ctx context.Context, repoDir, objectDir string) (ObjectInventory, error) {
	env := []string{"GIT_OBJECT_DIRECTORY=" + objectDir}
	// The inventory is bounded by the caller's object-count check on a shared
	// read budget: a hostile bundle cannot force an unbounded scan here.
	out, err := HardenedWithEnv(ctx, repoDir, env, "cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
	if err != nil {
		return ObjectInventory{}, fmt.Errorf("git inventory objects: %w", err)
	}
	inventory := ObjectInventory{}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.SplitN(line, " ", 3)
		if len(fields) != 3 || validOID(fields[0]) != nil {
			return ObjectInventory{}, errors.New("git inventory objects: malformed listing")
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 {
			return ObjectInventory{}, errors.New("git inventory objects: malformed object size")
		}
		inventory.Count++
		inventory.TotalBytes += size
		if fields[1] == "blob" && size > inventory.MaxBlobBytes {
			inventory.MaxBlobBytes = size
		}
	}
	if inventory.Count == 0 {
		return ObjectInventory{}, errors.New("git inventory objects: no objects found")
	}
	return inventory, nil
}

// FetchBundleIntoFetchHead fetches one named ref from a bundle without a
// target ref, leaving the result in FETCH_HEAD (a file, not a ref). The
// caller resolves it locally; the remote-claimed OID never reaches argv.
func FetchBundleIntoFetchHead(ctx context.Context, repoDir, bundlePath, remoteRef string) error {
	path, err := localBundlePath(bundlePath)
	if err != nil {
		return err
	}
	if remoteRef == "" || strings.HasPrefix(remoteRef, "-") {
		return errors.New("git bundle fetch: ref is required")
	}
	if _, err := Hardened(ctx, repoDir, "fetch", "--no-tags", "--", path, remoteRef); err != nil {
		return fmt.Errorf("git bundle fetch: %w", err)
	}
	return nil
}

// ResolveCommit resolves a revision expression to a full commit OID.
func ResolveCommit(ctx context.Context, repoDir, rev string) (string, error) {
	if rev == "" || strings.HasPrefix(rev, "-") {
		return "", errors.New("git resolve: revision is required")
	}
	out, err := Hardened(ctx, repoDir, "rev-parse", "--verify", "--quiet", rev)
	if err != nil {
		return "", fmt.Errorf("git resolve commit: %w", err)
	}
	oid := strings.TrimSpace(string(out))
	if validOID(oid) != nil {
		return "", errors.New("git resolve commit: invalid resolved object ID")
	}
	return strings.ToLower(oid), nil
}

// FetchBundleRef fetches one ref the bundle advertises into an exact local
// ref, force-updating it. This is for broker- or control-authored snapshot
// bundles with known ref names; worker bundles are imported refless via
// FetchFromBundle.
func FetchBundleRef(ctx context.Context, repoDir, bundlePath, remoteRef, localRef string) error {
	path, err := localBundlePath(bundlePath)
	if err != nil {
		return err
	}
	if remoteRef == "" || localRef == "" || strings.HasPrefix(remoteRef, "-") || strings.HasPrefix(localRef, "-") {
		return errors.New("git bundle fetch: refs are required")
	}
	if _, err := Hardened(ctx, repoDir, "fetch", "--no-tags", "--", path, "+"+remoteRef+":"+localRef); err != nil {
		return fmt.Errorf("git bundle fetch ref: %w", err)
	}
	return nil
}

// IsAncestor reports whether ancestor is reachable from descendant among the
// objects of repoDir. Objects missing from repoDir are an error, not "false".
func IsAncestor(ctx context.Context, repoDir, ancestor, descendant string) (bool, error) {
	if validOID(ancestor) != nil || validOID(descendant) != nil {
		return false, errors.New("git ancestry: invalid object ID")
	}
	err := HardenedExit(ctx, repoDir, "merge-base", "--is-ancestor", ancestor, descendant)
	if err == nil {
		return true, nil
	}
	var commandErr *CommandError
	if errors.As(err, &commandErr) && commandErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("git ancestry: %w", err)
}

// ChangedPaths lists every path that differs between two commits, with
// renames disabled so a rename appears as its delete and its add. Symlinks
// and gitlinks appear at their own path.
func ChangedPaths(ctx context.Context, repoDir, from, to string) ([]string, error) {
	if validOID(from) != nil || validOID(to) != nil {
		return nil, errors.New("git changed paths: invalid object ID")
	}
	out, err := Hardened(ctx, repoDir, "diff", "--no-renames", "--name-only", "-z", from, to)
	if err != nil {
		return nil, fmt.Errorf("git changed paths: %w", err)
	}
	var paths []string
	for _, record := range strings.Split(string(out), "\x00") {
		if record != "" {
			paths = append(paths, record)
		}
	}
	return paths, nil
}

// HeadOID resolves the current commit of a worktree.
func HeadOID(ctx context.Context, dir string) (string, error) {
	out, err := Hardened(ctx, dir, "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("git head: %w", err)
	}
	oid := strings.TrimSpace(string(out))
	if validOID(oid) != nil {
		return "", errors.New("git head: invalid resolved object ID")
	}
	return strings.ToLower(oid), nil
}

// ReadTreeInto resets a worktree's index and files to exactly the given
// tree, removing files the tree lacks. Hostile in-tree attributes cannot
// execute anything: no filter driver is configured in the hardened
// environment, so attribute-defined filters fail closed.
func ReadTreeInto(ctx context.Context, worktreeDir, treeish string) error {
	if _, err := Hardened(ctx, worktreeDir, "read-tree", "--reset", "-u", treeish); err != nil {
		return fmt.Errorf("git read-tree: %w", err)
	}
	return nil
}

// DiffIndexQuiet reports whether the worktree's index matches HEAD (git's
// --quiet exit 0). false means the index carries changes to commit.
func DiffIndexQuiet(ctx context.Context, worktreeDir string) (bool, error) {
	err := HardenedExit(ctx, worktreeDir, "diff-index", "--quiet", "HEAD")
	if err == nil {
		return true, nil
	}
	var commandErr *CommandError
	if errors.As(err, &commandErr) && commandErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("git diff-index: %w", err)
}

// commitIdentity is the fixed integration identity; commit messages carry the
// brief's handoff, and the identity is control's own, never configured from
// repository or model input.
const commitIdentityName = "user.name=courier"
const commitIdentityEmail = "user.email=courier@invalid"

// CommitIndexed creates exactly one commit from the current index with the
// given message and returns its OID. The message travels through a private
// message file, never argv: it may carry bounded untrusted text, and no
// untrusted value reaches a git argument. Hooks are skipped and the
// identity is control's own.
func CommitIndexed(ctx context.Context, worktreeDir, message string) (string, error) {
	if strings.TrimSpace(message) == "" {
		return "", errors.New("git commit: message is required")
	}
	file, err := os.CreateTemp(filepath.Join(worktreeDir, ".git"), "COMMIT-MSG-")
	if err != nil {
		return "", fmt.Errorf("git commit: message file unavailable: %w", err)
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err := file.WriteString(message); err != nil {
		file.Close()
		return "", fmt.Errorf("git commit: message write failed: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("git commit: message write failed: %w", err)
	}
	if _, err := Hardened(ctx, worktreeDir,
		"-c", commitIdentityName, "-c", commitIdentityEmail,
		"commit", "--no-verify", "--file", path); err != nil {
		return "", fmt.Errorf("git commit: %w", err)
	}
	return HeadOID(ctx, worktreeDir)
}

// BundleCreate writes a self-contained bundle of the full closure of refs to
// destination. The bundle is broker- or control-authored: it is produced from
// a private tree, never from untrusted input.
func BundleCreate(ctx context.Context, repoDir, destination string, refs ...string) error {
	if len(refs) == 0 {
		return errors.New("git bundle create: at least one ref is required")
	}
	if err := validRefDestination(destination); err != nil {
		return err
	}
	args := append([]string{"bundle", "create", destination}, refs...)
	if _, err := Hardened(ctx, repoDir, args...); err != nil {
		return fmt.Errorf("git bundle create: %w", err)
	}
	return nil
}

// UpdateRef points ref at an object that already exists locally.
func UpdateRef(ctx context.Context, repoDir, ref, oid string) error {
	if validOID(oid) != nil {
		return errors.New("git update-ref: invalid object ID")
	}
	if _, err := Hardened(ctx, repoDir, "update-ref", "--no-deref", ref, oid); err != nil {
		return fmt.Errorf("git update-ref: %w", err)
	}
	return nil
}

// MergeCommit merges the given commit into the current branch. A merge that
// stops on conflicts is reported as MergeConflictError with the conflicted
// paths; every other failure fails closed.
func MergeCommit(ctx context.Context, worktreeDir, commit string) error {
	if validOID(commit) != nil {
		return errors.New("git merge: invalid object ID")
	}
	if _, err := Hardened(ctx, worktreeDir,
		"-c", commitIdentityName, "-c", commitIdentityEmail,
		"merge", "--no-edit", "--no-verify", commit); err == nil {
		return nil
	}
	// Classify by inspecting the index, never by stderr content: unmerged
	// paths are actionable work, anything else is a hard failure.
	out, listErr := Hardened(ctx, worktreeDir, "diff", "--name-only", "--diff-filter=U")
	if listErr != nil {
		return fmt.Errorf("git merge: %w", listErr)
	}
	var paths []string
	for _, record := range strings.Split(string(out), "\n") {
		if record = strings.TrimSpace(record); record != "" {
			paths = append(paths, record)
		}
	}
	if len(paths) > 0 {
		return &MergeConflictError{Base: commit, Paths: paths}
	}
	return errors.New("git merge: merge failed and left no classifiable state")
}

// AbortMerge discards an in-progress merge and restores the worktree and
// index to HEAD. It is the recovery for a re-sync merge that control will
// not retry: a stale MERGE_HEAD would silently turn the next integration
// commit into a bogus two-parent merge.
func AbortMerge(ctx context.Context, worktreeDir string) error {
	if _, err := Hardened(ctx, worktreeDir, "merge", "--abort"); err != nil {
		// A merge that never started (no MERGE_HEAD) aborts with an error;
		// that is success for the caller.
		if _, checkErr := Hardened(ctx, worktreeDir, "rev-parse", "--verify", "--quiet", "MERGE_HEAD"); checkErr != nil {
			return nil
		}
		return fmt.Errorf("git merge abort: %w", err)
	}
	return nil
}

// MergeInProgress reports whether a merge is still pending (MERGE_HEAD set).
func MergeInProgress(ctx context.Context, worktreeDir string) (bool, error) {
	_, err := Hardened(ctx, worktreeDir, "rev-parse", "--verify", "--quiet", "MERGE_HEAD")
	if err == nil {
		return true, nil
	}
	var commandErr *CommandError
	if errors.As(err, &commandErr) && commandErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("git merge in progress check: %w", err)
}

// FetchRef fetches one remote ref into an exact local ref from one pinned
// endpoint. Credentials travel through a private askpass helper, never argv
// or URLs.
func FetchRef(ctx context.Context, repoDir, endpoint, remoteRef, localRef, username, token string) error {
	if err := validateBrokerURL(endpoint); err != nil {
		return err
	}
	if remoteRef == "" || localRef == "" || strings.HasPrefix(remoteRef, "-") || strings.HasPrefix(localRef, "-") {
		return errors.New("git fetch: refs are required")
	}
	env := []string{}
	if username != "" || token != "" {
		if username == "" || token == "" || hasControl(username) || hasControl(token) {
			return errors.New("git fetch: invalid transport credentials")
		}
		askpass, err := writeAskpassHelper(filepath.Dir(repoDir))
		if err != nil || askpass == "" {
			return errors.New("git fetch: cannot initialize credential helper")
		}
		defer os.Remove(askpass)
		env = append(env,
			askpassEnvPrefix+askpass,
			sshAskpassPrefix+askpass,
			"COURIER_GIT_USERNAME="+username,
			"COURIER_GIT_TOKEN="+token,
		)
	}
	refspec := "+" + remoteRef + ":" + localRef
	if _, _, err := hardened(ctx, repoDir, env, []string{"fetch", "--no-tags", "--", endpoint, refspec}); err != nil {
		return fmt.Errorf("git fetch: %w", err)
	}
	return nil
}

// ResolveRef resolves one exact local ref to its full OID.
func ResolveRef(ctx context.Context, repoDir, ref string) (string, bool, error) {
	out, err := Hardened(ctx, repoDir, "rev-parse", "--verify", "--quiet", ref)
	if err != nil {
		var commandErr *CommandError
		if errors.As(err, &commandErr) && commandErr.ExitCode() == 1 {
			return "", false, nil
		}
		return "", false, fmt.Errorf("git resolve ref: %w", err)
	}
	oid := strings.TrimSpace(string(out))
	if validOID(oid) != nil {
		return "", false, errors.New("git resolve ref: invalid resolved object ID")
	}
	return strings.ToLower(oid), true, nil
}

// localBundlePath validates that a bundle path is an absolute regular
// non-symlink file, the same contract the broker's import applies.
func localBundlePath(bundlePath string) (string, error) {
	bundlePath = filepath.Clean(strings.TrimSpace(bundlePath))
	if bundlePath == "." || !filepath.IsAbs(bundlePath) || hasControl(bundlePath) {
		return "", errors.New("git bundle: path must be an absolute local path")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(bundlePath))
	if err != nil {
		return "", errors.New("git bundle: path is unavailable")
	}
	bundlePath = filepath.Join(parent, filepath.Base(bundlePath))
	info, err := os.Lstat(bundlePath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("git bundle: bundle must be a regular non-symlink file")
	}
	return bundlePath, nil
}

func validRefDestination(destination string) error {
	if destination == "" || !filepath.IsAbs(destination) || hasControl(destination) {
		return errors.New("git bundle create: destination must be an absolute local path")
	}
	return nil
}
