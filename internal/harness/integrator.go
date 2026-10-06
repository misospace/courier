package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	couriergit "github.com/misospace/courier/internal/git"
)

// Snapshot ref names inside broker- and control-authored snapshot bundles.
// These are trusted artifacts: the broker renders the seed snapshot from the
// pinned refs it observed, and control renders per-brief snapshots from its
// own integration tree. Worker bundles never use these names.
const (
	snapshotWorkRef = "refs/courier/snapshot/work"
	snapshotBaseRef = "refs/courier/snapshot/base"
	// integrationBranch is the private tree's local branch. It never leaves
	// the control pod.
	integrationBranch = "refs/heads/integration"
)

// maxCommitMessageBytes bounds the untrusted summary that may ride into an
// integration commit message. It is a transcript-resource bound, not a work
// limit.
const maxCommitMessageBytes = 2048

// IntegrationRequest carries one completed brief's untrusted bundle to
// integration. DispatchedTip is the exact snapshot tip control dispatched
// for this brief — recorded at prepare time from trusted state, never from
// the worker.
type IntegrationRequest struct {
	BriefID       string
	DispatchedTip string
	Bundle        []byte
	Brief         Brief
	// Summary is the untrusted delegate result summary. It is bounded data
	// for the commit message only and is never validation input.
	Summary string
}

// Integration is the outcome of one integration. Commit is empty when the
// artifact carried no changes beyond the dispatched tip.
type Integration struct {
	Commit  string
	Changed bool
}

// Integrator owns the control pod's private integration tree: the trusted
// git worktree briefs are integrated into, one local commit per completed
// brief, seeded from the broker's snapshot and never shared with the worker.
type Integrator struct {
	Dir   string
	scope PathScope

	// baseTip is the seeded snapshot's base tip, refreshed on re-sync. It is
	// in-memory control state for one incarnation.
	baseTip string
}

// NewIntegrator prepares the integration tree container. The scope is the
// operator-resolved path policy (nil admits the whole repository).
func NewIntegrator(dir string, scope PathScope) *Integrator {
	return &Integrator{Dir: dir, scope: scope}
}

// BaseTip returns the seeded base tip.
func (t *Integrator) BaseTip() string { return t.baseTip }

// scratchDir creates a private staging directory beside the tree for bundle
// validation work.
func (t *Integrator) scratchDir() (string, error) {
	parent := filepath.Dir(t.Dir)
	dir, err := os.MkdirTemp(parent, "harness-artifact-")
	if err != nil {
		return "", fmt.Errorf("harness: artifact staging unavailable: %w", err)
	}
	return dir, nil
}

// Seed initializes or resets the private tree from the broker's snapshot
// bundle and returns the snapshot tip it checked out. The snapshot is a
// trusted artifact, but its shape is still validated: exactly the two
// snapshot refs, and nothing else.
func (t *Integrator) Seed(ctx context.Context, bundle []byte) (string, error) {
	if len(bundle) == 0 {
		return "", errors.New("harness: seed snapshot is empty")
	}
	if len(bundle) > MaxArtifactBundleBytes {
		return "", errors.New("harness: seed snapshot exceeds the bundle bound")
	}
	if err := os.MkdirAll(t.Dir, 0o700); err != nil {
		return "", fmt.Errorf("harness: integration tree unavailable: %w", err)
	}
	if _, err := couriergit.Hardened(ctx, "", "init", "--quiet", "--initial-branch", "integration", t.Dir); err != nil {
		return "", fmt.Errorf("harness: integration tree init failed: %w", err)
	}
	workDir, err := t.scratchDir()
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(workDir)
	path := filepath.Join(workDir, "snapshot.bundle")
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		return "", fmt.Errorf("harness: stage snapshot: %w", err)
	}
	heads, err := couriergit.BundleHeads(ctx, path)
	if err != nil {
		return "", fmt.Errorf("harness: seed snapshot is malformed: %w", err)
	}
	// The advertised ref set is checked against fixed names; the advertised
	// OIDs are used only for this comparison — the tips are resolved locally
	// from the fetched objects.
	tips := map[string]string{}
	for _, head := range heads {
		tips[head.Name] = head.OID
	}
	_, hasWork := tips[snapshotWorkRef]
	_, hasBase := tips[snapshotBaseRef]
	// Broker-rendered snapshots carry exactly the two snapshot refs; locally
	// rendered per-brief snapshots carry exactly the work ref (the base tip
	// is control's own state). Both shapes are accepted, nothing else —
	// extra or duplicate advertised refs are rejected.
	switch {
	case hasWork && hasBase && len(tips) == 2:
	case hasWork && len(tips) == 1:
	default:
		return "", errors.New("harness: seed snapshot does not carry the expected snapshot refs")
	}
	// Git refuses a fetch into the checked-out branch, so the snapshot refs
	// land on their mirror refs first and the integration branch is set
	// from the locally resolved work tip.
	if err := couriergit.FetchBundleRef(ctx, t.Dir, path, snapshotWorkRef, snapshotWorkRef); err != nil {
		return "", fmt.Errorf("harness: seed snapshot import failed: %w", err)
	}
	if hasBase {
		if err := couriergit.FetchBundleRef(ctx, t.Dir, path, snapshotBaseRef, snapshotBaseRef); err != nil {
			return "", fmt.Errorf("harness: seed snapshot base import failed: %w", err)
		}
	}
	workTip, err := couriergit.ResolveCommit(ctx, t.Dir, snapshotWorkRef)
	if err != nil {
		return "", fmt.Errorf("harness: seeded work tip is unavailable: %w", err)
	}
	baseTip := t.baseTip
	if hasBase {
		if baseTip, err = couriergit.ResolveCommit(ctx, t.Dir, snapshotBaseRef); err != nil {
			return "", fmt.Errorf("harness: seeded base tip is unavailable: %w", err)
		}
	}
	if baseTip == "" {
		return "", errors.New("harness: seed snapshot carries no base tip")
	}
	if err := couriergit.UpdateRef(ctx, t.Dir, integrationBranch, workTip); err != nil {
		return "", fmt.Errorf("harness: integration branch set failed: %w", err)
	}
	// HEAD stays on the integration branch; the worktree materializes at the
	// seeded tip.
	if _, err := couriergit.Hardened(ctx, t.Dir, "symbolic-ref", "HEAD", integrationBranch); err != nil {
		return "", fmt.Errorf("harness: integration branch select failed: %w", err)
	}
	if _, err := couriergit.Hardened(ctx, t.Dir, "reset", "--quiet", "--hard", integrationBranch); err != nil {
		return "", fmt.Errorf("harness: integration tree materialize failed: %w", err)
	}
	t.baseTip = baseTip
	return workTip, nil
}

// Head resolves the current integration commit.
func (t *Integrator) Head(ctx context.Context) (string, error) {
	return couriergit.HeadOID(ctx, t.Dir)
}

// Render produces the sanitized snapshot bundle for the next dispatch from
// the current integration head. The snapshot is control-authored: its single
// ref is the fixed snapshot work ref the worker's unpack fetches.
func (t *Integrator) Render(ctx context.Context) (Snapshot, error) {
	head, err := t.Head(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("harness: integration head unavailable: %w", err)
	}
	if err := couriergit.UpdateRef(ctx, t.Dir, snapshotWorkRef, head); err != nil {
		return Snapshot{}, err
	}
	workDir, err := t.scratchDir()
	if err != nil {
		return Snapshot{}, err
	}
	defer os.RemoveAll(workDir)
	path := filepath.Join(workDir, "snapshot.bundle")
	if err := couriergit.BundleCreate(ctx, t.Dir, path, snapshotWorkRef); err != nil {
		return Snapshot{}, fmt.Errorf("harness: snapshot render failed: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return Snapshot{}, errors.New("harness: snapshot render produced no bundle")
	}
	if info.Size() > MaxArtifactBundleBytes {
		return Snapshot{}, errors.New("harness: repository snapshot exceeds the bundle bound and cannot be dispatched")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, fmt.Errorf("harness: snapshot read failed: %w", err)
	}
	return Snapshot{Data: data, Tip: head, BaseTip: t.baseTip}, nil
}

// RefreshBase imports a fresh base tip (and its objects) from a newly
// observed broker snapshot bundle, for the §4 base-advance re-sync. The
// returned tip is the observed base tip.
func (t *Integrator) RefreshBase(ctx context.Context, bundle []byte) (string, error) {
	workDir, err := t.scratchDir()
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(workDir)
	path := filepath.Join(workDir, "snapshot.bundle")
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		return "", fmt.Errorf("harness: stage snapshot: %w", err)
	}
	heads, err := couriergit.BundleHeads(ctx, path)
	if err != nil {
		return "", errors.New("harness: refresh snapshot is malformed")
	}
	hasBase := false
	for _, head := range heads {
		if head.Name == snapshotBaseRef {
			hasBase = true
		}
	}
	if !hasBase {
		return "", errors.New("harness: refresh snapshot carries no base ref")
	}
	if err := couriergit.FetchBundleRef(ctx, t.Dir, path, snapshotBaseRef, snapshotBaseRef); err != nil {
		return "", fmt.Errorf("harness: base refresh import failed: %w", err)
	}
	baseTip, err := couriergit.ResolveCommit(ctx, t.Dir, snapshotBaseRef)
	if err != nil {
		return "", fmt.Errorf("harness: refreshed base tip is unavailable: %w", err)
	}
	t.baseTip = baseTip
	return baseTip, nil
}

// AbortMerge discards an in-progress re-sync merge, restoring the tree to
// HEAD. See git.AbortMerge for why the merge state must never outlive a
// refused re-sync.
func (t *Integrator) AbortMerge(ctx context.Context) error {
	return couriergit.AbortMerge(ctx, t.Dir)
}

// Absorb imports a fresh snapshot bundle's snapshot refs into the private
// tree's mirror refs and returns the locally resolved tips, so world
// classification runs against objects git verified rather than OIDs the
// broker asserted. The caller compares them against the claimed tips.
func (t *Integrator) Absorb(ctx context.Context, bundle []byte) (workTip, baseTip string, err error) {
	workDir, err := t.scratchDir()
	if err != nil {
		return "", "", err
	}
	defer os.RemoveAll(workDir)
	path := filepath.Join(workDir, "world.bundle")
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		return "", "", fmt.Errorf("harness: stage world snapshot: %w", err)
	}
	if err := couriergit.FetchBundleRef(ctx, t.Dir, path, snapshotWorkRef, snapshotWorkRef); err != nil {
		return "", "", fmt.Errorf("harness: world snapshot import failed: %w", err)
	}
	if err := couriergit.FetchBundleRef(ctx, t.Dir, path, snapshotBaseRef, snapshotBaseRef); err != nil {
		return "", "", fmt.Errorf("harness: world snapshot base import failed: %w", err)
	}
	if workTip, err = couriergit.ResolveCommit(ctx, t.Dir, snapshotWorkRef); err != nil {
		return "", "", fmt.Errorf("harness: world snapshot work tip is unavailable: %w", err)
	}
	if baseTip, err = couriergit.ResolveCommit(ctx, t.Dir, snapshotBaseRef); err != nil {
		return "", "", fmt.Errorf("harness: world snapshot base tip is unavailable: %w", err)
	}
	return workTip, baseTip, nil
}

// MergeBase merges the re-synced base tip into the integration branch, the
// §4 recovery for a base that advanced while the work ref stayed put. A
// conflicted merge is actionable work, not infrastructure failure: it is
// reported as *couriergit.MergeConflictError and leaves the merge in
// progress.
func (t *Integrator) MergeBase(ctx context.Context, baseTip string) error {
	return couriergit.MergeCommit(ctx, t.Dir, baseTip)
}

// Integrate validates a completed brief's untrusted bundle against the tip
// control dispatched, applies the operator-resolved path policy, materializes
// the validated tree, and commits exactly one integration commit. An empty
// diff integrates nothing and is not an error.
func (t *Integrator) Integrate(ctx context.Context, req IntegrationRequest) (Integration, error) {
	if !couriergit.ValidOID(req.DispatchedTip) {
		return Integration{}, errors.New("harness: dispatched snapshot tip is not an object ID")
	}
	// A merge left in progress by a refused re-sync would silently turn this
	// commit into a bogus two-parent merge; refuse until it is resolved or
	// aborted.
	if inProgress, err := couriergit.MergeInProgress(ctx, t.Dir); err != nil {
		return Integration{}, fmt.Errorf("harness: integration state check failed: %w", err)
	} else if inProgress {
		return Integration{}, errors.New("harness: a base re-sync merge is still in progress in the integration tree")
	}
	head, err := validateAndImportArtifact(ctx, t, req.BriefID, req.DispatchedTip, req.Bundle)
	if err != nil {
		return Integration{}, err
	}
	// Path policy (§5 step 5): the changed-path set of the result commit
	// against the dispatched tip must lie inside the operator-resolved
	// scope. Symlinks count at their own path; gitlinks are inert tree data
	// whose changed path counts against the scope.
	paths, err := couriergit.ChangedPaths(ctx, t.Dir, req.DispatchedTip, head)
	if err != nil {
		return Integration{}, rejected(rejectMalformed)
	}
	for _, path := range paths {
		if !t.scope.Contains(path) {
			return Integration{}, rejected(rejectPathScope)
		}
	}
	// Materialize the validated tree over the private worktree. Files the
	// artifact deleted are removed; artifact-supplied symlinks materialize
	// as symlink entries and nothing in trusted control reads through them.
	if err := couriergit.ReadTreeInto(ctx, t.Dir, head); err != nil {
		return Integration{}, fmt.Errorf("harness: integration materialize failed: %w", err)
	}
	quiet, err := couriergit.DiffIndexQuiet(ctx, t.Dir)
	if err != nil {
		return Integration{}, fmt.Errorf("harness: integration diff failed: %w", err)
	}
	if quiet {
		return Integration{Changed: false}, nil
	}
	message, err := integrationMessage(req)
	if err != nil {
		return Integration{}, err
	}
	commit, err := couriergit.CommitIndexed(ctx, t.Dir, message)
	if err != nil {
		return Integration{}, fmt.Errorf("harness: integration commit failed: %w", err)
	}
	return Integration{Commit: commit, Changed: true}, nil
}

// integrationMessage composes the one-commit-per-brief message from trusted
// brief fields plus the bounded untrusted summary. The untrusted summary is
// data in the message; it is never validation input.
func integrationMessage(req IntegrationRequest) (string, error) {
	summary := sanitizeMessage(req.Summary)
	if summary == "" {
		summary = sanitizeMessage(req.Brief.Objective)
	}
	if summary == "" {
		summary = "completed brief " + req.BriefID
	}
	message, err := couriergit.CommitMessage(couriergit.Brief{
		ID:        req.BriefID,
		Summary:   summary,
		Objective: sanitizeMessage(req.Brief.Objective),
	})
	if err != nil {
		return "", fmt.Errorf("harness: integration message: %w", err)
	}
	return message, nil
}

// sanitizeMessage bounds untrusted text for a commit message and strips
// control characters except newline and tab.
func sanitizeMessage(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > maxCommitMessageBytes {
		value = value[:maxCommitMessageBytes]
	}
	var b strings.Builder
	for _, r := range value {
		if r == '\n' || r == '\t' || (r >= 0x20 && r != 0x7f) {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

// PublishBundle renders the self-contained publication bundle for the
// current integration head: the closure the broker imports before its own
// policy enforcement.
func (t *Integrator) PublishBundle(ctx context.Context) (string, []byte, error) {
	head, err := t.Head(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("harness: integration head unavailable: %w", err)
	}
	workDir, err := t.scratchDir()
	if err != nil {
		return "", nil, err
	}
	defer os.RemoveAll(workDir)
	path := filepath.Join(workDir, "publish.bundle")
	if err := couriergit.UpdateRef(ctx, t.Dir, snapshotWorkRef, head); err != nil {
		return "", nil, err
	}
	if err := couriergit.BundleCreate(ctx, t.Dir, path, snapshotWorkRef); err != nil {
		return "", nil, fmt.Errorf("harness: publication bundle render failed: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", nil, errors.New("harness: publication bundle render produced no bundle")
	}
	if info.Size() > MaxArtifactBundleBytes {
		return "", nil, errors.New("harness: publication bundle exceeds the broker import bound")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", nil, fmt.Errorf("harness: publication bundle read failed: %w", err)
	}
	return head, data, nil
}
