package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	couriergit "github.com/misospace/courier/internal/git"
)

// Snapshot ref names inside the bundle the broker renders for control's
// seed. The names are fixed so trusted control's import and the worker's
// unpack fetch exactly these refs and nothing else.
const (
	snapshotWorkRef = "refs/courier/snapshot/work"
	snapshotBaseRef = "refs/courier/snapshot/base"
)

// ErrSnapshotTipMoved reports that a pinned ref moved between the engine's
// observation and the git fetch that rendered the bundle. It is a race with
// the live remote, not a policy failure: re-observing and re-rendering can
// resolve it.
var ErrSnapshotTipMoved = errors.New("broker snapshot: pinned ref moved while rendering")

// SnapshotRequest is one engine-observed world state to render. The bundle
// always carries the base ref; it carries the work ref only when the work
// ref exists, with the snapshot work tip falling back to the base tip.
type SnapshotRequest struct {
	WorkRepo   string
	WorkRef    string
	WorkOID    string
	WorkExists bool
	BaseRepo   string
	BaseRef    string
	BaseOID    string
}

// Snapshotter renders the sanitized seed snapshot bundle for control. It is
// implemented by the broker's pinned git transport: every fetch goes to a
// trusted registration endpoint for a policy-pinned repository and ref.
type Snapshotter interface {
	Snapshot(ctx context.Context, req SnapshotRequest) ([]byte, error)
}

// MaxSnapshotBundleBytes bounds the rendered seed bundle. It matches the §5
// bundle bound: a repository whose history exceeds it cannot be dispatched
// and the run fails closed.
const MaxSnapshotBundleBytes = 64 << 20

// Snapshot fetches the pinned refs from their pinned endpoints into a
// scratch repository, verifies each fetched tip still equals the engine's
// observed OID, and renders a self-contained bundle with the fixed snapshot
// refs. Credentials travel through the private askpass helper, never argv.
func (t *gitTransport) Snapshot(ctx context.Context, req SnapshotRequest) ([]byte, error) {
	if t.git == nil || t.git.Directory() == "" {
		return nil, errors.New("broker snapshot: git repository is unavailable")
	}
	if req.BaseOID == "" || req.BaseRepo != t.policy.BaseRepo || req.BaseRef != t.policy.BaseRef {
		return nil, errors.New("broker snapshot: base identity does not match the pinned policy")
	}
	if req.WorkExists && (req.WorkOID == "" || req.WorkRepo != t.policy.WorkRepo || req.WorkRef != t.policy.WorkRef) {
		return nil, errors.New("broker snapshot: work identity does not match the pinned policy")
	}
	scratch, err := os.MkdirTemp(filepath.Dir(t.git.Directory()), "git-snapshot-")
	if err != nil {
		return nil, errors.New("broker snapshot: scratch repository unavailable")
	}
	defer os.RemoveAll(scratch)
	if _, err := couriergit.Hardened(ctx, "", "init", "--bare", "--quiet", scratch); err != nil {
		return nil, errors.New("broker snapshot: scratch repository init failed")
	}
	baseEndpoint, ok := t.endpoint(req.BaseRepo)
	if !ok {
		return nil, errors.New("broker snapshot: base repository is not pinned")
	}
	if err := t.fetchPinned(ctx, scratch, req.BaseRepo, baseEndpoint, "refs/heads/"+req.BaseRef, snapshotBaseRef); err != nil {
		return nil, err
	}
	fetchedBase, ok, err := resolveLocalRef(ctx, scratch, snapshotBaseRef)
	if err != nil || !ok {
		return nil, errors.New("broker snapshot: fetched base ref is unavailable")
	}
	if fetchedBase != req.BaseOID {
		return nil, ErrSnapshotTipMoved
	}
	if req.WorkExists {
		workEndpoint, ok := t.endpoint(req.WorkRepo)
		if !ok {
			return nil, errors.New("broker snapshot: work repository is not pinned")
		}
		if err := t.fetchPinned(ctx, scratch, req.WorkRepo, workEndpoint, "refs/heads/"+req.WorkRef, snapshotWorkRef); err != nil {
			return nil, err
		}
		fetchedWork, ok, err := resolveLocalRef(ctx, scratch, snapshotWorkRef)
		if err != nil || !ok {
			return nil, errors.New("broker snapshot: fetched work ref is unavailable")
		}
		if fetchedWork != req.WorkOID {
			return nil, ErrSnapshotTipMoved
		}
	} else {
		// An initially-absent work ref: the snapshot tip is the base tip,
		// resolved locally from the fetched objects — the engine-observed
		// OID is only the comparison above.
		localBase, ok, err := resolveLocalRef(ctx, scratch, snapshotBaseRef)
		if err != nil || !ok {
			return nil, errors.New("broker snapshot: fetched base ref is unavailable")
		}
		if err := couriergit.UpdateRef(ctx, scratch, snapshotWorkRef, localBase); err != nil {
			return nil, errors.New("broker snapshot: snapshot work ref unavailable")
		}
	}
	bundlePath := filepath.Join(scratch, "snapshot.bundle")
	if err := couriergit.BundleCreate(ctx, scratch, bundlePath, snapshotWorkRef, snapshotBaseRef); err != nil {
		return nil, errors.New("broker snapshot: bundle render failed")
	}
	info, err := os.Lstat(bundlePath)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("broker snapshot: bundle render produced no bundle")
	}
	if info.Size() > MaxSnapshotBundleBytes {
		return nil, errors.New("broker snapshot: repository history exceeds the bundle bound")
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, errors.New("broker snapshot: bundle read failed")
	}
	return data, nil
}

// fetchPinned fetches one pinned ref from one pinned endpoint with the
// broker's credentials for that repository.
func (t *gitTransport) fetchPinned(ctx context.Context, repo, repoIdentity, endpoint, remoteRef, localRef string) error {
	username, token := "", ""
	if t.credentials != nil {
		credentials, err := t.credentials.Credentials(ctx, repoIdentity)
		if err != nil {
			return errors.New("broker snapshot: credential provider failed")
		}
		username, token = credentials.Username, credentials.Token
	}
	if err := couriergit.FetchRef(ctx, repo, endpoint, remoteRef, localRef, username, token); err != nil {
		return errors.New("broker snapshot: pinned ref fetch failed")
	}
	return nil
}

func resolveLocalRef(ctx context.Context, repo, ref string) (string, bool, error) {
	out, err := couriergit.Hardened(ctx, repo, "rev-parse", "--verify", "--quiet", ref)
	if err != nil {
		var commandErr *couriergit.CommandError
		if errors.As(err, &commandErr) && commandErr.ExitCode() == 1 {
			return "", false, nil
		}
		return "", false, errors.New("broker snapshot: local ref resolve failed")
	}
	oid := strings.TrimSpace(string(out))
	if !couriergit.ValidOID(oid) {
		return "", false, errors.New("broker snapshot: resolved ref is not an object ID")
	}
	return oid, true, nil
}
