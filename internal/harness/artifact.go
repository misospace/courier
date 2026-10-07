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

// Worker artifact bounds (HARNESS.md §5): fixed numeric constants the
// implementation does not derive from model or worker input. A bundle over
// any bound is rejected whole, never truncated, and bounds apply to every
// object the bundle delivers — objects beyond the result ref's closure are
// ignored for integration but still count against the bounds.
const (
	// MaxArtifactBundleBytes bounds the bundle file itself. It matches the
	// worker protocol's artifact transport bound and the broker's import
	// transport bound.
	MaxArtifactBundleBytes = 64 << 20
	// MaxArtifactUnpackedBytes bounds the total unpacked size of every object
	// the bundle delivers.
	MaxArtifactUnpackedBytes = 64 << 20
	// MaxArtifactObjects bounds the object count the bundle delivers.
	MaxArtifactObjects = 100000
	// MaxArtifactBlobBytes bounds the size of any one blob.
	MaxArtifactBlobBytes = 16 << 20
)

// ResultRefPrefix is the namespace a worker bundle's single result ref must
// live in. The exact name is refs/courier/briefs/<briefID>, derived by
// trusted control from the brief ID — never from the worker.
const ResultRefPrefix = "refs/courier/briefs/"

// ArtifactRejectedError reports one fixed, safe rejection category. Worker-
// or bundle-supplied strings never appear in the category: the artifact is
// untrusted data, and its content must not enter trusted error strings that
// flow into tool results and termination reasons.
type ArtifactRejectedError struct {
	Category string
}

func (e *ArtifactRejectedError) Error() string {
	return "harness: artifact rejected: " + e.Category
}

// Fixed rejection categories.
const (
	rejectMalformed  = "bundle is malformed or not self-contained (prerequisite-bearing bundles are not accepted)"
	rejectBundleSize = "bundle exceeds the size bound and is rejected whole"
	rejectVerify     = "bundle verification failed"
	rejectRefSet     = "bundle ref set does not match the expected result ref"
	rejectObjects    = "bundle object-count bound exceeded and the bundle is rejected whole"
	rejectUnpacked   = "bundle unpacked-size bound exceeded and the bundle is rejected whole"
	rejectBlob       = "bundle blob-size bound exceeded and the bundle is rejected whole"
	rejectAncestry   = "artifact head does not descend from the dispatched snapshot tip"
	rejectPathScope  = "artifact changes paths outside the operator-resolved scope"
)

func rejected(category string) error {
	return &ArtifactRejectedError{Category: category}
}

// PathScope is the operator-resolved path policy for one run's artifacts
// (HARNESS.md §4-§5): when entries are present, the changed-path set of an
// artifact must lie entirely inside the scope. It is trusted control
// configuration — never brief- or worker-supplied — and nil means the whole
// repository is in scope. There are no default path filters.
type PathScope []string

// NewPathScope validates and normalizes operator-supplied scope entries.
// Entries are repository-root-relative paths; a directory entry covers every
// path beneath it.
func NewPathScope(entries []string) (PathScope, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	scope := make(PathScope, 0, len(entries))
	for _, entry := range entries {
		clean := filepath.Clean(strings.TrimSpace(entry))
		if clean == "" || clean == "." {
			continue
		}
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") || strings.ContainsRune(clean, 0) {
			return nil, fmt.Errorf("harness: invalid path scope entry %q", entry)
		}
		scope = append(scope, clean)
	}
	if len(scope) == 0 {
		return nil, nil
	}
	return scope, nil
}

// Contains reports whether one repository-root-relative changed path is in
// scope. A nil scope admits the whole repository.
func (s PathScope) Contains(path string) bool {
	if len(s) == 0 {
		return true
	}
	clean := filepath.Clean(path)
	if clean == ".." || strings.HasPrefix(clean, "../") || filepath.IsAbs(clean) {
		return false
	}
	for _, entry := range s {
		if clean == entry || strings.HasPrefix(clean, entry+"/") {
			return true
		}
	}
	return false
}

// resultRefFor derives the exact ref name a brief's bundle must advertise.
// The brief ID comes from model data, so the ref-syntax rules are enforced
// here in Go — nothing derived from the ID may reach a git argument.
func resultRefFor(briefID string) (string, error) {
	if !isBriefID(briefID) {
		return "", errors.New("harness: invalid brief ID")
	}
	// Within the brief ID alphabet the remaining git ref hazards are exactly
	// these; the ref name itself is control's fixed prefix.
	if strings.HasPrefix(briefID, ".") || strings.HasPrefix(briefID, "-") ||
		strings.HasSuffix(briefID, ".") || strings.HasSuffix(briefID, ".lock") ||
		strings.HasSuffix(briefID, "-") || strings.Contains(briefID, "..") ||
		strings.Contains(briefID, "@{") {
		return "", errors.New("harness: brief ID is not a valid result ref component")
	}
	return ResultRefPrefix + briefID, nil
}

// validateAndImportArtifact runs the §5 validation pipeline in order against
// control's private tree and imports the artifact head reflessly. The bundle
// is untrusted end to end: every rejection uses a fixed category, and
// nothing but bounded, hash-verified objects reach the private tree.
func validateAndImportArtifact(ctx context.Context, tree *Integrator, briefID, dispatchedTip string, bundle []byte) (string, error) {
	ref, err := resultRefFor(briefID)
	if err != nil {
		return "", err
	}
	if len(bundle) == 0 {
		return "", rejected(rejectMalformed)
	}
	if len(bundle) > MaxArtifactBundleBytes {
		return "", rejected(rejectBundleSize)
	}
	workDir, err := tree.scratchDir()
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(workDir)
	path := filepath.Join(workDir, "artifact.bundle")
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		return "", fmt.Errorf("harness: stage artifact: %w", err)
	}
	// 1. git bundle verify against control's private tree: every
	// prerequisite present, object closure intact under git's own hash
	// checks.
	if err := couriergit.VerifyBundle(ctx, tree.Dir, path); err != nil {
		return "", rejected(rejectVerify)
	}
	// 2. Bounds, in a quarantine object directory: every object the bundle
	// delivers is unpacked there and counted, so off-closure objects cannot
	// evade the bounds and nothing hostile reaches the private tree yet.
	objectsDir := filepath.Join(workDir, "objects")
	if err := os.MkdirAll(objectsDir, 0o700); err != nil {
		return "", fmt.Errorf("harness: quarantine unavailable: %w", err)
	}
	if err := couriergit.Unbundle(ctx, tree.Dir, objectsDir, path); err != nil {
		return "", rejected(rejectMalformed)
	}
	inventory, err := couriergit.InventoryObjects(ctx, tree.Dir, objectsDir)
	if err != nil {
		return "", rejected(rejectMalformed)
	}
	if inventory.Count > MaxArtifactObjects {
		return "", rejected(rejectObjects)
	}
	if inventory.TotalBytes > MaxArtifactUnpackedBytes {
		return "", rejected(rejectUnpacked)
	}
	if inventory.MaxBlobBytes > MaxArtifactBlobBytes {
		return "", rejected(rejectBlob)
	}
	// 3. Ref-set equality: the bundle advertises exactly the expected result
	// ref — no extra refs, no unexpected names, nothing outside the
	// refs/courier/ namespace. The advertised OID is used only for this
	// comparison: the object to import is resolved locally after the fetch.
	heads, err := couriergit.BundleHeads(ctx, path)
	if err != nil {
		return "", rejected(rejectMalformed)
	}
	if len(heads) != 1 || heads[0].Name != ref {
		return "", rejected(rejectRefSet)
	}
	// 4. Refless import: fetch exactly the expected ref — a control-derived
	// name — so only FETCH_HEAD records it and no bundle-declared ref is
	// applied to the tree; then resolve the head locally from the fetched
	// objects. Bundle- or worker-derived strings never reach a git argument.
	if err := couriergit.FetchBundleIntoFetchHead(ctx, tree.Dir, path, ref); err != nil {
		return "", rejected(rejectMalformed)
	}
	head, err := couriergit.ResolveCommit(ctx, tree.Dir, "FETCH_HEAD^{commit}")
	if err != nil {
		return "", rejected(rejectMalformed)
	}
	// 5. Ancestry: the result head reaches the exact tip control dispatched.
	ancestor, err := couriergit.IsAncestor(ctx, tree.Dir, dispatchedTip, head)
	if err != nil {
		return "", rejected(rejectMalformed)
	}
	if !ancestor {
		return "", rejected(rejectAncestry)
	}
	return head, nil
}
