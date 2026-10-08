// Package evidence captures a run's worktree state as a redacted manifest plus
// a tar of admitted content. It is pure: no transport, no executor wiring, no
// Kubernetes.
package evidence

import (
	"encoding/json"
	"time"
)

// Bound constants governing what a capture may admit. They are shared with the
// pod builder and intake so every component agrees on the same envelope.
const (
	// SchemaVersion is the manifest schema version emitted by this package.
	SchemaVersion = 1
	// MaxTotalBytes bounds total uncompressed admitted content.
	MaxTotalBytes = 512 * 1024
	// MaxFileBytes bounds per-file admitted content.
	MaxFileBytes = 64 * 1024
	// MaxContentEntries bounds admitted (stored) content entries.
	MaxContentEntries = 256
	// MaxManifestEntries bounds manifest entry records.
	MaxManifestEntries = 2000
	// ManifestBudgetBytes bounds the serialized manifest byte size.
	ManifestBudgetBytes = 128 * 1024
	// MaxPathDisplayBytes caps path/linkTarget display in the manifest.
	MaxPathDisplayBytes = 1024
	// BinarySniffBytes is the NUL-scan window; text == no NUL in window AND
	// valid UTF-8.
	BinarySniffBytes = 8 * 1024
)

// Entry Class values: what the thing is, derived from porcelain XY, lstat, and
// commit discovery.
const (
	// ClassModified is a tracked file whose content changed (porcelain M).
	ClassModified = "modified"
	// ClassAdded is a newly added tracked file (A).
	ClassAdded = "added"
	// ClassDeleted is a tracked deletion (D); content stays in git history.
	ClassDeleted = "deleted"
	// ClassRenamed is a tracked rename (R); the origin path is recorded, not
	// followed.
	ClassRenamed = "renamed"
	// ClassCopied is a tracked copy (C).
	ClassCopied = "copied"
	// ClassUntracked is an untracked file (??).
	ClassUntracked = "untracked"
	// ClassCommit is a local commit not on the run branch (format-patch).
	ClassCommit = "commit"
)

// Entry Disposition values: what the capture did with the entry, one per
// entry.
const (
	// DispositionStored means content was admitted into the tar.
	DispositionStored = "stored"
	// DispositionWithheld means a scan matched the content, so it is NOT in
	// the tar; manifest-only.
	DispositionWithheld = "withheld"
	// DispositionOmittedBinary means the content is unscannable and never
	// stored.
	DispositionOmittedBinary = "omitted-binary"
	// DispositionOmittedOverLimit means the content is over a cap and never
	// stored.
	DispositionOmittedOverLimit = "omitted-over-limit"
	// DispositionOmittedBudget marks the synthetic single collapse entry used
	// when the manifest budget is hit.
	DispositionOmittedBudget = "omitted-manifest-budget"
	// DispositionSymlink means path+target were recorded; a symlink is NEVER a
	// tar member.
	DispositionSymlink = "symlink"
	// DispositionDeleted is a tracked deletion with no content stored.
	DispositionDeleted = "deleted"
)

// Trigger values: the capture moment.
const (
	// TriggerContinuation is a capture at a continuation point.
	TriggerContinuation = "continuation"
	// TriggerCrash is a capture after a crash.
	TriggerCrash = "crash"
	// TriggerTerminal is a capture at a terminal run state.
	TriggerTerminal = "terminal"
	// TriggerCancellation is a capture on cancellation.
	TriggerCancellation = "cancellation"
)

// RunIdentity identifies the run and pod that produced a capture.
type RunIdentity struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	RunUID    string `json:"runUID"`
	PodUID    string `json:"podUID"`
}

// WorkspaceIdentity identifies the repository state a capture was taken from.
type WorkspaceIdentity struct {
	BaseRepo string `json:"baseRepo"`
	HeadRepo string `json:"headRepo,omitempty"`
	Branch   string `json:"branch"`
	StartSHA string `json:"startSHA"`
	HeadSHA  string `json:"headSHA"`
}

// Entry is one manifest record describing a discovered change. Content is
// present only when Disposition is stored.
type Entry struct {
	Path        string `json:"path"`
	Class       string `json:"class"`
	Disposition string `json:"disposition"`
	LinkTarget  string `json:"linkTarget,omitempty"`
	Escaping    bool   `json:"escaping,omitempty"`
	CommitSHA   string `json:"commitSHA,omitempty"`
	StoredPath  string `json:"storedPath,omitempty"`
	// Bytes records the underlying content size for stored, omitted-binary,
	// and omitted-over-limit entries; it is 0 for deleted, symlink, and
	// withheld entries. For a local-commit entry capped during capture, it is
	// the bounded prefix (up to MaxFileBytes+1) read before the cap was hit
	// (the underlying patch is at least that large), not the full patch size.
	Bytes int64 `json:"bytes,omitempty"`
}

// Totals counts what was discovered in the world. Totals are authoritative:
// they are independent of manifest-entry collapsing (budget) or withholding. A
// budget collapse does NOT lower Files/Commits; withholding does NOT lower
// Files, it only moves an entry's Disposition to withheld.
type Totals struct {
	Files            int   `json:"files"`
	Commits          int   `json:"commits"`
	Stored           int   `json:"stored"`
	Withheld         int   `json:"withheld"`
	OmittedBinary    int   `json:"omittedBinary"`
	OmittedOverLimit int   `json:"omittedOverLimit"`
	OmittedByBudget  int   `json:"omittedByBudget"`
	Deleted          int   `json:"deleted"`
	Symlinks         int   `json:"symlinks"`
	StoredBytes      int64 `json:"storedBytes"`
}

// Manifest is the normative cross-component capture contract: what was
// discovered, what was admitted, and where it is stored.
type Manifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	Run           RunIdentity       `json:"run"`
	Workspace     WorkspaceIdentity `json:"workspace"`
	CapturedAt    time.Time         `json:"capturedAt"`
	Trigger       string            `json:"trigger"`
	Entries       []Entry           `json:"entries"`
	Totals        Totals            `json:"totals"`
}

// MarshalCanonical serializes the manifest as stable JSON. Field order follows
// struct order, so the result is deterministic for a given manifest. It is
// used by the manifest budget check.
func (m *Manifest) MarshalCanonical() ([]byte, error) {
	return json.Marshal(m)
}
