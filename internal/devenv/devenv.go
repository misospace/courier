// Package devenv implements the runtime side of the repository-owned
// development bootstrap contract (#414): detecting the contract files in a
// checked-out workspace, classifying them against the operator-approved base
// revision, producing the fixed task scripts the worker executes, and
// capturing the deterministic activated environment as a delta the caller
// applies over its own base environment.
//
// The package is model- and forge-agnostic and imports nothing from the repo:
// the contract is a per-repo convention (`.mise/config.toml` plus a committed
// `.mise/mise.lock`, settled in #416), and the runtime only ever sees bytes
// and revision-scoped hardened reads supplied by the caller.
package devenv

import (
	"context"
	"strings"
	"time"
)

// Repository-owned bootstrap contract paths, relative to the workspace root.
// This is the convention settled for #414 and implemented per-repo in #416.
const (
	ConfigPath = ".mise/config.toml" // repository-owned contract file
	LockPath   = ".mise/mise.lock"   // committed tool lock data
)

// DefaultBound is the liveness bound for one bootstrap or one
// environment-capture command (#414): a stuck command produces a structured
// failure, never an unbounded wait. It bounds stuckness, never a run's
// duration; a caller that wants its own value wraps this one in a var.
const DefaultBound = 15 * time.Minute

// State classifies the contract trust decision the runtime makes from the
// checked-out files against the operator-approved base revision.
type State string

const (
	// StateAbsent means no contract is checked out: repositories without a
	// contract are skipped, never failed.
	StateAbsent State = "absent"
	// StateApproved means the checked-out contract is byte-identical to the
	// operator-approved base revision.
	StateApproved State = "approved"
	// StateModified means the checked-out contract differs from the base:
	// an untrusted modification the runtime skips, never executes.
	StateModified State = "modified"
)

// Files carries the contract file contents at one revision. Config and Lock
// are nil when absent; HasConfig and HasLock report presence separately, so
// a read error is never folded into absence — callers handle read errors
// themselves and must not call Compare in that case. A contract needs both
// files: Compare approves a config without a lock at neither revision.
// ConfigRegular and LockRegular report a git regular blob at that revision
// (filemode 100644 or 100755): an executable contract path that is a symlink
// or any other non-regular entry is unprovable and must fail closed.
type Files struct {
	Config        []byte
	Lock          []byte
	HasConfig     bool
	HasLock       bool
	ConfigRegular bool
	LockRegular   bool
}

// Compare classifies the checked-out contract (head) against the
// operator-approved base revision's contract, failing closed on everything
// it cannot prove. An absent head is StateAbsent, which covers repositories
// without a contract. A base that is absent while the head is present is
// StateModified. The committed lockfile is part of the contract, not an
// optional companion: a config whose lock is absent or unprovable at either
// revision is StateModified, because without it the tool versions a
// bootstrap installs are unpinned. Otherwise every compared contract path
// must be a regular blob at both revisions (a symlinked or otherwise
// non-regular contract path is an untrusted modification), and the config
// and lock bytes must be byte-identical. The comparison is the
// APPROVAL GATE only: it decides whether the contract may run at all. What
// actually executes is never the head's bytes — the worker materializes the
// approved base revision from the git object store and runs there (see
// BootstrapScript) — so no enumeration of what mise might discover can
// drift between the compared bytes and the executed ones. Anything
// unprovable is StateModified: the runtime never executes a contract it did
// not approve.
func Compare(base, head Files) State {
	if !head.HasConfig {
		return StateAbsent
	}
	if !base.HasConfig {
		return StateModified
	}
	if !base.ConfigRegular || !head.ConfigRegular {
		return StateModified
	}
	// The contract is the config plus a COMMITTED lockfile (#414): the lock
	// is what pins the tool versions the bootstrap installs, so a contract
	// without one at either revision cannot be pinned and is never approved.
	// Absence on both sides is not a repo "without a contract" — that is the
	// absent config above — it is a half-present contract, and the only safe
	// reading of it is an unapproved one.
	if !base.HasLock || !head.HasLock {
		return StateModified
	}
	if !base.LockRegular || !head.LockRegular {
		return StateModified
	}
	if string(base.Config) != string(head.Config) || string(base.Lock) != string(head.Lock) {
		return StateModified
	}
	return StateApproved
}

// BlobRead supplies one revision-scoped, hardened read of a repository
// path: its bytes, whether the entry is a regular blob (not a symlink or
// gitlink), and whether it is present at all. A read error is real:
// callers must never fold it into absence.
type BlobRead func(ctx context.Context, revision, path string) (data []byte, regular bool, present bool, err error)

// Classify reads the contract at both revisions through read and returns
// Compare's verdict. It is the one orchestration both call sites (the
// harness integrator and the legacy executor) share, so the trust decision
// cannot drift between them: config and lock are read at head and base, and
// any read error is returned as an error — never folded into absence, never
// a verdict of its own. Callers treat an error as an unreadable contract
// and skip, never execute.
func Classify(ctx context.Context, read BlobRead, baseRev, headRev string) (State, error) {
	base, err := readContractFiles(ctx, read, baseRev)
	if err != nil {
		return "", err
	}
	head, err := readContractFiles(ctx, read, headRev)
	if err != nil {
		return "", err
	}
	return Compare(base, head), nil
}

// readContractFiles reads the two contract paths at one revision through
// read, mapping presence and regularity into Files without ever folding a
// read error into absence.
func readContractFiles(ctx context.Context, read BlobRead, revision string) (Files, error) {
	config, configRegular, hasConfig, err := read(ctx, revision, ConfigPath)
	if err != nil {
		return Files{}, err
	}
	lock, lockRegular, hasLock, err := read(ctx, revision, LockPath)
	if err != nil {
		return Files{}, err
	}
	return Files{
		Config:        config,
		Lock:          lock,
		HasConfig:     hasConfig,
		HasLock:       hasLock,
		ConfigRegular: configRegular,
		LockRegular:   lockRegular,
	}, nil
}

// ProtectedKeys reports whether an environment key must never be set or
// overridden by a bootstrap delta: Courier's own control plane (COURIER_*),
// git identity and credential plumbing (GIT_*), and dynamic-loader injection
// (LD_*, DYLD_*, plus the shell-splitting IFS and SHELL). PATH is deliberately
// NOT protected: deterministic PATH replacement is the whole point of the
// contract's activated environment.
func ProtectedKeys(name string) bool {
	switch name {
	case "IFS", "SHELL":
		return true
	}
	for _, prefix := range []string{"GIT_", "COURIER_", "LD_", "DYLD_"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// MergeEnv applies delta (KEY=VALUE pairs) over base, dropping protected keys,
// with last-write-wins on repeated names, preserving base order and appending
// new keys in delta order. The worker and the legacy executor both use it so
// the two wiring sites cannot drift.
func MergeEnv(base, delta []string) []string {
	applied := make(map[string]string)
	order := make([]string, 0, len(delta))
	for _, kv := range delta {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || ProtectedKeys(name) {
			continue
		}
		if _, seen := applied[name]; !seen {
			order = append(order, name)
		}
		applied[name] = value
	}
	// Base keys resolve by their last occurrence: exec.Cmd gives the last
	// entry for a duplicated name, so replacing only the first would let a
	// stale duplicate override the delta's value.
	lastIndex := make(map[string]int, len(base))
	for i, kv := range base {
		if name, _, ok := strings.Cut(kv, "="); ok {
			lastIndex[name] = i
		}
	}
	merged := make([]string, 0, len(base)+len(applied))
	emitted := make(map[string]bool, len(base)+len(applied))
	for i, kv := range base {
		name, _, ok := strings.Cut(kv, "=")
		if ok {
			if lastIndex[name] != i || emitted[name] {
				continue
			}
			if value, replaced := applied[name]; replaced {
				merged = append(merged, name+"="+value)
			} else {
				merged = append(merged, kv)
			}
			emitted[name] = true
			continue
		}
		merged = append(merged, kv)
	}
	for _, name := range order {
		if !emitted[name] {
			merged = append(merged, name+"="+applied[name])
			emitted[name] = true
		}
	}
	return merged
}
