// Package devenv implements the runtime side of the repository-owned
// development bootstrap contract (#414): detecting the contract files in a
// checked-out workspace, classifying them against the operator-approved base
// revision, executing the approved `mise run bootstrap` under a liveness
// bound, and capturing the deterministic activated environment as a delta
// the caller applies over its own base environment.
//
// The package is model- and forge-agnostic and imports nothing from the repo:
// the contract is a per-repo convention (`.mise/config.toml` plus a committed
// `.mise/mise.lock`, settled in #416), and the runtime only ever sees bytes
// and a mise executable path supplied by the caller.
package devenv

import (
	"strings"
)

// Repository-owned bootstrap contract paths, relative to the workspace root.
// This is the convention settled for #414 and implemented per-repo in #416.
const (
	ConfigPath = ".mise/config.toml" // repository-owned contract file
	LockPath   = ".mise/mise.lock"   // committed tool lock data
)

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
// are nil when absent; HasConfig and HasLock report presence separately, so a
// read error is never folded into absence — callers handle read errors
// themselves and must not call Digest or Compare in that case.
type Files struct {
	Config    []byte
	Lock      []byte
	HasConfig bool
	HasLock   bool
}

// Compare classifies the checked-out contract (head) against the
// operator-approved base revision's contract. An absent head is StateAbsent,
// which covers repositories without a contract. A head present and
// byte-identical to the base — both files, including lock presence — is
// StateApproved. A present head that differs from the base, or a base that is
// absent while the head is present, is StateModified: the runtime never
// executes a contract it did not approve.
func Compare(base, head Files) State {
	if !head.HasConfig {
		return StateAbsent
	}
	if !base.HasConfig {
		return StateModified
	}
	if string(base.Config) != string(head.Config) || base.HasLock != head.HasLock || string(base.Lock) != string(head.Lock) {
		return StateModified
	}
	return StateApproved
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
// new keys in delta order. Run uses it internally, and it is exported so the
// two wiring sites (legacy executor child env, protocol worker task env)
// cannot drift.
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
