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

// ScriptFile is one bootstrap-referenced script target as read at one
// revision. Present distinguishes "this ref was read at this revision and
// the path exists there" from "this ref was never read": Compare requires
// every base-referenced script to appear in both sides' Scripts maps, so a
// ref nobody read fails closed rather than approving by accident. Data is
// nil unless Present. Regular reports a git regular blob (filemode 100644
// or 100755) — never a 120000 symlink, a 160000 gitlink, or any other
// entry, all of which are Present but not Regular.
type ScriptFile struct {
	Data    []byte
	Regular bool
	Present bool
}

// Files carries the contract file contents at one revision. Config and Lock
// are nil when absent; HasConfig and HasLock report presence separately, so
// a read error is never folded into absence — callers handle read errors
// themselves and must not call Compare in that case. ConfigRegular
// and LockRegular report a git regular blob at that revision (filemode
// 100644 or 100755): an executable contract path that is a symlink or any
// other non-regular entry is unprovable and must fail closed. Scripts holds
// the bootstrap task's referenced script targets as read at this revision,
// keyed by the ref text from BootstrapScriptRefs; it is nil when no refs
// were read.
type Files struct {
	Config        []byte
	Lock          []byte
	HasConfig     bool
	HasLock       bool
	ConfigRegular bool
	LockRegular   bool
	Scripts       map[string]ScriptFile
}

// Compare classifies the checked-out contract (head) against the
// operator-approved base revision's contract, failing closed on everything
// it cannot prove. An absent head is StateAbsent, which covers repositories
// without a contract. A base that is absent while the head is present is
// StateModified. Otherwise every contract path must be a regular blob at
// both revisions (a symlinked or otherwise non-regular contract path is an
// untrusted modification), the config and lock bytes — including lock
// presence — must be byte-identical, and the base config's bootstrap task
// must parse: an unparseable bootstrap task is StateModified, never a guess
// about what it would run. When the task references script targets, each
// one must be present, regular, and byte-identical at BOTH revisions — a
// referenced script the base never read, or one that changed between base
// and head, is a modified contract. Anything unprovable is StateModified:
// the runtime never executes a contract it did not approve.
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
	if base.HasLock && head.HasLock && (!base.LockRegular || !head.LockRegular) {
		return StateModified
	}
	if string(base.Config) != string(head.Config) || base.HasLock != head.HasLock || string(base.Lock) != string(head.Lock) {
		return StateModified
	}
	refs, ok := BootstrapScriptRefs(base.Config)
	if !ok {
		return StateModified
	}
	for _, ref := range refs {
		baseScript, baseRead := base.Scripts[ref]
		headScript, headRead := head.Scripts[ref]
		if !baseRead || !headRead {
			return StateModified
		}
		if !baseScript.Present || !headScript.Present ||
			!baseScript.Regular || !headScript.Regular ||
			string(baseScript.Data) != string(headScript.Data) {
			return StateModified
		}
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
// cannot drift between them: config and lock are read at head and base, the
// base config's bootstrap refs (when any) are read at BOTH revisions and
// stored into both Files' Scripts maps so Compare can pin them, and any
// read error is returned as an error — callers treat an error as an
// unreadable contract and skip, never execute. A ref that is absent at a
// revision is still stored in that side's map with Present=false, so the
// absence itself is provable rather than silently unexamined. Refs are read
// only when both revisions carry a config: an absent head is already
// provably StateAbsent, and forcing script reads there would turn honest
// absence into a spurious error.
func Classify(ctx context.Context, read BlobRead, baseRev, headRev string) (State, error) {
	base, err := readContractFiles(ctx, read, baseRev)
	if err != nil {
		return "", err
	}
	head, err := readContractFiles(ctx, read, headRev)
	if err != nil {
		return "", err
	}
	if base.HasConfig && head.HasConfig {
		if refs, ok := BootstrapScriptRefs(base.Config); ok && len(refs) > 0 {
			base.Scripts = make(map[string]ScriptFile, len(refs))
			head.Scripts = make(map[string]ScriptFile, len(refs))
			for _, ref := range refs {
				baseData, baseRegular, basePresent, err := read(ctx, baseRev, ref)
				if err != nil {
					return "", err
				}
				headData, headRegular, headPresent, err := read(ctx, headRev, ref)
				if err != nil {
					return "", err
				}
				base.Scripts[ref] = ScriptFile{Data: baseData, Regular: baseRegular, Present: basePresent}
				head.Scripts[ref] = ScriptFile{Data: headData, Regular: headRegular, Present: headPresent}
			}
		}
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

// bootstrapSection is the mise task section the #414 contract's bootstrap
// lives in.
const bootstrapSection = "[tasks.bootstrap]"

// BootstrapScriptRefs extracts the script targets the contract's
// `[tasks.bootstrap]` task references in its `run` value, so the trust
// decision can pin every file the bootstrap would execute rather than only
// the two contract files. The parse is deliberately narrow and fails
// closed:
//
//   - A line whose whitespace-trimmed form is exactly `[tasks.bootstrap]`
//     opens the section; any other `[...]` header closes it. A section
//     without a `run` key before the next header is not ok.
//   - The `run` value must be a single- or double-quoted TOML string with
//     no backslash anywhere inside it, followed only by whitespace and/or a
//     `#` comment. Anything else is not ok — an unparseable task can never
//     be approved, only skipped.
//   - The string is tokenized on whitespace. A token carrying a shell
//     metacharacter (quotes, backticks, `$`, redirection, braces, globs,
//     `=`, `#`, or any ASCII control character) makes the whole result not
//     ok. Tokens starting with `-` (flags) or `/` (absolute paths) are
//     skipped silently — they are arguments, not repository-relative
//     targets. A token with a `..` path segment is not ok.
//
// The remaining tokens, in order and deduplicated, are the referenced
// script targets. When the config carries NO `[tasks.bootstrap]` section at
// all: a `bootstrap` mention anywhere else (an inline-table task form this
// parse does not understand, for example) is not ok — fail closed; a config
// with no bootstrap trace is ok with no refs, because `mise run bootstrap`
// then fails deterministically and honestly at run time, which is the
// contract's own failure, not a trust failure.
func BootstrapScriptRefs(config []byte) ([]string, bool) {
	text := string(config)
	inSection := false
	sectionFound := false
	sawRun := false
	var refs []string
	seen := make(map[string]bool)
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == bootstrapSection:
			inSection = true
			sectionFound = true
			sawRun = false
		case strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]"):
			if inSection && !sawRun {
				return nil, false
			}
			inSection = false
		case inSection:
			value, isRun := runValue(trimmed)
			if !isRun {
				continue
			}
			content, ok := quotedTOMLString(value)
			if !ok {
				return nil, false
			}
			tokens, ok := scriptRefTokens(content)
			if !ok {
				return nil, false
			}
			for _, token := range tokens {
				if !seen[token] {
					seen[token] = true
					refs = append(refs, token)
				}
			}
			sawRun = true
		}
	}
	if !sectionFound {
		if strings.Contains(strings.ToLower(text), "bootstrap") {
			return nil, false
		}
		return nil, true
	}
	if inSection && !sawRun {
		return nil, false
	}
	return refs, true
}

// runValue reports whether a trimmed section line is the task's `run` key
// and returns the raw text after the `=`.
func runValue(trimmed string) (string, bool) {
	if !strings.HasPrefix(trimmed, "run") {
		return "", false
	}
	rest := trimmed[len("run"):]
	for len(rest) > 0 && (rest[0] == ' ' || rest[0] == '\t') {
		rest = rest[1:]
	}
	if !strings.HasPrefix(rest, "=") {
		return "", false
	}
	return rest[1:], true
}

// quotedTOMLString parses one raw TOML string value: a single-quoted
// literal or double-quoted basic string with no backslash anywhere inside,
// followed only by whitespace and/or a `#` comment outside the quotes.
func quotedTOMLString(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || (raw[0] != '\'' && raw[0] != '"') {
		return "", false
	}
	quote := raw[0]
	end := strings.IndexByte(raw[1:], quote)
	if end < 0 {
		return "", false
	}
	content := raw[1 : 1+end]
	rest := strings.TrimSpace(raw[1+end+1:])
	if rest != "" && !strings.HasPrefix(rest, "#") {
		return "", false
	}
	if quote == '"' && strings.ContainsRune(content, '\\') {
		return "", false
	}
	return content, true
}

// forbiddenRefChars are the shell metacharacters and quote characters that
// make a run token unprovable: a token carrying any of them could change
// meaning under shell interpretation, so the whole task fails closed.
const forbiddenRefChars = "\"'`$|;&><(){}*?~\\=#"

// scriptRefTokens tokenizes the run value's string content on whitespace
// into candidate repository-relative script targets, failing closed on any
// metacharacter or `..` path segment and silently skipping flags and
// absolute paths.
func scriptRefTokens(content string) ([]string, bool) {
	var refs []string
	for _, token := range strings.Fields(content) {
		if hasForbiddenRefChar(token) {
			return nil, false
		}
		if strings.HasPrefix(token, "-") || strings.HasPrefix(token, "/") {
			continue
		}
		for _, segment := range strings.Split(token, "/") {
			if segment == ".." {
				return nil, false
			}
		}
		refs = append(refs, token)
	}
	return refs, true
}

// hasForbiddenRefChar reports whether a token carries a shell
// metacharacter or any ASCII control character.
func hasForbiddenRefChar(token string) bool {
	for _, r := range token {
		if r < 0x20 || r == 0x7f || strings.ContainsRune(forbiddenRefChars, r) {
			return true
		}
	}
	return false
}
