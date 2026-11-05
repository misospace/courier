package rebase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/misospace/courier/internal/git"
)

// semanticError marks a conflict the mechanical rules cannot resolve.
type semanticError struct {
	files  []string
	reason string
}

func (e *semanticError) Error() string {
	return fmt.Sprintf("semantic conflict in %d file(s): %s", len(e.files), e.reason)
}

// resolveConflicts classifies every conflicted path of an in-progress rebase
// with the closed mechanical rules and, when every hunk across every file
// classifies, stages the resolutions and continues the rebase until it
// completes. A single non-mechanical hunk stops the whole attempt with a
// semantic conflict: mechanical resolution is all-or-nothing, because a
// replay that resolves some hunks and stops midway leaves a half-written
// branch no better than it started.
//
// When opts.ModelResolver is nil the rules are the entire story, with one
// deliberate refinement: a conflicted path that is not a regular file — or
// not a name the filesystem accepts at all — stops the attempt for a human
// instead of being read or written through. When one is configured, the
// episode's first rejected set goes to exactly one model-assisted attempt
// (see assistRound); a later round in the same episode that rejects again
// has already spent that single attempt, and the attempt stops for a human
// with the new conflicts named. The fast path, the all-or-nothing property,
// and the closed rules themselves are untouched.
//
// The rules are deliberately narrow. A hunk resolves mechanically only when
// the base-side content proves which side moved:
//
//   - one-sided edit: one side's content equals the recorded base content, so
//     only the other side changed and its content is the resolution;
//   - comment-only overlap: every changed line on both sides is a comment or
//     blank line, and one side's lines are a subset of the other's, so the
//     superset preserves both edits without touching any code.
//
// Everything else — two-sided code edits, deletions against modifications,
// lockfile regeneration, version strings — needs a decision, and the attempt
// stops for a human with the file list. The rules never pick a "likely" side
// for code and never merge code text.
func resolveConflicts(ctx context.Context, w *git.Workspace, opts Options, origHead string, paths []string, assist *AssistReport) error {
	if len(paths) == 0 {
		return errors.New("rebase stopped without reporting conflicted paths")
	}
	sort.Strings(paths)
	// The episode's single model-assisted attempt (#732): spent on the first
	// round whose rejected set reaches the resolver, and never renewed
	// within the same Execute.
	assistSpent := false
	for round := 0; round < maxRebaseRounds; round++ {
		var rejected []string
		for _, path := range paths {
			resolved, err := resolveFile(w.Directory, path)
			if err != nil {
				var nonRegular *nonRegularConflictError
				if errors.As(err, &nonRegular) {
					// A conflicted path that can never be classified — not
					// a regular file, or not a name the filesystem accepts
					// — stops the attempt for a human with the path named,
					// exactly as a non-mechanical hunk would.
					return &semanticError{
						files:  []string{path},
						reason: nonRegular.Error(),
					}
				}
				return fmt.Errorf("classify conflict in %s: %w", path, err)
			}
			if !resolved {
				rejected = append(rejected, path)
				continue
			}
			if err := w.Stage(ctx, path); err != nil {
				return fmt.Errorf("stage mechanical resolution of %s: %w", path, err)
			}
		}
		if len(rejected) > 0 {
			if opts.ModelResolver == nil {
				return &semanticError{
					files:  rejected,
					reason: "the conflicts need a decision a mechanical rule cannot make; no side was picked",
				}
			}
			// The mechanical rules rejected this round's set and a resolver
			// is configured. The episode's single model-assisted attempt was
			// already spent on an earlier round: a new rejected set is a new
			// conflict the one bounded attempt cannot answer, so the attempt
			// stops for a human with the new conflicts named honestly.
			if assistSpent {
				return &semanticError{
					files:  rejected,
					reason: "the episode's single model-assisted attempt was already spent on an earlier conflict round; the new conflicts need a human decision",
				}
			}
			assistSpent = true
			if err := assistRound(ctx, w, opts, origHead, rejected, assist); err != nil {
				return err
			}
		}
		// Every conflict in this round resolved and staged: continue the
		// replay. The continue itself can stop again on the next replayed
		// commit, which loops as another round.
		if err := w.RebaseContinue(ctx, w.BaseRevisionRef()); err != nil {
			var conflictErr *git.RebaseConflictError
			if !errors.As(err, &conflictErr) {
				return err
			}
			paths = conflictErr.Paths
			continue
		}
		return nil
	}
	return fmt.Errorf("the replay needed more than %d resolution rounds", maxRebaseRounds)
}

// resolveFile rewrites one conflicted file's hunks in place. It returns false
// without touching the file when any hunk is non-mechanical, so a rejected
// file keeps git's markers exactly as the rebase left them for the human
// summary. A conflicted path that is not a regular file — git leaves the
// symlink itself in the worktree on a symlink content conflict — or not a
// name the filesystem accepts at all (a NUL byte or an over-long name, which
// git cannot produce as a tree entry) is refused as a
// nonRegularConflictError: reading or writing through the link would reach a
// path outside the workspace, a mechanical rule never writes bytes it cannot
// see the provenance of, and an unnamable path is anomalous rather than an
// infrastructure fault to leak.
func resolveFile(directory, path string) (bool, error) {
	full := filepath.Join(directory, filepath.FromSlash(path))
	info, err := os.Lstat(full)
	if err != nil {
		return false, lstatRefusal(path, err)
	}
	if !info.Mode().IsRegular() {
		return false, nonRegularConflict(path)
	}
	if info.Size() > maxConflictFileBytes {
		return false, nil
	}
	raw, err := os.ReadFile(full)
	if err != nil {
		return false, err
	}
	if !bytes.Contains(raw, []byte("<<<<<<<")) {
		// No text markers: binary content or an already-staged path. Nothing
		// a text rule can decide.
		return false, nil
	}
	hunks, ok := parseConflictHunks(raw)
	if !ok {
		return false, nil
	}
	if len(hunks) == 0 {
		return false, nil
	}
	var resolved bytes.Buffer
	cursor := 0
	for _, h := range hunks {
		ours, theirs, ok := mechanicalResolution(h)
		if !ok {
			return false, nil
		}
		resolved.Write(raw[cursor:h.start])
		resolved.WriteString(ours)
		resolved.WriteString(theirs)
		cursor = h.end
	}
	resolved.Write(raw[cursor:])
	if err := os.WriteFile(full, resolved.Bytes(), info.Mode().Perm()); err != nil {
		return false, err
	}
	return true, nil
}

// conflictHunk is one "<<<<<<< … >>>>>>>" region in a diff3-marked file. ours
// is the upstream (base) section, theirs the PR-side section, and base the
// recorded common ancestor when the marker style carried one.
type conflictHunk struct {
	start, end   int // byte offsets in the raw file
	ours, theirs []string
	base         []string
	baseSeen     bool
}

// parseConflictHunks finds every complete conflict region in a diff3-marked
// file. A malformed region — an unterminated marker, a stray separator —
// fails the whole file: partial parsing would risk writing a file the
// resolver did not actually understand.
func parseConflictHunks(raw []byte) ([]conflictHunk, bool) {
	type lineInfo struct {
		text   string
		offset int
	}
	var lines []lineInfo
	{
		offset := 0
		for _, line := range bytes.SplitAfter(raw, []byte("\n")) {
			lines = append(lines, lineInfo{text: string(line), offset: offset})
			offset += len(line)
		}
	}
	var hunks []conflictHunk
	for i := 0; i < len(lines); i++ {
		text := strings.TrimRight(lines[i].text, "\n\r")
		if !strings.HasPrefix(text, "<<<<<<<") {
			continue
		}
		hunk := conflictHunk{start: lines[i].offset}
		section := &hunk.ours
		complete := false
		for j := i + 1; j < len(lines); j++ {
			marker := strings.TrimRight(lines[j].text, "\n\r")
			switch {
			case strings.HasPrefix(marker, "|||||||"):
				if section != &hunk.ours {
					i = j
					section = nil
				} else {
					hunk.baseSeen = true
					section = &hunk.base
				}
			case marker == "=======":
				if section == &hunk.ours || section == &hunk.base {
					section = &hunk.theirs
				} else {
					i = j
					section = nil
				}
			case strings.HasPrefix(marker, ">>>>>>>"):
				if section != &hunk.theirs {
					i = j
					section = nil
				} else {
					hunk.end = lines[j].offset + len(lines[j].text)
					complete = true
					i = j
				}
			default:
				if section != nil {
					*section = append(*section, strings.TrimRight(marker, "\r"))
				}
			}
			if complete || section == nil {
				break
			}
		}
		if !complete {
			return nil, false
		}
		hunks = append(hunks, hunk)
	}
	return hunks, true
}

// mechanicalResolution returns the resolved upstream/PR content of one hunk
// under the closed mechanical rules, or false when the hunk needs a human.
func mechanicalResolution(h conflictHunk) (string, string, bool) {
	ours := strings.Join(h.ours, "\n")
	theirs := strings.Join(h.theirs, "\n")
	base := strings.Join(h.base, "\n")
	// One-sided edits: the recorded base content proves which side moved.
	if h.baseSeen {
		switch {
		case ours == base && theirs != base:
			return "", theirs, true
		case theirs == base && ours != base:
			return ours, "", true
		case ours == base && theirs == base:
			// Both sides equal the base: the conflict carried no change.
			return ours, "", true
		}
	}
	// Comment-only overlap where one side is a subset of the other: the
	// superset keeps both comment edits without touching code.
	if commentOnly(h.ours) && commentOnly(h.theirs) {
		switch {
		case isSubset(h.ours, h.theirs):
			return "", theirs, true
		case isSubset(h.theirs, h.ours):
			return ours, "", true
		}
	}
	return "", "", false
}

// commentOnly reports whether every line is blank or a comment in one of the
// recognized syntaxes. Anything that could be code is never comment-only.
func commentOnly(lines []string) bool {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch {
		case strings.HasPrefix(trimmed, "#"),
			strings.HasPrefix(trimmed, "//"),
			strings.HasPrefix(trimmed, "/*"),
			strings.HasPrefix(trimmed, "*"),
			strings.HasPrefix(trimmed, "*/"),
			strings.HasPrefix(trimmed, "<!--"),
			strings.HasPrefix(trimmed, "--"),
			strings.HasPrefix(trimmed, "%"),
			strings.HasPrefix(trimmed, ";"):
			continue
		}
		return false
	}
	return true
}

// isSubset reports whether a's non-blank lines all appear in b.
func isSubset(a, b []string) bool {
	set := make(map[string]struct{}, len(b))
	for _, line := range b {
		if strings.TrimSpace(line) == "" {
			continue
		}
		set[line] = struct{}{}
	}
	for _, line := range a {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if _, ok := set[line]; !ok {
			return false
		}
	}
	return true
}
