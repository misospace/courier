package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/misospace/courier/internal/log"
)

// Options describes one capture: the worktree to snapshot, the identity to
// record in the manifest, and the scanner that decides what is a credential.
type Options struct {
	// Directory is the repository worktree root (absolute).
	Directory string
	// UpstreamRef is the run-branch ref local commits are compared against
	// (e.g. "origin/<branch>"); commits reachable from it are excluded.
	UpstreamRef string
	// StartSHA is the run start SHA; it is the fallback comparison point when
	// UpstreamRef cannot be resolved.
	StartSHA string
	// Run identifies the run and pod producing the capture.
	Run RunIdentity
	// Workspace identifies the repository state being captured.
	Workspace WorkspaceIdentity
	// Trigger is one of the Trigger constants naming the capture moment.
	Trigger string
	// CapturedAt stamps the manifest; zero means time.Now().UTC().
	CapturedAt time.Time
	// Scanner decides fail-closed what is a credential; nil means
	// NewScanner() (pattern table only).
	Scanner *Scanner
}

// Bundle is the result of a capture: the manifest plus a gzip'd tar of the
// admitted (stored) content. Archive is nil when nothing was admitted.
type Bundle struct {
	Manifest Manifest
	Archive  []byte
}

// tarItem is one admitted file queued for the archive.
type tarItem struct {
	name string
	data []byte
}

// change is one porcelain worktree record.
type change struct {
	xy   string
	path string
}

// Capture snapshots the worktree and local commits into a manifest plus a tar
// of admitted content. It is read-only on the repository (status, rev-list,
// format-patch, and file reads) and honours ctx cancellation on git calls.
// The order of operations is normative: enumerate, classify, scan, then build
// the tar and the budget-bounded manifest.
func Capture(ctx context.Context, opts Options) (*Bundle, error) {
	scanner := opts.Scanner
	if scanner == nil {
		scanner = NewScanner()
	}
	dir := opts.Directory

	changes, err := listChanges(ctx, dir)
	if err != nil {
		return nil, err
	}

	var (
		entries  []Entry
		items    []tarItem
		totals   Totals
		stored   int
		runningB int64
	)
	totals.Files = len(changes)

	for _, ch := range changes {
		entry := Entry{Path: ch.path, Class: classFromXY(ch.xy)}
		full := filepath.Join(dir, ch.path)
		info, lerr := os.Lstat(full)
		switch {
		case lerr != nil:
			if strings.Contains(ch.xy, "D") {
				entry.Disposition = DispositionDeleted
				totals.Deleted++
			} else {
				entry.Disposition = DispositionOmittedBinary
				totals.OmittedBinary++
			}
		case info.Mode()&os.ModeSymlink != 0:
			entry.Disposition = DispositionSymlink
			totals.Symlinks++
			if target, rerr := os.Readlink(full); rerr == nil {
				entry.LinkTarget = target
				entry.Escaping = symlinkEscapes(dir, full, target)
			} else {
				entry.Escaping = true
			}
		case !info.Mode().IsRegular():
			// Non-regular files (fifo, socket, device) are unscannable.
			entry.Disposition = DispositionOmittedBinary
			totals.OmittedBinary++
		case info.Size() > MaxFileBytes:
			entry.Disposition = DispositionOmittedOverLimit
			entry.Bytes = info.Size()
			totals.OmittedOverLimit++
		default:
			// Deliberate: a file over MaxFileBytes is classified
			// omitted-over-limit WITHOUT a content scan, so an oversized
			// file is never read into memory whole; both omitted-binary
			// and omitted-over-limit store nothing.
			data, rerr := readCapped(full, MaxFileBytes+1)
			if rerr != nil {
				entry.Disposition = DispositionOmittedBinary
				entry.Bytes = info.Size()
				totals.OmittedBinary++
				break
			}
			if len(data) > MaxFileBytes {
				entry.Disposition = DispositionOmittedOverLimit
				entry.Bytes = info.Size()
				totals.OmittedOverLimit++
				break
			}
			switch {
			case scanner.Matched(string(data)) || scanner.Matched(ch.path):
				entry.Disposition = DispositionWithheld
				totals.Withheld++
			case isBinary(data):
				entry.Disposition = DispositionOmittedBinary
				entry.Bytes = int64(len(data))
				totals.OmittedBinary++
			case stored == MaxContentEntries || runningB+int64(len(data)) > MaxTotalBytes || unsafeTarName(ch.path):
				entry.Disposition = DispositionOmittedOverLimit
				entry.Bytes = int64(len(data))
				totals.OmittedOverLimit++
			default:
				entry.Disposition = DispositionStored
				entry.Bytes = int64(len(data))
				entry.StoredPath = path.Clean(filepath.ToSlash(ch.path))
				stored++
				runningB += int64(len(data))
				totals.Stored++
				totals.StoredBytes += int64(len(data))
				items = append(items, tarItem{name: entry.StoredPath, data: data})
			}
		}
		entries = append(entries, entry)
	}

	commits, err := localCommitEntries(ctx, dir, opts, scanner, &items, &totals, &stored, &runningB)
	if err != nil {
		return nil, err
	}
	totals.Commits = len(commits)
	entries = append(entries, commits...)

	for i := range entries {
		entries[i].Path = truncateDisplay(scanner.RedactMetadata(entries[i].Path))
		if entries[i].LinkTarget != "" {
			entries[i].LinkTarget = truncateDisplay(scanner.RedactMetadata(entries[i].LinkTarget))
		}
	}

	archive, err := buildTar(items)
	if err != nil {
		return nil, err
	}

	capturedAt := opts.CapturedAt
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	if entries == nil {
		entries = []Entry{}
	}
	manifest := Manifest{
		SchemaVersion: SchemaVersion,
		Run:           opts.Run,
		Workspace:     opts.Workspace,
		CapturedAt:    capturedAt.UTC(),
		Trigger:       opts.Trigger,
		Entries:       entries,
		Totals:        totals,
	}
	collapseToBudget(&manifest)
	return &Bundle{Manifest: manifest, Archive: archive}, nil
}

// localCommitEntries lists commits reachable from HEAD but not from the
// upstream ref (falling back to StartSHA when the ref cannot be resolved) and
// produces one entry per commit with its format-patch scanned fail closed.
// Commits draw from the same content-entry count and byte budget as files:
// stored and runningBytes are the shared running admission tallies.
func localCommitEntries(ctx context.Context, dir string, opts Options, scanner *Scanner, items *[]tarItem, totals *Totals, stored *int, runningBytes *int64) ([]Entry, error) {
	var out []Entry
	ref := strings.TrimSpace(opts.UpstreamRef)
	if !refResolvable(ctx, dir, ref) {
		ref = strings.TrimSpace(opts.StartSHA)
		if ref == "" || !refResolvable(ctx, dir, ref) {
			return out, nil
		}
	}
	list, err := runGit(ctx, dir, "rev-list", ref+"..HEAD")
	if err != nil {
		return out, err
	}
	shas := splitLines(string(list))
	// rev-list is newest-first; iterate oldest-first so patch numbering
	// ascends with history.
	for i, j := 0, len(shas)-1; i < j; i, j = i+1, j-1 {
		shas[i], shas[j] = shas[j], shas[i]
	}
	for i, sha := range shas {
		short := sha
		if len(short) > 7 {
			short = short[:7]
		}
		patch, overLimit, err := runGitCapped(ctx, dir, MaxFileBytes, "format-patch", "--stdout", "--no-binary", "-1", sha)
		if err != nil {
			return out, err
		}
		if overLimit {
			// Deliberate: a patch already over MaxFileBytes is classified
			// omitted-over-limit WITHOUT a content scan, so it is never kept
			// whole; the message fetch is skipped too. Bytes reports the
			// bounded prefix read before the cap, not the full patch size.
			totals.OmittedOverLimit++
			out = append(out, Entry{
				Path:        fmt.Sprintf("commits/%04d-%s.patch", i+1, short),
				Class:       ClassCommit,
				CommitSHA:   sha,
				Disposition: DispositionOmittedOverLimit,
				Bytes:       int64(len(patch)),
			})
			continue
		}
		// format-patch on a merge commit prints nothing; skip it.
		if len(bytes.TrimSpace(patch)) == 0 {
			continue
		}
		// Safe: the message is fully contained in the <= MaxFileBytes patch that passed the cap.
		message, err := runGit(ctx, dir, "show", "-s", "--format=%B", sha)
		if err != nil {
			return out, err
		}
		storedPath := fmt.Sprintf("commits/%04d-%s.patch", i+1, short)
		entry := Entry{Path: storedPath, Class: ClassCommit, CommitSHA: sha}
		switch {
		case scanner.Matched(string(patch)) || scanner.Matched(string(message)):
			entry.Disposition = DispositionWithheld
			totals.Withheld++
		case *stored >= MaxContentEntries || *runningBytes+int64(len(patch)) > MaxTotalBytes || len(patch) > MaxFileBytes || unsafeTarName(storedPath):
			entry.Disposition = DispositionOmittedOverLimit
			entry.Bytes = int64(len(patch))
			totals.OmittedOverLimit++
		default:
			entry.Disposition = DispositionStored
			entry.Bytes = int64(len(patch))
			entry.StoredPath = storedPath
			*stored++
			*runningBytes += int64(len(patch))
			totals.Stored++
			totals.StoredBytes += int64(len(patch))
			*items = append(*items, tarItem{name: storedPath, data: patch})
		}
		out = append(out, entry)
	}
	return out, nil
}

// refResolvable reports whether a git rev resolves in the repository.
func refResolvable(ctx context.Context, dir, ref string) bool {
	if ref == "" {
		return false
	}
	out, err := runGit(ctx, dir, "rev-parse", "--verify", "--quiet", ref)
	if err != nil {
		return false
	}
	return len(bytes.TrimSpace(out)) > 0
}

// listChanges enumerates worktree changes in porcelain v1 -z form, keeping the
// XY pair. A rename or copy record is followed by its origin path, which is
// consumed as part of the same entry.
func listChanges(ctx context.Context, dir string) ([]change, error) {
	out, err := runGit(ctx, dir, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--", ".")
	if err != nil {
		return nil, err
	}
	var changes []change
	records := strings.Split(string(out), "\x00")
	for i := 0; i < len(records); i++ {
		record := records[i]
		if len(record) < 4 {
			continue
		}
		xy := record[:2]
		if xy[0] == 'R' || xy[0] == 'C' {
			i++
		}
		changes = append(changes, change{xy: xy, path: record[3:]})
	}
	return changes, nil
}

// classFromXY maps a porcelain XY pair to an entry Class. Rename and copy
// win over the other letter. Unmerged porcelain pairs (UU, AA, DD, and the
// like) have no dedicated class of their own: each falls back to the closest
// existing class — added, deleted, or modified, with modified as the last
// resort.
func classFromXY(xy string) string {
	if xy == "??" {
		return ClassUntracked
	}
	for _, c := range xy {
		switch c {
		case 'R':
			return ClassRenamed
		case 'C':
			return ClassCopied
		}
	}
	for _, c := range xy {
		switch c {
		case 'A':
			return ClassAdded
		case 'D':
			return ClassDeleted
		case 'M':
			return ClassModified
		}
	}
	return ClassModified
}

// isBinary reports NUL bytes in the sniff window or invalid UTF-8 anywhere in
// the content.
func isBinary(data []byte) bool {
	window := data
	if len(window) > BinarySniffBytes {
		window = window[:BinarySniffBytes]
	}
	return bytes.IndexByte(window, 0) != -1 || !utf8.Valid(data)
}

// symlinkEscapes reports whether a symlink target resolves outside the
// repository root, without ever following the link.
func symlinkEscapes(dir, linkPath, target string) bool {
	var abs string
	if filepath.IsAbs(target) {
		abs = filepath.Clean(target)
	} else {
		a, err := filepath.Abs(filepath.Join(filepath.Dir(linkPath), target))
		if err != nil {
			return true
		}
		abs = a
	}
	cleanRoot := filepath.Clean(dir)
	return !(abs == cleanRoot || strings.HasPrefix(abs, cleanRoot+string(os.PathSeparator)))
}

// unsafeTarName reports whether a repo-relative path could escape the archive
// root (absolute, or containing a ".." element).
func unsafeTarName(relPath string) bool {
	slashed := filepath.ToSlash(relPath)
	if path.IsAbs(slashed) || filepath.IsAbs(relPath) {
		return true
	}
	for _, elem := range strings.Split(slashed, "/") {
		if elem == ".." {
			return true
		}
	}
	return false
}

// truncateDisplay caps a manifest display string at MaxPathDisplayBytes on a
// UTF-8 rune boundary, appending an ellipsis when truncated. The redaction
// placeholder is never truncated.
func truncateDisplay(s string) string {
	if len(s) <= MaxPathDisplayBytes || s == log.RedactedPlaceholder {
		return s
	}
	cut := MaxPathDisplayBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// readCapped opens path and reads at most limit bytes, stopping there even if
// the file on disk is larger.
func readCapped(path string, limit int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, int64(limit)))
}

// buildTar gzips a tar archive of the admitted entries only. An empty set
// yields a nil archive.
func buildTar(items []tarItem) ([]byte, error) {
	if len(items) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, item := range items {
		header := &tar.Header{
			Name:     item.name,
			Mode:     0o644,
			Size:     int64(len(item.data)),
			Typeflag: tar.TypeReg,
			Format:   tar.FormatPAX,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return nil, fmt.Errorf("evidence tar header %q: %w", item.name, err)
		}
		if _, err := tarWriter.Write(item.data); err != nil {
			return nil, fmt.Errorf("evidence tar member %q: %w", item.name, err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		return nil, fmt.Errorf("evidence tar close: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("evidence tar gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

// collapseToBudget enforces MaxManifestEntries and ManifestBudgetBytes by
// keeping the leading entries that fit and collapsing every remaining real
// entry into one synthetic omitted-manifest-budget record. Totals stay
// authoritative; a budget overflow never rejects the capture, even when only
// the synthetic entry fits.
func collapseToBudget(manifest *Manifest) {
	entries := manifest.Entries
	if len(entries) == 0 {
		return
	}
	if fits, err := manifestFits(manifest, entries); err == nil && fits && len(entries) <= MaxManifestEntries {
		return
	}
	keep := len(entries)
	if keep > MaxManifestEntries-1 {
		keep = MaxManifestEntries - 1
	}
	for ; keep > 0; keep-- {
		candidate := append([]Entry(nil), entries[:keep]...)
		candidate = append(candidate, Entry{Disposition: DispositionOmittedBudget})
		manifest.Entries = candidate
		if fits, err := manifestFits(manifest, candidate); err == nil && fits {
			break
		}
	}
	manifest.Entries = append(append([]Entry(nil), entries[:keep]...), Entry{Disposition: DispositionOmittedBudget})
	manifest.Totals.OmittedByBudget = len(entries) - keep
}

// manifestFits reports whether the manifest with the given entries serializes
// within ManifestBudgetBytes.
func manifestFits(manifest *Manifest, entries []Entry) (bool, error) {
	probe := *manifest
	probe.Entries = entries
	data, err := probe.MarshalCanonical()
	if err != nil {
		return false, err
	}
	return len(data) <= ManifestBudgetBytes, nil
}

// runGit executes a git command in dir and returns stdout, wrapping failures
// with the command's stderr.
func runGit(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// runGitCapped runs a git command in dir, reading at most limit bytes from
// stdout while the child produces output. If the output exceeds the limit the
// child is killed and overLimit is set with the bounded prefix returned, so a
// run that would otherwise buffer an unbounded result stops at the cap.
// A failing or cancelled command returns a wrapped error (with the command's
// trimmed stderr), never overLimit.
func runGitCapped(ctx context.Context, dir string, limit int, args ...string) (out []byte, overLimit bool, err error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, false, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	var stderr cappedBuffer
	stderr.limit = stderrLimit
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, false, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(stderr.data)))
	}
	out, _ = io.ReadAll(io.LimitReader(stdout, int64(limit)+1))
	if len(out) > limit {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return out, true, nil
	}
	if err := cmd.Wait(); err != nil {
		return nil, false, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(stderr.data)))
	}
	return out, false, nil
}

// stderrLimit bounds the child stderr captured by runGitCapped so a verbose
// or failing git cannot allocate unbounded memory there either.
const stderrLimit = 4 * 1024

// cappedBuffer is an io.Writer that keeps at most its limit bytes, silently
// discarding anything written beyond them.
type cappedBuffer struct {
	limit int
	data  []byte
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	room := b.limit - len(b.data)
	if room <= 0 {
		return len(p), nil
	}
	if room < len(p) {
		p = p[:room]
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// splitLines splits command output into non-empty trimmed lines.
func splitLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
