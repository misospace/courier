package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
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
// format-patch, and file reads) and honours ctx cancellation on git calls and
// on the per-file reads. The order of operations is normative: enumerate,
// classify, scan, then build the tar and the budget-bounded manifest.
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
			applySymlinkMetadata(&entry, &totals, dir, full)
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
			data, rerr := readFileForCapture(ctx, full, MaxFileBytes+1)
			// Cancellation is settled before the read result itself, not
			// only before the read-error taxonomy: the read seam may be
			// answered with completed bytes after the context is already
			// dead — a real read that won the select below, or any seam
			// implementation — and storing them would carry the capture
			// past the deadline its manifest-only degradation promises.
			// The same check also settles a read abandoned at the operation
			// deadline, whose context error recorded as an omitted binary
			// would both mask a stale content classification and keep the
			// capture going instead of handing the caller the error its
			// manifest-only degradation already handles.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if rerr != nil {
				if errors.Is(rerr, syscall.ELOOP) {
					// The path was swapped for a symlink between the Lstat
					// classification and this O_NOFOLLOW open: re-classify as
					// symlink metadata and store nothing, so a target the
					// classification pass never saw is never stored.
					applySymlinkMetadata(&entry, &totals, dir, full)
					break
				}
				if errors.Is(rerr, errNotRegular) {
					// The path was swapped for a fifo, directory, or device
					// between the Lstat classification and this open:
					// re-classify exactly as a path classified non-regular
					// outright — omitted-binary, storing nothing — so a peer
					// the classification pass never saw is never waited on and
					// never blocks the capture. A socket swap needs no branch
					// of its own: its read-only open fails outright and falls
					// to the generic failed-read case, which stores nothing.
					entry.Disposition = DispositionOmittedBinary
					totals.OmittedBinary++
					break
				}
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
	collapseToBudget(ctx, &manifest)
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
			// An unresolvable ref and a dead context are different answers,
			// and only one of them means "no commits here": refResolvable maps
			// every error, the context's included, to "not resolvable", so a
			// deadline that has already passed would leave the commit phase
			// reporting an empty commit list. The capture would then deliver
			// a manifest that reports the run's commits as absent while
			// looking like a success, where the design asks for the manifest
			// alone over the deadline. The dead context answers loudly.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return out, ctxErr
			}
			return out, nil
		}
	}
	// rev-list is read in full: its size is bounded by the commit count ahead
	// of ref, which the per-commit content caps bound downstream.
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
			// bounded prefix (up to MaxFileBytes+1) read before the cap was
			// hit, not the full patch size.
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
	// The porcelain change set is read in full: its size is bounded by the
	// number of worktree changes the capture must walk, not a single artifact.
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

// applySymlinkMetadata classifies an entry as metadata-only symlink and
// records the target and whether it escapes the repository root. It is used
// both for paths Lstat saw as symlinks and for paths swapped into symlinks
// before the O_NOFOLLOW read (ELOOP); the target is read at classification
// time, so a later swap to a different target is a metadata artifact, never
// stored content.
func applySymlinkMetadata(entry *Entry, totals *Totals, dir, full string) {
	entry.Disposition = DispositionSymlink
	totals.Symlinks++
	if target, rerr := os.Readlink(full); rerr == nil {
		entry.LinkTarget = target
		entry.Escaping = symlinkEscapes(dir, full, target)
	} else {
		entry.Escaping = true
	}
}

// symlinkEscapes reports whether a symlink target resolves outside the
// repository root, without ever following the link. Resolution is lexical
// (no EvalSymlinks; intermediate symlinks in the target path are not
// resolved), so an indirectly escaping target can be mislabeled as
// non-escaping. That is an accepted bound: symlinks are metadata-only,
// never followed or stored as tar members, so a mislabel is a
// classification artifact, not a write/escape hole.
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

// errNotRegular reports that the descriptor readCapped opened is not a
// regular file, so nothing was read and nothing can be stored.
var errNotRegular = errors.New("evidence: opened file is not regular")

// readFileForCapture reads, under the capture's own context, the content of a
// file classified as regular. Tests swap it to simulate a hostile worktree
// mutating a path between the Lstat classification and the read.
var readFileForCapture = readCapped

// readCapped opens path without following a trailing symlink (O_NOFOLLOW)
// and reads at most limit bytes, stopping there even if the file on disk is
// larger. A path swapped for a symlink after classification fails with
// ELOOP instead of following the new target.
//
// A path swapped for a non-regular file — fifo, directory, device — is
// opened O_NONBLOCK so the open cannot wait for a peer that may never come,
// then checked with fstat on the descriptor: a non-regular result fails with
// errNotRegular. fstat describes the object actually opened rather than the
// path, so the check cannot be swapped out from under it the way a second
// Lstat could. O_NONBLOCK is a no-op for the regular files this is reached
// for. A swap to a socket never reaches this check: a read-only open of a
// socket fails with ENXIO before any blocking is possible.
//
// The read is bounded by ctx and not by the file: one stalling read — a
// regular file on a hung filesystem — must not carry the capture past its
// operation deadline to the pod's SIGKILL, which would lose the manifest too.
// The two bounds are independent and compose: O_NONBLOCK keeps a non-regular
// swap from stalling the open, and the context bounds what survives that
// check, which is a read of a regular file the kernel has already accepted.
// Cancellation is checked before any filesystem work, so an already-expired
// context starts no goroutine at all. Otherwise the open and the capped read
// run in a goroutine whose result is received unless ctx is done first; on
// cancellation the abandoned goroutine and its descriptor are left to finish
// on their own, since closing a descriptor from another goroutine is not a
// reliable way to interrupt a read already blocked in the kernel. Its bytes
// are discarded and never stored. The pile-up question has a structural
// answer rather than a bounded one: reads are sequential and Capture returns
// at the first canceled read, so a capture abandons at most one goroutine and
// holds at most its capped prefix.
func readCapped(ctx context.Context, path string, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type readResult struct {
		data []byte
		err  error
	}
	// Buffered so the abandoned goroutine's send never blocks on a receiver
	// that is already gone.
	done := make(chan readResult, 1)
	go func() {
		f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			done <- readResult{err: err}
			return
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			done <- readResult{err: err}
			return
		}
		if !info.Mode().IsRegular() {
			done <- readResult{err: errNotRegular}
			return
		}
		data, err := io.ReadAll(io.LimitReader(f, int64(limit)))
		done <- readResult{data: data, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-done:
		// A read that completed while the context expired leaves both
		// channels ready, and the select breaks that tie at random —
		// cancellation is settled here too, so a completed read never
		// survives a dead context: past the deadline the bytes are
		// discarded and the capture degrades to the manifest alone.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return res.data, res.err
	}
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
// the synthetic entry fits. Each probe carries the candidate's real
// omittedByBudget count so a candidate picked near the budget boundary cannot
// exceed it once the authoritative count is serialized. The largest fitting
// keep is found by binary search — monotone, because one more real entry adds
// more serialized bytes than the shrinking omittedByBudget value can remove.
// If ctx is canceled mid-search, collapse settles for the largest keep already
// known to fit (0 if none), so a canceled capture still completes with a
// budget-enforcing manifest. The budget bounds entries and content; a
// keep=0 manifest is the accepted floor, whose only remaining size is
// caller-provided identity, not content.
func collapseToBudget(ctx context.Context, manifest *Manifest) {
	entries := manifest.Entries
	if len(entries) == 0 {
		return
	}
	if fits, err := manifestFits(manifest, entries, 0); err == nil && fits && len(entries) <= MaxManifestEntries {
		return
	}
	keep := 0
	lo, hi := 0, len(entries)
	if hi > MaxManifestEntries-1 {
		hi = MaxManifestEntries - 1
	}
	for lo <= hi {
		if ctx.Err() != nil {
			break
		}
		mid := lo + (hi-lo)/2
		candidate := append(append([]Entry(nil), entries[:mid]...), Entry{Disposition: DispositionOmittedBudget})
		if fits, err := manifestFits(manifest, candidate, len(entries)-mid); err == nil && fits {
			keep = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	manifest.Entries = append(append([]Entry(nil), entries[:keep]...), Entry{Disposition: DispositionOmittedBudget})
	manifest.Totals.OmittedByBudget = len(entries) - keep
}

// manifestFits reports whether the manifest with the given entries and
// omittedByBudget count serializes within ManifestBudgetBytes.
func manifestFits(manifest *Manifest, entries []Entry, omittedByBudget int) (bool, error) {
	probe := *manifest
	probe.Entries = entries
	probe.Totals.OmittedByBudget = omittedByBudget
	data, err := probe.MarshalCanonical()
	if err != nil {
		return false, err
	}
	return len(data) <= ManifestBudgetBytes, nil
}

// runGit executes a git command in dir and returns stdout, wrapping failures
// with the command's trimmed, capped stderr. A canceled context returns its
// own error even while a surviving grandchild holds a pipe open, and a command
// that exited successfully before its pipe was held returns its output rather
// than an exec.ErrWaitDelay error. An expired context discards the bytes on
// every success path: a completed command never survives a dead context, so
// bytes produced past the deadline are discarded rather than returned, and the
// keep-the-bytes rule above stands only for a live context.
func runGit(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	// Stdout and Stderr are io.Writers, so os/exec copies the child's output
	// through goroutines that Wait blocks on: a surviving grandchild holding a
	// pipe write end never sends EOF and would stall Wait itself, past the
	// context. The delay bounds that wait, as the executor's git calls already
	// do (cmd/courier-executor/main.go).
	cmd.WaitDelay = 2 * time.Second
	var stdout bytes.Buffer
	var stderr cappedBuffer
	stderr.limit = stderrLimit
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	// The guard that answers cancellation is observed once, after the reap and
	// ahead of every return below, so the expired context wins all of them:
	// bytes produced past the deadline are not a result. cmd.Run returns only
	// after Wait, so the read-versus-deadline race runGitCapped has to guard
	// against does not exist here; what remains is the policy question #448
	// answered for the git-side twin — a command that finished before the
	// deadline but whose caller returned after it does not hand its bytes
	// back under a dead context. The #421 rule below is a statement about a
	// live context — a grandchild-held pipe timing out Wait on a child that
	// already exited successfully is our timeout, not git's, and its bytes
	// stand — so it applies only once this guard has passed.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		// ErrWaitDelay arrives only for a process that exited successfully with
		// its pipes still open — a surviving grandchild — and ProcessState
		// confirms that here rather than leaning on os/exec's precedence. git
		// wrote its full output before exiting and the copy goroutine drained
		// the pipe before parking on the held write end, so the buffered bytes
		// are the complete result and the timeout is ours, not git's: keep
		// them. A child that exited non-zero keeps its exit status in the
		// wrapped error below.
		if errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
			return stdout.Bytes(), nil
		}
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(stderr.data)))
	}
	return stdout.Bytes(), nil
}

// runGitCapped runs a git command in dir, reading at most limit bytes from
// stdout while the child produces output. If the output exceeds the limit the
// child is killed and overLimit is set with the bounded prefix returned, so a
// run that would otherwise buffer an unbounded result stops at the cap.
// A failing command returns a wrapped error (with the command's trimmed
// stderr), including on the over-limit branch: killing an over-running child
// is not itself a failure, so a child the kill reached returns the prefix
// with overLimit and no error, while a child that had already exited
// non-zero returns its exit status and stderr beside that same prefix, so
// callers must check err before overLimit. A child that exits on its own is
// *exited* and a child this function kills is *signaled*, which is what
// separates the two; a still-running child that would have failed is
// indistinguishable from one that would have succeeded, and is not guessed at.
// A canceled context that reached the command returns its own error
// and never overLimit, even when the output had already exceeded the limit
// before the cancellation or a surviving grandchild left the stdout pipe open.
// A read that wins the select against an already-expired context does not
// settle cancellation either: a completed read never survives a dead context,
// so bytes read past the deadline are discarded rather than returned.
func runGitCapped(ctx context.Context, dir string, limit int, args ...string) (out []byte, overLimit bool, err error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	// cmd.Stderr is an io.Writer, so os/exec copies the child's stderr through a
	// goroutine that Wait blocks on: a surviving grandchild holding the stderr
	// write end would stall Wait itself, past the context. The delay bounds that
	// child-kill wait.
	cmd.WaitDelay = 2 * time.Second
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
	// io.LimitReader bounds bytes, not blocking, so the read runs off-thread: a
	// grandchild that outlives git holds the pipe write end open with no output
	// and would park the reader forever, past the context. On cancellation the
	// read end is closed to unblock the goroutine, its result drained so it
	// cannot leak, and the child reaped before returning; that order is
	// load-bearing, because Wait closes the same descriptor and a close on an
	// already-closed one no longer evicts the parked reader.
	read := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(io.LimitReader(stdout, int64(limit)+1))
		read <- data
	}()
	select {
	case out = <-read:
	case <-ctx.Done():
		_ = stdout.Close()
		<-read
		_ = cmd.Wait()
		return nil, false, ctx.Err()
	}
	if len(out) > limit {
		_ = cmd.Process.Kill()
		waitErr := cmd.Wait()
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		// Stopping an over-running child is not itself a failure, so a child
		// the kill reached is signaled rather than exited and the bounded
		// prefix stands. A child that had already exited on its own reaps with
		// its real status — the kill above is a no-op against a pid that is
		// already a zombie — and a truncated prefix of a failed command is
		// not a result, so the failure is surfaced beside the prefix instead
		// of reported as a clean over-limit.
		if state := cmd.ProcessState; state != nil && state.Exited() && !state.Success() {
			return out, true, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), waitErr, strings.TrimSpace(string(stderr.data)))
		}
		return out, true, nil
	}
	// The reap comes first, deliberately: this tail returns on the context as
	// well as on Wait, and skipping Wait strands the child as a zombie and
	// leaves os/exec's watchCtx goroutine blocked forever, so the guard below
	// decides only what is returned, never whether the child is reaped.
	waitErr := cmd.Wait()
	// The select settles cancellation in its Done branch, but a read that
	// completes under an already-expired context leaves both channels ready and
	// the tie is broken at random, so this success path can be handed bytes
	// produced past the deadline. A read past the deadline is not a result, and
	// the guard that answers that is observed once, after the reap and ahead of
	// every return below, so the expired context wins all of them. The #421 rule
	// that follows — a grandchild-held pipe timing out Wait on a child that
	// already exited successfully is our timeout, not git's, and its bytes stand
	// — is a statement about a live context, so it applies only once this guard
	// has passed. Its branch already answered on the context itself before this
	// guard existed; hoisting keeps one observation, not two, per call.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, false, ctxErr
	}
	if waitErr != nil {
		// ErrWaitDelay arrives only for a process that exited successfully with
		// its pipes still open — a surviving grandchild — and ProcessState
		// confirms that here rather than leaning on that precedence. stdout
		// reached EOF, so the bytes are the complete result and the timeout is
		// ours, not git's: keep them. A child that exited non-zero keeps its
		// exit status in the wrapped error below.
		if errors.Is(waitErr, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
			return out, false, nil
		}
		return nil, false, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), waitErr, strings.TrimSpace(string(stderr.data)))
	}
	return out, false, nil
}

// stderrLimit bounds the child stderr captured by runGit and runGitCapped so
// a verbose or failing git cannot allocate unbounded memory there either.
const stderrLimit = 4 * 1024

// cappedBuffer is an io.Writer that keeps at most its limit bytes, silently
// discarding anything written beyond them.
type cappedBuffer struct {
	limit int
	data  []byte
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	// The full length is always reported as consumed: returning the truncated
	// count once the limit is reached mid-chunk would be a short write, which
	// stops io.Copy — and the exec copy goroutine closes the pipe when its
	// copy stops, SIGPIPE-killing a child whose verbose stderr straddled the
	// cap instead of merely discarding the excess.
	room := b.limit - len(b.data)
	if room > 0 {
		if room < len(p) {
			b.data = append(b.data, p[:room]...)
		} else {
			b.data = append(b.data, p...)
		}
	}
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
