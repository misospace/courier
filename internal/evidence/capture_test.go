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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/misospace/courier/internal/log"
)

// TestMain isolates git from the developer's global and system config and pins
// a committer identity for the whole package, mirroring internal/git: the
// capture tests shell out to real git and must not inherit workstation
// settings such as commit.gpgsign.
func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "courier-evidence-test-")
	if err != nil {
		panic(err)
	}
	for key, value := range map[string]string{
		"GIT_CONFIG_GLOBAL":   filepath.Join(home, "gitconfig"),
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME":     "Courier Test",
		"GIT_AUTHOR_EMAIL":    "courier-test@example.invalid",
		"GIT_COMMITTER_NAME":  "Courier Test",
		"GIT_COMMITTER_EMAIL": "courier-test@example.invalid",
	} {
		if err := os.Setenv(key, value); err != nil {
			panic(err)
		}
	}
	code := m.Run()
	_ = os.RemoveAll(home)
	os.Exit(code)
}

const (
	testSecret        = "ghu_0123456789abcdefABCDEF"
	testPatternSecret = "ghp_AAAAAAAAAAAAAAAAAAAA"
)

type wantEntry struct {
	path       string // manifest path to look up; "" means "the single commit entry"
	class      string
	dispo      string
	linkTarget string // compared when non-empty
	escaping   *bool  // compared when non-nil
	bytes      int64  // compared when bytesSet is true
	bytesSet   bool
}

type captureCase struct {
	name  string
	setup func(t *testing.T, dir string) *Scanner
	// opts overrides the default captureOptions when set.
	opts func(dir string, scanner *Scanner) Options
	// skipEntryCheck skips the single-entry lookup and its dependent
	// assertions for cases that assert several entries in their check.
	skipEntryCheck bool
	want           wantEntry
	wantTar        map[string]string
	// ignoreStoredBytes skips StoredBytes in the exact Totals comparison for
	// cases whose admitted patch size is not known ahead of time; those cases
	// still assert StoredBytes equals the stored entry's Bytes and is positive.
	ignoreStoredBytes bool
	wantTotals        Totals
	wantAbsent        []string
	check             func(t *testing.T, bundle *Bundle, members map[string]string)
}

// TestCaptureAcceptance is the issue #197 acceptance table: every entry class
// and disposition, both fail-closed scan paths (content and path), and the
// shared file/commit admission caps. Each case builds its own git worktree so
// subtests run in parallel.
func TestCaptureAcceptance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var startSHA string
	cases := []captureCase{
		{
			name: "untracked small text file is stored",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFile(t, filepath.Join(dir, "notes.txt"), "hello evidence\n")
				return nil
			},
			want:       wantEntry{path: "notes.txt", class: ClassUntracked, dispo: DispositionStored, bytes: int64(len("hello evidence\n")), bytesSet: true},
			wantTar:    map[string]string{"notes.txt": "hello evidence\n"},
			wantTotals: Totals{Files: 1, Stored: 1, StoredBytes: int64(len("hello evidence\n"))},
		},
		{
			name: "tracked modified file is stored",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFile(t, filepath.Join(dir, "tracked.txt"), "changed content\n")
				return nil
			},
			want:       wantEntry{path: "tracked.txt", class: ClassModified, dispo: DispositionStored, bytes: int64(len("changed content\n")), bytesSet: true},
			wantTar:    map[string]string{"tracked.txt": "changed content\n"},
			wantTotals: Totals{Files: 1, Stored: 1, StoredBytes: int64(len("changed content\n"))},
		},
		{
			name: "untracked file over MaxFileBytes is omitted over limit",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFile(t, filepath.Join(dir, "big.txt"), "OVERSIZE-NEEDLE-"+strings.Repeat("q", MaxFileBytes))
				writeFile(t, filepath.Join(dir, "sib.txt"), "sibling one\n")
				return nil
			},
			want:       wantEntry{path: "big.txt", class: ClassUntracked, dispo: DispositionOmittedOverLimit, bytes: int64(MaxFileBytes) + int64(len("OVERSIZE-NEEDLE-")), bytesSet: true},
			wantTar:    map[string]string{"sib.txt": "sibling one\n"},
			wantTotals: Totals{Files: 2, Stored: 1, OmittedOverLimit: 1, StoredBytes: int64(len("sibling one\n"))},
			wantAbsent: []string{"OVERSIZE-NEEDLE"},
		},
		{
			name: "binary file with NUL byte is omitted",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFileBytes(t, filepath.Join(dir, "nul.bin"), []byte("pre\x00post"))
				writeFile(t, filepath.Join(dir, "sib.txt"), "sibling two\n")
				return nil
			},
			want:       wantEntry{path: "nul.bin", class: ClassUntracked, dispo: DispositionOmittedBinary, bytes: 8, bytesSet: true},
			wantTar:    map[string]string{"sib.txt": "sibling two\n"},
			wantTotals: Totals{Files: 2, Stored: 1, OmittedBinary: 1, StoredBytes: int64(len("sibling two\n"))},
			wantAbsent: []string{"pre\x00post"},
		},
		{
			name: "binary file with invalid UTF-8 and no NUL is omitted",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFileBytes(t, filepath.Join(dir, "invalid.bin"), bytes.Repeat([]byte{0xFF, 0xFE}, 16))
				writeFile(t, filepath.Join(dir, "sib.txt"), "sibling three\n")
				return nil
			},
			want:       wantEntry{path: "invalid.bin", class: ClassUntracked, dispo: DispositionOmittedBinary, bytes: 32, bytesSet: true},
			wantTar:    map[string]string{"sib.txt": "sibling three\n"},
			wantTotals: Totals{Files: 2, Stored: 1, OmittedBinary: 1, StoredBytes: int64(len("sibling three\n"))},
			wantAbsent: []string{"\xff\xfe"},
		},
		{
			name: "registered credential in file content is withheld",
			setup: func(t *testing.T, dir string) *Scanner {
				scanner := NewScanner()
				scanner.RegisterCredentials(map[string]string{"GITHUB_TOKEN": testSecret})
				writeFile(t, filepath.Join(dir, "leaky.txt"), "token: "+testSecret+"\n")
				writeFile(t, filepath.Join(dir, "clean.txt"), "nothing to see\n")
				return scanner
			},
			want:       wantEntry{path: "leaky.txt", class: ClassUntracked, dispo: DispositionWithheld, bytes: 0, bytesSet: true},
			wantTar:    map[string]string{"clean.txt": "nothing to see\n"},
			wantTotals: Totals{Files: 2, Withheld: 1, Stored: 1, StoredBytes: int64(len("nothing to see\n"))},
			wantAbsent: []string{testSecret},
		},
		{
			name: "pattern-table secret in file content is withheld without registration",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFile(t, filepath.Join(dir, "cfg.txt"), "token = "+testPatternSecret+"\n")
				return nil
			},
			want:       wantEntry{path: "cfg.txt", class: ClassUntracked, dispo: DispositionWithheld, bytes: 0, bytesSet: true},
			wantTar:    map[string]string{},
			wantTotals: Totals{Files: 1, Withheld: 1},
			wantAbsent: []string{testPatternSecret},
		},
		{
			// Fail-closed on matching PATHS: a file whose name carries a
			// credential shape is withheld even with clean content, because
			// storing it would persist the secret in the tar member name.
			name: "pattern-table secret in file path is withheld",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFileMkdir(t, filepath.Join(dir, "cfg", testPatternSecret+".conf"), "hello")
				writeFile(t, filepath.Join(dir, "sib.txt"), "sibling four\n")
				return nil
			},
			want:       wantEntry{path: log.RedactedPlaceholder, class: ClassUntracked, dispo: DispositionWithheld, bytes: 0, bytesSet: true},
			wantTar:    map[string]string{"sib.txt": "sibling four\n"},
			wantTotals: Totals{Files: 2, Withheld: 1, Stored: 1, StoredBytes: int64(len("sibling four\n"))},
			wantAbsent: []string{testPatternSecret, "hello"},
			check: func(t *testing.T, bundle *Bundle, members map[string]string) {
				entry := bundle.Manifest.Entries[0]
				if entry.Path != log.RedactedPlaceholder {
					t.Errorf("manifest path = %q, want %q", entry.Path, log.RedactedPlaceholder)
				}
				if entry.StoredPath != "" {
					t.Errorf("withheld entry carries StoredPath %q", entry.StoredPath)
				}
			},
		},
		{
			name: "internal symlink is metadata only",
			setup: func(t *testing.T, dir string) *Scanner {
				if err := os.Symlink("tracked.txt", filepath.Join(dir, "link")); err != nil {
					t.Fatal(err)
				}
				return nil
			},
			want:       wantEntry{path: "link", class: ClassUntracked, dispo: DispositionSymlink, linkTarget: "tracked.txt", escaping: boolPtr(false), bytes: 0, bytesSet: true},
			wantTar:    map[string]string{},
			wantTotals: Totals{Files: 1, Symlinks: 1},
		},
		{
			name: "escaping symlink is flagged and never stored",
			setup: func(t *testing.T, dir string) *Scanner {
				if err := os.Symlink("../../../../etc/passwd", filepath.Join(dir, "escape")); err != nil {
					t.Fatal(err)
				}
				return nil
			},
			want:       wantEntry{path: "escape", class: ClassUntracked, dispo: DispositionSymlink, linkTarget: "../../../../etc/passwd", escaping: boolPtr(true), bytes: 0, bytesSet: true},
			wantTar:    map[string]string{},
			wantTotals: Totals{Files: 1, Symlinks: 1},
		},
		{
			name: "absolute symlink target is flagged escaping",
			setup: func(t *testing.T, dir string) *Scanner {
				if err := os.Symlink("/etc/passwd", filepath.Join(dir, "abs-link")); err != nil {
					t.Fatal(err)
				}
				return nil
			},
			want:       wantEntry{path: "abs-link", class: ClassUntracked, dispo: DispositionSymlink, linkTarget: "/etc/passwd", escaping: boolPtr(true), bytes: 0, bytesSet: true},
			wantTar:    map[string]string{},
			wantTotals: Totals{Files: 1, Symlinks: 1},
		},
		{
			name: "deleted tracked file carries no content",
			setup: func(t *testing.T, dir string) *Scanner {
				if err := os.Remove(filepath.Join(dir, "tracked.txt")); err != nil {
					t.Fatal(err)
				}
				return nil
			},
			want:       wantEntry{path: "tracked.txt", class: ClassDeleted, dispo: DispositionDeleted, bytes: 0, bytesSet: true},
			wantTar:    map[string]string{},
			wantTotals: Totals{Files: 1, Deleted: 1},
		},
		{
			name: "symlink target containing registered credential is redacted",
			setup: func(t *testing.T, dir string) *Scanner {
				scanner := NewScanner()
				scanner.RegisterCredentials(map[string]string{"GITHUB_TOKEN": "sup3rs3cr3tvalue"})
				if err := os.Symlink("data/sup3rs3cr3tvalue", filepath.Join(dir, "link")); err != nil {
					t.Fatal(err)
				}
				return scanner
			},
			want:       wantEntry{path: "link", class: ClassUntracked, dispo: DispositionSymlink, linkTarget: log.RedactedPlaceholder, escaping: boolPtr(false), bytes: 0, bytesSet: true},
			wantTar:    map[string]string{},
			wantTotals: Totals{Files: 1, Symlinks: 1},
			wantAbsent: []string{"sup3rs3cr3tvalue"},
		},
		{
			name: "local commit is stored as patch",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFile(t, filepath.Join(dir, "feature.go"), "package feature\n")
				commit(t, dir, "feat: local work ahead of upstream")
				return nil
			},
			want:              wantEntry{class: ClassCommit, dispo: DispositionStored},
			wantTotals:        Totals{Commits: 1, Stored: 1},
			ignoreStoredBytes: true,
			check: func(t *testing.T, bundle *Bundle, members map[string]string) {
				entry := bundle.Manifest.Entries[0]
				if !strings.HasPrefix(entry.StoredPath, "commits/0001-") || !strings.HasSuffix(entry.StoredPath, ".patch") {
					t.Fatalf("storedPath = %q, want commits/0001-<shortsha>.patch", entry.StoredPath)
				}
				patch, ok := members[entry.StoredPath]
				if !ok {
					t.Fatalf("tar missing commit patch member %q", entry.StoredPath)
				}
				if !strings.Contains(patch, "feat: local work ahead of upstream") {
					t.Errorf("patch does not carry the commit message: %q", patch)
				}
				if bundle.Manifest.Totals.StoredBytes != int64(len(patch)) {
					t.Errorf("totals.storedBytes = %d, want %d", bundle.Manifest.Totals.StoredBytes, len(patch))
				}
			},
		},
		{
			// A substantially-oversized text commit: the patch is capped
			// while it is being produced, so neither the file content nor the
			// commit message may reach the archive or the manifest, and no
			// commit member is admitted.
			name: "local commit over MaxFileBytes patch size is omitted",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFile(t, filepath.Join(dir, "huge.txt"), "COMMIT-OVER-CAP-NEEDLE-"+strings.Repeat("x", MaxFileBytes+2))
				commit(t, dir, "feat: enormous file COMMIT-OVER-CAP-MSG-NEEDLE")
				return nil
			},
			want:       wantEntry{class: ClassCommit, dispo: DispositionOmittedOverLimit, bytes: int64(MaxFileBytes) + 1, bytesSet: true},
			wantTar:    map[string]string{},
			wantTotals: Totals{Commits: 1, OmittedOverLimit: 1},
			wantAbsent: []string{"COMMIT-OVER-CAP-NEEDLE", "COMMIT-OVER-CAP-MSG-NEEDLE"},
			check: func(t *testing.T, bundle *Bundle, members map[string]string) {
				for name := range members {
					if strings.HasPrefix(name, "commits/") {
						t.Errorf("archive carries commit member %q for a capped patch", name)
					}
				}
			},
		},
		{
			name: "local commit adding a binary file is stubbed as text",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFileBytes(t, filepath.Join(dir, "blob.dat"), repeatedMarker(64))
				commit(t, dir, "feat: add binary blob")
				return nil
			},
			want:              wantEntry{class: ClassCommit, dispo: DispositionStored},
			wantTotals:        Totals{Commits: 1, Stored: 1},
			ignoreStoredBytes: true,
			wantAbsent:        []string{binaryMarker},
			check: func(t *testing.T, bundle *Bundle, members map[string]string) {
				entry := bundle.Manifest.Entries[0]
				patch, ok := members[entry.StoredPath]
				if !ok {
					t.Fatalf("tar missing commit patch member %q", entry.StoredPath)
				}
				if !utf8.ValidString(patch) || strings.ContainsRune(patch, 0) {
					t.Errorf("--no-binary patch is not valid UTF-8 text: %q", patch[:min(len(patch), 120)])
				}
				if !strings.Contains(patch, "Binary files") {
					t.Errorf("--no-binary patch lacks the binary stub: %q", patch[:min(len(patch), 120)])
				}
			},
		},
		{
			name: "local commit with registered credential in message is withheld",
			setup: func(t *testing.T, dir string) *Scanner {
				scanner := NewScanner()
				scanner.RegisterCredentials(map[string]string{"GITHUB_TOKEN": testSecret})
				writeFile(t, filepath.Join(dir, "rotate.txt"), "rotate the token\n")
				commit(t, dir, "chore: rotate "+testSecret)
				return scanner
			},
			want:       wantEntry{class: ClassCommit, dispo: DispositionWithheld},
			wantTar:    map[string]string{},
			wantTotals: Totals{Commits: 1, Withheld: 1},
			wantAbsent: []string{testSecret},
		},
		{
			// Commits draw from the SAME MaxContentEntries/MaxTotalBytes
			// counters as files: with the content-entry cap already exhausted
			// by files, an otherwise-tiny commit is omitted-over-limit.
			name: "local commit omitted when files exhaust the shared content cap",
			setup: func(t *testing.T, dir string) *Scanner {
				for i := range MaxContentEntries {
					writeFile(t, filepath.Join(dir, fmt.Sprintf("f%03d.txt", i)), "x")
				}
				writeFile(t, filepath.Join(dir, "commit-me.txt"), "y\n")
				commitPaths(t, dir, "chore: only this file", "commit-me.txt")
				return nil
			},
			want:       wantEntry{class: ClassCommit, dispo: DispositionOmittedOverLimit},
			wantTar:    tinyTar(MaxContentEntries),
			wantTotals: Totals{Files: MaxContentEntries, Commits: 1, Stored: MaxContentEntries, OmittedOverLimit: 1, StoredBytes: MaxContentEntries},
			check: func(t *testing.T, bundle *Bundle, members map[string]string) {
				for name := range members {
					if strings.HasPrefix(name, "commits/") {
						t.Errorf("archive carries commit patch %q despite exhausted content cap", name)
					}
				}
			},
		},
		{
			// A rename records its origin as a separate NUL record that is
			// consumed as part of the same entry: exactly one change, the
			// new path, never a second entry for the origin.
			name: "tracked rename is one entry with the origin path consumed",
			setup: func(t *testing.T, dir string) *Scanner {
				git(t, dir, "mv", "tracked.txt", "renamed.txt")
				return nil
			},
			want:       wantEntry{path: "renamed.txt", class: ClassRenamed, dispo: DispositionStored, bytes: int64(len("base content\n")), bytesSet: true},
			wantTar:    map[string]string{"renamed.txt": "base content\n"},
			wantTotals: Totals{Files: 1, Stored: 1, StoredBytes: int64(len("base content\n"))},
		},
		{
			// UpstreamRef unresolvable: the capture falls back to StartSHA
			// and discovers the commit made after the run start.
			name: "local commit discovered via StartSHA when upstream ref is unresolvable",
			setup: func(t *testing.T, dir string) *Scanner {
				out, err := gitOutput(dir, "rev-parse", "HEAD")
				if err != nil {
					t.Fatal(err)
				}
				startSHA = strings.TrimSpace(string(out))
				writeFile(t, filepath.Join(dir, "later.txt"), "later work\n")
				commit(t, dir, "feat: work after start")
				return nil
			},
			opts: func(dir string, scanner *Scanner) Options {
				return Options{
					Directory:   dir,
					UpstreamRef: "origin/does-not-exist",
					StartSHA:    startSHA,
					Trigger:     TriggerTerminal,
					Scanner:     scanner,
				}
			},
			want:              wantEntry{class: ClassCommit, dispo: DispositionStored},
			wantTotals:        Totals{Commits: 1, Stored: 1},
			ignoreStoredBytes: true,
			check: func(t *testing.T, bundle *Bundle, members map[string]string) {
				entry := findCommitEntry(t, bundle)
				if !strings.HasPrefix(entry.StoredPath, "commits/0001-") || !strings.HasSuffix(entry.StoredPath, ".patch") {
					t.Errorf("storedPath = %q, want commits/0001-<shortsha>.patch", entry.StoredPath)
				}
				if _, ok := members[entry.StoredPath]; !ok {
					t.Errorf("tar missing commit patch member %q", entry.StoredPath)
				}
			},
		},
		{
			// Two commits ahead: patches are numbered ascending with history
			// (oldest first) and both are admitted to the tar.
			name: "two local commits are numbered ascending with history",
			setup: func(t *testing.T, dir string) *Scanner {
				writeFile(t, filepath.Join(dir, "one.txt"), "one\n")
				commit(t, dir, "feat: first change")
				writeFile(t, filepath.Join(dir, "two.txt"), "two\n")
				commit(t, dir, "feat: second change")
				return nil
			},
			skipEntryCheck:    true,
			wantTotals:        Totals{Commits: 2, Stored: 2},
			ignoreStoredBytes: true,
			check: func(t *testing.T, bundle *Bundle, members map[string]string) {
				var commits []Entry
				for _, e := range bundle.Manifest.Entries {
					if e.Class == ClassCommit {
						commits = append(commits, e)
					}
				}
				if len(commits) != 2 {
					t.Fatalf("manifest has %d commit entries, want 2: %+v", len(commits), bundle.Manifest.Entries)
				}
				wantMessages := []string{"feat: first change", "feat: second change"}
				for i, e := range commits {
					if e.Disposition != DispositionStored {
						t.Errorf("commit %d disposition = %q, want stored", i, e.Disposition)
						continue
					}
					prefix := fmt.Sprintf("commits/%04d-", i+1)
					if !strings.HasPrefix(e.StoredPath, prefix) || !strings.HasSuffix(e.StoredPath, ".patch") {
						t.Errorf("commit %d storedPath = %q, want %s<shortsha>.patch", i, e.StoredPath, prefix)
						continue
					}
					patch, ok := members[e.StoredPath]
					if !ok {
						t.Errorf("tar missing commit patch member %q", e.StoredPath)
						continue
					}
					if !strings.Contains(patch, wantMessages[i]) {
						t.Errorf("patch %q does not carry message %q", e.StoredPath, wantMessages[i])
					}
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := newRepo(t)
			scanner := tc.setup(t, dir)
			opts := captureOptions(dir, scanner)
			if tc.opts != nil {
				opts = tc.opts(dir, scanner)
			}
			bundle, err := Capture(ctx, opts)
			if err != nil {
				t.Fatalf("capture: %v", err)
			}

			var entry Entry
			if !tc.skipEntryCheck {
				if tc.want.path != "" {
					entry = findEntry(t, bundle, tc.want.path)
				} else {
					entry = findCommitEntry(t, bundle)
					if !strings.HasPrefix(entry.Path, "commits/0001-") || !strings.HasSuffix(entry.Path, ".patch") {
						t.Errorf("commit entry path = %q, want commits/0001-<shortsha>.patch", entry.Path)
					}
				}
				if entry.Class != tc.want.class {
					t.Errorf("class = %q, want %q", entry.Class, tc.want.class)
				}
				if entry.Disposition != tc.want.dispo {
					t.Errorf("disposition = %q, want %q", entry.Disposition, tc.want.dispo)
				}
				if tc.want.linkTarget != "" && entry.LinkTarget != tc.want.linkTarget {
					t.Errorf("linkTarget = %q, want %q", entry.LinkTarget, tc.want.linkTarget)
				}
				if tc.want.escaping != nil && entry.Escaping != *tc.want.escaping {
					t.Errorf("escaping = %v, want %v", entry.Escaping, *tc.want.escaping)
				}
				if tc.want.bytesSet && entry.Bytes != tc.want.bytes {
					t.Errorf("bytes = %d, want %d", entry.Bytes, tc.want.bytes)
				}
				if entry.Disposition != DispositionStored && entry.StoredPath != "" {
					t.Errorf("non-stored entry carries StoredPath %q", entry.StoredPath)
				}
			}

			members := untar(t, bundle.Archive)
			if !tc.skipEntryCheck {
				if _, ok := members[entry.StoredPath]; ok != (entry.Disposition == DispositionStored) {
					t.Errorf("tar membership for %q disagrees with disposition %q", entry.StoredPath, entry.Disposition)
				}
			}
			for name, content := range tc.wantTar {
				got, ok := members[name]
				if !ok {
					t.Errorf("tar missing member %q", name)
					continue
				}
				if got != content {
					t.Errorf("tar member %q = %q, want %q", name, got, content)
				}
			}
			expected := make(map[string]bool, len(tc.wantTar)+1)
			for name := range tc.wantTar {
				expected[name] = true
			}
			if !tc.skipEntryCheck && entry.Disposition == DispositionStored {
				expected[entry.StoredPath] = true
			}
			if !tc.skipEntryCheck && len(members) != len(expected) {
				t.Errorf("tar has %d members, want %d", len(members), len(expected))
			}
			for name := range expected {
				if _, ok := members[name]; !ok {
					t.Errorf("tar missing expected member %q", name)
				}
			}
			assertAbsent(t, bundle, tc.wantAbsent)

			gotTotals := bundle.Manifest.Totals
			wantTotals := tc.wantTotals
			if tc.ignoreStoredBytes {
				if !tc.skipEntryCheck {
					if gotTotals.StoredBytes <= 0 || gotTotals.StoredBytes != entry.Bytes {
						t.Errorf("totals.storedBytes = %d, want positive and equal to entry bytes %d", gotTotals.StoredBytes, entry.Bytes)
					}
				}
				gotTotals.StoredBytes, wantTotals.StoredBytes = 0, 0
			}
			if gotTotals != wantTotals {
				t.Errorf("totals = %+v, want %+v", gotTotals, wantTotals)
			}
			if tc.check != nil {
				tc.check(t, bundle, members)
			}
		})
	}
}

// TestReadCappedNoFollow pins the issue #237 primitive: a path that is a
// symlink when opened — as after a regular-file-to-symlink swap in the
// window between Lstat classification and the read — fails with ELOOP and
// yields no content; a plain regular file still reads through the cap.
func TestReadCappedNoFollow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	regular := filepath.Join(dir, "regular.txt")
	writeFile(t, regular, "regular content\n")
	data, err := readCapped(context.Background(), regular, MaxFileBytes+1)
	if err != nil {
		t.Fatalf("readCapped regular file: %v", err)
	}
	if string(data) != "regular content\n" {
		t.Errorf("readCapped regular file = %q, want %q", data, "regular content\n")
	}

	secret := filepath.Join(dir, "secret.txt")
	writeFile(t, secret, "SECRET-TARGET-CONTENT\n")
	victim := filepath.Join(dir, "victim.txt")
	writeFile(t, victim, "original bytes\n")
	// Simulate the swap that raced in after classification: the path the
	// read now opens is a symlink the classification pass never saw.
	if err := os.Remove(victim); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, victim); err != nil {
		t.Fatal(err)
	}
	data, err = readCapped(context.Background(), victim, MaxFileBytes+1)
	if !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("readCapped swapped symlink err = %v, want ELOOP", err)
	}
	if data != nil {
		t.Errorf("readCapped returned %q for a swapped symlink, want nil", data)
	}
}

// TestCaptureSymlinkSwapRace is the issue #237 regression: a hostile worktree
// swaps a classified-regular file for an escaping symlink in the window
// between the Lstat classification and the read. The capture must store
// symlink metadata only, never the target's bytes.
func TestCaptureSymlinkSwapRace(t *testing.T) {
	// Not parallel: the test replaces a package-level seam that parallel
	// captures share.
	dir := newRepo(t)
	writeFile(t, filepath.Join(dir, "swap.txt"), "SWAP-RACE-ORIGINAL\n")

	original := readFileForCapture
	t.Cleanup(func() { readFileForCapture = original })
	readFileForCapture = func(ctx context.Context, path string, limit int) ([]byte, error) {
		// The swap lands after Lstat classified the path as regular and
		// before the O_NOFOLLOW open.
		if err := os.Remove(path); err != nil {
			t.Errorf("swap remove: %v", err)
		}
		if err := os.Symlink("../../../../etc/shadow", path); err != nil {
			t.Errorf("swap symlink: %v", err)
		}
		return original(ctx, path, limit)
	}

	bundle, err := Capture(context.Background(), captureOptions(dir, nil))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	entry := findEntry(t, bundle, "swap.txt")
	if entry.Class != ClassUntracked {
		t.Errorf("class = %q, want %q", entry.Class, ClassUntracked)
	}
	if entry.Disposition != DispositionSymlink {
		t.Errorf("disposition = %q, want %q", entry.Disposition, DispositionSymlink)
	}
	if entry.LinkTarget != "../../../../etc/shadow" {
		t.Errorf("linkTarget = %q, want %q", entry.LinkTarget, "../../../../etc/shadow")
	}
	if !entry.Escaping {
		t.Errorf("escaping = false, want true for a target outside the repository root")
	}
	if entry.StoredPath != "" {
		t.Errorf("symlink entry carries StoredPath %q", entry.StoredPath)
	}
	if entry.Bytes != 0 {
		t.Errorf("symlink entry carries Bytes %d, want 0", entry.Bytes)
	}
	if members := untar(t, bundle.Archive); len(members) != 0 {
		t.Errorf("archive carries %d members for a swapped symlink: %v", len(members), members)
	}
	if got := bundle.Manifest.Totals; got.Files != 1 || got.Symlinks != 1 || got.Stored != 0 {
		t.Errorf("totals = %+v, want Files 1, Symlinks 1, Stored 0", got)
	}
	assertAbsent(t, bundle, []string{"SWAP-RACE-ORIGINAL"})
}

// TestReadCappedPrefersCancellationOverTheNonRegularGuard pins the #390
// composition that survives #389: a context that is already done is answered
// from the context, so a deadline never arrives as a read failure for Capture
// to classify — cancellation is settled ahead of the read-error taxonomy, and
// that ordering is what returns the error the executor degrades on. A
// readCapped that took a context and ignored it would answer errNotRegular
// here and fail.
//
// The bound's other half — abandoning a read already blocked in the kernel —
// is not constructible here and this test does not claim it. #389 opens
// O_NONBLOCK and refuses a non-regular descriptor with errNotRegular before any
// read begins, so the fifo that used to stand in for a stalled read now
// returns promptly, and the only read left that can stall is one of a regular
// file on a hung filesystem, which a unit test cannot construct without a
// privileged mount. Nor does this test pin the pre-check that keeps the fast
// path free of goroutines, and the reason is a measurement rather than an
// argument: #389 answers a fifo in two syscalls, so without the pre-check the
// goroutine's errNotRegular and the already-done context are sometimes both
// ready at the select, which breaks that tie at random. Measured over 500k
// calls, the pre-check's presence is the difference between 0 wrong answers and
// about 20 — roughly 1 in 25,000, well below what any test here can detect. The
// pre-check is therefore kept for what it actually buys, which is a fast path
// that starts no goroutine and issues no syscalls at all, not because this
// test can fail without it. What the bound buys is observable end to end, and
// TestCaptureReadPhaseBoundedByDeadline pins that through the seam.
func TestReadCappedPrefersCancellationOverTheNonRegularGuard(t *testing.T) {
	t.Parallel()
	// A fifo: refused for what it is, but only once the filesystem is
	// actually reached, which is what makes it the discriminator below.
	fifo := filepath.Join(t.TempDir(), "refused.fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}

	// A live context reaches the filesystem and is refused for being a fifo,
	// so the canceled case below answers from the context rather than
	// repeating this result by accident.
	if _, err := readCapped(context.Background(), fifo, MaxFileBytes+1); !errors.Is(err, errNotRegular) {
		t.Fatalf("readCapped fifo err = %v, want %v", err, errNotRegular)
	}

	type readResult struct {
		data []byte
		err  error
	}
	// The read runs off the test goroutine, so a regression that never
	// returns fails on the guard below instead of hanging the binary.
	done := make(chan readResult, 1)
	dead, stop := context.WithCancel(context.Background())
	stop()
	go func() {
		data, err := readCapped(dead, fifo, MaxFileBytes+1)
		done <- readResult{data: data, err: err}
	}()
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) {
			t.Errorf("readCapped with a canceled context err = %v, want context.Canceled", got.err)
		}
		if got.data != nil {
			t.Errorf("readCapped with a canceled context returned %d bytes, want nil", len(got.data))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("readCapped did not return within 10s of a canceled context")
	}
}

// lateDeadlineContext stands in for a context whose deadline expires while a
// read is already in flight: Err answers nil the first time it is observed —
// the pre-check at the top of readCapped, which must see a live context or the
// read never starts — and the deadline error from every observation after,
// which is when the select and the result guard run.
type lateDeadlineContext struct {
	observed atomic.Bool
	done     chan struct{}
}

func (c *lateDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *lateDeadlineContext) Done() <-chan struct{}       { return c.done }
func (c *lateDeadlineContext) Value(any) any               { return nil }
func (c *lateDeadlineContext) Err() error {
	if c.observed.CompareAndSwap(false, true) {
		return nil
	}
	return context.DeadlineExceeded
}

// TestReadCappedPrefersCancellationOverACompletedRead pins the #390 review
// precedence inside readCapped: a read that completes while the context is
// expired is discarded, even when the result wins the select — both channels
// are then ready and the tie is broken at random, so the guard after the
// select, not the select itself, is what keeps a completed read from
// surviving a dead context.
//
// The double's Done closes only after a delay, so the select deterministically
// sees the completed result first and the post-select guard is what decides;
// on a pathologically slow runner the Done branch answers instead, which the
// outcome below accepts either way.
func TestReadCappedPrefersCancellationOverACompletedRead(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "late.txt")
	writeFile(t, path, "READ-AFTER-DEADLINE\n")

	ctx := &lateDeadlineContext{done: make(chan struct{})}
	timer := time.AfterFunc(10*time.Millisecond, func() { close(ctx.done) })
	defer timer.Stop()
	data, err := readCapped(ctx, path, MaxFileBytes+1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("readCapped err = %v, want context.DeadlineExceeded", err)
	}
	if data != nil {
		t.Errorf("readCapped returned %q past its context, want nil", data)
	}
}

// TestCaptureNeverAcceptsAReadThatFinishedUnderADeadContext is the #390
// review regression at the seam: a read that completes after the context is
// already dead — a result that won the select against a ready Done, or any
// seam implementation — must be discarded, and Capture must fail with the
// context's error and no bundle, so the executor's manifest-only degradation
// is what the caller sees rather than content read past the deadline. The
// seam answers deterministically, so the precedence Capture owes is pinned
// without betting on the scheduler.
func TestCaptureNeverAcceptsAReadThatFinishedUnderADeadContext(t *testing.T) {
	// Not parallel: the test replaces a package-level seam that parallel
	// captures share.
	dir := newRepo(t)
	writeFile(t, filepath.Join(dir, "late.txt"), "READ-AFTER-DEADLINE\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	original := readFileForCapture
	t.Cleanup(func() { readFileForCapture = original })
	readFileForCapture = func(_ context.Context, _ string, _ int) ([]byte, error) {
		// The deadline expires while the read is in flight, and the read
		// completes anyway — the interleaving the select loses at random.
		cancel()
		return []byte("READ-AFTER-DEADLINE\n"), nil
	}

	bundle, err := Capture(ctx, captureOptions(dir, nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("capture err = %v, want context.Canceled", err)
	}
	if bundle != nil {
		t.Errorf("capture returned a bundle alongside cancellation, want nil so the caller degrades to manifest-only")
	}
}

// TestCaptureReadPhaseBoundedByDeadline is the issue #390 regression: a read
// phase that stalls past the operation deadline must fail the capture with the
// context's error and no bundle, so the executor's manifest-only degradation
// is reachable instead of the capture running on to SIGKILL. It pins the
// Capture-side half, where the swapped seam stands in for the stalled read;
// readCapped's own bounding of a kernel-blocked read is not unit-constructible
// — #389's guard refuses the fifo before any read begins — so see
// TestReadCappedPrefersCancellationOverTheNonRegularGuard for the part of it
// that is observable.
func TestCaptureReadPhaseBoundedByDeadline(t *testing.T) {
	// Not parallel: the test replaces a package-level seam that parallel
	// captures share.
	dir := newRepo(t)
	writeFile(t, filepath.Join(dir, "stall.txt"), "READ-PHASE-STALL\n")

	original := readFileForCapture
	t.Cleanup(func() { readFileForCapture = original })
	// reading proves the capture reached the read phase, so the deadline this
	// test observes belongs to the read phase rather than having been lost
	// earlier, in git status, before the bound was ever exercised.
	reading := make(chan struct{})
	var entered sync.Once
	readFileForCapture = func(ctx context.Context, path string, limit int) ([]byte, error) {
		// A hostile worktree whose read never returns: only the operation
		// deadline ends this phase.
		entered.Do(func() { close(reading) })
		<-ctx.Done()
		return nil, ctx.Err()
	}

	type captureResult struct {
		bundle *Bundle
		err    error
	}
	// The budget is capture-wide, so it must also cover the pre-read git status
	// fork/exec, the one part of the phase this test does not control; losing it
	// there would fail the test on a stall it never intended to measure. Widened
	// from 250ms to leave generous headroom on a -race CI runner, while the guard
	// below still fails fast if the bound is lost.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan captureResult, 1)
	go func() {
		bundle, err := Capture(ctx, captureOptions(dir, nil))
		result <- captureResult{bundle: bundle, err: err}
	}()

	select {
	case got := <-result:
		select {
		case <-reading:
		default:
			t.Fatal("capture never entered the read phase, so it did not exercise the bound")
		}
		if !errors.Is(got.err, context.DeadlineExceeded) {
			t.Fatalf("capture err = %v, want context.DeadlineExceeded", got.err)
		}
		if got.bundle != nil {
			t.Errorf("capture returned a bundle alongside a deadline abort, want nil so the caller degrades to manifest-only")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("capture did not return within 10s")
	}
}

// fifoSwapDeadline bounds the swap-race assertions: a read-only open of a fifo
// blocks until a writer arrives, so a regression that loses O_NONBLOCK stalls
// here instead of failing.
const fifoSwapDeadline = 10 * time.Second

// TestReadCappedRejectsNonRegular pins the issue #336 primitive: a path that
// is a fifo when opened — as after a regular-file-to-fifo swap in the window
// between Lstat classification and the read — fails with errNotRegular and
// yields no content, promptly and without waiting for a writer; a plain
// regular file still reads through the cap.
func TestReadCappedRejectsNonRegular(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	regular := filepath.Join(dir, "regular.txt")
	writeFile(t, regular, "regular content\n")
	data, err := readCapped(ctx, regular, MaxFileBytes+1)
	if err != nil {
		t.Fatalf("readCapped regular file: %v", err)
	}
	if string(data) != "regular content\n" {
		t.Errorf("readCapped regular file = %q, want %q", data, "regular content\n")
	}

	fifo := filepath.Join(dir, "fifo.txt")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	type read struct {
		data []byte
		err  error
	}
	done := make(chan read, 1)
	go func() {
		content, rerr := readCapped(ctx, fifo, MaxFileBytes+1)
		done <- read{data: content, err: rerr}
	}()
	select {
	case got := <-done:
		if !errors.Is(got.err, errNotRegular) {
			t.Fatalf("readCapped fifo err = %v, want %v", got.err, errNotRegular)
		}
		if got.data != nil {
			t.Errorf("readCapped returned %q for a fifo, want nil", got.data)
		}
	case <-time.After(fifoSwapDeadline):
		t.Fatalf("readCapped blocked on a fifo for more than %s", fifoSwapDeadline)
	}
}

// TestCaptureFifoSwapRace is the issue #336 regression: a hostile worktree
// swaps a classified-regular file for a fifo in the window between the Lstat
// classification and the read. The open must not block waiting for a writer,
// and the capture must store nothing for the entry.
func TestCaptureFifoSwapRace(t *testing.T) {
	// Not parallel: the test replaces a package-level seam that parallel
	// captures share.
	dir := newRepo(t)
	writeFile(t, filepath.Join(dir, "swap.txt"), "FIFO-SWAP-ORIGINAL\n")

	original := readFileForCapture
	t.Cleanup(func() { readFileForCapture = original })
	readFileForCapture = func(ctx context.Context, path string, limit int) ([]byte, error) {
		// The swap lands after Lstat classified the path as regular and
		// before the O_NONBLOCK open, so the descriptor this read opens is a
		// fifo the classification pass never saw.
		if err := os.Remove(path); err != nil {
			t.Errorf("swap remove: %v", err)
		}
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Errorf("swap mkfifo: %v", err)
		}
		return original(ctx, path, limit)
	}

	type capture struct {
		bundle *Bundle
		err    error
	}
	done := make(chan capture, 1)
	go func() {
		bundle, cerr := Capture(context.Background(), captureOptions(dir, nil))
		done <- capture{bundle: bundle, err: cerr}
	}()

	var bundle *Bundle
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("capture: %v", got.err)
		}
		bundle = got.bundle
	case <-time.After(fifoSwapDeadline):
		t.Fatalf("capture blocked on a fifo swap for more than %s", fifoSwapDeadline)
	}

	entry := findEntry(t, bundle, "swap.txt")
	if entry.Class != ClassUntracked {
		t.Errorf("class = %q, want %q", entry.Class, ClassUntracked)
	}
	if entry.Disposition != DispositionOmittedBinary {
		t.Errorf("disposition = %q, want %q", entry.Disposition, DispositionOmittedBinary)
	}
	if entry.StoredPath != "" {
		t.Errorf("fifo entry carries StoredPath %q", entry.StoredPath)
	}
	if entry.Bytes != 0 {
		t.Errorf("fifo entry carries Bytes %d, want 0", entry.Bytes)
	}
	if members := untar(t, bundle.Archive); len(members) != 0 {
		t.Errorf("archive carries %d members for a swapped fifo: %v", len(members), members)
	}
	if got := bundle.Manifest.Totals; got.Files != 1 || got.OmittedBinary != 1 || got.Stored != 0 {
		t.Errorf("totals = %+v, want Files 1, OmittedBinary 1, Stored 0", got)
	}
	assertAbsent(t, bundle, []string{"FIFO-SWAP-ORIGINAL"})
}

// TestRunGitCapped drives the bounded pipe reader directly: a
// substantially-oversized patch stops at exactly the cap with overLimit set,
// a small patch is returned in full, and a failing command wraps its stderr
// like runGit.
func TestRunGitCapped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dir := newRepo(t)
	writeFile(t, filepath.Join(dir, "big.txt"), strings.Repeat("a", 512*1024))
	commit(t, dir, "feat: oversized text file")
	bigSHA := headSHA(t, dir)

	out, over, err := runGitCapped(ctx, dir, MaxFileBytes, "format-patch", "--stdout", "--no-binary", "-1", bigSHA)
	if err != nil {
		t.Fatalf("runGitCapped (oversized): %v", err)
	}
	if !over {
		t.Errorf("over = false, want true for a ~512 KiB patch")
	}
	if len(out) != MaxFileBytes+1 {
		t.Errorf("len(out) = %d, want exactly MaxFileBytes+1 = %d", len(out), MaxFileBytes+1)
	}

	smallDir := newRepo(t)
	writeFile(t, filepath.Join(smallDir, "small.txt"), "hello\n")
	commit(t, smallDir, "feat: small change")
	smallSHA := headSHA(t, smallDir)
	full, err := runGit(ctx, smallDir, "format-patch", "--stdout", "--no-binary", "-1", smallSHA)
	if err != nil {
		t.Fatalf("runGit (small): %v", err)
	}
	out, over, err = runGitCapped(ctx, smallDir, MaxFileBytes, "format-patch", "--stdout", "--no-binary", "-1", smallSHA)
	if err != nil {
		t.Fatalf("runGitCapped (small): %v", err)
	}
	if over {
		t.Errorf("over = true, want false for a small patch")
	}
	if string(out) != string(full) {
		t.Errorf("capped output = %d bytes, want the full patch (%d bytes)", len(out), len(full))
	}

	if _, over, err := runGitCapped(ctx, smallDir, MaxFileBytes, "format-patch", "--stdout", "--no-binary", "-1", "not-a-sha"); err == nil {
		t.Errorf("runGitCapped with a bad sha: want an error, got none (over=%v)", over)
	} else {
		if over {
			t.Errorf("failing command reported over-limit, want false")
		}
	}
}

// TestRunGitCappedOverLimitFailure covers the combination #412 found: a git
// that writes past the cap AND exits non-zero. The over-limit branch kills and
// reaps the child, and it used to discard the reap's error, so a child that had
// already failed came back as a clean over-limit result with a nil error — the
// caller then recorded the failed patch as a benign over-limit omission with no
// failure anywhere in the manifest. Reverting the fix fails this on the missing
// error.
func TestRunGitCappedOverLimitFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dir := newRepo(t)
	// The shell behind the alias fails at once while a backgrounded writer
	// keeps filling stdout past the cap. The flood is deferred so the child is
	// provably a reaped exit status by the time the reader reaches the cap:
	// that is the window this pins. (A child still running when the cap is hit
	// is killed by the capture, and a signaled reap cannot say what that child
	// would have done — see the #412 entry in DESIGN.md.)
	writeFile(t, filepath.Join(dir, "flood-fail.sh"),
		"#!/bin/sh\n(sleep 0.05; head -c 70000 /dev/zero | tr '\\0' a) &\necho boom >&2\nexit 3\n")
	git(t, dir, "config", "alias.floodfail", "!sh ./flood-fail.sh")

	out, over, err := runGitCapped(ctx, dir, MaxFileBytes, "floodfail")
	if err == nil {
		t.Fatalf("runGitCapped (over-limit failure): err = nil, want the child's failure (over=%v, %d bytes): a failed command is not a clean over-limit", over, len(out))
	}
	if got := err.Error(); !strings.Contains(got, "exit status 3") {
		t.Errorf("err = %q, want it to report the child's exit status", got)
	} else if !strings.Contains(got, "boom") {
		t.Errorf("err = %q, want it to report the child's stderr", got)
	}
	if !over {
		t.Errorf("over = false, want true for output past the cap")
	}
	if len(out) != MaxFileBytes+1 {
		t.Errorf("len(out) = %d, want exactly MaxFileBytes+1 = %d", len(out), MaxFileBytes+1)
	}

	// The sibling case stays a success: killing a child for overrunning the
	// cap is not itself a failure, so the bounded prefix still comes back
	// with no error.
	git(t, dir, "config", "alias.floodok", "!sh -c 'head -c 70000 /dev/zero | tr \"\\0\" a'")
	out, over, err = runGitCapped(ctx, dir, MaxFileBytes, "floodok")
	if err != nil {
		t.Fatalf("runGitCapped (over-limit success): %v", err)
	}
	if !over {
		t.Errorf("over = false, want true for output past the cap")
	}
	if len(out) != MaxFileBytes+1 {
		t.Errorf("len(out) = %d, want exactly MaxFileBytes+1 = %d", len(out), MaxFileBytes+1)
	}
}

// TestRunGitCanceledWithHeldPipe covers the stall #408 found one function up
// from #392: a grandchild outlives git holding the stdout and stderr write
// ends open, so the copy goroutines inside cmd.Run never reach EOF and Wait
// parks on them past the context. WaitDelay closes those pipes so the call
// returns with the context error instead of stalling the capture to the pod's
// grace-period SIGKILL. Reverting the fix parks here until the guard trips.
func TestRunGitCanceledWithHeldPipe(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias leaves sleep holding git's stdout and stderr write ends, the
	// same shape as a git-remote-* helper that outlives its parent.
	git(t, dir, "config", "alias.holdpipe", "!sh -c 'sleep 30 &'")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := runGit(ctx, dir, "holdpipe")
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("runGit did not return within 20s of its context deadline: Wait is parked on the pipe the surviving grandchild still holds")
	}
}

// TestRunGitSurvivingPipeOnSuccess covers the sibling case: the child itself
// succeeds with real output while a surviving grandchild still holds the
// pipes. WaitDelay then expires with no git failure to report, and the
// buffered bytes are the complete result — sleep writes nothing, git flushed
// its output before exiting, and the copy goroutine drains the pipe before it
// parks on the held write end — so the call must return them instead of
// failing the capture over an expired timer.
func TestRunGitSurvivingPipeOnSuccess(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias prints its result and exits 0 while sleep outlives it holding
	// both write ends, which is what lets Wait outlast its own child with no
	// git failure behind it and no EOF behind that.
	git(t, dir, "config", "alias.holdout", "!sh -c 'sleep 30 >&2 & echo held-output'")

	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := runGit(context.Background(), dir, "holdout")
		done <- result{out, err}
	}()

	var got result
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGit did not return within 20s while a grandchild held the stderr pipe")
	}
	if got.err != nil {
		t.Fatalf("err = %v, want nil: a command that succeeded before the pipe was held is not a git failure", got.err)
	}
	if string(got.out) != "held-output\n" {
		t.Errorf("out = %q, want the complete output", got.out)
	}
}

// TestRunGitFailureWithHeldPipe is the failure side of that sibling case: a
// git command that exits non-zero while a surviving grandchild still holds a
// pipe must still fail the capture with its exit status and stderr, and the
// stderr kept in the wrapped error is capped, not the flood the child wrote:
// the cap must discard the excess while consuming the whole chunk, because a
// short write stops the exec copy goroutine and closes the pipe, and the child
// dies of SIGPIPE (141) with its real status lost. os/exec also gives the
// process error precedence over a WaitDelay overrun, so a failed child never
// reports ErrWaitDelay and the branch that keeps the bytes stays reachable
// only for a child that succeeded.
func TestRunGitFailureWithHeldPipe(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias fails on 65 KiB of stderr and exits 3 while sleep outlives it
	// holding both write ends, so WaitDelay elapses with a real git failure
	// behind it and an unbounded writer on the same pipe.
	git(t, dir, "config", "alias.holdfail", "!sh -c 'echo git-failed >&2; i=0; while [ $i -lt 1000 ]; do echo 012345678901234567890123456789012345678901234567890123456789012345678 >&2; i=$((i+1)); done; sleep 30 >&2 & exit 3'")

	done := make(chan error, 1)
	go func() {
		_, err := runGit(context.Background(), dir, "holdfail")
		done <- err
	}()

	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGit did not return within 20s while a grandchild held the stderr pipe")
	}
	if err == nil {
		t.Fatal("err = nil, want an error: a command that exited 3 is a git failure even with a grandchild holding a pipe")
	}
	if !strings.Contains(err.Error(), "exit status 3") {
		t.Errorf("err = %v, want the command's exit status", err)
	}
	if !strings.Contains(err.Error(), "git-failed") {
		t.Errorf("err = %v, want the command's stderr", err)
	}
	if n := len(err.Error()); n > len("git holdfail: exit status 3: ")+stderrLimit {
		t.Errorf("error carries %d bytes of stderr, want at most stderrLimit (%d)", n, stderrLimit)
	}
}

// TestRunGitPrefersCancellationOverACompletedSuccess is the #469 regression on
// the git side, the sibling of the #448 fix in runGitCapped: a command that
// finished before the deadline but whose caller returned after it handed its
// bytes back with a nil error, because runGit guarded ctx.Err() only in its
// error branch. cmd.Run returns only after Wait, so the read-versus-deadline
// race runGitCapped has to guard against does not exist here; the guard is a
// policy answer, not a race answer — a completed command never survives a dead
// context — so the expired context wins and the late bytes are discarded.
//
// The double's Done never closes, so os/exec's Start does not reject the call
// on an already-done context and the alias really runs: the plain success
// return is the one #469 slips through, and the returned error is what
// distinguishes the guarded answer (the context's error) from the unfixed one
// (nil). Restoring capture.go as it stood before #469 fails this test: the
// unfixed code answers with a nil error and the bytes it kept.
func TestRunGitPrefersCancellationOverACompletedSuccess(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias writes to stdout and exits 0 straight away, so the command
	// completes with real output, no held pipe, and no git failure to report:
	// the plain success path is the one #469 slips through.
	git(t, dir, "config", "alias.quickok", "!sh -c 'echo LATE-BYTES'")

	ctx := &expiredContext{done: make(chan struct{})}
	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := runGit(ctx, dir, "quickok")
		done <- result{out, err}
	}()

	var got result
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGit did not return within 20s of a command that completed under an expired context")
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded: a command that returned past the deadline is not a result", got.err)
	}
	if got.out != nil {
		t.Errorf("out = %q, want nil: bytes produced under an expired context are discarded", got.out)
	}
}

// TestRunGitExpiredContextBeatsTheErrWaitDelayKeepBytesRule holds the guard
// as wide as #421's keep-the-bytes branch, which is the one success return a
// narrower guard would miss: a surviving grandchild holding only the stderr
// write end makes Wait return exec.ErrWaitDelay with a successful
// ProcessState, and that branch returns the bytes as a clean success. That
// branch answered on the context itself before #469 — the error branch above
// it observed ctx.Err() first — so this test does not pin a pre-#469 defect
// and passes against the old code; it fails a fix that narrows the guard to
// the final `return stdout.Bytes(), nil` and drops the error branch's own
// context check, which leaves the rule below reachable with late bytes. #421
// remains a claim about a live context — the bytes stand because the timeout
// was ours rather than git's — so an expired context takes it away, and
// TestRunGitSurvivingPipeOnSuccess is the live-context half and must keep
// passing: #421 is narrowed here, not reversed.
func TestRunGitExpiredContextBeatsTheErrWaitDelayKeepBytesRule(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias writes to stdout and exits 0 while sleep keeps only the stderr
	// write end open — the held-stderr shape of TestRunGitSurvivingPipeOnSuccess,
	// plus stdout bytes to lose — so the 2s WaitDelay elapses and Wait reports
	// ErrWaitDelay behind a successful exit.
	git(t, dir, "config", "alias.holderr", "!sh -c 'echo LATE-BYTES; sleep 30 >&2 &'")

	ctx := &expiredContext{done: make(chan struct{})}
	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := runGit(ctx, dir, "holderr")
		done <- result{out, err}
	}()

	var got result
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGit did not return within 20s while a grandchild held the stderr pipe")
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded: #421's keep-the-bytes rule stands only for a live context (out = %q)", got.err, got.out)
	}
	if got.out != nil {
		t.Errorf("out = %q, want nil: #421's keep-the-bytes rule stands only for a live context", got.out)
	}
}

// TestRunGitCappedCanceledWithHeldPipe covers the stall where a grandchild
// outlives git holding the stdout pipe open with no output: the cap bounds
// bytes, not blocking, so the read must end with the context instead of parking
// the capture forever. Returning is the contract, and both halves of the fix
// carry it: the read/select ends the read at the context, and WaitDelay keeps
// the cancel path's reap off the same held pipe.
func TestRunGitCappedCanceledWithHeldPipe(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias leaves sleep holding git's stdout and stderr write ends, the same
	// shape as a git-remote-* helper that outlives its parent.
	git(t, dir, "config", "alias.holdpipe", "!sh -c 'sleep 30 &'")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	type capped struct {
		out  []byte
		over bool
		err  error
	}
	done := make(chan capped, 1)
	go func() {
		out, over, err := runGitCapped(ctx, dir, MaxFileBytes, "holdpipe")
		done <- capped{out: out, over: over, err: err}
	}()

	var got capped
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGitCapped did not return within 20s of its context deadline: the bounded read is parked on the pipe the surviving grandchild still holds")
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", got.err)
	}
	if got.over {
		t.Error("over = true, want false for a canceled context")
	}
	if len(got.out) != 0 {
		t.Errorf("out = %q, want no output", got.out)
	}
}

// TestRunGitCappedSurvivingPipeOnSuccess covers the sibling case where the
// child itself succeeds but a surviving grandchild still holds a pipe. WaitDelay
// then expires with no git failure to report, and stdout has already reached
// EOF, so the bytes read are the complete result: the call must return them
// instead of failing the capture over an expired timer.
func TestRunGitCappedSurvivingPipeOnSuccess(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias exits 0 while sleep keeps only the stderr write end open, which
	// is what lets Wait outlast its own child without any git failure behind it.
	git(t, dir, "config", "alias.holderr", "!sh -c 'sleep 30 >&2 &'")

	type held struct {
		out  []byte
		over bool
		err  error
	}
	done := make(chan held, 1)
	go func() {
		out, over, err := runGitCapped(context.Background(), dir, MaxFileBytes, "holderr")
		done <- held{out, over, err}
	}()

	var got held
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGitCapped did not return within 20s while a grandchild held the stderr pipe")
	}
	if got.err != nil {
		t.Fatalf("err = %v, want nil: a command that succeeded before the pipe was held is not a git failure", got.err)
	}
	if got.over {
		t.Error("over = true, want false")
	}
	if len(got.out) != 0 {
		t.Errorf("out = %q, want no output", got.out)
	}
}

// TestRunGitCappedFailureWithHeldPipe is the failure side of that sibling
// case: a git command that exits non-zero while a surviving grandchild still
// holds a pipe must still fail the capture, with its exit status and stderr.
// os/exec gives the process error precedence over a WaitDelay overrun, so a
// failed child never reports ErrWaitDelay and the branch that keeps the bytes
// stays reachable only for a child that succeeded. Nothing can fail the
// ProcessState guard alone — that precedence rules it out — so this pins the
// precedence itself; reverting capture.go past #392 fails it, parked on the
// held stderr pipe until the guard trips.
func TestRunGitCappedFailureWithHeldPipe(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias fails on stderr and exits 3 while sleep keeps only the stderr
	// write end open, so WaitDelay elapses with a real git failure behind it.
	git(t, dir, "config", "alias.holdfail", "!sh -c 'echo git-failed >&2; sleep 30 >&2 & exit 3'")

	type held struct {
		out  []byte
		over bool
		err  error
	}
	done := make(chan held, 1)
	go func() {
		out, over, err := runGitCapped(context.Background(), dir, MaxFileBytes, "holdfail")
		done <- held{out, over, err}
	}()

	var got held
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGitCapped did not return within 20s while a grandchild held the stderr pipe")
	}
	if got.err == nil {
		t.Fatalf("err = nil, want an error: a command that exited 3 is a git failure even with a grandchild holding a pipe (out = %q)", got.out)
	}
	if !strings.Contains(got.err.Error(), "exit status 3") {
		t.Errorf("err = %v, want the command's exit status", got.err)
	}
	if !strings.Contains(got.err.Error(), "git-failed") {
		t.Errorf("err = %v, want the command's stderr", got.err)
	}
	if got.over {
		t.Error("over = true, want false")
	}
}

// expiredContext stands in for a context that was already past its deadline by
// the time runGitCapped or runGit reached its post-reap guard: Err answers
// context.DeadlineExceeded from every observation, the first included, because
// #448 is about a context that expired while the git read was in flight — a
// read that completed under a dead context, not a call made under one.
//
// lateDeadlineContext cannot express that. Nothing in runGitCapped observes
// Err before the guard — the select receives on Done, and Start reads Err only
// when Done is already closed — so the guard would be that double's FIRST
// observation, its nil, and the guard would pass: these tests would fail
// against the fixed runGitCapped rather than pin it. runGit observes Err only
// in its guard for the same reason, so the same double pins #469.
//
// Done is supplied by the test and stays open for the whole call, because
// closing it decides nothing worth pinning: os/exec's Start fails the entire
// call when Done is already closed, and a Done that closes while the read is
// in flight makes the select's Done branch answer with the same
// context.DeadlineExceeded these tests expect, vacuously, against an unfixed
// runGitCapped as readily as a fixed one. With Done never ready the read
// branch is the only one the select can take, so the select is deterministic
// and the guard after the reap is the only thing that can decide either test
// below. runGit has no select to force: cmd.Run returns only after Wait, so
// the command runs to completion and the guard after the reap is the only
// thing that can decide its tests too.
type expiredContext struct {
	done <-chan struct{}
}

func (c *expiredContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *expiredContext) Done() <-chan struct{}       { return c.done }
func (c *expiredContext) Value(any) any               { return nil }
func (c *expiredContext) Err() error                  { return context.DeadlineExceeded }

// TestRunGitCappedPrefersCancellationOverACompletedRead is the #448 regression
// on the git side, the twin of the #390/#393 fix in readCapped: a stdout read
// that completes while the context is already expired leaves both channels
// ready after the select, the tie is broken at random, and a completed read
// that wins it reaches the success path with a nil error and bytes produced
// past the deadline. Cancellation has to be settled again after the reap, so
// the expired context wins over a read that finished under it and the late
// bytes are discarded rather than handed back as a result. Restoring
// capture.go as it stood before #448 fails this test on the returned bytes
// and error.
func TestRunGitCappedPrefersCancellationOverACompletedRead(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias writes to stdout and exits 0 straight away, so the read
	// completes on its own with no held pipe and no git failure to report: the
	// plain success path is the one #448 slips through.
	git(t, dir, "config", "alias.quickok", "!sh -c 'echo LATE-BYTES'")

	ctx := &expiredContext{done: make(chan struct{})}
	type capped struct {
		out  []byte
		over bool
		err  error
	}
	done := make(chan capped, 1)
	go func() {
		out, over, err := runGitCapped(ctx, dir, MaxFileBytes, "quickok")
		done <- capped{out, over, err}
	}()

	var got capped
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGitCapped did not return within 20s of a read that completed under an expired context")
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded: a read past the deadline is not a result", got.err)
	}
	if got.out != nil {
		t.Errorf("out = %q, want nil: bytes read under an expired context are discarded", got.out)
	}
	if got.over {
		t.Error("over = true, want false: a canceled context never reports overLimit")
	}
}

// TestRunGitCappedExpiredContextBeatsTheErrWaitDelayKeepBytesRule holds the
// guard as wide as #421's keep-the-bytes branch, which is the one success
// return a narrower guard would miss: a surviving grandchild holding only the
// stderr write end makes Wait return exec.ErrWaitDelay with a successful
// ProcessState, and that branch returns the bytes as a clean success. That
// branch answered on the context itself before #448 — the error branch
// above it observed ctx.Err() first — so this test does not pin a pre-#448
// defect and passes against the old code; it fails a fix that guards only
// the final `return out, false, nil`, which leaves the rule below reachable
// with late bytes. #421 remains a claim about a live context — the bytes stand
// because the timeout was ours rather than git's — so an expired context takes
// it away, and TestRunGitCappedSurvivingPipeOnSuccess is the live-context half
// and must keep passing: #421 is narrowed here, not reversed.
func TestRunGitCappedExpiredContextBeatsTheErrWaitDelayKeepBytesRule(t *testing.T) {
	t.Parallel()

	dir := newRepo(t)
	// The alias writes to stdout and exits 0 while sleep keeps only the stderr
	// write end open — the held-stderr shape of
	// TestRunGitCappedSurvivingPipeOnSuccess, plus stdout bytes to lose — so the
	// 2s WaitDelay elapses and Wait reports ErrWaitDelay behind a successful
	// exit.
	git(t, dir, "config", "alias.holderr", "!sh -c 'echo LATE-BYTES; sleep 30 >&2 &'")

	ctx := &expiredContext{done: make(chan struct{})}
	type held struct {
		out  []byte
		over bool
		err  error
	}
	done := make(chan held, 1)
	go func() {
		out, over, err := runGitCapped(ctx, dir, MaxFileBytes, "holderr")
		done <- held{out, over, err}
	}()

	var got held
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runGitCapped did not return within 20s while a grandchild held the stderr pipe")
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded: #421's keep-the-bytes rule stands only for a live context (out = %q)", got.err, got.out)
	}
	if got.out != nil {
		t.Errorf("out = %q, want nil: #421's keep-the-bytes rule stands only for a live context", got.out)
	}
	if got.over {
		t.Error("over = true, want false: a canceled context never reports overLimit")
	}
}

// TestCappedBufferWrite pins the discard contract of cappedBuffer: a chunk
// straddling the limit is taken as far as the cap and reported as fully
// consumed. Reporting the truncated count instead is a short write, and
// io.Copy — the exec stderr copier — stops on one, closing the child's pipe
// and SIGPIPE-killing a verbose git before its failure reaches the capture.
func TestCappedBufferWrite(t *testing.T) {
	t.Parallel()

	var b cappedBuffer
	b.limit = 4
	n, err := b.Write([]byte("abc"))
	if n != 3 || err != nil {
		t.Errorf("Write(3 of 4) = %d, %v, want 3, nil", n, err)
	}
	n, err = b.Write([]byte("defgh"))
	if n != 5 || err != nil {
		t.Errorf("Write(5 straddling the cap) = %d, %v, want 5, nil (full length, no short write)", n, err)
	}
	n, err = b.Write([]byte("ijk"))
	if n != 3 || err != nil {
		t.Errorf("Write(past the cap) = %d, %v, want 3, nil", n, err)
	}
	if string(b.data) != "abcd" {
		t.Errorf("data = %q, want %q (first cap bytes kept, excess discarded)", b.data, "abcd")
	}
}

// TestTruncateDisplay covers the manifest display cap directly: ASCII and
// multibyte truncation land on rune boundaries with an ellipsis, short values
// and the redaction placeholder pass through untouched.
func TestTruncateDisplay(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 2000)
	got := truncateDisplay(long)
	if utf8.RuneCountInString(got) > MaxPathDisplayBytes+1 {
		t.Errorf("truncated length = %d runes, want <= %d", utf8.RuneCountInString(got), MaxPathDisplayBytes+1)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncated string %q does not end with an ellipsis", got[len(got)-8:])
	}

	short := strings.Repeat("b", MaxPathDisplayBytes)
	if truncateDisplay(short) != short {
		t.Error("string of exactly MaxPathDisplayBytes was altered")
	}
	if truncateDisplay(log.RedactedPlaceholder) != log.RedactedPlaceholder {
		t.Error("redaction placeholder was altered")
	}

	// The multibyte rune straddles the byte cap: truncation must drop it
	// whole rather than emit half a rune.
	mixed := strings.Repeat("a", MaxPathDisplayBytes-1) + "é" + strings.Repeat("b", 8)
	got = truncateDisplay(mixed)
	if !utf8.ValidString(got) {
		t.Error("truncation split a multibyte rune")
	}
	if strings.ContainsRune(got, 'é') {
		t.Error("truncation kept the rune straddling the cap")
	}
	if !strings.HasSuffix(got, "…") {
		t.Error("multibyte truncation does not end with an ellipsis")
	}
}

// TestCaptureManifestBudgetCollapse fills the worktree with long-named
// untracked files until the serialized manifest busts ManifestBudgetBytes while
// staying under MaxManifestEntries, and checks the single synthetic collapse
// entry and that Totals stay authoritative.
func TestCaptureManifestBudgetCollapse(t *testing.T) {
	t.Parallel()

	const fileCount = 700
	dir := newRepo(t)
	filler := strings.Repeat("d", 190)
	for i := range fileCount {
		writeFileMkdir(t, filepath.Join(dir, fmt.Sprintf("artifacts/%04d-%s.dat", i, filler)), "x")
	}

	bundle, err := Capture(context.Background(), captureOptions(dir, nil))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	manifest := bundle.Manifest

	budgetEntries := 0
	for _, entry := range manifest.Entries {
		if entry.Disposition == DispositionOmittedBudget {
			budgetEntries++
		}
	}
	if budgetEntries != 1 {
		t.Errorf("entries with disposition %q = %d, want exactly 1", DispositionOmittedBudget, budgetEntries)
	}
	if manifest.Totals.Files != fileCount {
		t.Errorf("totals.files = %d, want the true file count %d", manifest.Totals.Files, fileCount)
	}
	if len(manifest.Entries) >= manifest.Totals.Files {
		t.Errorf("len(entries) = %d, want < totals.files %d", len(manifest.Entries), manifest.Totals.Files)
	}
	if len(manifest.Entries) > MaxManifestEntries {
		t.Errorf("len(entries) = %d, want <= %d", len(manifest.Entries), MaxManifestEntries)
	}
	kept := len(manifest.Entries) - 1
	if manifest.Totals.OmittedByBudget != manifest.Totals.Files-kept {
		t.Errorf("totals.omittedByBudget = %d, want %d", manifest.Totals.OmittedByBudget, manifest.Totals.Files-kept)
	}
	serialized, err := manifest.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if len(serialized) > ManifestBudgetBytes {
		t.Errorf("serialized manifest = %d bytes, want <= %d", len(serialized), ManifestBudgetBytes)
	}
}

// TestCollapseToBudgetEntryCap drives the entry-count branch of
// collapseToBudget directly, without a worktree: more than
// MaxManifestEntries entries collapse to the cap with exactly one synthetic
// budget entry and an authoritative OmittedByBudget count.
func TestCollapseToBudgetEntryCap(t *testing.T) {
	t.Parallel()

	const total = MaxManifestEntries + 25
	entries := make([]Entry, 0, total)
	for range total {
		entries = append(entries, Entry{Path: "p", Class: ClassUntracked, Disposition: DispositionStored})
	}
	manifest := &Manifest{
		SchemaVersion: SchemaVersion,
		Entries:       entries,
	}

	collapseToBudget(context.Background(), manifest)

	if len(manifest.Entries) > MaxManifestEntries {
		t.Errorf("len(entries) = %d, want <= %d", len(manifest.Entries), MaxManifestEntries)
	}
	budget := 0
	for _, entry := range manifest.Entries {
		if entry.Disposition == DispositionOmittedBudget {
			budget++
		}
	}
	if budget != 1 {
		t.Errorf("entries with disposition %q = %d, want exactly 1", DispositionOmittedBudget, budget)
	}
	kept := len(manifest.Entries) - 1
	if manifest.Totals.OmittedByBudget != total-kept {
		t.Errorf("totals.omittedByBudget = %d, want %d", manifest.Totals.OmittedByBudget, total-kept)
	}
}

// TestCollapseToBudgetOmittedCountBoundary is the regression for probing with
// a stale omittedByBudget: the fixture is tuned so the largest candidate that
// fits when probed with omittedByBudget=0 busts ManifestBudgetBytes once its
// real (three-digit) omitted count is serialized. collapseToBudget must settle
// one entry lower and still emit a budget-sized manifest with an authoritative
// omitted count.
func TestCollapseToBudgetOmittedCountBoundary(t *testing.T) {
	t.Parallel()

	const (
		pathPad = 900
		total   = 400
	)
	entries := make([]Entry, 0, total)
	for i := range total {
		entries = append(entries, Entry{
			Path:        fmt.Sprintf("artifacts/%s/%04d.dat", strings.Repeat("p", pathPad), i),
			Class:       ClassUntracked,
			Disposition: DispositionStored,
		})
	}
	manifest := &Manifest{
		SchemaVersion: SchemaVersion,
		Trigger:       strings.Repeat("t", 131),
		Entries:       entries,
	}

	candidateFits := func(keep, omitted int) bool {
		t.Helper()
		candidate := append(append([]Entry(nil), entries[:keep]...), Entry{Disposition: DispositionOmittedBudget})
		fits, err := manifestFits(manifest, candidate, omitted)
		if err != nil {
			t.Fatalf("manifestFits(keep=%d, omitted=%d): %v", keep, omitted, err)
		}
		return fits
	}

	// Pin down the largest keep that fits with a stale omittedByBudget of 0
	// and require the SAME candidate to NOT fit with its real omitted count,
	// so the fixture cannot decay into a vacuous no-op.
	kmax := -1
	for k := min(total, MaxManifestEntries-1); k >= 0; k-- {
		if candidateFits(k, 0) {
			kmax = k
			break
		}
	}
	if kmax < 0 {
		t.Fatal("fixture: no candidate fits with omittedByBudget=0")
	}
	if candidateFits(kmax, total-kmax) {
		t.Fatalf("fixture: candidate with keep=%d and real omitted count %d still fits; the budget boundary is not straddled", kmax, total-kmax)
	}
	if !candidateFits(kmax, 0) {
		t.Fatalf("fixture: candidate with keep=%d does not fit with omittedByBudget=0", kmax)
	}

	collapseToBudget(context.Background(), manifest)

	if len(manifest.Entries) > MaxManifestEntries {
		t.Errorf("len(entries) = %d, want <= %d", len(manifest.Entries), MaxManifestEntries)
	}
	if got := manifest.Entries[len(manifest.Entries)-1].Disposition; got != DispositionOmittedBudget {
		t.Errorf("last entry disposition = %q, want %q", got, DispositionOmittedBudget)
	}
	kept := len(manifest.Entries) - 1
	if manifest.Totals.OmittedByBudget != total-kept {
		t.Errorf("totals.omittedByBudget = %d, want %d", manifest.Totals.OmittedByBudget, total-kept)
	}
	serialized, err := manifest.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if len(serialized) > ManifestBudgetBytes {
		t.Errorf("serialized manifest = %d bytes, want <= %d", len(serialized), ManifestBudgetBytes)
	}
}

// TestCollapseToBudgetCanceledContext checks that a canceled capture still
// completes with a budget-enforcing manifest instead of rejecting: with the
// context already canceled the search settles immediately, leaving at most the
// real prefix plus the synthetic entry, an authoritative omitted count, and a
// manifest within budget.
func TestCollapseToBudgetCanceledContext(t *testing.T) {
	t.Parallel()

	const total = 500
	entries := make([]Entry, 0, total)
	for i := range total {
		entries = append(entries, Entry{
			Path:        fmt.Sprintf("artifacts/%s/%04d.dat", strings.Repeat("q", 600), i),
			Class:       ClassUntracked,
			Disposition: DispositionStored,
		})
	}
	manifest := &Manifest{
		SchemaVersion: SchemaVersion,
		Entries:       entries,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	collapseToBudget(ctx, manifest)

	if len(manifest.Entries) == 0 {
		t.Fatal("collapsed manifest holds no entries")
	}
	if len(manifest.Entries) > total+1 {
		t.Errorf("len(entries) = %d, want at most total %d plus the synthetic entry", len(manifest.Entries), total)
	}
	budget := 0
	for _, entry := range manifest.Entries {
		if entry.Disposition == DispositionOmittedBudget {
			budget++
		}
	}
	if budget != 1 {
		t.Errorf("entries with disposition %q = %d, want exactly 1", DispositionOmittedBudget, budget)
	}
	kept := len(manifest.Entries) - 1
	if manifest.Totals.OmittedByBudget != total-kept {
		t.Errorf("totals.omittedByBudget = %d, want %d", manifest.Totals.OmittedByBudget, total-kept)
	}
	serialized, err := manifest.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if len(serialized) > ManifestBudgetBytes {
		t.Errorf("serialized manifest = %d bytes, want <= %d", len(serialized), ManifestBudgetBytes)
	}
}

const binaryMarker = "\xde\xad\xbe\xef"

// repeatedMarker builds deterministic pseudo-binary content containing NUL and
// invalid UTF-8 alongside the marker sequence.
func repeatedMarker(n int) []byte {
	out := make([]byte, 0, n*8)
	for i := range n {
		out = append(out, []byte{byte(i), 0, 0xC3, 0x28, 0xFF, 0xFE}...)
		out = append(out, []byte(binaryMarker)...)
	}
	return out
}

// tinyTar describes the expected archive for n one-byte files named fNNN.txt.
func tinyTar(n int) map[string]string {
	members := make(map[string]string, n)
	for i := range n {
		members[fmt.Sprintf("f%03d.txt", i)] = "x"
	}
	return members
}

func newRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	initBare(t, remote)
	dir := filepath.Join(root, "work")
	initRepo(t, dir)
	writeFile(t, filepath.Join(dir, "tracked.txt"), "base content\n")
	commit(t, dir, "base: initial")
	git(t, dir, "checkout", "-b", "courier/run")
	git(t, dir, "remote", "add", "origin", remote)
	git(t, dir, "push", "-u", "origin", "courier/run")
	return dir
}

func captureOptions(dir string, scanner *Scanner) Options {
	return Options{
		Directory:   dir,
		UpstreamRef: "origin/courier/run",
		Trigger:     TriggerTerminal,
		Scanner:     scanner,
	}
}

func findEntry(t *testing.T, bundle *Bundle, path string) Entry {
	t.Helper()
	for _, entry := range bundle.Manifest.Entries {
		if entry.Path == path {
			return entry
		}
	}
	t.Fatalf("manifest has no entry for %q: %+v", path, bundle.Manifest.Entries)
	return Entry{}
}

func findCommitEntry(t *testing.T, bundle *Bundle) Entry {
	t.Helper()
	var found []Entry
	for _, entry := range bundle.Manifest.Entries {
		if entry.Class == ClassCommit {
			found = append(found, entry)
		}
	}
	if len(found) != 1 {
		t.Fatalf("manifest has %d commit entries, want exactly 1: %+v", len(found), bundle.Manifest.Entries)
	}
	return found[0]
}

func assertAbsent(t *testing.T, bundle *Bundle, needles []string) {
	t.Helper()
	if len(bundle.Archive) > 0 {
		zr, err := gzip.NewReader(bytes.NewReader(bundle.Archive))
		if err != nil {
			t.Fatalf("gunzip archive: %v", err)
		}
		raw, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read archive: %v", err)
		}
		for _, needle := range needles {
			if bytes.Contains(raw, []byte(needle)) {
				t.Errorf("archive contains %q", needle)
			}
		}
	}
	serialized, err := bundle.Manifest.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	for _, needle := range needles {
		if strings.Contains(string(serialized), needle) {
			t.Errorf("serialized manifest contains %q", needle)
		}
	}
}

func untar(t *testing.T, archive []byte) map[string]string {
	t.Helper()
	members := map[string]string{}
	if len(archive) == 0 {
		return members
	}
	zr, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("gunzip archive: %v", err)
	}
	tr := tar.NewReader(zr)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read tar member: %v", err)
		}
		members[header.Name] = string(data)
	}
	return members
}

func boolPtr(b bool) *bool { return &b }

func initBare(t *testing.T, directory string) {
	t.Helper()
	git(t, filepath.Dir(directory), "init", "--bare", directory)
}

func initRepo(t *testing.T, directory string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, directory, "init")
	git(t, directory, "config", "user.name", "Courier Test")
	git(t, directory, "config", "user.email", "courier-test@example.invalid")
}

func commit(t *testing.T, directory, message string) {
	t.Helper()
	git(t, directory, "add", "--all", "--", ".")
	git(t, directory, "commit", "-m", message)
}

// commitPaths stages only the named paths, leaving other worktree changes
// untracked in the same capture.
func commitPaths(t *testing.T, directory, message string, paths ...string) {
	t.Helper()
	git(t, directory, append([]string{"add", "--"}, paths...)...)
	git(t, directory, "commit", "-m", message)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	writeFileBytes(t, path, []byte(content))
}

func writeFileBytes(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeFileMkdir(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, content)
}

func git(t *testing.T, directory string, args ...string) {
	t.Helper()
	if _, err := gitOutput(directory, args...); err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
}

func headSHA(t *testing.T, dir string) string {
	t.Helper()
	out, err := gitOutput(dir, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

func gitOutput(directory string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = directory
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
