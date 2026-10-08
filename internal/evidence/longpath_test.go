package evidence

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/misospace/courier/internal/log"
)

// asciiPad returns an n-byte string of lowercase ASCII, cycling 'a'..'z' from
// an offset so distinct elements differ. Every byte is a single-byte UTF-8
// rune, so each element's byte length equals its rune count and stays a valid
// path component.
func asciiPad(n, offset int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + ((offset + i) % 26))
	}
	return string(b)
}

// longCleanRelPath builds a repo-relative path of nested directories, each
// <= 200 bytes, whose joined length exceeds MaxPathDisplayBytes. Multibyte
// runes are distributed across components, and the byte at index
// MaxPathDisplayBytes is a UTF-8 continuation byte: the "é" (0xC3 0xA9) in the
// sixth element starts at that element's offset 18, so its second byte lands
// exactly on the display cap and the truncation must back off to the rune
// boundary just before it.
func longCleanRelPath() string {
	e0 := asciiPad(200, 0)
	e1 := asciiPad(200, 2)
	e2 := asciiPad(200, 4)
	e3 := asciiPad(200, 6)
	e4 := asciiPad(200, 8)
	e5 := asciiPad(18, 5) + "é" + asciiPad(180, 20)
	file := "日本" + strings.Repeat("x", 166) + ".txt"
	return strings.Join([]string{e0, e1, e2, e3, e4, e5, file}, "/")
}

// longSecretRelPath builds a repo-relative path, longer than
// MaxPathDisplayBytes, whose file name carries a pattern-table secret shape,
// mirroring the "…path is withheld" acceptance case at length.
func longSecretRelPath() string {
	e0 := asciiPad(200, 10)
	e1 := asciiPad(200, 12)
	e2 := asciiPad(200, 14)
	e3 := asciiPad(200, 16)
	e4 := asciiPad(200, 18)
	e5 := asciiPad(200, 22)
	file := "cred-" + testPatternSecret + ".conf"
	return strings.Join([]string{e0, e1, e2, e3, e4, e5, file}, "/")
}

// checkLongPathPreconditions validates the shape of a constructed long path:
// longer than the display cap, every single component within the filesystem
// limit, valid UTF-8, and (for wantContinuationAtCap) a continuation byte
// exactly at the cap.
func checkLongPathPreconditions(t *testing.T, rel string, wantContinuationAtCap bool) {
	t.Helper()
	rb := []byte(rel)
	if len(rel) <= MaxPathDisplayBytes {
		t.Fatalf("constructed path is %d bytes, want > %d", len(rel), MaxPathDisplayBytes)
	}
	if !utf8.Valid(rb) {
		t.Fatalf("constructed path is not valid UTF-8")
	}
	for _, elem := range strings.Split(rel, "/") {
		if len(elem) > 200 {
			t.Fatalf("path element is %d bytes, want <= 200", len(elem))
		}
	}
	if wantContinuationAtCap {
		if rb[MaxPathDisplayBytes]&0xC0 != 0x80 {
			t.Fatalf("byte at index %d = 0x%02x, want a UTF-8 continuation byte", MaxPathDisplayBytes, rb[MaxPathDisplayBytes])
		}
	}
}

// TestLongPathStoredEndToEnd drives the long-path display cap through a full
// capture: a clean untracked file at a path longer than MaxPathDisplayBytes is
// admitted to the tar under its full, untruncated StoredPath, while the
// manifest's display Path is capped on a rune boundary with an ellipsis.
func TestLongPathStoredEndToEnd(t *testing.T) {
	t.Parallel()
	rel := longCleanRelPath()
	checkLongPathPreconditions(t, rel, true)

	dir := newRepo(t)
	const content = "long-path-evidence-payload\n"
	writeFileMkdir(t, filepath.Join(dir, rel), content)

	bundle, err := Capture(context.Background(), captureOptions(dir, NewScanner()))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	manifest := bundle.Manifest
	if manifest.Totals.Files != 1 {
		t.Fatalf("totals.files = %d, want 1: %+v", manifest.Totals.Files, manifest)
	}
	if len(manifest.Entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1: %+v", len(manifest.Entries), manifest.Entries)
	}
	entry := manifest.Entries[0]
	if entry.Class != ClassUntracked {
		t.Errorf("class = %q, want %q", entry.Class, ClassUntracked)
	}
	if entry.Disposition != DispositionStored {
		t.Errorf("disposition = %q, want %q", entry.Disposition, DispositionStored)
	}
	if entry.StoredPath != rel {
		t.Errorf("storedPath (len %d) != full path (len %d); want the FULL, untruncated path", len(entry.StoredPath), len(rel))
	}

	// The display Path is the capped, redacted form: a rune-boundary prefix of
	// the original plus an ellipsis, never a split multibyte rune.
	display := entry.Path
	if !strings.HasSuffix(display, "…") {
		t.Errorf("display path %q does not end with an ellipsis", display)
	}
	if !utf8.ValidString(display) {
		t.Errorf("display path is not valid UTF-8: %q", display)
	}
	body := strings.TrimSuffix(display, "…")
	if !strings.HasPrefix(rel, body) {
		t.Errorf("display body is not a prefix of the original path")
	}
	if len(body) > MaxPathDisplayBytes {
		t.Errorf("display body is %d bytes, want <= MaxPathDisplayBytes %d", len(body), MaxPathDisplayBytes)
	}
	if !utf8.RuneStart(rel[len(body)]) {
		t.Errorf("truncation point (original index %d, byte 0x%02x) is not a rune start", len(body), rel[len(body)])
	}
	if len(display) > MaxPathDisplayBytes+len("…") {
		t.Errorf("display length %d, want <= %d", len(display), MaxPathDisplayBytes+len("…"))
	}

	members := untar(t, bundle.Archive)
	if members[rel] != content {
		t.Errorf("tar member %q (len %d) content = %q, want %q", rel, len(members[rel]), members[rel], content)
	}
	if len(members) != 1 {
		t.Errorf("tar has %d members, want 1: %v", len(members), members)
	}

	if manifest.Totals.Stored != 1 {
		t.Errorf("totals.stored = %d, want 1", manifest.Totals.Stored)
	}
	if manifest.Totals.StoredBytes != int64(len(content)) {
		t.Errorf("totals.storedBytes = %d, want %d", manifest.Totals.StoredBytes, len(content))
	}
}

// TestLongPathSecretWithheld drives the fail-closed path scan at length: a
// long path whose name carries a pattern-table secret shape is withheld, its
// display Path collapses to the redaction placeholder (never truncated, never
// the raw secret), it carries no StoredPath, and the secret needle reaches
// neither the manifest nor the archive.
func TestLongPathSecretWithheld(t *testing.T) {
	t.Parallel()
	rel := longSecretRelPath()
	checkLongPathPreconditions(t, rel, false)

	dir := newRepo(t)
	const content = "benign long-path second case\n"
	writeFileMkdir(t, filepath.Join(dir, rel), content)

	bundle, err := Capture(context.Background(), captureOptions(dir, NewScanner()))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	manifest := bundle.Manifest
	if manifest.Totals.Files != 1 {
		t.Fatalf("totals.files = %d, want 1: %+v", manifest.Totals.Files, manifest)
	}
	if len(manifest.Entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1: %+v", len(manifest.Entries), manifest.Entries)
	}
	entry := manifest.Entries[0]
	if entry.Disposition != DispositionWithheld {
		t.Errorf("disposition = %q, want %q", entry.Disposition, DispositionWithheld)
	}
	if entry.Path != log.RedactedPlaceholder {
		t.Errorf("display path = %q, want %q", entry.Path, log.RedactedPlaceholder)
	}
	if entry.StoredPath != "" {
		t.Errorf("withheld entry carries StoredPath %q, want empty", entry.StoredPath)
	}
	if manifest.Totals.Withheld != 1 {
		t.Errorf("totals.withheld = %d, want 1", manifest.Totals.Withheld)
	}

	members := untar(t, bundle.Archive)
	if len(members) != 0 {
		t.Errorf("tar has %d members, want 0 for a fully withheld capture: %v", len(members), members)
	}
	assertAbsent(t, bundle, []string{testPatternSecret, content})
}
