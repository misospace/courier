package log

import (
	"bytes"
	"strings"
	"testing"
)

func TestRedactingWriterJoinsSecretsSplitAcrossChunks(t *testing.T) {
	var out bytes.Buffer
	redactor := NewRedactor()
	redactor.Register(fakeGitToken)
	writer := NewRedactingWriter(&out, redactor)

	// The registered secret arrives in ordinary Write chunks, the way a
	// pipe delivers a child's output; only the joined line contains it.
	for _, chunk := range []string{"clone using ", fakeGitToken[:9], fakeGitToken[9:]} {
		if n, err := writer.Write([]byte(chunk)); err != nil || n != len(chunk) {
			t.Fatalf("Write(%d bytes) = %d, %v", len(chunk), n, err)
		}
	}
	if out.Len() != 0 {
		t.Fatal("unfinished line must stay buffered until its newline")
	}
	if _, err := writer.Write([]byte(" failed\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if out.String() != "clone using "+RedactedPlaceholder+" failed\n" {
		t.Fatalf("redacted line has unexpected shape (length %d)", out.Len())
	}
}

func TestRedactingWriterRedactsPatternsJoinedAtLineBoundary(t *testing.T) {
	var out bytes.Buffer
	writer := NewRedactingWriter(&out, NewRedactor())
	for _, chunk := range []string{"Authorization: ", "Bearer ", fakeBearerToken, "\n"} {
		if _, err := writer.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if strings.Contains(out.String(), fakeBearerToken) {
		t.Fatal("bearer credential leaked across chunk boundary")
	}
	if !strings.Contains(out.String(), RedactedPlaceholder) {
		t.Fatal("joined line was not redacted")
	}
}

func TestRedactingWriterPreservesOrderingAndStreamSeparation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	redactor := NewRedactor()
	stdoutWriter := NewRedactingWriter(&stdout, redactor)
	stderrWriter := NewRedactingWriter(&stderr, redactor)

	lines := []string{"first\n", "second ", "line\n", "third\n"}
	for _, line := range lines {
		if _, err := stdoutWriter.Write([]byte(line)); err != nil {
			t.Fatalf("stdout Write() error = %v", err)
		}
		if _, err := stderrWriter.Write([]byte("error\n")); err != nil {
			t.Fatalf("stderr Write() error = %v", err)
		}
	}
	if got := stdout.String(); got != "first\nsecond line\nthird\n" {
		t.Fatalf("stdout = %q, want original lines in order", got)
	}
	if got := stderr.String(); got != "error\nerror\nerror\nerror\n" {
		t.Fatalf("stderr = %q, want its own lines only", got)
	}
}

func TestRedactingWriterFlushEmitsPartialFinalLine(t *testing.T) {
	var out bytes.Buffer
	redactor := NewRedactor()
	redactor.Register(fakeDispatchKey)
	writer := NewRedactingWriter(&out, redactor)
	if _, err := writer.Write([]byte("exited mid-sentence with " + fakeDispatchKey + " in hand")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if out.Len() != 0 {
		t.Fatal("partial line must wait for Flush")
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if strings.Contains(out.String(), fakeDispatchKey) {
		t.Fatal("flushed partial line leaked a registered secret")
	}
	if !strings.HasPrefix(out.String(), "exited mid-sentence with ") {
		t.Fatal("flushed partial line lost its ordinary prefix")
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("second Flush() error = %v", err)
	}
	if strings.Count(out.String(), "exited") != 1 {
		t.Fatal("second Flush must be a no-op")
	}
}

// A dedicated secret for the overlong-line regression tests, sized so each
// half is distinctive and neither half can match on its own.
const fakeBoundarySecret = "straddle-boundary-token-0123456789abcdefgh"

func TestRedactingWriterSuppressesSecretStraddlingTheBound(t *testing.T) {
	var out bytes.Buffer
	redactor := NewRedactor()
	redactor.Register(fakeBoundarySecret)
	writer := NewRedactingWriter(&out, redactor)

	firstHalf := fakeBoundarySecret[:len(fakeBoundarySecret)/2]
	secondHalf := fakeBoundarySecret[len(fakeBoundarySecret)/2:]
	// The prefix is sized so the secret's first half ends exactly at
	// maxBufferedLine: under segment-based redaction the two halves would
	// land in different emitted segments and both survive.
	prefix := strings.Repeat("x", maxBufferedLine-len(firstHalf))

	if _, err := writer.Write([]byte(prefix + firstHalf)); err != nil {
		t.Fatalf("Write(prefix+first half) error = %v", err)
	}
	if got := strings.Count(out.String(), overlongLineMarker); got != 1 {
		t.Fatalf("marker count = %d, want exactly one after the bound was hit", got)
	}
	if len(writer.buf) != 0 {
		t.Fatalf("buffer holds %d bytes after suppression, want released", len(writer.buf))
	}
	if strings.Contains(out.String(), firstHalf) || strings.Contains(out.String(), prefix[:16]) {
		t.Fatal("suppressed line emitted content from before the bound")
	}

	if _, err := writer.Write([]byte(secondHalf + " ordinary suffix\n")); err != nil {
		t.Fatalf("Write(second half) error = %v", err)
	}
	for _, leaked := range []string{fakeBoundarySecret, firstHalf, secondHalf} {
		if strings.Contains(out.String(), leaked) {
			t.Fatalf("bound-straddling credential fragment leaked (constant: %s)", boundarySecretPart(leaked))
		}
	}

	if _, err := writer.Write([]byte("next ordinary line\n")); err != nil {
		t.Fatalf("Write(next line) error = %v", err)
	}
	if out.String() != overlongLineMarker+"\nnext ordinary line\n" {
		t.Fatalf("output = marker + %d ordinary bytes, want marker line then the next line intact", out.Len()-len(overlongLineMarker))
	}
	if strings.Count(out.String(), overlongLineMarker) != 1 {
		t.Fatal("subsequent lines must not be suppressed")
	}
	if len(writer.buf) != 0 {
		t.Fatal("buffering must return to empty after ordinary lines complete")
	}
}

func TestRedactingWriterSuppressesOverlongFinalPartialLineOnFlush(t *testing.T) {
	var out bytes.Buffer
	redactor := NewRedactor()
	redactor.Register(fakeBoundarySecret)
	writer := NewRedactingWriter(&out, redactor)

	firstHalf := fakeBoundarySecret[:len(fakeBoundarySecret)/2]
	secondHalf := fakeBoundarySecret[len(fakeBoundarySecret)/2:]
	prefix := strings.Repeat("x", maxBufferedLine-len(firstHalf))

	if _, err := writer.Write([]byte(prefix + firstHalf)); err != nil {
		t.Fatalf("Write(prefix+first half) error = %v", err)
	}
	if _, err := writer.Write([]byte(secondHalf + " never terminated")); err != nil {
		t.Fatalf("Write(remainder) error = %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	for _, leaked := range []string{fakeBoundarySecret, firstHalf, secondHalf} {
		if strings.Contains(out.String(), leaked) {
			t.Fatalf("overlong final partial line leaked a fragment (constant: %s)", boundarySecretPart(leaked))
		}
	}
	if out.String() != overlongLineMarker+"\n" {
		t.Fatalf("flushed overlong partial line = %d bytes of output, want only the marker line", out.Len())
	}
	if len(writer.buf) != 0 || writer.discarding {
		t.Fatal("writer must be idle after flushing a suppressed partial line")
	}
}

func TestRedactingWriterSuppressesOverlongLineWithoutLosingTheNext(t *testing.T) {
	var out bytes.Buffer
	redactor := NewRedactor()
	redactor.Register(fakeGitToken)
	writer := NewRedactingWriter(&out, redactor)

	// One Write carrying an over-long complete line: the secret sits past
	// the bound and must never be emitted, and the writer must not swallow
	// the following ordinary line while resuming.
	huge := strings.Repeat("x", 2*maxBufferedLine) + fakeGitToken + "tail\n"
	if n, err := writer.Write([]byte(huge)); err != nil || n != len(huge) {
		t.Fatalf("Write() = %d, %v; want %d consumed", n, err, len(huge))
	}
	if _, err := writer.Write([]byte("after\n")); err != nil {
		t.Fatalf("Write(after) error = %v", err)
	}
	if strings.Contains(out.String(), fakeGitToken) || strings.Contains(out.String(), "tail") {
		t.Fatal("suppressed over-long line emitted content (constant: fakeGitToken)")
	}
	if out.String() != overlongLineMarker+"\nafter\n" {
		t.Fatalf("output = %d bytes, want the marker line followed by the next line", out.Len())
	}
	if len(writer.buf) > maxBufferedLine {
		t.Fatal("buffering exceeded the bound")
	}
}

func boundarySecretPart(value string) string {
	switch value {
	case fakeBoundarySecret:
		return "fakeBoundarySecret"
	case fakeBoundarySecret[:len(fakeBoundarySecret)/2]:
		return "first half of fakeBoundarySecret"
	case fakeBoundarySecret[len(fakeBoundarySecret)/2:]:
		return "second half of fakeBoundarySecret"
	default:
		return "unknown fragment of fakeBoundarySecret"
	}
}
