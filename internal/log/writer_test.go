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

func TestRedactingWriterBoundsOverlongLines(t *testing.T) {
	var out bytes.Buffer
	redactor := NewRedactor()
	redactor.Register(fakeGitToken)
	writer := NewRedactingWriter(&out, redactor)

	huge := strings.Repeat("x", 2*maxBufferedLine) + fakeGitToken + "tail\n"
	if n, err := writer.Write([]byte(huge)); err != nil || n != len(huge) {
		t.Fatalf("Write() = %d, %v; want %d consumed", n, err, len(huge))
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if strings.Contains(out.String(), fakeGitToken) {
		t.Fatal("overlong line leaked a registered secret")
	}
	if !strings.Contains(out.String(), "tail\n") {
		t.Fatal("overlong line lost its tail")
	}
	if out.Len() > len(huge) {
		t.Fatal("redaction must not add output bytes")
	}
}
