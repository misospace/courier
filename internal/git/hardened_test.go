package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestHardenedLinesStopsOnUnboundedOutput proves the streaming helper cannot
// be held hostage by output it cannot bound: a git that writes an endless
// line is stopped, and the helper returns instead of waiting on a child that
// blocks on a pipe nobody drains.
func TestHardenedLinesStopsOnUnboundedOutput(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "git")
	// A git that never writes a newline and never stops.
	script := "#!/bin/sh\nwhile : ; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// The fake git comes first; the rest of PATH stays for the shell.
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	done := make(chan error, 1)
	go func() {
		done <- hardenedLines(context.Background(), "", nil, []string{"anything"},
			func(string) (bool, error) { return false, nil })
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("streaming an endless line returned no error")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("streaming an endless line hung")
	}
}

// fakeGit installs a script named git at the front of PATH and returns it.
func fakeGit(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// The fake git comes first; the rest of PATH stays for the shell.
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestHardenedStdoutCapFailsClosed proves the capture bound is enforced: a
// git that writes more stdout than the cap allows is stopped, and the
// command fails instead of being buffered without limit.
func TestHardenedStdoutCapFailsClosed(t *testing.T) {
	// Thirty-three mebibytes of output: past the thirty-two mebibyte cap.
	fakeGit(t, "#!/bin/sh\nhead -c 34603008 /dev/zero\n")
	out, err := Hardened(context.Background(), "", "anything")
	if !errors.Is(err, errOutputTooLarge) {
		t.Fatalf("error = %v, want the capture-limit error", err)
	}
	if out != nil {
		t.Fatalf("stdout = %d bytes, want none", len(out))
	}
}

// TestHardenedStderrCapIsDiscarded proves stderr is bounded without failing
// the command: git's stderr is never propagated to callers, so output past
// the cap is dropped and the command's own result stands.
func TestHardenedStderrCapIsDiscarded(t *testing.T) {
	// Two mebibytes of stderr: past the one mebibyte cap.
	fakeGit(t, "#!/bin/sh\nhead -c 2097152 /dev/zero >&2\nprintf 'ok'\n")
	out, err := Hardened(context.Background(), "", "anything")
	if err != nil {
		t.Fatalf("error = %v, want the command to succeed", err)
	}
	if strings.TrimSpace(string(out)) != "ok" {
		t.Fatalf("stdout = %q, want %q", out, "ok")
	}
}
