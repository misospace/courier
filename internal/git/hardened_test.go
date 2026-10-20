package git

import (
	"context"
	"os"
	"path/filepath"
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
