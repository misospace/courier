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

// TestBlobAtRegularClassifiesEntryKinds pins the hardened regular-blob read
// the bootstrap contract's trust decision depends on (#414): a regular blob
// yields its bytes, a symlink is present but never regular (its target
// bytes are exactly what the caller must not consume), a directory pathspec
// match is not the queried path, and absence — of the path or of the whole
// revision — is reported as absence, never as an error.
func TestBlobAtRegularClassifiesEntryKinds(t *testing.T) {
	repo := t.TempDir()
	initRepo(t, repo)
	writeFile(t, filepath.Join(repo, "tracked.txt"), "base\n")
	commit(t, repo, "base")
	absentRevision, err := HeadOID(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(filepath.Join(repo, ".mise"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(repo, ".mise", "config.toml"), "[tools]\ngo = \"1.27\"\n")
	writeFile(t, filepath.Join(repo, ".mise", "target.txt"), "target\n")
	if err := os.Symlink("target.txt", filepath.Join(repo, ".mise", "link.txt")); err != nil {
		t.Fatal(err)
	}
	commit(t, repo, "contract")

	data, regular, present, err := BlobAtRegular(context.Background(), repo, "HEAD", ".mise/config.toml")
	if err != nil || !present || !regular {
		t.Fatalf("BlobAtRegular regular file = (%q, %v, %v, %v)", data, regular, present, err)
	}
	if string(data) != "[tools]\ngo = \"1.27\"\n" {
		t.Fatalf("BlobAtRegular content = %q", data)
	}

	data, regular, present, err = BlobAtRegular(context.Background(), repo, "HEAD", ".mise/link.txt")
	if err != nil {
		t.Fatalf("BlobAtRegular symlink: %v", err)
	}
	if !present || regular || data != nil {
		t.Fatalf("BlobAtRegular symlink = (%q, %v, %v), want present but not regular with no bytes", data, regular, present)
	}

	if _, regular, present, _ := BlobAtRegular(context.Background(), repo, "HEAD", "missing.txt"); present || regular {
		t.Fatalf("BlobAtRegular missing path = (regular %v, present %v), want absent", regular, present)
	}
	if _, regular, present, _ := BlobAtRegular(context.Background(), repo, absentRevision, ".mise/config.toml"); present || regular {
		t.Fatalf("BlobAtRegular at a revision without the path = (regular %v, present %v), want absent", regular, present)
	}
	if _, regular, present, _ := BlobAtRegular(context.Background(), repo, "no-such-revision", ".mise/config.toml"); present || regular {
		t.Fatalf("BlobAtRegular at an unresolvable revision = (regular %v, present %v), want absent", regular, present)
	}

	// ls-tree matches pathspecs, not only exact paths: querying a directory
	// lists the directory's own tree entry, which is present but — mode
	// 040000 — not a regular blob, so the read reports it as exactly that
	// and the caller fails closed.
	data, regular, present, err = BlobAtRegular(context.Background(), repo, "HEAD", ".mise")
	if err != nil {
		t.Fatalf("BlobAtRegular directory pathspec: %v", err)
	}
	if !present || regular || data != nil {
		t.Fatalf("BlobAtRegular directory pathspec = (%q, %v, %v), want present but not regular with no bytes", data, regular, present)
	}

	if _, _, _, err := BlobAtRegular(context.Background(), repo, "", ".mise/config.toml"); err == nil {
		t.Fatal("BlobAtRegular accepted an empty revision")
	}
}

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
