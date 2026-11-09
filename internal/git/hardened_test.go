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

// TestHardenedEnvCarriesScopedSafeDirectory pins the #778 fix: a hardened
// call that runs in a directory names exactly that directory in
// safe.directory through the env config, because the executor's global
// safe.directory exception is invisible to the replaced environment and a
// workspace top-level directory can be owned by someone other than the
// runtime uid. The exception must be scoped — never the wildcard, which
// would let a foreign repository's config and its diff drivers execute —
// and ext transport must stay refused alongside it. A call with no
// directory keeps the ownership gate fully armed.
func TestHardenedEnvCarriesScopedSafeDirectory(t *testing.T) {
	fakeGit(t, "#!/bin/sh\n"+
		"printf 'count=%s\\nkey0=%s\\nvalue0=%s\\nkey1=%s\\nvalue1=%s\\n' \\\n"+
		"  \"$GIT_CONFIG_COUNT\" \"$GIT_CONFIG_KEY_0\" \"$GIT_CONFIG_VALUE_0\" \\\n"+
		"  \"${GIT_CONFIG_KEY_1-unset}\" \"${GIT_CONFIG_VALUE_1-unset}\"\n")

	dir := t.TempDir()
	out, err := Hardened(context.Background(), dir, "diff")
	if err != nil {
		t.Fatalf("hardened call with a directory: %v", err)
	}
	got := string(out)
	for _, want := range []string{
		"count=2",
		"key0=protocol.ext.allow",
		"value0=never",
		"key1=safe.directory",
		"value1=" + dir,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Fatalf("hardened env for %s = %q, want %q", dir, got, want)
		}
	}
	if strings.Contains(got, "value1=*") {
		t.Fatalf("hardened env carries the safe.directory wildcard: %q", got)
	}

	out, err = Hardened(context.Background(), "", "diff")
	if err != nil {
		t.Fatalf("hardened call without a directory: %v", err)
	}
	got = string(out)
	if !strings.Contains(got, "count=1\n") || strings.Contains(got, "key1=safe.directory") {
		t.Fatalf("hardened env with no directory = %q, want the ownership gate unmodified", got)
	}
	if !strings.Contains(got, "key0=protocol.ext.allow\nvalue0=never\n") {
		t.Fatalf("hardened env with no directory lost the ext-transport refusal: %q", got)
	}
}

// TestHardenedScopedSafeDirectoryEndsDubiousOwnership pins the #778
// contract end to end with git's own different-owner test hook: a hardened
// call in a repository whose ownership reads as foreign succeeds because
// the environment carries the exception for that one directory, while a
// hardened call from a subdirectory of the same repository — where the
// exception names the subdirectory, not the discovered top level — stays
// refused. The second case is the scoping proof: the exception never
// widens to a wildcard, so a repository other than the named courier
// directory still fails closed.
func TestHardenedScopedSafeDirectoryEndsDubiousOwnership(t *testing.T) {
	repo := t.TempDir()
	initRepo(t, repo)
	writeFile(t, filepath.Join(repo, "tracked.txt"), "base\n")
	commit(t, repo, "base")
	hook := []string{"GIT_TEST_ASSUME_DIFFERENT_OWNER=1"}

	out, _, err := hardened(context.Background(), repo, hook, []string{"rev-parse", "--verify", "HEAD"})
	if err != nil {
		t.Fatalf("hardened call with the scoped exception was still refused: %v", err)
	}
	if strings.TrimSpace(string(out)) == "" {
		t.Fatal("hardened call with the scoped exception produced no OID")
	}

	sub := filepath.Join(repo, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	_, _, err = hardened(context.Background(), sub, hook, []string{"rev-parse", "--git-dir"})
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.ExitCode() != 128 {
		t.Fatalf("hardened call whose exception names a subdirectory = %v, want the exit-128 ownership refusal", err)
	}
}
