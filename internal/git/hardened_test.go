package git

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
