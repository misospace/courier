package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBrokerImportsValidatesAndPushesFastForward(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "base.txt"), "base\n")
	commit(t, source, "base")
	git(t, source, "branch", "-M", "main")
	base := strings.TrimSpace(string(mustGit(t, source, "rev-parse", "HEAD")))
	git(t, source, "push", remote, "HEAD:refs/heads/work")

	writeFile(t, filepath.Join(source, "work.txt"), "work\n")
	commit(t, source, "work")
	proposed := strings.TrimSpace(string(mustGit(t, source, "rev-parse", "HEAD")))
	bundle := filepath.Join(root, "work.bundle")
	git(t, source, "bundle", "create", bundle, "HEAD")

	broker, err := NewBroker(ctx, root)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer os.RemoveAll(broker.Directory())
	if err := broker.ImportBundle(ctx, bundle, proposed, base); err != nil {
		t.Fatalf("ImportBundle: %v", err)
	}
	if err := broker.Push(ctx, remote, "work", proposed); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got, exists, err := broker.Observe(ctx, remote, "work")
	if err != nil || !exists || got != proposed {
		t.Fatalf("Observe = %q, %v, %v; want proposed tip", got, exists, err)
	}
}

func TestBrokerAllowsAbsentExpectedRefAndObservesLiveAbsence(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	initBare(t, remote)
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "file"), "one\n")
	commit(t, source, "first")
	proposed := strings.TrimSpace(string(mustGit(t, source, "rev-parse", "HEAD")))
	bundle := filepath.Join(root, "work.bundle")
	git(t, source, "bundle", "create", bundle, "HEAD")
	broker, err := NewBroker(ctx, root)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer os.RemoveAll(broker.Directory())
	if err := broker.ImportBundle(ctx, bundle, proposed, ""); err != nil {
		t.Fatalf("ImportBundle for absent ref: %v", err)
	}
	if oid, exists, err := broker.Observe(ctx, remote, "new-work"); err != nil || exists || oid != "" {
		t.Fatalf("Observe absent = %q, %v, %v", oid, exists, err)
	}
	if err := broker.Push(ctx, remote, "new-work", proposed); err != nil {
		t.Fatalf("Push absent ref: %v", err)
	}
}

func TestBrokerRejectsNonFastForwardAndInvalidInputsWithoutLeakingURL(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	initRepo(t, source)
	writeFile(t, filepath.Join(source, "file"), "one\n")
	commit(t, source, "first")
	base := strings.TrimSpace(string(mustGit(t, source, "rev-parse", "HEAD")))
	writeFile(t, filepath.Join(source, "file"), "two\n")
	commit(t, source, "second")
	proposed := strings.TrimSpace(string(mustGit(t, source, "rev-parse", "HEAD")))
	git(t, source, "branch", "work", proposed)
	git(t, source, "checkout", "-b", "divergent", base)
	writeFile(t, filepath.Join(source, "other"), "divergent\n")
	commit(t, source, "divergent")
	divergent := strings.TrimSpace(string(mustGit(t, source, "rev-parse", "HEAD")))
	bundle := filepath.Join(root, "work.bundle")
	git(t, source, "bundle", "create", bundle, "work", "divergent")
	broker, err := NewBroker(ctx, root)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer os.RemoveAll(broker.Directory())
	if err := broker.ImportBundle(ctx, bundle, proposed, base); err != nil {
		t.Fatalf("ImportBundle rejected an expected ancestor: %v", err)
	}
	if err := broker.ImportBundle(ctx, bundle, proposed, divergent); err == nil {
		t.Fatal("ImportBundle accepted a non-ancestor expected tip")
	}
	for _, ref := range []string{"-f", "main:other", "main..other", "main\n--force"} {
		if err := broker.Push(ctx, filepath.Join(root, "remote.git"), ref, proposed); err == nil {
			t.Errorf("Push accepted hostile ref %q", ref)
		}
	}
	secretURL := "https://user:secret@example.invalid/repo.git"
	if err := broker.Push(ctx, secretURL, "main", proposed); err == nil {
		t.Fatal("Push accepted URL userinfo")
	} else if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "user") {
		t.Fatalf("error leaked URL credentials: %v", err)
	}
	if _, _, err := broker.Observe(ctx, "-oProxyCommand=bad", "main"); err == nil {
		t.Fatal("Observe accepted option-like URL")
	}
}

func mustGit(t *testing.T, directory string, args ...string) []byte {
	t.Helper()
	out, err := gitOutput(directory, args...)
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return out
}
