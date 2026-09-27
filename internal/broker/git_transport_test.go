package broker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	couriergit "github.com/misospace/courier/internal/git"
)

type staticGitEndpoints map[string]string

func (s staticGitEndpoints) Endpoint(_ context.Context, identity string) (string, error) {
	endpoint, ok := s[identity]
	if !ok {
		return "", os.ErrNotExist
	}
	return endpoint, nil
}

func TestGitPusherUsesOnlyPinnedEndpointAndExactRef(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "trusted.git")
	fork := filepath.Join(root, "fork.git")
	repo := filepath.Join(root, "source")
	gitTransportCommand(t, "", "init", "--bare", remote)
	gitTransportCommand(t, "", "init", "--bare", fork)
	gitTransportCommand(t, "", "init", repo)
	gitTransportCommand(t, repo, "config", "user.email", "test@example.invalid")
	gitTransportCommand(t, repo, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(repo, "file"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, repo, "add", "file")
	gitTransportCommand(t, repo, "commit", "-m", "base")
	gitTransportCommand(t, repo, "branch", "-M", "main")
	base := strings.TrimSpace(string(gitTransportCommand(t, repo, "rev-parse", "HEAD")))
	gitTransportCommand(t, repo, "push", remote, "HEAD:refs/heads/main")
	gitTransportCommand(t, repo, "push", fork, "HEAD:refs/heads/main")
	if err := os.WriteFile(filepath.Join(repo, "file"), []byte("proposed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, repo, "commit", "-am", "proposed")
	proposed := strings.TrimSpace(string(gitTransportCommand(t, repo, "rev-parse", "HEAD")))
	bundle := filepath.Join(root, "proposal.bundle")
	gitTransportCommand(t, repo, "bundle", "create", bundle, "HEAD")
	gitBroker, err := couriergit.NewBroker(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(gitBroker.Directory())
	if err := gitBroker.ImportBundle(ctx, bundle, proposed, base); err != nil {
		t.Fatal(err)
	}

	policy := Policy{RunUID: "run", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/project", BaseRef: "main", BaseOID: base, WorkRepo: "org/project", WorkRef: "courier/org/project/issue-1", WorkInitiallyAbsent: true}
	resolver := staticGitEndpoints{"org/project": remote}
	pusher, err := NewGitPusher(ctx, GitTransportConfig{Policy: policy, Git: gitBroker, Resolver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := pusher.IsAncestor(ctx, "org/project", base, proposed); err != nil || !ok {
		t.Fatalf("IsAncestor = %v, %v", ok, err)
	}
	if err := pusher.Push(ctx, "org/project", "main", proposed); err == nil {
		t.Fatal("accepted base ref as publication target")
	}
	if err := pusher.Push(ctx, "org/project", policy.WorkRef, proposed); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if got := strings.TrimSpace(string(gitTransportCommand(t, "", "--git-dir", remote, "rev-parse", "refs/heads/"+policy.WorkRef))); got != proposed {
		t.Fatalf("trusted remote tip = %s, want %s", got, proposed)
	}
	if err := pusher.Push(ctx, "org/project-fork", policy.WorkRef, proposed); err == nil {
		t.Fatal("accepted unpinned same-name fork identity")
	}
	if got := strings.TrimSpace(string(gitTransportCommand(t, "", "--git-dir", fork, "rev-parse", "refs/heads/main"))); got != base {
		t.Fatalf("fork changed unexpectedly: %s", got)
	}
}

func TestGitPusherRejectsHostilePinsAndNonFastForward(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	repo := filepath.Join(root, "source")
	gitTransportCommand(t, "", "init", "--bare", remote)
	gitTransportCommand(t, "", "init", repo)
	gitTransportCommand(t, repo, "config", "user.email", "test@example.invalid")
	gitTransportCommand(t, repo, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(repo, "file"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, repo, "add", "file")
	gitTransportCommand(t, repo, "commit", "-m", "one")
	gitTransportCommand(t, repo, "branch", "-M", "main")
	base := strings.TrimSpace(string(gitTransportCommand(t, repo, "rev-parse", "HEAD")))
	gitTransportCommand(t, repo, "push", remote, "HEAD:refs/heads/work")
	if err := os.WriteFile(filepath.Join(repo, "file"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, repo, "commit", "-am", "two")
	proposed := strings.TrimSpace(string(gitTransportCommand(t, repo, "rev-parse", "HEAD")))
	bundle := filepath.Join(root, "proposal.bundle")
	gitTransportCommand(t, repo, "bundle", "create", bundle, "HEAD")
	gb, err := couriergit.NewBroker(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(gb.Directory())
	if err := gb.ImportBundle(ctx, bundle, proposed, base); err != nil {
		t.Fatalf("ImportBundle with observed tip: %v", err)
	}
	policy := Policy{RunUID: "run", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/project", BaseRef: "main", BaseOID: base, WorkRepo: "org/project", WorkRef: "work", WorkInitiallyAbsent: false, WorkAnchorOID: base}
	for _, endpoint := range []string{"https://user:secret@example.invalid/repo.git", "ext::sh -c evil", "-oProxyCommand=bad"} {
		if _, err := NewGitPusher(ctx, GitTransportConfig{Policy: policy, Git: gb, Resolver: staticGitEndpoints{"org/project": endpoint}}); err == nil {
			t.Errorf("accepted endpoint %q", endpoint)
		}
	}
	if err := gb.ImportBundle(ctx, bundle, proposed, ""); err != nil {
		t.Fatal(err)
	}
	// A later trusted integration can move the base and publish another commit;
	// these OIDs need not be the admission anchors once the engine has observed
	// and checked their live identities.
	if err := os.WriteFile(filepath.Join(repo, "later"), []byte("later base/work\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, repo, "add", "later")
	gitTransportCommand(t, repo, "commit", "-m", "later")
	later := strings.TrimSpace(string(gitTransportCommand(t, repo, "rev-parse", "HEAD")))
	laterBundle := filepath.Join(root, "later.bundle")
	gitTransportCommand(t, repo, "bundle", "create", laterBundle, "HEAD")
	if err := gb.ImportBundle(ctx, laterBundle, later, proposed); err != nil {
		t.Fatalf("ImportBundle later sequential commit: %v", err)
	}
	if ok, err := NewGitPusher(ctx, GitTransportConfig{Policy: policy, Git: gb, Resolver: staticGitEndpoints{"org/project": remote}}); err != nil {
		t.Fatal(err)
	} else if ok, err := ok.IsAncestor(ctx, "org/project", proposed, later); err != nil || !ok {
		t.Fatalf("sequential IsAncestor = %v, %v", ok, err)
	}
	pusher, err := NewGitPusher(ctx, GitTransportConfig{Policy: policy, Git: gb, Resolver: staticGitEndpoints{"org/project": remote}})
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(root, "other")
	gitTransportCommand(t, "", "clone", remote, other)
	gitTransportCommand(t, other, "config", "user.email", "test@example.invalid")
	gitTransportCommand(t, other, "config", "user.name", "other")
	if err := os.WriteFile(filepath.Join(other, "other"), []byte("foreign\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, other, "add", "other")
	gitTransportCommand(t, other, "commit", "-m", "foreign")
	gitTransportCommand(t, other, "push", "--force", "origin", "HEAD:refs/heads/work")
	if err := pusher.Push(ctx, "org/project", "work", proposed); err == nil {
		t.Fatal("non-fast-forward push succeeded")
	}
}

func TestGitPusherIgnoresInheritedGitEnvironment(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	repo := filepath.Join(root, "source")
	gitTransportCommand(t, "", "init", "--bare", remote)
	gitTransportCommand(t, "", "init", repo)
	gitTransportCommand(t, repo, "config", "user.email", "test@example.invalid")
	gitTransportCommand(t, repo, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(repo, "file"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, repo, "add", "file")
	gitTransportCommand(t, repo, "commit", "-m", "one")
	gitTransportCommand(t, repo, "branch", "-M", "main")
	base := strings.TrimSpace(string(gitTransportCommand(t, repo, "rev-parse", "HEAD")))
	if err := os.WriteFile(filepath.Join(repo, "file"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitTransportCommand(t, repo, "commit", "-am", "two")
	proposed := strings.TrimSpace(string(gitTransportCommand(t, repo, "rev-parse", "HEAD")))
	bundle := filepath.Join(root, "proposal.bundle")
	gitTransportCommand(t, repo, "bundle", "create", bundle, "HEAD")
	gb, err := couriergit.NewBroker(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(gb.Directory())
	if err := gb.ImportBundle(ctx, bundle, proposed, base); err != nil {
		t.Fatal(err)
	}
	policy := Policy{RunUID: "run", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/project", BaseRef: "main", BaseOID: base, WorkRepo: "org/project", WorkRef: "work", WorkInitiallyAbsent: true}
	pusher, err := NewGitPusher(ctx, GitTransportConfig{Policy: policy, Git: gb, Resolver: staticGitEndpoints{"org/project": remote}})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "url.file:///tmp/hostile.insteadOf")
	t.Setenv("GIT_CONFIG_VALUE_0", remote)
	if err := pusher.Push(ctx, "org/project", "work", proposed); err != nil {
		t.Fatalf("Push with inherited hostile Git config: %v", err)
	}
	if got := strings.TrimSpace(string(gitTransportCommand(t, "", "--git-dir", remote, "rev-parse", "refs/heads/work"))); got != proposed {
		t.Fatalf("remote tip = %s, want %s", got, proposed)
	}
}

func gitTransportCommand(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}

func TestValidatePinnedEndpointLocalPathSafety(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "repo.git")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.git")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}

	parent, err := filepath.EvalSymlinks(filepath.Dir(dir))
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(parent, filepath.Base(dir))
	if got, err := validatePinnedEndpoint(dir); err != nil || got != want {
		t.Fatalf("valid non-symlink directory endpoint = %q, %v; want %q", got, err, want)
	}

	for _, tc := range []struct {
		name, in string
	}{
		{"symlink directory", link},
		{"file endpoint symlink", "file://" + link},
		{"regular file", file},
		{"missing path", filepath.Join(root, "missing.git")},
		{"null byte", dir + "\x00"},
		{"carriage return", dir + "\r"},
		{"newline", dir + "\n"},
		{"leading whitespace", " " + dir},
		{"option-like", "-oProxyCommand=bad"},
		{"userinfo", "https://user:secret@example.invalid/repo.git"},
		{"query", "https://example.invalid/repo.git?x=1"},
		{"fragment", "https://example.invalid/repo.git#x"},
		{"unsupported scheme", "ext::sh -c evil"},
		{"file remote host", "file://elsewhere/tmp/x"},
		{"relative path", "relative/path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := validatePinnedEndpoint(tc.in); err == nil {
				t.Fatalf("accepted unsafe endpoint %q", tc.in)
			}
		})
	}

	valid := "https://example.invalid/repo.git"
	if got, err := validatePinnedEndpoint(valid); err != nil || got != valid {
		t.Fatalf("valid https endpoint = %q, %v", got, err)
	}
}
