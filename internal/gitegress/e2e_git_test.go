package gitegress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/forge/forgetest"
)

const (
	e2eWorkRef   = "refs/heads/courier/run"
	e2eTokenUser = "x-access-token"
	e2eToken     = "sometoken"
)

type e2eProvider struct{}

func (e2eProvider) Credentials(_ context.Context, identity string) (Credential, error) {
	return Credential{Username: e2eTokenUser, Token: e2eToken}, nil
}

// e2eForge keeps the guard-specific adapters local while sharing the real git
// repository and CGI implementation with other suites.
type e2eForge struct {
	t     *testing.T
	forge *forgetest.SmartHTTPForge
	env   []string
	work  string
}

func e2eNewForge(t *testing.T, repos ...string) *e2eForge {
	t.Helper()
	forge := forgetest.NewSmartHTTPForge(t, repos...)
	return &e2eForge{t: t, forge: forge, env: forge.Env(), work: forge.WorkDir()}
}

func (f *e2eForge) remote(name, identity string, push bool) Remote {
	f.t.Helper()
	return Remote{URL: f.forge.UpstreamURL(name), Identity: identity, Push: push}
}

func (f *e2eForge) front(workRef string, remotes ...Remote) *httptest.Server {
	f.t.Helper()
	guard, err := New(Config{Remotes: remotes, WorkRef: workRef, Credentials: e2eProvider{}})
	if err != nil {
		f.t.Fatalf("New: %v", err)
	}
	server := httptest.NewServer(guard)
	f.t.Cleanup(server.Close)
	return server
}

func (f *e2eForge) frontOnListener(workRef string, listener net.Listener, remotes ...Remote) string {
	f.t.Helper()
	guard, err := New(Config{Remotes: remotes, WorkRef: workRef, Credentials: e2eProvider{}})
	if err != nil {
		f.t.Fatalf("New: %v", err)
	}
	server := &http.Server{Handler: guard}
	f.t.Cleanup(func() { _ = server.Close() })
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			f.t.Errorf("guard Serve: %v", err)
		}
	}()
	return "http://" + listener.Addr().String()
}

func (f *e2eForge) frontURL(server *httptest.Server, name string) string {
	f.t.Helper()
	return server.URL + "/" + name + ".git"
}

func (f *e2eForge) gitDir(name string) string   { return f.forge.GitDir(name) }
func (f *e2eForge) tip(name, ref string) string { return f.forge.Tip(name, ref) }

func (f *e2eForge) clone(t *testing.T, server *httptest.Server, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp(f.forge.WorkDir(), "clone-"+name+"-")
	if err != nil {
		t.Fatal(err)
	}
	forgetest.RunGit(t, f.forge.Env(), f.forge.WorkDir(), "clone", f.frontURL(server, name), dir)
	return dir
}

func TestE2ECloneThroughGuard(t *testing.T) {
	forge := e2eNewForge(t, "base", "work")
	front := forge.front(e2eWorkRef,
		forge.remote("base", "o/base", false),
		forge.remote("work", "o/work", true),
	)
	for _, repo := range []string{"base", "work"} {
		t.Run(repo, func(t *testing.T) {
			dir := forge.clone(t, front, repo)
			oid := strings.TrimSpace(forgetest.RunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))
			if oid != forge.tip(repo, "main") {
				t.Fatalf("clone of %s landed on %s, upstream main is %s", repo, oid, forge.tip(repo, "main"))
			}
			if _, err := os.Stat(filepath.Join(dir, "seed.txt")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestE2ENonLoopbackBindCloneAndPush(t *testing.T) {
	forge := e2eNewForge(t, "work")
	listener, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	front := forge.frontOnListener(e2eWorkRef, listener, forge.remote("work", "o/work", true))
	dir, err := os.MkdirTemp(forge.work, "clone-nonloopback-")
	if err != nil {
		t.Fatal(err)
	}
	forgetest.RunGit(t, forge.env, forge.work, "clone", front+"/work.git", dir)
	forgetest.RunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "served on pod address\n")
	head := strings.TrimSpace(forgetest.RunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))
	forgetest.RunGit(t, forge.env, dir, "push", front+"/work.git", "HEAD:"+e2eWorkRef)
	if got := forge.tip("work", e2eWorkRef); got != head {
		t.Fatalf("push through 127.0.0.2 landed %s, want %s", got, head)
	}
}

func TestE2EPushApprovedWorkRef(t *testing.T) {
	forge := e2eNewForge(t, "base", "work")
	front := forge.front(e2eWorkRef,
		forge.remote("base", "o/base", false),
		forge.remote("work", "o/work", true),
	)
	dir := forge.clone(t, front, "work")
	forgetest.RunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "published\n")
	head := strings.TrimSpace(forgetest.RunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))

	forgetest.RunGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:"+e2eWorkRef)

	if got := forge.tip("work", e2eWorkRef); got != head {
		t.Fatalf("approved push landed %s, want %s", got, head)
	}
}

func TestE2EPushOutsideWorkRefRefused(t *testing.T) {
	forge := e2eNewForge(t, "base", "work")
	front := forge.front(e2eWorkRef,
		forge.remote("base", "o/base", false),
		forge.remote("work", "o/work", true),
	)
	dir := forge.clone(t, front, "work")
	before := forge.tip("work", "refs/heads/main")
	e2eCommit(t, forge.env, dir, "main.txt", "should never land\n")

	out, err := forgetest.TryGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push outside the work ref succeeded:\n%s", out)
	}
	e2eAssertRefused(t, out)
	if got := forge.tip("work", "refs/heads/main"); got != before {
		t.Fatalf("main moved from %s to %s", before, got)
	}
}

func TestE2EDeleteApprovedWorkRefRefused(t *testing.T) {
	forge := e2eNewForge(t, "base", "work")
	front := forge.front(e2eWorkRef,
		forge.remote("base", "o/base", false),
		forge.remote("work", "o/work", true),
	)
	dir := forge.clone(t, front, "work")
	forgetest.RunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "published\n")
	forgetest.RunGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:"+e2eWorkRef)
	before := forge.tip("work", e2eWorkRef)

	out, err := forgetest.TryGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), ":"+e2eWorkRef)
	if err == nil {
		t.Fatalf("delete push of the work ref succeeded:\n%s", out)
	}
	e2eAssertRefused(t, out)
	if got := forge.tip("work", e2eWorkRef); got != before {
		t.Fatalf("work ref deleted: %s is gone, was %s", got, before)
	}
}

func TestE2EForceWithLeaseToWorkRefSucceeds(t *testing.T) {
	forge := e2eNewForge(t, "base", "work")
	front := forge.front(e2eWorkRef,
		forge.remote("base", "o/base", false),
		forge.remote("work", "o/work", true),
	)
	dir := forge.clone(t, front, "work")
	forgetest.RunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "first\n")
	forgetest.RunGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:"+e2eWorkRef)
	first := forge.tip("work", e2eWorkRef)

	e2eCommit(t, forge.env, dir, "work.txt", "second\n")
	forgetest.RunGit(t, forge.env, dir, "commit", "--amend", "--no-edit")
	rewritten := strings.TrimSpace(forgetest.RunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))

	forgetest.RunGit(t, forge.env, dir, "push", "--force-with-lease=courier/run:"+first,
		forge.frontURL(front, "work"), "HEAD:"+e2eWorkRef)

	if got := forge.tip("work", e2eWorkRef); got != rewritten {
		t.Fatalf("force-with-lease landed %s, want %s", got, rewritten)
	}
}

func TestE2EPushToReadOnlyRemoteRefused(t *testing.T) {
	forge := e2eNewForge(t, "base", "work")
	front := forge.front(e2eWorkRef,
		forge.remote("base", "o/base", false),
		forge.remote("work", "o/work", true),
	)
	dir := forge.clone(t, front, "base")
	baseMain := forge.tip("base", "refs/heads/main")
	forgetest.RunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "base.txt", "no\n")

	out, err := forgetest.TryGit(t, forge.env, dir, "push", forge.frontURL(front, "base"), "HEAD:"+e2eWorkRef)
	if err == nil {
		t.Fatalf("push to the read-only base remote succeeded:\n%s", out)
	}
	e2eAssertRefused(t, out)
	if got := forge.tip("base", "refs/heads/main"); got != baseMain {
		t.Fatalf("base main moved from %s to %s", baseMain, got)
	}
	if _, err := forgetest.TryGit(t, forge.env, filepath.Dir(forge.forge.GitDir("base")), "--git-dir", forge.gitDir("base"),
		"rev-parse", "--verify", e2eWorkRef); err == nil {
		t.Fatal("base gained the work ref")
	}
}

func TestE2ESameRepositoryRunRefusesOutsideWorkRef(t *testing.T) {
	forge := e2eNewForge(t, "same")
	front := forge.front(e2eWorkRef, forge.remote("same", "o/same", true))

	dir := forge.clone(t, front, "same")
	e2eCommit(t, forge.env, dir, "run.txt", "on the work ref\n")
	forgetest.RunGit(t, forge.env, dir, "push", forge.frontURL(front, "same"), "HEAD:"+e2eWorkRef)

	head := strings.TrimSpace(forgetest.RunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))
	if got := forge.tip("same", e2eWorkRef); got != head {
		t.Fatalf("work-ref push landed %s, want %s", got, head)
	}

	mainBefore := forge.tip("same", "refs/heads/main")
	out, err := forgetest.TryGit(t, forge.env, dir, "push", forge.frontURL(front, "same"), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("same-repository push outside the work ref succeeded:\n%s", out)
	}
	e2eAssertRefused(t, out)
	if got := forge.tip("same", "refs/heads/main"); got != mainBefore {
		t.Fatalf("same-repository main moved from %s to %s", mainBefore, got)
	}
}

func e2eCommit(t *testing.T, env []string, dir, name, content string) {
	forgetest.CommitFile(t, env, dir, name, content)
}

func e2eAssertRefused(t *testing.T, out string) {
	forgetest.AssertRefused(t, out)
}
