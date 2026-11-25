package gitegress

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
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

// e2eForge is a local forge: bare repositories under one project root served
// over smart HTTP by git http-backend, with a guard front server in front of
// it. Tests drive the real git binary against the front URL only.
type e2eForge struct {
	t        *testing.T
	root     string
	work     string
	upstream *httptest.Server
	env      []string
}

func e2eNewForge(t *testing.T, repos ...string) *e2eForge {
	t.Helper()
	e2eRequireGit(t)
	forge := &e2eForge{t: t, root: t.TempDir(), work: t.TempDir(), env: e2eGitEnv(t)}
	for _, repo := range repos {
		forge.seed(repo)
	}
	return forge
}

func (f *e2eForge) seed(name string) {
	f.t.Helper()
	repo := filepath.Join(f.root, name+".git")
	e2eRunGit(f.t, f.env, f.root, "-c", "init.defaultBranch=main", "init", "--bare", repo)
	e2eRunGit(f.t, f.env, repo, "config", "http.receivepack", "true")

	dev := filepath.Join(f.work, name+"-dev")
	e2eRunGit(f.t, f.env, f.work, "-c", "init.defaultBranch=main", "init", dev)
	if err := os.WriteFile(filepath.Join(dev, "seed.txt"), []byte("seed\n"), 0o600); err != nil {
		f.t.Fatal(err)
	}
	e2eRunGit(f.t, f.env, dev, "add", "seed.txt")
	e2eRunGit(f.t, f.env, dev, "commit", "-m", "seed")
	e2eRunGit(f.t, f.env, dev, "push", repo, "main")
}

// remote pins one seeded repository into a guard remote.
func (f *e2eForge) remote(name, identity string, push bool) Remote {
	f.t.Helper()
	return Remote{URL: f.upstreamURL(name), Identity: identity, Push: push}
}

// front starts a guard front server for the given remotes.
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

// upstreamURL turns a repository name into the upstream URL the guard pins.
func (f *e2eForge) upstreamURL(name string) string {
	f.t.Helper()
	if f.upstream == nil {
		f.upstream = httptest.NewServer(e2eBackendHandler(f.t, f.root))
		f.t.Cleanup(f.upstream.Close)
	}
	return f.upstream.URL + "/" + name + ".git"
}

// frontURL turns a repository name into the front URL git commands address.
func (f *e2eForge) frontURL(server *httptest.Server, name string) string {
	f.t.Helper()
	return server.URL + "/" + name + ".git"
}

func (f *e2eForge) gitDir(name string) string {
	return filepath.Join(f.root, name+".git")
}

func (f *e2eForge) tip(name, ref string) string {
	f.t.Helper()
	return strings.TrimSpace(e2eRunGit(f.t, f.env, f.root, "--git-dir", f.gitDir(name), "rev-parse", "--verify", ref))
}

// clone clones one repository through the front server into a fresh directory.
func (f *e2eForge) clone(t *testing.T, server *httptest.Server, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp(f.work, "clone-"+name+"-")
	if err != nil {
		t.Fatal(err)
	}
	e2eRunGit(t, f.env, f.work, "clone", f.frontURL(server, name), dir)
	return dir
}

// e2eBackendHandler serves GIT_PROJECT_ROOT over smart HTTP by spawning
// git http-backend as a CGI process per request.
func e2eBackendHandler(t *testing.T, root string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()

		var body []byte
		if r.Method == http.MethodPost {
			read, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("reading %s body: %v", r.URL.Path, err)
				http.Error(w, "courier-e2e: request body unreadable", http.StatusBadRequest)
				return
			}
			body = read
		}
		env := []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=" + root,
			"GIT_CONFIG_GLOBAL=/dev/null",
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_PROJECT_ROOT=" + root,
			"GIT_HTTP_EXPORT_ALL=1",
			"REMOTE_USER=" + e2eTokenUser,
			"REQUEST_METHOD=" + r.Method,
			"PATH_INFO=" + r.URL.Path,
			"QUERY_STRING=" + r.URL.RawQuery,
			"SERVER_PROTOCOL=HTTP/1.1",
			"GATEWAY_INTERFACE=CGI/1.1",
			// git http-backend insists on a byte count, so the count is the
			// one actually read: a guard that forwards chunked gives http-backend
			// no other way to know where the request ends.
			"CONTENT_LENGTH=" + strconv.Itoa(len(body)),
		}
		if contentType := r.Header.Get("Content-Type"); contentType != "" {
			env = append(env, "CONTENT_TYPE="+contentType)
		}
		// git http-backend reads the negotiated protocol version from
		// GIT_PROTOCOL; a real web server maps the request header onto it.
		if protocol := r.Header.Get("Git-Protocol"); protocol != "" {
			env = append(env, "GIT_PROTOCOL="+protocol)
		}

		cmd := exec.CommandContext(ctx, "git", "http-backend")
		cmd.Env = env
		cmd.Stdin = bytes.NewReader(body)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Errorf("git http-backend %s %s: %v: %s", r.Method, r.URL, err, stderr.String())
			http.Error(w, "courier-e2e: http-backend failed", http.StatusInternalServerError)
			return
		}
		e2eReplayCGI(t, w, stdout.Bytes())
	}
}

func e2eReplayCGI(t *testing.T, w http.ResponseWriter, raw []byte) {
	t.Helper()
	reader := bufio.NewReader(bytes.NewReader(raw))
	status := http.StatusOK
	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if err != nil {
				t.Errorf("http-backend response had no header terminator: %q", raw)
				http.Error(w, "courier-e2e: malformed CGI response", http.StatusBadGateway)
				return
			}
			break
		}
		name, value, found := strings.Cut(line, ":")
		if !found {
			t.Errorf("http-backend response header %q is malformed", line)
			http.Error(w, "courier-e2e: malformed CGI response", http.StatusBadGateway)
			return
		}
		if name == "Status" {
			code, _, _ := strings.Cut(strings.TrimLeft(value, " "), " ")
			parsed, err := strconv.Atoi(code)
			if err != nil {
				t.Errorf("http-backend status %q is malformed", value)
				http.Error(w, "courier-e2e: malformed CGI status", http.StatusBadGateway)
				return
			}
			status = parsed
			continue
		}
		if name == "Content-Length" {
			continue
		}
		w.Header().Add(name, value)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("reading http-backend body: %v", err)
		return
	}
	if status != http.StatusNotModified {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func e2eGitEnv(t *testing.T) []string {
	t.Helper()
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=Courier E2E",
		"GIT_AUTHOR_EMAIL=e2e@example.invalid",
		"GIT_COMMITTER_NAME=Courier E2E",
		"GIT_COMMITTER_EMAIL=e2e@example.invalid",
	}
}

// e2eRunGit runs git with the sanitized environment and returns combined
// output, failing the test when git exits non-zero.
func e2eRunGit(t *testing.T, env []string, dir string, args ...string) string {
	t.Helper()
	out, err := e2eTryGit(t, env, dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func e2eTryGit(t *testing.T, env []string, dir string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "advice.disabled=All"}, args...)...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// e2eRequire skips a missing local tool away from CI and fails on it: the
// push/fetch boundary coverage this suite provides must be visible, not
// silently skipped, wherever results are relied on.
func e2eRequire(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Fatalf("%s: the boundary suite must run in CI", reason)
	}
	t.Skip(reason)
}

func e2eRequireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		e2eRequire(t, "git binary unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "http-backend")
	cmd.Env = []string{}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.CombinedOutput()
	if err == nil {
		return
	}
	combined := string(out) + stderr.String()
	code := 0
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	}
	if code == 129 || strings.Contains(combined, "unknown") || strings.Contains(combined, "not a git command") {
		e2eRequire(t, "git http-backend unavailable")
	}
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
			oid := strings.TrimSpace(e2eRunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))
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
	e2eRunGit(t, forge.env, forge.work, "clone", front+"/work.git", dir)
	e2eRunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "served on pod address\n")
	head := strings.TrimSpace(e2eRunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))
	e2eRunGit(t, forge.env, dir, "push", front+"/work.git", "HEAD:"+e2eWorkRef)
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
	e2eRunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "published\n")
	head := strings.TrimSpace(e2eRunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))

	e2eRunGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:"+e2eWorkRef)

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

	out, err := e2eTryGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:refs/heads/main")
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
	e2eRunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "published\n")
	e2eRunGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:"+e2eWorkRef)
	before := forge.tip("work", e2eWorkRef)

	out, err := e2eTryGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), ":"+e2eWorkRef)
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
	e2eRunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "work.txt", "first\n")
	e2eRunGit(t, forge.env, dir, "push", forge.frontURL(front, "work"), "HEAD:"+e2eWorkRef)
	first := forge.tip("work", e2eWorkRef)

	e2eCommit(t, forge.env, dir, "work.txt", "second\n")
	e2eRunGit(t, forge.env, dir, "commit", "--amend", "--no-edit")
	rewritten := strings.TrimSpace(e2eRunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))

	e2eRunGit(t, forge.env, dir, "push", "--force-with-lease=courier/run:"+first,
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
	e2eRunGit(t, forge.env, dir, "checkout", "-B", "courier/run")
	e2eCommit(t, forge.env, dir, "base.txt", "no\n")

	out, err := e2eTryGit(t, forge.env, dir, "push", forge.frontURL(front, "base"), "HEAD:"+e2eWorkRef)
	if err == nil {
		t.Fatalf("push to the read-only base remote succeeded:\n%s", out)
	}
	e2eAssertRefused(t, out)
	if got := forge.tip("base", "refs/heads/main"); got != baseMain {
		t.Fatalf("base main moved from %s to %s", baseMain, got)
	}
	if _, err := e2eTryGit(t, forge.env, forge.root, "--git-dir", forge.gitDir("base"),
		"rev-parse", "--verify", e2eWorkRef); err == nil {
		t.Fatal("base gained the work ref")
	}
}

func TestE2ESameRepositoryRunRefusesOutsideWorkRef(t *testing.T) {
	forge := e2eNewForge(t, "same")
	front := forge.front(e2eWorkRef, forge.remote("same", "o/same", true))

	dir := forge.clone(t, front, "same")
	e2eCommit(t, forge.env, dir, "run.txt", "on the work ref\n")
	e2eRunGit(t, forge.env, dir, "push", forge.frontURL(front, "same"), "HEAD:"+e2eWorkRef)

	head := strings.TrimSpace(e2eRunGit(t, forge.env, dir, "rev-parse", "--verify", "HEAD"))
	if got := forge.tip("same", e2eWorkRef); got != head {
		t.Fatalf("work-ref push landed %s, want %s", got, head)
	}

	mainBefore := forge.tip("same", "refs/heads/main")
	out, err := e2eTryGit(t, forge.env, dir, "push", forge.frontURL(front, "same"), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("same-repository push outside the work ref succeeded:\n%s", out)
	}
	e2eAssertRefused(t, out)
	if got := forge.tip("same", "refs/heads/main"); got != mainBefore {
		t.Fatalf("same-repository main moved from %s to %s", mainBefore, got)
	}
}

func e2eCommit(t *testing.T, env []string, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	e2eRunGit(t, env, dir, "add", name)
	e2eRunGit(t, env, dir, "commit", "-m", "e2e: "+name)
}

func e2eAssertRefused(t *testing.T, out string) {
	t.Helper()
	lowered := strings.ToLower(out)
	for _, marker := range []string{"403", "reject", "refus", "denied", "not allowed", "hung up"} {
		if strings.Contains(lowered, marker) {
			return
		}
	}
	t.Fatalf("refusal output names no expected marker: %s", out)
}
