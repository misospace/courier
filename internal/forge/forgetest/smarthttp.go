package forgetest

// This file promotes the real-git-backed smart-HTTP forge double into the
// shared test-support home, so every suite that must exercise the git wire
// protocol uses one implementation instead of maintaining drifting copies.
//
// The double seeds bare repositories with real commits and serves Git's smart
// HTTP protocol through the real git http-backend CGI. It is test support only:
// no production package imports it.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
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

// SmartHTTPForge is a local forge backed by bare git repositories and served
// over smart HTTP by git http-backend.
type SmartHTTPForge struct {
	t        *testing.T
	root     string
	work     string
	upstream *httptest.Server
	env      []string
}

// NewSmartHTTPForge seeds each named repository with a main branch and one
// commit. Git and its http-backend CGI are required for this boundary test.
func NewSmartHTTPForge(t *testing.T, repos ...string) *SmartHTTPForge {
	t.Helper()
	RequireGit(t)
	forge := &SmartHTTPForge{t: t, root: t.TempDir(), work: t.TempDir(), env: gitEnv(t)}
	for _, repo := range repos {
		forge.Seed(repo)
	}
	return forge
}

// Seed creates a bare repository with an initial main commit and enables
// receive-pack so the guarded push path can be exercised.
func (f *SmartHTTPForge) Seed(name string) {
	f.t.Helper()
	repo := filepath.Join(f.root, name+".git")
	if err := os.MkdirAll(filepath.Dir(repo), 0o700); err != nil {
		f.t.Fatal(err)
	}
	RunGit(f.t, f.env, f.root, "-c", "init.defaultBranch=main", "init", "--bare", repo)
	RunGit(f.t, f.env, repo, "config", "http.receivepack", "true")

	dev := filepath.Join(f.work, name+"-dev")
	RunGit(f.t, f.env, f.work, "-c", "init.defaultBranch=main", "init", dev)
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dev, "seed.txt")), 0o700); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dev, "seed.txt"), []byte("seed\n"), 0o600); err != nil {
		f.t.Fatal(err)
	}
	RunGit(f.t, f.env, dev, "add", "seed.txt")
	RunGit(f.t, f.env, dev, "commit", "-m", "seed")
	RunGit(f.t, f.env, dev, "push", repo, "main")
}

// UpstreamURL turns a repository name into its local smart-HTTP URL.
func (f *SmartHTTPForge) UpstreamURL(name string) string {
	f.t.Helper()
	if f.upstream == nil {
		f.upstream = httptest.NewServer(backendHandler(f.t, f.root))
		f.t.Cleanup(f.upstream.Close)
	}
	return f.upstream.URL + "/" + name + ".git"
}

// GitDir returns the path of a seeded bare repository.
func (f *SmartHTTPForge) GitDir(name string) string { return filepath.Join(f.root, name+".git") }

// Tip returns the live object ID of a repository ref.
func (f *SmartHTTPForge) Tip(name, ref string) string {
	f.t.Helper()
	return strings.TrimSpace(RunGit(f.t, f.env, f.root, "--git-dir", f.GitDir(name), "rev-parse", "--verify", ref))
}

// Env returns a sanitized git subprocess environment.
func (f *SmartHTTPForge) Env() []string { return append([]string(nil), f.env...) }

// WorkDir returns the forge's isolated scratch directory for clones.
func (f *SmartHTTPForge) WorkDir() string { return f.work }

// RunGit runs git with a sanitized environment and fails the test on error.
func RunGit(t *testing.T, env []string, dir string, args ...string) string {
	t.Helper()
	out, err := TryGit(t, env, dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// TryGit runs git and returns its combined output and error.
func TryGit(t *testing.T, env []string, dir string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "advice.disabled=All"}, args...)...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// CommitFile writes, stages, and commits one file.
func CommitFile(t *testing.T, env []string, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	RunGit(t, env, dir, "add", name)
	RunGit(t, env, dir, "commit", "-m", "e2e: "+name)
}

// RequireGit skips missing local tools away from CI and fails on them in CI,
// keeping real git boundary coverage visible wherever results are relied on.
func RequireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		require(t, "git binary unavailable")
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
		require(t, "git http-backend unavailable")
	}
}

func require(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Fatalf("%s: the boundary suite must run in CI", reason)
	}
	t.Skip(reason)
}

// AssertRefused checks for a refusal marker in git's diagnostic output.
func AssertRefused(t *testing.T, out string) {
	t.Helper()
	lowered := strings.ToLower(out)
	for _, marker := range []string{"403", "reject", "refus", "denied", "not allowed", "hung up"} {
		if strings.Contains(lowered, marker) {
			return
		}
	}
	t.Fatalf("refusal output names no expected marker: %s", out)
}

func gitEnv(t *testing.T) []string {
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

func backendHandler(t *testing.T, root string) http.HandlerFunc {
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
			"REMOTE_USER=x-access-token",
			"REQUEST_METHOD=" + r.Method,
			"PATH_INFO=" + r.URL.Path,
			"QUERY_STRING=" + r.URL.RawQuery,
			"SERVER_PROTOCOL=HTTP/1.1",
			"GATEWAY_INTERFACE=CGI/1.1",
			// CGI needs the byte count actually read; a forwarded chunked body
			// gives the backend no other way to learn where the request ends.
			"CONTENT_LENGTH=" + strconv.Itoa(len(body)),
		}
		if contentType := r.Header.Get("Content-Type"); contentType != "" {
			env = append(env, "CONTENT_TYPE="+contentType)
		}
		// The web server maps this negotiated header to the CGI variable.
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
		replayCGI(t, w, stdout.Bytes())
	}
}

func replayCGI(t *testing.T, w http.ResponseWriter, raw []byte) {
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
