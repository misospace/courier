package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

// TestInstallationAskpassGitTransport runs the REAL git binary against a local
// git smart-HTTP server that demands the installation-document token as Basic
// auth, and a local HTTPS mock of the Kubernetes Secret API, with git's
// credential helper pointed at this test binary re-executed in the executor's
// askpass mode (the same dispatch precedent execsubprocess_test.go uses). It
// proves the shipped wire contract end to end: clone of the base and push of
// the work branch authenticate with the document tokens; a rotated document
// flows into the next push without a restart; an entry expiring inside the
// source's margin fails the push with no credential ever reaching the wire;
// and an un-pinned remote is refused before any API read.
func TestInstallationAskpassGitTransport(t *testing.T) {
	fixture := newInstallationGitFixture(t)

	// (a) clone of the base remote authenticates with the base entry.
	cloneDir := filepath.Join(fixture.root, "clone")
	if out, err := fixture.runGit("", "clone", "--quiet", fixture.baseURL, cloneDir); err != nil {
		t.Fatalf("clone of base failed: %v\n%s", err, out)
	}
	if seen := fixture.authorized("base.git"); len(seen) == 0 {
		t.Fatal("base clone never authenticated against the git server")
	} else if bad := fixture.unexpected("base.git", "base-token-1"); len(bad) != 0 {
		t.Fatalf("base clone used unexpected credentials: %v", bad)
	}

	// (b) push of the work branch authenticates with the work entry.
	fixture.commit(t, cloneDir, "work.txt", "one\n")
	if out, err := fixture.runGit(cloneDir, "push", "--quiet", fixture.workURL, "HEAD:refs/heads/work"); err != nil {
		t.Fatalf("push of work failed: %v\n%s", err, out)
	}
	if seen := fixture.authorized("work.git"); len(seen) == 0 {
		t.Fatal("work push never authenticated against the git server")
	} else if bad := fixture.unexpected("work.git", "work-token-1"); len(bad) != 0 {
		t.Fatalf("work push used unexpected credentials: %v", bad)
	}

	// (c) rotating the document token flows into the NEXT push: the helper
	// re-reads the Secret per prompt, so no restart or cache flush is needed.
	fixture.rotateWorkToken("work-token-2")
	rotatedFrom := fixture.authorizedCount("work.git")
	fixture.commit(t, cloneDir, "work.txt", "two\n")
	if out, err := fixture.runGit(cloneDir, "push", "--quiet", fixture.workURL, "HEAD:refs/heads/work"); err != nil {
		t.Fatalf("push after rotation failed: %v\n%s", err, out)
	}
	for _, seen := range fixture.authorized("work.git")[rotatedFrom:] {
		if seen != "x-access-token:work-token-2" {
			t.Fatalf("rotation push carried %q, want the rotated token", seen)
		}
	}

	// (d) an entry expiring inside the source's margin fails the push, and
	// no fallback credential reaches the wire at all.
	fixture.expireWorkEntry()
	beforeExpired := fixture.authorizedCount("work.git")
	if out, err := fixture.runGit(cloneDir, "push", "--quiet", fixture.workURL, "HEAD:refs/heads/never"); err == nil {
		t.Fatalf("push with an expired entry succeeded\n%s", out)
	}
	if leaked := fixture.authorizedCount("work.git") - beforeExpired; leaked != 0 {
		t.Fatalf("expired entry leaked %d authenticated request(s), want none", leaked)
	}

	// (e) a remote outside the pinned set is refused with no credentials and
	// not a single read of the Kubernetes API.
	fixture.resetKubeReads()
	if out, err := fixture.runGit(cloneDir, "push", "--quiet", fixture.otherURL, "HEAD:refs/heads/never"); err == nil {
		t.Fatalf("push to an un-pinned remote succeeded\n%s", out)
	}
	if reads := fixture.kubeReadCount(); reads != 0 {
		t.Fatalf("un-pinned push triggered %d Secret read(s), want none", reads)
	}
	if seen := fixture.authorized("other.git"); len(seen) != 0 {
		t.Fatalf("un-pinned remote carried credentials: %v", seen)
	}
}

type installationGitFixture struct {
	t    *testing.T
	root string

	baseURL, workURL, otherURL string

	mu            sync.Mutex
	document      topology.InstallationTokenDocument
	kubeReads     int
	authorizedLog map[string][]string

	kubeServer *httptest.Server
	gitServer  *httptest.Server
}

func newInstallationGitFixture(t *testing.T) *installationGitFixture {
	t.Helper()
	root := t.TempDir()
	validUntil := time.Now().Add(24 * time.Hour).UTC()
	fixture := &installationGitFixture{
		t: t, root: root,
		document: topology.InstallationTokenDocument{
			Version: topology.InstallationTokenDocumentVersion, Owner: "acme", Endpoint: "https://api.github.invalid",
			Entries: []topology.InstallationTokenEntry{
				{Repository: "acme/widgets", Token: "base-token-1", ExpiresAt: validUntil},
				{Repository: "fork/widgets", Token: "work-token-1", ExpiresAt: validUntil},
			},
		},
		authorizedLog: map[string][]string{},
	}
	seed := filepath.Join(root, "base-seed")
	if out, err := setupGit(root, "init", "-q", "-b", "main", seed); err != nil {
		t.Fatalf("seed base repo: %v\n%s", err, out)
	}
	if out, err := setupGit(seed, "commit", "-q", "--allow-empty", "-m", "base"); err != nil {
		t.Fatalf("seed base commit: %v\n%s", err, out)
	}
	if out, err := setupGit(root, "clone", "--quiet", "--bare", seed, filepath.Join(root, "base.git")); err != nil {
		t.Fatalf("create bare base: %v\n%s", err, out)
	}
	for _, name := range []string{"work.git", "other.git"} {
		if out, err := setupGit(root, "init", "--bare", "--quiet", filepath.Join(root, name)); err != nil {
			t.Fatalf("create bare %s: %v\n%s", name, err, out)
		}
	}
	fixture.kubeServer = httptest.NewTLSServer(http.HandlerFunc(fixture.handleKube))
	fixture.gitServer = httptest.NewTLSServer(http.HandlerFunc(fixture.handleGit))
	t.Cleanup(func() { fixture.kubeServer.Close(); fixture.gitServer.Close() })
	fixture.baseURL = fixture.gitServer.URL + "/base.git"
	fixture.workURL = fixture.gitServer.URL + "/work.git"
	fixture.otherURL = fixture.gitServer.URL + "/other.git"
	return fixture
}

func setupGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.DevNull,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.invalid",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.invalid",
		"GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0",
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	return out.String(), cmd.Run()
}

func (f *installationGitFixture) writePEM(name string, certDER []byte) string {
	f.t.Helper()
	path := filepath.Join(f.root, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0600); err != nil {
		f.t.Fatal(err)
	}
	return path
}

// askpassEnv is the full child environment for every git transport operation:
// no static COURIER_GIT_* credential exists anywhere in it, so any accepted
// credential on the wire can only have come from the installation document.
func (f *installationGitFixture) askpassEnv() []string {
	f.t.Helper()
	kubeURL, err := url.Parse(f.kubeServer.URL)
	if err != nil {
		f.t.Fatal(err)
	}
	tokenFile := filepath.Join(f.root, "projected-token")
	if err := os.WriteFile(tokenFile, []byte("projected-bearer-token"), 0600); err != nil {
		f.t.Fatal(err)
	}
	helper := filepath.Join(f.root, "askpass.sh")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nexec \"$COURIER_TEST_ASKPASS_BINARY\" \"$@\"\n"), 0700); err != nil {
		f.t.Fatal(err)
	}
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.DevNull,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_OPTIONAL_LOCKS=0",
		// App-mode pods must ask git to include the repository path in
		// credential prompts, or the prompt cannot be bound to a pinned
		// repository and the helper refuses. The launcher (pod.go) carries
		// the same configuration into the coordinator container.
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=credential.useHttpPath",
		"GIT_CONFIG_VALUE_0=true",
		"GIT_SSL_CAINFO=" + f.writePEM("git-ca.pem", f.gitServer.Certificate().Raw),
		"GIT_ASKPASS=" + helper,
		"GIT_ASKPASS_REQUIRE=force",
		"GIT_AUTHOR_NAME=test",
		"GIT_AUTHOR_EMAIL=test@example.invalid",
		"GIT_COMMITTER_NAME=test",
		"GIT_COMMITTER_EMAIL=test@example.invalid",
		executorHelperProcessEnv + "=1",
		"COURIER_ASKPASS=1",
		"COURIER_TEST_ASKPASS_BINARY=" + executorHelperPath(f.t),
		executor.EnvGitInstallation + "=1",
		executor.EnvInstallationOwner + "=acme",
		executor.EnvInstallationEndpoint + "=https://api.github.invalid",
		"POD_NAMESPACE=courier-system",
		executor.EnvInstallationSecret + "=run-7-provider-creds",
		"COURIER_REPO=acme/widgets",
		"COURIER_HEAD_REPO=fork/widgets",
		"COURIER_REPO_URL=" + f.workURL,
		"COURIER_BASE_REPO_URL=" + f.baseURL,
		executor.EnvAPITokenFile + "=" + tokenFile,
		executor.EnvAPICertFile + "=" + f.writePEM("kube-ca.pem", f.kubeServer.Certificate().Raw),
		"KUBERNETES_SERVICE_HOST=" + kubeURL.Hostname(),
		"KUBERNETES_SERVICE_PORT=" + kubeURL.Port(),
	}
}

// runGit runs the real git binary under the App-mode helper environment. An
// empty dir runs the command outside any repository.
func (f *installationGitFixture) runGit(dir string, args ...string) (string, error) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = f.askpassEnv()
	cmd.WaitDelay = 10 * time.Second
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

func (f *installationGitFixture) commit(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := f.runGit(dir, "add", "--", name); err != nil {
		t.Fatalf("git add: %v\n%s", err, out)
	}
	if out, err := f.runGit(dir, "commit", "-q", "-m", "fixture change"); err != nil {
		t.Fatalf("git commit: %v\n%s", err, out)
	}
}

func (f *installationGitFixture) rotateWorkToken(token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.document.Entries {
		if strings.EqualFold(f.document.Entries[i].Repository, "fork/widgets") {
			f.document.Entries[i].Token = token
			f.document.Entries[i].ExpiresAt = time.Now().Add(24 * time.Hour).UTC()
		}
	}
}

func (f *installationGitFixture) expireWorkEntry() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.document.Entries {
		if strings.EqualFold(f.document.Entries[i].Repository, "fork/widgets") {
			f.document.Entries[i].ExpiresAt = time.Now().Add(30 * time.Second).UTC()
		}
	}
}

func (f *installationGitFixture) authorized(repo string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.authorizedLog[repo]...)
}

func (f *installationGitFixture) authorizedCount(repo string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.authorizedLog[repo])
}

func (f *installationGitFixture) unexpected(repo, wantToken string) []string {
	var bad []string
	for _, seen := range f.authorized(repo) {
		if seen != "x-access-token:"+wantToken {
			bad = append(bad, seen)
		}
	}
	return bad
}

func (f *installationGitFixture) resetKubeReads() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kubeReads = 0
}

func (f *installationGitFixture) kubeReadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kubeReads
}

// handleKube serves GET /api/v1/namespaces/<ns>/secrets/<run-creds> for the
// fixed projected bearer token, always from the CURRENT document, so rotation
// and expiry take effect on the next read.
func (f *installationGitFixture) handleKube(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet ||
		r.URL.Path != "/api/v1/namespaces/courier-system/secrets/run-7-provider-creds" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("Authorization") != "Bearer projected-bearer-token" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	f.mu.Lock()
	f.kubeReads++
	document, err := json.Marshal(f.document)
	f.mu.Unlock()
	if err != nil {
		http.Error(w, "document unavailable", http.StatusInternalServerError)
		return
	}
	secret := corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "run-7-provider-creds"},
		Data:       map[string][]byte{executor.InstallationTokenDocumentKey: document},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(secret); err != nil {
		http.Error(w, "secret unavailable", http.StatusInternalServerError)
	}
}

// handleGit implements minimal git smart-HTTP by proxying stateless-rpc
// subprocesses for every configured repository, demanding that repository's
// current installation token as Basic auth and answering anything else with a
// fresh 401 challenge.
func (f *installationGitFixture) handleGit(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.Trim(r.URL.Path, "/")
	var repo, service, mode string
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(trimmed, "/info/refs") &&
		(r.URL.Query().Get("service") == "git-upload-pack" || r.URL.Query().Get("service") == "git-receive-pack"):
		repo, mode = strings.TrimSuffix(trimmed, "/info/refs"), "advertise"
		service = strings.TrimPrefix(r.URL.Query().Get("service"), "git-")
	case r.Method == http.MethodPost && strings.HasSuffix(trimmed, "/git-upload-pack"):
		repo, service, mode = strings.TrimSuffix(trimmed, "/git-upload-pack"), "upload-pack", "rpc"
	case r.Method == http.MethodPost && strings.HasSuffix(trimmed, "/git-receive-pack"):
		repo, service, mode = strings.TrimSuffix(trimmed, "/git-receive-pack"), "receive-pack", "rpc"
	default:
		http.NotFound(w, r)
		return
	}
	if repo != "base.git" && repo != "work.git" && repo != "other.git" {
		http.NotFound(w, r)
		return
	}
	if !f.authorize(w, r, repo) {
		return
	}
	if mode == "advertise" {
		f.advertise(w, service, filepath.Join(f.root, repo))
		return
	}
	f.rpc(w, r, service, filepath.Join(f.root, repo))
}

// authorize admits exactly the x-access-token / current un-expiring entry for
// the repository, and records the accepted credential identity for assertions.
func (f *installationGitFixture) authorize(w http.ResponseWriter, r *http.Request, repo string) bool {
	f.mu.Lock()
	var accepted string
	if repo != "other.git" {
		identity := "acme/widgets"
		if repo == "work.git" {
			identity = "fork/widgets"
		}
		if entry, ok := f.document.Entry(identity); ok && entry.ExpiresAt.After(time.Now().Add(2*time.Minute)) {
			want := "Basic " + base64.StdEncoding.EncodeToString([]byte(topology.DefaultGitUsername+":"+entry.Token))
			if r.Header.Get("Authorization") == want {
				accepted = topology.DefaultGitUsername + ":" + entry.Token
			}
		}
	}
	if accepted != "" {
		f.authorizedLog[repo] = append(f.authorizedLog[repo], accepted)
	}
	f.mu.Unlock()
	if accepted == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="git", charset="UTF-8"`)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "authentication required\n")
		return false
	}
	return true
}

func (f *installationGitFixture) advertise(w http.ResponseWriter, service, dir string) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("git", service, "--stateless-rpc", "--advertise-refs", dir)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 10 * time.Second
	if err := cmd.Run(); err != nil {
		http.Error(w, fmt.Sprintf("advertise failed: %s", stderr.String()), http.StatusInternalServerError)
		return
	}
	header := "# service=git-" + service + "\n"
	w.Header().Set("Content-Type", "application/x-git-"+service+"-advertisement")
	_, _ = fmt.Fprintf(w, "%04x%s0000", len(header)+4, header)
	_, _ = w.Write(stdout.Bytes())
}

func (f *installationGitFixture) rpc(w http.ResponseWriter, r *http.Request, service, dir string) {
	var stderr bytes.Buffer
	cmd := exec.Command("git", service, "--stateless-rpc", dir)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = r.Body, w, &stderr
	cmd.WaitDelay = 30 * time.Second
	w.Header().Set("Content-Type", "application/x-git-"+service+"-result")
	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			http.Error(w, fmt.Sprintf("rpc failed: %s", stderr.String()), http.StatusInternalServerError)
			return
		}
		http.Error(w, "rpc failed", http.StatusInternalServerError)
	}
}
