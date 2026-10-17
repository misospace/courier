package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type testAuthenticator struct {
	calls  int
	fail   bool
	runUID string
}
type serverObserver struct{}

type importTestObserver struct{ serverObserver }

func (importTestObserver) Repository(_ context.Context, repo, ref string) (RepositoryState, error) {
	if ref == "main" {
		return RepositoryState{Repo: repo, Ref: ref, Exists: true, OID: strings.Repeat("a", 40)}, nil
	}
	return RepositoryState{Repo: repo, Ref: ref, ProtectionKnown: true, WriteKnown: true, Writable: true}, nil
}

func (serverObserver) Repository(context.Context, string, string) (RepositoryState, error) {
	return RepositoryState{}, nil
}
func (serverObserver) PullRequest(context.Context, int) (PullRequestState, error) {
	return PullRequestState{}, nil
}
func (serverObserver) FindPullRequest(context.Context, string, string) ([]PullRequestState, error) {
	return nil, nil
}
func (serverObserver) CreatePullRequest(context.Context, CreatePullRequest) (int, error) {
	return 0, nil
}
func (serverObserver) UpdatePullRequest(context.Context, int, UpdatePullRequest) error { return nil }

type serverPusher struct{}
type serverImporter struct {
	calls    int
	path     string
	proposed string
	expected string
	err      error
}

func (i *serverImporter) ImportBundle(ctx context.Context, path, proposed, expected string) error {
	i.calls++
	i.path, i.proposed, i.expected = path, proposed, expected
	if _, err := os.Stat(path); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return i.err
}

func (serverPusher) IsAncestor(context.Context, string, string, string) (bool, error) {
	return false, nil
}
func (serverPusher) Push(context.Context, string, string, string) error { return nil }
func (a *testAuthenticator) Authenticate(_ context.Context, token string) (Identity, error) {
	a.calls++
	if a.fail || token != "valid-token" {
		return Identity{}, context.Canceled
	}
	if a.runUID != "" {
		return Identity{RunUID: types.UID(a.runUID)}, nil
	}
	return Identity{RunUID: "run-uid"}, nil
}

func serverForTest(t *testing.T) (*Server, *testAuthenticator) {
	t.Helper()
	cert, key := testTLSFiles(t)
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/repo", BaseRef: "main", BaseOID: strings.Repeat("a", 40), WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true, SourceIssue: SourceIssue{Owner: "org", Name: "repo", Number: 1}}, importTestObserver{}, serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	auth := &testAuthenticator{}
	s, err := NewServer(ServerConfig{Policy: engine, Authenticator: auth, Importer: &serverImporter{}, ScratchDir: t.TempDir(), TLSCertFile: cert, TLSKeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	return s, auth
}

func TestServerTypedRoutesAndAuthentication(t *testing.T) {
	s, auth := serverForTest(t)
	tests := []struct {
		name, method, path, header, body string
		want                             int
	}{
		{"unauthenticated", "POST", PathPublish, "", `{}`, http.StatusUnauthorized},
		{"wrong scheme", "POST", PathPublish, "Basic valid-token", `{}`, http.StatusUnauthorized},
		{"bearer spacing", "POST", PathPublish, "Bearer  valid-token", `{}`, http.StatusUnauthorized},
		{"bad token", "POST", PathPublish, "Bearer invalid", `{}`, http.StatusUnauthorized},
		{"wrong method", "GET", PathPublish, "Bearer valid-token", `{}`, http.StatusMethodNotAllowed},
		{"raw path", "POST", "/v1/raw", "Bearer valid-token", `{}`, http.StatusNotFound},
		{"arbitrary URL route", "POST", "/v1/proxy/https://example.com", "Bearer valid-token", `{}`, http.StatusNotFound},
		{"trusted status hidden", "POST", PathTrustedStatus, "Bearer valid-token", `{"lastCommit":"x"}`, http.StatusNotFound},
		{"unknown parameter", "POST", PathPublish, "Bearer valid-token", `{"url":"https://example.com"}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, req)
			if w.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, tt.want, w.Body.String())
			}
		})
	}
	if auth.calls == 0 {
		t.Fatal("expected authentication to run")
	}
}

func TestServerRejectsWrongRunIdentity(t *testing.T) {
	s, auth := serverForTest(t)
	auth.runUID = "other-run"
	req := httptest.NewRequest("POST", PathPublish, strings.NewReader(`{"proposedOID":"new"}`))
	req.Header.Set("Authorization", "Bearer valid-token")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want unauthorized", w.Code)
	}
}

func TestServerImportsBundleFromPrivateScratch(t *testing.T) {
	s, _ := serverForTest(t)
	importer := s.importer.(*serverImporter)
	req := httptest.NewRequest(http.MethodPost, PathImportBundle, strings.NewReader("pack bytes"))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("X-Courier-Expected-Tip", "")
	req.Header.Set("X-Courier-Proposed-OID", strings.Repeat("b", 40))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK || importer.calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", w.Code, importer.calls, w.Body.String())
	}
	if !strings.HasPrefix(importer.path, s.scratchDir+string(os.PathSeparator)) || importer.proposed != strings.Repeat("b", 40) || importer.expected != "" {
		t.Fatalf("unexpected importer args: %#v", importer)
	}
	if _, err := os.Stat(importer.path); !os.IsNotExist(err) {
		t.Fatalf("temporary bundle remains after import: err=%v", err)
	}
}

func TestServerRejectsInvalidBundleRequests(t *testing.T) {
	requests := []struct {
		name   string
		body   string
		mutate func(*http.Request)
	}{
		{"empty body", "", nil},
		{"invalid proposed OID", "pack bytes", func(r *http.Request) { r.Header.Set("X-Courier-Proposed-OID", "bad") }},
		{"invalid expected tip", "pack bytes", func(r *http.Request) { r.Header.Set("X-Courier-Expected-Tip", strings.Repeat("z", 40)) }},
	}
	for _, test := range requests {
		s, _ := serverForTest(t)
		importer := s.importer.(*serverImporter)
		req := httptest.NewRequest(http.MethodPost, PathImportBundle, strings.NewReader(test.body))
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Courier-Proposed-OID", strings.Repeat("b", 40))
		if test.mutate != nil {
			test.mutate(req)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code < 400 || importer.calls != 0 {
			t.Fatalf("%s: status=%d importer calls=%d", test.name, w.Code, importer.calls)
		}
	}
}

func TestServerServesSeedSnapshot(t *testing.T) {
	cert, key := testTLSFiles(t)
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/repo", BaseRef: "main", BaseOID: strings.Repeat("a", 40), WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true, SourceIssue: SourceIssue{Owner: "org", Name: "repo", Number: 1}}, importTestObserver{}, serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	handler := &stubSnapshotHandler{snapshot: EngineSnapshot{
		Tip: strings.Repeat("a", 40), BaseTip: strings.Repeat("b", 40),
		WorkRefExists: true, Data: []byte("bundle-bytes"),
	}}
	s, err := NewServer(ServerConfig{Policy: engine, Authenticator: &testAuthenticator{}, Importer: &serverImporter{}, Snapshot: handler, ScratchDir: t.TempDir(), TLSCertFile: cert, TLSKeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, PathSnapshot, nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != "bundle-bytes" {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if w.Header().Get("X-Courier-Snapshot-Tip") != strings.Repeat("a", 40) ||
		w.Header().Get("X-Courier-Snapshot-Base-Tip") != strings.Repeat("b", 40) ||
		w.Header().Get("X-Courier-Snapshot-Work-Ref-Exists") != "true" {
		t.Fatalf("snapshot headers = %v", w.Header())
	}
	if handler.calls != 1 {
		t.Fatalf("handler calls = %d", handler.calls)
	}
	// Unauthenticated requests are refused before any snapshot render.
	req = httptest.NewRequest(http.MethodGet, PathSnapshot, nil)
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || handler.calls != 1 {
		t.Fatalf("unauthenticated snapshot read: status=%d calls=%d", w.Code, handler.calls)
	}
}

type stubSnapshotHandler struct {
	snapshot EngineSnapshot
	err      error
	calls    int
}

func (h *stubSnapshotHandler) SnapshotForControl(context.Context) (EngineSnapshot, error) {
	h.calls++
	if h.err != nil {
		return EngineSnapshot{}, h.err
	}
	return h.snapshot, nil
}

func TestNewServerFailsClosed(t *testing.T) {
	cert, key := testTLSFiles(t)
	auth := &testAuthenticator{}
	for _, config := range []ServerConfig{
		{Authenticator: auth, TLSCertFile: cert, TLSKeyFile: key},
		{Policy: &PolicyEngine{}, TLSCertFile: cert, TLSKeyFile: key},
		{Policy: &PolicyEngine{}, Authenticator: auth},
		{Policy: &PolicyEngine{}, Authenticator: auth, Importer: &serverImporter{}, TLSCertFile: cert, TLSKeyFile: key},
	} {
		if _, err := NewServer(config); err == nil {
			t.Fatalf("NewServer(%+v) unexpectedly succeeded", config)
		}
	}
}

func TestBearerAcceptedForSemanticRoute(t *testing.T) {
	s, auth := serverForTest(t)
	req := httptest.NewRequest("POST", PathPublish, strings.NewReader(`{"expectedWorkOID":"","proposedOID":"next"}`))
	req.Header.Set("Authorization", "Bearer valid-token")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if auth.calls != 1 {
		t.Fatalf("Authenticate calls = %d, want 1", auth.calls)
	}
	// The request reaches the policy engine and fails its world preconditions, not auth.
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want policy denial", w.Code)
	}
}

func testTLSFiles(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "broker.test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{"broker.test"}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	b, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: b}), 0600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// TestServerCreatePRRefusalCarriesStructuredViolations: a criterion-evidence
// refusal must reach the caller as the precise, machine-readable violation
// set, not the bare denial every other rejection receives, and a body that
// failed linkage still gets the bare denial (only the criteria contract is
// model-repairable at this boundary).
func TestServerCreatePRRefusalCarriesStructuredViolations(t *testing.T) {
	set := pinMisoGallerySet(t)
	p := galleryResolvePolicy(set)
	p.RunUID = "run-uid"
	observer := galleryResolveObserver()
	engine, err := NewPolicyEngine(p, observer, serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	cert, key := testTLSFiles(t)
	s, err := NewServer(ServerConfig{Policy: engine, Authenticator: &testAuthenticator{}, TLSCertFile: cert, TLSKeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	post := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", PathCreatePR, strings.NewReader(`{"oid":"old","title":"t","body":`+mustJSON(t, body)+`,"draft":false}`))
		req.Header.Set("Authorization", "Bearer valid-token")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		return w
	}
	w := post("Closes #502.\n\nShips the assets with no criterion-evidence block.")
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d body %s", w.Code, w.Body.String())
	}
	var violation criteriaViolation
	if err := json.Unmarshal(w.Body.Bytes(), &violation); err != nil {
		t.Fatalf("refusal body is not the structured violation: %v: %s", err, w.Body.String())
	}
	if !violation.BlockMissing || violation.Reason == "" {
		t.Fatalf("violation = %+v, want the missing-block refusal with its reason", violation)
	}
	if observer.created.Body != "" {
		t.Fatal("forge write reached despite criteria refusal")
	}
	// The bare denial remains the shape for every non-criteria rejection.
	if w := post("no closing reference at all"); w.Code != http.StatusUnprocessableEntity || strings.TrimSpace(w.Body.String()) != "operation denied" {
		t.Fatalf("non-criteria denial = %d %q, want the bare 422", w.Code, w.Body.String())
	}
}

// TestServerCreatePRSucceedsSurfacesCoverage: a validated honest partial
// create answers normally and carries the derived coverage on the response.
func TestServerCreatePRSucceedsSurfacesCoverage(t *testing.T) {
	set := pinMisoGallerySet(t)
	p := galleryResolvePolicy(set)
	p.RunUID = "run-uid"
	engine, err := NewPolicyEngine(p, galleryResolveObserver(), serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	cert, key := testTLSFiles(t)
	s, err := NewServer(ServerConfig{Policy: engine, Authenticator: &testAuthenticator{}, TLSCertFile: cert, TLSKeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	body := loadFixture(t, "miso-gallery-505-pr-honest-partial.md")
	req := httptest.NewRequest("POST", PathCreatePR, strings.NewReader(`{"oid":"old","title":"t","body":`+mustJSON(t, body)+`,"draft":false}`))
	req.Header.Set("Authorization", "Bearer valid-token")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("honest partial create denied: %d %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Courier-Criteria-Coverage"); got != CoveragePartial {
		t.Fatalf("coverage header = %q, want partial", got)
	}
}

func mustJSON(t *testing.T, value string) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestServeTLSBoundsStalledClient: the model-facing API server must bound
// inbound reads, so a client that completes the TLS handshake and stalls
// mid-headers is dropped by the header-read bound instead of pinning the
// connection and its goroutine indefinitely.
func TestServeTLSBoundsStalledClient(t *testing.T) {
	s, _ := serverForTest(t)
	cert, key := testTLSFiles(t)
	srv := s.newHTTPServer("127.0.0.1:0")
	for name, production := range map[string]time.Duration{
		"ReadHeaderTimeout": srv.ReadHeaderTimeout,
		"ReadTimeout":       srv.ReadTimeout,
		"WriteTimeout":      srv.WriteTimeout,
		"IdleTimeout":       srv.IdleTimeout,
	} {
		if production <= 0 {
			t.Fatalf("%s = %v, want a finite inbound bound", name, production)
		}
	}
	// Compress the header bound so the regression does not wait a production window.
	bound := 500 * time.Millisecond
	srv.ReadHeaderTimeout = bound
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ServeTLS(ln, cert, key) }()
	t.Cleanup(func() {
		_ = srv.Close()
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("ServeTLS() = %v", err)
		}
	})

	certPEM, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("test certificate is not PEM")
	}

	// The bounds do not cut off a valid request: a real TLS round trip still
	// reaches authentication.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "broker.test"}}}
	resp, err := client.Post("https://"+ln.Addr().String()+PathPublish, "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("valid TLS request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("valid TLS request status = %d, want the unauthenticated denial", resp.StatusCode)
	}

	// A client that completes the handshake and never finishes its request
	// headers is closed by the bound, not waited on forever.
	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "broker.test"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("POST " + PathPublish + " HTTP/1.1\r\nHost: broker.test\r\n")); err != nil {
		t.Fatal(err)
	}
	// The generous read deadline is the failure margin: against an unbounded
	// server the connection is never closed, so the test must not hang on the
	// bug it pins.
	if err := conn.SetReadDeadline(time.Now().Add(bound + 10*time.Second)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	buf := make([]byte, 1)
	_, readErr := conn.Read(buf)
	elapsed := time.Since(start)
	if readErr == nil {
		t.Fatal("stalled incomplete request received a response")
	}
	var ne net.Error
	if errors.As(readErr, &ne) && ne.Timeout() {
		t.Fatalf("stalled request was never dropped: read hit the test deadline after %v", elapsed)
	}
	if elapsed < bound/2 {
		t.Fatalf("connection closed after %v, before a %v header-read bound could fire", elapsed, bound)
	}
	if elapsed > bound+5*time.Second {
		t.Fatalf("connection closed after %v, far beyond the %v header-read bound", elapsed, bound)
	}
}
