package broker

import (
	"bufio"
	"bytes"
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
	"fmt"
	"io"
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
	err    error
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
	if a.err != nil {
		return Identity{}, a.err
	}
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

// TestServerIdentityFailureSplit pins the availability split in the
// model-facing API: an identity backend that failed to answer is transient
// (503), so the caller retries, while a definite refusal stays 401.
func TestServerIdentityFailureSplit(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "transient backend failure", err: &IdentityUnavailableError{Err: errors.New("review control token: api server unavailable")}, want: http.StatusServiceUnavailable},
		{name: "definite refusal", err: errors.New("token is unauthenticated or lacks broker audience"), want: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, auth := serverForTest(t)
			auth.err = tt.err
			req := httptest.NewRequest(http.MethodPost, PathPublish, strings.NewReader(`{"proposedOID":"new"}`))
			req.Header.Set("Authorization", "Bearer valid-token")
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, req)
			if w.Code != tt.want {
				t.Fatalf("status = %d, want %d; body = %q", w.Code, tt.want, w.Body.String())
			}
			if auth.calls != 1 {
				t.Fatalf("authentication calls = %d, want one live auth", auth.calls)
			}
		})
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

// TestServerUsesSharedInboundBounds: the model-facing API listener must
// apply the broker's shared inbound bounds, so it and the trusted status
// listener are provably bounded the same way and cannot drift apart.
func TestServerUsesSharedInboundBounds(t *testing.T) {
	s, _ := serverForTest(t)
	srv := s.newHTTPServer("127.0.0.1:0")
	if srv.ReadHeaderTimeout != InboundReadHeaderTimeout ||
		srv.ReadTimeout != InboundReadTimeout ||
		srv.WriteTimeout != InboundWriteTimeout ||
		srv.IdleTimeout != InboundIdleTimeout {
		t.Fatalf("API listener bounds = %+v, want the shared inbound bounds %+v", srv, []time.Duration{
			InboundReadHeaderTimeout, InboundReadTimeout, InboundWriteTimeout, InboundIdleTimeout,
		})
	}
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

// TestAPIBodyReadBudgetFitsInsideWriteDeadline pins the arithmetic that keeps
// a bounded body read answerable: the budget plus the response reserve is the
// write bound exactly, so a read that ends inside its budget leaves the whole
// reserve to write the answer, and the read bound stays longer than the write
// bound — the shape that makes the handler-level bound necessary.
func TestAPIBodyReadBudgetFitsInsideWriteDeadline(t *testing.T) {
	if apiBodyReadBudget <= 0 || apiResponseReserve <= 0 {
		t.Fatalf("budget %v and reserve %v must both be finite", apiBodyReadBudget, apiResponseReserve)
	}
	if apiBodyReadBudget+apiResponseReserve != InboundWriteTimeout {
		t.Fatalf("body-read budget %v + response reserve %v != write bound %v; an answer written after the budget could outrun the write deadline", apiBodyReadBudget, apiResponseReserve, InboundWriteTimeout)
	}
	if InboundReadTimeout <= InboundWriteTimeout {
		t.Fatal("the read bound no longer exceeds the write bound; recheck why body reads need their own handler-level bound")
	}
	s, _ := serverForTest(t)
	if s.bodyReadBudget != apiBodyReadBudget {
		t.Fatalf("server body-read budget = %v, want the production %v", s.bodyReadBudget, apiBodyReadBudget)
	}
}

// TestAPIStalledBodyCannotOutliveWriteDeadline: a client that sends its
// headers and then stalls its body must not hold the model-facing API past
// its write deadline. Without a handler-level bound the body read waits for
// the longer read bound, the answer is written into an expired write
// deadline, and the caller sees a closed connection instead of the retryable
// answer. The body read instead takes the compressed budget, and the whole
// 408 arrives while the write deadline is still ahead.
func TestAPIStalledBodyCannotOutliveWriteDeadline(t *testing.T) {
	s, _ := serverForTest(t)
	cert, key := testTLSFiles(t)
	// Compress the bounds so the regression does not wait a production window.
	s.bodyReadBudget = 200 * time.Millisecond
	srv := s.newHTTPServer("127.0.0.1:0")
	srv.WriteTimeout = s.bodyReadBudget + 100*time.Millisecond
	// The production bug shape: the read bound outlives the write bound, so
	// only the handler-level bound can keep the answer inside the deadline.
	if srv.ReadTimeout <= srv.WriteTimeout {
		t.Fatalf("test needs a read bound longer than the write bound, got %v <= %v", srv.ReadTimeout, srv.WriteTimeout)
	}
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

	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "broker.test"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The headers arrive; the body never does.
	if _, err := conn.Write([]byte("POST " + PathImportBundle + " HTTP/1.1\r\nHost: broker.test\r\nAuthorization: Bearer valid-token\r\nX-Courier-Proposed-OID: " + strings.Repeat("b", 40) + "\r\nContent-Length: 64\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	// The generous read deadline is the failure margin: against the bug the
	// answer is truncated and the connection hangs until this deadline, so
	// the test must not wait forever on the defect it pins.
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "https://broker.test"+PathImportBundle, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), request)
	if err != nil {
		t.Fatalf("stalled body did not receive a whole answer: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if response.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408; a body that could not be read in time is an incomplete request, not a denial", response.StatusCode)
	}
	if string(payload) != "request timed out\n" {
		t.Fatalf("body = %q, want the whole retryable answer", payload)
	}
}

// stalledBodyReader sends a prefix of the declared body and then blocks until
// the test ends, standing in for a client that stalls its body mid-upload.
type stalledBodyReader struct {
	prefix string
	done   <-chan struct{}
}

func (r *stalledBodyReader) Read(p []byte) (int, error) {
	if r.prefix != "" {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	<-r.done
	return 0, errors.New("test ended")
}

// TestAPIStalledBodyCannotOutliveWriteDeadlineHTTP2: the same stalled-body
// guarantee over HTTP/2, where the bound is a per-stream read timer rather
// than the connection deadline. Today's harness transport speaks HTTP/1.1,
// but the listener negotiates HTTP/2, so the guarantee must hold on both
// inbound stacks.
func TestAPIStalledBodyCannotOutliveWriteDeadlineHTTP2(t *testing.T) {
	s, _ := serverForTest(t)
	cert, key := testTLSFiles(t)
	s.bodyReadBudget = 200 * time.Millisecond
	srv := s.newHTTPServer("127.0.0.1:0")
	srv.WriteTimeout = s.bodyReadBudget + 100*time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ServeTLS(ln, cert, key) }()
	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
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
	transport := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "broker.test"}}
	client := &http.Client{Transport: transport}
	t.Cleanup(client.CloseIdleConnections)
	// The request context is what releases the client's own stalled body
	// writer once the answer has been read; the server's answer itself must
	// arrive long before it, which the elapsed check below pins.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+ln.Addr().String()+PathImportBundle, &stalledBodyReader{prefix: "pack", done: done})
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = 64
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("X-Courier-Proposed-OID", strings.Repeat("b", 40))
	started := time.Now()
	response, err := client.Do(req)
	if err != nil {
		t.Fatalf("stalled body over HTTP/2 did not receive a whole answer: %v", err)
	}
	defer response.Body.Close()
	if response.Proto != "HTTP/2.0" {
		t.Fatalf("protocol = %s, want HTTP/2.0 for the second inbound stack", response.Proto)
	}
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= srv.WriteTimeout {
		t.Fatalf("answer took %v, want it inside the %v write deadline it must not outrun", elapsed, srv.WriteTimeout)
	}
	if response.StatusCode != http.StatusRequestTimeout || string(payload) != "request timed out\n" {
		t.Fatalf("status = %d body = %q, want the whole retryable 408 answer", response.StatusCode, payload)
	}
	// The answer is fully read. Release the client's own stalled body
	// writer before the deferred Close waits for stream cleanup.
	cancel()
}

// TestDecodeJSONDeadlineIsRetryableNotDenial pins the classification at the
// shared body-read choke point: a read cut short by the request deadline
// answers the retryable 408 the harness retry machinery reruns, while a body
// that failed on its merits keeps the definite 400 denial.
func TestDecodeJSONDeadlineIsRetryableNotDenial(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"read deadline", os.ErrDeadlineExceeded, http.StatusRequestTimeout},
		{"wrapped read deadline", fmt.Errorf("read bundle body: %w", os.ErrDeadlineExceeded), http.StatusRequestTimeout},
		{"connection reset", errors.New("connection reset by peer"), http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, PathPublish, errorReader{test.err})
			req.ContentLength = 64
			w := httptest.NewRecorder()
			var target struct{}
			if decodeJSON(w, req, &target) {
				t.Fatal("decode succeeded on a failing body")
			}
			if w.Code != test.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, test.want, w.Body.String())
			}
		})
	}
}

// TestDecodeJSONTrailingReadClassification pins the second Decode: a second
// JSON value after the first is the malformed body's definite 400, while a
// read that dies at the request deadline after an otherwise complete object
// stays the retryable 408.
func TestDecodeJSONTrailingReadClassification(t *testing.T) {
	for _, test := range []struct {
		name string
		body io.Reader
		want int
	}{
		{"trailing value", strings.NewReader(`{"expectedWorkOID":""} {"extra":true}`), http.StatusBadRequest},
		{"deadline after complete object", io.MultiReader(strings.NewReader(`{"expectedWorkOID":""}`), errorReader{os.ErrDeadlineExceeded}), http.StatusRequestTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, PathPublish, test.body)
			req.ContentLength = -1
			w := httptest.NewRecorder()
			var target struct {
				ExpectedWorkOID string `json:"expectedWorkOID"`
			}
			if decodeJSON(w, req, &target) {
				t.Fatal("decode accepted the body")
			}
			if w.Code != test.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, test.want, w.Body.String())
			}
		})
	}
}

// TestServerRejectsOversizedChunkedBundle: the 65 MiB transport bound stays a
// definite 400 when the size only reveals itself mid-read — a chunked body
// whose overflow error must not be mistaken for a request deadline.
func TestServerRejectsOversizedChunkedBundle(t *testing.T) {
	s, _ := serverForTest(t)
	importer := s.importer.(*serverImporter)
	req := httptest.NewRequest(http.MethodPost, PathImportBundle, io.MultiReader(
		bytes.NewReader(make([]byte, maxBundleRequestBytes)),
		strings.NewReader("one byte past the bundle bound"),
	))
	req.ContentLength = -1 // chunked: the limit is only provable mid-read
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("X-Courier-Proposed-OID", strings.Repeat("b", 40))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || importer.calls != 0 {
		t.Fatalf("status = %d importer calls = %d, want the definite 400 with no import", w.Code, importer.calls)
	}
}

// errorReader is a request body that always fails, standing in for a
// connection whose read deadline has just fired.
type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

// TestAPIHealthyBundleUploadCompletesInsideWriteDeadline: bounding the body
// read must not cut off a healthy upload. The production budget sits far
// above what a healthy bundle upload costs in-cluster; this full round trip
// over the production-bounded listener proves an import at the full 65 MiB
// bundle bound still reaches the importer and answers in full.
func TestAPIHealthyBundleUploadCompletesInsideWriteDeadline(t *testing.T) {
	s, _ := serverForTest(t)
	importer := s.importer.(*serverImporter)
	cert, key := testTLSFiles(t)
	srv := s.newHTTPServer("127.0.0.1:0")
	if srv.ReadTimeout <= srv.WriteTimeout {
		t.Fatalf("listener read bound %v must exceed the write bound %v, as in production", srv.ReadTimeout, srv.WriteTimeout)
	}
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
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "broker.test"}}}
	// The full bundle bound itself, streamed: a healthy import at exactly
	// maxBundleRequestBytes must pass the transport bound and import whole.
	chunk := make([]byte, 1<<20)
	var readers []io.Reader
	for range maxBundleRequestBytes / (1 << 20) {
		readers = append(readers, bytes.NewReader(chunk))
	}
	req, err := http.NewRequest(http.MethodPost, "https://"+ln.Addr().String()+PathImportBundle, io.MultiReader(readers...))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = maxBundleRequestBytes
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("X-Courier-Proposed-OID", strings.Repeat("b", 40))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("healthy bundle upload: %v", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read answer: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(payload) != "{\"imported\":true}\n" || importer.calls != 1 {
		t.Fatalf("status = %d body = %q importer calls = %d, want the whole import answer", resp.StatusCode, payload, importer.calls)
	}
}
