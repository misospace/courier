package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
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
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/repo", BaseRef: "main", BaseOID: strings.Repeat("a", 40), WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true}, importTestObserver{}, serverPusher{})
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
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/repo", BaseRef: "main", BaseOID: strings.Repeat("a", 40), WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true}, importTestObserver{}, serverPusher{})
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
