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
)

type testAuthenticator struct {
	calls int
	fail  bool
}
type serverObserver struct{}

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

func (serverPusher) IsAncestor(context.Context, string, string, string) (bool, error) {
	return false, nil
}
func (serverPusher) Push(context.Context, string, string, string) error { return nil }
func (a *testAuthenticator) Authenticate(_ context.Context, token string) (Identity, error) {
	a.calls++
	if a.fail || token != "valid-token" {
		return Identity{}, context.Canceled
	}
	return Identity{RunUID: "run-uid"}, nil
}

func serverForTest(t *testing.T) (*Server, *testAuthenticator) {
	t.Helper()
	cert, key := testTLSFiles(t)
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base", WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true}, serverObserver{}, serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	auth := &testAuthenticator{}
	s, err := NewServer(ServerConfig{Policy: engine, Authenticator: auth, TLSCertFile: cert, TLSKeyFile: key})
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

func TestNewServerFailsClosed(t *testing.T) {
	cert, key := testTLSFiles(t)
	auth := &testAuthenticator{}
	for _, config := range []ServerConfig{
		{Authenticator: auth, TLSCertFile: cert, TLSKeyFile: key},
		{Policy: &PolicyEngine{}, TLSCertFile: cert, TLSKeyFile: key},
		{Policy: &PolicyEngine{}, Authenticator: auth},
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
