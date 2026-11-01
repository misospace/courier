package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/topology"
)

func TestRunFailsClosedWithoutOperatorWiring(t *testing.T) {
	if err := runArgs(nil); err == nil || !strings.Contains(err.Error(), "requires address") {
		t.Fatalf("run() error = %v, want missing configuration", err)
	}
}

func TestVerifyProjectionRejectsMismatch(t *testing.T) {
	registration := &forge.Registration{
		Name:     "github",
		Type:     "github",
		Endpoint: "https://api.github.com/",
		Credentials: map[string]forge.CredentialRef{
			forge.PurposeForgeAPI: {SecretName: "src", Key: "token"},
		},
		Serves: []string{"*"},
	}
	projection := registration.Projection()
	document := &topology.BrokerPolicyDocument{
		PublicationPolicy: &courierv1alpha1.PublicationPolicy{
			ProviderConfigRef:   projection.Name,
			ProviderEndpoint:    projection.Endpoint,
			CredentialRefDigest: projection.Digest,
		},
		Provider: projection,
	}
	env := &brokerEnv{
		namespace: "runs", podName: "r-broker",
		controlSA: "r-control", controlSAUID: "sa-uid",
		providerName: "github", providerType: "github",
		providerEndpt: projection.Endpoint, providerGitTpl: projection.GitTemplate,
		forgeAPIToken: "tok", gitToken: "tok", apiTokenFile: "/tmp/token",
	}
	if err := verifyProjection(env, document); err != nil {
		t.Fatalf("consistent projection must verify: %v", err)
	}

	tampered := env
	tampered.providerEndpt = "https://evil.example.test/"
	if err := verifyProjection(tampered, document); err == nil {
		t.Fatal("a swapped endpoint must refuse service")
	}
	tampered = env
	tampered.providerName = "other"
	if err := verifyProjection(tampered, document); err == nil {
		t.Fatal("a swapped provider name must refuse service")
	}

	// A digest that does not recompute from the projection is rejected.
	badDocument := *document
	badDocument.PublicationPolicy.CredentialRefDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := verifyProjection(env, &badDocument); err == nil {
		t.Fatal("digest mismatch must refuse service")
	}
}

func TestLoadPolicyDocumentIsStrict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	valid := `{"publicationPolicy":{"runUID":"u","providerConfigRef":"github","baseRepo":"a/b","baseRef":"main","baseOID":"x","workRepo":"a/b","workRef":"w"},"provider":{"name":"github","type":"github","endpoint":"https://api.github.com/","gitEndpointTemplate":"https://github.com/%s.git","gitUsername":"","credentials":{"forge-api":{"secretName":"s","key":"k"}},"digest":"d"}}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	document, err := loadPolicyDocument(path)
	if err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if document.Provider.Name != "github" {
		t.Fatalf("provider = %+v", document.Provider)
	}
	if err := os.WriteFile(path, []byte(`{"publicationPolicy":{},"provider":{},"extra":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPolicyDocument(path); err == nil {
		t.Fatal("unknown fields must be rejected")
	}
}

func TestPinnedResolverOnlyServesPolicyRepositories(t *testing.T) {
	resolver := newPinnedResolver(broker.Policy{BaseRepo: "a/b", WorkRepo: "c/d"},
		forge.Projection{GitTemplate: "https://github.com/%s.git"})
	if endpoint, err := resolver.Endpoint(context.Background(), "a/b"); err != nil || endpoint != "https://github.com/a/b.git" {
		t.Fatalf("base endpoint = %q, %v", endpoint, err)
	}
	if _, err := resolver.Endpoint(context.Background(), "other/repo"); err == nil {
		t.Fatal("a repository outside the policy must have no endpoint")
	}
}

func TestNewProviderUsesRegisteredEndpoint(t *testing.T) {
	var gotPath, gotHost, gotAuth string
	var gotRequests int
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequests++
		gotPath, gotHost, gotAuth = r.URL.Path, r.Host, r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":7,"full_name":"acme/widgets","default_branch":"main"}`)
	}))
	defer fixture.Close()

	tests := []struct {
		name     string
		endpoint string
		wantPath string
	}{
		{name: "enterprise API base with path prefix", endpoint: fixture.URL + "/api/v3", wantPath: "/api/v3/repos/acme/widgets"},
		{name: "host-only API base", endpoint: fixture.URL, wantPath: "/repos/acme/widgets"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := &brokerEnv{
				providerName:  "primary",
				providerType:  "github",
				providerEndpt: test.endpoint,
				forgeAPIToken: "broker-resolved-token",
			}
			document := &topology.BrokerPolicyDocument{
				Provider: forge.Projection{Name: "primary", Type: "github", Endpoint: test.endpoint},
			}
			provider, err := newProvider(env, document)
			if err != nil {
				t.Fatalf("newProvider: %v", err)
			}
			if got := provider.Config().Endpoint; got != test.endpoint {
				t.Fatalf("provider endpoint = %q, want %q", got, test.endpoint)
			}
			gotRequests = 0
			repo, err := provider.ResolveRepository(context.Background(), "acme/widgets")
			if err != nil || repo.Canonical != "acme/widgets" {
				t.Fatalf("resolve repository = %#v, %v", repo, err)
			}
			if gotRequests != 1 {
				t.Fatalf("fixture saw %d requests, want exactly one per read", gotRequests)
			}
			if gotHost != fixture.Listener.Addr().String() {
				t.Fatalf("request host = %q, want the registered fixture host %q", gotHost, fixture.Listener.Addr().String())
			}
			if gotPath != test.wantPath {
				t.Fatalf("request path = %q, want %q", gotPath, test.wantPath)
			}
			if gotAuth != "Bearer broker-resolved-token" {
				t.Fatalf("authorization = %q, want the broker-resolved credential", gotAuth)
			}
		})
	}

	env := &brokerEnv{providerName: "primary", providerType: "github", providerEndpt: "not a url", forgeAPIToken: "tok"}
	document := &topology.BrokerPolicyDocument{Provider: forge.Projection{Name: "primary", Endpoint: "not a url"}}
	if _, err := newProvider(env, document); err == nil {
		t.Fatal("a malformed registered endpoint must fail closed, not fall back to the public API")
	}
}

func TestPolicyMatchesDetectsDrift(t *testing.T) {
	persisted := &courierv1alpha1.PublicationPolicy{
		BaseRepo: "a/b", BaseRef: "main", BaseOID: "x",
		WorkRepo: "a/b", WorkRef: "w", WorkInitiallyAbsent: true,
	}
	resolved := broker.Policy{
		BaseRepo: "a/b", BaseRef: "main", BaseOID: "x",
		WorkRepo: "a/b", WorkRef: "w", WorkInitiallyAbsent: true,
		Mode: broker.ModeResolveIssue, Provider: "github", RunUID: "u",
	}
	if err := policyMatches(resolved, persisted); err != nil {
		t.Fatalf("equal policies must match: %v", err)
	}
	resolved.WorkRef = "moved"
	if err := policyMatches(resolved, persisted); err == nil {
		t.Fatal("drifted work ref must be refused")
	}
}

// TestTrustedStatusListenerCarriesInboundBounds: the trusted status listener
// must apply the broker's shared inbound HTTP bounds. Without them a client
// that completes the header read and then stalls, or that leaves an
// established connection idle, pins its connection, goroutine, and file
// descriptor for the broker pod's entire lifetime.
func TestTrustedStatusListenerCarriesInboundBounds(t *testing.T) {
	srv := newStatusServer("127.0.0.1:0", http.NotFoundHandler())
	got := map[string]time.Duration{
		"ReadHeaderTimeout": srv.ReadHeaderTimeout,
		"ReadTimeout":       srv.ReadTimeout,
		"WriteTimeout":      srv.WriteTimeout,
		"IdleTimeout":       srv.IdleTimeout,
	}
	want := map[string]time.Duration{
		"ReadHeaderTimeout": broker.InboundReadHeaderTimeout,
		"ReadTimeout":       broker.InboundReadTimeout,
		"WriteTimeout":      broker.InboundWriteTimeout,
		"IdleTimeout":       broker.InboundIdleTimeout,
	}
	for name, bound := range got {
		if bound <= 0 {
			t.Fatalf("status listener %s = %v, want a finite inbound bound", name, bound)
		}
		if bound != want[name] {
			t.Fatalf("status listener %s = %v, want the API listener's %v", name, bound, want[name])
		}
	}
}

// TestStatusListenerReclaimsIdleConnection: the trusted status listener must
// close an idle connection instead of holding its file descriptor and
// goroutine for the broker pod's lifetime. The inbound bounds are
// transport-level, so the plain listener below exercises the same server
// configuration production serves over TLS.
func TestStatusListenerReclaimsIdleConnection(t *testing.T) {
	srv := newStatusServer("127.0.0.1:0", http.NotFoundHandler())
	// The production wiring must carry the bound this test then compresses:
	// without it the listener would hold every idle connection open.
	if srv.IdleTimeout != broker.InboundIdleTimeout {
		t.Fatalf("status listener IdleTimeout = %v, want the API listener's %v", srv.IdleTimeout, broker.InboundIdleTimeout)
	}
	// Compress the idle bound so the regression does not wait a production window.
	bound := 500 * time.Millisecond
	srv.IdleTimeout = bound
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve() = %v", err)
		}
	})

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	request, err := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("GET /status HTTP/1.1\r\nHost: broker.test\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, request)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	resp.Body.Close()

	// The connection is now idle: the server must close it at the idle bound
	// rather than hold it open. The read deadline is a test guard, not
	// evidence of closure: hitting it fails the test, because it means the
	// server left the connection open past the deadline.
	if err := conn.SetReadDeadline(time.Now().Add(bound + 10*time.Second)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("idle connection was not closed by the idle bound")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("idle connection was never closed: read hit the test deadline after %v", time.Since(start))
	}
	// The close must be the idle bound firing, not an unrelated early close.
	if elapsed := time.Since(start); elapsed < bound/2 {
		t.Fatalf("connection closed after %v, before the %v idle bound could fire", elapsed, bound)
	}
}
