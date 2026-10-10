package broker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/forge"
)

type staticCapabilities struct {
	report CapabilityReport
}

func (s staticCapabilities) CapabilityReport() CapabilityReport { return s.report }

// The capability report is read-only configuration data behind the same
// authenticated identity chain as every typed route; control uses it to name
// unavailable providers without holding any credential itself.
func TestCapabilitiesEndpoint(t *testing.T) {
	cert, key := testTLSFiles(t)
	engine, err := NewPolicyEngine(Policy{
		RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test",
		BaseRepo: "org/repo", BaseRef: "main", BaseOID: strings.Repeat("a", 40),
		WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true,
		SourceIssue: SourceIssue{Owner: "org", Name: "repo", Number: 1},
	}, importTestObserver{}, serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	auth := &testAuthenticator{}
	report := CapabilityReport{
		Provider: ProviderReport{Name: "test", Type: "github", Endpoint: "https://api.github.com/"},
		Operations: []OperationReport{
			{Name: string(forge.CapabilityReadWorkItem), Available: true},
			{Name: string(forge.CapabilityCreatePullRequest), Available: true},
			{Name: string(forge.CapabilityUpdatePullRequest), Available: false, Detail: "credential rejected"},
		},
		Git: GitReport{Endpoint: "https://github.com/%s.git", Ready: true},
	}
	s, err := NewServer(ServerConfig{
		Policy:        engine,
		Authenticator: auth,
		ScratchDir:    filepath.Join(t.TempDir(), "unused"),
		TLSCertFile:   cert,
		TLSKeyFile:    key,
		Capabilities:  staticCapabilities{report: report},
	})
	if err != nil {
		t.Fatal(err)
	}

	get := func(authorization string) (*httptest.ResponseRecorder, CapabilityReport) {
		req := httptest.NewRequest("GET", PathCapabilities, nil)
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		var decoded CapabilityReport
		if w.Code == http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
				t.Fatalf("decode report: %v", err)
			}
		}
		return w, decoded
	}

	w, decoded := get("Bearer valid-token")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if decoded.Provider.Name != "test" || decoded.Provider.Type != "github" {
		t.Fatalf("provider report = %+v", decoded.Provider)
	}
	if !decoded.Git.Ready || decoded.Git.Endpoint == "" {
		t.Fatalf("git report = %+v", decoded.Git)
	}
	if len(decoded.Operations) != 3 {
		t.Fatalf("operations = %+v", decoded.Operations)
	}
	for _, op := range decoded.Operations {
		if op.Name == string(forge.CapabilityUpdatePullRequest) && (op.Available || op.Detail != "credential rejected") {
			t.Fatalf("unavailable operation diagnostic = %+v", op)
		}
	}
	if auth.calls != 1 {
		t.Fatalf("authenticate calls = %d, want one per request", auth.calls)
	}

	if w, _ := get(""); w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", w.Code)
	}
	if w, _ := get("Bearer wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("bad token status = %d, want 401", w.Code)
	}

	// Wrong run identity is refused like every other typed route.
	auth.runUID = "other-run"
	if w, _ := get("Bearer valid-token"); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong run identity status = %d, want 401", w.Code)
	}
}

func TestCapabilitiesEndpointAbsentWithoutReporter(t *testing.T) {
	s, _ := serverForTest(t)
	req := httptest.NewRequest("GET", PathCapabilities, nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 without a capability reporter", w.Code)
	}
}
