package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	couriergit "github.com/misospace/courier/internal/git"
)

const (
	PathPublish       = "/v1/publication"
	PathImportBundle  = "/v1/bundles/import"
	PathSnapshot      = "/v1/git/snapshot"
	PathCreatePR      = "/v1/pull-requests"
	PathUpdatePR      = "/v1/pull-requests/update"
	PathTrustedStatus = "/trusted/v1/status"
)

// RequestAuthenticator verifies a bearer token for each request. Implementations
// are expected to perform a live TokenReview and incarnation check, not cache success.
type RequestAuthenticator interface {
	Authenticate(context.Context, string) (Identity, error)
}

// ServerConfig must be backed by the operator-resolved immutable run policy.
// The trusted status endpoint is intentionally not served by this model-facing API;
// #123 must provide a separate caller boundary before status writes are enabled.
type BundleImporter interface {
	ImportBundle(ctx context.Context, bundlePath, proposedOID, expectedTip string) error
}

// SnapshotHandler serves the seed-snapshot read for trusted control.
type SnapshotHandler interface {
	SnapshotForControl(ctx context.Context) (EngineSnapshot, error)
}

type ServerConfig struct {
	Policy        *PolicyEngine
	Authenticator RequestAuthenticator
	Importer      BundleImporter
	Snapshot      SnapshotHandler
	ScratchDir    string
	TLSCertFile   string
	TLSKeyFile    string
	// Capabilities optionally serves the read-only capability report. It is
	// captured at construction and never changes per request.
	Capabilities CapabilityReporter
}

type Server struct {
	policy        *PolicyEngine
	authenticator RequestAuthenticator
	importer      BundleImporter
	snapshot      SnapshotHandler
	scratchDir    string
	capabilities  CapabilityReporter
	importMu      sync.Mutex
	handler       http.Handler
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.Policy == nil || config.Authenticator == nil {
		return nil, errors.New("broker startup requires run-bound policy and identity backend")
	}
	if config.Importer != nil {
		if !filepath.IsAbs(config.ScratchDir) || strings.TrimSpace(config.ScratchDir) != config.ScratchDir {
			return nil, errors.New("broker startup requires an absolute private scratch directory for bundle import")
		}
		if err := os.MkdirAll(config.ScratchDir, 0o700); err != nil {
			return nil, errors.New("broker startup cannot create private scratch directory")
		}
		info, err := os.Lstat(config.ScratchDir)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("broker startup requires a non-symlink scratch directory private to the broker")
		}
		if err := os.Chmod(config.ScratchDir, 0o700); err != nil {
			return nil, errors.New("broker startup cannot secure private scratch directory")
		}
	}
	if config.TLSCertFile == "" || config.TLSKeyFile == "" {
		return nil, errors.New("broker startup requires TLS certificate and key")
	}
	if _, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile); err != nil {
		return nil, fmt.Errorf("load broker TLS identity: %w", err)
	}
	s := &Server{policy: config.Policy, authenticator: config.Authenticator, importer: config.Importer, snapshot: config.Snapshot, scratchDir: config.ScratchDir, capabilities: config.Capabilities}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+PathPublish, s.publish)
	if s.importer != nil {
		mux.HandleFunc("POST "+PathImportBundle, s.importBundle)
	}
	if s.snapshot != nil {
		mux.HandleFunc("GET "+PathSnapshot, s.snapshotForControl)
	}
	if s.capabilities != nil {
		mux.HandleFunc("GET "+PathCapabilities, s.capabilityHandler)
	}
	mux.HandleFunc("POST "+PathCreatePR, s.createPR)
	mux.HandleFunc("PATCH "+PathUpdatePR, s.updatePR)
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ServeMux normalizes dot segments and repeated slashes with redirects. Reject
		// those spellings instead of allowing a non-canonical request to be replayed.
		decoded, err := url.PathUnescape(r.URL.EscapedPath())
		if err != nil || decoded != r.URL.Path || r.URL.RawPath != "" || strings.Contains(r.URL.Path, "//") || strings.Contains(r.URL.Path, "\\") || strings.Contains(r.URL.Path, "/../") || strings.HasSuffix(r.URL.Path, "/..") || strings.Contains(r.URL.Path, "/./") || strings.HasSuffix(r.URL.Path, "/.") {
			http.NotFound(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
	return s, nil
}

// Handler exposes the typed HTTP API for embedding and tests; production serves it only over TLS.
func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) ServeTLS(addr string, certFile, keyFile string) error {
	if s == nil || s.handler == nil {
		return errors.New("broker server is not initialized")
	}
	return (&http.Server{Addr: addr, Handler: s.handler, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13}}).ListenAndServeTLS(certFile, keyFile)
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(value, "Bearer ")) == "" || strings.TrimSpace(strings.TrimPrefix(value, "Bearer ")) != strings.TrimPrefix(value, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Identity{}, false
	}
	identity, err := s.authenticator.Authenticate(r.Context(), strings.TrimPrefix(value, "Bearer "))
	if err != nil || identity.RunUID == "" || string(identity.RunUID) != s.policy.policy.RunUID {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Identity{}, false
	}
	return identity, true
}

// Bound request buffering and disk use; this is a transport resource limit, not a work-policy limit.
const maxBundleRequestBytes = 65 << 20

func (s *Server) importBundle(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	info, err := os.Lstat(s.scratchDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		http.Error(w, "bundle import unavailable", http.StatusInternalServerError)
		return
	}
	if r.URL.RawQuery != "" || r.ContentLength == 0 || r.ContentLength > maxBundleRequestBytes {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	// The proposed and expected OIDs travel in headers and the bundle bytes
	// form the raw body: the settled §5 bundle bound must be expressible on
	// the wire, and a text envelope would double it.
	req := struct {
		ExpectedTip string
		ProposedOID string
	}{
		ExpectedTip: r.Header.Get("X-Courier-Expected-Tip"),
		ProposedOID: r.Header.Get("X-Courier-Proposed-OID"),
	}
	bundle, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBundleRequestBytes))
	if err != nil || len(bundle) == 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if !validObjectID(req.ProposedOID) || req.ExpectedTip != "" && !validObjectID(req.ExpectedTip) {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if !s.validImportTip(r.Context(), req.ExpectedTip) {
		http.Error(w, "import denied", http.StatusUnprocessableEntity)
		return
	}
	if err := r.Context().Err(); err != nil {
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}
	s.importMu.Lock()
	defer s.importMu.Unlock()
	if err := r.Context().Err(); err != nil {
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}
	info, err = os.Lstat(s.scratchDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		http.Error(w, "bundle import unavailable", http.StatusInternalServerError)
		return
	}
	file, err := os.CreateTemp(s.scratchDir, "bundle-*")
	if err != nil {
		http.Error(w, "bundle import failed", http.StatusInternalServerError)
		return
	}
	path := file.Name()
	defer os.Remove(path)
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(bundle)
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		http.Error(w, "bundle import failed", http.StatusUnprocessableEntity)
		return
	}
	// Re-observe while serialized with other imports so an admitted tip cannot
	// change between the initial check and importing the worker bundle.
	if !s.validImportTip(r.Context(), req.ExpectedTip) {
		http.Error(w, "import denied", http.StatusUnprocessableEntity)
		return
	}
	if err = s.importer.ImportBundle(r.Context(), path, req.ProposedOID, req.ExpectedTip); err != nil {
		http.Error(w, "bundle import failed", http.StatusUnprocessableEntity)
		return
	}
	if err = r.Context().Err(); err != nil {
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}
	respond(w, map[string]bool{"imported": true}, nil)
}

func (s *Server) validImportTip(ctx context.Context, expected string) bool {
	p := s.policy.policy
	if err := ctx.Err(); err != nil {
		return false
	}
	s.policy.mu.Lock()
	defer s.policy.mu.Unlock()
	base, work, pr, err := s.policy.observe(ctx)
	if err != nil || s.policy.checkBase(base) != nil || s.policy.checkDestination(work) != nil {
		return false
	}
	if err := s.policy.checkPR(pr, work.OID, s.policy.confirmed); err != nil {
		return false
	}
	if expected == "" {
		return p.WorkInitiallyAbsent && !work.Exists
	}
	if !work.Exists || work.OID != expected {
		return false
	}
	return expected == p.WorkAnchorOID || expected == p.HeadAnchorOID || expected == s.policy.confirmed
}

// snapshotForControl serves the seed snapshot: the live observed tips in
// headers and the bundle as the raw body. Only authenticated control reads
// it, and the bundle is broker-authored from the pinned refs it observed.
func (s *Server) snapshotForControl(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	if r.URL.RawQuery != "" || r.Body != nil && r.ContentLength != 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	snap, err := s.snapshot.SnapshotForControl(r.Context())
	if err != nil {
		if errors.Is(err, ErrSnapshotTipMoved) {
			http.Error(w, "snapshot race, retry", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "operation denied", http.StatusUnprocessableEntity)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Courier-Snapshot-Tip", snap.Tip)
	w.Header().Set("X-Courier-Snapshot-Base-Tip", snap.BaseTip)
	w.Header().Set("X-Courier-Snapshot-Work-Ref-Exists", fmt.Sprintf("%t", snap.WorkRefExists))
	w.Header().Set("X-Courier-Snapshot-At-Anchor", fmt.Sprintf("%t", snap.AtAnchor))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(snap.Data); err != nil {
		return
	}
}

func validObjectID(oid string) bool { return couriergit.ValidOID(oid) }

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	var req struct {
		ExpectedWorkOID string `json:"expectedWorkOID"`
		ProposedOID     string `json:"proposedOID"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.policy.Publish(r.Context(), PublicationRequest{RunUID: s.policy.policy.RunUID, ExpectedWorkOID: req.ExpectedWorkOID, ProposedOID: req.ProposedOID})
	respond(w, result, err)
}

func (s *Server) createPR(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	var req struct {
		OID   string `json:"oid"`
		Title string `json:"title"`
		Body  string `json:"body"`
		Draft bool   `json:"draft"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.policy.CreatePullRequest(r.Context(), req.OID, req.Title, req.Body, req.Draft)
	respond(w, result, err)
}

func (s *Server) updatePR(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	var req struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	respond(w, map[string]bool{"updated": true}, s.policy.UpdateFixPR(r.Context(), UpdatePullRequest{Title: req.Title, Body: req.Body}))
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if r.URL.RawQuery != "" || r.ContentLength == 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return false
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return false
	}
	return true
}

func respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		http.Error(w, "operation denied", http.StatusUnprocessableEntity)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}

// LoadTLSIdentity is available to callers that need to validate cert material before serving.
func LoadTLSIdentity(certFile, keyFile string) (tls.Certificate, error) {
	if certFile == "" || keyFile == "" {
		return tls.Certificate{}, errors.New("TLS certificate and key are required")
	}
	if _, err := os.Stat(certFile); err != nil {
		return tls.Certificate{}, err
	}
	return tls.LoadX509KeyPair(certFile, keyFile)
}
