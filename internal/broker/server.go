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
	"time"

	couriergit "github.com/misospace/courier/internal/git"
)

const (
	PathPublish        = "/v1/publication"
	PathImportBundle   = "/v1/bundles/import"
	PathSnapshot       = "/v1/git/snapshot"
	PathCreatePR       = "/v1/pull-requests"
	PathUpdatePR       = "/v1/pull-requests/update"
	PathTrustedStatus  = "/trusted/v1/status"
	PathControlRequest = "/trusted/v1/control-request"
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
	// bodyReadBudget bounds each request body read inside the listener's
	// write deadline. It is apiBodyReadBudget in production; tests compress
	// it so the regressions below do not wait a production window.
	bodyReadBudget time.Duration
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
	s := &Server{policy: config.Policy, authenticator: config.Authenticator, importer: config.Importer, snapshot: config.Snapshot, scratchDir: config.ScratchDir, capabilities: config.Capabilities, bodyReadBudget: apiBodyReadBudget}
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
		// Bound the body read for every route before its handler runs, so a
		// client that sends its headers and then stalls its body cannot hold
		// the request past the listener's write deadline.
		s.boundBodyRead(w)
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

// Inbound HTTP bounds for every TLS listener the broker serves, matching the
// evidence intake's treatment of the same bounded-bundle workload: they
// reclaim stalled and idle connections, and sit far above what a healthy
// 65 MiB import or snapshot read costs in-cluster. They are exported so the
// broker's trusted status listener applies the same budgets as the
// model-facing API listener.
const (
	InboundReadHeaderTimeout = 10 * time.Second
	InboundReadTimeout       = 60 * time.Second
	InboundWriteTimeout      = 30 * time.Second
	InboundIdleTimeout       = 120 * time.Second
	// apiResponseReserve is the share of the write deadline kept for
	// writing the answer once the request body read is done.
	apiResponseReserve = 5 * time.Second
	// apiBodyReadBudget bounds one handler's request body read. The write
	// deadline starts when the request headers arrive and the read bound is
	// longer than it, so a body read admitted up to the read bound could
	// push the answer past an expired write deadline. The budget ends
	// inside the write deadline with the response reserve between, and
	// still sits far above what a healthy 65 MiB upload costs in-cluster.
	apiBodyReadBudget = InboundWriteTimeout - apiResponseReserve
)

func (s *Server) ServeTLS(addr string, certFile, keyFile string) error {
	if s == nil || s.handler == nil {
		return errors.New("broker server is not initialized")
	}
	return s.newHTTPServer(addr).ListenAndServeTLS(certFile, keyFile)
}

func (s *Server) newHTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           s.handler,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS13},
		ReadHeaderTimeout: InboundReadHeaderTimeout,
		ReadTimeout:       InboundReadTimeout,
		WriteTimeout:      InboundWriteTimeout,
		IdleTimeout:       InboundIdleTimeout,
	}
}

// boundBodyRead keeps the request body read inside the listener's write
// deadline. The listener's own read bound is longer than its write bound, so
// a client that sends its headers and then stalls its body would hold the
// request past the write deadline, and the answer would be written into an
// expired deadline. The body read instead takes the handler's budget, which
// ends inside the write deadline. A writer that does not support deadlines —
// an in-process test recorder — keeps the listener's own read bound.
func (s *Server) boundBodyRead(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(s.bodyReadBudget))
}

// requestDeadlineExceeded reports whether a body read ended because the
// request's read deadline expired. Both inbound stacks surface the bound as
// os.ErrDeadlineExceeded: HTTP/1 through the connection deadline and HTTP/2
// through the per-stream read timer.
func requestDeadlineExceeded(err error) bool {
	return errors.Is(err, os.ErrDeadlineExceeded)
}

// denyBodyRead answers a request whose body read failed. A read cut short by
// the request deadline is an incomplete request, not a malformed one: the
// answer stays retryable so the caller's retry machinery reruns it instead of
// treating a stalled transport as a definite denial.
func denyBodyRead(w http.ResponseWriter, err error) {
	if requestDeadlineExceeded(err) {
		http.Error(w, "request timed out", http.StatusRequestTimeout)
		return
	}
	http.Error(w, "invalid request", http.StatusBadRequest)
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(value, "Bearer ")) == "" || strings.TrimSpace(strings.TrimPrefix(value, "Bearer ")) != strings.TrimPrefix(value, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Identity{}, false
	}
	identity, err := s.authenticator.Authenticate(r.Context(), strings.TrimPrefix(value, "Bearer "))
	if err != nil {
		// A backend that failed to answer is availability, not denial: 503
		// lets the caller retry, while a definite refusal stays a 401.
		var unavailable *IdentityUnavailableError
		if errors.As(err, &unavailable) {
			http.Error(w, "identity backend unavailable", http.StatusServiceUnavailable)
			return Identity{}, false
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Identity{}, false
	}
	if identity.RunUID == "" || string(identity.RunUID) != s.policy.policy.RunUID {
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
	if err != nil {
		denyBodyRead(w, err)
		return
	}
	if len(bundle) == 0 {
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
	result, report, err := s.policy.CreatePullRequest(r.Context(), req.OID, req.Title, req.Body, req.Draft)
	if err == nil && report != nil {
		// A validated evidence block surfaces its coverage (claim and
		// derivation agree by construction) so the caller can record the
		// honest partial state without re-parsing its own body.
		w.Header().Set("X-Courier-Criteria-Coverage", report.Coverage)
	}
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
	if err := decodeJSONBody(r, target); err != nil {
		denyBodyRead(w, err)
		return false
	}
	return true
}

// decodeJSONBody reads one JSON body into target. The caller decides how a
// failed read is answered: a malformed body and a body that could not be read
// in time are different answers.
func decodeJSONBody(r *http.Request, target any) error {
	if r.URL.RawQuery != "" || r.ContentLength == 0 {
		return errors.New("invalid request")
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("invalid request")
	}
	return nil
}

func respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		var cerr *CriteriaError
		if errors.As(err, &cerr) {
			respondCriteriaViolation(w, cerr)
			return
		}
		http.Error(w, "operation denied", http.StatusUnprocessableEntity)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}

// criteriaViolation is the machine-readable refusal shape for the
// criterion-evidence contract (#249/#281): every violation class travels
// named — missing IDs, unknown IDs, duplicates, per-row digest mismatches,
// bad status shapes, and the coverage claim beside its derivation — so a
// corrected body is a fresh call fixing the exact defect instead of a
// guess against a bare denial. Everything in it is derived from the
// caller's own proposed body and the admission-pinned set the operator
// projected; no provider response or credential data is echoed.
type criteriaViolation struct {
	Error           string   `json:"error"`
	Reason          string   `json:"reason"`
	BlockMissing    bool     `json:"blockMissing"`
	Missing         []string `json:"missing,omitempty"`
	Unknown         []string `json:"unknown,omitempty"`
	Duplicate       []string `json:"duplicate,omitempty"`
	BadDigest       []string `json:"badDigest,omitempty"`
	BadStatus       []string `json:"badStatus,omitempty"`
	CoverageClaim   string   `json:"coverageClaim,omitempty"`
	CoverageDerived string   `json:"coverageDerived,omitempty"`
}

func respondCriteriaViolation(w http.ResponseWriter, cerr *CriteriaError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnprocessableEntity)
	_ = json.NewEncoder(w).Encode(criteriaViolation{
		Error:           "criterion-evidence violation",
		Reason:          cerr.Reason,
		BlockMissing:    errors.Is(cerr, ErrCriteriaBlockMissing),
		Missing:         cerr.Missing,
		Unknown:         cerr.Unknown,
		Duplicate:       cerr.Duplicate,
		BadDigest:       cerr.BadDigest,
		BadStatus:       cerr.BadStatus,
		CoverageClaim:   cerr.CoverageClaim,
		CoverageDerived: cerr.CoverageDerived,
	})
}
