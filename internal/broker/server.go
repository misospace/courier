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
	"strings"
)

const (
	PathPublish       = "/v1/publication"
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
type ServerConfig struct {
	Policy        *PolicyEngine
	Authenticator RequestAuthenticator
	TLSCertFile   string
	TLSKeyFile    string
}

type Server struct {
	policy        *PolicyEngine
	authenticator RequestAuthenticator
	handler       http.Handler
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.Policy == nil || config.Authenticator == nil {
		return nil, errors.New("broker startup requires run-bound policy and identity backend")
	}
	if config.TLSCertFile == "" || config.TLSKeyFile == "" {
		return nil, errors.New("broker startup requires TLS certificate and key")
	}
	if _, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile); err != nil {
		return nil, fmt.Errorf("load broker TLS identity: %w", err)
	}
	s := &Server{policy: config.Policy, authenticator: config.Authenticator}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+PathPublish, s.publish)
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
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return Identity{}, false
	}
	return identity, true
}

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
