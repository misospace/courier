// Package console implements the Courier operator console: a small,
// server-rendered scaffold for authenticated operators. It is deliberately
// independent of the operator binary and of the Kubernetes API — it imports
// only the standard library and never talks to a cluster. Pages are
// placeholders until the operator read API exists; the package carries the
// authentication, CSRF, and backend-probe groundwork that scaffold will grow
// into.
package console

import (
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

//go:embed templates static
var content embed.FS

// AuthMode selects how the console authenticates an operator. See
// docs/operator-console.md; the console must only be reachable from the
// trusted proxy in header mode, because the trusted header is the identity.
type AuthMode string

const (
	// AuthModeToken authenticates by a shared operator token submitted to
	// /login, in exchange for an HMAC-signed session cookie.
	AuthModeToken AuthMode = "token"
	// AuthModeHeader trusts an upstream proxy to authenticate operators and
	// to carry the identity in a fixed header.
	AuthModeHeader AuthMode = "header"
)

// Config configures a Server. Secrets are carried as bytes read from files
// by the caller; the console never reads them from environment values.
type Config struct {
	// Mode selects the authentication model. Required.
	Mode AuthMode
	// SessionKey is the HMAC signing key for session cookies and CSRF
	// tokens. Required in token mode.
	SessionKey []byte
	// OperatorTokens are the accepted shared operator tokens. Required in
	// token mode.
	OperatorTokens [][]byte
	// TrustedHeader names the header the upstream proxy sets to the
	// operator identity. Required in header mode.
	TrustedHeader string
	// BackendURL is the base URL of the (future) operator read API. Empty
	// renders the backend-unavailable states.
	BackendURL string
	// BackendTimeout bounds each backend probe. Defaults to 5s.
	BackendTimeout time.Duration
}

// Server is the console's HTTP server. Construct it with NewServer and serve
// Handler.
type Server struct {
	auth    *Authenticator
	backend Backend
	pages   map[string]*template.Template
	mux     *http.ServeMux
}

// templateFS was removed; parsePages takes an fs.FS directly.

// pageTemplates are the rendered pages; each is parsed together with the
// shared layout so {{define "content"}} stays page-local.
var pageTemplates = []string{"overview", "runs", "run", "login", "forbidden"}

// NewServer validates the configuration fail-closed and returns a console
// server. An invalid configuration is a construction error, never a runtime
// surprise.
func NewServer(cfg Config) (*Server, error) {
	auth, err := newAuthenticator(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.BackendTimeout == 0 {
		cfg.BackendTimeout = 5 * time.Second
	}
	pages, err := parsePages(content)
	if err != nil {
		return nil, fmt.Errorf("parse console templates: %w", err)
	}
	s := &Server{
		auth:    auth,
		backend: NewBackend(cfg.BackendURL, cfg.BackendTimeout),
		pages:   pages,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /static/{path}", s.handleStatic)
	mux.HandleFunc("GET /login", s.handleLogin)
	mux.HandleFunc("POST /login", s.handleLoginSubmit)
	mux.HandleFunc("POST /logout", s.handleLogout)
	s.mux = mux
	// Authenticated pages; unauthenticated token-mode requests are sent to
	// /login, header-mode requests without the trusted header are refused.
	mux.HandleFunc("GET /{$}", s.requireOperator(s.handleOverview))
	mux.HandleFunc("GET /runs", s.requireOperator(s.handleRuns))
	mux.HandleFunc("GET /runs/{name}", s.requireOperator(s.handleRun))
	return s, nil
}

// Handler returns the fully wired handler: security headers on every
// response, then the routes.
func (s *Server) Handler() http.Handler {
	return s.securityHeaders(s.mux)
}

// parsePages parses every page template against the shared layout. Each page
// is parsed into its own template set so the per-page "content" definition
// never collides with another page's.
func parsePages(fsys fs.FS) (map[string]*template.Template, error) {
	pages := make(map[string]*template.Template, len(pageTemplates))
	for _, name := range pageTemplates {
		tmpl, err := template.ParseFS(fsys, "templates/layout.html", "templates/"+name+".html")
		if err != nil {
			return nil, err
		}
		pages[name] = tmpl
	}
	return pages, nil
}

// handleHealth serves the unauthenticated liveness endpoint.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, "ok")
}

// handleStatic serves the single allowlisted stylesheet. The pattern already
// pins a single path segment; the explicit check keeps a nested or empty
// name from reaching the filesystem.
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("path")
	if name == "" || strings.ContainsRune(name, '/') {
		http.NotFound(w, r)
		return
	}
	http.ServeFileFS(w, r, content, "static/"+name)
}

// handleOverview renders the overview page with its explicit empty state,
// plus the backend probe error when one failed.
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	data := s.pageData(r, "Overview", "overview")
	data.BackendErr = s.backendError(r)
	s.render(w, http.StatusOK, "overview", data)
}

// handleRuns renders the runs list placeholder. It deliberately shows no
// table: the read API does not exist yet.
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	data := s.pageData(r, "Runs", "runs")
	data.BackendErr = s.backendError(r)
	s.render(w, http.StatusOK, "runs", data)
}

// handleRun renders the run detail placeholder for the requested run name.
// html/template escapes the name before it can reach the response.
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.NotFound(w, r)
		return
	}
	data := s.pageData(r, "Run "+name, "runs")
	data.RunName = name
	data.BackendErr = s.backendError(r)
	s.render(w, http.StatusOK, "run", data)
}

// handleLogin renders the token form. In header mode there is nothing to
// sign in to: identity comes from the trusted proxy, so the page redirects
// to the overview.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.auth.mode == AuthModeHeader {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	if _, ok := s.auth.SessionFromRequest(r); ok {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	data := s.pageData(r, "Sign in", "")
	s.render(w, http.StatusOK, "login", data)
}

// handleLoginSubmit exchanges an operator token for a session cookie. The
// login POST verifies a CSRF token only when the request already carries a
// session — the groundwork path — and always fails closed on a bad token:
// no cookie is set and the form re-renders.
func (s *Server) handleLoginSubmit(w http.ResponseWriter, r *http.Request) {
	if s.auth.mode == AuthModeHeader {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	// When a session already exists, its CSRF token must verify before the
	// request may mint another one.
	if sessionID, ok := s.auth.SessionFromRequest(r); ok {
		if !csrfVerify(s.auth.key, sessionID, r.PostFormValue("csrf")) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}
	cookie, ok := s.auth.Login(r.PostFormValue("token"), r)
	if !ok {
		data := s.pageData(r, "Sign in", "")
		data.LoginError = "That token was not accepted."
		s.render(w, http.StatusUnauthorized, "login", data)
		return
	}
	http.SetCookie(w, cookie)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// handleLogout destroys the session. The POST must carry the session's CSRF
// token; in header mode there is no session to destroy and no CSRF material,
// so the handler simply redirects.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if s.auth.mode == AuthModeHeader {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	sessionID, ok := s.auth.SessionFromRequest(r)
	if !ok {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if !csrfVerify(s.auth.key, sessionID, r.PostFormValue("csrf")) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	s.auth.Logout(sessionID)
	http.SetCookie(w, s.auth.expiredCookie(r))
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// backendError probes the operator read API and returns the error text for
// the page's state box. A console without a backend URL is the expected
// scaffold state, not an error, so it returns empty.
func (s *Server) backendError(r *http.Request) string {
	err := s.backend.Health(r.Context())
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrBackendUnavailable) {
		return ""
	}
	return err.Error()
}

// pageData is the flat view every page template renders. Pages read only the
// fields they care about.
type pageData struct {
	Title string
	// Active names the highlighted nav entry: "overview" or "runs".
	Active string
	// Authenticated reports whether the request carried a recognized
	// operator identity; it gates the sign-out form.
	Authenticated bool
	// CSRF is the current session's CSRF token, empty in header mode.
	CSRF string
	// LoginError carries the rejected-login message.
	LoginError string
	// BackendErr is the backend probe error, empty when healthy or when no
	// backend is configured.
	BackendErr string
	// RunName is the requested run name on the detail page.
	RunName string
}

// pageData builds the view for a page, resolving the authenticated state for
// the request's mode.
func (s *Server) pageData(r *http.Request, title, active string) pageData {
	data := pageData{Title: title, Active: active}
	if sessionID, ok := s.auth.sessionAndIdentity(r); ok {
		data.Authenticated = true
		if s.auth.mode == AuthModeToken {
			data.CSRF = csrfToken(s.auth.key, sessionID)
		}
	}
	return data
}

// render executes the page through the shared layout.
func (s *Server) render(w http.ResponseWriter, status int, page string, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := s.pages[page].ExecuteTemplate(w, "layout", data); err != nil {
		// Headers are already sent; the partial response is the best a
		// server-rendered page can do. The write error surfaces on the
		// connection.
		return
	}
}

// requireOperator guards an authenticated page: token mode redirects
// unauthenticated requests to /login; header mode refuses them with the
// forbidden page. The console must only be reachable from the trusted proxy
// that strips/sets the trusted header (docs/operator-console.md).
func (s *Server) requireOperator(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.auth.sessionAndIdentity(r); ok {
			next(w, r)
			return
		}
		if s.auth.mode == AuthModeToken {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		s.render(w, http.StatusForbidden, "forbidden", pageData{Title: "Forbidden"})
	}
}

// securityHeaders applies the fixed header posture to every response. The
// no-store cache directive applies to everything but the stylesheet, which
// carries no authenticated material.
func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'self'")
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		if !strings.HasPrefix(r.URL.Path, "/static/") {
			h.Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}
