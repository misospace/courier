// Package console implements the Courier operator console: a small,
// server-rendered interface for authenticated operators. It is deliberately
// independent of the operator binary and of the Kubernetes API — it imports
// only the standard library and never talks to a cluster. Its pages render
// the operator read API's allowlisted projections over fixed or validated
// GET paths; the read API client lives in backend.go and decodes
// projection JSON into its own view types rather than importing the read
// API package, so the standard-library-only invariant holds. Contributions
// submit through the operator's HTTP admission API (#829), and the
// contribution pages read one run's lifecycle status from a second
// backend — the contribution listener's status route — over a separate
// status-read credential, with the same fixed-path discipline
// (contributions.go).
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
	// BackendURL is the base URL of the operator read API. Empty renders the
	// backend-unavailable states and disables the run explorer.
	BackendURL string
	// BackendToken is the bearer token the console presents to the read
	// API's projection routes. Secrets reach it as bytes read from a mounted
	// file by the caller, never from an environment value. Empty disables
	// the run explorer (which needs the token) while leaving the
	// unauthenticated health probe working.
	BackendToken []byte
	// BackendTimeout bounds each backend request. Defaults to 5s.
	BackendTimeout time.Duration
	// ContributeURL is the base URL of the contribution admission API.
	ContributeURL string
	// ContributeToken authenticates requests to the contribution admission API.
	ContributeToken []byte
	// ContributeTimeout bounds each contribution request. Defaults to 20s.
	ContributeTimeout time.Duration
	// InsecureCookies opts out of the Secure session-cookie posture. The
	// default (false) sets Secure on every session cookie, because the
	// supported Helm deployment terminates TLS at a proxy and the hop that
	// reaches this listener is plaintext HTTP; the posture is never
	// inferred from r.TLS. Set true only for a console reachable solely
	// over plaintext the operator controls.
	InsecureCookies bool
	// ContributeStatusURL is the base URL of the contribution status API
	// (the manager's contribution listener serving GET
	// /v1/contributions/<run>). Empty renders the contribution pages'
	// not-configured state and sends no request.
	ContributeStatusURL string
	// ContributeStatusToken is the status-read bearer token the console
	// presents to the contribution status API. It is a different
	// credential than the admission token and the read API token: a
	// status-read token cannot admit, an admission token cannot read.
	// Secrets reach it as bytes read from a mounted file by the caller.
	ContributeStatusToken []byte
}

// Server is the console's HTTP server. Construct it with NewServer and serve
// Handler.
type Server struct {
	auth       *Authenticator
	backend    Backend
	contribute *httpContributeClient
	minter     contributeMinter
	settled    contributeSettled
	// contributions reads one contribution's lifecycle status from the
	// contribution listener. Nil when no status backend is configured.
	contributions contributeStatusClient
	pages         map[string]*template.Template
	mux           *http.ServeMux
}

// pageTemplates are the rendered pages; each is parsed together with the
// shared layout so {{define "content"}} stays page-local.
var pageTemplates = []string{"overview", "runs", "run", "contribute", "contributions", "contribution", "login", "forbidden"}

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
	if (cfg.ContributeURL == "") != (len(cfg.ContributeToken) == 0) {
		return nil, errors.New("console: contribution admission requires both ContributeURL and ContributeToken")
	}
	var contribute *httpContributeClient
	if cfg.ContributeURL != "" {
		if cfg.ContributeTimeout <= 0 {
			cfg.ContributeTimeout = 20 * time.Second
		}
		var err error
		contribute, err = newHTTPContributeClient(cfg.ContributeURL, cfg.ContributeToken, cfg.ContributeTimeout)
		if err != nil {
			return nil, err
		}
	}
	pages, err := parsePages(content)
	if err != nil {
		return nil, fmt.Errorf("parse console templates: %w", err)
	}
	s := &Server{
		auth:       auth,
		backend:    NewBackend(cfg.BackendURL, cfg.BackendToken, cfg.BackendTimeout),
		contribute: contribute,
		pages:      pages,
	}
	if cfg.ContributeStatusURL != "" {
		contributions, err := newHTTPContributeStatusClient(cfg.ContributeStatusURL, cfg.ContributeStatusToken)
		if err != nil {
			return nil, err
		}
		s.contributions = contributions
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
	mux.HandleFunc("GET /contribute", s.requireOperator(s.handleContribute))
	mux.HandleFunc("POST /contribute", s.requireOperator(s.handleContributeSubmit))
	mux.HandleFunc("POST /contribute/new", s.requireOperator(s.handleContributeNew))
	mux.HandleFunc("GET /contributions", s.requireOperator(s.handleContributions))
	mux.HandleFunc("GET /contributions/{run}", s.requireOperator(s.handleContribution))
	return s, nil
}

// Handler returns the fully wired handler: security headers on every
// response, then the routes.
func (s *Server) Handler() http.Handler {
	return s.securityHeaders(s.mux)
}

// parsePages parses every page template against the shared layout. Each page
// is parsed into its own template set so the per-page "content" definition
// never collides with another page's. The shared funcs are attached to each
// set so page templates may call them.
func parsePages(fsys fs.FS) (map[string]*template.Template, error) {
	pages := make(map[string]*template.Template, len(pageTemplates))
	for _, name := range pageTemplates {
		tmpl, err := template.New(name).Funcs(pageFuncs).ParseFS(fsys, "templates/layout.html", "templates/"+name+".html")
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

// handleOverview renders the namespace overview over the read API's
// summary, lane, and source projections. It lives in overview.go.

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
	cookie, ok := s.auth.Login(r.PostFormValue("token"))
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
	http.SetCookie(w, s.auth.expiredCookie())
	http.SetCookie(w, s.auth.expiredContributeIntentCookie())
	http.SetCookie(w, s.forgetContributeHistory())
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// backendError probes the operator read API and returns the error text for
// the overview page's backend status. A console without a backend URL is the
// expected unconfigured state, not a probe failure.
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
	// Active names the highlighted nav entry: "overview", "runs", or
	// "contributions".
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
	// RunName is the validated requested run name on the detail page.
	RunName     string
	Run         *RunDetailResult
	RunErr      string
	RunNotFound bool

	// Overview page view. Only the overview page populates these.

	// Overview is the decoded namespace summary; nil when OverviewErr is set.
	Overview *OverviewResult
	// OverviewErr replaces the whole page body with one state message when
	// the namespace summary could not be read: an outage never renders as
	// an empty namespace.
	OverviewErr string
	// Lanes / Sources are the per-entity health rows and LanesTruncated /
	// SourcesTruncated state that the scan behind them hit the read API's
	// fetch cap; LanesErr / SourcesErr replace only their own section when
	// that one list could not be read.
	Lanes            []LaneRow
	LanesTruncated   bool
	LanesErr         string
	Sources          []SourceRow
	SourcesTruncated bool
	SourcesErr       string
	// PhaseRows are the run-phase chips in lifecycle order; ConditionRows
	// list the allowlisted condition backlog.
	PhaseRows     []PhaseCount
	ConditionRows []LabeledCount
	// RefreshSeconds is the meta-refresh interval for the page. Zero means
	// no auto-refresh: the overview sets it only when there is a backend
	// worth polling (slowly when healthy, slower on a backend error, never
	// when the backend is unconfigured).
	RefreshSeconds int

	// Run explorer view. Only the runs page populates these; every other
	// page leaves them at their zero value, which the runs template is the
	// only one that reads.

	// Runs are the current page's rows (may be empty — an empty page is a
	// valid result, distinct from RunsErr, which means no data could be
	// shown).
	Runs []RunRow
	// RunsErr is a non-empty operator-facing state message that replaces
	// the table: the explorer is unconfigured, the read API is unreachable
	// or refused the token, or the submitted filters are invalid.
	RunsErr string
	// Filter echoes the current (validated) filter back into the form.
	Filter RunsFilter
	// PhaseOptions is the phase dropdown; PageSizes is the page-size
	// dropdown, always including the active page size.
	PhaseOptions []string
	PageSizes    []int
	// PageSize is the page size actually in effect, for the dropdown.
	PageSize int
	// PageNumber is the 1-based page number shown to the operator.
	PageNumber int
	// Total is the cheap count from the read API (a floor once filtered,
	// a window when Truncated).
	Total     int
	More      bool
	Truncated bool
	// Filtered is true when any filter is active (drives a clear-filters
	// affordance and the "matching filters" note).
	Filtered bool
	// PrevURL / NextURL are pre-built, filter-preserving pagination links.
	// Empty means no such page.
	PrevURL string
	NextURL string
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

// render executes the page through the shared layout. Pages carry their
// own view struct; the layout reads only the fields its chrome needs
// (title, active nav, authentication, refresh), which every page view
// provides.
func (s *Server) render(w http.ResponseWriter, status int, page string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	s.renderBody(w, page, data)
}

// renderBody executes the page template against the response body. Callers
// use it directly only after they already committed the status line and
// headers: the contribution submit path flushes the intent cookie before the
// admission call so a lost response cannot orphan the attempt's key.
func (s *Server) renderBody(w http.ResponseWriter, page string, data any) {
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
