package console

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// sessionCookieName is the console's session cookie. The value is
// "<session id>.<hex hmac>" — the id is meaningless without its signature.
const sessionCookieName = "courier_console_session"

// sessionTTL bounds a login session; a signed cookie older than this is
// refused even if the signature verifies.
const sessionTTL = 12 * time.Hour

// sessionIDBytes is the entropy of a session id.
const sessionIDBytes = 32

// session is one operator login: an in-memory record keyed by the id the
// cookie carries. Sessions are deliberately not durable — a console restart
// signs everyone out, which is the honest behavior for a scaffold with no
// persistence layer.
type session struct {
	expires time.Time
}

// sessionStore tracks live sessions. Expiry is checked on read, so a session
// that outlives its TTL is refused and reaped on the next request rather
// than kept alive by a timer; every put also sweeps the expired remainder.
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]time.Time
}

func (s *sessionStore) put(id string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]time.Time)
	}
	// Reap every expired session here, not only the one a stale cookie
	// re-presents: logins mint entries no other code path removes, so the
	// sweep is what keeps the map bounded by live sessions.
	for held, expires := range s.sessions {
		if now.After(expires) {
			delete(s.sessions, held)
		}
	}
	s.sessions[id] = now.Add(sessionTTL)
}

func (s *sessionStore) get(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	expires, ok := s.sessions[id]
	if !ok {
		return false
	}
	if now.After(expires) {
		delete(s.sessions, id)
		return false
	}
	return true
}

func (s *sessionStore) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

// Authenticator implements the console's one authorization model: every
// authenticated identity is an operator. It owns the mode selection, the
// accepted operator tokens, and the session store.
type Authenticator struct {
	mode          AuthMode
	trustedHeader string
	tokens        [][]byte
	key           []byte
	sessions      sessionStore
	// now is the clock; tests pin it.
	now func() time.Time
}

// newAuthenticator builds the authenticator from the server configuration,
// failing closed on an incomplete or unknown mode.
func newAuthenticator(cfg Config) (*Authenticator, error) {
	a := &Authenticator{
		mode:          cfg.Mode,
		trustedHeader: cfg.TrustedHeader,
		tokens:        cfg.OperatorTokens,
		key:           cfg.SessionKey,
		now:           time.Now,
	}
	switch cfg.Mode {
	case AuthModeToken:
		if len(a.key) == 0 {
			return nil, fmt.Errorf("token mode requires a session key")
		}
		if len(a.tokens) == 0 {
			return nil, fmt.Errorf("token mode requires at least one operator token")
		}
	case AuthModeHeader:
		if a.trustedHeader == "" {
			return nil, fmt.Errorf("header mode requires a trusted header name")
		}
		a.key = nil
	default:
		return nil, fmt.Errorf("unknown auth mode %q", cfg.Mode)
	}
	return a, nil
}

// IsOperator is the console's single authorization seam: the console serves
// authenticated operators only and has no per-user roles table. Today it
// returns true for any authenticated identity; a future per-identity policy
// lands here and nowhere else.
func IsOperator(identity string) bool {
	return identity != ""
}

// Identity returns the authenticated operator identity for the request, and
// the session id when one exists (token mode). An absent or forged identity
// is unauthenticated.
func (a *Authenticator) Identity(r *http.Request) (identity, sessionID string, ok bool) {
	switch a.mode {
	case AuthModeToken:
		sessionID, ok = a.SessionFromRequest(r)
		if !ok {
			return "", "", false
		}
		return "operator", sessionID, true
	case AuthModeHeader:
		identity = r.Header.Get(a.trustedHeader)
		if !IsOperator(identity) {
			return "", "", false
		}
		return identity, "", true
	default:
		return "", "", false
	}
}

// sessionAndIdentity is Identity's two-value form for handlers that care
// only about whether the request is authenticated.
func (a *Authenticator) sessionAndIdentity(r *http.Request) (sessionID string, ok bool) {
	_, sessionID, ok = a.Identity(r)
	return sessionID, ok
}

// Login checks the presented operator token against the accepted set and, on
// a match, mints a session cookie. The comparison is constant-time per
// candidate; a wrong token never yields a cookie.
func (a *Authenticator) Login(token string, r *http.Request) (*http.Cookie, bool) {
	if token == "" {
		return nil, false
	}
	presented := []byte(token)
	matched := 0
	for _, accepted := range a.tokens {
		if subtle.ConstantTimeCompare(accepted, presented) == 1 {
			matched = 1
		}
	}
	if matched == 0 {
		return nil, false
	}
	id, err := newSessionID()
	if err != nil {
		return nil, false
	}
	a.sessions.put(id, a.now())
	cookie := a.sessionCookie(r)
	cookie.Value = id + "." + a.sign(id)
	cookie.Expires = a.now().Add(sessionTTL)
	return cookie, true
}

// sessionCookie returns a fresh session cookie carrying the shared posture:
// HttpOnly, SameSite=Lax, and Secure keyed off TLS terminating on the pod
// (docs/operator-console.md). Login and its clearing counterpart must agree
// on these attributes: a browser is not guaranteed to remove a cookie whose
// attributes differ from the cookie a Set-Cookie replaces.
func (a *Authenticator) sessionCookie(r *http.Request) *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r != nil && r.TLS != nil,
	}
}

// SessionFromRequest validates the request's session cookie: the signature
// must verify, the id must be live in the store, and neither may be stale.
func (a *Authenticator) SessionFromRequest(r *http.Request) (string, bool) {
	if len(a.key) == 0 {
		return "", false
	}
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return "", false
	}
	id, sig, found := strings.Cut(cookie.Value, ".")
	if !found || id == "" || sig == "" {
		return "", false
	}
	if subtle.ConstantTimeCompare([]byte(a.sign(id)), []byte(sig)) != 1 {
		return "", false
	}
	if !a.sessions.get(id, a.now()) {
		return "", false
	}
	return id, true
}

// Logout destroys the session the cookie names. The signature still has to
// verify: a logout is only ever a session owner's action.
func (a *Authenticator) Logout(sessionID string) {
	a.sessions.delete(sessionID)
}

// expiredCookie returns the clearing cookie sent on logout. It mirrors the
// session cookie's attributes so the browser removes the cookie it replaces.
func (a *Authenticator) expiredCookie(r *http.Request) *http.Cookie {
	cookie := a.sessionCookie(r)
	cookie.Expires = a.now().Add(-time.Hour)
	cookie.MaxAge = -1
	return cookie
}

// sign returns the hex HMAC-SHA256 of the session id under the session key.
func (a *Authenticator) sign(id string) string {
	mac := hmac.New(sha256.New, a.key)
	mac.Write([]byte(id))
	return hex.EncodeToString(mac.Sum(nil))
}

// newSessionID mints a fresh random session id.
func newSessionID() (string, error) {
	buf := make([]byte, sessionIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
