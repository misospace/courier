package console

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// testSecrets carries the injected secret material a test server runs with.
// The markers are deliberately unique byte strings so the leak test can
// assert they never appear in any response body.
type testSecrets struct {
	dir            string
	sessionKeyFile string
	tokensFile     string
	sessionKey     string
	token          string
	secondaryToken string
}

func newTestSecrets(t *testing.T) *testSecrets {
	t.Helper()
	dir := t.TempDir()
	nonce := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	secrets := &testSecrets{
		dir:            dir,
		sessionKeyFile: filepath.Join(dir, "session-key"),
		tokensFile:     filepath.Join(dir, "tokens"),
		sessionKey:     "session-key-marker-" + nonce + "-0f4a9c",
		token:          "operator-token-marker-" + nonce + "-7b21de",
		secondaryToken: "second-token-marker-" + nonce + "-aa03bb",
	}
	body := secrets.token + "\n" + secrets.secondaryToken + "\n"
	if err := os.WriteFile(secrets.sessionKeyFile, []byte(secrets.sessionKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secrets.tokensFile, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return secrets
}

// newTestServer builds a token-mode server over the injected secret files
// and returns its base URL.
func newTestServer(t *testing.T, secrets *testSecrets, backendURL string) (*Server, string) {
	t.Helper()
	server, err := NewServer(Config{
		Mode:           AuthModeToken,
		SessionKey:     []byte(secrets.sessionKey),
		OperatorTokens: [][]byte{[]byte(secrets.token), []byte(secrets.secondaryToken)},
		BackendURL:     backendURL,
		BackendTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return server, httpServer.URL
}

// login exchanges the token for a session cookie and returns the client
// carrying it.
func login(t *testing.T, baseURL, token string) *http.Client {
	t.Helper()
	form := url.Values{"token": {token}}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/login", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /login status = %d, want 303", resp.StatusCode)
	}
	cookies := resp.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("login set %d cookies, want 1", len(cookies))
	}
	return &http.Client{
		Jar: &singleCookieJar{cookie: cookies[0]},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// get performs a GET and returns the response without following redirects.
func get(t *testing.T, client *http.Client, url string) (*http.Response, string) {
	t.Helper()
	if client == nil {
		client = &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		body.WriteString(scanner.Text())
		body.WriteString("\n")
	}
	return resp, body.String()
}

// noFollowClient is a client that never follows redirects.
func noFollowClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// TestUnauthenticatedOverviewRedirectsToLogin covers (a): an unauthenticated
// request to an authenticated page is sent to /login, never rendered.
func TestUnauthenticatedOverviewRedirectsToLogin(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newTestServer(t, secrets, "")
	resp, _ := get(t, nil, base+"/")
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("GET / status = %d, want 303", resp.StatusCode)
	}
	if location := resp.Header.Get("Location"); location != "/login" {
		t.Fatalf("GET / Location = %q, want /login", location)
	}
	for _, path := range []string{"/runs", "/runs/example-run"} {
		resp, _ := get(t, nil, base+path)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
			t.Fatalf("GET %s status = %d, want a 303 to /login", path, resp.StatusCode)
		}
	}
}

// TestTokenLoginRendersPages covers (b): a valid token login mints a session
// and every authenticated page renders 200 with the shared nav.
func TestTokenLoginRendersPages(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newTestServer(t, secrets, "")
	client := login(t, base, secrets.token)
	for _, path := range []string{"/", "/runs", "/runs/example-run"} {
		resp, body := get(t, client, base+path)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
		for _, want := range []string{`href="/"`, `href="/runs"`, "Courier console"} {
			if !strings.Contains(body, want) {
				t.Fatalf("GET %s body is missing the nav fragment %q", path, want)
			}
		}
	}
	// The run detail page names the requested run.
	_, body := get(t, client, base+"/runs/example-run")
	if !strings.Contains(body, "example-run") {
		t.Fatal("run detail page does not name the requested run")
	}
}

// TestWrongTokenRejected covers (c): a wrong token renders no cookie and an
// unauthorized status.
func TestWrongTokenRejected(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newTestServer(t, secrets, "")
	form := strings.NewReader("token=not-the-operator-token")
	req, err := http.NewRequest(http.MethodPost, base+"/login", form)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := noFollowClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-token POST /login status = %d, want 401", resp.StatusCode)
	}
	if cookies := resp.Cookies(); len(cookies) != 0 {
		t.Fatalf("wrong-token POST /login set cookies %v, want none", cookies)
	}
}

// TestHeaderModeIdentityCovers (d): the trusted header is the only identity
// source; its absence is a 403, its presence a rendered page.
func TestHeaderModeIdentity(t *testing.T) {
	server, err := NewServer(Config{Mode: AuthModeHeader, TrustedHeader: "X-Forwarded-User"})
	if err != nil {
		t.Fatal(err)
	}
	base := httptest.NewServer(server.Handler()).URL
	anonymous, _ := get(t, nil, base+"/")
	if anonymous.StatusCode != http.StatusForbidden {
		t.Fatalf("header-mode GET / without the header status = %d, want 403", anonymous.StatusCode)
	}
	_, anonymousBody := get(t, nil, base+"/")
	if strings.Contains(anonymousBody, "<h1>Overview</h1>") {
		t.Fatal("forbidden page must not render the overview content")
	}
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	request, err := http.NewRequest(http.MethodGet, base+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Forwarded-User", "ops")
	resp, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "Overview") {
		t.Fatalf("header-mode GET / with the header status = %d, want a 200 overview page", resp.StatusCode)
	}
}

// TestBackendUnavailableRendersEmptyState covers (e): pages stay 200 with a
// visible empty state or the probe's error text.
func TestBackendUnavailableRendersEmptyState(t *testing.T) {
	secrets := newTestSecrets(t)
	// No backend URL at all: the plain empty state.
	_, base := newTestServer(t, secrets, "")
	client := login(t, base, secrets.token)
	for _, path := range []string{"/", "/runs"} {
		resp, body := get(t, client, base+path)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
		if !strings.Contains(body, "the operator read API is not available yet") {
			t.Fatalf("GET %s body is missing the empty state", path)
		}
	}

	// A failing backend: the error text is visible on the page.
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend down", http.StatusInternalServerError)
	}))
	defer failing.Close()
	secretsFailing := newTestSecrets(t)
	_, failingBase := newTestServer(t, secretsFailing, failing.URL)
	failingClient := login(t, failingBase, secretsFailing.token)
	resp, body := get(t, failingClient, failingBase+"/")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / with a failing backend status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(body, "backend health check returned status 500") {
		t.Fatalf("GET / with a failing backend is missing the probe error text:\n%s", body)
	}
}

// TestSecurityHeadersOnEveryResponse covers (f): the fixed posture rides
// every response, authenticated or not, page or asset.
func TestSecurityHeadersOnEveryResponse(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newTestServer(t, secrets, "")
	client := login(t, base, secrets.token)
	for _, path := range []string{"/", "/runs", "/login", "/healthz", "/static/main.css"} {
		resp, _ := get(t, client, base+path)
		h := resp.Header
		if got, want := h.Get("Content-Security-Policy"), "default-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'self'"; got != want {
			t.Fatalf("GET %s Content-Security-Policy = %q, want %q", path, got, want)
		}
		if got, want := h.Get("X-Frame-Options"), "DENY"; got != want {
			t.Fatalf("GET %s X-Frame-Options = %q, want %q", path, got, want)
		}
		if got, want := h.Get("X-Content-Type-Options"), "nosniff"; got != want {
			t.Fatalf("GET %s X-Content-Type-Options = %q, want %q", path, got, want)
		}
		if got, want := h.Get("Referrer-Policy"), "no-referrer"; got != want {
			t.Fatalf("GET %s Referrer-Policy = %q, want %q", path, got, want)
		}
		if !strings.HasPrefix(path, "/static/") {
			if got, want := h.Get("Cache-Control"), "no-store"; got != want {
				t.Fatalf("GET %s Cache-Control = %q, want %q", path, got, want)
			}
		}
	}
}

// TestLogoutRequiresCSRF covers (g): the logout POST must carry the
// session's CSRF token.
func TestLogoutRequiresCSRF(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newTestServer(t, secrets, "")
	client := login(t, base, secrets.token)

	// The token arrives on the rendered page, so fetch it from the overview.
	_, body := get(t, client, base+"/")
	csrf := findCSRFToken(t, body)
	if csrf == "" {
		t.Fatal("authenticated page does not carry the CSRF token")
	}

	// Without the token: 403, session survives.
	form := strings.NewReader("")
	request, err := http.NewRequest(http.MethodPost, base+"/logout", form)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	applyCookie(t, client, request)
	resp, err := noFollowClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("logout without a CSRF token status = %d, want 403", resp.StatusCode)
	}
	after, _ := get(t, client, base+"/")
	if after.StatusCode != http.StatusOK {
		t.Fatalf("overview after a refused logout status = %d, want the session to survive", after.StatusCode)
	}

	// With the token: the session is destroyed.
	form = strings.NewReader("csrf=" + csrf)
	request, err = http.NewRequest(http.MethodPost, base+"/logout", form)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	applyCookie(t, client, request)
	resp, err = noFollowClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout with the CSRF token status = %d, want 303", resp.StatusCode)
	}
	final, _ := get(t, client, base+"/")
	if final.StatusCode != http.StatusSeeOther {
		t.Fatalf("overview after logout status = %d, want the session to be gone", final.StatusCode)
	}
}

// findCSRFToken pulls the hidden csrf field out of a rendered page.
func findCSRFToken(t *testing.T, body string) string {
	t.Helper()
	re := regexp.MustCompile(`name="csrf" value="([0-9a-f]+)"`)
	m := re.FindStringSubmatch(body)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

// TestSessionCookieFlags covers (h): the session cookie is HttpOnly and
// SameSite=Lax, and Secure under TLS; and the logout clearing cookie mirrors
// those attributes, because a Set-Cookie whose attributes differ from the
// cookie it replaces is not guaranteed to remove it.
func TestSessionCookieFlags(t *testing.T) {
	secrets := newTestSecrets(t)
	server, err := NewServer(Config{
		Mode:           AuthModeToken,
		SessionKey:     []byte(secrets.sessionKey),
		OperatorTokens: [][]byte{[]byte(secrets.token)},
		BackendTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	tlsServer := httptest.NewTLSServer(server.Handler())
	defer tlsServer.Close()
	form := url.Values{"token": {secrets.token}}
	req, err := http.NewRequest(http.MethodPost, tlsServer.URL+"/login", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// The httptest TLS server's own client trusts its certificate; redirect
	// following is disabled so the login response itself is observed.
	client := tlsServer.Client()
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /login status = %d, want 303", resp.StatusCode)
	}
	cookies := resp.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("login set %d cookies, want 1", len(cookies))
	}
	cookie := cookies[0]
	if !cookie.HttpOnly {
		t.Fatal("session cookie is not HttpOnly")
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie SameSite = %v, want Lax", cookie.SameSite)
	}
	if !cookie.Secure {
		t.Fatal("session cookie over TLS is not Secure")
	}
	if cookie.Expires.IsZero() || time.Until(cookie.Expires) > 12*time.Hour {
		t.Fatalf("session cookie expiry %v is not within 12h", cookie.Expires)
	}

	// Log out over the same TLS server: the clearing cookie must mirror the
	// login cookie's attributes. The CSRF token comes from an authenticated
	// page; this client carries no jar, so the cookie rides manually.
	overviewReq, err := http.NewRequest(http.MethodGet, tlsServer.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	overviewReq.AddCookie(cookie)
	overviewResp, err := client.Do(overviewReq)
	if err != nil {
		t.Fatal(err)
	}
	overviewBody := readAll(t, overviewResp)
	overviewResp.Body.Close()
	csrf := findCSRFToken(t, overviewBody)
	if csrf == "" {
		t.Fatal("authenticated page over TLS does not carry the CSRF token")
	}
	logoutReq, err := http.NewRequest(http.MethodPost, tlsServer.URL+"/logout",
		strings.NewReader("csrf="+csrf))
	if err != nil {
		t.Fatal(err)
	}
	logoutReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	logoutReq.AddCookie(cookie)
	logoutResp, err := client.Do(logoutReq)
	if err != nil {
		t.Fatal(err)
	}
	logoutResp.Body.Close()
	if logoutResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout status = %d, want 303", logoutResp.StatusCode)
	}
	clearing := logoutResp.Cookies()
	if len(clearing) != 1 {
		t.Fatalf("logout set %d cookies, want 1", len(clearing))
	}
	cleared := clearing[0]
	if cleared.Name != cookie.Name || cleared.Value != "" {
		t.Fatalf("logout cookie = %q with value %q, want the session cookie name cleared",
			cleared.Name, cleared.Value)
	}
	if cleared.Path != cookie.Path {
		t.Fatalf("logout cookie path = %q, want %q", cleared.Path, cookie.Path)
	}
	if !cleared.HttpOnly {
		t.Fatal("logout clearing cookie is not HttpOnly")
	}
	if cleared.SameSite != cookie.SameSite {
		t.Fatalf("logout clearing cookie SameSite = %v, want %v", cleared.SameSite, cookie.SameSite)
	}
	if cleared.Secure != cookie.Secure {
		t.Fatalf("logout clearing cookie Secure = %v, want the login cookie's %v",
			cleared.Secure, cookie.Secure)
	}
	if !cleared.Expires.Before(time.Now()) {
		t.Fatalf("logout clearing cookie expiry %v is not in the past", cleared.Expires)
	}
}

// TestResponsesNeverLeakSecretMaterial covers (i): the operator token bytes
// and the session key bytes never appear in any response body, on any route,
// before or after login.
func TestResponsesNeverLeakSecretMaterial(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newTestServer(t, secrets, "")
	paths := []string{"/", "/runs", "/runs/example-run", "/login", "/logout", "/healthz", "/static/main.css"}
	for _, path := range paths {
		_, body := get(t, nil, base+path)
		for _, marker := range []string{secrets.token, secrets.secondaryToken, secrets.sessionKey} {
			if strings.Contains(body, marker) {
				t.Fatalf("GET %s (unauthenticated) leaked secret material in the response body", path)
			}
		}
	}
	client := login(t, base, secrets.token)
	for _, path := range paths {
		resp, body := get(t, client, base+path)
		// A wrong-token 401 re-renders; a 303 carries no body worth
		// scanning, but scan it anyway.
		_ = resp
		for _, marker := range []string{secrets.token, secrets.secondaryToken, secrets.sessionKey} {
			if strings.Contains(body, marker) {
				t.Fatalf("GET %s (authenticated) leaked secret material in the response body", path)
			}
		}
	}
	// A failed login must not echo the secret key either.
	form := strings.NewReader("token=wrong")
	req, err := http.NewRequest(http.MethodPost, base+"/login", form)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := noFollowClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if strings.Contains(readAll(t, resp), secrets.sessionKey) {
		t.Fatal("failed login leaked the session key")
	}
}

// TestIsOperatorIsTheAuthorizationSeam pins the seam: today any
// authenticated identity is an operator; the test documents that the policy
// lives in exactly one place.
func TestIsOperatorIsTheAuthorizationSeam(t *testing.T) {
	if !IsOperator("ops") {
		t.Fatal("IsOperator must accept any authenticated identity today")
	}
	if IsOperator("") {
		t.Fatal("IsOperator must refuse an empty identity")
	}
}

// TestBackendHealthAllowlist pins the read groundwork: the backend probe
// GETs exactly the allowlisted path, on the configured base URL, and
// reports the unconfigured state distinctly.
func TestBackendHealthAllowlist(t *testing.T) {
	var gotPath, gotMethod string
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		fmt.Fprint(w, "ok")
	}))
	defer healthy.Close()
	backend := NewBackend(healthy.URL, time.Second)
	if err := backend.Health(context.Background()); err != nil {
		t.Fatalf("Health on a healthy backend: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/healthz" {
		t.Fatalf("backend probe reached %s %s, want GET /healthz", gotMethod, gotPath)
	}

	if err := NewBackend("", time.Second).Health(context.Background()); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("Health without a backend URL = %v, want ErrBackendUnavailable", err)
	}
}

// singleCookieJar is the minimal cookie jar the tests need: one fixed
// cookie on every request.
type singleCookieJar struct {
	cookie *http.Cookie
}

func (j *singleCookieJar) Cookies(u *url.URL) []*http.Cookie {
	c := *j.cookie
	return []*http.Cookie{&c}
}

func (*singleCookieJar) SetCookies(*url.URL, []*http.Cookie) {}

// applyCookie copies the client's jar cookie onto a manually built request.
func applyCookie(t *testing.T, client *http.Client, request *http.Request) {
	t.Helper()
	jar, ok := client.Jar.(*singleCookieJar)
	if !ok || jar == nil {
		t.Fatal("client carries no session cookie")
	}
	request.AddCookie(jar.cookie)
}

// readAll drains and returns a response body.
func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	var body strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		body.WriteString(scanner.Text())
		body.WriteString("\n")
	}
	return body.String()
}
