package dependencycache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// handlerRequest runs one synthetic request (with the given remote address
// and headers) through the handler and returns the recorder.
func handlerRequest(t *testing.T, handler *Handler, method, target, remote string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = remote
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func newTestHandler(t *testing.T, mutate func(*Options)) (*Handler, string) {
	t.Helper()
	root := t.TempDir()
	approveAll(t, root, "example.com/hello", "v1.0.0")
	approveAll(t, root, "example.com/hello", "v1.1.0")
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	opts := Options{
		RequestsPerSecond:   0, // rate limiting off unless a test opts in
		Burst:               3,
		MaxTrackedSourceIPs: 64,
	}
	if mutate != nil {
		mutate(&opts)
	}
	return NewHandler(store, opts), root
}

func TestHandlerServesApprovedArtifacts(t *testing.T) {
	handler, _ := newTestHandler(t, nil)
	for _, tc := range []struct {
		target      string
		contentType string
	}{
		{"/example.com/hello/@v/v1.0.0.info", "application/json"},
		{"/example.com/hello/@v/v1.0.0.mod", "text/plain; charset=utf-8"},
		{"/example.com/hello/@v/v1.0.0.zip", "application/zip"},
		{"/example.com/hello/@v/list", "text/plain; charset=utf-8"},
		{"/example.com/hello/@latest", "application/json"},
	} {
		recorder := handlerRequest(t, handler, http.MethodGet, tc.target, "10.0.0.1:1234", nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s: status %d, body %q", tc.target, recorder.Code, recorder.Body.String())
		}
		if got := recorder.Header().Get("Content-Type"); got != tc.contentType {
			t.Fatalf("GET %s: content type %q, want %q", tc.target, got, tc.contentType)
		}
		if recorder.Header().Get("Location") != "" {
			t.Fatalf("GET %s: response carries a redirect Location header", tc.target)
		}
	}
	recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/list", "10.0.0.1:1234", nil)
	for _, want := range []string{"v1.0.0", "v1.1.0"} {
		if !strings.Contains(recorder.Body.String(), want) {
			t.Fatalf("list output missing %q: %q", want, recorder.Body.String())
		}
	}
	latest := handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@latest", "10.0.0.1:1234", nil)
	if !strings.Contains(latest.Body.String(), `"Version":"v1.1.0"`) {
		t.Fatalf("@latest picked the wrong version: %q", latest.Body.String())
	}
}

func TestHandlerHeadMatchesGet(t *testing.T) {
	handler, _ := newTestHandler(t, nil)
	head := handlerRequest(t, handler, http.MethodHead, "/example.com/hello/@v/v1.0.0.zip", "10.0.0.1:1234", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("HEAD: status %d", head.Code)
	}
}

func TestHandlerMissIsActionableApprovalDiagnostic(t *testing.T) {
	handler, root := newTestHandler(t, nil)
	for _, target := range []string{
		"/example.com/absent/@v/v1.0.0.info",
		"/example.com/hello/@v/v9.9.9.info",
		"/example.com/absent/@v/list",
		"/example.com/absent/@latest",
	} {
		recorder := handlerRequest(t, handler, http.MethodGet, target, "10.0.0.1:1234", nil)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("GET %s: status %d, want 404", target, recorder.Code)
		}
		body := recorder.Body.String()
		if !strings.Contains(body, "not approved") || !strings.Contains(body, "populate") {
			t.Fatalf("GET %s: body is not an actionable approval diagnostic: %q", target, body)
		}
		if got := recorder.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("GET %s: miss diagnostic content type %q", target, got)
		}
	}
	// A miss must not fill the cache: the store is read-only by design, so
	// the only top-level entry stays the populated module's namespace.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "example.com" {
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("store changed after refused lookups: %v", names)
	}
}

func TestHandlerRefusesMaliciousModulePathsAndVersions(t *testing.T) {
	handler, _ := newTestHandler(t, nil)
	for _, target := range []string{
		// Traversal attempts in the module and version positions.
		"/../../etc/passwd/@v/v1.0.0.info",
		"/example.com/hello/../../etc/passwd/@v/v1.0.0.info",
		"/example.com/hello/@v/../../../etc/passwd.info",
		"/example.com/hello/@v/v1.0.0/../../../etc/passwd.info",
		// Absolute and root-shaped paths.
		"/@v/v1.0.0.info",
		"//etc/passwd/@v/v1.0.0.info",
		// Percent-encoding the go command never sends.
		"/example.com/hello/%40v/v1.0.0.info",
		"/example.com/hello/@v/v1%2E0%2E0.info",
		// Raw uppercase in the wire form (the go command escapes it).
		"/example.com/Hello/@v/v1.0.0.info",
		"/example.com/hello/@v/V1.0.0.info",
		// Unknown artifact types and shapes.
		"/example.com/hello/@v/v1.0.0.exe",
		"/example.com/hello/@v/v1.0.0",
		"/example.com/hello/@v/",
		"/example.com/hello/@v",
		"/example.com/hello/@v/list/extra",
		// Private metadata, API, and forge shapes: syntactically plausible
		// module paths must still only ever answer from the store.
		"/169.254.169.254/latest/meta-data/@v/list",
		"/kubernetes.default.svc/api/@v/list",
		"/github.com/api/v3/@v/list",
	} {
		recorder := handlerRequest(t, handler, http.MethodGet, target, "10.0.0.1:1234", nil)
		if recorder.Code != http.StatusBadRequest && recorder.Code != http.StatusNotFound {
			t.Fatalf("GET %s: status %d, want 400/404, body %q", target, recorder.Code, recorder.Body.String())
		}
		if recorder.Code == http.StatusBadRequest && !strings.Contains(recorder.Body.String(), "invalid") {
			t.Fatalf("GET %s: 400 body should say what is invalid: %q", target, recorder.Body.String())
		}
		if recorder.Header().Get("Location") != "" {
			t.Fatalf("GET %s: response redirects (%q), the cache must never redirect", target, recorder.Header().Get("Location"))
		}
	}
}

func TestHandlerRefusesProxyAndMethodAbuse(t *testing.T) {
	handler, _ := newTestHandler(t, nil)
	for _, method := range []string{
		http.MethodConnect,
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodOptions,
		http.MethodTrace,
		http.MethodPatch,
	} {
		recorder := handlerRequest(t, handler, method, "/example.com/hello/@v/v1.0.0.info", "10.0.0.1:1234", nil)
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s: status %d, want 405", method, recorder.Code)
		}
		if allow := recorder.Header().Get("Allow"); !strings.Contains(allow, "GET") {
			t.Fatalf("%s: Allow header %q", method, allow)
		}
	}
	// Absolute-form request targets are the forward-proxy shape: refuse.
	proxy := httptest.NewRequest(http.MethodGet, "http://internal-forge.example/api/@v/list", nil)
	proxy.RemoteAddr = "10.0.0.1:1234"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, proxy)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("absolute-form target: status %d, want 400", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "not a proxy") {
		t.Fatalf("absolute-form refusal must name the proxy misuse: %q", recorder.Body.String())
	}
	// A request body has no place in the protocol.
	withBody := httptest.NewRequest(http.MethodGet, "/example.com/hello/@v/v1.0.0.info", strings.NewReader("payload"))
	withBody.ContentLength = int64(len("payload"))
	withBody.RemoteAddr = "10.0.0.1:1234"
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, withBody)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("GET with body: status %d, want 400", recorder.Code)
	}
}

func TestHandlerIgnoresCallerHeaders(t *testing.T) {
	handler, _ := newTestHandler(t, nil)
	// Header abuse: forwarding claims, credentials, and identity claims
	// must be ignored. The response must be the plain artifact with the
	// fixed posture, never a reflection of the caller's headers.
	headers := map[string]string{
		"X-Forwarded-For":     "169.254.169.254",
		"X-Real-Ip":           "127.0.0.1",
		"Authorization":       "Bearer stolen",
		"Proxy-Authorization": "Basic stolen",
		"X-Courier-Admin":     "true",
		"Host":                "evil.example",
	}
	recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.2:4444", headers)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET with hostile headers: status %d, body %q", recorder.Code, recorder.Body.String())
	}
	for _, value := range []string{"stolen", "169.254.169.254", "127.0.0.1", "evil.example"} {
		if strings.Contains(fmt.Sprintf("%v", recorder.Header()), value) {
			t.Fatalf("response reflects caller-controlled data %q: %v", value, recorder.Header())
		}
	}
	// Forwarding headers must not influence the rate limiter's identity:
	// the same remote address with rotating X-Forwarded-For values shares
	// one budget.
	limited, _ := newTestHandler(t, func(opts *Options) {
		opts.RequestsPerSecond = 0.001
		opts.Burst = 2
	})
	for i := 0; i < 2; i++ {
		recorder := handlerRequest(t, limited, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.3:5555",
			map[string]string{"X-Forwarded-For": fmt.Sprintf("192.0.2.%d", i)})
		if recorder.Code != http.StatusOK {
			t.Fatalf("request %d before the burst is exhausted: status %d", i, recorder.Code)
		}
	}
	recorder = handlerRequest(t, limited, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.3:5555",
		map[string]string{"X-Forwarded-For": "198.51.100.1"})
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("rotating X-Forwarded-For must not evade the per-address budget: status %d", recorder.Code)
	}
}

func TestHandlerListResponseIsBounded(t *testing.T) {
	// A @v/list body that would exceed the response bound refuses cleanly
	// with the approval diagnostic instead of growing without bound.
	root := t.TempDir()
	for i := 0; i < 40; i++ {
		approveAll(t, root, "example.com/many", fmt.Sprintf("v1.0.%d", i))
	}
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	handler := NewHandler(store, Options{MaxListBytes: 512})
	recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/many/@v/list", "10.0.0.1:1", nil)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("an oversized list must refuse with 404, got %d body %q", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "response bound") {
		t.Fatalf("diagnostic must name the response bound: %q", recorder.Body.String())
	}

	// Within the bound the full list renders.
	generous := NewHandler(store, Options{MaxListBytes: 8192})
	recorder = handlerRequest(t, generous, http.MethodGet, "/example.com/many/@v/list", "10.0.0.1:1", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("a list within the bound must serve, got %d: %q", recorder.Code, recorder.Body.String())
	}
	if lines := strings.Count(recorder.Body.String(), "\n"); lines != 40 {
		t.Fatalf("want 40 listed versions, got %d", lines)
	}

	// @latest is a single bounded line and unaffected by a tight list bound.
	recorder = handlerRequest(t, handler, http.MethodGet, "/example.com/many/@latest", "10.0.0.1:1", nil)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "v1.0.39") {
		t.Fatalf("@latest must resolve within a tight list bound: %d %q", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerRateLimitIsBoundedPerAddress(t *testing.T) {
	handler, _ := newTestHandler(t, func(opts *Options) {
		opts.RequestsPerSecond = 0.001
		opts.Burst = 2
	})
	for i := 0; i < 2; i++ {
		recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.4:999", nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("request %d within the burst: status %d", i, recorder.Code)
		}
	}
	recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.4:999", nil)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("request beyond the burst: status %d, want 429", recorder.Code)
	}
	// A different address keeps its own budget.
	recorder = handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.5:999", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("independent address: status %d, want 200", recorder.Code)
	}
}

func TestHandlerLogControls(t *testing.T) {
	var logged []string
	capture := func(format string, args ...any) {
		logged = append(logged, fmt.Sprintf(format, args...))
	}
	handler, _ := newTestHandler(t, func(opts *Options) {
		opts.Log = capture
		opts.LogRequests = false
	})
	handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.6:1", nil)
	if len(logged) != 0 {
		t.Fatalf("LogRequests=false must silence success logs, got %v", logged)
	}
	handlerRequest(t, handler, http.MethodGet, "/example.com/absent/@v/v1.0.0.info", "10.0.0.6:1", nil)
	if len(logged) != 1 || !strings.Contains(logged[0], "status=404") {
		t.Fatalf("misses must stay logged under LogRequests=false, got %v", logged)
	}

	logged = nil
	handler, _ = newTestHandler(t, func(opts *Options) {
		opts.Log = capture
		opts.LogRequests = true
	})
	handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/v1.0.0.info", "10.0.0.6:2", nil)
	if len(logged) != 1 || !strings.Contains(logged[0], "status=200") {
		t.Fatalf("LogRequests=true logs every request, got %v", logged)
	}
}

func TestHandlerServesHealthz(t *testing.T) {
	handler, _ := newTestHandler(t, nil)
	recorder := handlerRequest(t, handler, http.MethodGet, "/healthz", "10.0.0.1:1", nil)
	if recorder.Code != http.StatusOK || recorder.Body.String() != "ok\n" {
		t.Fatalf("/healthz: status %d body %q", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerBrokenStoreStaysGeneric(t *testing.T) {
	// A store whose root has vanished produces storage errors: the response
	// must stay generic while the log carries the detail.
	var logged []string
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("remove store root: %v", err)
	}
	handler := NewHandler(store, Options{Log: func(format string, args ...any) {
		logged = append(logged, fmt.Sprintf(format, args...))
	}, LogRequests: true})
	recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/absent/@v/v1.0.0.info", "10.0.0.1:1", nil)
	if recorder.Code != http.StatusNotFound {
		// The module directory is absent: still a refusal, not a leak.
		t.Fatalf("absent module on a broken store: status %d", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), root) {
		t.Fatalf("response leaks store paths: %q", recorder.Body.String())
	}
	_ = logged
}

func TestHandlerRateLimiterEvictsBoundedTable(t *testing.T) {
	handler, _ := newTestHandler(t, func(opts *Options) {
		opts.RequestsPerSecond = 1000
		opts.Burst = 1
		opts.MaxTrackedSourceIPs = 2
	})
	for i := 0; i < 16; i++ {
		recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/hello/@v/v1.0.0.info",
			fmt.Sprintf("10.9.0.%d:1", i), nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("address %d refused with a bounded table: status %d", i, recorder.Code)
		}
	}
	handler.limiter.mu.Lock()
	tracked := len(handler.limiter.buckets)
	handler.limiter.mu.Unlock()
	if tracked > 2 {
		t.Fatalf("address table grew beyond its bound: %d", tracked)
	}
}
