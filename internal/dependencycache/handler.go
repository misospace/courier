package dependencycache

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
	"golang.org/x/time/rate"
)

// Server bounds. A worker request completes the header read or loses the
// connection: the cache serves untrusted, hostile callers, so nothing may
// pin a connection, goroutine, or file descriptor for the pod's lifetime.
// MaxHeaderBytes keeps request headers tiny: the protocol has no use for
// caller-selected headers, and a caller that needs more than 8 KiB of them
// is abusing the listener.
const (
	ReadHeaderTimeout = 10 * time.Second
	ReadTimeout       = 60 * time.Second
	WriteTimeout      = 600 * time.Second // bounded artifact writes (the zip bound is 500 MiB)
	IdleTimeout       = 120 * time.Second
	MaxHeaderBytes    = 8192
)

// LogFunc receives the cache's minimal request log. The request key (module
// path and version) is worker-visible data and appears in these lines: this
// is the documented, bounded exfiltration channel (HARNESS.md §2) — the log
// is intentionally one small line and never carries request headers.
type LogFunc func(format string, args ...any)

// Options configure the worker-facing handler.
type Options struct {
	// Limits bound served artifact sizes. Zero uses DefaultLimits.
	// RequestsPerSecond and Burst bound each client IP's request rate;
	// RequestsPerSecond <= 0 disables rate limiting. (Artifact size bounds
	// live on the Store, not the handler.)
	RequestsPerSecond float64
	Burst             int
	// MaxTrackedSourceIPs bounds the rate-limiter's address table; a full
	// table evicts one tracked address rather than refusing new callers.
	MaxTrackedSourceIPs int
	// Log, when set, receives one line per request. LogRequests=false
	// restricts the log to denials and misses, minimizing the approved-key
	// exfiltration surface while keeping every refusal auditable.
	Log         LogFunc
	LogRequests bool
}

// Handler serves the read-only GOPROXY subset from a Store to untrusted
// workers. It applies the fixed response posture: GET/HEAD only, no
// redirects, no CONNECT or proxying, no authentication, no caller-selected
// headers, bounded artifacts, and per-IP rate limiting.
type Handler struct {
	store   *Store
	limiter *ipLimiter
	log     LogFunc
	logAll  bool
}

// NewHandler builds the worker-facing handler.
func NewHandler(store *Store, opts Options) *Handler {
	handler := &Handler{store: store, log: opts.Log, logAll: opts.LogRequests}
	if opts.RequestsPerSecond > 0 {
		burst := opts.Burst
		if burst <= 0 {
			burst = 1
		}
		maxIPs := opts.MaxTrackedSourceIPs
		if maxIPs <= 0 {
			maxIPs = 1024
		}
		handler.limiter = &ipLimiter{
			buckets: make(map[string]*rate.Limiter),
			rps:     rate.Limit(opts.RequestsPerSecond),
			burst:   burst,
			max:     maxIPs,
		}
	}
	return handler
}

func (h *Handler) logf(format string, args ...any) {
	if h.log != nil {
		h.log(format, args...)
	}
}

// ServeHTTP answers one worker request. Every response below 500 is derived
// only from the request line and the store's contents; request headers are
// never read for authorization, forwarding, or reflection.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	recorder := &responseRecorder{ResponseWriter: w}
	defer func() {
		if !h.logAll && recorder.status < http.StatusBadRequest {
			return
		}
		h.logf("request status=%d method=%s target=%q bytes=%d duration=%s remote=%q",
			recorder.status, r.Method, r.URL.RequestURI(), recorder.bytes,
			time.Since(start).Round(time.Microsecond), clientIP(r))
	}()

	// Method filter: the proxy protocol is GET (HEAD is the same lookup
	// without a body). CONNECT and every other method — including any
	// tunneling or upgrade attempt — is refused outright.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(recorder, "method not allowed: the dependency cache serves read-only GET requests", http.StatusMethodNotAllowed)
		return
	}

	// Absolute-form request targets (GET http://evil/…) are the classic
	// forward-proxy shape: refuse them before anything else, so the cache
	// can never be addressed as an arbitrary destination.
	if r.URL.Host != "" || r.URL.Scheme != "" {
		http.Error(recorder, "invalid request target: the dependency cache is not a proxy", http.StatusBadRequest)
		return
	}

	// The protocol carries no request body; a caller sending one is not a
	// go command.
	if r.ContentLength > 0 || len(r.TransferEncoding) > 0 {
		http.Error(recorder, "unexpected request body", http.StatusBadRequest)
		return
	}

	if r.URL.EscapedPath() == "/healthz" {
		recorder.Header().Set("Content-Type", "text/plain; charset=utf-8")
		recorder.Header().Set("X-Content-Type-Options", "nosniff")
		recorder.WriteHeader(http.StatusOK)
		_, _ = recorder.Write([]byte("ok\n"))
		return
	}

	// Per-IP request budget. X-Forwarded-For and every other forwarding
	// header are deliberately ignored: the network boundary, not caller
	// claims, identifies the source.
	if h.limiter != nil && !h.limiter.allow(clientIP(r)) {
		http.Error(recorder, "rate limit exceeded: retry later", http.StatusTooManyRequests)
		return
	}

	h.serve(recorder, r)
}

func (h *Handler) serve(w *responseRecorder, r *http.Request) {
	escaped := r.URL.EscapedPath()
	// The go command never percent-encodes proxy paths (module escaping
	// uses the !x form), so any percent-encoding is hostile input.
	if strings.Contains(escaped, "%") {
		http.Error(w, "invalid request path: percent-encoding is not accepted", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(escaped, "/") {
		http.Error(w, "invalid request path", http.StatusBadRequest)
		return
	}
	idx := strings.LastIndex(escaped, "/@v/")
	if idx <= 0 {
		// No separator at all, or an empty module part ("/@v/…").
		http.Error(w, "invalid request path: expected <module>/@v/<artifact>", http.StatusBadRequest)
		return
	}
	escapedModule, tail := escaped[1:idx], escaped[idx+len("/@v/"):]
	if escapedModule == "" || tail == "" {
		http.Error(w, "invalid request path: expected <module>/@v/<artifact>", http.StatusBadRequest)
		return
	}
	// Exactly one /@v/ separator can exist in a valid request: "@v" is not
	// a legal element of an escaped module path, but check anyway so a
	// crafted path can never shift the module/tail boundary.
	if strings.Contains(escapedModule, "/@v/") {
		http.Error(w, "invalid module path", http.StatusBadRequest)
		return
	}

	modulePath, err := module.UnescapePath(escapedModule)
	if err != nil || canonicalEscape(modulePath, module.EscapePath) != escapedModule {
		http.Error(w, "invalid module path: "+moduleErrText(modulePath, err), http.StatusBadRequest)
		return
	}

	switch {
	case tail == "list":
		versions, err := h.store.Versions(modulePath)
		if err != nil {
			h.reportMiss(w, modulePath, "", err)
			return
		}
		var body strings.Builder
		for _, version := range versions {
			body.WriteString(version.Version)
			if version.Time != "" {
				body.WriteString(" ")
				body.WriteString(version.Time)
			}
			body.WriteString("\n")
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body.String()))
	case tail == "latest":
		latest, err := h.store.Latest(modulePath)
		if err != nil {
			h.reportMiss(w, modulePath, "", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(latest.LatestJSON()))
	default:
		ext, escapedVersion, ok := splitArtifact(tail)
		if !ok {
			http.Error(w, "invalid request path: expected <module>/@v/<version>.info|.mod|.zip", http.StatusBadRequest)
			return
		}
		version, err := module.UnescapeVersion(escapedVersion)
		if err != nil || canonicalEscape(version, module.EscapeVersion) != escapedVersion || !semver.IsValid(version) {
			http.Error(w, "invalid version: "+versionErrText(escapedVersion, err), http.StatusBadRequest)
			return
		}
		file, _, err := h.store.Open(modulePath, version, ext)
		if err != nil {
			h.reportMiss(w, modulePath, version, err)
			return
		}
		defer file.Close()
		w.Header().Set("Content-Type", contentTypeFor(ext))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		// ServeContent streams the bounded file; it honors Range and HEAD
		// and never issues a redirect.
		http.ServeContent(w, r, escapedVersion+ext, time.Time{}, file)
	}
}

// reportMiss turns a refused lookup into the actionable approval diagnostic.
// The body is text/plain in UTF-8 because the go command surfaces proxy error
// bodies verbatim — this text is what the worker's build output shows.
func (h *Handler) reportMiss(w *responseRecorder, modulePath, version string, err error) {
	var notApproved *NotApprovedError
	if !errors.As(err, &notApproved) {
		// Store and population errors other than refusals are operator
		// incidents: log the detail, return a generic message that names
		// nothing about the host filesystem.
		h.logf("store error module=%q version=%q err=%v", modulePath, version, err)
		http.Error(w, "dependency cache storage error: contact the deployment administrator", http.StatusInternalServerError)
		return
	}
	target := modulePath
	if version != "" {
		target += "@" + version
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(w, "dependency cache: %q is not approved for this deployment (%s).\n"+
		"The cache never fetches on demand. Ask the deployment administrator to approve and\n"+
		"populate this module version in the dependency cache store (HARNESS.md §2: populate\n"+
		"with `go mod download` from a trusted machine and re-sync the store volume).\n",
		target, notApproved.Detail)
}

// canonicalEscape re-escapes a decoded value and returns the escaped form,
// so callers can require the wire form to be the canonical encoding.
func canonicalEscape(value string, escape func(string) (string, error)) string {
	escaped, err := escape(value)
	if err != nil {
		return "\x00unreachable-escape-error"
	}
	return escaped
}

func moduleErrText(path string, err error) string {
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("non-canonical escaped module path %q", path)
}

func versionErrText(version string, err error) string {
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("non-canonical escaped version %q", version)
}

// splitArtifact splits "<escaped-version><ext>" into its parts.
func splitArtifact(tail string) (ext, escapedVersion string, ok bool) {
	switch {
	case strings.HasSuffix(tail, ExtZip):
		return ExtZip, strings.TrimSuffix(tail, ExtZip), true
	case strings.HasSuffix(tail, ExtMod):
		return ExtMod, strings.TrimSuffix(tail, ExtMod), true
	case strings.HasSuffix(tail, ExtInfo):
		return ExtInfo, strings.TrimSuffix(tail, ExtInfo), true
	}
	return "", "", false
}

func contentTypeFor(ext string) string {
	switch ext {
	case ExtInfo:
		return "application/json"
	case ExtMod:
		return "text/plain; charset=utf-8"
	default:
		return "application/zip"
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ipLimiter is a bounded per-address token-bucket table. The table cannot
// grow without bound: when full it evicts one arbitrary tracked address, so
// a hostile swarm of source addresses churns the table instead of exhausting
// the cache's memory.
type ipLimiter struct {
	mu      sync.Mutex
	buckets map[string]*rate.Limiter
	rps     rate.Limit
	burst   int
	max     int
}

func (l *ipLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	bucket, ok := l.buckets[ip]
	if !ok {
		if len(l.buckets) >= l.max {
			for tracked := range l.buckets {
				delete(l.buckets, tracked)
				break
			}
		}
		bucket = rate.NewLimiter(l.rps, l.burst)
		l.buckets[ip] = bucket
	}
	return bucket.Allow()
}

// responseRecorder captures the status and byte count for the request log.
type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *responseRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}
