package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrBackendUnavailable reports that no operator read API is configured. It
// is the expected scaffold state, not a probe failure, and the pages render
// their plain empty state for it.
var ErrBackendUnavailable = errors.New("operator read API is not configured")

// backendAllowlist is the only backend path the console will ever GET, by
// construction and re-checked per request. The console never proxies
// arbitrary paths and ships no client-side JavaScript, so there is no other
// route from a browser to the backend.
var backendAllowlist = map[string]bool{"/healthz": true}

// backendHealthPath is today's sole allowlist entry.
const backendHealthPath = "/healthz"

// Backend is the allowlisted read groundwork toward the (future) operator
// read API.
type Backend interface {
	// Health probes the backend. ErrBackendUnavailable means no backend is
	// configured; any other error is a probe failure.
	Health(ctx context.Context) error
}

// HTTPBackend is the Backend implementation backed by the operator read API.
type HTTPBackend struct {
	baseURL string
	client  *http.Client
}

// NewBackend builds a backend probe for the given base URL. An empty base
// URL yields a backend that always reports ErrBackendUnavailable.
func NewBackend(baseURL string, timeout time.Duration) Backend {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTPBackend{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: timeout},
	}
}

// Health GETs the allowlisted health path on the backend base URL. The path
// is re-checked against the allowlist before the request leaves, so the
// console can never be talked into an open proxy by a future code path
// threading a path through here.
func (b *HTTPBackend) Health(ctx context.Context) error {
	if b.baseURL == "" {
		return ErrBackendUnavailable
	}
	if !backendAllowlist[backendHealthPath] {
		return fmt.Errorf("backend path %q is not allowlisted", backendHealthPath)
	}
	target, err := url.Parse(b.baseURL + backendHealthPath)
	if err != nil {
		return fmt.Errorf("backend url: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return fmt.Errorf("backend health request: %w", err)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("backend health check: %w", err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)); err != nil {
		return fmt.Errorf("backend health check: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backend health check returned status %d", resp.StatusCode)
	}
	return nil
}
