package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrBackendUnavailable reports that no operator read API is configured. It
// is a valid deployment state — a console built to probe only its own
// liveness — not a probe failure, and the pages render their plain
// not-configured state for it.
var ErrBackendUnavailable = errors.New("operator read API is not configured")

// ErrBackendTokenMissing reports that a projection route was requested while
// the console carries no read API bearer token. The unauthenticated health
// probe still works without one; every projection route is behind the token,
// so a data page must say the explorer is not configured rather than present
// a 401 the operator cannot act on.
var ErrBackendTokenMissing = errors.New("operator read API bearer token is not configured")

// ErrBackendUnexpectedResponse reports that the backend answered a projection
// request with a 200 the console could not read as a run list: an unparseable
// body, or a body whose kind is not the run-list kind. It exists so that a
// mis-routed or mis-proxied 200 never renders as an empty namespace, which is
// the same outage-masquerading-as-empty failure a 5xx would otherwise cause.
var ErrBackendUnexpectedResponse = errors.New("operator read API did not return a run list")

// BackendError reports a projection reply that carried a non-200 status. It
// carries only the status code, never the response body, so an operator-
// facing message can name the failure without echoing anything the backend
// (or, transitively, a run's source or model content) put in the body.
type BackendError struct {
	Status int
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("operator read API returned status %d", e.Status)
}

// backendAllowlist is the only backend path the console will ever GET, by
// construction and re-checked per request. The console never proxies
// arbitrary paths and ships no client-side JavaScript, so there is no other
// route from a browser to the backend. The projection path is fixed; only its
// query string varies, so a filter value can never redirect the request to a
// different path or host.
var backendAllowlist = map[string]bool{
	backendHealthPath: true,
	backendRunsPath:   true,
}

const (
	// backendHealthPath is the unauthenticated liveness probe.
	backendHealthPath = "/healthz"
	// backendRunsPath is the paginated run-list projection the explorer
	// reads. It mirrors readapi.PathRuns; the console never imports that
	// package, so this constant is the wire contract the two agree on.
	backendRunsPath = "/v1/runs"

	// maxRunListBody bounds how much of a projection response the console
	// will decode. The read API caps its own pages well below this; the
	// bound is a defensive ceiling so a hostile or confused upstream cannot
	// make the console buffer an unbounded body.
	maxRunListBody = 4 << 20

	// kindRunList is the envelope kind the run-list projection must carry.
	// It mirrors readapi.KindRunList; a 200 carrying any other kind is an
	// unexpected response, not an empty list. A test pins it against the
	// read API's constant so the two cannot drift.
	kindRunList = "RunSummaryList"
)

// Backend is the allowlisted read client toward the operator read API. It
// only ever GETs an allowlisted path on the configured base URL.
type Backend interface {
	// Health probes the backend's unauthenticated liveness path.
	// ErrBackendUnavailable means no backend is configured; any other error
	// is a probe failure.
	Health(ctx context.Context) error
	// Runs fetches one page of the run list. The filter is expected to be
	// already validated and bounded by the caller. ErrBackendUnavailable
	// means no backend; ErrBackendTokenMissing means no bearer token is
	// configured; a *BackendError carries a non-200 status; any other error
	// is a transport or decode failure. The returned result never carries
	// the bearer token or the raw response body.
	Runs(ctx context.Context, filter RunsFilter) (*RunListResult, error)
}

// RunsFilter is one validated run-list request. The console bounds and
// validates every field before it reaches the backend, so the backend never
// sees an unbounded or hostile query. Empty filter fields are omitted, not
// sent as empty parameters.
type RunsFilter struct {
	Lane     string
	Source   string
	Phase    string
	Page     int
	PageSize int
}

// RunRow is one run's explorer row, decoded from the read API's RunSummary.
// It is a decoder type, not a shared import: the console imports only the
// standard library and never links the read API package, so the response
// shape it depends on lives here and mirrors the read API's JSON contract.
// Only the bounded, allowlisted fields a list row needs are decoded; a field
// the read API does not project (source receipts, opaque identifiers) has no
// place to leak into. Timestamps are kept as raw strings — the row shows age,
// not absolute times — so no time-parsing dependency crosses into the console.
type RunRow struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
	Phase      string `json:"phase"`
	Terminal   bool   `json:"terminal"`
	AgeSeconds *int64 `json:"ageSeconds,omitempty"`
	Source     string `json:"source"`
	Lane       string `json:"lane"`
	PRNumber   *int   `json:"prNumber,omitempty"`
	Restarts   int    `json:"restarts"`
}

// RunListResult is one decoded run-list page, shaped for the template.
type RunListResult struct {
	Items     []RunRow
	Page      int
	PageSize  int
	Total     int
	More      bool
	Truncated bool
}

// decode types mirroring the read API envelope. Unknown fields are ignored,
// so a projection that grows a field never breaks the console; a field the
// console decodes must match the wire name exactly.
type (
	runListEnvelope struct {
		APIVersion string      `json:"apiVersion"`
		Kind       string      `json:"kind"`
		Payload    runListPage `json:"payload"`
	}
	runListPage struct {
		Items      []RunRow      `json:"items"`
		Pagination runPagination `json:"pagination"`
	}
	runPagination struct {
		Page      int  `json:"page"`
		PageSize  int  `json:"pageSize"`
		Total     int  `json:"total"`
		More      bool `json:"more"`
		Truncated bool `json:"truncated"`
	}
)

// HTTPBackend is the Backend implementation backed by the operator read API.
type HTTPBackend struct {
	baseURL string
	token   []byte
	client  *http.Client
}

// NewBackend builds a backend client for the given base URL and bearer token.
// An empty base URL yields a backend that always reports ErrBackendUnavailable
// and never makes a request. A nil/empty token leaves the unauthenticated
// health probe working and makes every projection request fail with
// ErrBackendTokenMissing.
func NewBackend(baseURL string, token []byte, timeout time.Duration) Backend {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTPBackend{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+backendHealthPath, nil)
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

// Runs GETs the allowlisted run-list projection with the bearer token and the
// validated filter's query parameters. Only the query string varies; the path
// is the constant allowlist entry, so a filter value never steers the request
// to another path or host. A non-200 reply becomes a *BackendError carrying
// only the status; the body is discarded up to a small bound and never
// reaches the browser.
func (b *HTTPBackend) Runs(ctx context.Context, filter RunsFilter) (*RunListResult, error) {
	if b.baseURL == "" {
		return nil, ErrBackendUnavailable
	}
	if len(b.token) == 0 {
		return nil, ErrBackendTokenMissing
	}
	if !backendAllowlist[backendRunsPath] {
		return nil, fmt.Errorf("backend path %q is not allowlisted", backendRunsPath)
	}
	target, err := url.Parse(b.baseURL + backendRunsPath)
	if err != nil {
		return nil, fmt.Errorf("backend runs url: %w", err)
	}
	query := target.Query()
	if filter.Lane != "" {
		query.Set("lane", filter.Lane)
	}
	if filter.Source != "" {
		query.Set("source", filter.Source)
	}
	if filter.Phase != "" {
		query.Set("phase", filter.Phase)
	}
	if filter.Page > 0 {
		query.Set("page", strconv.Itoa(filter.Page))
	}
	if filter.PageSize > 0 {
		query.Set("pageSize", strconv.Itoa(filter.PageSize))
	}
	target.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("backend runs request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+string(b.token))

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("backend runs request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Drain a bounded amount so the connection can be reused; the body
		// never crosses into a response or a log.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, &BackendError{Status: resp.StatusCode}
	}

	var env runListEnvelope
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxRunListBody))
	if err := dec.Decode(&env); err != nil {
		// A 200 whose body is not valid projection JSON is an unexpected
		// response, never an empty list; the body itself is never surfaced.
		return nil, fmt.Errorf("%w: %v", ErrBackendUnexpectedResponse, err)
	}
	if env.Kind != kindRunList {
		// The read API always stamps the run-list kind, even on an empty
		// page. Any other kind, including empty (a bare JSON error body or
		// null), is an unexpected response, never an empty list.
		return nil, fmt.Errorf("%w: got kind %q", ErrBackendUnexpectedResponse, env.Kind)
	}
	return &RunListResult{
		Items:     env.Payload.Items,
		Page:      env.Payload.Pagination.Page,
		PageSize:  env.Payload.Pagination.PageSize,
		Total:     env.Payload.Pagination.Total,
		More:      env.Payload.Pagination.More,
		Truncated: env.Payload.Pagination.Truncated,
	}, nil
}
