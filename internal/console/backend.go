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
// request with a 200 the console could not read as the expected projection:
// an unparseable body, or a body whose kind is not the kind the route must
// carry. It exists so that a mis-routed or mis-proxied 200 never renders as
// an empty namespace, which is the same outage-masquerading-as-empty failure
// a 5xx would otherwise cause.
var ErrBackendUnexpectedResponse = errors.New("operator read API returned an unexpected projection")

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
	backendHealthPath:   true,
	backendRunsPath:     true,
	backendOverviewPath: true,
	backendLanesPath:    true,
	backendSourcesPath:  true,
}

const (
	// backendHealthPath is the unauthenticated liveness probe.
	backendHealthPath = "/healthz"
	// backendRunsPath is the paginated run-list projection the explorer
	// reads. It mirrors readapi.PathRuns; the console never imports that
	// package, so this constant is the wire contract the two agree on.
	backendRunsPath = "/v1/runs"
	// backendRunDetailPrefix prefixes a validated single-run name. It mirrors
	// readapi.PathRuns + "/"; the name validation and path allowlist prevent
	// a requested name from steering the backend request.
	backendRunDetailPrefix = "/v1/runs/"
	// backendOverviewPath is the single-namespace summary the overview page
	// reads. It mirrors readapi.PathOverview.
	backendOverviewPath = "/v1/overview"
	// backendLanesPath is the per-lane health list the overview page reads.
	// It mirrors readapi.PathLanes.
	backendLanesPath = "/v1/lanes"
	// backendSourcesPath is the per-source health list the overview page
	// reads. It mirrors readapi.PathSources.
	backendSourcesPath = "/v1/sources"

	// maxProjectionBody bounds how much of a projection response the console
	// will decode. The read API caps its own pages well below this; the
	// bound is a defensive ceiling so a hostile or confused upstream cannot
	// make the console buffer an unbounded body.
	maxProjectionBody = 4 << 20

	// kindRunList is the envelope kind the run-list projection must carry.
	// It mirrors readapi.KindRunList; a 200 carrying any other kind is an
	// unexpected response, not an empty list. A test pins it against the
	// read API's constant so the two cannot drift.
	kindRunList = "RunSummaryList"
	// kindRunDetail mirrors readapi.KindRunDetail.
	kindRunDetail = "RunDetail"
	// kindOverview mirrors readapi.KindOverview.
	kindOverview = "Overview"
	// kindLaneList mirrors readapi.KindLaneList.
	kindLaneList = "LaneHealthList"
	// kindSourceList mirrors readapi.KindSourceList.
	kindSourceList = "SourceHealthList"
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
	// Run fetches one validated run detail. The same error contract as Runs
	// applies; an invalid DNS-subdomain name is reported as a 404 without a
	// backend request.
	Run(ctx context.Context, name string) (*RunDetailResult, error)
	// Overview fetches the single-namespace summary. The same error contract
	// as Runs applies.
	Overview(ctx context.Context) (*OverviewResult, error)
	// Lanes fetches the per-lane health list. The same error contract as
	// Runs applies. Truncated reports that the run scan behind the health
	// counts stopped at the read API's fetch cap, so the counts describe a
	// bounded window, not the whole namespace.
	Lanes(ctx context.Context) (*LaneListResult, error)
	// Sources fetches the per-source health list. The same error contract as
	// Runs applies, and Truncated carries the same window caveat as Lanes.
	Sources(ctx context.Context) (*SourceListResult, error)
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

// RunDetailResult is the template-facing view of one decoded RunDetail payload.
type RunDetailResult struct {
	Name           string `json:"name"`
	Namespace      string `json:"namespace"`
	Phase          string `json:"phase"`
	Terminal       bool   `json:"terminal"`
	AgeSeconds     *int64 `json:"ageSeconds"`
	CreatedAt      string `json:"createdAt"`
	PhaseEnteredAt string `json:"phaseEnteredAt"`
	Source         string `json:"source"`
	Lane           string `json:"lane"`
	PRNumber       *int   `json:"prNumber"`
	Branch         string `json:"branch"`
	HeadSHA        string `json:"headSHA"`
	Restarts       int    `json:"restarts"`
	TerminalReason string `json:"terminalReason"`
	Heartbeat      struct {
		HasHeartbeat    bool   `json:"hasHeartbeat"`
		LastHeartbeatAt string `json:"lastHeartbeatAt"`
		Freshness       string `json:"freshness"`
	}
	Admission struct {
		AdmittedAt   string `json:"admittedAt"`
		Disagreement bool   `json:"disagreement"`
		Note         string `json:"note"`
	}
	Checkpoint struct {
		Present    bool `json:"present"`
		BriefCount int  `json:"briefCount"`
	}
	Publication struct {
		LastCommit                string `json:"lastCommit"`
		HasCheckFingerprint       bool   `json:"hasCheckFingerprint"`
		HasFailedCheckFingerprint bool   `json:"hasFailedCheckFingerprint"`
		DeclaredNoChange          bool   `json:"declaredNoChange"`
	}
	CIPark *struct {
		Head        string `json:"head"`
		ParkedAt    string `json:"parkedAt"`
		Attempts    int    `json:"attempts"`
		LastOutcome string `json:"lastOutcome"`
	}
	LifecycleReport struct {
		ConditionPresent    bool   `json:"conditionPresent"`
		ConditionStatus     string `json:"conditionStatus"`
		ObservedAt          string `json:"observedAt"`
		HasActiveRetryToken bool   `json:"hasActiveRetryToken"`
		HasLastRetryToken   bool   `json:"hasLastRetryToken"`
	}
	Coordinator *struct {
		Category   string `json:"category"`
		ExitCode   *int32 `json:"exitCode"`
		Outcome    string `json:"outcome"`
		Reason     string `json:"reason"`
		ObservedAt string `json:"observedAt"`
		Freshness  string `json:"freshness"`
		Container  *struct {
			Name              string `json:"name"`
			RestartCount      int32  `json:"restartCount"`
			TerminationReason string `json:"terminationReason"`
		} `json:"container"`
	}
	Reap struct {
		MarkerAnnotation string   `json:"markerAnnotation"`
		MarkerAt         string   `json:"markerAt"`
		Reason           string   `json:"reason"`
		EligibleByPhase  bool     `json:"eligibleByPhase"`
		Holds            []string `json:"holds"`
	}
	WorkspaceSafety *struct {
		State  string `json:"state"`
		Branch string `json:"branch"`
		Head   string `json:"head"`
	}
	NativeResultKind string `json:"nativeResultKind"`
	NativeResultAt   string `json:"nativeResultAt"`
	Conditions       []struct {
		Type       string `json:"type"`
		Status     string `json:"status"`
		ObservedAt string `json:"observedAt"`
	} `json:"conditions"`
	Timeline []struct {
		At     string `json:"at"`
		Label  string `json:"label"`
		Detail string `json:"detail"`
	} `json:"timeline"`
	TimelineTruncated bool `json:"timelineTruncated"`
}

// OverviewResult is the decoded GET /v1/overview payload. Freshness,
// Truncated, and the per-state source counts keep the read API's explicit
// state vocabulary: the page renders each state by name, so a zero count is
// never read as "no problem" for a state the projection did not classify.
// Timestamps stay raw strings; the page shows the state words, not derived
// clock arithmetic.
type OverviewResult struct {
	Namespace      string
	Freshness      string
	Truncated      bool
	RunsTotal      int
	RunsByPhase    map[string]int
	LaneCount      int
	LanesSaturated bool
	LanesSuspended bool
	LanesCooldown  bool
	LanesUnhealthy int
	SourceCount    int
	SourcesHealthy int
	SourcesEmpty   int
	SourcesStale   int
	SourcesUnavail int
	SourcesBlocked int
	ConditionRuns  map[string]int
}

// LaneListResult is the decoded GET /v1/lanes payload. Truncated reports that
// the bounded run scan behind the counts stopped at the read API's fetch cap:
// the counts then describe a window, not the whole namespace, and the page
// says so instead of presenting a partial list as complete.
type LaneListResult struct {
	Items     []LaneRow
	Truncated bool
}

// SourceListResult is the decoded GET /v1/sources payload, with the same
// truncation contract as LaneListResult.
type SourceListResult struct {
	Items     []SourceRow
	Truncated bool
}

// LaneRow is one lane's decoded health from GET /v1/lanes. Admitted counts
// runs holding an execution-capacity slot; Reserved counts runs holding a
// discovery reservation and is a superset of Admitted, so the page renders
// them as two labeled counts and never sums them. PendingOldestSeconds is nil
// when no Pending run exists: absence renders as an em dash, never a zero.
type LaneRow struct {
	Name                  string       `json:"name"`
	Availability          string       `json:"availability"`
	Freshness             string       `json:"freshness"`
	Notes                 []string     `json:"notes,omitempty"`
	ConfiguredConcurrency int          `json:"configuredConcurrency"`
	Admitted              int          `json:"admitted"`
	Reserved              int          `json:"reserved"`
	Saturated             bool         `json:"saturated"`
	Suspended             bool         `json:"suspended"`
	Cooldown              *CooldownRow `json:"cooldown,omitempty"`
	PendingOldestSeconds  *int64       `json:"pendingOldestSeconds,omitempty"`
}

// CooldownRow is a lane's active capacity cooldown. NextEligibleAt is the
// raw projected timestamp string; the page shows it verbatim rather than
// deriving a countdown the console's clock could be wrong about.
type CooldownRow struct {
	CapacityID      string `json:"capacityID"`
	Reason          string `json:"reason"`
	ExhaustionCount int    `json:"exhaustionCount"`
	NextEligibleAt  string `json:"nextEligibleAt,omitempty"`
}

// SourceRow is one source's decoded health from GET /v1/sources. ObservedAt,
// LastSuccessAt, and LastErrorAt are raw projected timestamp strings, empty
// when the source carries no run activity or no recorded success or failure;
// the page pairs ObservedAt with the freshness word and the outcome
// timestamps with explicit "no … recorded" wording, so absence is never read
// as "now", as a zero, or as proof the source never failed.
type SourceRow struct {
	Name                 string   `json:"name"`
	Availability         string   `json:"availability"`
	Freshness            string   `json:"freshness"`
	Notes                []string `json:"notes,omitempty"`
	ObservedAt           string   `json:"observedAt,omitempty"`
	LastSuccessAt        string   `json:"lastSuccessAt,omitempty"`
	LastErrorAt          string   `json:"lastErrorAt,omitempty"`
	TotalRuns            int      `json:"totalRuns"`
	OldestPendingSeconds *int64   `json:"oldestPendingSeconds,omitempty"`
}

// decode types mirroring the read API envelope. Unknown fields are ignored,
// so a projection that grows a field never breaks the console; a field the
// console decodes must match the wire name exactly.
type (
	// projectionEnvelope is the shared envelope every projection route
	// carries. Payload is kept raw so the kind can be checked before any of
	// the payload is interpreted: a 200 of the wrong kind is an unexpected
	// response, never a partially decoded surprise.
	projectionEnvelope struct {
		APIVersion string          `json:"apiVersion"`
		Kind       string          `json:"kind"`
		Payload    json.RawMessage `json:"payload"`
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
	overviewPage struct {
		Namespace string `json:"namespace"`
		Freshness string `json:"freshness"`
		Truncated bool   `json:"truncated"`
		Runs      struct {
			Total   int            `json:"total"`
			ByPhase map[string]int `json:"byPhase"`
		} `json:"runs"`
		Lanes struct {
			Count     int  `json:"count"`
			Saturated bool `json:"saturated"`
			Suspended bool `json:"suspended"`
			Cooldown  bool `json:"cooldown"`
			Unhealthy int  `json:"unhealthy"`
		} `json:"lanes"`
		Sources struct {
			Count       int `json:"count"`
			Healthy     int `json:"healthy"`
			Empty       int `json:"empty"`
			Stale       int `json:"stale"`
			Unavailable int `json:"unavailable"`
			Blocked     int `json:"blocked"`
		} `json:"sources"`
		ConditionRuns map[string]int `json:"conditionRuns"`
	}
	laneListPage struct {
		Items      []LaneRow `json:"items"`
		Pagination struct {
			Truncated bool `json:"truncated"`
		} `json:"pagination"`
	}
	sourceListPage struct {
		Items      []SourceRow `json:"items"`
		Pagination struct {
			Truncated bool `json:"truncated"`
		} `json:"pagination"`
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
	if !allowlistedBackendPath(backendHealthPath) {
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

// projection GETs an allowlisted projection path with the bearer token,
// checks the envelope kind, and decodes the payload into out. Only rawQuery
// varies per call; the path is a constant the caller passes and is re-checked
// against the allowlist before the request leaves, so no call site can steer
// the console to another path or host. A non-200 reply becomes a
// *BackendError carrying only the status; the body is discarded up to a small
// bound and never reaches the browser. A 200 whose body is not valid JSON, or
// whose kind is not wantKind, is ErrBackendUnexpectedResponse: an outage
// signature, never an empty result.
func allowlistedBackendPath(path string) bool {
	if backendAllowlist[path] {
		return true
	}
	if !strings.HasPrefix(path, backendRunDetailPrefix) {
		return false
	}
	return validRunName(strings.TrimPrefix(path, backendRunDetailPrefix))
}

func (b *HTTPBackend) projection(ctx context.Context, path, rawQuery, wantKind string, out any) error {
	if b.baseURL == "" {
		return ErrBackendUnavailable
	}
	if len(b.token) == 0 {
		return ErrBackendTokenMissing
	}
	if !allowlistedBackendPath(path) {
		return fmt.Errorf("backend path %q is not allowlisted", path)
	}
	target, err := url.Parse(b.baseURL + path)
	if err != nil {
		return fmt.Errorf("backend %s url: %w", wantKind, err)
	}
	target.RawQuery = rawQuery

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return fmt.Errorf("backend %s request: %w", wantKind, err)
	}
	req.Header.Set("Authorization", "Bearer "+string(b.token))

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("backend %s request: %w", wantKind, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Drain a bounded amount so the connection can be reused; the body
		// never crosses into a response or a log.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return &BackendError{Status: resp.StatusCode}
	}

	var env projectionEnvelope
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxProjectionBody))
	if err := dec.Decode(&env); err != nil {
		// A 200 whose body is not valid projection JSON is an unexpected
		// response, never an empty list; the body itself is never surfaced.
		return fmt.Errorf("%w: %v", ErrBackendUnexpectedResponse, err)
	}
	if env.Kind != wantKind {
		// The read API always stamps the envelope kind, even on an empty
		// page. Any other kind, including empty (a bare JSON error body or
		// null), is an unexpected response, never an empty list.
		return fmt.Errorf("%w: got kind %q", ErrBackendUnexpectedResponse, env.Kind)
	}
	// A correct kind with a null or missing payload is also an unexpected
	// response: the read API always sends an object payload, and decoding
	// null over a fresh value would silently render an outage as an empty
	// namespace.
	if len(env.Payload) == 0 || string(env.Payload) == "null" {
		return fmt.Errorf("%w: got a null payload", ErrBackendUnexpectedResponse)
	}
	if err := json.Unmarshal(env.Payload, out); err != nil {
		return fmt.Errorf("%w: %v", ErrBackendUnexpectedResponse, err)
	}
	return nil
}

// validRunName mirrors DNS1123-subdomain validation without importing Kubernetes.
// Names are at most 253 bytes, use lowercase alphanumerics, '-' and '.', and
// each non-empty label starts and ends alphanumeric, so a validated name cannot
// add a path segment or escape the composed detail endpoint.
func validRunName(name string) bool {
	if len(name) == 0 || len(name) > maxExplorerFilterLen {
		return false
	}
	labelLength := 0
	var previous byte
	for i := 0; i < len(name); i++ {
		c := name[i]
		alnum := c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
		if alnum {
			labelLength++
			previous = c
			continue
		}
		if c == '.' {
			if labelLength == 0 || labelLength > 63 || previous == '-' {
				return false
			}
			labelLength = 0
			previous = c
			continue
		}
		if c == '-' {
			if labelLength == 0 || previous == '.' || labelLength >= 63 {
				return false
			}
			labelLength++
			previous = c
			continue
		}
		return false
	}
	return labelLength > 0 && labelLength <= 63 && previous != '-'
}

// Runs GETs the allowlisted run-list projection with the validated filter's
// query parameters. Only the query string varies; the path is the constant
// allowlist entry, so a filter value never steers the request to another path
// or host.
func (b *HTTPBackend) Runs(ctx context.Context, filter RunsFilter) (*RunListResult, error) {
	query := url.Values{}
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
	var page runListPage
	if err := b.projection(ctx, backendRunsPath, query.Encode(), kindRunList, &page); err != nil {
		return nil, err
	}
	return &RunListResult{
		Items:     page.Items,
		Page:      page.Pagination.Page,
		PageSize:  page.Pagination.PageSize,
		Total:     page.Pagination.Total,
		More:      page.Pagination.More,
		Truncated: page.Pagination.Truncated,
	}, nil
}

// Overview GETs the allowlisted namespace summary. The projection carries no
// query state: the read API serves exactly the namespace it is configured
// for, so nothing from a browser can shape the request.
// Run fetches one run detail. Invalid names map to the read API's not-found
// state locally; syntactically invalid names receive 404 there as well.
func (b *HTTPBackend) Run(ctx context.Context, name string) (*RunDetailResult, error) {
	if b.baseURL == "" {
		return nil, ErrBackendUnavailable
	}
	if len(b.token) == 0 {
		return nil, ErrBackendTokenMissing
	}
	if !validRunName(name) {
		return nil, &BackendError{Status: http.StatusNotFound}
	}
	var detail RunDetailResult
	if err := b.projection(ctx, backendRunDetailPrefix+name, "", kindRunDetail, &detail); err != nil {
		return nil, err
	}
	return &detail, nil
}

func (b *HTTPBackend) Overview(ctx context.Context) (*OverviewResult, error) {
	var page overviewPage
	if err := b.projection(ctx, backendOverviewPath, "", kindOverview, &page); err != nil {
		return nil, err
	}
	return &OverviewResult{
		Namespace:      page.Namespace,
		Freshness:      page.Freshness,
		Truncated:      page.Truncated,
		RunsTotal:      page.Runs.Total,
		RunsByPhase:    page.Runs.ByPhase,
		LaneCount:      page.Lanes.Count,
		LanesSaturated: page.Lanes.Saturated,
		LanesSuspended: page.Lanes.Suspended,
		LanesCooldown:  page.Lanes.Cooldown,
		LanesUnhealthy: page.Lanes.Unhealthy,
		SourceCount:    page.Sources.Count,
		SourcesHealthy: page.Sources.Healthy,
		SourcesEmpty:   page.Sources.Empty,
		SourcesStale:   page.Sources.Stale,
		SourcesUnavail: page.Sources.Unavailable,
		SourcesBlocked: page.Sources.Blocked,
		ConditionRuns:  page.ConditionRuns,
	}, nil
}

// Lanes GETs the allowlisted per-lane health list.
func (b *HTTPBackend) Lanes(ctx context.Context) (*LaneListResult, error) {
	var page laneListPage
	if err := b.projection(ctx, backendLanesPath, "", kindLaneList, &page); err != nil {
		return nil, err
	}
	return &LaneListResult{Items: page.Items, Truncated: page.Pagination.Truncated}, nil
}

// Sources GETs the allowlisted per-source health list.
func (b *HTTPBackend) Sources(ctx context.Context) (*SourceListResult, error) {
	var page sourceListPage
	if err := b.projection(ctx, backendSourcesPath, "", kindSourceList, &page); err != nil {
		return nil, err
	}
	return &SourceListResult{Items: page.Items, Truncated: page.Pagination.Truncated}, nil
}
