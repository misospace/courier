package readapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The routes the read API serves. The /v1 routes are the projections, all
// behind the bearer token; the run-detail route is a ServeMux pattern whose
// {name} segment is read with r.PathValue. /healthz is unauthenticated
// liveness for probes that carry no credentials (the operator console's
// backend probe): it serves no data, so it must never sit behind the token.
const (
	PathHealthz  = "/healthz"
	PathOverview = "/v1/overview"
	PathLanes    = "/v1/lanes"
	PathSources  = "/v1/sources"
	PathRuns     = "/v1/runs"
	PathRun      = "/v1/runs/{name}"
)

const (
	// MaxPageSize is the largest page size a client may request. A larger
	// pageSize is a client bug, not a clamp target: the request is rejected
	// so a caller cannot mistake a silently rewritten page for the one it
	// asked for.
	MaxPageSize = 200

	// ListPageSize is the per-round-trip page size of a server-side
	// namespace-scoped List pass: how many objects one API round trip
	// asks for at most. It is larger than MaxPageSize because it bounds
	// fetch traffic, not the response window: the same hard cap is
	// reached in fewer round trips.
	ListPageSize = 500

	// MaxListItems caps how many objects one bounded list pass fetches
	// across all of its pages. A namespace holding more than this gets an
	// explicitly truncated window (Pagination.Truncated,
	// Overview.Truncated), never an unbounded response.
	MaxListItems = 10 * MaxPageSize

	// MaxListRoundTrips caps the round trips of one list pass independently
	// of the item cap: the API server may hand back short or empty pages
	// with a live continuation token, so items alone do not bound the
	// traffic. The bound is twice the minimum number of full pages to the
	// item cap, so legitimately short pages still make progress while a
	// pathological walk is cut off and reported as truncated.
	MaxListRoundTrips = 2 * (MaxListItems / ListPageSize)

	// MaxConcurrentReads is the process-wide budget for concurrent cluster
	// reads — scan passes and named-object gets alike, matching the #836
	// status-read precedent that budgets even single-object reads. A full
	// budget sheds work with an immediate 503 instead of queueing
	// unbounded readers behind a saturated API server.
	MaxConcurrentReads = 4

	// requestTimeout bounds one request's cluster round trips. A stalled
	// API server that accepts the connection but never answers maps to a
	// 503 deadline instead of holding the connection open forever.
	requestTimeout = 30 * time.Second

	unauthorizedMessage = "the read API requires a valid bearer token"
	internalMessage     = "the read API failed to project the requested state"
	busyMessage         = "the read API is at its concurrent-read budget; retry shortly"
	timeoutMessage      = "the read API timed out before the cluster answered"
)

// ServerConfig is the construction input for a Server. Token, Bind, and
// Namespace are required when the server is enabled; the constructor fails
// closed on a missing one so a wiring mistake surfaces at startup, never as
// a listener that authenticates nothing or answers across namespaces.
type ServerConfig struct {
	// Reader is the read-only cluster access the projections list through.
	// Only namespace-scoped paginated List calls and name-scoped Get calls
	// are ever made. Deployments must wire a direct (uncached) API reader
	// so the per-page Limit and the continuation walk are enforced by the
	// API server rather than absorbed by an informer cache. Required.
	Reader ctrlclient.Reader
	// Token is the static bearer token clients present. Required; it is
	// compared in constant time and never logged or echoed.
	Token string
	// Bind is the listener address (e.g. ":8084"). Empty disables the
	// server entirely: the constructor returns a no-op that never listens.
	Bind string
	// Namespace is the single namespace the read model serves runs from.
	// Required. LaneProfiles are read from this namespace too, because
	// the operator resolves a run's lane profile in the run's own namespace
	// and a profile from anywhere else governs nothing.
	Namespace string
}

// Server serves the read-only HTTP projection API over CoderRun and
// LaneProfile objects. It is a thin transport layer: authentication, query
// parsing, bounding, and error mapping happen here; every state judgment is
// the projection layer's. Namespace scans are bounded every way they can
// be: each List round trip asks for at most ListPageSize items and follows
// the continuation token, one pass fetches at most MaxListItems objects
// over at most MaxListRoundTrips round trips and says so explicitly when
// it stops early, and at most MaxConcurrentReads cluster reads — scan
// passes and named-object gets alike — run at a time; a saturated budget
// is a 503, never a queue. A named run detail is a single Get, never a
// list. Each request reads through the configured reader with no cache of
// its own; the manager wires the direct API reader, so reads are live.
// Runs and LaneProfiles are both read from the served namespace: the
// operator resolves a run's lane profile in the run's own namespace, so a
// profile elsewhere governs nothing.
type Server struct {
	reader    ctrlclient.Reader
	token     string
	bind      string
	namespace string
	readSlots chan struct{}
	disabled  bool
}

// NewServer builds the server. An empty Bind disables it: the constructor
// returns a no-op that never listens, so a deployment can leave the read
// API off without a wiring branch at the call site. When enabled, a missing
// reader, token, or namespace is a construction error.
func NewServer(config ServerConfig) (*Server, error) {
	if strings.TrimSpace(config.Bind) == "" {
		return &Server{disabled: true}, nil
	}
	switch {
	case config.Reader == nil:
		return nil, errors.New("readapi: server requires a reader")
	case config.Token == "":
		return nil, errors.New("readapi: server requires a bearer token")
	case strings.TrimSpace(config.Namespace) == "":
		return nil, errors.New("readapi: server requires a namespace")
	}
	return &Server{
		reader:    config.Reader,
		token:     config.Token,
		bind:      strings.TrimSpace(config.Bind),
		namespace: strings.TrimSpace(config.Namespace),
		readSlots: make(chan struct{}, MaxConcurrentReads),
	}, nil
}

// NeedLeaderElection reports that the server must run on every replica.
// Like the admission and intake listeners, it is a network endpoint, not a
// reconciler: every replica can answer read-only projections.
func (s *Server) NeedLeaderElection() bool { return false }

// Start binds the listener and serves the read API until the context is
// done, then shuts down gracefully. The disabled server never listens. The
// shape mirrors the admission server's Start.
func (s *Server) Start(ctx context.Context) error {
	if s.disabled {
		return nil
	}
	ln, err := net.Listen("tcp", s.bind)
	if err != nil {
		return fmt.Errorf("readapi: server listening on %s: %w", s.bind, err)
	}
	srv := &http.Server{
		// Requests are GET-only with no body, so the read-side timeouts are
		// generous but finite; the write timeout exceeds the per-request
		// deadline by a margin so a deadline-path response is still written.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      requestTimeout + 10*time.Second,
		IdleTimeout:       120 * time.Second,
		Handler:           s.Handler(),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
		return nil
	case err := <-errCh:
		_ = ln.Close()
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

// Handler returns the server's HTTP handler. Authentication wraps the
// projection routes, so it answers before any routing: an unauthenticated
// caller learns nothing about which routes exist. One constant message
// covers missing, malformed, and wrong tokens alike. /healthz sits outside
// the token: it is liveness for credential-less probes and serves no data.
func (s *Server) Handler() http.Handler {
	projections := http.NewServeMux()
	projections.HandleFunc("GET "+PathOverview, s.handleOverview)
	projections.HandleFunc("GET "+PathLanes, s.handleLanes)
	projections.HandleFunc("GET "+PathSources, s.handleSources)
	projections.HandleFunc("GET "+PathRuns, s.handleRuns)
	projections.HandleFunc("GET "+PathRun, s.handleRun)
	authed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) != 1 {
			writeError(w, http.StatusUnauthorized, unauthorizedMessage)
			return
		}
		projections.ServeHTTP(w, r)
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+PathHealthz, s.handleHealthz)
	mux.Handle("/", authed)
	return mux
}

// handleHealthz serves GET /healthz: unauthenticated liveness. It carries no
// state — not the served namespace, not counts, not the token's presence —
// so a probe without credentials learns only that the listener is up.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleOverview serves GET /v1/overview: the single-namespace snapshot.
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if !s.acquireReadSlot() {
		writeBusy(w)
		return
	}
	defer s.releaseReadSlot()
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, runsTruncated, err := s.listRuns(ctx)
	if err != nil {
		writeClusterError(w, ctx, err)
		return
	}
	lanes, lanesTruncated, err := s.listLanes(ctx)
	if err != nil {
		writeClusterError(w, ctx, err)
		return
	}
	overview := ProjectOverview(s.namespace, runs, lanes, s.options())
	overview.Truncated = runsTruncated || lanesTruncated
	writeJSON(w, http.StatusOK, NewEnvelope(KindOverview, overview))
}

// handleLanes serves GET /v1/lanes: every configured lane's health. Lanes
// are a small configuration set, so the list is served in one unpaginated
// page; the run scan behind the health counts is the same bounded pass the
// run list uses, and a truncated scan is stated in the page. Per-source
// health is its own route (GET /v1/sources), not part of this payload.
func (s *Server) handleLanes(w http.ResponseWriter, r *http.Request) {
	if !s.acquireReadSlot() {
		writeBusy(w)
		return
	}
	defer s.releaseReadSlot()
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, runsTruncated, err := s.listRuns(ctx)
	if err != nil {
		writeClusterError(w, ctx, err)
		return
	}
	lanes, lanesTruncated, err := s.listLanes(ctx)
	if err != nil {
		writeClusterError(w, ctx, err)
		return
	}
	healths := ProjectLaneHealths(lanes, runs, s.options())
	list := LaneHealthList{
		Items: healths,
		Pagination: Pagination{
			Page:      0,
			PageSize:  len(healths),
			Total:     len(healths),
			Truncated: runsTruncated || lanesTruncated,
		},
	}
	writeJSON(w, http.StatusOK, NewEnvelope(KindLaneList, list))
}

// handleSources serves GET /v1/sources: one health entry per source visible
// in the served namespace's runs. Sources are a small, name-keyed set, so the
// list is served in one unpaginated page; the run scan behind it is the same
// bounded pass the run list uses, and a truncated scan is stated in the page.
func (s *Server) handleSources(w http.ResponseWriter, r *http.Request) {
	if !s.acquireReadSlot() {
		writeBusy(w)
		return
	}
	defer s.releaseReadSlot()
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, truncated, err := s.listRuns(ctx)
	if err != nil {
		writeClusterError(w, ctx, err)
		return
	}
	healths := ProjectSourceHealths(runs, nil, s.options())
	list := SourceHealthList{
		Items: healths,
		Pagination: Pagination{
			Page:      0,
			PageSize:  len(healths),
			Total:     len(healths),
			Truncated: truncated,
		},
	}
	writeJSON(w, http.StatusOK, NewEnvelope(KindSourceList, list))
}

// handleRuns serves GET /v1/runs: the paginated run list with exact-match
// lane/source/phase filters, sorted newest-first. The namespace scan is the
// bounded pass in listRuns; filters apply to the fetched window, so Total
// and More describe that window, and a capped scan is stated as truncated.
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	query, err := parseRunsQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.acquireReadSlot() {
		writeBusy(w)
		return
	}
	defer s.releaseReadSlot()
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, truncated, err := s.listRuns(ctx)
	if err != nil {
		writeClusterError(w, ctx, err)
		return
	}

	filtered := make([]courierv1alpha1.CoderRun, 0, len(runs))
	for i := range runs {
		run := &runs[i]
		if query.lane != "" && run.Spec.Lane != query.lane {
			continue
		}
		if query.source != "" && run.Spec.Source != query.source {
			continue
		}
		if query.phase != "" && string(run.Status.Phase) != query.phase {
			continue
		}
		filtered = append(filtered, *run)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if !filtered[i].CreationTimestamp.Time.Equal(filtered[j].CreationTimestamp.Time) {
			return filtered[i].CreationTimestamp.Time.After(filtered[j].CreationTimestamp.Time)
		}
		return filtered[i].Name < filtered[j].Name
	})

	list := ProjectRunSummaries(filtered, query.page+1, query.pageSize, s.options())
	// The HTTP contract counts pages from zero; the projector counts from
	// one. The response echoes the page the client asked for, so the
	// client can verify it got the window it requested. A capped fetch
	// pass is stated here, never hidden: Total and More then describe
	// only the served window.
	list.Pagination.Page = query.page
	list.Pagination.Truncated = truncated
	writeJSON(w, http.StatusOK, NewEnvelope(KindRunList, list))
}

// handleRun serves GET /v1/runs/{name}: one run's detail, fetched as a
// single namespace-scoped Get by name. A named lookup never pays for a
// namespace scan, but it still holds a read-budget slot: the reader is
// uncached, so a Get is a real API-server round trip and the #836
// status-read precedent budgets single-object reads too. An unknown or
// syntactically invalid name is a 404.
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if len(validation.IsDNS1123Subdomain(name)) > 0 {
		writeError(w, http.StatusNotFound, "no such run in namespace "+s.namespace)
		return
	}
	if !s.acquireReadSlot() {
		writeBusy(w)
		return
	}
	defer s.releaseReadSlot()
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	var run courierv1alpha1.CoderRun
	if err := s.reader.Get(ctx, ctrlclient.ObjectKey{Namespace: s.namespace, Name: name}, &run); err != nil {
		if k8serrors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "no such run in namespace "+s.namespace)
			return
		}
		writeClusterError(w, ctx, err)
		return
	}
	writeJSON(w, http.StatusOK, NewEnvelope(KindRunDetail, ProjectRunDetail(&run, s.options())))
}

// runsQuery is the parsed /v1/runs query string. Page is the 0-based offset
// the HTTP contract uses; PageSize is already defaulted.
type runsQuery struct {
	lane     string
	source   string
	phase    string
	page     int
	pageSize int
}

// parseRunsQuery validates the /v1/runs query string. Every input is
// bounded: a page size outside 1..MaxPageSize, a negative page, a
// non-integer page or pageSize, or a phase outside the enum is a client
// bug and is rejected with a 400 rather than clamped, so a caller never
// mistakes a rewritten request for the one it sent.
func parseRunsQuery(r *http.Request) (runsQuery, error) {
	params := r.URL.Query()
	query := runsQuery{
		lane:     params.Get("lane"),
		source:   params.Get("source"),
		phase:    params.Get("phase"),
		page:     0,
		pageSize: DefaultPageSize,
	}
	if raw := params.Get("pageSize"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > MaxPageSize {
			return query, fmt.Errorf("pageSize must be an integer between 1 and %d", MaxPageSize)
		}
		query.pageSize = n
	}
	if raw := params.Get("page"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return query, errors.New("page must be a non-negative integer")
		}
		// A page whose start offset could overflow int must be rejected,
		// not clamped: a wrapped negative offset would index the list
		// out of bounds, and a clamped one would serve a window the
		// client did not ask for.
		if n > maxPage {
			return query, fmt.Errorf("page must be at most %d", maxPage)
		}
		query.page = n
	}
	if query.phase != "" && !ValidPhase(courierv1alpha1.Phase(query.phase)) {
		return query, errors.New("phase is not a known CoderRun phase")
	}
	return query, nil
}

// listRuns fetches the namespace's CoderRuns through a bounded, live,
// page-walked pass: at most MaxListItems objects over at most
// MaxListRoundTrips round trips. The boolean reports fetch truncation: the
// pass stopped at a bound while the API server still had a continuation
// token, so the namespace was not exhausted.
func (s *Server) listRuns(ctx context.Context) ([]courierv1alpha1.CoderRun, bool, error) {
	var runs []courierv1alpha1.CoderRun
	continueToken := ""
	for roundTrips := 1; ; roundTrips++ {
		var page courierv1alpha1.CoderRunList
		if err := s.listPage(ctx, &page, continueToken); err != nil {
			return nil, false, fmt.Errorf("list CoderRuns: %w", err)
		}
		runs = append(runs, page.Items...)
		continueToken = page.Continue
		if continueToken == "" {
			return runs, false, nil
		}
		if len(runs) >= MaxListItems || roundTrips >= MaxListRoundTrips {
			if len(runs) > MaxListItems {
				runs = runs[:MaxListItems]
			}
			return runs, true, nil
		}
	}
}

// listLanes fetches the namespace's LaneProfiles through the same bounded,
// live, page-walked pass as listRuns. LaneProfiles are read from the served
// namespace alone: the operator resolves a run's lane profile in the run's
// own namespace, so a profile from another namespace governs nothing and
// must not stand in for a missing one.
func (s *Server) listLanes(ctx context.Context) ([]courierv1alpha1.LaneProfile, bool, error) {
	var lanes []courierv1alpha1.LaneProfile
	continueToken := ""
	for roundTrips := 1; ; roundTrips++ {
		var page courierv1alpha1.LaneProfileList
		if err := s.listPage(ctx, &page, continueToken); err != nil {
			return nil, false, fmt.Errorf("list LaneProfiles: %w", err)
		}
		lanes = append(lanes, page.Items...)
		continueToken = page.Continue
		if continueToken == "" {
			return lanes, false, nil
		}
		if len(lanes) >= MaxListItems || roundTrips >= MaxListRoundTrips {
			if len(lanes) > MaxListItems {
				lanes = lanes[:MaxListItems]
			}
			return lanes, true, nil
		}
	}
}

// listPage issues one namespace-scoped List round trip: at most
// ListPageSize items, resuming from the continuation token when one is
// carried. Every List the read API makes goes through here, so no call
// site can ask the API server for an unbounded collection.
func (s *Server) listPage(ctx context.Context, list ctrlclient.ObjectList, continueToken string) error {
	opts := []ctrlclient.ListOption{
		ctrlclient.InNamespace(s.namespace),
		ctrlclient.Limit(ListPageSize),
	}
	if continueToken != "" {
		opts = append(opts, ctrlclient.Continue(continueToken))
	}
	return s.reader.List(ctx, list, opts...)
}

// acquireReadSlot takes one cluster-read budget slot without queueing. A
// full budget sheds the request instead of stacking unbounded readers
// behind a saturated API server.
func (s *Server) acquireReadSlot() bool {
	select {
	case s.readSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseReadSlot() {
	<-s.readSlots
}

func (s *Server) options() Options {
	return Options{Now: time.Now()}
}

// bearerToken extracts the bearer token from the Authorization header, the
// same shape the admission server accepts.
func bearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || strings.ToLower(scheme) != "bearer" || token == "" {
		return "", false
	}
	return token, true
}

// writeError writes a JSON error response with the given status code. The
// message is safe to surface: callers pass fixed strings that name no
// secrets or tokens.
func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// writeBusy answers a shed request: the read budget is full. Retry-After
// is advisory; the budget is process-local, so another replica may answer.
func writeBusy(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, busyMessage)
}

// writeClusterError maps a cluster round-trip failure. A request that
// outlived its own deadline is a 503 with a Retry-After hint — the
// deadline, not the caller, is at fault, matching the #836 status-read
// precedent — and everything else is a 500 with the fixed internal
// message; raw cluster error text never crosses this surface.
func writeClusterError(w http.ResponseWriter, ctx context.Context, err error) {
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, timeoutMessage)
		return
	}
	writeError(w, http.StatusInternalServerError, internalMessage)
}

// writeJSON writes one JSON response with the application/json content type.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
