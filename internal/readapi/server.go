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

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The routes the read API serves, all under /v1. The run-detail route is a
// ServeMux pattern; its {name} segment is read with r.PathValue.
const (
	PathOverview = "/v1/overview"
	PathLanes    = "/v1/lanes"
	PathRuns     = "/v1/runs"
	PathRun      = "/v1/runs/{name}"
)

const (
	// MaxPageSize is the largest page size a client may request. A larger
	// pageSize is a client bug, not a clamp target: the request is rejected
	// so a caller cannot mistake a silently rewritten page for the one it
	// asked for.
	MaxPageSize = 200

	// MaxListItems caps how many runs one request pass projects. It bounds
	// the per-request cost of a list endpoint: a namespace holding more
	// runs than this gets a truncated page, not an unbounded response.
	MaxListItems = 10 * MaxPageSize

	// requestTimeout bounds one request's cluster round trips. A stalled
	// API server that accepts the connection but never answers maps to a
	// 503 deadline instead of holding the connection open forever.
	requestTimeout = 30 * time.Second

	unauthorizedMessage = "the read API requires a valid bearer token"
	internalMessage     = "the read API failed to project the requested state"
)

// ServerConfig is the construction input for a Server. Token, Bind, and
// Namespace are required when the server is enabled; the constructor fails
// closed on a missing one so a wiring mistake surfaces at startup, never as
// a listener that authenticates nothing or answers across namespaces.
type ServerConfig struct {
	// Reader is the read-only cluster access the projections list through.
	// Only namespace-scoped List calls are ever made. Required.
	Reader ctrlclient.Reader
	// Token is the static bearer token clients present. Required; it is
	// compared in constant time and never logged or echoed.
	Token string
	// Bind is the listener address (e.g. ":8084"). Empty disables the
	// server entirely: the constructor returns a no-op that never listens.
	Bind string
	// Namespace is the single namespace the read model serves. Required.
	Namespace string
}

// Server serves the read-only HTTP projection API over CoderRun and
// LaneProfile objects. It is a thin transport layer: authentication, query
// parsing, bounding, and error mapping happen here; every state judgment is
// the projection layer's. Each request lists the namespace live — there is
// no cache for v1 — so the server holds no state of its own beyond config.
type Server struct {
	reader    ctrlclient.Reader
	token     string
	bind      string
	namespace string
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

// Handler returns the server's HTTP handler. Authentication wraps the mux,
// so it answers before any routing: an unauthenticated caller learns
// nothing about which routes exist. One constant message covers missing,
// malformed, and wrong tokens alike.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+PathOverview, s.handleOverview)
	mux.HandleFunc("GET "+PathLanes, s.handleLanes)
	mux.HandleFunc("GET "+PathRuns, s.handleRuns)
	mux.HandleFunc("GET "+PathRun, s.handleRun)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) != 1 {
			writeError(w, http.StatusUnauthorized, unauthorizedMessage)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// handleOverview serves GET /v1/overview: the single-namespace snapshot.
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, err := s.listRuns(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, internalMessage)
		return
	}
	lanes, err := s.listLanes(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, internalMessage)
		return
	}
	writeJSON(w, http.StatusOK, NewEnvelope(KindOverview, ProjectOverview(s.namespace, runs, lanes, s.options())))
}

// handleLanes serves GET /v1/lanes: every configured lane's health, plus the
// sources visible in the namespace's runs. Lanes are a small configuration
// set, so the list is served in one unpaginated page.
func (s *Server) handleLanes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, err := s.listRuns(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, internalMessage)
		return
	}
	lanes, err := s.listLanes(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, internalMessage)
		return
	}
	healths := ProjectLaneHealths(lanes, runs, s.options())
	list := LaneHealthList{
		Items: healths,
		Pagination: Pagination{
			Page:     0,
			PageSize: len(healths),
			Total:    len(healths),
		},
	}
	writeJSON(w, http.StatusOK, NewEnvelope(KindLaneList, list))
}

// handleRuns serves GET /v1/runs: the paginated run list with exact-match
// lane/source/phase filters, sorted newest-first.
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	query, err := parseRunsQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, err := s.listRuns(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, internalMessage)
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

	// Bound the pass: past the cap the newest window is served and the
	// truncation is stated in the response, never hidden.
	truncated := false
	if len(filtered) > MaxListItems {
		filtered = filtered[:MaxListItems]
		truncated = true
	}

	list := ProjectRunSummaries(filtered, query.page+1, query.pageSize, s.options())
	// The HTTP contract counts pages from zero; the projector counts from
	// one. The response echoes the page the client asked for, so the
	// client can verify it got the window it requested.
	list.Pagination.Page = query.page
	list.Pagination.Truncated = truncated
	writeJSON(w, http.StatusOK, NewEnvelope(KindRunList, list))
}

// handleRun serves GET /v1/runs/{name}: one run's detail. The lookup is a
// namespace-scoped list and name match, keeping the read surface to List
// calls only; an unknown name is a 404.
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	runs, err := s.listRuns(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, internalMessage)
		return
	}
	for i := range runs {
		if runs[i].Name != name {
			continue
		}
		writeJSON(w, http.StatusOK, NewEnvelope(KindRunDetail, ProjectRunDetail(&runs[i], s.options())))
		return
	}
	writeError(w, http.StatusNotFound, "no such run in namespace "+s.namespace)
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

// listRuns lists the namespace's CoderRuns live, namespace-scoped only.
func (s *Server) listRuns(ctx context.Context) ([]courierv1alpha1.CoderRun, error) {
	var list courierv1alpha1.CoderRunList
	if err := s.reader.List(ctx, &list, ctrlclient.InNamespace(s.namespace)); err != nil {
		return nil, fmt.Errorf("list CoderRuns: %w", err)
	}
	return list.Items, nil
}

// listLanes lists the namespace's LaneProfiles live, namespace-scoped only.
func (s *Server) listLanes(ctx context.Context) ([]courierv1alpha1.LaneProfile, error) {
	var list courierv1alpha1.LaneProfileList
	if err := s.reader.List(ctx, &list, ctrlclient.InNamespace(s.namespace)); err != nil {
		return nil, fmt.Errorf("list LaneProfiles: %w", err)
	}
	return list.Items, nil
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

// writeJSON writes one JSON response with the application/json content type.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
