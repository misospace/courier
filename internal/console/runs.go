package console

import (
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// The run explorer reads the operator read API's paginated run projection and
// renders it server-side. There is no client-side JavaScript: filters and
// pagination are plain GET query state, so every view is a shareable URL and
// the console stays a thin, escaping renderer over an allowlisted projection.

// pageFuncs are the helpers page templates may call. They take only bounded,
// already-projected inputs and return plain strings; html/template escapes
// their results in context.
var pageFuncs = template.FuncMap{
	"humanDuration": humanDuration,
}

// The explorer's filter bounds. They mirror the read API's own projection
// limits so the console rejects an out-of-range request up front (a friendly
// inline message) instead of forwarding a query the read API would answer
// with a bare 400. maxExplorerPageSize mirrors readapi.MaxPageSize and
// defaultPageSize mirrors readapi.DefaultPageSize; a test pins both so the two
// cannot drift. maxExplorerPage mirrors the read API's page ceiling. The phase
// set is validated against readapi.ValidPhase by a test.
const (
	maxExplorerPageSize  = 200
	defaultPageSize      = 50
	maxExplorerPage      = 1_000_000
	maxExplorerFilterLen = 253
)

// explorerPageSizes is the page-size dropdown. A requested size outside this
// set (a hand-edited query, still within the read API's 1..200) is prepended
// so the dropdown always reflects the active page.
var explorerPageSizes = []int{10, 25, 50, 100, 200}

// phaseFilterOptions is the phase dropdown, in lifecycle order. It is the set
// of phases the read API accepts as a filter; ValidPhase guards the boundary
// in a test.
var phaseFilterOptions = []string{
	"Pending",
	"Claimed",
	"Running",
	"Verifying",
	"AwaitingReview",
	"HandedOff",
	"NeedsHuman",
	"BlockedExternal",
	"Done",
	"AlreadyDone",
	"Failed",
}

var knownPhases = func() map[string]bool {
	set := make(map[string]bool, len(phaseFilterOptions))
	for _, p := range phaseFilterOptions {
		set[p] = true
	}
	return set
}()

// handleRuns serves the run explorer: it validates the query state, reads one
// page from the read API, and renders it. A backend that is unreachable or
// misconfigured renders a distinct state message, so an operator never reads
// an outage as an empty namespace.
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	data := s.pageData(r, "Runs", "runs")
	data.PhaseOptions = phaseFilterOptions

	filter, fieldErr := parseRunsFilter(r.URL.Query())
	data.Filter = filter
	if fieldErr != "" {
		// Keep the operator's (invalid) input visible in the form so they can
		// correct it; send nothing to the backend.
		data.PageSize = orDefaultPageSize(filter.PageSize)
		data.PageSizes = pageSizesWithOptions(data.PageSize)
		data.RunsErr = fieldErr
		s.render(w, http.StatusOK, "runs", data)
		return
	}

	result, err := s.backend.Runs(r.Context(), filter)
	if err != nil {
		data.PageSize = orDefaultPageSize(filter.PageSize)
		data.PageSizes = pageSizesWithOptions(data.PageSize)
		data.RunsErr = runsErrorMessage(err)
		s.render(w, http.StatusOK, "runs", data)
		return
	}

	data.Runs = result.Items
	data.Total = result.Total
	data.More = result.More
	data.Truncated = result.Truncated
	// The page number, the dropdown, and the prev/next links all derive from
	// the request, so they cannot desynchronise from each other even if a
	// middlebox echoes a page the operator did not ask for. The backend only
	// supplies the rows and the more/truncated flags.
	data.PageNumber = filter.Page + 1
	data.PageSize = orDefaultPageSize(filter.PageSize)
	data.PageSizes = pageSizesWithOptions(data.PageSize)
	data.Filtered = filter.Lane != "" || filter.Source != "" || filter.Phase != ""
	if filter.Page > 0 {
		data.PrevURL = runsURL(filter.Page-1, filter)
	}
	if result.More {
		data.NextURL = runsURL(filter.Page+1, filter)
	}
	s.render(w, http.StatusOK, "runs", data)
}

// parseRunsFilter reads the explorer's query state into a RunsFilter and
// validates it. Every input is bounded: an out-of-range page or page size, an
// unknown phase, or an over-long or control-character-bearing filter value is
// rejected with an operator-facing message rather than forwarded, so the
// browser never drives an unbounded backend query. A returned non-empty
// message means the filter must not be sent.
func parseRunsFilter(q url.Values) (RunsFilter, string) {
	filter := RunsFilter{
		Lane:   strings.TrimSpace(q.Get("lane")),
		Source: strings.TrimSpace(q.Get("source")),
		Phase:  strings.TrimSpace(q.Get("phase")),
	}
	for _, value := range []string{filter.Lane, filter.Source} {
		if len(value) > maxExplorerFilterLen {
			return filter, fmt.Sprintf("Lane and source filters must be at most %d characters.", maxExplorerFilterLen)
		}
		if strings.ContainsFunc(value, unicode.IsControl) {
			return filter, "Lane and source filters may not contain control characters."
		}
	}
	if filter.Phase != "" && !knownPhases[filter.Phase] {
		return filter, "The phase filter must be one of the listed CoderRun phases."
	}
	if raw := q.Get("page"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > maxExplorerPage {
			return filter, "The page number must be between 0 and the maximum page."
		}
		filter.Page = n
	}
	if raw := q.Get("pageSize"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxExplorerPageSize {
			return filter, fmt.Sprintf("The page size must be between 1 and %d.", maxExplorerPageSize)
		}
		filter.PageSize = n
	}
	return filter, ""
}

// runsErrorMessage maps a backend error to an operator-facing message. Each
// branch distinguishes a state the operator can act on: no backend, no token,
// a token the read API refuses, a read API that is busy, a 200 that is not a
// run list, a request that timed out, and an unreachable read API. None of
// them echo the backend's response body.
func runsErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrBackendUnavailable):
		return "The run explorer reads the operator read API, which is not configured for this console."
	case errors.Is(err, ErrBackendTokenMissing):
		return "The run explorer needs a read API bearer token. This console has a read API URL but no token, so it cannot authenticate."
	case errors.Is(err, ErrBackendUnexpectedResponse):
		return "The read API answered, but not with a run list. Check that the console's read API URL points at the read API and not through a proxy that rewrites the response."
	}
	var backendErr *BackendError
	if errors.As(err, &backendErr) {
		switch backendErr.Status {
		case http.StatusUnauthorized:
			return "The read API refused this console's bearer token. Check the console's read API token against the read API's configured token."
		case http.StatusBadRequest:
			return "The read API rejected the request. Adjust the filters or page size."
		case http.StatusServiceUnavailable:
			return "The read API is at its read budget or past its own deadline. Try again shortly."
		default:
			return fmt.Sprintf("The read API returned an unexpected status (%d).", backendErr.Status)
		}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return "The read API request timed out before it answered. Raise --backend-timeout or try again."
	}
	return "The operator read API could not be reached."
}

// runsURL builds a filter-preserving link to a page. The values are
// percent-encoded by url.Values, so the result carries no raw HTML; the
// template renders it in an href attribute context, which escapes it again.
func runsURL(page int, filter RunsFilter) string {
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
	if filter.PageSize > 0 {
		query.Set("pageSize", strconv.Itoa(filter.PageSize))
	}
	if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}
	if encoded := query.Encode(); encoded != "" {
		return "/runs?" + encoded
	}
	return "/runs"
}

// pageSizesWithOptions returns the dropdown options, with an active size that
// is not one of the preset sizes inserted first so the select reflects it.
func pageSizesWithOptions(active int) []int {
	for _, size := range explorerPageSizes {
		if size == active {
			return explorerPageSizes
		}
	}
	if active > 0 {
		return append([]int{active}, explorerPageSizes...)
	}
	return explorerPageSizes
}

func orDefaultPageSize(size int) int {
	if size <= 0 {
		return defaultPageSize
	}
	return size
}

// humanDuration renders a run's age in coarse units. The input is a bounded,
// projected count; a nil value (no creation timestamp) reads as an em dash,
// never a fabricated zero age.
func humanDuration(seconds *int64) string {
	if seconds == nil {
		return "—"
	}
	s := *seconds
	if s < 0 {
		s = 0
	}
	d := time.Duration(s) * time.Second
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", s)
	case d < time.Hour:
		return fmt.Sprintf("%dm", int64(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%02dm", int64(d.Hours()), int64(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd", int64(d.Hours())/24)
	}
}
