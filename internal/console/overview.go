package console

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
)

// The overview page is a server-rendered consumer of three read API
// projections: the namespace summary (GET /v1/overview), the per-lane health
// list (GET /v1/lanes), and the per-source health list (GET /v1/sources).
// There is still no client-side JavaScript and no Kubernetes watch: the page
// reloads itself through a bounded meta-refresh tag the server stamps only
// when there is a backend worth polling. Every state the read API names —
// Healthy, Empty, Stale, Unavailable, Unsupported, Blocked, Unknown — renders
// as its own labeled badge, so unavailable or stale data never reads as a
// green board, and data the projection cannot supply renders as Unknown
// rather than a zero.

const (
	// overviewRefreshSeconds is the meta-refresh interval on a page that
	// read successfully. It is deliberately slow: each overview request
	// costs the read API up to three bounded namespace passes, and the
	// console has no incremental channel.
	overviewRefreshSeconds = 60
	// overviewRefreshBackoffSeconds is the interval when the read API is
	// configured but failing. The page keeps a slow retry path instead of
	// hammering a backend that is down, busy, or refusing the token.
	overviewRefreshBackoffSeconds = 300
)

// PhaseCount is one phase chip on the overview page. URL is the
// filter-preserving runs-list link; it is empty for a phase the runs explorer
// cannot filter on (an out-of-enum phase value), so the chip renders as text
// rather than a link that the explorer would reject.
type PhaseCount struct {
	Phase string
	Label string
	URL   string
	Count int
}

// LabeledCount is one condition-type row in the overview condition backlog.
type LabeledCount struct {
	Label string
	Count int
}

// handleOverview renders the namespace overview: the run summary with
// per-phase drilldowns, the lane table with capacity, reservation, and
// cooldown state, and the source table with availability and freshness. A
// failed summary fetch replaces the whole page with one state message, so an
// operator never reads an outage as an empty namespace. When the summary
// succeeds but a detail list fails, that section alone states its error; the
// rest of the page keeps rendering.
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	data := s.pageData(r, "Overview", "overview")
	ctx := r.Context()

	overview, err := s.backend.Overview(ctx)
	if err != nil {
		data.OverviewErr = overviewErrorMessage(err)
		if !errors.Is(err, ErrBackendUnavailable) {
			// A configured backend that failed: back off, but keep polling.
			data.RefreshSeconds = overviewRefreshBackoffSeconds
		}
		// ErrBackendUnavailable is a static configuration state, not a
		// transient failure: the page stops polling entirely.
		s.render(w, http.StatusOK, "overview", data)
		return
	}

	data.Overview = overview
	data.PhaseRows = phaseRows(overview.RunsByPhase)
	data.ConditionRows = conditionRows(overview.ConditionRuns)

	lanes, err := s.backend.Lanes(ctx)
	if err != nil {
		data.LanesErr = overviewErrorMessage(err)
	} else {
		data.Lanes = lanes
	}

	sources, err := s.backend.Sources(ctx)
	if err != nil {
		data.SourcesErr = overviewErrorMessage(err)
	} else {
		data.Sources = sources
	}

	data.RefreshSeconds = overviewRefreshSeconds
	s.render(w, http.StatusOK, "overview", data)
}

// overviewErrorMessage maps a projection error to an operator-facing message
// for the overview page. It mirrors the explorer's states — no backend, no
// token, a refused token, a busy read API, a 200 that is not the expected
// projection, a timeout, an unreachable read API — and echoes nothing from
// the backend's body.
func overviewErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrBackendUnavailable):
		return "The overview reads the operator read API, which is not configured for this console."
	case errors.Is(err, ErrBackendTokenMissing):
		return "The overview needs a read API bearer token. This console has a read API URL but no token, so it cannot authenticate."
	case errors.Is(err, ErrBackendUnexpectedResponse):
		return "The read API answered, but not with the projection the overview expects. Check that the console's read API URL points at the read API and not through a proxy that rewrites the response."
	}
	var backendErr *BackendError
	if errors.As(err, &backendErr) {
		switch backendErr.Status {
		case http.StatusUnauthorized:
			return "The read API refused this console's bearer token. Check the console's read API token against the read API's configured token."
		case http.StatusServiceUnavailable:
			return "The read API is at its read budget or past its own deadline. The page retries on its own; try again shortly."
		case http.StatusBadRequest:
			return "The read API rejected the request."
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

// phaseRows orders the run-phase counts for display: known phases in
// lifecycle order first, then any out-of-enum phase values in name order,
// with a link only where the runs explorer accepts the phase as a filter. A
// run whose phase was never stamped ("") is counted last, labeled as such,
// and never linked, because the explorer's phase filter cannot name it.
func phaseRows(byPhase map[string]int) []PhaseCount {
	var rows []PhaseCount
	for _, phase := range phaseFilterOptions {
		if count := byPhase[phase]; count > 0 {
			rows = append(rows, PhaseCount{
				Phase: phase,
				Label: phase,
				URL:   runsPhaseURL(phase),
				Count: count,
			})
		}
	}
	var extra []string
	for phase, count := range byPhase {
		if count <= 0 || phase == "" || knownPhases[phase] {
			continue
		}
		extra = append(extra, phase)
	}
	sort.Strings(extra)
	for _, phase := range extra {
		rows = append(rows, PhaseCount{
			Phase: phase,
			Label: phase + " (unknown phase)",
			Count: byPhase[phase],
		})
	}
	if count := byPhase[""]; count > 0 {
		rows = append(rows, PhaseCount{Label: "no phase recorded", Count: count})
	}
	return rows
}

// conditionRows orders the condition-type run counts by name.
func conditionRows(byCondition map[string]int) []LabeledCount {
	names := make([]string, 0, len(byCondition))
	for name := range byCondition {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]LabeledCount, 0, len(names))
	for _, name := range names {
		rows = append(rows, LabeledCount{Label: name, Count: byCondition[name]})
	}
	return rows
}

// runsPhaseURL builds the runs-list link filtered to one phase. The value is
// percent-encoded by url.Values, and html/template escapes the result again
// in the href context.
func runsPhaseURL(phase string) string {
	query := url.Values{"phase": {phase}}
	return "/runs?" + query.Encode()
}

// runsLaneURL builds the runs-list link filtered to one lane.
func runsLaneURL(lane string) string {
	query := url.Values{"lane": {lane}}
	return "/runs?" + query.Encode()
}

// runsLanePhaseURL builds the runs-list link filtered to one lane and one
// phase: the drilldown for a reservation wedge's Pending runs.
func runsLanePhaseURL(lane, phase string) string {
	query := url.Values{"lane": {lane}, "phase": {phase}}
	return "/runs?" + query.Encode()
}

// runsSourceURL builds the runs-list link filtered to one source.
func runsSourceURL(source string) string {
	query := url.Values{"source": {source}}
	return "/runs?" + query.Encode()
}

// availabilityClass maps the read API's availability vocabulary to a badge
// style class. An unrecognized value — a vocabulary the console has not seen
// — maps to the neutral unknown class and is never green.
func availabilityClass(value string) string {
	switch value {
	case "Healthy":
		return "badge badge-healthy"
	case "Empty":
		return "badge badge-empty"
	case "Stale":
		return "badge badge-stale"
	case "Unavailable":
		return "badge badge-unavailable"
	case "Unsupported":
		return "badge badge-unsupported"
	case "Blocked":
		return "badge badge-blocked"
	default:
		return "badge badge-unknown"
	}
}

// availabilityLabel names the availability state for the badge. An
// unrecognized value reads "Unknown (projected: <value>)": the console shows
// the raw enum member so drift is visible to the operator, and html/template
// escapes it.
func availabilityLabel(value string) string {
	switch value {
	case "Healthy", "Empty", "Stale", "Unavailable", "Unsupported", "Blocked", "Unknown":
		return value
	case "":
		return "Unknown"
	default:
		return "Unknown (projected: " + value + ")"
	}
}

// freshnessClass maps the read API's freshness vocabulary to a badge class.
// Unknown is neutral, never green: absence of an observation is not proof of
// health.
func freshnessClass(value string) string {
	switch value {
	case "Fresh":
		return "badge badge-fresh"
	case "Stale":
		return "badge badge-stale"
	default:
		return "badge badge-unknown"
	}
}

// freshnessLabel names the freshness state. An unrecognized value reads
// "Unknown".
func freshnessLabel(value string) string {
	switch value {
	case "Fresh", "Stale", "Unknown":
		return value
	default:
		return "Unknown"
	}
}
