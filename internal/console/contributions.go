package console

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// The contribution status pages answer the contributor's question — was my
// submission admitted, is it running, is it waiting on a human, did a pull
// request get published — from the contribution status API's recorded state
// alone: GET /v1/contributions/<run> on the manager's contribution listener,
// authenticated with the status-read bearer token. This is deliberately a
// second backend: the operator read API serves namespace-wide operator
// projections, the admission credential writes, and the status-read
// credential reads one contribution and nothing else. The console keeps all
// three credentials server-side; the browser sees rendered state only.
//
// Like every console page there is no client-side JavaScript. Freshness is a
// meta-refresh the server stamps per lifecycle state — fast while a run is
// active, slow while it is parked, never once it is terminal or the page is
// in a static state — and a transient backend failure retries slowly instead
// of hammering. The page never renders a verdict the API did not record: a
// published pull request is a published pull request, never a merged one,
// and an unrecognized phase renders as the raw word marked unknown.

const (
	// contributeStatusPrefix mirrors the contribution status route prefix
	// (adhoc.PathContributions + "/") in internal/source/adhoc. The console
	// imports only the standard library, so this constant is the wire
	// contract the two agree on; a test pins it against the adhoc package's
	// exported PathContributions.
	contributeStatusPrefix = "/v1/contributions/"

	// contributeStatusTimeout bounds one status request. It mirrors the
	// status endpoint's own 10-second read deadline, so the console gives up
	// exactly when the server would have answered a deadline anyway.
	contributeStatusTimeout = 10 * time.Second

	// maxContributeStatusBody bounds the decoded response. The status
	// projection is a fixed-shape object with every string length-capped
	// server-side; anything approaching this bound is a confused upstream.
	maxContributeStatusBody = 64 * 1024

	// contributeStatusRefreshSeconds is the meta-refresh interval while a
	// run is active (no phase yet, Pending, Claimed, Running, Verifying).
	contributeStatusRefreshSeconds = 15
	// contributeStatusParkedSeconds is the interval while a run waits on
	// something outside the pipeline: review, a human, or an external
	// condition. Movement is possible but no longer pipeline-driven.
	contributeStatusParkedSeconds = 60
	// contributeStatusBackoffSeconds is the interval after a transient
	// status failure (busy, timed out, unreachable). The page keeps a slow
	// retry path instead of hammering a backend that is down.
	contributeStatusBackoffSeconds = 300

	// contributeHistoryCookie holds this browser's recently viewed
	// contributions: a signed list of namespace-and-run links, nothing else.
	contributeHistoryCookie = "courier_console_contribute_history"
	// contributeHistoryScope binds the history HMAC to this use so a signed
	// history value can never verify as another cookie's signature.
	contributeHistoryScope = "contribute-history:"
	// maxContributeHistoryEntries bounds the recently-viewed list.
	maxContributeHistoryEntries = 8
	// maxContributeHistoryCookieBytes bounds the serialized cookie. Object
	// names are at most 253 bytes, so the entry cap alone could still exceed
	// a browser's practical cookie budget; over-long tails are dropped.
	maxContributeHistoryCookieBytes = 2048
)

// contributeRunNamePattern and maxContributeRunNameBytes mirror the status
// endpoint's object-name guard: a name that could never address a run is
// answered locally, without a backend round trip.
var contributeRunNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

const maxContributeRunNameBytes = 253

// contributeNamespacePattern is the Kubernetes namespace shape (DNS-1123
// label). The endpoint enforces its own owned-namespace boundary; the
// console pre-validates shape so a hostile query never reaches the backend.
var contributeNamespacePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const maxContributeNamespaceBytes = 63

// contributionSource is the run source that marks a contribution
// (internal/source/adhoc's SourceName). The run explorer links only these
// rows to the status page; a test pins the constant against the adhoc
// package so the two cannot drift.
const contributionSource = "adhoc"

// contributeStatusTerminalPhases are the recorded endings the console treats
// as final for polling: nothing in Courier moves them without a new
// admission. The endpoint classes AwaitingReview, HandedOff, NeedsHuman, and
// BlockedExternal as recorded endings too, but those wait on a person or an
// outside condition that can resolve at any time and retention may reap them,
// so the page keeps checking slowly there (see contributeStatusParkedPhases)
// and stops only here. Nothing in this split says "merged", which Courier
// never claims.
var contributeStatusTerminalPhases = map[string]bool{
	"Done":        true,
	"AlreadyDone": true,
	"Failed":      true,
}

// contributeStatusParkedPhases are runs waiting outside the pipeline:
// review, a human decision, or an external condition.
var contributeStatusParkedPhases = map[string]bool{
	"AwaitingReview":  true,
	"HandedOff":       true,
	"NeedsHuman":      true,
	"BlockedExternal": true,
}

// phaseContributeUnknown mirrors adhoc.PhaseUnknown: the controller has not
// stamped a phase onto the run yet. It is not a CoderRun phase.
const phaseContributeUnknown = "unknown"

// contributionStatus is the console's decode of the status projection. It
// mirrors the JSON contract the status endpoint serves and decodes only
// that contract; timestamps stay raw strings and render verbatim, so no
// clock arithmetic is claimed beyond a clamped phase age derived from two
// stamps in the same response.
type contributionStatus struct {
	Run            string                   `json:"run"`
	Namespace      string                   `json:"namespace"`
	Lane           string                   `json:"lane"`
	BaseRepo       string                   `json:"baseRepo"`
	WorkRepo       string                   `json:"workRepo"`
	Phase          string                   `json:"phase"`
	CIParked       bool                     `json:"ciParked"`
	Publication    *contributionPublication `json:"publication,omitempty"`
	Cause          *contributionCause       `json:"cause,omitempty"`
	AdmittedAt     string                   `json:"admittedAt,omitempty"`
	StartedAt      string                   `json:"startedAt,omitempty"`
	PhaseEnteredAt string                   `json:"phaseEnteredAt,omitempty"`
	FinishedAt     string                   `json:"finishedAt,omitempty"`
	ObservedAt     string                   `json:"observedAt"`
}

type contributionPublication struct {
	Branch   string `json:"branch,omitempty"`
	HeadRepo string `json:"headRepo,omitempty"`
	HeadSHA  string `json:"headSha,omitempty"`
	PR       string `json:"pr,omitempty"`
}

type contributionCause struct {
	Category string `json:"category,omitempty"`
	Outcome  string `json:"outcome,omitempty"`
}

// ErrContributeStatusUnexpected reports a 200 the console could not read as
// a contribution status: an unparseable or over-large body, or a body that
// parses as JSON but does not describe the requested run. Like the read
// API client's counterpart, it exists so a broken upstream never renders as
// "no contribution". The not-configured case is not an error: a console
// without a status backend has a nil client and renders its
// not-configured state without a request.
var ErrContributeStatusUnexpected = errors.New("contribution status API returned an unexpected response")

// contributeUnexpectedMessage is the one copy of the misconfiguration
// message every 200 the console cannot read as a contribution status
// renders: a static state, so a broken upstream never loops a retry.
const contributeUnexpectedMessage = "The contribution status API answered, but not with a contribution status. Check that the console's contribution status URL points at the contribution listener."

// ContributeStatusError reports a status reply that carried a non-200
// status. It names the status only, never the response body, so an
// operator-facing message can distinguish "not found" from "credential
// refused" without echoing anything the backend put in the body.
type ContributeStatusError struct {
	Status int
}

func (e *ContributeStatusError) Error() string {
	return fmt.Sprintf("contribution status API returned status %d", e.Status)
}

// contributeStatusClient is the status read, abstracted so the handlers are
// testable without a listener.
type contributeStatusClient interface {
	// Status reads one contribution. namespace may be empty, which asks the
	// endpoint its own default. A console with no backend has a nil client,
	// not an error; *ContributeStatusError carries a non-200; any other
	// error is a transport or decode failure, and a 200 the client could
	// not read as a status of the requested run fails as
	// ErrContributeStatusUnexpected. The bearer token and the raw body
	// never cross into the result.
	Status(ctx context.Context, namespace, run string) (contributionStatus, error)
}

// httpContributeStatusClient is the contributeStatusClient backed by the
// contribution listener. Only the run segment and the optional namespace
// parameter vary per call; the path prefix is a constant, and the run name
// is re-validated against the object-name shape before the request leaves,
// so no input steers the client to another path or host.
type httpContributeStatusClient struct {
	baseURL string
	token   []byte
	client  *http.Client
}

// newHTTPContributeStatusClient validates the base URL fail-closed: an
// http(s) origin with no credentials, query, or fragment, mirroring the
// admission client's posture.
func newHTTPContributeStatusClient(base string, token []byte) (*httpContributeStatusClient, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(base, "#") {
		return nil, errors.New("console: invalid contribution status API URL")
	}
	return &httpContributeStatusClient{
		baseURL: strings.TrimRight(base, "/"),
		token:   append([]byte(nil), token...),
		client:  &http.Client{Timeout: contributeStatusTimeout},
	}, nil
}

func (c *httpContributeStatusClient) Status(ctx context.Context, namespace, run string) (contributionStatus, error) {
	if !validContributeRunName(run) {
		// Unreachable from the handlers, which validate first; kept so the
		// client cannot be talked into a hostile path segment by a future
		// call site.
		return contributionStatus{}, fmt.Errorf("contribution status request: invalid run name")
	}
	requestCtx, cancel := context.WithTimeout(ctx, contributeStatusTimeout)
	defer cancel()
	target := c.baseURL + contributeStatusPrefix + url.PathEscape(run)
	if namespace != "" {
		target += "?namespace=" + url.QueryEscape(namespace)
	}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, target, nil)
	if err != nil {
		return contributionStatus{}, fmt.Errorf("contribution status request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+string(c.token))
	resp, err := c.client.Do(req)
	if err != nil {
		return contributionStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Drain a bounded amount so the connection can be reused; the body
		// never crosses into a response or a log.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return contributionStatus{}, &ContributeStatusError{Status: resp.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxContributeStatusBody+1))
	if err != nil {
		return contributionStatus{}, fmt.Errorf("%w: %v", ErrContributeStatusUnexpected, err)
	}
	if len(body) > maxContributeStatusBody {
		return contributionStatus{}, fmt.Errorf("%w: response exceeds %d bytes", ErrContributeStatusUnexpected, maxContributeStatusBody)
	}
	var status contributionStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return contributionStatus{}, fmt.Errorf("%w: %v", ErrContributeStatusUnexpected, err)
	}
	if !status.describes(run) {
		// The endpoint always echoes the run's name and stamps the read's
		// observedAt. A 200 that parses as JSON but does not — say a bare
		// {} from a misconfigured status URL pointed at some other service —
		// fails as the documented misconfiguration error instead of
		// decoding to a zero status the page would render as an empty
		// "found".
		return contributionStatus{}, fmt.Errorf("%w: response does not describe the requested run", ErrContributeStatusUnexpected)
	}
	return status, nil
}

// describes reports whether the decoded body carries the two fields the
// status endpoint always projects: the echo of the requested run's name and
// the read's observedAt. The run name the endpoint echoes is the object name
// it was addressed by — always the validated request name, never a
// truncated form, because the projection's field cap exceeds the object-name
// cap the request is validated against.
func (s contributionStatus) describes(run string) bool {
	return s.Run == run && s.ObservedAt != ""
}

// validContributeRunName reports whether the name could address a run on the
// status endpoint (the shape guard only, not existence).
func validContributeRunName(name string) bool {
	return name != "" && len(name) <= maxContributeRunNameBytes && contributeRunNamePattern.MatchString(name)
}

// validContributeNamespace reports whether the value could name a namespace.
// The empty namespace is valid and means "let the endpoint use its default".
func validContributeNamespace(namespace string) bool {
	return namespace == "" || (len(namespace) <= maxContributeNamespaceBytes && contributeNamespacePattern.MatchString(namespace))
}

// contributionData is the view both contribution pages render. State picks
// the page body: "notconfigured", "invalid", "notfound", "forbidden",
// "denied", "error", or "found".
type contributionData struct {
	Title          string
	Active         string
	Authenticated  bool
	CSRF           string
	RefreshSeconds int

	State        string
	ErrorMessage string

	RunName   string
	Namespace string

	BackendConfigured bool
	HistoryAvailable  bool

	Status       contributionStatus
	PhaseLabel   string
	PhaseClass   string
	PhaseAge     string
	PRLink       string
	BaseRepoLink string
	HeadRepoLink string
	CommitLink   string
	HasPublished bool

	History []contributeHistoryEntry
}

// handleContributions serves the contribution lookup page: a run-name form
// (a plain GET the detail route consumes via redirect) and, in token mode,
// this browser's signed recently-viewed list.
func (s *Server) handleContributions(w http.ResponseWriter, r *http.Request) {
	data := s.contributionPageData(r, "Contributions")
	if run := strings.TrimSpace(r.URL.Query().Get("run")); run != "" {
		if !validContributeRunName(run) {
			data.History = s.readContributeHistory(r)
			data.State = "invalid"
			data.ErrorMessage = "Enter the exact run name the contribution response returned. Names are lowercase letters, digits, dots, and hyphens, at most 253 characters."
			s.renderContribution(w, http.StatusOK, "contributions", data)
			return
		}
		http.Redirect(w, r, contributionRunURL(run, ""), http.StatusSeeOther)
		return
	}
	data.History = s.readContributeHistory(r)
	s.renderContribution(w, http.StatusOK, "contributions", data)
}

// handleContribution serves one contribution's status detail. The run name
// is the correlation identity an admission response handed the submitter,
// and ?namespace= pins the admission namespace for a durable deep link.
func (s *Server) handleContribution(w http.ResponseWriter, r *http.Request) {
	run := r.PathValue("run")
	namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
	data := s.contributionPageData(r, "Contribution "+run)
	data.RunName = run
	data.Namespace = namespace
	data.History = s.readContributeHistory(r)

	switch {
	case s.contributions == nil:
		data.State = "notconfigured"
	case !validContributeRunName(run):
		// An impossible name and an unknown one render alike without a
		// backend round trip; the page says which shape was rejected so a
		// typo never looks like a vanished run.
		data.State = "invalid"
		data.ErrorMessage = "That is not a valid run name. Names are lowercase letters, digits, dots, and hyphens, at most 253 characters."
	case !validContributeNamespace(namespace):
		data.State = "invalid"
		data.ErrorMessage = "That is not a valid namespace. Names are lowercase letters, digits, and hyphens, at most 63 characters."
	default:
		s.renderContributionStatus(w, r, run, namespace, data)
		return
	}
	s.renderContribution(w, http.StatusOK, "contribution", data)
}

// renderContributionStatus performs the read and maps every outcome to one
// honest state. No state renders a guess: the projection's words decide the
// page's words, and the refresh interval follows the lifecycle state.
func (s *Server) renderContributionStatus(w http.ResponseWriter, r *http.Request, run, namespace string, data contributionData) {
	status, err := s.contributions.Status(r.Context(), namespace, run)
	if err != nil {
		data.State = "error"
		switch {
		case errors.Is(err, ErrContributeStatusUnexpected):
			// A 200 that is not a contribution status is a misconfigured
			// URL or a rewriting proxy: a static state, no point retrying.
			data.ErrorMessage = contributeUnexpectedMessage
		default:
			var statusErr *ContributeStatusError
			if errors.As(err, &statusErr) {
				switch statusErr.Status {
				case http.StatusNotFound:
					// The endpoint cannot distinguish never-admitted from
					// reaped from foreign, and neither does the page.
					data.State = "notfound"
					data.ErrorMessage = contributeStateMessage(data.State)
				case http.StatusForbidden:
					data.State = "forbidden"
					data.ErrorMessage = contributeStateMessage(data.State)
				case http.StatusUnauthorized:
					data.State = "denied"
					data.ErrorMessage = contributeStateMessage(data.State)
				case http.StatusServiceUnavailable:
					data.ErrorMessage = "The contribution status API is at capacity or its cluster read timed out. The page retries on its own; try again shortly."
					data.RefreshSeconds = contributeStatusBackoffSeconds
				default:
					data.ErrorMessage = fmt.Sprintf("The contribution status API returned an unexpected status (%d).", statusErr.Status)
					data.RefreshSeconds = contributeStatusBackoffSeconds
				}
			} else {
				data.ErrorMessage = "The contribution status API could not be reached. The page retries on its own; try again shortly."
				data.RefreshSeconds = contributeStatusBackoffSeconds
			}
		}
		s.renderContribution(w, http.StatusOK, "contribution", data)
		return
	}

	// Defensive re-check of the decode boundary: the client rejects a body
	// that does not describe the requested run, but "found" is the one
	// state that renders backend content, so it validates here too. A
	// status that echoes another run or carries no observedAt is the same
	// misconfiguration as an unparseable body and renders as its static
	// state — never blank fields, an active refresh, or a history entry.
	if !status.describes(run) {
		data.State = "error"
		data.ErrorMessage = contributeUnexpectedMessage
		s.renderContribution(w, http.StatusOK, "contribution", data)
		return
	}

	data.State = "found"
	data.Status = status
	// The response echoes the canonical namespace; the deep link and the
	// history entry carry it, so a reload stays pinned even when the first
	// read used the endpoint's default.
	if status.Namespace != "" {
		data.Namespace = status.Namespace
	}
	data.PhaseLabel, data.PhaseClass = contributePhaseLabel(status.Phase)
	data.PhaseAge = contributePhaseAge(status.PhaseEnteredAt, status.ObservedAt)
	if status.Publication != nil {
		data.HasPublished = status.Publication.PR != "" || status.Publication.Branch != "" ||
			status.Publication.HeadRepo != "" || status.Publication.HeadSHA != ""
		data.PRLink = safeContributionURL(status.Publication.PR)
		// The repository and commit links are derived, never projected:
		// their host is the vetted PR URL's own, so a projected value can
		// at worst point at a wrong path on a host the page already links.
		data.HeadRepoLink = contributionRepoLink(data.PRLink, status.Publication.HeadRepo)
		data.CommitLink = contributionCommitLink(data.PRLink, status.Publication.HeadSHA)
	}
	data.BaseRepoLink = contributionRepoLink(data.PRLink, status.BaseRepo)
	data.RefreshSeconds = contributeRefreshSeconds(status.Phase)
	s.rememberContributeHistory(w, r, status.Namespace, run)
	s.renderContribution(w, http.StatusOK, "contribution", data)
}

// contributeStateMessage names the states that the page renders with a
// fixed message (the mapping above leaves those ErrorMessage fields empty so
// the copy lives in exactly one place; the default branch is a defensive
// fallback for a state no caller maps).
func contributeStateMessage(state string) string {
	switch state {
	case "notconfigured":
		return "The contribution status page reads the contribution status API, which is not configured for this console."
	case "notfound":
		return "No contribution run with this name was found. It may never have been admitted, it may have been reaped after it finished, or it is not a contribution. The status API does not say which."
	case "forbidden":
		return "That namespace is outside the namespaces the contribution API owns, so it will not read a run there."
	case "denied":
		return "The contribution status API refused this console's status credential. Check the console's status token against the manager's contribution read key."
	default:
		return "The contribution status could not be read."
	}
}

// contributePhaseLabel maps the recorded phase to one honest word and a
// badge class. Unknown words render as unknown; nothing says merged or
// successful.
func contributePhaseLabel(phase string) (label, class string) {
	switch phase {
	case "", phaseContributeUnknown:
		return "No phase recorded yet", "badge badge-empty"
	case "Pending":
		return "Admitted, waiting to start", "badge badge-empty"
	case "Claimed", "Running":
		return "Executing", "badge badge-fresh"
	case "Verifying":
		return "Verifying", "badge badge-fresh"
	case "AwaitingReview", "HandedOff":
		return "Awaiting review", "badge badge-stale"
	case "NeedsHuman":
		return "Operator action needed", "badge badge-stale"
	case "BlockedExternal":
		return "Blocked outside Courier", "badge badge-stale"
	case "Done":
		return "Finished", "badge badge-healthy"
	case "AlreadyDone":
		return "Finished (already done)", "badge badge-healthy"
	case "Failed":
		return "Failed", "badge badge-unavailable"
	default:
		return "Unknown (projected: " + phase + ")", "badge badge-unknown"
	}
}

// contributeRefreshSeconds sets the meta-refresh interval from the lifecycle
// state: active runs poll fastest, parked runs slower, terminal runs never,
// and an unrecognized phase — state the console cannot reason about —
// polls at the parked interval.
func contributeRefreshSeconds(phase string) int {
	switch {
	case contributeStatusTerminalPhases[phase]:
		return 0
	case contributeStatusParkedPhases[phase]:
		return contributeStatusParkedSeconds
	case phase == "" || phase == phaseContributeUnknown:
		return contributeStatusRefreshSeconds
	}
	if knownPhases[phase] {
		// Known but neither terminal nor parked: Pending, Claimed, Running,
		// Verifying. Active.
		return contributeStatusRefreshSeconds
	}
	// An out-of-vocabulary phase could still move; check back slowly.
	return contributeStatusParkedSeconds
}

// contributePhaseAge renders how long the run has sat in its current phase,
// derived only from two stamps in the same response and clamped at zero, so
// a skewed pair never renders a negative age. A missing or unparseable stamp
// returns the empty string and the page says nothing about age, never a
// fabricated zero.
func contributePhaseAge(phaseEnteredAt, observedAt string) string {
	entered, err1 := time.Parse(time.RFC3339, phaseEnteredAt)
	observed, err2 := time.Parse(time.RFC3339, observedAt)
	if err1 != nil || err2 != nil {
		return ""
	}
	seconds := int64(observed.Sub(entered).Seconds())
	if seconds < 0 {
		seconds = 0
	}
	return humanDuration(&seconds)
}

// safeContributionURL returns raw when it is a plain https URL with a host
// and no embedded credentials — the only shape the page renders as a link.
// Everything else (http, javascript, data, schemeless, credential-bearing)
// returns empty and the value renders as escaped text instead.
func safeContributionURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return ""
	}
	return u.String()
}

// contributeRepoPattern is the forge "owner/name" repository shape, bounded
// to the segment lengths forges accept. Admission accepts a looser shape
// (internal/source/adhoc's repoPattern), so this is the link-safety bound,
// not the identity bound: only a value of this shape is ever embedded in a
// derived repository link, and anything longer stays plain text.
var contributeRepoPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}/[A-Za-z0-9._-]{1,100}$`)

// contributeSHAPattern is a hexadecimal commit SHA. Only this shape is ever
// embedded in a derived commit link.
var contributeSHAPattern = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)

// contributionRepoLink derives a repository link from a
// safeContributionURL-vetted PR URL: the link's host is that URL's host, so
// no projected value chooses the scheme or host. With no vetted PR URL — the
// common case before a pull request is published — or a value outside the
// "owner/name" shape it returns empty and the page renders plain text.
func contributionRepoLink(prLink, repo string) string {
	if prLink == "" || !contributeRepoPattern.MatchString(repo) {
		return ""
	}
	u, err := url.Parse(prLink)
	if err != nil {
		return ""
	}
	u.Path = "/" + repo
	u.RawQuery, u.Fragment, u.RawPath = "", "", ""
	return u.String()
}

// contributionCommitLink derives a commit link under a vetted PR URL's host
// (every major forge serves /commit/<sha> and a pull request's commits under
// the same origin). With no vetted PR URL or a value outside the
// hexadecimal-SHA shape it returns empty and the page renders plain text.
func contributionCommitLink(prLink, sha string) string {
	if prLink == "" || !contributeSHAPattern.MatchString(sha) {
		return ""
	}
	u, err := url.Parse(prLink)
	if err != nil {
		return ""
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/commit/" + sha
	u.RawQuery, u.Fragment, u.RawPath = "", "", ""
	return u.String()
}

// contributionRunURL builds the deep link to one contribution's status
// page. The run segment is path-escaped and the namespace query
// percent-encoded, and html/template escapes the result again in href.
func contributionRunURL(run, namespace string) string {
	link := "/contributions/" + url.PathEscape(run)
	if namespace != "" {
		link += "?namespace=" + url.QueryEscape(namespace)
	}
	return link
}

// contributeHistoryEntry is one recently viewed contribution: a link and
// nothing else. No lifecycle data, no goal text, no credential.
type contributeHistoryEntry struct {
	Namespace string `json:"n"`
	Run       string `json:"r"`
}

// contributeHistoryLink is the entry's rendered deep link.
func (e contributeHistoryEntry) Link() string { return contributionRunURL(e.Run, e.Namespace) }

// readContributeHistory returns this browser's validated recently-viewed
// entries. Without a session key (header mode) there is nothing to sign or
// verify and the list is empty; a forged, stale-shaped, or over-full cookie
// degrades to its valid prefix, never past it.
func (s *Server) readContributeHistory(r *http.Request) []contributeHistoryEntry {
	if len(s.auth.key) == 0 {
		return nil
	}
	cookie, err := r.Cookie(contributeHistoryCookie)
	if err != nil || cookie.Value == "" {
		return nil
	}
	payload, sig, found := strings.Cut(cookie.Value, ".")
	if !found || payload == "" || sig == "" {
		return nil
	}
	if subtle.ConstantTimeCompare([]byte(s.signContributeHistory(payload)), []byte(sig)) != 1 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}
	var entries []contributeHistoryEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil
	}
	var out []contributeHistoryEntry
	for _, entry := range entries {
		if len(out) >= maxContributeHistoryEntries {
			break
		}
		if !validContributeRunName(entry.Run) || !validContributeNamespace(entry.Namespace) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// rememberContributeHistory moves the just-read contribution to the front of
// the browser's list and stores it under signature. Token mode only (the
// session key is the signing material); a serialization that still exceeds
// the cookie budget after dropping its tail stores nothing this round rather
// than a truncated signature.
func (s *Server) rememberContributeHistory(w http.ResponseWriter, r *http.Request, namespace, run string) {
	if len(s.auth.key) == 0 {
		return
	}
	if namespace == "" {
		namespace = strings.TrimSpace(r.URL.Query().Get("namespace"))
	}
	if !validContributeRunName(run) || !validContributeNamespace(namespace) {
		return
	}
	entries := []contributeHistoryEntry{{Namespace: namespace, Run: run}}
	for _, entry := range s.readContributeHistory(r) {
		if entry.Run == run && entry.Namespace == namespace {
			continue
		}
		if len(entries) >= maxContributeHistoryEntries {
			break
		}
		entries = append(entries, entry)
	}
	for len(entries) > 0 {
		raw, err := json.Marshal(entries)
		if err != nil {
			return
		}
		payload := base64.RawURLEncoding.EncodeToString(raw)
		value := payload + "." + s.signContributeHistory(payload)
		if len(value) <= maxContributeHistoryCookieBytes {
			http.SetCookie(w, s.auth.contributeHistoryCookie(value))
			return
		}
		entries = entries[:len(entries)-1]
	}
}

// forgetContributeHistory returns the clearing cookie, sent on logout so a
// shared browser does not carry the previous operator's links.
func (s *Server) forgetContributeHistory() *http.Cookie {
	cookie := s.auth.contributeHistoryCookie("")
	cookie.Expires = s.auth.now().Add(-time.Hour)
	cookie.MaxAge = -1
	return cookie
}

func (a *Authenticator) contributeHistoryCookie(value string) *http.Cookie {
	cookie := a.sessionCookie()
	cookie.Name = contributeHistoryCookie
	cookie.Value = value
	cookie.MaxAge = int(sessionTTL.Seconds())
	cookie.Expires = a.now().Add(sessionTTL)
	return cookie
}

// signContributeHistory is the history HMAC under the session key, scoped so
// it never verifies as a session or CSRF signature.
func (s *Server) signContributeHistory(payload string) string {
	mac := hmac.New(sha256.New, s.auth.key)
	mac.Write([]byte(contributeHistoryScope + payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// contributionPageData starts a contribution view from the shared chrome.
// The history list is token-mode material, so header-mode requests get
// nothing; the detail itself works in both modes.
func (s *Server) contributionPageData(r *http.Request, title string) contributionData {
	common := s.pageData(r, title, "contributions")
	return contributionData{
		Title:             common.Title,
		Active:            common.Active,
		Authenticated:     common.Authenticated,
		CSRF:              common.CSRF,
		BackendConfigured: s.contributions != nil,
		HistoryAvailable:  len(s.auth.key) > 0,
	}
}

// renderContribution executes a contribution page.
func (s *Server) renderContribution(w http.ResponseWriter, status int, page string, data contributionData) {
	if data.ErrorMessage == "" && data.State != "" && data.State != "found" && data.State != "invalid" {
		data.ErrorMessage = contributeStateMessage(data.State)
	}
	s.render(w, status, page, data)
}
