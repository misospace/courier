package dispatch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultTimeout = 30 * time.Second

	// maxPRFixExclusions is the Dispatch server contract ceiling on
	// exclude_pr_fix entries per request; requests beyond it are rejected
	// server-side, so the client truncates instead.
	maxPRFixExclusions = 32

	// outcomeWaitingExternal is the Dispatch task-report outcome Courier
	// sends for a recoverable dependency block (source.ResultBlockedExternal).
	// Dispatch is expected to treat it as a no-op for fix-attempt accounting:
	// do not advance generation, do not increment fixAttempts, do not park
	// the item for a human, and keep it discoverable so the source can
	// re-issue the work once the prerequisite is satisfied. A legacy
	// Dispatch that does not yet recognise this outcome will reject the
	// report; that is a known cross-repo gap tracked alongside #267.
	outcomeWaitingExternal = "waiting-external"

	// outcomeAlreadyDone is the Dispatch task-report outcome Courier sends
	// for a grounded already-done close proposal
	// (source.ResultAlreadyDone, #720). Dispatch requires the outcome on an
	// implement task together with a non-empty close criteria set and an
	// idempotency key, and answers with its own close resolution.
	outcomeAlreadyDone = "already_done"
)

var (
	ErrInvalidBaseURL       = errors.New("dispatch client: invalid base URL")
	ErrMissingToken         = errors.New("dispatch client: bearer token is required")
	ErrUnknownWorkID        = errors.New("dispatch client: invalid work item ID")
	ErrPRStateCheckerNeeded = errors.New("dispatch client: upstream pull request state checker is required")
)

// HTTPDoer is the HTTP transport used by HTTPClient.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Task is a Dispatch queue response.
type Task struct {
	Type        string       `json:"type"`
	ShouldRun   bool         `json:"shouldRun"`
	Issue       *Issue       `json:"issue,omitempty"`
	PullRequest *PullRequest `json:"pullRequest,omitempty"`
	PRFixItem   *PRFixItem   `json:"prFixItem,omitempty"`
	Reasons     []string     `json:"reasons,omitempty"`
}

// PRFixItem identifies one generation of a queued PR followup. FixType is
// the queue item's classified followup kind (misospace/dispatch#1327); it is
// carried, never interpreted as an execution capability. A "rebase" FixType
// names work Courier executes only through its own rebase path (#715) and
// only after the rebase capability contract is negotiated — it must never be
// mapped onto ordinary fix-pr work.
type PRFixItem struct {
	ID           string `json:"id"`
	Generation   int    `json:"generation"`
	HandoutToken string `json:"handoutToken,omitempty"`
	FixType      string `json:"fixType,omitempty"`
	// OriginalClaimant is the Dispatch identity that originally claimed or
	// holds this PR-fix item: the ownership-evidence contract Dispatch
	// #1569 supplies. Courier never infers ownership from the handout
	// recipient, the PR author, a branch prefix, or the fix type; the
	// claimant on the item is the only ownership evidence it consults.
	OriginalClaimant string `json:"originalClaimant,omitempty"`
}

// Issue identifies the issue associated with a Dispatch task.
type Issue struct {
	ID     string `json:"id"`
	Repo   string `json:"repoFullName"`
	Number int    `json:"number"`
}

// PullRequest identifies the pull request associated with a Dispatch task.
type PullRequest struct {
	Repo   string `json:"repoFullName"`
	Number int    `json:"number"`
	URL    string `json:"url,omitempty"`
	State  string `json:"state,omitempty"`
	Merged bool   `json:"merged,omitempty"`
	Closed bool   `json:"closed,omitempty"`
}

// PullRequestState is the upstream state used to reject stale follow-up work.
type PullRequestState struct {
	State  string
	Merged bool
}

// PullRequestStateChecker reads the current upstream pull request state.
type PullRequestStateChecker interface {
	CheckPullRequestState(context.Context, string, int) (PullRequestState, error)
}

// PullRequestStateCheckerFunc adapts a function to PullRequestStateChecker.
type PullRequestStateCheckerFunc func(context.Context, string, int) (PullRequestState, error)

func (f PullRequestStateCheckerFunc) CheckPullRequestState(ctx context.Context, repo string, number int) (PullRequestState, error) {
	return f(ctx, repo, number)
}

type workDescriptor struct {
	Type         string `json:"type"`
	IssueID      string `json:"issueId,omitempty"`
	IssueRepo    string `json:"issueRepo,omitempty"`
	IssueNumber  int    `json:"issueNumber,omitempty"`
	Repo         string `json:"repo,omitempty"`
	Number       int    `json:"number,omitempty"`
	PRURL        string `json:"prURL,omitempty"`
	PRFixID      string `json:"prFixId,omitempty"`
	Generation   int    `json:"generation,omitempty"`
	HandoutToken string `json:"handoutToken,omitempty"`
	// FixType preserves the queue item's followup kind through the opaque ID
	// so a rebase-marked attempt stays rebase-marked across recovery, launch,
	// and settlement (#715). It never changes the wire task type: every
	// Dispatch exchange still speaks followup-pr.
	FixType string `json:"fixType,omitempty"`
	// OriginalClaimant preserves the ownership evidence Dispatch supplies for
	// the PR-fix item through the opaque ID, so every boundary that can still
	// refuse the attempt — the claim, the hand-out acknowledgement, the
	// pre-launch revalidation, and the recovery path — revalidates the same
	// evidence the acceptance gate consulted (#761). It is never inferred from
	// the handout recipient, the pull-request author, a branch prefix, or the
	// fix type.
	OriginalClaimant string `json:"originalClaimant,omitempty"`
}

// APIError reports a non-2xx Dispatch response.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("Dispatch API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("Dispatch API returned HTTP %d: %s", e.StatusCode, e.Message)
}

// isPermanentRefusal reports the status codes that classify a failed
// exchange as a durable source refusal. 400 covers closed targets and
// request validation an identical report payload trips again; 403 covers a
// worker-bound identity acting on work it no longer owns. Contention, rate
// limits, and upstream outages arrive as 409, 429, or 5xx and stay
// retryable.
func isPermanentRefusal(statusCode int) bool {
	return statusCode == http.StatusBadRequest || statusCode == http.StatusForbidden
}

var _ source.HandoutAcker = (*HTTPClient)(nil)

// HTTPClient implements the Dispatch HTTP API behind the source adapter.
type HTTPClient struct {
	baseURL                  *url.URL
	token                    string
	agentName                string
	queueLane                string
	httpClient               HTTPDoer
	prStateChecker           PullRequestStateChecker
	prFixExclusionsEnabled   bool
	prFixExclusionLimit      int
	reportIdempotencyEnabled bool
	diagnostics              *connector.Emitter
	connectorName            string
	// ciParkUnsupported latches the permanent observation that this
	// Dispatch deployment has not adopted the CI park lifecycle routes at
	// all (internal/source/dispatch/cipark.go): a bare 404 on a park-family
	// POST is version-skew evidence, never a per-item condition, and once
	// latched every park-family exchange short-circuits without an HTTP
	// round trip for the rest of the process.
	ciParkUnsupported atomic.Bool
	// retryUnsupported latches the permanent observation that this Dispatch
	// deployment has not adopted the fenced attempt-requeue routes at all
	// (internal/source/dispatch/retry.go): a bare 404 on a requeue POST is
	// version-skew evidence, never a per-item condition, and once latched every
	// further requeue short-circuits without an HTTP round trip for the rest of
	// the process, so a durable retry request settles unsupported instead of
	// retrying a route the server will never take.
	retryUnsupported atomic.Bool
	// observationsUnsupported latches the permanent observation that this
	// Dispatch deployment has not adopted the execution-observation route at
	// all (internal/source/dispatch/observations.go): a bare 404/405/501 on the
	// observations POST is version-skew evidence, and once latched further
	// reports short-circuit without an HTTP round trip for the life of the
	// process.
	observationsUnsupported atomic.Bool
	// rebaseCapabilityEnabled declares that THIS connector is configured as a
	// rebase executor (#715): discovery sends the versioned rebase-only
	// intake declaration (dispatch#1533) and accepts rebase work only on the
	// exact matching intake acknowledgment. It is a per-binding operator
	// choice, never a startup default, so ordinary lanes keep negotiating
	// nothing and stay operational with Dispatch deployments that predate the
	// contract entirely.
	rebaseCapabilityEnabled bool
	// ownershipBindings is the configured-ownership authority for PR repair:
	// claimant Dispatch agent name → configured Dispatch queue lane. A
	// followup-pr handout is accepted only when its original claimant is
	// bound here to the receiving connector's own queue lane. A nil map
	// keeps the gate active — every claimant is then unknown — so the gate
	// is unconditional and an older Dispatch peer that supplies no evidence
	// has every fix-pr handout declined gracefully.
	ownershipBindings map[string]string
	// handoutDeclines records, per declined generation, that the decline was
	// already settled with the deployment: the recoverable release landed, or
	// the deployment refused it and the refusal stands as the evidence. The
	// latch covers both decline kinds — a rebase offer Courier cannot safely
	// execute, and a followup-pr handout its configured ownership bindings do
	// not authorize. A declined generation must not be re-released on every
	// later poll — one release per generation per process — so the decline
	// keeps surfacing with its ownership evidence intact while the source's
	// own stale-handout reclaimer remains the documented bounded recovery
	// path.
	handoutDeclines sync.Map
}

// NewClient creates a Dispatch HTTP client without a queue lane override.
func NewClient(baseURL, agentName, token string, timeout time.Duration, clients ...HTTPDoer) (*HTTPClient, error) {
	return newClient(baseURL, agentName, "", token, timeout, clients...)
}

// NewClientWithLane creates a Dispatch HTTP client with a queue lane override.
func NewClientWithLane(baseURL, agentName, queueLane, token string, timeout time.Duration, clients ...HTTPDoer) (*HTTPClient, error) {
	return newClient(baseURL, agentName, queueLane, token, timeout, clients...)
}

// WithPullRequestStateChecker configures stale follow-up detection.
func (c *HTTPClient) WithPullRequestStateChecker(checker PullRequestStateChecker) *HTTPClient {
	c.prStateChecker = checker
	return c
}

// WithReportIdempotency configures whether the Dispatch report handler accepts idempotencyKey.
func (c *HTTPClient) WithReportIdempotency(enabled bool) *HTTPClient {
	c.reportIdempotencyEnabled = enabled
	return c
}

// WithPRFixExclusions opts into Dispatch's acknowledged PR-fix exclusion contract.
// Enable only after deployment has confirmed support for that contract.
func (c *HTTPClient) WithPRFixExclusions(enabled bool) *HTTPClient {
	c.prFixExclusionsEnabled = enabled
	return c
}

// WithPRFixExclusionLimit overrides the exclusion contract maximum for this
// client. The limit is connector-declared data exported through
// ExclusionLimit; the truncation behavior itself is unchanged. A limit below
// one is ignored and keeps the existing default of maxPRFixExclusions.
func (c *HTTPClient) WithPRFixExclusionLimit(limit int) *HTTPClient {
	if limit >= 1 {
		c.prFixExclusionLimit = limit
	}
	return c
}

// WithRebaseCapability declares this connector a rebase executor under the
// versioned rebase-only intake contract (#715, misospace/dispatch#1532 as
// shipped in dispatch#1533). Enabled per Dispatch binding — never by default
// — discovery then declares work_scope=rebase-only, work_scope_version=1 and
// capabilities=rebase-executor:1 on every next-task call, and enforces,
// fail-closed:
//
//   - REBASE-marked work is accepted only on the exact v1 intake
//     acknowledgment (contract version 1, the rebase-only scope, the scope
//     enforced, rebase-executor granted at the requested version), and only
//     as rebase mode — never mapped onto fix-pr;
//   - ordinary work returned to a rebase-only connector is an ignored scope,
//     a protocol error, and never accepted;
//   - an absent intake acknowledgment (an older Dispatch), a different
//     contract version, an unenforced scope, and a capability verdict that
//     is not granted or requested at another version are all failed
//     negotiation, never success.
//
// Connectors that do not enable this option keep negotiating nothing: their
// discovery bytes are unchanged, which is what keeps existing lanes
// operational with Dispatch deployments that predate the contract.
func (c *HTTPClient) WithRebaseCapability(enabled bool) *HTTPClient {
	c.rebaseCapabilityEnabled = enabled
	return c
}

// WithOwnershipBindings installs the configured-ownership authority for PR
// repair (#761): a map of claimant Dispatch agent name → configured Dispatch
// queue lane. The map is copied on set, so later mutation of the caller's
// map cannot change a live client's verdicts. The gate itself is always on:
// with no bindings configured, every claimant is unknown and every fix-pr
// handout is declined through the receipt-fenced ownership path.
func (c *HTTPClient) WithOwnershipBindings(bindings map[string]string) *HTTPClient {
	installed := make(map[string]string, len(bindings))
	for claimant, lane := range bindings {
		installed[claimant] = lane
	}
	c.ownershipBindings = installed
	return c
}

// WithConnectorDiagnostics attaches the connector diagnostic envelope the
// client emits its own diagnostics through, together with the bounded
// connector identity every emission carries. A nil emitter leaves every
// emission a no-op, so existing call sites are unchanged. Registration of the
// connector name, vocabulary, exclusion limit, and capability catalogue
// happens once at adapter construction.
func (c *HTTPClient) WithConnectorDiagnostics(em *connector.Emitter, connectorName string) *HTTPClient {
	validateConnectorName(connectorName)
	c.diagnostics = em
	c.connectorName = connectorName
	return c
}

func newClient(baseURL, agentName, queueLane, token string, timeout time.Duration, clients ...HTTPDoer) (*HTTPClient, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, ErrInvalidBaseURL
	}
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, ErrInvalidBaseURL
	}
	if strings.TrimSpace(agentName) == "" {
		return nil, errors.New("dispatch client: agent name is required")
	}
	if strings.TrimSpace(token) == "" {
		return nil, ErrMissingToken
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if len(clients) > 1 {
		return nil, errors.New("dispatch client: at most one HTTP client may be supplied")
	}
	httpClient := HTTPDoer(&http.Client{Timeout: timeout})
	if len(clients) == 1 && clients[0] != nil {
		httpClient = clients[0]
	}
	return &HTTPClient{
		reportIdempotencyEnabled: true,
		baseURL:                  u,
		token:                    strings.TrimSpace(token),
		agentName:                strings.TrimSpace(agentName),
		queueLane:                strings.TrimSpace(queueLane),
		httpClient:               httpClient,
		prFixExclusionLimit:      maxPRFixExclusions,
		connectorName:            defaultConnectorName,
	}, nil
}

func (c *HTTPClient) Discover(ctx context.Context) ([]source.WorkItem, error) {
	return c.discover(ctx, nil)
}

func (c *HTTPClient) DiscoverForCapacity(ctx context.Context, capacityID string, exclusions []source.WorkItem) ([]source.WorkItem, error) {
	query := url.Values{"capacity": {capacityID}}
	if len(exclusions) > 0 && c.prFixExclusionsEnabled {
		return c.discoverWithExclusions(ctx, query, exclusions)
	}
	return c.discover(ctx, query)
}

// DiscoverExcluding discovers work while asking Dispatch to skip the excluded
// PR-fix attempts. Exclusions arrive most-relevant-first from the runner; when
// more unique valid entries qualify than the server contract allows, the most
// relevant head is kept and the tail is dropped with a warning — an overflow
// must degrade discovery, not starve it (#286). Dropped entries may be
// re-offered; the runner's same-identity dedupe absorbs the repeat without a
// duplicate run.
func (c *HTTPClient) DiscoverExcluding(ctx context.Context, exclusions []source.WorkItem) ([]source.WorkItem, error) {
	if !c.prFixExclusionsEnabled || len(exclusions) == 0 {
		return c.Discover(ctx)
	}
	return c.discoverWithExclusions(ctx, nil, exclusions)
}

func (c *HTTPClient) discoverWithExclusions(ctx context.Context, query url.Values, exclusions []source.WorkItem) ([]source.WorkItem, error) {
	if query == nil {
		query = url.Values{}
	}
	query.Set("exclude_pr_fix_version", "1")
	seen := make(map[string]struct{})
	values := make([]string, 0, min(len(exclusions), c.prFixExclusionLimit))
	for _, item := range exclusions {
		d, err := decodeWorkID(item.ID)
		if err != nil || d.Type != "followup-pr" || d.PRFixID == "" || d.Generation < 1 || d.Repo == "" {
			continue
		}
		repo := strings.ToLower(d.Repo)
		if !dispatchRepoPattern.MatchString(repo) || d.Number > maxSafeInteger || d.Generation > maxSafeInteger {
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonInvalidDescriptor)
			return nil, fmt.Errorf("dispatch client: invalid PR-fix exclusion descriptor")
		}
		value := fmt.Sprintf("%s#%d@%d", repo, d.Number, d.Generation)
		if len(value) > 300 {
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonInvalidDescriptor)
			return nil, fmt.Errorf("dispatch client: PR-fix exclusion exceeds maximum 300 characters")
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		if len(seen) <= c.prFixExclusionLimit {
			values = append(values, value)
		}
	}
	if dropped := len(seen) - c.prFixExclusionLimit; dropped > 0 {
		log.FromContext(ctx).Info("dispatch client: PR-fix exclusions exceed contract maximum, keeping the most relevant", "agent", c.agentName, "kept", c.prFixExclusionLimit, "dropped", dropped)
	}
	c.diagnostics.ExclusionCycle(c.connectorName, len(seen), len(values))
	for _, value := range values {
		query.Add("exclude_pr_fix", value)
	}
	if len(values) == 0 {
		query.Del("exclude_pr_fix_version")
		return c.discover(ctx, query)
	}
	return c.discover(ctx, query)
}

const maxSafeInteger = 9007199254740991

var dispatchRepoPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}/[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$`)

type nextTaskResponse struct {
	Task
	PRFixExclusions json.RawMessage `json:"prFixExclusions"`
	// Intake is Dispatch's versioned intake acknowledgment (rebase.go),
	// returned only to a poll that declared an intake contract. Absent
	// otherwise — which is exactly how an older Dispatch (no intake field)
	// fails closed.
	Intake json.RawMessage `json:"intake"`
}

func (c *HTTPClient) discover(ctx context.Context, query url.Values) (items []source.WorkItem, err error) {
	// Availability is connector-declared (§3.5), observed per discovery
	// attempt: a successful read of any shape is available, a transport
	// failure is unreachable, and every other failure is degraded.
	defer func() {
		if err != nil {
			if ex := exchangeOf(err); ex != nil && ex.reason == reasonTransportError {
				c.diagnostics.Availability(c.connectorName, connector.AvailabilityUnreachable)
				return
			}
			c.diagnostics.Availability(c.connectorName, connector.AvailabilityDegraded)
			return
		}
		c.diagnostics.Availability(c.connectorName, connector.AvailabilityAvailable)
	}()
	var response nextTaskResponse
	endpoint := "/api/agents/" + url.PathEscape(c.agentName) + "/next-task"
	queryParts := make([]string, 0, 2)
	if c.rebaseCapabilityEnabled {
		// The versioned rebase-only intake declaration rides every next-task
		// call of a rebase executor connector (#715): the rebase-only work
		// scope, the scope version, and the rebase-executor capability, all
		// at the exact values Dispatch negotiated in dispatch#1533. A
		// deployment that has not adopted the contract refuses the unknown
		// parameters (400) or ignores them; either way the response
		// validation below is what keeps that skew from becoming silent
		// acceptance.
		queryParts = append(queryParts,
			workScopeQueryParam+"="+rebaseOnlyScope,
			workScopeVersionQueryParam+"="+rebaseOnlyScopeVersion,
			capabilityQueryParam+"="+rebaseCapabilityRequest)
	}
	if query != nil {
		if encoded := query.Encode(); encoded != "" {
			queryParts = append(queryParts, encoded)
		}
	}
	if len(queryParts) > 0 {
		endpoint += "?" + strings.Join(queryParts, "&")
	}
	if err := c.do(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	if query != nil && query.Has("exclude_pr_fix_version") {
		var ack map[string]json.RawMessage
		if len(response.PRFixExclusions) == 0 {
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalAck, reasonAckMissing)
			return nil, fmt.Errorf("dispatch client: missing or invalid PR-fix exclusions acknowledgment")
		}
		if json.Unmarshal(response.PRFixExclusions, &ack) != nil || ack == nil {
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalAck, reasonAckInvalid)
			return nil, fmt.Errorf("dispatch client: missing or invalid PR-fix exclusions acknowledgment")
		}
		version, ok := ack["version"]
		var versionValue int
		if !ok || json.Unmarshal(version, &versionValue) != nil || versionValue != 1 {
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalAck, reasonAckVersionUnsupported)
			return nil, fmt.Errorf("dispatch client: unsupported PR-fix exclusions acknowledgment")
		}
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalAck, reasonAckOK)
	}
	task := response.Task
	if !task.ShouldRun || task.Type == "idle" {
		if c.rebaseCapabilityEnabled {
			// Idle answers nothing about the intake contract, so idleness is
			// never read as negotiation success. When the server did not
			// answer the request at all, surface it once per process so an
			// operator sees a rebase lane polling a peer that cannot speak
			// the contract — without turning every idle poll into a failure.
			if negotiated, reason := validateRebaseIntake(response.Intake); !negotiated && reason == reasonRebaseCapabilityMissing {
				c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonRebaseCapabilityMissing)
			}
		}
		return nil, nil
	}
	if task.Type != "implement" && task.Type != "followup-pr" {
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonUnsupportedTaskType)
		return nil, fmt.Errorf("dispatch client: unsupported task type %q", task.Type)
	}
	if c.rebaseCapabilityEnabled {
		negotiated, reason := validateRebaseIntake(response.Intake)
		if !negotiated {
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reason)
			// Old Dispatch stamped the handout BEFORE returning this task:
			// the queue write (agentHandouts push, handout receipt, attemptType
			// lane stamp) is committed before next-task responds. Refusing
			// with the negotiation error alone would leave that live handout
			// reserved until the source's stale-handout recovery. When the
			// offer carries a valid receipt, release that exact handout
			// through the bounded decline path — generation-fenced, at most
			// once per generation, never fabricated, never accepted — before
			// surfacing the failure (#715, dispatch#1533 review). An offer
			// without a valid receipt (issue implementation work carries no
			// prFixItem) has nothing to release.
			if task.PRFixItem != nil && strings.TrimSpace(task.PRFixItem.ID) != "" && task.PRFixItem.Generation >= 1 {
				return nil, c.declineRebase(ctx, task, fmt.Sprintf("intake negotiation failed (%s)", reason))
			}
			return nil, fmt.Errorf("dispatch client: rebase intake negotiation failed (%s); offered %s work is declined, never accepted", reason, task.Type)
		}
	}
	itemID, err := taskWorkItem(task)
	if err != nil {
		return nil, err
	}
	repo, ref, err := taskRef(task)
	if err != nil {
		return nil, err
	}
	mode := "resolve-issue"
	if task.Type == "followup-pr" {
		// Configured-ownership gate (#761): a followup-pr handout is accepted
		// only when the Dispatch evidence on the item names an original
		// claimant that Courier's configured dispatch bindings bind to this
		// connector's own queue lane. The gate runs before the merged/closed
		// stale check so foreign work is never marked stale, and before the
		// rebase classification so a foreign or evidence-less rebase offer is
		// declined by ownership even when the capability is negotiated. The
		// configured queue-lane values are compared with exact string
		// equality; no alias normalization, prefix match, or case folding is
		// applied, and LaneProfile names are never Dispatch lane identities.
		claimant := ""
		if task.PRFixItem != nil {
			claimant = strings.TrimSpace(task.PRFixItem.OriginalClaimant)
		}
		if reason, why := c.ownershipVerdict(claimant); reason != "" {
			return nil, c.declineOwnership(ctx, task, reason, why)
		}
		state, err := c.checkPullRequestState(ctx, task.PullRequest)
		if err != nil {
			return nil, err
		}
		if state.Merged || strings.EqualFold(state.State, "closed") {
			if task.PRFixItem != nil {
				if err := c.markPRFixStale(ctx, task.PullRequest, state, task.PRFixItem.Generation); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
		mode = "fix-pr"
		if task.PRFixItem != nil && isRebaseFixType(task.PRFixItem.FixType) {
			// REBASE-marked work rides this transport but never rides the
			// fix-pr execution path (#715). Accept it only as rebase work,
			// only when this connector negotiated the capability, and only
			// with a valid queue receipt; otherwise decline it through the
			// defined release path and surface why — a silent fallback to
			// fix-pr is the exact failure this contract exists to prevent.
			if !c.rebaseCapabilityEnabled {
				return nil, c.declineRebase(ctx, task, "rebase capability is not negotiated for this connector")
			}
			if strings.TrimSpace(task.PRFixItem.ID) == "" || task.PRFixItem.Generation < 1 {
				c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonRebaseTaskInvalid)
				return nil, fmt.Errorf("dispatch client: rebase task carries no valid PR-fix receipt")
			}
			mode = "rebase"
		} else if c.rebaseCapabilityEnabled {
			// A negotiated rebase-only connector was offered ordinary work:
			// the rebase-only scope was ignored. That is a protocol error,
			// not ordinary work, and it is never accepted — accepting it
			// would silently broaden the rebase lane back into the fix loop.
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonRebaseFilterIgnored)
			return nil, fmt.Errorf("dispatch client: rebase-only connector was offered non-rebase work; the rebase-only scope was ignored and the work is declined unclaimed")
		}
	}
	context := ""
	if task.PRFixItem != nil {
		var receipt dispatchContext
		receipt.PRFixID, receipt.Generation = task.PRFixItem.ID, task.PRFixItem.Generation
		receipt.HandoutToken = task.PRFixItem.HandoutToken
		receipt.FixType = task.PRFixItem.FixType
		encoded, _ := json.Marshal(receipt)
		context = string(encoded)
	}
	return []source.WorkItem{{ID: itemID, Context: context, Mode: mode, Repo: repo, Ref: ref, Assignment: newAssignment(task.Reasons)}}, nil
}

type dispatchContext struct {
	ClaimToken   string `json:"claimToken,omitempty"`
	PRFixID      string `json:"prFixId,omitempty"`
	Generation   int    `json:"generation,omitempty"`
	HandoutToken string `json:"handoutToken,omitempty"`
	// FixType survives in the durable receipt so a rebase-marked attempt
	// stays rebase-marked through recovery and settlement (#715).
	FixType string `json:"fixType,omitempty"`
}

func encodeDispatchContext(existing, token, fixID string, generation int) string {
	var value dispatchContext
	_ = json.Unmarshal([]byte(existing), &value)
	if token != "" {
		value.ClaimToken = token
	}
	if fixID != "" {
		value.PRFixID, value.Generation = fixID, generation
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func (c *HTTPClient) PrepareCapacityRecovery(ctx context.Context, id, opaqueContext string) (source.CapacityRecoveryProof, error) {
	// Resolve the issue ID the same way Claim does: Dispatch's task offer may
	// carry only repo+number, and the fence still rides the claim receipt, so
	// the state lookup adds no trust the claim did not already establish.
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return source.CapacityRecoveryProof{}, err
	}
	var receipt dispatchContext
	if json.Unmarshal([]byte(opaqueContext), &receipt) != nil {
		return source.CapacityRecoveryProof{}, source.ErrUnsupportedCapacityRecovery
	}
	if d.Type == "implement" && d.IssueID != "" && receipt.ClaimToken != "" {
		return source.CapacityRecoveryProof{FenceToken: receipt.ClaimToken, WorkIdentity: WorkIdentity(id), SafetyProof: "trusted workspace safety established by core"}, nil
	}
	if d.Type == "followup-pr" && d.PRFixID != "" && d.Generation > 0 && receipt.PRFixID == d.PRFixID && receipt.Generation == d.Generation && receipt.HandoutToken != "" {
		return source.CapacityRecoveryProof{FenceToken: receipt.HandoutToken, WorkIdentity: WorkIdentity(id), SafetyProof: "trusted workspace safety established by core"}, nil
	}
	return source.CapacityRecoveryProof{}, source.ErrUnsupportedCapacityRecovery
}

func (c *HTTPClient) SettleCapacityExhaustion(ctx context.Context, id, opaqueContext string, handoff source.CapacityHandoff) error {
	// Same resolution as PrepareCapacityRecovery: the server-side lease check
	// on claimToken remains the authority for both paths.
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return err
	}
	var receipt dispatchContext
	if json.Unmarshal([]byte(opaqueContext), &receipt) != nil || handoff.FenceToken == "" || handoff.WorkIdentity != WorkIdentity(id) {
		return source.ErrUnsupportedCapacityRecovery
	}
	if d.Type == "implement" && d.IssueID != "" && receipt.ClaimToken == handoff.FenceToken {
		if d.IssueRepo == "" || d.IssueNumber <= 0 || d.IssueID == "" {
			return source.ErrUnsupportedCapacityRecovery
		}
		marker := capacityReceiptMarker(handoff.FenceToken, handoff.IdempotencyKey, handoff.WorkIdentity)
		settlement := capacitySettlementTag(handoff.IdempotencyKey, handoff.WorkIdentity)
		if c.issueCapacityHandoffAudited(ctx, d, marker, settlement) {
			return nil
		}
		postHandoff := func(claimToken, receiptMarker string) error {
			note := receiptMarker
			if settlement != "" {
				note += "; settlement:" + settlement
			}
			body := map[string]any{"issueId": d.IssueID, "repoFullName": d.IssueRepo, "issueNumber": d.IssueNumber, "agentName": c.agentName, "claimToken": claimToken, "capacity": handoff.CapacityIdentity, "note": note}
			var response struct {
				Success bool `json:"success"`
			}
			if err := c.do(ctx, http.MethodPost, "/api/issues/capacity-handoff", body, &response); err != nil {
				return err
			}
			if response.Success {
				return nil
			}
			return errors.New("dispatch client: capacity handoff response did not confirm success")
		}
		err := postHandoff(handoff.FenceToken, marker)
		if err == nil {
			return nil
		}
		if c.issueCapacityHandoffAudited(ctx, d, marker, settlement) {
			return nil
		}
		// Claim churn around admission rotates the lease token after the run's
		// receipt was persisted, and a released claim leaves no fence at all.
		// Re-presenting the claim recovers the live token — Dispatch keeps the
		// token across same-agent renewals — so one bounded retry fences
		// against the live lease instead of an inert one, and settlement of a
		// BlockedExternal capacity run never wedges (#388). Refusals that are
		// not rotation (new owner, human hold, expiry, closed issue) never
		// re-claim.
		if claimLeaseRotated(err) {
			if adopted, adoptErr := c.reacquireClaimToken(ctx, d); adoptErr == nil && adopted != "" && adopted != handoff.FenceToken {
				adoptedMarker := capacityReceiptMarker(adopted, handoff.IdempotencyKey, handoff.WorkIdentity)
				if c.issueCapacityHandoffAudited(ctx, d, adoptedMarker, settlement) {
					return nil
				}
				if retryErr := postHandoff(adopted, adoptedMarker); retryErr == nil {
					return nil
				} else if c.issueCapacityHandoffAudited(ctx, d, adoptedMarker, settlement) {
					return nil
				} else {
					return classifyCapacityHandoff(retryErr)
				}
			}
		}
		return classifyCapacityHandoff(err)
	}
	if d.Type == "followup-pr" && receipt.HandoutToken == handoff.FenceToken && receipt.PRFixID == d.PRFixID && receipt.Generation == d.Generation && d.PRFixID != "" && d.Generation > 0 {
		body := map[string]any{"taskType": d.Type, "outcome": "capacity-exhausted", "repoFullName": d.Repo, "pullRequestNumber": d.Number, "capacity": handoff.CapacityIdentity, "idempotencyKey": handoff.IdempotencyKey, "prFixItem": map[string]any{"id": d.PRFixID, "generation": d.Generation, "handoutToken": handoff.FenceToken}}
		var response struct {
			Resolution struct {
				Matched bool   `json:"matched"`
				Action  string `json:"action"`
				ItemID  string `json:"itemId"`
			} `json:"prFixResolution"`
		}
		if err := c.do(ctx, http.MethodPost, "/api/agents/"+url.PathEscape(c.agentName)+"/tasks/report", body, &response); err != nil {
			return classifyCapacityHandoff(err)
		}
		if !response.Resolution.Matched || response.Resolution.ItemID != d.PRFixID || response.Resolution.Action != "deferred" {
			return source.ErrStaleCapacitySettlement
		}
		return nil
	}
	return source.ErrUnsupportedCapacityRecovery
}

func capacityReceiptMarker(claimToken, idempotencyKey, workIdentity string) string {
	sum := sha256.Sum256([]byte(claimToken + "\x00" + idempotencyKey + "\x00" + workIdentity))
	return fmt.Sprintf("capacity-receipt:%x", sum[:])
}

// capacitySettlementTag is the token-independent identity of one settlement
// attempt, stamped onto every receipt note beside the token-specific marker.
// A handoff that lands under an adopted token while its success is never
// persisted (lost response, controller crash) would otherwise be invisible to
// later passes, whose receipt still names the original token and whose re-claim
// mints a third one — each pass deriving a fresh marker and posting a duplicate
// settlement (#395). The tag lets any pass replay the landed row from durable
// audit evidence without conflating a previous settlement: the key is the run's
// settlement identity, unique per attempt, and only this adapter writes it.
func capacitySettlementTag(idempotencyKey, workIdentity string) string {
	if idempotencyKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("settlement\x00" + idempotencyKey + "\x00" + workIdentity))
	return fmt.Sprintf("%x", sum[:])
}

type capacityAuditEntry struct {
	Action      string `json:"action"`
	Success     bool   `json:"success"`
	Repo        string `json:"repoFullName"`
	IssueNumber int    `json:"issueNumber"`
	IssueID     string `json:"issueId"`
	Notes       string `json:"notes"`
}

func (c *HTTPClient) issueCapacityHandoffAudited(ctx context.Context, d workDescriptor, marker, settlement string) bool {
	endpoint := "/api/audit?repo=" + url.QueryEscape(d.IssueRepo) + "&limit=100"
	var entries []capacityAuditEntry
	if err := c.do(ctx, http.MethodGet, endpoint, nil, &entries); err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.Action == "issue_capacity_handoff" && entry.Success && entry.Repo == d.IssueRepo && entry.IssueNumber == d.IssueNumber && entry.IssueID == d.IssueID {
			// A note carries this settlement if it holds the receipt marker for
			// a token this pass knows (legacy notes held only the marker), or
			// the settlement tag written under any token (#395).
			if entry.Notes == marker || strings.HasSuffix(entry.Notes, "; "+marker) ||
				(settlement != "" && strings.HasSuffix(entry.Notes, "; settlement:"+settlement)) {
				return true
			}
		}
	}
	return false
}

func (c *HTTPClient) Claim(ctx context.Context, id string) error {
	_, err := c.ClaimWithReceipt(ctx, id, "")
	return err
}

func (c *HTTPClient) ClaimWithReceipt(ctx context.Context, id, existingContext string) (string, error) {
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return "", err
	}
	if d.Type == "followup-pr" {
		// The configured-ownership gate is rechecked at the claim/admission
		// boundary (#761), not only where the handout was discovered: the
		// bindings this connector enforces are the authority, and an attempt
		// whose evidence does not name an authorized claimant must not be
		// admitted for execution. The refusal releases the reservation with
		// the exact receipt and keeps the item discoverable for the lane that
		// owns it.
		if err := c.refuseOwnership(ctx, d); err != nil {
			return "", err
		}
		return encodeDispatchContext(existingContext, "", d.PRFixID, d.Generation), nil
	}
	if d.IssueID == "" {
		return existingContext, nil
	}
	body := map[string]any{"issueId": d.IssueID, "repoFullName": d.IssueRepo, "issueNumber": d.IssueNumber, "agentName": c.agentName}
	var response struct {
		ClaimToken string `json:"claimToken"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/issues/claim", body, &response); err != nil {
		return "", err
	}
	if strings.TrimSpace(response.ClaimToken) == "" {
		// Older servers can execute ordinary work but cannot fence recovery.
		return existingContext, nil
	}
	return encodeDispatchContext(existingContext, response.ClaimToken, "", 0), nil
}

func (c *HTTPClient) Release(ctx context.Context, id string) error {
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return err
	}
	if d.Type == "followup-pr" || d.IssueID == "" {
		return nil
	}
	body := map[string]any{
		"issueId":      d.IssueID,
		"repoFullName": d.IssueRepo,
		"issueNumber":  d.IssueNumber,
		"agentName":    c.agentName,
	}
	return c.do(ctx, http.MethodPost, "/api/issues/unclaim", body, nil)
}

func (c *HTTPClient) SetStatus(ctx context.Context, id, status string) error {
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return err
	}
	if d.Type == "followup-pr" {
		return nil
	}
	if strings.TrimPrefix(status, "status/") == "needs-human" {
		status = "blocked"
	}
	if d.IssueID == "" {
		return nil
	}
	body := map[string]any{
		"issueId":      d.IssueID,
		"repoFullName": d.IssueRepo,
		"issueNumber":  d.IssueNumber,
		"status":       strings.TrimPrefix(status, "status/"),
		"agentName":    c.agentName,
	}
	if strings.TrimPrefix(status, "status/") == "blocked" {
		body["blockedReason"] = "Courier run requires human intervention"
	}
	return c.do(ctx, http.MethodPost, "/api/issues/status", body, nil)
}

func (c *HTTPClient) checkPullRequestState(ctx context.Context, pullRequest *PullRequest) (PullRequestState, error) {
	if pullRequest == nil || pullRequest.Repo == "" || pullRequest.Number <= 0 {
		return PullRequestState{}, errors.New("dispatch client: followup task has no valid pull request")
	}
	if c.prStateChecker == nil {
		return PullRequestState{}, ErrPRStateCheckerNeeded
	}
	return c.prStateChecker.CheckPullRequestState(ctx, pullRequest.Repo, pullRequest.Number)
}

// asSuperseded maps only an explicitly stale generation to a terminal source
// result. A superseded generation is a normal, expected identity outcome
// (§3.4): it counts under identity/generation-advanced, and the underlying
// 409 exchange — deferred past the do() choke point — is absorbed rather
// than also counted as a protocol error. A 409 that is not a stale
// generation is an ordinary failure and counts under its HTTP class here,
// where the classification is final.
func (c *HTTPClient) asSuperseded(err error) error {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict &&
		(strings.Contains(strings.ToLower(apiErr.Message), "generation mismatch") || strings.Contains(strings.ToLower(apiErr.Message), "expected generation")) {
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalIdentity, reasonIdentityGenerationAdvanced)
		absorbExchange(err)
		return fmt.Errorf("%w: %w", source.ErrSuperseded, err)
	}
	c.countPendingExchange(err)
	return err
}

// claimLeaseRotated reports a Dispatch 409 proving the receipt's claim token is
// no longer the live fence: a re-claim rotated the lease (stale_claim_token) or
// the fenced claim is gone entirely (no_active_claim), leaving the same agent
// free to re-present its own claim. Ownership changes, human holds, expiry, and
// closed/done refusals are not rotation — re-claiming through one would
// overwrite a new owner or reverse a human — so they stay on the ordinary
// classification path. Dispatch currently returns these refusals as plain
// {error: message} bodies without machine-readable codes, so the code is
// honored when present and the message otherwise.
func claimLeaseRotated(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		return false
	}
	var response struct {
		Code string `json:"code"`
	}
	if json.Unmarshal([]byte(apiErr.Message), &response) == nil {
		switch response.Code {
		case "stale_claim_token", "no_active_claim":
			return true
		case "claim_expired", "ownership_changed", "human_hold", "issue_done", "issue_closed", "handoff_race_lost":
			return false
		}
	}
	message := strings.ToLower(apiErr.Message)
	return strings.Contains(message, "claimtoken does not match") ||
		strings.Contains(message, "re-issued claim invalidates") ||
		strings.Contains(message, "no active tokened claim")
}

// reacquireClaimToken re-presents this agent's claim to recover the token the
// live lease holds. Dispatch renews a same-agent lease without rotating its
// token, so this adopts the winning fence rather than stealing a newer claim's.
// Any refusal fails here and settlement stays on its original error.
func (c *HTTPClient) reacquireClaimToken(ctx context.Context, d workDescriptor) (string, error) {
	body := map[string]any{"issueId": d.IssueID, "repoFullName": d.IssueRepo, "issueNumber": d.IssueNumber, "agentName": c.agentName}
	var response struct {
		ClaimToken string `json:"claimToken"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/issues/claim", body, &response); err != nil {
		return "", err
	}
	return strings.TrimSpace(response.ClaimToken), nil
}

// classifyCapacityHandoff maps explicit ownership/human conflicts to stale work;
// contention and infrastructure failures remain retryable, unknown codes unchanged.
func classifyCapacityHandoff(err error) error {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		return err
	}
	var response struct {
		Code string `json:"code"`
	}
	if json.Unmarshal([]byte(apiErr.Message), &response) != nil {
		return err
	}
	switch response.Code {
	case "stale_claim_token", "claim_expired", "ownership_changed", "human_hold", "no_active_claim", "issue_done", "issue_closed":
		return fmt.Errorf("%w: %w", source.ErrStaleCapacitySettlement, err)
	case "handoff_race_lost":
		return err
	default:
		return err
	}
}

// AckHandout acknowledges the PR-fix hand-out stamped on a queue-backed
// follow-up attempt (dispatch#1211). Dispatch records the ack per
// `(agent, generation)` the moment the worker's run durably materializes, so
// the stale-handout reclaimer can tell a live attempt from a lost one and a
// settled park leaves the hand-out fully accounted (#456). The call is
// generation-pinned and idempotent server-side: a repeat ack is a success,
// not an error. Only the endpoint's known moot refusals prove the hand-out
// state has already moved on and degrade to a no-op: 404 with the known
// `pr-fix item not found` response (the item is gone), 403 with
// `reason: "not-handed"` (the hand-out was never ours), or 409 with a known
// moot reason (the generation was re-issued, the item left QUEUED, or the
// hand-out released). Any other 403 or 404 — worker-scope or auth
// rejections, a non-bearer auth mode, an older deployment, or a missing
// route — is not proof the ledger moved on and returns an error, as does an
// unknown 409 reason, a transport failure, or a server failure: the hand-out
// stays in the reclaimable state the reclaimer exists for, and the caller
// logs without blocking materialization.
func (c *HTTPClient) AckHandout(ctx context.Context, item source.WorkItem) error {
	d, err := decodeWorkID(item.ID)
	if err != nil {
		return fmt.Errorf("dispatch client: decode work item for hand-out ack: %w", err)
	}
	if d.Type != "followup-pr" || d.PRFixID == "" || d.Generation < 1 {
		// Not a queue-backed PR-fix attempt: nothing to acknowledge.
		return nil
	}
	// The configured-ownership gate is rechecked before the hand-out is
	// acknowledged (#761): an acknowledgement tells Dispatch this agent owns
	// the attempt, so a hand-out the configured bindings do not authorize must
	// never be acknowledged. The refusal releases the reservation with the
	// exact receipt and keeps the item discoverable for the lane that owns it.
	if err := c.refuseOwnership(ctx, d); err != nil {
		return err
	}
	repo := strings.ToLower(d.Repo)
	if d.Repo == "" || !dispatchRepoPattern.MatchString(repo) || d.Number <= 0 || d.Number > maxSafeInteger || d.Generation > maxSafeInteger {
		return fmt.Errorf("dispatch client: invalid PR-fix hand-out descriptor")
	}
	err = c.do(ctx, http.MethodPost, "/api/pr-fix-queue/ack", map[string]any{
		"repo": repo, "pr": d.Number, "generation": d.Generation, "agentName": c.agentName,
	}, nil)
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case http.StatusNotFound:
			// Only the endpoint's known not-found response proves the item
			// is gone; a route-level 404 (older deployment, missing route)
			// is not ledger evidence and must surface.
			if strings.Contains(strings.ToLower(apiErr.Message), "pr-fix item not found") {
				return nil
			}
		case http.StatusForbidden:
			// Only the known not-handed refusal proves the hand-out was
			// never ours; a worker-scope, auth-mode, or other 403 is a
			// configuration failure, not ledger evidence, and must surface.
			var response struct {
				Reason string `json:"reason"`
			}
			if json.Unmarshal([]byte(apiErr.Message), &response) == nil && response.Reason == "not-handed" {
				return nil
			}
		case http.StatusConflict:
			// Only the known moot refusals are no-ops; an unknown refusal
			// reason must surface, never silently drop ledger evidence.
			var response struct {
				Reason string `json:"reason"`
			}
			if json.Unmarshal([]byte(apiErr.Message), &response) == nil {
				switch response.Reason {
				case "generation-mismatch", "not-queued", "not-stamped":
					return nil
				}
			}
		}
	}
	// A 4xx refusal that reached here is a real failure, not ledger evidence;
	// its protocol-error class was deferred past the choke point and is
	// counted now, once.
	c.countPendingExchange(err)
	return err
}

func (c *HTTPClient) markPRFixBlocked(ctx context.Context, d workDescriptor, note string) error {
	if d.Type != "followup-pr" || d.PRFixID == "" {
		return nil
	}
	if strings.TrimSpace(note) == "" {
		note = "Courier run requires human intervention"
	}
	return c.asSuperseded(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
		"repo": d.Repo, "pr": d.Number, "status": "BLOCKED", "note": note,
		"generation": d.Generation,
	}, nil))
}

// markPRFixWaitingExternal records a Courier-classified dependency wait on
// the issued PR-fix generation. Unlike a BLOCKED mark it must not surface to
// a human; unlike a STALE mark it must not retire the work. Dispatch treats
// it as a recoverable, generation-fenced wait: the attempt is not reissued,
// fixAttempts is not advanced, and the item is left discoverable for the
// next cycle once the prerequisite is satisfied. See DESIGN.md §267 for the
// cross-repo contract this depends on.
func (c *HTTPClient) markPRFixWaitingExternal(ctx context.Context, d workDescriptor, note string) error {
	if d.Type != "followup-pr" || d.PRFixID == "" {
		return nil
	}
	if strings.TrimSpace(note) == "" {
		note = "Courier run is waiting on an external prerequisite"
	}
	return c.asSuperseded(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
		"repo": d.Repo, "pr": d.Number, "status": "WAITING_EXTERNAL", "note": note,
		"generation": d.Generation,
	}, nil))
}

func (c *HTTPClient) markPRFixStale(ctx context.Context, pullRequest *PullRequest, state PullRequestState, generation int) error {
	if pullRequest == nil {
		return errors.New("dispatch client: cannot mark missing pull request stale")
	}
	note := "upstream pull request is closed without merge"
	if state.Merged {
		note = "upstream pull request is merged"
	}
	return c.asSuperseded(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
		"repo": pullRequest.Repo, "pr": pullRequest.Number, "status": "STALE", "note": note,
		"generation": generation,
	}, nil))
}

func (c *HTTPClient) reportTaskWithPR(ctx context.Context, d workDescriptor, outcome, failure, observedPR, idempotencyKey string, telemetry json.RawMessage, startedAt *time.Time) error {
	body := map[string]any{
		"taskType":     d.Type,
		"outcome":      outcome,
		"repoFullName": d.Repo,
	}
	// The merged Dispatch tasks/report route does not consume idempotencyKey;
	// only send it when an explicitly configured deployment supports it.
	if idempotencyKey != "" && c.reportIdempotencyEnabled {
		body["idempotencyKey"] = idempotencyKey
	}
	if startedAt != nil {
		body["startedAt"] = *startedAt
	}
	if d.Type == "followup-pr" {
		body["pullRequestNumber"] = d.Number
		if d.PRURL != "" {
			body["pullRequestUrl"] = d.PRURL
		}
	} else {
		body["issueNumber"] = d.Number
	}
	if d.Type == "followup-pr" && d.IssueNumber > 0 {
		body["issueNumber"] = d.IssueNumber
	}
	// Dispatch settles a queue-backed follow-up only against the attempt it
	// issued, so echo that token back; linked-PR follow-ups have none. The
	// token fences Dispatch's hand-out release to the exact issuance on every
	// outcome, so a stale same-generation report can never release a newer
	// re-issued hand-out. Legacy work ids carry no token and omit it.
	if d.Type == "followup-pr" && d.PRFixID != "" {
		body["prFixItem"] = map[string]any{"id": d.PRFixID, "generation": d.Generation}
		if d.HandoutToken != "" {
			body["prFixItem"].(map[string]any)["handoutToken"] = d.HandoutToken
		}
	}
	if number, rawURL := parsePullRequest(observedPR); number > 0 {
		body["pullRequestNumber"] = number
		if rawURL != "" {
			body["pullRequestUrl"] = rawURL
		}
	}
	if failure != "" {
		body["error"] = failure
	}
	if len(telemetry) > 0 {
		body["telemetry"] = telemetry
	}
	err := c.do(ctx, http.MethodPost, "/api/agents/"+url.PathEscape(c.agentName)+"/tasks/report", body, nil)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusBadRequest && isInvalidOutcomeResponse(apiErr.Message) {
		// The source rejected the outcome; this classification path owns the
		// exchange's diagnostic under the unsupported-task-type class, and
		// the report outcome itself is counted at the Report boundary.
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonUnsupportedTaskType)
		absorbExchange(err)
		return fmt.Errorf("%w: %w", source.ErrUnsupportedOutcome, apiErr)
	}
	return err
}

func isInvalidOutcomeResponse(message string) bool {
	var response struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(message), &response); err != nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(response.Error), "Invalid outcome. Must be one of:")
}

// reportClose is the already_done sibling of reportTaskWithPR: valid only
// for implement tasks, always idempotency-keyed (Dispatch requires the key
// for this outcome regardless of reportIdempotencyEnabled), and never
// carrying PR, PR-fix, or telemetry fields. The coordinator's proposal is
// never closure authorization: an accepted response without a close
// resolution means the outcome was not recorded, so it fails closed, and an
// empty resolution object with a missing action fails the same way.
func (c *HTTPClient) reportClose(ctx context.Context, d workDescriptor, lifecycle source.Lifecycle) (source.CloseResolution, error) {
	if d.Type != "implement" {
		return source.CloseResolution{}, fmt.Errorf("%w: close reports are valid only for implement tasks, got %q", source.ErrUnsupportedOutcome, d.Type)
	}
	if d.Repo == "" || d.Number <= 0 {
		return source.CloseResolution{}, errors.New("dispatch client: close report requires a valid issue reference")
	}
	if strings.TrimSpace(lifecycle.IdempotencyKey) == "" {
		return source.CloseResolution{}, errors.New("dispatch client: close report requires an idempotency key")
	}
	if lifecycle.Close == nil || len(lifecycle.Close.Criteria) == 0 {
		return source.CloseResolution{}, errors.New("dispatch client: close report requires at least one grounded criterion")
	}
	criteria := make([]map[string]any, 0, len(lifecycle.Close.Criteria))
	for _, criterion := range lifecycle.Close.Criteria {
		criteria = append(criteria, map[string]any{"criterion": criterion.Criterion, "path": criterion.Path, "excerpt": criterion.Excerpt})
	}
	body := map[string]any{
		"taskType":       d.Type,
		"outcome":        outcomeAlreadyDone,
		"repoFullName":   d.Repo,
		"issueNumber":    d.Number,
		"idempotencyKey": lifecycle.IdempotencyKey,
		"close":          map[string]any{"criteria": criteria},
	}
	if lifecycle.Close.Evidence != "" {
		body["evidence"] = lifecycle.Close.Evidence
	}
	if lifecycle.StartedAt != nil {
		body["startedAt"] = *lifecycle.StartedAt
	}
	var response struct {
		CloseResolution *struct {
			Action         string   `json:"action"`
			Reason         string   `json:"reason"`
			Reasons        []string `json:"reasons"`
			HeadSHA        string   `json:"headSha"`
			EvidenceDigest string   `json:"evidenceDigest"`
		} `json:"closeResolution"`
	}
	err := c.do(ctx, http.MethodPost, "/api/agents/"+url.PathEscape(c.agentName)+"/tasks/report", body, &response)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusBadRequest && isInvalidOutcomeResponse(apiErr.Message) {
		// The source rejected the outcome; this classification path owns the
		// exchange's diagnostic under the unsupported-task-type class, and
		// the report outcome itself is counted at the ReportClose boundary.
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonUnsupportedTaskType)
		absorbExchange(err)
		return source.CloseResolution{}, fmt.Errorf("%w: %w", source.ErrUnsupportedOutcome, apiErr)
	}
	if err != nil {
		return source.CloseResolution{}, err
	}
	if response.CloseResolution == nil {
		return source.CloseResolution{}, errors.New("dispatch client: close report accepted without a close resolution")
	}
	if strings.TrimSpace(response.CloseResolution.Action) == "" {
		return source.CloseResolution{}, errors.New("dispatch client: close resolution carries no action")
	}
	// The action passes through verbatim, including strings this client does
	// not know: Dispatch's resolution is authoritative and the controller
	// decides what each action settles to.
	return source.CloseResolution{
		Action:         response.CloseResolution.Action,
		Reason:         response.CloseResolution.Reason,
		Reasons:        response.CloseResolution.Reasons,
		HeadSHA:        response.CloseResolution.HeadSHA,
		EvidenceDigest: response.CloseResolution.EvidenceDigest,
	}, nil
}

func parsePullRequest(value string) (int, string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, ""
	}
	if number, err := strconv.Atoi(value); err == nil && number > 0 {
		return number, ""
	}
	u, err := url.Parse(value)
	if err != nil || u.Path == "" {
		return 0, ""
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if strings.EqualFold(parts[i], "pull") {
			number, err := strconv.Atoi(parts[i+1])
			if err == nil && number > 0 {
				return number, value
			}
		}
	}
	return 0, ""
}

func (c *HTTPClient) PreLaunch(ctx context.Context, id string) error {
	d, err := decodeWorkID(id)
	if err != nil {
		return err
	}
	if d.Type != "followup-pr" {
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalRevalidation, reasonRevalidationCurrent)
		return nil
	}
	// The configured-ownership gate is rechecked here, the last boundary
	// before a coordinator starts (#761). This is the boundary the admission
	// path and the recovery path both pass through — a run that was claimed
	// before this gate existed, or one whose connector restarted under a
	// different binding configuration, is refused here instead of executing
	// foreign work. The refusal runs before the merged/closed check so work
	// this connector does not own is never marked stale.
	if err := c.refuseOwnership(ctx, d); err != nil {
		return err
	}
	pullRequest := &PullRequest{Repo: d.Repo, Number: d.Number}
	state, err := c.checkPullRequestState(ctx, pullRequest)
	if err != nil {
		return err
	}
	if state.Merged || strings.EqualFold(state.State, "closed") {
		if d.PRFixID != "" {
			if err := c.markPRFixStale(ctx, pullRequest, state, d.Generation); err != nil {
				return err
			}
			// The STALE mark released the queue-backed attempt's hand-out,
			// so the compare-and-set claim of this attempt is gone before
			// launch (§3.4): a normal outcome, counted under identity.
			c.diagnostics.Diagnostic(c.connectorName, connector.SignalIdentity, reasonIdentityClaimReleased)
		}
		reason := reasonRevalidationClosedUnmerged
		if state.Merged {
			reason = reasonRevalidationMerged
		}
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalRevalidation, reason)
		return fmt.Errorf("%w: upstream pull request %s#%d is no longer open", source.ErrStaleWork, d.Repo, d.Number)
	}
	c.diagnostics.Diagnostic(c.connectorName, connector.SignalRevalidation, reasonRevalidationCurrent)
	return nil
}

func (c *HTTPClient) Report(ctx context.Context, id string, lifecycle source.Lifecycle) error {
	d, err := decodeWorkID(id)
	if err != nil {
		return err
	}
	var final error
	switch lifecycle.Result {
	case source.ResultReady:
		outcome := "pr_updated"
		if d.Type == "implement" {
			outcome = "pr_opened"
		}
		final = c.reportTaskWithPR(ctx, d, outcome, "", lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
	case source.ResultBlocked:
		reportErr := c.reportTaskWithPR(ctx, d, "blocked", lifecycle.Error, lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
		if lifecycle.BlockedReportParksPRFix {
			final = reportErr
		} else {
			final = errors.Join(reportErr, c.markPRFixBlocked(ctx, d, lifecycle.Error))
		}
	case source.ResultCapacityExhausted:
		// Do not publish a waiting-external report: settlement is exclusively
		// performed by the fenced CapacityRecovery capability. No report was
		// attempted, so no report outcome is fabricated here.
		return nil
	case source.ResultBlockedExternal:
		// Recoverable dependency block. The task report uses a dedicated
		// outcome so Dispatch's existing failed-attempt accounting does not
		// fire; for a queue-backed follow-up the queue mark keeps the issued
		// generation current with a distinct recoverable status (no
		// advancement, no human park). See DESIGN.md §267 for the cross-repo
		// contract this depends on and the failure modes each signal rules
		// out.
		reportErr := c.reportTaskWithPR(ctx, d, outcomeWaitingExternal, lifecycle.Error, lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
		final = errors.Join(reportErr, c.markPRFixWaitingExternal(ctx, d, lifecycle.Error))
	case source.ResultFailed:
		// Dispatch's failed report requeues eligible PR-fix work under its
		// existing attempt cap. A separate BLOCKED mark can target a stale
		// generation after the report advances the queue item.
		final = c.reportTaskWithPR(ctx, d, "failed", lifecycle.Error, lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
	case source.ResultHandedOff:
		// This is truthful, non-parking evidence; Dispatch discovers red checks
		// through its own PR-followup sync. It must not be Ready (green) or
		// BLOCKED (which parks the issue).
		final = c.reportTaskWithPR(ctx, d, "handed-off", lifecycle.Error, lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
	case source.ResultNoChange:
		// Retain enum compatibility, but a bare no-change assertion carries
		// no source-verified feedback evidence and cannot authorize FIXED.
		final = source.ErrUnsupportedOutcome
	default:
		return nil
	}
	c.emitReportOutcome(final)
	return final
}

// emitReportOutcome counts one lifecycle-report outcome (§3) at the Report
// boundary. A superseded report must be dropped rather than retried, so it
// counts under its identity signal instead of a report reason.
func (c *HTTPClient) emitReportOutcome(final error) {
	switch {
	case final == nil:
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalReport, reasonReportPublished)
	case errors.Is(final, source.ErrSuperseded):
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalIdentity, reasonIdentitySuperseded)
	case errors.Is(final, source.ErrUnsupportedOutcome):
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalReport, reasonReportUnsupported)
	case errors.Is(final, source.ErrPermanentRefusal):
		// The source refused the report for a durable reason: settled, not
		// an ambiguous transient the next retry might clear.
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalReport, reasonReportPermanentRefusal)
	default:
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalReport, reasonReportRetryable)
	}
}

// ReportClose settles an evidence-grounded already-done conclusion
// (source.ResultAlreadyDone, #720) through Dispatch's close-gated task
// report. It never treats an accepted HTTP response as proof of closure:
// the returned CloseResolution is Dispatch's authoritative answer, and a
// report that records no resolution is an error.
func (c *HTTPClient) ReportClose(ctx context.Context, id string, lifecycle source.Lifecycle) (source.CloseResolution, error) {
	d, err := decodeWorkID(id)
	if err != nil {
		return source.CloseResolution{}, err
	}
	resolution, final := c.reportClose(ctx, d, lifecycle)
	c.emitReportOutcome(final)
	return resolution, final
}

func (c *HTTPClient) Resolve(ctx context.Context, id string) error {
	d, err := decodeWorkID(id)
	if err != nil {
		return err
	}
	if d.Type == "followup-pr" {
		return nil
	}
	return c.SetStatus(ctx, id, "done")
}

// EncodeWorkID creates the opaque source ID carried by a CoderRun.
func EncodeWorkID(task Task) string {
	d := workDescriptor{Type: task.Type}
	if task.Type == "followup-pr" && task.PRFixItem != nil {
		d.PRFixID = strings.TrimSpace(task.PRFixItem.ID)
		d.Generation = task.PRFixItem.Generation
		d.HandoutToken = task.PRFixItem.HandoutToken
		d.FixType = task.PRFixItem.FixType
		d.OriginalClaimant = strings.TrimSpace(task.PRFixItem.OriginalClaimant)
	}
	if task.Issue != nil {
		if task.Type == "implement" {
			d.IssueID = strings.TrimSpace(task.Issue.ID)
		}
		d.IssueRepo = task.Issue.Repo
		d.IssueNumber = task.Issue.Number
		d.Repo = task.Issue.Repo
		d.Number = task.Issue.Number
	}
	if task.PullRequest != nil {
		d.Repo = task.PullRequest.Repo
		d.Number = task.PullRequest.Number
		// Dispatch can hand out a CI job URL here; only a pull request URL
		// belongs in the report's pullRequestUrl.
		if strings.Contains(task.PullRequest.URL, "/pull") {
			d.PRURL = task.PullRequest.URL
		}
	}
	payload, _ := json.Marshal(d)
	return base64.RawURLEncoding.EncodeToString(payload)
}

type issueState struct {
	IssueID string `json:"issueId"`
}

func (c *HTTPClient) resolveIssue(ctx context.Context, id string) (workDescriptor, error) {
	d, err := decodeWorkID(id)
	if err != nil {
		return workDescriptor{}, err
	}
	if d.Type == "followup-pr" || d.IssueRepo == "" || d.IssueNumber <= 0 {
		return d, nil
	}
	path := "/api/issues/state?repo=" + url.QueryEscape(d.IssueRepo) + "&number=" + strconv.Itoa(d.IssueNumber)
	var state issueState
	if err := c.do(ctx, http.MethodGet, path, nil, &state); err != nil {
		return workDescriptor{}, err
	}
	if strings.TrimSpace(state.IssueID) == "" {
		return workDescriptor{}, errors.New("dispatch client: issue state response has no issue ID")
	}
	d.IssueID = state.IssueID
	return d, nil
}

// WorkIdentity is the dedupe identity of a Dispatch work ID. A queue-backed
// follow-up is one attempt per PR-fix item generation, whatever URL Dispatch
// attached to the offer; everything else keeps its opaque ID.
func WorkIdentity(id string) string {
	d, err := decodeWorkID(id)
	if err != nil || d.Type != "followup-pr" || d.PRFixID == "" {
		return id
	}
	return prFixWorkIdentity(d.Repo, d.Number, d.PRFixID, d.Generation)
}

// prFixWorkIdentity spells the dedupe identity of one queue-backed attempt
// generation. The retry requeue returns the reissued generation through the
// exact same spelling, so the identity a predecessor records correlates with
// what the runner's dedupe normalizes a fresh offer to.
func prFixWorkIdentity(repo string, number int, prFixID string, generation int) string {
	return fmt.Sprintf("followup-pr/%s#%d/%s@%d", repo, number, prFixID, generation)
}

func decodeWorkID(id string) (workDescriptor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return workDescriptor{}, fmt.Errorf("%w: %v", ErrUnknownWorkID, err)
	}
	var d workDescriptor
	if err := json.Unmarshal(payload, &d); err != nil || d.Type == "" || d.Repo == "" || d.Number <= 0 {
		return workDescriptor{}, ErrUnknownWorkID
	}
	if d.IssueID != "" && (d.IssueRepo == "" || d.IssueNumber <= 0) {
		return workDescriptor{}, ErrUnknownWorkID
	}
	if (d.PRFixID == "") != (d.Generation == 0) {
		return workDescriptor{}, ErrUnknownWorkID
	}
	return d, nil
}

func (c *HTTPClient) do(ctx context.Context, method, requestPath string, body any, response any) error {
	relative, err := url.Parse(requestPath)
	if err != nil || relative.IsAbs() {
		return fmt.Errorf("dispatch client: invalid request path")
	}
	requestURL := *c.baseURL
	basePath := requestURL.EscapedPath()
	if basePath == "" {
		basePath = "/"
	}
	rawPath := path.Join(basePath, relative.EscapedPath())
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return fmt.Errorf("dispatch client: invalid request path")
	}
	requestURL.Path = decodedPath
	requestURL.RawPath = rawPath
	if requestURL.EscapedPath() == requestURL.Path {
		requestURL.RawPath = ""
	}
	requestURL.RawQuery = relative.RawQuery
	if relative.Path == "/api/agents/"+url.PathEscape(c.agentName)+"/next-task" {
		query := requestURL.Query()
		query.Set("lane", c.queueLane)
		requestURL.RawQuery = query.Encode()
	}
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return c.failExchange(err, reasonTransportError)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		message := strings.TrimSpace(string(data))
		message = strings.ReplaceAll(message, c.token, "[redacted]")
		apiErr := &APIError{StatusCode: resp.StatusCode, Message: message}
		reason := httpReason(resp.StatusCode)
		var failure error
		if exchangeReclassified(method, requestPath, apiErr) {
			// The caller reclassifies this exchange into an identity or
			// report outcome and owns its diagnostic; the protocol-error
			// class is deferred to that site so one exchange counts once.
			failure = newExchange(apiErr, reason)
		} else {
			failure = c.failExchange(apiErr, reason)
		}
		if isPermanentRefusal(resp.StatusCode) {
			// 400 and 403 name a refusal whose cause lives in source
			// state, not the transport: a closed target, a validation the
			// same payload trips again, or a worker-bound identity that no
			// longer owns the work. Dispatch codes every transient
			// condition — lease contention, rate limits, upstream
			// unavailability — as 409, 429, or 5xx, so an identical retry
			// can never clear these. The sentinel lets a terminal
			// lifecycle report settle instead of retrying forever. Sites
			// that reclassify a specific 4xx into their own terminal
			// outcome re-wrap the bare APIError, keeping that
			// classification disjoint from this one.
			return fmt.Errorf("%w: %w", source.ErrPermanentRefusal, failure)
		}
		return failure
	}
	if response == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return c.failExchange(err, reasonMalformedResponse)
	}
	return nil
}

// exchangeReclassified reports whether a failed exchange's diagnostic is
// owned by its caller rather than the choke point in do(), so one exchange
// counts exactly once (§6.2): a superseded PR-fix mark reclassifies into an
// identity outcome (§3.4), a report whose outcome the source rejects
// reclassifies into ErrUnsupportedOutcome, and the hand-out ack endpoint's
// known refusals are moot no-ops that AckHandout settles — counting their
// 4xx classes here would fabricate failures for successful no-ops. The CI
// park family's coded refusals reclassify like the mark family: the caller
// maps them into identity or stale-work outcomes (classifyCIRefusal) and
// owns the diagnostic, and the attempt-requeue family (retry.go) does the
// same through classifyRetryRefusal, whose refusals are 409-coded into the
// retry vocabulary.
func exchangeReclassified(method, requestPath string, apiErr *APIError) bool {
	if method != http.MethodPost {
		return false
	}
	if requestPath == "/api/pr-fix-queue/mark" && apiErr.StatusCode == http.StatusConflict {
		return true
	}
	if requestPath == "/api/pr-fix-queue/ack" && apiErr.StatusCode >= http.StatusBadRequest && apiErr.StatusCode < http.StatusMultipleChoices {
		return true
	}
	switch requestPath {
	case "/api/pr-fix-queue/cipark", "/api/pr-fix-queue/cireserve", "/api/pr-fix-queue/cirepair",
		"/api/pr-fix-queue/attempt-requeue", "/api/issues/requeue":
		return apiErr.StatusCode == http.StatusConflict
	}
	return strings.HasSuffix(requestPath, "/tasks/report") &&
		apiErr.StatusCode == http.StatusBadRequest &&
		isInvalidOutcomeResponse(apiErr.Message)
}

func taskWorkItem(task Task) (string, error) {
	if task.Type == "implement" && (task.Issue == nil || task.Issue.Repo == "" || task.Issue.Number <= 0) {
		return "", errors.New("dispatch client: implement task has no valid issue")
	}
	if task.Type == "followup-pr" && (task.PullRequest == nil || task.PullRequest.Repo == "" || task.PullRequest.Number <= 0) {
		return "", errors.New("dispatch client: followup task has no valid pull request")
	}
	if task.Type == "followup-pr" && task.Issue != nil && (task.Issue.Repo == "" || task.Issue.Number <= 0) {
		return "", errors.New("dispatch client: followup task has invalid issue")
	}
	if task.Type == "followup-pr" && task.PRFixItem != nil && (strings.TrimSpace(task.PRFixItem.ID) == "" || task.PRFixItem.Generation < 1) {
		return "", errors.New("dispatch client: followup task has invalid PR-fix item")
	}
	return EncodeWorkID(task), nil
}

func taskRef(task Task) (string, int, error) {
	if task.Type == "implement" && task.Issue != nil {
		return task.Issue.Repo, task.Issue.Number, nil
	}
	if task.Type == "followup-pr" && task.PullRequest != nil {
		return task.PullRequest.Repo, task.PullRequest.Number, nil
	}
	return "", 0, errors.New("dispatch client: task has no valid reference")
}
