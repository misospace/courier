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
	"time"

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

// PRFixItem identifies one generation of a queued PR followup.
type PRFixItem struct {
	ID           string `json:"id"`
	Generation   int    `json:"generation"`
	HandoutToken string `json:"handoutToken,omitempty"`
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
	reportIdempotencyEnabled bool
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
	values := make([]string, 0, min(len(exclusions), maxPRFixExclusions))
	for _, item := range exclusions {
		d, err := decodeWorkID(item.ID)
		if err != nil || d.Type != "followup-pr" || d.PRFixID == "" || d.Generation < 1 || d.Repo == "" {
			continue
		}
		repo := strings.ToLower(d.Repo)
		if !dispatchRepoPattern.MatchString(repo) || d.Number > maxSafeInteger || d.Generation > maxSafeInteger {
			return nil, fmt.Errorf("dispatch client: invalid PR-fix exclusion descriptor")
		}
		value := fmt.Sprintf("%s#%d@%d", repo, d.Number, d.Generation)
		if len(value) > 300 {
			return nil, fmt.Errorf("dispatch client: PR-fix exclusion exceeds maximum 300 characters")
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		if len(seen) <= maxPRFixExclusions {
			values = append(values, value)
		}
	}
	if dropped := len(seen) - maxPRFixExclusions; dropped > 0 {
		log.FromContext(ctx).Info("dispatch client: PR-fix exclusions exceed contract maximum, keeping the most relevant", "agent", c.agentName, "kept", maxPRFixExclusions, "dropped", dropped)
	}
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
}

func (c *HTTPClient) discover(ctx context.Context, query url.Values) ([]source.WorkItem, error) {
	var response nextTaskResponse
	endpoint := "/api/agents/" + url.PathEscape(c.agentName) + "/next-task"
	if query != nil {
		endpoint += "?" + query.Encode()
	}
	if err := c.do(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	if query != nil && query.Has("exclude_pr_fix_version") {
		var ack map[string]json.RawMessage
		if len(response.PRFixExclusions) == 0 || json.Unmarshal(response.PRFixExclusions, &ack) != nil || ack == nil {
			return nil, fmt.Errorf("dispatch client: missing or invalid PR-fix exclusions acknowledgment")
		}
		version, ok := ack["version"]
		var versionValue int
		if !ok || json.Unmarshal(version, &versionValue) != nil || versionValue != 1 {
			return nil, fmt.Errorf("dispatch client: unsupported PR-fix exclusions acknowledgment")
		}
	}
	task := response.Task
	if !task.ShouldRun || task.Type == "idle" {
		return nil, nil
	}
	if task.Type != "implement" && task.Type != "followup-pr" {
		return nil, fmt.Errorf("dispatch client: unsupported task type %q", task.Type)
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
	}
	context := ""
	if task.PRFixItem != nil {
		var receipt dispatchContext
		receipt.PRFixID, receipt.Generation = task.PRFixItem.ID, task.PRFixItem.Generation
		receipt.HandoutToken = task.PRFixItem.HandoutToken
		encoded, _ := json.Marshal(receipt)
		context = string(encoded)
	}
	return []source.WorkItem{{ID: itemID, Context: context, Mode: mode, Repo: repo, Ref: ref}}, nil
}

type dispatchContext struct {
	ClaimToken   string `json:"claimToken,omitempty"`
	PRFixID      string `json:"prFixId,omitempty"`
	Generation   int    `json:"generation,omitempty"`
	HandoutToken string `json:"handoutToken,omitempty"`
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

// asSuperseded maps only an explicitly stale generation to a terminal source result.
func asSuperseded(err error) error {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict &&
		(strings.Contains(strings.ToLower(apiErr.Message), "generation mismatch") || strings.Contains(strings.ToLower(apiErr.Message), "expected generation")) {
		return fmt.Errorf("%w: %w", source.ErrSuperseded, err)
	}
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
// not an error. A refusal proves the hand-out state has already moved on —
// the item is gone (404), the hand-out was never ours (403), or the
// generation was re-issued, the item left QUEUED, or the hand-out released
// (409 with a known moot reason) — so there is nothing outstanding for this
// agent to acknowledge and the ack degrades to a no-op. An unknown 409
// reason, transport failure, or server failure returns an error: the
// hand-out stays in the reclaimable state the reclaimer exists for, and the
// caller logs without blocking materialization.
func (c *HTTPClient) AckHandout(ctx context.Context, item source.WorkItem) error {
	d, err := decodeWorkID(item.ID)
	if err != nil {
		return fmt.Errorf("dispatch client: decode work item for hand-out ack: %w", err)
	}
	if d.Type != "followup-pr" || d.PRFixID == "" || d.Generation < 1 {
		// Not a queue-backed PR-fix attempt: nothing to acknowledge.
		return nil
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
		case http.StatusNotFound, http.StatusForbidden:
			// The hand-out state has already moved on: item gone, or never
			// handed to this agent. Nothing outstanding remains to
			// acknowledge.
			return nil
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
	return err
}

func (c *HTTPClient) markPRFixBlocked(ctx context.Context, d workDescriptor, note string) error {
	if d.Type != "followup-pr" || d.PRFixID == "" {
		return nil
	}
	if strings.TrimSpace(note) == "" {
		note = "Courier run requires human intervention"
	}
	return asSuperseded(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
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
	return asSuperseded(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
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
	return asSuperseded(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
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
	// issued, so echo that token back; linked-PR follow-ups have none.
	if d.Type == "followup-pr" && d.PRFixID != "" {
		body["prFixItem"] = map[string]any{"id": d.PRFixID, "generation": d.Generation}
		if outcome == "capacity-exhausted" && d.HandoutToken != "" {
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
		return nil
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
		}
		return fmt.Errorf("%w: upstream pull request %s#%d is no longer open", source.ErrStaleWork, d.Repo, d.Number)
	}
	return nil
}

func (c *HTTPClient) Report(ctx context.Context, id string, lifecycle source.Lifecycle) error {
	d, err := decodeWorkID(id)
	if err != nil {
		return err
	}
	switch lifecycle.Result {
	case source.ResultReady:
		outcome := "pr_updated"
		if d.Type == "implement" {
			outcome = "pr_opened"
		}
		return c.reportTaskWithPR(ctx, d, outcome, "", lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
	case source.ResultBlocked:
		reportErr := c.reportTaskWithPR(ctx, d, "blocked", lifecycle.Error, lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
		if lifecycle.BlockedReportParksPRFix {
			return reportErr
		}
		return errors.Join(reportErr, c.markPRFixBlocked(ctx, d, lifecycle.Error))
	case source.ResultCapacityExhausted:
		// Do not publish a waiting-external report: settlement is exclusively
		// performed by the fenced CapacityRecovery capability.
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
		return errors.Join(reportErr, c.markPRFixWaitingExternal(ctx, d, lifecycle.Error))
	case source.ResultFailed:
		// Dispatch's failed report requeues eligible PR-fix work under its
		// existing attempt cap. A separate BLOCKED mark can target a stale
		// generation after the report advances the queue item.
		return c.reportTaskWithPR(ctx, d, "failed", lifecycle.Error, lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
	case source.ResultHandedOff:
		// This is truthful, non-parking evidence; Dispatch discovers red checks
		// through its own PR-followup sync. It must not be Ready (green) or
		// BLOCKED (which parks the issue).
		return c.reportTaskWithPR(ctx, d, "handed-off", lifecycle.Error, lifecycle.PR, lifecycle.IdempotencyKey, lifecycle.Telemetry, lifecycle.StartedAt)
	case source.ResultNoChange:
		// Retain enum compatibility, but a bare no-change assertion carries
		// no source-verified feedback evidence and cannot authorize FIXED.
		return source.ErrUnsupportedOutcome
	default:
		return nil
	}
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
	return fmt.Sprintf("followup-pr/%s#%d/%s@%d", d.Repo, d.Number, d.PRFixID, d.Generation)
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
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		message := strings.TrimSpace(string(data))
		message = strings.ReplaceAll(message, c.token, "[redacted]")
		return &APIError{StatusCode: resp.StatusCode, Message: message}
	}
	if response == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(response)
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
