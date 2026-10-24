package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

// ParkLifecycleClient is the optional Dispatch API capability used for the
// CI park-and-resume lifecycle (issues #475/#495). A client implementing it
// coordinates Courier's repair opportunities with Dispatch's PR-fix queue;
// the adapter surface (source.CIParkLifecycle) accordingly reports the
// connector capability.
type ParkLifecycleClient interface {
	ParkCI(ctx context.Context, id string, notice source.CIParkNotice) error
	ReserveCIRepair(ctx context.Context, id string, reservation source.CIRepairReservation) (source.CIRepairGrant, error)
	SettleCIRepair(ctx context.Context, id string, outcome source.CIRepairOutcome) error
}

var _ ParkLifecycleClient = (*HTTPClient)(nil)

// maxCIIdempotencyKeyLength bounds the CI park lifecycle idempotency key: the
// key dedupes retried reports of the same transition, so it must be bounded
// but large enough for any core-generated identity.
const maxCIIdempotencyKeyLength = 512

// ciHeadPattern bounds a park-lifecycle head to a trimmed byte-for-byte commit
// SHA spelling: 7 to 64 hexadecimal characters, nothing else.
var ciHeadPattern = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)

// ReportCIParked announces that the run parked on pending CI for its published
// head (#495), so Dispatch defers its own PR-fix handouts while the Courier
// repair opportunity is active. Work Dispatch cannot defer reports
// source.ErrUnsupportedCIPark; the core records it and never emulates the
// capability with ordinary lifecycle calls.
func (a *Adapter) ReportCIParked(ctx context.Context, item source.WorkItem, notice source.CIParkNotice) error {
	if err := a.validate(item); err != nil {
		return err
	}
	client, ok := a.client.(ParkLifecycleClient)
	if !ok {
		return source.ErrUnsupportedCIPark
	}
	return client.ParkCI(ctx, item.ID, notice)
}

// ReserveCIRepair fences one bounded repair opportunity against Dispatch's
// PR-fix queue: the reservation re-reads the pull request state and refuses
// closed or merged work with source.ErrStaleWork, while a superseded
// generation or a lost fence returns source.ErrSuperseded. Clients without
// the park lifecycle capability grant nothing.
func (a *Adapter) ReserveCIRepair(ctx context.Context, item source.WorkItem, reservation source.CIRepairReservation) (source.CIRepairGrant, error) {
	if err := a.validate(item); err != nil {
		return source.CIRepairGrant{}, err
	}
	client, ok := a.client.(ParkLifecycleClient)
	if !ok {
		return source.CIRepairGrant{}, source.ErrUnsupportedCIPark
	}
	return client.ReserveCIRepair(ctx, item.ID, reservation)
}

// ReportCIRepair publishes the fenced, idempotent outcome of one repair
// attempt (repushed, exhausted, or unsupported). A fence Dispatch no longer
// records is refused with source.ErrSuperseded rather than mutating a newer
// owner; stale work is refused with source.ErrStaleWork.
func (a *Adapter) ReportCIRepair(ctx context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	if err := a.validate(item); err != nil {
		return err
	}
	client, ok := a.client.(ParkLifecycleClient)
	if !ok {
		return source.ErrUnsupportedCIPark
	}
	return client.SettleCIRepair(ctx, item.ID, outcome)
}

// parkQueueDescriptor decodes the opaque work item ID and requires the
// queue-backed follow-up work Dispatch can defer. Implement runs and
// queue-less follow-ups are not parkable at all, so they report
// source.ErrUnsupportedCIPark, while a corrupt or out-of-range ID stays a
// plain invalid-descriptor error matching AckHandout: an unreadable or
// oversized ID is not evidence about what Dispatch can defer.
func parkQueueDescriptor(id string) (workDescriptor, error) {
	d, err := decodeWorkID(id)
	if err != nil {
		return workDescriptor{}, fmt.Errorf("dispatch client: decode work item for CI park lifecycle: %w", err)
	}
	if d.Type != "followup-pr" || d.PRFixID == "" || d.Generation < 1 {
		return workDescriptor{}, source.ErrUnsupportedCIPark
	}
	repo := strings.ToLower(d.Repo)
	if d.Repo == "" || !dispatchRepoPattern.MatchString(repo) || d.Number <= 0 || d.Number > maxSafeInteger || d.Generation > maxSafeInteger {
		return workDescriptor{}, fmt.Errorf("dispatch client: invalid PR-fix descriptor for CI park lifecycle")
	}
	return d, nil
}

// matchParkPRIdentity pins every lifecycle call to the work item's own pull
// request identity: the queue item pivots by repo+pr+generation, so a notice,
// reservation, or outcome naming a different PR must not touch it. A pull
// request spelled as a URL also names a repository, which must be the
// descriptor's — a same-number pull request from another repo must not pass —
// while a bare number carries no repository identity and leans on the item's.
func matchParkPRIdentity(d workDescriptor, pr string) error {
	number, rawURL := parsePullRequest(pr)
	if number != d.Number {
		return fmt.Errorf("dispatch client: CI park lifecycle pull request %q does not match %s#%d", pr, d.Repo, d.Number)
	}
	if repo, ok := pullRequestRepo(rawURL); ok && !strings.EqualFold(repo, d.Repo) {
		return fmt.Errorf("dispatch client: CI park lifecycle pull request %q does not match %s#%d", pr, d.Repo, d.Number)
	}
	return nil
}

// pullRequestRepo extracts the owner/repo path segment of a pull-request URL,
// reporting ok=false when the URL names none. parsePullRequest only returns a
// non-empty raw value when its path carries a pull/<number> segment, so the
// owner and repo precede it when present.
func pullRequestRepo(rawURL string) (string, bool) {
	if rawURL == "" {
		return "", false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if strings.EqualFold(parts[i], "pull") {
			if i < 2 {
				return "", false
			}
			return parts[i-2] + "/" + parts[i-1], true
		}
	}
	return "", false
}

// validateCIHead accepts a trimmed 7..64 character hexadecimal head for park,
// reservation, and repush payloads, returning the trimmed spelling to send.
func validateCIHead(head string) (string, error) {
	trimmed := strings.TrimSpace(head)
	if !ciHeadPattern.MatchString(trimmed) {
		return "", fmt.Errorf("dispatch client: CI park lifecycle head %q is not 7-64 hexadecimal characters", trimmed)
	}
	return trimmed, nil
}

// validateCIIdempotencyKey accepts a bounded non-empty idempotency key; the
// contract requires one on every lifecycle call so a retry after a lost
// response replays the committed transition instead of double-applying it.
func validateCIIdempotencyKey(key string) (string, error) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return "", errors.New("dispatch client: CI park lifecycle idempotency key is required")
	}
	if len(trimmed) > maxCIIdempotencyKeyLength {
		return "", fmt.Errorf("dispatch client: CI park lifecycle idempotency key exceeds %d bytes", maxCIIdempotencyKeyLength)
	}
	return trimmed, nil
}

// validateCIFenceToken accepts the non-empty fence token a reservation
// presents and an outcome presents back.
func validateCIFenceToken(token string) (string, error) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return "", errors.New("dispatch client: CI park lifecycle fence token is required")
	}
	return trimmed, nil
}

// ParkCI posts the CI park notice to /api/pr-fix-queue/cipark
// (misospace/courier#495). The queue item pivots by repo+pr+generation
// exactly like the mark/ack family, so the body carries no prFixId.
func (c *HTTPClient) ParkCI(ctx context.Context, id string, notice source.CIParkNotice) error {
	d, err := parkQueueDescriptor(id)
	if err != nil {
		return err
	}
	if err := matchParkPRIdentity(d, notice.PR); err != nil {
		return err
	}
	head, err := validateCIHead(notice.Head)
	if err != nil {
		return err
	}
	key, err := validateCIIdempotencyKey(notice.IdempotencyKey)
	if err != nil {
		return err
	}
	return c.classifyCIExchange(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/cipark", map[string]any{
		"repo": strings.ToLower(d.Repo), "generation": d.Generation, "agentName": c.agentName,
		"head": head, "idempotencyKey": key,
	}, nil))
}

// ReserveCIRepair posts the fenced repair reservation to
// /api/pr-fix-queue/cireserve (misospace/courier#495). Before the POST the
// pull request state is re-read like PreLaunch does; a grant echoes back the
// fence token Dispatch pinned for the one repair opportunity.
func (c *HTTPClient) ReserveCIRepair(ctx context.Context, id string, reservation source.CIRepairReservation) (source.CIRepairGrant, error) {
	d, err := parkQueueDescriptor(id)
	if err != nil {
		return source.CIRepairGrant{}, err
	}
	if err := matchParkPRIdentity(d, reservation.PR); err != nil {
		return source.CIRepairGrant{}, err
	}
	head, err := validateCIHead(reservation.Head)
	if err != nil {
		return source.CIRepairGrant{}, err
	}
	key, err := validateCIIdempotencyKey(reservation.IdempotencyKey)
	if err != nil {
		return source.CIRepairGrant{}, err
	}
	fence, err := validateCIFenceToken(reservation.FenceToken)
	if err != nil {
		return source.CIRepairGrant{}, err
	}
	// The reservation needs the pull request still open: refuse a grant
	// against closed or merged work instead of reserving, without healing
	// the queue item — PreLaunch owns originating the STALE mark.
	state, err := c.checkPullRequestState(ctx, &PullRequest{Repo: d.Repo, Number: d.Number})
	if err != nil {
		return source.CIRepairGrant{}, err
	}
	if state.Merged || strings.EqualFold(state.State, "closed") {
		// The refusal is a checker read, not a counted exchange, but it is
		// the same revalidation census PreLaunch emits for a closed or
		// merged pull request, named with the actual staleness cause.
		reason := reasonRevalidationClosedUnmerged
		if state.Merged {
			reason = reasonRevalidationMerged
		}
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalRevalidation, reason)
		return source.CIRepairGrant{}, fmt.Errorf("%w: upstream pull request %s#%d is no longer open", source.ErrStaleWork, d.Repo, d.Number)
	}
	var response struct {
		FenceToken string `json:"fenceToken"`
	}
	err = c.do(ctx, http.MethodPost, "/api/pr-fix-queue/cireserve", map[string]any{
		"repo": strings.ToLower(d.Repo), "generation": d.Generation, "agentName": c.agentName,
		"head": head, "idempotencyKey": key, "fenceToken": fence,
	}, &response)
	if err != nil {
		return source.CIRepairGrant{}, c.classifyCIExchange(err)
	}
	token := strings.TrimSpace(response.FenceToken)
	if token == "" {
		// An unanswered reservation grants nothing: handing out an empty
		// token would let an unfenced outcome mutate the queue item later.
		return source.CIRepairGrant{}, errors.New("dispatch client: reserve response has no fence token")
	}
	return source.CIRepairGrant{FenceToken: token}, nil
}

// SettleCIRepair posts the fenced, idempotent outcome of one repair attempt
// to /api/pr-fix-queue/cirepair (misospace/courier#495). The head travels
// only with a repush, per source.CIRepairOutcome.
func (c *HTTPClient) SettleCIRepair(ctx context.Context, id string, outcome source.CIRepairOutcome) error {
	d, err := parkQueueDescriptor(id)
	if err != nil {
		return err
	}
	if err := matchParkPRIdentity(d, outcome.PR); err != nil {
		return err
	}
	fence, err := validateCIFenceToken(outcome.FenceToken)
	if err != nil {
		return err
	}
	key, err := validateCIIdempotencyKey(outcome.IdempotencyKey)
	if err != nil {
		return err
	}
	switch outcome.Result {
	case source.CIRepairRepushed, source.CIRepairExhausted, source.CIRepairUnsupported:
	default:
		return fmt.Errorf("dispatch client: unknown CI repair result %q", outcome.Result)
	}
	body := map[string]any{
		"repo": strings.ToLower(d.Repo), "generation": d.Generation, "agentName": c.agentName,
		"idempotencyKey": key, "fenceToken": fence, "result": outcome.Result,
	}
	if outcome.Result == source.CIRepairRepushed {
		head, err := validateCIHead(outcome.Head)
		if err != nil {
			return err
		}
		body["head"] = head
	} else if strings.TrimSpace(outcome.Head) != "" {
		return fmt.Errorf("dispatch client: CI repair %q outcome carries a head; only %q repushes one", outcome.Result, source.CIRepairRepushed)
	}
	return c.classifyCIExchange(c.do(ctx, http.MethodPost, "/api/pr-fix-queue/cirepair", body, nil))
}

// classifyCIRefusal maps the CI park family's coded 409 refusals to the
// source vocabulary. Dispatch signals a refusal with a JSON body
// {"reason": "<code>"}; codes are the cross-repo contract pinned by
// internal/source/dispatch/cipark_test.go's fixture:
//
//	fence-held / fence-gone — the reservation or deferral belongs to a
//	  different attempt → source.ErrSuperseded (never mutate a newer owner)
//	opportunity-missing / head-superseded / fence-settled — no active
//	  deferral for this head, or the work already settled → source.ErrStaleWork
//
// A coded refusal is classified by its reason code alone, and that same
// reason is the refusal's census: this site counts identity/<code> once
// (§3.4), so an operator tells competing fences from failures. Under the
// exchange-reclassified contract the exchange was deferred past the do()
// choke point (exchangeReclassified) and this site owns it: the census
// counts here and the exchange is absorbed, so it is never also counted a
// second time as protocol-error/http-4xx. Bodies without a recognized code
// stay outside the identity census: an unrecognized code keeps a deferred
// exchange counted once as protocol-error/http-4xx and returns it
// unchanged, and code-less bodies are left to the caller's asSuperseded
// sniff — the generation-mismatch literal ("generation mismatch: expected
// N") still classifies as identity under the mark family's semantics.
func (c *HTTPClient) classifyCIRefusal(err error) error {
	if err == nil {
		return nil
	}
	code, ok := ciRefusalCode(err)
	if !ok {
		return err
	}
	// One switch names a code's census reason and sentinel together, so a
	// future code addition cannot be half-wired.
	var reason string
	var superseded bool
	switch code {
	case "fence-held":
		reason, superseded = reasonIdentityFenceHeld, true
	case "fence-gone":
		reason, superseded = reasonIdentityFenceGone, true
	case "opportunity-missing":
		reason = reasonIdentityOpportunityMissing
	case "head-superseded":
		reason = reasonIdentityHeadSuperseded
	case "fence-settled":
		reason = reasonIdentityFenceSettled
	default:
		// An unrecognized code is no identity evidence: the exchange was
		// still deferred to this site, so it counts its pending 4xx class
		// once and returns unchanged.
		c.countPendingExchange(err)
		return err
	}
	// Count the identity census for the refusal, then absorb the deferred
	// exchange so its protocol-error class is never also counted: one
	// refusal, one counted diagnostic.
	c.diagnostics.Diagnostic(c.connectorName, connector.SignalIdentity, reason)
	absorbExchange(err)
	if superseded {
		return fmt.Errorf("%w: %w", source.ErrSuperseded, err)
	}
	return fmt.Errorf("%w: %w", source.ErrStaleWork, err)
}

// classifyCIExchange classifies one deferred CI park family exchange whose
// diagnostic the caller owns: a reason-coded 409 body maps through
// classifyCIRefusal — the code wins over any embedded text, keeping one
// refusal one sentinel, so a coded body embedding the generation mismatch
// literal never satisfies both sentinels — and a body without a coded
// reason falls to the shared asSuperseded sniff ("generation mismatch:
// expected N" under the mark family's identity semantics). A 404 stays an
// ordinary error: a route the server has not adopted and a queue item that
// no longer exists are indistinguishable at this layer, and the park
// window — not a retry loop — bounds either one.
func (c *HTTPClient) classifyCIExchange(err error) error {
	if err == nil {
		return nil
	}
	if _, coded := ciRefusalCode(err); coded {
		return c.classifyCIRefusal(err)
	}
	return c.asSuperseded(err)
}

// ciRefusalCode reports the reason code of a CI park family 409 whose body
// is a reason-carrying JSON object; any such shape is classified by its
// code, whatever value it carries. A plain text, a non-object body, or any
// non-conflict exchange is not coded.
func ciRefusalCode(err error) (string, bool) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		return "", false
	}
	var response struct {
		Reason string `json:"reason"`
	}
	if json.Unmarshal([]byte(apiErr.Message), &response) != nil || response.Reason == "" {
		return "", false
	}
	return response.Reason, true
}
