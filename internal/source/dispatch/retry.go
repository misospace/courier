package dispatch

// The two attempt-requeue routes this file calls, their wire shapes, and their
// coded 409 refusals are Courier's proposed cross-repo contract for the
// Dispatch server's explicit-retry requeue (#97, misospace/courier#608). The
// server has not adopted it: Dispatch main's only requeue route,
// POST /api/pr-fix-queue/requeue, is an unfenced and non-idempotent maintainer
// reset (src/lib/pr-fix-queue.ts requeuePrFixItem) that bumps a generation on
// every call and accepts no receipt or key. Until the server lands the contract
// pinned by internal/source/dispatch/retry_test.go, every real deployment
// answers these routes with a bare 404, which is permanent version-skew
// evidence: the client latches it (reclassifyRetryRoute404) and the adapter
// fails closed source.ErrUnsupportedRetry, never approximating the requeue with
// the ordinary claim/unclaim/status/mark/report calls.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

// RequeueClient is the optional Dispatch API capability behind the fenced
// attempt requeue (source.AttemptRequeuer). The adapter forwards the run's
// issued receipt and the request's run-scoped idempotency key; a client without
// the capability cannot fence a requeue at all.
type RequeueClient interface {
	RequeueAttempt(context.Context, string, source.AttemptRequeueRequest) (source.AttemptRequeueResult, error)
}

var _ RequeueClient = (*HTTPClient)(nil)

const (
	// maxRetryIdempotencyKeyLength bounds the requeue idempotency key. The
	// core's key is a fixed prefix plus a hex digest and an optional UID, so
	// the bound is far above any honest key; an over-long key is a
	// configuration fault, not requeue evidence.
	maxRetryIdempotencyKeyLength = 512

	// maxRetryNoteLength bounds the operator note carried for the source's
	// audit. It is untrusted data: forwarded bounded, never executed or
	// interpreted.
	maxRetryNoteLength = 300
)

// retryRoute names which requeue route one opaque work ID resolves to.
type retryRoute string

const (
	// retryRouteIssue is implement work: the fence is the issued issue claim
	// token and the requeue releases the issue back to offerable.
	retryRouteIssue retryRoute = "issue"
	// retryRoutePRFix is queue-backed PR follow-up work: the fence is the
	// issued hand-out receipt and the requeue reactivates the queue item under
	// a fresh generation.
	retryRoutePRFix retryRoute = "pr-fix"
)

// retryTarget is one work descriptor paired with the route it requeues on.
type retryTarget struct {
	route      retryRoute
	descriptor workDescriptor
}

// retryCodes are the Dispatch requeue family's coded 409 refusals and the
// source-neutral classification each carries. The codes are the cross-repo
// contract pinned by retry_test.go; the classification is #97's:
//
//	stale-generation — the fence names an attempt the source no longer records
//	  as current, so the current owner is never touched (the posture
//	  source.ErrStaleCapacitySettlement carries for capacity settlement);
//	superseded — the work already moved on without this request's key, which is
//	  the moot family rather than a refusal of this request;
//	item-closed — the upstream item is closed and can never be requeued;
//	human-hold — an explicit hold this retry is not authorized to lift.
const (
	retryCodeStaleGeneration = "stale-generation"
	retryCodeSuperseded      = "superseded"
	retryCodeItemClosed      = "item-closed"
	retryCodeHumanHold       = "human-hold"
)

// The refusal reasons the adapter reports to the core. They must stay equal to
// api/v1alpha1's closed retry vocabulary — retry_test.go pins the equality, so
// this source-neutral package keeps no Kubernetes dependency while drift still
// trips a test.
const (
	retryReasonStaleGeneration = "stale-generation"
	retryReasonHeldForHuman    = "held-for-human"
	retryReasonSourceRefused   = "source-refused"
	retryReasonAlreadyRequeued = "already-requeued"
)

// RequeueAttempt posts the fenced requeue for exactly one attempt. The request
// must carry the run-scoped idempotency key — the route has no unfenced
// operator — and the run's issued receipt as its fence; a request missing
// either reports source.ErrUnsupportedRetry before any exchange, because an
// unfenced requeue would mutate whatever attempt currently owns the work.
//
// A committed key replays its original result server-side, so a retry of the
// same request never requeues twice and a lost response resolves to the
// original success. Once the deployment has answered a requeue route with a bare
// 404 the exchange is never attempted again (retryUnsupported): the posture is
// permanent for the process, and re-sending to a route the deployment has not
// adopted only churns the same 404.
func (c *HTTPClient) RequeueAttempt(ctx context.Context, id string, req source.AttemptRequeueRequest) (source.AttemptRequeueResult, error) {
	if c.retryUnsupported.Load() {
		return source.AttemptRequeueResult{}, source.ErrUnsupportedRetry
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	if key == "" {
		return source.AttemptRequeueResult{}, errors.New("dispatch client: attempt requeue idempotency key is required")
	}
	if len(key) > maxRetryIdempotencyKeyLength {
		return source.AttemptRequeueResult{}, fmt.Errorf("dispatch client: attempt requeue idempotency key exceeds %d bytes", maxRetryIdempotencyKeyLength)
	}
	target, err := resolveRetryTarget(id)
	if err != nil {
		return source.AttemptRequeueResult{}, err
	}
	fence, err := retryFence(target, req.FenceToken)
	if err != nil {
		return source.AttemptRequeueResult{}, err
	}
	if target.route == retryRoutePRFix {
		return c.requeuePRFixAttempt(ctx, target.descriptor, fence, key, req.Reason)
	}
	return c.requeueIssueAttempt(ctx, id, target.descriptor, fence, key, req.Reason)
}

// resolveRetryTarget resolves one opaque work ID to exactly one requeue route.
// Queue-less follow-ups have no issued hand-out receipt and no generation, and
// a descriptor missing its issue identity gives the server nothing to fence
// against, so both report source.ErrUnsupportedRetry: the source cannot fence
// this attempt, which is not evidence about the routes. A corrupt or
// out-of-range ID stays a plain invalid-descriptor error, as in the park and
// hand-out families — an unreadable ID says nothing about what Dispatch can
// requeue.
func resolveRetryTarget(id string) (retryTarget, error) {
	d, err := decodeWorkID(id)
	if err != nil {
		return retryTarget{}, fmt.Errorf("dispatch client: decode work item for attempt requeue: %w", err)
	}
	switch d.Type {
	case "followup-pr":
		if d.PRFixID == "" || d.Generation < 1 {
			return retryTarget{}, fmt.Errorf("%w: follow-up work carries no queue-backed attempt", source.ErrUnsupportedRetry)
		}
		repo := strings.ToLower(d.Repo)
		if d.Repo == "" || !dispatchRepoPattern.MatchString(repo) || d.Number <= 0 || d.Number > maxSafeInteger || d.Generation > maxSafeInteger {
			return retryTarget{}, fmt.Errorf("dispatch client: invalid PR-fix descriptor for attempt requeue")
		}
		return retryTarget{route: retryRoutePRFix, descriptor: d}, nil
	case "implement":
		if d.IssueRepo == "" || d.IssueNumber <= 0 {
			return retryTarget{}, fmt.Errorf("%w: implement work carries no issue identity", source.ErrUnsupportedRetry)
		}
		return retryTarget{route: retryRouteIssue, descriptor: d}, nil
	default:
		return retryTarget{}, fmt.Errorf("%w: unsupported work type %q", source.ErrUnsupportedRetry, d.Type)
	}
}

// retryFence extracts the token the requeue fences from the run's issued
// receipt. The fence the core presents IS the Dispatch receipt, so a fence that
// does not parse as one proves nothing about this attempt and is reported
// unsupported rather than guessed at; the same rule keeps a receipt that fences
// different queue work (another item or generation) off the wire, because
// presenting it would mis-fence the requeue against work this run never owned.
func retryFence(target retryTarget, fenceToken string) (string, error) {
	token := strings.TrimSpace(fenceToken)
	if token == "" {
		return "", fmt.Errorf("%w: the run carries no issued claim receipt", source.ErrUnsupportedRetry)
	}
	var receipt dispatchContext
	if json.Unmarshal([]byte(token), &receipt) != nil {
		return "", fmt.Errorf("%w: the retry fence is not a Dispatch claim receipt", source.ErrUnsupportedRetry)
	}
	if target.route == retryRoutePRFix {
		d := target.descriptor
		if receipt.PRFixID != d.PRFixID || receipt.Generation != d.Generation {
			return "", fmt.Errorf("%w: the issued receipt fences %s@%d, not this attempt %s@%d",
				source.ErrUnsupportedRetry, receipt.PRFixID, receipt.Generation, d.PRFixID, d.Generation)
		}
		token = strings.TrimSpace(receipt.HandoutToken)
	} else {
		token = strings.TrimSpace(receipt.ClaimToken)
	}
	if token == "" {
		return "", fmt.Errorf("%w: the issued receipt carries no fence for this attempt", source.ErrUnsupportedRetry)
	}
	return token, nil
}

// requeuePRFixAttempt reactivates a queue-backed attempt on
// POST /api/pr-fix-queue/attempt-requeue, pivoting on repo+pr+generation like
// the mark/ack family and fencing with the issued hand-out receipt. The response
// names the reissued item and its fresh generation, from which the adapter
// derives the attempt identity in exactly the spelling the runner's dedupe
// normalizes offers to — that is what lets a successor correlate to this
// predecessor. A response that promises no identity is honest, not a failure:
// discovery observes the successor later.
func (c *HTTPClient) requeuePRFixAttempt(ctx context.Context, d workDescriptor, fence, key, reason string) (source.AttemptRequeueResult, error) {
	var response struct {
		Success   bool `json:"success"`
		Replayed  bool `json:"replayed"`
		PRFixItem *struct {
			ID         string `json:"id"`
			Generation int    `json:"generation"`
		} `json:"prFixItem"`
	}
	err := c.do(ctx, http.MethodPost, "/api/pr-fix-queue/attempt-requeue", map[string]any{
		"repo":           strings.ToLower(d.Repo),
		"pr":             d.Number,
		"generation":     d.Generation,
		"agentName":      c.agentName,
		"fenceToken":     fence,
		"idempotencyKey": key,
		"note":           retryNote(reason),
	}, &response)
	if err != nil {
		return source.AttemptRequeueResult{}, c.classifyRetryExchange(err)
	}
	if !response.Success {
		return source.AttemptRequeueResult{}, errors.New("dispatch client: attempt requeue response did not confirm success")
	}
	// The client never branches on `replayed`: a committed key's replay is the
	// same attested success as a fresh commit and both settle Requeued. It is
	// part of the pinned wire shape, and the fixture answers a replayed key with
	// success, so a server that stops distinguishing replays on the wire cannot
	// silently change what Courier sends.
	if response.PRFixItem == nil {
		return source.AttemptRequeueResult{}, nil
	}
	if response.PRFixItem.ID != d.PRFixID || response.PRFixItem.Generation < 1 {
		return source.AttemptRequeueResult{}, fmt.Errorf("dispatch client: attempt requeue reissued %s@%d for %s@%d",
			response.PRFixItem.ID, response.PRFixItem.Generation, d.PRFixID, d.Generation)
	}
	return source.AttemptRequeueResult{AttemptID: prFixWorkIdentity(d.Repo, d.Number, response.PRFixItem.ID, response.PRFixItem.Generation)}, nil
}

// requeueIssueAttempt requeues implement work on POST /api/issues/requeue,
// fencing with the issued issue claim token exactly as the capacity handoff
// does. The issue identity is resolved the way Claim resolves it, so the fence
// still rides the claim receipt and the state lookup adds no trust the claim did
// not already establish. An issue has no generation to hand back: any attempt
// identity the server offers is forwarded verbatim, and a bare success is
// complete in itself, because the core correlates a successor only by an
// identity it can normalize and otherwise discovers it.
func (c *HTTPClient) requeueIssueAttempt(ctx context.Context, id string, d workDescriptor, fence, key, reason string) (source.AttemptRequeueResult, error) {
	resolved, err := c.resolveIssue(ctx, id)
	if err != nil {
		return source.AttemptRequeueResult{}, err
	}
	if resolved.IssueID == "" || resolved.IssueRepo == "" || resolved.IssueNumber <= 0 {
		return source.AttemptRequeueResult{}, fmt.Errorf("%w: the issue identity is not resolvable for requeue", source.ErrUnsupportedRetry)
	}
	var response struct {
		Success   bool   `json:"success"`
		Replayed  bool   `json:"replayed"`
		AttemptID string `json:"attemptId"`
	}
	err = c.do(ctx, http.MethodPost, "/api/issues/requeue", map[string]any{
		"issueId":        resolved.IssueID,
		"repoFullName":   resolved.IssueRepo,
		"issueNumber":    resolved.IssueNumber,
		"agentName":      c.agentName,
		"claimToken":     fence,
		"idempotencyKey": key,
		"note":           retryNote(reason),
	}, &response)
	if err != nil {
		return source.AttemptRequeueResult{}, c.classifyRetryExchange(err)
	}
	if !response.Success {
		return source.AttemptRequeueResult{}, errors.New("dispatch client: attempt requeue response did not confirm success")
	}
	return source.AttemptRequeueResult{AttemptID: strings.TrimSpace(response.AttemptID)}, nil
}

// retryNote bounds the untrusted operator note forwarded for the source's audit,
// truncating on a rune boundary so a multi-byte value never arrives mangled. An
// empty note is forwarded empty: the note is audit context, never required.
func retryNote(reason string) string {
	reason = strings.TrimSpace(reason)
	if len(reason) <= maxRetryNoteLength {
		return reason
	}
	cut := maxRetryNoteLength
	for cut > 0 && !utf8.RuneStart(reason[cut]) {
		cut--
	}
	return reason[:cut]
}

// classifyRetryExchange classifies one failed requeue exchange whose coded
// refusals the caller owns. A bare 404 is route evidence and reclassifies first
// into the permanent unsupported posture (reclassifyRetryRoute404). A 409 whose
// body carries a known reason code maps through classifyRetryRefusal. Anything
// else proves nothing about the requeue's fate and stays transient for the
// retry's durable record: a 5xx or transport failure, a 400/403 (which the
// choke point also wraps in source.ErrPermanentRefusal — that sentinel is how
// the ordinary lifecycle report stops retrying, and the retry record reads only
// the coded reasons and ErrUnsupportedRetry, so an uncoded refusal keeps
// retrying rather than settling on a body Courier cannot read), and an
// unrecognized or absent 409 reason code. A durable retry request would rather
// keep retrying under its own key than invent a settlement the source never
// attested.
func (c *HTTPClient) classifyRetryExchange(err error) error {
	if err == nil {
		return nil
	}
	if reclassified, ok := c.reclassifyRetryRoute404(err); ok {
		return reclassified
	}
	code, coded := retryRefusalCode(err)
	if !coded {
		c.countPendingExchange(err)
		return err
	}
	return c.classifyRetryRefusal(code, err)
}

// classifyRetryRefusal maps one coded requeue refusal to the source-neutral
// vocabulary, counting the refusal under its identity census and absorbing the
// deferred exchange so one refusal counts exactly once (§6.2). The moot family
// is `superseded`: the work moved on without this request's key, so the core
// settles Moot and this request's barrier deliberately does not release. Every
// other code refuses this request and settles it Refused with its bounded
// reason. Neither branch touches the current owner, which is the source's own
// fence to enforce — and the reason a stale fence is refused instead of retried.
func (c *HTTPClient) classifyRetryRefusal(code string, err error) error {
	var reason string
	var refusal *source.RetryRefusalError
	switch code {
	case retryCodeStaleGeneration:
		reason = reasonIdentityRequeueStaleGeneration
		refusal = &source.RetryRefusalError{Reason: retryReasonStaleGeneration, Err: fmt.Errorf("%w: %w", source.ErrStaleCapacitySettlement, err)}
	case retryCodeSuperseded:
		reason = reasonIdentityRequeueSuperseded
		refusal = &source.RetryRefusalError{Reason: retryReasonAlreadyRequeued, Moot: true, Err: fmt.Errorf("%w: %w", source.ErrSuperseded, err)}
	case retryCodeItemClosed:
		reason = reasonIdentityRequeueItemClosed
		refusal = &source.RetryRefusalError{Reason: retryReasonSourceRefused, Err: err}
	case retryCodeHumanHold:
		reason = reasonIdentityRequeueHumanHold
		refusal = &source.RetryRefusalError{Reason: retryReasonHeldForHuman, Err: err}
	default:
		c.countPendingExchange(err)
		return err
	}
	c.diagnostics.Diagnostic(c.connectorName, connector.SignalIdentity, reason)
	absorbExchange(err)
	return refusal
}

// retryRefusalCode reports the reason code of a requeue 409 whose body is a
// reason-carrying JSON object, the shape the CI park family's refusals use. A
// plain-text body, a non-object body, or any non-conflict exchange is not coded.
func retryRefusalCode(err error) (string, bool) {
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

// reclassifyRetryRoute404 reclassifies a bare 404 on a requeue POST into the
// permanent unsupported posture and latches the observation (retryUnsupported),
// exactly as the CI park family latches its routes (reclassifyCIParkRoute404):
// the requeue refusal vocabulary is 409-coded, so a 404 on one of these routes
// is evidence the deployment has adopted neither of them — version skew, never a
// per-item condition — and every further requeue would retry the same 404 while
// holding a durable retry request and the run behind its reap gate. Unlike
// AckHandout, which reads a known item-not-found body as per-item ledger
// evidence, this family discriminates no 404 body: the contract grants 404 no
// per-item meaning at all, so the classification is deliberately status-only and
// the consequence deliberately per-process. The 404's protocol-error class was
// already counted at the do() choke point (a requeue 404 is not an
// exchangeReclassified status), so wrapping the sentinel adds no diagnostic, and
// nothing here flips the capability catalogue: the retry posture is recorded on
// the run's status.retryRequest and its condition, which is the audit trail. A
// 5xx, a transport failure, a coded 409, or a 400/403 is not route evidence and
// is returned unchanged.
func (c *HTTPClient) reclassifyRetryRoute404(err error) (error, bool) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return err, false
	}
	c.retryUnsupported.CompareAndSwap(false, true)
	return fmt.Errorf("%w: %w", source.ErrUnsupportedRetry, err), true
}

// retryRequeueUnsupported reports the latched version-skew observation: whether
// this client has seen a bare 404 on a requeue route and therefore knows the
// deployment has not adopted them. False until observed, and permanent once
// latched for the life of the process.
func (c *HTTPClient) retryRequeueUnsupported() bool {
	return c.retryUnsupported.Load()
}
