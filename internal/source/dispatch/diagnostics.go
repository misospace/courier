package dispatch

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misospace/courier/internal/connector"
)

// defaultConnectorName is the bounded connector label value an adapter
// reports under when no configured identity overrides it: the legacy bare
// Dispatch source. Multiple configured adapters must each carry their own
// identity (see WithConnectorName) so their metric series stay independent;
// the label is the configured adapter, never a Dispatch endpoint, agent
// queue, or lane.
const defaultConnectorName = "dispatch"

// MaxConnectorNameLength bounds the configured connector label value.
const MaxConnectorNameLength = 64

// validateConnectorName fails loudly on a configured connector identity that
// is empty, over-long, or outside the bounded label charset. A connector is
// identified by its label, so an unbounded or colliding identity is a
// configuration conflict that would silently merge independent metric
// series; it panics instead.
func validateConnectorName(name string) {
	if name == "" {
		panic("dispatch source: connector name must not be empty")
	}
	if len(name) > MaxConnectorNameLength {
		panic(fmt.Sprintf("dispatch source: connector name %q exceeds %d characters", name, MaxConnectorNameLength))
	}
	alphanumeric := false
	for _, r := range name {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			alphanumeric = true
			continue
		}
		if r == '.' || r == '_' || r == '-' || r == ':' {
			continue
		}
		panic(fmt.Sprintf("dispatch source: invalid connector name %q: use letters, digits, dot, underscore, hyphen, or colon", name))
	}
	if !alphanumeric {
		panic(fmt.Sprintf("dispatch source: invalid connector name %q: must contain at least one letter or digit", name))
	}
}

// Connector-declared reason vocabulary (§3). These values are owned by this
// connector; the envelope package owns only the signal names. They are closed
// and bounded, and every unrecognised value folds onto connector.ReasonOther
// at the envelope boundary.
const (
	reasonAckOK                 = "ok"
	reasonAckMissing            = "missing"
	reasonAckInvalid            = "invalid"
	reasonAckVersionUnsupported = "version-unsupported"

	reasonTransportError      = "transport-error"
	reasonAuthError           = "auth-error"
	reasonHTTP4xx             = "http-4xx"
	reasonHTTP5xx             = "http-5xx"
	reasonMalformedResponse   = "malformed-response"
	reasonUnsupportedTaskType = "unsupported-task-type"
	reasonInvalidDescriptor   = "invalid-descriptor"

	reasonReportPublished        = "published"
	reasonReportUnsupported      = "unsupported"
	reasonReportRetryable        = "retryable"
	reasonReportPermanentRefusal = "permanent-refusal"

	reasonRevalidationCurrent        = "current"
	reasonRevalidationStaleHead      = "stale-head"
	reasonRevalidationClosedUnmerged = "closed-unmerged"
	reasonRevalidationMerged         = "merged"

	reasonIdentityGenerationAdvanced = "generation-advanced"
	reasonIdentitySuperseded         = "identity-superseded"
	reasonIdentityClaimReleased      = "claim-released"
	reasonIdentityFenceHeld          = "fence-held"
	reasonIdentityFenceGone          = "fence-gone"
	reasonIdentityOpportunityMissing = "opportunity-missing"
	reasonIdentityHeadSuperseded     = "head-superseded"
	reasonIdentityFenceSettled       = "fence-settled"

	reasonIdentityRequeueStaleGeneration = "requeue-stale-generation"
	reasonIdentityRequeueSuperseded      = "requeue-superseded"
	reasonIdentityRequeueItemClosed      = "requeue-item-closed"
	reasonIdentityRequeueHumanHold       = "requeue-human-hold"
)

// reasonVocabulary is the closed reason vocabulary declared per signal at
// registration (§6.1). It must include connector.ReasonOverflow under the
// exclusion signal so overflow cycles count under their catalogue reason.
func reasonVocabulary() map[string][]string {
	return map[string][]string{
		connector.SignalExclusion:     {connector.ReasonOverflow},
		connector.SignalAck:           {reasonAckOK, reasonAckMissing, reasonAckInvalid, reasonAckVersionUnsupported},
		connector.SignalProtocolError: {reasonTransportError, reasonAuthError, reasonHTTP4xx, reasonHTTP5xx, reasonMalformedResponse, reasonUnsupportedTaskType, reasonInvalidDescriptor},
		connector.SignalReport:        {reasonReportPublished, reasonReportUnsupported, reasonReportRetryable, reasonReportPermanentRefusal},
		connector.SignalRevalidation:  {reasonRevalidationCurrent, reasonRevalidationStaleHead, reasonRevalidationClosedUnmerged, reasonRevalidationMerged},
		connector.SignalIdentity:      {reasonIdentityGenerationAdvanced, reasonIdentitySuperseded, reasonIdentityClaimReleased, reasonIdentityFenceHeld, reasonIdentityFenceGone, reasonIdentityOpportunityMissing, reasonIdentityHeadSuperseded, reasonIdentityFenceSettled, reasonIdentityRequeueStaleGeneration, reasonIdentityRequeueSuperseded, reasonIdentityRequeueItemClosed, reasonIdentityRequeueHumanHold},
		connector.SignalAvailability:  {connector.AvailabilityAvailable, connector.AvailabilityDegraded, connector.AvailabilityUnreachable, connector.AvailabilityUnknown},
	}
}

// exchangeError marks a failed Dispatch exchange with its bounded
// protocol-error reason (§3.3). The reason is a class, never a body, URL, or
// upstream string. do() classifies every exchange it performs; the emission
// happens once per exchange, either at the choke point in do() or, when the
// caller reclassifies the failure into an identity or report outcome, at that
// classification site instead.
type exchangeError struct {
	cause   error
	reason  string
	counted bool
}

func (e *exchangeError) Error() string { return e.cause.Error() }
func (e *exchangeError) Unwrap() error { return e.cause }

func exchangeOf(err error) *exchangeError {
	var ex *exchangeError
	if errors.As(err, &ex) {
		return ex
	}
	return nil
}

// httpReason maps a Dispatch status code to its bounded protocol-error
// reason class.
func httpReason(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return reasonAuthError
	case status >= http.StatusInternalServerError:
		return reasonHTTP5xx
	default:
		return reasonHTTP4xx
	}
}

// failExchange counts one protocol-error diagnostic for a failed exchange and
// returns the failure wrapped with its classification. This is the canonical
// emission choke point: one failed exchange counts exactly once.
func (c *HTTPClient) failExchange(cause error, reason string) error {
	ex := &exchangeError{cause: cause, reason: reason}
	c.countExchangeError(ex)
	return ex
}

// newExchange wraps a failed exchange whose diagnostic the caller owns
// because it reclassifies the failure into an identity or report outcome. The
// exchange is wrapped but not counted yet; the classification site either
// counts its own signal and absorbs this one, or counts the pending
// protocol-error class.
func newExchange(cause error, reason string) error {
	return &exchangeError{cause: cause, reason: reason}
}

// countExchangeError counts the protocol-error diagnostic for a classified
// exchange exactly once.
func (c *HTTPClient) countExchangeError(ex *exchangeError) {
	if ex == nil || ex.counted {
		return
	}
	ex.counted = true
	c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, ex.reason)
}

// absorbExchange marks a classified exchange as counted under the caller's
// own signal so it is never also counted as a protocol error (§6.2).
func absorbExchange(err error) {
	if ex := exchangeOf(err); ex != nil {
		ex.counted = true
	}
}

// countPendingExchange counts a failed exchange whose classification was
// deferred to the caller but which the caller did not reclassify into an
// identity or report outcome.
func (c *HTTPClient) countPendingExchange(err error) {
	c.countExchangeError(exchangeOf(err))
}
