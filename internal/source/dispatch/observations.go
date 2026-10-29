package dispatch

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

const (
	// observationsRoute is the Dispatch execution-observation upsert route
	// (misospace/courier#633).
	observationsRoute = "/api/agent-work/observations"

	// observationsContractVersion is the wire contract version every
	// observation body carries so a server can reject a shape it does not
	// understand.
	observationsContractVersion = 1
)

// observationWorkType maps the source-neutral work mode to the Dispatch work
// type the observations route understands; a mode with no Dispatch type
// carries "mode" only.
func observationWorkType(mode string) (string, bool) {
	switch mode {
	case "resolve-issue":
		return "implement", true
	case "fix-pr":
		return "followup-pr", true
	default:
		return "", false
	}
}

// ReportExecution publishes one execution observation to the Dispatch
// execution-observation route (misospace/courier#633). The body is an
// idempotent current-state upsert keyed by (run UID, state, observation
// time): a process restart re-reports the current state and the source
// converges instead of appending, so the idempotency key is derived from
// exactly those fields.
//
// Invariants: the observation never carries claim receipts, fence tokens, or
// lease data — telemetry must not claim source ownership — and it must never
// affect claim, report, or settlement paths; a failure only degrades this
// feature. Authentication failures (401/403), rate limits (429), and 5xx are
// distinct retryable classes and never read as "unsupported".
//
// A bare 404/405/501 on the route is permanent version-skew evidence that the
// deployment has not adopted it at all (reclassifyObservationsRoute): the
// client latches the observation unsupported for the life of the process and
// every further report short-circuits without an HTTP round trip.
func (c *HTTPClient) ReportExecution(ctx context.Context, obs source.ExecutionObservation) error {
	if c.observationsUnsupported.Load() {
		return source.ErrUnsupportedExecutionObservations
	}
	if obs.RunUID == "" || obs.WorkItemID == "" {
		return fmt.Errorf("dispatch client: execution observation requires a run UID and a work item ID")
	}
	body := map[string]any{
		"version":        observationsContractVersion,
		"idempotencyKey": fmt.Sprintf("execution/%s/%s/%d", obs.RunUID, obs.State, obs.ObservedAt.UTC().UnixMilli()),
		"agent":          c.agentName,
		"source":         obs.Source,
		"run":            map[string]any{"uid": obs.RunUID, "namespace": obs.RunNamespace, "name": obs.RunName},
		"work":           observationsWorkBody(obs),
		"state":          string(obs.State),
		"observedAt":     obs.ObservedAt.UTC(),
	}
	if number, rawURL := parsePullRequest(obs.PR); number > 0 {
		body["pullRequestNumber"] = number
		if rawURL != "" {
			body["pullRequestUrl"] = rawURL
		}
	}
	err := c.do(ctx, http.MethodPost, observationsRoute, body, nil)
	if reclassified, ok := c.reclassifyObservationsRoute(err); ok {
		return reclassified
	}
	return err
}

// observationsWorkBody spells the body's work object: the opaque work item
// ID, the stable dedupe identity, and the source-neutral mode. The Dispatch
// work type is added only for a mode the route understands; a mode with no
// mapping keeps "mode" and carries no "type".
func observationsWorkBody(obs source.ExecutionObservation) map[string]any {
	body := map[string]any{
		"itemId":   obs.WorkItemID,
		"identity": obs.WorkIdentity,
		"mode":     obs.Mode,
	}
	if workType, ok := observationWorkType(obs.Mode); ok {
		body["type"] = workType
	}
	return body
}

// reclassifyObservationsRoute reclassifies a bare 404/405/501 on the
// observations POST into the permanent unsupported posture and latches the
// observation (observationsUnsupported), exactly as the CI park family latches
// its routes (reclassifyCIParkRoute404): the route has no per-item 404
// vocabulary — its body is never read for item evidence — so a 404 (route not
// registered), 405 (POST not adopted), or 501 (route not implemented) is
// evidence the deployment has not adopted the route at all: version skew,
// never a per-run condition, and every further report would retry the same
// refusal. The exchange's protocol-error class was already counted exactly
// once at the do() choke point (the route is not an exchangeReclassified
// status), so wrapping the sentinel adds no diagnostic; on the winning latch
// the capability catalogue gauge flips to the unsupported posture the
// exchange just proved. Anything else — a 400/403 refusal, a 409, a 429, a
// 5xx, a transport failure, a malformed response — is not route evidence and
// is returned unchanged. The boolean reports whether the error was
// reclassified, so the caller never has to compare error identities.
func (c *HTTPClient) reclassifyObservationsRoute(err error) (error, bool) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return err, false
	}
	switch apiErr.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
	default:
		return err, false
	}
	if c.observationsUnsupported.CompareAndSwap(false, true) {
		c.diagnostics.Capability(c.connectorName, connector.CapabilityExecutionObservations, connector.CapabilityUnsupported)
	}
	return fmt.Errorf("%w: %w", source.ErrUnsupportedExecutionObservations, err), true
}

// executionObservationsUnsupported reports the latched version-skew
// observation: whether this client has seen a bare 404/405/501 on the
// observations route and therefore knows the deployment has not adopted it.
// False until observed, and permanent once latched, for the life of the
// process.
func (c *HTTPClient) executionObservationsUnsupported() bool {
	return c.observationsUnsupported.Load()
}
