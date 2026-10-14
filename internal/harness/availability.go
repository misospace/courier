package harness

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ProviderAvailability is the harness's bounded, backend-neutral vocabulary
// for why a model call did not return a completion. It lets trusted control
// tell a recoverable allowance exhaustion from a transient rate limit, an
// authentication failure, a transport fault, a context-limit rejection, or an
// error it does not recognize — without retaining provider response text
// (#320). The vocabulary is deliberately generic: Courier core names no
// provider and ships no provider-specific policy, and a signal it does not
// recognize is preserved as ProviderUnknown rather than guessed.
type ProviderAvailability string

const (
	// ProviderAvailable is the absence of a classified failure. It is not
	// emitted by the classifier; it exists so callers have a zero-value
	// "no availability problem" token.
	ProviderAvailable ProviderAvailability = "available"
	// ProviderTransientRateLimit is a rate limit that an immediate retry can
	// plausibly clear (a 429 with no exhaustion signal).
	ProviderTransientRateLimit ProviderAvailability = "transient-rate-limit"
	// ProviderAllowanceExhausted is a confirmed, recoverable exhaustion of a
	// model/provider allowance or credential pool: retrying in-process cannot
	// help, and the run should wait for capacity rather than fail.
	ProviderAllowanceExhausted ProviderAvailability = "allowance-exhausted"
	// ProviderAuthenticationFailed is a rejected credential or authorization.
	ProviderAuthenticationFailed ProviderAvailability = "authentication-failed"
	// ProviderRequestRejected is a malformed or unsupported request (bad
	// model, missing route, invalid shape, oversized body).
	ProviderRequestRejected ProviderAvailability = "request-rejected"
	// ProviderContextExhausted is a request rejected for exceeding a model or
	// deployment context/token limit.
	ProviderContextExhausted ProviderAvailability = "context-exhausted"
	// ProviderUnavailable is a gateway- or provider-side outage (5xx, request
	// timeout) that an immediate retry can plausibly clear.
	ProviderUnavailable ProviderAvailability = "provider-unavailable"
	// ProviderUnknown preserves a failure the classifier cannot place. It is
	// never silently folded into a known class.
	ProviderUnknown ProviderAvailability = "unknown"
)

// maxProviderErrorBody bounds how much of a provider error response the
// classifier reads. It is a transport resource bound, not a work limit: a
// provider error body is a short JSON object, and a larger one is broken or
// hostile.
const maxProviderErrorBody = 64 << 10

// maxProviderSignalLength bounds a retained structured signal (a provider
// error code or type). A real code is a short token; anything longer is not
// retained, so no unbounded provider string can reach a diagnostic or label.
const maxProviderSignalLength = 64

// maxRetryAfter bounds an honored Retry-After hint. A reset hint beyond this
// horizon is not something a single coordinator incarnation can wait out; it
// is reported (when known) and left to source re-offer, never spun on.
const maxRetryAfter = 24 * time.Hour

// ExhaustionSignals names the structured provider signals that mean "this
// allowance is spent" rather than "you are going too fast". The defaults are
// provider-neutral OpenAI-compatible tokens; a deployment whose gateway
// surfaces provider-specific codes (for example a numeric plan-limit code)
// extends them through configuration. Matching is case-insensitive.
type ExhaustionSignals struct {
	Codes []string
	Types []string
}

// DefaultExhaustionSignals is the provider-neutral default signal set.
func DefaultExhaustionSignals() ExhaustionSignals {
	return ExhaustionSignals{
		Codes: []string{
			"insufficient_quota",
			"quota_exceeded",
			"usage_limit_reached",
			"insufficient_balance",
			"billing_hard_limit_reached",
		},
		Types: []string{
			"insufficient_quota",
			"quota_exceeded",
			"billing_error",
		},
	}
}

// Empty reports whether no signals are configured, so a caller can fall back
// to the provider-neutral defaults rather than matching nothing.
func (s ExhaustionSignals) Empty() bool {
	return len(s.Codes) == 0 && len(s.Types) == 0
}

// ParseExhaustionSignals merges comma-separated deployment-configured signals
// onto the provider-neutral defaults. Configuration can only add signals;
// it cannot remove the defaults, so a deployment cannot accidentally make a
// known exhaustion look transient. Empty overrides keep the defaults.
func ParseExhaustionSignals(codes, types string) ExhaustionSignals {
	s := DefaultExhaustionSignals()
	s.Codes = appendUniqueSignals(s.Codes, splitSignals(codes)...)
	s.Types = appendUniqueSignals(s.Types, splitSignals(types)...)
	return s
}

func splitSignals(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func appendUniqueSignals(base []string, extra ...string) []string {
	seen := make(map[string]struct{}, len(base)+len(extra))
	for _, value := range base {
		seen[strings.ToLower(value)] = struct{}{}
	}
	for _, value := range extra {
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		base = append(base, value)
	}
	return base
}

// ProviderClassification is the bounded result of classifying a model
// provider failure. It carries only fixed classes and allowlisted short
// signals — never the provider response body, which may quote credentials,
// endpoints, or model-controlled text.
type ProviderClassification struct {
	// Availability is the classification. ProviderUnknown means the failure
	// could not be placed.
	Availability ProviderAvailability
	// Code is the structured provider error code or type when it was
	// recognized and bounded; empty otherwise. It is safe to surface.
	Code string
	// RetryAfter is the authoritative reset/retry hint when the provider or
	// gateway supplied one and it parsed; zero when absent or unknown.
	RetryAfter time.Duration
	// Retryable reports whether an immediate in-process retry can plausibly
	// succeed. Confirmed exhaustion, auth, context, and request rejections
	// are not retryable; transport, 5xx, and a plain 429 are.
	Retryable bool
}

// Deferred reports whether the failure is a resource-availability condition
// the run should not fail on: a confirmed exhaustion, a rate limit that
// persisted through the in-process retries, or a transient failure whose
// authoritative reset hint is longer than a single incarnation should spin on.
// A deferred failure is recoverable and belongs to source re-offer, not to a
// hard run failure or a human decision.
func (c ProviderClassification) Deferred() bool {
	switch c.Availability {
	case ProviderAllowanceExhausted, ProviderTransientRateLimit:
		return true
	}
	return c.Retryable && c.RetryAfter > maxInProcessRetryAfter
}

// ClassifyProviderError classifies one non-2xx model-provider response. It
// reads only a bounded prefix of the body for structured code/type signals
// and the standard Retry-After header; the raw body is never retained.
func ClassifyProviderError(status int, header http.Header, body []byte, signals ExhaustionSignals) ProviderClassification {
	if signals.Empty() {
		signals = DefaultExhaustionSignals()
	}
	classification := ProviderClassification{Availability: ProviderUnknown}
	classification.RetryAfter = parseRetryAfter(header)
	code, errType := structuredErrorSignal(body)

	switch {
	case isContextSignal(code, errType):
		classification.Availability = ProviderContextExhausted
		classification.Code = firstSignal(code, errType)
		return classification
	case signals.matches(code, errType):
		classification.Availability = ProviderAllowanceExhausted
		classification.Code = firstSignal(code, errType)
		return classification
	case status == http.StatusPaymentRequired:
		classification.Availability = ProviderAllowanceExhausted
		classification.Code = firstSignal(code, errType)
		return classification
	}

	switch {
	case status == http.StatusTooManyRequests:
		classification.Availability = ProviderTransientRateLimit
		classification.Retryable = true
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		classification.Availability = ProviderAuthenticationFailed
	case status == http.StatusRequestTimeout:
		classification.Availability = ProviderUnavailable
		classification.Retryable = true
	case status >= 500:
		classification.Availability = ProviderUnavailable
		classification.Retryable = true
	case status == http.StatusBadRequest || status == http.StatusNotFound ||
		status == http.StatusUnprocessableEntity || status == http.StatusRequestEntityTooLarge:
		classification.Availability = ProviderRequestRejected
	}
	return classification
}

func (s ExhaustionSignals) matches(code, errType string) bool {
	for _, candidate := range []string{code, errType} {
		if candidate == "" {
			continue
		}
		for _, signal := range s.Codes {
			if strings.EqualFold(candidate, signal) {
				return true
			}
		}
		for _, signal := range s.Types {
			if strings.EqualFold(candidate, signal) {
				return true
			}
		}
	}
	return false
}

func firstSignal(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// isContextSignal recognizes the provider-neutral structured signals for a
// context/token-limit rejection.
func isContextSignal(code, errType string) bool {
	for _, candidate := range []string{code, errType} {
		switch strings.ToLower(candidate) {
		case "context_length_exceeded", "context_length_exceeded_error", "max_tokens_exceeded", "string_above_max_length":
			return true
		}
	}
	return false
}

// providerErrorEnvelope is the OpenAI-compatible error shape. Some providers
// nest under "error"; others place the fields at the top level. Only the
// structured code and type are read; the message is untrusted and ignored.
type providerErrorEnvelope struct {
	Error *providerErrorFields `json:"error"`
	Code  json.RawMessage      `json:"code"`
	Type  string               `json:"type"`
}

type providerErrorFields struct {
	Code json.RawMessage `json:"code"`
	Type string          `json:"type"`
}

// structuredErrorSignal extracts a bounded code and type from a provider error
// body. It returns empty strings when the body is not JSON or carries no
// usable signal. The code is normalized from a JSON string or number and
// dropped if it is not a short safe token.
func structuredErrorSignal(body []byte) (code, errType string) {
	if len(body) == 0 {
		return "", ""
	}
	var envelope providerErrorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return "", ""
	}
	rawCode := envelope.Code
	fields := envelope.Error
	errType = envelope.Type
	if fields != nil {
		if len(fields.Code) > 0 {
			rawCode = fields.Code
		}
		if fields.Type != "" {
			errType = fields.Type
		}
	}
	return normalizeProviderSignal(rawCode), normalizeProviderSignalText(errType)
}

// normalizeProviderSignal renders a JSON code (string or number) as a bounded
// safe token, or empty when it is not one.
func normalizeProviderSignal(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		var number json.Number
		if err := json.Unmarshal(raw, &number); err != nil {
			return ""
		}
		// Normalize through float64 so a numeric code renders the same string
		// on both the body and the decoded-stream paths (see wireCodeString).
		if value, err := number.Float64(); err == nil {
			text = strconv.FormatFloat(value, 'f', -1, 64)
		} else {
			text = number.String()
		}
	}
	return normalizeProviderSignalText(text)
}

// normalizeProviderSignalText keeps only a short, safe token: a real provider
// code is alphanumeric with underscores, dots, or hyphens. Anything else
// (including whitespace or prose) is dropped rather than surfaced.
func normalizeProviderSignalText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxProviderSignalLength {
		return ""
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '.' || r == '-':
		default:
			return ""
		}
	}
	return value
}

// parseRetryAfter reads the standard Retry-After header as either a number of
// seconds or an HTTP date. An unparseable or negative value yields zero: the
// harness never invents a reset time.
func parseRetryAfter(header http.Header) time.Duration {
	if header == nil {
		return 0
	}
	raw := strings.TrimSpace(header.Get("Retry-After"))
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil {
		if seconds <= 0 {
			return 0
		}
		return boundRetryAfter(time.Duration(seconds) * time.Second)
	}
	if when, err := http.ParseTime(raw); err == nil {
		until := time.Until(when)
		if until <= 0 {
			return 0
		}
		return boundRetryAfter(until)
	}
	return 0
}

func boundRetryAfter(d time.Duration) time.Duration {
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	return d
}

// classifyStreamSignal classifies a provider error object that arrived inside
// an HTTP 200 stream, where there is no status code to classify from. It
// recognizes exhaustion and context signals and otherwise reports unknown; the
// raw type/message are never retained.
func classifyStreamSignal(code, errType string, signals ExhaustionSignals) ProviderClassification {
	if signals.Empty() {
		signals = DefaultExhaustionSignals()
	}
	classification := ProviderClassification{Availability: ProviderUnknown}
	code = normalizeProviderSignalText(code)
	errType = normalizeProviderSignalText(errType)
	switch {
	case isContextSignal(code, errType):
		classification.Availability = ProviderContextExhausted
		classification.Code = firstSignal(code, errType)
	case signals.matches(code, errType):
		classification.Availability = ProviderAllowanceExhausted
		classification.Code = firstSignal(code, errType)
	}
	return classification
}

// providerClassificationOf extracts the classification an error carries, if
// any. Transport failures and unclassified errors return ok=false.
func providerClassificationOf(err error) (ProviderClassification, bool) {
	if err == nil {
		return ProviderClassification{}, false
	}
	var gatewayErr *GatewayError
	if errors.As(err, &gatewayErr) {
		return gatewayErr.classification(), true
	}
	return ProviderClassification{}, false
}
