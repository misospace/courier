package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/executor"
)

func TestClassifyProviderError(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		header         http.Header
		body           string
		signals        ExhaustionSignals
		want           ProviderAvailability
		wantCode       string
		wantRetryable  bool
		wantRetryAfter time.Duration
		wantDeferred   bool
	}{
		{
			name:         "configured plan-limit code is exhaustion",
			status:       http.StatusTooManyRequests,
			body:         `{"error":{"code":2056,"message":"Token Plan usage limit reached"}}`,
			signals:      ExhaustionSignals{Codes: []string{"2056"}},
			want:         ProviderAllowanceExhausted,
			wantCode:     "2056",
			wantDeferred: true,
		},
		{
			name:         "provider-neutral insufficient_quota type is exhaustion",
			status:       http.StatusTooManyRequests,
			body:         `{"error":{"type":"insufficient_quota"}}`,
			want:         ProviderAllowanceExhausted,
			wantCode:     "insufficient_quota",
			wantDeferred: true,
		},
		{
			name:         "payment required is exhaustion",
			status:       http.StatusPaymentRequired,
			want:         ProviderAllowanceExhausted,
			wantDeferred: true,
		},
		{
			name:          "plain 429 is a transient rate limit",
			status:        http.StatusTooManyRequests,
			body:          `{"error":{"message":"slow down"}}`,
			want:          ProviderTransientRateLimit,
			wantRetryable: true,
			// A rate limit that persists through the in-process retries is
			// still a recoverable availability condition, not a hard failure.
			wantDeferred: true,
		},
		{
			name:           "transient 429 with a long reset hint is deferred",
			status:         http.StatusTooManyRequests,
			header:         http.Header{"Retry-After": []string{"3600"}},
			want:           ProviderTransientRateLimit,
			wantRetryable:  true,
			wantRetryAfter: time.Hour,
			wantDeferred:   true,
		},
		{
			name:          "non-allowlisted rate-limit type stays transient",
			status:        http.StatusTooManyRequests,
			body:          `{"error":{"type":"rate_limit_error"}}`,
			want:          ProviderTransientRateLimit,
			wantRetryable: true,
			wantDeferred:  true,
		},
		{
			name:         "allowlisted code on a 5xx is still exhaustion",
			status:       http.StatusInternalServerError,
			body:         `{"error":{"code":"insufficient_quota"}}`,
			want:         ProviderAllowanceExhausted,
			wantCode:     "insufficient_quota",
			wantDeferred: true,
		},
		{
			name:   "forbidden is an auth failure",
			status: http.StatusForbidden,
			want:   ProviderAuthenticationFailed,
		},
		{
			name:          "server error is retryable provider unavailability",
			status:        http.StatusServiceUnavailable,
			want:          ProviderUnavailable,
			wantRetryable: true,
		},
		{
			name:     "context-length rejection is context exhaustion",
			status:   http.StatusBadRequest,
			body:     `{"error":{"code":"context_length_exceeded"}}`,
			want:     ProviderContextExhausted,
			wantCode: "context_length_exceeded",
		},
		{
			name:   "not found is a request rejection",
			status: http.StatusNotFound,
			want:   ProviderRequestRejected,
		},
		{
			name:   "unrecognized status is preserved as unknown",
			status: http.StatusTeapot,
			want:   ProviderUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyProviderError(tt.status, tt.header, []byte(tt.body), tt.signals)
			if got.Availability != tt.want {
				t.Fatalf("availability = %q, want %q", got.Availability, tt.want)
			}
			if got.Code != tt.wantCode {
				t.Fatalf("code = %q, want %q", got.Code, tt.wantCode)
			}
			if got.Retryable != tt.wantRetryable {
				t.Fatalf("retryable = %v, want %v", got.Retryable, tt.wantRetryable)
			}
			if got.RetryAfter != tt.wantRetryAfter {
				t.Fatalf("retry-after = %v, want %v", got.RetryAfter, tt.wantRetryAfter)
			}
			if got.Deferred() != tt.wantDeferred {
				t.Fatalf("deferred = %v, want %v", got.Deferred(), tt.wantDeferred)
			}
		})
	}
}

func TestClassifyProviderErrorDefaultsWhenSignalsEmpty(t *testing.T) {
	got := ClassifyProviderError(http.StatusTooManyRequests, nil, []byte(`{"error":{"type":"insufficient_quota"}}`), ExhaustionSignals{})
	if got.Availability != ProviderAllowanceExhausted {
		t.Fatalf("empty signals must fall back to the defaults: %+v", got)
	}
}

func TestStructuredErrorSignalIsBoundedAndSafe(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode string
		wantType string
	}{
		{"nested string code", `{"error":{"code":"insufficient_quota","type":"billing_error"}}`, "insufficient_quota", "billing_error"},
		{"nested numeric code", `{"error":{"code":2056}}`, "2056", ""},
		{"top-level code", `{"code":"quota_exceeded"}`, "quota_exceeded", ""},
		{"unsafe code with spaces", `{"error":{"code":"has space"}}`, "", ""},
		{"oversized code", fmt.Sprintf(`{"error":{"code":%q}}`, strings.Repeat("x", maxProviderSignalLength+1)), "", ""},
		{"non-json", `not json`, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, errType := structuredErrorSignal([]byte(tt.body))
			if code != tt.wantCode || errType != tt.wantType {
				t.Fatalf("structuredErrorSignal = (%q, %q), want (%q, %q)", code, errType, tt.wantCode, tt.wantType)
			}
		})
	}
}

func TestParseExhaustionSignalsExtendsDefaults(t *testing.T) {
	signals := ParseExhaustionSignals("2056, codeX, 2056", "typeY")
	if !containsFold(signals.Codes, "insufficient_quota") {
		t.Fatalf("configured signals dropped the defaults: %+v", signals)
	}
	if !containsFold(signals.Codes, "2056") || !containsFold(signals.Codes, "codeX") {
		t.Fatalf("configured codes missing: %+v", signals)
	}
	if countFold(signals.Codes, "2056") != 1 {
		t.Fatalf("duplicate codes not deduped: %+v", signals.Codes)
	}
	if !containsFold(signals.Types, "typeY") || !containsFold(signals.Types, "billing_error") {
		t.Fatalf("configured types missing: %+v", signals)
	}
}

func containsFold(values []string, want string) bool {
	return countFold(values, want) > 0
}

func countFold(values []string, want string) int {
	count := 0
	for _, value := range values {
		if strings.EqualFold(value, want) {
			count++
		}
	}
	return count
}

func TestProviderClassificationOfUnwrapsGatewayError(t *testing.T) {
	base := gatewayStatusError(http.StatusTooManyRequests, nil, []byte(`{"error":{"type":"insufficient_quota"}}`), ExhaustionSignals{})
	wrapped := fmt.Errorf("stream failed: %w", base)
	got, ok := providerClassificationOf(wrapped)
	if !ok || got.Availability != ProviderAllowanceExhausted {
		t.Fatalf("providerClassificationOf = (%+v, %v)", got, ok)
	}
	if _, ok := providerClassificationOf(errors.New("plain")); ok {
		t.Fatal("a non-gateway error must not classify")
	}
}

// --- integration: retry policy, gateway redaction, coordinator routing ------

func TestConfirmedExhaustionIsNotRetriedInProcess(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(gatewayResponse{
		status:  http.StatusTooManyRequests,
		errBody: `{"error":{"type":"insufficient_quota"}}`,
	})
	defer cleanup()
	session, _ := NewSession(gateway, testBindings(), RoleCoordinator, nil)
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindError {
		t.Fatalf("terminal event = %+v, want error", last)
	}
	if got := mocker.requestCount(); got != 1 {
		t.Fatalf("gateway requests = %d, want 1 (exhaustion is not retried in-process)", got)
	}
}

func TestTransientRateLimitIsRetriedInProcess(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusTooManyRequests, errBody: `{"error":{"message":"slow down"}}`},
		gatewayResponse{body: sse(contentChunk("working"), finishChunk("stop"))},
	)
	defer cleanup()
	session, _ := NewSession(gateway, testBindings(), RoleCoordinator, nil)
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if got := mocker.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2 (transient 429 retried)", got)
	}
}

func TestClassifiedGatewayErrorDoesNotEchoProviderBody(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{
		status:  http.StatusTooManyRequests,
		errBody: `{"error":{"code":"insufficient_quota","message":"secret-key sk-abc"}}`,
	})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindError {
		t.Fatalf("terminal event = %+v", last)
	}
	var gatewayErr *GatewayError
	if !errors.As(last.Err, &gatewayErr) {
		t.Fatalf("error type = %T, want *GatewayError", last.Err)
	}
	if gatewayErr.Availability != ProviderAllowanceExhausted || gatewayErr.ProviderCode != "insufficient_quota" {
		t.Fatalf("classification = %+v", gatewayErr)
	}
	if strings.Contains(last.Err.Error(), "secret-key") || strings.Contains(last.Err.Error(), "sk-abc") {
		t.Fatalf("classified error echoed the provider body: %v", last.Err)
	}
}

func TestGatewayParsesRetryAfterHint(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{
		status:  http.StatusTooManyRequests,
		header:  http.Header{"Retry-After": []string{"120"}},
		errBody: `{"error":{"message":"slow down"}}`,
	})
	defer cleanup()
	events, err := gateway.StreamChat(context.Background(), ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	var gatewayErr *GatewayError
	if !errors.As(events[len(events)-1].Err, &gatewayErr) {
		t.Fatalf("error type = %T", events[len(events)-1].Err)
	}
	if gatewayErr.RetryAfter != 120*time.Second {
		t.Fatalf("retry-after = %v, want 120s", gatewayErr.RetryAfter)
	}
}

func TestCoordinatorEndsBlockedExternalOnAllowanceExhaustion(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{
		status:  http.StatusTooManyRequests,
		errBody: `{"error":{"type":"insufficient_quota"}}`,
	})
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{Gateway: gateway, Bindings: testBindings()})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err != nil {
		t.Fatalf("exhaustion must not be an infrastructure error: %v", result.Err)
	}
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q, want blocked_external", result.Outcome)
	}
	if !strings.Contains(result.Reason, "allowance-exhausted") {
		t.Fatalf("reason = %q, want the availability class", result.Reason)
	}
}

func TestCoordinatorEndsBlockedExternalOnPersistentRateLimit(t *testing.T) {
	script := make([]gatewayResponse, maxStreamRetries+1)
	for i := range script {
		script[i] = gatewayResponse{status: http.StatusTooManyRequests, errBody: `{"error":{"message":"slow down"}}`}
	}
	gateway, mocker, cleanup := gatewayFor(script...)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{Gateway: gateway, Bindings: testBindings()})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err != nil {
		t.Fatalf("a persistent rate limit must be recoverable, not an infra error: %v", result.Err)
	}
	if result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("outcome = %q, want blocked_external", result.Outcome)
	}
	if got := mocker.requestCount(); got != maxStreamRetries+1 {
		t.Fatalf("gateway requests = %d, want %d retries before deferring", got, maxStreamRetries+1)
	}
}

func TestCoordinatorKeepsInfraErrorOnProviderOutage(t *testing.T) {
	script := make([]gatewayResponse, maxStreamRetries+1)
	for i := range script {
		script[i] = gatewayResponse{status: http.StatusServiceUnavailable}
	}
	gateway, _, cleanup := gatewayFor(script...)
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{Gateway: gateway, Bindings: testBindings()})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err == nil || result.Outcome != "" {
		t.Fatalf("a provider outage must stay an infrastructure failure: %+v", result)
	}
}

func TestExhaustedModelBindingGateIsRecoverable(t *testing.T) {
	deps := ProbeDeps{
		Gateway:  &Gateway{BaseURL: "http://example.invalid"},
		Bindings: testBindings(),
		Broker: capabilityBroker(map[string]any{
			"provider": map[string]any{"name": "acme-forge"},
			"operations": []map[string]any{
				{"name": "read-work-item", "available": true},
				{"name": "create-pull-request", "available": true},
				{"name": "update-pull-request", "available": true},
				{"name": "comment", "available": true},
			},
			"git": map[string]any{"ready": true},
		}, 0),
		Mode: "resolve-issue",
		ModelProbe: func(_ context.Context, model string) error {
			if model == "test/coordinator" {
				return gatewayStatusError(http.StatusTooManyRequests, nil, []byte(`{"error":{"type":"insufficient_quota"}}`), ExhaustionSignals{})
			}
			return nil
		},
		PublicationPresent: true,
		SnapshotPresent:    true,
	}
	caps := ProbeCapabilities(context.Background(), deps)
	coordCap, ok := findCapability(caps, "model-binding:coordinator")
	if !ok || coordCap.Availability != ProviderAllowanceExhausted {
		t.Fatalf("coordinator capability = %+v, want allowance-exhausted", coordCap)
	}
	_, err := Gate(caps)
	var gateErr *GateError
	if !errors.As(err, &gateErr) || !gateErr.Exhausted() {
		t.Fatalf("gate error = %v, want a recoverable exhausted gate", err)
	}
}
