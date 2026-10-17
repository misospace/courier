package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// The wiring proof for #296 (implementation child of #243): the gateway read
// of a bound model's advertised input limit, the per-role context-budget
// capability it feeds, and the per-session resolution of the effective
// budget. budget_test.go remains the source of truth for the resolution
// policy itself; this file proves only that the three seams reach it.

type modelInfoRecord struct {
	method string
	path   string
	auth   string
	accept string
}

type modelInfoRecorder struct {
	mu      sync.Mutex
	records []modelInfoRecord
}

func (r *modelInfoRecorder) requests() []modelInfoRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]modelInfoRecord(nil), r.records...)
}

// modelInfoGateway serves the gateway with handler, records what the
// model-info read asked for, and reports the closed server. The API key is
// set so a test can prove it never reaches an error message.
func modelInfoGateway(t *testing.T, handler http.HandlerFunc) (*Gateway, *modelInfoRecorder, func()) {
	t.Helper()
	recorder := &modelInfoRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.mu.Lock()
		recorder.records = append(recorder.records, modelInfoRecord{
			method: r.Method,
			path:   r.URL.Path,
			auth:   r.Header.Get("Authorization"),
			accept: r.Header.Get("Accept"),
		})
		recorder.mu.Unlock()
		handler(w, r)
	}))
	gateway := &Gateway{BaseURL: server.URL, APIKey: "sk-test-key", HTTP: server.Client()}
	return gateway, recorder, server.Close
}

func servingModelInfo(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/model/info" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func TestModelInputLimitReadsAdvertisedLimit(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		model string
		want  int
	}{
		{
			name:  "the nested model_info limit is the advertised limit",
			body:  `{"data":[{"model_name":"test/coordinator","model_info":{"max_input_tokens":128000}}]}`,
			model: "test/coordinator",
			want:  128000,
		},
		{
			name:  "an entry-level limit is the fallback",
			body:  `{"data":[{"model_name":"test/coordinator","max_input_tokens":96000}]}`,
			model: "test/coordinator",
			want:  96000,
		},
		{
			name:  "the nested limit outranks the entry-level one",
			body:  `{"data":[{"model_name":"test/coordinator","max_input_tokens":96000,"model_info":{"max_input_tokens":128000}}]}`,
			model: "test/coordinator",
			want:  128000,
		},
		{
			name:  "an entry id matches the bound model",
			body:  `{"data":[{"id":"test/coder","model_info":{"max_input_tokens":64000}}]}`,
			model: "test/coder",
			want:  64000,
		},
		{
			name:  "the first matching entry wins",
			body:  `{"data":[{"model_name":"other/model","max_input_tokens":1024},{"model_name":"test/coordinator","max_input_tokens":96000},{"model_name":"test/coordinator","max_input_tokens":2048}]}`,
			model: "test/coordinator",
			want:  96000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway, recorder, cleanup := modelInfoGateway(t, servingModelInfo(tt.body))
			defer cleanup()

			got, err := gateway.ModelInputLimit(context.Background(), tt.model)
			if err != nil {
				t.Fatalf("ModelInputLimit: %v", err)
			}
			if got != tt.want {
				t.Fatalf("limit = %d, want %d", got, tt.want)
			}
			requests := recorder.requests()
			if len(requests) != 1 {
				t.Fatalf("model info reads = %d, want exactly 1 (%+v)", len(requests), requests)
			}
			if requests[0].method != http.MethodGet || requests[0].path != "/model/info" {
				t.Fatalf("read %s %s, want GET /model/info", requests[0].method, requests[0].path)
			}
			if requests[0].auth != "Bearer sk-test-key" || requests[0].accept != "application/json" {
				t.Fatalf("read headers = %+v, want bearer auth and a JSON accept", requests[0])
			}
		})
	}
}

func TestModelInputLimitStatusFailureIsRedacted(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"message":"sk-test-key provider-internal detail","code":"upstream_down"}}`))
	})
	defer cleanup()

	limit, err := gateway.ModelInputLimit(context.Background(), "test/coordinator")
	if limit != 0 {
		t.Fatalf("limit = %d, want 0 on a failed read", limit)
	}
	var gatewayErr *GatewayError
	if !errors.As(err, &gatewayErr) {
		t.Fatalf("error = %v (%T), want *GatewayError", err, err)
	}
	if gatewayErr.Status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", gatewayErr.Status)
	}
	rendered := fmt.Sprintf("%v %+v", err, *gatewayErr)
	for _, secret := range []string{"sk-test-key", "provider-internal detail", "upstream_down"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("error %q retained provider body or key material", rendered)
		}
	}
	if !isProbeRetryable(err) {
		t.Fatalf("a 503 read must classify as retryable: %v", err)
	}
}

func TestModelInputLimitTransportFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	gateway := &Gateway{BaseURL: server.URL, HTTP: server.Client()}
	server.Close()

	limit, err := gateway.ModelInputLimit(context.Background(), "test/coordinator")
	if limit != 0 {
		t.Fatalf("limit = %d, want 0 on a transport failure", limit)
	}
	var transportErr *GatewayTransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("error = %v (%T), want *GatewayTransportError", err, err)
	}
	if !isProbeRetryable(err) {
		t.Fatalf("a transport failure must classify as retryable: %v", err)
	}
}

func TestModelInputLimitFailureMessages(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		model string
		want  string
	}{
		{
			name:  "an unknown model names the model",
			body:  `{"data":[{"model_name":"other/model","max_input_tokens":1024}]}`,
			model: "test/coordinator",
			want:  `harness: gateway model info names no entry for model "test/coordinator"`,
		},
		{
			name:  "an entry without a limit is not a small window",
			body:  `{"data":[{"model_name":"test/coordinator"}]}`,
			model: "test/coordinator",
			want:  `harness: gateway advertises no input limit for model "test/coordinator"`,
		},
		{
			name:  "a zero limit is not a usable window",
			body:  `{"data":[{"model_name":"test/coordinator","max_input_tokens":0}]}`,
			model: "test/coordinator",
			want:  `harness: gateway advertises no input limit for model "test/coordinator"`,
		},
		{
			name:  "an undecodable envelope fails closed",
			body:  `{"data": [`,
			model: "test/coordinator",
			want:  "harness: gateway model info is undecodable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(tt.body))
			defer cleanup()

			limit, err := gateway.ModelInputLimit(context.Background(), tt.model)
			if limit != 0 {
				t.Fatalf("limit = %d, want 0", limit)
			}
			if err == nil || err.Error() != tt.want {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

// TestModelInputLimitOverLongBodyFailsAsUndecodable pins the read bound: a
// hostile 200 that pads past maxModelInfoBody is cut off and fails as an
// unreadable envelope rather than being buffered at startup.
func TestModelInputLimitOverLongBodyFailsAsUndecodable(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		body := `{"data":[{"model_name":"test/coordinator","max_input_tokens":128000}],"pad":"` +
			strings.Repeat("a", maxModelInfoBody) + `"}`
		_, _ = w.Write([]byte(body))
	})
	defer cleanup()

	limit, err := gateway.ModelInputLimit(context.Background(), "test/coordinator")
	if limit != 0 {
		t.Fatalf("limit = %d, want 0", limit)
	}
	if err == nil || err.Error() != "harness: gateway model info is undecodable" {
		t.Fatalf("error = %v, want the fixed undecodable message", err)
	}
}

// --- capability: the per-role context budget -------------------------------

// budgetProbeDeps is a probe dependency set whose every other capability is
// healthy, so the gate outcome a test asserts is the context budget's alone.
func budgetProbeDeps(gateway *Gateway) ProbeDeps {
	return ProbeDeps{
		Gateway:  gateway,
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
		Mode:               "resolve-issue",
		ModelProbe:         func(context.Context, string) error { return nil },
		PublicationPresent: true,
		SnapshotPresent:    true,
	}
}

func TestContextBudgetCapabilityStates(t *testing.T) {
	declared := ContextPolicy{
		CompactAtPercent:  75,
		OutputReservation: 4096,
		Roles: map[string]RoleContextPolicy{
			"coordinator": {MaxInputTokens: 128000},
			"coder":       {MaxInputTokens: 32000},
		},
	}
	tests := []struct {
		name               string
		body               string
		context            ContextPolicy
		gateway            bool
		wantState          map[string]string
		wantRequired       map[string]bool
		wantGate           bool
		wantDetail         string
		wantDetailContains map[string]string
	}{
		{
			name:      "a gateway-verified limit is healthy",
			body:      `{"data":[{"model_name":"test/coordinator","model_info":{"max_input_tokens":128000}},{"model_name":"test/coder","model_info":{"max_input_tokens":32000}}]}`,
			context:   declared,
			gateway:   true,
			wantState: map[string]string{"coordinator": StateHealthy, "coder": StateHealthy},
			wantRequired: map[string]bool{
				"coordinator": true, "coder": true,
			},
			wantGate: true,
		},
		{
			name:      "a ceiling the gateway cannot confirm is configured",
			body:      `{"data":[]}`,
			context:   declared,
			gateway:   true,
			wantState: map[string]string{"coordinator": StateConfigured, "coder": StateConfigured},
			wantRequired: map[string]bool{
				"coordinator": true, "coder": true,
			},
			wantGate: true,
			// The ceiling is in use, but the operator still needs to know the
			// gateway could not confirm it and why.
			wantDetailContains: map[string]string{
				"coordinator": `the gateway read failed: harness: gateway model info names no entry for model "test/coordinator"`,
			},
		},
		{
			name:      "a declared budget with no capacity at all fails the gate",
			body:      `{"data":[]}`,
			context:   ContextPolicy{CompactAtPercent: 50},
			gateway:   true,
			wantState: map[string]string{"coordinator": StateUnavailable, "coder": StateUnavailable},
			wantRequired: map[string]bool{
				"coordinator": true, "coder": true,
			},
			wantGate: false,
			wantDetailContains: map[string]string{
				"coordinator": `gateway model info names no entry for model "test/coordinator"`,
			},
		},
		{
			name:      "an undeclared lane proceeds degraded",
			body:      `{"data":[]}`,
			gateway:   true,
			wantState: map[string]string{"coordinator": StateUnavailable, "coder": StateUnavailable},
			wantGate:  true,
			wantDetailContains: map[string]string{
				"coordinator": `gateway model info names no entry for model "test/coordinator"`,
				"coder":       `gateway model info names no entry for model "test/coder"`,
			},
		},
		{
			// Without a gateway the model binding is required-unavailable too,
			// so the gate fails on that capability; this row pins the budget
			// row's own detail wording.
			name:       "a build with no gateway says so",
			gateway:    false,
			wantState:  map[string]string{"coordinator": StateUnavailable, "coder": StateUnavailable},
			wantGate:   false,
			wantDetail: "no model gateway is configured in this build",
		},
		{
			name:      "an all-zero per-role entry declares nothing",
			body:      `{"data":[]}`,
			context:   ContextPolicy{Roles: map[string]RoleContextPolicy{"coordinator": {}}},
			gateway:   true,
			wantState: map[string]string{"coordinator": StateUnavailable},
			// B2: a bare `roles: {coordinator: {}}` entry declares nothing, so
			// the role is named without gating every run.
			wantRequired:       map[string]bool{"coordinator": false},
			wantGate:           true,
			wantDetailContains: map[string]string{"coordinator": `gateway model info names no entry`},
		},
		{
			name:      "a declared role this run does not bind is named",
			body:      `{"data":[{"model_name":"test/coordinator","model_info":{"max_input_tokens":128000}}]}`,
			context:   ContextPolicy{Roles: map[string]RoleContextPolicy{"coordinator": {MaxInputTokens: 128000}, "codr": {MaxInputTokens: 32000}}},
			gateway:   true,
			wantState: map[string]string{"coordinator": StateHealthy, "codr": StateUnavailable},
			wantRequired: map[string]bool{
				"coordinator": true, "codr": false,
			},
			wantGate:           true,
			wantDetailContains: map[string]string{"codr": `the lane declares a context budget for role "codr", which this run does not bind`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := budgetProbeDeps(nil)
			if tt.gateway {
				gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(tt.body))
				defer cleanup()
				deps.Gateway = gateway
			}
			deps.Context = tt.context
			caps := ProbeCapabilities(context.Background(), deps)

			for role, wantState := range tt.wantState {
				capability, ok := findCapability(caps, "context-budget:"+role)
				if !ok {
					t.Fatalf("capability table = %+v", caps)
				}
				if capability.State != wantState {
					t.Fatalf("context-budget:%s = %+v, want state %s", role, capability, wantState)
				}
				if capability.Required != tt.wantRequired[role] {
					t.Fatalf("context-budget:%s Required = %v, want %v", role, capability.Required, tt.wantRequired[role])
				}
				if tt.wantDetail != "" && capability.Detail != tt.wantDetail {
					t.Fatalf("context-budget:%s detail = %q, want %q", role, capability.Detail, tt.wantDetail)
				}
				if tt.wantDetailContains[role] != "" && !strings.Contains(capability.Detail, tt.wantDetailContains[role]) {
					t.Fatalf("context-budget:%s detail = %q, want it to contain %q", role, capability.Detail, tt.wantDetailContains[role])
				}
			}
			assertShortBudgetDetails(t, caps)
			_, gateErr := Gate(caps)
			if (gateErr == nil) != tt.wantGate {
				t.Fatalf("gate err = %v, want pass = %v", gateErr, tt.wantGate)
			}
			if gateErr != nil {
				var failed *GateError
				if !errors.As(gateErr, &failed) {
					t.Fatalf("gate error = %v (%T), want *GateError", gateErr, gateErr)
				}
				for _, capability := range failed.Failed {
					role := strings.TrimPrefix(capability.Name, "context-budget:")
					if strings.HasPrefix(capability.Name, "context-budget:") && !tt.wantRequired[role] {
						t.Fatalf("an undeclared role's budget gated the run: %+v", capability)
					}
				}
			}
		})
	}
}

// assertShortBudgetDetails pins that a budget capability's reason stays short:
// it flows into the trusted termination reason, which is byte-bounded.
func assertShortBudgetDetails(t *testing.T, caps []Capability) {
	t.Helper()
	for _, capability := range caps {
		if !strings.HasPrefix(capability.Name, "context-budget:") {
			continue
		}
		if len(capability.Detail) > 200 {
			t.Fatalf("%s detail is %d bytes: %q", capability.Name, len(capability.Detail), capability.Detail)
		}
	}
}

// TestContextBudgetCapabilityRedactsProbeFailure pins that a failing gateway
// read reaches the capability table as a classified, redacted reason while a
// declared ceiling still resolves to configured rather than unavailable.
func TestContextBudgetCapabilityRedactsProbeFailure(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"sk-test-key internal leak"}}`))
	})
	defer cleanup()
	deps := budgetProbeDeps(gateway)
	deps.Context = ContextPolicy{Roles: map[string]RoleContextPolicy{"coordinator": {MaxInputTokens: 128000}}}

	caps := ProbeCapabilities(context.Background(), deps)
	capability, ok := findCapability(caps, "context-budget:coordinator")
	if !ok {
		t.Fatalf("capability table = %+v", caps)
	}
	if capability.State != StateConfigured || !capability.Required {
		t.Fatalf("declared ceiling with a failed read = %+v, want configured and required", capability)
	}
	if strings.Contains(capability.Detail, "sk-test-key") || strings.Contains(capability.Detail, "internal leak") {
		t.Fatalf("detail retained provider body or key material: %q", capability.Detail)
	}
	if _, err := Gate(caps); err != nil {
		t.Fatalf("a configured budget must not fail the gate: %v", err)
	}
}

// TestContextBudgetCapabilityRetryable pins that a transient probe failure on
// an undeclared lane is reported as a retryable, non-gating unavailability.
func TestContextBudgetCapabilityRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	gateway := &Gateway{BaseURL: server.URL, HTTP: server.Client()}
	server.Close()

	caps := ProbeCapabilities(context.Background(), budgetProbeDeps(gateway))
	capability, ok := findCapability(caps, "context-budget:coordinator")
	if !ok {
		t.Fatalf("capability table = %+v", caps)
	}
	if capability.State != StateUnavailable || capability.Required || !capability.Retryable {
		t.Fatalf("unreachable gateway on an undeclared lane = %+v, want optional retryable unavailability", capability)
	}
	if _, err := Gate(caps); err != nil {
		t.Fatalf("an undeclared budget must not gate the run: %v", err)
	}
}

// --- resolution: one entry point, shared by the probe and the loop ----------

// TestContextPolicyRoleBudgetTruthTable is the wiring-level restatement of the
// budget truth table: ContextPolicy.RoleBudget is the single resolution entry
// point, so the probe and the session loop cannot disagree on the effective
// limit.
func TestContextPolicyRoleBudgetTruthTable(t *testing.T) {
	tests := []struct {
		name      string
		policy    ContextPolicy
		probed    int
		wantState string
		wantLimit int
		wantErr   error
	}{
		{
			name:      "effective is the lower of declared and probed",
			policy:    ContextPolicy{Roles: map[string]RoleContextPolicy{"coordinator": {MaxInputTokens: 128000}}},
			probed:    96000,
			wantState: StateHealthy, wantLimit: 96000,
		},
		{
			name:      "a verified limit alone is healthy",
			policy:    ContextPolicy{CompactAtPercent: 75},
			probed:    128000,
			wantState: StateHealthy, wantLimit: 128000,
		},
		{
			name:      "a declared ceiling alone is configured",
			policy:    ContextPolicy{Roles: map[string]RoleContextPolicy{"coordinator": {MaxInputTokens: 128000}}},
			probed:    0,
			wantState: StateConfigured, wantLimit: 128000,
		},
		{
			name:    "a declared budget with no capacity fails closed",
			policy:  ContextPolicy{CompactAtPercent: 75},
			probed:  0,
			wantErr: ErrNoContextCapacity,
		},
		{
			name: "a per-role compact override alone is declared and fails closed without a limit",
			policy: ContextPolicy{Roles: map[string]RoleContextPolicy{
				"coordinator": {CompactAtPercent: 80},
			}},
			probed:  0,
			wantErr: ErrNoContextCapacity,
		},
		{
			name: "a per-role reservation override alone is declared and fails closed without a limit",
			policy: ContextPolicy{Roles: map[string]RoleContextPolicy{
				"coordinator": {OutputReservation: 512},
			}},
			probed:  0,
			wantErr: ErrNoContextCapacity,
		},
		{
			name: "per-role overrides with a verified limit resolve at that limit",
			policy: ContextPolicy{Roles: map[string]RoleContextPolicy{
				"coordinator": {CompactAtPercent: 80, OutputReservation: 512},
			}},
			probed:    128000,
			wantState: StateHealthy, wantLimit: 128000,
		},
		{
			name:      "an undeclared lane has no budget and no error",
			policy:    ContextPolicy{},
			probed:    0,
			wantState: "",
		},
		{
			name:      "an all-zero per-role entry is undeclared, not a declared budget",
			policy:    ContextPolicy{Roles: map[string]RoleContextPolicy{"coordinator": {}}},
			probed:    0,
			wantState: "",
		},
		{
			name:    "a compact fraction at the ceiling is rejected",
			policy:  ContextPolicy{CompactAtPercent: 150},
			probed:  128000,
			wantErr: errors.New("compactAtPercent"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			budget, err := tt.policy.RoleBudget("coordinator", tt.probed)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) && (err == nil || !strings.Contains(err.Error(), tt.wantErr.Error())) {
					t.Fatalf("error = %v, want it to match %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantState == "" {
				if budget != (ContextBudget{}) {
					t.Fatalf("budget = %+v, want the zero budget meaning no policy", budget)
				}
				return
			}
			if budget.State != tt.wantState || budget.EffectiveLimit != tt.wantLimit {
				t.Fatalf("budget = %+v, want state %s at limit %d", budget, tt.wantState, tt.wantLimit)
			}
			if budget.Role != "coordinator" {
				t.Fatalf("budget role = %q, want the resolved role", budget.Role)
			}
		})
	}
}

// TestContextBudgetProbeFailureClassification pins that a failed gateway read
// and a bad declaration are told apart for the operator: the read carries the
// classified, redacted status reason and stays retryable, the declaration
// carries the resolution error and is not retryable. Only a retryable gate
// failure keeps courier-control's re-probe loop applying.
func TestContextBudgetProbeFailureClassification(t *testing.T) {
	declared := ContextPolicy{Roles: map[string]RoleContextPolicy{"coordinator": {MaxInputTokens: 128000}}}

	t.Run("an invalid declaration is not retryable", func(t *testing.T) {
		gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(
			`{"data":[{"model_name":"test/coordinator","model_info":{"max_input_tokens":128000}}]}`))
		defer cleanup()
		deps := budgetProbeDeps(gateway)
		deps.Context = ContextPolicy{CompactAtPercent: 150}

		caps := ProbeCapabilities(context.Background(), deps)
		capability, ok := findCapability(caps, "context-budget:coordinator")
		if !ok {
			t.Fatalf("capability table = %+v", caps)
		}
		if capability.State != StateUnavailable || capability.Retryable {
			t.Fatalf("invalid declaration = %+v, want a non-retryable unavailability", capability)
		}
		if !strings.Contains(capability.Detail, "compactAtPercent 150") {
			t.Fatalf("detail = %q, want the resolution error", capability.Detail)
		}
		_, gateErr := Gate(caps)
		var failed *GateError
		if !errors.As(gateErr, &failed) {
			t.Fatalf("gate error = %v, want *GateError", gateErr)
		}
		if failed.AllRetryable() {
			t.Fatalf("a bad declaration must not look transient: %v", gateErr)
		}
	})

	t.Run("a transient read failure is retryable and named", func(t *testing.T) {
		gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		defer cleanup()
		deps := budgetProbeDeps(gateway)
		deps.Context = declared

		caps := ProbeCapabilities(context.Background(), deps)
		capability, ok := findCapability(caps, "context-budget:coordinator")
		if !ok {
			t.Fatalf("capability table = %+v", caps)
		}
		// A declared ceiling plus a failed read still resolves to configured:
		// the ceiling is in use, the gateway simply could not confirm it.
		if capability.State != StateConfigured || !capability.Required {
			t.Fatalf("declared ceiling with a 503 read = %+v, want configured and required", capability)
		}

		unreadable, _, cleanupUnreachable := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})
		defer cleanupUnreachable()
		deps.Gateway = unreadable
		// Declared fraction with no ceiling: the read is the only source of
		// capacity, so the failure is the reason there is no budget.
		deps.Context = ContextPolicy{CompactAtPercent: 75}
		caps = ProbeCapabilities(context.Background(), deps)
		capability, _ = findCapability(caps, "context-budget:coordinator")
		if capability.State != StateUnavailable || capability.Retryable {
			t.Fatalf("a 404 read = %+v, want a non-retryable unavailability", capability)
		}
		if !strings.Contains(capability.Detail, "404") || !strings.Contains(capability.Detail, "model or route not found") {
			t.Fatalf("detail = %q, want the classified status category", capability.Detail)
		}
		assertShortBudgetDetails(t, caps)
	})

	t.Run("an invalid declaration outranks an unrelated read failure", func(t *testing.T) {
		gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		defer cleanup()
		deps := budgetProbeDeps(gateway)
		deps.Context = ContextPolicy{CompactAtPercent: 150}

		caps := ProbeCapabilities(context.Background(), deps)
		capability, ok := findCapability(caps, "context-budget:coordinator")
		if !ok {
			t.Fatalf("capability table = %+v", caps)
		}
		if capability.State != StateUnavailable || capability.Retryable || capability.Availability != "" {
			t.Fatalf("invalid declaration plus a 503 read = %+v, want a non-retryable unavailability", capability)
		}
		if !strings.Contains(capability.Detail, "compactAtPercent 150") {
			t.Fatalf("detail = %q, want the declaration error first", capability.Detail)
		}
		if !strings.Contains(capability.Detail, "; the gateway read failed: harness: gateway returned status 503") {
			t.Fatalf("detail = %q, want the read failure appended as secondary", capability.Detail)
		}
		_, gateErr := Gate(caps)
		var failed *GateError
		if !errors.As(gateErr, &failed) {
			t.Fatalf("gate error = %v, want *GateError", gateErr)
		}
		if failed.AllRetryable() {
			t.Fatalf("a permanent misconfiguration must not send the re-probe loop into a long backoff: %v", gateErr)
		}
		assertShortBudgetDetails(t, caps)
	})

	t.Run("a transport failure renders only its fixed class", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		unreachable := &Gateway{BaseURL: server.URL, APIKey: "sk-test-key", HTTP: server.Client()}
		server.Close()

		deps := budgetProbeDeps(unreachable)
		deps.Context = declared
		caps := ProbeCapabilities(context.Background(), deps)
		capability, ok := findCapability(caps, "context-budget:coordinator")
		if !ok {
			t.Fatalf("capability table = %+v", caps)
		}
		// configured is not an unavailable state, so Retryable is not
		// meaningful there; the gate never reads it off a non-failure.
		if capability.State != StateConfigured {
			t.Fatalf("declared ceiling plus an unreachable gateway = %+v, want configured", capability)
		}
		if !strings.Contains(capability.Detail, "the gateway read failed: harness: gateway transport failed") {
			t.Fatalf("detail = %q, want the fixed transport class", capability.Detail)
		}
		for _, leak := range []string{server.URL, "127.0.0.1", "dial tcp", "sk-test-key"} {
			if strings.Contains(capability.Detail, leak) {
				t.Fatalf("detail = %q, must not carry %q", capability.Detail, leak)
			}
		}
		assertShortBudgetDetails(t, caps)
	})

	t.Run("a declared budget with a transient read failure keeps the gate retryable", func(t *testing.T) {
		gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		defer cleanup()
		deps := budgetProbeDeps(gateway)
		// Declared fraction with no ceiling: the gateway read is the only
		// source of capacity, so its failure is the reason for no budget.
		deps.Context = ContextPolicy{CompactAtPercent: 75}

		caps := ProbeCapabilities(context.Background(), deps)
		capability, ok := findCapability(caps, "context-budget:coordinator")
		if !ok {
			t.Fatalf("capability table = %+v", caps)
		}
		if capability.State != StateUnavailable || !capability.Required || !capability.Retryable {
			t.Fatalf("declared budget with a 503 read = %+v, want a required retryable unavailability", capability)
		}
		if capability.Availability != ProviderUnavailable {
			t.Fatalf("availability = %q, want %q", capability.Availability, ProviderUnavailable)
		}
		_, gateErr := Gate(caps)
		var failed *GateError
		if !errors.As(gateErr, &failed) {
			t.Fatalf("gate error = %v, want *GateError", gateErr)
		}
		if !failed.AllRetryable() {
			t.Fatalf("gate failure = %v, want every failure retryable", gateErr)
		}
		assertShortBudgetDetails(t, caps)
	})
}

// TestContextBudgetPerRoleOverrideIsDeclared pins that a per-role
// compactAtPercent/outputReservation override is a declaration on its own:
// forRole honors it, so declaresRole must too, or an explicitly configured
// role is silently treated as undeclared and never gets compaction.
func TestContextBudgetPerRoleOverrideIsDeclared(t *testing.T) {
	t.Run("each override alone with a failed read is required and fails the gate", func(t *testing.T) {
		gateway, _, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		defer cleanup()
		for _, policy := range []ContextPolicy{
			{Roles: map[string]RoleContextPolicy{"coordinator": {CompactAtPercent: 80}}},
			{Roles: map[string]RoleContextPolicy{"coordinator": {OutputReservation: 512}}},
		} {
			deps := budgetProbeDeps(gateway)
			deps.Context = policy
			caps := ProbeCapabilities(context.Background(), deps)
			capability, ok := findCapability(caps, "context-budget:coordinator")
			if !ok {
				t.Fatalf("capability table = %+v", caps)
			}
			if capability.State != StateUnavailable || !capability.Required || !capability.Retryable {
				t.Fatalf("override with a 503 read = %+v, want a required retryable unavailability", capability)
			}
			_, gateErr := Gate(caps)
			var failed *GateError
			if !errors.As(gateErr, &failed) {
				t.Fatalf("gate error = %v, want *GateError", gateErr)
			}
			if !failed.AllRetryable() {
				t.Fatalf("gate failure = %v, want every failure retryable", gateErr)
			}
			assertShortBudgetDetails(t, caps)
		}
	})

	t.Run("overrides with a verified limit are healthy and required", func(t *testing.T) {
		gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(
			`{"data":[{"model_name":"test/coordinator","model_info":{"max_input_tokens":128000}}]}`))
		defer cleanup()
		deps := budgetProbeDeps(gateway)
		deps.Context = ContextPolicy{Roles: map[string]RoleContextPolicy{
			"coordinator": {CompactAtPercent: 80, OutputReservation: 512},
		}}

		caps := ProbeCapabilities(context.Background(), deps)
		capability, ok := findCapability(caps, "context-budget:coordinator")
		if !ok || capability.State != StateHealthy || !capability.Required {
			t.Fatalf("overrides with a verified limit = %+v, want healthy and required", capability)
		}
		if _, err := Gate(caps); err != nil {
			t.Fatalf("gate = %v, want the declared override to pass", err)
		}
		assertShortBudgetDetails(t, caps)
	})
}

// TestContextBudgetOneReadPerModel pins that two roles bound to one model
// share a single authenticated metadata read.
func TestContextBudgetOneReadPerModel(t *testing.T) {
	gateway, recorder, cleanup := modelInfoGateway(t, servingModelInfo(
		`{"data":[{"model_name":"test/shared","model_info":{"max_input_tokens":128000}}]}`))
	defer cleanup()
	deps := budgetProbeDeps(gateway)
	shared, err := BindRoles(map[string]string{"coordinator": "test/shared", "coder": "test/shared"})
	if err != nil {
		t.Fatalf("BindRoles: %v", err)
	}
	deps.Bindings = shared

	caps := ProbeCapabilities(context.Background(), deps)
	if got := len(recorder.requests()); got != 1 {
		t.Fatalf("model info reads = %d, want exactly 1 for two roles on one model", got)
	}
	for _, role := range []string{"coordinator", "coder"} {
		capability, ok := findCapability(caps, "context-budget:"+role)
		if !ok || capability.State != StateHealthy {
			t.Fatalf("context-budget:%s = %+v, want healthy from the shared read", role, capability)
		}
	}
}

// TestContextBudgetFailedReadIsCachedPerModel pins the failure half of the
// per-model cache: a shared read that fails is attempted once, and both roles
// report the same classified reason rather than re-issuing the read.
func TestContextBudgetFailedReadIsCachedPerModel(t *testing.T) {
	gateway, recorder, cleanup := modelInfoGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	defer cleanup()
	deps := budgetProbeDeps(gateway)
	shared, err := BindRoles(map[string]string{"coordinator": "test/shared", "coder": "test/shared"})
	if err != nil {
		t.Fatalf("BindRoles: %v", err)
	}
	deps.Bindings = shared
	// Declared fraction, no ceiling: the read is the only capacity source, so
	// both roles report the failed read.
	deps.Context = ContextPolicy{CompactAtPercent: 75}

	caps := ProbeCapabilities(context.Background(), deps)
	if got := len(recorder.requests()); got != 1 {
		t.Fatalf("model info reads = %d, want exactly 1 for two roles on one model", got)
	}
	coordinator, _ := findCapability(caps, "context-budget:coordinator")
	coder, _ := findCapability(caps, "context-budget:coder")
	if coordinator.State != StateUnavailable || !coordinator.Retryable {
		t.Fatalf("coordinator = %+v, want a retryable unavailability", coordinator)
	}
	if coordinator.Detail != coder.Detail || coordinator.Detail != "harness: gateway returned status 503 (gateway unavailable)" {
		t.Fatalf("shared read reason = %q / %q, want the same classified 503 reason", coordinator.Detail, coder.Detail)
	}
	if coordinator.Availability != ProviderUnavailable || coder.Availability != ProviderUnavailable {
		t.Fatalf("availability = %q / %q, want %q", coordinator.Availability, coder.Availability, ProviderUnavailable)
	}
	assertShortBudgetDetails(t, caps)
}

// TestContextBudgetUnboundDeclaredRoleIsNamed pins S4's typo case: a declared
// role this run does not bind is a named capability, never a gate failure.
func TestContextBudgetUnboundDeclaredRoleIsNamed(t *testing.T) {
	gateway, _, cleanup := modelInfoGateway(t, servingModelInfo(
		`{"data":[{"model_name":"test/coordinator","model_info":{"max_input_tokens":128000}}]}`))
	defer cleanup()
	deps := budgetProbeDeps(gateway)
	deps.Context = ContextPolicy{Roles: map[string]RoleContextPolicy{
		"coordinator": {MaxInputTokens: 128000},
		"reviewer":    {MaxInputTokens: 64000},
		"architect":   {MaxInputTokens: 32000},
		"coder":       {},
	}}

	caps := ProbeCapabilities(context.Background(), deps)
	var named []string
	for _, capability := range caps {
		if strings.HasPrefix(capability.Name, "context-budget:") && strings.Contains(capability.Detail, "does not bind") {
			named = append(named, capability.Name)
			if capability.Required {
				t.Fatalf("%s must not gate a run that legitimately does not bind it: %+v", capability.Name, capability)
			}
		}
	}
	if len(named) != 2 || named[0] != "context-budget:architect" || named[1] != "context-budget:reviewer" {
		t.Fatalf("unbound declared roles = %v, want architect and reviewer in sorted order", named)
	}
	if _, err := Gate(caps); err != nil {
		t.Fatalf("an unbound declared role must not fail the gate: %v", err)
	}
	assertShortBudgetDetails(t, caps)
}
