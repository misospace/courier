package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
)

// The control-entrypoint wiring proof for #636: the LaneProfile's declared
// context policy rides the run-context environment into courier-control, the
// capability gate verifies it against the gateway, and the coordinator's
// sessions execute under exactly the limits the gate verified — reaching
// real session compaction and the session.compacted telemetry. The
// resolution truth table itself lives in internal/harness/budget_test.go and
// is not re-proved here.

// staticTokenSource serves the broker-audience token the probe client sends.
type staticTokenSource struct{}

func (staticTokenSource) BrokerToken() ([]byte, error) { return []byte("test-token"), nil }

// budgetBroker serves the broker capability report the gate requires, so the
// wiring tests exercise the real gate rather than a budget-only slice.
func budgetBroker(t *testing.T) *harness.BrokerProbeClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/capabilities" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"provider":{"name":"test","type":"github"},` +
			`"operations":[{"name":"read-work-item","available":true},{"name":"create-pull-request","available":true},` +
			`{"name":"update-pull-request","available":true},{"name":"comment","available":true}],` +
			`"git":{"endpoint":"https://git.invalid/acme/widgets","ready":true}}`))
	}))
	t.Cleanup(server.Close)
	return &harness.BrokerProbeClient{BaseURL: server.URL, TokenSource: staticTokenSource{}, HTTP: server.Client()}
}

// budgetModelInfoGateway serves the gateway model-info read with body and
// reports the closer.
func budgetModelInfoGateway(t *testing.T, handler http.HandlerFunc) (*harness.Gateway, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &harness.Gateway{BaseURL: server.URL, APIKey: "sk-test-key", HTTP: server.Client()}, server.Close
}

func servingCoordinatorAndCoderInfo() http.HandlerFunc {
	return servingModelInfo(`{"data":[` +
		`{"model_name":"test/coordinator","model_info":{"max_input_tokens":128000}},` +
		`{"model_name":"test/coder","model_info":{"max_input_tokens":64000}}]}`)
}

// servingModelInfo serves the gateway model-info read with a fixed body.
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

// budgetEnv renders the run-context environment an operator would pin into
// the control pod at admission, with the lane's declared context policy.
func budgetEnv(policy *courierv1alpha1.ContextPolicy, roles map[string]string) map[string]string {
	inv := executor.Invocation{
		RunName:   "run-budget",
		Namespace: "courier-system",
		Mode:      courierv1alpha1.ModeResolveIssue,
		Ref:       7,
		Goal:      "Resolve issue #7.",
		Model:     roles["coordinator"],
		Roles:     roles,
		Workspace: "/workspace",
		Context:   policy,
	}
	values := map[string]string{}
	for _, env := range executor.RunContextEnvironment(inv) {
		values[env.Name] = env.Value
	}
	return values
}

// wiredBudgets mirrors runArgs' budget wiring exactly (#636): reconstruct
// the invocation from the pinned env, convert the declared policy, record
// every declared role's resolution input through the probe, pass the gate,
// and resolve the budgets once in NewCoordinator. The gate must pass or the
// test fails: these are wiring proofs, not gate-failure proofs.
func wiredBudgets(t *testing.T, values map[string]string, gateway *harness.Gateway) (*harness.Coordinator, harness.Bindings, map[string]harness.ContextBudgetInput) {
	t.Helper()
	inv, err := executor.InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("InvocationFromEnv: %v", err)
	}
	policy := harness.ContextPolicyFromLane(inv.Context)
	bindings, err := harness.BindRoles(inv.Roles)
	if err != nil {
		t.Fatalf("BindRoles: %v", err)
	}
	budgetInputs := make(map[string]harness.ContextBudgetInput)
	deps := harness.ProbeDeps{
		Gateway:            gateway,
		Bindings:           bindings,
		Broker:             budgetBroker(t),
		Mode:               string(inv.Mode),
		PublicationPresent: true,
		SnapshotPresent:    true,
		Context:            policy,
		OnContextBudget: func(role string, input harness.ContextBudgetInput) {
			if policy.DeclaresRole(role) {
				budgetInputs[role] = input
			}
		},
		ModelProbe: func(context.Context, string) error { return nil },
	}
	caps := harness.ProbeCapabilities(context.Background(), deps)
	if _, err := harness.Gate(caps); err != nil {
		t.Fatalf("gate failed: %v", err)
	}
	coordinator, err := harness.NewCoordinator(harness.CoordinatorConfig{
		Gateway:        gateway,
		Bindings:       bindings,
		ContextBudgets: budgetInputs,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return coordinator, bindings, budgetInputs
}

// TestControlWiringCarriesLaneBudgetToCoordinator is the accepted path: a
// LaneProfile-declared budget reconstructed from the pinned env reaches the
// gate, is verified against the gateway for both the coordinator and a
// delegated worker role, and resolves in the coordinator as the session
// policy — effective limit min(declared, verified), the declared compact
// fraction, and per-role overrides carried through.
func TestControlWiringCarriesLaneBudgetToCoordinator(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, servingCoordinatorAndCoderInfo())
	defer closeGateway()
	policy := &courierv1alpha1.ContextPolicy{
		Roles: map[string]courierv1alpha1.ContextBudget{
			"coordinator": {MaxInputTokens: 96000},
			"coder":       {MaxInputTokens: 32000, OutputReservation: 2048},
		},
	}
	roles := map[string]string{"coordinator": "test/coordinator", "coder": "test/coder", "reviewer": "test/coder"}
	values := budgetEnv(policy, roles)

	coordinator, _, budgetInputs := wiredBudgets(t, values, gateway)

	// The probe recorded exactly the declared roles — the bound-but-
	// undeclared reviewer is absent, so it keeps the unbudgeted legacy
	// behavior instead of failing NewCoordinator.
	if len(budgetInputs) != 2 {
		t.Fatalf("budget inputs = %+v, want coordinator and coder only", budgetInputs)
	}
	if _, ok := budgetInputs["reviewer"]; ok {
		t.Fatalf("an undeclared bound role reached the coordinator budgets: %+v", budgetInputs)
	}
	// The recorded inputs carry the verified limits the gate read.
	if input := budgetInputs["coordinator"]; input.DeclaredMaxInputTokens != 96000 || input.ProbedMaxInputTokens != 128000 {
		t.Fatalf("coordinator input = %+v, want declared 96000 verified 128000", input)
	}
	if input := budgetInputs["coder"]; input.DeclaredMaxInputTokens != 32000 || input.ProbedMaxInputTokens != 64000 {
		t.Fatalf("coder input = %+v, want declared 32000 verified 64000", input)
	}

	// The coordinator resolves the session policy from those same inputs.
	for role, wantLimit := range map[string]int{"coordinator": 96000, "coder": 32000} {
		budget, ok := coordinator.Budget(role)
		if !ok {
			t.Fatalf("the coordinator carries no budget for role %s", role)
		}
		if budget.State != harness.StateHealthy || budget.EffectiveLimit != wantLimit {
			t.Fatalf("%s budget = %+v, want healthy at %d", role, budget, wantLimit)
		}
		// Default compact fraction 75% of the effective limit.
		if budget.CompactPoint != wantLimit*75/100 {
			t.Fatalf("%s compact point = %d, want %d", role, budget.CompactPoint, wantLimit*75/100)
		}
	}
	coder, _ := coordinator.Budget("coder")
	if coder.OutputReservation != 2048 {
		t.Fatalf("coder output reservation = %d, want the declared 2048", coder.OutputReservation)
	}
	coordinatorBudget, _ := coordinator.Budget("coordinator")
	if coordinatorBudget.OutputReservation != 0 {
		t.Fatalf("coordinator output reservation = %d, want the undeclared zero", coordinatorBudget.OutputReservation)
	}
}

// TestControlWiringDeclaredOnlyBudgetIsConfigured pins the declared-only
// classification end to end: a ceiling the gateway could not verify is
// configured at the gate — in use, never healthy, never a gate failure — and
// the coordinator enforces the declared limit, never an invented window.
func TestControlWiringDeclaredOnlyBudgetIsConfigured(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/model/info" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeGateway()
	policy := &courierv1alpha1.ContextPolicy{
		Roles: map[string]courierv1alpha1.ContextBudget{"coordinator": {MaxInputTokens: 128000}},
	}
	values := budgetEnv(policy, map[string]string{"coordinator": "test/coordinator"})

	coordinator, _, budgetInputs := wiredBudgets(t, values, gateway)
	input := budgetInputs["coordinator"]
	if input.DeclaredMaxInputTokens != 128000 || input.ProbedMaxInputTokens != 0 {
		t.Fatalf("recorded input = %+v, want the declared ceiling with no verified limit", input)
	}
	budget, ok := coordinator.Budget("coordinator")
	if !ok {
		t.Fatal("the coordinator carries no budget for the declared role")
	}
	if budget.State != harness.StateConfigured || budget.EffectiveLimit != 128000 {
		t.Fatalf("budget = %+v, want configured at the declared 128000", budget)
	}
}

// TestControlWiringUndeclaredRoleStaysUnbudgeted pins the degraded legacy
// path: a bound role the lane declares nothing for is never budgeted and
// never gates, even when its gateway read fails.
func TestControlWiringUndeclaredRoleStaysUnbudgeted(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/model/info" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeGateway()
	policy := &courierv1alpha1.ContextPolicy{
		Roles: map[string]courierv1alpha1.ContextBudget{"coordinator": {MaxInputTokens: 128000}},
	}
	values := budgetEnv(policy, map[string]string{"coordinator": "test/coordinator", "coder": "test/coder"})

	coordinator, _, budgetInputs := wiredBudgets(t, values, gateway)
	if _, ok := budgetInputs["coder"]; ok {
		t.Fatalf("an undeclared role reached the coordinator budgets: %+v", budgetInputs)
	}
	if _, ok := coordinator.Budget("coder"); ok {
		t.Fatal("an undeclared role carries a budget it never opted into")
	}
	if _, ok := coordinator.Budget("coordinator"); !ok {
		t.Fatal("the declared role lost its budget")
	}
}

// TestControlWiringDefaultedLaneBudgetsEveryBoundRole pins the production
// policy shape (#636): the LaneProfile CRD defaults compactAtPercent to 75
// whenever a context block exists, so a lane that declares only a coder
// ceiling declares a usable policy for every bound role — the coordinator
// runs budgeted at the gateway-verified window, and its model-info read
// becomes a required capability. An operator who caps one role has opted
// every role into budget enforcement.
func TestControlWiringDefaultedLaneBudgetsEveryBoundRole(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, servingCoordinatorAndCoderInfo())
	defer closeGateway()
	// Exactly the shape the API server stores after defaulting: the global
	// fraction 75 is always present once a context block is.
	policy := &courierv1alpha1.ContextPolicy{
		CompactAtPercent: 75,
		Roles:            map[string]courierv1alpha1.ContextBudget{"coder": {MaxInputTokens: 32000}},
	}
	values := budgetEnv(policy, map[string]string{"coordinator": "test/coordinator", "coder": "test/coder"})

	coordinator, _, budgetInputs := wiredBudgets(t, values, gateway)
	if len(budgetInputs) != 2 {
		t.Fatalf("budget inputs = %+v, want every bound role under the defaulted fraction", budgetInputs)
	}
	coordinatorBudget, ok := coordinator.Budget("coordinator")
	if !ok || coordinatorBudget.State != harness.StateHealthy || coordinatorBudget.EffectiveLimit != 128000 {
		t.Fatalf("coordinator budget = %+v, want healthy at the verified window", coordinatorBudget)
	}
	if coordinatorBudget.CompactPoint != 128000*75/100 {
		t.Fatalf("coordinator compact point = %d, want the defaulted 75%%", coordinatorBudget.CompactPoint)
	}
	coderBudget, ok := coordinator.Budget("coder")
	if !ok || coderBudget.EffectiveLimit != 32000 {
		t.Fatalf("coder budget = %+v, want the declared ceiling", coderBudget)
	}
}

// TestControlWiringDefaultedLaneFailsClosedOnUnverifiableCoordinator pins the
// gate consequence of the defaulted fraction: with a context block present,
// a bound role the lane declared no ceiling for still requires a verified
// read, so a gateway that cannot report the coordinator's model fails the
// run closed rather than leaving it silently unbudgeted.
func TestControlWiringDefaultedLaneFailsClosedOnUnverifiableCoordinator(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, servingModelInfo(
		`{"data":[{"model_name":"test/coder","model_info":{"max_input_tokens":64000}}]}`))
	defer closeGateway()
	policy := &courierv1alpha1.ContextPolicy{
		CompactAtPercent: 75,
		Roles:            map[string]courierv1alpha1.ContextBudget{"coder": {MaxInputTokens: 32000}},
	}
	values := budgetEnv(policy, map[string]string{"coordinator": "test/coordinator", "coder": "test/coder"})

	inv, err := executor.InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	wired := harness.ContextPolicyFromLane(inv.Context)
	bindings, err := harness.BindRoles(inv.Roles)
	if err != nil {
		t.Fatal(err)
	}
	deps := harness.ProbeDeps{
		Gateway:            gateway,
		Bindings:           bindings,
		Broker:             budgetBroker(t),
		Mode:               string(inv.Mode),
		PublicationPresent: true,
		SnapshotPresent:    true,
		Context:            wired,
		ModelProbe:         func(context.Context, string) error { return nil },
	}
	caps := harness.ProbeCapabilities(context.Background(), deps)
	_, gateErr := harness.Gate(caps)
	if gateErr == nil {
		t.Fatal("a defaulted lane whose coordinator model cannot be verified must fail the gate closed")
	}
	if !strings.Contains(gateErr.Error(), "context-budget:coordinator") {
		t.Fatalf("gate error = %v, want it to name the coordinator's budget capability", gateErr)
	}
}

// TestControlWiringInvalidDeclarationFailsTheGate pins fail-closed
// classification for a declaration the resolver cannot honor: it is an
// unavailable required capability, never a degraded run on an invented
// policy.
func TestControlWiringInvalidDeclarationFailsTheGate(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, servingCoordinatorAndCoderInfo())
	defer closeGateway()
	policy := &courierv1alpha1.ContextPolicy{
		CompactAtPercent: 200, // outside the resolver's [1,99]
		Roles:            map[string]courierv1alpha1.ContextBudget{"coordinator": {MaxInputTokens: 96000}},
	}
	values := budgetEnv(policy, map[string]string{"coordinator": "test/coordinator"})

	inv, err := executor.InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	wired := harness.ContextPolicyFromLane(inv.Context)
	bindings, err := harness.BindRoles(inv.Roles)
	if err != nil {
		t.Fatal(err)
	}
	deps := harness.ProbeDeps{
		Gateway:            gateway,
		Bindings:           bindings,
		Broker:             budgetBroker(t),
		Mode:               string(inv.Mode),
		PublicationPresent: true,
		SnapshotPresent:    true,
		Context:            wired,
		ModelProbe:         func(context.Context, string) error { return nil },
	}
	caps := harness.ProbeCapabilities(context.Background(), deps)
	if _, err := harness.Gate(caps); err == nil {
		t.Fatal("an invalid declaration must fail the gate closed")
	}
}

// TestControlWiringMissingPolicyKeepsLegacyBehavior pins rollout
// compatibility: an invocation with no context policy — an old LaneProfile —
// gates, starts, and carries no budget anywhere. The unbudgeted legacy
// behavior is documented, not silently claimed as enforcement.
func TestControlWiringMissingPolicyKeepsLegacyBehavior(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, servingCoordinatorAndCoderInfo())
	defer closeGateway()
	values := budgetEnv(nil, map[string]string{"coordinator": "test/coordinator", "coder": "test/coder"})

	coordinator, _, budgetInputs := wiredBudgets(t, values, gateway)
	if len(budgetInputs) != 0 {
		t.Fatalf("budget inputs = %+v, want none for a lane with no policy", budgetInputs)
	}
	for _, role := range []string{"coordinator", "coder"} {
		if budget, ok := coordinator.Budget(role); ok {
			t.Fatalf("%s carries budget %+v, want none — no policy means the harness owns none", role, budget)
		}
	}
}

// TestControlWiringFailsClosedOnMalformedPolicy pins the entrypoint's
// fail-closed contract: a malformed policy is a configuration failure
// surfaced before any probe or session, never a silently unbudgeted run.
func TestControlWiringFailsClosedOnMalformedPolicy(t *testing.T) {
	values := budgetEnv(nil, map[string]string{"coordinator": "test/coordinator"})
	values[executor.EnvContextPolicy] = "{not a policy"
	if _, err := executor.InvocationFromEnv(func(name string) string { return values[name] }); !errors.Is(err, executor.ErrInvalidContextPolicy) {
		t.Fatalf("malformed policy error = %v, want ErrInvalidContextPolicy", err)
	}
}

// TestControlWiringRestartReconstructsIdenticalPolicy pins the
// persistence/restart contract: the env is pinned into the pod spec at
// admission, so a restarted control pod reconstructs the identical policy
// and resolves the identical budgets — the policy is pinned, not reloaded
// from the live LaneProfile.
func TestControlWiringRestartReconstructsIdenticalPolicy(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, servingCoordinatorAndCoderInfo())
	defer closeGateway()
	policy := &courierv1alpha1.ContextPolicy{
		Roles: map[string]courierv1alpha1.ContextBudget{"coordinator": {MaxInputTokens: 96000}},
	}
	values := budgetEnv(policy, map[string]string{"coordinator": "test/coordinator"})

	first, _, _ := wiredBudgets(t, values, gateway)
	second, _, _ := wiredBudgets(t, values, gateway)
	firstBudget, firstOK := first.Budget("coordinator")
	secondBudget, secondOK := second.Budget("coordinator")
	if firstOK != secondOK || firstBudget != secondBudget {
		t.Fatalf("restart resolved a different budget: %+v then %+v", firstBudget, secondBudget)
	}
}

// compactingSink captures the session.compacted payload a session emits.
type compactingSink struct {
	events []harness.CompactEvent
}

func (s *compactingSink) SessionCompacted(event harness.CompactEvent) {
	s.events = append(s.events, event)
}

// TestControlWiringBudgetDrivesSessionCompaction closes the chain: the
// budget the coordinator resolved from the gate's recorded inputs is the
// budget a real session enforces — a transcript that crosses the compact
// point compacts at the step boundary, a reservation that would overrun the
// effective limit compacts below it, and the session.compacted payload
// carries the role and its bound model.
func TestControlWiringBudgetDrivesSessionCompaction(t *testing.T) {
	gateway, closeGateway := budgetModelInfoGateway(t, servingCoordinatorAndCoderInfo())
	defer closeGateway()
	policy := &courierv1alpha1.ContextPolicy{
		Roles: map[string]courierv1alpha1.ContextBudget{
			"coordinator": {MaxInputTokens: 8000, OutputReservation: 3000},
		},
	}
	values := budgetEnv(policy, map[string]string{"coordinator": "test/coordinator"})

	coordinator, bindings, _ := wiredBudgets(t, values, gateway)
	budget, ok := coordinator.Budget("coordinator")
	if !ok {
		t.Fatal("the coordinator carries no budget for the declared role")
	}
	if budget.EffectiveLimit != 8000 || budget.CompactPoint != 6000 || budget.OutputReservation != 3000 {
		t.Fatalf("resolved budget = %+v, want effective 8000, compact point 6000, reservation 3000", budget)
	}

	sink := &compactingSink{}
	session, err := harness.NewSession(gateway, bindings, "coordinator", nil,
		harness.WithContextBudget(budget), harness.WithCompactionSink(sink))
	if err != nil {
		t.Fatal(err)
	}
	session.Pin(harness.Message{Role: "system", Content: "goal"})
	// The transcript estimate is content length over 4 (tokensPerChar): 800
	// characters is ~200 tokens. Thirteen assistant/tool turn pairs land at
	// ~5240 tokens — below the 6000-token compact point, but prompt plus the
	// declared 3000-token output reservation crosses the 8000-token limit,
	// so the step boundary must fire on the reservation alone.
	for i := 0; i < 13; i++ {
		session.Append(harness.Message{Role: "assistant", Content: strings.Repeat("a", 800)})
		session.Append(harness.Message{Role: "tool", Content: strings.Repeat("t", 800), ToolCallID: "c"})
	}
	event, compacted, err := session.Compact(nil)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if !compacted {
		t.Fatalf("Compact() did not fire on the output reservation; budget = %+v", budget)
	}
	if len(sink.events) != 1 || sink.events[0] != event {
		t.Fatalf("sink events = %+v, want exactly the returned event", sink.events)
	}
	if event.Role != "coordinator" {
		t.Fatalf("event role = %q, want the coordinator role", event.Role)
	}
	if event.Model != "test/coordinator" {
		t.Fatalf("event model = %q, want the session's bound model", event.Model)
	}
	if event.After >= event.Before || event.NetSaved != event.Before-event.After {
		t.Fatalf("event accounting = %+v, want after < before with net = before-after", event)
	}
	if !budget.FitsAfterCompaction(event.After) {
		t.Fatalf("the compacted transcript (%d after) does not fit the effective limit with the reservation", event.After)
	}
}
