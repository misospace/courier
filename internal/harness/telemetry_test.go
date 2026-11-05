package harness

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
)

// --- run model-telemetry tally (#738) --------------------------------------
//
// The pure aggregation lives here; the wiring that feeds it (the capability
// probe, the sessions, the coordinator) is covered by their own tests. These
// tests pin the invariants an operator reads from the terminal summary: the
// request split by class and by role/model cell, the usage outcome, the
// provider token sums versus the harness estimate, the compaction account,
// nil-receiver safety, and the chained-sink forwarding the structured-event
// adapter rides.

func probeObservation(role, model string) RequestObservation {
	return RequestObservation{Class: RequestProbe, Role: role, Model: model, OK: true}
}

func workObservation(role, model string, attempt int, usage *Usage, estimated int) RequestObservation {
	return RequestObservation{Class: RequestWork, Role: role, Model: model, Attempt: attempt, OK: true, Usage: usage, EstimatedPromptTokens: estimated}
}

// TestModelTelemetryTalliesProbeAndWork pins the class split: probes and work
// count separately and together, and a work attempt that was a retry is
// counted as a retry in both the total and its cell.
func TestModelTelemetryTalliesProbeAndWork(t *testing.T) {
	tally := NewModelTelemetry(nil)
	tally.ModelRequestObserved(probeObservation("coordinator", "test/coordinator"))
	tally.ModelRequestObserved(probeObservation("coder", "test/coder"))
	tally.ModelRequestObserved(workObservation("coordinator", "test/coordinator", 0, nil, 100))
	tally.ModelRequestObserved(workObservation("coordinator", "test/coordinator", 1, nil, 150))
	tally.ModelRequestObserved(workObservation("coder", "test/coder", 0, nil, 80))

	summary := tally.Summary()
	if summary.ProbeRequests != 2 {
		t.Fatalf("ProbeRequests = %d, want 2", summary.ProbeRequests)
	}
	if summary.WorkRequests != 3 {
		t.Fatalf("WorkRequests = %d, want 3", summary.WorkRequests)
	}
	if summary.TotalRequests != 5 {
		t.Fatalf("TotalRequests = %d, want 5", summary.TotalRequests)
	}
	if summary.Retries != 1 {
		t.Fatalf("Retries = %d, want 1 (one work attempt at index 1)", summary.Retries)
	}

	coordinator := summary.cell(t, "coordinator", "test/coordinator")
	if coordinator.Requests != 3 {
		t.Fatalf("coordinator cell Requests = %d, want 3 (one probe + two work)", coordinator.Requests)
	}
	if coordinator.Retries != 1 {
		t.Fatalf("coordinator cell Retries = %d, want 1", coordinator.Retries)
	}
	coder := summary.cell(t, "coder", "test/coder")
	if coder.Requests != 2 {
		t.Fatalf("coder cell Requests = %d, want 2 (one probe + one work)", coder.Requests)
	}
	if coder.Retries != 0 {
		t.Fatalf("coder cell Retries = %d, want 0", coder.Retries)
	}
}

// summary.cell looks up one role/model cell in a sorted summary, failing the
// test if it is absent.
func (s ModelTelemetrySummary) cell(t *testing.T, role, model string) RoleModelRequests {
	t.Helper()
	for _, cell := range s.ByRoleModel {
		if cell.Role == role && cell.Model == model {
			return cell
		}
	}
	t.Fatalf("no cell for %s/%s in %v", role, model, s.ByRoleModel)
	return RoleModelRequests{}
}

// TestModelTelemetrySplitsUsageReportedAndMissing pins that a request whose
// provider reported no usage is an explicit absence, tallied separately from
// the requests whose did — never folded into a zero.
func TestModelTelemetrySplitsUsageReportedAndMissing(t *testing.T) {
	tally := NewModelTelemetry(nil)
	tally.ModelRequestObserved(workObservation("coordinator", "test/coordinator", 0, &Usage{InputTokens: 10}, 10))
	tally.ModelRequestObserved(workObservation("coordinator", "test/coordinator", 1, nil, 20))
	tally.ModelRequestObserved(probeObservation("coder", "test/coder")) // probe with no usage is an absence

	summary := tally.Summary()
	if summary.UsageReported != 1 {
		t.Fatalf("UsageReported = %d, want 1", summary.UsageReported)
	}
	if summary.UsageMissing != 2 {
		t.Fatalf("UsageMissing = %d, want 2", summary.UsageMissing)
	}
	if got := summary.cell(t, "coder", "test/coder").UsageMissing; got != 1 {
		t.Fatalf("coder cell UsageMissing = %d, want 1 (the probe reported no usage)", got)
	}
}

// TestModelTelemetrySumsProviderTokensAndPeaks pins that the token sums run
// only over the requests whose provider reported usage, that the input peak
// is the largest single reported input, and that the harness-estimated peak
// is tracked separately — never conflated with the provider-reported figures.
func TestModelTelemetrySumsProviderTokensAndPeaks(t *testing.T) {
	tally := NewModelTelemetry(nil)
	tally.ModelRequestObserved(workObservation("coordinator", "test/coordinator", 0, &Usage{InputTokens: 100, OutputTokens: 20, CacheReadTokens: 5, CacheWriteTokens: 3}, 90))
	tally.ModelRequestObserved(workObservation("coordinator", "test/coordinator", 1, &Usage{InputTokens: 300, OutputTokens: 10, CacheReadTokens: 0, CacheWriteTokens: 0}, 300))
	tally.ModelRequestObserved(workObservation("coder", "test/coder", 0, nil, 400)) // no provider usage, but the largest estimate

	summary := tally.Summary()
	if summary.InputTokens != 400 {
		t.Fatalf("InputTokens = %d, want 400 (100+300, the missing one excluded)", summary.InputTokens)
	}
	if summary.OutputTokens != 30 {
		t.Fatalf("OutputTokens = %d, want 30 (20+10)", summary.OutputTokens)
	}
	if summary.CacheReadTokens != 5 {
		t.Fatalf("CacheReadTokens = %d, want 5", summary.CacheReadTokens)
	}
	if summary.CacheWriteTokens != 3 {
		t.Fatalf("CacheWriteTokens = %d, want 3", summary.CacheWriteTokens)
	}
	if summary.MaxInputTokens != 300 {
		t.Fatalf("MaxInputTokens = %d, want 300 (largest single reported input)", summary.MaxInputTokens)
	}
	if summary.MaxEstimatedPromptTokens != 400 {
		t.Fatalf("MaxEstimatedPromptTokens = %d, want 400 (the missing-usage request's estimate)", summary.MaxEstimatedPromptTokens)
	}
}

// TestModelTelemetryTalliesCompactions pins the compaction account: the count,
// the total reclaimed, and the before/after of the last compaction applied.
func TestModelTelemetryTalliesCompactions(t *testing.T) {
	tally := NewModelTelemetry(nil)
	tally.SessionCompacted(CompactEvent{Role: "coordinator", Model: "test/coordinator", Before: 1000, After: 600, NetSaved: 400})
	tally.SessionCompacted(CompactEvent{Role: "coder", Model: "test/coder", Before: 900, After: 500, NetSaved: 400})

	summary := tally.Summary()
	if summary.Compactions != 2 {
		t.Fatalf("Compactions = %d, want 2", summary.Compactions)
	}
	if summary.CompactionsNetSaved != 800 {
		t.Fatalf("CompactionsNetSaved = %d, want 800", summary.CompactionsNetSaved)
	}
	if summary.LastCompactionBefore != 900 || summary.LastCompactionAfter != 500 {
		t.Fatalf("last compaction before/after = %d/%d, want 900/500 (the last applied)", summary.LastCompactionBefore, summary.LastCompactionAfter)
	}
}

// TestModelTelemetryNilReceiverIsANoOp pins that a nil tally absorbs every
// method without panicking: wiring a tally is optional, and a nil tally
// reports the zero summary.
func TestModelTelemetryNilReceiverIsANoOp(t *testing.T) {
	var tally *ModelTelemetry
	tally.ModelRequestObserved(probeObservation("coordinator", "test/coordinator"))
	tally.ModelRequestObserved(workObservation("coder", "test/coder", 0, &Usage{InputTokens: 5}, 5))
	tally.SessionCompacted(CompactEvent{Before: 100, After: 50, NetSaved: 50})

	summary := tally.Summary()
	if summary.TotalRequests != 0 || summary.ProbeRequests != 0 || summary.WorkRequests != 0 ||
		summary.UsageReported != 0 || summary.UsageMissing != 0 || summary.InputTokens != 0 ||
		summary.Compactions != 0 || summary.CompactionsNetSaved != 0 || len(summary.ByRoleModel) != 0 {
		t.Fatalf("nil receiver Summary = %+v, want the zero summary", summary)
	}
}

// TestModelTelemetryForwardsToNextSinkInOrder pins that a telemetry built with
// a next sink delivers every request observation to it, in the order it
// tallies them — the seam the structured-event adapter rides.
func TestModelTelemetryForwardsToNextSinkInOrder(t *testing.T) {
	var received []RequestObservation
	next := sinkFunc(func(observation RequestObservation) {
		received = append(received, observation)
	})
	tally := NewModelTelemetry(next)

	probe := probeObservation("coordinator", "test/coordinator")
	work := workObservation("coordinator", "test/coordinator", 0, nil, 10)
	tally.ModelRequestObserved(probe)
	tally.ModelRequestObserved(work)

	if len(received) != 2 {
		t.Fatalf("next sink received %d observations, want 2", len(received))
	}
	if received[0] != probe || received[1] != work {
		t.Fatalf("next sink received %+v, want the observations in order", received)
	}
}

// sinkFunc adapts a function to ModelRequestSink for tests.
type sinkFunc func(RequestObservation)

func (f sinkFunc) ModelRequestObserved(observation RequestObservation) { f(observation) }

// TestModelTelemetrySortsByRoleThenModel pins the summary table's stable
// ordering: by role, then by model, so an operator reading the summary sees a
// deterministic layout regardless of the order requests arrived.
func TestModelTelemetrySortsByRoleThenModel(t *testing.T) {
	tally := NewModelTelemetry(nil)
	// Insert out of order: coder, then coordinator twice with two models.
	tally.ModelRequestObserved(probeObservation("coder", "z/coder"))
	tally.ModelRequestObserved(probeObservation("coordinator", "b/coordinator"))
	tally.ModelRequestObserved(probeObservation("coordinator", "a/coordinator"))

	summary := tally.Summary()
	if len(summary.ByRoleModel) != 3 {
		t.Fatalf("ByRoleModel has %d cells, want 3", len(summary.ByRoleModel))
	}
	want := [][2]string{{"coder", "z/coder"}, {"coordinator", "a/coordinator"}, {"coordinator", "b/coordinator"}}
	for i, cell := range summary.ByRoleModel {
		if cell.Role != want[i][0] || cell.Model != want[i][1] {
			t.Fatalf("ByRoleModel[%d] = %s/%s, want %s/%s", i, cell.Role, cell.Model, want[i][0], want[i][1])
		}
	}
}

// TestModelTelemetryIsConcurrentSafe pins that the tally's mutex holds under
// concurrent delivery from many goroutines — the coordinator's sessions and
// the probe all deliver to one shared tally. The race detector would flag a
// missing lock; this also pins the final totals are exact.
func TestModelTelemetryIsConcurrentSafe(t *testing.T) {
	tally := NewModelTelemetry(nil)
	const (
		goroutines = 16
		per        = 25
	)
	var wait sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wait.Add(1)
		go func(g int) {
			defer wait.Done()
			for i := 0; i < per; i++ {
				usage := &Usage{InputTokens: 10, OutputTokens: 1}
				if g%2 == 1 {
					usage = nil // a share report nothing
				}
				tally.ModelRequestObserved(workObservation("coordinator", "test/coordinator", 0, usage, 5))
			}
			tally.SessionCompacted(CompactEvent{Before: 100, After: 50, NetSaved: 50})
		}(g)
	}
	wait.Wait()

	total := goroutines * per
	summary := tally.Summary()
	if summary.TotalRequests != total {
		t.Fatalf("TotalRequests = %d, want %d", summary.TotalRequests, total)
	}
	if summary.WorkRequests != total {
		t.Fatalf("WorkRequests = %d, want %d", summary.WorkRequests, total)
	}
	if summary.Compactions != goroutines {
		t.Fatalf("Compactions = %d, want %d", summary.Compactions, goroutines)
	}
	if summary.CompactionsNetSaved != goroutines*50 {
		t.Fatalf("CompactionsNetSaved = %d, want %d", summary.CompactionsNetSaved, goroutines*50)
	}
}

// TestProbeRecordsOneObservationPerUniqueModel pins the capability gate's
// model accounting (#738): the gate probes each unique bound model once,
// attributes the observation to the first sorted role bound to it, and rides
// the provider's usage from the probe's terminal when it reported one. Roles
// that share a model share the single probe; the /model/info reads are gateway
// metadata and are never recorded.
func TestProbeRecordsOneObservationPerUniqueModel(t *testing.T) {
	sink := &recordingRequestSink{}
	bindings, err := BindRoles(map[string]string{"coordinator": "shared/model", "coder": "shared/model"})
	if err != nil {
		t.Fatal(err)
	}
	deps := ProbeDeps{
		Transport: &Gateway{},
		Bindings:  bindings,
		ModelProbe: func(context.Context, string) ([]Event, error) {
			return []Event{{Kind: KindFinal, Usage: &Usage{InputTokens: 50, OutputTokens: 5}}}, nil
		},
		Requests: sink,
	}
	_ = probeModelBindings(context.Background(), deps)

	if len(sink.observations) != 1 {
		t.Fatalf("probe observations = %d, want 1 (one per unique model, not per role)", len(sink.observations))
	}
	obs := sink.observations[0]
	if obs.Class != RequestProbe {
		t.Fatalf("probe observation class = %s, want probe", obs.Class)
	}
	if obs.Role != "coder" {
		t.Fatalf("probe observation role = %s, want coder (the first sorted role bound to the model)", obs.Role)
	}
	if obs.Model != "shared/model" {
		t.Fatalf("probe observation model = %s, want shared/model", obs.Model)
	}
	if !obs.OK {
		t.Fatal("probe observation is not OK, but the probe succeeded")
	}
	if obs.Usage == nil || obs.Usage.InputTokens != 50 || obs.Usage.OutputTokens != 5 {
		t.Fatalf("probe observation usage = %+v, want the probe's terminal report", obs.Usage)
	}
	if obs.EstimatedPromptTokens != 0 {
		t.Fatalf("probe observation estimated prompt = %d, want 0 (a probe carries no transcript estimate)", obs.EstimatedPromptTokens)
	}
}

// TestProbeRecordsAFailedObservation pins that a probe whose stream errors is
// still recorded — as a non-OK probe observation with no usage — so the run's
// summary accounts for the request the gate sent, whether it succeeded or not.
func TestProbeRecordsAFailedObservation(t *testing.T) {
	sink := &recordingRequestSink{}
	bindings, err := BindRoles(map[string]string{"coordinator": "solo/model"})
	if err != nil {
		t.Fatal(err)
	}
	deps := ProbeDeps{
		Transport: &Gateway{},
		Bindings:  bindings,
		ModelProbe: func(context.Context, string) ([]Event, error) {
			return []Event{{Kind: KindError, Err: errors.New("provider: unavailable")}}, errors.New("provider: unavailable")
		},
		Requests: sink,
	}
	caps := probeModelBindings(context.Background(), deps)
	if len(caps) != 1 || caps[0].State != StateUnavailable {
		t.Fatalf("capabilities = %+v, want one unavailable", caps)
	}
	if len(sink.observations) != 1 {
		t.Fatalf("probe observations = %d, want 1", len(sink.observations))
	}
	obs := sink.observations[0]
	if obs.OK {
		t.Fatal("a failed probe is recorded OK; it must not be")
	}
	if obs.Usage != nil {
		t.Fatalf("a failed probe carries usage %v; this stub's terminal reports none", obs.Usage)
	}
}

// TestProbeRecordsTheRejectedUsageRequest pins the probe's total request
// accounting across the usage-field seam (#738): a probe whose transport's
// first request the provider rejected is recorded as its own non-OK probe
// request, before the probe's own, with an explicit usage absence — the
// rejected request the provider received is part of the run's model spend,
// never an unnamed one.
func TestProbeRecordsTheRejectedUsageRequest(t *testing.T) {
	sink := &recordingRequestSink{}
	bindings, err := BindRoles(map[string]string{"coordinator": "solo/model"})
	if err != nil {
		t.Fatal(err)
	}
	deps := ProbeDeps{
		Transport: &Gateway{},
		Bindings:  bindings,
		ModelProbe: func(context.Context, string) ([]Event, error) {
			return []Event{{Kind: KindFinal, Usage: &Usage{InputTokens: 50, OutputTokens: 5}, RejectedRequests: 1}}, nil
		},
		Requests: sink,
	}
	_ = probeModelBindings(context.Background(), deps)
	if len(sink.observations) != 2 {
		t.Fatalf("probe observations = %d, want 2 (the rejected request and the probe's own)", len(sink.observations))
	}
	rejected := sink.observations[0]
	if rejected.Class != RequestProbe {
		t.Fatalf("observation 0 class = %s, want probe", rejected.Class)
	}
	if rejected.OK {
		t.Fatal("observation 0 (the rejected request) is OK; it must not be")
	}
	if rejected.Usage != nil {
		t.Fatalf("observation 0 carried usage %v; a request rejected before any stream reports none", rejected.Usage)
	}
	own := sink.observations[1]
	if rejected.Role != own.Role || rejected.Model != own.Model {
		t.Fatalf("observation 0 attribution = %s/%s, want the probe's own %s/%s", rejected.Role, rejected.Model, own.Role, own.Model)
	}
	if own.Class != RequestProbe || !own.OK {
		t.Fatalf("observation 1 = %+v, want an OK probe", own)
	}
	if own.Usage == nil || own.Usage.InputTokens != 50 || own.Usage.OutputTokens != 5 {
		t.Fatalf("observation 1 usage = %+v, want the probe's terminal report", own.Usage)
	}
}

// TestCoordinatorSessionTalliesRequestThroughTelemetry pins the end-to-end
// wiring: a coordinator built with a telemetry produces a session that reports
// each request attempt through it, so the run's summary accounts for the work
// its sessions sent.
func TestCoordinatorSessionTalliesRequestThroughTelemetry(t *testing.T) {
	gateway, _, cleanup := gatewayFor(gatewayResponse{body: sse(contentChunk("ok"), finishChunk("stop"))})
	defer cleanup()
	tally := NewModelTelemetry(nil)
	coordinator, err := NewCoordinator(CoordinatorConfig{Transport: gateway, Bindings: testBindings(), Telemetry: tally})
	if err != nil {
		t.Fatal(err)
	}
	session, err := coordinator.newSession(RoleCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	session.Append(Message{Role: "user", Content: "go"})
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal = %+v, want final", last)
	}
	summary := tally.Summary()
	if summary.WorkRequests != 1 {
		t.Fatalf("tally work requests = %d, want 1 (the coordinator session's turn)", summary.WorkRequests)
	}
	if len(summary.ByRoleModel) != 1 || summary.ByRoleModel[0].Role != RoleCoordinator {
		t.Fatalf("tally by-role-model = %+v, want one cell for the coordinator", summary.ByRoleModel)
	}
}

// TestCoordinatorSessionChainsCompactionToPrimaryAndTally pins that a
// coordinator built with both a primary compaction sink and a telemetry
// delivers each applied compaction to both: the primary keeps exactly what it
// received before, and the tally gains the compaction account.
func TestCoordinatorSessionChainsCompactionToPrimaryAndTally(t *testing.T) {
	gateway, _, cleanup := gatewayFor()
	defer cleanup()
	primary := &recordingSink{}
	tally := NewModelTelemetry(nil)
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport:   gateway,
		Bindings:    testBindings(),
		Compactions: primary,
		Telemetry:   tally,
		ContextBudgets: map[string]ContextBudgetInput{
			RoleCoordinator: {Role: RoleCoordinator, DeclaredMaxInputTokens: 4000, ProbedMaxInputTokens: 4000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := coordinator.newSession(RoleCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	session.Pin(Message{Role: "system", Content: "goal"})
	appendTurns(session, 15) // crosses the 3000-token compact point of a 4000 budget
	event, compacted, err := session.Compact(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !compacted {
		t.Fatalf("the transcript crossed the compact point but no compaction ran: %+v", event)
	}
	if len(primary.events) != 1 {
		t.Fatalf("primary sink got %d events, want 1 (the run's original compaction telemetry)", len(primary.events))
	}
	summary := tally.Summary()
	if summary.Compactions != 1 || summary.CompactionsNetSaved != event.NetSaved {
		t.Fatalf("tally compaction account = %+v, want one compaction netting %d", summary, event.NetSaved)
	}
}

// TestChainedCompactionsDeliversToBoth pins that the coordinator's compaction
// chain delivers each applied-compaction event to both the run's primary
// sink and the model-telemetry tally, and that either half may be absent
// without affecting the other.
func TestChainedCompactionsDeliversToBoth(t *testing.T) {
	primary := &recordingSink{}
	tally := NewModelTelemetry(nil)
	chain := chainedCompactions{primary: primary, tally: tally}

	chain.SessionCompacted(CompactEvent{Role: "coordinator", Model: "test/coordinator", Before: 1000, After: 600, NetSaved: 400})

	if len(primary.events) != 1 || primary.events[0].NetSaved != 400 {
		t.Fatalf("primary sink got %+v, want one event with NetSaved 400", primary.events)
	}
	if summary := tally.Summary(); summary.Compactions != 1 || summary.CompactionsNetSaved != 400 {
		t.Fatalf("tally got %+v, want one compaction netting 400", summary)
	}

	// A fully-nil chain (no primary, no tally) is a no-op, and a chain with a
	// nil primary still reaches the tally.
	var nilChain chainedCompactions // both halves nil
	var recovered bool
	func() {
		defer func() {
			recovered = recover() != nil
		}()
		nilChain.SessionCompacted(CompactEvent{Before: 1, After: 0, NetSaved: 1})
	}()
	if recovered {
		t.Fatal("a fully-nil chain panicked; it must be a no-op")
	}

	tallyOnly := NewModelTelemetry(nil)
	noPrimary := chainedCompactions{tally: tallyOnly}
	noPrimary.SessionCompacted(CompactEvent{Before: 10, After: 5, NetSaved: 5})
	if summary := tallyOnly.Summary(); summary.Compactions != 1 || summary.CompactionsNetSaved != 5 {
		t.Fatalf("a nil-primary chain left the tally at %+v, want one compaction netting 5", summary)
	}
}

// TestModelTelemetryForwardIsolation pins that a panicking downstream sink
// cannot fail the tally's caller (#738): the tally records the observation,
// the forward fails where it stands, and delivery returns normally.
func TestModelTelemetryForwardIsolation(t *testing.T) {
	tally := NewModelTelemetry(panickingSink{})
	tally.ModelRequestObserved(RequestObservation{Class: RequestWork, Role: RoleCoordinator, Model: "m"})
	summary := tally.Summary()
	if summary.TotalRequests != 1 || summary.WorkRequests != 1 {
		t.Fatalf("summary = %+v, want the observation tallied despite the forward's failure", summary)
	}
}

// TestSessionCountsOneAttemptForAFallbackRejectedTurn pins the attempt-scoped
// accounting seam (#738): a turn whose first request is rejected for the
// usage field and whose retry without it succeeds is one attempt to the
// session — the transport-level fallback does not advance the attempt index.
// Both provider requests are nonetheless observed, the rejected one first,
// so the per-run request counts stay a total.
func TestSessionCountsOneAttemptForAFallbackRejectedTurn(t *testing.T) {
	shrinkRetryDelays(t)
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusBadRequest, errBody: `{"error":{"message":"unrecognized request argument supplied: stream_options","type":"invalid_request_error"}}`},
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))},
	)
	defer cleanup()
	sink := &recordingRequestSink{}
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil, WithRequestSink(sink))
	if err != nil {
		t.Fatal(err)
	}
	session.Append(Message{Role: "user", Content: "resolve the issue"})
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	// Two provider requests left the pod; the session observed both, under
	// the one attempt index the fallback retry does not advance.
	if got := mocker.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2 (the rejected ask and the retry)", got)
	}
	if len(sink.observations) != 2 {
		t.Fatalf("observations = %d, want 2 (one per provider request the turn sent)", len(sink.observations))
	}
	for i, obs := range sink.observations {
		if obs.Attempt != 0 {
			t.Fatalf("observation %d attempt = %d, want 0 (the fallback is the transport's, not a new attempt)", i, obs.Attempt)
		}
		if obs.Usage != nil {
			t.Fatalf("observation %d carried usage %v; the seam's requests report none", i, obs.Usage)
		}
	}
	if obs := sink.observations[0]; obs.OK {
		t.Fatalf("observation 0 (the rejected request) is OK; it must not be: %+v", obs)
	}
	if obs := sink.observations[1]; !obs.OK {
		t.Fatalf("observation 1 (the accepted retry) is not OK: %+v", obs)
	}
}

// TestSessionTallyCountsTheSeamRequestButNotAsARetry pins the tally
// semantics for a turn that hits the usage-field seam mid-retry (#738):
// every provider request the turn sent is tallied — the in-process retry's
// transport-rejected first request included — but the rejected request
// shares its attempt's index without becoming a retry: it is the
// transport's fallback, not an attempt of its own.
func TestSessionTallyCountsTheSeamRequestButNotAsARetry(t *testing.T) {
	shrinkRetryDelays(t)
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusServiceUnavailable},
		gatewayResponse{status: http.StatusBadRequest, errBody: `{"error":{"message":"unrecognized request argument supplied: stream_options","type":"invalid_request_error"}}`},
		gatewayResponse{body: sse(contentChunk("done"), finishChunk("stop"))},
	)
	defer cleanup()
	recording := &recordingRequestSink{}
	tally := NewModelTelemetry(recording)
	session, err := NewSession(gateway, testBindings(), RoleCoordinator, nil, WithRequestSink(tally))
	if err != nil {
		t.Fatal(err)
	}
	session.Append(Message{Role: "user", Content: "resolve the issue"})
	events := session.Turn(context.Background(), nil)
	if last := events[len(events)-1]; last.Kind != KindFinal {
		t.Fatalf("terminal event = %+v, want final", last)
	}
	if got := mocker.requestCount(); got != 3 {
		t.Fatalf("gateway requests = %d, want 3 (failed attempt, rejected usage request, accepted retry)", got)
	}
	if len(recording.observations) != 3 {
		t.Fatalf("observations = %d, want 3 (one per provider request the turn sent)", len(recording.observations))
	}
	first, rejected, accepted := recording.observations[0], recording.observations[1], recording.observations[2]
	if first.Attempt != 0 || first.OK {
		t.Fatalf("first observation = attempt %d ok %v, want the failed attempt 0", first.Attempt, first.OK)
	}
	if rejected.Attempt != 1 || rejected.OK || !rejected.Rejected || rejected.Usage != nil {
		t.Fatalf("second observation = %+v, want the transport-rejected request: attempt 1, non-OK, explicit absence", rejected)
	}
	if accepted.Attempt != 1 || !accepted.OK || accepted.Rejected {
		t.Fatalf("third observation = attempt %d ok %v rejected %v, want the accepted retry at attempt 1", accepted.Attempt, accepted.OK, accepted.Rejected)
	}
	summary := tally.Summary()
	if summary.TotalRequests != 3 || summary.WorkRequests != 3 {
		t.Fatalf("summary totals = %d/%d, want 3/3: every provider request is tallied", summary.TotalRequests, summary.WorkRequests)
	}
	if summary.Retries != 1 {
		t.Fatalf("summary retries = %d, want 1: only the accepted attempt is an in-process retry; the transport-rejected request is not", summary.Retries)
	}
	if len(summary.ByRoleModel) != 1 || summary.ByRoleModel[0].Retries != 1 {
		t.Fatalf("cell tallies = %+v, want one cell with one retry", summary.ByRoleModel)
	}
}

// TestProbeObservesTheSeamRequestOfARealGateway pins the probe-side
// accounting end to end (#738): a probe whose gateway rejects the usage
// request sends two provider requests, and both are observed — the
// rejected one as a non-OK probe request with an explicit usage absence,
// then the accepted retry with the usage its stream reported.
func TestProbeObservesTheSeamRequestOfARealGateway(t *testing.T) {
	gateway, mocker, cleanup := gatewayFor(
		gatewayResponse{status: http.StatusBadRequest, errBody: `{"error":{"message":"unrecognized request argument supplied: stream_options","type":"invalid_request_error"}}`},
		gatewayResponse{body: sse(contentChunk("pong"), finishChunk("stop"), usageChunk(50, 5, 0))},
	)
	defer cleanup()
	sink := &recordingRequestSink{}
	bindings, err := BindRoles(map[string]string{"coordinator": "solo/model"})
	if err != nil {
		t.Fatal(err)
	}
	deps := ProbeDeps{
		Transport: gateway,
		Bindings:  bindings,
		ModelProbe: func(probeCtx context.Context, model string) ([]Event, error) {
			return gateway.StreamChat(probeCtx, ChatRequest{Model: model, Messages: []Message{{Role: "user", Content: "ping"}}})
		},
		Requests: sink,
	}
	caps := probeModelBindings(context.Background(), deps)
	if len(caps) != 1 || caps[0].State != StateHealthy {
		t.Fatalf("capabilities = %+v, want one healthy binding (the seam must not turn the probe away)", caps)
	}
	if got := mocker.requestCount(); got != 2 {
		t.Fatalf("gateway requests = %d, want 2 (rejected attempt + accepted retry)", got)
	}
	if len(sink.observations) != 2 {
		t.Fatalf("probe observations = %d, want 2 (one per provider request the probe sent)", len(sink.observations))
	}
	rejected, accepted := sink.observations[0], sink.observations[1]
	if rejected.Class != RequestProbe || rejected.OK || !rejected.Rejected || rejected.Usage != nil {
		t.Fatalf("first probe observation = %+v, want the transport-rejected request: non-OK, explicit absence", rejected)
	}
	if accepted.Class != RequestProbe || !accepted.OK || accepted.Rejected {
		t.Fatalf("second probe observation = %+v, want the accepted retry", accepted)
	}
	if accepted.Usage == nil || accepted.Usage.InputTokens != 50 || accepted.Usage.OutputTokens != 5 {
		t.Fatalf("accepted retry usage = %+v, want the provider's report", accepted.Usage)
	}
}
