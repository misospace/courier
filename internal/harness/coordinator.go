package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/devenv"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/protocol"
)

// ForgeOps is the trusted seam for model-influenced forge reads. It routes
// through the run's broker, which enforces the resolved policy; it is never
// reachable from the worker or a brief. #125 wires the typed broker calls.
type ForgeOps interface {
	// Call executes one named forge operation. The operation name and
	// arguments are model data; the implementation validates them against
	// the run's policy before the broker acts.
	Call(ctx context.Context, operation string, arguments string) (string, error)
}

// Tool names are the model's view of the trusted tools. The toolset carries
// no publish and no merge tool: publication is not model-influenced.
const (
	toolShell       = "shell"
	toolCheck       = "check"
	toolDelegate    = "delegate"
	toolCancelBrief = "cancel_brief"
	toolForge       = "forge"
	// toolSkill is the model's name for the trusted skill-loading tool
	// (#659): the one deterministic path every role — coordinator and
	// delegated sub-agent alike — reads skill instructions through.
	// Trusted control renders the pinned skill's verified SKILL.md body as
	// tool data. The tool executes nothing and dispatches nothing; skill
	// content is untrusted data, and any script a skill ships runs only as
	// ordinary untrusted worker shell.
	toolSkill = "skill"
)

// Coordinator is the native harness: the trusted control pod's model client
// and delegation supervisor. It implements executor.Harness. Model text is
// data; the loop owns the terminal contract, and every model-controlled
// command executes on the untrusted worker, never in the control pod.
type Coordinator struct {
	gateway  ModelTransport
	bindings Bindings
	worker   *Delegator
	forge    ForgeOps

	snapshot      SnapshotProvider
	snapshotMu    sync.Mutex
	snapshotReady bool
	snapshotTip   string
	// snapshotGen is the coordinator-local witness that the shared worker
	// workspace was re-established (wiping all worker-side mutation) at the
	// moment of a materialization: every successful destructive
	// materialization advances it, so a check receipt recorded against one
	// generation describes a workspace that no longer exists once any later
	// materialization has run.
	snapshotGen uint64

	publisher Publisher
	activity  ActivitySink
	briefs    *BriefRegistry
	checks    *checkRegistry
	status    *StatusReporter
	control   *ControlWatcher
	// budgets is the context policy resolved per role at startup; a role
	// with no entry has no policy and never compacts.
	budgets map[string]ContextBudget
	// compactions receives applied-compaction telemetry.
	compactions CompactionSink
	// telemetry tallies the run's model requests and applied compactions
	// for the per-run model-telemetry summary (#738); nil disables the
	// tally and the run's behavior never changes.
	telemetry *ModelTelemetry
	// bootstrapReceipts receives one structured receipt per bounded
	// bootstrap execution.
	bootstrapReceipts BootstrapReceiptSink
	// workRefBeyondAnchor is the soft-stop world probe: the broker's live
	// observation of whether the pinned work ref holds a tip beyond the
	// admission anchor. The soft-stop finish derives the outcome from that
	// observation, never from a model declaration.
	workRefBeyondAnchor     func(context.Context) (bool, error)
	capacityWorkspaceSafety func(context.Context) (executor.WorkspaceSafetyEvidence, error)
	// skills is the run's loaded, verified skill view (#659); nil is a
	// skillless run.
	skills *SkillSet

	// deferredAvailability carries a sub-agent stream failure classified as
	// deferred model-provider availability (#320) from the delegation
	// boundary to the coordinator loop, which ends the run recoverably
	// before any further request can burn the exhausted binding.
	deferredAvailability   error
	deferredAvailabilityMu sync.Mutex

	// bootstrapBlockerErr carries a terminal environment blocker (#414):
	// the approved development-bootstrap contract failed, or its captured
	// environment was unusable. Recorded at the snapshot-preparation
	// boundary, it ends the run recoverably blocked-external at the next
	// turn boundary — never a model-visible retry loop — because the
	// failure is the environment's, not the work's.
	bootstrapBlockerErr error
	bootstrapBlockerMu  sync.Mutex

	// workerFenceErr, once set, refuses every further worker dispatch
	// (#414 review): an operation expired with unverified termination, so
	// the worker may still be running repository-authored work. The fence
	// is sticky for this incarnation — the honest recovery is worker
	// replacement, not another dispatch onto possibly-live work.
	workerFenceErr error
	workerFenceMu  sync.Mutex

	// prerequisite is the incarnation's circuit breaker for workspace
	// prerequisites (#737): once trusted control knows the workspace cannot
	// be prepared for this incarnation, the model never takes another turn
	// to rediscover the same condition.
	prerequisite prerequisiteLatch

	// bootstrapEnv holds the latest applied development-bootstrap
	// environment delta (#414): the filtered capture of the approved
	// contract's activated environment, merged over the worker's own
	// environment for every subsequent model-influenced task.
	bootstrapEnv   []string
	bootstrapEnvMu sync.Mutex
}

// recordDeferredAvailability keeps the first deferred provider-availability
// failure raised across a delegation boundary. Only the structured
// classification travels; the raw provider error never reaches the
// coordinator's transcript as tool data.
func (c *Coordinator) recordDeferredAvailability(err error) {
	c.deferredAvailabilityMu.Lock()
	defer c.deferredAvailabilityMu.Unlock()
	if c.deferredAvailability == nil {
		c.deferredAvailability = err
	}
}

// takeDeferredAvailability removes and returns a recorded deferred
// provider-availability failure, if any.
func (c *Coordinator) takeDeferredAvailability() error {
	c.deferredAvailabilityMu.Lock()
	defer c.deferredAvailabilityMu.Unlock()
	err := c.deferredAvailability
	c.deferredAvailability = nil
	return err
}

// recordBootstrapBlocker keeps the first bootstrap environment blocker
// (#414). Only the typed blocker — a fixed reason, never a raw process
// transcript — crosses this boundary.
func (c *Coordinator) recordBootstrapBlocker(err error) {
	c.bootstrapBlockerMu.Lock()
	defer c.bootstrapBlockerMu.Unlock()
	if c.bootstrapBlockerErr == nil {
		c.bootstrapBlockerErr = err
	}
}

// fenceWorker records the worker fence (#414 review): once an operation's
// termination could not be observed, no further dispatch may run against
// this worker. First cause wins; fencing never un-fences. The fence is a
// typed error so the prerequisite classifier can name it as the final fact
// it is, while its text stays exactly what it always was. Establishing the
// fence is itself the observation (#737): the fence is a final fact, so the
// breaker trips at the boundary that witnessed it — on every dispatch path,
// not one preparation later.
func (c *Coordinator) fenceWorker(opID string, cause error) {
	c.workerFenceMu.Lock()
	defer c.workerFenceMu.Unlock()
	if c.workerFenceErr == nil {
		c.workerFenceErr = &workerFence{reason: fmt.Sprintf("the worker is fenced: termination of operation %s could not be verified (%v); no further workspace operation may run against it", opID, cause)}
		c.prerequisite.observe(fencedPrerequisite())
	}
}

// observePrerequisite records one workspace-preparation failure against the
// incarnation's circuit breaker (#737). Only the failures trusted control
// classifies as stable count; a transient failure, an unclassified error, and
// the #414 bootstrap blocker change nothing.
func (c *Coordinator) observePrerequisite(err error) {
	c.prerequisite.observe(classifyPrerequisite(err))
}

// workerFenced returns the sticky worker fence error, if set.
func (c *Coordinator) workerFenced() error {
	c.workerFenceMu.Lock()
	defer c.workerFenceMu.Unlock()
	return c.workerFenceErr
}

// takeBootstrapBlocker removes and returns a recorded bootstrap environment
// blocker, if any.
func (c *Coordinator) takeBootstrapBlocker() error {
	c.bootstrapBlockerMu.Lock()
	defer c.bootstrapBlockerMu.Unlock()
	err := c.bootstrapBlockerErr
	c.bootstrapBlockerErr = nil
	return err
}

// hasBootstrapBlocker reports whether a bootstrap environment blocker is
// waiting, without consuming it.
func (c *Coordinator) hasBootstrapBlocker() bool {
	c.bootstrapBlockerMu.Lock()
	defer c.bootstrapBlockerMu.Unlock()
	return c.bootstrapBlockerErr != nil
}

// storeBootstrapEnv records the latest applied bootstrap environment delta
// (#414): it replaces any earlier delta, because each applied bootstrap is
// the environment's current truth.
func (c *Coordinator) storeBootstrapEnv(delta []string) {
	c.bootstrapEnvMu.Lock()
	defer c.bootstrapEnvMu.Unlock()
	c.bootstrapEnv = append([]string(nil), delta...)
}

// currentBootstrapEnv returns a copy of the applied bootstrap environment
// delta, or nil when none was applied: tasks without a delta run under the
// worker's own environment.
func (c *Coordinator) currentBootstrapEnv() []string {
	c.bootstrapEnvMu.Lock()
	defer c.bootstrapEnvMu.Unlock()
	if len(c.bootstrapEnv) == 0 {
		return nil
	}
	return append([]string(nil), c.bootstrapEnv...)
}

// CoordinatorConfig assembles the harness. Worker, snapshot, forge, and
// publisher may be absent; each absence is surfaced as data to the model or
// named by capability health, never a silent fallback. Status, when present,
// persists earned heartbeats and dispatch evidence through the run's broker.
type CoordinatorConfig struct {
	// Transport is the model boundary the lane declared: OpenAI-compatible
	// or Anthropic Messages (#716). Construction fails closed on an unknown
	// declared transport or a missing endpoint.
	Transport ModelTransport
	Bindings  Bindings
	Worker    *Delegator
	Snapshot  SnapshotProvider
	Forge     ForgeOps
	Publisher Publisher
	Activity  ActivitySink
	Status    *StatusReporter
	// Control optionally serves the operator soft-stop path (#238): the
	// broker-sourced control request, its durable acknowledgement through
	// the status reporter, the stop latch, and the injected control turn.
	// Absent, the run has no soft-stop path.
	Control *ControlWatcher
	// WorkRefBeyondAnchor optionally reports the broker's live observation
	// of whether the pinned work ref holds a tip beyond the admission
	// anchor. The soft-stop finish derives the run's outcome from it.
	WorkRefBeyondAnchor     func(context.Context) (bool, error)
	CapacityWorkspaceSafety func(context.Context) (executor.WorkspaceSafetyEvidence, error)
	// ContextBudgets is the per-role context policy input (#243), keyed by
	// role. It is resolved once here — the session loop never derives a
	// budget — and a role named with an input that does not resolve fails
	// startup closed rather than running on an invented context window. A
	// role absent from the map gets the zero budget: the harness owns no
	// policy for it and never compacts. #636 populates this map from the
	// lane's declared policy and the gateway-verified model input limit the
	// capability gate verified; a lane that declares no policy populates
	// nothing, which is the documented unbudgeted legacy behavior.
	ContextBudgets map[string]ContextBudgetInput
	// Compactions optionally receives one telemetry event per applied
	// step-boundary compaction (#243). Emission is best-effort and
	// observability only: it never changes a transcript or a run outcome.
	Compactions CompactionSink
	// Telemetry optionally tallies the run's model requests and applied
	// compactions for the per-run model-telemetry summary (#738). Every
	// session the coordinator opens reports each request attempt through
	// it, and its compaction tally chains the run's primary compaction sink
	// alongside: the primary keeps exactly what it received before, and the
	// tally gains the compaction account. Nil disables the tally; the run's
	// behavior never changes.
	Telemetry *ModelTelemetry
	// BootstrapReceipts optionally receives one structured receipt per
	// bounded bootstrap execution (#414): statuses, per-task elapsed
	// times, exit codes, the environment-key count, and the validated
	// mise diagnostics — no secret values, no raw worker output. Emission
	// is best-effort and observability only, like the compaction sink.
	BootstrapReceipts BootstrapReceiptSink
	// Skills is the run's loaded, verified skill view (#659): the bounded
	// index pinned into every role's system message and the one trusted
	// path — the skill tool — full instructions are served through. Nil is
	// a skillless run: no section, no tool, no behavior change.
	Skills *SkillSet
}

// NewCoordinator fails closed on an unusable model configuration: without a
// gateway or a coordinator binding there is no harness.
func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if config.Transport == nil {
		return nil, errors.New("harness: model gateway is not configured")
	}
	if config.Bindings.Model(RoleCoordinator) == "" {
		return nil, errors.New("harness: no model is bound to the coordinator role")
	}
	// The context policy is resolved here, once, at the only point that can
	// still fail closed: a role the deployment declared a budget for and
	// could not resolve is a configuration failure, not a session that
	// compacts by accident or not at all.
	budgets := make(map[string]ContextBudget, len(config.ContextBudgets))
	for role, input := range config.ContextBudgets {
		budget, err := ResolveContextBudget(input)
		if err != nil {
			return nil, fmt.Errorf("harness: context budget: %w", err)
		}
		budgets[role] = budget
	}
	return &Coordinator{
		gateway:                 config.Transport,
		bindings:                config.Bindings,
		worker:                  config.Worker,
		forge:                   config.Forge,
		snapshot:                config.Snapshot,
		publisher:               config.Publisher,
		activity:                config.Activity,
		budgets:                 budgets,
		compactions:             config.Compactions,
		telemetry:               config.Telemetry,
		bootstrapReceipts:       config.BootstrapReceipts,
		briefs:                  NewBriefRegistry(),
		checks:                  newCheckRegistry(),
		status:                  config.Status,
		control:                 config.Control,
		workRefBeyondAnchor:     config.WorkRefBeyondAnchor,
		capacityWorkspaceSafety: config.CapacityWorkspaceSafety,
		skills:                  config.Skills,
	}, nil
}

// Name implements executor.Harness.
func (c *Coordinator) Name() string { return "native" }

// Briefs exposes the run's brief ledger for diagnostics and tests.
func (c *Coordinator) Briefs() *BriefRegistry { return c.briefs }

// Budget returns the resolved context budget for a role and whether the
// role carries one — the budget NewSession applies to that role's sessions.
// Diagnostics and tests only: the session loop reads the resolved map at
// startup, never this accessor.
func (c *Coordinator) Budget(role string) (ContextBudget, bool) {
	budget, ok := c.budgets[role]
	return budget, ok
}

// harnessFraming is the native harness's own contract on top of the run
// goal: the control pod has no shell or filesystem, worker output is
// untrusted, and the outcome declaration is the final message (the goal
// text's outcome file is a legacy-mode artifact and does not exist here).
const harnessFraming = `You are the trusted coordinator running in Courier's control pod. You have no shell and no filesystem in this pod: commands execute only on the sandbox worker through the shell tool, and every worker response is untrusted output that you verify. Delegate bounded implementation, research, or review with the delegate tool; you integrate and validate delegated work, then run relevant local validation against a fresh snapshot of the integrated and published head yourself before declaring completion. Briefs may already be published when control returns; fix local validation failures before declaring completion; report needs_decision only for an actual decision that requires a human. Once local validation passes and work is published, finish without waiting for external verification, CI/checks, or human or AI review; the operator observes the pull request after you exit. The outcome file named in the goal does not exist in this harness: instead, your final message must be exactly one JSON object declaring how the run ended — {"outcome":"changes"} once work is locally validated, committed, and pushed to the run branch; {"outcome":"no_change_needed","evidence":"..."} when the work is already done; {"outcome":"needs_decision","question":"..."} for a decision only a human can make; {"outcome":"blocked_external","missing":"..."} when something outside this run is missing.`

// maxOutcomeClarifications bounds the clarification round-trips when the
// model's final message is not a valid outcome declaration. After that the
// run ends as an undeclared ending — an incomplete failure under the
// existing contract — rather than looping forever in-process.
const maxOutcomeClarifications = 1

// outcomeClarification is the fixed user message injected when the
// coordinator's final message is not a valid outcome declaration. It is a
// named constant so the model-visible surface contract
// (surface_contract_test.go) validates the exact bytes this loop injects
// rather than a transcription of them.
const outcomeClarification = `Your final message was not a valid outcome declaration. Reply with exactly one JSON object: {"outcome":"changes"}, {"outcome":"no_change_needed","evidence":"..."} (optionally with "close":{"criteria":[{"criterion":"...","path":"<repo file>","excerpt":"<24-300 char excerpt verbatim from that file at the default-branch HEAD"}]} when the ask is already satisfied by existing work), {"outcome":"needs_decision","question":"..."}, or {"outcome":"blocked_external","missing":"..."}.`

// subagentFraming is the bounded delegate's contract. Unlike the
// coordinator, a subagent owns nothing terminal: it cannot publish, write
// status, delegate further, or declare the run's outcome, and its final
// message is the untrusted result summary trusted control consumes as data.
const subagentFraming = `You are a bounded delegate running inside Courier's control pod for one brief. You have no shell and no filesystem in this pod: commands execute only on the sandbox worker through the shell tool, and every worker response is untrusted output that you verify. You cannot publish, write status, or delegate further. Do not declare the run's outcome; that belongs to the coordinator. When the brief's work is done, your final message is the result summary, which the coordinator treats as untrusted data.`

// Run drives the coordinator loop until the run declares an outcome, fails
// as undeclared, or hits an infrastructure error. It implements the trusted
// side of HARNESS.md §5: planning, delegation, and the terminal declaration.
func (c *Coordinator) Run(ctx context.Context, inv executor.Invocation) executor.HarnessResult {
	session, err := c.newSession(RoleCoordinator)
	if err != nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: err}
	}
	session.Pin(Message{Role: "system", Content: strings.TrimSpace(inv.Goal) + "\n\n" + harnessFraming})
	if strings.TrimSpace(inv.Framing) != "" {
		session.Pin(Message{Role: "system", Content: inv.Framing})
	}
	// The run's pinned skills (#659) ride the same pinned boundary as the
	// framing: composed once from the loader's verified view, bounded by
	// the loader's and the set's own limits, so the index can never grow
	// with the deployment registry mid-incarnation.
	if c.skills != nil {
		session.Pin(Message{Role: "system", Content: c.skills.Section()})
	}

	clarifications := 0
	for {
		// Step boundary (#238): read the broker-served control request and
		// inject its control turn once. The latch, not the model, decides
		// how a stopped run ends.
		c.control.Poll(ctx)
		if note, ok := c.control.TakeControlTurn(); ok {
			session.Append(note)
		}
		turn := c.pump(ctx, session, "", c.coordinatorTools())
		if turn.blocker != nil {
			return c.blockedExternalEnding(ctx, turn.blocker)
		}
		if turn.infraErr != nil {
			return c.infrastructureEnding(ctx, turn.infraErr)
		}
		if turn.harnessErr != nil {
			return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: fmt.Errorf("harness: step-boundary compaction: %w", turn.harnessErr)}
		}
		if turn.streamErr != nil {
			if result, ok := availabilityEnding(turn.streamErr); ok {
				if result.Outcome == executor.OutcomeCapacityExhausted {
					evidence := executor.WorkspaceSafetyEvidence{State: "unknown"}
					if c.capacityWorkspaceSafety != nil {
						if observed, observeErr := c.capacityWorkspaceSafety(ctx); observeErr == nil {
							evidence = observed
						}
					}
					result.WorkspaceSafety = &evidence
				}
				return result
			}
			return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: fmt.Errorf("harness: model stream failed: %w", turn.streamErr)}
		}
		if turn.final {
			// The model never decides whether a workspace prerequisite is
			// terminal (#737): a tripped latch ends the run here, whatever
			// the model said and whatever the operator's soft stop asked
			// for. The model sees the same condition as a tool error it can
			// rediscover; the latch is the trusted fact the model cannot
			// argue with, and it outranks the model's own declaration. This
			// is the defensive re-check: the tool boundary below normally
			// consumes the trip first, so a final turn over a tripped latch
			// means a trip arrived without one.
			if failure := c.prerequisite.peek(); failure != nil {
				if failure.kind == courierv1alpha1.NativeResultInfrastructureFailed {
					return c.infrastructureEnding(ctx, failure)
				}
				return c.blockedExternalEnding(ctx, failure)
			}
			if c.control.Stopped() {
				// The run loop exits only with the latch set and every
				// brief terminal: in-flight work always reaches its
				// integration before the soft-stop finish runs.
				if c.briefs.HasInFlight() {
					continue
				}
				return c.finishSoftStop(ctx)
			}
			outcome, noChange, parseErr := parseOutcome(turn.text)
			if parseErr == nil {
				return c.finish(ctx, inv, outcome, turn.text, noChange)
			}
			if clarifications < maxOutcomeClarifications {
				clarifications++
				session.Append(Message{Role: "user", Content: outcomeClarification})
				continue
			}
			return executor.HarnessResult{
				Kind:   courierv1alpha1.NativeResultWorkloadFailed,
				Reason: fmt.Sprintf("undeclared ending: the coordinator's final message was not a valid outcome declaration: %v", parseErr),
			}
		}
	}
}

// availabilityEnding maps a deferred model-provider availability failure — a
// confirmed allowance exhaustion, or a transient rate limit the in-process
// policy does not retry (an unhinted 429, or one whose reset hint exceeds this
// incarnation's bound) — to a recoverable blocked-external ending. It is not a
// human decision, a code failure, or a claim of no progress: the source
// re-offers the work when capacity returns (#320).
func availabilityEnding(err error) (executor.HarnessResult, bool) {
	classification, ok := providerClassificationOf(err)
	if !ok || !classification.Deferred() {
		return executor.HarnessResult{}, false
	}
	outcome := executor.OutcomeBlockedExternal
	kind := courierv1alpha1.NativeResultBlockedExternal
	if classification.Availability == ProviderAllowanceExhausted {
		outcome = executor.OutcomeCapacityExhausted
		kind = courierv1alpha1.NativeResultCapacityExhausted
	}
	result := executor.HarnessResult{Outcome: outcome, Kind: kind, Reason: providerAvailabilityReason(classification)}
	return result, true
}

// providerAvailabilityReason renders a bounded, redacted explanation of a
// deferred provider-availability failure. It names only the fixed class and,
// when recognized, the short allowlisted provider code.
func providerAvailabilityReason(classification ProviderClassification) string {
	reason := "model provider capacity is temporarily unavailable (" + string(classification.Availability) + ")"
	if classification.Code != "" {
		reason += ", provider code " + classification.Code
	}
	return reason + "; the run is recoverable and will resume when capacity returns"
}

// turnResult is one processed model turn.
type turnResult struct {
	// text is the turn's accumulated content when it terminalized without
	// tool requests.
	text string
	// final reports a terminalized turn with no tool requests.
	final bool
	// toolResults holds the KindToolResult events emitted this turn.
	toolResults []Event
	// streamErr reports a failed model stream.
	streamErr error
	// harnessErr reports a trusted-control failure that is not the model's:
	// today a step-boundary compaction whose protected state cannot fit
	// (#243). The transcript is unchanged, so the run ends as an
	// infrastructure failure rather than continuing on truncated state.
	harnessErr error
	// blocker carries a terminal environment blocker recorded at the
	// snapshot-preparation boundary (#414): the run ends recoverably
	// blocked-external at this turn boundary, before any further model
	// request.
	blocker error
	// infraErr carries a workspace-prerequisite failure whose class means
	// the run cannot continue at all (#737): the worker is fenced, so no
	// further workspace operation may run against it. The run ends as an
	// infrastructure failure for the operator to relaunch — the honest
	// recovery is worker replacement, not another model turn.
	infraErr error
}

// newSession opens one role-bound session carrying that role's resolved
// context policy, the compaction telemetry sink, and the run's model
// telemetry (#738). A role with no resolved budget gets a session that owns
// no policy and never compacts. When the run carries a model telemetry, the
// session's compaction sink chains the run's primary sink beside the tally:
// the primary keeps exactly what it received before, and the tally gains the
// compaction account; without one the primary stands alone.
func (c *Coordinator) newSession(role string) (*Session, error) {
	var compactions CompactionSink
	if c.telemetry != nil {
		compactions = chainedCompactions{primary: c.compactions, tally: c.telemetry}
	} else {
		compactions = c.compactions
	}
	options := []SessionOption{
		WithContextBudget(c.budgets[role]),
		WithCompactionSink(compactions),
	}
	if c.telemetry != nil {
		options = append(options, WithRequestSink(c.telemetry))
	}
	return NewSession(c.gateway, c.bindings, role, c.activity, options...)
}

// pump drives one model turn through the normalized event contract: the
// provider stream is consumed as events, requested tools execute through
// their trusted implementations, and every verified tool completion is
// emitted as a KindToolResult event into the transcript.
//
// The step-boundary compaction decision (#243) runs first, between turns and
// never mid-turn: a compaction decided after a request is sent would be
// reacting to a context the provider has already accepted. It receives the
// same tool declarations as the turn it guards, because they ride every
// request and consume the input limit the boundary is enforcing.
func (c *Coordinator) pump(ctx context.Context, session *Session, briefID string, tools []ToolDef) turnResult {
	if _, _, err := session.Compact(tools); err != nil {
		return turnResult{harnessErr: err}
	}
	events := session.Turn(ctx, tools)
	var text strings.Builder
	var calls []ToolCall
	// blocks is the assistant turn's provider-native content blocks from the
	// turn's terminal event — opaque continuation data (#716) that rides the
	// transcript verbatim so the next request keeps provider semantics. It is
	// nil for transports with no continuation payload.
	var blocks []json.RawMessage
	final := false
	for _, event := range events {
		switch event.Kind {
		case KindDelta:
			text.WriteString(event.Text)
		case KindToolRequest:
			calls = append(calls, event.Tool)
		case KindFinal:
			final = true
		case KindError:
			return turnResult{streamErr: event.Err}
		}
		if len(event.Blocks) > 0 {
			blocks = event.Blocks
		}
	}
	session.Append(AssistantMessageWithBlocks(text.String(), calls, blocks))
	if final {
		return turnResult{text: text.String(), final: true}
	}
	if len(calls) == 0 {
		// Turn guarantees a terminal event; a final-less turn with no tool
		// requests never terminalized.
		return turnResult{streamErr: ErrStreamTruncated}
	}
	var toolResults []Event
	for _, call := range calls {
		result := c.runTool(ctx, call, briefID)
		// The verified tool boundary is a normalized tool-result event:
		// trusted control produced it after verified termination, and the
		// transcript consumes it as data.
		event := Event{Kind: KindToolResult, Result: result}
		session.Append(ToolResultMessage(event.Result))
		toolResults = append(toolResults, event)
		// A delegated session that died on deferred provider availability
		// (#320) ends the run now, before this session's next request: the
		// coordinator's own binding may be healthy, and letting it take
		// another turn would let it re-delegate onto the exhausted binding
		// or declare a false ending. The run ends through the same
		// availability ending as the coordinator's own stream failure.
		// The hand-off is scoped to the coordinator's own loop: a
		// sub-agent pump never consumes it, so a recorded failure can
		// only abort the session that owns the run. The transcript may
		// hold fewer tool results than the assistant message requested,
		// which is safe because the ending discards the session; the
		// earned events are still returned, not dropped.
		if briefID == "" {
			if deferred := c.takeDeferredAvailability(); deferred != nil {
				return turnResult{streamErr: deferred, toolResults: toolResults}
			}
			// A recorded bootstrap environment blocker (#414) ends the run
			// the same way deferred availability does: the ending is
			// scoped to the coordinator's own loop — a sub-agent pump
			// never consumes it, so it can only abort the session that
			// owns the run. The ending carries only the fixed blocker
			// reason; the turn's earned tool results are already in the
			// transcript, and the ending discards the session with it.
			if blocker := c.takeBootstrapBlocker(); blocker != nil {
				return turnResult{blocker: blocker, toolResults: toolResults}
			}
			// A workspace-prerequisite failure that trusted control has
			// classified as stable ends the run here (#737): the model gets
			// no second turn to rediscover a condition the workspace itself
			// cannot satisfy. The ending is scoped to the coordinator's own
			// loop — a sub-agent pump never consumes it, so the trip can
			// only abort the session that owns the run.
			if failure := c.prerequisite.take(); failure != nil {
				if failure.kind == courierv1alpha1.NativeResultInfrastructureFailed {
					return turnResult{infraErr: failure, toolResults: toolResults}
				}
				return turnResult{blocker: failure, toolResults: toolResults}
			}
		} else if c.prerequisite.peek() != nil {
			// The delegated session's own tool boundary enforces the latch
			// too (#737): once the trip is observed, no further call in this
			// turn runs, so nothing the latch has already ended the brief's
			// right to ask for reaches the worker. peek, never take — only
			// the coordinator's own loop consumes the trip and ends the run.
			// The boundary is a post-check: the turn's first call always
			// executes, and the brief's entry guard one level up owns the
			// already-tripped case. The transcript may hold fewer tool
			// results than the assistant message requested, which is safe
			// because the aborting brief discards this session; the earned
			// events are still returned, not dropped.
			return turnResult{toolResults: toolResults}
		}
	}
	return turnResult{toolResults: toolResults}
}

// settleStatus closes the run's durable status obligations before an ending
// is reported. It flushes retained operation clears opportunistically — a
// failure there never changes the ending, because the operator supersedes
// this incarnation's entries regardless — and then reconciles the debt a
// completed brief leaves behind when its durable record never landed. A
// completed unit whose durable status record never landed can never be
// turned into a successful ending by a model declaration. The broker
// revalidates the recorded OID against the live work ref on the retry, so the
// recovery half re-checks the world; a persistent failure is an infrastructure
// failure for the operator to relaunch, which resumes remote-first (§7).
func (c *Coordinator) settleStatus(ctx context.Context) error {
	_ = c.status.Flush(ctx)
	if err := c.status.ReconcileDebt(ctx); err != nil {
		return fmt.Errorf("harness: a completed brief lacks a durable status record: %w", err)
	}
	return nil
}

// blockedExternalEnding reports a terminal environment blocker: the run ends
// recoverably blocked-external with the blocker's fixed reason. The run's
// durable status obligations are settled first, so a brief whose durable
// record never landed is an infrastructure failure rather than a clean
// blocked-external ending.
func (c *Coordinator) blockedExternalEnding(ctx context.Context, blocker error) executor.HarnessResult {
	if err := c.settleStatus(ctx); err != nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: err}
	}
	return executor.HarnessResult{
		Kind:    courierv1alpha1.NativeResultBlockedExternal,
		Outcome: executor.OutcomeBlockedExternal,
		Reason:  blocker.Error(),
	}
}

// infrastructureEnding reports a terminal infrastructure failure: the
// condition cannot be continued past (a fenced worker), so the honest
// recovery is operator relaunch. The run's durable status obligations settle
// first, exactly as a blocked-external ending settles them: a completed
// brief whose durable record never landed is itself an infrastructure
// failure, and the relaunch reconciles it remote-first.
func (c *Coordinator) infrastructureEnding(ctx context.Context, cause error) executor.HarnessResult {
	if err := c.settleStatus(ctx); err != nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: err}
	}
	return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: cause}
}

// finish applies the declared outcome. Publication is the coordinator's own
// trusted act, never the model's: the declared "changes" outcome reaches the
// publisher only here, and a publish failure classifies the run — a blocked
// world is NeedsHuman, transport failure is an infrastructure failure for
// the operator to relaunch, and a declared-changes-with-no-work ending is a
// workload failure.
func (c *Coordinator) finish(ctx context.Context, inv executor.Invocation, outcome executor.DeclaredOutcome, summary string, noChange *courierv1alpha1.NoChangeClose) executor.HarnessResult {
	// The run is ending: the durable status obligations are settled here, in
	// trusted control, before any declared outcome is honored (§7).
	if err := c.settleStatus(ctx); err != nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: err}
	}
	switch outcome {
	case executor.OutcomeChanges:
		if c.publisher == nil {
			return executor.HarnessResult{
				Kind:   courierv1alpha1.NativeResultInfrastructureFailed,
				Reason: "the coordinator declared changes but this build has no publication path",
				Err:    errors.New("harness: publication is not configured"),
			}
		}
		publication, err := c.publisher.Publish(ctx)
		if err != nil {
			if errors.Is(err, ErrNothingToPublish) {
				return executor.HarnessResult{
					Kind:   courierv1alpha1.NativeResultWorkloadFailed,
					Reason: "the coordinator declared changes but no work was integrated or published in this incarnation: " + err.Error(),
				}
			}
			var blocked *PublicationBlockedError
			if errors.As(err, &blocked) {
				return executor.HarnessResult{
					Kind:    courierv1alpha1.NativeResultBlockedExternal,
					Outcome: executor.OutcomeBlockedExternal,
					Reason:  blocked.Error(),
				}
			}
			return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Outcome: outcome, Err: fmt.Errorf("harness: publication failed: %w", err)}
		}
		reason := "published through the broker"
		if publication.AlreadyPublished {
			reason = "publication already confirmed at " + publication.OID
		}
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultPublished, Outcome: outcome, Reason: reason}
	case executor.OutcomeNoChangeNeeded, executor.OutcomeNeedsDecision, executor.OutcomeBlockedExternal:
		kind := courierv1alpha1.NativeResultNoChange
		if outcome == executor.OutcomeNeedsDecision {
			kind = courierv1alpha1.NativeResultNeedsHuman
		}
		if outcome == executor.OutcomeBlockedExternal {
			kind = courierv1alpha1.NativeResultBlockedExternal
		}
		// A grounded close proposal (#720) parses only for no_change_needed;
		// it rides the result for trusted control to gate on mode and persist
		// fenced. The free-form summary stays the handoff text; the proposal
		// carries the declared evidence explicitly for the durable record.
		return executor.HarnessResult{Kind: kind, Outcome: outcome, Reason: summary, NoChange: noChange}
	default:
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultWorkloadFailed, Reason: fmt.Sprintf("undeclared outcome %q", outcome)}
	}
}

// finishSoftStop closes a soft-stopped run (#238). The latch, not the model,
// owns this ending: it reconciles status debt and flushes active operations
// like finish, then derives the outcome from the world — changes iff the
// broker's live observation shows the work ref holding a tip beyond the
// admission anchor, confirmed through the publisher (whose re-confirm is
// idempotent); otherwise no_change_needed. A broker publication block
// promotes the outcome to blocked_external. A SIGTERM that cancelled the
// context takes precedence and is the caller's stream-failure path, never a
// soft-stop outcome.
func (c *Coordinator) finishSoftStop(ctx context.Context) executor.HarnessResult {
	if err := ctx.Err(); err != nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: fmt.Errorf("harness: model stream failed: %w", err)}
	}
	_ = c.status.Flush(ctx)
	if err := c.status.ReconcileDebt(ctx); err != nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: fmt.Errorf("harness: a completed brief lacks a durable status record: %w", err)}
	}
	if c.workRefBeyondAnchor == nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: errors.New("harness: soft stop has no work-ref observation configured")}
	}
	beyond, err := c.workRefBeyondAnchor(ctx)
	if err != nil {
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: fmt.Errorf("harness: soft stop could not observe the work ref: %w", err)}
	}
	if !beyond {
		return executor.HarnessResult{
			Kind:    courierv1alpha1.NativeResultNoChange,
			Outcome: executor.OutcomeNoChangeNeeded,
			Reason:  "soft stop: the work ref holds no tip beyond the admission anchor",
		}
	}
	if c.publisher == nil {
		return executor.HarnessResult{
			Kind:   courierv1alpha1.NativeResultInfrastructureFailed,
			Reason: "soft stop found work beyond the admission anchor but this build has no publication path",
			Err:    errors.New("harness: publication is not configured"),
		}
	}
	publication, err := c.publisher.Publish(ctx)
	if err != nil {
		var blocked *PublicationBlockedError
		if errors.As(err, &blocked) {
			return executor.HarnessResult{
				Kind:    courierv1alpha1.NativeResultBlockedExternal,
				Outcome: executor.OutcomeBlockedExternal,
				Reason:  blocked.Error(),
			}
		}
		return executor.HarnessResult{Kind: courierv1alpha1.NativeResultInfrastructureFailed, Err: fmt.Errorf("harness: soft stop publication failed: %w", err)}
	}
	reason := "soft stop: published through the broker"
	if publication.AlreadyPublished {
		reason = "soft stop: publication already confirmed at " + publication.OID
	}
	return executor.HarnessResult{Kind: courierv1alpha1.NativeResultPublished, Outcome: executor.OutcomeChanges, Reason: reason}
}

// parseOutcome parses the model's final message as the outcome declaration.
// The declaration is data: an unparseable message is never an outcome. A
// no_change_needed declaration may carry an optional grounded close proposal
// (#720); a malformed or over-bound proposal is dropped — the plain
// no_change_needed ending stands, because losing a valid ending to a bad
// evidence row would turn a conclusion into a clarification loop or a
// workload failure, while the broker re-validates anything that survives.
func parseOutcome(text string) (executor.DeclaredOutcome, *courierv1alpha1.NoChangeClose, error) {
	var declaration struct {
		Outcome  string           `json:"outcome"`
		Evidence string           `json:"evidence"`
		Question string           `json:"question"`
		Missing  string           `json:"missing"`
		Close    *json.RawMessage `json:"close"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &declaration); err != nil {
		return "", nil, fmt.Errorf("final message is not a JSON object: %w", err)
	}
	outcome := executor.DeclaredOutcome(strings.TrimSpace(declaration.Outcome))
	switch outcome {
	case executor.OutcomeChanges:
		if declaration.Evidence != "" || declaration.Question != "" || declaration.Missing != "" || declaration.Close != nil {
			return "", nil, errors.New("changes declaration carries no extra fields")
		}
		return outcome, nil, nil
	case executor.OutcomeNoChangeNeeded:
		if strings.TrimSpace(declaration.Evidence) == "" {
			return "", nil, errors.New("no_change_needed requires evidence")
		}
		if declaration.Close == nil {
			return outcome, nil, nil
		}
		return outcome, parseNoChangeClose(*declaration.Close, declaration.Evidence), nil
	case executor.OutcomeNeedsDecision:
		if strings.TrimSpace(declaration.Question) == "" {
			return "", nil, errors.New("needs_decision requires a question")
		}
		return outcome, nil, nil
	case executor.OutcomeBlockedExternal:
		if strings.TrimSpace(declaration.Missing) == "" {
			return "", nil, errors.New("blocked_external requires the missing dependency")
		}
		return outcome, nil, nil
	default:
		return "", nil, fmt.Errorf("unknown outcome %q", declaration.Outcome)
	}
}

// parseNoChangeClose validates one declared close proposal against the API
// bounds (#720) and returns the bounded typed value, or nil when the shape
// is malformed or over bound — the caller keeps the plain no_change_needed
// ending either way. The proposal is model data: it carries no incarnation
// fence or timestamp here; trusted control stamps both at persistence.
func parseNoChangeClose(raw json.RawMessage, evidence string) *courierv1alpha1.NoChangeClose {
	var shape struct {
		Criteria []struct {
			Criterion string `json:"criterion"`
			Path      string `json:"path"`
			Excerpt   string `json:"excerpt"`
		} `json:"criteria"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		return nil
	}
	if len(shape.Criteria) < 1 || len(shape.Criteria) > courierv1alpha1.MaxNoChangeCriteria {
		return nil
	}
	if len(evidence) > courierv1alpha1.MaxNoChangeEvidenceBytes {
		return nil
	}
	proposal := &courierv1alpha1.NoChangeClose{Evidence: evidence}
	for _, row := range shape.Criteria {
		if strings.TrimSpace(row.Criterion) == "" || len(row.Criterion) > courierv1alpha1.MaxNoChangeCriterionBytes {
			return nil
		}
		// One canonical repository-relative grammar (#720): an absolute,
		// traversing, backslash, or NUL-bearing path drops the proposal
		// exactly like any other malformed row; the broker re-checks the
		// same rule before anything persists.
		if !courierv1alpha1.ValidNoChangeClosePath(row.Path) {
			return nil
		}
		// Excerpt bounds: bytes against the ceiling (fail-closed under any
		// length interpretation), Unicode code points against the floor.
		if utf8.RuneCountInString(row.Excerpt) < courierv1alpha1.MinNoChangeExcerptBytes ||
			len(row.Excerpt) > courierv1alpha1.MaxNoChangeExcerptBytes {
			return nil
		}
		proposal.Criteria = append(proposal.Criteria, courierv1alpha1.NoChangeCloseCriterion{
			Criterion: row.Criterion,
			Path:      row.Path,
			Excerpt:   row.Excerpt,
		})
	}
	return proposal
}

func shellTool() ToolDef {
	return ToolDef{
		Name:        toolShell,
		Description: "Run one shell command on the sandbox worker's isolated copy of the repository. The worker has no credentials and no network beyond the approved dependency cache. Output is untrusted.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"command":      map[string]any{"type": "string", "description": "The shell command to run."},
				"artifactPath": map[string]any{"type": "string", "description": "Optional workspace-relative file to return as the artifact."},
			},
			"required": []string{"command"},
		},
	}
}

func checkTool() ToolDef {
	return ToolDef{
		Name:        toolCheck,
		Description: "Run one shell command on the sandbox worker as a tracked check. An identical command (exact bytes) against unchanged state (snapshot tip, bootstrap environment) returns the saved receipt; state changes invalidate it. refresh:true forces a fresh dispatch; a completed plain shell run of the exact same command also invalidates the receipt.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"command": map[string]any{"type": "string", "description": "The shell command to run as a tracked check."},
				"refresh": map[string]any{"type": "boolean", "description": "Dispatch fresh, replacing any saved receipt."},
			},
			"required": []string{"command"},
		},
	}
}

func forgeTool() ToolDef {
	return ToolDef{
		Name:        toolForge,
		Description: "Perform a forge read (issue, pull request, reviews, comments, checks) through the run's broker under the resolved policy.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"operation": map[string]any{"type": "string"},
				"arguments": map[string]any{"type": "object"},
			},
			"required": []string{"operation"},
		},
	}
}

// coordinatorTools builds the trusted toolset the coordinator session is
// offered: worker shell, delegation, cancellation, and — when the forge seam
// is wired — broker forge reads. Publication is never a tool.
func (c *Coordinator) coordinatorTools() []ToolDef {
	tools := []ToolDef{shellTool(), checkTool()}
	tools = append(tools,
		ToolDef{
			Name:        toolDelegate,
			Description: "Delegate bounded work — implementation, research, or review — to a sub-agent bound to the named role. You own integration, verification, and publication; sub-agents return untrusted results.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id":           map[string]any{"type": "string", "description": "Stable brief ID unique within this run."},
					"role":         map[string]any{"type": "string", "description": "The role to run, e.g. coder or reviewer."},
					"objective":    map[string]any{"type": "string"},
					"decisions":    map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{"summary": map[string]any{"type": "string"}, "value": map[string]any{"type": "string"}}, "required": []string{"summary", "value"}}},
					"ownedFiles":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"nonGoals":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"successCheck": map[string]any{"type": "string", "description": "The observable check that decides success."},
				},
				"required": []string{"role", "objective", "successCheck"},
			},
		},
		ToolDef{
			Name:        toolCancelBrief,
			Description: "Permanently tombstone a brief's work unit: no dispatch or retry under its ID can ever run again. Delegation runs to completion once started; this cannot interrupt an in-flight brief — it only forbids any future use of the ID.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id": map[string]any{"type": "string"},
				},
				"required": []string{"id"},
			},
		})
	if c.forge != nil {
		tools = append(tools, forgeTool())
	}
	if c.skills != nil {
		tools = append(tools, skillTool())
	}
	return tools
}

// subagentTools builds the tool declarations a bounded delegate is offered:
// worker shell only. Forge reads, delegation, and cancellation are
// coordinator-only capabilities; a sub-agent's results return to trusted
// control, and it can never publish.
func (c *Coordinator) subagentTools() []ToolDef {
	tools := []ToolDef{shellTool()}
	// The skill tool is a delegated role's only skill path (#659): trusted
	// control serves the verified instructions as data, and the sub-agent
	// can no more execute skill content locally than it can shell locally.
	if c.skills != nil {
		tools = append(tools, skillTool())
	}
	return tools
}

// runTool routes one tool request through its trusted implementation.
// The request is data; nothing here executes model-controlled commands
// locally — worker commands travel over the signed protocol, and the
// coordinator pod has no execution path for them. The context is enforced,
// not just the declared toolset: a subagent (briefID set) can never delegate
// or cancel, no matter what it emits, so untrusted output can neither spawn
// nested sessions nor tombstone sibling work units.
func (c *Coordinator) runTool(ctx context.Context, call ToolCall, briefID string) ToolResult {
	result := ToolResult{ToolCallID: call.ID, Name: call.Name}
	subagent := briefID != ""
	switch call.Name {
	case toolShell:
		return c.runShell(ctx, call, briefID, result)
	case toolCheck:
		if subagent {
			result.IsError = true
			result.Content = "tracked checks are not available to sub-agents"
			return result
		}
		return c.runCheck(ctx, call, briefID, result)
	case toolDelegate:
		if subagent {
			result.IsError = true
			result.Content = "delegation is not available to sub-agents"
			return result
		}
		return c.runDelegate(ctx, call, result)
	case toolCancelBrief:
		if subagent {
			result.IsError = true
			result.Content = "cancellation is not available to sub-agents"
			return result
		}
		return c.runCancelBrief(ctx, call, result)
	case toolForge:
		if subagent {
			result.IsError = true
			result.Content = "forge operations are not available to sub-agents"
			return result
		}
		return c.runForge(ctx, call, result)
	case toolSkill:
		// The one trusted skill path (#659), coordinator and sub-agent
		// alike: verified instructions as data, nothing executed.
		return c.runSkill(call, result)
	default:
		result.IsError = true
		result.Content = fmt.Sprintf("unknown tool %q", call.Name)
		return result
	}
}

// runShell dispatches one worker task. This is the only execution path a
// model-controlled command ever takes, and it ends at the untrusted worker
// over the signed protocol — never at a local process. Coordinator-level
// commands bind to the synthetic "control" work unit; subagent commands bind
// to their brief. Each coordinator command gets a fresh snapshot of the
// trusted integration tree, so validation sees the exact published state even
// when publication re-synced the base after the previous shell task.
func (c *Coordinator) runShell(ctx context.Context, call ToolCall, briefID string, result ToolResult) ToolResult {
	if c.worker == nil {
		result.IsError = true
		result.Content = "no worker is configured in this build"
		return result
	}
	if briefID == "" {
		briefID = "control"
	}
	if briefID != "control" && c.briefs.Cancelled(briefID) {
		result.IsError = true
		result.Content = fmt.Sprintf("brief %s is cancelled", briefID)
		return result
	}
	var args struct {
		Command      string `json:"command"`
		ArtifactPath string `json:"artifactPath"`
	}
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil || strings.TrimSpace(args.Command) == "" {
		result.IsError = true
		result.Content = "shell arguments must carry a command string"
		return result
	}
	var err error
	var tip string
	if briefID == "control" {
		tip, err = c.prepareSnapshot(ctx, briefID)
	} else {
		err = c.ensureSnapshot(ctx)
	}
	if err != nil {
		result.IsError = true
		result.Content = "workspace snapshot is unavailable"
		return result
	}
	// Only model-influenced tasks carry the applied bootstrap environment
	// delta (#414): fixed control scripts (unpack, pack) run under the
	// worker's own environment.
	env := c.currentBootstrapEnv()
	task := protocol.Task{Command: []string{"sh", "-c", args.Command}, ArtifactPath: args.ArtifactPath, Env: env}
	workerResult, err := c.dispatchAndAwait(ctx, newOpID("shell", briefID), briefID, task)
	if err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	// A completed control shell run is fresh evidence against the exact
	// command-and-state identity the tracked checks use: the shell path's
	// own prepare just re-materialized the workspace at the current tip, so
	// the workspace no longer holds the state ANY receipt for this
	// command-and-tip identity describes — whether or not the re-captured
	// bootstrap environment matches the one a stored receipt recorded. Drop
	// the matching receipt regardless of environment: an env-mismatched
	// receipt was already unservable under the checks' serve-time guard,
	// and dropping it is strictly conservative. Only a completed run counts
	// (completedRun): a failing exit is fresh failing evidence —
	// the worker reports a nonzero exit as ResultFailed — while a
	// cancelled dispatch or a run that never produced an exit status
	// produced no evidence and leaves the registry untouched, and an
	// unrelated command leaves every receipt in place. The match is exact
	// on the command bytes; an equivalent command spelled differently
	// leaves the receipt to its own identity. If an in-flight check for
	// the same identity completes after this run, its receipt is equally
	// fresh evidence only when its dispatch postdates this run's
	// re-materialization; otherwise the tracked checks' generation guard
	// refuses it as evidence from an older workspace generation.
	if briefID == "control" && completedRun(workerResult.Status, workerResult.ExitCode) {
		c.checks.invalidate(checkStateKey(args.Command, tip))
	}
	return workerResultToTool(workerResult, result)
}

// runDelegate registers and runs one brief on a sub-agent session, then
// closes the brief's terminal chain: pack the worker's committed state,
// validate and integrate it as one local commit, and publish through the
// broker. The sub-agent's shell commands reach the worker; its result is
// untrusted and publication stays unreachable from it — integration and
// publication happen here, in trusted control.
func (c *Coordinator) runDelegate(ctx context.Context, call ToolCall, result ToolResult) ToolResult {
	// Soft-stop latch (#238): no new briefs once the operator has stopped
	// this incarnation. Shell, cancellation, and forge stay available so
	// the model can validate, commit, and publish what it has.
	if c.control.Stopped() {
		result.IsError = true
		result.Content = "a soft stop is in effect: this incarnation dispatches no new briefs; let in-flight work stand, run final validation, and declare your outcome"
		return result
	}
	var args struct {
		ID           string     `json:"id"`
		Role         string     `json:"role"`
		Objective    string     `json:"objective"`
		Decisions    []Decision `json:"decisions"`
		OwnedFiles   []string   `json:"ownedFiles"`
		NonGoals     []string   `json:"nonGoals"`
		SuccessCheck string     `json:"successCheck"`
	}
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
		result.IsError = true
		result.Content = "delegate arguments must be a JSON object"
		return result
	}
	brief := Brief{
		ID:           args.ID,
		Role:         args.Role,
		Objective:    args.Objective,
		Decisions:    args.Decisions,
		OwnedFiles:   args.OwnedFiles,
		NonGoals:     args.NonGoals,
		SuccessCheck: args.SuccessCheck,
	}
	// Structural validation and the role binding are checked before the
	// snapshot prepare: a delegation the harness cannot run must neither
	// burn the stable brief ID nor pay for a snapshot render.
	if err := brief.Validate(); err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	if c.bindings.Model(brief.Role) == "" {
		result.IsError = true
		result.Content = fmt.Sprintf("no model is bound to role %q", brief.Role)
		return result
	}
	registered, err := c.briefs.Register(brief)
	if err != nil {
		// The ledger rejects duplicates and tombstoned IDs before any
		// worker contact, so a repeated ID consumes nothing.
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	// Whatever this brief's fate — result, failed session, failed
	// integration, or failed durable write — its work unit ends terminal
	// when this call returns, so a soft-stop exit never waits on it again.
	defer c.briefs.MarkTerminal(registered.ID)
	// The brief's own fresh snapshot: it must carry every integrated but
	// possibly unpublished brief, so delegates never reuse a stale
	// workspace. A transient failure here fails the brief; the model
	// re-delegates the work under a fresh ID.
	dispatchedTip, err := c.prepareSnapshot(ctx, brief.ID)
	if err != nil {
		result.IsError = true
		result.Content = "workspace snapshot could not be prepared: " + safeToolError(err)
		return result
	}
	summary, completed, unavailable := c.runBrief(ctx, registered)
	if unavailable != nil {
		// Deferred provider availability on the role-bound binding (#320):
		// the structured classification crosses the delegation boundary
		// through the coordinator loop, which ends the run blocked-external
		// before any further request. The brief keeps its spent ID and its
		// partial commits stay untrusted worker data — nothing is packed,
		// integrated, or published — and the tool-visible text is fixed and
		// redacted, never the provider body.
		c.recordDeferredAvailability(unavailable)
		result.IsError = true
		result.Content = "the delegated model binding is temporarily unavailable; the run ends recoverably and this brief's incomplete work is not integrated"
		return result
	}
	if !completed {
		// A brief that never reached its final result has no outcome to
		// integrate: its partial commits stay untrusted worker data and
		// never reach pack, integration, or publication. The failure is the
		// tool-visible result, the brief ID is spent, and corrected work
		// re-delegates under a fresh ID.
		result.IsError = true
		result.Content = summary
		return result
	}
	integration, err := c.packAndIntegrate(ctx, registered, dispatchedTip, summary)
	if err != nil {
		// A rejected artifact is untrusted data and the brief fails; the
		// fixed rejection category is the tool-visible result. The registry
		// keeps the ID spent, so corrected work re-delegates under a new ID.
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	outcome := summary + "\n[no committed changes; nothing integrated]"
	if integration.Changed {
		outcome = fmt.Sprintf("%s\n[integrated as %s and published]", summary, integration.Commit)
	}
	// The delegate's actual outcome is retained under the stable brief ID;
	// the recorded commit is the local integration commit, and publication
	// of it has already been confirmed by the broker.
	c.briefs.RecordResult(registered.ID, BriefResult{BriefID: registered.ID, Summary: outcome, Commit: integration.Commit})
	// §7: the completed brief — and, when the broker confirmed the remote
	// publication, that confirmed OID as lastCommit — is recorded through the
	// trusted status path. The unit is not acknowledged complete until the
	// write is durable; a persistent write failure fails the brief even
	// though its work is on the branch, because an unacknowledged unit is
	// re-read from the world first on resume.
	if c.status != nil {
		lastCommit := ""
		if integration.Changed {
			lastCommit = integration.Commit
		}
		checkpoint := &courierv1alpha1.Checkpoint{CompletedBriefs: c.briefs.CompletedBriefs()}
		if err := c.status.CheckpointPublished(ctx, checkpoint, lastCommit); err != nil {
			result.IsError = true
			result.Content = "completed brief could not be recorded durably: " + safeToolError(err)
			return result
		}
	}
	result.Content = outcome
	return result
}

// packAndIntegrate closes one brief: dispatch the fixed pack task that
// renders the workspace's committed state as the brief's result bundle, then
// validate and integrate it, then publish the integration head through the
// broker (§7's commit-per-brief cadence). An artifact with no committed
// changes integrates nothing and is not an error.
func (c *Coordinator) packAndIntegrate(ctx context.Context, brief Brief, dispatchedTip, summary string) (Integration, error) {
	if c.publisher == nil {
		return Integration{}, errors.New("no integration path is configured in this build")
	}
	task := protocol.Task{
		Command:      []string{"sh", "-c", workerPackScript(brief.ID, dispatchedTip)},
		ArtifactPath: "artifact." + brief.ID + ".bundle",
	}
	workerResult, err := c.dispatchAndAwait(ctx, newOpID("pack", brief.ID), brief.ID, task)
	if err != nil {
		return Integration{}, err
	}
	if workerResult.ExitCode != 0 {
		return Integration{}, errors.New("artifact packing failed on the worker")
	}
	if len(workerResult.Artifact) == 0 {
		return Integration{Changed: false}, nil
	}
	integration, err := c.publisher.Integrate(ctx, IntegrationRequest{
		BriefID:       brief.ID,
		DispatchedTip: dispatchedTip,
		Bundle:        workerResult.Artifact,
		Brief:         brief,
		Summary:       summary,
	})
	if err != nil {
		return Integration{}, err
	}
	if !integration.Changed {
		return integration, nil
	}
	if _, err := c.publisher.Publish(ctx); err != nil {
		return Integration{}, err
	}
	return integration, nil
}

// runBrief drives one sub-agent session. It reports whether the brief
// reached a terminal final result: only a completed session's summary may
// enter the artifact chain. A failed or truncated session returns the
// failure as data with completed=false — whatever the worker committed
// before the failure is partial work that must never be packed,
// integrated, or published (§7's cadence is per completed unit).
//
// A stream failure classified as deferred model-provider availability is
// the exception (#320): it returns the typed error as unavailable instead
// of flattening it to tool data, because the coordinator's own binding may
// stay healthy while the role-bound binding is exhausted, and treating
// that as ordinary brief failure invites further delegations onto the
// exhausted binding.
func (c *Coordinator) runBrief(ctx context.Context, brief Brief) (summary string, completed bool, unavailable error) {
	session, err := c.newSession(brief.Role)
	if err != nil {
		return safeToolError(err), false, nil
	}
	session.Pin(Message{Role: "system", Content: briefText(brief) + "\n\n" + subagentFraming})
	// Delegated roles load the same pinned skills through the same trusted
	// tool (#659): the index is identical to the coordinator's, so a brief
	// that needs a skill does not depend on the coordinator's transcript.
	if c.skills != nil {
		session.Pin(Message{Role: "system", Content: c.skills.Section()})
	}
	// The brief's settled state is re-injected verbatim into every digest:
	// a delegate that loses its decisions mid-brief must not re-derive them.
	session.Settle(settledStateBlock(brief))
	for {
		// Trusted control's own conditions gate the brief at every model
		// boundary (#737): before the request, so a latch already tripped
		// when the brief starts never buys a turn, and after it, so a trip
		// during the turn's own tool calls aborts without a further one.
		if abort := c.delegatedTripAbort(); abort != "" {
			return abort, false, nil
		}
		turn := c.pump(ctx, session, brief.ID, c.subagentTools())
		if abort := c.delegatedTripAbort(); abort != "" {
			return abort, false, nil
		}
		if turn.harnessErr != nil {
			// Compaction could not fit the protected tail (#243). The
			// transcript is unchanged, so this is a brief failure like any
			// other — tool-visible, bounded, never a silent truncation —
			// and the coordinator re-delegates under a fresh brief ID.
			return "sub-agent context could not be compacted: " + safeToolError(turn.harnessErr), false, nil
		}
		if turn.streamErr != nil {
			if classification, ok := providerClassificationOf(turn.streamErr); ok && classification.Deferred() {
				return "", false, turn.streamErr
			}
			// Deliberate: an ordinary subagent stream failure becomes
			// tool-visible data for the coordinator, which owns the
			// brief's fate; the coordinator's own next turn surfaces a
			// gateway outage as the §8 infrastructure failure.
			return "sub-agent stream failed: " + safeToolError(turn.streamErr), false, nil
		}
		if turn.final {
			return turn.text, true, nil
		}
		if turn.toolResults == nil {
			return "sub-agent ended without a result", false, nil
		}
	}
}

// delegatedTripAbort names the trusted-control condition that aborts a
// delegated brief without a further model request: a bootstrap environment
// blocker recorded while this brief's workspace was prepared (#414), or a
// workspace-prerequisite failure the latch has tripped on for the whole
// incarnation (#737). The workspace the brief would work in cannot serve it,
// so the fixed abort summary is the brief's whole result and the
// coordinator's own loop ends the run. Empty when the brief may continue.
func (c *Coordinator) delegatedTripAbort() string {
	if c.hasBootstrapBlocker() {
		return "workspace bootstrap failed; the brief is aborted"
	}
	if c.prerequisite.peek() != nil {
		return "workspace prerequisite failed; the brief is aborted"
	}
	return ""
}

func briefText(brief Brief) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Brief %s\nRole: %s\nObjective: %s\nSuccess check: %s", brief.ID, brief.Role, brief.Objective, brief.SuccessCheck)
	if block := settledStateBlock(brief); block != "" {
		b.WriteString(block)
	}
	if len(brief.NonGoals) > 0 {
		b.WriteString("\nNon-goals: " + strings.Join(brief.NonGoals, "; "))
	}
	return b.String()
}

func (c *Coordinator) runCancelBrief(ctx context.Context, call ToolCall, result ToolResult) ToolResult {
	var args struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil || strings.TrimSpace(args.ID) == "" {
		result.IsError = true
		result.Content = "cancel_brief arguments must carry the brief ID"
		return result
	}
	if _, err := c.briefs.Cancel(args.ID); err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	result.Content = "brief " + args.ID + " is cancelled; new work under this ID is rejected"
	return result
}

func (c *Coordinator) runForge(ctx context.Context, call ToolCall, result ToolResult) ToolResult {
	if c.forge == nil {
		result.IsError = true
		result.Content = "forge operations are unavailable in this build"
		return result
	}
	var args struct {
		Operation string          `json:"operation"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
		result.IsError = true
		result.Content = "forge arguments must be a JSON object"
		return result
	}
	content, err := c.forge.Call(ctx, args.Operation, string(args.Arguments))
	if err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	result.Content = content
	return result
}

// reconcileGrace is the fixed bound for a cancellation reconciliation
// issued on an already-expired or cancelled caller context (#414): a bound
// expiry or a parent cancellation must still DELIVER the worker
// cancellation — the worker kills the task's process group — and observe
// the result. A reconcile issued on the already-expired context can never
// reach the worker, so the liveness bound would classify the run without
// ever stopping the task. The reconcile context ignores the caller
// context's deadline and cancellation but carries its own fixed grace, and
// verifiedResult is called with the reconcile state through it, so the
// verified termination lands even though the caller's context is gone.
const reconcileGrace = 30 * time.Second

// statusPersistFailure reports that an operation's status entry could not be
// persisted, so the operation was never dispatched. It is not a workspace
// condition: the run's trusted status path is what failed, and the harness
// classifies such a failure as an infrastructure failure elsewhere. It is
// typed so the prerequisite classifier (#737) never counts it as a refusal
// of the workspace preparation.
type statusPersistFailure struct {
	opID string
	err  error
}

func (e *statusPersistFailure) Error() string {
	return fmt.Sprintf("operation %s was not dispatched because its status entry could not be persisted: %v", e.opID, e.err)
}

func (e *statusPersistFailure) Unwrap() error { return e.err }

// dispatchAndAwait dispatches one operation and polls it to termination. The
// operation's active-operation entry is persisted through the trusted status
// path before the task is sent (§6: persist before dispatch), and cleared
// only on independently observed termination — a verified completion also
// writes the earned tool heartbeat in the same update; a verified
// cancellation or death clears without one. An ambiguous transport failure is
// reconciled through cancellation — never automatic redelivery — and the
// observed state is returned when the reconciliation proves the operation
// stopped; a reconciliation that cannot prove it (it failed, or it observed
// work still live in the task tree) is an unverified termination: the worker
// is fenced and the error carries ErrTerminationUnverified, so no caller can
// treat it as a settled outcome. A reconciliation that cannot prove
// termination retains the entry, because an operation that may still be
// executing is never claimed cleared.
func (c *Coordinator) dispatchAndAwait(ctx context.Context, opID, briefID string, task protocol.Task) (protocol.Result, error) {
	if err := c.workerFenced(); err != nil {
		return protocol.Result{}, err
	}
	if err := c.status.BeginOperation(ctx, opID, briefID); err != nil {
		return protocol.Result{}, &statusPersistFailure{opID: opID, err: err}
	}
	if _, err := c.worker.Dispatch(ctx, opID, briefID, task); err != nil {
		if definiteWorkerRejection(err) {
			// The worker answered with a definite rejection: the operation
			// is not running, and no redelivery or cancel theater applies.
			c.closeOperation(ctx, opID, false)
			return protocol.Result{}, err
		}
		// Ambiguous transport: the worker may have accepted the task.
		// Reconcile through cancellation; never redeliver. The reconcile
		// runs on its own bounded context (reconcileGrace) so it still
		// reaches the worker when the caller's context is already expired.
		reconcileCtx, reconcileCancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileGrace)
		defer reconcileCancel()
		state, reconcileErr := c.worker.ReconcileAmbiguous(reconcileCtx, opID, briefID)
		if reconcileErr != nil {
			// A reconciliation that could not observe termination leaves the
			// operation's fate unknown: the cancellation may never have
			// reached the worker. The worker is fenced and the error carries
			// the unverified-termination fact, so no caller can treat this
			// as a settled outcome or classify the operation as safely
			// stopped.
			c.fenceWorker(opID, reconcileErr)
			return protocol.Result{}, fmt.Errorf("dispatch of %s failed and reconciliation failed: dispatch: %v; reconcile: %w: %w", opID, err, reconcileErr, ErrTerminationUnverified)
		}
		if !terminationObserved(state) {
			// The reconciliation came back without proving the operation
			// stopped: work is still live in its task tree, or the worker
			// still reports it as running. That is an unverified
			// termination, never a settled outcome: the worker is fenced
			// and the error carries the unverified-termination fact.
			detail := unverifiedTerminationDetail(state)
			c.fenceWorker(opID, detail)
			return protocol.Result{}, fmt.Errorf("dispatch of %s failed and reconciliation did not observe termination: %v: %w", opID, detail, ErrTerminationUnverified)
		}
		result, resultErr := c.verifiedResult(reconcileCtx, opID, state)
		if resultErr == nil {
			c.releaseConsumed(reconcileCtx, opID, briefID)
		}
		return result, resultErr
	}
	for {
		state, err := c.worker.Result(ctx, opID)
		if err != nil {
			// A failed poll leaves the operation unobserved — it may still
			// be running. Reconcile through cancellation so termination is
			// proven (or observed absence) before this call gives up;
			// abandoning the op here would orphan live work. The reconcile
			// runs on its own bounded context (reconcileGrace) so it still
			// reaches the worker — and delivers the cancellation that kills
			// the task's process group — when the caller's liveness bound
			// has already expired.
			reconcileCtx, reconcileCancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileGrace)
			defer reconcileCancel()
			reconciled, reconcileErr := c.worker.ReconcileAmbiguous(reconcileCtx, opID, briefID)
			if reconcileErr != nil {
				// A reconcile that could not observe termination leaves the
				// operation's fate unknown: the cancellation may never have
				// reached the worker. The worker is fenced and the error
				// carries the unverified-termination fact, so no caller can
				// treat this as a settled outcome or classify the operation
				// as safely stopped.
				c.fenceWorker(opID, reconcileErr)
				return protocol.Result{}, fmt.Errorf("result poll for %s failed and reconciliation failed: poll: %v; reconcile: %v: %w", opID, err, reconcileErr, ErrTerminationUnverified)
			}
			if !terminationObserved(reconciled) {
				// The reconciliation came back without proving the operation
				// stopped: work is still live in its task tree, or the
				// worker still reports it as running. That is an unverified
				// termination, never a settled outcome: the entry stays
				// retained, the worker is fenced against every further
				// dispatch, and the error carries the unverified-termination
				// fact.
				detail := unverifiedTerminationDetail(reconciled)
				c.fenceWorker(opID, detail)
				return protocol.Result{}, fmt.Errorf("result poll for %s failed and reconciliation did not observe termination: %v: %w", opID, detail, ErrTerminationUnverified)
			}
			result, resultErr := c.verifiedResult(reconcileCtx, opID, reconciled)
			if resultErr == nil {
				c.releaseConsumed(reconcileCtx, opID, briefID)
			}
			return result, resultErr
		}
		switch state.Status {
		case protocol.ResultCompleted, protocol.ResultFailed, protocol.ResultCancelled:
			if state.Result == nil {
				c.closeOperation(ctx, opID, false)
				return protocol.Result{}, fmt.Errorf("operation %s ended as %s without a result", opID, state.Status)
			}
			result, resultErr := c.verifiedResult(ctx, opID, state)
			if resultErr == nil {
				c.releaseConsumed(ctx, opID, briefID)
			}
			return result, resultErr
		case "":
			// The worker definitively does not know the operation: it is not
			// running, and the entry says otherwise only because the worker
			// lost its in-process state.
			c.closeOperation(ctx, opID, false)
			return protocol.Result{}, fmt.Errorf("operation %s is unknown to the worker", opID)
		}
		// Still running: keep polling. Polling is observation, not a
		// heartbeat and not a duration limit. The throttled control read
		// (#238) lets a soft-stop latch land while an operation waits, so
		// in-flight briefs finish and integrate before the stop boundary.
		select {
		case <-ctx.Done():
			// Expiry inside the wait must deliver the worker kill exactly
			// like expiry inside an in-flight poll: the real worker answers
			// "running" immediately, so the bound usually expires here, and
			// returning without reconciling would leave the task running
			// with no cancellation ever reaching the worker. Reconcile
			// through cancellation on a non-cancellable bounded context —
			// cancelling an op whose caller context died is always
			// correct. Only a reconciliation that OBSERVES termination
			// (the worker no longer knows the op, or the op ended with an
			// empty process tree) may classify as the plain bound expiry:
			// the bound's contract is that expiry delivers and observes the
			// process-group kill. A reconcile that fails or observes live
			// work keeps the operation's entry retained, fences the worker
			// against every further dispatch, and returns an error that
			// carries both the bound expiry and the unverified-termination
			// fact, so no caller can report a stopped task it never
			// observed stopping.
			reconcileCtx, reconcileCancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileGrace)
			defer reconcileCancel()
			reconciled, reconcileErr := c.worker.ReconcileAmbiguous(reconcileCtx, opID, briefID)
			if reconcileErr == nil && terminationObserved(reconciled) {
				if _, resultErr := c.verifiedResult(reconcileCtx, opID, reconciled); resultErr == nil {
					c.releaseConsumed(reconcileCtx, opID, briefID)
				}
				return protocol.Result{}, ctx.Err()
			}
			detail := reconcileErr
			if detail == nil {
				detail = unverifiedTerminationDetail(reconciled)
			}
			c.fenceWorker(opID, detail)
			return protocol.Result{}, fmt.Errorf("%w: termination of operation %s is unverified: %v: %w", ctx.Err(), opID, detail, ErrTerminationUnverified)
		case <-time.After(200 * time.Millisecond):
		}
		c.control.PollThrottled(ctx)
	}
}

// unverifiedTerminationDetail names the observed fact that left an
// operation's termination unproven: work still live in the task tree, or a
// worker that still reports the operation as live. Every unverified
// termination reads alike, whatever path observed it.
func unverifiedTerminationDetail(state protocol.ResultState) error {
	if state.Result != nil && state.Result.LiveDescendants > 0 {
		return fmt.Errorf("%d live processes still unaccounted for in the worker pod", state.Result.LiveDescendants)
	}
	return fmt.Errorf("the worker still reports the operation as %q", state.Status)
}

// verifiedResult renders an independently observed terminal state as an
// execution result. It clears the operation's entry — with the earned tool
// heartbeat on a verified completion, without one on a verified cancellation
// or failure — before the result travels. A terminal observation that
// carries live descendants is never a verified termination: the entry stays
// retained and the worker is fenced here, for every caller, so no path that
// observed this state can leave a further dispatch running against a worker
// whose task tree may still hold live work (#414 review).
func (c *Coordinator) verifiedResult(ctx context.Context, opID string, state protocol.ResultState) (protocol.Result, error) {
	if state.Result == nil {
		// Reconciliation observed the operation's definitive non-execution —
		// a cancelled or unknown observation carries no result. That is a
		// verified termination, so the entry is cleared without a heartbeat
		// exactly as the poll loop clears it (§6 step 3); retaining it here
		// would leave suppression evidence with nothing live behind it.
		c.closeOperation(ctx, opID, false)
		return protocol.Result{}, fmt.Errorf("reconciliation of %s observed status %q without a result", opID, state.Status)
	}
	if state.Result.LiveDescendants > 0 {
		// The worker reported work that outlived the task, so its terminal
		// observation is not a verified termination: the entry is retained
		// and the result is returned as an error, never as a cleared,
		// suppressible operation (#372). The count is a pod-level floor —
		// the worker's sweep reports every unaccounted live process, which
		// can include another operation's adopted escapee (#399) — so the
		// wording stays at the pod, not the task tree. The error carries the
		// unverified-termination fact, and the fence is set HERE, for every
		// caller of dispatchAndAwait — not only the bootstrap stage that
		// re-fences on the sentinel it receives (#414 review): descendants
		// of the ended task may still be executing repository-authored
		// work, so no further dispatch may run against this worker on any
		// path that observed this state.
		err := fmt.Errorf("operation %s ended as %s with %d live processes still unaccounted for in the worker pod: %w", opID, state.Status, state.Result.LiveDescendants, ErrTerminationUnverified)
		c.fenceWorker(opID, err)
		return *state.Result, err
	}
	c.closeOperation(ctx, opID, state.Status == protocol.ResultCompleted)
	return *state.Result, nil
}

// releaseConsumed is best-effort after a verified result is held in control
// memory: a failed release leaves the worker's retained payload in place
// until worker replacement and never fails a verified outcome.
func (c *Coordinator) releaseConsumed(ctx context.Context, opID, briefID string) {
	_ = c.worker.Release(ctx, opID, briefID)
}

// closeOperation clears one operation entry after verified termination and
// records the earned tool boundary on a verified completion. A clear failure
// is retained inside the status reporter and never fails an already-verified
// outcome: the operation is over, and the entry errs toward suppression until
// a later flush clears it.
func (c *Coordinator) closeOperation(ctx context.Context, opID string, completed bool) {
	if err := c.status.EndOperation(ctx, opID, completed); err != nil {
		_ = err // retained; flushed opportunistically and on the finish path
	}
	if completed && c.activity != nil {
		// When the reporter is also the activity sink this coalesces to a
		// no-op: EndOperation already wrote the same earned heartbeat.
		c.activity.ToolBoundary()
	}
}

// ensureSnapshot uploads the sanitized workspace snapshot before the first
// worker task of this incarnation. A transient failure is retried on the
// next task rather than cached: only a successful prepare is sticky. Delegate
// shell tasks share their brief's prepared workspace; coordinator shell
// commands prepare fresh snapshots so local validation sees all integrated
// and published work (see prepareSnapshot).
func (c *Coordinator) ensureSnapshot(ctx context.Context) error {
	c.snapshotMu.Lock()
	if c.snapshotReady {
		c.snapshotMu.Unlock()
		return nil
	}
	c.snapshotMu.Unlock()
	snap, err := c.renderSnapshot(ctx)
	if err != nil {
		return err
	}
	return c.materializeSnapshot(ctx, "control", snap)
}

// prepareSnapshot renders the sanitized snapshot, uploads it to the worker,
// and dispatches the fixed unpack task that materializes the worker's
// ephemeral workspace at the snapshot tip. It returns the exact tip the
// snapshot was built from: the base tip every artifact of the named work
// unit must reach. Delegate briefs always prepare fresh — the snapshot must
// carry every integrated-but-possibly-unpublished brief — and coordinator shell commands also prepare fresh snapshots so publication
// base resyncs cannot leave validation on a stale worker workspace.
func (c *Coordinator) prepareSnapshot(ctx context.Context, briefID string) (string, error) {
	snap, err := c.renderSnapshot(ctx)
	if err != nil {
		return "", err
	}
	if err := c.materializeSnapshot(ctx, briefID, snap); err != nil {
		return "", err
	}
	return snap.Tip, nil
}

// currentSnapshotGen returns the current snapshot generation: the count of
// successful destructive materializations this incarnation has performed.
func (c *Coordinator) currentSnapshotGen() uint64 {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	return c.snapshotGen
}

// renderSnapshot re-renders the sanitized snapshot bundle from the
// integration head and returns it. It is purely local control-side work — no
// mutex and no worker interaction — and in particular nothing destructive:
// the bundle is rendered from trusted state and nothing is dispatched. The
// destructive half (the upload and the workspace-resetting unpack) lives in
// materializeSnapshot, so a caller that only needs the snapshot tip to
// classify a request — the tracked checks' registry key — never touches the
// shared worker workspace.
//
// Because rendering runs outside the mutex, two concurrent prepares may
// render before either materializes, and their materializations may run in
// either order. The last writer settles snapshotReady, snapshotTip, and the
// generation consistently with the workspace it materialized, and the
// production tool pump executes tool calls serially, so no consumer currently
// depends on render/materialize ordering across callers.
func (c *Coordinator) renderSnapshot(ctx context.Context) (Snapshot, error) {
	if c.snapshot == nil {
		err := errors.New("no snapshot provider is configured in this build")
		c.observePrerequisite(err)
		return Snapshot{}, err
	}
	snap, err := c.snapshot(ctx)
	if err != nil {
		// Every preparation path renders the snapshot, so the render
		// failure is the one observation point that covers them all (#737).
		c.observePrerequisite(err)
		return Snapshot{}, err
	}
	return snap, nil
}

// materializeSnapshot uploads the rendered snapshot bundle to the worker and
// dispatches the fixed unpack task that materializes the worker's ephemeral
// workspace at the snapshot tip, then settles the applied bootstrap
// environment the workspace now holds. This is the destructive step: the
// unpack resets and cleans the shared worker workspace, so it must run only
// after any registry claim that guards a check against the workspace state.
// Each full success bumps the snapshot generation — the coordinator-local
// witness that the workspace was re-established, wiping all worker-side
// mutation, at the moment of this materialization. As with renderSnapshot,
// concurrent prepares may materialize in either order; the last writer
// settles the tip, readiness, and generation consistently with the workspace
// it materialized.
func (c *Coordinator) materializeSnapshot(ctx context.Context, briefID string, snap Snapshot) error {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if c.worker == nil {
		err := errors.New("no worker is configured in this build")
		c.observePrerequisite(err)
		return err
	}
	if err := c.worker.client.UploadSnapshot(ctx, snap.Data); err != nil {
		c.observePrerequisite(err)
		return err
	}
	// The unpack is trusted control's own fixed script: it materializes the
	// snapshot bundle into the worker's ephemeral workspace at the snapshot
	// tip with a throwaway identity. It is dispatched over the signed
	// protocol like any task, under the work unit's own brief ID.
	task := protocol.Task{Command: []string{"sh", "-c", workerUnpackScript()}}
	result, err := c.dispatchAndAwait(ctx, newOpID("snapshot", briefID), briefID, task)
	if err != nil {
		c.observePrerequisite(err)
		return err
	}
	if result.ExitCode != 0 {
		// The fixed preparation task ran and failed: one failure may still
		// be transient, so it is observed as the task-failed class and the
		// latch decides whether it has become a condition (#737).
		failure := failedPrerequisiteTask(result.ExitCode)
		c.observePrerequisite(failure)
		return failure
	}
	// The unpack echoes the tip it materialized. On the honest path it must
	// equal the dispatched tip; a mismatch means the workspace does not hold
	// the snapshot control dispatched, and nothing may be dispatched
	// against it. A mismatch is a final fact, never a transient: the latch
	// trips on this one observation (#737).
	if strings.TrimSpace(result.StdoutTail) != snap.Tip {
		failure := mismatchedPrerequisite()
		c.observePrerequisite(failure)
		return failure
	}
	// The workspace is materialized at the dispatched tip: if the approved
	// development-bootstrap contract applies (#414), it runs now, after
	// checkout and before anything else is dispatched against this
	// workspace. A modified or absent contract dispatches nothing.
	switch snap.ContractState {
	case devenv.StateApproved:
		if err := c.bootstrapWorkspace(ctx, briefID, snap.BaseTip); err != nil {
			// The blocker, if this was one, is recorded before the error
			// returns: the run ends blocked-external at the next turn
			// boundary, whatever consumed this preparation. The #414
			// mechanism owns that class, so the prerequisite breaker never
			// observes it.
			var blocker *bootstrapBlocker
			if errors.As(err, &blocker) {
				c.recordBootstrapBlocker(blocker)
			}
			c.observePrerequisite(err)
			return err
		}
	case devenv.StateAbsent, devenv.StateModified:
		// Nothing to apply: the contract was never approved, or the work
		// tip modified it and only a human approval may run it. The
		// captured environment is cleared with it — an unpack wipes the
		// workspace a stale delta's paths point at, so feeding it forward
		// would silently break every later command instead of saying so.
		c.storeBootstrapEnv(nil)
	default:
		// An unclassified contract state skips the same way: fail closed
		// on the bootstrap and on any environment it once captured.
		c.storeBootstrapEnv(nil)
	}
	c.snapshotReady = true
	c.snapshotTip = snap.Tip
	c.snapshotGen++
	return nil
}

// bootstrapBlockerReasonPrefix is the fixed prefix every bootstrap
// environment blocker reason carries (#414): the durable reason names the
// class and the exit code only — worker output is untrusted and never
// enters a durable reason or the model transcript.
const bootstrapBlockerReasonPrefix = "the approved bootstrap contract failed"

// bootstrapBlocker is a terminal environment blocker (#414): the approved
// bootstrap contract failed, or its captured environment was unusable, so
// nothing model-influenced may run against this workspace. The reason is
// fixed text plus, where relevant, the task's own exit status — never an
// excerpt of untrusted worker output.
type bootstrapBlocker struct {
	reason string
}

func (b *bootstrapBlocker) Error() string { return b.reason }

// bootstrapBound is each development-bootstrap task's liveness bound (#414):
// the bootstrap task and the environment-capture task each get their own
// bound, never one shared budget. It is a var only so the liveness
// classification is testable; production code never reassigns it.
var bootstrapBound = devenv.DefaultBound

// recordBootstrapReceipt emits the structured bootstrap receipt (#414)
// through the optional sink. Best-effort, like the compaction precedent:
// no sink records nothing, and emission never changes the run's outcome.
func (c *Coordinator) recordBootstrapReceipt(receipt BootstrapReceipt) {
	if c.bootstrapReceipts == nil {
		return
	}
	c.bootstrapReceipts.BootstrapRecorded(receipt)
}

// bootstrapElapsedMs renders one stage's wall time as receipt milliseconds.
func bootstrapElapsedMs(started time.Time) int64 {
	return time.Since(started).Milliseconds()
}

// bootstrapWorkspace dispatches the fixed control-authored bootstrap tasks —
// the contract's own scripts for the operator-approved base revision
// baseOID, run by the worker as `sh -c` with no model-supplied value — and,
// on success, stores the filtered environment delta the capture produced, so
// every subsequent model-influenced task merges it over the worker's own
// environment. The scripts materialize the base revision from the git
// object store and run inside it (see devenv.BootstrapScript), so the
// executed bytes are the approved base revision's bytes by construction.
// The two tasks are dispatched separately and each bounded on its own. A
// failure is a terminal environment blocker, never a tool error the model
// could steer: it is recorded before it returns, so the run ends
// blocked-external at the next turn boundary. Every terminal outcome —
// applied and each blocker class — also composes a structured
// BootstrapReceipt and emits it through the optional sink.
func (c *Coordinator) bootstrapWorkspace(ctx context.Context, briefID, baseOID string) error {
	parent := ctx
	bootstrapScript, err := devenv.BootstrapScript(baseOID)
	if err != nil {
		// A base OID that fails script validation is an ordinary
		// snapshot-preparation error, not a bootstrap blocker: the dispatch
		// is refused before any task exists to fail.
		return fmt.Errorf("bootstrap task could not be prepared: %w", err)
	}
	captureScript, err := devenv.EnvCaptureScript(baseOID)
	if err != nil {
		return fmt.Errorf("environment-capture task could not be prepared: %w", err)
	}
	bootstrapStarted := time.Now()
	bootstrapCtx, cancelBootstrap := context.WithTimeout(parent, bootstrapBound)
	defer cancelBootstrap()
	bootstrapOpID := newOpID("bootstrap", briefID)
	task := protocol.Task{Command: []string{"sh", "-c", bootstrapScript}}
	result, err := c.dispatchAndAwait(bootstrapCtx, bootstrapOpID, briefID, task)
	if parent.Err() != nil {
		// The run's own cancellation superseded the stage: an ordinary
		// snapshot-preparation error, never the bootstrap's blocker — the
		// blocker classes name the bootstrap's own failures only.
		if err == nil {
			err = fmt.Errorf("operation %s was cancelled with the run", bootstrapOpID)
		}
		return err
	}
	if errors.Is(err, ErrTerminationUnverified) {
		// Unverified termination is not the bootstrap's own failure (#414
		// review): the process-group kill was never observed, so
		// repository-authored work may still be executing, and the run must
		// not classify blocked-external over it. The active-operation entry
		// stays retained and the worker is fenced — the fence is set here,
		// not only where the bound expiry produced the error, so an
		// unverified termination observed on any path stops the next
		// dispatch against a worker that may still be running repository
		// work. The error fails closed through the ordinary
		// snapshot-preparation path.
		c.fenceWorker(bootstrapOpID, err)
		return err
	}
	if bootstrapCtx.Err() != nil {
		// Bound expiry is classified from the stage context itself, not by
		// unwrapping the error: the stage context past its deadline while
		// the parent is still live is the stage's own liveness bound,
		// whether the expiry surfaced as an error or as the reconciled
		// cancellation dispatchAndAwait delivered. A transport failure with
		// a live parent is an ordinary snapshot-preparation failure.
		reason := bootstrapBlockerReasonPrefix + ": the bootstrap task exceeded its liveness bound"
		c.recordBootstrapReceipt(BootstrapReceipt{
			Status:             BootstrapReceiptFailed,
			Reason:             reason,
			BootstrapElapsedMs: bootstrapElapsedMs(bootstrapStarted),
			BootstrapExitCode:  -1,
			CaptureExitCode:    -1,
		})
		return &bootstrapBlocker{reason: reason}
	}
	if err != nil {
		return err
	}
	// The bootstrap task emitted something; its fixed diagnostic lines are
	// the receipt's validated facts, whatever the outcome turns out to be.
	diagnostics := devenv.ParseBootstrapDiagnostics(result.StdoutTail)
	bootstrapElapsed := bootstrapElapsedMs(bootstrapStarted)
	if result.ExitCode != 0 {
		if result.Status == protocol.ResultCancelled {
			reason := bootstrapBlockerReasonPrefix + ": the bootstrap task was cancelled before it completed"
			c.recordBootstrapReceipt(BootstrapReceipt{
				Status:             BootstrapReceiptFailed,
				Reason:             reason,
				BootstrapElapsedMs: bootstrapElapsed,
				BootstrapExitCode:  result.ExitCode,
				CaptureExitCode:    -1,
				Diagnostics:        diagnostics,
			})
			return &bootstrapBlocker{reason: reason}
		}
		reason := fmt.Sprintf("%s with exit code %d", bootstrapBlockerReasonPrefix, result.ExitCode)
		c.recordBootstrapReceipt(BootstrapReceipt{
			Status:             BootstrapReceiptFailed,
			Reason:             reason,
			BootstrapElapsedMs: bootstrapElapsed,
			BootstrapExitCode:  result.ExitCode,
			CaptureExitCode:    -1,
			Diagnostics:        diagnostics,
		})
		return &bootstrapBlocker{reason: reason}
	}
	captureStarted := time.Now()
	captureCtx, cancelCapture := context.WithTimeout(parent, bootstrapBound)
	defer cancelCapture()
	captureOpID := newOpID("bootstrap-env", briefID)
	captureTask := protocol.Task{Command: []string{"sh", "-c", captureScript}}
	capture, err := c.dispatchAndAwait(captureCtx, captureOpID, briefID, captureTask)
	if parent.Err() != nil {
		if err == nil {
			err = fmt.Errorf("operation %s was cancelled with the run", captureOpID)
		}
		return err
	}
	if errors.Is(err, ErrTerminationUnverified) {
		// The same unverified-termination rule as the bootstrap stage: a
		// termination that was never observed is not an environment blocker.
		// The entry stays retained, the worker is fenced, and the error fails
		// closed through the ordinary path.
		c.fenceWorker(captureOpID, err)
		return err
	}
	if captureCtx.Err() != nil {
		reason := bootstrapBlockerReasonPrefix + ": the environment-capture task exceeded its liveness bound"
		c.recordBootstrapReceipt(BootstrapReceipt{
			Status:             BootstrapReceiptFailed,
			Reason:             reason,
			BootstrapElapsedMs: bootstrapElapsed,
			BootstrapExitCode:  result.ExitCode,
			CaptureElapsedMs:   bootstrapElapsedMs(captureStarted),
			CaptureExitCode:    -1,
			Diagnostics:        diagnostics,
		})
		return &bootstrapBlocker{reason: reason}
	}
	if err != nil {
		return err
	}
	captureElapsed := bootstrapElapsedMs(captureStarted)
	if capture.ExitCode != 0 {
		if capture.Status == protocol.ResultCancelled {
			reason := bootstrapBlockerReasonPrefix + ": the environment-capture task was cancelled before it completed"
			c.recordBootstrapReceipt(BootstrapReceipt{
				Status:             BootstrapReceiptFailed,
				Reason:             reason,
				BootstrapElapsedMs: bootstrapElapsed,
				BootstrapExitCode:  result.ExitCode,
				CaptureElapsedMs:   captureElapsed,
				CaptureExitCode:    capture.ExitCode,
				Diagnostics:        diagnostics,
			})
			return &bootstrapBlocker{reason: reason}
		}
		reason := fmt.Sprintf("%s with exit code %d", bootstrapBlockerReasonPrefix, capture.ExitCode)
		c.recordBootstrapReceipt(BootstrapReceipt{
			Status:             BootstrapReceiptFailed,
			Reason:             reason,
			BootstrapElapsedMs: bootstrapElapsed,
			BootstrapExitCode:  result.ExitCode,
			CaptureElapsedMs:   captureElapsed,
			CaptureExitCode:    capture.ExitCode,
			Diagnostics:        diagnostics,
		})
		return &bootstrapBlocker{reason: reason}
	}
	delta, keys, err := devenv.ParseEnvCapture(capture.StdoutTail, nil)
	if err != nil {
		reason := bootstrapBlockerReasonPrefix + ": bootstrap environment capture malformed"
		c.recordBootstrapReceipt(BootstrapReceipt{
			Status:             BootstrapReceiptFailed,
			Reason:             reason,
			BootstrapElapsedMs: bootstrapElapsed,
			BootstrapExitCode:  result.ExitCode,
			CaptureElapsedMs:   captureElapsed,
			CaptureExitCode:    capture.ExitCode,
			Diagnostics:        diagnostics,
		})
		return &bootstrapBlocker{reason: reason}
	}
	c.recordBootstrapReceipt(BootstrapReceipt{
		Status:             BootstrapReceiptApplied,
		BootstrapElapsedMs: bootstrapElapsed,
		BootstrapExitCode:  result.ExitCode,
		CaptureElapsedMs:   captureElapsed,
		CaptureExitCode:    capture.ExitCode,
		EnvKeys:            len(keys),
		Diagnostics:        diagnostics,
	})
	c.storeBootstrapEnv(delta)
	return nil
}

// workerUnpackScript materializes the uploaded snapshot bundle as the
// worker's ephemeral workspace: the fixed snapshot work ref becomes the
// local work branch, the worktree is reset to it, and untracked leftovers
// of earlier tasks are removed. No brief- or model-supplied value enters
// this script.
func workerUnpackScript() string {
	return strings.Join([]string{
		"set -eu",
		"git init -q --initial-branch=integration .",
		"git config user.name courier",
		"git config user.email courier@invalid",
		"git fetch -q --no-tags snapshot.pack +refs/courier/snapshot/work:refs/heads/work",
		"git symbolic-ref HEAD refs/heads/work",
		"git reset -q --hard refs/heads/work",
		"git clean -qfdx",
		"git rev-parse HEAD",
	}, "\n")
}

// workerPackScript is the fixed per-brief artifact task: it captures the
// workspace's committed state as the brief's result ref and renders the
// self-contained bundle control validates. When the workspace carries no
// committed work beyond the dispatched tip it says so and produces no
// artifact — an honest research brief integrates nothing. The brief ID is
// already validated to the ref- and filename-safe alphabet.
func workerPackScript(briefID, dispatchedTip string) string {
	bundle := "artifact." + briefID + ".bundle"
	return strings.Join([]string{
		"set -eu",
		`head="$(git rev-parse HEAD)"`,
		`if [ "$head" = "` + dispatchedTip + `" ]; then`,
		`  echo "no committed changes beyond the dispatched snapshot tip"`,
		"  exit 0",
		"fi",
		"git update-ref " + ResultRefPrefix + briefID + ` "$head"`,
		"rm -f " + bundle,
		"git bundle create " + bundle + " " + ResultRefPrefix + briefID,
		"git bundle list-heads " + bundle,
	}, "\n")
}

// maxInlineArtifactBytes bounds the artifact bytes inlined into the model
// transcript. It is a transport resource bound (same class as
// maxStreamLineBytes), not a work limit: bulk artifacts stay worker-side
// data that #125 validates and integrates as git bundles.
const maxInlineArtifactBytes = 64 << 10

func workerResultToTool(result protocol.Result, tool ToolResult) ToolResult {
	var b strings.Builder
	if result.ExitCode != 0 {
		tool.IsError = true
	}
	fmt.Fprintf(&b, "exit code: %d\n", result.ExitCode)
	if result.StdoutTail != "" {
		b.WriteString("stdout:\n" + result.StdoutTail + "\n")
	}
	if result.StderrTail != "" {
		b.WriteString("stderr:\n" + result.StderrTail + "\n")
	}
	if len(result.Artifact) > 0 {
		if len(result.Artifact) > maxInlineArtifactBytes {
			fmt.Fprintf(&b, "artifact: %d bytes, not inlined (display bound; brief artifacts integrate only through the pack flow)\n", len(result.Artifact))
		} else {
			b.WriteString("artifact:\n" + string(result.Artifact) + "\n")
		}
	}
	tool.Content = b.String()
	return tool
}

// safeToolError renders an error for a tool result. Tool results are model
// data; internal error chains can name configuration and identities but
// never credentials, because no credential reaches this code path.
func safeToolError(err error) string {
	return sanitizeExcerpt(err.Error())
}

// sanitizeExcerpt bounds untrusted output for a tool-visible message:
// newlines become spaces, remaining control bytes are dropped, and the
// excerpt is truncated. safeToolError applies it to an error's text before
// that text reaches the model transcript.
func sanitizeExcerpt(value string) string {
	value = strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case r < 0x20 || r == 0x7f:
			return -1
		default:
			return r
		}
	}, value)
	if len(value) > 500 {
		value = value[:500]
	}
	return value
}

// definiteWorkerRejection reports whether the worker definitely refused the
// operation (a 4xx class protocol rejection): the operation is not running.
// Anything else — transport failure, 5xx, unknown — is ambiguous and must be
// reconciled rather than redelivered.
func definiteWorkerRejection(err error) bool {
	var transportErr *protocol.TransportError
	if !errors.As(err, &transportErr) {
		return false
	}
	return definiteWorkerRejectionStatus(transportErr.StatusCode)
}

// definiteWorkerRejectionStatus reports whether one worker response status is
// a definite refusal. The rule is the same wherever the status is read: a
// request timeout and a rate limit are ambiguous, and every other 4xx is the
// worker refusing outright.
func definiteWorkerRejectionStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	default:
		return status >= 400 && status < 500
	}
}
