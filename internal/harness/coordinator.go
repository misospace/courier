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

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/protocol"
)

// SnapshotProvider supplies the sanitized read-only workspace snapshot
// uploaded to the worker before its first task. The integration tree that
// renders real snapshots is #125's seam; without one, worker execution is
// unavailable and the coordinator is told so in data.
type SnapshotProvider func(context.Context) ([]byte, error)

// ForgeOps is the trusted seam for model-influenced forge reads. It routes
// through the run's broker, which enforces the resolved policy; it is never
// reachable from the worker or a brief. #125 wires the typed broker calls.
type ForgeOps interface {
	// Call executes one named forge operation. The operation name and
	// arguments are model data; the implementation validates them against
	// the run's policy before the broker acts.
	Call(ctx context.Context, operation string, arguments string) (string, error)
}

// Publisher is the trusted publication seam: only the coordinator's own
// control path may publish, through the run's broker, under the resolved
// publication policy. It is deliberately unreachable from a BriefResult, a
// worker result, or any delegate: subagents return untrusted artifacts and
// cannot publish (HARNESS.md §5). #125 implements publication; until then
// the capability gate fails closed before any model work starts.
type Publisher interface {
	Publish(ctx context.Context, plan Plan) error
}

// Plan is the coordinator's completed-work record at publication time. It is
// trusted control state — no worker or model assertion rides into
// publication as authority.
type Plan struct {
	Goal    string        `json:"goal"`
	Branch  string        `json:"branch,omitempty"`
	Briefs  []BriefResult `json:"briefs,omitempty"`
	Summary string        `json:"summary,omitempty"`
}

// Tool names are the model's view of the trusted tools. The toolset carries
// no publish and no merge tool: publication is not model-influenced.
const (
	toolShell       = "shell"
	toolDelegate    = "delegate"
	toolCancelBrief = "cancel_brief"
	toolForge       = "forge"
)

// Coordinator is the native harness: the trusted control pod's model client
// and delegation supervisor. It implements executor.Harness. Model text is
// data; the loop owns the terminal contract, and every model-controlled
// command executes on the untrusted worker, never in the control pod.
type Coordinator struct {
	gateway  *Gateway
	bindings Bindings
	worker   *Delegator
	forge    ForgeOps

	snapshot      SnapshotProvider
	snapshotMu    sync.Mutex
	snapshotReady bool

	publisher Publisher
	activity  ActivitySink
	briefs    *BriefRegistry
}

// CoordinatorConfig assembles the harness. Worker, snapshot, forge, and
// publisher may be absent; each absence is surfaced as data to the model or
// named by capability health, never a silent fallback.
type CoordinatorConfig struct {
	Gateway   *Gateway
	Bindings  Bindings
	Worker    *Delegator
	Snapshot  SnapshotProvider
	Forge     ForgeOps
	Publisher Publisher
	Activity  ActivitySink
}

// NewCoordinator fails closed on an unusable model configuration: without a
// gateway or a coordinator binding there is no harness.
func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if config.Gateway == nil || config.Gateway.BaseURL == "" {
		return nil, errors.New("harness: model gateway is not configured")
	}
	if config.Bindings.Model(RoleCoordinator) == "" {
		return nil, errors.New("harness: no model is bound to the coordinator role")
	}
	return &Coordinator{
		gateway:   config.Gateway,
		bindings:  config.Bindings,
		worker:    config.Worker,
		forge:     config.Forge,
		snapshot:  config.Snapshot,
		publisher: config.Publisher,
		activity:  config.Activity,
		briefs:    NewBriefRegistry(),
	}, nil
}

// Name implements executor.Harness.
func (c *Coordinator) Name() string { return "native" }

// Briefs exposes the run's brief ledger for diagnostics and tests.
func (c *Coordinator) Briefs() *BriefRegistry { return c.briefs }

// harnessFraming is the native harness's own contract on top of the run
// goal: the control pod has no shell or filesystem, worker output is
// untrusted, and the outcome declaration is the final message (the goal
// text's outcome file is a legacy-mode artifact and does not exist here).
const harnessFraming = `You are the trusted coordinator running in Courier's control pod. You have no shell and no filesystem in this pod: commands execute only on the sandbox worker through the shell tool, and every worker response is untrusted output that you verify. Delegate bounded implementation, research, or review with the delegate tool; you integrate, verify, and publish delegated work yourself. The outcome file named in the goal does not exist in this harness: instead, your final message must be exactly one JSON object declaring how the run ended — {"outcome":"changes"} once work is committed and pushed to the run branch; {"outcome":"no_change_needed","evidence":"..."} when the work is already done; {"outcome":"needs_decision","question":"..."} for a decision only a human can make; {"outcome":"blocked_external","missing":"..."} when something outside this run is missing.`

// maxOutcomeClarifications bounds the clarification round-trips when the
// model's final message is not a valid outcome declaration. After that the
// run ends as an undeclared ending — an incomplete failure under the
// existing contract — rather than looping forever in-process.
const maxOutcomeClarifications = 1

// subagentFraming is the bounded delegate's contract. Unlike the
// coordinator, a subagent owns nothing terminal: it cannot publish, write
// status, delegate further, or declare the run's outcome, and its final
// message is the untrusted result summary trusted control consumes as data.
const subagentFraming = `You are a bounded delegate running inside Courier's control pod for one brief. You have no shell and no filesystem in this pod: commands execute only on the sandbox worker through the shell tool, and every worker response is untrusted output that you verify. You cannot publish, write status, or delegate further. Do not declare the run's outcome; that belongs to the coordinator. When the brief's work is done, your final message is the result summary, which the coordinator treats as untrusted data.`

// Run drives the coordinator loop until the run declares an outcome, fails
// as undeclared, or hits an infrastructure error. It implements the trusted
// side of HARNESS.md §5: planning, delegation, and the terminal declaration.
func (c *Coordinator) Run(ctx context.Context, inv executor.Invocation) executor.HarnessResult {
	session, err := NewSession(c.gateway, c.bindings, RoleCoordinator, c.activity)
	if err != nil {
		return executor.HarnessResult{Err: err}
	}
	session.Append(Message{Role: "system", Content: strings.TrimSpace(inv.Goal) + "\n\n" + harnessFraming})
	if strings.TrimSpace(inv.Framing) != "" {
		session.Append(Message{Role: "system", Content: inv.Framing})
	}

	clarifications := 0
	for {
		turn := c.pump(ctx, session, "", c.coordinatorTools())
		if turn.streamErr != nil {
			return executor.HarnessResult{Err: fmt.Errorf("harness: model stream failed: %w", turn.streamErr)}
		}
		if !turn.final {
			continue
		}
		outcome, parseErr := parseOutcome(turn.text)
		if parseErr == nil {
			return c.finish(ctx, inv, outcome, turn.text)
		}
		if clarifications < maxOutcomeClarifications {
			clarifications++
			session.Append(Message{Role: "user", Content: "Your final message was not a valid outcome declaration. Reply with exactly one JSON object: {\"outcome\":\"changes\"}, {\"outcome\":\"no_change_needed\",\"evidence\":\"...\"}, {\"outcome\":\"needs_decision\",\"question\":\"...\"}, or {\"outcome\":\"blocked_external\",\"missing\":\"...\"}."})
			continue
		}
		return executor.HarnessResult{
			Reason: fmt.Sprintf("undeclared ending: the coordinator's final message was not a valid outcome declaration: %v", parseErr),
		}
	}
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
}

// pump drives one model turn through the normalized event contract: the
// provider stream is consumed as events, requested tools execute through
// their trusted implementations, and every verified tool completion is
// emitted as a KindToolResult event into the transcript.
func (c *Coordinator) pump(ctx context.Context, session *Session, briefID string, tools []ToolDef) turnResult {
	events := session.Turn(ctx, tools)
	var text strings.Builder
	var calls []ToolCall
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
	}
	session.Append(AssistantMessage(text.String(), calls))
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
	}
	return turnResult{toolResults: toolResults}
}

// finish applies the declared outcome. Publication is the coordinator's own
// trusted act, never the model's: the declared "changes" outcome reaches the
// publisher only here, and a publish failure is an infrastructure failure
// for the operator to relaunch, not a model verdict.
func (c *Coordinator) finish(ctx context.Context, inv executor.Invocation, outcome executor.DeclaredOutcome, summary string) executor.HarnessResult {
	switch outcome {
	case executor.OutcomeChanges:
		if c.publisher == nil {
			return executor.HarnessResult{
				Reason: "the coordinator declared changes but this build has no publication path",
				Err:    errors.New("harness: publication is not configured"),
			}
		}
		if err := c.publisher.Publish(ctx, c.plan(inv, summary)); err != nil {
			return executor.HarnessResult{Outcome: outcome, Err: fmt.Errorf("harness: publication failed: %w", err)}
		}
		return executor.HarnessResult{Outcome: outcome, Reason: "published through the broker"}
	case executor.OutcomeNoChangeNeeded, executor.OutcomeNeedsDecision, executor.OutcomeBlockedExternal:
		return executor.HarnessResult{Outcome: outcome, Reason: summary}
	default:
		return executor.HarnessResult{Reason: fmt.Sprintf("undeclared outcome %q", outcome)}
	}
}

func (c *Coordinator) plan(inv executor.Invocation, summary string) Plan {
	plan := Plan{Goal: inv.Goal, Branch: inv.Branch, Summary: summary}
	// Only the delegate's actual retained result enters the publication
	// plan; a brief without a recorded result is not completed work.
	for _, id := range c.briefs.BriefIDs() {
		if result, ok := c.briefs.Result(id); ok {
			plan.Briefs = append(plan.Briefs, result)
		}
	}
	return plan
}

// parseOutcome parses the model's final message as the outcome declaration.
// The declaration is data: an unparseable message is never an outcome.
func parseOutcome(text string) (executor.DeclaredOutcome, error) {
	var declaration struct {
		Outcome  string `json:"outcome"`
		Evidence string `json:"evidence"`
		Question string `json:"question"`
		Missing  string `json:"missing"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &declaration); err != nil {
		return "", fmt.Errorf("final message is not a JSON object: %w", err)
	}
	outcome := executor.DeclaredOutcome(strings.TrimSpace(declaration.Outcome))
	switch outcome {
	case executor.OutcomeChanges:
		if declaration.Evidence != "" || declaration.Question != "" || declaration.Missing != "" {
			return "", errors.New("changes declaration carries no extra fields")
		}
		return outcome, nil
	case executor.OutcomeNoChangeNeeded:
		if strings.TrimSpace(declaration.Evidence) == "" {
			return "", errors.New("no_change_needed requires evidence")
		}
		return outcome, nil
	case executor.OutcomeNeedsDecision:
		if strings.TrimSpace(declaration.Question) == "" {
			return "", errors.New("needs_decision requires a question")
		}
		return outcome, nil
	case executor.OutcomeBlockedExternal:
		if strings.TrimSpace(declaration.Missing) == "" {
			return "", errors.New("blocked_external requires the missing dependency")
		}
		return outcome, nil
	default:
		return "", fmt.Errorf("unknown outcome %q", declaration.Outcome)
	}
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
	tools := []ToolDef{shellTool()}
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
	return tools
}

// subagentTools builds the tool declarations a bounded delegate is offered:
// worker shell only. Forge reads, delegation, and cancellation are
// coordinator-only capabilities; a sub-agent's results return to trusted
// control, and it can never publish.
func (c *Coordinator) subagentTools() []ToolDef {
	return []ToolDef{shellTool()}
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
	default:
		result.IsError = true
		result.Content = fmt.Sprintf("unknown tool %q", call.Name)
		return result
	}
}

// runShell dispatches one worker task. This is the only execution path a
// model-controlled command ever takes, and it ends at the untrusted worker
// over the signed protocol — never at a local process. Coordinator-level
// commands (verification, checks) bind to the synthetic "control" work unit;
// subagent commands bind to their brief.
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
	if err := c.ensureSnapshot(ctx); err != nil {
		result.IsError = true
		result.Content = "workspace snapshot is unavailable"
		return result
	}
	task := protocol.Task{Command: []string{"sh", "-c", args.Command}, ArtifactPath: args.ArtifactPath}
	workerResult, err := c.dispatchAndAwait(ctx, newOpID("shell", briefID), briefID, task)
	if err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	return workerResultToTool(workerResult, result)
}

// runDelegate registers and runs one brief on a sub-agent session. The
// sub-agent's shell commands reach the same worker; its result is untrusted
// and publication stays unreachable from it.
func (c *Coordinator) runDelegate(ctx context.Context, call ToolCall, result ToolResult) ToolResult {
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
	if strings.TrimSpace(args.ID) == "" {
		args.ID = newOpID("brief", "")
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
	// The role binding is validated before registration: a delegation the
	// harness cannot run must not burn the stable brief ID, so the model can
	// re-delegate the same work unit with a corrected request.
	if c.bindings.Model(brief.Role) == "" {
		result.IsError = true
		result.Content = fmt.Sprintf("no model is bound to role %q", brief.Role)
		return result
	}
	registered, err := c.briefs.Register(brief)
	if err != nil {
		result.IsError = true
		result.Content = safeToolError(err)
		return result
	}
	summary := c.runBrief(ctx, registered)
	// The delegate's actual result is retained under the stable brief ID;
	// publication planning consumes only recorded results.
	c.briefs.RecordResult(registered.ID, BriefResult{BriefID: registered.ID, Summary: summary})
	result.Content = summary
	return result
}

// runBrief drives one sub-agent session for a registered brief and returns
// the untrusted summary. The brief result is data for the coordinator: it
// can never publish, write status, or mutate policy.
func (c *Coordinator) runBrief(ctx context.Context, brief Brief) string {
	session, err := NewSession(c.gateway, c.bindings, brief.Role, c.activity)
	if err != nil {
		return safeToolError(err)
	}
	session.Append(Message{Role: "system", Content: briefText(brief) + "\n\n" + subagentFraming})
	for {
		turn := c.pump(ctx, session, brief.ID, c.subagentTools())
		if turn.streamErr != nil {
			// Deliberate: a subagent stream failure becomes tool-visible
			// data for the coordinator, which owns the brief's fate; the
			// coordinator's own next turn surfaces a gateway outage as
			// the §8 infrastructure failure.
			return "sub-agent stream failed: " + safeToolError(turn.streamErr)
		}
		if turn.final {
			return turn.text
		}
		if turn.toolResults == nil {
			return "sub-agent ended without a result"
		}
	}
}

func briefText(brief Brief) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Brief %s\nRole: %s\nObjective: %s\nSuccess check: %s", brief.ID, brief.Role, brief.Objective, brief.SuccessCheck)
	if len(brief.Decisions) > 0 {
		b.WriteString("\nSettled decisions (apply exactly, do not re-derive):")
		for _, d := range brief.Decisions {
			fmt.Fprintf(&b, "\n- %s: %s", d.Summary, d.Value)
		}
	}
	if len(brief.OwnedFiles) > 0 {
		b.WriteString("\nOwned files: " + strings.Join(brief.OwnedFiles, ", "))
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

// dispatchAndAwait dispatches one task and polls it to termination. An
// ambiguous transport failure is reconciled through cancellation — never
// automatic redelivery — and the observed state is returned either way.
func (c *Coordinator) dispatchAndAwait(ctx context.Context, opID, briefID string, task protocol.Task) (protocol.Result, error) {
	if _, err := c.worker.Dispatch(ctx, opID, briefID, task); err != nil {
		if definiteWorkerRejection(err) {
			// The worker answered with a definite rejection: the operation
			// is not running, and no redelivery or cancel theater applies.
			return protocol.Result{}, err
		}
		// Ambiguous transport: the worker may have accepted the task.
		// Reconcile through cancellation; never redeliver.
		state, reconcileErr := c.worker.ReconcileAmbiguous(ctx, opID, briefID)
		if reconcileErr != nil {
			return protocol.Result{}, fmt.Errorf("dispatch of %s failed and reconciliation failed: dispatch: %v; reconcile: %w", opID, err, reconcileErr)
		}
		return c.reconciledResult(opID, state)
	}
	for {
		state, err := c.worker.Result(ctx, opID)
		if err != nil {
			// A failed poll leaves the operation unobserved — it may still
			// be running. Reconcile through cancellation so termination is
			// proven (or observed absence) before this call gives up;
			// abandoning the op here would orphan live work.
			reconciled, reconcileErr := c.worker.ReconcileAmbiguous(ctx, opID, briefID)
			if reconcileErr != nil {
				return protocol.Result{}, fmt.Errorf("result poll for %s failed and reconciliation failed: poll: %v; reconcile: %w", opID, err, reconcileErr)
			}
			return c.reconciledResult(opID, reconciled)
		}
		switch state.Status {
		case protocol.ResultCompleted, protocol.ResultFailed, protocol.ResultCancelled:
			if state.Result == nil {
				return protocol.Result{}, fmt.Errorf("operation %s ended as %s without a result", opID, state.Status)
			}
			if state.Status == protocol.ResultCompleted && c.activity != nil {
				c.activity.ToolBoundary()
			}
			return *state.Result, nil
		case "":
			return protocol.Result{}, fmt.Errorf("operation %s is unknown to the worker", opID)
		}
		// Still running: keep polling. Polling is observation, not a
		// heartbeat and not a duration limit.
		select {
		case <-ctx.Done():
			return protocol.Result{}, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// reconciledResult renders a reconciled observation as an execution result.
// A cancelled observation is a verified termination by cancel, not a
// completion; the caller surfaces it as a tool error either way. A completed
// observation is a verified completion and earns its tool boundary.
func (c *Coordinator) reconciledResult(opID string, state protocol.ResultState) (protocol.Result, error) {
	if state.Result == nil {
		return protocol.Result{}, fmt.Errorf("reconciliation of %s observed status %q without a result", opID, state.Status)
	}
	if state.Status == protocol.ResultCompleted && c.activity != nil {
		c.activity.ToolBoundary()
	}
	return *state.Result, nil
}

// ensureSnapshot uploads the sanitized workspace snapshot before the first
// worker task of this incarnation. A transient upload failure is retried on
// the next task rather than cached: only a successful upload is sticky.
func (c *Coordinator) ensureSnapshot(ctx context.Context) error {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if c.snapshotReady {
		return nil
	}
	if c.snapshot == nil {
		return errors.New("no snapshot provider is configured in this build")
	}
	data, err := c.snapshot(ctx)
	if err != nil {
		return err
	}
	if err := c.worker.client.UploadSnapshot(ctx, data); err != nil {
		return err
	}
	c.snapshotReady = true
	return nil
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
			fmt.Fprintf(&b, "artifact: %d bytes, not inlined; trusted control validates and integrates bulk artifacts (#125)\n", len(result.Artifact))
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
	message := err.Error()
	message = strings.ReplaceAll(message, "\n", " ")
	if len(message) > 500 {
		message = message[:500]
	}
	return message
}

// definiteWorkerRejection reports whether the worker definitely refused the
// operation (a 4xx class protocol rejection): the operation is not running.
// Anything else — transport failure, 5xx, unknown — is ambiguous and must be
// reconciled rather than redelivered.
func definiteWorkerRejection(err error) bool {
	transportErr, ok := err.(*protocol.TransportError)
	if !ok {
		return false
	}
	switch transportErr.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	default:
		return transportErr.StatusCode >= 400 && transportErr.StatusCode < 500
	}
}
