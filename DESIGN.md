# Courier

Courier is a Kubernetes operator that runs autonomous coding *coordinators* in
pods. You feed it an issue (or a PR with feedback); it runs a coordinator that
plans, delegates to model sub-agents, locally validates and publishes a change,
then hands control back to the operator to observe CI and a human to merge or
send back with feedback.

It replaces [Foreman](https://github.com/llmkube) as the executor for a
self-hosted coding loop, and it subsumes the scheduled "cronjob" work that
preceded Foreman. It is agnostic — dispatch is one source adapter, not a
dependency — and it is meant to be run by other people, not just its author. It
dogfoods itself: Courier develops Courier.

## Motivation

Foreman kneecaps local models. It runs each task in a narrow pod, gives the
coder no feedback mid-run, and treats a reviewer NO-GO as a reason to kill the
workload rather than to let the coder *fix the thing*. But local models do their
best work exactly when they can iterate locally: delegate, integrate, run
relevant checks, and converge over many turns before publishing. That is how an
opencode session left running for hours turns out a locally validated PR, and it
is the loop Foreman structurally prevents.

The result is model-agnostic — the same local-iteration loop serves cloud
models just as well. Local is simply the case that needed it most, and the case
most executors ignore; the operator owns external verification after handoff.

Courier inverts Foreman's stance. Its guiding principle:

**Inform, don't constrain.** Anything driven by an LLM is a soft limit anyway,
and the more hard limits you stack on it, the less effective it gets. So Courier
gives the coordinator the *context* to make good choices — the hardware reality,
live load metrics, the tools to see and change the world — and trusts its
judgment, instead of boxing it in with gates, caps, and one-shot pods.

## Design principles

- **Inform, don't constrain.** Context and tools over enforcement machinery.
  Prompts are goals plus tools, not paragraphs of scaffolding. The models are
  not dumb.
- **The world is the source of truth.** Git, the PR, and CI are ground truth.
  Internal run state is a hint that is always reconciled toward the world, and
  the world wins on conflict.
- **Thin harness, lean on what exists.** Git for durable state, Loki/
  VictoriaLogs for transcripts, Kubernetes for lifecycle. Courier builds only
  what nothing else provides.
- **Agnostic, and model-agnostic.** The core speaks generic interfaces.
  Dispatch, GitHub labels, cron, CLI, and a web UI are all *adapters*; nothing in
  the core depends on dispatch or on any one forge. Models are the same: a lane
  runs whatever a `LaneProfile` names — Anthropic, OpenAI, MiniMax, a local vLLM,
  any mix. Courier is **local-capable, not local-constrained**. Its author runs
  it local-only; nothing in the design requires that of anyone else.
- **No wall-clock deadlines.** Consumer hardware is slow; an 18-hour run that is
  making progress is fine. Courier bounds *stuck*, not *duration*.

## Architecture

The deployed legacy path is one pod in which OpenCode, model-controlled tools,
and forge credentials share a trust boundary; it is explicitly insecure. The
target path in [HARNESS.md](./HARNESS.md) separates trusted harness control,
trusted broker, and untrusted worker. The target is a design, not a claim that
all workers or isolation controls are implemented or ready.

```
 sources                       operator (Courier)                legacy: one pod
 ┌─────────┐   creates    ┌───────────────────────┐  launches   ┌────────────────┐
 │dispatch │─────────────▶│ reconcile CoderRun    │────────────▶│ OpenCode +     │
 │gh-label │              │  - admit / claim      │             │ model tools    │
 │cron     │              │  - launch / resume    │   status    │ + credentials  │
 │cli / web│◀─── status ──│  - liveness / reap    │◀────────────│ insecure       │
 └─────────┘              └───────────────────────┘             └────────────────┘
                                    │ target: control / broker / worker
                                    ▼
                        trusted harness ──▶ trusted broker ──▶ forge/git
                              │
                              └──▶ untrusted, isolated worker
```

- **Sources** create `CoderRun` objects. A source adapter is also how work-state
  flows back (claim, in-progress, in-review, needs-human).
- **The operator** reconciles `CoderRun`s: admits under a lane's concurrency
  limit, claims the work in its source, launches (or resumes) the current legacy
  pod or, when implemented, the target harness topology, watches liveness, and
  drives phase transitions.
- **Legacy coordinator pod (current):** OpenCode and model-controlled tools share
  a pod with forge credentials. Prompt permissions, process separation, MCP, and
  NetworkPolicy do not make this boundary secure.
- **Target harness (the isolation wiring and model-facing harness are
  implemented as opt-in secure mode; artifact integration and authenticated
  status are #125/#126):** each run has trusted
  control, a dedicated trusted broker behind a run-specific Service, and an
  isolated untrusted worker. Control owns model calls, orchestration, integration,
  and signed worker tasks; the broker holds credentials and enforces semantic
  forge, git-repository, ref, and trusted-status policy. The worker has no
  credentials or workload identity and can reach only an administrator-populated
  Go module cache, without worker DNS, not the broker or forge. Secure mode requires
  enforced network policy, live deny probes, and fail-closed preflight before
  workload exposure. See [HARNESS.md](./HARNESS.md) for the detailed contract,
  remaining implementation blockers, and acceptance criteria.
- **The forge and git** hold durable output: branches, commits, PRs, and CI.

## The coordinator

The coordinator role method is **plan, delegate, integrate, locally validate,
and publish — do not implement yourself.** It settles the design and owns
integration and local validation; it hands small, file-scoped briefs to a
bounded delegate (objective, settled decisions with exact values, owned files,
out-of-scope, an observable success check) and asks for a compact handoff. A
review pass may be useful, but the coordinator does not wait for review or
external CI before handing published work to the operator.

That method is **agnostic** and lives in a static role skeleton. The specifics —
which models fill the coordinator/coder/reviewer roles, which provider they're on
(a cloud API or a local server), the hardware and its limits — are **not** in the
skeleton. They come from a `LaneProfile` (below) and are injected as framing. A
lane may be entirely cloud (Claude, GPT, MiniMax), entirely local, or a mix. This
split is what lets anyone point Courier at their own roster without touching the
role.

### Modes

- **resolve-issue** — "Open a PR to address {{issue}}. Run relevant local
  validation, publish the change, and hand off for external verification. Route
  every forge read and write through the configured forge capability, not a
  forge-specific CLI. Delegate implementation, research, and review to
  sub-agents, but you own completion: integrate and validate their work, then
  run relevant local validation against the integrated changes yourself before
  declaring completion and fix failures before declaring completion. Commit and push the run branch,
  and open or update the pull request yourself — never stop at a local commit or
  branch when a pull request is required. Publish only to the run branch
  {{branch}} and never create or publish work from any other branch."
- **fix-pr** — "Take over PR #{{pr}}. Inspect the current pull request state,
  CI/checks, and existing review feedback to determine what work is needed;
  address feedback already present without repeatedly searching for new reviews.
  Run relevant local validation, publish the change, and hand off for external
  verification. Route every forge read and write through the configured forge
  capability, not a forge-specific CLI. Delegate implementation, research, and
  review to sub-agents, but you own completion: integrate and validate their
  work, then run relevant local validation against the integrated changes
  yourself before declaring completion and fix failures before declaring completion. Commit and
  push the run branch, and open or update the pull request yourself — never stop
  at a local commit or branch when a pull request is required. Publish only to
  the run branch {{branch}} and never create or publish work from any other
  branch."

Goals stay short — a goal plus tools — but each carries one non-negotiable
contract: delegation covers bounded work, never the coordinator's ownership
of completion and forge publication. The publication hint names the run branch
as the single place work may land; it informs rather than constrains (a cheap
nudge, enforced only at exit, below). Each goal also names the exact outcome
file path under executor-owned, per-run scratch outside the target worktree.
The coordinator writes its declaration there; it is control metadata, not a
repository file. This avoids stale declarations without wiping `.courier` or
manipulating the git index. Scratch is disposable, not a checkpoint.

### Terminal states

A coordinator run ends after one of:

- **Locally validated work is published** → the coordinator is done. The
  operator keeps the run in **Verifying** and owns external PR/CI observation;
  subsequent review and merge remain human-gated. The coordinator has no merge
  capability (see [docs/repository-settings.md](./docs/repository-settings.md)).
- **needs-human** → a declared decision or external prerequisite for doing the
  work, or an operator condition such as a crashloop, requires a human.
  Coordinator-declared blocks include a redacted issue/PR comment and a blocked
  source report.

External verification can later reach a separate outcome: stable green advances
**Verifying** to **AwaitingReview**; a red check ends the run as **Failed** and
emits a failed lifecycle result. The source decides how to handle that failure;
queue-backed PR-fix work follows its existing retry policy.

The coordinator declares its ending in the executor-provided outcome file,
whose exact path is in the goal. The executor validates the declaration against
the world: git state remains authoritative for whether declared changes exist
on the run branch; the declaration never substitutes for that check (#169):

- **`changes`** — the coordinator runs relevant local validation against the
  integrated changes itself and fixes failures before declaring completion; then work is
  committed and pushed and the pull request is opened or updated. Verified
  commits reachable from the run branch → exit `0`, **Verifying**. Work committed
  only elsewhere is not run success.
- **`no_change_needed`** — nothing to do, with evidence. The executor posts the
  complete evidence as a redacted issue/PR comment, then exits `3`: a
  resolve-issue run ends in **AwaitingReview** and moves the source to
  `in-review` for a human to settle; a fix-pr run ends **NeedsHuman** and sends
  a blocked lifecycle report. That report parks the PR-fix item as
  `BLOCKED`/needs-human. Its `BlockedReportParksPRFix` flag suppresses only the
  redundant follow-up queue mark; it does not wake a reviewer or request
  another review. Neither mode resolves the source on the coordinator's word.
- **`needs_decision`** — a real decision the coordinator cannot make. The
  executor posts the complete question as a redacted issue/PR comment and exits
  `2` to **NeedsHuman** with a blocked source report.
- **`blocked_external`** — an external prerequisite needed to perform the work
  is missing. The executor posts the complete `missing` explanation as a
  redacted issue/PR comment and exits `2` to **NeedsHuman** with a blocked source
  report. A CI failure discovered after publication is not this outcome: it
  follows the operator's verification and source failure policy.

Comments preserve the full declared evidence, question, or missing explanation
after redaction; they are not shortened to the termination reason. Comment
posting is best-effort: failures are logged as `outcome.comment` and do not change
the run ending. The separate termination reason is bounded and redacted before it
is published. An undeclared ending is not interpreted as a coordinator's human
request. Verified
commits on the run branch may proceed to **Verifying**; otherwise, when a
session was captured, recoverable workspace states resume that session with a
short state message under the #170 continuation budget and no-progress guard.
Without a session to resume, undeclared dirty, off-branch, or no-work endings
fail as incomplete. Crash recovery uses the same budget and backoff described
below.

Feedback or a merge conflict does not reopen the run. It spawns a **fresh
`fix-pr` `CoderRun`** (via the source — dispatch's pr-fix queue, or a
CHANGES_REQUESTED label). Each run is one immutable attempt at one goal; the
history is the sequence of runs linked by the PR and branch. This is why there is
no attempt counter anywhere in the spec.

### Own harness, not headless opencode

The coordinator method and prompts are proven in opencode, but the current
single-pod OpenCode bootstrap is a legacy, explicitly insecure execution path:
model-controlled processes can access credentials available in that pod, and
prompt permissions do not establish isolation. The target is a native resumable
harness with trusted control, a credential-holding policy-enforcing broker, and
an untrusted isolated worker. Its contract and outstanding blockers are detailed
in [HARNESS.md](./HARNESS.md); describing that target does not imply that its
executor or worker isolation is implemented or production-ready.

## Contention and concurrency

Two different problems, two different owners.

**Cross-run** is the operator's job: a `LaneProfile` carries a `concurrency`
limit (default 1 for a local lane). The operator admits at most that many
`Claimed` + `Running` runs on the lane. `Verifying` runs do not consume
LaneProfile execution capacity because their coordinator pods have exited. This
is a plain count, not a slot object. Its real purpose is to guarantee there is
exactly one controller reasoning about a given GPU at a time, so the within-run
throttle below never has to reason about competing controllers.

**Within-run** is the coordinator's job, and it is *informed, not enforced*. The
coordinator's framing tells it the reality ("single card, may queue behind a
shared pool, keep parallelism modest") and gives it a **metrics tool** to look at
current load — the same number you watch on the vLLM dashboard, exposed as a tool
call. It decides how many sub-agents to fan out. The backpressure signal that
matters is the queue/`waiting` gauge, not `running`: a queue forming means the
serving backend is already saturated. (Confirm the exact gauge the target model
server exposes.)

The metrics tool is **optional, per lane** — it is an input, never a
requirement. A cloud lane doesn't need it: capacity is elastic, so the framing
just says "fan out freely." A local lane whose model server doesn't expose
metrics — remote hardware, a runtime with no `/metrics` endpoint — simply runs
without the tool and leans on the framing plus the concurrency limit. Missing
metrics removes one input the coordinator would have had; it never blocks a run.

There is deliberately **no** slot CRD and **no** GitOps of runtime load. GPU
saturation is observed, not declared. An earlier design reached for a
`ContentionDomain` CRD; that was wrong — you cannot declare a limit you can only
discover at runtime.

### The metrics mini-MCP

Nothing off-the-shelf exposes "current vLLM queue depth" as a tool, so Courier
ships a tiny read-only MCP server that scrapes the model endpoint's `/metrics`
and returns the running/waiting numbers as one tool call. Stateless, ~one
endpoint. It is deployed per model endpoint that exposes metrics; an endpoint
that doesn't simply has no tool for that lane, which is fine. It is also the
natural home for any other "look at reality" tool (GPU memory, other pool depth)
added later.

## Liveness and stuck detection

No wall-clock. Courier bounds *stuck*, never *duration*.

The only safe progress signal is **successful model streaming or a completed
tool call** — real activity. Commits are unsafe (opencode-style runs commit
late; a deep sub-agent has none for a long time) and CI state is unsafe for the
same reason. A merely in-flight tool call does not count: counting it, and
"keeping the heartbeat warm" on it, is the superseded design — a wedged silent
tool would then look alive (#102). "Successful" is load-bearing: a model that is
down and being retry-stormed is active but not successful, and should be allowed
to die.

- The target harness writes an **activity heartbeat** to CR status on
  successful stream chunks and verified tool boundaries. Production legacy pods
  currently do not populate heartbeat, checkpoint, or `lastCommit` (#102).
- **Superseded, and now designed:** the earlier design "kept the heartbeat
  warm" while a long tool call (a big test suite) was in flight. An in-flight
  tool is not liveness evidence, and the harness must not emit a heartbeat it
  has not earned — no fake heartbeat. Safe handling of the long silent tool is
  settled by #119 ([HARNESS.md](./HARNESS.md) §6): a harness-owned
  **active-operation set** (op IDs, owning control-pod UID, worker-pod UID,
  diagnostic dispatch time) is persisted *before* dispatch. It suppresses
  stale-heartbeat reaping only while the operator independently observes the
  matching control and worker pods running. The heartbeat carries the
  control-pod UID to fence old activity. There is no duration cap; a genuinely wedged tool (alive, silent) is not detected — the escape is
  manual `needs-human`. This is an intentional safety tradeoff (indefinite
  wedge over false reap), not a liveness proof. #102 stays blocked for
  production until #126 implements it and a production e2e proves the behavior.
- The coordinator **identifies a human decision or external blocker** (missing
  access, ambiguous ask) → `needs-human`. CI failure after publication is an
  operator-observed verification outcome reported to the source as `Failed`.
- A pod that dies (infra) or is reaped is relaunched and resumed. A **crashloop**
  — the relaunch counter *reaching* the ceiling (`restarts >= maxRestarts`) →
  `needs-human` with the counter left at the ceiling, rather than one further
  relaunch. This is the one counter that matters, and it is death-detection, not
  work-retry.
- A pod's **disappearance** — no coordinator pod **object** at all — is
  infrastructure loss on its own terms, detected without any heartbeat,
  because there is no pod left to heartbeat from. A pod whose coordinator exit
  state is observable is not pod loss: the recorded exit goes through the
  exit-code mapping like any other termination. The cache is not trusted for
  the loss judgement, so the absence of a coordinator is confirmed **against
  the API server** with a live read and no recorded state — one confirmation
  rule for the disappearance signal: a coordinator pod found live means the
  cache lagged and nothing is charged, a coordinator pod whose deletion is
  already in flight is neither live nor a confirmed loss and the run is
  re-observed, a failed read is re-observed the same way, and only an API
  server with no coordinator pod for the run charges the bounded relaunch
  with its restart ceiling. A `Running` run with a confirmed loss returns to
  `Claimed` and resumes the **retained** branch, never one recreated from
  base. Pod loss and a reaped wedge share one ceiling and one relaunch path.
  A lost **control** pod is loss even while the worker and broker survive;
  the replacement round is fenced and re-provisioned by the existing launch
  path. An orphan with no pod to watch recovers on the next reconcile — for
  a run with no events at all, the controller's periodic resync; the manager
  configures no cache resync, so that recovery rides controller-runtime's
  default ~10-hour periodic resync.
- An **unobservable dead coordinator** — a coordinator pod in a terminal
  phase with no recorded coordinator termination status — is pod loss on the
  same terms. Kubernetes can transition a pod to **Failed** (eviction, failed
  scheduling) without ever recording container statuses, and no model outcome
  may be inferred from a state Kubernetes never reported. The disappearance
  backstop confirms it with the same live read, and the confirmed dead object
  is deleted so the run's deterministic pod name is free for the replacement —
  except at the restart ceiling, where it survives as the `NeedsHuman`
  hand-off's only record of the cause. Below the ceiling the charge is
  deferred until a live read shows the deleted object gone on **both** delete
  paths — the disappearance backstop and every delete of the wedge's reap —
  so one physical loss is charged once and the replacement can
   never attach to a dying object the launcher would tolerate. A
   stale-heartbeat run whose cache already shows such a pod is reaped by the
   wedge path first, and below the ceiling **every** wedge-path delete is
   re-confirmed the same way (#105): a cached view can lag the API server's
   deletion timestamp, and deleting an already-terminating object succeeds
   without deleting anything, so a no-op delete must not charge a second
   wedge for one physical loss. A reap whose live re-confirmation finds the
   object gone charges in place with the generic message; a deferred charge
   is left to the disappearance backstop, which confirms the loss against
   the API server and charges it with its pod-loss cause. At the ceiling
   there is no charge to defer: the wedge preserves the inert unrecoverable
   object exactly like the backstop and terminalizes in the same reconcile.
- The crashloop counter bounds a **consecutive** streak of wedges, not a lifetime
  total: a run that demonstrates liveness — a fresh heartbeat within the window
  while a recoverable coordinator is observable — resets the streak to zero, so
  a relaunch long in the past cannot terminalize a run that has since proven
  itself alive. The reset suspends while the coordinator is gone or
  unobservable: the last heartbeat is then evidence of the recent past, not of
  current liveness, and a detection must not reset the counter it is about to
  charge.
- A just-relaunched pod gets a **startup grace** so it is not re-reaped on the
  previous incarnation's stale heartbeat before it can send its first one: a pod
  created within the window (or whose coordinator started within it) is never
  reaped. Freshness is judged only on observable pod/run state — the operator
  never writes the harness-owned heartbeat. A pod already terminating is neither
  reapable nor counted, and the wedge-path charge re-confirms every delete
  against the API server, so a delete racing a reconcile cannot double-count a
  single wedge through pod-cache lag (#105); the residual window — a cached run
  status lagging a sibling's already-charged patch — is a charge-proof problem
  reserved for #126's uncached-read rule.

This heartbeat catches a *wedged* run but not a *spinning* one (busy-looping,
streaming happily, converging on nothing). #170 implemented a bounded,
executor-level no-progress guard — the same workspace-state fingerprint and the
same last assistant message twice, or the continuation cap — that terminates a
stuck run as `NeedsHuman` with reason `looping`. A broader conservative
content-based loop-detector — the same tool call, same args, same result, N
times — remains a possible **V2** addition; it is orthogonal to the heartbeat
and must be tuned not to false-kill genuinely slow, varied work.

## Operator-initiated soft-stop (#238)

**Problem.** A Running coordinator has no supported way to receive an operator
message. Suspending a lane only lets the run finish normally; suspension never
implies takeover. The other choice is to let it run to completion. Pod deletion is
the only outside signal that reaches an in-flight coordinator, but it cancels the
session and loses in-flight work — the fix-pr #988 / issue #975 case required
capturing a 105 KB dirty patch by hand.

**One channel.** Human ingress is the `CoderRun` annotation
`courier.misospace.dev/soft-stop`; its non-empty, bounded value is an opaque
request ID. The controller alone translates it into the operator-owned
`status.controlRequest` record `{id, kind: "soft-stop", targetPodUID,
requestedAt}`. `ControlRequest.kind` is CRD-enum-closed to `soft-stop` (admission
refuses any other value), and `id` is bounded (max 128 characters). Trusted control
never reads the annotation or `CoderRun` directly: at a step boundary it pulls
the record from the broker over the authenticated trusted-control path, using the
same run-bound identity, TokenReview, and control-pod-incarnation path as the
trusted status listener. This is the only transport: no exec/attach,
control-pod HTTP server, direct annotation read, or broker push/stream.
Exec/attach races the coordinator and exists only on legacy; a control-pod server
adds a reachable surface and cannot authenticate an external caller; direct
annotation reads lack operator validation and fencing; a broker stream is
machinery a boundary poll does not need. See [HARNESS.md](./HARNESS.md) for the
wire contract.

**Trusted writer identity.** Only the operator writes
`status.controlRequest`; it already owns lifecycle and condition status fields
under #126's split. The broker's trusted status listener accepts only the
harness-owned schema — checkpoint, heartbeat, `lastCommit`, active operations,
and the new acknowledgement — so trusted control, the worker, and a forge
cannot forge an operator request. The model and worker have no path to status,
and a model-authored tool request cannot reach either the request field or the
read route.

**UID and incarnation fencing.** The controller stamps a request only for a
Running run; other phases stamp nothing. It takes `targetPodUID` from the live
coordinator pod it observes, the same UID that fences heartbeats and active
operations (#126). The broker serves the record only when the run UID matches
and `targetPodUID` equals the live control pod UID; otherwise it is absent (404).
A request addressed to a dead incarnation is void, never handed to its
replacement: graceful handoff is not rescue, and crash/liveness recovery remains
the fallback. On relaunch the controller clears `status.controlRequest` and
surfaces `SoftStopObserved`, reason `Unacknowledged`, for a request the named
incarnation never acknowledged; the condition clears on a fresh annotation or
acknowledgement, and the operator drops `status.controlRequest` when the run
reaches a terminal phase. Re-targeting requires a new annotation value.

**Acknowledgement and idempotency.** The annotation value is the opaque request
ID. If `status.controlRequest.id` already equals the current annotation value,
the operator does not re-stamp it. Control keeps a mutex-guarded consumed set
per incarnation. On accepting a matching request it first durably writes the
harness-owned `controlRequestAck {id, at}` through the broker's trusted status
path (resourceVersion CAS, as for other status writes), then adds the ID to the
set and flips the stop latch. A request is never consumed without a durable
acknowledgement, mirroring persist-before-dispatch. Redelivery of a consumed ID
to the same incarnation is a no-op: no second acknowledgement or control turn.
The broker rejects an acknowledgement for an ID it did not serve to the live
incarnation. The operator treats the request as settled on observing the
acknowledgement or terminalization. A crash between read and acknowledgement
drops the request: it is fenced and void, consistent with the crash-fallback
non-goal.

**Checkpoint-safe stop boundary.** A soft-stop never cancels the session or
deletes the pod. On acceptance, the latch blocks new brief dispatches only;
shell, brief cancellation, and forge tools stay available so the model can run
final local validation, commit, and declare `changes`. Every in-flight brief
reaches terminal, integrates, and publishes, preserving live partial work in
each per-brief commit. After the last in-flight brief integrates, the model turn
loop ends, then a dedicated soft-stop finish path reconciles status debt, flushes
active operations, and derives the outcome from the world rather than trusting a
model declaration: `changes` iff the run's work ref holds a tip beyond the
admission anchor, then republish through the broker (whose re-confirm is
idempotent); otherwise `no_change_needed`. A broker publication block promotes
the outcome to `blocked_external`. The latch, not the model, owns this decision
and enforces exit.

The request injects exactly once per consumed request ID as a `system`-role
control turn: a 512-byte-capped, redacted note marked
`[courier-control, control-plane]` that carries only the fixed marker and its
instruction — never the request ID, `targetPodUID`, the run name, or a pod
UID — telling the model to stop new work, let
in-flight briefs finish, publish, and reply with valid outcome JSON. It must not
quote or reference the note in model text, tool arguments, or tool results. The
boundary is checked between model turns and inside the brief-wait loop. SIGTERM
during soft-stop wind-down takes precedence: the harness returns the
stream-cancelled error and the run ends `Failed`; the operator clears the request
on the next reconcile. Legacy SIGTERM pod deletion remains distinct; this
protocol targets the native secure harness, while legacy OpenCode retains only
SIGTERM/relaunch.

**What it cannot do.** The control turn cannot mutate run policy: publication
policy, branch, and merge gate remain operator/forge-owned and set-once. A
soft-stop is not a merge, and the coordinator still cannot merge. The existing
recovery ladder still governs data loss; this feature bounds only a reachable
run's remaining work.

**Decomposition.** The single implementing child is #255, split into file-scoped
slices for API types, `internal/status`, broker status read/ack routes, harness
latch/injection/ack, `cmd/courier-control`, and controller annotation
translation. It remains `status/blocked` until this design lands.

## State and checkpointing

**Git is the durable floor. The checkpoint stores only what the world can't tell
you.** Everything the world already knows is re-read fresh on resume, and the
world wins on conflict.

- **Checkpoint (CR status, kilobytes):** only the plan and ordered completed
  briefs with their compact handoffs (`id`, summary, commit). PR/phase state,
  branch, and last-pushed commit belong to ordinary run status and are recovered
  or verified from the external world where applicable; they are not checkpoint
  fields. Compact handoffs keep this well under etcd's ~1.5 MB object limit.
  The current `Checkpoint` type and trusted status contract are detailed in
  [HARNESS.md](./HARNESS.md).
- **Re-read on resume (the world):** git branch and commits, PR state, CI status,
  source/dispatch state. Never checkpointed; always looked up.
- **Workspace: ephemeral `emptyDir`,** rebuilt from git on every start. Nothing
  durable lives on disk, so there is no workspace PVC to sprawl.

### Uncommitted failure work (#109)

The shipped bootstrap keeps its termination handoff in a separate runtime
`emptyDir` and provides the per-run scratch mount outside the checkout with
narrowly permitted OpenCode access: temp environment and framing, and
narrowly verified permissions for the pinned OpenCode runtime and delegates
(#114, shipped). Scratch is disposable, not a checkpoint.

A terminal `NeedsHuman` or `Failed` run may still have uncommitted edits in its
checkout. Those edits are **not durable** in the workspace itself: when the pod
is removed, the emptyDir and its dirty work disappear. Logs and status are not
a recoverable patch. The evidence mechanism below (#115) makes that state
recoverable within bounded limits; do not push incomplete work, persist raw
diffs in logs or CR status, or treat a fresh retry (#97) as recovery of the old
checkout.

### Failure evidence for dirty runs (#115)

Committed, remote-held work is in the world and is never duplicated.
Unrecoverable state — uncommitted edits, local commits the remote run branch
does not hold — is what evidence preserves, in bounded form. The division of labor
is the design: **the executor captures** (only it has the git context and the
timing), **the operator persists** (the legacy pod is explicitly insecure, so
anything it can write must be narrowly scoped). The pod gains no Kubernetes
identity and no new credential scope: the executor POSTs a bounded bundle to an
operator-hosted intake listener, authenticated by a stateless per-incarnation
token valid only for that incarnation's evidence slot. Retrieval and
application are human actions. This is bootstrap-scoped; the native harness
(#123+) may replace both capture and storage.

The invariants the mechanism must hold:

1. Evidence is operator-owned durable data in the run's namespace, GC'd with
   the run; it never appears in logs, stdout, CR status content, git, or the
   forge.
2. The coordinator pod gains no Kubernetes identity and no credential scope
   beyond a token that can write only its own incarnation's slot, to a
   listener with no read path.
3. Nothing matching the scan is ever persisted, per entry, by one symmetric
   rule at both trust levels; a withheld entry exists only in the manifest,
   never in the stored tar; structural violations are rejected wholesale
   against bounds the intake enforces itself.
4. Unscannable (binary) and over-limit content is never stored; it is named in
   the manifest as present-but-not-preserved, and a manifest alone is never
   claimed to be a recoverable patch.
5. Capture is best-effort and bounded; it never changes an ending's phase,
   exit code, or lifecycle report, and never delays termination beyond its
   20-second operation deadline inside the 45-second grace.
6. Evidence is per-incarnation and slot names are intake-derived; no capture
   — gated, clean, or stolen-token — can destroy another incarnation's
   evidence.
7. Retrieval and application are human actions; no run or retry ever adopts,
   publishes, or implies publication of evidence.
8. Evidence exists only for states the world cannot recover; remote-held work
   is never duplicated, and stored evidence is deleted when the world proves
   the work landed (AwaitingReview, Done).

**Capture moments** (`internal/evidence`, `cmd/courier-executor/main.go`).
Every moment is gated on the verified worktree showing unrecoverable state —
uncommitted changes, local commits the **remote** run branch does not hold
(checked with one bounded fetch-and-compare, since the world is the remote, not
the local ref — a commit pushed nowhere is as unrecoverable as an uncommitted
edit), or a failed world read that leaves the state unknown.
A clean or fully-pushed worktree never captures, from any incarnation: a
relaunched pod starts from a fresh clone, finds nothing unrecoverable, and
cannot overwrite the previous incarnation's evidence with an empty bundle.

1. **Continuation-point inspections.** The #170 loop inspects the world at
   every successful child exit; the crash-continuation branch gains the same
   inspection and capture before either resuming or terminalizing, so a run
   that crashes dirty is not uncovered.
2. **Terminal classification.** `Failed`/`NeedsHuman` endings capture before
   exiting. The classification label itself never decides — the gate's remote
   check does. Endings classified `Verifying` therefore capture nothing when
   the remote run branch holds the commits (the ordinary case), and capture
   when they are confirmed only locally, with the operator deleting that
   evidence at AwaitingReview/Done if the world then confirms the PR.
   Ordering is by construction: the capture and its synchronous persist
   complete before the executor exits, and the operator writes `Verifying`
   only after observing the container exited — so the phase gate can never
   reject a locally-confirmed `Verifying`-classified capture. Untracked
   residue alongside
   remote-confirmed committed work is the known #93
   classification, accepted as lost-with-the-pod.
3. **Cancellation.** On pod deletion (liveness reap/relaunch) the executor
   traps SIGTERM **by cancelling the run context only** — never by capturing
   in a signal-handler goroutine, which `os.Exit` would race. The context
   cancellation kills the model child's whole process group — new behavior the
   executor must add deliberately (`SysProcAttr` with `Setpgid` at child
   start, process-group signal on cancellation): today's
   `exec.CommandContext` kills only the direct child, which would leave
   grandchildren writing into the snapshot. `process.Run`
   returns into the existing crash/terminal code paths, and those call sites —
   the one capture family — check the gate and capture within the termination
   grace period. Git lock contention or read failure degrades to the manifest
   alone. SIGKILL, OOM, and node loss defeat this; the relaunch proceeds
   regardless of the capture's outcome.

Capture runs under a 20-second bounded operation deadline (an operation bound,
not a run wall-clock — the same kind of bound as the 30s MCP preflight); over
deadline the bundle degrades to the manifest alone. The pod sets
`terminationGracePeriodSeconds: 45` when capture is enabled, sized to fit the
deadline plus the child-kill wait plus exit margin. Delivery allows at most two
attempts inside the deadline, then stops — no cross-reconcile retry, no
requeue; loss is recorded in a redacted `evidence.capture` event. A capture the
deadline itself aborts earns one fresh five-second manifest-only delivery
window inside the grace (2026-10-09 decision); a stalled intake does not.
Capture never changes a phase, exit code, or lifecycle report.

**Bundle, bounds, manifest.** One gzip'd tar plus a JSON manifest; the manifest
is always present, content is best-effort within constants (these bound storage
and untrusted input, not model behavior): total uncompressed ≤ 512 KiB, per
file ≤ 64 KiB, ≤ 256 content entries, ≤ 2,000 manifest entries with a 128 KiB
manifest budget. When the entry list does not fit the budget — a worktree full
of untracked build artifacts hits this easily — entries beyond it collapse into
one synthetic `omitted-manifest-budget` entry; `totals` remain authoritative,
so overflow is visible instead of a structural reject. *Text* means no NUL byte
in the first 8 KiB and valid UTF-8.
Tracked modified/staged/deleted and untracked files are included when text,
under the caps, and scanned clean; deletions are recorded as deletions (their
content stays recoverable from git history). Over-limit files and **binary
files** are metadata-only — unscannable content cannot be proven secret-free,
so it is never stored (fail closed); the manifest marks them
present-but-not-preserved, and a manifest alone is never claimed to be a
recoverable patch. Symlinks are never stored as tar members —
the manifest records the path and link target (never followed, escaping
targets flagged), so extraction can never create links. Local-only commits
ride along as `git
format-patch` output (non-binary stubbing; messages scanned like content) under
the same caps. The manifest is a normative cross-component contract: run
identity (name, namespace, run UID, pod UID), workspace identity (base repo,
head repo for fork fix-pr, branch, start SHA, head SHA), capture time, trigger
(`continuation | crash | terminal | cancellation`), per-entry
class/disposition, and totals. The `evidence.capture` event carries totals and
outcome only — never paths or content.

**Secret policy — fail closed, per entry, symmetric.** One rule at both trust
levels, over every string and content the bundle carries (file content, commit
messages, patch text, manifest paths, symlink targets): the scan uses the run's
registered credentials plus the log redactor's defensive pattern table, and a
match **withholds the entry** — and a withheld entry exists only in the
manifest: **the tar contains admitted entries only**. The executor builds the
tar after scanning; when the intake's re-scan finds something the untrusted
executor scan missed, the intake rebuilds the bundle (decompress, drop the
entries, recompute, recompress) rather than persisting the POST verbatim — the
persisted manifest and its totals are intake-authored. A matching
metadata-only string (a path or symlink target that itself matches) is
substituted with `[REDACTED]` in the manifest, since metadata is not evidence
and substitution corrupts nothing. Content is never redacted in place:
substitution would corrupt the evidence and imply a sanitization guarantee the
pattern table cannot make. Structural violations are the whole-request
rejects: body or decompressed stream over the caps, entry count over the cap,
absolute or `..` tar entry paths (tar-slip), truncated or unparsable
tar/manifest, or manifest totals disagreeing with the entries under the
overflow rule above.

The intake's credential set comes from the pod builder, not from hope: kubelet,
not the operator, resolves the run's credential env, so one shared pod-builder
function produces the `(env name, SecretKeyRef)` pairs for the per-key
credential refs — the single source of truth for what the executor receives
and what the intake re-scans against. The executor environment Secret arrives
by a different route — `envFrom`, which maps each Secret key 1:1 to an env
name the builder never enumerates — so the intake adds that Secret's keys
directly, under the same shape rule. The
intake resolves those refs at persist time and registers the values whose env
names are secret-shaped (the shape rule tests env names, never Secret keys, so
a custom `--git-token-key` is not silently skipped; over-registering a
harmless value only costs a substitution). The git username is not
secret-shaped and is unregistered on both sides. The **presented bearer
token itself is registered as `COURIER_EVIDENCE_TOKEN`** so a sender that
smuggles its own valid capture credential into the bundle has it withheld
the same way any other credential would: the per-incarnation capability
that authenticated the POST cannot be persisted in the body it
authenticated. A referenced Secret that
the builder actually injects and that is missing or unreadable at scan time fails
the persist (what cannot be verified is not stored). Residual: credentials
delivered outside those Secrets (image-baked env, tokens embedded in MCP URLs)
are invisible to the intake and covered only by the executor's own untrusted
scan; a credential Secret deleted mid-run makes every persist fail while the
executor keeps working with its cached env.

The re-scan covers every string the bundle carries. Per-entry `Path`,
`StoredPath`, and `LinkTarget` are redacted in place — a matching
storedPath drops the tar member, a matching Path or LinkTarget becomes
`[REDACTED]`, and the totals move in lockstep. Manifest metadata that
the executor controls but the persisted manifest must preserve
(`Workspace.*`, `Trigger`, `Run.PodUID`, `Entry.CommitSHA`) is checked
as a whole-request reject: redacting a branch or a SHA would mutate the
recorded repository state and imply a sanitization guarantee the
artifact cannot keep, so a credential in any of those fields fails the
POST with no Secret written.

**Transport** (`cmd/main.go`). The operator process serves a small write-only
intake listener (own bind address, empty disables; chart renders a ClusterIP
Service, optionally ingress-restricted to coordinator pods). HTTP with a bearer
token follows the in-cluster transport convention settled by #120's
dependency-cache design: plain HTTP, network-boundary + token authentication,
no CA in clients. The executor's delivery request shape is settled by #198: a
POST of `multipart/form-data` carrying a file part named `manifest` (the
canonical manifest JSON) and, only when the capture admitted content, a file
part named `archive` (the gzip'd tar); the intake dispatches on part name,
never on the parts' stamped Content-Type. Validation is stateless and the persist idempotent, and the
listener runnable declares `NeedLeaderElection() → false`, so with leader
election every replica serves — without that, standbys would refuse POSTs the
Service load-balances onto them and exhaust the executor's two-attempt budget.
The per-run, per-incarnation token is `base64(nonce ‖ HMAC-SHA256(key,
namespace/name/run-UID/hex(nonce)))` (here `hex` is lowercase hex of the nonce;
the token carries the raw nonce bytes ahead of the MAC, and the Secret slot name
comes from the first 8 hex characters): the launcher generates the nonce at pod
build (the pod UID is not assigned yet, so it cannot be the binding), injects token
and nonce as env (the token's name ends in `TOKEN`, so the executor's
name-shape redactor registration covers it), and the intake validates with
`hmac.Equal` after recomputing the HMAC from an **uncached** live read of the
run (the same uncached-read discipline HARNESS.md §6 applies to destructive
liveness decisions, adopted here because a persist is hard to take back). The
**CoderRun UID is not in the sender's reach** — the downward API cannot
expose it — so the executor sets `manifest.Run.RunUID = ""` and the
bearer HMAC is the run's identity. The intake accepts the empty sender
UID, stamps the live run's UID into the intake-authored persisted
manifest, and rejects a non-empty sender UID that does not match the
live run as a forged identity. The
phase gate is
an allowlist over **non-resolved versus resolved**: the intake accepts
`Pending`, `Claimed`, `Running`, `Failed`, and `NeedsHuman`, and rejects
`Verifying`, `AwaitingReview`, and `Done`. The admitted set is broad because
the operator moves runs around a dying pod: an ordinary liveness reap deletes
the pod and writes `Claimed` in the same reconcile (and a failed relaunch can
reach `Pending`) while the previous pod's SIGTERM grace capture is still in
flight, and a crashloop-ceiling reap writes `NeedsHuman` the same way —
betting on read ordering would defeat the grace capture, which is the only
coverage for a wedged pod's work. The rejected phases are resolved ones:
`AwaitingReview` and `Done` are world-proven, and a `Verifying` pod has
exited with its capture window closed. A recreated same-name run's new UID
rejects old
tokens; a deleted run fails the live read. A stolen token can write only its
own incarnation's slot — strictly narrower than the git push credential the
legacy pod already holds. Persist is synchronous: a 2xx response means the
Secret write has landed.

The intake re-reads the run uncached immediately before the persist: a
run that moved to `AwaitingReview` or `Done` (or was deleted) since the
initial authentication read has its deletion pending, and a Secret
written now would briefly resurrect evidence the operator has already
proven landed. A phase change or a UID change in that window fails the
persist; the world is the source of truth at the moment of write.

**Storage** (intake persist path). The **intake derives the slot identity from
the validated token** — the nonce in the token names the incarnation's Secret,
so a compromised executor cannot address another incarnation's slot by
guessing its name. The Secret is `courier-evidence-<run>-<nonce8>` (name
truncated and hash-suffixed exactly like `podName` when long), labeled
`courier.misospace.dev/evidence: <run>`, owner-referenced to the CoderRun. The
manifest's real pod UID (downward API) is carried for human correlation, and a
manifest whose run name/namespace disagrees with the validated token is
rejected; the run UID is stamped from the live read because the sender
cannot supply it. Per-incarnation keying is a correctness decision:
cross-incarnation overwrites are structurally impossible — a relaunched clean
clone captures nothing (the gate), and even a stolen token cannot address a
previous incarnation's slot — the delete-recreate GC-lag race disappears, and
each Secret stays well under the 1 MiB API limit; the bound is `maxRestarts +
1` Secrets per run. Retention: the owner reference
deletes evidence with the run (Done reaping, operator deletion, #97
delete-as-retry) — no sweeper, no separate clock, zero standing footprint; the
operator additionally deletes a run's evidence Secrets when the run reaches
**AwaitingReview or Done** — the states where the operator's own world
observation has proven the work landed (Invariant 8). A `Verifying` run that
falls to `NeedsHuman` keeps its evidence: its world proof never arrived.
The pre-persist uncached re-read covers the brief window between
authentication and write: a run that moved to `AwaitingReview` or `Done`
in that window has its evidence already cleaned, and the re-read refuses
to write a Secret for a phase the operator has world-proven as resolved.
Access control is namespace RBAC: the operator gains
`create/get/list/patch/delete` on `secrets` through a namespaced
Role/RoleBinding rendered by the chart — deliberately not the generated
operator ClusterRole, whose ClusterRoleBinding would widen the grant
cluster-wide (`list` is required by the
condition derivation and the AwaitingReview/Done cleanup; both use uncached
API-reader lists) — see
Security and boundaries. Encryption at rest is the deployment's cluster
configuration; Courier recommends enabling it. Status carries one condition,
`EvidenceCaptured`, derived by the reconcile loop from the world through an
**uncached** list (terminal runs rarely reconcile again, so a cached informer
read could race its own lag and leave the condition absent forever): any
evidence Secret → `True/Captured`; no Secret → no condition. The condition is
informational — a grace capture can land after the terminal reconcile, so
absence is never proof of absence, and retrieval is always by label, never
condition-driven. The conditions array is operator-owned and patched
wholesale, so the intake handler never writes it. No condition ever changes a
phase, exit, or lifecycle report.

**Recovery.** The operator lists a run's evidence Secrets by label, reads the
manifest, extracts to a scratch directory outside any checkout with
traversal-safe tooling, reviews the content, and applies chosen files onto a
branch of a fresh checkout through normal human review. Evidence is
model-authored untrusted data: read it before applying it, never execute it,
never let a tool apply it unattended. Retrieve before deleting the run —
deletion is the moment evidence dies with it. No run, retry, or automation
ever adopts, publishes, or implies publication of evidence (#97: a retry is a
fresh attempt, never a restoration of the old checkout; old run history stays
distinct from new attempts).

**Failure and race semantics.** A graceful `Failed`/`NeedsHuman` ending with
unrecoverable state captures and persists before exit. A crash stores the last
inspection's bundle; work after it is lost with the pod. A wedged pod's
SIGTERM capture races nothing (the child's process group is terminated first)
and never delays the relaunch. A relaunched clean worktree captures nothing. A
crashloop-ceiling reap writes `NeedsHuman` before the grace capture runs, and
the intake's phase allowlist admits it — that capture is the only coverage for
a wedged pod's work. OOM, SIGKILL, and node
loss capture nothing — the standing residual. Intake unreachability,
deadline misses, and structural rejects are bounded, evented, and never alter
the ending; executor-side per-entry withholds persist the rest of the bundle;
intake-side withholds do the same at persist (the intake rebuilds the bundle).
The newest capture per incarnation is stored; superseded bundles are replaced,
and a failed replacement leaves the older bundle in place. An operator restart
may lose one
in-flight POST; the bounded retry may cover it, otherwise the loss is
documented. A run deleted mid-POST rejects the POST harmlessly. Same-name
recreation gets new UIDs and fresh nonces while the old Secret is GC'd
asynchronously — harmless because slot names are nonce-derived and unique. A
run reaching
AwaitingReview or Done after a continuation capture has its evidence deleted;
the world holds the work, and a `Verifying` run that falls to `NeedsHuman`
keeps it (a capture validated while `Running` but persisted after that
deletion can briefly resurrect a Secret — stated race, bounded by the run's
own GC).

**Rejected alternatives.** Raw diffs in logs/stdout: redaction is not content
safety and logs hold partial fragments — precisely what failed in #109.
Patches in CR status or the termination message: kilobyte bounds, etcd
pressure, secrets-in-status, forbidden on its face. Automatic push to the
forge (quarantine refs, an evidence repo): world-visible evidence, hard to
expire (git GC, forks, caches), unsafe by default, a violation of the broker
publication policy's pinned destinations and of world-truth (#134: off-branch
refs are the confusion to avoid) — and in legacy mode the model holds the push
credential and could spoof evidence. Per-run PVC: the storage sprawl this
design rejected for checkpoints, provisioner-dependent, heavier GC, no
security gain. Executor-written Secrets: requires API identity in a
model-controlled pod — a strict regression against removing identity from
coordinator pods, and a model-controlled Secret-creation channel is an
exfiltration primitive. Operator-side pull (exec/`kubectl cp`): impossible for
the graceful ending — the #109 class — where the container has exited and no
exec path exists; a standing `pods/exec` grant buys nothing but surface.
Transcript/session recovery: OpenCode sessions are in-pod conversational
state, not worktree evidence. Rejecting preservation entirely remains the
named fallback for deployments that cannot accept the Secret-authority
expansion; #109 shows the graceful-dirty class is real and recurring, so the
mechanism above is the default.

**Residual risks.** Hard kills and node loss lose work since the last
inspection. Novel pattern-unmatched secrets can pass both scans, as can
credentials split across fields or base64/URL-encoded in content, and weak or
short registered values inflate the `withheld` counts (deployment hygiene is
the mitigation) — the same residual family as log redaction, with higher
stakes because storage persists. In legacy
mode the model can read the evidence token and nonce and tamper with its own
worktree before capture; the token's blast radius is its own incarnation's
slot, evidence was never tamper-proof, and integrity rests on human review.
Evidence content is
stored inert and applied only by reviewed human action. Untracked residue
alongside remote-confirmed committed work (the #93 class) is what a
`Verifying` ending leaves uncaptured — the remote check, not the label, draws
that line — and it dies with the pod.

**Decomposition.** Implementation is split into bounded, dependency-ordered,
file-scoped issues, none scheduled `status/ready` until this design lands:
capture core (#197), executor triggers (#198), pod wiring (#199), intake +
persistence + RBAC (#200), operator runbook (#201), and an envtest end-to-end
(#202). The issue's required test classes
map as: secret-bearing diffs → capture core, intake, envtest;
oversized/binary/untracked → capture core, executor triggers; abrupt pod loss →
executor triggers, envtest; duplicate capture → executor triggers;
cleanup/expiry → intake, envtest; retrieval/application → runbook, envtest.

### Toolchain reference for bootstrap lanes (#153)

Repository toolchains (Go today) keep their module and build caches on
writable storage, but the bootstrap contract grants OpenCode no broad home or
`/tmp` access. The coordinator pod therefore mounts one per-run `emptyDir`
`toolchain-cache` volume **read-write at `/courier-toolchain-cache`** — where
the runtime image pins its toolchain caches (`GOMODCACHE`, `GOCACHE`) — and
**read-only at `/courier-toolchain`**. OpenCode's `external_directory`
permission allows reads of the scratch mount plus the read-only reference and
denies edits there; because the reference mount is genuinely read-only, a
write also fails at the filesystem level. The read-write twin is written only
by lane-toolchain child processes (e.g. `go mod download`); the model's own
tool calls that target it stay gated as for any external path, and
unparsed-bash reachability matches the pre-change baseline (no new hole).
This is bootstrap ergonomics for the pinned runtime, distinct from the #136
secure dependency cache.

#### Dispatch binding identity

Dispatch discovery bindings all persist `spec.source: dispatch`; the optional,
immutable `spec.sourceAgent` identifies the Dispatch agent used for lifecycle
calls. The operator registers qualified adapters as `dispatch:<agentName>` and
resolves non-empty identities only through that exact key (never falling back
to the bare adapter). The bare `dispatch` adapter is retained for old runs
whose `sourceAgent` is empty. Keeping source stable means deterministic run
names and Kubernetes `Create` remain the atomic dedupe boundary across runners
and retained runs; Dispatch lease observations are not atomic and do not prevent
duplicate materialization. At rollout, runners also recognize prior
`dispatch:<queueLane>` runs so retained work is not recreated under the new
identity. The CRD must be installed before an operator that writes
`sourceAgent` is rolled out.

## Commit cadence

Commit at **completed-brief boundaries** — not mid-thought (a foot-gun for a long
sub-agent), not only at the very end (loses everything on a mid-run death). A
finished brief is a coherent unit worth committing, and its commit turns git into
a real resume point. Commit messages carry the brief's objective and outcome —
they double as the handoff record when the checkpoint is gone (see Tier 2).

### Recovery ladder

Degrades gracefully; never loses committed work.

1. **Pod restart, checkpoint intact** (CR status): reload plan + handoffs,
   re-clone, fetch the branch to the last commit, continue. Minimal
   re-reasoning.
2. **Checkpoint gone, remote branch exists, no PR:** a fresh run derives the
   branch name from the issue, fetches it, reads the commit log (each per-brief
   commit is a completed unit with a descriptive message), and re-plans the
   remainder. Loses the plan scaffolding, loses **zero committed work**.
3. **Nothing exists:** fresh start.

Per-brief commits are what make Tier 2 possible; end-only commits would drop a
dead 12-hour run straight to Tier 3.

### Two disciplines and a landmine

- **Deterministic branch names**, a pure function of repo + issue (+ mode), so any
  run finds its own branch and a new run finds an orphaned one. **Never a fallback
  number** — a lost issue number defaulting to 0 is how tasks collide on a shared
  branch.
- **Substantive commit messages** — the commit log is the Tier-2 handoff record.
- **Base-sync on adoption (landmine):** when a run adopts an existing branch
  (Tier 2, or any fix-pr takeover), main may have moved. Sync to base *first*, or
  the eventual PR reverts what landed on main in the meantime. Adoption's first
  step is always sync-to-base. A sync that stops on conflicts is handed to the
  coordinator mid-merge, with the base and conflicted paths in its goal; the run
  cannot leave a success phase until the base is merged into HEAD.

Scope: **resolve-issue** adopts an orphaned branch only when a branch exists but
no pull request points at it. When the deterministic branch already carries an
**open** PR, the run refuses to adopt it and instead reports that PR to the
source for review — a live PR means the work is in flight, not that a human is
needed. A branch whose only PRs are closed or merged still refuses adoption and
hands to a human. **fix-pr** always adopts the existing PR's head repository and
branch (fork-aware), keeping the base repo as the PR/publication target.

## Observability and transcripts

Courier does not natively persist transcripts. It logs **structured event lines
to stdout** — one JSON line per event (model call, tool call, tool result,
sub-agent brief, brief completion), each labeled with `run_id`, issue, repo, and
brief id — and lets Loki/VictoriaLogs pick them up. Retention is the log stack's
job.

- **The debug flag is a per-run log level** (a field on the `CoderRun`, setting
  the pod's log-level env), not a global switch. Crank verbosity on one
  suspicious run; leave the rest quiet.
- **Redact before stdout.** Verbose tool I/O will otherwise dump the GitHub token
  and litellm keys into the log store and retain them.
- **Live-follow for free:** a Grafana Explore query keyed by `run_id` *is* the
  run-watching UI. `kubectl logs -f` for the terminal.
- The same activity events inform the liveness heartbeat, but the heartbeat is an
  explicit CR-status write, **not** the operator parsing logs.

The operator also exports run metrics on the controller-runtime `:8080`
endpoint. Run durations are histograms
(`courier_coderun_run_duration_seconds`, `courier_coderun_queue_wait_seconds`)
and total runs a counter (`courier_coderuns_total`); all three carry only
`lane`, `mode`, and terminal-phase labels, never `repo` or `issue`, so
cardinality stays bounded. `courier_coderuns_in_flight` and
`courier_lane_suspended` are gauges read live from the cluster at scrape time
rather than pushed counters, so they survive an operator restart and always match
the world. `courier_metrics_scrape_errors_total` counts scrape failures. Scraping
is opt-in: the chart's `serviceMonitor.enabled` renders a metrics `Service` and a
Prometheus Operator `ServiceMonitor`, off by default.

### Per-run telemetry (#172)

The executor already relays the coordinator's stdout through a pass-through tap,
so it tallies the OpenCode JSON event stream as it passes and reports it three
ways on a finished run — a `run.summary` event logged next to the
`COURIER_TERMINATION` handoff, a compact `status.telemetry` on the `CoderRun`,
and the source (Dispatch) lifecycle report body. Tallied per session and for the
run: model calls (step-finish), tool calls by name, tokens (input, output,
reasoning, cache read/write, total), peak context (`input + cache.read +
cache.write`), wall time from tool timings, and `task`-call subagents tallied by
(agent, model) with calls, errors, and runtime. Subagent sessions do not stream
their own steps to the coordinator's stdout, so subagent **tokens** are not
derivable here — only their calls/errors/runtime; run token totals come from the
coordinator's own steps. The compact status/report form is integers and small (a
bounded per-agent list): it rides the pod termination-message budget alongside
the reason, and durations are milliseconds so the CRD stays float-free. Only the
rich form reaches the (uncapped) log. Continuations are counted from the
executor's own counter; metrics export lands with #168.

## Custom resources

### `CoderRun`

One per work item. Immutable spec, self-reaping (the checkpoint lives in its own
status and dies with the CR).

```yaml
spec:                       # set once by the source adapter, then immutable
  mode: resolve-issue | fix-pr
  source: dispatch | github-label | cron | cli | web
  sourceAgent: <optional stable identity for a qualified source binding>
  workItemID: <opaque ID understood by the source adapter>
  repo: owner/name
  ref: <issue# or pr#>
  lane: local | cloud | ...        # names a LaneProfile
  debug: false                     # per-run: bumps pod log level, nothing else
  # no attempts field — by design
status:
  phase: Pending | Claimed | Running | Verifying | AwaitingReview | NeedsHuman | Done | Failed
  branch: <derived resolve branch or adopted PR head>
  headRepo: <PR head repo; spec.repo for a same-repo PR, the fork's owner/name for a fork PR>
  headSHA: <head commit SHA>
  pr: <#/url>
  admittedAt: <ts>                 # set-once: left Pending past the lane gate
  startedAt: <ts>                  # set-once: observed from the coordinator container status
  finishedAt: <ts>                 # set-once: the run's own execution ended (any terminal phase)
  waitDuration: <Go duration>      # derived once: creation to start, the Waited column
  runDuration: <Go duration>       # derived once: start to finish, the Ran column
  lastCommit: <sha>
  checkpoint:
    plan: <...>
    completedBriefs: [{id, summary, commit}]
  heartbeat: {at: <ts>, kind: stream | tool}
  restarts: <n>                    # consecutive infra crashloop counter, reset by a fresh heartbeat while a coordinator is observable
  conditions: [...]
```

### `LaneProfile`

The reusable, agnostic unit. Where "concurrency 1, tunable" lives, and the seam
that makes Courier portable.

```yaml
spec:
  concurrency: 1                   # max Claimed + Running CoderRuns admitted on this lane
  roles:
    coordinator: <model>
    coder: <model>
    reviewer: <model>
    # ...
  framing: |                       # free-text lane context injected into the prompt
    single 3090, may queue behind a shared pool, keep parallelism modest,
    use the load tool before you fan out
  runtimeImage: <optional image>   # repository-specific coordinator/toolchain image
```

`runtimeImage` is an optional execution-environment selector. It is an image
capability seam, not a Go or Kubernetes requirement: a lane can select a
repository-specific image containing whatever tools its workflow needs, while
the deployment default remains the minimal bootstrap image.

Roles name whatever models the operator has configured — cloud (`anthropic/...`,
`openai/...`, `litellm-anthropic/MiniMax-M3`), local (`litellm/qwen3.8-27b`), or
a mix. A cloud lane's `framing` reads more like "capacity is elastic, fan out
freely" and it sets `concurrency` high; a local lane's reads "single card, keep
it modest" with `concurrency: 1`. Same schema, no local assumption baked in.

The manager can create and update deployment-managed LaneProfiles itself:
`internal/bootstrap` reconciles profiles from a YAML config file given by
`--bootstrap-lane-profiles-file`, labeling what it owns with
`courier.misospace.dev/bootstrap-managed: "true"`. This lets a GitOps installer
supply lanes without applying LaneProfile CRs beside the Helm release, where the
CRD may not be established when the CR is rendered. A same-name profile without
the label is never adopted or overwritten — the name collision surfaces as an
actionable error. Removal from the config intentionally leaves the CR in place;
there is no delete verb because active runs reference lanes. (#73)

## Reconcile loop

- **Pending** — created by a source. The operator checks the lane's `concurrency`
  against admitted (`Claimed` + `Running`) runs on that lane. Over capacity → stays Pending (this is the
  concurrency limit; a count, no slot object). Under → proceed.
- **Claimed** — the source adapter claims the work (dispatch: claim +
  status=in-progress; label/cron/cli: no-op or equivalent). Branch name derived.
  If the pod then fails to launch, **release the claim** — never strand work as
  in-progress.
- **Running** — pod launches: ephemeral workspace, clone, **adopt the branch if it
  exists and base-sync first**, inject LaneProfile framing + roles, wire the MCP
  tools, set log level from `debug`. For a fork PR the workspace's `origin` is
  the fork and base-sync fetches a second fetch-only `upstream` remote pointing
  at the base repository, so the merge-before-work invariant always merges the
  real base rather than the fork's possibly stale base branch. The target
  harness commits per brief and
  writes heartbeat and checkpoint to status; the legacy bootstrap does not
  populate these fields (#102). The executor classifies its declaration against
  the world before the run terminalizes: exit `0` (`changes`) reaches
  **Verifying** only when the coordinator validates integrated work locally,
  fixes failures, and committed work is reachable from the run branch; work
  committed elsewhere is not success.
  Exit `2` (`needs_decision` or `blocked_external`) reaches **NeedsHuman**;
  exit `3` (`no_change_needed`)
  reaches **AwaitingReview** for resolve-issue and **NeedsHuman** for fix-pr.
  Other failure exits reach **Failed**. When no outcome is declared, recoverable
  endings — uncommitted changes, commits off the run branch, or no commit and no
  workspace changes — resume the same session with a short state message, up to
  `COURIER_MAX_CONTINUATIONS` times (default 3). Invalid, zero, or negative
  values use the default; any positive value, including 1, is accepted. A
  no-progress guard terminates earlier as **NeedsHuman** with reason `looping`
  and the state history; reaching the continuation cap also terminates as
  **NeedsHuman** with reason `looping after N continuations` plus the state
  history. Without a captured session,
  undeclared endings fail as incomplete. A crash resumes the session
  with exponential backoff (default 5s, `COURIER_RESUME_BACKOFF_SECONDS`),
  sharing the same budget, before the run transitions to **Failed**; a crash
  before any session ID is observed transitions to **Failed** immediately. A
  child exit code alone is not a declared outcome. A pod death or heartbeat
  stall relaunches/resumes it; a crashloop reaches NeedsHuman.
- **Verifying** — no coordinator pod or liveness meaning. The operator polls
  the external PR and CI world indefinitely, with a reconciliation cadence and
  no deadline. Observer errors remain Verifying and requeue. A missing observer
  or missing/draft PR reaches **NeedsHuman**. A failed check on a fix-pr run
  reaches **Failed** so its source can apply its retry policy. Resolve-issue
  runs remain **Verifying** on red checks so the original issue can still reach
  review after a follow-up repairs the PR. Queue-backed PR-fix sources issue
  another attempt under their existing cap. A PR with no checks or pending
  checks remains **Verifying**; the PR is persisted. Green is declared
  only from **two consecutive all-green observations of the same check set**:
  every all-green observation records a compact fingerprint of the check
  identities (head commit plus sorted check names) on the run status, and an
  observation may transition to **AwaitingReview** only when it matches the
  previous all-green observation's fingerprint. Any pending or empty
  observation clears that recorded candidate — checks that have not registered
  yet can still appear at any later poll, so a pending observation can never
  pre-settle an identity — and a changed set (a new check, a new push) resets
  it the same way. A partial snapshot cannot pass. The source becomes
  `in-review` with the transition. Verifying does not consume LaneProfile
  execution capacity; the source remains `in-progress` while polling, publishes
  `in-review` after stable green observations. Fix-pr attempts receive `failed`
  if checks turn red; initial issue observers remain active without lane capacity.
- **AwaitingReview** is terminal for this run. For a PR, human merges → operator
  marks **Done** and resolves the source; feedback/conflict → the source spawns a
  fresh `fix-pr` run without reusing the previous run. A `no_change_needed`
  resolve-issue run also waits here with its evidence posted for human review;
  Courier does not resolve that source from the coordinator's declaration. The
  previous run remains auditable; its completion does not settle later feedback.
- **Reap:** Done runs are deleted (checkpoint dies with the CR). NeedsHuman runs
  are kept for inspection and deleted on request. Zero standing footprint between
  runs — a strict improvement over Foreman's ownerRef-less audit ConfigMaps,
  which require an external sweeper.

**Terminalization writes the run's own phase before it reports to the source.**
A rejected or failed source report never holds a run in `Running`: the phase is
written first, which releases lane capacity, and only then is the lifecycle
report published — on its own, retried via a bounded requeue and deduplicated by
the run's stable idempotency key. A Dispatch generation-mismatch conflict means
this attempt was superseded by a newer generation of the same work, so the report
is dropped rather than retried (the newer generation belongs to another run) and
the run still terminalizes; a transient report failure leaves the run in its
terminal phase with the report retried on a later reconcile. (#178)

## Sources

Pluggable adapters over a generic interface. An adapter both *creates* runs and
*transitions* work-state back.

- **dispatch** (first-party, native — no bridge). The operator *is* the dispatch
  client: claim, in-progress, in-review, needs-human, and watching the pr-fix
  queue to materialize `fix-pr` runs. This replaces the foreman-dispatch-bridge
  entirely.
- **github-label / gitlab / forgejo / tangled** — a labeled issue becomes a
  resolve-issue run; CHANGES_REQUESTED becomes a fix-pr run.
- **cron** — scheduled work (the weekly audit, backlog grooming). This is what
  makes Courier the successor to the pre-Foreman cronjobs: scheduled and
  queue-driven work collapse into one mechanism.
- **cli / web** — ad-hoc injection: "run this issue that isn't in dispatch."

Dispatch is an adapter, not a dependency. Dispatch is a separate product; Courier
(a coding executor) must never make dispatch features depend on it.

### Lane bindings (#173)

One deployment binds one or more Dispatch queue lanes to Courier LaneProfiles;
each binding runs its own discovery runner polling `next-task` with its lane.
Discovery is the only lane-scoped call — claim, status, and reports are
addressed by issue identity, so bindings share them. Two bindings never admit
the same work item because CoderRun dedupe keys on source plus canonical work
identity, not lane. The run name hashes that same identity so concurrent creates
remain atomic even when incidental metadata makes the opaque IDs differ; the
winning run still stores its original `WorkItemID` for lifecycle calls. Suspending
or capacitating one LaneProfile affects only that profile; several bindings may
share a LaneProfile, and then they share its suspend gate and capacity. Before
Dispatch discovery, each runner counts empty-phase, Pending, Claimed, and Running
CoderRuns on its profile and pauses discovery when those reservations meet
`spec.concurrency`. This is deliberately more conservative than operator admission,
which counts only Claimed and Running: Pending and empty-phase runs represent
already-materialized but not-yet-admitted work, and reserving them prevents a
binding from hoarding more Dispatch items while it waits. Do not extend this
predicate to operator admission: a Pending run at concurrency one would count
itself and block its own admission. Verifying and later phases do not reserve
execution capacity. The gate uses one pre-discovery list snapshot, not an atomic
capacity lease, and checks only once per poll; stale reads or a source returning a
batch may still materialize work beyond available slots. It also cannot make
Dispatch hide an already-discovered Pending head from a free sibling profile;
that needs Dispatch-side queue exclusion. While discovery is paused, Dispatch
`next-task` and stale-item bookkeeping are deferred until a later poll.

### Dispatch follow-up attempts (#98)

Dispatch owns the actionable attempt, not Courier's runner. A queue-backed
`followup-pr` carries `prFixItem.{id,generation}`; the row ID identifies the PR
queue item and its persisted integer generation identifies one dispatchable
attempt. Additional evidence while that attempt is QUEUED belongs to the same
attempt; a transition from a settled item back to QUEUED creates a new one.
Courier's opaque `WorkItemID` includes both fields, so a retained
`AwaitingReview` CoderRun deduplicates an exact repeat but cannot suppress a
new generation. The CoderRun and its lifecycle report remain records of the
*old* attempt, even if Dispatch observes newer feedback before the report
arrives. Report retries use the run's stable idempotency key; a repeated report
cannot apply settlement twice.

The Dispatch settlement contract to implement is: bind settlement to the issued
item ID and generation, and compare against the head SHA observed **at attempt
start**, not the mutable head SHA on
the queue row at report time. If the head did not advance, do not claim a fix;
if new feedback or a new generation arrived, do not overwrite it. Status and
history should agree in one conditional transition; a late report may be
audited as stale but cannot settle or requeue newer work. The PR-fix queue,
report handler, and head guard are Dispatch-owned. Courier's Dispatch adapter
only transports the token and maps the outcome. A successful `fix-pr` report
must not mark the linked issue done: only a verified merge can resolve it.
Linked-issue associations from the queue must be checked before any issue
mutation, never used as authority on their own.

A linked-PR follow-up served from a recomputed issue-health flag has *no*
`prFixItem` identity. Courier cannot infer a durable generation from PR head,
reason strings, issue ID, or wall-clock time: unchanged-head new reviews and
stale health snapshots defeat those heuristics. Until Dispatch gives this path
persistent attempt identity and settlement semantics, Courier must not fabricate
one. That separate Dispatch change is tracked by misospace/dispatch#1045; an
adapter fallback/dedup change is conditional on its wire contract, not a generic
core retry or a metadata-hash substitute.

## MCP surface

The legacy coordinator pod currently receives forge access through MCP, alongside
model-controlled tools and credentials. MCP is a tool interface, not a trust or
security boundary; this path is explicitly insecure. In the target architecture,
model-influenced forge requests pass through trusted control code and a trusted
broker that enforce semantic forge, repository, and ref policy. MCP may be used
as a provider interface only where those same typed semantics are enforced. The
MCP/provider contract and blockers are detailed in [HARNESS.md](./HARNESS.md).

Other possible tools include **context7** for library documentation and the
optional **metrics mini-MCP** for current model load; neither grants forge
authority.

Availability is preflighted: before the goal runs, the bootstrap makes one
bounded check of the configured servers using the run's own config and
environment. A server that is configured but unreachable is named to the
coordinator — in its framing, in a `capability.status` event, and in the
no-work terminal reason — so the model knows the tool is absent instead of
hunting for it. A failed optional capability never fails or gates the run; it
only informs.

## Security and boundaries

- **Current legacy mode is insecure:** the coordinator pod includes model-
  controlled processes and forge credentials, and its raw push credential may
  write any ref the credential permits. MCP restrictions, process conventions,
  and prompt-level merge denial do not contain a compromised or misbehaving
  process. The existing deployment must rely on credential scope and protected
  default-branch configuration as external mitigations, not as a semantic ref
  boundary.
- **Target boundary (designed; implemented for the isolation slice as opt-in
  deployment-level secure mode — the legacy coordinator remains the default
  and is explicitly insecure, and the model-facing harness is #124+):** trusted harness control makes
  model calls and validates model-influenced operations; one trusted broker per
  run holds forge/git credentials, enforces resolved repository/ref policy, and
  writes only that run's harness-owned status. Control authenticates with a
  projected, pod-bound 600-second `courier-broker` audience token; the broker
  TokenReviews every request and checks the authenticated service-account UID
  and current control-pod incarnation. Runs live in a namespace dedicated to
  that purpose where no subject other than the operator can create, patch, or
  delete pods: a clean-spec pod copy in the run namespace could otherwise
  present itself to the broker as control, because no namespace-resident
  material distinguishes it. Control-to-worker tasks use signed
  run/incarnation-bound envelopes; the worker has no secret, service-account
  token, or signing key. Worker network access is limited to the dedicated Go
  module/checksum cache and no DNS, never broker or forge. Enforced
  network policy, live deny probes, and secure preflight fail closed before
  workload exposure; the preflight separates static admission/spec checks from
  live probes, and neither proves a privileged cluster actor cannot create or
  mutate pods outside those controls — a stated residual risk, not a closed
  hole. [HARNESS.md](./HARNESS.md) specifies the contract; the isolation wiring
  (pods, broker, signed worker protocol, preflight, revocation) and the native
  model client (stream normalization, role binding, brief delegation,
  capability health) are implemented behind secure mode, while artifact
  integration and publication (#125), authenticated status semantics (#126),
  and real-cluster e2e acceptance remain outstanding.
- Merge remains a human gate. Neither the target broker nor autonomous roles may
  merge, mutate the queue, or access destinations outside the resolved policy.
  The target coordinator may request permitted publication, but cannot bypass
  broker policy.
- Source-state transitions (claim, in-review, needs-human, resolve) are the
  operator's, done by deterministic code — the one place non-determinism would be
  dangerous, kept mechanical.
- Provider credentials for model access are a per-deployment secret concern,
  and the core assumes no particular provider. In legacy mode the pod itself
  holds whatever its lane needs (an Anthropic, OpenAI, or MiniMax token, a
  local endpoint's key, any combination); in the target design the provider key
  belongs to trusted harness control, and workers do not call models directly.
- Secrets are redacted from logs before stdout.
- **Failure-evidence surfaces (#115):** the operator serves an authenticated,
  write-only evidence intake listener (in-cluster, token-gated, no read path)
  and holds namespaced Secret CRUD to persist evidence bundles. The evidence
  verbs are provisioned as a namespaced Role/RoleBinding in the chart,
  deliberately never inside the generated operator ClusterRole, whose
  ClusterRoleBinding would widen them cluster-wide. The authority expansion is
  real but bounded: the chart's namespaced `credential-reader` Role already
  grants the operator's service account `get` on every Secret in the namespace
  (no `resourceNames`), so the evidence grant's marginal authority is `list`
  and the mutating `create/patch/delete`. Deployments that cannot accept
  Secret mutation should not enable evidence capture, and
  reject-preservation remains the named fallback in the #115
  design. Evidence content itself is model-authored untrusted data stored
  inert in run-owned Secrets: never executed, never consumed by tooling,
  applied only by reviewed human action.
- Dispatch is an adapter; the product boundary (dispatch is not Courier, Courier
  is not dispatch) is preserved in the interface.

## Dogfooding and rollout

- Courier develops Courier: its own repo's issues flow through the same loop.
- **Keep opencode as the manual escape hatch.** A self-hosting operator that
  develops itself has a bootstrap trap: a bad release of the thing that fixes the
  thing locks you out. opencode (and plain `kubectl`) remain the deliberate way
  to hand-fix a wedged operator or an abandoned PR.
- Replace Foreman incrementally, lane by lane.

### Later, not V1

- **An LLM operator-manager** — reprioritizing the queue, recommending
  abandon-vs-retry, triaging stuck runs — as an *advisor above* the deterministic
  core. It proposes; the core executes. Its hands stay mechanical even as its
  decisions grow an LLM. V1 must not depend on it.
- **Transcript retention** as a debug convenience — already covered by the
  stdout→Loki path; no native support planned.
- **The busy-loop (spinning) detector** — the conservative content-based V2
  described under Liveness.

## Open questions and blockers

The harness executor contract is settled in [HARNESS.md](./HARNESS.md); these
named items remain unresolved and must not be described as production-ready:

- **#102 liveness:** #119 settles safe handling of long silent tools in
  [HARNESS.md](./HARNESS.md) §6; #126 still must implement the status path and
  operator decision, then prove it in production e2e. A live wedged silent tool
  may remain wedged indefinitely (an intentional safety tradeoff).
- **#80 broker policy:** #118 settles the operator-resolved immutable run
  publication policy and race-safe publication in
  [HARNESS.md](./HARNESS.md) §4 — the full policy schema, admission inputs,
  pre/post-push live revalidation, the `NeedsHuman`/retryable matrix, and
  provider-neutral semantics with fail-closed unsupported providers, including
  the actual writable fork head required by #94. #121 supplies the typed forge
  contract, and #122 has landed the broker primitive layer: the pinned ordinary
  git transport, the typed forge observer adapter, the authenticated
  bundle-import endpoint, the separate trusted status handler, and the immutable
  run-UID-bound policy engine with race-safe publication. Those primitives
  become a secure deployment through the per-run pod wiring, credential
  delivery, and isolation preflight, which #123 has landed as opt-in
  deployment-level secure mode; the native model client (#124) and the
  authenticated status semantics (#126) remain outstanding. The forge provider
  registration surface is
  settled in [HARNESS.md](./HARNESS.md) §4: one deployment-level registry file
  loaded by the operator, pattern-based run selection with fail-closed
  ambiguity, canonical-identity enforcement, and a single-provider broker
  projection.
- **#104 isolation:** #120 settles the per-run broker, pod-bound workload
  identity, signed control-to-worker protocol, and network boundary. The isolated
  topology and secure preflight still need implementation and acceptance tests.
- **Artifact validation:** settled in [HARNESS.md](./HARNESS.md) §5 — git
  bundle only, exact base-tip ancestry binding, fixed size/object bounds, exact
  ref-set equality, and operator-resolved path scope; worker metadata is never
  authority. #125 implements and tests it.
- **Worker egress:** the first supported slice is an administrator-populated Go module/checksum
  cache (#136). Worker requests never trigger upstream access; arbitrary worker
  network access remains prohibited.
- **OpenCode adapter:** prove isolation and status guarantees before any secure
  routing; otherwise retain it only as explicitly insecure legacy mode.
- The exact mechanics of injecting `LaneProfile` framing + roles into the
  coordinator prompt and sub-agent bindings.
- The web UI and CLI adapter surfaces.
- The exact vLLM gauge for backpressure on the target model server.

## Decisions

- **2026-10-10 — #259: intake accepts the sender's empty RunUID and stamps
  the live UID; the bearer itself is a credential; untrusted manifest
  metadata is whole-request rejected; the run is re-read uncached
  immediately before persist.** The PR #257 executor sender cannot read
  the CoderRun UID (the downward API does not expose it), so it sets
  `manifest.Run.RunUID = ""` and treats the bearer HMAC as authoritative.
  Rejecting empty sender UIDs would 400 every real capture. The fix:
  accept the empty sender UID, stamp the live run's UID into the
  intake-authored persisted manifest, and reject only a non-empty sender
  UID that does not match. The same review also caught a credential
  re-scan gap: the presented bearer was not registered (so a sender
  that smuggled its own valid capture credential into the bundle
  persisted a reusable capability), and untrusted manifest metadata
  (`Workspace.*`, `Trigger`, `Run.PodUID`, `Entry.CommitSHA`) was
  written through to the persisted manifest without scanning. The
  bearer is now registered as `COURIER_EVIDENCE_TOKEN`; the metadata
  fields are checked as a whole-request reject because redacting a
  branch or a SHA would imply a sanitization guarantee the artifact
  cannot keep. The pre-persist uncached re-read closes the
  AwaitingReview / Done / recreated-UID window between authentication
  and write: a Secret would have briefly resurrected evidence the
  operator has already proven landed. (#259, PR #257 review
  #5477066262)

- **2026-10-10 — Dispatch discovery reserves pending work conservatively.**
  Each LaneProfile runner gates discovery on one pre-discovery CoderRun list,
  counting empty-phase and Pending alongside Claimed and Running. This is
  intentionally stricter than operator admission, which counts only Claimed and
  Running: once work is materialized, the binding should stop fetching more while
  that work awaits a slot, so a free sibling binding can discover other work.
  Admission must not count Pending against its own concurrency or a Pending run
  could never advance. This is a poll-level heuristic, not an atomic capacity
  lease: one stale snapshot or a multi-item discovery response can still
  over-materialize. Nor can it hide an already-visible Pending queue head from a
  free sibling; that requires Dispatch-side exclusion. Pausing discovery also
  defers next-task and stale-item bookkeeping until a later poll. (#260)

- **2026-10-09 — #198: a deadline-expired capture still delivers the manifest.**
  Issue #198 bounds delivery attempts inside the 20-second operation deadline,
  while this section promises "over deadline the bundle degrades to the
  manifest alone." When the deadline itself is what aborts the snapshot, an
  attempt budget bound by that same expired deadline would silently void the
  promise. The executor therefore gives the manifest-only bundle one fresh
  five-second window (still inside the pod's 45-second grace) after an
  operation-deadline abort; a stalled *intake* never earns this — it is for
  the capture failure alone. Rejected: dropping the manifest (breaks the
  degradation guarantee and hides that a capture was attempted) and a longer
  window (stacks against the grace with the 10-second child-kill wait).

- **2026-10-09 — #238: operator soft-stop is one broker-mediated control
  request with a checkpoint-safe stop boundary.** One channel — annotation
  ingress translated by the operator into a fenced status record and served by
  the broker to trusted control — keeps human intent out of model-controlled
  paths; operator-only writer identity prevents trusted control, the worker, or a
  forge from forging requests. UID fencing voids requests for stale incarnations
  rather than redirecting them, and a durable acknowledgement precedes the latch
  flip so consumed requests survive status observation. In-flight briefs finish
  and integrate before exit, preserving published partial work without pod
  deletion; suspension remains distinct from takeover. The single implementation
  child is #255. (#238, #126)

- **2026-10-05 — #126: harness status is authenticated through the broker and
  liveness is fenced by UID.** The broker alone holds the status-write
  identity: trusted control asks the broker's trusted status listener, which
  re-reviews every token, re-reads the live run and control pod, and applies
  typed patches (heartbeat, checkpoint, `lastCommit`, active-operation
  add/clear/reconcile) under resourceVersion CAS against a fresh read —
  clearing one operation can never drop another entry or another
  incarnation's state. The worker has no path to status. Heartbeats carry the
  dispatching control pod's UID and are earned only by successful model
  streams and verified tool boundaries, coalesced per cadence; a UID that
  does not match the current coordinator pod is never evidence for it. Each
  dispatched operation is persisted as an active-operation entry before its
  task is sent — a write failure prevents dispatch — and cleared only on
  independently observed termination, with the earned tool heartbeat in the
  same update; an uncertain reconciliation retains the entry, and a fresh
  control incarnation reconciles the whole set at process start. A completed
  brief is acknowledged only after its checkpoint and the broker-confirmed
  remote OID are durable, and that gate is mechanical: a failed checkpoint
  write becomes status debt in trusted control, and the terminal path
  reconciles the debt (with live-OID revalidation) before honoring any
  declared outcome — a model declaration can never turn an unpersisted
  completed unit into a successful ending. Transient status-backend
  failures (API-server reads, provider reads inside live-world validation)
  surface as retryable 503, distinct from definite identity or live-world
  mismatches. The operator's reap decision reads the run and the
  run's pods through the direct API reader, deletes only under the observed
  pod's UID precondition (a conflict re-observes), suppresses
  stale-heartbeat reaping only for entries whose control and worker pods are
  alive, and treats a nil, unattributable, or foreign heartbeat as no
  evidence; a vanished or terminating worker or broker under a live control
  fences the round as infrastructure loss. A silent live wedge still holds
  lane capacity until a human intervenes — no finite wedge detection is
  promised. (#126, #119)
- **2026-10-05 — #125: worker artifacts are validated and integrated in
  trusted control, and publication flows through the broker.** The §5
  contract is implemented as written: a returned artifact is a git bundle
  validated in order — `bundle verify` against the private tree, fixed
  bounds (64 MiB bundle, 64 MiB unpacked, 10⁵ objects, 16 MiB per blob)
  applied to every delivered object in a quarantine object dir, exact
  ref-set equality on `refs/courier/briefs/<briefID>`, refless import,
  exact dispatched-base-tip ancestry, and the operator-resolved path scope
  (none is resolved yet, so the whole repository is in scope). Integration
  commits exactly one control-authored commit per brief; publication sends
  a full-closure bundle to the broker's `/v1/bundles/import` (now a raw
  body with OID headers, so the settled 64 MiB bound fits the wire) and
  then `/v1/publication`, whose independent policy enforcement and
  post-push observation decide the outcome. Remote-claimed OIDs (broker
  headers, bundle advertisements, model data) never reach a git argument
  as raw request data: every tip that enters a trusted git call is either
  resolved locally from hash-verified objects or rebuilt byte-by-byte
  from verified hex at the git boundary (`git.canonicalOID`), and claimed
  values are otherwise only cross-checked against those local facts. The worker's sanitized snapshot is rendered from
  control's integration tree, seeded per
  incarnation through a new broker snapshot endpoint (the broker bundles
  the live pinned work ref — or the base ref when the work ref was admitted
  absent), so crash recovery reconstructs from the remote and a crash after
  a confirmed push is an idempotent no-op, never a duplicate publication. A
  foreign work tip is classified NeedsHuman and never adopted or rebased
  onto; a base advance is re-synced with a merge and retried. The brief
  failure path stays fail-closed in both directions: only a sub-agent
  session that reached its terminal final result enters the pack,
  integrate, and publish chain (a failed or truncated session's partial
  commits stay untrusted worker data and the failure is the tool-visible
  result), a rejected artifact burns the brief ID, the fixed rejection
  category is the only tool-visible diagnostic. A declared-changes outcome
  with nothing integrated beyond the seed-time tip is classified by world
  evidence, never by the live OID: a seed at the admission anchor (or a
  work ref absent at seed) is no-work, a seed beyond the anchor is the
  crash-after-push case that the broker confirms idempotently without a
  duplicate push while it holds the trusted evidence, and a broker that
  cannot vouch for the seeded tip blocks the run — ownership is never
  inferred from the live world.
  Forge reads and PR creation are still unwired on the native path (#127's
  terminal contract); CR status wiring remains #126. (#125)
- **2026-10-04 — #124: the native model client is implemented in
  `internal/harness`; publication and artifact integration remain #125.**
  Trusted control normalizes every provider stream into the fixed §5
  vocabulary (`delta`, `tool-request`, `tool-result`, `final`, `error`) in
  one OpenAI-compatible gateway client, binds LaneProfile roles to gateway
  models, delegates through typed briefs with stable IDs (duplicate IDs
  rejected, cancellation tombstones final, ambiguous dispatch reconciled via
  cancel — never redelivered), and reports startup capability health with
  redacted reasons: required capabilities fail closed before model work,
  optional ones proceed degraded. The gateway endpoint and key are
  deployment configuration (`--model-gateway-*` flags, chart
  `secure.modelGateway`); the key is a per-run Secret copy mounted only into
  trusted control. The coordinator's tool router dispatches model-controlled
  commands only to the untrusted worker over the signed protocol — the
  control pod has no local execution path for them — and the `Publisher`
  seam is reachable only from the coordinator's own finish path, so subagent
  results can never publish. (#124, #123)
- **2026-10-04 — Deployment-managed LaneProfiles are bootstrapped by the
  manager, not applied beside the release.** A GitOps consumer cannot safely
  put a LaneProfile in the same apply/render set that installs Courier's CRD —
  discovery may reject the CR before the CRD is established — so the manager
  creates and updates the profiles itself from a YAML config file
  (`--bootstrap-lane-profiles-file`), labeling each one it owns with
  `courier.misospace.dev/bootstrap-managed: "true"`. That label is the
  explicit adoption guard: a same-name profile without it is never touched, so
  a name collision surfaces as an actionable error instead of a silent
  overwrite. Deletion is a conservative MVP — removal from the config leaves
  the CR in place, and the RBAC carries write verbs only (create, update),
  no delete, because active runs reference lanes. (#73)
- **2026-10-03 — #123: the secure-topology wiring is implemented as opt-in
  deployment-level secure mode; legacy remains the default and is explicitly
  insecure.** The operator provisions and garbage-collects, per run, one
  trusted control pod, one untrusted worker pod, and one trusted broker pod
  behind a run-specific Service, with the deployment-level provider registry
  (`--forge-providers-file`), fail-closed preflight (static admission checks
  plus multi-node live deny/allow probes), the signed worker protocol, the
  set-once persisted publication policy, and ordered idempotent revocation.
  The decision recorded here is the rollout shape: secure mode is deployment
  configuration (`--secure-mode`), never a per-run or per-lane choice, and the
  legacy single-pod coordinator keeps running unchanged until the native model
  client (#124) and the remaining harness slices land — the control binary
  currently declares `NeedsHuman` rather than silently falling back. Real
  cluster e2e acceptance remains outstanding before secure mode is enabled in
  any deployment. ([HARNESS.md](./HARNESS.md) §2-§5) (#123)

A running log of architectural decisions and their reasoning, newest first. The
body above describes the current architecture; this log preserves *why* and what
was superseded.

- **2026-10-03 — #80/#8: settle the forge provider registration surface and
  the worker artifact contract; last open HARNESS.md design blockers.** The
  registry is one deployment-level configuration file loaded by the operator
  (`--forge-providers-file`, chart-rendered, references only), not flags (no
  per-provider expressiveness) and not a CRD (new API surface and a reconciler
  for administrator-static config). Runs select their provider by
  case-insensitive `owner/name` patterns over `spec.repo`; zero or multiple
  matches fail closed, and the selected provider's canonical repository
  identity — never the raw spec string — is what every later comparison binds,
  so selection and enforcement cannot disagree. Per-run broker pods receive a
  projection of exactly the one pinned registration with credential references
  as env `SecretKeyRef`s; capabilities come from the implementation's typed
  surface and cannot be configured into existence. The worker artifact contract
  accepts git bundles only — never patches — with exact base-tip ancestry
  binding to the snapshot control dispatched, fixed size/object bounds, exact
  ref-set equality, and operator-resolved path scope, so a hostile worker can
  neither smuggle history nor have its metadata treated as authority. #123
  consumes the registry in the operator wiring; #125 implements the artifact
  contract ([HARNESS.md](./HARNESS.md) §4/§5/§10). An adversarial pass over the
  merged #104 design also amended it: the isolation assumption now explicitly
  covers clean-spec pod creation (runs live in a dedicated namespace where only
  the operator creates or mutates pods, preflight-verified to the extent RBAC
  review allows), credential-bound write checks moved from operator admission
  into the broker's own startup preflight, push endpoints must derive from the
  same registration as the identity/protection reads, interrupted-push
  recovery is bounded to one control incarnation, the protected-ref no-bypass
  property is a provisioning attestation rather than a verified fact, and
  legacy mode may not share a provider credential with secure runs.
- **2026-10-02 — #115: preserve bounded failure evidence; capture in the
  executor, persist by the operator.** After the #109 production loss of six
  uncommitted files, the design settles preservation rather than accepting the
  loss: the executor captures a bounded, fail-closed-scanned bundle whenever
  the verified worktree shows unrecoverable state (continuation points, crash
  branches, terminal `Failed`/`NeedsHuman` classifications, and a SIGTERM trap
  during operator-initiated deletion), and the operator persists it through an
  authenticated write-only intake listener into per-incarnation, run-owned
  Secrets GC'd with the run. The trust split is load-bearing: the legacy pod
  is model-controlled and must gain no Kubernetes identity, so persistence
  cannot live in the pod; conversely only the pod can read the worktree at the
  moment it dies. Per-incarnation keying makes cross-incarnation overwrites
  structurally impossible — a relaunched clean clone can never destroy the
  evidence of the work it replaced. Secrets are excluded per entry before
  storage (never redacted in place), binary and over-limit content is named
  but never stored, and retrieval/application stay human actions so evidence
  is never adopted, published, or implied published by a run or a #97 retry.
  The accepted cost is a namespaced Secret-CRUD authority expansion on the
  operator (not narrowable by `resourceNames`), stated in Security and
  boundaries with reject-preservation as the named fallback for deployments
  that cannot accept it. Rejected: log/stdout/status/termination-message
  carriers, forge pushes (quarantine refs or evidence repos), per-run PVCs,
  executor-held API identity, and operator-side exec pull (impossible once
  the container has exited — the #109 class). (#115, #109)
- **2026-10-02 — #168: export run metrics; scraping is an opt-in chart flag.**
  Run metrics were ad-hoc `kubectl` and log archaeology, so the operator now
  exposes them on the controller-runtime `:8080` endpoint. Labels are limited to
  `lane`, `mode`, and terminal phase to bound cardinality — never `repo` or
  `issue`. The run and queue-wait duration histograms derive from the #167
  set-once status timestamps. The in-flight and lane-suspended gauges read the
  world live at scrape time rather than being push counters, so they survive an
  operator restart and match the world-is-source-of-truth principle. Terminal
  totals count a run once, at its terminal transition, so a future post-merge
  `AwaitingReview`→`Done` edge must not double-count it. Scraping is opt-in via
  the chart's `serviceMonitor.enabled`, which renders a metrics `Service` and a
  `ServiceMonitor` (off by default). (#168)
- **2026-10-02 — One deployment can serve multiple Dispatch lane bindings.**
  It pairs each Dispatch queue lane with a LaneProfile, with one discovery
  runner per binding, so a Deployment can serve an escalation lane without a
  second manager reconciling the same CoderRuns. Lifecycle stays
  lane-agnostic. (#173)
- **2026-10-01 — #167: record run timing on CoderRun status.** AdmittedAt,
  StartedAt, and FinishedAt are set-once status timestamps, with Waited/Ran
  printer columns derived from them. StartedAt is read from the coordinator
  pod's container status (the world), not the reconciler clock. FinishedAt
  records the run's own terminal transition, so AwaitingReview carries run time
  even though merge-settling is human-gated; a later operator-marked Done never
  moves it. StartedAt also rides the Dispatch lifecycle report so sources can
  record real AgentRun durations. (#167)
- **2026-10-01 — #172: tally run telemetry in the executor, publish compact-only to
  status and source.** The OpenCode event stream already passes the executor's
  stdout tap, so the tally rides it rather than adding a new collector. The full
  summary is logged (`run.summary`, always-on detail) but only a compact integer
  form crosses into `CoderRun.status` and the Dispatch report, because that
  payload shares the pod's 4 KiB termination-message budget. Durations are
  milliseconds (integers) so the CRD needs no `float` (which controller-gen
  rejects). Subagent **tokens** are deliberately absent: subagent sessions do not
  stream their own steps to the coordinator, so only their task-call
  counts/errors/runtime are observable here; run token totals come from the
  coordinator's own steps. Metrics export is deferred to #168 and richer
  continuation accounting to the companion continuation issue. (#172)
- **2026-09-30 — #170: preserve continuation-state boundaries in loop fingerprints.**
  Delimiter-joined fields can collide when paths or other values contain those
  delimiters. Hashing typed JSON preserves field boundaries and keeps raw paths
  and assistant text out of continuation events; termination history remains
  separately redacted. (#170)
- **2026-10-06 — external verification belongs to the operator.** A coordinator
  validates integrated work locally, publishes it, and completes; CI and review
  observation remain in the operator's `Verifying` phase. A red CI observation
  ends a fix-pr attempt as `Failed` instead of parking it as `NeedsHuman`;
  initial issue runs keep observing so later repairs can advance the original
  issue. Sources apply their own failure policy. Dispatch's failed report performs the retry/cap
  decision for queue-backed PR-fix work, so Courier must not send a second
  `BLOCKED` mark for the same now-advanced generation.
- **2026-09-30 — #169/#175: declare outcomes outside the worktree; never settle
  from a declaration alone.** #169 replaced inference of coordinator intent from
  workspace state with explicit `changes`, `no_change_needed`, `needs_decision`,
  and `blocked_external` declarations, checked against the world. Review on #175
  corrected the handoff boundary: the exact per-run outcome file is under
  executor-owned scratch outside the target worktree, so stale declarations are
  avoided without deleting `.courier` or manipulating the git index. `changes`
  requires relevant local validation of the integrated work and commits
  reachable from the run branch. `no_change_needed` posts its complete redacted
  evidence and exits `3`: resolve-issue waits in
  `AwaitingReview`/`in-review`, while fix-pr ends `NeedsHuman`; the blocked report
  itself parks the PR-fix item as `BLOCKED`/needs-human, and
  `BlockedReportParksPRFix` skips only the redundant queue-mark call — it does not
  wake a reviewer. Neither outcome resolves the source. `needs_decision` and
  `blocked_external` post the complete redacted question or missing explanation
  and exit `2` to `NeedsHuman` with a blocked report. Only the distinct
  termination reason is bounded before publication. The controller carries the
  bounded blocked-external reason into the source lifecycle report and preserves
  it across durable report retries. A red external CI observation after
  publication ends in `Failed`; Dispatch's failed report advances a queue-backed
  PR-fix attempt or routes it to a human at the existing cap. If the report is
  skipped because the queue generation has already moved, Courier does not issue
  a separate queue mark against that generation. The earlier #169 contract
  resolved `no_change_needed` and treated `blocked_external` as retryable
  `Failed`; both are superseded. (#169, #175)
- **2026-09-29 — #178: a run's own terminal phase is never gated on a source
  report.** `transitionTerminal` used to publish the source transition and
  lifecycle report before writing the phase, so a rejected report kept the run
  `Running` and holding lane capacity while it retried on backoff. A real case: a
  `fix-pr` run whose settle was rejected with Dispatch's generation-mismatch 409
  (the queue item had advanced to a newer attempt) sat `Running` for hours behind
  a concurrency-1 lane, starving every other run. The reconcile now writes the
  terminal phase first — capacity frees regardless of the source — then publishes
  the report on its own, retried via a bounded requeue and deduplicated by the
  run's idempotency key. A generation mismatch is mapped in the Dispatch adapter
  to a typed superseded error the controller recognizes and *drops*: the report
  is a record of the old attempt and the newer generation belongs to another run,
  matching the #98 settlement contract. A retry cap was deliberately not added —
  it would be a governor on a harmless idempotent retry, and the run itself is
  never held. (#178)
- **2026-09-29 — Resume the coordinator session on recoverable endings.** A
  run that ends recoverably — uncommitted changes, commits off the run branch,
  no commit and no declared outcome — and a run whose session crashed, no longer
  terminates at once. The executor resumes the same session with a short state
  message built from world facts, up to `COURIER_MAX_CONTINUATIONS` times
  (default 3); a no-progress guard — the same workspace-state fingerprint and
  the same last assistant message twice — terminates earlier as `NeedsHuman`
  with reason `looping` and the state history. A crash resumes with exponential
  backoff before the run fails. Exit `2` remains an immediate `NeedsHuman`
  passthrough only for an explicit declaration; a raw child exit `2` remains a
  failure, and a recoverable ending without a captured session fails as
  incomplete. This extends the 2026-09-22 honest-termination decision (#81) and
  the 2026-09-27 run-branch decision (#134), and follows the "inform, don't
  constrain" principle: the model gets the facts and decides, and the harness
  only stops genuinely stuck runs. (#170)
- **2026-09-27 — #122 lands the broker publication primitives.** The broker owns
  the whole publication path: a pinned, non-force git transport to one explicit
  ref, a typed forge observer (never raw forge calls), an authenticated
  bundle-import endpoint that validates imported objects before publication, a
  separate trusted status handler, and an immutable run-UID-bound policy engine.
  The engine re-reads the live world before and after a single ordinary push and
  decides publication solely on that observation: the push's own success,
  failure or lost report never decides the outcome. An uncertain push is
  confirmed idempotently when the exact proposed OID is live and every base,
  protection and PR check still passes; a fresh broker after a crash cannot
  infer ownership from a caller-supplied OID or ancestry, because in-memory
  confirmation is not durable and the admission anchor is the only trusted
  restart evidence. These primitives are landed, but they are not a secure
  deployment: the per-run pod wiring, credential delivery and isolation
  preflight in #123 are still required before the broker is safe to expose.
  ([HARNESS.md](./HARNESS.md) §4) (#122, #118, #80, #123)
- **2026-09-27 — Read-only toolchain reference for bootstrap lanes (#153).**
  OpenCode's `external_directory` permission cannot distinguish a read from a
  write, so the toolchain's writable cache directory was not allowed wholesale.
  The coordinator pod mounts the per-run `toolchain-cache` `emptyDir`
  read-write at `/courier-toolchain-cache` (where runtime images pin
  `GOMODCACHE`/`GOCACHE`) and again read-only at `/courier-toolchain`, so the
  reference stays readable while writes are denied at the filesystem level.
  This is bootstrap ergonomics for the pinned runtime, distinct from the #136
  secure dependency cache.
- **2026-09-27 — A lane can be suspended without stopping work in flight.**
  Setting `courier.misospace.dev/suspend: "true"` on a LaneProfile pauses that
  lane: its source runner stops discovering work and the operator stops
  admitting its Pending runs, while Claimed, Running and Verifying runs finish
  normally. This frees a lane's models without killing anything. It is an
  annotation rather than a spec field, so an operator can toggle it with
  `kubectl annotate` without fighting the manifest's owner, and a suspended
  lane shows in the LaneProfile's Suspended column. (#162)
- **2026-09-26 — #118 settles the run publication policy.** The operator
  resolves and persists an immutable run-UID-bound policy from the spec,
  provider configuration, and live reads: canonical base repo/ref/OID,
  work repo/ref, and for `fix-pr` the actual PR number and head repo/ref/OID,
  including a writable fork. The base OID is a snapshot; movement on the
  same base ref calls for re-sync, not a change of destination. The head OID
  is an admission anchor, not a frozen tip: confirmed own pushes advance it;
  foreign heads stop publication. Every publication compares a fresh expected
  tip and rechecks PR identity, base identity, protection and write permission,
  uses only ordinary non-force git push, then re-observes the exact proposed
  OID and PR head before recording `lastCommit`. No status/forge atomicity is
  assumed; uncertain outcomes require exact live confirmation, not inference
  from ancestry. Both base and work default/protected refs are excluded,
  including a fork's; provider-side protection must reject races, with no
  broker credential bypass. Missing provider semantics fail closed. No model
  policy derivation, same-name branch substitution, arbitrary destination,
  merge, or force push. This is a design contract, not shipped broker support.
  ([HARNESS.md](./HARNESS.md) §4) (#118, #94, #80)
- **2026-09-26 — fix-pr adopts the PR head's repository, not just its branch name.**
  A `fix-pr` run previously resolved only the PR head's branch name, so the
  executor cloned and pushed the base repository and could fail to find the real
  fork branch — or worse, adopt a same-named branch in the base repo. The GitHub
  observer now resolves full head identity (head repository, branch, commit SHA)
  and the controller persists it on the run as `status.headRepo` and
  `status.headSHA` alongside `status.branch`. The coordinator pod builds its
  clone/push remote from the head repository, so `origin` is the fork and a
  colliding same-named base branch can never be adopted; `spec.repo` (the base)
  stays the PR/publication target and a fetch-only `upstream` remote keeps
  base-sync merging the real base rather than the fork's. Observation queries
  the base repository with the fork's owner as the head qualifier and reads
  checks for the head SHA from the base. A deleted fork (`head.repo: null`)
  terminalizes NeedsHuman instead of cycling claim and release. Before
  preparing the workspace the executor
  verifies the head branch exists on the fork remote, and an unavailable or
  unwritable fork head is an actionable NeedsHuman — no reset, replacement
  branch, or force-push is introduced. Same-repository fix-pr runs are
  unchanged: `status.headRepo` equals `spec.repo`. (#94)
- **2026-09-26 — Base-sync conflicts are the coordinator's work, not a setup
  failure.** Adoption previously treated any nonzero `git merge` as fatal, so a
  fix-pr run on a conflicting PR ended `Failed` before OpenCode started, and
  conflicted PRs could never be fixed by Courier. A conflict is now left in
  progress on the checked-out branch and named in the coordinator goal (base and
  paths); resolving it comes before any other change. After the coordinator
  exits, a pending merge, remaining unmerged paths, or a HEAD that does not
  contain the base (an abandoned merge) ends the run `NeedsHuman`, so an
  unsynced branch is never reported ready. Non-conflict git failures still fail
  closed. (#96)
- **2026-09-26 — A PR merged during a run ends it Done, not NeedsHuman.** The
  observer used to match only open PRs, so a run whose PR someone else merged
  while it worked looked like a run with no PR and blocked its source item for
  shipped work (#107 and #108 did this). A merged PR on the run branch now moves
  a Verifying run to Done, which resolves the source and applies the reap
  policy. A PR closed without merging still needs a human. (#146)
- **2026-09-25 — #119 settles the long-tool liveness design.** A silent
  legitimate operation and a wedged one are observationally identical, so no
  design can both reap a wedged silent tool in finite time and never reap a
  legitimate one; the design chooses to never false-reap. A harness-owned
  active-operation set (op IDs, owning control-pod UID, worker-pod UID) is
  persisted before dispatch and suppresses stale-heartbeat reaping only while
  the operator independently observes those pods running. A
  pod-UID-fenced heartbeat prevents prior activity from resetting the new
  incarnation's crashloop streak. There is no duration cap; a wedged tool is not detected and the escape is manual `needs-human`.
  This is an intentional safety tradeoff, not a liveness proof; #102 now blocks
  on #126 implementation and a production e2e, not on the design.
  ([HARNESS.md](./HARNESS.md) §6) (#119, #102)
- **2026-09-25 — A long in-flight tool call no longer keeps the heartbeat warm.**
  The earlier liveness design treated a long in-flight tool call as activity and
  "kept the heartbeat warm" through it. That is superseded: an in-flight tool is
  not liveness evidence, and the harness must not emit a heartbeat it has not
  earned (no fake heartbeat), so a wedged silent tool no longer looks alive.
  Safe handling of long silent tools — suppressing reap without a fake
  heartbeat — is designed in #119 ([HARNESS.md](./HARNESS.md) §6) as an
  intentional safety tradeoff: a harness-owned active-operation set suppresses
  stale-heartbeat reaping while the operator observes a matching task running,
  with no duration cap and no wedge detection. #102 now blocks on #126
  implementation and a production e2e, not on the design. (#102, #119)
- **2026-09-26 — #120 settles the isolated per-run trust topology.** Each run
  gets trusted control, a dedicated broker and run-specific Service, plus a
  named per-run Role limited to that run's status subresource, and an untrusted
  worker. Control uses a pod-bound projected 600-second
  `courier-broker` audience token; the broker TokenReviews every request and
  checks authenticated service-account UID plus the current pod/run incarnation.
  Control-to-worker tasks are signed and incarnation-bound, with no worker
  secret, service-account token, or signing key. Worker egress is limited to a
  administrator-populated Go module/checksum cache and no DNS; arbitrary URLs,
  proxies, broker, forge, and other cluster access stay blocked. The separate Go
  cache remains to be implemented; approved cache request keys retain a bounded
  information-leakage risk. Secure mode requires enforced network
  policy, live deny probes, and fail-closed preflight before exposing workloads.
  The preflight separates static admission/spec checks (the cluster enforces the
  required admission; the operator's own rendered specs are clean) from live
  probes, and the broker's no-extra-grants RBAC property is guaranteed by
  provisioning and verified by reading the Role/RoleBinding objects — an
  access review proves a grant exists, never that no extra grant does. The
  cache is a read-only pre-populated store: no upstream I/O on worker demand,
  `GOSUMDB=off` checksums, IP-literal addressing. A privileged cluster actor
  who can bypass RBAC and admission is an explicit residual risk.
  This resolves #120's design choices, not their implementation or readiness;
  #123 and a separate cache issue #136 still own implementation. MCP remains an
  interface, not a security boundary, and the legacy single-pod path remains
  explicitly insecure. [HARNESS.md](./HARNESS.md) holds the detailed contract.
  (#120, #104)
- **2026-09-25 — Separate safe scratch from durable dirty-work recovery.**
  A per-run scratch mount and narrow OpenCode permissions address unattended
  temp-file use without expanding access to `/tmp` or polluting the checkout
  (#109, #114). Scratch is not recovery: the bootstrap still loses uncommitted
  edits when its ephemeral pod is removed. Secure evidence storage, limits and
  retrieval need a separate design before implementation (#115). An explicit
  retry (#97) is a fresh attempt, not an implicit restoration.
- **2026-09-25 — The executor informs the coordinator of unreachable MCP
  capabilities.** Before the goal runs, the bootstrap performs one
  liveness-bounded (30s, never a run timeout) `mcp list` preflight against the
  same config and environment as the run. Configured-but-unavailable servers are
  named in the coordinator's framing, emitted as a structured
  `capability.status` diagnostic (redacted; detail forced visible because the
  point is non-debug visibility), and appended to the terminal reason when the
  run produced no work. Probe output never reaches the run's streams or the
  prompt unredacted, and every probe failure mode degrades to "no
  information." Informing never constrains: an unavailable optional
  capability never fails a run. (#101)
- **2026-09-25 — Dispatch owns follow-up attempt identity and settlement.**
  A retained run is historical, not a lease on all future review rounds.
  Queue-backed work already has a persisted `(id, generation)` identity;
  Courier encodes it in the opaque work ID. A changed PR head or health reason
  alone cannot identify a fresh review attempt, and a report for an older
  generation cannot resolve new feedback. Dispatch must compare the issued
  attempt against its start-head baseline and settle conditionally. The
  generation-less linked-PR path requires its own persistent source identity
  before Courier can distinguish new work from stale re-polls. (#98,
  misospace/dispatch#1045)
- **2026-09-24 — The crashloop counter bounds a consecutive streak, not a
  lifetime total.** A reaped run's relaunch counter resets to zero whenever the
  run demonstrates liveness — a fresh, in-window heartbeat — and reaching the
  ceiling (`restarts >= maxRestarts`) hands the run to a human with the counter
  at the ceiling instead of relaunching it once more. This bounds *stuck* (a run
  that keeps wedging without ever proving itself alive) while a long-lived run
  that recovered is not left one unrelated wedge away from `needs-human`. It
  supersedes the lifetime-total reading and the first implementation's
  `restarts > maxRestarts` (which allowed an N+1-th relaunch). Clearing the
  counter to zero required `status.OperatorPatch.restarts` to become nullable,
  since a JSON merge patch omits a zero-valued `int` — mirroring how
  `branch`/`checkFingerprint` are nullable so the operator can clear them. The
  startup grace for a just-relaunched pod is derived only from observable pod
  state (creation time / container start), never by having the operator write the
  harness-owned heartbeat. (#12, #100)
- **2026-10-06 — External verification is an operator handoff.** Supersedes
  the 2026-09-22 coordinator-completion boundary below: the coordinator owns
  local validation of integrated work, publication, and opening/updating the PR,
  but does not wait for external CI or review. The operator retains `Verifying`;
  stable green moves to review, while red CI follows the source's retry policy.
  For Dispatch's queue-backed PR-fix work, the failed report itself advances the
  attempt or applies the existing cap, so Courier does not send a second
  `BLOCKED` queue mark after a generation change.
- **2026-09-22 — The coordinator owns completion and forge publication.** A
  coordinator may delegate research, implementation, review, and tests, but
  never the run's terminal contract: reading the work item, integrating
  delegated work, verifying it, pushing the branch, and opening or updating
  the PR stay the coordinator's, performed through the configured forge
  capability rather than a forge-specific CLI. A local commit or pushed
  branch with no required PR is not completion. The shipped OpenCode
  bootstrap states this explicitly (the resolve-issue/fix-pr goals carry
  it); the planned resumable harness (#8) will enforce it structurally by
  granting forge-mutation and push tools only to the coordinator and no
  merge capability to any autonomous role. (#90)
- **2026-09-22 — Forge access is through a generic MCP, never a per-forge CLI.**
  The coordinator reads issues and opens/updates/comments on change requests via
  a forge MCP wired under a generic slot; the deployment selects the provider
  (GitHub / GitLab / Forgejo / Bitbucket). Installing `gh` (or `glab`, `tea`, …)
  into the coordinator image was rejected: it hardcodes the forge into the one
  place meant to be swappable. This settled provider agnosticism, not a security
  boundary; the 2026-09-25 decision above supersedes the MCP-only access model.
  (#80, #87)
- **2026-09-26 — The forge boundary is a typed, capability-advertised contract.**
  `internal/forge` defines the forge-agnostic interface the core uses to read
  work items and change requests and to create/update/comment on them; concrete
  forges live in sibling packages (`internal/github` is the first). The contract
  makes **merge and raw passthrough unrepresentable** — there is no merge or
  raw/do verb, and the capability vocabulary is closed: `Capabilities.Register`
  refuses any capability outside the defined constants, so no capability can be
  registered for them. The interface cannot expose merge or raw operations to
  the core, preserving the human merge gate. Providers
  advertise operations through a `Capabilities` set and decline the rest with
  `ErrUnsupported`. Diagnostics and provider errors are passed through
  `RedactDetail`, a minimal last-line guard (URL userinfo, bearer tokens), not
  a guarantee that an arbitrary credential is removed: providers must keep
  secrets out of diagnostic detail, and the GitHub adapter sanitizes API error
  responses and boundary errors the same way, with the typed error preserved.
  `ProviderConfig`
  carries only an endpoint, a name, and a credential *reference* that the broker
  resolves, and it stays independent of `LaneProfile` (which describes lanes,
  never the forge or its credentials). Forge-provider pull request reads carry
  the live base repository/ref/OID and actual head repository/ref/OID, including fork heads;
  missing repository identity is not replaced with a same-named base branch.
  The GitHub adapter implements only covered operations. Source adapters and
  broker transport remain out of scope. (#121, #80, #118)
- **2026-09-22 — Repository toolchains are per-lane runtime images, not baked
  into one universal coordinator image.** A `LaneProfile.runtimeImage` selects a
  coordinator image carrying the target repo's toolchain (e.g. `courier-go`
  layers Go, make, controller-gen, and helm onto the bootstrap image). The
  default coordinator image stays minimal. (#79)
- **2026-09-22 — The run terminates honestly when nothing was produced.** The
  executor exits a run as `NeedsHuman` when the coordinator produced no commit or
  PR, rather than reporting success; the operator's world-verification (no PR →
  NeedsHuman) is the backstop. (#81)
- **2026-09-26 — A resolve branch with an open PR is reported for review, not
  blocked.** The adoption guard previously ended any resolve-issue run whose
  deterministic branch already had a pull request as `NeedsHuman`, marking the
  issue blocked while a PR (often a sibling `fix-pr` run started moments
  earlier) was actively in flight. The guard now branches on state: an **open**
  PR leaves the run refusing to adopt but exiting `0`, so the operator's
  world-verification observes the real pull request and publishes it to the
  source as `in-review` (`pr_opened`); a branch whose only PRs are closed or
  merged still ends `NeedsHuman`, since a human must decide whether to reuse it.
  This reuses the existing verify/observe path rather than adding a new terminal
  state. Review-readiness is still the operator's to decide by re-reading the
  world: a draft pull request or failing checks continues to hand the run to a
  human. (#135)
- **2026-09-27 — Committed work must be on the run branch, checked against the
  branch ref.** A coordinator or delegate could open its PR from a branch other
  than the run's, so the operator — which observes only the run branch — read
  an empty branch as "no work" and handed a finished issue to a human. Two
  changes close the gap. The goals name the run branch as the single place work
  may be committed, pushed, and opened from (a hint that informs, not a hard
  gate). At exit the bootstrap inspects the run branch's own ref for commits
  ahead of the workspace start, not wherever HEAD happens to point — the world
  wins over the current checkout — and reports `NeedsHuman` with a specific
  reason when committed work is absent from the run branch, instead of
  `Verifying`. Checking HEAD alone was rejected: it falsely downgrades work
  that landed on the run branch while HEAD moved elsewhere. The durable fix —
  a broker that publishes only to the pinned work ref — is (#122); this is the
  interim detection plus framing. (#134)
- **2026-10-04 — Pod disappearance is infrastructure loss, distinct from the
  #12 heartbeat wedge.** No pod means no heartbeat can exist, so requiring one
  would leave an evicted run `Running` forever — which is what happened in
  production. The live read of the API server is the sole confirmation of
  every charge for a missing coordinator — one rule for both the disappearance
  and the stale-heartbeat signals — with no timer and no recorded state: a
  coordinator found live means the cache lagged and nothing is charged, a
  terminating coordinator is re-observed, and only an API server with no
  coordinator pod for the run charges the relaunch — which is also what lets a
  run that was already orphaned before this backstop existed recover. The
  **retained** branch is adopted, never recreated from base. Pod loss and a
  reaped wedge share one ceiling and one relaunch path. (#106, #12)
- **2026-10-05 — A terminal-phase coordinator pod without container statuses
  is pod loss, not an exit.** Kubernetes can transition a coordinator pod to a
  terminal phase (eviction, failed scheduling) without ever recording container
  statuses, and no model exit code is inferred from a state Kubernetes never
  reported. Such a pod is an unobservable dead coordinator: it is confirmed by
  the same live read as a disappearance, its inert object is deleted so the
  deterministic pod name is free for the replacement, and the relaunch is
  charged as pod loss against the shared ceiling — except at the ceiling
  itself, where the inert object survives as the `NeedsHuman` hand-off's only
  record of the cause. A fresh heartbeat resets the crashloop streak only
  while a recoverable coordinator is observable: an eviction detected inside
  the window must not reset the counter the same reconcile charges. Eviction
  is exactly the class #106 must repair. (#106)
- **2026-10-05 — The ceiling evidence-preservation rule now covers both
  infra-loss hand-offs.** A stale-heartbeat run whose cached coordinator is an
  unobservable dead object is reaped by the wedge path before the
  disappearance backstop can route it, and the wedge path deleted that inert
  object before the restart ceiling was even consulted — so at the ceiling the
  `NeedsHuman` hand-off lost the only record of the cause the backstop
  deliberately preserves. The wedge path now applies the same rule: an
  unrecoverable coordinator is preserved, not deleted, at the ceiling,
  mirroring the backstop — the hand-off must keep the only record of the
  cause regardless of which path detected the loss, and no replacement needs
  the name. Below the ceiling the object is still deleted so the
  deterministic pod name stays free. (#106)
- **2026-10-05 — The pod-loss charge now fires only after the live read
  confirms the deleted dead object is gone.** A delete is not a
  disappearance: a confirmed dead object can still be terminating, held by a
  finalizer (deletionTimestamp set, object persists), and the relaunch path
  tolerates an existing object — so a replacement could attach to the dying
  object and the same physical loss be charged a second time when it finally
   vanishes. The re-confirmation now guards both legacy delete paths that can
   remove a dead coordinator — the disappearance backstop and the wedge's
   reap; the secure path's equivalent is #223. The disappearance backstop
   re-confirms with one more live read after the delete and before the
   relaunch is charged, and the wedge's reap of a dead coordinator below the
   ceiling defers the charge the same way — any surviving coordinator object
   defers it, so the deterministic name is provably free before the relaunch.
   The deferred wedge charge is then taken by the disappearance backstop,
   which confirms the loss against the API server and charges it with its
   pod-loss reason — one physical loss, one ceiling charge, found in AI review
   of PR #218. (#106)
- **2026-10-05 — #105: the wedge-path charge re-confirms every delete, not
  just the dead-object one.** `checkLiveness` charged the crashloop counter
  immediately after a `Delete` returned, so a sibling reconcile whose cached
  view had not yet caught up with a terminating object could count a no-op
  delete as a second wedge for one physical loss. The post-delete live
  re-confirmation (`coordinatorObjectPresentLive`) now guards every wedge-path
  delete below the ceiling, mirroring the rule #106 established for
  dead-object deletes: a surviving coordinator object defers the charge and
  the disappearance backstop takes the deferred loss with its pod-loss cause.
  At the ceiling there is no charge to defer and the hand-off terminalizes in
  the same reconcile. The counter stays bound to *observed* wedges; no timer
  and no duration bound was introduced. The window it does not close — a
  cached run status lagging a sibling's already-charged status patch — needs
  charge-proof writes and stays with #126. (#105)
