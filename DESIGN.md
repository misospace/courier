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

Capacity-exhaustion recovery is an authoritative source boundary: after uncached topology observation and cooldown, an adapter must explicitly prepare a source-owned receipt proving the current claim/generation and that trusted workspace/remote state holds every partial write. The operator then submits that receipt to an atomic, idempotent source settlement. Missing capability or incomplete proof keeps ownership held; run identity, evidence absence, and model declarations are not proof. No production adapter currently implements this verifier, so production recovery remains safely unsupported.

External verification can later reach a separate outcome: stable green advances
**Verifying** to **AwaitingReview**; a red check on a fix-pr run ends it as
**Failed** and emits a failed lifecycle result, while a resolve-issue run whose
published PR has stable, confirmed red checks reaches **HandedOff** and hands
the PR to the source's independent repair mechanism. The source decides how to
handle the failure; queue-backed PR-fix work follows its existing retry policy.

A park-and-resume mechanical CI repair loop (#475) extends this boundary with
contracts the operator and sources implement against; the operator's park,
resume, and handoff wiring around them lands in follow-up work. The durable
record is `CoderRun.status.cipark`: the exact published head the park observes,
when the park began, how many repair opportunities the run consumed, and the
outcome of the most recent one — enough to survive pod deletion and controller
restart, with repo, PR, branch, and head identity already carried by existing
status fields. Strict trigger separation is a pure classification, not a
watcher behavior: only explicitly configured mechanical checks failing on the
exact recorded head, with repair budget remaining, may wake a coordinator;
pending or unregistered checks hold the park, and a merged, unobservable, or
superseded-head observation, a non-mechanical failure, or an exhausted budget
always finishes without waking. The optional source-side counterpart is the
`CIParkLifecycle` capability: a fenced, idempotent park notice, repair-opportunity
reservation, and outcome report so a connector that manages PR-fix queues can
defer dispatch while a Courier repair opportunity is active and refuse stale
generations instead of racing them. Sources that cannot fence must not
implement the capability, and the core never emulates it with ordinary
lifecycle calls.

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
  complete evidence as a redacted issue/PR comment, then exits `3`. A
  resolve-issue run ends in **AwaitingReview** and moves the source to
  `in-review` for a human to settle. A fix-pr run is settled against the world
  through **Verifying**: a merged/already-addressed pull request reaches
  **Done**; an open pull request with stable green checks reaches recoverable
  **BlockedExternal** because CI cannot verify queued feedback resolution.
  A red check reaches retryable **Failed**; an unobservable pull request reaches
  **NeedsHuman**. The open no-change attempt reports `waiting-external`, never
  `pr_updated` or `already_addressed`; its declaration is not settlement authority.
  Neither mode resolves the source on the coordinator's word alone.
- **`needs_decision`** — a real decision the coordinator cannot make. The
  executor posts the complete question as a redacted issue/PR comment and exits
  `2` to **NeedsHuman** with a blocked source report.
- **`blocked_external`** — an external prerequisite needed to perform the work
  is missing. The executor posts the complete `missing` explanation as a
  redacted issue/PR comment and exits `2` to the terminal, recoverable phase
  **BlockedExternal**. The source item is kept active (`in-progress`) and
  reported with a recoverable lifecycle result, so the source can re-issue the
  work once the named prerequisite is satisfied; it is never parked as a human
  block. The recoverable signal must reach the source in a way it cannot
  mistake for a normal failure: publishing the lifecycle as a regular
  `failed` report causes a queue-backed source to advance its generation and
  burn fix-attempt budget on a prerequisite Courier cannot satisfy, ending
  exactly in the human park this outcome is designed to avoid. The Dispatch
  adapter therefore publishes a dedicated `waiting-external` task outcome
  and, for queue-backed follow-ups, a generation-fenced `WAITING_EXTERNAL`
  queue mark — both recoverable, neither a `BLOCKED` human park. A CI
  failure discovered after publication is not this outcome: it follows the
  operator's verification and source failure policy.

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

### Coordinator pod process lifetime

The legacy pod runs the executor directly as PID 1 — the pod sets `Command`,
and the coordinator image declares no entrypoint — so the kernel hands the
executor every orphaned descendant its model child leaves behind (npm, node,
esbuild, a `tail` that never exits), and PID 1 reaps none of them on its own.
The executor therefore reaps: it asks to be a child subreaper, which is a no-op
when it is already PID 1 and is what makes the same behaviour hold when it is
not, then re-enumerates its own threads' children on every SIGCHLD and collects
the dead ones. It only ever collects. A live orphan keeps running exactly as
long as its dead parent left it running; the reaper never signals, kills, or
touches a process group, and termination propagation is unchanged.

Collecting never takes a status from a waiter. Every child the executor starts
goes through one funnel that registers the pid before the start is released, so
its exit status stays with `Wait`; and a descendant is collectable only when it
sits in a process group a supervised child led. That last condition is what
separates the model child's tool tree from the git children `internal/git` and
the evidence capture start directly: the kernel makes an adopted orphan an
ordinary child and offers nothing else to tell the two apart, so without it a
drain pass would consume a real git status and silently change a run's ending.
Both children the executor starts with their own group — the model child and
the preflight `mcp list` probe — are therefore covered, and the groups are not
removed once their children exit, because the orphans worth collecting only
exist once they have. What that leaves uncollected is stated rather than
implied away: an orphan that made a group or session of its own (`setsid`, a
detached spawn), and the git, ssh and askpass descendants a killed run orphans
into the executor's own group.

### Worker task containment beyond the process group

The native worker's tasks run `sh -c ...` in a process group of their own, and
cancellation kills and proves that group (#372). Two process shapes sit
outside any group contract, and the worker closes both without granting the
untrusted pod a single privilege it does not already have — no PID namespace,
no cgroup, no capability: the worker pod's security context (all capabilities
dropped, RuntimeDefault seccomp, non-root) is unchanged. A per-task PID
namespace (`CLONE_NEWPID`) or a per-task cgroup would end the escapee class in
the kernel, but both need `CAP_SYS_ADMIN` for the clone or cgroup delegation,
and against an untrusted sandbox that means weakening the exact boundary the
worker exists to keep.

The first shape is the escapee. A descendant that makes a session of its own
(`setsid`, a daemonising tool) leaves the group, and no group signal reaches
it. The worker is PID 1 of its pod, so the kernel hands it every orphaned
descendant its tasks leave behind; the subreaper flag keeps that true for any
shape where it is not PID 1. At every grouped task's termination — cancelled
and completed alike — the worker sweeps its adopted trees: it enumerates the
pod's processes, kills every live process it has adopted regardless of the
group or session it moved to, and reaps what it killed. Termination is by
identity, never by number: every process the worker snapshots or supervises
is pinned with a pidfd before it is read or signalled, the kernel keeps a
pinned pid from being recycled while the pin lives, and a signal travels
through the descriptor — so a recycled pid can never receive an old task's
group signal or sweep kill, and a supervised release can never claim a new
task's entry. A process whose state cannot be read for any reason other than
a confirmed disappearance fails the whole proof: an unreadable process is
not evidence of absence. A completed task's
escapee is swept too, deliberately: a daemon a successful task left behind is
still untrusted work mutating the workspace, and control would otherwise
clear its suppression on a completion the worker never proved. The sweep's
count is a pod-level floor, so the sharp edge is real and deliberate: a task
can be reported unverified — even a successfully completed one — because of
an unrelated concurrent operation's adopted escapee. That is the containment
policy, not a false positive to fix; the worker cannot attribute a reparented
process to its task, so it refuses to promise anything less than a clean pod.

The termination claim stays honest in both directions. What survives the bound
keeps the result unverified — `LiveDescendants > 0`, retained, exactly #372's
refusal contract — and a clean sweep is itself the proof that every writer of
the task's output closed: an escapee that redirected its own output no longer
forces the unverified verdict #372 had to report, because nothing live is
left in the pod but the worker and the trees of the still-registered tasks.
The bounds compose: the descriptor grace (5s), the group proof (5s) and the
sweep (5s) worst-case stack to 15s per operation against the pod's 30s
termination budget, which is why all three are ceilings on cleanup, never on
a run's duration.

The second shape is the zombie. PID 1 reaps nothing on its own, so every
cancelled or daemonised descendant that dies would pile up as a zombie for the
pod's lifetime. The worker runs an adopted-child reaper in the same shape as
the legacy executor's (#347): a SIGCHLD-driven pass that collects zombie
children, never signals, and never takes the exit status of a supervised task
leader, whose pid is registered before its start is released and released only
after its Wait. The sweep reaps what it kills on the same passes, so a
cancellation converges without waiting on the signal-driven drain.

One ambiguity is stated rather than hidden: a process whose intermediate
ancestor died while its task still runs is reparented past the live task —
the kernel offers nothing else — and is therefore indistinguishable from a
dead task's escapee. The sweep kills it too. Containment of untrusted
processes wins over a descendant whose supervisor is already gone.

## Capacity exhaustion and cross-worker recovery

A structured `capacity_exhausted` outcome is distinct from task failure, an
external task prerequisite, and a human decision. It is accepted only when the
executor/runtime positively classifies capacity exhaustion; Courier does not
infer it from HTTP 429 or provider text. The operator records a recoverable
`BlockedExternal` terminal diagnostic and starts a durable, idempotent handoff.

An outage without an authoritative exhaustion signal must strand nothing
(#327). Attempts that end without the provider ever serving a session id
crash into a dedicated availability budget, separate from the task-repair
continuation budget and backed off exponentially (capped, never a tight loop).
Exhausting it ends the run on the same recovery channel — the
`capacity_exhausted` outcome with the honest `provider-unavailable` diagnostic
category — never a quota verdict the evidence does not support, never
`Failed`, never NeedsHuman-for-model-outage. The other ending on that channel
names its own cause instead: a model/agent child that kept exiting non-zero
until its repair budget was spent carries the `model-transport-error`
category, so the durable diagnostic never tells an operator a remote provider
failed when the failure was local. A forge read that could not complete on a
transient availability condition joins the same channel as a `blocked_external`
declaration with the `forge-unavailable` category, and only that pairing opens
the workspace-safety evidence gate described under Terminal runtime
diagnostics. A raw child process status is
never an outcome declaration: exit `2`, which merely matches Courier's own
NeedsHuman exit constant, takes this same shared recovery path like any other
unclassified nonzero exit; only a verified outcome declaration may request a
human. Settlement, safety gating, and
cooldown semantics are identical to confirmed exhaustion: an unverified or
unsafe workspace still retains ownership.

`LaneProfile.spec.capacityGroup` optionally names lanes sharing an execution
capacity identity; otherwise the lane name is the identity. Confirmed
exhaustion persists a default 15-minute cooldown on LaneProfile status. Source
discovery and admission pause only for that identity; already-running work and
unrelated pools continue. The interval is an anti-hammering window, not a
provider reset estimate. Operators may inspect the `CapacityRecovery`
condition and LaneProfile `capacityCooldown` status.

Before settlement, the controller reads run pods through the uncached API
reader and fails closed if any topology writer is running, pending, unknown, or
terminating. A source may implement optional `CapacityRecoveryPreparer` and
`CapacityRecovery` interfaces: prepare supplies opaque source fencing and an
affirmative trusted safety proof; settle atomically applies it. Incomplete proof,
unsupported capability, or stale fencing retains ownership. Transient settlement
errors are retried with the same run-scoped idempotency key after controller
reconstruction. The fake contract exercises both resolve-issue and fix-PR paths,
including preserved branch/PR metadata; Dispatch implements the interfaces via the merged #1259 receipt-fenced endpoints. Issue settlement is fenced by the issued claim token; because a re-claim around admission rotates that token while an admission re-claim re-emits the rotated receipt onto the run, a settlement refused for a rotated or gone lease re-presents the claim once and retries against the live token — rotation is adopted, never treated as staleness, while ownership, human-hold, expiry, and closed refusals still stop settlement (#388). If its POST response is lost or cannot be confirmed, the adapter makes one bounded best-effort lookup in the latest 100 audit rows, matching an exact SHA-256 capacity receipt marker and issue identity, or the run-scoped settlement identity tag every receipt note now carries beside its token-specific marker (a settlement presenting no identity key stamps the marker alone, legacy-shaped) — so a handoff that landed under an adopted token replays from durable evidence on later passes whose receipt names only the original token, instead of adopting again under a freshly minted third token and posting a duplicate settlement (#395); a positive match replays success without another POST. This is not durable server-side idempotency: audit pagination/retention, malformed responses, or lookup failures leave the outcome unknown and ownership held for retry. PR-fix capacity settlement uses the issued per-handout receipt. Lifecycle task reports separately support payload-bound replay when given a stable idempotency key. Discovery and dedupe bypass history only for a
`BlockedExternal` capacity-exhaustion run with `CapacityRecovery=True/Settled`
and `LifecycleReported=True`; a deterministic predecessor-UID successor name permits stable source IDs while
retaining diagnostic history and active-run deduplication. Other, incomplete, and human
histories remain barriers. Retained terminal history is never deleted.

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
  state is observable **and whose deletion is not in flight** is not pod loss:
  the recorded exit goes through the exit-code mapping like any other
  termination. A **terminating** coordinator's recorded exit is never mapped
  to a run outcome — the deletion may itself have produced that exit (a
  wedge-reaped coordinator carries a cancelled-stream failure exit, or even
  exits 0 inside the shutdown window), so it is re-observed like any
  in-flight deletion, and once the object is provably gone the disappearance
  backstop takes the deferred charge as pod loss. The mirror case is decided
  with it: a coordinator that exited with a meaningful code and was deleted
  **afterwards** (node drain, manual delete) takes the same relaunch, because
  a timestamp ordering test cannot distinguish the two exits, so a deleted
  coordinator is untrusted and the resumed run re-reads the world. (#369)
  The cache is not trusted for
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
  on the detection reconcile the surviving round is fenced — the worker is
  deleted, broker ingress is disabled before the broker pod (HARNESS.md §3),
  and the broker pod is deleted — so the untrusted executor and the
  credentialed broker cannot outlive the missing supervisor. At the
  crashloop ceiling the same fence runs before the `NeedsHuman` hand-off
  terminalizes the run, because a fence deferred past the detection
  reconcile would never run on a terminal run; the replacement round that
  follows a below-ceiling detection is then re-provisioned by the existing
  launch path. (#215) An orphan with no pod to watch recovers on the next reconcile — for
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
   deferred until a live read shows the deleted object gone on **every** delete
   path that removes such a dead object — the disappearance backstop and the
   secure topology's broken-topology relaunch, which deletes the same inert
   control object for the same reason — so one physical loss is charged once
   and the replacement can
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
    object exactly like the backstop and terminalizes in the same reconcile,
    and `SecureControl`'s broken-topology relaunch preserves an unrecoverable
    control object the same way, so the `NeedsHuman` hand-off keeps the only
    record of the cause whichever path detected the loss (#106, #223).
   The wedge path's pod set — reap and startup grace — is the run's
   **coordinator** pod, identified by the coordinator container, never
   every pod bearing the run's label: a secure round's worker and broker
   are torn down by `SecureControl`'s fence, not reaped here. The wedge
   detection reconcile fences the surviving round exactly as the
   disappearance backstop does — below the ceiling before returning to
   `Claimed`, at the ceiling before the `NeedsHuman` hand-off, because a
   terminal run is never relaunched and a fence deferred past the
   detection reconcile would never run (#215). One wedge therefore charges
   one crashloop credit and tears the round down in one place: the wedge
   reaps the coordinator, `SecureControl` fences the rest. (#222)
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
  reaped — judged on the run's **coordinator** pod only, since a freshly
  provisioned worker or broker of a secure round says nothing about the
  coordinator's wedge (#222). Freshness is judged only on observable pod/run
  state — the operator
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

## Operator-initiated soft-stop

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

### Uncommitted failure work

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

### Failure evidence for dirty runs

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
   20-second operation deadline plus the bounded child-kill waits — the
   cancellation path's 10-second wait for the model's process group and the
   capped git reads' 2-second `WaitDelay` — inside the 45-second grace.
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
deadline plus the child-kill wait plus exit margin. The reads that back the
bundle are bounded by that same deadline, not by how long a git child keeps
its pipes: a grandchild outliving git (`git-remote-*`, askpass) adds at most
the capped read's 2-second child-kill wait inside the grace. Delivery allows
at most two attempts inside the deadline, then stops — no cross-reconcile
retry, no requeue; loss is recorded in a redacted `evidence.capture` event. A
capture the deadline itself aborts earns one fresh five-second manifest-only
delivery window inside the grace (2026-10-09 decision); a stalled intake does
not. Capture never changes a phase, exit code, or lifecycle report.

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
`Pending`, `Claimed`, `Running`, `Failed`, `NeedsHuman`, and
`BlockedExternal`, and rejects `Verifying`, `AwaitingReview`, and `Done`. The
admitted set is broad because
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
Role/RoleBinding rendered by the chart — deliberately not added to the
generated operator ClusterRole, whose ClusterRoleBinding would widen the
evidence verbs cluster-wide and attach them to installs that keep evidence
disabled. The residual: the generated ClusterRole already
carries `create/delete/get/list` on `secrets` cluster-wide for the secure-mode
launcher, so against a shipped install the namespaced evidence grant's marginal
verb is `patch`, which the intake uses to supersede a slot it already wrote
(`list` is required by the condition derivation and the AwaitingReview/Done
cleanup; both use uncached API-reader lists) — see Security and boundaries.
Encryption at rest is the deployment's cluster
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

### Terminal runtime diagnostics

A terminal phase and exit code alone cannot distinguish an executor crash, a
model transport error, continuation exhaustion, a verifier rejection, or a
Kubernetes/container failure. The operator therefore captures a bounded,
redacted `status.terminalDiagnostic` on the run **before pod cleanup**, so the
cause survives pod deletion and controller restart; #198/#200 dirty-worktree
evidence is a separate bundle pipeline and is not a substitute.

The executor emits a machine-readable `category` in its `COURIER_TERMINATION`
handoff; the operator classifies from the strongest available evidence and
never invents a cause. A concrete executor category wins; otherwise
causal Kubernetes/container failure evidence (`OOMKilled`,
`ContainerStatusUnknown`, authoritative eviction, scheduling, or node-loss
reasons; a `PodFailed` with no recorded terminated state and no valid handoff;
or a missing pod); otherwise a declared `needs_decision`/`blocked_external`;
otherwise a non-zero exit with no valid handoff is `executor-crash`; otherwise
`unknown`. Display allowlists for kubelet/pod reasons are broader than this
evidence predicate: `Error` and `Completed` are safe to retain for inspection,
but do not alone establish infrastructure failure; pod reason `Shutdown` is
display-only as well. Missing or
ambiguous evidence is `unknown`. Categories: `executor-crash`,
`model-transport-error`, `continuation-exhaustion`, `verifier-rejection`,
`kubernetes-failure`, `needs-decision`, `blocked-external`,
`capacity-exhaustion`, `provider-unavailable`, `forge-unavailable`, `unknown`.

Several endings share the recoverable capacity channel, and each names the
cause it can actually evidence, so the durable diagnostic points at the
dependency that failed instead of at whichever one shares the channel. A
confirmed structured exhaustion is `capacity-exhaustion`. A model provider
that never served an attempt — no session id was ever produced — is
`provider-unavailable`. A model/agent child process that kept exiting
non-zero until its repair budget was spent is `model-transport-error` — a
local execution-process failure, never a provider outage. A forge **API** read
that could not complete on a transient availability condition — a rate limit,
a server error, or a transport failure — is `forge-unavailable`: the world
was never observed, so the run ends as a `blocked_external` declaration on
the recovery channel, never as a `verifier-rejection`, which stays reserved
for worlds the observation actually read and found contradictory. The
git-side remote-publication read behind the same observation is not classified
that way yet: the git transport cannot yet tell an unreachable remote from a
command that ran and failed, so its failures stay a fail-closed
`verifier-rejection`. Only the exact pairing — a declared `blocked_external`
outcome carried by a validated termination handoff whose category is
`forge-unavailable` — opens the workspace-safety evidence gate; a
task-shaped `blocked_external` with any other category opens nothing and
never enters capacity recovery. A failed read never publishes a `Verifying`
success and never claims the pull request is missing.

The diagnostic carries the observed exit code, the executor-declared outcome
kind when any, an **operator-derived** short reason (a category label, the
kubelet's allowlisted `Terminated.Reason` and pod `Status.Reason`, and a
fixed override for the wedge-reap and pod-loss ceiling paths — never the
executor model text, and the kubelet/apiserver text is constrained to a
small allowlist of values the runtime itself writes), and the coordinator
container's identity and termination metadata (`podName`, `podUID`,
`containerID`, `image`, `nodeName`, `restartCount`, `terminationReason`)
for kubectl-describe-equivalent inspection after the pod is gone. The
reason is re-bounded to 1024 bytes and re-redacted through the defensive
credential table as a defense-in-depth pass on the operator-derived string,
and the `container.terminationReason` field is hard-capped at 128 bytes
(the CRD's `+kubebuilder:validation:MaxLength=128`) with the same
allowlist, so an overlong kubelet value cannot cause the API server to
reject the terminal status patch and block terminalization. The executor
model text never reaches the field by construction: a model's
`needs-decision` question, a `blocked-external` missing description, a
verifier fail-reason, or a bootstrap error string cannot land on the
durable status, because no path from the executor's `termination.Reason`
to the diagnostic's reason exists. User-facing explanations live on the
source issue/PR comment posted by the executor's terminal handler.
Capture is best-effort and set-once: the terminal transition captures when
the pod is present; the pod-loss/restart ceiling captures
`kubernetes-failure` with no fabricated container details when it is not. A
later reconcile never clears an already-captured diagnostic, and capture
never changes a phase, exit, retry, or source report. Operator workflow is
in [docs/terminal-diagnostics.md](./docs/terminal-diagnostics.md).

### Model-provider availability and allowance exhaustion

Provider allowance exhaustion (for example an HTTP 429 carrying a verified
plan-limit code) is a **recoverable resource-availability condition**, not a
code failure, a human decision, or a claim of no progress. The native harness
classifies it at its only structured model boundary — the LiteLLM-compatible
gateway — and routes it to a `blocked_external` ending so the work waits for
capacity and the source re-offers it, rather than burning retries, failing the
attempt, or parking it for a human. This section settles the behavior; the
first implementation covers the native gateway path, and the remaining scope
is named at the end.

**Classification.** `ClassifyProviderError` reads at most a bounded prefix of a
provider error body and the standard `Retry-After` header and returns a fixed
`ProviderAvailability`: `allowance-exhausted`, `transient-rate-limit`,
`authentication-failed`, `request-rejected`, `context-exhausted`,
`provider-unavailable`, or `unknown`. A 429 alone is **not** exhaustion: only a
structured signal (a provider error `code`/`type` on the configured allowlist,
or HTTP 402) confirms it. Unrecognized failures stay `unknown` and are never
folded into a known class. The classifier retains only the fixed class and, at
most, a short allowlisted code; the provider body, message, and any credential
it might quote are never retained or surfaced.

**Retry policy.** Transport failures, 5xx, request timeouts, and a plain 429
remain retryable in-process (bounded by the existing stream-retry budget). A
confirmed exhaustion, an auth rejection, a context-limit rejection, and a
request rejection are not retried in-process. A retryable failure whose
authoritative `Retry-After` exceeds the in-process bound (5s) is likewise not
spun on: the run ends recoverable and the source re-offers it. The harness
never invents a reset time; an absent or unparseable hint is reported as
unknown, not guessed.

**Recovery ending.** A **deferred** failure — a confirmed exhaustion, or a
transient failure with a reset hint longer than the in-process bound — ends the
run as `blocked_external`. The operator already maps exit 2 plus the
`blocked_external` outcome to `PhaseBlockedExternal`, which reports the
source-neutral `blocked-external` lifecycle result (Dispatch `waiting-external`
plus the generation-fenced `WAITING_EXTERNAL` mark); that path is recoverable,
does not park the item for a human, does not erase failed attempts, and keeps
the item re-offerable. A provider outage (5xx) that exhausts its in-process
retries stays an infrastructure failure for the operator's relaunch path, and a
genuine task blocker is unchanged.

**Scoping and fallback.** Classification is per role-bound model binding: an
exhausted coordinator binding fails the gate for that run without disabling
unrelated lanes, profiles, or bindings. At startup the capability probe carries
the classification, and a required model-binding gate failure that is a
confirmed exhaustion declares `blocked_external` instead of `NeedsHuman`.
Courier adds no automatic fallback, no silent model switch, and no paid
capacity: a compatible fallback is a future, explicitly configured behavior.

**Configuration and diagnostics.** The default exhaustion signals are
provider-neutral, OpenAI-compatible tokens. A deployment extends them (never
replaces them) with `COURIER_MODEL_EXHAUSTION_CODES` and
`COURIER_MODEL_EXHAUSTION_TYPES` (comma-separated); Courier core hard-codes no
provider policy. Diagnostics expose the fixed class and the allowlisted code
only — never the response body, credentials, or an unbounded string as a label.

**Unknowns and remaining scope.** The legacy OpenCode executor delegates model
calls to an external binary and has no structured provider signal, so it is not
yet classified here; until it is, its exhaustion still consumes continuations
and can end `NeedsHuman`/`failed`. In-run long cooldown (waiting out a reset
inside one incarnation) and honoring a source-side cooldown are not
implemented: recovery today is source re-offer after the blocked-external
ending. Gateway retries that happen below Courier (LiteLLM/SDK) are outside its
observation and are not counted. The control process reads the
`COURIER_MODEL_EXHAUSTION_CODES`/`_TYPES` extension signals when the deployment
projects them; wiring the operator-rendered control pod to a deployment
setting is follow-up, so today the provider-neutral defaults apply. (#320)

### Toolchain reference for bootstrap lanes

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

### Repository development bootstrap execution

Courier executes an opt-in, repository-owned development bootstrap after
checkout and before model work. The contract is the convention settled with
the per-repo implementations (#416 and the linked repos): `.mise/config.toml`
plus a committed `.mise/mise.lock`, executed as `mise run bootstrap`. The
runtime owns four decisions, shared by the integrator, the secure harness,
and the legacy executor's classification through `internal/devenv`:

- **Trust rule.** The contract is approved only when the checked-out
  `.mise/config.toml` and `.mise/mise.lock` are byte-identical to those at
  the run's operator-approved base revision, read with hardened git reads
  (`git cat-file`) at the resolved head and base commits — never from a
  working tree, never by executing anything to decide. The committed
  lockfile is part of the contract, not an optional companion: a config
  whose lock is absent or unprovable at either revision is unapproved and
  skipped, because without the lock the tool versions a bootstrap
  installs are unpinned, and a repository that has committed a config
  has a contract rather than none. Every compared contract path must be a
  regular blob: a symlink (120000) or a gitlink
  (160000) at either revision is unprovable and fails closed. The
  comparison is the APPROVAL GATE only: a contract modified in a PR is an
  untrusted modification and is skipped, never executed. What actually
  executes is never the head's bytes — the secure worker materializes the
  approved base revision from the git object store (`git archive` to a
  temporary tar, extracted only after the command succeeds, so a failed
  archive can never leave a partial tree behind a successful extractor),
  verifies the contract file exists in the materialization, and runs
  `mise run bootstrap` inside it — so the executed bytes are the approved
  base revision's bytes BY CONSTRUCTION, and mise's task discovery (config
  fragments, `conf.d`, task directories, includes, whatever surfaces it
  reads) cannot reach head content. No enumeration of what mise might
  discover is attempted, because none can be complete; enumeration-based
  pinning of referenced scripts was tried and removed as unsound. On
  success the task transfers the materialization's repository-local `bin/`
  outputs into the workspace checkout: a contract whose bootstrap installs
  tools under the repository root's `bin/` (Courier's own `hack/bootstrap.sh`
  writes `bin/controller-gen`, `bin/setup-envtest`, and `bin/.tool-pins`)
  would otherwise produce them inside the temporary materialization its EXIT
  trap deletes, leaving every later workspace command to reinstall them.
  The transferred bytes are outputs of the approved base's own bootstrap —
  the same trusted chain as the executed bytes — a failed bootstrap
  transfers nothing, and nothing is copied from head.
  Repositories without a contract are skipped (documented no-contract
  behavior, not a failure); so are a contract that differs from the base
  and one that is only half-committed (a config with no lock). All three
  skip classes dispatch nothing and are never executed.
- **Bounded setup.** The secure harness dispatches the bootstrap task
  (`mise run bootstrap` plus its fixed diagnostic lines, emitted exactly
  once from the script's EXIT trap — the terminal emission that runs last
  on success and on every failure path) and the environment capture
  (`mise x -- env -0`) as TWO
  separately dispatched worker operations, each under its own fixed liveness
  bound (15 minutes) — never one shared budget, so a hung bootstrap cannot
  consume the capture's liveness too. On expiry, control classifies the
  expiry from the stage's own context and reconciles the operation through
  cancellation on a fresh bounded context that ignores the expired caller
  context — a reconcile issued on the expired context could never reach the
  worker, and the bound must still DELIVER the worker's process-group kill
  and observe the result before the run ends blocked-external with a
  structured reason (fixed text naming the stage and the exit code only —
  untrusted worker output never enters a durable reason), never a
  model-visible retry loop and never a model scavenger hunt for tooling.
  The expiry is delivered wherever it lands — inside an in-flight poll or
  inside the poll wait itself: a bound that expires while control merely
  waits between polls must still reconcile the running operation through
  cancellation, because the worker answers "running" immediately and only
  an in-flight poll would otherwise carry the kill.
  A termination that was never OBSERVED is not an environment blocker:
  repository-authored work may still be executing, so the run must never be
  reported blocked-external over it. The stage's active-operation entry
  stays retained, the worker is fenced against every further dispatch, and
  the failure fails closed through the ordinary error path carrying the
  unverified-termination fact. The fence for a terminal observation that
  still carries live descendants is set centrally where the terminal result
  is verified, so every caller of the dispatch-and-await path — not only the
  bootstrap stage that re-fences on the sentinel it receives — stops further
  dispatch against a worker whose task tree still holds live work. Every
  path that can leave a termination
  unobserved produces that fact: a reconciliation that failed, one that
  observed work still live in the task tree or an operation the worker
  still reports as running, and an ordinary observation of a task tree that
  outlived its task. Only an independently observed termination — the worker
  no longer knows the operation, or a terminal observation with an empty
  process tree — may classify a bound expiry as the environment blocker it
  is.
  The environment capture is size-capped at the source (64 KiB, paired with
  the `Task.Env` dispatch bound and the worker's 64 KiB stdout tail): an
  oversized capture fails the task honestly with a nonzero exit, never a
  truncation.
- **Deterministic environment.** The captured post-bootstrap environment is
  applied so every subsequent execution inherits it without interactive
  shell activation. Protected keys — Courier's control plane (`COURIER_*`),
  git plumbing (`GIT_*`), dynamic-loader injection (`LD_*`, `DYLD_*`),
  `IFS`, `SHELL` — are never overridden; `PATH` deliberately is, because
  deterministic PATH replacement is the point. The secure harness attaches
  the filtered capture to model-influenced worker tasks (`Task.Env`), and
  the worker merges it over its own environment with the same filter —
  fixed control scripts (unpack, pack) never carry it, so a poisoned PATH
  cannot redirect the trusted git plumbing.
- **Boundaries.** The legacy executor pod is credential-bearing and NEVER
  executes repository contract content: it classifies the contract with
  pure git reads, and an approved contract there is skipped with an honest
  receipt naming exactly that — execution happens only in the secure
  worker's credential-free context, which honors the existing
  dependency-egress policy (no bypass and no extra grant; a bootstrap that
  cannot reach its pinned downloads fails as the environment blocker
  above). Receipts carry no secret values and no raw worker output: the
  legacy executor emits a `workspace.bootstrap` structured event with a
  fixed skip reason; the control pod emits a structured `workspace.bootstrap`
  receipt (status, failure reason, per-task elapsed time, exit codes,
  environment-key count, and the mise executable path, version, and cache
  location) through the same best-effort event emitter as session
  compaction, with diagnostic values validated to a fixed key set and a
  printable-ASCII charset.

mise provisioning for the runtime images is #415's coordinated work; until
it lands, an approved contract on an unprovisioned environment ends
deterministically blocked-external naming the missing mise executable. The
same holds for the tool artifacts a contract installs: the secure worker's
dependency-egress policy admits no arbitrary fetch, so a secure-mode
contract is expected to be pre-materialized in the provisioned image —
runtime downloads are not an affordance either topology grants, and their
absence is the same honest blocker, not an egress bypass.

Two cost/trust properties are deliberate. The secure workspace is
re-materialized from a fresh snapshot for every coordinator shell command
and every brief, and each unpack wipes untracked workspace state with
`git clean -qfdx` — so the bootstrap runs after every unpack, and its
idempotency plus out-of-workspace caches (user-scoped installs) are what
keep repeat runs cheap; a workspace-local install that must survive is the
contract author's problem, not the runtime's. And the executed bytes are
the approved base revision's bytes by construction — the worker
materializes the base revision from the git object store and runs inside
it — while the approval gate was computed from control's tree at head and
base: control dispatches only at an approved state, and the captured
environment never flows back into a trusted process. Approval covers the
compared contract paths as regular blobs at the resolved commits — a
symlinked or substituted contract path fails closed instead of being
compared through its target — and the base materialization's `test -f`
proves the contract file is really present in what will run. The
materialization assumes the base revision is reachable from the dispatched
snapshot's work ref — true because integration is built on the base; a
base whose objects are absent from the worker's store fails closed at the
materialization guard, never runs something else instead.

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

**Source-agnostic telemetry and health semantics (#290).**
[docs/observability-signals-and-health.md](./docs/observability-signals-and-health.md)
specifies Courier's own signal catalogue and health model: a source-neutral
`courier_source_*` discovery layer that exists even when no `CoderRun` is
created, so an idle empty queue, a failed discovery loop, and unavailable lane
capacity are distinguishable; lane capacity and reservation signals (including
the reservation age that makes staleness observable); bounded run phase,
execution-outcome, lifecycle-report, and retention/reaping signals; and a state
model that separates executor completion from external disposition and renders
unknown/stale/absent as states distinct from idle or healthy. It defines units,
clocks, counter/gauge reset behaviour, and a bounded-label privacy budget that
keeps run ids, issue/PR numbers, repo names, arbitrary error strings, and secrets
out of labels, with per-run correlation left to the structured log. #291 owns
the connector diagnostics and #292 the views and alerts that consume these
signals; the thresholds and windows are design candidates, not defaults, and no
implementation, exporter, dashboard, or alert rule is authorized here.

**Connector diagnostics without core coupling (#291).**
[docs/connector-diagnostics.md](./docs/connector-diagnostics.md) specifies
adapter-owned diagnostics for optional source connectors so an operator can see
protocol pressure — exclusion overflow, acknowledgement support/mismatch,
protocol errors, superseded identities, provider availability — **without**
Courier core learning a connector's name or its limits. Core keeps a
source-neutral signal layer (`courier_source_*`); each connector declares a
bounded capability set (mirroring the optional `internal/source` interfaces) and
reports values inside one generic `connector.diagnostic` envelope, with the
configured cap carried as **data** (`courier_connector_exclusion_limit`), never
as a core constant or label. A dashboard enumerates connectors from
`courier_connector_capability_info` and renders a panel only for declared
capabilities, so a non-Dispatch connector needs no Dispatch panel and Courier
works with an alternate source. Absent and unsupported diagnostics are distinct
from success, core and connector counters are joined rather than summed, labels
stay bounded, and trace correlation is optional and never a metric label. §11
of that document names the bounded-label, duplicate-counting, privacy, and
correlation assertions the instrumentation must satisfy; the unit-level subset
of those assertions runs as `go test` checks today (the envtest no-double-count
fixture, the AST-walk free-form-label check, and the scrape-time images-job
invariants remain the contract for later follow-ups). The
envelope is implemented in
`internal/connector`, and the Dispatch and manual adapters declare their
capability catalogues and emit their diagnostics through it at construction and
at the exchange classification sites in `internal/source/dispatch`. Core wiring
is generic — the operator builds one emitter on the manager's metric registry
and the shared run-event log and passes it to the adapters; the connector
names, reason vocabularies, and capability states belong to each adapter. The
design records the #286 exclusion-overflow outage as the
motivating case while leaving the overflow *strategy* to #286 and retention to
#287; #290 owns the source-health semantics and #292 the views this layer feeds.

**Portable views and alerts (#292).**
[docs/observability-dashboards-and-alerts.md](./docs/observability-dashboards-and-alerts.md)
specifies the backend-neutral dashboard information architecture and alert
catalogue that render the signals above: source health, lane capacity, run
execution and outcomes, lifecycle reporting, retention, and optional connector
panels. It separates the portable specification from optional executable
dashboard/rule assets, names the evaluator an OTel-based integration must
supply (OTLP is transport, not an alert engine), and records the
instrumentation gaps — no discovery, reservation, lifecycle-report, or
retention metrics — that a later instrumentation decision must close before
those views exist. #290 owns the signal/health semantics and #291 the connector
diagnostics this layer consumes; neither is required for a Courier dashboard to
be useful standalone.

### Per-run telemetry

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
  phase: Pending | Claimed | Running | Verifying | AwaitingReview | HandedOff | NeedsHuman | Done | Failed | BlockedExternal
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
  resources:                       # optional CPU/memory sizing for this lane's pods
    requests: {cpu: 500m, memory: 1Gi}
    limits: {cpu: "4", memory: 8Gi}
```

`runtimeImage` is an optional execution-environment selector. It is an image
capability seam, not a Go or Kubernetes requirement: a lane can select a
repository-specific image containing whatever tools its workflow needs, while
the deployment default remains the minimal bootstrap image.

`resources` is the lane's pod sizing, and the only source of the lane-sized
pods' sizing. The lane's vocabulary is a deliberate subset of
`corev1.ResourceRequirements`: only `requests` and `limits` are expressible,
because no Courier renderer sets `pod.spec.resourceClaims`, so a lane cannot
configure a value Courier would ignore or that would only fail at pod
admission (#403). Every requests/limits key is honoured, subject only to the
CPU and memory request floor described below: extended-resource keys pass
through to pods and ride normal Kubernetes scheduling and quota semantics,
where Kubernetes defaults an extended resource's request to its limit — a key
set in limits alone is valid and takes that request, a key set in both maps
must carry equal quantities, and a key set in requests alone makes every pod
on the lane invalid at admission. Limits are used exactly as written and may
be omitted; requests keep whatever the lane configured, but CPU and memory
requests are floored to Courier's built-in default (250m/512Mi requested,
2 CPU/4Gi limited) wherever they are absent or non-positive — so every
Courier-created pod, the coordinator and the secure topology's control,
worker, broker, and probe pods, carries finite CPU and memory requests
whatever the lane says, which lets the scheduler bin-pack, keeps one runaway
build from taking a node, and keeps these pods off the front of the eviction
queue. Two deliberate exceptions do not follow the lane. The preflight probe
pod's sizing is fixed (10m CPU / 32Mi memory requested, 500m CPU / 256Mi
memory limited), because preflight creates one probe per eligible node
simultaneously and a seconds-lived network check reserving lane-scale capacity
can stay Pending on a request-tight cluster and wedge preflight; the fixed
finite requests keep it schedulable and never BestEffort, and the fixed
generous limits keep it admissible where a ResourceQuota tracks limits.cpu or
limits.memory — such a quota rejects any container missing the matching
limit, and the probe image's real usage (a shell doing DNS and network
checks) peaks in the tens of MiB, far under the limit, so the limit cannot
OOM-kill the probe and falsify the isolation result it exists to produce.
Likewise the workspace-observer sidecar and its seed init container on the
coordinator pod always carry the fixed small `DefaultObserverResources`
(25m/64Mi requested, 1 CPU/512Mi limited) whatever the lane says: they are
Courier-internal helpers — baseline capture and workspace observation — and
the lane sizes the work a model does, not the helper that hashes the
workspace, so sizing them from the lane would multiply the pod's scheduler
reservation for processes that never grow with it. Unsized, they were the one
container class a fully-sized lane could still leave unbounded on a node.
A lane that configures neither half gets the default whole; a limits-only lane
gets the default requests; a requests-only lane is rendered with exactly its
own requests plus the floored halves, so its pods
really do run limit-less by its own choice. Zero or negative CPU/memory
quantities are rejected before launch rather than floored into silence:
`ValidatePodResources` fires from both launch paths on a lane that declares
them, terminalizing the run `NeedsHuman` with reason `LaneConfig` before any
pod is created — a lane's sizing is an operator decision, and an operator who
wants the default omits the key rather than zeroing it (the render-layer
floor still backs callers that bypass launch validation). One consequence: a
limit below the floored default request (say `limits.cpu: 100m` on a lane that
sets no requests) renders requests above limits, which the API server rejects.
Requeueing cannot change a permanently invalid spec, so the rejection is caught
where it happens and the run stops rather than cycling claim, preflight, and
release on every reconcile. The secure path's dry-run proof is the primary
catch: it admits
the control, worker, and broker pods, and the probe pod too on the passes that
create probes live, so no pass creates a spec it has not proved. The secure
workload creates behind that proof return ordinary errors and rely on it; the two
creates a dry run cannot stand in for — the live probe create and the legacy
coordinator's own create — classify an `Invalid` where it happens. That class,
`PodAdmission`, terminalizes: the reconciler ends the run `NeedsHuman`
from both `LaunchSecure` call sites, recording `SecurePreflightFailed`, and from
the legacy launcher, which carries the cause in the run's terminal diagnostic
instead. A lane-sized pod in that class names something the operator can change;
a probe pod in it does not, because its sizing is an implementation constant, so
its rejection implicates the probe image or the cluster rather than the lane —
which is why the probe is in the class at all, and stopping is still right
there. One more verdict terminalizes: `ProbeIdentity` — the pod a node's
probe name resolves to is pinned to another node, so whatever it observed is
not evidence about the node under test. Its persistent causes — a 32-bit
digest collision in the probe tag, which re-pins the one shared pod to the
last node probed on every pass, or a foreign pod holding the name — outlast a
requeue, and the read cannot tell them from a stray pod a later pass's
delete-and-create would clear, so the run stops and names the probe and both
nodes rather than cycling probe phases indefinitely.
Every other preflight verdict keeps the claim/release requeue, because
each of them clears on a later pass — a live probe can outrun the dataplane
programming of the NetworkPolicy the same reconcile just wrote, and an access
review can read an informer cache that has not yet caught up with a Role created
moments ago — so terminalizing them would strand a run that was always going to
succeed. The secure round is fenced before it terminalizes: a resumed launch
reaches the launcher with a live worker and a credentialed broker, and a
`NeedsHuman` run is no longer observed, so a fence deferred past that reconcile
would leave the untrusted executor running and the broker holding credentials
until the run's retention window ends. A fence that fails does not terminalize —
the claim is released and the next pass retries it — and the legacy path needs no
fence at all, since the create that failed left no pod to tear down. The sizing is
resolved once per launch pass, so the spec the dry run admits and the pods
created afterwards are one artifact rather than two reads of a lane that may have
moved. One lane-level value sizes every lane-sized pod on the lane; per-role
sizing is deliberately not modelled, and the probe pods above are the one
role-level exception.

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
  in-progress. A launch whose create collides with an already-occupied
  coordinator pod name (`AlreadyExists`) is verified before it may count as
  success, against the API server's current state rather than the informer
  cache: the collision resolves to an idempotent, no-second-charge launch
  **only** when the existing pod carries a controller `OwnerReference` whose
  UID is the run's and is not being torn down. Every other shape — a failed
  verification read (NotFound, transient, Forbidden), a terminating pod, or a
  pod not bound to this run's UID (foreign, or a live coordinator left by a
  deleted-and-recreated same-name run) — surfaces as **provisioning in
  flight**: the run stays **Claimed** and requeues, and `Running` is never
  written over a slot not verified as a live, run-owned pod. The wait rides
  the pod's delete event with a bounded requeue as the backstop; no wall-clock
  bound is introduced. This mirrors the secure topology's in-flight signal for
  a terminating control pod rather than inventing a second shape (#217).
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
  Exit `2` reaches **NeedsHuman** for a `needs_decision` declaration and the
  recoverable **BlockedExternal** phase for a `blocked_external` declaration;
  exit `3` (`no_change_needed`) reaches **AwaitingReview** for resolve-issue and
  is settled against the world through **Verifying** for fix-pr.
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
  no deadline. Observer errors remain Verifying and requeue. The poll reads the
  run head's pull requests through the identity-checked forge listing, so a
  record whose base or head repository identity differs from the run's — a
  same-owner fork claiming `owner:branch` — and a response carrying no
  repository identity at all both fail the poll closed: they are the same
  unverifiable world as a read error, never a "no pull request" claim
  published as this run's state (#365). A missing observer
  or missing/draft PR reaches **NeedsHuman**. A failed check on a fix-pr run
  reaches **Failed** so its source can apply its retry policy. A resolve-issue
  run whose published PR has stable, confirmed red checks reaches **HandedOff**
  rather than sitting in **Verifying** forever: the PR was published but
  external verification did not pass, so the source's independent PR-repair
  mechanism takes over using the carried PR identity. Queue-backed PR-fix
  sources issue another attempt under their existing cap. A PR with no checks or
  pending checks remains **Verifying**; the PR is persisted.

  Green is declared only from **two consecutive all-green observations of the
  same check set**: every all-green observation records a compact fingerprint of
  the check identities (head commit plus sorted check names) on
  `status.checkFingerprint`, and an observation may transition to
  **AwaitingReview** only when it matches the previous all-green observation's
  fingerprint. Any pending or empty observation clears that recorded candidate —
  checks that have not registered yet can still appear at any later poll, so a
  pending observation can never pre-settle an identity — and a changed set (a
  new check, a new push) resets it the same way. A partial snapshot cannot pass.
  The source becomes `in-review` with the transition.

  Red settles symmetrically. A fully-failed observation records the compact
  identity of the check set on `status.failedCheckFingerprint` — the same head
  plus sorted check names, independent of check state — and a **second
  consecutive fully-failed observation with the same fingerprint** moves a
  resolve-issue run to **HandedOff**. A pending or empty observation clears both
  candidates; a changed check set resets the red candidate; an all-green
  observation clears the red candidate and starts or continues the
  two-consecutive-green settle. A red check combined with any pending check is
  not fully-failed and does not settle. On settle the run preserves `status.pr`
  and the head/check identity and publishes **no source `State` transition** —
  neither `in-review` nor `needs-human` — and exactly one lifecycle report:
  `handed-off`, carrying the observed PR and the reason `external verification
  failed`, under the run's stable `coderun/<namespace>/<name>/HandedOff`
  idempotency key, with the run UID appended when set. A failed report leaves the
  run **HandedOff** and is retried on
  a later reconcile with the same key. Verifying does not consume LaneProfile
  execution capacity; the source remains `in-progress` while polling and
  publishes `in-review` after stable green observations. Fix-pr attempts receive
  `failed` if checks turn red; a resolve-issue run hands off to **HandedOff**
  instead.
- **AwaitingReview** is terminal for this run. For a PR, human merges → a slow,
  bounded world observation (#250) reads the run's **exact recorded PR identity**
  — base repository plus `status.pr` number through the forge read-pull-request
  capability, head repository and ref from `status` — and only a merge the forge
  confirms on that identity moves the run to **Done** and resolves the source;
  an open PR, a forge error, a missing PR, or a PR whose head identity no
  longer matches keeps the run in **AwaitingReview** and retries at the slow
  cadence. An exact PR closed without merge instead transitions the run to
  **NeedsHuman** with a durable `PullRequestClosedUnmerged` condition (#277).
  That read is bounded per request (per page for a paginated walk), not per
  run: the GitHub adapter's default transport carries a finite 30-second request
  bound covering the header wait and the response-body read, and an earlier
  caller cancellation or deadline stays authoritative, so a stalled forge fails
  the read instead of holding a reconcile — or an unrelated run — open until
  process shutdown (#341).
  feedback/conflict → the source spawns a fresh `fix-pr` run without reusing the
  previous run. A `no_change_needed`
  resolve-issue run also waits here with its evidence posted for human review;
  Courier does not resolve that source from the coordinator's declaration. A
  `no_change_needed` **fix-pr** run instead waits in **BlockedExternal** on an
  open green PR: neither CI nor a clean workspace proves review feedback was
  addressed. Only a verified merge can settle that attempt successfully. A
  retained pre-migration AwaitingReview no-change report also fails closed to
  `waiting-external`, without claiming a push or bypassing the head guard.
  The previous run remains auditable; its completion does not settle later feedback.
  **HandedOff** is the separate terminal phase for a published resolve-issue PR
  whose stable red checks the source will repair; it is not a success, so it is
  not reaped like **Done** and does not publish `in-review`.
- **Reap:** Done runs are deleted after their window; Failed and NeedsHuman runs
  are reaped after their (seven-day default) window and only once the lifecycle
  report has reached a positive, settled state (never on a missing, `Unknown`, or
  still-`False` report). The reaper always retains the most recent
  `Failed`/`NeedsHuman` runs per repository (default five), counting only the
  phases the floor governs: Done runs are exempt because their source work is
  already resolved so the five-minute window is the whole retention, and the
  never-reaped phases cannot drain the quota. It emits
  `courier_coderuns_reap_requests_total` (accepted delete requests) and
  `courier_coderuns_oldest_terminal_age_seconds` (what is still retained).
  AwaitingReview is kept until the world moves it; HandedOff and BlockedExternal
  are still kept (not reaped) — HandedOff is deleted on request, and
  BlockedExternal stays recoverable so its source can re-issue it; see
  [Terminal retention and closed-unmerged PRs](#terminal-retention-and-closed-unmerged-prs-251).

**Terminalization writes the run's own phase before it reports to the source.**
A rejected or failed source report never holds a run in `Running`: the phase is
written first, which releases lane capacity, and only then is the lifecycle
report published — on its own, retried via a bounded requeue and deduplicated by
the run's stable idempotency key. A Dispatch generation-mismatch conflict means
this attempt was superseded by a newer generation of the same work, so the report
is dropped rather than retried (the newer generation belongs to another run) and
the run still terminalizes; a transient report failure leaves the run in its
terminal phase with the report retried on a later reconcile. (#178)

## Terminal retention and closed-unmerged PRs

**The leak:** `reapDone` (`internal/controller/reap.go`) deletes only resolved `Done` runs after a default five minutes. `HandedOff`, `NeedsHuman`, and `Failed` are never reaped: `Reconcile` (`internal/controller/coderun_controller.go`) revisits a terminal phase only to retry a pending lifecycle report and otherwise returns without a timer. `AwaitingReview` was part of the same gap — the controller never re-observed a PR after a run settled — until #250 added the slow, bounded read of the recorded pull request that turns a confirmed merge into `Done`; a PR closed without merge still keeps its run indefinitely, which this design fixes below. The #179 dogfood deployment left 26 `AwaitingReview`, 6 `NeedsHuman`, 1 `Failed`, and 17 retained coordinator pods. `source.Runner.Poll` (`internal/source/runner.go`) lists every CoderRun on every poll to build its dedupe set, so the list grows without bound.

**Retention clock:** Retention is a bounded inspection window keyed to a durable, **per-phase "reap-eligible" annotation** — not `FinishedAt`, not `CreationTimestamp`, and not the source report condition alone. `FinishedAt` records the run's execution end and is set-once on the first terminal transition (`AwaitingReview`, `HandedOff`, `NeedsHuman`, or `Failed`); it must remain immutable so a later transition (notably the months-late `AwaitingReview → NeedsHuman` from a closed-unmerged PR) does not distort `RunDuration` or the merge-to-`Done` semantics, and so `Done`'s `RunDuration` is always the work's wall-clock cost rather than the reaper's view of it. Therefore `FinishedAt` is **not** the retention clock for `NeedsHuman`/`Failed`. The controller instead writes a new set-once annotation when the run becomes reap-eligible for that phase: `courier.misospace.dev/needs-human-at` on entry to `NeedsHuman` and `courier.misospace.dev/failed-at` on entry to `Failed`, each an RFC3339 timestamp stamped once after the lifecycle report reaches a settled state (see Deletion gates). For a direct `Running`/`Verifying → NeedsHuman`/`Failed`, the annotation is written on the first reconcile after the lifecycle report settles — the reconcile immediately after `FinishedAt` is set — so the window starts at the actual end of execution; for a late `AwaitingReview → NeedsHuman` (the closed-unmerged case), the annotation is written on the first reap-eligible reconcile after the close-unmerged transition's report settles, **not** the original `FinishedAt`, so the seven-day window is the operator's inspection window for the new outcome, not a zero-length residue of months-old execution time. `Done` keeps its existing separate `courier.misospace.dev/done-at` annotation (`doneAtAnnotation`) because a `Done` transition can occur long after `FinishedAt` (and the source `Resolve` precedes the marker, not the phase change). A missing or malformed marker starts a fresh window and never deletes on the same pass; a future-dated marker (clock skew) is capped at the window and never deletes early — the same fail-safe rules `reapDone` already applies. `AwaitingReview` is deliberately outside this window: it is a success ending awaiting a human merge, not a terminal failure, so it is kept until the world moves it — #250 resolves a merge to `Done`, and this design resolves a closed-unmerged PR to `NeedsHuman`; only then does a retention window start. A PR that stays open keeps its run, which is the run's live-work record and the human gate. The annotations live on the object, so controller restart re-derives the remaining window from the durable marker without re-stamping, re-arming, or distorting `FinishedAt`.

**Defaults and deployment knob:** `Done` stays five minutes. `Failed` and `NeedsHuman` default to 168 hours (seven days), measured from each run's per-phase annotation (`courier.misospace.dev/failed-at` and `courier.misospace.dev/needs-human-at`) rather than from `FinishedAt`, so a late `AwaitingReview → NeedsHuman` transition starts a fresh window. Expose four manager flags (`--done-retention`, `--failed-retention`, `--needs-human-retention`, `--terminal-audit-keep`) and a chart `retention:` block (`done`, `failed`, `needsHuman`, `auditKeep`) rendered into `controllers.main.containers.main.args` like the existing `evidence:` block. `CoderRunReconciler` carries `DoneRetention`, `FailedRetention`, `NeedsHumanRetention`, and `TerminalAuditKeep` fields with zero-falls-back-to-default semantics; a negative `TerminalAuditKeep` disables the audit floor. **The audit floor's candidate set is exactly the phases it governs:** a run counts toward — and is protected by — the floor only in `Failed` or `NeedsHuman`. `Done` runs are exempt from the floor (their source work is already resolved) and `AwaitingReview`, `HandedOff`, and `BlockedExternal` are never reaped at all, so counting either as a newer entry would let a burst of short-lived Done runs, or one permanently held run, drain the protected quota and permanently delete the runs the floor promises to keep. **The metric contract is a request, not a removal:** the reaper increments `courier_coderuns_reap_requests_total{phase}` on every accepted delete request — the API server accepts the delete on a secure run while the topology finalizer still holds the object — and the scrape-time `TerminalAgeCollector` reports `courier_coderuns_oldest_terminal_age_seconds{phase}` so an operator can see what is actually retained (a finalizer-held object stays on the gauge) and whether anything is aging past its window (a stuck report or an audit-floor hold).

**Closed-unmerged PRs:** A run terminalizes as `NeedsHuman` with a precise reason; no new CRD phase. Today a `Verifying` run whose PR is closed without merge observes an empty `PRObservation` and transitions to `NeedsHuman` (#146); the gap is a PR closed after the run already settled `AwaitingReview`, which is never observed. The slow, bounded world observation added for #250 (merged -> `Done`) classifies the run's exact recorded PR identity as: merged -> `Done` (existing resolve + reap); open -> stay `AwaitingReview`; closed without merge -> `NeedsHuman` with a new `PullRequestClosedUnmerged` condition (`Status=True`, reason `ClosedUnmerged`) whose message carries the recorded PR identity, under the existing needs-human source policy (a `blocked` lifecycle report; the source parks the item and owns retry/escalation); absent, ambiguous, or a transient forge error -> no transition, stay `AwaitingReview`, and retry the observation later. A missing or unreadable PR is never treated as closed. The `NeedsHuman` transition publishes a `blocked` source lifecycle, and the per-phase `courier.misospace.dev/needs-human-at` annotation is stamped by the reaper on the first reap-eligible reconcile after that report settles (not derived from the original `FinishedAt`), so the seven-day inspection window starts at the operator-visible close event. No new CRD phase: `Done` would falsely attest shipped work, and a bespoke `Closed` phase is unnecessary because an existing non-successful phase already covers it. Implementation hook (#277): the `AwaitingReview` observation branch reads the run's exact recorded pull request through the configured forge `read-pull-request` capability (`PullRequestReader` / `forge.PullRequest`, the same #250 seam), where `State` distinguishes a closed pull request from an open or absent one; no `PRObservation` signal is needed, because the `Verifying` path already maps a missing pull request to `NeedsHuman`.

**Deletion gates:** A terminal run is deleted only when all hold: (a) phase is `Done` (existing path) or `Failed`/`NeedsHuman` past its per-phase annotation window; (b) the lifecycle report has reached a **positive, settled state** — the run carries a `LifecycleReported` condition with `Status=True` and a permitted reason (`Published` or `Superseded` — the only reasons the condition's `Status=True` carries; `Accepted` belongs to the separate `LifecycleReportSupported` capability condition); a missing, `Unknown`, or `False` condition does **not** satisfy this gate, because a legacy, partially-upgraded, or corrupted terminal CoderRun with no published/superseded report proof must not be deleted (it is the only durable retry intent and a transient `Unknown` does not prove success); a superseded report means a newer generation owns the item and the older run is reclaimable; (c) the source report reached settlement: for `NeedsHuman` and non-CI `Failed` that is a `blocked` transition, which parks the source item so `source.Runner.Poll` will not re-offer it; for `Done`, `Resolve` (source `done`) precedes the clock; (d) owned resources: evidence Secrets and coordinator pods are owner-referenced to the run and are garbage-collected with it, so the retention window is also the evidence inspection window. Operators who need longer raise the TTL before it expires or retrieve evidence out-of-band (label `courier.misospace.dev/evidence=<run>`). No separate evidence clock or sweeper is added; this aligns #198/#200/#202 without duplicating them. The implementation must include regression tests for the legacy missing-condition case, the `Unknown` capability case, the still-`False` (pending) case, the `True-Published` case, the `True-Superseded` case, and the controller-restart case (gate evaluation is derived from the condition and the per-phase annotation on the object, not process memory).

**No accidental resubmission:** The runner dedupes only against currently-existing CoderRuns. Reaping a `Failed`/`NeedsHuman` run whose source report is positively settled is safe because the source, not the CoderRun, is the dedupe authority: the blocked report parks the item. If a source later re-offers the same identity (for example a human unblocks it), that is a new, intended dispatch that materializes a fresh CoderRun, not a duplicate of the reaped attempt. Deleting before the report reaches a positive, settled state is forbidden by gate (b). No automatic retry loop is added to fix retention; retry and escalation stay source-owned.

**Covered cases:** controller restart (the per-phase annotation and the `LifecycleReported` condition are durable on the run, so a new process recomputes the remaining window from the object without re-stamping, re-arming, or distorting `FinishedAt`); absent PR (no transition, no annotation, no clock start); transient source/forge failure (no transition, no early reap); stale generation (a superseded report satisfies the settled-state gate and the newer run dedupes the identity); re-discovery (after a reaped run, a source that still offers an already-blocked/resolved item does not recreate it); late `AwaitingReview → NeedsHuman` from a closed-unmerged PR (the seven-day window starts at the fresh `courier.misospace.dev/needs-human-at` annotation stamped after the close-unmerged transition's report settles, not at the original `FinishedAt`); a missing or `Unknown`/`False` `LifecycleReported` condition (the run stays unreaped until the report reaches a positive settled state or an operator intervenes — there is no fail-open delete path).

**Implementation:** Bounded retention for `Failed`/`NeedsHuman` is implemented (#287, absorbing the #276 brief). `reapTerminal` (`internal/controller/reap.go`) generalizes the reaper with the per-phase `courier.misospace.dev/needs-human-at` and `courier.misospace.dev/failed-at` annotations (set-once on the first reconcile after the lifecycle report settles, never derived from `FinishedAt`), the four flags/chart values above, and the positive `LifecycleReported` settled-state gate (legacy missing/`False`/`Unknown` runs stay unreaped). It adds a per-repository audit floor — the most recent `terminalAuditKeep` `Failed`/`NeedsHuman` runs are retained past their window, and the floor counts only those phases — and the retention metrics (`courier_coderuns_reap_requests_total` for accepted deletes, `courier_coderuns_oldest_terminal_age_seconds` for what is still retained). The stale "terminal runs are never reaped" comments and assertions were retired, scoped to `AwaitingReview` and `HandedOff`. Closed-unmerged terminalization is implemented (#277): the `AwaitingReview` observation branch (`observeMergedReviewPR`, `internal/controller/merge_observation.go`) classifies the run's exact recorded pull request identity read through the forge `read-pull-request` capability — merged -> `Done`; open or unknown -> stay `AwaitingReview` at the slow cadence; closed without merge -> `NeedsHuman` with a `PullRequestClosedUnmerged` condition (`Status=True`, reason `ClosedUnmerged`, message carrying the recorded pull request identity) and the existing `blocked` source report; absent, identity-mismatched, or unreadable -> no transition. The `needs-human-at` annotation is stamped fresh by the reaper, so a late `AwaitingReview` age never causes an immediate reap, and the per-phase annotation survives controller restarts without re-stamping. `HandedOff` retention and an independent evidence TTL remain out of scope.

## Cross-worker capacity recovery

Confirmed execution-resource exhaustion is distinct from a task blocker and an
unknown failure. `TerminalDiagnosticCategoryCapacityExhaustion` is the durable
structured signal vocabulary, and `source.CapacityRecovery` is the optional
adapter boundary for fenced settlement. Neither is inferred from HTTP status
alone, and neither permits ordinary `Release` or clearing a human hold. The
executor recognizes the structured `capacity_exhausted` outcome, maps it to
`BlockedExternal`, and retains the capacity diagnostic. Provider-specific
classification remains separate (#320); unknown failures and plain HTTP 429
responses do not authorize recovery. The operator invokes optional adapter
preparation and settlement only after an uncached read confirms all run pods
are inert. Adapters without those capabilities fail closed with diagnostics.

A safe handoff requires durable source fencing, not simply lease expiry: the
old attempt must be unable to settle after a replacement claim, while the new
attempt reuses the existing branch/PR and preserves pushed partial work. Dirty
unpushed work must be durably handed off or retained by the old owner; absent
that mechanism, recovery fails closed and does not release ownership. Cooldown
belongs to the exhausted capacity profile, so unrelated profiles remain
eligible. Namespace-wide discovery and admission consult all profiles sharing
`capacityGroup`; unrelated capacity remains eligible. The default cooldown is
15 minutes, a retry-control window rather than a claimed provider reset time.
Retries of the same exhaustion do not extend its deadline. Successfully settled
exhaustion history permits rediscovery with a fresh opaque source attempt ID;
the successor name incorporates the settled predecessor UID, and the old run
is retained for diagnostics. Source-issued ownership is reacquired before launch.

The legacy OpenCode runtime is pinned in the image by `Dockerfile`'s
`OPENCODE_VERSION` (currently 1.18.31). Its `run --format json` event stream
is the only accepted exhaustion seam: `error.name=APIError` carries provider
evidence in `error.data.statusCode` and `error.data.responseBody`. The response
must confirm exhaustion using the shared classifier or carry a
configured exhaustion code/type (with MiniMax code 2056 configured by that
deployment); generic 429, ordinary process failures, assistant prose, and
stderr do not prove exhaustion. On the recognized event the executor kills the OpenCode
process group before observing git state and never resumes that turn. Git status
includes tracked and untracked changes; an empty snapshot requires no commits
since start and no local changes, while pushed work requires fetching the exact
run branch and matching its remote tip to HEAD. Unverifiable state remains held.

Before enabling this in a deployment, configure the verified provider token
(for MiniMax 2056, `COURIER_MODEL_EXHAUSTION_CODES=2056`), build and publish
the executor image with the pinned runtime, deploy to a non-production lane
using the same `opencode`
executor, and run both (1) an issue claim and (2) an existing PR-fix claim on
the shared queue. Induce a confirmed MiniMax 2056 response while each run has
an existing branch/PR; verify the exhaustion event is followed by process-group
termination, `BlockedExternal` plus `CapacityRecovery=True/Settled`, no further
model calls, retained predecessor history, and a fresh local-lane successor on
the same issue branch or PR without a duplicate PR. Also verify generic 429,
unknown provider errors, dirty unpushed work, and remote mismatch do not settle
ownership. Capture run names, event excerpts, source state, and successor
identity for both paths. Do not close #327 until this deployed smoke succeeds.

Dispatch and Courier are independent systems connected by an adapter, not hard
dependencies. Dispatch #1259 supplies dedicated fenced capacity handoff APIs;
only the Dispatch adapter knows their HTTP routes, issued claim/handout receipts,
and generation semantics. Discovery declares the configured opaque capacity,
claims preserve source receipts in bounded opaque run metadata, and PR-fix
settlement validates the returned resolution rather than treating HTTP 200 as
proof of release. Ordinary external waits and task failures keep their existing
semantics; capacity exhaustion does not first park the queue as an external wait.

The core requires trusted, semantically validated workspace evidence before
invoking `CapacityRecoveryPreparer` and `CapacityRecovery`. The native control
runtime's workspace observer is the trusted producer; model-writable terminal
text and legacy OpenCode termination labels are not trusted evidence. Missing
evidence, dirty work, malformed claims, and unknown remote state hold ownership.
The native producer proves only a clean, unchanged integration workspace safe;
committed work is safe only when a fresh trusted broker snapshot confirms the
existing work ref at exactly the integration HEAD; local-only commits remain
held. Evidence absence and a run UID are not proof. No automatic dirty-work
transfer is claimed. Source-specific receipts are sensitive opaque metadata,
never model inputs or capacity identities.

Legacy recovery uses a separate workspace-observer container and UID in the
existing coordinator pod. A trusted init container pins the initial remote head
before OpenCode starts. Only the observer mounts its private baseline, Kubernetes
read token, and termination output; the writer mounts none of these. The observer
uses a run-scoped service account with `get` permission for its own pod only.
The complete run-scoped identity — ServiceAccount, Role, RoleBinding — is
provisioned and ownership-verified before the coordinator pod is created, so a
foreign same-named resource is never admitted under or silently adopted; a
provisioning failure releases the claim only when the slot is proven empty,
otherwise it stays Claimed as provisioning-in-flight. It
waits for kubelet's coordinator-container `Terminated` state before reading the
workspace read-only. Process-group cancellation is not itself release proof.

The observer never runs Git in the writer's repository. It fetches the
operator-pinned remote URL and exact branch into private storage, disregarding
workspace Git configuration, hooks, index, replacement refs, and alternates. It
compares every workspace file (including ignored/untracked files, excluding only
the root `.git` directory) against the trusted remote tree, including modes and
symlink targets. Unsupported/submodule trees, unreadable paths, local-only heads,
and ambiguity fail closed. An absent work branch can be `empty` only when the
workspace matches the independently pinned baseline; existing branches require
an exact freshly fetched `pushed` head.

The operator re-reads the pod uncached, verifies the run owner UID, pod UID,
writer container ID, isolated observer specification, protected init baseline,
and observer termination result before persisting safety evidence. Model-written
executor termination text remains a claim only. The existing source recovery
path still waits for the entire pod to terminate and applies its unchanged
source fencing, capacity cooldown, retained-history, and successor rules. Native
workspace observation remains unchanged. Keep #327 open until both legacy
handoff paths and the isolated deployment smoke have recorded passing evidence.

PR-fix reports carry the stable settlement idempotency key and must return a
matching deferred resolution. Dispatch issue handoff currently lacks a durable
same-key success replay surface: after source release succeeds but the run status
write is lost, a stale-token response remains a conservative hold rather than
manufactured success. Normal issue and PR-fix handoffs materialize successors
while retaining predecessor history, and fresh remote proof permits takeover of
pushed work on the existing branch/PR. An issue release with both its response
and matching audit evidence lost is not automatically recoverable: diagnostics
and history remain held, and retrying the consumed receipt cannot release a
replacement claim. Dirty/unpushed work likewise requires manual preservation.

Manual recovery for ambiguous or unsupported handoffs is item-specific:

- Inspect live source ownership, the run and its pods, the remote branch/PR,
  and the exhaustion evidence. Unknown failures and explicit human holds do
  not authorize release; quota reset alone does not authorize requeue.
- Stop and confirm termination of every old writer before changing ownership.
  Preserve dirty/unpushed work using the existing evidence retrieval procedure
  before removing its workspace; evidence capture is bounded and best-effort,
  not an automatic transfer or proof that every edit survived.
- If release is ambiguous, compare live source ownership and the retained
  attempt receipt with source audit/history. Do not mark the old attempt settled
  without positive evidence, or clear a replacement owner's claim to force a
  retry. If evidence is unavailable, leave the diagnostic hold and use the
  source's explicit item-specific operator recovery procedure.
- Use the source's explicit operator recovery procedure only after confirming
  ownership again. Current Dispatch issue unclaim is not attempt-fenced, so
  even repeated reads are not a substitute for atomic fencing. Do not automate
  release/reclaim races or bulk requeues with that endpoint.
- Confirm the replacement uses the existing branch/PR and fresh source attempt
  identity. Retain the old run and diagnostics; do not delete history to bypass
  rediscovery. Capable connectors can use the implemented core settlement and
  fresh-attempt discovery path; unsupported connectors still require manual
  recovery and must not emulate atomic fencing with ordinary release calls.

## Sources

Pluggable adapters over a generic interface. An adapter both *creates* runs and
*transitions* work-state back.

- **dispatch** (first-party, native — no bridge). The operator *is* the dispatch
  client: claim, in-progress, in-review, needs-human, and watching the pr-fix
  queue to materialize `fix-pr` runs. This replaces the foreman-dispatch-bridge
  entirely. A resolve-issue `handed-off` report sends a task report with
  `outcome="handed-off"`, the PR, and the verification error; it is neither
  `ready` nor a `BLOCKED` queue mark, because Dispatch discovers the failed
  checks through its own `POST /api/pr-followup/sync` and enqueues repair
  independently. Dispatch accepts this neutral outcome since
  misospace/dispatch#1226; its task report does not settle the PR-fix queue.
  Courier still recognizes unsupported-outcome HTTP 400 responses from older
  or incompatible Dispatch servers and waits for explicit operator retry.
- **github-label / gitlab / forgejo / tangled** — a labeled issue becomes a
  resolve-issue run; CHANGES_REQUESTED becomes a fix-pr run.
- **cron** — scheduled work (the weekly audit, backlog grooming). This is what
  makes Courier the successor to the pre-Foreman cronjobs: scheduled and
  queue-driven work collapse into one mechanism.
- **cli / web** — ad-hoc injection: "run this issue that isn't in dispatch."

Dispatch is an adapter, not a dependency. Dispatch is a separate product; Courier
(a coding executor) must never make dispatch features depend on it.

### Lane bindings

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
that needs Dispatch-side queue exclusion. Where supported, discovery passes
retained same-source work IDs as opaque exclusions when the Dispatch client is
explicitly opted in; exclusion support defaults off and must be enabled only
after deployment has confirmed the Dispatch contract. Enabled requests are
bounded to a maximum of 32 unique valid entries of 300 characters each — the
historical contract ceiling, kept as the default and overridable per client
(`WithPRFixExclusionLimit`) with the configured maximum exported as
`courier_connector_exclusion_limit` data. The runner orders
exclusions most-relevant-first — pre-terminal runs before terminal retained
runs (whose attempts are usually settled; ranking is a truncation heuristic,
not settlement authority, and a terminal run with a still-pending report ranks
with the retained group), newest first within each group — so when more entries
qualify than the cap allows, the client keeps the most relevant head and drops
the tail with a warning instead of failing closed (#286); an exclusion overflow
must degrade discovery, not starve it. Progress after truncation is a
two-sided contract: Dispatch's per-agent, per-generation handout ledger scans
past candidates the agent was already handed, and Courier's same-identity
dedupe absorbs any re-offered truncated-out generation without materializing a
duplicate run. Every response must acknowledge `prFixExclusions.version: 1`
before either a task or idle result is processed. Missing or malformed
acknowledgements fail
closed without retry or side effects. Exclusions are dedupe hints, not claims
or ownership. The hand-out itself is acknowledged, not excluded: the moment a
runner materializes the CoderRun that owns a queue-backed attempt, the adapter
acks the stamped hand-out through Dispatch's generation-pinned, idempotent
`pr-fix-queue/ack` endpoint (#456) — the #1211 worker contract that keeps a
settled park's hand-out fully accounted and keeps the stale-handout reclaimer
safe to evaluate; a failed ack is logged, never fatal, because a lost
ack must degrade to the reclaimable state the reclaimer exists for rather
than block materialization. The ack is attempted at materialization with a
bounded retry, and re-attempted whenever the source re-offers an item
whose run already exists: the ack is idempotent and only the endpoint's
known moot refusals are no-ops — 404 with the known `pr-fix item not found`
response, 403 with `reason: "not-handed"`, and 409 with a known moot
reason — so a repeat attempt is always safe, while an unexpected 403 or 404
(worker-scope or auth rejection, older deployment, missing route) is not
ledger evidence and surfaces as an error like any other failure, and a
hand-out whose ack failed at
materialization is recovered on the next poll that still sees the item rather
than waiting on the reclaimer. Acknowledgement is a second phase of the
pass, never inline work: each offered item is queued and every item is
materialized first, then the queued acks drain under a concurrency cap (4),
a per-attempt deadline (3s, so a stalled endpoint cannot spend the shared
Dispatch client timeout), and one drain budget (15s) for the whole pass.
The budget is sized so a single item's worst case — three attempts plus two
backoffs, ~9.2s — still fits inside it, so an item draining on its own keeps
its full retry budget, while total ack latency per poll stays capped
regardless of how many items the source offers. Three attempts is an upper
bound, not a per-item guarantee: an item queued behind a stalled endpoint gets
fewer — possibly just one — before the budget abandons it, because how many
attempts an item gets depends on its queue position. Whatever the budget
does not cover is abandoned with an info log, never an error return, and never
blocks another item's materialization. A persistently failing ack still
degrades to the reclaimable state the reclaimer exists for, and terminal
settlement reports release the hand-out regardless. While
discovery is paused, Dispatch `next-task` and stale-item bookkeeping are
deferred until a later poll.

### Dispatch follow-up attempts

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

#### Recoverable dependency blocks across the Dispatch boundary

A `BlockedExternal` run must reach Dispatch as a recoverable dependency wait,
not as a regular failure: `resolvePrFixFromAgentReport` treats `outcome:"failed"`
as an attempt to be reissued, advancing the issued generation and incrementing
`fixAttempts` until it eventually surfaces `status:"BLOCKED", lane:"NEEDS_HUMAN"`
at the cap — exactly the human park the new phase is designed to avoid. The
Dispatch adapter therefore publishes a dedicated `outcome:"waiting-external"`
task report and, for queue-backed follow-ups, a generation-fenced
`status:"WAITING_EXTERNAL"` queue mark. Dispatch is expected to treat both as
no-ops for fix-attempt accounting: do not advance generation, do not increment
`fixAttempts`, do not surface as a human block, and keep the item discoverable
so a future discovery cycle can reactivate the work. Recovery requires a fresh
source-owned durable attempt identity. Dispatch's waiting queue reactivation
issues a new generation after its cooldown; Courier materializes a distinct
CoderRun and retains the previous run, diagnostic, report/retry intent, and
owner-referenced evidence. Same-identity reoffers remain suppressed, including
legacy Dispatch bindings, regardless of lifecycle report publication. Courier
never deletes history to escape dedupe and never invents a generation or retry
policy for adapters lacking durable attempt identities. Omitting blocked work
from discovery exclusions allows the source to reactivate it, not permission
to rerun the retained attempt every poll. Until
Dispatch
learns these signals, Courier emits them forward-compatibly; a legacy Dispatch
rejects the report, and the controller holds it as a pending report on the
unsupported-outcome path — automatic retries stop until an operator retry
annotation re-publishes it once Dispatch accepts the signal. The contract
change is tracked alongside #267; the source-neutral `blocked-external` lifecycle result and
`BlockedExternal` phase in the operator are unchanged.

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
- The gateway response is untrusted, unbounded input, and trusted control
  bounds both its shape and what it will hold: one SSE line is capped at 1 MiB,
  and per completion the buffered content deltas (8 MiB), the buffered
  tool-call arguments across all fragments (4 MiB), the tool-call identity
  bytes sent across all fragments (64 KiB), the distinct tool-call indexes
  (128), and the retained events (65536) are each capped. Each is checked
  before the bytes, entry, or event that would cross it is retained, so
  crossing one ends the turn with one fixed error naming the bound and never
  quoting provider text. Payload bytes alone would not bound retention — an
  `Event` costs a slot whatever it carries, and identity fields are overwritten
  rather than accumulated — so the event count and the identity bytes are
  bounded too. Tool requests are emitted only after the whole stream is read,
  so an overflowing stream can never yield an executable partial request, and
  the one terminal error a failed turn appends is exempt from the event bound so
  the failure is still reportable. These are transport resource bounds, not
  work, duration, or model-behavior limits: they say what one well-formed
  response may retain — its text, arguments, identity, and events — not what a
  completion may cost the control pod in any other way, and never how much work
  the model may do.
- Secrets are redacted from logs before stdout.
- **Failure-evidence surfaces (#115):** the operator serves an authenticated,
  write-only evidence intake listener (in-cluster, token-gated, no read path)
  and holds namespaced Secret CRUD to persist evidence bundles. The evidence
  verbs are provisioned as a namespaced Role/RoleBinding in the chart,
  deliberately never added to the generated operator ClusterRole, whose
  ClusterRoleBinding would widen them cluster-wide and attach them to installs
  that keep evidence disabled. The authority expansion is real but bounded.
  The whole residual: the generated
  ClusterRole already grants `create/delete/get/list` on `secrets` cluster-wide
  for the secure-mode launcher, and the chart's namespaced `credential-reader`
  Role grants the operator's service account `get` on every Secret in the run
  namespace (no `resourceNames`), so the evidence Role's marginal verb against a
   shipped install is `patch` — the intake's supersede path. Evidence capture
   therefore adds one verb and no new read, create, or delete surface to an
   operator that already mutates Secrets, alongside the intake listener and the
   run-owned Secret shape, and reject-preservation remains the named fallback in
   the #115 design. Evidence content itself is model-authored untrusted data stored
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

## Current limitations

- The default OpenCode bootstrap remains explicitly insecure: model-controlled
  processes and forge credentials share a pod. Prompt and tool restrictions
  are not an isolation boundary. The opt-in trusted-control / untrusted-worker
  topology and its acceptance gates are documented in [HARNESS.md](./HARNESS.md).
- A silent live tool or process can remain stuck indefinitely when Courier
  cannot safely distinguish a slow operation from a dead one. Human
  intervention remains the escape; Courier deliberately avoids wall-clock run
  deadlines.
