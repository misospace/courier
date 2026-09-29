# Courier

Courier is a Kubernetes operator that runs autonomous coding *coordinators* in
pods. You feed it an issue (or a PR with feedback); it runs a coordinator that
plans, delegates to model sub-agents, opens a PR, drives CI green, and leaves it
in a mergeable state — then hands control back to a human to merge or to send
back with feedback.

It replaces [Foreman](https://github.com/llmkube) as the executor for a
self-hosted coding loop, and it subsumes the scheduled "cronjob" work that
preceded Foreman. It is agnostic — dispatch is one source adapter, not a
dependency — and it is meant to be run by other people, not just its author. It
dogfoods itself: Courier develops Courier.

## Motivation

Foreman kneecaps local models. It runs each task in a narrow pod, gives the
coder no feedback mid-run, and treats a reviewer NO-GO as a reason to kill the
workload rather than to let the coder *fix the thing*. But local models do their
best work exactly when they can iterate: open a PR, watch CI, read the failures,
get a second opinion, and converge over many turns. That is how an opencode
session left running for hours turns out a clean PR, and it is the loop Foreman
structurally prevents.

The result is model-agnostic — the same iterate-to-green loop serves cloud
models just as well. Local is simply the case that needed it most, and the case
most executors ignore.

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
- **Target harness (designed, not yet established):** each run has trusted
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

The coordinator role method is the one proven in the opencode `coordinator-local`
agent (its *reference*, not its required configuration): **plan, delegate,
integrate, verify — do not implement yourself.** It settles the design and owns integration and
verification; it hands small, file-scoped briefs to a coder sub-agent (objective,
settled decisions with exact values, owned files, out-of-scope, an observable
success check) and asks for a compact handoff. It runs an independent,
different-family reviewer pass over the diff before declaring done.

That method is **agnostic** and lives in a static role skeleton. The specifics —
which models fill the coordinator/coder/reviewer roles, which provider they're on
(a cloud API or a local server), the hardware and its limits — are **not** in the
skeleton. They come from a `LaneProfile` (below) and are injected as framing. A
lane may be entirely cloud (Claude, GPT, MiniMax), entirely local, or a mix. This
split is what lets anyone point Courier at their own roster without touching the
role.

### Modes

- **resolve-issue** — "Open a PR to address {{issue}} and drive it to a
  review-ready state with CI green. Route every forge read and write through
  the configured forge capability, not a forge-specific CLI. Delegate
  implementation, research, and review to sub-agents, but you own completion:
  integrate and verify their work, push the branch, and open or update the
  pull request yourself — never stop at a local commit or branch when a pull
  request is required. Publish only to the run branch {{branch}}: commit on,
  push, and open or update the pull request from that single branch, and never
  create or publish work from any other branch."
- **fix-pr** — "Take over PR #{{pr}}. Inspect the current pull request state,
  CI/checks, and review feedback to determine what's blocking it, then return
  it to a review-ready state. Route every forge read and write through the
  configured forge capability, not a forge-specific CLI. Delegate
  implementation, research, and review to sub-agents, but you own completion:
  integrate and verify their work, push the branch, and open or update the
  pull request yourself — never stop at a local commit or branch when a pull
  request is required. Publish only to the run branch {{branch}}: commit on,
  push, and open or update the pull request from that single branch, and never
  create or publish work from any other branch."

Goals stay short — a goal plus tools — but each carries one non-negotiable
contract: delegation covers bounded work, never the coordinator's ownership
of completion and forge publication. The publication hint names the run branch
as the single place work may land; it informs rather than constrains (a cheap
nudge, enforced only at exit, below). Each goal also names the outcome
declaration: the coordinator declares how the run ended, and the run's ending
is classified from that declaration rather than inferred from git state.

### Terminal states

A run ends at exactly one of:

- **PR open, CI green, coordinator declares ready** → the run is done. What
  happens next is human-gated: merge it, or add feedback. The coordinator has
  no merge capability; merging is an explicit human-maintainer action, or an
  auto-merge a maintainer enabled (see
  [docs/repository-settings.md](./docs/repository-settings.md)).
- **needs-human** → the coordinator asked for a decision (see below), or the
  operator (on crashloop) could not reach a healthy state and labels the
  PR/issue for a human.

The coordinator declares its ending by writing `.courier/outcome.json` in the
workspace, and the executor classifies from that declaration verified against
the world — not from git state, which misread "the work is already done" as a
human problem (#169):

- **`changes`** — work is committed and pushed; confirmed the old way (commits
  reachable from the run branch) → **Verifying**.
- **`no_change_needed`** — nothing to do, with evidence; the executor posts the
  evidence to the issue/PR and the run never resolves the source on the
  coordinator's word — a resolve-issue run's source moves to `in-review` for a
  human to settle, and a fix-pr run ends **NeedsHuman** — its blocked report
  settles the PR-fix attempt and wakes the reviewer rather than parking the
  queue item — until Dispatch accepts
  an explicit `already_addressed` settlement (#1121 companion).
- **`needs_decision`** — a real design question; the executor posts the
  question to the issue/PR and the run ends **NeedsHuman**. This is the only
  NeedsHuman a coordinator declaration can produce; the deterministic
  bootstrap guards (refusing adoption of a branch whose PRs are closed, an
  abandoned base-sync merge) may still hand a run to a human, because no
  coordinator decision was involved.
- **`blocked_external`** — something outside the run is missing; the run ends
  **Failed** naming what is missing.

An undeclared zero-exit ending is never NeedsHuman: committed work verified on
the run branch still stands as **Verifying**, everything else fails as
incomplete until the continuation loop (#170) resumes the session instead of
terminalizing.

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
- The coordinator **self-declares stuck** (can't get CI green after real
  attempts, missing access, ambiguous ask) → `needs-human`.
- A pod that dies (infra) or is reaped is relaunched and resumed. A **crashloop**
  — the relaunch counter *reaching* the ceiling (`restarts >= maxRestarts`) →
  `needs-human` with the counter left at the ceiling, rather than one further
  relaunch. This is the one counter that matters, and it is death-detection, not
  work-retry.
- The crashloop counter bounds a **consecutive** streak of wedges, not a lifetime
  total: a run that demonstrates liveness — a fresh heartbeat within the window —
  resets the streak to zero, so a relaunch long in the past cannot terminalize a
  run that has since proven itself alive.
- A just-relaunched pod gets a **startup grace** so it is not re-reaped on the
  previous incarnation's stale heartbeat before it can send its first one: a pod
  created within the window (or whose coordinator started within it) is never
  reaped. Freshness is judged only on observable pod/run state — the operator
  never writes the harness-owned heartbeat. A pod already terminating is neither
  reapable nor counted, so a delete racing a reconcile cannot double-count a
  single wedge.

This heartbeat catches a *wedged* run but not a *spinning* one (busy-looping,
streaming happily, converging on nothing). A conservative content-based
loop-detector — the same tool call, same args, same result, N times — is a
possible **V2** addition; it is orthogonal to the heartbeat and must be tuned not
to false-kill genuinely slow, varied work.

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
`emptyDir`, but does not yet provide OpenCode a permitted scratch directory.
#114 owns the bounded scratch fix: a per-run mount outside the checkout, temp
environment and framing, and narrowly verified permissions for the pinned
OpenCode runtime and delegates. Scratch is disposable, not a checkpoint.

A terminal `NeedsHuman` or `Failed` run may still have uncommitted edits in its
checkout. Those edits are **not durable**: when the pod is removed, the emptyDir
and its dirty work disappear. Logs and status are not a recoverable patch.
#115 owns the separate design of secure, bounded, operator-retrievable failure
evidence before any preservation implementation. Until that mechanism is
reviewed, do not push incomplete work, persist raw diffs in logs or CR status,
or treat a fresh retry (#97) as recovery of the old checkout.

### Commit cadence

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

## Custom resources

### `CoderRun`

One per work item. Immutable spec, self-reaping (the checkpoint lives in its own
status and dies with the CR).

```yaml
spec:                       # set once by the source adapter, then immutable
  mode: resolve-issue | fix-pr
  source: dispatch | github-label | cron | cli | web
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
  lastCommit: <sha>
  checkpoint:
    plan: <...>
    completedBriefs: [{id, summary, commit}]
  heartbeat: {at: <ts>, kind: stream | tool}
  restarts: <n>                    # consecutive infra crashloop counter, reset by a fresh heartbeat
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
  populate these fields (#102). Exit `0` transitions to **Verifying** before
  any external observation, releasing the lane capacity — but only when the
  committed work is actually on the run branch: the bootstrap reads the run
  branch's own ref (not wherever HEAD happens to point), and work that fails
  this verification ends **Failed** naming where it actually landed, rather
  than a later operator read of an empty branch as "no work" or a NeedsHuman
  the coordinator never asked for (#134, reclassified by #169). Exit `2` (a
  declared `needs_decision`) transitions to **NeedsHuman**; exit `3` (a
  declared `no_change_needed`) transitions to **AwaitingReview** (source
  `in-review`) for resolve-issue runs and **NeedsHuman** for fix-pr runs,
  never resolving the source; the operator reaches **Done** only through the
  merged-mid-run observation, while a `no_change_needed` ending leaves the
  source's resolution to the human it escalates to. Any other exit transitions
  to **Failed**. A pod death or
  heartbeat stall relaunches/resumes it; a crashloop reaches NeedsHuman.
- **Verifying** — no coordinator pod or liveness meaning. The operator polls
  the external PR and CI world indefinitely, with a reconciliation cadence and
  no deadline. Observer errors remain Verifying and requeue. A missing observer,
  missing/draft PR, or failed check reaches **NeedsHuman**. A PR with no checks
  or pending checks remains Verifying; the PR is persisted. Green is declared
  only from **two consecutive all-green observations of the same check set**:
  every all-green observation records a compact fingerprint of the check
  identities (head commit plus sorted check names) on the run status, and an
  observation may transition to **AwaitingReview** only when it matches the
  previous all-green observation's fingerprint. Any pending or empty
  observation clears that recorded candidate — checks that have not registered
  yet can still appear at any later poll, so a pending observation can never
  pre-settle an identity — and a changed set (a new check, a new push) resets
  it the same way. A partial snapshot cannot pass. The source becomes
  in-review with the transition. Verifying does not consume LaneProfile
  execution capacity, and the source remains in-progress throughout it.
- **AwaitingReview** is terminal for this run. Human merges → operator marks
  **Done** and resolves the source; a `no_change_needed` ending arrives here
  with no PR, settled by the human reviewing the posted evidence; or
  feedback/conflict → the source spawns a fresh `fix-pr` run without reusing
  the previous run. The previous run remains auditable; its completion does
  not settle later feedback.
- **Reap:** Done runs are deleted (checkpoint dies with the CR). NeedsHuman runs
  are kept for inspection and deleted on request. Zero standing footprint between
  runs — a strict improvement over Foreman's ownerRef-less audit ConfigMaps,
  which require an external sweeper.

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
- **Target boundary (designed, not implemented):** trusted harness control makes
  model calls and validates model-influenced operations; one trusted broker per
  run holds forge/git credentials, enforces resolved repository/ref policy, and
  writes only that run's harness-owned status. Control authenticates with a
  projected, pod-bound 600-second `courier-broker` audience token; the broker
  TokenReviews every request and checks the authenticated service-account UID
  and current control-pod incarnation. Control-to-worker tasks use signed
  run/incarnation-bound envelopes; the worker has no secret, service-account
  token, or signing key. Worker network access is limited to the dedicated Go
  module/checksum cache and no DNS, never broker or forge. Enforced
  network policy, live deny probes, and secure preflight fail closed before
  workload exposure; the preflight separates static admission/spec checks from
  live probes, and neither proves a privileged cluster actor cannot create or
  mutate pods outside those controls — a stated residual risk, not a closed
  hole. [HARNESS.md](./HARNESS.md) specifies the contract and the
  implementation blockers; none of this claims current implementation readiness.
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
  run-UID-bound policy engine with race-safe publication. Those primitives are
  not a secure deployment until the per-run pod wiring, credential delivery, and
  isolation preflight in #123 land.
- **#104 isolation:** #120 settles the per-run broker, pod-bound workload
  identity, signed control-to-worker protocol, and network boundary. The isolated
  topology and secure preflight still need implementation and acceptance tests.
- **Artifact validation:** define artifact format/size and validate objects, refs,
  base ancestry, and policy without trusting worker metadata.
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

A running log of architectural decisions and their reasoning, newest first. The
body above describes the current architecture; this log preserves *why* and what
was superseded.

- **2026-09-29 — A declared `no_change_needed` no longer settles the source.**
  A declared `no_change_needed` now posts its evidence to the issue/PR but no
  longer resolves the source on the coordinator's word. The revision came from
  two failure shapes: for a followup-pr item a Resolve is a no-op, so the
  Dispatch item stayed queued and the lane kept re-offering the same task; for
  an issue a wrong 'already done' silently dropped work with nothing verifying
  the claim. Interim, a `no_change_needed` ending maps a resolve-issue run to
  `in-review` with the evidence comment posted, for a human to settle; a
  fix-pr run ends `NeedsHuman`, its blocked report settling the PR-fix
  attempt and waking the reviewer rather than parking the queue item, until
  dispatch#1121 lands an explicit
  `already_addressed`/`already_done` settlement. Done — and therefore a source
  resolve — now comes only from the world: the merged-mid-run observation.
  (#169, review on PR #175)
- **2026-09-28 — Run endings are classified from a coordinator-declared outcome.**
  NeedsHuman was inferred from workspace state, so every odd ending escalated:
  of 65 runs in one deployment's five days, 19 ended NeedsHuman and not one was
  the coordinator asking for anything — the largest class was simply the
  coordinator deciding the work was already done. The coordinator now declares
  its ending in `.courier/outcome.json` — `changes`, `no_change_needed` (with
  evidence), `needs_decision` (with the question), `blocked_external` (with
  what is missing) — and the executor classifies from the declaration verified
  against the world: `changes` still requires commits reachable from the run
  branch; `no_change_needed` posts the evidence to the issue/PR and exits to
  **Done** (exit code 3), resolving the source (source settlement revised
  2026-09-29, above); `needs_decision` posts the question and is the only
  NeedsHuman a declaration can produce (the deterministic bootstrap guards
  may still hand a run to a human);
  `blocked_external` fails naming the missing prerequisite. An undeclared
  zero-exit ending never terminates NeedsHuman directly — verified commits on
  the run branch still stand as Verifying, everything else fails as incomplete
  until #170's continuation loop resumes the session instead. Work committed
  off the run branch is Failed, not NeedsHuman: the specific reason is the
  inform, and the operator's world-read still decides what is real. The child's
  own exit 2 is no longer a human-attention signal (#169; companions #170,
  #171, #172, and dispatch's `already_addressed` settlement). This supersedes
  the NeedsHuman reading of the #134 off-branch case.
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
