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

```
 sources                      operator (Courier)                 coordinator pod
 ┌─────────┐   creates    ┌──────────────────────┐  launches   ┌───────────────┐
 │dispatch │─────────────▶│ reconcile CoderRun    │────────────▶│ harness       │
 │gh-label │              │  - admit (concurrency)│             │  coordinator  │
 │cron     │              │  - claim source       │  heartbeat  │   ├ coder     │
 │cli / web│◀─── status ──│  - launch / resume    │◀────────────│   └ reviewer  │
 └─────────┘  transitions │  - liveness / reap    │  checkpoint │  git · MCP    │
                          └──────────────────────┘  (CR status) └───────────────┘
                                                                  │ commits/PR
                                                                  ▼
                                                             GitHub (branch, PR, CI)
```

- **Sources** create `CoderRun` objects. A source adapter is also how work-state
  flows back (claim, in-progress, in-review, needs-human).
- **The operator** reconciles `CoderRun`s: admits under a lane's concurrency
  limit, claims the work in its source, launches (or resumes) a coordinator pod,
  watches liveness, and drives phase transitions.
- **The coordinator pod** runs the harness: a coordinator model that plans and
  delegates to coder/reviewer sub-agents, with git, a scoped GitHub credential,
  and MCP tools. It commits per completed unit, checkpoints to the CR status,
  and heartbeats.
- **GitHub** holds the durable output: the branch, the commits, the PR, CI.

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

- **resolve-issue** — "Open a PR to address {{issue}}. Make sure CI is green and
  it's ready for review, and delegate as much as possible to keep your context
  clean."
- **fix-pr** — "Take over PR #{{pr}} for issue #{{issue}}. Check why it's blocked
  (changes requested, conflicts, etc.) and get it back to a healthy state based
  on the feedback."

Prompts are that short by design. Goal plus tools; the coordinator owns the how.

### Terminal states

A run ends at exactly one of:

- **PR open, CI green, coordinator declares ready** → the run is done. What
  happens next is human-gated: merge it, or add feedback. The coordinator has
  no merge capability; merging is an explicit human-maintainer action, or an
  auto-merge a maintainer enabled (see
  [docs/repository-settings.md](./docs/repository-settings.md)).
- **needs-human** → the coordinator (or the operator, on crashloop) could not
  reach a healthy state and labels the PR/issue for a human.

Feedback or a merge conflict does not reopen the run. It spawns a **fresh
`fix-pr` `CoderRun`** (via the source — dispatch's pr-fix queue, or a
CHANGES_REQUESTED label). Each run is one immutable attempt at one goal; the
history is the sequence of runs linked by the PR and branch. This is why there is
no attempt counter anywhere in the spec.

### Own harness, not headless opencode

The coordinator ensemble already exists and is tuned in opencode. It is tempting
to run opencode headless in the pod. We won't, for one disqualifying reason:
**opencode needs a human to re-prompt it after a pod or backend-model restart.**
An operator exists to run work unattended across restarts; an executor that
requires a human to resume is structurally incompatible with that.

So Courier builds its own harness. What is rebuilt is only the *executor* — an
agent loop, a spawn-sub-agent tool, litellm model bindings, an MCP client, and
resume-from-checkpoint. The expensive, tuned part — the delegation prompts — is
just strings and ports over. The executor's headline capability, the thing
opencode structurally lacks, is **durable, resumable run state** (see
Checkpointing).

> The harness executor is the largest single build and needs its own detailed
> design pass. This document specifies its *contract* (heartbeat, checkpoint,
> commit-per-brief, terminal signals); its internals are deferred.

**V1 path:** to de-risk, V1 may wrap opencode as a stand-in executor to prove the
operator, sources, and dispatch loop — accepting "manual re-prompt on restart" as
a *known* V1 gap. V2 swaps in the resumable harness. The restart-resilience is
exactly the V1→V2 delta, so it's a clean seam, not a rewrite.

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

The only safe progress signal is **successful model streaming or a completed/
in-flight tool call** — real activity. Commits are unsafe (opencode-style runs
commit late; a deep sub-agent has none for a long time) and CI state is unsafe
for the same reason. "Successful" is load-bearing: a model that is down and being
retry-stormed is active but not successful, and should be allowed to die.

- The harness writes an **activity heartbeat** to the CR status on successful
  stream chunks and tool-call boundaries, and keeps it warm while a long tool
  call (a big test suite) is in flight.
- The operator **reaps on heartbeat stall** past a generous window — the run is
  wedged. It does *not* parse logs to decide this; control decisions must not
  depend on log-parsing, which is fragile exactly when a run is wedged.
- The coordinator **self-declares stuck** (can't get CI green after real
  attempts, missing access, ambiguous ask) → `needs-human`.
- A pod that dies (infra) or is reaped is relaunched and resumed. A **crashloop**
  (N relaunches) → `needs-human`. This is the one counter that matters, and it is
  death-detection, not work-retry.

This heartbeat catches a *wedged* run but not a *spinning* one (busy-looping,
streaming happily, converging on nothing). A conservative content-based
loop-detector — the same tool call, same args, same result, N times — is a
possible **V2** addition; it is orthogonal to the heartbeat and must be tuned not
to false-kill genuinely slow, varied work.

## State and checkpointing

**Git is the durable floor. The checkpoint stores only what the world can't tell
you.** Everything the world already knows is re-read fresh on resume, and the
world wins on conflict.

- **Checkpoint (CR status, kilobytes):** the plan, the ordered list of completed
  briefs with their compact handoffs, the PR ref, the last-pushed commit, the
  phase. This is the reasoning scaffolding, unrecoverable from the world. Compact
  handoffs keep this well under etcd's ~1.5 MB object limit.
- **Re-read on resume (the world):** git branch and commits, PR state, CI status,
  source/dispatch state. Never checkpointed; always looked up.
- **Workspace: ephemeral `emptyDir`,** rebuilt from git on every start. Nothing
  durable lives on disk, so there is no workspace PVC to sprawl.

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
  step is always sync-to-base.

Scope: **resolve-issue** adopts an orphaned branch only when a branch exists but
no PR. **fix-pr** always adopts the existing PR's branch. A branch with a PR is
never a resolve concern.

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
  pr: <#/url>
  lastCommit: <sha>
  checkpoint:
    plan: <...>
    completedBriefs: [{id, summary, commit}]
  heartbeat: {at: <ts>, kind: stream | tool}
  restarts: <n>                    # infra crashloop counter, not work-retry
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
  tools, set log level from `debug`. The pod heartbeats and checkpoints to
  status and commits per brief. Exit `0` transitions to **Verifying** before
  any external observation, releasing the lane capacity. Exit `2` transitions
  to **NeedsHuman**; any other exit transitions to **Failed**. A pod death or
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
  **Done** and resolves the source; or feedback/conflict → the source spawns a
  fresh `fix-pr` run, and this one goes Done/archived.
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

## MCP surface

The coordinator pod gets:

- **GitHub** — read (issues, PR feedback, code, CI/checks) **and** write for PR
  operations (create, push, comment). **Not merge.** "The autonomous agent
  cannot land code on its own" is the security property that matters; everything
  short of merge is reviewable. The blast radius of a hallucinating coordinator
  is "opened a bad PR."
- **context7** — library documentation.
- **metrics mini-MCP** — current model load.

## Security and boundaries

- The coordinator can read, push a branch, and open/update a PR. It cannot merge,
  cannot mutate the queue, cannot make an irreversible outward change.
- **Deployment invariant:** the repository's default branch is protected so
  merge remains a human-maintainer action, never a Courier one. The Courier
  identity is not an administrator and has no bypass permission for branch
  protection.
  Courier's runtime push identity must be a dedicated GitHub App installation
  token or a fine-grained PAT scoped to the work repository, with permission to
  trigger the repository's workflows. It must not be an Actions
  `GITHUB_TOKEN`, whose lifecycle and permissions are tied to an individual
  workflow run.
- A raw push credential can still write to any ref that the credential permits;
  branch protection is therefore a required deployment control, not a property
  supplied by the GitHub client. Scope the push credential to the work
  repository and keep its permissions no broader than the coordinator needs.
  The GitHub API credential may be separate from the push credential and should
  be narrower when the deployment only needs PR, comment, and check-run access.
- Source-state transitions (claim, in-review, needs-human, resolve) are the
  operator's, done by deterministic code — the one place non-determinism would be
  dangerous, kept mechanical.
- The pod holds whatever provider credentials its lane needs — an Anthropic,
  OpenAI, or MiniMax API token, a local endpoint's key, any combination. These
  are a per-deployment secret concern; the core assumes no particular provider.
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

## Open questions

- The harness executor internals (agent loop, spawn-sub-agent tool,
  resume-from-checkpoint) — needs its own design pass.
- The exact mechanics of injecting `LaneProfile` framing + roles into the
  coordinator prompt and sub-agent bindings.
- The web UI and CLI adapter surfaces.
- The exact vLLM gauge for backpressure on the target model server.
