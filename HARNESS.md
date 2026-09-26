# HARNESS.md — Durable Coordinator Harness

Design pass for the resumable coordinator harness (#8), consuming the forge
capability boundary (#80) and credential/process isolation problem (#104). It
settles the trust and execution contract; it does not implement it. The current
OpenCode bootstrap remains an explicitly insecure legacy mode until a safe
replacement is proven.

DESIGN.md remains authoritative. In particular: inform, don't constrain model
behavior; git, the PR, and CI are source of truth; no wall-clock run deadlines;
provider configuration is not a `LaneProfile` concern; and Courier can publish
but never merge.

## 1. Scope and current state

This design covers the pod topology, broker capabilities, native harness,
untrusted shell worker, liveness, status/publication, migration, and acceptance
tests. Source adapters, transcript storage, and web/CLI surfaces are out of
scope.

Observed in the current tree:

- `internal/executor/pod.go` creates one pod/container with an `emptyDir`,
  non-root UID, and no service-account token. It injects `COURIER_GIT_USERNAME`,
  `COURIER_GIT_TOKEN`, and `GITHUB_TOKEN` from `courier-github` into the
  container; spawned model processes can read them.
- `internal/executor/opencode_config.go` writes MCP configuration containing a
  `Bearer {env:GITHUB_TOKEN}` template to a pod annotation and DownwardAPI
  volume. The template is not itself the secret; the model can read the
  environment variable it references. The merge deny is prompt-level only.
- `cmd/courier-executor/main.go` uses `GITHUB_TOKEN` in the adoption guard, and
  `COURIER_ASKPASS=1` can print the git token on demand. A model-spawned process
  can invoke that helper.
- `internal/status/status.go` has writer/heartbeat primitives, but production
  pods have no Kube identity and do not populate heartbeat, checkpoint, or
  `lastCommit` (#102). The operator reaper is consequently inert for these
  runs.

These are real credential and status-boundary failures, not issues a prompt or
process convention can fix.

## 2. Target topology

The trust boundaries are settled: trusted harness control and the untrusted
shell worker occupy separate pods; the credential broker runs in a separate
trusted pod or deployment, never in a pod with model-controlled processes. The diagram shows a per-run broker pod as one
possible deployment; #120 selects per-run versus shared broker topology and
run-bound identity. In either case there is no worker-to-broker or
worker-to-forge network path.

```
                         CoderRun
       ┌───────────────────┼───────────────────┐
       │ control / harness │ broker (example)  │
       │ trusted           │ trusted           │
       │ model client,     │ forge + git creds,│
       │ orchestration,    │ typed APIs,       │
       │ integration tree, │ status writer     │
       │ status requests   │                   │
       └─────────┬─────────┴──────────┬────────┘
                 │ controller API     │ forge / git remote / API server
                 │                    │
                 ▼                    ▼
       ┌──────────────────┐
       │ sandbox worker   │
       │ untrusted shell  │
       │ no secret, cert, │
       │ SA, or network   │
       └──────────────────┘
```

- **Control/harness pod:** trusted native harness, task supervisor, and sole
  model client. It holds the model-provider key and its trusted workload
  identity, but MUST NOT execute model-controlled shell, tools, plugins, or
  code. The integration tree and coordinator state live here and are not
  mounted in the worker. Trusted code dispatches bounded tasks over the worker
  API and treats every response as untrusted.
- **Broker service (separate trusted pod or pods):** owns forge/git credentials
  and the only Kubernetes identity authorized to write harness-owned status.
  It exposes typed APIs, not an arbitrary HTTP/MCP proxy. Every request is
  bound to exactly one run's resolved repository policy. Whether its deployment
  is per-run or shared is for #120 to decide; sharing must not allow cross-run
  requests or broaden status-write authority.
- **Sandbox worker pod:** untrusted model-controlled shell and tools. It has no
  certificate, service-account token, secret, host socket, or network egress;
  NetworkPolicy is pod-level defense in depth, not process isolation. The worker
  accepts tasks only from the trusted controller API and returns untrusted
  artifacts/results. It cannot contact the broker or forge.

The trusted control process and the worker shell MUST NOT share a pod. A
supervisor in the shell's pod cannot protect its identity merely by being a
different process: the child may inspect or interfere with process state, and
pod-level NetworkPolicy cannot distinguish the shell from its supervisor. The
worker therefore receives no identity to protect. Model requests that need
forge access travel through trusted control code, which applies semantic
validation before asking the broker.

One known tension with this topology: build and package managers often need
network for legitimate dependency fetches, and the worker has none. The egress
policy for that case is an open question (§11), not resolved here. Egress
restriction is a defense layer, not the security boundary, and the direction
is an explicit curated dependency proxy operated by trusted control — not an
arbitrary network bypass for the worker.

## 3. Identity and API boundary

- Authenticate control-to-broker with an externally verified workload identity
  bound to the run and role. Control-to-worker tasks are authenticated by a
  signed, run-bound request verified with a public key in the worker (or an
  ingress proxy outside the worker pod); the worker holds no signing secret.
  Headers and request fields do not establish identity. The worker has no
  private workload identity.
- The broker distinguishes trusted harness operations from model-influenced
  operations by separate trusted code paths and typed APIs, not merely because
  both carry the harness certificate. Every model-influenced request is
  validated against its semantic contract. Model input cannot write status or
  choose an arbitrary destination.
- Network policy allows control to the broker, model gateway, and worker API;
  broker to the API server and configured forge/git endpoints; worker has no
  egress. Deny cross-run access. If the cluster cannot enforce these conditions,
  do not launch the secure mode. No generic proxy and no arbitrary MCP
  forwarding.
- No forge or git credential appears in control/worker env, argv, files,
  annotations, DownwardAPI volumes, logs, or tool responses. The provider key
  belongs to the trusted harness only; workers do not call models directly.

Role grants, precisely:

| Role | Pod | May | May not |
|---|---|---|---|
| Coordinator (harness) | control | call models under all role bindings; plan, integrate, verify; dispatch and cancel briefs; write trusted status; request publication | run model-controlled shell; merge; read broker credentials |
| Coder | worker | run shell/tools on sanitized snapshots; return untrusted artifacts | credentials, network, broker, forge, model calls, status |
| Reviewer | control (model role) | read integrated diffs; run read-only checks through the worker | forge writes; merge; anything beyond its verdict |
| Broker | broker | typed forge/git operations under the run's resolved policy | merge; raw REST; generic MCP passthrough; destinations outside the pinned policy |
| Operator | control plane | CR lifecycle, phases, relaunch/reap | write harness-owned status; treat model output as control |

## 4. Broker contract

### Provider registry

Forge access is registered per provider as deployment/control-plane
configuration, never a `LaneProfile` field: a provider name, an endpoint, a
credential reference (a secret ref resolved only inside the broker), and the
set of semantic operations and error categories it supports. GitHub is the
first registered provider. A provider may be exposed through MCP only when
the broker can enforce the same typed semantics on it; untyped MCP passthrough
is not a registration path.

The registry keeps **observer** and **source** distinct: the provider that
serves a run's forge reads/writes is a broker concern, and the source adapter
that created the run and receives work-state transitions is an operator
concern. A deployment may use different providers for the two, and a run never
conflates them.

### Model-influenced forge operations

Expose registered semantic reads and writes (issue/PR/review/comment/check
reads, create/update PR, comment), never merge, close, queue mutation, admin,
raw REST, or generic MCP passthrough. Forge credentials stay in the broker.
Model-originated intent is not authorization: the trusted control API validates
each operation against the run's resolved policy before the broker acts.

### Git policy and publication

The operator resolves an immutable run policy at admission: expected repository,
base ref/OID, permitted work ref, and—on `fix-pr`—the expected PR and head
repository/ref. The broker pins all operations to that policy; it does not trust
model-supplied repo/ref/URL values. The broker applies no path filters by
default: a path scope binds only when the run's resolved policy explicitly
declares one. The default is the repository's own layout, not a
Courier-imposed subset (inform, don't constrain).

- Reject an unexpected repository, foreign base, arbitrary URL, or publication
  to the base repository's default/protected refs. Reject any `fix-pr`
  publication whose repository/ref no longer matches the operator-resolved PR
  head identity. Re-read live PR/repository
  state before publication. A fork head is not itself forbidden: preserve the
  actual head repository/ref, never substitute a same-named base branch, and
  return actionable `NeedsHuman` if the configured identity cannot write that
  head (#94). #118 settles the exact fork writability and protected-head policy.
- The harness publishes an integration commit as a bounded git bundle/artifact.
  The broker validates the objects and target ref, verifies the proposed update
  is a fast-forward from the live tip, and uses a provider-supported conditional
  update where available or a normal non-force git push followed by a fresh
  remote observation. Never use `--force` or `--force-with-lease`; a competing
  update must fail safely or be re-observed before declaring success. On a race, reject and let
  trusted control reread the world and reconcile; do not claim branch/status
  atomicity that the forge does not provide.
- Git reads use a sanitized, read-only repository pack/bundle or workspace
  supplied to the worker. The worker has no remote access. Returned commit
  bundles are untrusted artifacts: control validates repository/object format,
  base, refs, ancestry, and permitted paths/policy before integration and
  publication.
- PR creation/update and comments use typed forge methods bound to the same
  resolved repo/ref/PR policy. Comments cannot carry instructions that cause
  arbitrary URL fetches, merges, or broker proxying.

### Trusted status operations

Only trusted harness control code may request status writes. The broker accepts
those calls on a dedicated trusted API path with run-bound identity, current
pod-incarnation check, and schema validation; model-facing tool requests cannot
reach that path. The broker's Kubernetes authorization is confined to the
named run's status subresource; read-only access for binding may be separately
scoped. No deployment-wide secret or status patch permission is mounted into
model-facing processes. No model-writable
status fields or worker status calls. Checkpoint contents are limited to
reasoning not recoverable from the world; on resume re-read git, PR, CI, and
source state and let the world win on conflict.

## 5. Native harness and worker protocol

The harness is the only model client. Every provider stream is normalized in
trusted control code into a fixed event vocabulary — `delta` (model text
chunk), `tool-request`, `tool-result`, `final`, `error` — before anything else
sees it. Provider wire formats never reach the supervisor, the worker, or
status. Model text is data: no event lets a model assert liveness, completion,
or status.

**Model binding.** At run start, each role named in the lane's
`LaneProfile.roles` (coordinator, coder, reviewer, …) is bound to the model
identifier the profile supplies and resolved through the LiteLLM gateway's
OpenAI-compatible API. The gateway endpoint and key are deployment/model
configuration, not hardware specifics in `LaneProfile`; the profile supplies
only the per-role model identifiers.

**Coordinator ownership.** The coordinator owns the run's terminal contract:
plan, integrate, verify, push, and open/update the PR. Delegation covers
bounded work only — implementation, research, review — never the terminal
contract. The coordinator publishes through the broker, never through a
worker.

**Spawn brief schema.** Each delegation is a typed brief carrying: a stable
`id` (unique within the run), objective, settled decisions with exact values,
owned files, non-goals, and an observable success check. The worker returns a
result keyed by the brief `id`, with a summary and artifact bundle.
Cancellation and retry are trusted control actions keyed on the brief `id`:
a cancelled shell is terminated and its result ignored; an interrupted brief
may re-execute, but integration deduplicates its ID against the live branch and
checkpoint. Publication reconciles the remote ref before retrying. Forge writes
use a provider idempotency key where supported or re-read the live PR/comment
before retrying; if duplicate comments cannot be ruled out, surface the
uncertainty rather than blindly replaying them. Subagents cannot publish.

**Capability health at start.** At run start the harness probes each semantic
capability it depends on — model bindings per role, broker forge operations,
git remote — and records its state as `configured`, `healthy`, or
`unavailable`, with a redacted reason (never a secret value). The probe is
optional and not a gate: an unavailable capability does not hard-fail
startup. A run whose coordinator finds the work impossible (missing access,
an ambiguous ask, a capability that will not come back) self-declares
needs-human and exits 2; a run that can proceed does, degraded capabilities
and all.

The harness supplies the worker a sanitized read-only repo snapshot (for
example, a git bundle unpacked into an ephemeral workspace), plus only the
task inputs. The worker executes shell and returns a summary, evidence, and
artifact bundle. Results and evidence are untrusted claims. Trusted control
verifies bundles, integrates changes in its private tree, runs required checks
through the worker as needed, and owns all publication and status transitions.
Workers never publish, access the broker, write status, or see credentials.

The trusted integration tree and coordinator implementation must not be
shell-accessible. A worker may receive another sanitized snapshot for a later
task; it never receives the control pod's writable tree or credentials.

## 6. Liveness and #102

No expiring in-flight lease, no wall-clock deadline, and no hard duration cap
may reap legitimate long-running work. This section settles the exact schema,
ordering, and decision rules for the active-operation record that #119 owns.
It is a safety tradeoff, not a liveness proof.

### The indistinguishability argument

A silent *legitimate* operation (a long build or test making progress but
emitting no milestone) and a *wedged* operation (alive, silent, making no
progress) are **observationally identical** to the operator. Both present as:
a valid dispatched operation, a quiet heartbeat, and a live worker pod. The
only signals that would differ — a verifiable milestone, or a confirmed death
— are absent in both. So no design can at once (a) reap a wedged silent
operation in finite time and (b) never reap a legitimate silent operation.
This design **chooses (b)**: it suppresses reaping while a confirmed active
operation is in flight and accepts that a genuinely wedged one stays wedged
indefinitely. It is an explicit, intentional trade — not a claim that the
wedge will be detected.

### Two signals, never conflated

- **Heartbeat** — only independent, verifiable milestones: a successful
  model-stream event, or a tool boundary that trusted control has verified.
  A sub-agent's model stream is trusted activity — the harness is the sole
  model client and sees every stream — so a real stream is a real, earned
  heartbeat. **Not** milestones (never keep a run alive): failed attempts,
  retries, a retry storm, raw stdout, a structured terminal result, or a tool
  process merely being alive (process liveness). The harness must not emit a
  heartbeat it has not earned — no fake heartbeat, no periodic timer, no
  "keep it warm while the tool runs."
- **Active-operation record** — a harness-owned status field recording the
  dispatched operation (schema below). It is a **suppression signal**, not a
  heartbeat: it tells the operator "do not reap on a stale heartbeat while
  this operation is in flight." It is dispatch evidence, not progress
  evidence.

### The ActiveOperation record (harness-owned, CR status)

Trusted control persists, via the broker's trusted status path, a record with
exactly these fields:

| Field | Type | Meaning |
|---|---|---|
| `opID` | string | the dispatched operation/brief identity (unique within the run) |
| `coordinatorPodUID` | string | the owning control/coordinator pod UID (the fence) |
| `workerPodUID` | string | the worker pod UID the operation was dispatched to |
| `dispatchedAt` | timestamp | when it was dispatched; **diagnostic only**, never used to reap |
| `phase` | enum | `preparing` \| `active` (see Prepare vs. dispatch) |

The record exists to survive a control-pod restart so the operator can observe
an in-flight operation it did not dispatch. It is written and cleared only by
trusted control.

### Record validity (operator-observed, authoritative)

The operator re-derives, from its **own** Kubernetes observation (informers /
API reads — never a stale cache, never any worker declaration), whether a
record is **valid**. A record suppresses reaping only while valid:

- **Control pod present** — the control/coordinator pod with
  `coordinatorPodUID` currently exists. A record whose control pod is absent
  is invalid → **no suppression**.
- **Worker observed running** — the worker pod with `workerPodUID` is observed
  running by the operator. A missing, terminated, or UID-mismatched worker is
  a death/invalid signal → **no suppression**, and the operator does **not**
  trust any worker-reported "alive." The worker has no status write path; its
  liveness is the operator's pod observation, full stop.
- **Owner UID fencing** — the record is bound to the `coordinatorPodUID`.
  When the control pod is relaunched it gets a new UID; a stale record bound
  to the old UID is fenced out and cannot suppress the new incarnation. A
  prior pod's stale record and stale heartbeat must not reset or extend the
  new incarnation's crashloop streak.

### Prepare vs. dispatch (worker UID availability)

The worker pod UID is only known once the pod is created and observed. The
settled approach **pre-creates the worker pod, observes its UID, then
persists the active record, then dispatches the operation** — so a record
that suppresses reaping always carries an observed, authoritative worker UID.
A two-phase `preparing → active` record (persist with the coordinator UID
first, attach the worker UID before dispatch) is an alternative only if the
worker must be created per task. It must **not** suppress reaping during
`preparing` indefinitely: `preparing` is a trusted control step, and if it
hangs it is a separate control-side wedge bounded by that control step's
completion, not by wall clock. Indefinite suppression for an unstarted tool
is not allowed.

### Lifecycle ordering

1. **Prepare** — create/observe the worker pod; obtain `workerPodUID`.
2. **Persist** — write the ActiveOperation record to CR status via the
   trusted status path. **This must succeed before dispatch.**
3. **Persist fails** — do **not** dispatch. No operation is sent without a
   durable record.
4. **Dispatch** — send the operation to the worker.
5. **Dispatch fails** — **clear** the record (it was persisted but the
   operation never went out; a stale record would wrongly suppress).
6. **Verified completion** — **clear** the record and **write a heartbeat**
   (the verified tool boundary is a real milestone).
7. **Cancellation** — **clear** the record and write **no success heartbeat**
   (cancellation is not a success milestone; the next heartbeat comes from the
   next real activity).
8. **Clear fails** — the record stays, so suppression stays (over-suppress
   rather than falsely reap). It is reconciled by idempotent retry until it
   lands.

### The dispatch / status / cache race

The operator's controller may hold a **stale cache** while the worker is
launched and then killed during a status patch. Safe behavior:

- The operator validates the **current incarnation's** heartbeat and active
  status from a **live** re-read (informers / API), not a stale cache, before
  any reaping decision.
- A **just-relaunched** control or worker pod (created / container-started
  within the startup-grace window) is **never reaped** on a prior
  incarnation's stale heartbeat. The operator never writes the harness-owned
  heartbeat; grace is derived only from observable pod state.
- A **possible false reap** can occur if the operator acts on a stale cache
  showing a stale heartbeat before the new incarnation's first heartbeat and
  a valid record land. Startup grace is what prevents it. The design accepts
  that a kill racing a status patch may leave a transiently inconsistent view;
  the reconciliation (live re-read + grace) is what makes the outcome safe,
  not an atomic forge/status guarantee (two systems).

### Who observes worker death

The **operator**, from its own Kubernetes observation of the worker pod. The
worker is untrusted and has no status write capability; the operator does not
trust any worker liveness declaration. A confirmed infrastructure death (pod
gone, node lost, OOM kill) is a relaunch/resume signal bounded by the
crashloop counter — it is **death detection, not active-age detection**.

### No automatic wedge detector

A wedged live worker (alive, silent, no progress, valid record) **cannot be
detected automatically** without a reliable external oracle that the operation
is making progress. This design provides **no such oracle** and **does not
promise to detect a wedge**. The escape is **manual**: an operator (or a human
maintainer) intervenes and moves the run to `NeedsHuman`. #12 (the operator
reaper) is accordingly **limited to runs with no valid active operation** —
an honest downgrade from "detects all wedges" to "reaps stale-heartbeat runs
with nothing in flight; a run with a valid in-flight operation is out of
scope and may wedge indefinitely."

### Crashloop rules

- The crashloop counter increments **only on a confirmed infrastructure
  death** (pod crash, OOM, node loss), **never on active age** (a long silent
  operation is not a death).
- A **fresh, in-window heartbeat from the current incarnation** resets the
  consecutive streak to zero.
- A **stale heartbeat from a prior incarnation** does **not** reset the new
  incarnation's streak; freshness is judged only on the current incarnation's
  observable pod state and heartbeat.
- Reaching the ceiling (`restarts >= maxRestarts`) hands the run to a human
  with the counter at the ceiling, rather than one further relaunch.

### Decision table (deterministic)

All inputs are operator-observed and authoritative (live re-read, never a
stale cache, never a worker declaration). "Valid record" means the
ActiveOperation record satisfies the validity rules above.

| # | Operator-observed state | Decision |
|---|---|---|
| 1 | Valid active record (control pod present, worker observed running, UIDs match) | **Suppress** stale-heartbeat reaping. No duration cap. |
| 2 | Active record present, but control pod absent (UID mismatch / gone) | **No suppression.** Reap on stale heartbeat as normal. |
| 3 | Active record present, but worker missing / terminated / UID mismatch | **No suppression.** Treat as death/invalid; relaunch or classify. Do not trust worker "alive." |
| 4 | No active record, no fresh heartbeat, pod within startup grace | **No reaping** (startup grace). |
| 5 | No active record, no fresh heartbeat, not within startup grace | **Reap** on stale heartbeat; relaunch/resume. |
| 6 | Confirmed infrastructure death (pod gone, OOM, node lost) | **Relaunch/resume**; increment crashloop counter. Not an active-age event. |
| 7 | Valid active record, worker alive, silent, no progress (wedge) | **Stays wedged.** No automatic detection. Manual `NeedsHuman` intervention. |
| 8 | Relaunch with a stale record (owner `coordinatorPodUID` changed) | **Ignore** the stale record. The new incarnation is not suppressed by it. |

### Tests for this section

- **Long silent work**: a valid record + quiet heartbeat + live worker → no
  reap for an arbitrarily long duration (rows 1, 7).
- **Retries / retry storm**: model down, being retried → no heartbeat
  refresh; a run with no in-flight operation is reaped per normal policy; a
  run with a valid in-flight operation is not reaped on the quiet heartbeat.
- **Orphaned coordinator**: control pod gone, stale record → no suppression,
  normal reap (row 2); the stale record does not reset the crashloop streak.
- **Dead worker**: worker pod terminated / missing / UID mismatch → no
  suppression, death/relaunch (row 3); worker "alive" declaration ignored.
- **Wedged live worker**: valid record + live, silent worker → stays wedged,
  no automatic terminal, manual NeedsHuman only (row 7).
- **Relaunch with stale record**: new control pod UID, old record fenced out
  (row 8); old stale heartbeat does not reset the new streak.
- **Status write errors**: persist fails → no dispatch; dispatch fails →
  record cleared; clear fails → suppression stays until reconciled.
- **Cache races**: stale cache + kill during patch → live re-read + startup
  grace prevent a false reap; a just-relaunched pod is never reaped on a
  prior stale heartbeat.

This is an intentional safety tradeoff, not a proven-safe mechanism: it
accepts an indefinite wedge over a false reap of legitimate long work. It is
the current best answer and **not a liveness proof**. It does **not** resolve
#102, which remains an open liveness problem and production blocker (see §11).
The crashloop/reaper behavior and status schema above are the contract that
#126 implements and #12 enforces; no new exit-code interpretation is wired
into the operator by this document alone.

## 7. Checkpoint, heartbeat, and publication ordering

Git, PR, and CI are the durable world. Checkpoints store only what that world
cannot tell, and keep the exact existing `Checkpoint` type — `plan` plus
ordered `completedBriefs` of `{id, summary, commit}` — with no new fields.

**Commit per completed brief.** For each completed integration unit: control
validates and integrates the worker artifact and commits it in its private
tree; the broker confirms a non-force fast-forward publication; then trusted
control records the completed brief and the confirmed remote OID through the
trusted status path. The local commit is a work product; only a
broker-confirmed remote OID is recorded as `lastCommit`. A local commit is not
a published commit, and a failed push advances no checkpoint. If the status
write fails, retry idempotently and do not acknowledge the unit complete
until it is durable.

**Heartbeat.** The harness writes the activity heartbeat only on successful
model-stream activity and verified tool boundaries, coalescing writes to at
most one patch per cadence. Failed attempts, retries, and raw stdout are
excluded — a retry storm is active, not alive.

On resume, reread the world first; resolve conflicts in favor of the world,
then use checkpoint and branch history as recovery hints. Do not promise
atomicity between a forge push and Kubernetes status: those are two systems.
Use idempotent retries and reconciliation.

## 8. Failure classification

The operator's current mapping is: exit `0` → `Verifying` (the operator then
verifies the world — the PR exists and checks are observed — before the run is
done), exit `2` → `NeedsHuman`, any other nonzero exit → `Failed`. This design
preserves that mapping; a separately reviewed operator change may update it,
and this document does not.

The native harness adds an explicit result contract on top of the exit code:
a structured terminal result (outcome + reason) written by trusted control
before process exit. The operator classifies from that trusted evidence plus
its own world verification, keeping two failure kinds distinct:

- **Workload failure** — tests failing, CI red, a plan that does not converge.
  This is the work, not a harness fault: the coordinator iterates, and a
  coordinator that cannot get there self-declares needs-human (exit 2).
- **Infrastructure failure** — pod crash, OOM, model gateway down, a status
  write that will not land. This is the relaunch/resume case bounded by the
  crashloop counter, not a terminal verdict on the work.

A raw nonzero exit is not a classification; the explicit result plus the world
is.

## 9. Migration

- **Legacy mode (current):** OpenCode and model-controlled tools share a pod
  containing credentials. Label it insecure; do not describe it as protected by
  prompt permissions, process boundaries, or NetworkPolicy. It may remain
  available as a stopgap, with its known risks explicit.
- **Phase 1:** establish the broker and typed policy APIs, but do not claim the
  existing OpenCode shim is secure merely by routing calls through a broker. It
  still cannot safely run model-controlled tools in the trusted harness pod or
  author trusted status. Keep legacy routing labeled insecure until replaced or
  until a bounded adapter is independently proven to preserve the isolation
  contract. No false flag-day promise.
- **Secure replacement:** add isolated harness/worker pods and the separately
  deployed trusted broker selected by #120, plus the native harness/worker
  protocol. Preserve the coordinator, source, checkpoint, and publication
  contracts at their interfaces; route runs to the secure path only when the
  full boundary and evidence are implemented and tested.

The MCP annotation's bearer-header template is removed as part of migration,
but it is not itself a credential. Remove the underlying model-readable forge
secret and unsafe askpass path. #101 preflight remains separate; it cannot
substitute for isolation. Do not mark #102 production-ready until §6 evidence
and the acceptance tests below are satisfied.

## 10. Acceptance tests

- **Isolation:** control contains no forge/git secret and runs no model shell;
  worker contains no secret, cert, SA token, or network egress. The integration
  tree is not worker-accessible. Verify across env, argv, `/proc`, files,
  annotations, mounts, and logs.
- **Worker boundary:** worker accepts tasks only from trusted control, cannot
  reach broker/forge/API/model gateway, and can return only untrusted artifacts.
  Tampered summaries and bundles are rejected or treated as data.
- **Broker policy:** deny merge, force push, default/protected refs, foreign
  base/repository, unexpected `fix-pr` head identity, arbitrary URLs, comments
  that trigger proxying, stale expected OIDs, and ref races. Verify an allowed
  writable fork head works without substituting a same-named base branch; an
  unwritable fork returns actionable `NeedsHuman` (#94). Verify semantic
  validation on model-influenced requests, not just certificate checks.
- **Publication:** validate artifacts before integration; publish only a
  non-force fast-forward, then observe the remote ref; concurrent update fails
  safely or triggers reconciliation;
  retry is idempotent. No test assumes forge push and status are atomic.
- **Resume/status:** only trusted control writes status; checkpoint is not a
  world mirror; conflict recovery rereads git/PR/CI and follows the world.
- **Liveness:** prove the exact successful stream/tool evidence path and the
  §6 decision table. A valid active record (control pod present, worker
  observed running, UIDs matching) suppresses stale-heartbeat reaping with
  no duration cap; an invalid or absent record (control pod gone, worker
  missing/terminated/UID-mismatched, or stale after a relaunch) does not.
  A wedged live worker (valid record, silent) stays wedged — no automatic
  terminal, manual `NeedsHuman` only. Test supervisor loss, silent
  operations, retries that do not refresh the heartbeat, status-write errors
  (persist fails → no dispatch; dispatch fails → record cleared; clear fails
  → suppression stays until reconciled), and cache races (live re-read plus
  startup grace prevent a false reap). No lease-expiry or hidden timeout
  test may stand in for evidence; the wedge is not detected, so #102 stays
  blocked for production until the §12 gates are met.
- **Failure semantics:** workload failure and infrastructure failure are
  distinguishable from trusted evidence; operator exit-code mapping remains
  unchanged until its own approved change.

## 11. Open blockers

1. Long-tool liveness: the design is settled in #119 (HARNESS §6) — the exact
   ActiveOperation schema, the persist-before-dispatch ordering, the
   operator-observed record-validity rules, the deterministic decision
   table, and the test matrix. It is an intentional safety tradeoff, not a
   liveness proof: a wedged tool process stays wedged indefinitely until the
   OS/Kubernetes detects its death, and a silent in-flight operation is not
   proven safe and the wedge is not detected. What remains is implementation
   (#126) and production e2e proof, not further design.
2. Specify the operator-resolved run policy schema and race-safe publication
   contract, including repo/base/PR-head pinning and provider capabilities.
3. Define artifact format/size and validation (object/ref checks, base ancestry,
   path policy) without treating worker metadata as authority.
4. #120 selects workload identity and per-run versus shared broker deployment;
   either choice must preserve run-scoped authorization and pod isolation.
5. Define the narrow forge provider registration/configuration surface. It is
   deployment/control-plane configuration, not a `LaneProfile` field.
6. Prove any OpenCode adapter's isolation and status guarantees before routing
   runs through it as secure; otherwise it remains legacy insecure mode.
7. Worker egress policy for legitimate dependency fetches: build and package
   managers often need network, and the worker has none. Egress restriction is
   a defense layer, not the security boundary; the direction is an explicit
   curated dependency proxy operated by trusted control, not an arbitrary
   network bypass.

## 12. Issue map and gates

Design follow-ups #118 (run publication policy) and #120 (workload identity
and dependency egress) are ready for bounded design work once this document
lands. #119 (long-tool liveness) is a **satisfied design gate**: its exact
schema, ordering, record-validity rules, decision table, and tests are
settled in §6 above. Implementation issues stay blocked until their named
dependencies are settled and merged:

| Issue | Seam | Depends on |
|---|---|---|
| #121 | semantic forge provider contract | #118 |
| #122 | credential broker and git/forge enforcement | #118, #120, #121 |
| #123 | trusted control/broker/untrusted worker pods | #120, #122 |
| #124 | model streams, role grants, brief protocol | #121, #123 |
| #125 | validate artifacts, integrate/publish briefs | #122, #123, #124 |
| #126 | authenticated status, recovery, liveness | #119 (satisfied), #122-#125 |
| #127 | native terminal result and operator verification | #124-#126 |

#80 is consumed by #118/#121/#122; #104 by #120/#122/#123. #101 stays a
separate, temporary OpenCode capability preflight, not a security boundary.
#102 readiness is exactly after its prereqs: #119 (**satisfied** — design
settled in §6), #126 (still **blocked** until #122-#125 are settled and
merged), and a production e2e proving authenticated heartbeat, meaningful
checkpoint, confirmed `lastCommit`, long-running tool behavior per the §6
decision table, and resume. That e2e proves the suppression and
no-false-reap behavior; it does not — and this design does not promise —
that a wedge is ever *detected*. The operator-side #12 reaper is implemented
but, per §6, is limited to runs with no valid active operation and cannot
complete its production promise until that evidence exists. #8 records the
design; it is not a claim the secure path has landed.
