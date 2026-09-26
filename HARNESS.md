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

## 6. Long-tool liveness contract (#119)

**Limit of observation.** A legitimate silent build and an indefinitely wedged
live build can have identical Kubernetes state, model-stream events, and tool
results: a running worker, a quiet heartbeat, and no terminal event. With no
independent progress oracle, no algorithm can both reap every wedged build in
finite time and never reap a legitimate one. We choose safety: an acknowledged
in-flight operation can suppress *heartbeat-stall* reaping for any duration.
This does not make it live. A confirmed infrastructure death, explicit trusted
cancellation, or a human decision still ends it. There is no operation-age
limit, periodic keepalive, arbitrary-stdout heartbeat, process-alive heartbeat,
or failed-model-retry heartbeat. #12 consequently detects stalls only where no
valid in-flight operation protects them; a silent live wedge can hold lane
capacity until a human intervenes. Expose that diagnostic and a manual
`NeedsHuman` path rather than claiming automatic wedge detection.

### Target status and authority

#126 adds the following **new** harness-owned CR-status state and regenerates
the CRD/deepcopy. It does not already exist in `CoderRunStatus` or
`status.HarnessPatch`:

- `heartbeat` gains `coordinatorPodUID` alongside existing `at` and `kind`.
  A stream chunk from *any* role-bound model session is an earned `stream`
  heartbeat; a trusted completed tool boundary is an earned `tool` heartbeat.
  Coalesce as today; retries, raw output, dispatch, cancellation, or a claimed
  terminal result are not successful activity. Only a heartbeat whose UID
  matches the **current** coordinator pod may reset the consecutive restart
  streak or count as fresh for its incarnation.
- `activeOperations` is a map keyed by a harness-generated `opID` unique within
  the run, including across retries and pod restarts, for each concurrently
  dispatched tool/subagent operation. Retries reuse that ID until the original
  execution is conclusively completed or cancelled; brief IDs identify work
  units, not transport attempts. Each value contains `briefID`,
  `coordinatorPodUID`, `workerPodUID`, and `dispatchedAt` (diagnostic only).
  This is dispatch evidence, **not** liveness or a checkpoint. No `preparing`
  phase or time-based expiry exists. A single slot would falsely reap another
  parallel subagent when one finishes; a set is required. The trusted control
  process alone may request broker status patches for these fields; the worker
  cannot write or impersonate them. The broker serializes per-run writes and
  uses resource-version-conditional status updates (retry conflict after a
  fresh read) so clearing one op cannot drop another op or another
  incarnation's state. Ordinary unconditional merge patches are insufficient.

The broker checks the run and current coordinator pod UID on each write, and
refuses stale incarnations. A stale map entry is **ignored**, not trusted or
silently adopted, after restart; new control rereads the world and starts its
own operations with its own UID. Neither the operator nor a model writes the
harness-owned heartbeat or map. No full transcript or running child process
is treated as durable checkpoint state.

### Start, finish, and failure ordering

1. Obtain a run-owned worker pod (reusable or per-task) and observe its UID
   and running shell container **before** starting a task. Multiple operations
   may use one worker if its task API supports concurrency; each still has a
   separate op ID and terminal observation. Worker provisioning is not an
   active operation and does not suppress reap. A worker that is merely Pending
   is not eligible for dispatch. Existing startup grace protects the control
   pod while worker provisioning is in progress, but a provisioning hang after
   that grace remains a control-side stall, not a protected long-running task.
2. Persist the new map entry via the authenticated broker **before** sending
   the task. If the write fails, do not dispatch. The broker write must be
   acknowledged before dispatch; an uncertain acknowledgment is re-read.
3. Dispatch the uniquely identified operation. If transport reports failure
   ambiguously, do **not** clear merely because the RPC failed: the worker may
   have accepted the task. Cancel it and observe its termination (or worker
   death) before clearing. Redelivery of the same ID must not start a second
   execution; only trusted control may retry with a reconciled result.
4. On independently observed terminal completion, remove only that entry and
   write the earned tool heartbeat **in the same conditional status update**.
   On verified cancellation or death, remove only that entry **without** a success
   heartbeat. If the clear fails, retain the entry and retry/reconcile; never
   claim it cleared while an operation may still be executing. A late
   completion from a fenced pod is ignored. Resuming ordinary stale-heartbeat
   reaping after a clear is allowed (cancellation is not progress).
5. A dead worker ends its operations as infrastructure failure; trusted
   control cancels/reconciles remaining tasks and removes their entries. If
   control itself dies, the operator's independent pod observation supersedes
   those entries. Do not wait forever for a dead control process to clear
   status. Relaunch may reconstruct reasoning from checkpoint and the world,
   but not a dead in-flight tool's process memory.

### Reap decision

Before any destructive liveness decision, use an **uncached API read** for the
run status and the run-owned control/worker pods (not just `r.Get`/`r.List` on
the current controller-runtime cached client); #126 must wire a direct API
reader for this decision. Read errors, uncertain identity, or contradictory
observations mean **do not reap; requeue and retry observation** with a
structured diagnostic, not a successful run heartbeat. If phase, resource
version, or pod UID changes before deletion, re-evaluate. Delete with a
Kubernetes UID precondition on the observed pod, never by name alone. The
current `checkLiveness` implementation does neither and must change in #126.
Persistent API unavailability is an infrastructure incident requiring human
intervention, not proof the run is healthy.

An entry protects only its matching control incarnation when that pod exists
and is not terminating, and its recorded run-owned worker UID exists, is not
terminating, and has a running worker shell container. Match pod owner UID and run
identity as well as pod UID; a same-name replacement, another brief's pod, or an
unrelated pod does not qualify. Any valid entry suppresses *stale-heartbeat*
reaping of the control pod, regardless of age; an unrelated active operation
cannot supply a heartbeat. Invalid or stale entries are ignored for
suppression. A dead/missing control or worker triggers operator-owned
infrastructure recovery after authoritative confirmation, including when
heartbeat is nil;
#126 must observe worker pod termination in `observeRunning` and fence stale
entries. The operator ignores old entries rather than patching harness-owned
fields; the new control incarnation may clear them via the broker after
reconciling the world. No entry protects a dead pod.

In the absence of valid entries, retain #12's behavior: a **nil heartbeat is
not stall evidence**, a fresh current-incarnation heartbeat resets the
crashloop streak, a stale current-incarnation heartbeat permits reap outside
the existing observable-pod startup grace, and terminating pods are not
re-deleted or double-counted. A previous incarnation's heartbeat can never
reset the streak or be used to declare the new pod live. The existing
`livenessWindow` governs heartbeat staleness and startup grace, **not** how
long a tool may run. Do not infer an old timestamp belongs to the new pod.

| Observation (authoritative) | Decision |
|---|---|
| Valid entry, control and worker running, heartbeat stale, even for hours | Suppress stall reap; report active operation and last earned activity. |
| Valid entry, live worker silently wedged | Same decision; manual `NeedsHuman` intervention, no automatic wedge claim. |
| Worker terminated/missing or control terminated/missing | No suppression; recover infrastructure independently of heartbeat, fence old UID. |
| Only stale/foreign entries | Ignore entries; apply normal heartbeat and startup-grace rules to current incarnation. |
| No valid entry; nil heartbeat | No heartbeat-stall reap; separately detect confirmed pod death. |
| No valid entry; stale current heartbeat; startup grace ended | Reap observed control UID once, resume from world/checkpoint; crashloop backstop. |
| Any status/pod observation fails or changes during decision | Defer action and re-observe; never use a stale cached snapshot to authorize deletion. |

**Replayable acceptance tests (#126):** a long silent build stays protected
beyond repeated liveness windows; an equally silent wedged live worker is
*not* automatically detected and can be handed to a human; failed model
streams and raw stdout never refresh heartbeat; two parallel operations
remain protected when one completes; failed or ambiguous dispatch cannot
clear a possibly running task; status-write failure prevents dispatch or
retains an active entry until reconciled; a terminated worker and orphaned
control recover without trusting worker assertions; prior-incarnation entries
and heartbeat cannot shield a new pod or reset its streak; informer lag,
concurrent status patches, and delete/recreate between read and reap never
cause a wrong-UID deletion. Tests need fake clock, conflicting patches and
real API-backed pod observations, not just prompt or log assertions. These
are target tests once the isolated worker and broker exist, not tests the
legacy single-pod executor can satisfy.

#119 settles the design tradeoff and test contract. #102 remains blocked
until #126 implements it and production e2e proves earned heartbeats, safe
suppression, checkpoint/recovery and published `lastCommit`; no test can prove
finite detection of an observationally indistinguishable live wedge.

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

1. The long-tool safety policy is settled in #119 (§6), but no progress oracle
   exists for a silent live wedge. It can persist indefinitely; human
   intervention remains the escape. #126 must implement and test the
   active-operation set, UID fencing, and API-backed operator decisions before
   #102 moves.
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
