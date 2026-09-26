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

Three separate pods per run. There is no worker-to-broker or worker-to-forge
network path.

```
                         CoderRun
       ┌───────────────────┼───────────────────┐
       │ control / harness │ broker            │
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
- **Broker pod:** trusted, owns forge/git credentials and the only run-scoped
  Kubernetes status identity. It exposes typed APIs, not an arbitrary HTTP/MCP
  proxy. It serves only its run's resolved repository policy.
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

- Reject an unexpected repository, foreign base, arbitrary URL, default or
  protected branch, and any `fix-pr` target whose head points at the default or
  protected branch, a fork, or any repository/ref other than the operator-
  resolved PR head. Re-read live PR/repository state before publication.
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

Do not use expiring in-flight leases or a wall-clock deadline to reap
legitimate long-running work. Two distinct signals must not be conflated:

- **Heartbeat** — only independent, verifiable milestones: a successful
  model-stream event, or a tool boundary that trusted control has verified.
  Failed attempts, retries, and raw stdout are not milestones and never keep
  a run alive.
- **Active-operation record** — trusted control persists the dispatched tool
  identity and brief in a harness-owned status field via the broker, so the
  operator can observe it after a restart. It is cleared on trusted completion,
  cancellation, or observed worker death. A stale record from a prior control
  incarnation cannot suppress reaping of the new one; the operator binds it to
  the pod UID and verifies that pod still exists. This is dispatch evidence,
  not evidence of progress; #119 owns the exact schema and stall policy.

The long-tool dilemma resolves as follows. While an operation is recorded
active and no verifiable milestone exists (a long silent build or test), the
operator **cannot safely reap on a stale heartbeat** — the heartbeat is quiet
by construction, not because the run is dead. Reaping is therefore
**suppressed for the duration of the recorded active operation**, resuming
only on an explicit trusted-supervisor terminal (the tool returned, complete
or failed) or an infrastructure death signal (pod gone, node lost, OOM kill).
A genuinely wedged tool process — alive, silent, making no progress — stays
wedged indefinitely until the OS or Kubernetes reliably detects its death.

This is an intentional safety tradeoff, not a proven-safe mechanism: it
accepts an indefinite wedge over a false reap of legitimate long work. It is
the current best answer and it is not liveness proof. **It does not resolve
#102, which remains an open liveness problem / production blocker** (see
§11).

No periodic timers and no fake heartbeats: the harness must not emit a
heartbeat merely to look alive while a tool runs silently, and the
active-operation record is a suppression signal, not a heartbeat. The
crashloop/reaper behavior and status schema are not changed by this document
alone; in particular, no new exit-code interpretation is wired into the
operator here.

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
- **Secure replacement:** add the three-pod topology and native harness/worker
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
  base/repository, fork or unexpected `fix-pr` head, arbitrary URLs, comments
  that trigger proxying, stale expected OIDs, and ref races. Verify semantic
  validation on model-influenced requests, not just certificate checks.
- **Publication:** validate artifacts before integration; publish only a
  non-force fast-forward, then observe the remote ref; concurrent update fails
  safely or triggers reconciliation;
  retry is idempotent. No test assumes forge push and status are atomic.
- **Resume/status:** only trusted control writes status; checkpoint is not a
  world mirror; conflict recovery rereads git/PR/CI and follows the world.
- **Liveness:** prove the exact successful stream/tool evidence path. Test
  supervisor loss and silent operations; with a dispatch record showing an
  operation active, verify a stale heartbeat does not trigger reap. No
  lease-expiry or hidden timeout test may stand in for evidence; unresolved
  cases block production #102.
- **Failure semantics:** workload failure and infrastructure failure are
  distinguishable from trusted evidence; operator exit-code mapping remains
  unchanged until its own approved change.

## 11. Open blockers

1. Long-tool liveness: the chosen resolution (suppress reap while a dispatch
   record shows an operation active) is an intentional safety tradeoff, not a
   proof. A wedged tool process stays stuck indefinitely until the
   OS/Kubernetes detects its death; nothing yet proves a silent in-flight
   operation safe or detects a wedge. Until stronger evidence exists,
   silent-tool liveness is unresolved.
2. Specify the operator-resolved run policy schema and race-safe publication
   contract, including repo/base/PR-head pinning and provider capabilities.
3. Define artifact format/size and validation (object/ref checks, base ancestry,
   path policy) without treating worker metadata as authority.
4. Select workload-identity and per-run/shared broker deployment mechanisms.
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

Design follow-ups #118 (run publication policy), #119 (long-tool liveness),
and #120 (workload identity and dependency egress) are ready for bounded design
work once this document lands. Implementation issues stay blocked until their
named dependencies are settled and merged:

| Issue | Seam | Depends on |
|---|---|---|
| #121 | semantic forge provider contract | #118 |
| #122 | credential broker and git/forge enforcement | #118, #120, #121 |
| #123 | trusted control/broker/untrusted worker pods | #120, #122 |
| #124 | model streams, role grants, brief protocol | #121, #123 |
| #125 | validate artifacts, integrate/publish briefs | #122, #123, #124 |
| #126 | authenticated status, recovery, liveness | #119, #122-#125 |
| #127 | native terminal result and operator verification | #124-#126 |

#80 is consumed by #118/#121/#122; #104 by #120/#122/#123. #101 stays a
separate, temporary OpenCode capability preflight, not a security boundary.
#102 stays blocked until #119 and #126 are complete **and** a production e2e
proves authenticated heartbeat, meaningful checkpoint, confirmed `lastCommit`,
long-running tool behavior, and resume. The operator-side #12 reaper is
implemented but cannot complete its production promise until that evidence
exists. #8 records the design; it is not a claim the secure path has landed.
