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

The trust boundaries and #120 topology are settled: each `CoderRun` gets one
trusted control pod, one untrusted worker pod, and one trusted broker pod behind
a run-specific ClusterIP Service. The broker is never shared across runs and
never shares a pod with model-controlled processes. There is no worker-to-broker
or worker-to-forge network path. The operator owns and garbage-collects each
run's service accounts, RBAC, pods, Service, and NetworkPolicies; no legacy
service-account token Secret is created for this design.

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
       │ SA; cache only   │
       └──────────────────┘
```

- **Control/harness pod:** trusted native harness, task supervisor, and sole
  model client. It holds the model-provider key and its trusted workload
  identity, but MUST NOT execute model-controlled shell, tools, plugins, or
  code. The integration tree and coordinator state live here and are not
  mounted in the worker. Trusted code dispatches bounded tasks over the worker
  API and treats every response as untrusted.
- **Broker (one trusted pod per run, behind that run's ClusterIP Service):**
  owns forge/git credentials and the only Kubernetes identity authorized to
  patch that run's harness-owned status. It exposes typed APIs, not an arbitrary
  HTTP/MCP proxy. Every request is bound to exactly one run's resolved repository
  policy. A broker is never shared across runs; its Service and credentials are
  isolated and garbage-collected with the run.
- **Sandbox worker pod:** untrusted model-controlled shell and tools. It has no
  certificate, service-account token, secret, or host socket. Its pod has
  `automountServiceAccountToken: false`, a dedicated unbound ServiceAccount,
  and no image pull credential visible in the container; network access is
  limited to the dedicated dependency cache by pinned address. NetworkPolicy is
  pod-level defense in depth, not process isolation. The worker
  accepts signed tasks from trusted control and returns untrusted
  artifacts/results. It cannot contact the broker or forge.

The trusted control process and the worker shell MUST NOT share a pod. A
supervisor in the shell's pod cannot protect its identity merely by being a
different process: the child may inspect or interfere with process state, and
pod-level NetworkPolicy cannot distinguish the shell from its supervisor. The
worker therefore receives no identity to protect. Model requests that need
forge access travel through trusted control code, which applies semantic
validation before asking the broker.

Worker dependency egress is deliberately narrow: the first supported slice is
a dedicated **read-only, pre-populated** cache serving only
administrator-approved immutable Go module versions (`.info`, `.mod`, `.zip`)
and checksums. Administrators populate it ahead of time through a separate
trusted path; the worker-facing path is read-only, and a cache miss returns a
diagnostic naming the missing module/version and how to request approval — it
never triggers upstream lookup, fetch, redirect, or cache fill on worker
demand. The Go proxy protocol accepts only validated module paths and versions
with bounded request and response sizes, no arbitrary URLs, `CONNECT`,
authentication, or caller-selected headers. The worker reaches the cache at the
operator-pinned ClusterIP as an **IP literal** (`GOPROXY=http://<ip>:<port>`):
the worker has no DNS, so no name can be re-resolved or redirected, and the
transport is plain HTTP with **no CA certificate or TLS secret mounted** in the
worker — channel security comes from the network boundary (pinned ClusterIP and
no other egress), not from TLS. Checksum verification runs in offline mode:
`GOSUMDB=off` disables checksum-database lookups, and go verifies against the
workspace `go.sum` and the approved, immutable cache contents; because the
store is read-only and pre-populated, a worker that tampers with `go.sum`
cannot obtain anything beyond administrator-approved versions. Changing
environment variables cannot bypass network isolation. Dogfood starts with
approved dependencies for this Go workspace. Other workflows (npm/pnpm/yarn,
Python, registries, Helm, OS packages, docs) need pre-materialized artifacts
or fail with actionable diagnostics. Cache request keys/logs still permit
bounded exfiltration of worker-visible data; minimize logs and do not promise
source confidentiality. This component needs its own issue, separate from
#123 (#136). NetworkPolicy is defense in depth, not identity.

### Why a per-run broker

A shared broker would amortize startup, memory, and forge connections and scale
horizontally behind one Service, but it would aggregate credential mounts and
status authority across runs. A request-routing bug or compromised instance
could mix runs even with per-request token checks; an audit stream would need
explicit identity partitioning. One broker per run costs a pod, Service, SA,
Role/Binding, TLS material, policy, cold start, and operator GC per run. It
scales with run concurrency rather than a central replica count, and a broker
failure disrupts only its own run. Run UID in pod ownership, token verification,
policy binding, named status RBAC, and per-run logs make spoofing and audit
clearer. Shared *provider* credentials can still extend the blast radius as
noted below. The broker has no deployment-wide status writer. This is the
selected topology, not a configurable shared-broker fallback.

## 3. Identity and API boundary

- Give each run a dedicated control ServiceAccount with
  `automountServiceAccountToken: false`. The control pod explicitly projects a
  kubelet-rotated pod-bound TokenRequest token with audience `courier-broker`
  and `expirationSeconds: 600`; no legacy token Secret is created. Only control
  can read it. Over TLS, control validates the per-run broker server identity
  using an operator-provisioned trust anchor; the broker alone mounts the TLS
  private key. The broker authenticates to the API with its own pod-bound
  projected token (API audience, pod name/UID extras), so the broker's own API
  calls — TokenReview creation, pod and run reads, status patches — are replay-
  bounded to the broker's own live pod, exactly as control's token is bounded
  to control's pod. The broker uses that token to perform a TokenReview on
  *every* control request, specifying `courier-broker` as audience and
  rejecting review errors, unauthenticated results, or missing pod-bound
  username/UID extras. It verifies the authenticated control SA name and UID,
  bound pod name and UID, then reads that live, non-terminating pod and the
  `CoderRun` through an uncached API client on every request: pod SA, owner run
  UID and current control
  incarnation must all match this broker's immutable run UID. That run binding
  is not taken from the broker's own configuration: at startup the broker reads
  its own pod from the API and follows the pod's ownerReference to the
  `CoderRun`, adopting that `CoderRun` UID — an immutable Kubernetes object
  UID — as its run binding, and refuses to serve if the chain is missing or
  inconsistent; every request check compares against the API's live view. No
  header, URL parameter, label, annotation, env value, or caller-supplied run
  ID establishes identity — in particular none that a spec-mutating actor
  could rewrite. A cluster that cannot supply verifiable pod-bound token
  identity cannot run secure mode.
  This bounds bearer replay to the token lifetime *and* current live pod; a
  stolen live token remains usable within its run, so control must never share
  its pod, process namespace, or projected volume with model-controlled code.
- The broker has a dedicated SA and a Role limited by `resourceNames` to the
  named `CoderRun` for `get` and `patch` on `coderruns/status` (plus named run
  read), with named pod reads for current-incarnation checks; TokenReview
  creation is cluster-scoped. It cannot write another run's status. Preflight
  confirms this scope in two distinct ways. *Presence* of the required grants
  is verified with an access review (SelfSubjectAccessReview as the broker SA,
  or SubjectAccessReview): a positive answer proves the named grant exists. No
  access review can prove the *absence* of extra grants — an `allow` only
  establishes that at least the reviewed permission exists, and there is no
  API to enumerate a subject's full grant set — so the no-extra-grants
  property is not checked by review. The operator provisions one Role with
  exactly the named rules and one binding to the broker SA; preflight compares
  those owned objects to the intended rules. This does not exclude other grants:
  secure mode requires a namespace/RBAC boundary preventing untrusted subjects
  from granting permissions to run SAs. Extra grants by privileged cluster
  administrators remain outside this guarantee.
  The operator provisions and garbage-collects per-run SAs, Roles/Bindings,
  TLS and signing-key Secrets, pods, Service, and policies on replacement and
  termination. The operator uses owner references for namespaced children and
  a CoderRun finalizer for ordered revocation (stop worker/control, disable
  broker ingress and delete broker, then remove run-only keys, RBAC, Service
  and policies). Replacement and deletion are serialized: a broker replacement
  first removes the old Service endpoint and confirms the old pod terminated
  before starting a new broker; a deleting run never starts a replacement.
  The finalizer is idempotent: each step treats an
  already-removed object (404) as done, a requeue after partial failure — an
  operator crash mid-sequence — resumes the sequence from the live object
  state, and a rerun never re-provisions a grant or re-adds work. Provider
  credentials shared with other runs are never deleted
  by run GC; broker pod death is an infrastructure failure and requires a
  fresh pod UID and revalidation before resuming. Operator shutdown does not
  authorize a credentialed orphan to continue serving. The
  broker mounts only its configured forge credential. A deployment may reuse
  the same provider credential across runs: broker *policy and RBAC* are
  run-scoped, but compromise of one such credential compromises its provider
  scope across runs. Prefer run-scoped provider installations/tokens where the
  provider supports them; never claim pod isolation narrows a shared token.
- The operator creates a per-control-incarnation Ed25519 keypair. The private
  key is held in a Secret mounted only into trusted control; the public key is
  passed to the worker in immutable pod-creation configuration. Replacing
  control rotates the key and fences/replaces old workers. The worker pod uses
  `restartPolicy: Never`, so a same-incarnation worker container restart cannot
  silently lose replay state. The worker has no authority to register or change
  its own trusted public key. The operator binds the key to the control-pod
  UID and worker-pod UID at creation; keys from prior incarnations are rejected.
- A compromised worker can bypass its own in-memory replay verifier and fake
  results, but cannot sign as control or gain control/broker authority. Replay
  checks protect the honest worker listener from stale or foreign dispatch;
  control never trusts worker assertions as proof of authorized publication.
- The broker distinguishes trusted harness operations from model-influenced
  operations by separate trusted code paths and typed APIs, not merely because
  both carry the harness certificate. Every model-influenced request is
  validated against its semantic contract. Model input cannot write status or
  choose an arbitrary destination.
- Network policy is run-scoped: control may reach its broker, model gateway,
  and worker; broker may reach the API server and configured forge/git endpoints;
  worker may reach only the dedicated Go dependency cache at an
  operator-pinned ClusterIP/port; it has no DNS egress. The operator
  refreshes that address on cache Service replacement and reprobes before new
  dispatch; stale addresses fail closed with a cache-unavailable diagnostic.
  Deny all worker access to the Kubernetes API, metadata endpoints,
  other cluster services, other runs, broker, forge, and model gateway. The
  worker uses no host network, host mounts/sockets, or privileged resources.
  NetworkPolicy is defense in depth, not the primary boundary: secure mode
  requires enforced policy and refuses clusters that cannot enforce it. Launch
  no workload exposure before policy is applied and live deny probes pass.
  Restricted Pod Security plus separately enforced admission controls deny
  host namespaces, host mounts/sockets, privileged workloads, added sidecars,
  and ephemeral-container injection. These are **static** guarantees about
  what the cluster's API admits, distinct from the **live** network probes,
  and neither proves that an arbitrary actor cannot create or mutate pods with
  unauthorized specs. The design assumes no subject other than the trusted
  operator components can (a) create a pod whose spec the configured admission
  would reject, (b) mutate a run pod's spec — including adding an ephemeral
  container through the `ephemeralcontainers` subresource (`kubectl debug`)
  to a running pod, or injecting volumes, env, or sidecars —, or (c) disable
  or bypass admission or NetworkPolicy. That assumption rests on cluster RBAC
  (the `ephemeralcontainers` subresource and run-pod mutation denied to every
  subject but the operator) plus the admission controls, which are
  cluster-level guarantees, not per-pod ones. A privileged cluster actor — a
  node administrator, a `cluster-admin` subject, or direct etcd access — can
  do all of these and defeats the design; that is an explicit residual risk
  stated in §12, not a closed hole. The cache may use DNS to reach its own
  configured upstreams during administrator-controlled population, never on
  worker demand. No generic proxy and no arbitrary MCP forwarding.
- No forge or git credential appears in control/worker env, argv, files,
  annotations, DownwardAPI volumes, logs, or tool responses. The provider key
  belongs to the trusted harness only; workers do not call models directly.

Role grants, precisely:

| Role | Pod | May | May not |
|---|---|---|---|
| Coordinator (harness) | control | call models under all role bindings; plan, integrate, verify; dispatch and cancel briefs; write trusted status; request publication | run model-controlled shell; merge; read broker credentials |
| Coder | worker | run shell/tools on sanitized snapshots; read approved dependency artifacts; return untrusted artifacts | credentials, arbitrary network, broker, forge, model calls, status |
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

**Spawn brief schema and authenticated worker protocol.** Each delegation is a
typed brief carrying a stable `briefID` (unique within the run), objective,
settled decisions with exact values, owned files, non-goals, and an observable
success check. Each dispatch or cancellation is a signed canonical envelope
containing the `CoderRun` UID, control-pod UID, worker-pod UID, `briefID`,
`opID`, kind (`dispatch` or `cancel`), payload digest, sequence, nonce, and
admission expiration. Expiration limits admission/replay only; it is not an
operation-duration limit. The worker verifies the Ed25519 signature using the public key provisioned
by the operator, checks the payload digest and exact run,
control, and worker incarnation; it keeps in-memory replay state and
cancellation tombstones keyed by `opID`/sequence; dispatch is sequence 0,
cancellation has a strictly greater sequence. A cancellation is the signed
envelope for the *same* `opID` as its dispatch — same key, same run/control/
worker UID binding, strictly greater sequence — so the worker can bind it to
the original operation. A cancellation tombstone is
accepted even if it arrives before dispatch. If cancel precedes dispatch, the worker tombstones the ID and must never run
that dispatch. A tombstone is final for its `opID`: any later dispatch or
retry with that ID is rejected, regardless of arrival order; new work gets a
fresh `opID` after termination is confirmed. The worker acknowledges cancellation
only after verified process termination (or confirmation it never started); only then may trusted control clear
active-operation status. A replacement worker (new pod UID) that never saw
that `opID` treats a late cancel as unknown data and runs nothing; the
authoritative termination of a fenced stale worker is the operator's pod kill,
and op cleanup follows observed pod termination, not a worker acknowledgement.
A worker result is keyed by `briefID` and contains a
summary and artifact bundle, all treated as untrusted.

If dispatch transport fails ambiguously, control does not automatically
redeliver: it cancels and reconciles until process termination is proven. If
control dies, the operator fences and replaces the old worker. The worker's
replay state is intentionally process-memory-only because its pod cannot restart
its container in place. Integration deduplicates completed work against the live
branch and checkpoint. Publication reconciles the remote ref before retrying.
Forge writes use a provider idempotency key where supported or re-read the live
PR/comment before retrying; if duplicate comments cannot be ruled out, surface
the uncertainty rather than blindly replaying them. Subagents cannot publish.

**Capability health at start.** At run start the harness probes each semantic
capability it depends on — model bindings per role, broker forge operations,
git remote — and records its state as `configured`, `healthy`, or
`unavailable`, with a redacted reason (never a secret value). Before launching
secure workloads, the operator runs a preflight in two distinct classes.
**Static checks** verify what the cluster's API admits and what the operator
itself renders: restricted admission and pod/token projection support are
enforced; the operator's own RBAC includes the RBAC-object reads it needs; the
broker TLS identity and signing-key material are present; the broker's named
status RBAC is in place and its Role/RoleBinding rule set matches the intended
named grants (verified by reading the objects — an access review proves a grant
exists, never that no extra grant does); the immutable run binding resolves
(pod ownerReference to the CoderRun UID); the approved read-only cache
configuration is present; and a dry-run create of the operator-rendered pod
specs, plus negative dry-runs of known-bad specs (added sidecar, ephemeral
container, credential mount, host path), confirm the rendered specs are clean
and the configured admission rejects the bad patterns. **Live probes** then
run from disposable identity-less pods using the same scheduling constraints
on every eligible node and IP family: allowed paths (control-to-broker,
control-to-worker, broker-to-API/forge, worker-to-cache) and denied paths (pod
and Service addresses, API, metadata, DNS, forge, broker, gateway, and
cross-run traffic). A dry-run of the operator's own spec and a probe sample at
preflight time do not prove that no other actor can create or mutate pods with
unauthorized specs; that rests on the RBAC/admission assumption in §3, and a
privileged cluster actor remains outside it. The operator applies policies
before workloads; probe success is a prerequisite, not a proof against later
CNI drift; a runtime
guard stops dispatch on observed policy failure and reports isolation lost.
Secure mode fails closed if policy enforcement or required probes are unsupported
or fail; permanent deployment misconfiguration sets `SecurePreflightFailed` condition
and `NeedsHuman`, while transient API/probe failures requeue without launching
and eventually require operator intervention rather than silently proceeding.
Only an explicitly selected legacy mode may bypass these checks, and it remains
labeled insecure. A run whose coordinator finds the work impossible (missing
access, an ambiguous ask, a capability that will not come back) self-declares
needs-human and exits 2; a run that can proceed does, degraded capabilities
and all.

The harness supplies the worker a sanitized read-only repo snapshot (for
example, a git bundle unpacked into an ephemeral workspace), plus only the
task inputs. The worker executes shell and returns a summary, evidence, and
artifact bundle. Its server is a minimal run-bound signed-protocol listener, not
a trusted security boundary: a compromised worker may inspect or interfere
with the verifier and fabricate results. Neither it nor the shell has private
workload identity or secrets; control validates all returned artifacts. Results and evidence are untrusted claims.
Trusted control verifies bundles, integrates changes in its private tree, runs
required checks through the worker as needed, and owns all publication and
status transitions. Workers never publish, access the broker, write status, or
see credentials.

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

The broker checks the run UID and current control pod UID on each write, and
refuses stale incarnations. Its ServiceAccount may `get` and patch only the
named run's `CoderRun/status`, `get` that `CoderRun`, and `get` the named current control pod
using `resourceNames`; it may create cluster-scoped TokenReviews.
It has no other Kubernetes authority. A stale map entry is **ignored**, not
trusted or silently adopted, after restart; new control rereads the world and
starts its own operations with its own UID. Neither the operator nor a model
writes the harness-owned heartbeat or map. No full transcript or running child
process is treated as durable checkpoint state.

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
- **Secure replacement:** add isolated control/worker pods and one broker pod
  plus ClusterIP Service per run, with per-run identities, RBAC, signing keys,
  network policy, and the native signed worker protocol. Preserve the
  coordinator, source, checkpoint, and publication contracts at their
  interfaces; route runs to the secure path only when the full boundary and
  evidence are implemented and tested. A dedicated Go dependency cache is a
  separate implementation seam, not part of #123's pod wiring.

The MCP annotation's bearer-header template is removed as part of migration,
but it is not itself a credential. Remove the underlying model-readable forge
secret and unsafe askpass path. #101 preflight remains separate; it cannot
substitute for isolation. Do not mark #102 production-ready until §6 evidence
and the acceptance tests below are satisfied.

## 10. Acceptance tests

- **Isolation and identity:** each run has exactly one broker pod and ClusterIP
  Service; control has no forge/git credential and runs no model shell. Verify
  control's projected audience-bound token is pod-bound, expires/rotates, and is
  the only control-readable token; the broker's own API-audience token is
  likewise pod-bound. Broker TokenReviews every request and checks
  authenticated SA username/UID, required bound pod identity, live run UID, and
  current control UID. Wrong audience, expired/revoked token, forged headers,
  wrong run/pod UID, stale control, and cross-run identity all fail. Verify the
  broker's narrow named-run RBAC by inspecting the Role/RoleBinding objects —
  the rule set is exactly the intended named grants (an access review proves a
  grant exists, never that no extra grant does) — and that no generated token
  Secret exists. Verify the broker's run binding resolves independently from
  the API (own pod ownerReference to the CoderRun UID) and that a broker pod
  without a verifiable owner chain refuses to serve.
  Worker has `automountServiceAccountToken: false`, a dedicated unbound SA,
  no private volume/cert/token/signing key, and no host access. Static checks:
  a dry-run create of the operator-rendered specs plus negative dry-runs of
  known-bad specs (added sidecar, ephemeral container, credential mount, host
  path) confirm the operator's own specs are clean and the configured admission
  rejects the bad patterns; a dry-run of the operator's own spec does not prove
  an arbitrary actor cannot create an unauthorized pod, so the RBAC/admission
  assumption of §3 is stated, not assumed away. Inspect env, argv, image pull
  configuration, volumes, and running `/proc` as well as rendered pod specs.
- **Worker protocol:** tampering with signed fields/payload, replayed sequence or
  nonce, wrong incarnation, and expired admission are rejected. Test cancellation
  tombstones before dispatch, acknowledgements only after process termination,
  ambiguous dispatch without automatic redelivery, control replacement fencing,
  and worker replacement after death. A cancel carrying the *same* `opID` as
  its dispatch (strictly greater sequence) tombstones the operation; a late
  cancel for an unknown `opID` reaches a replacement worker as inert data; a
  fenced stale worker is terminated by the operator and its ops clear on
  observed pod death, not on worker acknowledgement. Worker returns only
  untrusted artifacts.
- **Network boundary:** before workload exposure, concrete live deny probes in
  both directions prove worker cannot reach control, broker, API, metadata,
  model gateway, forge, or other cluster/run services; allowed control-to-worker,
  control-to-broker, broker-to-API/forge, and worker-to-cache paths are tested.
  The worker-to-cache path is exercised at the pinned ClusterIP as an IP
  literal with no DNS and no CA/secret mount in the worker; a cache Service
  replacement refreshes the pinned address and reprobes before new dispatch,
  and a stale address fails closed. Verify enforced NetworkPolicy and
  secure-mode refusal on unsupported CNI, policy-not-ready, or failed probes.
  Test Pod Security/admission denial of host
  mounts, host networking/sockets, ephemeral containers, and debug privileges.
- **Dependency cache:** the store is read-only and pre-populated; Go module and
  checksum requests serve only approved cached versions; a miss returns the
  diagnostic with no upstream I/O; arbitrary CONNECT/URLs, redirects, oversized
  requests, and unsupported ecosystems are rejected with actionable
  diagnostics. The worker runs `GOSUMDB=off` (offline checksum verification
  against the workspace `go.sum` and the approved store). Verify logs omit
  repository content while acknowledging approved cache keys remain a bounded
  exfiltration channel.
- **Worker boundary:** worker accepts tasks only from trusted control and can
  return only untrusted artifacts. Tampered summaries and bundles are rejected
  or treated as data.
- **Revocation:** the CoderRun finalizer is idempotent — a rerun after partial
  failure (operator crash mid-sequence) resumes the ordered revocation,
  already-removed objects are treated as done, and no grant is re-provisioned.
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
4. #120 settles per-run broker topology, workload identity, worker signing,
   network policy, and the first dependency-egress slice. #123 wires the isolated
   pods/protocol but does not implement the Go cache; create a separate bounded
   implementation issue #136 for that capability before claiming dependency egress is
   ready. The residual sanctioned-path source-exfiltration risk remains explicit.
5. Define the narrow forge provider registration/configuration surface. It is
   deployment/control-plane configuration, not a `LaneProfile` field.
6. Prove any OpenCode adapter's isolation and status guarantees before routing
   runs through it as secure; otherwise it remains legacy insecure mode.

## 12. Threat model and issue gates

The trusted computing base is operator/controller code, control, broker,
Kubernetes API/admission, node runtime/administrators, and cache administrator.
A hostile repository and model-controlled worker are not trusted. Compromised
trusted components remain a residual risk; NetworkPolicy is not an identity
system and cannot defend against node/cluster administrators.

| Attack | Disposition in secure mode |
|---|---|
| Worker reads `/proc` or mounted volumes | Prevents credential theft by placing all private identity, provider keys, integration tree and broker creds in other pods; worker may read its own untrusted files and process state. |
| Worker scans localhost | Accepts access to its own pod loopback only; no trusted sidecar or host namespace/socket may be there. Admission/preflight rejects injected sidecars/host access. |
| Worker reaches another run or forges run-ID headers | Denies with tested ingress/egress policy and independently authenticated broker token plus UID/policy checks; request fields never grant authority. |
| Worker replays signed task | Honest listener rejects nonce/op sequence, digest, admission expiry and incarnation mismatch; compromised worker may execute anyway but cannot acquire a signing key or have results trusted. |
| Worker calls Kubernetes API or cloud metadata | Denies pod identity/mount and tested default-deny network policy including direct IP, DNS and host paths; reject secure mode if enforcement cannot be demonstrated. |
| Worker reaches forge, broker or model gateway | Denies direct addresses and Service paths in both policy directions; no token to authenticate; forged response remains untrusted. |
| Worker abuses fetch service as generic proxy | Only approved immutable module/version artifacts are served; cache miss cannot trigger upstream I/O, no arbitrary URLs, redirects or CONNECT. Accepts bounded information leakage via approved cache request keys/logs; do not claim source confidentiality from a networked hostile worker. |
| Compromised control requests another run's broker operation | TokenReview and live run/pod UID binding reject it; its own run's authority is compromised. |
| Shared broker bug mixes run identities | Prevented by not sharing a broker; reused provider credentials can still span runs and remain an explicitly accepted provider-scope risk. |
| NetworkPolicy missing, ineffective or drifts | Refuse launch on unsupported/failed multi-node probes; runtime failures stop dispatch and flag isolation loss. Probes cannot prove future enforcement: a CNI regression remains a residual risk. |
| Privileged cluster actor (node admin, `cluster-admin` RBAC, etcd) mutates a run pod spec, adds an ephemeral container, mounts a secret, or disables policy | Explicit residual risk: admission, RBAC, and NetworkPolicy bind subjects acting through the API under RBAC; an actor who can bypass them defeats the design. Preflight verifies the enforcement is present, not that no actor can override it. |
| Pod recreated with new UID or stale task arrives after replacement | Broker rejects old pod-bound token/current-UID mismatch; operator rotates signing key and replaces worker, signed envelopes bind both UIDs. Control reconciles old operation against live pods/status rather than re-dispatching blindly. |

#120 (workload identity, per-run broker, signed worker protocol, and dependency
egress) is settled by this document. The Go cache remains a separate
implementation issue (#136); #123 covers pod/protocol wiring, not the cache. #119
(long-tool liveness) is a **satisfied design gate**: its exact schema, ordering,
record-validity rules, decision table, and tests are settled in §6 above.
Implementation issues stay blocked until their named dependencies are settled
and merged:

| Issue | Seam | Depends on |
|---|---|---|
| #121 | semantic forge provider contract | #118 |
| #122 | credential broker and git/forge enforcement | #118, #120, #121 |
| #123 | trusted control/broker/untrusted worker pods and protocol | #120, #122 |
| #136 | dedicated Go module/checksum cache and constrained egress | #120, #123 |
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
