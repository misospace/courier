# HARNESS.md — Durable Coordinator Harness

The trust and execution contract for the resumable coordinator harness (#8),
consuming the forge capability boundary (#80) and credential/process isolation
problem (#104). The topology it settles has landed and is available as the
opt-in deployment-level secure mode (`--secure-mode`); §11 records what is
still unwired, chiefly the native path's forge reads and PR creation (#127) and
the Go dependency cache (#136). The OpenCode bootstrap remains the default,
explicitly insecure legacy mode until a safe replacement is proven.

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
  runs. Secure mode closes this for its own runs (#126): trusted control
  writes earned heartbeats, dispatch evidence, the completed-brief
  checkpoint, and the confirmed `lastCommit` through the broker's trusted
  status path, and the operator's reap decision honors them. Legacy
  single-pod runs remain inert.

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
  cluster-level guarantees, not per-pod ones.
  The assumption must also cover **clean-spec** pod creation, because the API
  cannot distinguish an operator-created control pod from a copy: any principal
  that can create a pod in a run's namespace can reference that namespace's
  Secrets and run service accounts by name, mint a pod-bound `courier-broker`
  token, replicate the control pod's labels and owner reference, and present
  itself to the broker as control during a relaunch window. No possession proof
  can substitute inside one namespace — such a principal can mount any
  namespace-resident Secret, so no namespace-resident material distinguishes a
  forged pod. Runs therefore live in a namespace dedicated to that purpose
  where no subject other than the operator can create, patch, or delete pods or
  workload-referencing specs; preflight verifies that namespace RBAC boundary
  to the extent reviews allow (a grant's existence is provable, its absence is
  not). A privileged cluster actor — a node administrator, a `cluster-admin`
  subject, or direct etcd access — can do all of these and defeats the design;
  that is an explicit residual risk stated in §12, not a closed hole. The cache
  may use DNS to reach its own configured upstreams during
  administrator-controlled population, never on worker demand. No generic proxy
  and no arbitrary MCP forwarding.
- No forge or git credential appears in control/worker env, argv, files,
  annotations, DownwardAPI volumes, logs, or tool responses. The provider key
  belongs to the trusted harness only; workers do not call models directly.

Role grants, precisely:

| Role | Pod | May | May not |
|---|---|---|---|
| Coordinator (harness) | control | call models under all role bindings; plan, integrate, locally validate, publish; dispatch and cancel briefs; write trusted status; request publication | run model-controlled shell; merge; read broker credentials |
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

### Provider registration surface

The registry is one deployment-level configuration file loaded by the operator
at startup (`--forge-providers-file`), rendered by the chart from values. A
file rather than flags or a CRD: flags cannot express several providers, a CRD
would add API surface and a reconciler for administrator-static configuration,
and a file stays inside the trusted control plane and projects unchanged into
per-run broker pods. The registry is deliberately **not** an API object: a
namespaced registration that broader subjects could create would let them
shadow a provider with their own endpoint and credential reference, and the
broker's byte-exact `ProviderConfigRef` check would pass. The file is rendered
with the deployment and changes only through that deployment's configuration
path. The file carries **references only** — credential values never appear in
chart values, the rendered file, logs, or diagnostics.

Each registration carries:

- `name` — the registry key; the exact string `PublicationPolicy.ProviderConfigRef`
  persists, and the broker requires equality with its own projection (the
  binding check in `PolicyFromRun`).
- `type` — the implementation selector; `github` is the first. Capabilities are
  **not** configurable: they come from the implementation's typed surface, and
  an admission that needs a policy read the implementation does not provide
  fails closed (§4). Configuration cannot invent a capability.
- `endpoint` — the canonical API base. No request ever names an endpoint. The
  registration is also the sole source of the **git push endpoint**: the
  endpoint used to push each pinned repository is derived from the same
  registration that serves the identity and protection reads, and the broker
  refuses to push when the resolved endpoint is not that registration's git
  endpoint for the canonical repository. Reads and writes therefore always
  happen in the same provider world; request- or model-supplied URLs are never
  endpoints.
- `credentials` — purpose → `{secretName, key}` references, purposes `forge-api`
  and `git`; `git` defaults to the `forge-api` reference when omitted. Both
  resolve only inside the broker pod.
- `serves` — match patterns over the `owner/name`-shaped `spec.repo` (for
  example `misospace/*`), case-insensitive because hosting providers treat
  repository case as insignificant. Patterns are administrator configuration,
  not policy: they select which registration resolves a run, they never
  widen a destination.

**Selection.** At admission the operator selects the unique registration whose
`serves` patterns match the run's immutable `spec.repo`; zero matches or more
than one is `NeedsHuman` naming the absent or ambiguous selection. No spec
field, source header, or model input names a provider. The selected provider's
`ResolveRepository` then canonicalizes the repository identity, and every later
comparison — persisted policy, broker binding, publication — uses the canonical
form, never the raw spec string. Selection and enforcement can therefore never
disagree about which provider serves a run: the broker's projection is pinned
at pod creation from the same registration the operator selected.

**Startup validation.** A structurally invalid registry — duplicate names,
unknown `type`, missing endpoint, malformed credential references — fails
operator startup outright. Semantic problems such as an unreachable endpoint or
a missing Secret are admission-time failures: fail closed with an actionable
`NeedsHuman`, never a silent fallback to another provider or to legacy
injection. A healthy registry feeds §5's capability health, so an unavailable
provider is named, not silently dropped.

**Broker projection.** The operator renders each per-run broker pod with
exactly the one registration the run's policy pins — name, type, endpoint,
credential references as env `SecretKeyRef`s through the existing git
credential seam — never the whole registry. The projection is frozen at pod
creation and is configuration, not request state: no request can add a
provider, change an endpoint, or substitute a credential reference. The
persisted policy records enough of the registration — at minimum its endpoint
and credential-reference digest — for the broker to verify its projection
against the policy it binds and refuse service on mismatch, so selection and
enforcement are provably the same record even across broker replacement.

**Rotation.** Registry changes apply on operator restart to future admissions
only; a running run's policy never mutates (§4). Credential rotation is a
Secret content change plus broker-pod replacement — a fresh pod UID that
revalidates the full identity chain per §3 — because env-captured credentials
do not hot-reload. Run GC never deletes a Secret shared with other runs or with
legacy mode.

**Legacy mapping.** Today's `--git-credential-secret`,
`--github-credential-secret`, and `--github-mcp-url` flags remain legacy-mode
configuration and are untouched until §9's migration removes the model-readable
injection. The registry is the secure path's only provider source; legacy mode
does not read it.

### Model-influenced forge operations

Expose registered semantic reads and writes (issue/PR/review/comment/check
reads, create/update PR, comment), never merge, close, queue mutation, admin,
raw REST, or generic MCP passthrough. Forge credentials stay in the broker.
Model-originated intent is not authorization: the trusted control API validates
each operation against the run's resolved policy before the broker acts.

### Authoritative source-issue linkage (#248)

Both active publication paths enforce one shared, machine-readable source-issue
linkage contract: the PR body must carry a closing-keyword reference to the
trusted source issue, never a vague "Addresses" or substring match, and a
model cannot pick a different issue than the operator admitted. The native
broker validates linkage before `CreatePullRequest`; the legacy executor
validates the live PR body before claiming a successful handoff. The
validator is a single pure function in `internal/broker/linkage.go` with a
test table both paths consume; the table is the source of truth, and the
paths share acceptance, not implementation.

- **Trusted identity.** The broker binding resolves
  `Policy.SourceIssue{Owner, Name, Number}` from the immutable
  `CoderRun` spec at admission (`spec.Repo` is the canonical source
  repository; `spec.Ref` is the issue number). The legacy executor
  derives the same identity from `COURIER_REPO` and `COURIER_REF`. A
  model cannot influence either value.
- **Accepted closing keywords.** GitHub's documented set —
  `close`, `closes`, `closed`, `fix`, `fixes`, `fixed`, `resolve`,
  `resolves`, `resolved` — matched as a word-bounded token. The bare
  form is `Closes #<N>`; the cross-repo form is
  `Closes <owner>/<name>#<N>` and is accepted only when the
  cross-repo owner/name matches the trusted identity.
- **Rejected forms.** `Addresses`, `See also`, `Refs`, `Part of`, and
  any other non-keyword reference. Substring matches (e.g. `discloses`,
  `closer`) are not closing references: a leading and trailing
  word boundary is the only thing that protects against them. An
  ambiguous body with multiple closing references that disagree is
  also rejected; the model cannot silence a correct reference with
  an incorrect sibling.
- **Native path.** `PolicyEngine.CreatePullRequest` runs the
  validator under the engine lock, before the observer's
  `CreatePullRequest` is reached. A model that names a wrong
  issue, a wrong repository, an ambiguous body, or no supported
  closing keyword never gets to create a PR; the engine returns
  the structured `*LinkageError` so the caller (and the
  observability surface) can see the precise mismatch.
- **Legacy path.** Before the `Verifying` handoff — declared or
  undeclared — the executor performs a bounded, identity-checked
  post-turn observation of the live pull request state for the run
  branch through the configured forge capability
  (`PullRequestHeadLister`, all states, paginated; never a
  forge-specific CLI). A candidate is adopted only when the forge
  response carries authoritative `repo.full_name` identity on both
  sides of the head/base pair; a response without it is a read
  failure, not a pull request. A record that claims the
  owner:branch head selector from a *different* repository (a
  same-owner fork, or a response that does not carry the queried
  base) is kept visible and fails the observation closed — it is
  neither adopted nor silently dropped toward a "missing" claim,
  because the listing then settles nothing about the run's own
  head. An open candidate is adopted only when its actual base
  ref is the run's pinned base; a wrong-base open PR fails closed
  rather than being adopted, re-targeted, or paralleled. Both
  paths share one `applyHandoffPlan` helper (formerly
  `applyLinkagePlan`, #282), which applies the criterion-evidence
  contract (§4) to the same live body alongside linkage — including its
  legacy-only referential check that cited file paths exist in the head
  the observation just verified (#367) — so the check
  behaves identically regardless of how the run reached `Verifying`:
  - On a published PR with a body that disagrees with the trusted
    identity, the run asks the captured OpenCode session to
    repair the body under the bounded continuation budget
    (#170) and only escalates to `NeedsHuman` when the budget
    is exhausted, or escalates immediately when no captured
    session exists.
  - On an observation read failure the check is fail-closed: a
    verbose `linkage.read.failed` event is logged and the run
    either retries inside the captured session's bounded
    continuation budget with the factual `pr-unverified`
    re-verification instruction (#247: the message disclaims
    any missing-PR claim, because a failed read is not
    evidence of absence) or escalates to `NeedsHuman` with the
    precise error as the reason. A transient outage cannot quietly
    become a `Verifying` success; the existing PR observer does
    not perform this check, so the legacy executor owns it.
  - The observation classifies the whole head, not just one PR, and
    each kind has a fixed consequence (#247). Publication is a sha
    proof, not a ref-existence proof: the remote must advertise the
    run's *current head commit*, and every resumable or passing
    kind (open, draft, missing) requires it — a remote holding an
    older commit means local work never pushed, on the missing
    route exactly as behind an already-open PR. On that footing,
    **Missing** (no pull request at all) on a head confirmed
    *published* resumes the captured session with a factual message
    naming the repository, branch, and trusted source issue, under
    the same #170 budget. **Draft** resumes the same session asking
    that exact PR be marked ready, never a duplicate. **Ambiguous**
    (more than one open PR), **closed-or-merged only**,
    **foreign-head** (a record claiming the head selector from
    another repository), **wrong-base** (an open PR on the verified
    head targeting a base other than the pinned one), and an
    unpublished head (ref absent or stale relative to the run's
    current commit) are fail-closed on sight: no continuation, no
    adoption, no duplicate, and no create prompt over work the
    remote does not carry. Where there is no captured session or
    the budget is exhausted, every resumable kind fails closed to
    `NeedsHuman` with the precise state in the reason; the
    executor's terminating `Verifying` handoff is emitted only for
    a verified open, non-draft PR on the pinned base with the
    run's commit published to the remote. (The separate adoption
    guard that reviews a pre-existing pull request before dispatch
    reads through the same identity-checked head listing (#343):
    it reports only an open, non-draft PR whose full base and head
    identity and base ref match the run, and sends a foreign,
    wrong-base, draft, ambiguous, or closed-or-merged-only world
    to `NeedsHuman` with the precise state, but it does not itself
    check linkage; the controller's `Verifying` observation settles
    what that early handoff leaves open.) That controller observation
    reads its own head listing through the same identity-checked
    capability (#365): a record whose base or head repository identity
    differs from the run's — a same-owner fork claiming `owner:branch`
    — or one carrying no repository identity at all fails the poll
    closed and requeues, so another repository's pull request is never
    published as this run's state.
  The repair instruction names the trusted identity, the supported
  closing keywords, the bare and cross-repo forms, and the
  existing PR number so the model can fix the body
  deterministically rather than re-derive the requirement.
- **Shared truth table.** `internal/broker/linkage_test.go`
  covers the accepted keyword vocabulary, vague and substring
  rejections, the wrong-number and wrong-repository failure
  modes, the cross-repo form, redundant cross-repo references,
  and ambiguous multiple references. The legacy executor's
  repair-message tests assert the same vocabulary, so a
  regression in the validator fails both paths simultaneously.

### Acceptance-criterion evidence contract (#249)

Linkage (#248) guarantees the PR *names* the trusted source issue; it says
nothing about whether the PR's completion claim answers the issue's
acceptance criteria. The dogfood failure that split this contract off #204 —
misospace/miso-gallery#502/#505 — omitted the built-container endpoint
criterion from the PR body while presenting a Dockerfile-shape check as if
it demonstrated the full requirement, and declared a manually-inspected
`.dockerignore` invariant as a maintained one while noting no test enforced
it. One executor-neutral criterion-evidence contract closes that class in
both active paths, with the same shape as linkage: a pure validator in
`internal/broker/criteria.go`, two consumption boundaries, one shared truth
table in `internal/broker/criteria_test.go`.

**Identification and revision pinning.** Today nothing trusted ever reads
the source issue body — the model reads it through forge tools, which is
exactly why the completion claim cannot be checked against it. Admission
therefore reads the issue through the registered provider's typed read (a
new trusted operator-side capability; the dispatch task and the immutable
`CoderRun` spec already pin which issue, never the model) and runs
`ExtractCriteria`, a pure, deterministic parser: criteria are the top-level
bullet items of the issue's single `Acceptance` section (`## Acceptance`,
`**Acceptance:**`, or `Acceptance:`; task-list checkboxes accepted), ending
at the next heading-like line — an ATX heading, a bold label, or a bare
`Label:` line standing alone; prose with content after a colon is not a
heading, so unindented prose inside the section neither ends it nor
truncates the criteria. Each criterion gets an ordinal ID (`AC-1`,
`AC-2`, …) and a sha256 digest of its normalized text; the set digest
covers the trusted issue identity plus every criterion digest in order.
No recognizable section, or a section with no bullets, pins an **empty
set** — a legitimate pin: the evidence block is then not required and
linkage remains the only PR-body contract. More than one Acceptance
section, more than 32 criteria, or a criterion over 512 bytes fails
extraction fail-closed at admission (`NeedsHuman` naming why): an
unpinnable contract must not become an unchecked one, and an over-bound
set is never silently truncated. The pinned set is **revision-pinned by
content**: an edit to the issue after admission does not add, remove, or
rewrite this run's criteria — a human changing requirements re-queues the
work through admission; the model never supplies criteria at any point.

**Carriage to the boundary (#280, shipped).** Native: the pinned set joins
the immutable run publication policy (`Status.PublicationPolicy`:
`sourceIssue`, `acceptanceCriteria`, `acceptanceCriteriaDigest`), resolved
at admission through the provider's typed work-item read (`ReadWorkItem`,
the operator's read identity, read-only) and re-verified by the broker
binding like every other policy field — per-criterion digests, extraction
ordinals, and the set digest are recomputed, so a tampered pin fails the
binding. An empty pinned set carries its identity-only digest, which is
what distinguishes a pinned-empty contract from an unpinned policy
(pre-#280 runs bind with no pin — backward compatible). Legacy: the
operator projects the identical set into the executor pod as trusted
environment (`COURIER_CRITERIA`, bounded JSON in the broker criterion-set
shape: issue identity, criteria with IDs/texts/digests, set digest) beside
`COURIER_REPO`/`COURIER_REF`, rendered from the persisted policy by the
legacy launcher and the secure control invocation alike; a fix-pr run and
a run without a pin receive no envelope, so absence is meaningful. Legacy
pod visibility is fine — the set is public issue text; the contract is
that the *validator's* comparison target is operator-authored, and the
model cannot change it.

**Data shape.** The PR body carries one machine-readable block — the only
machine-read coverage surface; prose outside it is not a coverage claim:

```
courier-criteria:v1 source=<owner/name>#<n> set=sha256:<64hex> coverage=<full|partial>

- AC-<n> sha256:<64hex>: <status> - <reference>
```

The header names the trusted source identity and the pinned set digest;
each row names the criterion's pinned digest, one status from a closed
vocabulary — `implementation-evidence`, `regression-check`,
`manual-verification`, `not-implemented`, `unknown` — and a bounded
(≤400-byte) reference. `regression-check` and `implementation-evidence`
must cite at least one file path or check URL; `manual-verification` and
`not-implemented` must carry a non-empty note; `unknown` may be bare. A
criterion's status says what *kind* of evidence exists, never that the
evidence is semantically sufficient.

**What the validator mechanically proves — and what it does not.**
`ValidateCriterionEvidence(prBody, set)` enforces: exactly one block; the
header's source and set digest equal the trusted pin (no model-authored
criteria revision); every pinned criterion ID appears exactly once (no
missing, added, duplicated, or unknown IDs); per-row digest agreement with
the pin; the per-status reference shapes; and coverage honesty — `full` is
*derived* (every pinned criterion present exactly once and recorded
`regression-check` with a citable location) and the header's claim must
match the derivation. Inside a block's contiguous row list, a line shaped
like an AC row that does not parse is an explicit malformed-row rejection,
never silent prose that ends the block — a trailing invented or malformed
row must not fail open. That last rule is the mechanically caught form of
"assert a weaker check as if it demonstrated the full requirement", and the
reference shape rule is the mechanically caught form of "no test
represented as regression-enforced". The validator **does not and must not
claim** that a cited test actually fails when its criterion regresses:
presence of a reference is traceability, not semantic proof. Whether
`test_dockerfile_packages_assets` really detects an omitted COPY line is
established by mutation evidence in the target repository and audited by
the reviewer, who reads each criterion's citations next to the linked
issue. Boundary implementations may additionally verify that cited *paths*
exist in the proposed head (the legacy executor holds the run branch
locally; the native engine may read the tree at the proposed OID) —
existence is referential integrity, still not semantics. The legacy
pre-handoff plan does exactly that, and only that (#367): a body the
validator *passed* has the file paths cited by its
`implementation-evidence` and `regression-check` rows probed against the
exact pushed head `confirmRemotePublication` just verified, in the run's
own workspace, through the hardened-git environment (`ls-tree <head> --
<path>`; stderr dropped, no config trust). Properties of the check, each
deliberate:

- **Non-semantic.** It asks whether the cited file is in that tree. It
  never reads, fetches, or interprets a citation, and whether the cited
  check enforces its criterion stays the reviewer's audit.
- **Citations only.** Only `implementation-evidence` and
  `regression-check` rows are probed — the two statuses whose reference
  the contract requires to be a file path or a check URL, so their
  reference is a claim about a citable artifact. `manual-verification`
  and `not-implemented` carry a free-text note and `unknown` carries
  none; probing prose would manufacture defects the model could only
  "fix" by mislabelling the criterion.
- **URLs are never fetched.** A reference containing `://` is dropped
  whole before extraction, so a URL's host and path can never read as a
  path. No network call happens here at all beyond git plumbing against
  the local repository.
- **Paths are extracted by the validator's own grammar.** Whitespace
  tokens, `://` dropped, then the validator's path alternative
  (`[A-Za-z0-9_.-]+(/[A-Za-z0-9_.()-]+)+`) — so a bare top-level name is
  not a citable location and is never probed. Candidates are normalized
  (leading `./` dropped, `:12`, `:12-40` and `::TestName` suffixes cut,
  `..`/`.`/empty segments and a leading `-` rejected) and deduplicated.
  What survives that charset admits no whitespace, `:` or quote, no glob
  or pathspec-magic character, and never begins with `-`, so a candidate
  can neither split the argv it travels in nor be read as an option or a
  pathspec expression; `(` and `)` *can* survive and are inert, because
  the hardened boundary builds an argv directly and no shell ever sees
  the value. That restriction is what permits sanitized text from a
  model-authored body to reach `git` at all, and the caller owns it —
  `internal/git` does not sanitize. The candidate is passed as the
  pathspec after `--`, never as part of a `rev:path` argument. The
  candidate count is bounded by the operator-pinned set (`MaxCriteria`
  rows, `MaxReferenceBytes` per reference); the executor adds no cap of
  its own.
- **Candidates are rooted at the published tree before they are
  probed.** A repository path starts at a real top-level entry of the
  commit, so the check reads that commit's top-level entry names once
  (`ls-tree -z --full-name <head>`, no pathspec, no recursion — bounded
  by the number of top-level entries, not by the tree's size; blob and
  tree names both count) and keeps only the candidates whose first
  segment is one of them. Everything else is prose this check cannot
  speak about: a ratio ("3/4"), a date ("2026/10/17"), a port
  ("8080/health"), another repository's path, a module path, a job name.
  Extraction is deliberately over-matching — it judges shape, not
  meaning — so rooting is what separates a path from a token that merely
  contains a slash, and the trade-off is stated: an absent path under a
  real top-level entry is still found and still named, while a citation
  the extractor cannot resolve to a path-shaped candidate, or whose
  top-level entry the published head does not carry, is left alone
  rather than guessed at.
- **Fails open.** A candidate is named only on the positive observation
  that reports its absence: exit success with empty output. Any probe
  that cannot be completed — unresolvable head, unreadable top-level
  listing, git unavailable, cancelled context — yields no finding and no
  error, as does a citation that survived extraction but rooted nowhere.
  (`ls-tree` rather than `cat-file -e` for exactly this reason: git
  exits 128 for both an absent path and an unreadable head, so exit
  status cannot separate absence from probe failure.) The one
  fail-open verdict in a plan that is otherwise fail-closed, because a
  defect the model cannot repair by editing the body must never be handed
  to it as one. No new cap, budget, timer, or gate.
- **Rides the existing repair.** A named missing path is a criterion
  defect: it earns the same bounded `criteria-repair` continuation as a
  validator error, the same combined `body-repair` when linkage also
  failed, and both `NeedsHuman` reasons name the paths and the probed
  head's short sha. One shared #170 slot, no second budget, and the
  repair instruction states the observation plus the two honest options
  — cite a path that exists in the published head, or record the
  criterion honestly (e.g. a `manual-verification` row with a note) —
  rather than adding a rule the pinned set does not carry.
- **Unchanged without a pin.** No envelope pinned, or an empty pinned
  set, remains a vacuous pass with no probe and no finding, exactly as
  before (#249).

No component of
this contract runs tests, interprets reference text, or grows an LLM proof
system.

**Unknown and unverified criteria.** A criterion recorded `unknown`,
`not-implemented`, or `manual-verification` is a **legal, visible record**,
not a repair trigger: forcing the model to keep working would constrain
behavior the harness cannot judge. What it forfeits is the claim — derived
coverage is `partial`, the run's outcome comment and telemetry carry the
per-status counts and coverage, and the PR never presents itself as fully
verified. Merge stays a human gate.

**Missing or malformed evidence is repairable, then terminal.** A missing
block, a missing criterion ID, a stale digest, an invented ID, a bad
status shape, or a coverage lie is a *repairable* handoff defect: the
native engine refuses the `CreatePullRequest` (or body update) with the
structured `*CriteriaError` before any forge write; the legacy path asks
the captured OpenCode session to repair the body under the **same** #170
continuation budget as linkage — one combined repair instruction naming
every violation (both contracts' failures, the trusted identity,
pinned criteria verbatim, the exact grammar, the PR number, the remaining
budget), so one continuation can fix both. A citation of a file the
published head does not carry joins that class on the legacy path only
(#367, described above): it rides the identical bounded repair — one
`criteria-repair`, or one combined `body-repair` when linkage also failed,
one #170 slot — and the instruction names the paths and the probed head
instead of adding a rule. When the budget is exhausted,
no session was captured, or the run is shutting down, the legacy path
terminates `NeedsHuman` with reason `incomplete handoff: criterion
evidence …` (category `verifier-rejection`), preserving the declared
outcome tag exactly as the linkage plan does. A forge read failure during
the legacy check is fail-closed exactly as #248's is. A pinned envelope
that decodes but names a source issue other than the run's trusted
`COURIER_REPO`/`COURIER_REF` identity — or that cannot be bound because
the run carries no valid trusted identity — is not a repairable defect:
it is a policy mismatch between operator-side inputs, so the legacy check
fails closed to `NeedsHuman` without consuming the budget, before the
forge is read and before the observation is classified, so a missing or
draft PR — whose own #247 continuation would otherwise fire — never
spends a slot on an admission-time fault (#282). The binding is checked
before the pinned-empty shortcut, and it mirrors the native engine's
`Criteria.Issue == SourceIssue` binding in `internal/broker/policy.go`.
An unbound envelope fails closed in the same
position, so a malformed `COURIER_REPO`/`COURIER_REF` cannot hand off
`Verifying` with neither contract enforced; an envelope that does not decode
is the same admission-time fault (it is read once from the pod environment
and no later observation can change it) and likewise fails closed on sight,
with `linkage.read.failed` kind `criteria` and no #170 slot spent. Because
the envelope check precedes the forge read, a run carrying a broken
envelope is settled as an envelope fault even when the forge is itself
unreadable — the admission-time fault is reported under its own `kind`
rather than as a retryable `linkage` read failure. A run that never
opened a PR passes the *evidence* check vacuously — PR absence is not
an evidence violation; it is caught separately by the #247 pre-handoff
observation, which refuses the handoff before evidence is ever
consulted. An empty pinned set also passes vacuously.

**Retry and idempotency.** Validation is pure over `(body, pinned set)`:
native rejections happen before any write, so a corrected body is a fresh
call with nothing to undo, and the existing duplicate-PR guard still
precedes creation; legacy repairs go through the captured session (the
executor never edits the PR itself), and each continuation re-reads the
live PR body — the world wins over any cached read. A repair that changes
nothing hits the existing no-progress fingerprint guard and terminates
`NeedsHuman (looping)`. Repeated validation of the same body is a no-op.

**Reviewer consumption.** The block renders as a plain list on the PR:
each pinned criterion's status and citations sit next to the closing
reference, so a reviewer audits per-criterion sufficiency — the judgment
the validator explicitly does not make — in one place, and the coverage
header is the one-glance completeness signal. The worker holds no
authority to rewrite issue requirements: it cannot add criteria (unknown
IDs refused), delete them (missing IDs refused), restate them (digests
pinned), or upgrade its own claim past the derived coverage.

**Shared truth table.** `internal/broker/criteria_test.go` pins the real
dogfood material — the live miso-gallery#502 issue body and reconstructed
stages of #505 — plus the mechanical rule table: the deficient handoff
(no block), the partial block omitting the built-image endpoint criterion
while claiming full, the overstated block adding a `.dockerignore`
criterion as regression-enforced with no citable automation, the honest
full record, and the honest partial record with manual verification.
Revision-pin mutations (edit/reorder/add/delete change the set digest) and
extraction grammar and bounds are covered there too. Both paths consume
the same pure functions; divergent acceptance rules between the paths
would be a correctness regression.

The boundary wiring is landed issue by issue, not in the design change
itself: the admission snapshot (trusted issue read + pin, #280) and native
enforcement at `PolicyEngine.CreatePullRequest` (#281 — refusal before any
forge write, the structured `*CriteriaError` surfaced through the broker API,
and derived coverage on the create response) are merged; legacy enforcement
at the handoff (#282) remains one bounded implementation issue. The contract
is the tested pure seam both paths call.

### Git policy and publication (#118)

The operator owns one immutable, run-UID-bound publication policy. It resolves
and durably binds this policy at admission from the immutable `CoderRun` spec,
trusted provider configuration, and live provider reads; a broker must verify
the same policy on restart, not reconstruct it from model input or mutable
`status.branch`. Forge credentials come only from administrator-configured provider
credentials, never the run or lane; the per-run Kubernetes broker identity is
separate (§3). The existing branch-only
`ExistingPRHeadResolver` and GitHub `Ref` are insufficient: #94/#121/#122 must
carry the actual PR head repository/ref/OID and persist the resolved policy
before secure publication can be enabled. No legacy fallback is authorized.

| Immutable field | Admission source and meaning |
|---|---|
| `provider` | registered provider configuration, including canonical forge endpoint and credential reference (broker-only) |
| `baseRepo`, `baseRef`, `baseOID` | provider-canonical repository resolved from `spec.repo`; `resolve-issue` uses its live default branch, `fix-pr` the existing PR's live target branch; record that ref's tip OID, and require the PR base repository/ref to match on later reads |
| `workRepo`, `workRef` | `resolve-issue`: `baseRepo` and `courier/<owner>/<repo>/issue-<number>` derived by the operator from the canonical repository and issue number; `fix-pr`: the live PR's *actual* head repository and ref, including a fork |
| `prNumber`, `headAnchorOID` | `fix-pr` only: `spec.ref` and the live PR head OID at admission; `resolve-issue` has no existing PR or head anchor |

The provider returns canonical stable repository identities, exact branch refs
and opaque git OIDs (including SHA-256 repositories); broker and operator
compare repository identities using that provider's rules and ref/OID bytes
exactly. Admission checks the effective protection for the pinned destination
in both repositories (once if identical). Every credential-bound check — write
access and protection-under-credential — runs in the **broker's own startup
preflight**, the only component that resolves the credential: the broker
verifies the configured credential can write the pinned destinations before it
serves, and a broker that cannot is the §5 capability-health failure path
(`NeedsHuman`), not a degraded launch. The operator never resolves a provider
credential.
Missing/deleted/ambiguous repository, base, head, ref, protection
information, or insufficient read permission denies admission rather than
assuming a branch is safe. No arbitrary clone URL or forge endpoint is
accepted from the model. `baseOID` is an admission snapshot, not a permanent
fence: ordinary movement of the pinned base ref requires control to re-sync
before publishing work against the new base; it neither changes the pinned
base identity nor alone causes `NeedsHuman`. A changed/deleted base identity
or a PR retargeted to a different base ref/repository does.

For `resolve-issue`, the pinned destination is the base repository's derived
work ref. A ref absent at admission is recorded as initially absent; a ref
already present needs an explicit live tip at admission. A new ref racing into
existence is a foreign tip unless it equals the exact proposed OID; never
accept an arbitrary occupant of the same ref as this run's publication. The adoption guard must search for an existing PR by the *full*
head repository/ref: an open PR is reported for review without adoption or
publication; a closed/merged PR on that head requires `NeedsHuman`. A PR from
a fork with the same branch name is not this run's PR. Creation is permitted
only with pinned base and work identities after confirming the run's proposed
OID is the live head and no existing PR has the pinned head identity. Re-read
the *returned PR number* and verify its live base and full head repo/ref/OID
before claiming success. A concurrent duplicate PR or divergent head requires
`NeedsHuman`; do not claim someone else's PR as the run's own.
For `fix-pr`, publication always goes to the pinned actual PR head repository
and ref; a same-name base-repository branch is never a fallback (#94). PR
updates may change only title/body/draft and comments on the pinned PR;
creating or retargeting a `fix-pr` PR is denied. Both modes deny merge, close,
force push, raw REST/MCP passthrough, arbitrary destinations and URL proxying.
Any path scope, if present, must also be operator-resolved, never model-supplied.

A default or effectively protected ref in **either** repository is not a
publication destination. Protection includes provider rulesets and branch
policies that apply to this ref and credential, not merely a list of explicitly
protected branch names. The broker obtains authoritative live effective
protection and work-repository write permission immediately before every
push; it does not infer writability from a successful read or from a rule
bypass credential. A writable unprotected fork head is allowed. If protection
cannot be established reliably or the credential cannot write the actual fork
head, deny with actionable `NeedsHuman`, never create a replacement branch.
The broker credential must have no protected-ref bypass grant: the provider
must reject a write that becomes protected between the live read and push.
Post-push observation detects a policy change but cannot undo a completed
write; this is a required provider-side safety property, not a claimed atomic
read/push transaction. No provider API can prove the *absence* of a bypass
grant, so the property is a provisioning attestation, not a verified fact: the
deployment must provision the broker's provider credential as a dedicated,
non-administrative machine identity whose scope excludes bypass, and a
deployment that cannot provision such a credential fails closed. That
attestation is a stated residual risk in §12.

**Normal non-force publication, including crashes and races:**

1. Trusted control validates the untrusted artifact (objects, ancestry,
   repository and any operator-resolved path policy), integrates it in its
   private tree, and supplies the proposed commit OID and the expected work
   tip from a fresh observation. Broker independently checks even authenticated control request values
   against its persisted policy and fresh git observation; control must not
   forward model-provided destinations or tip assertions as authority. For a new resolve work ref,
   the expected tip is absent; only creation of that exact absent ref is
   permitted. Otherwise the candidate must fast-forward the expected tip.
2. Broker re-reads live base identity/tip, full `fix-pr` PR base and head
   repo/ref/OID, work tip and both repositories' effective protection and
   write access. It refuses a mismatch with the expected tip, a foreign PR
   head, a protected destination or a changed base identity. A base tip that
   moved requires control to re-sync; it never authorizes a new destination.
3. Broker uses only an ordinary git push of the proposed commit to the pinned
   work ref (never force or force-with-lease). The pre-read is not atomic with
   the push: a concurrent fast-forward may win, in which case the push fails
   non-fast-forward; even a concurrent advancement that remains an ancestor
   of the candidate is not adopted without re-observation. No provider-side
   conditional ref update is required or used as a substitute for this push.
4. Whether the push succeeded, failed or its outcome was lost, broker re-reads
   the work tip, effective protections, and (for fix-pr) the live PR base and
   full head repo/ref/OID. It confirms publication only if the tip equals the
   exact proposed OID, the PR still points to that pinned head and OID, and
   the base/protection checks still pass. If the tip has moved again, do not
   claim success even if this run's commit is an ancestor. Re-observe and
   classify the new world; never silently push over it.

A `fix-pr` run recognizes as its own prior head only the admission anchor or
a proposed OID confirmed at the live remote for this run. A broker-confirmed
`status.lastCommit` is one OID, not a chain: recheck exact equality against
the live work tip and PR head on resume. A pending proposed OID after an
interrupted push may be recovered only from the run-UID-bound trusted control
integration tree or a trusted checkpoint written before pushing, and confirmed
by *exact* live equality before status advances. That recovery is bounded to
one control incarnation: the §7 checkpoint type has no pending-OID field by
design, and the integration tree is pod-local, so a replaced control pod or a
replaced broker cannot reconstruct a pending OID. An interrupted push that
survives neither is terminal `NeedsHuman` — it fails closed rather than
inferring ownership from ancestry or retrying an uncertain push. Any other tip
is foreign, including a descendant of a run commit
advanced by another actor, and requires `NeedsHuman` rather than an automatic
rebase. The live world always wins over a checkpoint. A repeated OID is an
idempotent no-op only while the live tip and (on `fix-pr`) PR head still
equal it. The broker rereads live state before each status attempt and
uses Kubernetes `metadata.resourceVersion` compare-and-swap against the
current named CoderRun, checking its UID and current control-pod incarnation;
a conflict restarts world verification, and a stale confirmation is
dropped. A failed/uncertain push advances neither the completed-brief checkpoint nor `lastCommit`. There is no atomic
forge/status transaction.

| Condition after live re-observation | Outcome |
|---|---|
| Temporary read/push transport failure, rate limit or provider outage, with no evidence of a changed identity | Retry with same immutable policy; first re-read world, no checkpoint advance |
| Base tip advances on the same pinned ref | Re-sync trusted integration with live base and retry; never mutate the policy |
| Pre-push tip differs from expected, non-fast-forward or post-push tip differs from proposal | Re-observe; exact confirmed proposed OID is idempotent success; an unchanged own anchor/confirmed tip permits retry from that exact tip; otherwise foreign/ambiguous tip is `NeedsHuman` |
| PR base/head repo/ref changes, or PR head OID is not the anchor, confirmed own tip or exact pending proposal | `NeedsHuman` with actual and pinned PR identities; never follow the moved head |
| Work ref deleted after admission, or pinned base ref/repository disappears | `NeedsHuman`; only an initially absent resolve work ref may be created |
| Work ref becomes default/protected in either repository; protection or writability cannot be verified | `NeedsHuman` identifying repo/ref and missing or conflicting capability |
| Actual fork head is unwritable by broker-configured credential | `NeedsHuman` identifying fork repo/ref and missing write grant (#94) |
| Provider lacks required head/base/ref reads, effective protection/write checks, normal git push or post-push observations | `NeedsHuman` at admission with missing capability; no raw fallback |

Provider adapters translate these semantic reads and errors without exposing
provider-specific verbs to core. The current GitHub client does **not** yet
expose full head-repo identity or effective protection; it is not a secure
broker implementation. #121 provides the typed provider contract and #122
must implement its enforcement; unsupported providers fail closed rather than
silently dropping a check. Terminal `NeedsHuman` diagnostics name the pinned
and observed identities and required human action without leaking credentials.

Trusted control relays to the worker only a sanitized, read-only repository
pack or
workspace, never network access. Trusted control validates worker artifacts
before integration; the broker independently enforces the policy at every
git/forge mutation. Comments are data, never instructions to fetch URLs.

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

### Operator control request and soft-stop (#238)

The controller translates the human ingress annotation
`courier.misospace.dev/soft-stop` into operator-owned
`CoderRun.status.controlRequest`. It reads the annotation in `observeRunning`
(where the live coordinator pod is already resolved) and writes the record
through the existing status patch path when the value differs from the recorded
`id`; the field is **not** set-once, unlike the lifecycle timestamps and the
publication policy. The annotation value is a non-empty, bounded opaque request
ID; clearing or changing it clears or replaces the record. The record contains
`id`, `kind` (CRD enum closed to `soft-stop` in v1), `targetPodUID`, and
`requestedAt`.

`CoderRun.status.controlRequestAck` is harness-owned and contains `id` and `at`,
added to the harness-owned patch set. Trusted control writes it through the
existing trusted status path after the request is served and before setting the
stop latch. Inside the CAS update, the broker requires a live
`status.controlRequest`, its `id` equal to the acknowledgement ID, and its
`targetPodUID` equal to the authenticated live control pod UID; otherwise it
returns 422.

The route constant is `PathControlRequest = "/trusted/v1/control-request"`,
with a canonical-path check mirroring the status route. An authenticated GET on
the same listener as `/trusted/v1/status` returns `200` with
`{id, kind, targetPodUID, requestedAt}` iff the run UID matches and
`targetPodUID` equals the live control pod UID; otherwise it returns `404`. The
broker's existing `get` access to the named run and control pod is sufficient;
no new broker or operator RBAC is needed.

The harness keeps a mutex-guarded consumed set per incarnation and adds an ID
only after the ack write succeeds; a consumed ID delivered again emits no second
ack or control turn. `BriefRegistry.HasInFlight()` — registered briefs with no
recorded terminal result, including one still integrating — gates exit. The
latch stops new `runDelegate` calls only; the request is polled at step
boundaries and inside the existing per-operation brief-wait poll, with no new
timer, busy loop, or wall-clock bound. Stop ordering is serve, durable ack,
stop new briefs, let in-flight briefs reach terminal and integrate and publish,
end the model loop, reconcile status debt, flush active operations, derive the
outcome, and exit. The soft-stop finish derives `changes`, `no_change_needed`,
or `blocked_external` from the work-ref snapshot beyond the admission anchor
via the existing broker snapshot, not from a model declaration. `ctx`
cancellation (SIGTERM) remains a separate path and is never conflated with
soft-stop; the boundary preserves per-brief publication and never deletes the
pod (#238).

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
plan, integrate, locally validate, publish, and hand off. Delegation covers
bounded work only — implementation, research, review — never the terminal
contract. The coordinator validates delegated results against the integrated
work and runs relevant local checks itself, fixing failures before declaring completion.
Once local validation passes and publication succeeds, the operator owns external
verification of the pull request and CI. The coordinator publishes
through the broker, never through a worker. Completed briefs may publish before
control returns; local validation therefore runs on a freshly materialized
snapshot of the published integration head before declaring completion, not as
a claim that every publication was preceded by coordinator validation.

**Spawn brief schema and authenticated worker protocol.** Each delegation is a
typed brief carrying a stable `briefID` (unique within the run), objective,
settled decisions with exact values, owned files, non-goals, and an observable
success check. Each dispatch or cancellation is a signed canonical envelope
containing the `CoderRun` UID, control-pod UID, worker-pod UID, `briefID`,
`opID`, kind (`dispatch`, `cancel`, or `release`), payload digest, sequence, nonce, and
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
fresh `opID` after termination is confirmed. Each task leads its own process
group, so cancellation terminates the task's whole tree — the tasks that reach
the worker are `sh -c ...`, whose real work runs in descendants — and the
worker acknowledges cancellation only after it has proven that no live process
remains in that group (or confirmation it never started). Work that leaves the
group cannot be proven gone, so it is reported as unverified rather than
acknowledged, and trusted control keeps the operation's active status until
termination is verified. Only then may trusted control clear
active-operation status. A replacement worker (new pod UID) that never saw
that `opID` treats a late cancel as unknown data and runs nothing; the
authoritative termination of a fenced stale worker is the operator's pod kill,
and op cleanup follows observed pod termination, not a worker acknowledgement.
A worker result is keyed by `briefID` and contains a
summary and artifact bundle, all treated as untrusted. Control releases a
consumed result payload with a signed release envelope bound to the same
`opID`; the worker retains only the compact terminal status and continues to
reject redelivery of that `opID`.

If dispatch transport fails ambiguously, control does not automatically
redeliver: it cancels and reconciles until process termination is proven. If
control dies, the operator fences and replaces the old worker. The worker's
replay state is intentionally process-memory-only because its pod cannot restart
its container in place. Integration deduplicates completed work against the live
branch and checkpoint. Publication reconciles the remote ref before retrying.
Forge writes use a provider idempotency key where supported or re-read the live
PR/comment before retrying; if duplicate comments cannot be ruled out, surface
the uncertainty rather than blindly replaying them. Subagents cannot publish.

### Worker artifact contract

The only accepted artifact is a **git bundle** produced inside the worker's
sanitized snapshot — never patches, diffs, or worker-computed text to re-apply —
so integration consumes git's hash-verified objects rather than re-deriving
work from worker-authored text. The bundle carries exactly one result ref,
whose name control derives from the `briefID` (for example
`refs/courier/briefs/<briefID>`); its head commit's ancestry must contain the
exact base tip of the snapshot control dispatched. Control knows that OID
because it rendered the snapshot, so a bundle built on any other base is
rejected without interpretation.

Validation runs in trusted control, in order, before any integration:

1. `git bundle verify` against control's private tree: every prerequisite is
   present and the object closure is intact under git's own hash checks.
2. **Bounds**, as fixed numeric constants the implementation does not derive
   from model or worker input: total bundle size, unpacked size, unpacked
   object count, and per-blob size (initially 64 MiB bundle, 64 MiB unpacked,
   10⁵ objects, 16 MiB per blob). A bundle over any bound is rejected whole,
   never truncated; objects beyond the result ref's closure are ignored and
   never integrated, and bounds apply regardless.
3. Ref-set equality: the bundle advertises exactly the expected result ref —
   no extra refs, no unexpected names, no ref outside the `refs/courier/`
   namespace. The import is **refless**: control fetches exactly the expected
   OID and never applies the bundle's own ref declarations, so no
   worker-supplied ref name can enter the private tree.
4. Ancestry: the result head reaches the dispatched base tip.
5. **Path policy**: when the operator-resolved policy scopes paths, the
   changed-path set of the result commit against the base must lie entirely
   inside that scope; absent an explicit scope, the whole repository is in
   scope for the pinned work ref. Path policy evaluates changed diff paths: a
   new or modified symlink is a change to its own path only; gitlinks
   (submodule pointers) are permitted as inert tree data, and the changed
   gitlink path counts against the scope. Path scope is operator-resolved
   (§4), never brief- or worker-supplied.

Integration and every inspection of worker-supplied objects run with the same
hostile-repository git hardening the broker uses — a scrubbed environment, no
system or repository config trust, no hooks, no attribute-defined filters or
drivers, no submodule initialization or fetch, `protocol.*.allow=never` — and
artifact content is data that is never executed. Control materializes
integration trees without following artifact-supplied symlinks.

Worker-declared metadata — summaries, file lists, claimed test results, exit
claims — is never validation input; only bundle contents checked against
control's own record of the dispatch. A rejected artifact is untrusted data:
the brief fails, control may re-dispatch a corrected brief, and repeated
rejection is a workload signal under §8, not infrastructure failure. Accepted
bundles are fetched into the private tree, integrated per the brief, committed
at §7's cadence, and published only through the broker's independent §4
enforcement. Nothing in the artifact path publishes, writes status, or mutates
policy.

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

The harness supplies the worker a sanitized read-only repo snapshot — a git
bundle unpacked into an ephemeral workspace — plus only the task inputs.
Control's private integration tree is the source of every snapshot: the tree
is seeded once per control incarnation through the broker's typed snapshot
endpoint (the broker renders a bundle from the pinned work ref it observes
live, or the pinned base ref when the work ref was admitted absent), and
each brief's snapshot is then rendered locally from the integrated tree, so
a snapshot always carries every integrated — and possibly still unpublished
— brief. The worker's unpack and pack tasks are control-composed fixed
scripts over that bundle: unpack materializes the snapshot work ref as the
workspace; pack captures the workspace's committed state as the brief's
`refs/courier/briefs/<briefID>` bundle. The worker executes shell and returns a summary, evidence, and
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

### Context budgets and compaction (#243)

The harness owns the context policy; the executor implements it. Today the
native harness declares no budget and the OpenCode executor's auto-compaction
fires reactively at its own default threshold, so a worker can ride its prompt
past the bound model alias's input limit, LiteLLM diverts the oversize request
to a fallback alias, and only then does compaction land. That is capacity
management by accident. The policy below makes the limit a declared,
profile-owned value and the compaction trigger a mechanical, pre-ceiling,
step-boundary decision. It does not change the OpenCode default: an immediate
legacy mitigation, if warranted, is a separate bounded executor/config ticket,
and no implicit context-window fallback is ever treated as successful capacity
management. The pure resolution, trigger, and preservation rules live in
`internal/harness/budget.go` with the shared truth table in
`internal/harness/budget_test.go`.

**Per-role budget schema.** A lane may declare an optional `spec.context`
policy on `LaneProfileSpec`, beside the existing per-role `roles` map:

- `context.compactAtPercent` (default 75, range 1–99) is the fraction of the
  effective input limit at which a step-boundary compaction fires.
- `context.outputReservation` reserves tokens for the next turn's maximum
  output.
- `context.roles.<role>.maxInputTokens` declares a ceiling for the role's
  bound model; `context.roles.<role>` may override `compactAtPercent` and
  `outputReservation`. An absent `context` leaves the harness owning no budget
  and the executor default standing — explicitly not capacity management.

**Effective limit = min(declared, verified).** At session start the harness
reads the bound model's advertised input limit from the gateway's model-info
endpoint and takes the **minimum** of that verified capability and the lane's
declared ceiling: a profile can only lower the effective limit, never raise it
past what the gateway verifies. Resolution reuses the §5 capability
vocabulary: a verified read is `healthy`; a declared ceiling used because the
gateway could not verify is `configured` — in use, never treated as healthy;
neither declared nor verified is `unavailable`, and the harness refuses to
substitute an implicit default. The gateway read is redacted like every other
stream/status path: a fixed category, never a provider body or key material.

**Compact before the ceiling, at a step boundary.** The compact point is
`floor(effective × compactAtPercent / 100)`. At a step boundary — between
turns, never mid-turn — the harness compacts when the transcript has reached
the compact point **or** when the next turn's prompt plus the output
reservation would exceed the effective limit. The boundary is measured over
the full next request: the transcript **and** the serialized tool
declarations the gateway sends with every turn, since they consume input
capacity each turn even though no compaction digests them. After compaction
the invariant `promptTokens + outputReservation ≤ effective` must hold, on
that same full-request account; if it cannot, the harness fails closed and
surfaces the condition rather than truncating mid-task. The two acceptance conditions fail independently: a digest can
reclaim tokens and still leave the protected tail too large to fit, in which
case every digestible turn has already been spent and there is no progress left
to make.

A successful compaction also returns a calibrated `BudgetAfter` whose compact
point is raised to at least `After+1` so the next step boundary does not
re-fire on the just-compacted state. The compact point alone is not a
sufficient exit condition for a session loop: a transcript that compacts at
the point can still land above it, and re-digesting the digest would reclaim
zero tokens and surface `ErrCompactionCannotFit` even though the
post-condition held. The executor (and any caller of repeated compactions)
must use `BudgetAfter`, not the original budget, for the next decision.

**Digest, not prune; preserve settled state.** Compaction replaces the oldest
non-protected turns with a single harness-composed digest — a bounded index of
what was compacted plus the settled-state block, not a second model call: the
digest's cost has to be known before it is sent, and an extra turn to produce
it would itself be context the policy is trying to bound. The digest lands
whole at the start of the span it replaces, never between an assistant's tool
requests and the results answering them. It never discards the
brief's pinned objective, the settled `Decision`s, the `OwnedFiles`, or the
most recent turn: those are protected, re-injected verbatim into the digest.
The durable record remains the §7 checkpoint ladder — `plan` plus ordered
`completedBriefs` — so a resumed session reconstructs settled state from the
world and the checkpoint, never from a lossy transcript. A digest is accepted
only when it both fits the post-condition and actually reclaims tokens
(`After < Before`): a digest that costs as much as it replaces would re-trigger
on the next boundary forever, so that degenerate case fails closed too.
Prune-only compaction is rejected because it silently drops task state.

**Telemetry.** Every compaction emits the run event `session.compacted`
(`log.EventSessionCompacted`) carrying the role, the bound model, and
before/after token counts plus both the net savings (the user-facing
"reclaimed" — `Before - After`) and the gross digest size (the sum of message
tokens the digest replaced), so operators observe the behavior in run
telemetry instead of inferring it from gateway failover alerts. It is a log
event, never status, and never lets a model assert liveness or completion.

Implementation is three bounded, file-scoped tickets — #295 (CRD per-role
budget schema + bootstrap mirror), #296 (gateway model-capability probe +
effective-limit resolution + capability gate), #297 (step-boundary compaction
enforcement + `session.compacted` emitter). #297 has landed: the session loop
calls the policy between turns and never mid-turn, per-role budgets are
resolved once in `NewCoordinator` from an injected `ContextBudgets` map, a
digest that cannot fit fails the run rather than truncating, and
`courier-control` emits `session.compacted`. A role with no resolved budget
still owns no policy and never compacts: #296 supplies the verified side, but
nothing populates `ContextBudgets` until #295's declared schema lands, so no
production session carries a budget yet.

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

#126 implements the following **harness-owned CR-status state** and ships the
regenerated CRD/deepcopy:

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
the current controller-runtime cached client); #126 wires a direct API
reader for this decision, and the reap decision reads the live API. Read
errors, uncertain identity, or contradictory
observations mean **do not reap; requeue and retry observation** with a
structured diagnostic, not a successful run heartbeat. If phase, resource
version, or pod UID changes before deletion, re-evaluate. Delete with a
Kubernetes UID precondition on the observed pod, never by name alone.
`checkLiveness` reads the run and the run's pods through the direct API
reader for the whole decision, and its reap delete carries the observed
pod's UID precondition; a conflicted delete re-observes instead of acting.
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
`observeRunning` observes worker pod termination through the topology
observation — a worker or broker that is gone or terminating while control
still runs fences the round and relaunches against the crashloop ceiling —
and the reap decision's entry-validity rules fence stale entries by ignoring
them. The operator ignores old entries rather than patching harness-owned
fields; the new control incarnation clears them via the broker at process
start after reconciling the world. No entry protects a dead pod.

In the absence of valid entries, retain #12's behavior: a **nil heartbeat is
not stall evidence**, a fresh current-incarnation heartbeat resets the
crashloop streak while a recoverable coordinator is observable, a stale
current-incarnation heartbeat permits reap outside
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
| No valid entry; heartbeat present but not attributable to the current coordinator pod (prior incarnation); startup grace ended | No heartbeat-stall reap — the timestamp is not evidence for this pod; the pod-death backstop still acts. |
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
until it is durable. The durability gate is mechanical, not
model-respected: a status write that failed through its retry budget is
retained as status debt in trusted control, and the run's terminal path
reconciles that debt before honoring any declared outcome — a model
declaration can never turn an unpersisted completed unit into a successful
run ending. The reconciliation retry revalidates the recorded OID against
the live work ref; a persistent failure is an infrastructure failure for
the operator to relaunch, which resumes remote-first.

**Heartbeat.** The harness writes the activity heartbeat only on successful
model-stream activity and verified tool boundaries, coalescing writes to at
most one patch per cadence. Failed attempts, retries, and raw stdout are
excluded — a retry storm is active, not alive.

On resume, reread the world first; resolve conflicts in favor of the world,
then use checkpoint and branch history as recovery hints. Do not promise
atomicity between a forge push and Kubernetes status: those are two systems.
Use idempotent retries and reconciliation.

## 8. Failure classification

The coordinator declares its outcome in the exact file path supplied in its
goal. That file lives in executor-owned per-run scratch outside the target
worktree; it is control metadata, not a repository file. The executor validates
the declaration against the world before terminalizing: exit `0` (`changes`)
reaches `Verifying` only when the coordinator validates integrated work locally,
fixes failures, and commits are reachable from the run branch; exit `2`
(`needs_decision`) reaches `NeedsHuman` and `blocked_external` reaches the
terminal, recoverable `BlockedExternal` phase; exit `3` (`no_change_needed`)
reaches `AwaitingReview` with source `in-review` for resolve-issue runs, and for
fix-pr runs are settled through `Verifying`: a verified merge reaches `Done`, an open green no-change PR
waits recoverably in `BlockedExternal`, red CI reaches retryable `Failed`, and
an unobservable PR reaches `NeedsHuman`. Green CI cannot certify queued review
feedback was addressed, so open no-change attempts report `waiting-external`
rather than authorizing `already_addressed` or claiming a push. Recovery requires
a fresh source attempt identity; prior runs and evidence remain retained.
An externally observed failed check reaches `Failed` and follows the source's
failure policy, while other nonzero exits also reach `Failed`. Session recovery
for undeclared recoverable endings is shipped: with a captured session, dirty,
off-branch, and no-work states resume under the #170 continuation budget and
no-progress guard; without a captured session, those endings fail as
incomplete. This does not change the declared-outcome exit mapping above. A
`no_change_needed` result never resolves the source on the coordinator's word
alone. A `blocked_external` result keeps the source item active (`in-progress`)
and reports a recoverable lifecycle result, so the source can re-issue the work
once the prerequisite is satisfied; it is never parked as a human `BLOCKED`
item. The recoverable signal must reach the source in a form it cannot mistake
for a normal failure: the Dispatch adapter publishes a dedicated
`waiting-external` task outcome and, for queue-backed follow-ups, a
generation-fenced `WAITING_EXTERNAL` queue mark, so the prerequisite wait does
not advance the issued generation or burn fix-attempt budget.

Outcome comments carry the full declared evidence, question, or missing
explanation after redaction. They are not shortened to the termination reason;
that separate reason is bounded and redacted before publication. Comment posting
is best-effort: failures are logged as `outcome.comment` and do not change the
run ending.

The criterion-evidence contract (#249, §4) adds one classification: a
Verifying handoff whose criterion evidence is missing or malformed beyond
its continuation budget terminates `NeedsHuman` with an explicit
`incomplete handoff` reason (`verifier-rejection` category) — the run does
not hand off as complete. A handoff whose criteria are recorded but
unverified (`manual-verification`, `not-implemented`, `unknown`) is **not**
a failure: it proceeds with derived coverage `partial` surfaced in the
outcome comment and telemetry, and never claims fully verified completion.

The native harness adds an explicit result contract on top of the exit code:
a structured terminal result (outcome + reason) written by trusted control
before process exit. The operator classifies from that trusted evidence plus
its own world verification, keeping two failure kinds distinct:

- **Workload failure** — local validation fails before completion. The
  coordinator uses its bounded run to address it or reports a genuine human
  decision; after publication, CI is observed by the operator and a failed check
  reaches `Failed` for the source to handle.
- **Infrastructure failure** — pod crash, OOM, model gateway down, a status
  write that will not land. This is the relaunch/resume case bounded by the
  crashloop counter, not a terminal verdict on the work.

A raw nonzero exit is not a classification; the explicit result plus the world
is.

## 9. Migration

- **Legacy mode (current):** OpenCode and model-controlled tools share a pod
  containing credentials. Label it insecure; do not describe it as protected by
  prompt permissions, process boundaries, or NetworkPolicy. It may remain
  available as a stopgap, with its known risks explicit. A credential exposed
  to any legacy pod is tainted for provider-scope purposes: it must not be
  referenced by any broker registration or secure run, because one legacy run
  otherwise defeats the isolation every secure run sharing that credential
  claims.
- **Legacy compatibility of the PR-body contracts:** while OpenCode remains
  the live runtime, the legacy executor enforces the same source-issue
  linkage (#248) and criterion-evidence (#249) contracts as the native
  broker, against the operator-projected trusted identity and pinned
  criterion set, with one shared #170 continuation budget for repairs.
  Native-only enforcement would leave today's running system exposed; the
  legacy wiring is temporary and is removed with the executor, while the
  pure validators and truth tables are shared forever.
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
  assumption of §3 is stated, not assumed away. The dedicated run namespace is
  part of that verification: preflight checks the namespace boundary under
  which only the operator creates or mutates pods, to the extent RBAC review
  allows. Inspect env, argv, image pull
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
  or treated as data. The artifact contract (§5) is exercised directly: a
  bundle built on any base other than the dispatched snapshot tip is rejected
  without interpretation; a bundle over any bound (bundle size, unpacked size,
  object count, blob size) is rejected whole; extra refs, unexpected ref
  names, or refs outside the `refs/courier/` namespace are rejected, and
  bundle-declared refs are never applied to the private tree (refless import);
  a changed-path set outside the operator-resolved path scope is rejected,
  including a symlink counted at its own path; artifacts carrying hostile
  repository config — hooks, attribute-defined filters, `.gitmodules`
  initialization, `protocol.ext` transport — are integrated only under the
  hardened git environment and the hostile surfaces are inert; and valid
  worker-declared metadata never changes a validation outcome. An honest
  bundle on the expected base with in-scope changes integrates and publishes
  through §4 enforcement.
- **Revocation:** the CoderRun finalizer is idempotent — a rerun after partial
  failure (operator crash mid-sequence) resumes the ordered revocation,
  already-removed objects are treated as done, and no grant is re-provisioned.
- **Provider registry (§4):** a run whose `spec.repo` matches zero or more
  than one registration fails closed at admission with an actionable
  `NeedsHuman`; selection is case-insensitive on `owner/name`; the persisted
  policy and the broker projection carry the canonical identity and the same
  registration name, and a `ProviderConfigRef` mismatch refuses service; a
  projection whose endpoint or credential-reference digest does not match the
  persisted policy refuses service; a push endpoint not derived from the
  pinned registration's git endpoint is refused; a broker pod's projection
  holds exactly one provider and no request can add, swap, or re-endpoint it;
  structurally invalid registries fail operator startup; an unreachable
  endpoint or missing Secret is an admission-time fail-closed `NeedsHuman`,
  never a fallback.
- **Broker policy:** deny merge, force push, default/protected refs in **both**
  the base and the work repository, foreign base/repository, an arbitrary
  destination, unexpected `fix-pr` head identity, arbitrary URLs, comments that
  trigger proxying, stale expected OIDs, and ref races. Verify an allowed
  writable fork head works without substituting a same-named base branch; an
  unwritable fork returns actionable `NeedsHuman` naming the missing write
  grant (#94); a same-named branch in the base repository is a different
  identity and is never selected. Verify semantic validation on model-influenced
  requests, not just certificate checks.
- **Publication policy (#118):** the operator resolves the full policy from the
  `CoderRun` spec plus a live provider read at admission, and the broker pins
  every operation to it with no model input entering the policy. Pre-push
  revalidation reads the live tip, PR head identity, base ref, and both live
  protected sets; the post-push observation — not the push's exit status —
  declares success. A base-OID test advances the live base past the admission
  OID and sees the run re-sync and publish, never a denial. A head-anchor test
  confirms the run's own pushes advance past the anchor and are recognized on
  resume, while a foreign head (neither anchor nor published) stops publication
  and reaches `NeedsHuman`. Each row of the §4 failure matrix is exercised:
  transient errors requeue without mutating the policy; a competing tip is
  classified by exact own-OID equality, and a foreign tip reaches
  `NeedsHuman`; a protected-ref target, a
  deleted work ref, a deleted base ref, an unwritable fork, and a
  capability-missing provider each produce the classified actionable
  `NeedsHuman`, and an unsupported provider fails closed at admission.
  `lastCommit` confirms are idempotent only while the live tip still matches:
  a repeat is a no-op then, but a stale confirm is dropped, and a world conflict follows the world. No test assumes forge
  push and status are atomic.
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
  distinguishable from trusted evidence; the operator's exit-code mapping
  follows DESIGN.md's exit contract.
- **Criterion-evidence contract (§4, #249):** the shared truth table in
  `internal/broker/criteria_test.go` pins the dogfood material — the live
  miso-gallery#502 issue body pins exactly AC-1..AC-3 (its Evidence
  section's bullets are not criteria); the deficient #505 handoff (no
  block) fails with the missing-block sentinel naming the trusted issue
  and set digest; the partial block omitting the built-image endpoint
  criterion fails naming AC-2 and its `coverage=full` claim; the
  overstated block adding a `.dockerignore` criterion recorded
  regression-enforced without a citable location fails on the unknown ID,
  the status shape, and the coverage lie together; the honest full record
  passes; the honest partial record (manual verification) passes with
  derived `partial`. Revision-pin mutations (edit/reorder/add/delete
  criterion text) change the set digest and reject a stale block. These
  fixtures prove the validator's decision table — pin integrity, exact
  coverage, status/reference shape, coverage honesty — and deliberately
  prove nothing about whether a cited test semantically enforces its
  criterion: that audit belongs to the reviewer and, where demonstrated,
  to mutation evidence in the target repository.
- **Context budget and compaction (§5, #243):** the shared truth table in
  `internal/harness/budget_test.go` pins the four settled choices. Resolution:
  `effective = min(declared, gateway-verified)`; a verified read is `healthy`,
  a declared-only ceiling is `configured`, and neither is
  `ErrNoContextCapacity` — no implicit default. Trigger: a step boundary
  compacts at `floor(effective × compactAtPercent)` or when the next turn plus
  the output reservation would cross the effective limit, and never mid-turn.
  Preservation: digest selection never includes a protected turn (pinned
  brief/objective, settled `Decision`s, `OwnedFiles`, the most recent turn),
  the post-condition `After + outputReservation ≤ effective` holds whenever it
  compacts and the digest actually reclaims (`After < Before`), and it fails
  closed (`ErrCompactionCannotFit`) rather than truncating protected state.
  Hysteresis: a successful compaction returns a calibrated `BudgetAfter` whose
  compact point is raised above the post-compaction `After`, so the next step
  boundary's `ShouldCompact` does not re-fire on the just-compacted state —
  the post-condition alone is not a sufficient exit condition because
  re-digesting the digest itself would reclaim zero tokens. Telemetry: the
  canonical `session.compacted` event name lives in `internal/log/events.go`
  (`log.EventSessionCompacted`), the harness payload is `CompactEvent`
  carrying before/after/`NetSaved`/`DigestedTokens` counts (the net is the
  user-facing "reclaimed"; the gross is the size of the span the digest
  replaced), and the truth table proves both the digest accounting and the
  two-boundary hysteresis. The table proves the pure policy
  and deliberately proves nothing about the executor wiring, which
  #295/#296/#297 own.

## 11. Open blockers

1. The long-tool safety policy is settled in #119 (§6), but no progress oracle
   exists for a silent live wedge. It can persist indefinitely; human
   intervention remains the escape. #126 implements and tests the
   active-operation set, UID fencing, and API-backed operator decisions;
   #102 moves once the §12 gates are met.
2. Settled in #118 (§4): the immutable run publication policy schema, the
   operator-resolved admission inputs, pre/post-push live revalidation, the
   `NeedsHuman`/retryable matrix, and provider-neutral semantics with
   fail-closed unsupported providers, including the writable fork head required
   by #94. #122's broker enforcement layer, #123's per-run pod wiring and
   admission resolution, #124's native model client — stream
   normalization, LaneProfile role binding over the gateway, brief
   delegation with cancellation/retry dedupe, and startup capability
   health — and #125's worker-artifact validation, integration, and
   broker-mediated publication have landed (deployment-level secure mode).
   The native path's forge reads (`ForgeOps`) and pull-request
   creation/update are not yet wired to the broker's typed API; until they
   are, a published native run reaches `Verifying` without opening its PR —
   that wiring belongs to the terminal-result slice (#127), and the model
   client's forge tool stays unwired in the meantime.
3. Settled in §5 and implemented in #125: the worker artifact contract —
   git bundle only, exact base-tip ancestry binding, fixed size/object
   bounds, exact ref-set equality, operator-resolved path scope (none is
   resolved yet, so the whole repository is in scope), and worker metadata
   as never authority.
4. #120 settles per-run broker topology, workload identity, worker signing,
   network policy, and the first dependency-egress slice. #123's isolated
   pod/protocol wiring has landed but does not implement the Go cache; #136 is
   that separate bounded implementation issue, and dependency egress is not
   ready until it lands. The residual sanctioned-path source-exfiltration risk
   remains explicit.
5. Settled in §4: the provider registration surface — one deployment-level
   registry file loaded by the operator, pattern-based run selection with
   fail-closed ambiguity, canonical-identity enforcement, and a single-provider
   broker projection. #123 consumes it in the operator wiring; it is
   deployment/control-plane configuration, not a `LaneProfile` field.
6. Prove any OpenCode adapter's isolation and status guarantees before routing
   runs through it as secure; otherwise it remains legacy insecure mode.
7. Settled in §4 (#249): the acceptance-criterion evidence contract —
   trusted admission snapshot of the source issue, revision-pinned criterion
   set, evidence-block grammar and closed status vocabulary, mechanical
   completeness rules with their explicit non-claims (no semantic proof from
   cited tests), and the shared native/legacy enforcement shape with one
   bounded repair budget. Implementation is three bounded tickets — #280
   (admission pinning), #281 (native boundary), #282 (legacy boundary) —
   and until they land the contract exists as the tested pure seam in
   `internal/broker/criteria.go` that both paths will call.
8. Settled in §5 (#243): the harness-owned per-role context budget and
   pre-ceiling, step-boundary compaction policy. Implementation is three
   bounded tickets — #295 (CRD per-role budget schema + bootstrap mirror),
   #296 (gateway model-capability probe + effective-limit resolution +
   capability gate), #297 (step-boundary compaction enforcement +
   `session.compacted` telemetry) — and until they land the policy exists as
   the tested pure seam in `internal/harness/budget.go`. The OpenCode default
   threshold is unchanged by this design; any legacy mitigation is a separate
   bounded ticket.

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
| Namespace-local principal with pod-create in a run namespace forges a control, worker, or broker pod (same SA, labels, owner reference; mounts namespace Secrets by name; mints its own `courier-broker` token) | Deployment requirement (§3): runs live in a namespace dedicated to that purpose where no subject other than the operator can create, patch, or delete pods or workload-referencing specs; preflight verifies that boundary to the extent RBAC review allows — a grant's existence is provable, its absence is not. Where the boundary cannot be proven, this is a residual risk, not a closed hole. |
| Broker provider credential carries a protected-ref bypass grant | Not verifiable through provider APIs (§4); the deployment attests it by provisioning a dedicated non-administrative machine identity, and secure admission fails closed when it cannot. Residual where the attestation is wrong. |
| Pod recreated with new UID or stale task arrives after replacement | Broker rejects old pod-bound token/current-UID mismatch; operator rotates signing key and replaces worker, signed envelopes bind both UIDs. Control reconciles old operation against live pods/status rather than re-dispatching blindly. |

#120 (workload identity, per-run broker, signed worker protocol, and dependency
egress) is settled by this document. The Go cache remains a separate
implementation issue (#136); #123 covers pod/protocol wiring, not the cache. #119
(long-tool liveness) is a **satisfied design gate**: its exact schema, ordering,
record-validity rules, decision table, and tests are settled in §6 above.
#118 (run publication policy) is a **satisfied design gate**: its exact
schema, operator-resolved admission inputs, pre/post-push live revalidation,
`NeedsHuman`/retryable matrix, and provider-neutral semantics are settled in
§4 above. Implementation issues stay blocked until their named dependencies
are settled and merged:

| Issue | Seam | Depends on |
|---|---|---|
| #121 | semantic forge provider contract | #118 (satisfied) |
| #122 | credential broker and git/forge enforcement | #118 (satisfied), #120, #121 |
| #123 | trusted control/broker/untrusted worker pods and protocol | #120, #122 |
| #136 | dedicated Go module/checksum cache and constrained egress | #120, #123 |
| #124 | model streams, role grants, brief protocol | #121, #123 |
| #125 | validate artifacts, integrate/publish briefs | #122, #123, #124 |
| #126 | authenticated status, recovery, liveness | #119 (satisfied), #122-#125 |
| #127 | native terminal result and operator verification | #124-#126 |

#80 is consumed by #121/#122, and its last open design question — the
provider registration surface — is settled in §4 and consumed by #123's
operator wiring. #104 by #120/#122/#123. #101 stays a
separate, temporary OpenCode capability preflight, not a security boundary.
#102 readiness is exactly after its prereqs: #119 (**satisfied** — design
settled in §6), #126 (**landed** — authenticated status writes, the
active-operation set, and direct-API-reader liveness with
UID-preconditioned deletes), and a production e2e proving authenticated
heartbeat, meaningful checkpoint, confirmed `lastCommit`, long-running
tool behavior per the §6 decision table, and resume. That e2e proves the
suppression and no-false-reap behavior; it does not — and this design
does not promise — that a wedge is ever *detected*. The operator-side #12
reaper is implemented but, per §6, is limited to runs with no valid active
operation and cannot complete its production promise until that evidence
exists. #8 records the design; it is not a claim the secure path has
landed.
