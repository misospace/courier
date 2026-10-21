# Connector diagnostics without core coupling

This document specifies how Courier's **optional source connectors** report their
own diagnostics without teaching Courier core anything about a particular
source's protocol. It is a design deliverable (#291), a child of the portable
observability umbrella (#289), and it is **design only**: no protocol change, no
implementation, no shared application telemetry schema, no mandatory cross-app
identifier, no deployment, and no dashboard is authorized here. Where it names a
signal, it names the signal a later, separately-scoped instrumentation decision
would emit; it does not add one.

The motivating incident is the `PR-fix exclusions exceed maximum 32` discovery
stall described in #289 and #286 — since resolved by truncating to the cap
(#293), which is why the stall no longer appears in logs. The pressure that
caused it does not go away with it, so this document exists so an operator can
*see* that pressure without Courier core learning Dispatch's name or its cap. It
does **not** settle the truncation/overflow behavior — #286 owns the strategy
and #287 owns retention — and it must not be read as choosing a behavioral
workaround or as describing the connector's current overflow path. Observability
reports pressure; it never silently decides how much work to drop.

## 1. Purpose, scope, and boundaries

The umbrella design (#289) makes Courier observable as an independently useful
application. Its three child designs own distinct layers:

- **#290 — source-agnostic telemetry and health semantics.** Owns the generic
  signal catalogue and the state/health model (`healthy`, `idle`, `degraded`,
  `unknown`) for Courier itself.
- **#291 — this document.** Owns adapter-owned diagnostics for optional source
  connectors and the mechanism by which a dashboard discovers which optional
  capabilities a connector offers, **without requiring a Dispatch adapter**.
- **#292 — dashboard and alert specifications.** Owns the backend-neutral views
  and alert conditions that render #290's semantics and #291's connector signals.

**Settled boundaries.** Courier core remains source-agnostic. Dispatch remains
usable by other clients. Neither application depends on the other's telemetry
names, dashboards, runtime, or release cadence. A connector translates
protocols; it does not impose a shared application contract on the app behind
it. Concretely:

- Courier core never names a connector's protocol, endpoint, task types, or
  limits in a health rule or a signal definition.
- Courier's own connector (the Dispatch adapter, `internal/source/dispatch`) may
  report Dispatch-protocol diagnostics, but those diagnostics are adapter-owned
  data, registered in a generic envelope — not compiled into core logic.
- Dispatch's own server-side telemetry is Dispatch's. Courier neither reads it
  nor depends on it. When Courier cannot observe something, it says `unknown`.

**Out of scope.** No protocol or wire change; no `internal/source` or
`internal/source/dispatch` behavior change; no shared telemetry schema across
applications; no mandatory cross-app identifier; no deployment, scrape wiring,
dashboard JSON, or executable rule; no choice between overflow strategies.

## 2. Two layers: core signals and connector diagnostics

The failure mode to avoid is a core health rule that reads
`if source == "dispatch" && exclusions > 32`. The design separates two layers
with different owners and different naming.

### 2.1 Layer 1 — core source-neutral signals (Courier-owned)

These describe *Courier's own* relationship to whatever source is configured.
They exist for every adapter, including a connector that offers nothing beyond
`Discover`. They are named `courier_source_*` and carry only bounded labels
(`source` = the configured source identity, i.e. the `CoderRun.spec.source`
value and never the agent name; `mode`, `lane`, `phase`, plus closed enums).
The source-neutral seams already exist in `internal/source/source.go`
(`Adapter`, `Reporter`, `WorkIdentifier`, `DiscoveryExcluder`, `PreLauncher`)
and the runner already owns the lifecycle boundaries in
`internal/source/runner.go` (poll, lane gate, materialize, dedupe).

| Core signal | Meaning | Owner | Absence means |
| --- | --- | --- | --- |
| `courier_source_discovery_attempts_total` | one poll of the source | core runner | not yet polled |
| `courier_source_discovery_successes_total` | a poll that returned without error | core runner | — |
| `courier_source_discovery_failures_total` | a poll whose `Discover`/`DiscoverExcluding` returned an error | core runner | — |
| `courier_source_discovery_paused_total` | polls skipped by the lane gate (missing/suspended/at capacity/capacity cooldown) | core runner | — |
| `courier_source_items_offered_total` | items returned before core validation/dedupe | core runner | — |
| `courier_source_items_materialized_total` | items that became `CoderRun`s | core runner | — |
| `courier_source_lifecycle_report_outcomes_total` | report result (`published`, `superseded`, `unsupported`, `retryable`) | core controller | no report attempted |

These are the signals #290 owns semantically; #291 only fixes their boundary
against connector diagnostics. The discovery signals above are exported as
metrics today (`internal/source/metrics.go`); the lifecycle-report outcome
signal is not — the controller still records lifecycle conditions on
`CoderRun` status instead. That remaining gap is recorded, not closed, here.

### 2.2 Layer 2 — connector diagnostics (adapter-owned)

These describe *the protocol exchange* between a connector and its source. They
are only emitted by a connector that declares the corresponding capability.
They are carried in one generic envelope whose **shape** Courier defines but
whose **vocabulary values** each connector owns. Core never interprets them; a
dashboard renders them per the connector's declared capability.

- **Envelope (core-defined, connector-agnostic):** a `connector.diagnostic`
  event type and a matching metric family, with bounded labels
  `connector` (configured adapter name), `signal` (closed core vocabulary),
  `reason` (connector-declared bounded vocabulary), and `state` (see §4). Numeric
  observations (`requested`, `limit`, `accepted`, latency) travel as **values**,
  never as label values.
- **Values (connector-owned):** the Dispatch adapter owns the meanings of
  `exclude_pr_fix`, `prFixExclusions.version`, HTTP status classes, generation
  numbers, and the exclusion cap. A non-Dispatch connector owns its own.

Naming rule: the metric family is `courier_connector_*` (Courier owns the
transport and shape), and the connector's identity is a bounded label, so a
dashboard can discover and render any connector without a Dispatch-specific
code path. The connector never ships a metric name that core must know, and core
never ships a rule that mentions Dispatch. When a connector type admits several
configured instances, each instance carries its own bounded identity (the
Dispatch adapter uses its configured adapter name, e.g. `dispatch:<agent>`),
so independent configured connectors never share — or overwrite — each
other's series.

## 3. Connector signal / capability catalogue

A **capability** is an optional operation a connector declares; a **signal** is
a bounded diagnostic a capability may emit. The capability vocabulary is closed
and mirrors the optional interfaces Courier already defines, so a connector
cannot advertise a capability Courier has no seam for.

| Capability | Courier seam | Signals it may emit | Owner of values |
| --- | --- | --- | --- |
| `discovery` | `source.Adapter.Discover` | `discovery` (attempts/success/failure), `protocol-error` | connector |
| `discovery-exclusions` | `source.DiscoveryExcluder` | `exclusion` (requested/accepted/overflow), `ack` (ok/missing/invalid/version-unsupported) | connector |
| `lifecycle-report` | `source.Reporter` | `report` (published/superseded/unsupported/retryable), `protocol-error` | connector |
| `pre-launch-revalidation` | `source.PreLauncher` | `revalidation` (stale/current/error) | connector |
| `work-identity` | `source.WorkIdentifier` | `identity` (superseded) | connector |
| `availability` | always declared | `availability` (available/degraded/unreachable/unsupported) | connector |

### 3.1 Exclusion pressure and overflow

Courier's Dispatch adapter already bounds an acknowledged exclusion request:
`exclude_pr_fix_version=1`, one `exclude_pr_fix=<repo>#<number>@<generation>`
value per retained PR-fix generation, each value at most 300 characters, and at
most 32 unique values per request (`internal/source/dispatch/client.go:208-246`;
the cap is mirrored in DESIGN.md's Sources section). The cap is a **contract
maximum**; the design treats it as connector-declared data rather than a core
constant, even though in today's adapter it is a hardcoded value rather than a
configurable one.

The connector emits, per discovery cycle in which exclusions were attempted:

| Signal | Reason | Value | Meaning |
| --- | --- | --- | --- |
| `exclusion` | `requested` | count | unique valid descriptors the connector intended to send |
| `exclusion` | `accepted` | count | descriptors the connector sent (≤ limit) |
| `exclusion` | `overflow` | count | descriptors dropped or refused because `requested > limit` |
| `exclusion` | `pressure` | ratio | `requested / limit`; a derived, non-authoritative convenience |

The configured limit is exported as **data**: a `courier_connector_exclusion_limit`
gauge labelled only by `connector`. `requested` and `accepted` are values, so a
health rule compares them to the connector's declared limit. **No core rule, and
no alert, hardcodes 32.** A connector with a different limit — or no exclusion
capability at all — needs no rule change.

`overflow` is the observable that lets an operator see exclusion pressure under
whichever strategy a connector takes. Which strategy that is stays a **behavior
decision this document does not make** — but the current state of the code is
stated here so the design does not describe a path that no longer ships.

**Shipped today: truncation.** The Dispatch adapter keeps the first
`maxPRFixExclusions` unique valid descriptors and drops the tail with a log line
naming what was kept and dropped, instead of failing the request closed
(`internal/source/dispatch/client.go:232-238`; the behavior decision is #286's,
shipped by #293). Truncation is one connector's answer; a connector that refuses
the request, or truncates under a different rule, reports the same three values.

**Fail-closed, and a different thing entirely.** Two paths still fail the whole
request closed, and neither one is overflow handling:

- **Invalid exclusion descriptors.** A descriptor whose repo does not match the
  contract's repo pattern, whose number or generation falls outside the safe
  range, or whose value exceeds the 300-character maximum, fails discovery
  rather than being dropped (`internal/source/dispatch/client.go:221-227`). A
  work ID that does not decode to a PR-fix generation at all is skipped, not
  fatal (`internal/source/dispatch/client.go:216-219`).
- **Acknowledgement gate.** A response carrying no `prFixExclusions` object, or
  one naming a version other than 1, fails closed before any task or idle result
  is processed (`internal/source/dispatch/client.go:266-276`; §3.2). That is a
  `missing`/`invalid`/`version-unsupported` acknowledgement, never an `overflow`.

Because the two are distinct, an operator reading `overflow = 0` alongside a
`missing` acknowledgement sees a failed contract, not a connector that found
nothing to exclude. In every strategy the pressure stays visible, and a partial
exclusion set must not be reported as full coverage: `accepted < requested` is
always accompanied by an `overflow` observation.

### 3.2 Acknowledgement support and mismatch

The exclusion contract requires the source to acknowledge
`prFixExclusions.version: 1` before a task or idle result is processed
(`internal/source/dispatch/client.go:266-276`). Acknowledgement is distinct
from success and has its own reasons:

| Reason | State | Meaning |
| --- | --- | --- |
| `ok` | observed | a well-formed acknowledgement at a supported version |
| `missing` | error | the response carried no acknowledgement |
| `invalid` | error | the acknowledgement was not a JSON object |
| `version-unsupported` | unsupported | the acknowledgement named a version this connector cannot use |

`missing`/`invalid`/`version-unsupported` are never rendered as success or as a
zero. `version-unsupported` is specifically the signal that the source is older
than the connector's exclusion contract; the operator can then decide whether to
disable the opt-in (`WithPRFixExclusions(false)`) rather than read a silent
failure as "no exclusions needed".

### 3.3 Protocol errors

Protocol errors are reported as a bounded reason from a closed class, never as a
raw body, URL, or upstream string:

| Reason | Meaning |
| --- | --- |
| `transport-error` | DNS, connection, or TLS failure before a response |
| `auth-error` | credential rejected (401/403) |
| `http-4xx` | other client error (e.g. 400 unsupported outcome, 404 missing item) |
| `http-5xx` | source-side failure |
| `malformed-response` | response did not match the connector's expected shape |
| `unsupported-task-type` | the source offered a task the connector cannot run |
| `invalid-descriptor` | a local input failed validation before send |

The reason is a class, not a message. The message, when needed, rides the
redacted debug `detail` of the `connector.diagnostic` event and never becomes a
label.

### 3.4 Superseded and stale-work identities

Identity and generation semantics are source-specific, so the connector owns
them. `internal/source/source.go` already names two source-neutral outcomes —
`ErrSuperseded` (a newer generation of the same work exists) and `ErrStaleWork`
(claimed work no longer needs a coordinator) — which have distinct retry
semantics, and the connector reports a bounded reason beneath them:

| Reason | Sentinel | Meaning |
| --- | --- | --- |
| `generation-advanced` | `ErrSuperseded` | the source re-issued the work at a higher generation |
| `identity-superseded` | `ErrSuperseded` | `WorkIdentity` collapsed two offers into one |
| `stale-head` | `ErrStaleWork` | the observed head no longer matches the attempt's start head |
| `closed-unmerged` | `ErrStaleWork` | the linked PR closed without merging |
| `claim-released` | `ErrStaleWork` | a compare-and-set claim was released before launch |

A superseded or stale identity is a **normal, expected** outcome, not an error;
it is counted so an operator can tell churn from failure, and the two sentinels
stay distinguishable because they imply different retry behavior.

### 3.5 Provider / source availability

Availability is a connector-declared state, not an inference from the absence of
errors:

| State | Meaning |
| --- | --- |
| `available` | the last bounded probe/read succeeded |
| `degraded` | the connector is reachable but failing a subset of operations |
| `unreachable` | the connector could not be reached at all |
| `unsupported` | the connector does not implement this capability |
| `unknown` | availability was not established (not yet probed, or no telemetry) |

`available` is the only success state. `unsupported` and `unknown` are distinct
from both `available` and `unreachable`, so a dashboard never reads "no data" as
"healthy".

## 4. Version and capability negotiation

Capabilities are negotiated, not assumed. A connector declares, at startup and
on each handshake with its source:

- its **connector protocol version** (the shape of the envelope it emits), and
- a **capability set** with a per-capability state of `supported`,
  `unsupported`, or `unknown`.

For the exclusion contract specifically, the existing wire already carries a
negotiation: the request names `exclude_pr_fix_version=1` and the response must
acknowledge a compatible version. The connector reports the negotiated version
as data; a source that does not support version 1 yields
`ack=version-unsupported`, not a failed request dressed up as success.

**Absent and unsupported diagnostics are first-class states, distinct from
success.** Every signal resolves to exactly one of:

1. `ok` / `observed` — a success was observed;
2. `unsupported` — the connector or source does not offer the capability;
3. `unknown` / `absent` — the capability is offered but no observation exists
   (not yet probed, upstream telemetry missing, or the source stayed silent);
4. `error` — the capability was exercised and failed.

A dashboard renders these four differently. In particular, `unsupported` and
`unknown` must never be summed into a success count, and an alert condition on
absence must be written explicitly rather than derived from a zero. These four
states describe a **single signal's observation**; the availability states in
§3.5 are an orthogonal, connector-declared axis, so `degraded` is an
availability value rather than a fifth observation state.

## 5. How a dashboard discovers optional capabilities

The capability catalogue is **data**, emitted by each connector:

- `courier_connector_capability_info{connector, capability, state}` = 1, an
  info-style gauge whose value is always 1 and whose state is the negotiated
  `supported`/`unsupported`/`unknown`; and
- the `connector.diagnostic` event stream for point-in-time observations.

A dashboard therefore:

1. enumerates `connector` values from `courier_connector_capability_info`;
2. renders a panel for a capability only where `state="supported"`, shows it as
   "unsupported" where `state="unsupported"`, and as "unknown" otherwise;
3. joins generic `courier_source_*` panels (always present) with connector
   diagnostics by the `connector` label.

This requires **no Dispatch adapter**: a deployment whose only source is a
non-Dispatch connector still yields a complete dashboard — generic source
panels plus that connector's declared capabilities. Where a capability is
absent, the panel is absent or explicitly "unsupported", never a fabricated
zero. Core's generic rules are written against `courier_source_*` and the
capability gauge; they never reference a connector name.

## 6. Bounded labels, duplicate counting, privacy, correlation

### 6.1 Bounded labels

Permitted label values are drawn only from:

- `connector` — the deployment's configured adapters (small, bounded by config);
- `mode` (`resolve-issue` | `fix-pr`), `lane`, `phase` — existing bounded sets;
- `signal` — the closed core vocabulary in §3;
- `reason` — a connector-declared vocabulary, itself closed and capped;
- `state` — `ok`/`unsupported`/`unknown`/`error` (and the availability states).

Forbidden as label values, ever: repository, issue or PR number, branch, head
SHA, PR-fix id or generation, URL, model, brief text, or any free-form string. A
connector that encounters an unrecognised reason maps it to a single `other`
value so cardinality cannot grow with upstream data. Numeric observations
(limits, counts, ratios, latencies) are metric **values**, not labels — this is
what keeps a configured cap like 32 out of the label space and out of core
health rules.

### 6.2 Duplicate counting between core and connector

Core and connector may describe the same underlying event from two sides. The
rule is that each signal counts **once, at its owner**, and a diagnostic that
*explains* a core signal is joined, never added:

- The core `courier_source_discovery_failures_total` counter is the
  authoritative discovery-failure rate; it counts the failed poll.
- The connector's `connector.diagnostic{signal="protocol-error", reason="http-5xx"}`
  is the **cause breakdown** for that same poll. It is not added to the core
  failure counter, and the core failure counter is not added to it. A dashboard
  that wants "failures by cause" joins the two on `connector` and time.
- Within a connector, one protocol exchange is counted once. A bounded retry is
  a new attempt and is counted again, with `attempt` distinguished by reason
  where it matters.
- `exclusion overflow` is counted once per discovery cycle in which it occurs,
  not once per dropped descriptor; the dropped count is the `overflow` **value**.

The anti-pattern this forbids: a dashboard summing a core counter and a
connector counter that describe the same failure, which double-reports and
inflates the rate.

### 6.3 Privacy

Connector diagnostics carry counts, bounded enums, numeric limits, and latencies.
They never carry request/response bodies, credentials, tokens, repository
names, PR titles, review text, or upstream error strings as metric labels or
structured field values; a bounded, redacted message may ride the debug
`detail` of the event (§3.3). The existing structured-log redactor
(`internal/log/redact.go`) remains the last-line guard, and any free-form
`detail` is debug-gated and redacted. A connector that needs to correlate to a
specific item does so through the log `run_id` (below), never through a metric
label.

### 6.4 Optional trace correlation

Correlation is optional and additive. The `connector.diagnostic` envelope has a
log half and a metric half (§2.2); correlation rides the **log half only**. A
connector **may** attach a `run_id` (and, where an OTel pipeline exists, a
`trace_id`/span link) to a `connector.diagnostic` log line so a single incident
can be followed across Courier core and the connector. Correlation is **not** a
metric label — it would be unbounded — and it is **not a prerequisite** for
either application: a connector that emits no trace context, and a deployment
with no tracing backend, both remain fully functional and fully observable
through the bounded signals above. Neither app depends on the other's tracing.

## 7. Worked example: the Dispatch connector

The Dispatch adapter (`internal/source/dispatch`) declares the full capability
set and supplies Dispatch-specific values:

| Capability | State | Connector-owned values |
| --- | --- | --- |
| `discovery` | supported | task types `implement`/`followup-pr`; idle vs task |
| `discovery-exclusions` | supported (opt-in) | `exclude_pr_fix_version=1`; descriptor `<repo>#<number>@<generation>`; limit **32**; max value length 300 |
| `lifecycle-report` | supported | outcomes `ready`/`blocked`/`blocked-external`/`failed`/`handed-off`; `waiting-external` |
| `pre-launch-revalidation` | supported | stale/current from `PreLaunch` |
| `work-identity` | supported | PR-fix id + generation |
| `availability` | supported | bounded preflight of `next-task` |

**Observing an exclusion overflow, step by step, with no Dispatch logic in
core.** A binding retains enough PR-fix generations that the connector's unique
descriptor set exceeds its configured limit.

1. The runner polls and passes retained same-source work IDs to
   `DiscoverExcluding` (`internal/source/runner.go:141-170`).
2. The connector decodes them, counts unique valid descriptors, and finds
   `requested = 34 > limit = 32`. It emits
   `connector.diagnostic{connector="dispatch", signal="exclusion",
   reason="overflow", value=2, limit=32}` and increments
   `courier_connector_diagnostics_total{connector="dispatch",
   signal="exclusion", reason="overflow"}`.
3. It exports `courier_connector_exclusion_requested{connector="dispatch"} = 34`
   and `courier_connector_exclusion_limit{connector="dispatch"} = 32`.
4. Core's generic signals show discovery paused or failing
   (`courier_source_discovery_*`); the connector diagnostic explains why.
5. A **connector-agnostic** dashboard panel "Exclusion pressure" reads
   `requested` and `limit` for every connector that declares the
   `discovery-exclusions` capability, plots `requested/limit`, and alerts when
   `requested >= limit`. The rule contains no `dispatch` string and no `32`.
6. The behavior taken at overflow — refuse, truncate, or something else — is
   #286's decision (the current adapter truncates and logs the drop, §3.1) and is
   visible in the `accepted` value. The observability layer reports it; it does
   not choose it.

**What is unknown when Dispatch telemetry is absent.** Courier can only report
what it sent and what Dispatch acknowledged. It cannot observe Dispatch's
internal queue depth, why Dispatch declined a task, whether a partial exclusion
set actually starved a sibling lane, Dispatch's own dedupe/retention state, or
Dispatch's internal latency. Those remain `unknown`; they are neither `healthy`
nor zero. If Dispatch never acknowledges the exclusion contract, the connector
reports `ack=version-unsupported` (or `missing`) and the operator knows that
exclusion-based dedupe is not in effect for that deployment.

## 8. Worked example: a non-Dispatch connector

Consider a label-driven forge connector (the `github-label` adapter sketched in
DESIGN.md's Sources section) or the `manual` adapter that exists today
(`internal/source/manual/adapter.go`). It implements `Discover` and the other
required methods but no optional interface.

| Capability | State | Consequence |
| --- | --- | --- |
| `discovery` | supported | emits generic core signals; `protocol-error` reasons from its forge client |
| `discovery-exclusions` | unsupported | no exclusion panel; `requested`/`limit` absent, not zero |
| `lifecycle-report` | unsupported | no report-outcome signal; `CoderRun` status still records the local result |
| `pre-launch-revalidation` | unsupported | no revalidation signal |
| `work-identity` | unsupported (or supported) | identity churn is core-dedupe, not a connector diagnostic |
| `availability` | supported | `available`/`unreachable` from its own client |

Its catalogue reports `unsupported` for exclusions; a dashboard renders the
generic source panels and simply omits the exclusion panel. Nothing in Courier
core changes, and the Dispatch exclusion machinery is neither required nor
referenced. This is the portability check the umbrella (#289) requires: Courier
works with an alternate source, and a connector that offers fewer capabilities
degrades to fewer panels rather than to a broken dashboard.

## 9. Relationship to the behavior fixes

- **#286** (observed overflow) owns whether and how an over-limit exclusion set
  is truncated; #293 shipped truncation with a warning (§3.1). This document
  makes the pressure and the `accepted`/`requested` gap observable, so the
  chosen strategy is visible to an operator; it does not prescribe a truncation
  algorithm. A partial exclusion set is safe to send because the exclusion
  request is a dedupe hint, not a claim — but only the `accepted < requested`
  gap proves that happened, which is why the gap is a first-class observation.
- **#287** (retention) owns how many retained generations exist to be excluded
  in the first place. Retention and observability must agree on counts: the
  exclusion set is a set of decoded/deduplicated PR-fix generation descriptors,
  not the total `CoderRun` count.
- **#288** (Pending display) is a reporting question, out of scope here.

## 10. Open questions left to implementation

These are recorded so a later instrumentation ticket can settle them; none is
authorized by this document:

- Which signals become Prometheus metrics versus log-only events. The envelope
  is designed for both; the core discovery and reservation signals are metrics
  today, but the connector envelope itself has no instrumentation yet, so its
  first cut may be log-only.
- Whether the capability catalogue is a scrape-time gauge, a startup event, or
  both.
- The exact cardinality cap for connector-declared `reason` vocabularies. The
  design's guideline is that each capability declares a closed `reason`
  vocabulary of at most a small fixed size (working figure: 16 per signal), with
  every unrecognised value mapped to a single `other`, so the bound is enforced
  even against a source that invents new reasons.
- How #290's source-health states and #291's connector states compose in the
  #292 views without double-counting (see §6.2).

## 11. Verification contract for §6

§6 fixes rules that are easy to violate by accident: a label key slips in,
a connector sums its diagnostic onto the core failure counter, a free-form
string becomes a label value, or a `run_id` turns into a high-cardinality
metric label. The instrumentation that emits `courier_connector_*` and
`connector.diagnostic` is now implemented (`internal/connector`, with the
Dispatch and manual adapters as emitters), and the unit-level subset of the
assertions below runs in `go test`; the envtest fixtures, AST/grep static
checks, and scrape-time images-job invariants remain the contract items still
to land. Every checklist item names the test
or scrape-time invariant that will trip CI when the rule is broken.

A connector change lands only when each rule
in §6 has a passing assertion of the form listed below. New connectors that
declare new capabilities inherit the same assertions. The list is the
requirement-trace target the design itself closes against.

### 11.1 Bounded labels (§6.1)

- **Allowlist assertion.** A unit test enumerates every `courier_connector_*`
  metric family and every `courier_source_*` metric family emitted by the
  controller, runner, and connector adapters, and fails the build if any
  family exposes a label key outside the closed set
  `{connector, mode, lane, phase, signal, reason, state, capability}`
  (plus, for core signals, the existing closed enums already enumerated
  elsewhere in this document). The set itself is defined in code by a
  single source-of-truth slice used by the test and by the helper that
  validates a family's labels at registration time.
- **Closed-reason assertion.** A table-driven test asserts that, for every
  `(connector, capability, signal)` tuple registered, the connector
  declares a closed `reason` vocabulary of small bounded size, and that the
  emitter maps any unrecognised reason to a single `other` value before
  the event crosses the metric/log boundary. The test fails if a connector
  ships a `reason` label whose value is not in its declared vocabulary or
  `other`.
- **Numeric-as-value assertion.** A static check (CI unit test) parses every
  registered metric family and fails the build if any label has a `float64`
  or `int64` value at runtime — i.e., asserts that numeric observations
  (`requested`, `limit`, `accepted`, `pressure`, latency) live as metric
  **values**, never as label values.

### 11.2 Duplicate-counting (§6.2)

- **No-double-count fixture.** An envtest fixture runs a synthetic
  discovery poll that both fails the core `Discover`/`DiscoverExcluding`
  call *and* emits a connector `protocol-error` diagnostic for the same
  cycle, then asserts: `courier_source_discovery_failures_total` increments
  by exactly one, `courier_connector_diagnostics_total{...,reason=...}`
  increments by exactly one, and a helper that joins the two on
  `(connector, cycle_id)` returns 1, not 2. The fixture's pre-baked
  expected series is checked with `testutil.CollectAndCompare`.
- **One overflow per cycle.** A separate fixture forces
  `requested > limit` and asserts that
  `courier_connector_diagnostics_total{...,reason="overflow"}` increments
  by exactly one for the cycle, regardless of how many descriptors were
  dropped — the drop count lives in the `overflow` value (or its
  equivalents), not in additional events. The fixture also asserts that
  `accepted < requested ⇒ overflow >= 1`, so a partial exclusion set can
  never be reported as full coverage.
- **Bounded-retry assertion.** A test fixture with a bounded retry policy
  asserts that each attempt increments the connector diagnostic once per
  attempt, distinguished by `signal`/`reason` where the design calls for
  it, and that the total equals the number of attempts the fixture
  recorded (no coalescing, no fan-out).

### 11.3 Privacy (§6.3)

- **Redaction guard.** A unit test feeds canonical forbidden strings
  (tokens from the existing fixture corpus, repo names, PR numbers, head
  SHAs, model names, free-form narrative over the configured length cap)
  through the structured-log redactor at `internal/log/redact.go` and
  asserts each is replaced/redacted before any line reaches a logger
  wired to metric output. The same corpus is run against the metric
  emitter with a mock registry; the collector's label-set dump must
  contain none of the forbidden substrings.
- **No-free-form-label assertion.** A static check (Go AST walk over the
  connectors and a CI grep over the metric registration helpers) fails
  the build if any label value is built from `fmt.Sprintf` over a string
  that is not in the connector's declared closed vocabulary or one of the
  bounded enums.
- **Detail-channel assertion.** A unit test asserts that the only field
  on the `connector.diagnostic` event that carries a free-form string is
  the debug `detail`, that `detail` is debug-gated (never emitted at the
  default log level), and that the existing redactor still passes
  before write.

### 11.4 Optional trace correlation (§6.4)

- **No-correlation-label invariant.** A scrape-time test in envtest
  exports the full metric set with a `run_id` populated on every
  connector diagnostic log line and asserts that no `courier_connector_*`
  metric family has a `run_id`, `trace_id`, `span_id`, or any other
  correlation identifier as a label. The collector's label dump is the
  evidence.
- **Log-only assertion.** A unit test asserts that the only code path
  that writes a correlation identifier is the logging half of the
  envelope; the metric half's emitter helper rejects any event whose
  payload contains a correlation key, with a clear error in unit tests
  (it is unreachable in production).
- **Untraced-deployment compatibility.** An end-to-end test runs the
  executor with no tracing backend configured and asserts that
  `courier_connector_capability_info`, `courier_connector_*` counters
  and gauges, and the bounded `connector.diagnostic` log stream still
  emit normally. Absence of a tracing pipeline must not silence any
  bounded signal.

The instrumentation PR is not complete until every assertion above is
checked into the test tree and passing in `go test` (and, for the
scrape-time checks, in the `images` job that already runs the live
manager). Until then, every signal in §3 is a paper contract; the §11
checklist is the only thing that turns it into enforced behavior.
