# Portable observability: dashboard and alert specifications

This document specifies Courier's **backend-neutral** observability views and
health conditions, and the rules by which an optional backend integration may
render them. It is a design deliverable (#292), not an implementation: it
contains no code, no dashboard JSON, no executable alert rules, no exporter
dependencies, and no deployment manifests. Nothing here authorizes an
instrumentation change; the instrumentation gaps it names are inputs to a
later, separately-scoped decision.

## 1. Purpose, scope, and relationship to the umbrella design

The umbrella design (#289) makes Courier observable as an independently useful
application without coupling it to Dispatch or to any operator's monitoring
stack. It has three child designs, each owning a distinct layer:

- **#290 — source-agnostic telemetry and health semantics.** Owns the signal
  catalogue and the state/health model: what is measured, in what units, with
  which counter/gauge/reset semantics, and what `ok`, `idle`, `degraded`,
  `unavailable`, and `unknown` mean. Courier core is source-agnostic, so #290 defines a
  source-level health view that exists even when discovery produces no
  `CoderRun`.
- **#291 — connector diagnostics without core coupling.** Owns adapter-owned
  diagnostics for optional source connectors (Dispatch and others): exclusion
  pressure, acknowledgement support, protocol errors, provider availability.
  These stay at the adapter boundary and are never a prerequisite for Courier
  health.
- **#292 — this document.** Owns the **dashboard information architecture** and
  the **alert catalogue**: the views an operator should see, the health
  conditions worth alerting on, and how any backend renders them. It consumes
  #290's semantics and #291's connector signals; where a sibling has settled a
  dependency, this document consumes it, and where one leaves an explicit
  unknown, this document states the dependency rather than inventing a signal.

**In scope.** A backend-neutral view catalogue (source health, lane capacity,
run execution and outcomes, lifecycle reporting, retention, optional connector
panels); a backend-neutral alert catalogue with explicit behaviour under
missing telemetry, startup, disabled sources, empty queues, and low traffic; an
integration matrix separating portable specification from optional executable
assets; illustrative mappings to Prometheus/VictoriaMetrics and an OTel-based
pipeline with an explicitly named evaluator; a template for adding a new
integration; and the constraints (units, aggregation, reset, cardinality,
privacy, unsupported combinations) that keep every rendering equivalent.

**Out of scope.** No code, dashboard JSON, executable alert rules, exporter
dependencies, deployment manifests, or home-ops wiring. No instrumentation PR
is authorized. No behaviour change to lifecycle, retention, or reporting is
implied. Installation, credentials, scraping/export wiring, and notification
routing are downstream concerns.

### Portability rules

1. **Application semantics first.** Every view and condition below is stated in
   terms of Courier's own signals and concepts. A backend is a rendering of
   those semantics, never their definition.
2. **No environment assumptions.** The specification must not depend on
   home-ops namespaces, cluster or node names, credential sources, notification
   routes, Grafana, or Alertmanager. An integration may *optionally* use any of
   these; the specification must remain implementable without them.
3. **Prometheus and VictoriaMetrics share a PromQL-compatible integration.**
   Their metrics and rule semantics are close enough that one mapping serves
   both; differences that matter are called out in the integration matrix.
4. **OpenTelemetry is a transport and data model, not a universal dashboard or
   alert engine.** OTLP carries signals; evaluation requires a backend with a
   rules engine (for example a Prometheus server, VictoriaMetrics' `vmalert`,
   or a Mimir ruler). The OTel Collector's processors are not a general alert
   evaluator. Any "OTel integration" in this document names its evaluator.
5. **Any solution may implement the same views and conditions.** The catalogue
   is normative; the assets are not. There is no promise of identical files,
   identical queries, or feature parity across backends.

## 2. Signal inventory and instrumentation gaps

The views below are only as good as the signals behind them. This section
records what exists today, with citations, and what does not.

### Existing signals

**Operator metrics** (`internal/controller/metrics.go`), served by
controller-runtime on the `--metrics-bind-address` endpoint (default `:8080`,
`cmd/main.go`):

| Metric | Type | Labels | Semantics |
| --- | --- | --- | --- |
| `courier_coderun_run_duration_seconds` | histogram | `lane`, `mode`, `phase` | `StartedAt`→`FinishedAt`, observed once on terminal transition |
| `courier_coderun_queue_wait_seconds` | histogram | `lane`, `mode`, `phase` | creation→`AdmittedAt`, observed once on terminal transition |
| `courier_coderuns_total` | counter | `lane`, `mode`, `phase` | runs reaching a terminal phase — label is the **first** terminal phase (one increment per run; a later `AwaitingReview → Done` does not increment `{phase="Done"}`, per #290 §3.4/§4.1) |
| `courier_coderuns_in_flight` | gauge | `lane` | live cluster read at scrape; counts `Claimed`+`Running` |
| `courier_lane_suspended` | gauge | `lane` | 1 suspended, 0 active; live read at scrape |
| `courier_metrics_scrape_errors_total` | counter | — | failed `List` calls during lane collection |

controller-runtime additionally exposes `go_*`, `process_*`, and
`controller_runtime_*` series on the same registry. The chart's
`serviceMonitor.enabled` renders a metrics `Service` and a Prometheus Operator
`ServiceMonitor`; it is off by default, so scraping is opt-in.

**Structured stdout log events** (`internal/log/events.go`): one JSON line per
event, carrying `time`, `event`, `run_id`, `repo`, `ref`, `mode`, `brief`,
`role`, `model`, `status`, and a `detail` object. Event types include
`run.start`, `run.exit`, `workspace.ready`, `executor.start`,
`executor.continuation`, `model.call`, `tool.call`, `tool.result`,
`brief.start`, `brief.complete`, `phase.transition`, `capability.status`,
`outcome.declared`, `outcome.comment`, `linkage.read.failed`, `run.summary`, and
`evidence.capture`. Log verbosity is set by `COURIER_LOG_LEVEL`, with a per-run
`debug` flag gating `detail` inclusion. Courier does **not** emit Kubernetes
Events; all controller "events" are these stdout lines.

**`CoderRun` status** (`api/v1alpha1/coderun_types.go`): the phase
(`Pending`, `Claimed`, `Running`, `Verifying`, `AwaitingReview`, `HandedOff`,
`NeedsHuman`, `Done`, `Failed`, `BlockedExternal`); the timestamps `AdmittedAt`,
`StartedAt`, `FinishedAt`, and the derived `WaitDuration`/`RunDuration`;
liveness state (`Checkpoint`, `Heartbeat`, `ActiveOperations`, `Restarts`);
per-run `Telemetry`; the `TerminalDiagnostic` category; `Conditions` including
`LifecycleReported`; the lifecycle retry tokens; `DeclaredNoChange`; the CI
check fingerprints; and publication state (`Branch`, `HeadRepo`, `HeadSHA`,
`PR`, `LastCommit`).

**Source contract** (`internal/source/source.go`): `Adapter` with `Discover`,
`Claim`, `Release`, `Transition`, `Resolve`; the optional `Reporter`,
`WorkIdentifier`, `DiscoveryExcluder`, and `PreLauncher` interfaces. The
discovery runner (`internal/source/runner.go`) polls every 30s and pauses when
the lane is missing, suspended, or at capacity, logging those transitions. It
logs a discovery error at `internal/source/runner.go:69,80`.

**Retention** (`internal/controller/reap.go`): resolved `Done` runs are deleted
after five minutes via the `courier.misospace.dev/done-at` annotation.
`Failed`/`NeedsHuman` runs are reaped after their seven-day default, measured
from the `courier.misospace.dev/failed-at` and
`courier.misospace.dev/needs-human-at` annotations, only once the lifecycle
report reached a positive settled state, and never out of the per-repository
audit floor; the reaper emits `courier_coderuns_reap_requests_total` (accepted
delete requests) and `courier_coderuns_oldest_terminal_age_seconds` (age of the
oldest retained terminal run per phase). `HandedOff` is not reaped.

### Instrumentation gaps

These are the gaps that constrain which views are implementable today. They are
**not** authorized work; they are the evidence base a future instrumentation
decision needs, and the reason several views below are marked "gap".

- **Source/discovery health:** no metric for discovery attempts, successes,
  failures, returned items, or filtered/materialized counts; no per-source
  availability or latency. An empty discovery and a failed discovery both
  yield zero new runs; today only the latter writes an error log.
- **Reservations:** no metric for claimed-but-unlaunched age or stale
  reservations.
- **Lifecycle reporting:** no counter of report outcomes (published,
  superseded, unsupported, retryable) or report failures, and no report
  latency histogram.
- **Retention:** `courier_coderuns_reap_requests_total` and
  `courier_coderuns_oldest_terminal_age_seconds` exist, but there is no gauge of
  retained-run backlog (a count of terminal runs by phase).
- **Per-phase timing:** run duration is only observed at the terminal
  transition; there is no histogram of time spent in each non-terminal phase.
- **Liveness:** `Restarts` is in status but not exported; no restart-cause
  distribution.
- **Connectors/MCP:** `capability.status` is logged but not aggregated per
  server; no per-connector availability or latency.
- **Model/token accounting:** token totals are collected per run into
  `status.telemetry` but are not exported as metrics; no per-lane token or
  model-error metric.
- **Evidence:** no success/failure/deadline metric for evidence capture.

## 3. Dashboard information architecture

The architecture is a set of **panels** grouped into six sections. Each panel
states its intent, the signals that back it, the portable semantics (what it
computes and in what units), how it renders empty and unknown, and whether it is
implementable today or blocked on a gap. Panels are backend-neutral: any
integration that can compute the stated semantics satisfies them.

A Courier dashboard must be useful **standalone**. Nothing below requires
Dispatch telemetry, Dispatch queues, or a Dispatch dashboard. Where an operator
also runs Dispatch, the optional connector panels in section 3.6 compose; they
never gate the other sections.

### 3.1 Source health

Source health answers: *is Courier's supply of work working, and is it
currently idle or broken?* It must be computable for a source that has produced
no runs.

| Panel | Intent | Backing signals | Portable semantics | Empty / unknown | Status |
| --- | --- | --- | --- | --- | --- |
| Discovery outcomes | Show whether discovery is succeeding, failing, or idle | discovery counters (gap), `source discovery failed` log | attempts, successes, failures per source over a window; failure ratio | no attempts ⇒ "not yet polled"; never render as healthy | **gap** |
| Discovery freshness | Age of the last successful discovery | last-success timestamp (gap) | seconds since last success; compare to poll interval | no success yet ⇒ "unknown" | **gap** |
| Discovered vs materialized | Are items offered but not becoming runs? | discovery counts (gap), CoderRun creation | offered, deduped/excluded, created | zero offered ⇒ "idle", distinct from "failed" | **gap** |
| Source enabled/disabled | Is a source configured at all? | configuration / lane bindings | enabled boolean per source | disabled ⇒ panels show "disabled", not "down" | partial |
| Source-level run counts | Runs by source and mode | `courier_coderuns_total` (by lane/mode/phase) | terminal counts per lane/mode | no runs ⇒ zero, not absent | today |

**Idle vs failure.** A source that is enabled, has capacity, and repeatedly
discovers nothing is **idle**, not failing. A source whose discovery call errors
is **degraded**, becoming **unavailable** once no success falls within the
window (#290's source-state model). These must render differently: idle is a
normal steady state (no alert), degraded and unavailable are actionable (alert,
section 4). The distinction is lost
today because both produce zero runs and only the error path logs.

### 3.2 Lane capacity

Lane capacity answers: *are lanes admitting work, saturated, or suspended?*

| Panel | Intent | Backing signals | Portable semantics | Empty / unknown | Status |
| --- | --- | --- | --- | --- | --- |
| In-flight per lane | Execution capacity in use | `courier_coderuns_in_flight` | gauge, runs per lane consuming capacity (`Claimed`+`Running`) | no lane ⇒ no series | today |
| Lane suspension | Lanes intentionally paused | `courier_lane_suspended` | gauge 0/1 per lane | — | today |
| Admission saturation | Time spent at capacity | `courier_coderuns_in_flight`, lane concurrency, `courier_coderun_queue_wait_seconds` | in-flight vs concurrency; queue-wait distribution | no queue-wait samples ⇒ "no recent admissions" | today/partial |
| Reservation pressure | Materialized-but-unadmitted work | reservation counts (gap; runner counts empty/`Pending`/`Claimed`/`Running`, `internal/source/runner.go`) | reservations vs concurrency | — | **gap** |

Capacity saturation is a **supply** condition, not a failure: a lane pinned at
its concurrency limit with a healthy source is doing its job. It becomes
interesting only in combination with rising queue wait or with a source that is
*also* failing. The dashboard must not paint saturation as an incident by
itself. The reservation-pressure panel pairs with A3 (stuck reservation) once
the reservation-age and admission-progress signals are exposed; A3 covers work
that is materialised but wedged unadmitted — at or below lane capacity — while
A2 covers healthy saturation with rising queue wait. Until those signals exist
the panel is informational only.

### 3.3 Run execution and outcomes

Run execution answers: *what happened to the runs, and why?*

| Panel | Intent | Backing signals | Portable semantics | Empty / unknown | Status |
| --- | --- | --- | --- | --- | --- |
| Terminal outcomes | Outcome mix over time | `courier_coderuns_total` | rate of terminal runs by `lane`,`mode`,`phase` (label = first terminal phase; a later `AwaitingReview → Done` does not increment `{phase="Done"}` — render merge throughput separately, see §3.4) | no runs ⇒ zero | today |
| Run duration | How long runs take | `courier_coderun_run_duration_seconds` | histogram quantiles, seconds, by lane/mode/phase | no samples ⇒ "no completed runs" | today |
| Queue wait | Admission delay | `courier_coderun_queue_wait_seconds` | histogram quantiles, seconds | no samples ⇒ "no recent admissions" | today |
| Terminal diagnostic mix | Why runs ended badly | `TerminalDiagnostic` category (status), `run.exit` log | counts by category | absent diagnostic ⇒ "unknown", never "success" | log/status |
| Per-run timeline | One run's phases | `phase.transition` log, `run_id` correlation | event sequence for a run id | run absent ⇒ "not found" | log |
| Liveness/restarts | Runs losing their coordinator | `Restarts` status (gap: not exported) | restart counts by cause | — | **gap** |
| Token/model usage | Cost and model behaviour | `status.telemetry`, `run.summary` log (gap: not exported) | tokens by type, per lane/mode where exported | absent telemetry ⇒ "not reported" | log/status |

Outcomes are split deliberately: **executor completion** (the run reached a
terminal phase — the run's first terminal phase, per #290 §3.4/§4.1, which
is the label on `courier_coderuns_total` and the entry the
`courier_coderun_run_duration_seconds` histogram observes) is not the same
as **external disposition** (CI, review, merge). The dashboard shows
first-terminal-phase mix as executor completion; external disposition
appears in section 3.4, and a current-state per-phase gauge or a
disposition event counter is a separate future measurement, not a
reinterpretation of `courier_coderuns_total{phase="Done"}` (the metric
never increments `Done` for a run that terminalized at `AwaitingReview`).

### 3.4 Lifecycle reporting

Lifecycle reporting answers: *did the operator tell the source what happened?*

| Panel | Intent | Backing signals | Portable semantics | Empty / unknown | Status |
| --- | --- | --- | --- | --- | --- |
| `LifecycleReported` condition | Report settlement per run | `Conditions` | condition status/reason per terminal run | missing condition ⇒ "unknown", never "reported" | status |
| Pending reports | Reports not yet settled | `LifecycleReported=False`, retry tokens | count and age of unsettled reports | none ⇒ zero | status/gap for age |
| Report outcome mix | Published/superseded/unsupported/retryable | report counters (gap) | counts by outcome | — | **gap** |
| Report latency | Time from terminal to settled | report latency histogram (gap) | seconds | — | **gap** |

A terminal run whose report never settles is a real degradation: the source
does not know the work finished. This is `LifecycleReported` in `False`/`Unknown`
for longer than the expected settle window. The retention design already
requires a positive settled state before a run may be reaped, so an unsettled
report also delays reaping — the dashboard should make that correlation
visible.

### 3.5 Retention

Retention answers: *is the operator reclaiming finished runs, and is anything
stuck?*

| Panel | Intent | Backing signals | Portable semantics | Empty / unknown | Status |
| --- | --- | --- | --- | --- | --- |
| Retained-run backlog | How many terminal runs persist | `CoderRun` list / count gauge (gap) | runs by terminal phase | none ⇒ zero | **gap** |
| Reap rate | Runs reclaimed over time | `courier_coderuns_reap_requests_total` | rate of accepted delete requests by retention class | — | status |
| Oldest retained run | Is retention progressing? | `courier_coderuns_oldest_terminal_age_seconds` | age of oldest retained terminal run per phase | none ⇒ "none retained" | status |
| Stuck retention | Runs past window but not reaped | annotations + `LifecycleReported` (gap) | count past window with unsettled report | — | **gap** |

The retention design bounds `Done` (five minutes) and
`Failed`/`NeedsHuman` (seven days); `HandedOff` is explicitly out of scope
there. The reap counter counts delete requests the API server accepted, so a
secure run held by its topology finalizer is still counted there; the age gauge
is what shows such a run is still retained. A growing retained backlog is the
observable symptom of the class of failure that motivated the umbrella design
(#289), so it belongs on the dashboard even though its count is currently
uninstrumented.

### 3.6 Optional connector panels

Connector panels answer adapter-owned questions and are **optional**. They
appear only when a connector is configured and capable (#291), and their
absence must not affect any panel above.

| Panel | Intent | Backing signals | Portable semantics | Empty / unknown | Status |
| --- | --- | --- | --- | --- | --- |
| Connector availability | Is the connector reachable? | connector diagnostics (#291) | availability state (`available`/`degraded`/`unreachable`, per #291) and latency | not configured ⇒ panel hidden | #291 |
| Exclusion pressure | Is discovery excluding work? | connector diagnostics (#291) | exclusion count vs configured limit (limit as data, not a baked constant) | unsupported ⇒ "unsupported", not "ok" | #291 |
| Capability negotiation | Does the connector support a feature? | `capability.status` log, connector diagnostics (#291) | supported/unsupported/unknown per capability | absent ⇒ "unknown" | #291 |
| Protocol errors | Connector-specific failures | connector diagnostics (#291) | counts by error class | unsupported ⇒ "unsupported" | #291 |

Connector panels must render **unsupported/absent distinctly from success**
(#291). A connector that cannot report exclusion pressure is not a connector
reporting zero pressure.

## 4. Health conditions and alert catalogue

Each candidate below is a **design candidate**, not a fixed production default.
Thresholds and windows are starting points an operator tunes; the rationale
states what the threshold is protecting against. Severity is advisory.

Common rules that apply to every alert:

- **Idle is not failure.** Low or zero traffic is a normal state and never
  fires an alert on its own.
- **Distinguish supply from discovery.** A source that returns no work is
  distinct from a source whose discovery call fails; only the latter alerts.
- **No Dispatch requirement.** No alert below requires Dispatch telemetry.
- **Unknown is not healthy.** Missing telemetry, absent series, and unset
  conditions render as unknown/degraded, never as OK. Alerts that depend on a
  missing signal must say so explicitly.

### A1. Discovery failing while enabled with capacity

| Field | Value |
| --- | --- |
| Condition | the source is enabled, the binding is **not paused** (every `courier_source_discovery_paused{reason}` is zero — neither `lane-suspended` nor `lane-at-capacity` and not otherwise paused), the lane has free capacity (`courier_lane_reservations < courier_lane_concurrency` for the lane), **and** within the window the failure counter is strictly rising (covers at least one additional `courier_source_discovery_failures_total` increment) **with no success** in the same evaluation window (no new `courier_source_discovery_successes_total` and `courier_source_last_success_timestamp_seconds` older than the window, or absent — a binding that has never succeeded satisfies the no-success gate, subject to the startup grace below) |
| Evidence | `courier_source_discovery_attempts_total`, `courier_source_discovery_successes_total`, `courier_source_discovery_failures_total`, `courier_source_last_success_timestamp_seconds` (gap), `courier_source_discovery_paused{reason}` (gap), `courier_lane_concurrency` (gap; not exported today — `LaneProfile.Spec.Concurrency`), `courier_lane_reservations`, `courier_lane_suspended`, `source discovery failed` log lines |
| Window | 3× the configured poll interval (default 90s at the 30s poll) |
| Threshold rationale | One failed poll can be a transient API blip; three consecutive windows with capacity available **and** no successful discovery indicates a persistent supply failure (a success anywhere in the window resets the predicate so an isolated blip does not latch) |
| Recovery | the predicate clears when **any** of: a successful discovery is observed in the evaluation window (`courier_source_last_success_timestamp_seconds` refreshes), the binding becomes paused (a non-zero `reason` appears in `courier_source_discovery_paused`), or the lane loses free capacity (`courier_lane_reservations >= courier_lane_concurrency`) |
| Severity | warning |
| Expected action | inspect the connector/provider, credentials, and rate limits; check whether the lane is intentionally suspended. If this is the only configured source, treat it as page-worthy (paging policy, not severity) |
| Missing telemetry | without discovery counters, fall back to log-based detection; if the paused/reservations/concurrency signals are also missing, the gate cannot be evaluated and this alert is **unavailable** (not silently green) |
| Startup | suppressed only for a **finite** startup grace while `courier_source_last_success_timestamp_seconds` is absent — candidate 10× the poll interval, matching #290's `SourceNeverSucceeded` grace, so an unbound wait for a first success that may never come cannot silence the alert. All-failed startup: once the grace elapses, the no-success gate plus rising failures fires — an all-failing source alerts instead of staying silent forever; this is A1's projection of #290's `SourceNeverSucceeded` onto a binding with capacity and not paused (per #290 §4.4 a *paused* never-succeeded binding stays silent here and speaks through the pause reason, alerting as soon as capacity frees). Empty-but-successful discovery: the first zero-item success establishes `last_success` and normal A1 semantics immediately; zero items is healthy and never fires. Ordinary initial grace: no fire. If the first poll has not yet been attempted when the grace elapses, this is `SourceDiscoveryStalled`-territory, not A1 — the alert stays suppressed until at least one attempt is recorded |
| Disabled source | does not fire (source disabled); a paused binding (`lane-suspended` / `lane-at-capacity` / any other `reason`) likewise does not fire — pausing is orthogonal to source health |
| Empty queue | this alert fires precisely because discovery *failed*, independent of whether work was returned |
| Low traffic | unaffected; it measures failure, not volume |

### A2. Lane saturated with rising queue wait

| Field | Value |
| --- | --- |
| Condition | in-flight equals lane concurrency for a sustained window **and** queue-wait quantiles are rising |
| Evidence | `courier_coderuns_in_flight`, lane concurrency (gap; not exported today — `LaneProfile.Spec.Concurrency`), `courier_coderun_queue_wait_seconds` |
| Window | 15m |
| Threshold rationale | saturation alone is normal; pairing it with a rising wait distinguishes healthy full utilisation from work that cannot start |
| Recovery | in-flight drops below concurrency and queue-wait quantiles stop rising for the window |
| Severity | info (not a page) |
| Expected action | confirm the lane is intentionally sized; consider capacity or scheduling changes downstream |
| Missing telemetry | if lane concurrency is not exposed, the alert is **unavailable** (not silently green); if queue-wait has no samples, fire only the saturation half at info severity, or suppress |
| Startup | suppressed until at least one admission is observed |
| Disabled/suspended source | does not fire (lane suspended ⇒ no admissions expected) |
| Empty queue | does not fire (no in-flight runs) |
| Low traffic | does not fire |

### A3. Stuck reservation (unadmitted work wedged, distinct from saturation)

| Field | Value |
| --- | --- |
| Condition | the lane is not suspended (`courier_lane_suspended == 0`), **capacity is free but unused** (`courier_coderuns_in_flight < courier_lane_concurrency`), **at least one reservation is unadmitted** (`courier_lane_reservations > courier_coderuns_in_flight`: reservations span empty-phase + `Pending` + `Claimed` + `Running` while in-flight counts only `Claimed` + `Running`, so the difference is exactly the materialised-but-unadmitted work), **admission has not progressed** in the window (no `Claimed` transition for the lane: `increase(courier_coderun_phase_transitions_total{to="Claimed"}[window]) == 0`), **and** the oldest reservation is older than the freshness window (`courier_lane_oldest_reservation_age_seconds` ≥ window). Deliberately *not* gated on the lane being `open` (#290 §4.5: `courier_lane_reservations < courier_lane_concurrency`): the fully reserved wedge — concurrency 1, one `Pending` reservation, in-flight 0 — must fire here, and an open-lane gate would reject it (`1 < 1` false) while A2 sees no in-flight run and A1 is paused for capacity. The discriminator against capacity-blocking is `in_flight` vs `concurrency`, not `reservations` vs `concurrency`: work waiting because every slot is genuinely busy is an availability fact (A2's territory), work waiting *while a slot is free* is the wedge |
| Evidence | `courier_lane_reservations`, `courier_coderuns_in_flight`, `courier_coderun_phase_transitions_total{to="Claimed"}` (admission-progress gate), `courier_lane_oldest_reservation_age_seconds` (gap), `courier_coderuns_pending` (gap; narrows the unadmitted set to `Pending` for display), `courier_lane_concurrency` (gap), `courier_lane_suspended`, `phase.transition` log lines for the affected `run_id`s |
| Window | 15m (operator-tuned; the reservation-freshness window is independent of the 30s poll interval because the failure mode is admission, not discovery) |
| Threshold rationale | `courier_coderuns_in_flight` excludes `Pending` and the queue-wait histogram only advances on terminalization, so a lane that has materialised work but cannot start any run — at *or below* capacity — looks *idle* on the existing panels. The free-capacity + unadmitted gates (`in_flight < concurrency` **and** `reservations > in_flight`) are what distinguish this failure from every healthy shape: a saturated lane whose reservations are all `Claimed`/`Running` and progressing (no unadmitted work), a 2-slot lane with one 20-minute `Running` task (no unadmitted work), and a genuinely busy lane with a legitimate `Pending` backlog (all slots held, `in_flight == concurrency` — #290 classes capacity-blocked `Pending` as an availability fact, surfaced by A2's rising queue wait, not a page). The age gate carries the duration; the stalled-admission gate carries "no progress" |
| Caveats | `courier_lane_oldest_reservation_age_seconds` is creation-time age of the oldest reservation across **all** reservation phases: it does **not** reset on admission or any phase transition, and it does not isolate the unadmitted subset. The alert therefore uses it as a duration floor and relies on the free-capacity + unadmitted + stalled-admission gates for precision; a per-phase reservation age is a candidate refinement. A freshly materialised item is normally admitted within one reconcile, so the just-arrived sliver is self-limiting: the first `Claimed` transition clears the predicate. #290's `StaleReservation` now uses the same `in_flight` vs `concurrency` discriminator and the same stalled-admission gate (#290 §4.5/§5, [#318](https://github.com/misospace/courier/issues/318)); the two documents must not diverge on this predicate |
| Recovery | the unadmitted backlog drains — `courier_lane_reservations` falls back to `courier_coderuns_in_flight` as `Pending`/empty-phase work is claimed, or a `Claimed` transition is observed again in the window (**not** an age reset: the age gauge measures `metadata.creationTimestamp` and admission does not clear it); or the lane becomes suspended; or the reservation terminates (failed/reaped), shrinking `courier_lane_reservations`; or a reconfiguration removes the free capacity (in-flight reaches the new concurrency) — the predicate clears through the gates, never through an age reset |
| Severity | warning |
| Expected action | inspect lane capacity, the controlling `LaneProfile`, and the affected `run_id`s; check for an admission-side wedge (controller stalled reconcile, broker handoff blocked, webhook not reachable) before treating it as a sizing problem |
| Missing telemetry | if `courier_lane_oldest_reservation_age_seconds` or `courier_lane_concurrency` (or the constituents of either) is absent, the gates cannot be evaluated and the alert is **unavailable** (not silently green); the queue-wait histogram alone cannot detect this condition |
| Startup | self-suppressing: the age gate cannot hold until some reservation is older than the freshness window; the transition counter resets on controller restart, so the admission-progress gate re-arms from the restart |
| Disabled/suspended source | does not fire (lane suspended). A lane full of admitted, progressing reservations is healthy saturation (A2); a lane full of reservations that stay *unadmitted* fires here — the wedge A2 cannot see |
| Empty queue | does not fire (no reservations ⇒ no oldest age and no unadmitted work) |
| Low traffic | unaffected; age and admission progress are the signals, not volume |

### A4. Elevated run failure rate

| Field | Value |
| --- | --- |
| Condition | rate of terminal runs in a failure phase (`Failed`, and optionally `NeedsHuman`) exceeds the threshold over the window |
| Evidence | `courier_coderuns_total{phase=...}` |
| Window | 1h |
| Threshold rationale | a small absolute failure count can be normal; a sustained ratio above the operator's baseline is actionable |
| Recovery | failure ratio returns below threshold for one full window |
| Severity | warning |
| Expected action | inspect terminal diagnostic mix (A6) and recent `run.exit` events to find the common cause |
| Missing telemetry | if no terminal runs occurred, no ratio exists ⇒ no fire (low traffic is not failure) |
| Startup | suppressed until enough terminal runs exist to form a ratio |
| Disabled source | does not fire |
| Empty queue | does not fire |
| Low traffic | use an absolute-count floor alongside the ratio so one failure in one run does not page |

### A5. Lifecycle reports not settling

| Field | Value |
| --- | --- |
| Condition | terminal runs with `LifecycleReported` in `False`/`Unknown` (or missing) exceed the threshold and are older than the settle window |
| Evidence | `Conditions`, retry tokens; report-age metric (gap) |
| Window | 15m sustained, plus an age floor of one report retry cadence plus headroom (operator-tuned) |
| Threshold rationale | reports legitimately retry; a report still unsettled well past the retry cadence indicates a broken source contract or credentials |
| Recovery | the condition reaches `True` with a permitted settled reason, or the run is superseded |
| Severity | warning |
| Expected action | inspect the source report path and the `LifecycleReported` reason |
| Missing telemetry | without a report-age metric, derive age from status timestamps where present; otherwise this alert is **unavailable** |
| Startup | legacy/partially-upgraded runs with no condition are excluded by requiring a condition to exist |
| Disabled source | if the source is disabled, exclude its runs |
| Empty queue | does not fire |
| Low traffic | an absolute floor prevents a single slow report from paging |

### A6. Terminal diagnostic concentration

| Field | Value |
| --- | --- |
| Condition | failures in a single `TerminalDiagnostic` category (for example `kubernetes-failure` or `model-transport-error`) exceed the threshold over the window |
| Evidence | `TerminalDiagnostic` category, `run.exit` log |
| Window | 1h |
| Threshold rationale | a category spike points at one cause (infra, transport) faster than a flat failure rate does |
| Recovery | category count returns below threshold |
| Severity | warning |
| Expected action | act per category: infrastructure for `kubernetes-failure`, provider/transport for `model-transport-error` |
| Missing telemetry | absent diagnostics count as `unknown`, not as a known category |
| Startup | suppressed until diagnostics are observed |
| Disabled source | does not fire |
| Empty queue | does not fire |
| Low traffic | absolute floor applies |

### A7. Metrics scrape failing

| Field | Value |
| --- | --- |
| Condition | `courier_metrics_scrape_errors_total` increases over the window |
| Evidence | `courier_metrics_scrape_errors_total` |
| Window | 10m |
| Threshold rationale | a failing scrape means lane gauges are stale; even a low rate is worth surfacing because it silently degrades sections 3.2 |
| Recovery | counter stops increasing |
| Severity | warning |
| Expected action | inspect the operator's RBAC and API server reachability for `LaneProfile`/`CoderRun` list calls |
| Missing telemetry | if the counter is absent, the operator predates the metric; treat as unknown |
| Startup | counter is monotonic; rate() naturally ignores the initial zero |
| Disabled/empty/low traffic | unaffected; this is a collection-health signal |

### A8. Controller target down

| Field | Value |
| --- | --- |
| Condition | the Courier metrics target is unscrapable (`up == 0` or the series disappear) |
| Evidence | backend-provided target health (for example Prometheus `up`); controller-runtime `process_start_time_seconds` for restart detection |
| Window | 5m |
| Threshold rationale | a missing controller is the most severe Courier condition; without it nothing else is observed |
| Recovery | target scrapes again |
| Severity | critical |
| Expected action | inspect the operator deployment and pod |
| Missing telemetry | absence of all series is the signal; this alert does not depend on Courier's own instrumentation |
| Startup | a target that has never been up is a deployment problem, not a false positive |
| Disabled/empty/low traffic | unaffected |

### A9. Connector capability absent or protocol errors (optional)

| Field | Value |
| --- | --- |
| Condition | a configured connector reports an unsupported capability it previously supported, or its protocol error rate exceeds threshold |
| Evidence | connector diagnostics (#291) |
| Window | 15m |
| Threshold rationale | capability regression or protocol errors break discovery in a source-specific way; it is intentionally kept out of core health |
| Recovery | capability returns or error rate subsides |
| Severity | warning |
| Expected action | inspect the connector and its upstream provider |
| Missing telemetry | connector diagnostics absent ⇒ render "unsupported/unknown", never OK |
| Startup | suppressed until the connector reports once |
| Disabled source | does not fire (connector not configured) |
| Empty queue / low traffic | unaffected |

## 5. Integration matrix

Rows are portability layers; columns are backends. "Yes" means the layer can be
implemented; "optional" means it is a downstream choice; "no"/"n/a" means the
specification does not promise it. **No backend is promised identical assets or
feature parity.**

| Layer | Prometheus | VictoriaMetrics | OTel-based pipeline | Other |
| --- | --- | --- | --- | --- |
| Portable specification (views + conditions) | yes | yes | yes | yes — required of any integration |
| Optional executable dashboard assets | optional (Grafana or other PromQL UI) | optional | optional (any OTLP/TSDB UI) | optional |
| Optional executable alert/rule assets | optional (Prometheus rules) | optional (`vmalert` rules) | optional (rules in the named evaluator: a Prometheus server, `vmalert`, or a Mimir ruler — see "OTel pipeline: transport versus evaluator" below) | optional |
| Transport / export support | scrape `/metrics` | scrape `/metrics` | OTLP metrics/logs via Collector | backend-defined |
| Downstream install / routing | operator-owned | operator-owned | operator-owned | operator-owned |

### OTel pipeline: transport versus evaluator

OTLP is transport. A viable OTel-based integration names its evaluator:

- **Transport:** the OpenTelemetry Collector receives Courier's metrics (and,
  optionally, its structured logs via a filelog/logs receiver) over OTLP or a
  scrape receiver, and exports them.
- **Evaluator:** alert rules run in a backend that can evaluate them —
  a Prometheus server, VictoriaMetrics' `vmalert`, or a Grafana Mimir ruler.
  The Collector's `routing`/`filter` processors are not a general alert
  evaluator; they may pre-process but do not replace rule evaluation.
- **Dashboard:** any UI reading the resulting TSDB. Grafana is one option, not
  a requirement.

An integration that exports OTLP with no evaluator can render dashboards but
**cannot** evaluate the alert catalogue; that is an unsupported combination
(section 7).

## 6. Backend mapping examples and integration template

The sketches below are **illustrative and non-executable**. They show the shape
of a mapping, not deployable configuration.

### Prometheus / VictoriaMetrics

PromQL-compatible renderings of representative panels:

```promql
# Run failure ratio (A4)
sum(rate(courier_coderuns_total{phase="Failed"}[1h]))
  / sum(rate(courier_coderuns_total[1h]))

# Lane saturation (A2)
courier_coderuns_in_flight
  / on(lane) group_left()
max by (lane) (courier_lane_concurrency)   # concurrency exposure is a gap

# Scrape errors (A7)
increase(courier_metrics_scrape_errors_total[10m]) > 0

# Median run duration by lane (3.3)
histogram_quantile(
  0.5,
  sum by (le, lane) (rate(courier_coderun_run_duration_seconds_bucket[1h]))
)
```

`histogram_quantile` over `_bucket` and `rate`/`increase` over counters are the
shared semantics. VictoriaMetrics accepts the same query shapes.

### OTel-based pipeline

```text
Courier /metrics ──scrape──▶ OTel Collector ──OTLP──▶ TSDB ──rules──▶ evaluator
Courier stdout   ──filelog─▶ OTel Collector ──OTLP──▶ log store ──correlate──▶ run_id
```

The evaluator is explicitly named at install time (Prometheus, `vmalert`, or a
Mimir ruler). If only a log store is deployed, alerts are not evaluated.

### Template for a new integration

An implementer adding a backend answers every line; blanks are unsupported, not
implicitly OK:

1. **Transport:** how Courier metrics reach the backend (scrape, push, OTLP);
   how structured logs reach it (if at all).
2. **Evaluator:** which component evaluates alert rules; if none, say so and
   stop — the alert catalogue is unsupported.
3. **Dashboard mechanism:** what renders the views (a PromQL UI, OTLP UI, or
   bespoke); how it discovers panels.
4. **Signal coverage:** which signals in section 2 exist in this backend, and
   which gaps in section 2 make a view unavailable.
5. **Units and aggregation:** how counters, gauges, and histograms map; how
   reset and staleness are handled.
6. **Cardinality and privacy:** how label budgets and forbidden labels are
   enforced.
7. **Install and routing:** who installs the assets and who owns notification
   routing (always downstream, never the specification).
8. **Limitations:** every unsupported combination, stated explicitly.

## 7. Unsupported combinations and open questions

**Not promised:**

- Identical dashboards, rules, or queries across backends.
- Feature parity: a backend may implement a subset of views, but it must mark
  unavailable views rather than render them as healthy.
- OTel-only alerting: OTLP with no evaluator cannot evaluate alerts (see the
  per-alert "Missing telemetry" rows — every alert whose fallback is
  log-based is unavailable when logs are not collected either).
- Any dependency on Dispatch telemetry for Courier health.
- Any dependency on home-ops namespaces, cluster/node names, credential
  sources, notification routes, Grafana, or Alertmanager.
- Deployment manifests, exporter dependencies, or home-ops wiring from this
  specification.
- A single universal dashboard or rule format.

**Open questions** (owned by sibling designs; this document consumes the
answers):

Settled by the siblings on their current heads and consumed here, no longer
open:

- Discovery attempt/success/failure signal names and units — settled by #290
  (`courier_source_discovery_attempts_total`, `..._successes_total`,
  `..._failures_total`, with `..._paused_total` counting polls skipped before
  the source was called, not as attempts).
- The source-level state model separating idle from degraded — settled by #290
  (`disabled`/`starting`/`ok`/`degraded`/`unavailable`/`unknown`, with pausing
  orthogonal).
- Connector capability negotiation and how dashboards discover optional
  capabilities — settled by #291 (negotiation states plus a
  `courier_connector_capability_info` discovery procedure; how that catalogue is
  emitted is still open below).
- Reservation staleness as a first-class condition — settled by #290 as the
  admission-wedge condition (`in_flight < concurrency` free-but-unused with an
  unadmitted reservation and no `Claimed` progress, backed by a reservation-age
  signal), matching this document's A3 alert.
- Per-phase timing and token metrics with bounded dimensions — settled by #290
  (`courier_coderun_phase_duration_seconds` by `lane`/`mode`/`phase`;
  `courier_coderun_tokens_total` by `lane`/`mode`/`kind`).

Still open on the siblings' own unknown lists, and consumed as such:

- Whether per-phase duration and token counters ship in a first cut or after
  the discovery/lane layer (#290).
- Whether the connector capability catalogue is a scrape-time gauge, a startup
  event, or both (#291).

## 8. Units, aggregation, and reset semantics

These rules make any two renderings equivalent:

- **Counters** (`courier_coderuns_total`,
  `courier_metrics_scrape_errors_total`) are monotonic per series and reset to
  zero when the operator restarts. Panels and alerts use `rate`/`increase`, not
  raw values, and tolerate a reset as a step, not a negative.
- **Gauges** (`courier_coderuns_in_flight`, `courier_lane_suspended`) are read
  live from the cluster at scrape time, so they survive an operator restart and
  always match the world. They are point-in-time values, never accumulated.
- **Histograms** (`courier_coderun_run_duration_seconds`,
  `courier_coderun_queue_wait_seconds`) are observed once per terminal run, not
  per reconcile, so a quantile window must contain a meaningful number of
  terminal runs and an empty window is the normal idle path (section 3.3), not
  a failure.
  Quantiles are computed over a window with `histogram_quantile`; they are
  approximations bounded by the configured buckets, not exact percentiles. The
  run-duration buckets top out at 4h and the queue-wait buckets at 1h; a value
  above the last bucket is reported in the `+Inf` bucket and must be shown as
  "≥ top bucket", not a precise duration.
- **Timestamps** are recorded by the controller; durations are derived from
  them, so clock skew is clamped (negative durations fold to zero).
- **Missing series vs zero:** an absent series means "no data" (unknown), while
  a present zero means "measured zero". Panels must not conflate them; a lane
  with no runs keeps reporting zero in-flight (the collector seeds lanes at
  zero), while a lane that has never been observed is absent.
- **Evaluation windows** are multiples of the 30s poll interval where they
  describe discovery; other windows are stated per alert. Alert rules must
  include a `for`/sustained duration so a single scrape cannot fire.

## 9. Cardinality and privacy constraints

- **No high-cardinality labels.** Run IDs, issue numbers, PR numbers, arbitrary
  error strings, repository names, and secret values must never be metric
  labels. The existing metrics carry only `lane`, `mode`, and terminal `phase`
  (plus `lane` alone on the gauges), which is the intended budget.
- **Correlate in logs, not labels.** Per-run correlation (`run_id`) belongs to
  the structured log stream, joined in the dashboard, never promoted to a
  metric dimension.
- **Bounded dimensions only.** Any new dimension (#290/#291; see section 7
  "Open questions") must be a closed, bounded vocabulary (for example a
  diagnostic category), not free text, and the label budget above applies to
  it unchanged.
- **Redaction before export.** Verbose tool I/O is redacted before it reaches
  stdout, so log-based panels inherit that redaction. Integrations must not
  re-introduce unredacted content.
- **Retention is the backend's job.** Courier does not persist transcripts;
  retention windows for logs and metrics belong to the deployed stack, and the
  dashboard should state the effective window it is showing.
- **Privacy of telemetry.** Per-run telemetry (tokens, subagents) is
  operational metadata; it must not be used to identify individuals or leak
  prompt content into metric labels.

## 10. Ownership and composition

Courier owns this specification and its standalone dashboard. Dispatch owns its
own observability design and dashboard; the two are independently useful. An
operator may compose them (for example, a Courier panel showing Dispatch queue
health alongside Courier lane health), but composition is optional and neither
application requires the other's telemetry, dashboard, runtime, or release
cadence. Where a composed view is offered, both halves must remain usable when
the other is absent.
