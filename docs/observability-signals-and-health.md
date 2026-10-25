# Source-agnostic telemetry and health semantics (#290)

This document specifies Courier's own telemetry signals and health semantics: the
measurements the operator emits, the state model those measurements resolve to,
and the conditions an operator can reason about. It is a **design-only** child of
the portable-observability umbrella (#289). It defines no code, exporter,
dashboard, alert rule, deployment, or environment-specific configuration, and it
does not authorize any implementation. The behaviour issues it borders — #286
(exclusion overflow) and #288 (`Pending` display) — remain separate work; #287
(per-phase retention) has since landed independently and is read here as
existing behaviour (§3.7), not as something this document proposes. Nothing
here changes lifecycle behaviour or retention policy.

The grounding surfaces are the existing metrics
(`internal/controller/metrics.go`), the source-neutral interfaces
(`internal/source/source.go`), the discovery runner
(`internal/source/runner.go`), the structured event vocabulary
(`internal/log/events.go`), the `CoderRun`/`LaneProfile` types
(`api/v1alpha1/`), and the terminal-diagnostic taxonomy
(`internal/controller/diagnostics.go`). Sibling designs consume this one:
#291 owns adapter/connector diagnostics and #292 owns the dashboard and alert
views. Where this document answers a question #292 recorded as open, it says so.

## 1. Boundaries

- **No source may be assumed.** Core signals are keyed on a configured adapter
  name and a lane, never on Dispatch queues, PR-fix generations, its exclusion
  cap, or GitHub. A deployment whose only source is a non-Dispatch adapter emits
  a complete core signal set.
- **Core is source-neutral.** Everything defined in §3.2–§3.8 lives behind the
  `internal/source` boundary. A source's protocol, request/response shapes, and
  error strings are never a core label or constant; they are adapter-owned
  diagnostics (#291).
- **Courier observes; it does not govern.** This document defines how state is
  *displayed*, not new gates, caps, or timeouts. No signal here blocks a run.
- **Uncertainty is a state.** Absent, unsupported, stale, and unknown are
  first-class and distinct from both success and failure.

## 2. The core problem: a source-level health view that exists with no runs

Courier's run-centric observability — `courier_coderuns_total`,
`courier_coderun_run_duration_seconds`, and `courier_coderun_queue_wait_seconds`
are observed only when a run reaches a terminal phase
(`internal/controller/metrics.go:221-240`); `courier_coderuns_in_flight` and
`courier_lane_suspended` are read live from the cluster
(`internal/controller/metrics.go:421-510`) — **cannot distinguish an idle,
healthy, empty queue from a broken discovery loop**. The source-level layer
below does, because a discovery failure is counted
(`courier_source_discovery_failures_total`) as well as logged.

That **source-level** layer, `courier_source_*`, is emitted by the discovery
runner independently of whether any `CoderRun` is created. Three outcomes that
look identical in a run-centric view become distinguishable:

| Situation | What is true | What the source view must say |
| --- | --- | --- |
| Idle empty queue | discovery succeeded, returned zero items | `ok` (idle) |
| Failed discovery | discovery call errored, capacity available | `degraded` / `unavailable` |
| Unavailable capacity | lane suspended, or reservations meet concurrency, so discovery is paused before the source is called | paused (`lane-suspended` / `lane-at-capacity`), not a source failure |

This is the acceptance requirement that the view exists even when discovery
produces no `CoderRun`.

## 3. Signal catalogue

### 3.1 Naming, kinds, units, ownership

- **Prefix.** Existing run/lane metrics keep their `courier_*` names. The new
  source-neutral discovery layer uses `courier_source_*`, matching the name
  #291 already assumes for `courier_source_discovery_failures_total`.
- **Units.** Prometheus base units: seconds for time, a bare count for
  counters, a ratio/boolean (0 or 1) for state gauges. Status durations stay Go
  duration strings or integer milliseconds (`api/v1alpha1/coderun_types.go`).
- **Kinds.** *Counters* are monotonic per series and reset on process restart.
  *Gauges* are point-in-time; cluster-derived gauges are read live at scrape and
  survive restart, while emitted gauges (timestamps) are re-established after a
  restart. *Histograms* accumulate from process start and are observed once per
  event, never per reconcile.
- **Ownership.** A signal is owned by exactly one producer. Core owns
  `courier_source_*` and the lane/run signals; an adapter owns its own
  diagnostics (#291). When two layers describe one event, they are joined, never
  summed (§10).

Every label in the catalogue is drawn from a **closed, bounded vocabulary**:
`source` (the configured source identity — the `CoderRun.spec.source` value,
never the agent name), `lane` (configured profiles), `mode`
(`resolve-issue` | `fix-pr`), `phase` (§3.4), `from`/`to` (phase), `category`
(§3.5), `result` (§3.5, §3.6), and per-signal `reason` enums. §7 states the
forbidden values and the budget.

### 3.2 Discovery (core, source-neutral)

Emitted by the discovery runner for each configured binding, where a **binding**
is a `(source, lane)` pair (`internal/source/runner.go:33-39`). A binding whose
source runner is not configured emits nothing.

| Signal | Kind | Unit | Labels | Semantics |
| --- | --- | --- | --- | --- |
| `courier_source_discovery_attempts_total` | counter | attempts | `source`, `lane` | One per poll that actually called the source. A poll skipped for lane-waiting/suspended/capacity is **not** an attempt (§3.3). |
| `courier_source_discovery_successes_total` | counter | polls | `source`, `lane` | Source calls that returned without error, **including zero items**. This is what makes an idle queue observable as healthy. |
| `courier_source_discovery_failures_total` | counter | polls | `source`, `lane` | Source calls that returned an error. The authoritative discovery-failure rate (#291 §6.2). |
| `courier_source_items_offered_total` | counter | items | `source`, `lane` | Items returned by discovery before core validation and dedupe. |
| `courier_source_discovery_skipped_total` | counter | items | `source`, `lane`, `reason` | Items dropped by runner validation; `reason` ∈ `missing-id`, `unsupported-mode`, `invalid-ref` (`internal/source/runner.go:330-345`). |
| `courier_source_items_materialized_total` | counter | runs | `source`, `lane` | Items that became `CoderRun`s. Dedupe and exclusions may make this smaller than `items_offered_total` (`internal/source/runner.go:350-443`). |
| `courier_source_last_attempt_timestamp_seconds` | gauge | unix seconds | `source`, `lane` | When the source was last called. |
| `courier_source_last_success_timestamp_seconds` | gauge | unix seconds | `source`, `lane` | When discovery last succeeded. **Absent until the first success** — absence is `starting`/`unknown`, never `idle`. |
| `courier_source_discovery_paused_total` | counter | polls | `source`, `lane`, `reason` | Polls skipped before the source was called; `reason` ∈ `lane-waiting`, `lane-suspended`, `lane-at-capacity`, `capacity-cooldown` (`internal/source/runner.go:174-250`). |
| `courier_source_discovery_paused` | gauge | 0/1 | `source`, `lane`, `reason` | 1 while the runner is currently paused before calling the source, 0 otherwise. Exactly one reason is 1 per paused binding; all are 0 when not paused. This is the current-state half of the counter above. |

`attempts = successes + failures` at steady state; skipped polls are counted
under `discovery_paused_total`, not as attempts. The current-state
`discovery_paused` gauge is published as one atomic snapshot per binding: a
scrape never observes two reasons at 1, nor a half-applied reason transition,
even when several runners share one `(source, lane)` pair. The gap between
`items_offered_total` and `items_materialized_total` is the observable cost of
dedupe/exclusion, not an error. The failure **cause** (transport, auth, HTTP
class, malformed response) is deliberately **not** a core label: it is the
connector's `connector.diagnostic` breakdown (#291 §3.3, §6.2), joined by the
shared adapter name.

The `reason` tokens in this catalogue are a **structured metric vocabulary**
owned by the emitter. They are deliberately kebab-case and distinct from the
free-form log prose the runner prints today (for example the runner logs
`"waiting for LaneProfile"` at `internal/source/runner.go:177`, which the metric
emits as `reason="lane-waiting"`); the log message is not a label. The
implemented vocabulary (`internal/source/metrics.go`) carries a fourth pause
reason, `capacity-cooldown`, for the confirmed capacity-exhaustion cooldown
pause (#327). The vocabulary stays closed; a new reason means extending that
slice, not a new string at an emission site.

### 3.3 Lane capacity and reservations (core)

Reservations exist as an internal predicate
(`laneCapacityReservations`, `internal/source/runner.go:610-620`). The runner
reports a lane at capacity in one log line
(`internal/source/runner.go:237-244`); the collector exports the reservation
count and the oldest-reservation age, so the lane's queue is observable.

| Signal | Kind | Unit | Labels | Semantics |
| --- | --- | --- | --- | --- |
| `courier_lane_concurrency` | gauge | slots | `lane` | Configured `LaneProfile.Spec.Concurrency` (default 1). Closes the gap #292 recorded for the saturation alert. |
| `courier_lane_suspended` | gauge | 0/1 | `lane` | Existing: `LaneProfile.Suspended()` (`api/v1alpha1/laneprofile_types.go:13,71`). |
| `courier_coderuns_in_flight` | gauge | runs | `lane` | Existing: runs admitted by the operator, i.e. `Claimed` + `Running` (`internal/controller/admission.go:21-23`). |
| `courier_lane_reservations` | gauge | runs | `lane` | Discovery reservations: empty-phase + `Pending` + `Claimed` + `Running` (`internal/source/runner.go:610-620`). Deliberately broader than `in_flight`. |
| `courier_coderuns_pending` | gauge | runs | `lane` | Runs in `PhasePending` (a precise subset of reservations, for the queue display). |
| `courier_lane_oldest_reservation_age_seconds` | gauge | seconds | `lane` | Age of the oldest reservation by `metadata.creationTimestamp`; absent when there are none. The evidence for the stale-reservation condition (§5). |

The two reservation predicates must not be merged: operator admission counts
only `Claimed`+`Running`, while discovery counts `Pending` and empty-phase too,
so that a binding does not hoard materialized-but-unadmitted work (the
"Lane bindings (#173)" section of [DESIGN.md](../DESIGN.md)). The design keeps
both visible rather than collapsing them.

### 3.4 Run phases, transitions, and age (core)

Phases are the existing enum (`api/v1alpha1/coderun_types.go:83-104`): `Pending`,
`Claimed`, `Running`, `Verifying`, `AwaitingReview`, `HandedOff`, `NeedsHuman`,
`Done`, `Failed`, `BlockedExternal`. Terminal phases carry `FinishedAt`
(`internal/controller/coderun_controller.go:988-1055`).

| Signal | Kind | Unit | Labels | Semantics |
| --- | --- | --- | --- | --- |
| `courier_coderun_phase_transitions_total` | counter | transitions | `lane`, `mode`, `from`, `to` | One per observed phase change. `from`/`to` are the bounded phase enum. |
| `courier_coderun_phase_duration_seconds` | histogram | seconds | `lane`, `mode`, `phase` | Time spent in a phase, observed once on leaving it, and only when the phase had a durable entry marker. Buckets are the run-duration buckets (`internal/controller/metrics.go:20-22`). |
| `courier_coderuns_total` | counter | runs | `lane`, `mode`, `phase` | Existing: terminal runs by **first** terminal phase. The label is set once — `transitionTerminal` records the metric exactly once on the run's first terminal transition (`firstTerminal := FinishedAt == nil`, `internal/controller/coderun_controller.go:999-1053`), and a later transition that corrects the ending (for example `AwaitingReview → Done`, #250) does not increment the new phase. The label preserved is the phase at the first terminal transition, not the run's final disposition. |
| `courier_coderun_run_duration_seconds` | histogram | seconds | `lane`, `mode`, `phase` | Existing: `StartedAt`→`FinishedAt`. |
| `courier_coderun_queue_wait_seconds` | histogram | seconds | `lane`, `mode`, `phase` | Existing: creation→`AdmittedAt`. |

Phase **age** (how long a run has been in its current phase) is not a metric
dimension: a per-run label would be unbounded. Before the durable
`Status.PhaseEnteredAt` marker, the status timestamps covered a subset of
phases only; what is recoverable from `Status` without it is:

| Phase | Recoverable from `Status` without the marker | What was missing |
| --- | --- | --- |
| `Pending` | yes — `now − metadata.creationTimestamp` | — |
| `Claimed` | partial — `now − AdmittedAt` overstates `Claimed` age once the run advances to `Running` | a `Claimed`-entry timestamp |
| `Running` | yes — `now − StartedAt` | — |
| `Verifying` | **no** — `StartedAt` is the `Running` start, not the `Verifying` start; `FinishedAt` is nil; no condition `lastTransitionTime` is stamped for `Verifying` (`internal/controller/coderun_controller.go:691-708`). A view that derives `Verifying` age from `Status` overstates it by the entire `Running` duration | the `Verifying`-entry timestamp |
| `AwaitingReview` / `HandedOff` / `Failed` / `NeedsHuman` / `BlockedExternal` | partial — `FinishedAt` is the run's first terminal transition (set-once, `internal/controller/coderun_controller.go:1014-1023`); a later `AwaitingReview → Done` does **not** move `FinishedAt` and never records a `Done`-entry timestamp, so `now − FinishedAt` overstates a `Done` run's age by the gap from `AwaitingReview` to merge | a per-phase entry timestamp |
| `Done` | **no** — never an entry timestamp; the terminal histogram captures aggregate distribution, not per-run age | the `Done`-entry timestamp |

The recoverable cells are the only safe derivations from the pre-#324 status
shape; the **unknown** cells are exactly the cases the rule in §4.3 demands:
uncertainty is a state, not a measured zero (a view that shows `now −
StartedAt` as `Verifying` age is silently wrong, not just imprecise). The
histogram in this section captures aggregate distribution without a per-run
label. Per-run phase age across restart is answered by the durable
`Status.PhaseEnteredAt` marker shipped with #324: it is restamped on every
phase change inside the status patch, so `now − PhaseEnteredAt` is truthful
for every phase, and a run recorded before the marker existed reports no
duration rather than a fabricated one.

### 3.5 Execution outcomes (core)

Execution outcomes are what the operator observed about the run's own execution,
kept separate from external disposition (§4.1).

| Signal | Kind | Unit | Labels | Semantics |
| --- | --- | --- | --- | --- |
| `courier_coderun_terminal_diagnostics_total` | counter | runs | `lane`, `mode`, `category` | One per terminal run, by `TerminalDiagnostic.Category`; `category` is the closed enum in `api/v1alpha1/coderun_types.go:194-222`. |
| `courier_coderun_infra_restarts_total` | counter | restarts | `lane`, `mode` | Coordinator infra relaunches (`Status.Restarts`, `internal/controller/liveness.go:593-620`). |
| `courier_coderun_verification_outcomes_total` | counter | runs | `lane`, `mode`, `result` | External verification disposition observed by the operator: `result` ∈ `green` (stable green → `AwaitingReview`), `red` (confirmed red → `HandedOff`/`Failed`), `unobservable` (draft/no-PR/unreadable → `NeedsHuman`) (`internal/controller/coderun_controller.go:885-963`). |
| `courier_coderun_tokens_total` | counter | tokens | `lane`, `mode`, `kind` | Aggregate token totals from `RunTelemetry`; `kind` ∈ `input`, `output`, `reasoning`, `cache-read`, `cache-write`. `total` is the sum of these parts and is derived, not a separate series. Per-run and per-session detail stays in `status.telemetry` and the `run.summary` log; only the bounded aggregate is a metric. |

### 3.6 Lifecycle reporting (core)

Reporting state already exists as two conditions —
`LifecycleReported` (`api/v1alpha1/coderun_types.go:67-70)` and the support
condition `LifecycleReportSupported`
(`internal/controller/coderun_controller.go:47`) — and a bounded retry cadence
(`lifecycleReportRequeueDelay = 30s`, `internal/controller/coderun_controller.go:36`).

| Signal | Kind | Unit | Labels | Semantics |
| --- | --- | --- | --- | --- |
| `courier_source_lifecycle_report_outcomes_total` | counter | reports | `source`, `lane`, `mode`, `result` | One per terminal report attempt outcome; `result` ∈ `published`, `superseded`, `unsupported`, `retryable`, `permanent-refusal`. |
| `courier_coderuns_lifecycle_pending` | gauge | runs | `lane` | Terminal runs whose lifecycle report is not positively settled (condition not `True` with reason `Published`/`Superseded`/`PermanentRefusal`) — the unsettled-report backlog, mirroring the retention hold gate. |
| `courier_coderuns_lifecycle_pending_oldest_age_seconds` | gauge | seconds | `lane` | Age of the oldest unsettled report, from `FinishedAt`. Evidence for the reporting-failure condition. |

The metric `result` vocabulary is a bounded projection of the two existing
conditions, not a new source of truth. It maps to the code's reasons
(`internal/controller/coderun_controller.go:1099-1121`):
`published` ← `LifecycleReported=True` reason `Published`; `superseded` ←
`LifecycleReported=True` reason `Superseded`; `permanent-refusal` ←
`LifecycleReported=True` reason `PermanentRefusal`; `unsupported` ←
`LifecycleReportSupported=False` reason `UnsupportedOutcome`; `retryable` ←
`LifecycleReportSupported` reason `Retryable`. The `Pending*` reasons (`Pending`,
`PendingBlockedReportParksPRFix`, `PendingWithError`,
`PendingExternalVerificationFailure`) are the unsettled state counted by
`courier_coderuns_lifecycle_pending`, not a `result`.

The retry tokens (`LifecycleReportRetryToken`,
`LifecycleReportActiveRetryToken`, `api/v1alpha1/coderun_types.go:499-507`) stay
per-run status and are **not** labels; only their aggregate count is observed.

### 3.7 Retention and reaping (core)

Retention is already per-phase and already partly observable, so this section
records what exists and names the one remaining gap rather than proposing the
whole layer. Each reaped phase's window is a constant in
`internal/controller/reap.go:29-40`: `defaultDoneRetention = 5m` for `Done`,
`defaultFailedRetention` and `defaultNeedsHumanRetention = 168h` for `Failed`
and `NeedsHuman`, and `defaultTerminalAuditKeep = 5` — the per-repository audit
floor that keeps the most recent floor-governed runs past their window. Each
window runs from its own durable marker (`courier.misospace.dev/done-at`,
`failed-at`, `needs-human-at`, `internal/controller/reap.go:24-26`) rather than
`creationTimestamp`, because a run can exist far longer than its window before it
terminalizes. Deletion additionally waits on the source lifecycle report: the
reconciler holds a run in place while `lifecycleReportSettled` is false — on the
`AwaitingReview` path before it may become `Done`
(`internal/controller/coderun_controller.go:270`) and on the
`Failed`/`NeedsHuman` path before reaping
(`internal/controller/coderun_controller.go:284-288`; predicate at
`internal/controller/coderun_controller.go:1266`) — and `reapTerminal` records
that as the caller's precondition rather than checking it itself
(`internal/controller/reap.go:58-61`). A run reaches `Done` by two routes and
only one sits behind that gate: from `AwaitingReview`, so the run arrives with an
already-settled report; and from `Verifying` on a merged-PR observation
(`internal/controller/coderun_controller.go:895-899`), where the load-bearing
gate is instead the source resolve succeeding (`resolveCompleted`,
`internal/controller/coderun_controller.go:619-629`). Three phases reach
`reapTerminal`: `Failed` and `NeedsHuman` directly
(`internal/controller/coderun_controller.go:289-292`) and `Done` through `reapDone`
once its source work has resolved (`internal/controller/reap.go:42-49`);
`AwaitingReview` goes to `observeMergedReviewPR` instead, and `HandedOff` and
`BlockedExternal` fall through to a log line, so those phases are never reaped
(`internal/controller/coderun_controller.go:258-295`). Nothing here changes that
policy.

| Signal | Kind | Unit | Labels | Semantics |
| --- | --- | --- | --- | --- |
| `courier_coderuns_reap_requests_total` | counter | requests | `phase` | Existing: terminal-run deletions the reaper asked the API server to accept, by the terminal phase they were requested from. It counts **requests, not deletions** — the API server accepts a delete while a finalizer still holds the object, so a securely-topology-held run is counted here and not yet gone. |
| `courier_coderuns_oldest_terminal_age_seconds` | gauge | seconds | `phase` | Existing: age of the oldest retained terminal run per phase, read live from the cluster at scrape time, so it survives an operator restart. Paired with the counter above it is what separates "the reaper is asking" from "the run is actually still there". |
| `courier_coderuns_awaiting_reap` | gauge | runs | `phase` | Shipped with #324: terminal runs retained for inspection and not yet reaped. The age gauge answers *how stale* the oldest retained run is; this answers *how many* runs are held, which is the retained-backlog view #292 needs. |

Both shipped retention signals are labelled `phase` only, never `lane`
(`reapMetricLabels`, `internal/controller/metrics.go:36`): retention is a
per-terminal-phase property, and a `lane` label would multiply the series by
lane for no diagnostic gain.

### 3.8 Crosswalk with existing signals

| Existing signal | Disposition in this design |
| --- | --- |
| `courier_coderuns_total` | retained (§3.4) |
| `courier_coderun_run_duration_seconds` | retained (§3.4) |
| `courier_coderun_queue_wait_seconds` | retained (§3.4) |
| `courier_coderuns_in_flight` | retained (§3.3) |
| `courier_lane_suspended` | retained (§3.3) |
| `courier_metrics_scrape_errors_total` | retained; it is the collection-health signal for the whole catalogue |
| `courier_coderuns_reap_requests_total` | retained (§3.7) |
| `courier_coderuns_oldest_terminal_age_seconds` | retained (§3.7) |
| `run.exit`, `phase.transition`, `run.summary`, `capability.status`, `evidence.capture`, `outcome.*` events | retained as the log half; `phase.transition` is the source for `courier_coderun_phase_transitions_total`, `run.exit` for the terminal diagnostic category |

## 4. State model

### 4.1 Execution completion versus external disposition

Courier's own work and the world's judgement are different events and must not
collapse into one "done".

- **Executor completion** is the run leaving `Running`: it entered `Verifying`
  (executor exit `0`, which `phaseForExit` routes there without checking
  publication), or it reached a declared terminal phase
  (`needs-human`, `blocked-external`, and a declared no-change outside fix-pr
  mode — a fix-pr declared no-change goes through `Verifying` like a normal
  exit, `phaseForExit`, `internal/controller/coderun_controller.go:1747-1764`).
  Current evidence does
  **not** measure executor completion directly. `FinishedAt` is set on the run's
  first *terminal* transition (`transitionTerminal`,
  `internal/controller/coderun_controller.go:988-1055`) and
  `courier_coderuns_total` plus the run-duration histogram are recorded at the
  same point (`ObserveTerminal`, `internal/controller/metrics.go:221-240`); the
  `Running → Verifying` transition (`internal/controller/coderun_controller.go:691-708`)
  records neither. So a run parked in `Verifying` is not yet counted, and its
  eventual `StartedAt → FinishedAt` duration includes the `Verifying`
  observation wait: the terminal histogram measures execution *plus*
  disposition, not executor time alone. For runs that terminalize directly
  from `Running` without a `Verifying` stop, the first terminal transition *is*
  the end of execution and `FinishedAt` equals executor completion there.
  Executor completion as such is evidenced by the `phase.transition`
  log event entering `Verifying` (§3.8) and by §3.4's shipped
  `courier_coderun_phase_transitions_total` /
  `courier_coderun_phase_duration_seconds` — the `Running` phase's duration is
  observed on leaving it — not by a reinterpretation of the terminal duration.

  The **`phase` label** of `courier_coderuns_total` is the phase at the
  first terminal transition, **not** the run's final disposition. A run
  that reaches `Verifying`, is observed green, advances to `AwaitingReview`,
  and later resolves to `Done` (#250) records exactly one
  `{phase="AwaitingReview"}` increment and never a `{phase="Done"}`:
  `transitionTerminal`'s `firstTerminal := FinishedAt == nil`
  (`internal/controller/coderun_controller.go:999-1053`) gates the
  `ObserveTerminal` call, and `ObserveTerminal`'s own comment
  (`internal/controller/metrics.go:216-220`) is explicit that a later
  `AwaitingReview → Done` must not count again. The terminal histogram
  keeps the first terminal-phase label as well. Treating the metric as
  "runs by final phase" would invite a dashboard to read `{phase="Done"}`
  as merge throughput — a count the metric never emits. Run-level
  current state per phase is a separate future measurement: a
  current-state per-phase gauge derived from the cluster
  (e.g. `courier_coderuns_by_phase`), or a `courier_coderuns_disposition_total`
  event counter for merge/handoff transitions, neither of which is
  authorized here.
- **External disposition** is what happens after Courier stopped working: CI
  advancing to green, a red check, a human review, a merge, or the source's own
  handling. `Verifying` is a *live* state — Courier owns observation but has no
  disposition yet. External disposition is represented by
  `courier_coderun_verification_outcomes_total` (§3.5) and by the source
  lifecycle report result (§3.6), never by reinterpreting an execution success as
  a merged PR.

A run that reached `Verifying` and is later observed green is `AwaitingReview`,
not `Done`; a `Done` run is one the world confirmed merged/resolved. A run whose
`Verifying` observation is unavailable is `NeedsHuman`, not "in review".

### 4.2 What `Pending` means

`Pending` is a **queue** state: the `CoderRun` exists but the operator has not
admitted it (no `AdmittedAt`). It is neither healthy nor unhealthy by itself.
It has an **age** (`now − metadata.creationTimestamp`) and it competes for lane
capacity. `Pending` becomes actionable only when it is compared against
capacity:

- `Pending` with reservations below concurrency and a healthy source → normal
  transient queuing; the same holds at or above concurrency while the lane
  shows a `Claimed` transition in the window — busy, still transient.
- `Pending` while `courier_coderuns_in_flight == courier_lane_concurrency` →
  every slot is genuinely held; blocked on capacity (an availability fact, not
  a failure).
- `Pending` while the lane is not suspended, a slot is free
  (`courier_coderuns_in_flight < courier_lane_concurrency`) yet it is not
  admitted and admission shows no progress → the wedge of §5, not capacity.
- `Pending` while `courier_lane_suspended == 1` → blocked on a suspended lane.

The view must render `Pending` with its age and the lane state that explains it;
it must never render a stalled `Pending` as idle. The exact display contract is
#288's; this document supplies the semantics and the signals.

### 4.3 Unknown, stale, and absent are not idle

Every signal resolves to one of four observation states, and a view renders them
distinctly:

1. **observed** — a value was measured, including a measured zero.
2. **absent** — the series does not exist (never emitted, or scrape lost).
3. **stale** — a timestamp-bearing signal is older than its freshness window
   while its producer is expected to be live.
4. **unknown** — the producing capability is not established (startup, no
   telemetry, unsupported by this deployment).

A measured zero (`courier_coderuns_in_flight == 0` on a seeded lane) is
*observed zero*; a missing series is *absent*. An empty queue is `observed` with
`successes_total` increasing and `last_success` fresh; a failed discovery is
`observed` with `failures_total` increasing and `last_success` aging. The rule
is absolute: **uncertain evidence is displayed as stale/unknown, never as idle or
healthy.**

### 4.4 Source health states

A source-level health state exists independently of run count. It is derived
from §3.2 and is per binding:

| State | Derivation |
| --- | --- |
| `disabled` | the adapter is not configured for this deployment (a configuration fact, not a signal). |
| `starting` | enabled, no successful poll yet (`last_success` absent) and within the startup grace. |
| `ok` | the last poll succeeded (`failures_total` flat since the last success; `last_success` fresh). **Zero items is still `ok`.** |
| `degraded` | recent failures but a success within the evaluation window, or a partial result (`skipped_total` rising) without total failure. |
| `unavailable` | enabled, not paused, and no success within the evaluation window. |
| `unknown` | telemetry absent for a source the view believes is enabled (e.g. metrics not scraped), or a restart before the first poll. |

Pausing is **orthogonal**: a binding paused for `lane-suspended` or
`lane-at-capacity` is not a source failure, so its health is reported alongside
the pause reason and excluded from failure conditions. This is how the design
separates "no capacity" from "broken source".

The view enumerates bindings from the same deployment configuration that starts
the runners (the chart's source/binding values), so the enabled/disabled boundary
is configuration, not a signal. A configured binding that emits no
`courier_source_*` series is `unknown`; a source absent from the configuration is
simply not rendered, never shown as idle.

### 4.5 Lane health states

| State | Derivation |
| --- | --- |
| `open` | not suspended and reservations below concurrency. |
| `saturated` | reservations meet or exceed concurrency. Normal, informational; `stalled` outranks it when that row's gates hold. |
| `suspended` | `courier_lane_suspended == 1`. |
| `stalled` | not suspended, capacity free but unused (`in_flight < concurrency`), at least one reservation unadmitted (`reservations > in_flight`), no `Claimed` transition for the lane in the window, and the oldest reservation older than the reservation-freshness window (§5). The discriminator is `in_flight` vs `concurrency`, not `reservations` vs `concurrency`, so the fully-reserved unadmitted wedge — which the capacity test classifies `saturated` — is still diagnosable. |
| `unknown` | lane signals absent. |

Saturation alone never makes a lane `stalled`: a busy lane's reservations age
naturally and work waiting while every slot is genuinely held is an
availability fact, not a failure (§4.2). Only *free* capacity paired with an
unadmitted reservation that makes no admission progress is the wedge (§5).

## 5. Health conditions and evidence

Each condition is a derived predicate over §3, not a raw signal. Thresholds and
evaluation windows below are **design candidates**, not production defaults;
`T_poll` is the discovery poll interval (`defaultPollInterval = 30s`,
`internal/source/runner.go:30`). Every condition has an explicit behaviour when
its evidence is missing — a condition with no evidence is `unknown`, never
satisfied.

| Condition | True when | Evidence | Candidate window | Missing evidence |
| --- | --- | --- | --- | --- |
| **SourceDiscoveryFailing** | enabled source, not paused, failures rising and no success in window | `discovery_failures_total`, `discovery_successes_total`, `last_success`, and the `discovery_paused` gauge reading 0 for every `reason` | 3·`T_poll` | `unknown` if discovery signals absent |
| **SourceDiscoveryStalled** | enabled, not paused, attempts stopped (no attempt in window) | `last_attempt`, and the `discovery_paused` gauge reading 0 for every `reason` | 3·`T_poll` | `unknown` |
| **SourceNeverSucceeded** | enabled, `starting` persists past grace | `last_success` absent, process uptime | 10·`T_poll` | `unknown` |
| **LaneSaturated** | reservations ≥ concurrency, sustained | `lane_reservations`, `lane_concurrency` | 5 min | `unknown` if `lane_concurrency` absent |
| **StaleReservation** | not suspended; capacity free but unused (`in_flight < concurrency`); at least one reservation unadmitted (`reservations > in_flight`); no `Claimed` transition for the lane in the window; oldest reservation older than window | `lane_reservations`, `coderuns_in_flight`, `lane_concurrency`, `coderun_phase_transitions_total{to="Claimed"}`, `lane_oldest_reservation_age_seconds`, `coderuns_pending`, `lane_suspended` | 15 min | `unknown` if the reservation or progress evidence is absent |
| **LifecycleReportFailing** | terminal runs with `LifecycleReported=False` older than cadence + headroom | `coderuns_lifecycle_pending`, `..._oldest_age_seconds` | retry cadence (30 s) + 5 min headroom | `unknown` |
| **RunFailureRateHigh** | terminal failure ratio over window above threshold with an absolute floor | `coderuns_total{phase}` | 1 h, floor of ≥ 5 failures | no ratio ⇒ no fire |
| **MetricsScrapeFailing** | `metrics_scrape_errors_total` increasing | `metrics_scrape_errors_total` | 10 min | `unknown` if absent |
| **ControllerDown** | no scrape at all | backend-provided target health (e.g. Prometheus `up`); see §11 | 5 min | absence is the signal |

Two conditions deserve emphasis because the issue names them:

- **Discovery failure while enabled with capacity.** `SourceDiscoveryFailing`
  requires all three of: the source is enabled, it is *not* paused
  (`discovery_paused` all zero), and it has capacity (not `lane-suspended`, not
  `lane-at-capacity`). If the binding is paused for capacity, the same failure
  counters are visible but the condition does not fire: the operator is
  deliberately not calling the source.
- **Stale reservations.** `StaleReservation` is the admission wedge, not the
  age of a busy lane. It requires all of: the lane is not suspended; capacity
  is *free but unused* (`in_flight < concurrency`); at least one reservation is
  unadmitted (`reservations > in_flight` — reservations span empty-phase +
  `Pending` + `Claimed` + `Running` while in-flight counts only
  `Claimed`+`Running` (§3.3), so the difference is exactly the
  materialized-but-unadmitted work); admission is not progressing (no
  `Claimed` transition for the lane in the window); and the oldest reservation
  exceeds the freshness window. The discriminator is `in_flight` vs
  `concurrency`, **not** `open` (`reservations < concurrency`): the runner
  pauses discovery at `reserved >= capacity`
  (`internal/source/runner.go:125-138`), so the fully-reserved unadmitted
  wedge — say concurrency 1, one `Pending` reservation, nothing in flight —
  reads `saturated` on the capacity test and an open-lane gate would never
  fire on it. Work waiting while every slot is genuinely held
  (`in_flight == concurrency`) stays an availability fact (§4.2), not this
  condition. The `age` gauge measures `metadata.creationTimestamp` and does
  not reset on admission, so recovery runs through the gates (the backlog
  drains, a `Claimed` transition is seen, or the lane suspends), never through
  an age reset. This predicate matches the A3 alert condition #292 settled in
  [docs/observability-dashboards-and-alerts.md](./observability-dashboards-and-alerts.md);
  the two documents must not diverge on it. A stale reservation is a lane
  condition, not a run failure: the run may be perfectly healthy but unable to
  start.
- **Reporting failures.** `LifecycleReportFailing` fires when the unsettled
  backlog exists and its oldest member exceeds the report retry cadence plus
  headroom; a single slow report must not fire it (absolute floor).

Conditions never assert a healthy state from a measured zero. `LaneSaturated` is
informational, not a page; `SourceDiscoveryFailing` is the page-worthy source
condition. The alert **rules** (windows, severities, routing) are #292's; this
document supplies the conditions and the evidence they require.

## 6. Example traces

All traces use one binding: source `github-label`, lane `cloud`,
concurrency 2, poll interval 30 s. Values are illustrative.

### 6.1 Healthy

A normal work day. Every poll succeeds, items arrive, runs flow through. Counters
are cumulative since process start; `offered` and `materialized` are
`courier_source_items_offered_total` and `courier_source_items_materialized_total`.

| Time | attempts | successes | failures | offered | materialized | last_success age | reservations | in_flight | pending |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10:00:00 | 100 | 100 | 0 | 40 | 40 | 8 s | 1 | 1 | 0 |
| 10:00:30 | 101 | 101 | 0 | 41 | 41 | 10 s | 2 | 2 | 0 |
| 10:01:00 | 102 | 102 | 0 | 42 | 42 | 12 s | 2 | 2 | 0 |

State: source `ok`; lane `saturated` (reservations == concurrency) — informational.
Runs progress `Pending`→`Claimed`→`Running`→`Verifying`→`AwaitingReview`;
`verification_outcomes_total{result="green"}` advances as CI settles.

### 6.2 Idle (empty queue, healthy)

The source is reachable and returns nothing. This is the case a run-centric view
cannot see.

| Time | attempts | successes | failures | offered | materialized | last_success age | reservations | in_flight |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11:00:00 | 200 | 200 | 0 | 42 | 42 | 9 s | 0 | 0 |
| 11:10:00 | 220 | 220 | 0 | 42 | 42 | 11 s | 0 | 0 |
| 11:30:00 | 260 | 260 | 0 | 42 | 42 | 7 s | 0 | 0 |

State: source `ok`; lane `open`. `successes_total` still rises and
`last_success` stays fresh, which is exactly what distinguishes idle from
degraded. No condition fires. A view that keyed on "no new runs" would wrongly
call this unhealthy; keying on discovery success does not.

### 6.3 Degraded (discovery failing while enabled with capacity)

The source becomes unreachable at 12:00. The lane is open and not suspended.

| Time | attempts | successes | failures | last_success age | paused | source state |
| --- | --- | --- | --- | --- | --- | --- |
| 11:59:30 | 239 | 239 | 0 | 10 s | none | `ok` |
| 12:01:00 | 242 | 239 | 3 | 100 s | none | `degraded` |
| 12:05:00 | 250 | 239 | 11 | 340 s | none | `unavailable` |

`SourceDiscoveryFailing` fires once `last_success` exceeds 3·`T_poll` (90 s) with
capacity available. The failure **cause** is the connector's diagnostic (#291):
`connector.diagnostic{connector="github-label", signal="protocol-error",
reason="transport-error"}`, joined to the core counter, never added to it.

Contrast, same counters, different meaning: if the lane were suspended at 12:00,
`discovery_paused{reason="lane-suspended"}` would be 1 and attempts would stop
rising. `SourceDiscoveryFailing` must **not** fire — the operator is deliberately
not calling the source. If instead attempts stopped while paused was 0, that is
`SourceDiscoveryStalled` (a wedged runner), a different condition with a
different remedy.

### 6.4 Stale reservation (admission wedge, distinct from saturation)

Concurrency is 2. The first rows are a **busy, healthy** lane whose
reservations are all admitted; the last row is the wedge. Saturation alone is
identical in kind — the discriminator is unadmitted work paired with free
capacity and stalled admission.

| Time | reservations | in_flight | pending | oldest_reservation_age | `Claimed` transitions (15 min) | capacity test | lane state |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 13:00:00 | 2 | 2 | 0 | 300 s | yes | `saturated` | `saturated` |
| 13:10:00 | 2 | 2 | 0 | 940 s | yes | `saturated` | `saturated` |
| 13:20:00 | 2 | 1 | 1 | 1240 s | none | `saturated` | `stalled` |

Age alone would have fired at 13:10 against a lane whose every reservation is
a progressing `Claimed`/`Running` run — nothing is unadmitted, so the
condition stays false however old those reservations get. At 13:20 a slot is
free (`in_flight 1 < concurrency 2`) yet the `Pending` reservation behind it
(`reservations 2 > in_flight 1`) has not advanced to `Claimed` in the window,
and the oldest reservation exceeds it — the age gate is a lane-level floor
that does not isolate the unadmitted subset (§3.3); the free-capacity and
unadmitted gates carry the precision. `StaleReservation` fires even though
the capacity test still reads `saturated`. What the lane cannot explain as
sizing is the wedge itself — wedged admission, an unschedulable launch, a
reservation parked in the empty phase — and the condition reports the
observable fact, leaving the mechanism to diagnosis. The source is `ok` (its
discovery is paused `lane-at-capacity`, not failing); the run is not `Failed`;
the remedy is capacity or scheduling, not source repair. Recovery clears
through the gates — the backlog drains as the reservation is claimed and the
`Claimed` transition resumes — never through an age reset.

## 7. Privacy and cardinality budget

### 7.1 Allowed labels

Only the closed vocabularies in §3.1 may appear as label values: the configured
`source` identity and `lane`, `mode`, `phase`/`from`/`to`, terminal `category`,
verification/lifecycle `result`, and each signal's closed `reason` enum.

### 7.2 Forbidden labels

Never a label, at any layer: run id or `run_id`, work-item id, issue or PR
number, repository name, branch, head SHA, PR URL, model name, agent name,
free-form error string, request/response body, or any secret. Correlation to a
specific item is done in the **structured log** via `run_id`
(`internal/log/events.go:143-175`) and joined at view time, never promoted to a
metric dimension. This preserves the existing budget
(`DESIGN.md:1070-1075`) and the redaction guarantee
(`internal/log/redact.go`).

### 7.3 Budget

Series are bounded by configuration, not by traffic or history:

- discovery signals: `O(S · L)` where `S` = configured sources, `L` = lanes;
- lane gauges: `O(L)`;
- run counters/histograms: `O(L · M · |phase|)`, `M = 2`;
- terminal diagnostics: `O(L · M · |category|)`, `|category| = 8`;
- phase transitions: `O(L · M · |phase|²)` — the dominant term. With
  `|phase| = 10` this is `200·L` series; if `L` grows or the term is judged too
  costly, dropping `from` (entering-phase only) reduces it to `20·L`. The
  shipped counter keeps `from`/`to`, so this is its standing cardinality cost.
- lifecycle outcomes: `O(S · L · M · |result|)`; verification outcomes
  `O(L · M · |result|)`;
- retention/reaping: `O(|terminal phase|)` — the retention signals are
  `phase`-only, never `lane` (§3.7).

No series is created per run, per item, or per error string. A working ceiling
of a few thousand active series per deployment is the design target; the exact
cap is an instrumentation decision (§11).

### 7.4 Redaction

Any free-form text that must be emitted (e.g. a diagnostic `detail`) is
debug-gated and redacted by the existing redactor before it reaches stdout; no
free-form text becomes a metric label or a structured field.

## 8. Clocks, units, resets, and telemetry loss

### 8.1 Clocks and timestamps

- Authoritative timestamps are the cluster's: `metadata.creationTimestamp`,
  `AdmittedAt`, `StartedAt`, `FinishedAt` (`api/v1alpha1/coderun_types.go:445-471`),
  condition `lastTransitionTime`, and `Heartbeat.At`. Age and duration windows
  are computed from these against the API server's clock as observed by the
  operator, using the injectable `CoderRunReconciler.Now`
  (`internal/controller/coderun_controller.go:148-150`) and `(*CoderRunReconciler).clock()`
  (`internal/controller/reap.go:227-232`).
- `*_timestamp_seconds` gauges are emitted from the operator clock and are used
  for **freshness only**, never for authoritative durations.
- Negative durations from clock skew clamp to zero, matching existing helpers
  (`nonNegativeSeconds`, `internal/controller/metrics.go:373-381`;
  `durationString`, `internal/controller/coderun_controller.go:1664-1671`).

### 8.2 Units

Seconds (Prometheus base unit) for all time signals; bare counts for counters;
0/1 for state gauges. Status keeps Go duration strings and integer milliseconds
so the CRD stays float-free (the "Per-run telemetry (#172)" section of
[DESIGN.md](../DESIGN.md)).

### 8.3 Reset behaviour

- Counters reset to 0 on operator restart; views use `rate`/`increase`, never raw
  values, and treat a reset as a step.
- Cluster-derived gauges (`in_flight`, `reservations`, `pending`, `suspended`,
  `concurrency`, retention gauges) are read live at scrape and survive restart,
  so they always match the world.
- Timestamp gauges and process-start histograms reset; after a restart a source
  is `starting` until its first successful poll, and the health model must not
  read the reset as staleness.

### 8.4 Telemetry loss

- A failed scrape makes every series absent for that scrape. Views render
  `unknown`, not zero, and conditions requiring evidence do not fire.
- Missing `last_success` is `starting`/`unknown`, never idle.
- Missing `status.telemetry` on a run is unknown token usage, not zero.
- Loss of structured logs removes correlation only; metric-derived health still
  works. Loss of metrics removes metric-derived health; log-derived correlation
  still works. Neither is a prerequisite for the other.

### 8.5 No traffic, disabled sources, startup

- **No traffic:** discovery succeeds with zero items → `ok`, idle; conditions do
  not fire.
- **Disabled source:** a configuration fact; the source is excluded from
  discovery-failure conditions and rendered `disabled`, not `unknown`.
- **Startup:** `starting` until the first success; conditions are suppressed for
  a grace period. A source that has *never* succeeded is a deployment problem,
  not a false positive.

## 9. Alternate source adapter without Dispatch

A label-driven forge connector (`github-label` in the "Sources" section of
[DESIGN.md](../DESIGN.md)) uses the same core layer with no Dispatch concepts:

| Capability | Behaviour under this design |
| --- | --- |
| discovery | emits `courier_source_discovery_*` from its own forge client; failure causes are its own `connector.diagnostic` reasons (#291) |
| exclusions | absent — no `courier_source_discovery_skipped` beyond core validation; no exclusion signals required |
| lifecycle report | optional; if unsupported, runs still record their terminal phase and `LifecycleReportSupported=False` is simply not set |
| reservations/capacity | identical core signals; the connector knows nothing about them |
| health | `ok`/`degraded`/`unavailable`/`starting`/`disabled` from the same rules |

The degenerate case is the `manual` adapter (`internal/source/manual/adapter.go`),
which has no external discovery (`Discover` returns `nil, nil`) and is used for
directly-created `CoderRuns`: it emits no `courier_source_*` discovery signals,
so at the source layer it is not a discovery source at all, while the run-,
lane-, and retention-level signals still apply unchanged. Nothing in the health
model requires a queue, a PR-fix generation, or a forge.

## 10. Composition with #291 and #292

- **#291 (connector diagnostics).** Core owns the source-neutral failure **rate**
  (`courier_source_discovery_failures_total`); the connector owns the failure
  **cause** (`connector.diagnostic{signal,reason}`). They are joined on the
  shared adapter name and time, never summed (#291 §6.2). The four observation
  states in §4.3 and #291's `ok`/`unsupported`/`unknown`/`error` are the same
  discipline applied at two layers.
- **#292 (views and alerts).** This document answers the questions #292 recorded
  as open: the discovery signal names and units (§3.2), the state model that
  separates idle from degraded at the source level (§4.4), reservation staleness
  as a first-class condition backed by an age signal (§3.3, §5), and bounded
  per-phase and token metrics (§3.4, §3.5). The alert catalogue itself — windows,
  severities, routing — remains #292's, consuming these signals and conditions.

## 11. Explicit unknowns

These are recorded so a later instrumentation ticket can settle them; none is
authorized here:

- The exact bounded `reason` vocabularies and cardinality caps (working figure:
  ≤ 16 values per signal, every unrecognised value mapped to `other`).
- How "enabled/disabled" is declared to a view: a configuration source, or a
  startup info metric.
- Whether a source's primary freshness signal is the `last_success` timestamp
  gauge or an info-state gauge.
- The backend target-health signal `ControllerDown` depends on (for example
  Prometheus `up`) — the only backend-specific item, recorded here so no backend
  is hardcoded into core.
- The exact thresholds and evaluation windows in §5 — all candidates.
- How #290 source states and #291 connector states compose in #292's views
  without double counting, beyond the join-not-sum rule.

Settled by the shipped run-signal exporter (#324), and no longer open: the
phase-transition counter carries `from`/`to`, the phase-duration histogram and
the token counters are exported, external verification disposition is a counter,
and the lifecycle-report outcome counter carries a `source` label.

## 12. Non-goals

No code, exporter, dashboard, alert rule, deployment manifest, or
environment-specific configuration. No change to lifecycle behaviour or
retention policy. #286 (exclusion overflow) and #288 (`Pending` display)
remain separate behaviour work and are not authorized by this document; #287
(per-phase retention) has landed independently and is read here as existing
behaviour, not authorized by this document.
