# Explicit retry of terminal CoderRuns

Contracts for an explicit, operator-authorized retry of a terminal `Failed` or
`NeedsHuman` CoderRun (#97). The retry is a **semantic, source-aware,
idempotent requeue**, not deletion, not a phase flip, and never automatic.
After an operator fixes an external blocker, this is the supported way to let
the same work run again on an available lane while the historical attempt
stays inspectable.

The operator UI (#510) later exposes this contract as one action; the UI is
never the source of truth for retry semantics. The production symptom where
manual pruning is required merely to unblock lane progress is bug #509 and is
not solved here.

## What a retry is not

- **Not automatic.** Nothing in Courier requests a retry on its own. Retention
  (#276/#287), the capacity-exhaustion recovery channel (#327/#328), and the
  source's own re-offer semantics are unchanged and remain the only automatic
  paths. Capacity-exhausted runs settle through their existing fenced channel
  and never need this one.
- **Not deletion.** The predecessor run is never deleted, reaped early, or
  phase-flipped. Its phase, terminal diagnostic, evidence, status, and source
  report remain exactly what terminalization wrote. The `BlockedExternal`
  sweep precedent (manual pruning) is the anti-pattern this contract
  replaces.
- **Not recovery of the old checkout.** A retry is a fresh attempt with a
  fresh source identity. It never adopts, restores, publishes, or implies
  publication of predecessor evidence (#115: retrieval and application are
  human actions; uncommitted work dies with the pod; a fresh retry is not
  recovery of the old checkout). Pushed work on the adopted branch is in the
  world and is reused by branch adoption, not by evidence.
- **Not a bypass of source state.** Source state is requeued through the
  source adapter's fenced capability. Courier core never clears Kubernetes
  state to make work discoverable, never emulates fencing with ordinary
  Claim/Release/Transition calls, and never invents a generation or attempt
  identity.

## Request surface: the retry annotation

Human ingress is the CoderRun annotation
`courier.misospace.dev/retry`; its non-empty, bounded value is an opaque
request ID. This mirrors the settled soft-stop ingress (#238): the
spec is immutable and source-owned, status is controller-owned, and the
annotation is the established operator channel that Kubernetes RBAC already
gates. Automation (a CLI, or the #510 action API) requests a retry by
annotating the CR; no special controller bypass and no direct status or spec
mutation exists.

The controller alone translates the annotation into the operator-owned
durable record `status.retryRequest`. An annotation on a non-terminal run
consumes nothing: the controller records a refusal (below) and leaves the run
untouched. A changed annotation value is a new request; re-requests are
always explicit new values.

The distinct annotation `courier.misospace.dev/retry-lifecycle-report` keeps
its separate meaning: it re-publishes a pending lifecycle report on the
unsupported-outcome path. It does not requeue work and this contract does not
change it.

## Durable record and state machine

`status.retryRequest` is the single durable record of the retry:

```yaml
status:
  retryRequest:
    id: <annotation value, verbatim>   # opaque request ID, bounded 128 chars
    requestedAt: <ts>                  # controller-stamped, controller clock
    state: Requested | Requeued | Moot | Refused
    reason: <bounded fixed refusal/moot reason vocabulary>
    attemptID: <fresh source attempt identity the adapter returned, when any>
    settledAt: <ts>                    # when the record reached a settled state
```

- **Requested** — durable intent, persisted *before* the source call
  (persist-before-dispatch, the same discipline as the CI-park close
  #534). A crash before the source call loses nothing; the record replays.
- **Requeued** — the adapter confirmed the fenced source transition. Settled.
- **Moot** — a classified refusal proving the transition already happened
  without this record's key: the work was already requeued or superseded
  (the `ErrStaleWork`/`ErrSuperseded` family, `fence-settled`/`fence-gone`
  semantics from the park vocabulary). Settled; no replacement is launched
  from this record. Replaying a key that *did* commit under this request
  returns the original success and settles `Requeued`, never `Moot` (see
  Eligibility and refusal ownership, "The two lost-response outcomes").
- **Refused** — the request can never succeed for this predecessor. Settled.
  Reasons are a closed, bounded vocabulary: `not-retryable-phase`,
  `automatic-recovery-channel`, `active-owner-exists`, `already-requeued`,
  `upstream-closed`, `unsupported-source`, `stale-generation`,
  `source-refused`, `held-for-human`.

One condition, `RetryRequest`, mirrors the record for observability:
`Unknown/Requested`, `True/Requeued`, `True/Moot`, `False/Refused` with the
same reason in the condition. Every state transition emits a structured
`run.retry` event carrying the request ID and the settled reason. Structured
events and the successor run's spec are the audit trail; the record is not a
changelog.

**Record lifecycle.** `Requested` is the only mutable state. A settled record
is immutable except as follows: a *new annotation value* on a predecessor
whose record settled `Refused` or `Moot` may open a fresh request (the
operator fixed the blocker and asks again); the old record's outcome remains
in events. A predecessor whose record settled `Requeued` never opens another
request: exactly one replacement attempt is launched per predecessor
lifetime (below). A later annotation on such a run is refused
`already-requeued` and recorded in events only.

A changed or removed annotation never cancels a `Requested` record: the
intent may already have committed source-side, so the controller first
resolves the outstanding request by replaying its idempotency key. Only once
the old record settles does the next request open — and if that replay
settles `Requeued`, the new request refuses `already-requeued` like any
other. An operator who needs to stop a pending retry waits for it to settle;
there is no path that abandons a possibly-committed source transition.

## Eligibility and refusal ownership

Core and adapter refuse different things; neither can do the other's job.

**Core refuses (structural, source-agnostic, decided before any source call):**

- Phase not in `Failed` or `NeedsHuman` → `not-retryable-phase`.
  `BlockedExternal` refuses with the dedicated reason
  `automatic-recovery-channel`: it is recoverable by design and its source
  re-issues the work without a retry request. `AwaitingReview`, `HandedOff`,
  `Verifying`, and `Done` are not terminal failures; the source's own repair
  and review paths own them.
- The record already settled `Requeued` → `already-requeued`.
- Another run for the same work is still active. Core matches on
  `spec.Source` (qualified identity included) plus `spec.Repo` + `spec.Ref`
  — the source-granular work identity — across all runs in the namespace,
  refusing when any such run is in `Pending`, `Claimed`, `Running`, or
  `Verifying` → `active-owner-exists`. This is the "never bypass active
  ownership" invariant checked against the world, independent of what the
  source reports.
- The adapter does not implement the requeue capability →
  `unsupported-source`. This is permanent for the adapter; the core never
  falls back to ordinary release/requeue calls to imitate fencing (the same
  rule the capacity-recovery boundary settled).

**Core additionally refuses, for `fix-pr` only (forge guard):** the run's
recorded pull request identity (`status.pr`) is read **from `spec.repo`** —
the repository the run was created against — through the source-neutral
forge `read-pull-request` capability, and the returned head repository is
then verified against the recorded `status.headRepo` (an empty
`status.headRepo` means a same-repository head). This is exactly how the
merge observation reads a PR; the PR number is only meaningful within
`spec.repo`'s numbering space, so reading from `status.headRepo` itself
could inspect an unrelated PR. The guard refuses when that request reports
merged or closed without merge → `upstream-closed`, permanent for this
predecessor. A merged or closed upstream request must never be retried;
this is the forge-truth check, not a source queue check, so it belongs to
core where the capability is available. A transient or unreadable forge
read proves nothing: it does not consume the request, no record is written
yet, and the reconcile retries validation later. An unreadable forge never
fails open into a requeue.

**The adapter refuses (source-state grounds, after `Requested` is durable):**

- The item's attempt/generation is no longer the one this run owned
  (a newer generation exists, or the source no longer records this claim) →
  `stale-generation`.
- The item is under an explicit human hold the retry is not authorized to
  lift, or the source's own state contradicts a requeue (unknown ownership,
  item gone) → `held-for-human` / `source-refused`.
- The upstream source-side state (issue closed, queue item closed) refuses →
  `source-refused`.

Adapter refusals settle the record `Refused` with the mapped reason, except
the moot family (already requeued / superseded), which settles `Moot`. The
two lost-response outcomes are distinct and must not be conflated:

- If **this request's idempotency key committed the source transition** and
  only the adapter's response or the controller's status write was lost,
  replay returns the committed success — the original
  `AttemptRequeueResult` — and the record settles `Requeued`. This is the
  outcome that releases the discovery barrier and lets `settledPredecessor`
  link the fresh attempt to this predecessor; it is the only deterministic
  answer to "did this request take effect?" A committed key is never
  reclassified moot on replay.
- `Moot` is reserved for evidence that the work moved **without this
  request's key**: the source holds no committed transition under this key,
  but its state shows the item already requeued or superseded by some other
  request or path. It is a classified refusal of a fresh presentation, not
  a replayed result, and it never releases the barrier.

A source that cannot distinguish "committed under this key" from "moved by
something else" must not implement the capability: replaying a committed
success as a moot classification would strand a transition that needs its
`Requeued` settlement and suppress its replacement.

## Source requeue capability

A new optional source capability, shaped after the fenced capacity-settlement
boundary (#327/#328) rather than the ordinary lifecycle:

```go
// AttemptRequeueRequest carries the fenced identity of exactly one retry.
type AttemptRequeueRequest struct {
    // FenceToken is the source-issued receipt fencing this attempt
    // (the run's SourceReceipt). A source with real fencing rejects a
    // token whose claim it no longer records.
    FenceToken string
    // IdempotencyKey identifies this retry so a retried call cannot
    // requeue the attempt twice and a lost response can replay. It is
    // derived from the immutable run incarnation and the request ID,
    // never from model, worker, or lane names.
    IdempotencyKey string
    // Reason is the bounded operator note; untrusted data, never executed.
    Reason string
}

// AttemptRequeueResult carries the fresh attempt identity the source will
// re-offer, when the source can promise one. Empty means the source requeued
// without exposing the new identity; discovery observes it later.
type AttemptRequeueResult struct {
    AttemptID string
}

// AttemptRequeuer is the optional fenced requeue boundary for an explicit
// operator retry (#97). In one atomic source-side transition it stops the
// named attempt from owning the work, releases any human hold this explicit
// request is authorized to lift, and makes the work discoverable again for a
// compatible worker — without consuming task-repair budget, without
// resolving anything, and without touching any other attempt. A committed
// key MUST replay its original result without mutating a newer owner.
type AttemptRequeuer interface {
    RequeueAttempt(context.Context, WorkItem, AttemptRequeueRequest) (AttemptRequeueResult, error)
}
```

Semantics mirror `CapacityRecovery` exactly where they should:

- The adapter implements the interface only when its source can fence
  atomically. Adapters whose source cannot must not implement it and must
  never emulate it with ordinary calls; the core treats a missing
  implementation as `unsupported-source`.
- The idempotency key makes retries of the same request collapse into one
  source-side transition, and a committed key replays its original result —
  the same contract Dispatch's capacity endpoints follow.
- The refusal vocabulary is 409-coded in the Dispatch connector's style;
  a bare 404 on the requeue route is permanent version-skew evidence and
  latches the same way the CI-park family latches (`ErrUnsupportedRetry`
  for the process lifetime).

This is the answer to "how #510 authenticates/invokes the same operation":
#510 annotates the CR (or a later server-side action endpoint that
annotates); the controller is the only caller of the fenced capability, and
the UI gains no bypass. Retry semantics are the controller's, and the UI
action is one more automation client with ordinary RBAC.

## Replacement identity, discovery, and successor naming

The replacement attempt is **source-issued**: for Dispatch queue-backed
follow-ups, the requeue transitions the item back to offerable and Dispatch
issues a new generation; the fresh `prFixItem{id,generation}` makes a fresh
opaque `WorkItemID` (the #98 semantics). For issue work, the reactivated
item re-offers under a fresh attempt identity. Core never derives identity
from wall-clock time, PR head, reason strings, or request IDs (the
`#1045` fallback rule stands for adapters without durable attempt identity;
such an adapter simply does not implement the capability).

The discovery barrier gains a second settled-history state — but **not** by
extending the existing one. Today `settledCapacityExhaustion` is the sole
state that releases the identity barrier, and its release is total: the
runner skips a settled-capacity run both in the discovery-exclusion scan
and in the dedupe map (`internal/source/runner.go`), because capacity
recovery must let the *same* identity rematerialize. A retry successor
needs the opposite map behavior, so the two contracts stay distinct:

- A predecessor whose `retryRequest` settles `Requeued` contributes a
  distinct settled-history predicate (`settledRetryRequeue`), which
  participates **only** in predecessor lookup and successor naming — the
  same slot `settledCapacityExhaustion` occupies for capacity recovery,
  never in the dedupe map or the discovery-exclusion scan. A
  retry-settled predecessor is terminal `Failed`/`NeedsHuman`, so the two
  predicates are disjoint on any one run.
- **Predecessor correlation is by recorded attempt identity, not by work
  identity.** The existing `settledPredecessor` lookup compares the
  normalized work identity of the incoming offer against each
  predecessor's `spec.workItemID`; that comparison cannot match a retry
  successor, because the fresh generation's identity differs from the
  predecessor's (Dispatch's `WorkIdentity` keeps the generation:
  `followup-pr/<repo>#<n>/<id>@<gen>`). The retry predicate therefore
  correlates the incoming offer with a predecessor by matching the
  offered fresh identity — normalized through the same `WorkIdentity`
  normalization the dedupe map applies — against the `attemptID` the
  adapter returned and the controller recorded in
  `status.retryRequest.attemptID` when the record settled `Requeued`
  (newest-creation-timestamp tie-break, as today). The matched
  predecessor's UID then names the successor
  `successorRunName(source, freshIdentity, predecessorUID)` — the same
  deterministic predecessor-UID successor naming capacity handoff uses,
  preserving history while keeping creation atomic as the dedupe
  boundary.
- When the source requeued without promising an identity (`attemptID`
  empty — discovery observes it later), no predecessor link is claimed:
  the fresh offer materializes under the ordinary deterministic
  `runName(source, freshIdentity)`, and correlation lives in the
  `run.retry` events. Naming is history preservation, never a correctness
  gate: if the predecessor is reaped before the fresh offer arrives (its
  ordinary window may already have expired), the fresh attempt still
  materializes under the ordinary name with no correctness loss, because
  the fresh identity's dedupe is owned by the successor run once it
  exists.
- The retry predicate deliberately does **not** take the
  `settledCapacityExhaustion` skip branches. The predecessor's *old*
  WorkItemID stays in the dedupe map — that attempt is consumed; a stale
  or duplicate re-offer of the old identity dedupes as before, instead of
  rematerializing the consumed attempt.
- The old WorkItemID also stays in discovery exclusions; exclusions are
  opaque per-attempt IDs and cannot suppress a fresh generation (the #328
  handout-ledger scan already absorbs re-offered truncated-out
  generations).
- `Moot` is deliberately **not** a barrier release: the work moved without
  this record; a successor, if any, arrives through whichever path moved it.

Exactly-one-replacement holds at every layer: the request settles `Requeued`
once (idempotent key), the predecessor requeues once ever, the source offers
the fresh attempt under one identity, and the runner's dedupe map plus
`Create` `AlreadyExists` absorb re-offers. Two concurrent annotations are
two requests; only the first can settle `Requeued`.

## Ordering, atomicity, and crash recovery

The reconcile that observes a new annotation runs:

1. **Resolve any outstanding record first**: a changed annotation over a
   `Requested` record replays the old request's key to settle it before
   anything else (see Record lifecycle).
2. **Validate** (core refusals, the fix-pr forge guard). Transient
   validation failures write nothing and requeue; refusals settle `Refused`.
3. **Persist `Requested`** with the derived idempotency key. From here the
   request is durable intent.
4. **Call the adapter** with the fence and key. Outcomes: success → persist
   `Requeued` (+ returned attempt ID); classified moot → persist `Moot`
   (a moot classification means the source committed nothing under this
   key — the work moved by some other path); permanent refusal → persist
   `Refused`; transient error → leave `Requested`, retry on a later
   reconcile with the same key.
5. **A crash between 4 and the outcome write** is recovered by replay: the
   next reconcile re-presents the same key and the source replays its
   committed transition under that key — the original success result — so
   the record settles `Requeued`, never a moot classification. The status
   write is the proof of delivery, exactly as the park settlement and
   lifecycle-report intents work.

**Retention interplay:** a run carrying a `Requested` retry record is not
reap-eligible, by the same gate that holds a run owing a park close —
deletion would drop the durable intent with the object. The gate is
decided against the live object on every pass, never from cached
eligibility state: a per-phase retention marker may already be set and its
window already expired when the operator annotates, and an expired window
is not standing delete authority. The delete decision must therefore be
made from the same read that issues it, and the mere presence of the
`courier.misospace.dev/retry` annotation — regardless of whether the
record is persisted yet — aborts it; the first reconcile that observes
the annotation runs the record translation (persisting `Requested`, or a
refusal) before the run can be reconsidered for reaping.
controller-runtime serializes
reconciles per object, so a retry reconcile cannot interleave with the
reaper's pass; to bound the remaining read-to-delete window, the reaper
re-verifies the gate on the object state the `Delete` is issued from,
and the annotation event that lands mid-pass wakes the object for the
record translation immediately. The retention window itself never
restarts and never extends: the per-phase window follows its existing
ordinary rules (stamped set-once once the lifecycle report settles, never
derived from `FinishedAt`), and retry adds only the hold — an outstanding
`Requested` record holds deletion, and a settled record never holds the
reaper. Once the record settles, the ordinary rules resume exactly where
they were: a `Refused` or `Moot` settlement returns the run to its
ordinary `Failed`/`NeedsHuman` window and audit floor, which may already
have expired, making the run reap-eligible again at the next pass; a
`Requeued` settlement leaves the predecessor on that same original window,
and if it has already expired the run becomes reap-eligible again too —
losing the predecessor to reaping before its successor materializes costs
only the history-preserving successor name (correlation stays in the
`run.retry` events), never correctness. Retry never deletes, never clears
evidence, and never shortens a window.

**Partial failure** is therefore bounded: before the source call (nothing
sent), after the source call (key replay), or at the source (refusal
vocabulary). No state requires a human to repair; the worst case is a
pending `Requested` record that keeps retrying until it settles, while
retention holds the run until it does.

## `NeedsHuman` and `Failed` share one path

Both terminal phases ride the same annotation, the same validation, the same
capability, and the same successor naming. The terminal diagnostic category
is reported in the retry event for correlation but **never gates
eligibility**: the operator's explicit request is the human authorization
the invariants require, and a category whitelist would only invent a second
opinion about a human decision. Source-state refusals (stale generation,
closed upstream, holds) remain the safety boundary. `infrastructure_failed`
endings that terminalized at the crashloop ceiling are therefore retryable
by the same explicit act once the operator has addressed the underlying
cause — deliberately not automatic.

## Branch, PR, and base semantics

The successor run is a normal fresh run and follows the existing adoption
rules unchanged; retry adds nothing to them:

- **fix-pr** adopts the existing PR's head repository and branch
  (fork-aware), keeping the base repo as the publication target. The
  adopted branch is preserved and never reset or force-pushed because a
  retry occurred; base-sync on adoption applies as always, pinned to the
  request's live base.
- **resolve-issue** adopts an orphaned branch only under the existing rule
  (branch exists, no open PR); a branch with an open PR refuses adoption
  and reports the PR; a branch whose only PRs are closed still refuses and
  hands to a human.
- The predecessor's published work is in the world; the successor builds on
  it through adoption, never through evidence restoration.

## Observability

One bounded counter in the #290 catalogue vocabulary:
`courier_coderun_retry_requests_total{lane,mode,result}` where `result` is
`requested | requeued | moot | refused`, labels bounded to lane, mode, and
result — never repo, issue, or run identity. Per-run correlation stays in
the structured `run.retry` events. The #510 read model projects
`status.retryRequest` and the condition directly.

## Decomposition

Implementation is split into bounded, dependency-ordered, file-scoped issues,
none scheduled `status/ready` until this design lands. Status: the core
request surface and controller state machine of item 1 landed on main via
#613; items 2–4 remain open, and until an adapter implements the
`AttemptRequeuer` capability a retry settles `unsupported-source`.

1. **Core request surface and controller state machine** — API types
   (`api/v1alpha1/coderun_types.go`: annotation constant, `status.retryRequest`,
   `RetryRequest` condition, CRD max-length markers), controller translation,
   validation, idempotency, retention-gate hold, events and metrics
   (`internal/controller/retry.go`, reap gate touch), plus the generated
   CRD/deepcopy output.
2. **Source capability and discovery integration** — `AttemptRequeuer` and
   request/result types in `internal/source/source.go`; the fake's
   implementation; the runner's distinct retry settled-history predicate
   (predecessor lookup and successor naming only, correlated by the
   recorded `attemptID` rather than work identity — the
   `settledCapacityExhaustion` map-skip branches stay capacity-only)
   (`internal/source/runner.go`).
3. **Dispatch adapter transport** — fenced requeue route in
   `internal/source/dispatch` (client + adapter), 404 version-skew latch,
   refusal mapping. Depends on the Dispatch server contract (tracked in the
   Dispatch repository, the #1259 pattern); the adapter fails closed
   `unsupported-source` until the server ships.
4. **Manual adapter path** — the manual adapter implements the capability by
   minting the fresh attempt identity from its own namespace and returning
   it; because the manual source has no source-side flow to re-enter (the
   annotation *is* the source action), the controller materializes the
   successor run directly for manual source with the minted identity. The
   one documented exception to discovery-driven materialization, bounded to
   `source: manual`.

## Test classes

The issue's required test classes map onto the decomposition:

| Class | Where |
| --- | --- |
| Duplicate retry requests | #1 controller unit + envtest: same annotation re-delivered is a no-op; a new value after `Requeued` refuses `already-requeued`; a new value after `Refused` opens a fresh request. |
| Partial failure | #1 + #2 + #3: status write lost after adapter success → key replay settles `Requeued` with the original result, never `Moot`; adapter reports moved-without-this-key (no committed transition under the key) → settles `Moot`; transient adapter error → record stays `Requested` and retries; crash between call and write covered by the replay path. |
| Closed upstream work | #1 forge guard (merged/closed fix-pr PR refuses; unreadable forge does not consume the request) and #3 adapter refusal vocabulary. |
| Historical auditability | #1 + #2: predecessor phase, diagnostic, evidence, and report unchanged across a retry; successor named by predecessor UID; old identity stays deduped. |
| One replacement run only | #2 runner tests (re-offer of the old identity dedupes under a retry-settled predecessor — the capacity map-skip branches stay capacity-only; successor naming; `Moot` keeps the barrier) and #1 (one `Requeued` per predecessor, concurrent requests resolved deterministically). |

Every acceptance scenario from the backlog disposition composes from these:
one existing PR fix, no active writers, retained failure; an explicit retry
on the same PR/branch; exactly one new run; idempotent repeats; old
diagnostics still readable; and refusal on stale-generation,
active-ownership, and closed-upstream cases.
