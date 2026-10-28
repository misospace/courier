# Cron schedules

The cron source adapter (`internal/source/cron`) materializes scheduled work
as ordinary `CoderRun`s through the same source-neutral run path as every
other adapter. It is the successor to ad-hoc coding cronjobs, not a second
scheduler inside the coordinator: all scheduling and source-specific logic
stays in the adapter, and the core `CoderRun` lifecycle is unchanged
(issue #15).

## Configuration surface

One manager flag points at a YAML file of scheduled jobs; the Helm chart
renders `cron.schedules` values into a release ConfigMap mounted at
`/etc/courier/cron-schedules/schedules.yaml` and passes the flag. An
explicit `--cron-schedules-file` arg under
`controllers.main.containers.main.args` takes precedence and disables the
chart transport. An empty or unset file disables the cron source entirely;
other sources are unaffected.

```yaml
schedules:
  - name: nightly-triage
    repo: misospace/courier
    issue: 15            # issue or pull request number the run addresses
    lane: frontier       # LaneProfile assigned to materialized runs
    schedule: "30 2 * * *"
    timezone: America/New_York   # optional IANA name; default UTC
    mode: resolve-issue  # optional; resolve-issue | fix-pr; default resolve-issue
    debug: false         # optional
```

Every field is validated when the file is loaded at manager startup. Any
invalid entry — an unparseable cron expression, an unknown timezone, a
missing or malformed field, a duplicate job name — fails startup with an
error naming the job and field, exactly like the bootstrap LaneProfiles
transport. There is no per-entry soft-failure at discovery time.

## Schedule syntax and timezone semantics

Standard five-field cron expressions (minute hour day-of-month month
day-of-week), parsed by `github.com/hashicorp/cronexpr`. Wider cronexpr
syntax — six or seven fields with an explicit year, or `@descriptor` forms
— is rejected: a year-pinned expression exhausts its match list, and the
adapter's recurring cursor must never see a nonexistent boundary. The
expression is evaluated in the configured IANA location (`timezone`),
defaulting to UTC: wall-clock boundaries follow local time, including DST
transitions. A boundary that falls on a nonexistent local time (a
spring-forward gap) is skipped by the scheduler — no run is created for it,
and the next boundary is the next real occurrence. An expression that can
never fire is rejected at load.

## Occurrence identity

The `WorkItemID` of one occurrence is `<jobName>@<unix-seconds-of-scheduled-
boundary>`. The boundary is the scheduled fire instant in the job's
timezone, never the poll time, so identity is stable across repeated polls,
manager restarts, and replicas, while future occurrences remain distinct.
The materialized run's deterministic name derives from this identity, so a
duplicate Create is an idempotent `AlreadyExists`.

## Missed occurrences

No catch-up. Each job keeps an in-process cursor that starts at the first
boundary strictly after manager startup; occurrences that fired while the
process was down are skipped (never caught up), and a single poll gap that
spans several boundaries offers only the latest one. The cursor never
rewinds. This is deliberate: the runner's identity barrier disappears when
a completed run is reaped, so any catch-up rule could re-materialize an
already-completed occurrence after retention cleanup; and a stale burst of
catch-up coding runs is rarely what an operator wants. An operator who
wants the next occurrence simply lets the schedule fire again.

## Overlap policy

Deduplication is per occurrence identity only; the adapter keeps no
overlap state. The lane's concurrency limit serializes execution: while a
prior run holds the lane's capacity, source discovery pauses entirely, so
the next occurrence is not materialized as a Pending run behind it — no
CoderRun exists yet. When capacity returns, the latest due occurrence
materializes and any boundary that passed while discovery was paused is
skipped by the same latest-only rule as any other gap. At most one
occurrence per job is ever outstanding, and operators pace work through
the schedule — pick periods longer than a typical run duration.

## Delivery shape

Together with the no-catch-up rule, delivery of one occurrence is
at-most-once: the cursor advances when the occurrence is offered to the
runner, so a materialization that fails after the offer (a Create error, a
shutdown between offer and Create) is not retried — the next boundary is
the recovery point. This is the deliberate trade for never
re-materializing a completed occurrence after retention cleanup.

## Clock and restart behavior

No duplicate run after a manager restart or clock jitter:

- identity is the scheduled boundary, so jitter and poll timing cannot
  change it;
- a restarted manager resets its cursor past every boundary before startup,
  so it never re-offers an occurrence;
- within a process lifetime the cursor deduplicates, and across replicas
  the deterministic run name plus the runner's identity barrier make
  duplicate materialization idempotent.

## Capability surface

Like the manual adapter, the cron adapter has no external lifecycle to
mutate: discovery and availability are its only supported capabilities, and
every other connector capability is explicitly unsupported.

With one or more lanes in the schedule file, each lane gets its own adapter
and discovery runner. Every adapter reports under the connector label
`cron:<lane>`, every run is materialized with `spec.source` `cron` and
`spec.sourceAgent` `<lane>`, and the reconciler resolves lifecycle calls
through the same `cron:<lane>` registry entry. The mapping is stable when
lanes are added or removed, so retained runs never lose their adapter.
