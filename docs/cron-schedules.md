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
    issue: 15            # positive issue number the run addresses
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
day-of-week), parsed by `github.com/hashicorp/cronexpr`. The expression is
evaluated in the configured IANA location (`timezone`), defaulting to UTC:
wall-clock boundaries follow local time, including DST transitions. A
boundary that falls on a nonexistent local time (a spring-forward gap) is
skipped by the scheduler — no run is created for it, and the next boundary
is the next real occurrence.

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

Occurrences are materialized regardless of any still-active prior run.
Deduplication is per occurrence identity only; the lane's concurrency limit
serializes execution and Pending runs queue behind it. Operators pace work
through the schedule — pick periods longer than a typical run duration.

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
every other connector capability is explicitly unsupported. With more than
one lane in the schedule file, each lane's adapter reports under
`cron:<lane>` so independent adapters never share metric series.
