// Package cron implements the source lifecycle for CoderRuns materialized
// from declared schedules. Every scheduled job is ordinary source work: the
// adapter offers one WorkItem per due occurrence through the source-neutral
// run path, and all scheduling and source-specific logic stays in this
// package — core CoderRun lifecycle is unchanged.
//
// The settled semantics (issue #15) are documented in
// docs/cron-schedules.md: no catch-up of occurrences missed while the
// process was down, deterministic occurrence identity, and no adapter-side
// overlap policy — the lane's concurrency limit serializes execution.
package cron
