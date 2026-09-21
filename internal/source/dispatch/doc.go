// Package dispatch is Courier's first-party source adapter for Dispatch.
//
// Dispatch's HTTP transport remains behind the injected Client boundary so the
// source-agnostic package does not depend on Dispatch request or response types.
// Implement tasks use Dispatch issue claim, release, status, and done endpoints.
// Followup-pr tasks treat any linked issue as context only: their lifecycle uses
// tasks/report and, when applicable, the PR-fix queue, never issue endpoints.
//
// Followup discovery treats both an upstream merged PR and a closed-unmerged PR
// as terminal stale work. The stale queue notes deliberately distinguish those
// states so an operator can tell why Dispatch declined the task.
package dispatch
