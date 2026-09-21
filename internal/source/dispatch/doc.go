// Package dispatch is Courier's first-party source adapter for Dispatch.
//
// Dispatch's HTTP transport remains behind the injected Client boundary so the
// source-agnostic package does not depend on Dispatch request or response types.
// The boundary covers discovery, claim/unclaim, ordinary issue status, the
// optional pre-launch stale-work check, and the task report used for lifecycle
// audit. The HTTP client implements the
// live Dispatch contract without exposing its wire details to the core.
package dispatch
