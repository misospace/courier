// Package dispatch is Courier's first-party source adapter for Dispatch.
//
// Dispatch's transport and wire details are intentionally not specified by
// Courier yet. Client is therefore an injected, fakeable boundary with only
// the operations Courier needs: claim an opaque item ID and publish one of the
// three lifecycle statuses. A production client can add HTTP, authentication,
// retries, and response decoding without changing the source interface.
package dispatch
