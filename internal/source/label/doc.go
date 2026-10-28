// Package label implements the forge label source adapter (#16): an issue
// carrying the configured trigger label becomes a resolve-issue run, and a
// pull request carrying the configured fix label becomes a fix-pr run.
//
// Every forge read and write routes through the forge-agnostic forge.Provider
// seam — discovery lists through forge.LabeledWorkItemLister and settlement
// removes the trigger label through forge.WorkItemLabelMutator — so the core
// never touches a forge-specific client or CLI.
//
// The label state is the source truth. There is no claim endpoint to race:
// the runner's source-plus-work-identity dedupe is the claim, so Claim,
// Release, and Transition are deliberate no-ops. Resolve is the one write the
// adapter makes: a settled run removes its trigger (or fix) label so the same
// work is not rediscovered after retention expires. The removal is idempotent
// by the mutator contract, so a retried settlement never fabricates a
// failure.
//
// Known v1 boundaries, kept explicit rather than papered over:
//
//   - Runs ending NeedsHuman or Failed leave the label in place — the source
//     has no park vocabulary — so after that run's retention expires the work
//     is re-offered as a fresh run. Operators retire such work by removing
//     the label; a parking policy needs a forge-side vocabulary this contract
//     does not have yet.
//   - A durable label-removal refusal (for example an authorization failure)
//     keeps a settled Done run retrying instead of reaping: the controller
//     treats every Resolve error as retryable and the mutator contract does
//     not carry a typed permanent refusal.
//   - The provider is built against the forge's default endpoint with the
//     operator's own credential; per-repository routing through the secure
//     provider registry is the observer's read-side infrastructure and is not
//     wired for discovery here yet.
package label
