package dispatch

import (
	"context"
	"fmt"
	"strings"

	"github.com/misospace/courier/internal/source"
)

// ownershipVerdict evaluates Courier's configured-ownership gate for one
// followup-pr attempt (#761). The verdict is Courier's own and never the
// peer's: the original claimant Dispatch names on the PR-fix item must be
// present in the configured binding map, and that claimant's configured queue
// lane must equal the receiving connector's own queue lane by exact string
// equality. No alias normalization, prefix match, or case folding applies,
// LaneProfile names are never Dispatch queue-lane identities, and ownership is
// never inferred from the handout recipient, the pull-request author, a branch
// prefix, or the fix type.
//
// An empty reason means the attempt is authorized for this connector.
func (c *HTTPClient) ownershipVerdict(claimant string) (reason, why string) {
	claimant = strings.TrimSpace(claimant)
	if claimant == "" {
		return reasonOwnershipEvidenceMissing, "the followup-pr attempt carries no original-claimant evidence"
	}
	boundLane, known := c.ownershipBindings[claimant]
	if !known {
		return reasonOwnershipClaimantUnknown, fmt.Sprintf("original claimant %q is not in Courier's configured dispatch bindings", claimant)
	}
	if boundLane != c.queueLane {
		return reasonOwnershipLaneMismatch, fmt.Sprintf("original claimant %q is bound to queue lane %q, not this connector's queue lane %q", claimant, boundLane, c.queueLane)
	}
	return "", ""
}

// refuseOwnership revalidates one followup-pr attempt's ownership evidence at
// a boundary that can still refuse it, and releases the reservation with the
// exact receipt the attempt carries when the gate does not authorize it.
//
// The attempt descriptor is the durable identity of the queue-backed attempt:
// it carries the PR-fix item id, its generation, the stamped hand-out token,
// and the original claimant the Dispatch evidence supplied, so the verdict
// and the release speak the same receipt the attempt was handed out with. A
// refusal never marks the item stale, never resets another writer's attempt,
// and never leaves the reservation held: the item stays discoverable for the
// lane that owns it.
//
// The refusal is terminal for the attempt on this connector — it carries
// source.ErrStaleWork so the core drops the run instead of re-admitting it
// into the same refusal — while the dispatch-level sentinel
// (ErrOwnershipDeclined) keeps the reason visible to a caller that reads the
// error directly.
func (c *HTTPClient) refuseOwnership(ctx context.Context, d workDescriptor) error {
	reason, why := c.ownershipVerdict(d.OriginalClaimant)
	if reason == "" {
		return nil
	}
	task := Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: d.Repo, Number: d.Number},
		PRFixItem: &PRFixItem{
			ID:               d.PRFixID,
			Generation:       d.Generation,
			HandoutToken:     d.HandoutToken,
			FixType:          d.FixType,
			OriginalClaimant: strings.TrimSpace(d.OriginalClaimant),
		},
	}
	decline := c.declineOwnership(ctx, task, reason, why)
	if decline == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", source.ErrStaleWork, decline)
}
