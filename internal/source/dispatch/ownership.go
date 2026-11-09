package dispatch

import (
	"context"
	"fmt"
	"strings"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

// prOwnershipEvidenceCapabilityRequest is the exact capabilities value this
// client version sends for the #1569 ownership-evidence contract. The
// request is constant so the request and the accepted acknowledgment
// version cannot drift apart silently.
const prOwnershipEvidenceCapabilityRequest = "pr-ownership-evidence:1"

// OwnershipEvidence is Dispatch's #1569 ownership-evidence block, emitted on
// followup-pr tasks only to polls that declared the pr-ownership-evidence
// intake capability (dispatch#1582): a poll that never negotiated receives
// no block at all, which is exactly how the evidence stays invisible to
// unversioned consumers. Courier reads only the claimant; the remaining
// fields are Dispatch's own provenance record.
type OwnershipEvidence struct {
	// OriginalClaimant is the Dispatch identity that originally claimed or
	// holds this PR-fix item. It is the only ownership evidence Courier
	// consults; it is never inferred from the handout recipient, the PR
	// author, a branch prefix, or the fix type.
	OriginalClaimant string `json:"originalClaimant"`
}

// originalClaimantEvidence resolves the original claimant Dispatch supplied
// for one followup-pr task. The negotiated ownership block is authoritative;
// the prFixItem field is the forward-compatible fallback for a peer that
// inlines the evidence there instead. An absent block and an absent field
// both resolve to empty — the same evidence-missing verdict either way, so
// the gate fails closed identically against old and new peers.
func originalClaimantEvidence(task Task) string {
	if task.Ownership != nil {
		if claimant := strings.TrimSpace(task.Ownership.OriginalClaimant); claimant != "" {
			return claimant
		}
	}
	if task.PRFixItem != nil {
		return strings.TrimSpace(task.PRFixItem.OriginalClaimant)
	}
	return ""
}

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

// refuseOwnershipAck refuses to acknowledge a hand-out the configured
// bindings do not authorize, without releasing the reservation.
//
// The acknowledgement is a ledger note: it tells Dispatch this agent's run
// owns the attempt, so a hand-out the bindings do not authorize must never be
// acknowledged. It is not an execution boundary, though — the run that owns
// the attempt may already be executing it — so the refusal leaves the hand-out
// stamped to this agent and reclaimable by the source's bounded stale-handout
// reclaimer. Releasing it here would park an attempt whose coordinator is
// still running. The claim and pre-launch boundaries release the exact receipt
// for every attempt this connector refuses to execute, so no reservation is
// left held once such an attempt is dropped.
func (c *HTTPClient) refuseOwnershipAck(d workDescriptor) error {
	reason, why := c.ownershipVerdict(d.OriginalClaimant)
	if reason == "" {
		return nil
	}
	c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reason)
	return fmt.Errorf("%w: %s; receipt %s@%d (handout %q) is not acknowledged", ErrOwnershipDeclined, why, d.PRFixID, d.Generation, d.HandoutToken)
}
