package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/misospace/courier/internal/connector"
)

// The versioned rebase-only intake contract (#715, misospace/dispatch#1532 as
// shipped in misospace/dispatch#1533).
//
// A rebase executor connector declares its intake contract on every next-task
// request:
//
//	work_scope=rebase-only
//	work_scope_version=1
//	capabilities=rebase-executor:1
//
// A deployment that has adopted the contract answers with a versioned intake
// acknowledgment on the task:
//
//	{"intake": {"version": 1, "scope": "rebase-only", "scopeEnforced": true,
//	            "capabilities": [{"name": "rebase-executor", "requested": 1,
//	                              "granted": true}]}}
//
// Anything else — no intake field at all (an older Dispatch), a different
// contract version, a scope other than rebase-only, a scope Dispatch did not
// enforce, a rebase-executor verdict that is not granted or requested at a
// different version, or a malformed body — is failed negotiation. An absent
// acknowledgment, an ignored filter, or an unknown version is never read as
// success: the offered work is declined before any claim, run, branch change,
// or publication, and the decline never falls back to ordinary fix-pr
// execution.
const (
	// The rebase-executor capability is the client-side execution contract
	// the mechanical rebase lane requires; the version is part of the claim.
	rebaseCapabilityName    = "rebase-executor"
	rebaseCapabilityVersion = 1

	capabilityQueryParam       = "capabilities"
	workScopeQueryParam        = "work_scope"
	workScopeVersionQueryParam = "work_scope_version"

	// rebaseOnlyScope is the only work scope defined by contract version 1.
	rebaseOnlyScope = "rebase-only"
	// rebaseOnlyScopeVersion is the only work_scope_version value defined by
	// contract version 1. The scope version is a query-string token; the
	// intake acknowledgment's version is the contract version itself.
	rebaseOnlyScopeVersion = "1"

	// rebaseFixType is the queue-side classification Dispatch stamps on
	// mechanical rebase followups. It is source-owned data, not a protocol
	// constant, and deliberately separate from rebaseCapabilityName.
	rebaseFixType = "rebase"
)

// rebaseCapabilityRequest is the exact capabilities value this client version
// sends; it is a constant so the request and the accepted acknowledgment
// version cannot drift apart silently.
const rebaseCapabilityRequest = "rebase-executor:1"

// ErrRebaseDeclined marks a discovered rebase offer Courier refused under the
// versioned capability contract. The offer was never claimed, materialized,
// or executed; the error carries the receipt evidence the decline preserves.
var ErrRebaseDeclined = errors.New("dispatch client: rebase work declined")

// ErrOwnershipDeclined marks a followup-pr handout Courier refused under the
// configured-ownership bindings (#761). The handout was never claimed,
// materialized, or executed; the error carries the receipt evidence the
// decline preserves.
var ErrOwnershipDeclined = errors.New("dispatch client: followup-pr ownership declined")

// isRebaseFixType reports whether a PR-fix item's FixType classifies the
// followup as rebase work. The comparison is case-insensitive because the
// queue's classification is source-owned data, not a protocol constant; the
// task type itself (followup-pr) is never changed by it.
func isRebaseFixType(fixType string) bool {
	return strings.EqualFold(strings.TrimSpace(fixType), rebaseFixType)
}

// rebaseIntakeAck mirrors Dispatch's `intake` acknowledgment (dispatch#1533):
// the versioned verdict on the intake contract this client declared.
type rebaseIntakeAck struct {
	Version       int    `json:"version"`
	Scope         string `json:"scope"`
	ScopeEnforced bool   `json:"scopeEnforced"`
	Capabilities  []struct {
		Name      string `json:"name"`
		Requested int    `json:"requested"`
		Granted   bool   `json:"granted"`
	} `json:"capabilities"`
}

// validateRebaseIntake evaluates a next-task response's `intake`
// acknowledgment against the contract this client declared. ok is true only
// for the exact v1 rebase-only negotiation: contract version 1, the
// rebase-only scope, the scope enforced, and rebase-executor granted at the
// requested version. reason is a bounded diagnostic reason from the connector
// vocabulary for every other shape.
func validateRebaseIntake(raw json.RawMessage) (ok bool, reason string) {
	if len(raw) == 0 {
		return false, reasonRebaseCapabilityMissing
	}
	var ack rebaseIntakeAck
	if err := json.Unmarshal(raw, &ack); err != nil {
		return false, reasonRebaseCapabilityInvalid
	}
	if ack.Version != rebaseCapabilityVersion {
		return false, reasonRebaseCapabilityVersion
	}
	if ack.Scope != rebaseOnlyScope {
		return false, reasonRebaseScopeWrong
	}
	if !ack.ScopeEnforced {
		return false, reasonRebaseScopeUnenforced
	}
	for _, capability := range ack.Capabilities {
		if capability.Name != rebaseCapabilityName {
			continue
		}
		if !capability.Granted || capability.Requested != rebaseCapabilityVersion {
			return false, reasonRebaseCapabilityDenied
		}
		return true, reasonRebaseCapabilityOK
	}
	return false, reasonRebaseCapabilityMissing
}

// declineHandout is the shared receipt-fenced decline core behind
// declineRebase and declineOwnership. It emits no diagnostics — each wrapper
// owns its own vocabulary reason — and keeps the exact decline semantics for
// both kinds: an offer without a valid receipt is declined without a release
// call, and otherwise the release is the generation-fenced waiting-external
// mark presented with the exact receipt (repo, PR, generation) the offer
// carried, so the source keeps the item discoverable for its rightful
// consumer without consuming attempt budget, fabricating completion, or
// touching any other writer's attempt.
//
// The release presents Dispatch's #1250 structured external-wait contract
// (misospace/dispatch#1273) with the cooldown-recheck predicate: a decline
// waits on no forge-verifiable dependency, and that predicate keeps the item
// out of offer until the queue's bounded recheck deadline elapses and the
// item is reissued — the recoverable re-offer the decline documents. A mark
// without the structured contract would fail closed to a PARKED/invalid_wait
// park nothing re-offers, so the contract is part of the release, not
// optional metadata.
//
// When the deployment refuses that release — an older Dispatch without the
// waiting-external mark, or any refusal this client cannot prove moot — the
// decline surfaces with the full ownership evidence and latches for this
// exact generation in the shared per-generation latch: one release attempt
// per generation per process, never an indefinite retry. The source's
// stale-handout reclaimer remains the documented bounded recovery path for
// the unacknowledged handout, and the work stays safely discoverable either
// way. decline is the sentinel the returned error wraps, kind is the noun
// the receipt-less message names, and note is the recoverable-wait note the
// release carries.
func (c *HTTPClient) declineHandout(ctx context.Context, task Task, decline error, kind, note, why string) error {
	if task.PRFixItem == nil || strings.TrimSpace(task.PRFixItem.ID) == "" || task.PRFixItem.Generation < 1 {
		// An offer without a valid receipt cannot be released against
		// anything, and an attempt without a receipt could never be settled
		// truthfully. Decline without acceptance and without a release call;
		// there is nothing this client owns to hand back.
		return fmt.Errorf("%w: %s carries no valid PR-fix receipt (%s); declined without acceptance or release", decline, kind, why)
	}
	receipt := fmt.Sprintf("receipt %s@%d (handout %q)", task.PRFixItem.ID, task.PRFixItem.Generation, task.PRFixItem.HandoutToken)
	d := workDescriptor{Type: task.Type, PRFixID: task.PRFixItem.ID, Generation: task.PRFixItem.Generation, FixType: task.PRFixItem.FixType}
	if task.PullRequest != nil {
		d.Repo, d.Number = task.PullRequest.Repo, task.PullRequest.Number
	}
	key := fmt.Sprintf("%s/%d#%s@%d", d.Repo, d.Number, d.PRFixID, d.Generation)
	if _, settled := c.handoutDeclines.Load(key); settled {
		return fmt.Errorf("%w: %s; the decline of %s was already settled with this deployment, so no further release is attempted (bounded recovery: the source's stale-handout reclaimer)", decline, why, receipt)
	}
	err := c.markPRFixWaitingExternal(ctx, d, note)
	// Either way this generation's decline is settled: the release landed, or
	// the deployment refused it and the evidence stands — never a per-poll
	// release loop.
	c.handoutDeclines.Store(key, struct{}{})
	if err == nil {
		return fmt.Errorf("%w: %s; released %s through the recoverable waiting-external mark", decline, why, receipt)
	}
	return fmt.Errorf("%w: %s; release of %s was refused by the source (%v); the decline stands with ownership evidence preserved and will not be retried for this generation", decline, why, receipt, err)
}

// declineRebase releases a rebase offer Courier cannot safely execute and
// returns the error discovery surfaces in its place. The offer is never
// claimed, materialized, or executed; the release path is the defined
// recoverable wait — the generation-fenced waiting-external mark with the
// #1250 structured cooldown-recheck dependency (see declineHandout) —
// presented with the exact receipt (repo, PR, generation) the offer
// carried, so the source keeps the item discoverable for a compatible
// consumer without consuming attempt budget, fabricating completion, or
// touching any other writer's attempt.
//
// When the deployment refuses that release — an older Dispatch without the
// waiting-external mark, or any refusal this client cannot prove moot — the
// decline surfaces with the full ownership evidence and latches for this
// exact generation: one release attempt per generation per process, never an
// indefinite retry. The source's stale-handout reclaimer remains the
// documented bounded recovery path for the unacknowledged handout, and the
// work stays safely discoverable either way.
func (c *HTTPClient) declineRebase(ctx context.Context, task Task, why string) error {
	c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonRebaseDeclined)
	if task.PRFixItem == nil || strings.TrimSpace(task.PRFixItem.ID) == "" || task.PRFixItem.Generation < 1 {
		c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reasonRebaseTaskInvalid)
	}
	return c.declineHandout(ctx, task, ErrRebaseDeclined, "rebase offer",
		"Courier declined a rebase task it cannot safely execute: "+why+"; the item stays discoverable for a compatible rebase executor",
		why)
}

// declineOwnership releases a followup-pr handout Courier's configured
// ownership bindings do not authorize (#761) and returns the error discovery
// surfaces in its place. The handout is never claimed, materialized, or
// executed; reason is the specific bounded ownership-diagnostic reason the
// caller picked (evidence missing, claimant unknown, or lane mismatch) and
// why is the human-readable refusal. The release path is the same shared
// receipt-fenced core the rebase decline uses, so the item stays discoverable
// for its owning lane.
func (c *HTTPClient) declineOwnership(ctx context.Context, task Task, reason, why string) error {
	c.diagnostics.Diagnostic(c.connectorName, connector.SignalProtocolError, reason)
	return c.declineHandout(ctx, task, ErrOwnershipDeclined, "followup-pr offer",
		"Courier declined a followup-pr handout its configured bindings do not authorize: "+why+"; the item stays discoverable for its owning lane",
		why)
}
