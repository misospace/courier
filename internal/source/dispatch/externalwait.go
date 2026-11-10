package dispatch

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Dispatch's structured external-wait contract (#1250, shipped in
// misospace/dispatch#1273) makes every new WAITING_EXTERNAL writer — the
// /api/pr-fix-queue/mark route and the worker tasks/report waiting-external
// path — require a bounded, typed dependency: kind, authoritative ref,
// observed unsatisfied evidence, and a recovery trigger with defined
// evaluator semantics. A mark or report without a valid contract fails
// closed to a generation-pinned PARKED/invalid_wait park (operator requeue
// required), so a free-text note alone no longer records a recoverable wait.
//
// Courier's waiting-external writes — the shared receipt-fenced decline
// release behind declineRebase and declineOwnership, and the
// blocked-external lifecycle settlement — wait on no forge-verifiable
// dependency: the item must stay discoverable and be re-offered after the
// queue's bounded cooldown, without consuming attempt budget. The contract
// that names that intent is the trigger "cooldown_recheck", the one
// predicate Dispatch defines as having no live verifier: recovery happens
// when the recheck deadline (retryAt) elapses and the queue reissues the
// item. The kind is "external_service" with an opaque ref because no
// authoritative forge identity (org/repo#N, check:name@sha) applies.
const (
	externalWaitKindService    = "external_service"
	externalWaitTriggerRecheck = "cooldown_recheck"

	externalWaitRefMax      = 200
	externalWaitEvidenceMax = 500
)

// externalWaitContract mirrors Dispatch's bounded external-wait schema
// (misospace/dispatch#1273). The caps match Dispatch's validation so a
// contract this client builds can never be refused as malformed.
type externalWaitContract struct {
	Kind     string              `json:"kind"`
	Ref      string              `json:"ref"`
	Evidence string              `json:"evidence"`
	Trigger  externalWaitTrigger `json:"trigger"`
}

type externalWaitTrigger struct {
	Kind string `json:"kind"`
	Note string `json:"note,omitempty"`
}

// boundString truncates free text to at most max bytes, cutting on a rune
// boundary so a multi-byte value never arrives mangled (the same convention
// retry.go's retryNote sets).
func boundString(text string, max int) string {
	if len(text) <= max {
		return text
	}
	for max > 0 && !utf8.RuneStart(text[max]) {
		max--
	}
	return text[:max]
}

// cooldownExternalWaitRef names the wait identity: the attempt receipt
// (pr-fix id and generation). The generation suffix survives bounding, so
// two generations of the same item can never present the same ref — the
// ref, not the free-text evidence, is what makes the wait generation-pinned
// and writer-agnostic.
func cooldownExternalWaitRef(d workDescriptor) string {
	suffix := fmt.Sprintf("@%d", d.Generation)
	body := boundString("courier:cooldown-recheck:"+strings.TrimSpace(d.PRFixID), externalWaitRefMax-len(suffix))
	return body + suffix
}

// cooldownExternalWait builds the structured contract Dispatch's #1250
// schema requires for a WAITING_EXTERNAL write whose dependency has no
// forge-verifiable identity: the cooldown_recheck predicate holds the item
// out of offer until retryAt elapses and the queue reissues it — exactly
// the recoverable, generation-fenced wait the decline release and the
// blocked-external settlement document. evidence is the observed
// unsatisfied state and is never empty (Dispatch rejects an empty
// evidence field fail-closed). The construction is deterministic in the
// receipt (pr-fix id and generation), so the task report and the queue
// mark for one wait carry the same contract and Dispatch's same-generation
// duplicate fencing sees identical payloads.
func cooldownExternalWait(d workDescriptor, evidence string) externalWaitContract {
	evidence = strings.TrimSpace(evidence)
	if evidence == "" {
		evidence = "Courier run is waiting on an external prerequisite"
	}
	return externalWaitContract{
		Kind:     externalWaitKindService,
		Ref:      cooldownExternalWaitRef(d),
		Evidence: boundString(evidence, externalWaitEvidenceMax),
		Trigger:  externalWaitTrigger{Kind: externalWaitTriggerRecheck},
	}
}
