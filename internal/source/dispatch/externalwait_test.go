package dispatch

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

// The decline release and the blocked-external settlement must present
// Dispatch's #1250 structured external-wait contract: without a valid
// dependency, a modern deployment parks the item PARKED/invalid_wait and
// nothing re-offers it (#786). The contract Courier presents is the
// cooldown-recheck predicate — no forge-verifiable dependency, re-offer on
// the queue's bounded recheck deadline.
func TestCooldownExternalWaitContract(t *testing.T) {
	d := workDescriptor{Type: "followup-pr", PRFixID: "item-1", Generation: 3}
	contract := cooldownExternalWait(d, "missing prerequisite: secrets/prod")
	// Literal pins, not the production constants: a typo in
	// externalWaitKindService or externalWaitTriggerRecheck would otherwise
	// ship green and fail-close every Courier wait into a PARKED/invalid_wait
	// park nothing re-offers (#786).
	if contract.Kind != "external_service" || contract.Ref != "courier:cooldown-recheck:item-1@3" ||
		contract.Evidence != "missing prerequisite: secrets/prod" ||
		contract.Trigger.Kind != "cooldown_recheck" || contract.Trigger.Note != "" {
		t.Fatalf("contract = %#v", contract)
	}
	if externalWaitKindService != "external_service" || externalWaitTriggerRecheck != "cooldown_recheck" {
		t.Fatalf("production constants drifted from Dispatch's #1250 vocabulary: kind=%q trigger=%q", externalWaitKindService, externalWaitTriggerRecheck)
	}
	// Deterministic: the task report and the queue mark for one wait carry
	// identical payloads, so Dispatch's same-generation duplicate fencing
	// never sees a contract it would read as a deadline reset.
	if again := cooldownExternalWait(d, "missing prerequisite: secrets/prod"); again != contract {
		t.Fatalf("contract not deterministic: %#v vs %#v", again, contract)
	}
	// An empty observation still satisfies Dispatch's non-empty evidence
	// rule fail-closed.
	if empty := cooldownExternalWait(d, "   "); empty.Evidence != "Courier run is waiting on an external prerequisite" {
		t.Fatalf("default evidence = %q", empty.Evidence)
	}
	// Field values stay within Dispatch's bounded schema caps.
	bounded := cooldownExternalWait(d, strings.Repeat("x", 900))
	if len(bounded.Evidence) != externalWaitEvidenceMax {
		t.Fatalf("evidence length = %d, want capped at %d", len(bounded.Evidence), externalWaitEvidenceMax)
	}
	if len(bounded.Ref) > externalWaitRefMax {
		t.Fatalf("ref length = %d, want at most %d", len(bounded.Ref), externalWaitRefMax)
	}
	// Truncation never splits a rune (the retry.go convention).
	runeSafe := cooldownExternalWait(d, strings.Repeat("é", 600))
	if !utf8.ValidString(runeSafe.Evidence) {
		t.Fatalf("evidence truncated mid-rune: %q", runeSafe.Evidence)
	}
	// Bounding never drops the generation suffix: two generations of the
	// same item must never share a ref.
	longID := workDescriptor{Type: "followup-pr", PRFixID: strings.Repeat("i", 300), Generation: 9}
	if got := cooldownExternalWaitRef(longID); !strings.HasSuffix(got, "@9") || len(got) > externalWaitRefMax {
		t.Fatalf("ref = %q, want bounded with the @9 generation suffix kept", got)
	}
	// The JSON shape is the object Dispatch's parseExternalWaitInput expects.
	blob, err := json.Marshal(contract)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(blob, &wire); err != nil {
		t.Fatal(err)
	}
	trigger, _ := wire["trigger"].(map[string]any)
	if wire["kind"] != externalWaitKindService || wire["ref"] != contract.Ref ||
		wire["evidence"] != contract.Evidence || trigger["kind"] != externalWaitTriggerRecheck {
		t.Fatalf("wire = %#v", wire)
	}
}

// wantCooldownExternalWait asserts a decoded wire body's externalWait field
// carries the #1250 cooldown-recheck contract for the receipt.
func wantCooldownExternalWait(t *testing.T, raw any, d workDescriptor, wantEvidence string) {
	t.Helper()
	blob, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var got externalWaitContract
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	if want := cooldownExternalWait(d, wantEvidence); got != want {
		t.Fatalf("externalWait = %#v, want the #1250 cooldown-recheck contract %#v", got, want)
	}
}
