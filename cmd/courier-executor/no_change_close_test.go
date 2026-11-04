package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReadOutcomeIgnoresCloseKey pins the legacy executor shim (#720): the
// declaration it reads may carry a grounded close block written by a newer
// coordinator, and the shim must keep parsing the plain ending — the close
// proposal is durable through trusted control's fenced status path, never
// through this file, so the shim neither rejects nor interprets the key.
func TestReadOutcomeIgnoresCloseKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outcome.json")
	body := `{"outcome":"no_change_needed","evidence":"already done","close":{"criteria":[{"criterion":"c","path":"p","excerpt":"an excerpt of at least twenty-four bytes long"}]}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	decl, ok, err := readOutcome(path)
	if err != nil || !ok {
		t.Fatalf("readOutcome = (%v, %v), want the plain no_change_needed ending to parse", ok, err)
	}
	if decl.Outcome != outcomeNoChangeNeeded || decl.Evidence != "already done" {
		t.Fatalf("declaration = %#v", decl)
	}
}
