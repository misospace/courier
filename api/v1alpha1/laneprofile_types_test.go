package v1alpha1

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestContextPolicyWire pins the persisted declaration schema: the wire keys
// are the harness policy's vocabulary (#243), and an empty policy marshals to
// the minimum — the compact fraction rides along once defaulted so absent
// means "no policy", never "policy at zero".
func TestContextPolicyWire(t *testing.T) {
	raw, err := json.Marshal(&LaneProfileSpec{
		Context: &ContextPolicy{
			CompactAtPercent:  75,
			OutputReservation: 8192,
			Roles: map[string]ContextBudget{
				"coder": {MaxInputTokens: 200000, CompactAtPercent: 60, OutputReservation: 24576},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal spec: %v", err)
	}
	contextRaw, ok := wire["context"]
	if !ok {
		t.Fatalf("spec missing context: %s", raw)
	}
	var contextWire map[string]json.RawMessage
	if err := json.Unmarshal(contextRaw, &contextWire); err != nil {
		t.Fatalf("unmarshal context: %v", err)
	}
	for _, key := range []string{"compactAtPercent", "outputReservation", "roles"} {
		if _, ok := contextWire[key]; !ok {
			t.Fatalf("context missing %s: %s", key, raw)
		}
	}
	var roleWire map[string]json.RawMessage
	if err := json.Unmarshal(contextWire["roles"], &roleWire); err != nil {
		t.Fatalf("unmarshal roles: %v", err)
	}
	coder := string(roleWire["coder"])
	for _, key := range []string{"maxInputTokens", "compactAtPercent", "outputReservation"} {
		if !strings.Contains(coder, key) {
			t.Fatalf("coder budget missing %s: %s", key, coder)
		}
	}
	// An unset inherited field must not marshal: 0 means "inherit the lane
	// policy", and writing it out would freeze the default onto the role.
	bare, err := json.Marshal(ContextBudget{MaxInputTokens: 1000})
	if err != nil {
		t.Fatalf("marshal bare budget: %v", err)
	}
	if strings.Contains(string(bare), "compactAtPercent") {
		t.Fatalf("zero compactAtPercent marshalled: %s", bare)
	}
}

// TestCRDManifestContextSchema pins the generated LaneProfile CRD to the
// declaration bounds (#295): the lane policy defaults its fraction to 75
// within [1,99]; a per-role percent stays within [1,99] with no default, so
// unset keeps inheriting the lane fraction; ceilings are positive and
// reservations non-negative.
func TestCRDManifestContextSchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_laneprofiles.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var block []string
	inContext := false
	indent := ""
	for _, line := range lines {
		if !inContext {
			if i := strings.Index(line, "context:"); i >= 0 {
				inContext = true
				indent = line[:i]
			}
			continue
		}
		// A sibling key at the same indent ends the context block.
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		block = append(block, line)
	}
	if !inContext {
		t.Fatal("CRD manifest has no spec.context schema")
	}
	text := strings.Join(block, "\n")
	for _, want := range []string{"compactAtPercent:", "default: 75", "maximum: 99", "minimum: 1", "maxInputTokens:", "outputReservation:", "minimum: 0"} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec.context schema missing %q:\n%s", want, text)
		}
	}
	// The per-role percent must not be defaulted: a defaulted role could not
	// inherit a different lane-wide fraction.
	rolesIdx := strings.Index(text, "roles:")
	if rolesIdx < 0 {
		t.Fatal("spec.context schema has no roles map")
	}
	if strings.Contains(text[rolesIdx:], "default:") {
		t.Fatal("spec.context.roles defaults compactAtPercent; unset must inherit the lane policy")
	}
}
