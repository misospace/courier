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

// TestSkillsWire round-trips the skill selection (#658): a declared list
// persists under the skills key with its order intact, and an undeclared
// selection is absent from the wire so absence and an empty list are the same
// lane on read.
func TestSkillsWire(t *testing.T) {
	spec := LaneProfileSpec{
		Skills: []string{"simple-english", "repo-tour"},
	}
	raw, err := json.Marshal(&spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal spec: %v", err)
	}
	skillsRaw, ok := wire["skills"]
	if !ok {
		t.Fatalf("spec missing skills: %s", raw)
	}
	var got []string
	if err := json.Unmarshal(skillsRaw, &got); err != nil {
		t.Fatalf("unmarshal skills: %v", err)
	}
	if len(got) != 2 || got[0] != "simple-english" || got[1] != "repo-tour" {
		t.Fatalf("skills = %#v, want the declared order preserved on the wire", got)
	}
	bare, err := json.Marshal(&LaneProfileSpec{})
	if err != nil {
		t.Fatalf("marshal bare spec: %v", err)
	}
	if strings.Contains(string(bare), "skills") {
		t.Fatalf("bare spec serializes skills: %s", bare)
	}
}

// TestCRDManifestSkillsSchema pins the generated LaneProfile CRD to the
// skill-selection bounds (#658): at most sixteen approved logical names, each
// a DNS-1123 label of at most 63 characters, so a lane can never carry a URL
// or path where the registry expects a name.
func TestCRDManifestSkillsSchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_laneprofiles.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var block []string
	inSkills := false
	indent := ""
	for _, line := range lines {
		if !inSkills {
			if i := strings.Index(line, "skills:"); i >= 0 {
				inSkills = true
				indent = line[:i]
			}
			continue
		}
		// A sibling key at the same indent ends the skills block.
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		block = append(block, line)
	}
	if !inSkills {
		t.Fatal("CRD manifest has no spec.skills schema")
	}
	text := strings.Join(block, "\n")
	for _, want := range []string{"maxItems: 16", "maxLength: 63", "pattern: ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", "x-kubernetes-list-type: atomic"} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec.skills schema missing %q:\n%s", want, text)
		}
	}
}

// TestLaneProfileCIRepairPolicyDeepCopyIndependence pins that a copied lane
// owns its repair policy: the check names of a copy must not be shared with
// the lane it was copied from, or a policy edit would rewrite the checks of
// the lane a running run is bound to.
func TestLaneProfileCIRepairPolicyDeepCopyIndependence(t *testing.T) {
	profile := &LaneProfile{
		Spec: LaneProfileSpec{
			CIRepair: &CIRepairPolicy{
				MechanicalChecks:         []string{"unit", "lint"},
				MaxAttempts:              2,
				ObservationWindowSeconds: 900,
			},
		},
	}
	copied := profile.DeepCopy()
	copied.Spec.CIRepair.MechanicalChecks[0] = "mutated"
	copied.Spec.CIRepair.MaxAttempts = 5
	if profile.Spec.CIRepair.MechanicalChecks[0] != "unit" {
		t.Fatalf("original mechanical checks mutated through the copy: %v", profile.Spec.CIRepair.MechanicalChecks)
	}
	if profile.Spec.CIRepair.MaxAttempts != 2 {
		t.Fatalf("original max attempts mutated through the copy: %d", profile.Spec.CIRepair.MaxAttempts)
	}
}

// TestCRDManifestCIRepairPolicySchema pins the generated LaneProfile CRD to
// the declared repair-policy bounds: the budget defaults to zero, so a lane
// that declares the policy without a budget repairs nothing, and the
// observation window defaults to half an hour within [60, 21600] seconds.
func TestCRDManifestCIRepairPolicySchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_laneprofiles.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var block []string
	inRepair := false
	indent := ""
	for _, line := range lines {
		if !inRepair {
			if i := strings.Index(line, "ciRepair:"); i >= 0 {
				inRepair = true
				indent = line[:i]
			}
			continue
		}
		// A sibling key at the same indent ends the repair-policy block.
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		block = append(block, line)
	}
	if !inRepair {
		t.Fatal("CRD manifest has no spec.ciRepair schema")
	}
	text := strings.Join(block, "\n")
	for _, want := range []string{"mechanicalChecks:", "maxAttempts:", "default: 0", "maximum: 5", "minimum: 0", "observationWindowSeconds:", "default: 1800", "maximum: 21600", "minimum: 60"} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec.ciRepair schema missing %q:\n%s", want, text)
		}
	}
}
