package v1alpha1

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestPublicationPolicyWire pins the persisted policy schema: it is bound to
// the run UID and must not carry control-pod identity, which belongs to the
// auth/status path only.
func TestPublicationPolicyWire(t *testing.T) {
	p := &PublicationPolicy{
		RunUID:              "run-uid",
		ProviderConfigRef:   "forge-a",
		BaseRepo:            "org/base",
		BaseRef:             "main",
		BaseOID:             "base-oid",
		WorkRepo:            "org/work",
		WorkRef:             "feature/x",
		WorkInitiallyAbsent: true,
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if _, ok := wire["controlPodUID"]; ok {
		t.Fatalf("policy persists control-pod identity: %s", raw)
	}
	if _, ok := wire["runUID"]; !ok {
		t.Fatalf("policy missing runUID binding: %s", raw)
	}
}

// TestCRDManifestLaneProfileResourcesSchema pins the generated CRD manifest to
// the narrowed LaneProfile pod-resources vocabulary.
func TestCRDManifestLaneProfileResourcesSchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_laneprofiles.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var resources []string
	inProperties := false
	inResources := false
	indent := ""
	for _, line := range lines {
		if !inProperties {
			if strings.Contains(line, "properties:") {
				inProperties = true
			}
			continue
		}
		if !inResources {
			if i := strings.Index(line, "resources:"); i >= 0 {
				inResources = true
				indent = line[:i]
			}
			continue
		}
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		resources = append(resources, line)
	}
	block := strings.Join(resources, "\n")
	if strings.Contains(block, "claims:") {
		t.Fatal("LaneProfile CRD resources schema still accepts claims")
	}
	for _, key := range []string{"requests:", "limits:"} {
		if !strings.Contains(block, key) {
			t.Fatalf("LaneProfile CRD resources schema missing %s", key)
		}
	}
}

// TestCRDManifestPolicySchema pins the generated CRD manifest to the same
// invariant: no controlPodUID anywhere under the publicationPolicy schema.
func TestCRDManifestPolicySchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_coderruns.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var policy []string
	inPolicy := false
	indent := ""
	for _, line := range lines {
		if !inPolicy {
			if i := strings.Index(line, "publicationPolicy:"); i >= 0 {
				inPolicy = true
				indent = line[:i]
			}
			continue
		}
		// A sibling key at the same indent ends the policy block.
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		policy = append(policy, line)
	}
	block := strings.Join(policy, "\n")
	if strings.Contains(block, "controlPodUID") {
		t.Fatal("CRD manifest persists controlPodUID under publicationPolicy")
	}
	if !strings.Contains(block, "runUID") {
		t.Fatal("CRD manifest missing runUID under publicationPolicy")
	}
}

// TestCIRepairContextWire pins the persisted repair-context schema: the head
// and the attempt are always present, because a repair context without them
// cannot be attributed to a head or counted against the run's budget.
func TestCIRepairContextWire(t *testing.T) {
	raw, err := json.Marshal(&CIRepairContext{
		Head:         "abc123",
		Attempt:      2,
		FailedChecks: []string{"unit"},
		Logs:         "unit: 1 failed",
	})
	if err != nil {
		t.Fatalf("marshal repair context: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal repair context: %v", err)
	}
	for _, key := range []string{"head", "attempt", "failedChecks", "logs"} {
		if _, ok := wire[key]; !ok {
			t.Fatalf("repair context missing %s: %s", key, raw)
		}
	}
	bare, err := json.Marshal(&CIRepairContext{Head: "abc123", Attempt: 1})
	if err != nil {
		t.Fatalf("marshal bare repair context: %v", err)
	}
	if strings.Contains(string(bare), "failedChecks") || strings.Contains(string(bare), "logs") {
		t.Fatalf("bare repair context carries optional evidence: %s", bare)
	}
}

// TestCIRepairContextDeepCopyIndependence pins that a copied run owns its
// repair evidence: the failed-check list of a copy must not be shared with
// the run it was copied from, or a repair attempt would rewrite the evidence
// of the attempt it is repairing.
func TestCIRepairContextDeepCopyIndependence(t *testing.T) {
	run := &CoderRun{
		Status: CoderRunStatus{
			CIRepair: &CIRepairContext{
				Head:         "abc123",
				Attempt:      1,
				FailedChecks: []string{"unit", "lint"},
				Logs:         "unit: 1 failed",
			},
		},
	}
	copied := run.DeepCopy()
	copied.Status.CIRepair.FailedChecks[0] = "mutated"
	copied.Status.CIRepair.Head = "def456"
	if run.Status.CIRepair.FailedChecks[0] != "unit" {
		t.Fatalf("original failed checks mutated through the copy: %v", run.Status.CIRepair.FailedChecks)
	}
	if run.Status.CIRepair.Head != "abc123" {
		t.Fatalf("original head mutated through the copy: %q", run.Status.CIRepair.Head)
	}
}

// TestCRDManifestCIRepairStatusSchema pins the generated CoderRun CRD to the
// repair-context bounds: the head is never empty (a repair belongs to a
// head) and the attempt is 1-based (a repair context never describes the
// zeroth attempt).
func TestCRDManifestCIRepairStatusSchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_coderruns.yaml")
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
		// A sibling key at the same indent ends the repair-context block.
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		block = append(block, line)
	}
	if !inRepair {
		t.Fatal("CRD manifest has no status.ciRepair schema")
	}
	text := strings.Join(block, "\n")
	for _, want := range []string{"head:", "minLength: 1", "attempt:", "minimum: 1", "failedChecks:", "logs:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("status.ciRepair schema missing %q:\n%s", want, text)
		}
	}
}
