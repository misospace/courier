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
