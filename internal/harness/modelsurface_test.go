package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The model-surface provenance digest (#730): stability, order independence,
// representative material changes, wiring reflection, and the exclusion
// contract (prompt bodies, skill bodies, and goal text never reach the
// encoding). The digest is derived from production assembly code — the
// surface the surface_contract_test validates — so these tests cover the
// identity computation over that same coordinator construction.

func modelsurfaceSkillPinsJSON(t *testing.T, pins ...courierv1alpha1.SkillPin) string {
	t.Helper()
	if len(pins) == 0 {
		return ""
	}
	encoded, err := json.Marshal(pins)
	if err != nil {
		t.Fatalf("marshal skill pins: %v", err)
	}
	return string(encoded)
}

func modelsurfaceCriteriaJSON(t *testing.T, digest string) string {
	t.Helper()
	if digest == "" {
		return ""
	}
	encoded, err := json.Marshal(map[string]string{"issue": "owner/name#7", "digest": digest})
	if err != nil {
		t.Fatalf("marshal criteria envelope: %v", err)
	}
	return string(encoded)
}

func modelsurfaceBaseInputs() ModelSurfaceInputs {
	return ModelSurfaceInputs{
		GatewayTransport: "openai",
	}
}

func modelsurfaceCoordinator(t *testing.T, skillSet *SkillSet) *Coordinator {
	t.Helper()
	c, err := NewCoordinator(CoordinatorConfig{
		Transport: surfaceStubTransport{},
		Bindings:  testBindings(),
		Skills:    skillSet,
	})
	if err != nil {
		t.Fatalf("coordinator construction failed: %v", err)
	}
	return c
}

func TestModelSurfaceIsStable(t *testing.T) {
	skillSet := surfaceSkillSet(t)
	inputs := ModelSurfaceInputs{
		GatewayTransport: "openai",
		ContextPolicy:    &courierv1alpha1.ContextPolicy{CompactAtPercent: 80},
		SkillsJSON:       modelsurfaceSkillPinsJSON(t, courierv1alpha1.SkillPin{Name: "bundle-a", Digest: "sha256:" + strings.Repeat("a", 64)}),
		CriteriaJSON:     modelsurfaceCriteriaJSON(t, "sha256:"+strings.Repeat("b", 64)),
	}
	first, err := modelsurfaceCoordinator(t, skillSet).ModelSurface(inputs)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	second, err := modelsurfaceCoordinator(t, skillSet).ModelSurface(inputs)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("digest is not stable across equivalent constructions: %s vs %s", first.Digest, second.Digest)
	}
	if string(first.Encoding) != string(second.Encoding) {
		t.Fatalf("canonical encoding is not stable:\n%s\n%s", first.Encoding, second.Encoding)
	}
	if first.SchemaVersion != courierv1alpha1.ModelSurfaceSchemaV2 {
		t.Fatalf("schema version = %d, want %d", first.SchemaVersion, courierv1alpha1.ModelSurfaceSchemaV2)
	}
	// The digest is the sha256 of the canonical encoding, verifiable
	// independently of the implementation.
	sum := sha256.Sum256(first.Encoding)
	if want := "sha256:" + hex.EncodeToString(sum[:]); first.Digest != want {
		t.Fatalf("digest = %s, want %s (sha256 of the canonical encoding)", first.Digest, want)
	}
}

// TestModelSurfaceEncodesPlacement pins the placement inputs of the V2
// encoding: every section records the transcript role it pins under, and the
// task turn's placement is explicit (#762 regression). An encoder that stops
// carrying placement — or a production shape that moves a section between
// roles — fails here instead of silently keeping a placement-blind digest.
func TestModelSurfaceEncodesPlacement(t *testing.T) {
	surface, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai"})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	var decoded modelSurface
	if err := json.Unmarshal(surface.Encoding, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.TaskTurn != modelSurfaceTaskTurnRole {
		t.Fatalf("task turn placement = %q; want %q", decoded.TaskTurn, modelSurfaceTaskTurnRole)
	}
	roles := make(map[string]string, len(decoded.Sections))
	for _, section := range decoded.Sections {
		roles[section.Name] = section.Role
	}
	want := map[string]string{
		"coordinator.framing":   "system",
		"subagent.framing":      "system",
		"control.turn":          "system",
		"outcome.clarification": "user",
	}
	for name, role := range want {
		if roles[name] != role {
			t.Fatalf("section %q placement = %q; want %q", name, roles[name], role)
		}
	}
}

func TestModelSurfaceIsOrderIndependent(t *testing.T) {
	skillSet := surfaceSkillSet(t)
	// The same three pins in two different envelope orders: Select persists
	// a sorted set, but the digest must not depend on the envelope order
	// even if a caller upstream of Select changes.
	forward := modelsurfaceSkillPinsJSON(t,
		courierv1alpha1.SkillPin{Name: "bundle-a", Digest: "sha256:" + strings.Repeat("a", 64)},
		courierv1alpha1.SkillPin{Name: "bundle-b", Digest: "sha256:" + strings.Repeat("b", 64)},
		courierv1alpha1.SkillPin{Name: "bundle-c", Digest: "sha256:" + strings.Repeat("c", 64)},
	)
	reverse := modelsurfaceSkillPinsJSON(t,
		courierv1alpha1.SkillPin{Name: "bundle-c", Digest: "sha256:" + strings.Repeat("c", 64)},
		courierv1alpha1.SkillPin{Name: "bundle-b", Digest: "sha256:" + strings.Repeat("b", 64)},
		courierv1alpha1.SkillPin{Name: "bundle-a", Digest: "sha256:" + strings.Repeat("a", 64)},
	)
	first, err := modelsurfaceCoordinator(t, skillSet).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: forward})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	second, err := modelsurfaceCoordinator(t, skillSet).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: reverse})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("digest changed with skill-envelope order: %s vs %s", first.Digest, second.Digest)
	}

	// Role bindings arrive from a map: the digest must not depend on map
	// iteration order. Two lanes with the same pairs, keyed in different
	// orders (Go randomizes map iteration, so this also holds run to run).
	laneA := map[string]string{"coordinator": "test/coordinator", "coder": "test/coder", "reviewer": "test/reviewer"}
	laneB := map[string]string{"reviewer": "test/reviewer", "coordinator": "test/coordinator", "coder": "test/coder"}
	bindingsA, err := BindRoles(laneA)
	if err != nil {
		t.Fatalf("bind roles: %v", err)
	}
	bindingsB, err := BindRoles(laneB)
	if err != nil {
		t.Fatalf("bind roles: %v", err)
	}
	surfaceA, err := (&Coordinator{bindings: bindingsA}).ModelSurface(modelsurfaceBaseInputs())
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	surfaceB, err := (&Coordinator{bindings: bindingsB}).ModelSurface(modelsurfaceBaseInputs())
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if surfaceA.Digest != surfaceB.Digest {
		t.Fatalf("digest changed with role-map key order: %s vs %s", surfaceA.Digest, surfaceB.Digest)
	}

	// A context policy re-encoded from a different JSON field order is the
	// same declaration and must yield the same digest.
	var policy courierv1alpha1.ContextPolicy
	reordered := `{"roles":{"coder":{"maxInputTokens":90000}},"compactAtPercent":80}`
	if err := json.Unmarshal([]byte(reordered), &policy); err != nil {
		t.Fatalf("decode reordered policy: %v", err)
	}
	surfaceC, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", ContextPolicy: &policy})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	surfaceD, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", ContextPolicy: &courierv1alpha1.ContextPolicy{CompactAtPercent: 80, Roles: map[string]courierv1alpha1.ContextBudget{"coder": {MaxInputTokens: 90000}}}})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if surfaceC.Digest != surfaceD.Digest {
		t.Fatalf("digest changed with context-policy JSON key order: %s vs %s", surfaceC.Digest, surfaceD.Digest)
	}
}

func TestModelSurfaceMaterialChanges(t *testing.T) {
	skillSet := surfaceSkillSet(t)
	basePins := modelsurfaceSkillPinsJSON(t, courierv1alpha1.SkillPin{Name: "bundle-a", Digest: "sha256:" + strings.Repeat("a", 64)})
	base, err := modelsurfaceCoordinator(t, skillSet).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: basePins})
	if err != nil {
		t.Fatalf("compute base: %v", err)
	}
	// A changed section digest — the preimage's representation of a prompt
	// change — must change the digest. Production section identities are
	// constants, so the preimage level is where a framing edit lands.
	var decoded modelSurface
	if err := json.Unmarshal(base.Encoding, &decoded); err != nil {
		t.Fatalf("decode base encoding: %v", err)
	}
	decoded.Sections[0].Digest = "sha256:" + strings.Repeat("f", 64)
	mutated, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if "sha256:"+hex.EncodeToString(textDigestBytes(mutated)) == base.Digest {
		t.Fatal("a changed section identity did not change the digest")
	}

	// A changed notice digest — the preimage's representation of an edit to
	// a fixed refusal, denial, compaction-header line, or skill-result
	// scaffolding — must change the digest too (#730 review).
	decoded.Notices[0].Digest = "sha256:" + strings.Repeat("e", 64)
	mutated, err = json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if "sha256:"+hex.EncodeToString(textDigestBytes(mutated)) == base.Digest {
		t.Fatal("a changed fixed-notice identity did not change the digest")
	}

	// A changed section placement — the transcript role a section pins
	// under — must change the digest too (#762 regression): the same words
	// through a different channel are a different model-visible contract.
	decoded.Sections[0].Role = "user"
	mutated, err = json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if "sha256:"+hex.EncodeToString(textDigestBytes(mutated)) == base.Digest {
		t.Fatal("a changed section placement did not change the digest")
	}

	// A changed task-turn placement must change the digest: the task text
	// itself is never hashed (source-owned data), so the placement field is
	// the only record of which channel carried it.
	decoded.TaskTurn = "system"
	mutated, err = json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if "sha256:"+hex.EncodeToString(textDigestBytes(mutated)) == base.Digest {
		t.Fatal("a changed task-turn placement did not change the digest")
	}

	// Representative material changes through the production inputs, each
	// of which must move the digest. A nil skillSet builds a skillless
	// coordinator — the wiring difference is itself a material change.
	variations := []struct {
		name     string
		inputs   ModelSurfaceInputs
		skillSet *SkillSet
	}{
		{
			name:     "transport change",
			inputs:   ModelSurfaceInputs{GatewayTransport: "anthropic", SkillsJSON: basePins},
			skillSet: skillSet,
		},
		{
			name:     "added pinned skill",
			inputs:   ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: modelsurfaceSkillPinsJSON(t, courierv1alpha1.SkillPin{Name: "bundle-a", Digest: "sha256:" + strings.Repeat("a", 64)}, courierv1alpha1.SkillPin{Name: "bundle-b", Digest: "sha256:" + strings.Repeat("b", 64)})},
			skillSet: skillSet,
		},
		{
			name:     "changed skill digest",
			inputs:   ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: modelsurfaceSkillPinsJSON(t, courierv1alpha1.SkillPin{Name: "bundle-a", Digest: "sha256:" + strings.Repeat("d", 64)})},
			skillSet: skillSet,
		},
		{
			name:     "context policy declared",
			inputs:   ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: basePins, ContextPolicy: &courierv1alpha1.ContextPolicy{CompactAtPercent: 60}},
			skillSet: skillSet,
		},
		{
			name:     "criteria set pinned",
			inputs:   ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: basePins, CriteriaJSON: modelsurfaceCriteriaJSON(t, "sha256:"+strings.Repeat("e", 64))},
			skillSet: skillSet,
		},
		{
			name:     "skills wiring removed",
			inputs:   ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: basePins},
			skillSet: nil,
		},
	}
	for _, variation := range variations {
		variation := variation
		t.Run(variation.name, func(t *testing.T) {
			got, err := modelsurfaceCoordinator(t, variation.skillSet).ModelSurface(variation.inputs)
			if err != nil {
				t.Fatalf("compute: %v", err)
			}
			if got.Digest == base.Digest {
				t.Fatalf("%s did not change the digest", variation.name)
			}
		})
	}
}

// TestModelSurfaceCarriesEveryFixedNotice pins the fixed-notice coverage the
// #730 review required: the canonical encoding carries the production
// fixed-notice table — name plus digest over the exact text or fmt template
// the tool paths emit — so a material change to any fixed refusal, denial,
// argument rejection, compaction-header line, or skill-result scaffolding
// changes the digest. The encoding sorts by name and the table's names are
// unique, so the encoding is byte-deterministic for the same table.
func TestModelSurfaceCarriesEveryFixedNotice(t *testing.T) {
	surface, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai"})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	var decoded modelSurface
	if err := json.Unmarshal(surface.Encoding, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Notices) != len(fixedNoticeTable) {
		t.Fatalf("encoding carries %d notices, want the table's %d", len(decoded.Notices), len(fixedNoticeTable))
	}
	byDigest := make(map[string]string, len(decoded.Notices))
	for _, notice := range decoded.Notices {
		if _, dup := byDigest[notice.Digest]; dup {
			t.Fatalf("encoding carries duplicate notice digest %s", notice.Digest)
		}
		byDigest[notice.Digest] = notice.Name
	}
	expected := make([]string, 0, len(fixedNoticeTable))
	for _, notice := range fixedNoticeTable {
		expected = append(expected, notice.name)
		digest := textDigest(notice.text)
		if byDigest[digest] != notice.name {
			t.Fatalf("notice %q: the encoding does not carry its exact digest %s (the digest belongs to %q)", notice.name, digest, byDigest[digest])
		}
	}
	sort.Strings(expected)
	for i, name := range expected {
		if got := decoded.Notices[i].Name; got != name {
			t.Fatalf("encoding notices[%d].name = %q, want the sorted %q", i, got, name)
		}
	}
}

// TestModelSurfacePinsRawLaneFraming pins the framing preimage (#730
// review): Run pins the raw framing text the invocation carries — the trim
// decides only whether a framing exists — so a whitespace-only framing
// change alters the model-visible prompt and must alter the digest, while a
// whitespace-only framing is no framing at all.
func TestModelSurfacePinsRawLaneFraming(t *testing.T) {
	plain, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", LaneFraming: "LANE FRAMING"})
	if err != nil {
		t.Fatalf("compute plain: %v", err)
	}
	padded, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", LaneFraming: "LANE FRAMING  \n"})
	if err != nil {
		t.Fatalf("compute padded: %v", err)
	}
	if padded.Digest == plain.Digest {
		t.Fatal("a whitespace-only framing change did not change the digest")
	}
	absent, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai"})
	if err != nil {
		t.Fatalf("compute absent: %v", err)
	}
	blank, err := modelsurfaceCoordinator(t, nil).ModelSurface(ModelSurfaceInputs{GatewayTransport: "openai", LaneFraming: "   \n\t"})
	if err != nil {
		t.Fatalf("compute blank: %v", err)
	}
	if blank.Digest != absent.Digest {
		t.Fatal("a whitespace-only framing must not appear in the digest: Run would pin nothing")
	}
}

func TestModelSurfaceReflectsWiring(t *testing.T) {
	// A skills-wired run declares the skill tool to both roles; a skillless
	// run does not. The wiring difference is a material contract change and
	// must be visible in the digest without any second surface description.
	skillSet := surfaceSkillSet(t)
	skillful, err := modelsurfaceCoordinator(t, skillSet).ModelSurface(modelsurfaceBaseInputs())
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	skillless, err := modelsurfaceCoordinator(t, nil).ModelSurface(modelsurfaceBaseInputs())
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if skillful.Digest == skillless.Digest {
		t.Fatal("the skills wiring did not change the digest")
	}
	var decoded modelSurface
	if err := json.Unmarshal(skillful.Encoding, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	foundSkillTool := 0
	for _, tool := range decoded.Tools {
		if tool.Name == toolSkill {
			foundSkillTool++
		}
	}
	if foundSkillTool != 2 {
		t.Fatalf("skill tool declared %d times across roles, want 2 (coordinator and sub-agent)", foundSkillTool)
	}
	if err := json.Unmarshal(skillless.Encoding, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, tool := range decoded.Tools {
		if tool.Name == toolSkill {
			t.Fatal("skillless run declares the skill tool")
		}
	}
}

func TestModelSurfaceExcludesPromptBodiesAndUntrustedText(t *testing.T) {
	skillSet := surfaceSkillSet(t)
	surface, err := modelsurfaceCoordinator(t, skillSet).ModelSurface(ModelSurfaceInputs{
		GatewayTransport: "openai",
		LaneFraming:      "LANE-FRAMING-SECRET-MARKER lane-level framing text",
		SkillsJSON:       modelsurfaceSkillPinsJSON(t, courierv1alpha1.SkillPin{Name: "bundle-a", Digest: "sha256:" + strings.Repeat("a", 64)}),
		CriteriaJSON:     modelsurfaceCriteriaJSON(t, "sha256:"+strings.Repeat("b", 64)),
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	encoding := string(surface.Encoding)
	// The pinned framing bodies are prompt text: only their digests travel.
	if strings.Contains(encoding, "You are the trusted coordinator") {
		t.Fatal("the canonical encoding carries the coordinator framing body")
	}
	if strings.Contains(encoding, "bounded delegate") {
		t.Fatal("the canonical encoding carries the sub-agent framing body")
	}
	// The lane framing text and the skill instruction bodies are excluded
	// the same way.
	if strings.Contains(encoding, "LANE-FRAMING-SECRET-MARKER") {
		t.Fatal("the canonical encoding carries the lane framing body")
	}
	if strings.Contains(encoding, "Fixture bundle skill body") {
		t.Fatal("the canonical encoding carries a skill instruction body")
	}
	// Criterion texts are source text already covered by the pinned set
	// digest; the encoding carries the digest only.
	if strings.Contains(encoding, modelsurfaceCriteriaJSON(t, "sha256:"+strings.Repeat("b", 64))) {
		t.Fatal("the canonical encoding carries the raw criterion envelope")
	}
	if !strings.Contains(encoding, "sha256:"+strings.Repeat("b", 64)) {
		t.Fatal("the canonical encoding does not carry the pinned criteria set digest")
	}
}

func TestModelSurfaceFailsClosedOnMalformedEnvelopes(t *testing.T) {
	cases := []struct {
		name   string
		inputs ModelSurfaceInputs
		want   string
	}{
		{
			name:   "unknown transport",
			inputs: ModelSurfaceInputs{GatewayTransport: "grpc"},
			want:   "unknown model transport",
		},
		{
			name:   "malformed skill envelope",
			inputs: ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: `{`},
			want:   "skill-pin envelope is malformed",
		},
		{
			name:   "skill pin with empty digest",
			inputs: ModelSurfaceInputs{GatewayTransport: "openai", SkillsJSON: modelsurfaceSkillPinsJSON(t, courierv1alpha1.SkillPin{Name: "bundle-a"})},
			want:   "empty name or digest",
		},
		{
			name:   "malformed criteria envelope",
			inputs: ModelSurfaceInputs{GatewayTransport: "openai", CriteriaJSON: `not-json`},
			want:   "criterion envelope is malformed",
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := modelsurfaceCoordinator(t, nil).ModelSurface(testCase.inputs); err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v, want it to contain %q", err, testCase.want)
			}
		})
	}
}
