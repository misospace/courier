package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/devenv"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/skills"
	"github.com/misospace/courier/internal/topology"
)

// The model-visible surface contract (#729): one test that collects the
// complete model-visible surface for each native role — the coordinator and
// the delegated sub-agent ("worker") role — from production assembly code,
// and validates it as one contract. The contract exists to catch semantic
// drift between what the harness tells a role and what that role can
// actually do:
//
//  1. a tool name referenced by model-visible text that is not declared to
//     the role being instructed;
//  2. enum/state names referenced by prompts or notices that disagree with
//     the schema the role can actually call;
//  3. an environment variable named in model-visible text that the harness
//     path does not register or materialize;
//  4. cross-role tool leakage — a coordinator-only tool instructed to a
//     sub-agent or vice versa;
//  5. removed or renamed resources and terminology silently remaining in
//     prompts, skills, or notices.
//
// Source of truth: production assembly. Every prompt, notice, tool
// declaration, and enum below is collected by referencing production
// symbols (harnessFraming, controlTurnText, the tool builders,
// coordinatorTools/subagentTools) or by executing the production paths that
// emit fixed tool-result text (runTool and its callees). The test maintains
// no handwritten copy of the surface. The one handwritten input is the
// retired-terminology ledger below: a negative list of terms the surface
// must not carry again, each grounded in the repo's history or the
// production contract comments — the complement of the surface, never a
// copy of it.
//
// Non-goals: no prompt-architecture rewrite, no general policy engine, no
// byte-level snapshots of every prompt, and no claim that prompt text is an
// authorization boundary. The runTool role enforcement remains the real
// boundary; this contract detects drift in the model-visible surface the
// harness pins next to it.
//
// Budget: the per-role token-proxy budget below is the documented bound on
// the combined prompt + tool-definition surface. Failures report the
// per-section breakdown with each section's owning surface, so an
// over-budget role names the surface that grew, not just an aggregate.

// --- collector -------------------------------------------------------------

// surfaceStubTransport satisfies ModelTransport for coordinator
// construction. The contract never drives a model turn: a driven turn is a
// test bug.
type surfaceStubTransport struct{}

func (surfaceStubTransport) StreamChat(context.Context, ChatRequest) ([]Event, error) {
	return []Event{{Kind: KindError, Err: errors.New("surface contract never drives a model turn")}}, nil
}

func (surfaceStubTransport) ModelInputLimit(context.Context, string) (int, error) {
	return 0, errors.New("surface contract never reads a model input limit")
}

// surfaceStubForge satisfies ForgeOps for the forge-wired construction. The
// contract never calls it.
type surfaceStubForge struct{}

func (surfaceStubForge) Call(context.Context, string, string) (string, error) {
	return "", errors.New("surface contract never calls the forge")
}

// surfaceSection is one model-visible section of text with the production
// surface that owns it. Owners are named so every failure — budget or
// semantic — points at the surface to fix.
type surfaceSection struct {
	owner string
	text  string
}

// roleSurface is one native role's model-visible surface for one wiring:
// the pinned prompt sections, the fixed tool-result text the role's tool
// paths emit (denials, refusals, and successful skill loads), and the tool
// declarations, all collected from production.
type roleSurface struct {
	role     string
	wiring   string
	sections []surfaceSection
	tools    []ToolDef
}

func (s roleSurface) toolNames() []string {
	names := make([]string, 0, len(s.tools))
	for _, tool := range s.tools {
		names = append(names, tool.Name)
	}
	return names
}

// surfaceSkillSet builds the skills view from the loader's verified shape,
// the same type production loading produces, so the pinned section the
// contract sees is the composed output of NewSkillSet — not a transcription.
func surfaceSkillSet(t *testing.T) *SkillSet {
	t.Helper()
	// The repository-local body's reference to the shell tool is a positive
	// control: both roles declare that tool, so the tool-reference validator
	// must resolve a name that arrived only through a successful skill load
	// — renaming the tool in the fixture must trip the contract.
	loaded := skills.LoadedSkills{
		Visible: []skills.SkillView{
			{Name: "surface-fixture-repo", Origin: skills.OriginRepository, Instructions: "Fixture repository-local skill body. Inspect the checkout with the shell tool before editing."},
			{Name: "surface-fixture-bundle", Origin: skills.OriginBundle, Digest: "sha256:fixture", Description: "Fixture bundle skill.", Instructions: "Fixture bundle skill body.", Source: "/opt/courier/skills/surface-fixture-bundle", Files: []string{"template.md"}},
		},
		Shadowed: []string{"surface-fixture-shadowed"},
	}
	set, err := NewSkillSet(loaded)
	if err != nil {
		t.Fatalf("surface contract: skill fixture failed to load: %v", err)
	}
	return set
}

// surfaceCoordinator constructs a coordinator through NewCoordinator with the
// requested wiring, exactly as production construction does.
func surfaceCoordinator(t *testing.T, forge ForgeOps, skillSet *SkillSet, control *ControlWatcher) *Coordinator {
	t.Helper()
	c, err := NewCoordinator(CoordinatorConfig{
		Transport: surfaceStubTransport{},
		Bindings:  testBindings(),
		Forge:     forge,
		Skills:    skillSet,
		Control:   control,
	})
	if err != nil {
		t.Fatalf("surface contract: coordinator construction failed: %v", err)
	}
	return c
}

// surfaceCall drives one production tool dispatch and returns the result
// the role's transcript would carry.
func surfaceCall(c *Coordinator, name, args, briefID string) ToolResult {
	return c.runTool(context.Background(), ToolCall{ID: "surface-harvest", Name: name, Arguments: args}, briefID)
}

// surfaceTombstonedBrief registers and cancels one brief on a fresh
// coordinator so the cancellation notice is harvested from the live ledger
// path.
func surfaceTombstonedBrief(t *testing.T) (c *Coordinator, briefID string) {
	t.Helper()
	c = surfaceCoordinator(t, nil, nil, nil)
	briefID = "tombstoned"
	registered, err := c.briefs.Register(Brief{ID: briefID, Role: "coder", Objective: "surface harvest", SuccessCheck: "surface harvest"})
	if err != nil {
		t.Fatalf("surface contract: registering the tombstone fixture brief failed: %v", err)
	}
	if _, err := c.briefs.Cancel(registered.ID); err != nil {
		t.Fatalf("surface contract: cancelling the tombstone fixture brief failed: %v", err)
	}
	return c, briefID
}

// latchedControlWatcher returns a soft-stop watcher with the stop latch set,
// so the soft-stop denial notice is harvested from the production guard
// without a broker. The fields are package-internal; setting them here
// mirrors the state Poll produces after a consumed request.
func latchedControlWatcher() *ControlWatcher {
	w := &ControlWatcher{consumed: map[string]struct{}{}}
	w.latched = true
	return w
}

// surfaceSuccessfulSkill harvests one successful production runSkill result:
// the exact tool-result text a role's transcript carries after loading a
// fixture skill — the rendered instructions plus the asset listing. Skills
// are model-visible surface (#729), so the instruction bodies and the
// rendering around them ride the same validators as every other section.
// The load is asserted successful so the success path cannot silently rot
// into yet another error notice.
func surfaceSuccessfulSkill(t *testing.T, c *Coordinator, name, briefID string) string {
	t.Helper()
	result := surfaceCall(c, toolSkill, fmt.Sprintf(`{"name":%q}`, name), briefID)
	if result.IsError {
		t.Fatalf("surface contract: loading fixture skill %q must succeed: %s", name, result.Content)
	}
	return result.Content
}

// surfaceCoordinatorSurface collects the coordinator role's surface for one
// wiring. Sections mirror exactly what Run pins and what the coordinator's
// own tool paths emit.
func surfaceCoordinatorSurface(t *testing.T, skillSet *SkillSet) roleSurface {
	t.Helper()
	forge := ForgeOps(nil)
	if skillSet != nil {
		forge = surfaceStubForge{}
	}
	c := surfaceCoordinator(t, forge, skillSet, nil)
	sections := []surfaceSection{
		{owner: "coordinator pinned framing (harnessFraming)", text: harnessFraming},
	}
	if skillSet != nil {
		sections = append(sections, surfaceSection{owner: "pinned skills section (SkillSet.Section)", text: skillSet.Section()})
	}
	sections = append(sections, surfaceCoordinatorNotices(t, c, skillSet != nil)...)
	return roleSurface{role: RoleCoordinator, wiring: surfaceWiringName(skillSet), sections: sections, tools: c.coordinatorTools()}
}

// surfaceCoordinatorNotices harvests the fixed text the coordinator's own
// production tool paths emit, by driving runTool with the dependencies each
// path refuses, plus — when skills are wired — the successful skill loads.
// Dynamic content (safeToolError output of transport and snapshot failures,
// worker output) is out of scope: it is data, not fixed surface.
func surfaceCoordinatorNotices(t *testing.T, c *Coordinator, skillful bool) []surfaceSection {
	t.Helper()
	sections := []surfaceSection{
		{owner: "notice: injected control turn (controlTurnText)", text: controlTurnText},
		{owner: "notice: outcome clarification (outcomeClarification)", text: outcomeClarification},
		{owner: "notice: step-boundary compaction digest (compactDigest)", text: compactDigest(2, "", []Message{
			{Role: "user", Content: "earlier directive"},
			{Role: "tool", Content: "earlier tool output"},
		}).Content},
		{owner: "notice: shell without a worker (runShell)", text: surfaceCall(c, toolShell, `{"command":"true"}`, "").Content},
		{owner: "notice: check without a worker (runCheck)", text: surfaceCall(c, toolCheck, `{"command":"true"}`, "").Content},
		{owner: "notice: delegate with malformed arguments (runDelegate)", text: surfaceCall(c, toolDelegate, `{`, "").Content},
		{owner: "notice: delegate to an unbound role (runDelegate)", text: surfaceCall(c, toolDelegate, `{"role":"surface-unbound","objective":"o","successCheck":"s"}`, "").Content},
		{owner: "notice: cancel_brief with malformed arguments (runCancelBrief)", text: surfaceCall(c, toolCancelBrief, `{}`, "").Content},
		{owner: "notice: cancel_brief for an unknown ID (runCancelBrief)", text: surfaceCall(c, toolCancelBrief, `{"id":"surface-unknown"}`, "").Content},
		{owner: "notice: soft stop in effect (runDelegate)", text: surfaceCall(surfaceCoordinator(t, nil, nil, latchedControlWatcher()), toolDelegate, `{"role":"coder","objective":"o","successCheck":"s"}`, "").Content},
		{owner: "notice: unknown tool (runTool)", text: surfaceCall(c, "surface-mystery", `{}`, "").Content},
	}
	if skillful {
		sections = append(sections,
			surfaceSection{owner: "notice: skill tool with malformed arguments (runSkill)", text: surfaceCall(c, toolSkill, `{`, "").Content},
			surfaceSection{owner: "notice: skill tool without a name (runSkill)", text: surfaceCall(c, toolSkill, `{"name":""}`, "").Content},
			// Successful loads, one per fixture origin: the instruction
			// bodies the role reads only through the skill tool, plus the
			// production rendering around them.
			surfaceSection{owner: "skill result: repository-local skill instructions (runSkill)", text: surfaceSuccessfulSkill(t, c, "surface-fixture-repo", "")},
			surfaceSection{owner: "skill result: bundle skill instructions (runSkill)", text: surfaceSuccessfulSkill(t, c, "surface-fixture-bundle", "")},
		)
	} else {
		sections = append(sections,
			surfaceSection{owner: "notice: forge without the forge seam (runForge)", text: surfaceCall(c, toolForge, `{"operation":"read_issue"}`, "").Content},
			surfaceSection{owner: "notice: skill tool in a skillless run (runSkill)", text: surfaceCall(c, toolSkill, `{"name":"surface-fixture-repo"}`, "").Content},
		)
	}
	tombstoned, briefID := surfaceTombstonedBrief(t)
	sections = append(sections, surfaceSection{owner: "notice: shell on a cancelled brief (runShell)", text: surfaceCall(tombstoned, toolShell, `{"command":"true"}`, briefID).Content})
	return sections
}

// surfaceSubagentSurface collects the delegated sub-agent role's surface for
// one wiring, mirroring what runBrief pins and what a sub-agent's tool paths
// emit. The sub-agent surface is role-generic: every delegated role gets the
// same framing and toolset.
func surfaceSubagentSurface(t *testing.T, skillSet *SkillSet) roleSurface {
	t.Helper()
	forge := ForgeOps(nil)
	if skillSet != nil {
		forge = surfaceStubForge{}
	}
	c := surfaceCoordinator(t, forge, skillSet, nil)
	const briefID = "surface-brief"
	sections := []surfaceSection{
		{owner: "sub-agent pinned framing (subagentFraming)", text: subagentFraming},
	}
	if skillSet != nil {
		sections = append(sections, surfaceSection{owner: "pinned skills section (SkillSet.Section)", text: skillSet.Section()})
	}
	sections = append(sections,
		surfaceSection{owner: "notice: shell without a worker (runShell, sub-agent)", text: surfaceCall(c, toolShell, `{"command":"true"}`, briefID).Content},
		surfaceSection{owner: "notice: tracked checks denied to sub-agents (runTool)", text: surfaceCall(c, toolCheck, `{"command":"true"}`, briefID).Content},
		surfaceSection{owner: "notice: delegation denied to sub-agents (runTool)", text: surfaceCall(c, toolDelegate, `{"role":"coder","objective":"o","successCheck":"s"}`, briefID).Content},
		surfaceSection{owner: "notice: cancellation denied to sub-agents (runTool)", text: surfaceCall(c, toolCancelBrief, `{"id":"x"}`, briefID).Content},
		surfaceSection{owner: "notice: forge denied to sub-agents (runTool)", text: surfaceCall(c, toolForge, `{"operation":"read_issue"}`, briefID).Content},
	)
	if skillSet != nil {
		sections = append(sections,
			surfaceSection{owner: "notice: skill tool with malformed arguments (runSkill, sub-agent)", text: surfaceCall(c, toolSkill, `{`, briefID).Content},
			// Successful loads, one per fixture origin, harvested as the
			// sub-agent role reads them.
			surfaceSection{owner: "skill result: repository-local skill instructions (runSkill, sub-agent)", text: surfaceSuccessfulSkill(t, c, "surface-fixture-repo", briefID)},
			surfaceSection{owner: "skill result: bundle skill instructions (runSkill, sub-agent)", text: surfaceSuccessfulSkill(t, c, "surface-fixture-bundle", briefID)},
		)
	} else {
		sections = append(sections, surfaceSection{owner: "notice: skill tool in a skillless run (runSkill, sub-agent)", text: surfaceCall(c, toolSkill, `{"name":"surface-fixture-repo"}`, briefID).Content})
	}
	return roleSurface{role: "sub-agent (delegated role)", wiring: surfaceWiringName(skillSet), sections: sections, tools: c.subagentTools()}
}

// surfaceWiringName names the wiring a surface was collected under.
func surfaceWiringName(skillSet *SkillSet) string {
	if skillSet != nil {
		return "skills+forge wired"
	}
	return "minimal (skillless, forgeless)"
}

// surfaceToolSection renders one tool declaration as its model-visible text:
// description plus the parameter schema the wire carries.
func surfaceToolSection(tool ToolDef) surfaceSection {
	encoded := ""
	if len(tool.Parameters) > 0 {
		if bytes, err := json.Marshal(tool.Parameters); err == nil {
			encoded = string(bytes)
		}
	}
	return surfaceSection{owner: fmt.Sprintf("tool declaration %q", tool.Name), text: tool.Description + "\n" + encoded}
}

// surfaceWithTools returns the surface's sections plus one section per tool
// declaration: the complete text a role reads.
func surfaceWithTools(s roleSurface) []surfaceSection {
	sections := append([]surfaceSection(nil), s.sections...)
	for _, tool := range s.tools {
		sections = append(sections, surfaceToolSection(tool))
	}
	return sections
}

// --- contract surfaces ------------------------------------------------------

func TestModelVisibleSurfaceContract(t *testing.T) {
	skillSet := surfaceSkillSet(t)
	surfaces := []roleSurface{
		surfaceCoordinatorSurface(t, skillSet),
		surfaceCoordinatorSurface(t, nil),
		surfaceSubagentSurface(t, skillSet),
		surfaceSubagentSurface(t, nil),
	}

	// The digest drift guard (#730 review): every fixed notice the tool
	// paths emit — harvested above by driving the production paths — must
	// be pinned in the model-surface digest, either by its exact constant
	// digest or, for a templated notice, by its slug with the template
	// rendered against the same fixture the harvest used. A new literal in
	// a tool path that never entered the fixed-notice table fails here.
	t.Run("every fixed notice is pinned in the model-surface digest", func(t *testing.T) {
		surface, err := AssembleModelSurface(testBindings(), nil, nil, ModelSurfaceInputs{GatewayTransport: "openai"})
		if err != nil {
			t.Fatalf("assemble: %v", err)
		}
		var decoded modelSurface
		if err := json.Unmarshal(surface.Encoding, &decoded); err != nil {
			t.Fatalf("decode: %v", err)
		}
		digests := make(map[string]struct{}, len(decoded.Notices)+len(decoded.Sections))
		names := make(map[string]struct{}, len(decoded.Notices))
		for _, notice := range decoded.Notices {
			digests[notice.Digest] = struct{}{}
			names[notice.Name] = struct{}{}
		}
		// The control turn and the outcome clarification are pinned as
		// Sections, not notices.
		for _, section := range decoded.Sections {
			digests[section.Digest] = struct{}{}
		}
		// The templated notices render against the same fixtures the
		// harvest used; the digest pins the template, not the rendering.
		templated := map[string]string{
			"shell.brief-cancelled": fmt.Sprintf(noticeBriefCancelledFmt, "tombstoned"),
			"tool.unknown":          fmt.Sprintf(noticeUnknownToolFmt, "surface-mystery"),
		}
		for _, roleSurface := range surfaces {
			for _, section := range roleSurface.sections {
				// The pinned framing and skills sections are the digest's
				// Sections list, not the notice table; only the tool paths'
				// own fixed output is guarded here.
				if !strings.HasPrefix(section.owner, "notice:") && !strings.HasPrefix(section.owner, "skill result:") {
					continue
				}
				if _, pinned := digests[textDigest(section.text)]; pinned {
					continue
				}
				// The templated notices render against the same fixtures the
				// harvest used; the digest pins the template, not the
				// rendering. The compaction harvest is a composite — header
				// template plus dynamic digest lines — so a prefix match,
				// and a ledger sentinel wrapped with a dynamic ID renders
				// as prefix plus the ID.
				prefixPinned := []struct{ slug, prefix string }{
					{"compaction.header", fmt.Sprintf(noticeCompactedFmt, 2)},
					{"brief.unknown", noticeBriefUnknown},
					{"brief.duplicate", noticeBriefDuplicate},
					{"brief.cancelled-ledger", noticeBriefCancelledLedger},
				}
				matched := false
				for slug, rendered := range templated {
					if section.text != rendered {
						continue
					}
					if _, hasSlug := names[slug]; !hasSlug {
						t.Fatalf("notice %q renders the harvested text but its slug is not pinned: %q", slug, section.owner)
					}
					matched = true
					break
				}
				for _, pinned := range prefixPinned {
					if !strings.HasPrefix(section.text, pinned.prefix) {
						continue
					}
					if _, hasSlug := names[pinned.slug]; !hasSlug {
						t.Fatalf("notice %q renders the harvested text but its slug is not pinned: %q", pinned.slug, section.owner)
					}
					matched = true
					break
				}
				if matched {
					continue
				}
				if strings.HasPrefix(section.owner, "skill result:") {
					// The skill-result bodies are data whose identity rides
					// the pinned skill pairs; the scaffolding around them —
					// the repository banner or the asset listing — must be
					// pinned.
					if !strings.Contains(section.text, noticeSkillRepoBanner) && !strings.Contains(section.text, noticeSkillAssetsFmt) && !strings.Contains(section.text, fmt.Sprintf(noticeSkillRepoFilesFmt, "surface-fixture-repo")) {
						t.Fatalf("harvested skill result %q carries no pinned scaffolding", section.owner)
					}
					continue
				}
				t.Fatalf("harvested fixed notice %q is not pinned in the model-surface digest: %q", section.owner, section.text)
			}
		}
	})

	t.Run("tool references exist for the instructed role", func(t *testing.T) {
		// Failure mode 1 (and the text half of 4): every tool a role's
		// model-visible text instructs it to use must be declared to that
		// role. The production text convention is "the <name> tool"; the
		// scan is deliberately narrow so prose mentioning the word "tool"
		// cannot false-positive.
		pattern := regexp.MustCompile(`\bthe ([a-z][a-z0-9_]*) tool\b`)
		for _, surface := range surfaces {
			declared := surface.toolNames()
			for _, section := range surfaceWithTools(surface) {
				for _, match := range pattern.FindAllStringSubmatch(section.text, -1) {
					if !slices.Contains(declared, match[1]) {
						t.Errorf("%s / %s: text references %q but the declared toolset is %v (owner: %s)",
							surface.role, surface.wiring, match[1], declared, section.owner)
					}
				}
			}
		}
	})

	t.Run("role tool boundary", func(t *testing.T) {
		// Failure mode 4, structural half: the sub-agent toolset must be a
		// subset of the coordinator's, the coordinator-only capabilities
		// must be behaviorally refused to a sub-agent, and the retired tool
		// names must never return. Production carries no publish and no
		// merge tool by contract (the tool-name const block in
		// coordinator.go); that comment is enforceable, so it is.
		var coordinatorFull, subagentFull roleSurface
		for _, surface := range surfaces {
			if surface.role == RoleCoordinator && surface.wiring == surfaceWiringName(skillSet) {
				coordinatorFull = surface
			}
			if surface.role != RoleCoordinator && surface.wiring == surfaceWiringName(skillSet) {
				subagentFull = surface
			}
		}
		if coordinatorFull.tools == nil || subagentFull.tools == nil {
			t.Fatalf("full-wiring surfaces missing: coordinator=%v subagent=%v", coordinatorFull.tools, subagentFull.tools)
		}
		for _, subTool := range subagentFull.toolNames() {
			if !slices.Contains(coordinatorFull.toolNames(), subTool) {
				t.Errorf("sub-agent declares tool %q that the coordinator does not declare", subTool)
			}
		}
		coordinatorOnly := []string{}
		for _, name := range coordinatorFull.toolNames() {
			if !slices.Contains(subagentFull.toolNames(), name) {
				coordinatorOnly = append(coordinatorOnly, name)
			}
		}
		wantCoordinatorOnly := []string{toolCheck, toolDelegate, toolCancelBrief, toolForge}
		sort.Strings(coordinatorOnly)
		sort.Strings(wantCoordinatorOnly)
		if !reflect.DeepEqual(coordinatorOnly, wantCoordinatorOnly) {
			t.Errorf("coordinator-only tools = %v, want exactly %v", coordinatorOnly, wantCoordinatorOnly)
		}
		// Behavioral boundary: every coordinator-only tool is refused to a
		// sub-agent by runTool, the same production router the declarations
		// feed.
		c := surfaceCoordinator(t, surfaceStubForge{}, skillSet, nil)
		for _, name := range wantCoordinatorOnly {
			result := surfaceCall(c, name, `{}`, "surface-brief")
			if !result.IsError {
				t.Errorf("tool %q is coordinator-only by declaration but runTool did not refuse it to a sub-agent", name)
			}
		}
		retired := []string{"publish", "merge"}
		for _, surface := range surfaces {
			for _, name := range surface.toolNames() {
				if slices.Contains(retired, name) {
					t.Errorf("%s / %s: retired tool %q is declared again; publication and merge are never model-influenced", surface.role, surface.wiring, name)
				}
			}
		}
	})

	t.Run("outcome schema agreement", func(t *testing.T) {
		// Failure mode 2: the outcome enum the coordinator's text teaches
		// must be exactly the enum parseOutcome accepts — both directions —
		// and every JSON shape the text documents must be accepted by the
		// production parser. The enum's source of truth is the executor's
		// model-declarable outcomes; capacity_exhausted is harness-derived,
		// never model-declared, and must stay out of the text.
		coordinatorFull := surfaces[0]
		var text strings.Builder
		for _, section := range surfaceWithTools(coordinatorFull) {
			text.WriteString(section.text)
			text.WriteString("\n")
		}
		documented := map[string]bool{}
		for _, match := range regexp.MustCompile(`"outcome":"([a-z_]+)"`).FindAllStringSubmatch(text.String(), -1) {
			documented[match[1]] = true
		}
		accepted := map[string]bool{
			string(executor.OutcomeChanges):         true,
			string(executor.OutcomeNoChangeNeeded):  true,
			string(executor.OutcomeNeedsDecision):   true,
			string(executor.OutcomeBlockedExternal): true,
		}
		for outcome := range documented {
			if !accepted[outcome] {
				t.Errorf("coordinator text documents outcome %q which parseOutcome does not accept", outcome)
			}
		}
		for outcome := range accepted {
			if !documented[outcome] {
				t.Errorf("parseOutcome accepts outcome %q which the coordinator text never documents; the model cannot declare it", outcome)
			}
		}
		// Every flat outcome shape the text carries must parse through the
		// production parser (the "..." placeholders are non-empty strings,
		// exactly as the parser sees them).
		for _, shape := range regexp.MustCompile(`\{"outcome":[^{}]*\}`).FindAllString(text.String(), -1) {
			if _, _, err := parseOutcome(shape); err != nil {
				t.Errorf("coordinator text documents outcome shape the parser rejects: %s: %v", shape, err)
			}
		}
		// The close shape rides inside the flat regex's blind spot (nested
		// braces); probe it through parseOutcome as the parser reads it:
		// the documented shape must be accepted, and a renamed key inside
		// it would silently drop the close proposal.
		closeShape := regexp.MustCompile(`\{"criterion":"[^"]*","path":"[^"]*","excerpt":"[^"]*"\}`).FindString(text.String())
		if closeShape == "" {
			t.Fatalf("coordinator text documents no close criterion shape; the no-change close grammar left the surface")
		}
		probe := `{"outcome":"no_change_needed","evidence":"...","close":{"criteria":[` + closeShape + `]}}`
		if _, _, err := parseOutcome(probe); err != nil {
			t.Errorf("coordinator text documents a close shape the parser rejects: %s: %v", probe, err)
		}
		// The close keys the text teaches must be the keys the persisted
		// contract actually carries, derived from the API type's own wire
		// encoding — not a transcription. Key sets compare, not placeholder
		// values.
		persisted, err := json.Marshal(courierv1alpha1.NoChangeCloseCriterion{Criterion: "c", Path: "p", Excerpt: "e"})
		if err != nil {
			t.Fatalf("marshal NoChangeCloseCriterion: %v", err)
		}
		keysOf := func(encoded string) map[string]bool {
			fields := map[string]any{}
			if err := json.Unmarshal([]byte(encoded), &fields); err != nil {
				t.Fatalf("decoding wire shape %s: %v", encoded, err)
			}
			keys := map[string]bool{}
			for key := range fields {
				keys[key] = true
			}
			return keys
		}
		if !reflect.DeepEqual(keysOf(closeShape), keysOf(string(persisted))) {
			t.Errorf("close criterion keys in text = %v, but the persisted NoChangeCloseCriterion wire keys = %v; the prompt and the schema disagree", keysOf(closeShape), keysOf(string(persisted)))
		}
	})

	t.Run("tool schemas are internally consistent and routable", func(t *testing.T) {
		// Failure mode 2, tool half: a declaration's required list must be
		// its own properties' subset, and every declared tool must route in
		// runTool as the role it is declared to — a declared tool the router
		// does not know is a silent no-op the model can call forever, and a
		// declared tool the router refuses by role is a promise the surface
		// makes that the enforced boundary breaks.
		for _, surface := range surfaces {
			for _, tool := range surface.tools {
				var schema struct {
					Properties map[string]any `json:"properties"`
					Required   []string       `json:"required"`
				}
				if len(tool.Parameters) > 0 {
					encoded, err := json.Marshal(tool.Parameters)
					if err != nil {
						t.Errorf("%s / %s: tool %q parameters do not marshal: %v", surface.role, surface.wiring, tool.Name, err)
						continue
					}
					if err := json.Unmarshal(encoded, &schema); err != nil {
						t.Errorf("%s / %s: tool %q parameters are not a JSON schema object: %v", surface.role, surface.wiring, tool.Name, err)
						continue
					}
				}
				for _, required := range schema.Required {
					if _, ok := schema.Properties[required]; !ok {
						t.Errorf("%s / %s: tool %q requires %q which it does not declare as a property", surface.role, surface.wiring, tool.Name, required)
					}
				}
				c := surfaceCoordinator(t, surfaceWiringForge(skillSet), surfaceWiringSkillSet(surface, skillSet), nil)
				briefID := ""
				if surface.role != RoleCoordinator {
					briefID = "surface-brief"
				}
				result := surfaceCall(c, tool.Name, `{}`, briefID)
				if result.IsError && strings.Contains(result.Content, "unknown tool") {
					t.Errorf("%s / %s: declared tool %q is not routable in runTool: %s", surface.role, surface.wiring, tool.Name, result.Content)
					continue
				}
				if surface.role == RoleCoordinator || !result.IsError {
					continue
				}
				// Role-refusal differential: the malformed-arguments error is
				// role-independent — the identical dispatch as the coordinator
				// produces the identical bytes, because the error is emitted
				// after the role gate. An error only the sub-agent dispatch
				// produces is runTool refusing, by role, a tool this surface
				// declares to it.
				asCoordinator := surfaceCall(c, tool.Name, `{}`, "")
				if result.Content != asCoordinator.Content {
					t.Errorf("%s / %s: declared tool %q is refused to the sub-agent role: as sub-agent %q, but as coordinator %q", surface.role, surface.wiring, tool.Name, result.Content, asCoordinator.Content)
				}
			}
		}
	})

	t.Run("environment names are registered", func(t *testing.T) {
		// Failure mode 3: an environment variable named in model-visible
		// text must be one the harness path registers or materializes.
		// The registry is production-anchored: the topology name constants,
		// devenv's protected-key predicate (the COURIER_/GIT_/LD_/DYLD_
		// families the worker and control env own), and the shell's
		// universal base names. Naming anything else in a prompt is a
		// contract failure — register it in topology first.
		registry := map[string]bool{
			topology.EnvPodNamespace: true,
			topology.EnvGoProxy:      true,
			topology.EnvGoSumDB:      true,
			"PATH":                   true,
			"HOME":                   true,
			"TMPDIR":                 true,
		}
		registered := func(name string) bool {
			return registry[name] || devenv.ProtectedKeys(name)
		}
		// The scan is deliberately a bare uppercase-name scan, not a
		// delimiter scan ($VAR, VAR=): a prompt that says "set MISE_DATA_DIR"
		// names an environment variable as surely as one that says
		// "$MISE_DATA_DIR". The lexicon holds the prose tokens that are not
		// environment names and do occur in the surface — the complete
		// inventory of bare uppercase tokens when this contract landed was
		// HEAD and JSON — so a genuinely new uppercase name in the text must
		// either register or fix the text.
		lexicon := map[string]bool{"HEAD": true, "JSON": true}
		pattern := regexp.MustCompile(`\b([A-Z][A-Z0-9_]{2,})\b`)
		for _, surface := range surfaces {
			for _, section := range surfaceWithTools(surface) {
				for _, match := range pattern.FindAllStringSubmatch(section.text, -1) {
					if lexicon[match[1]] || registered(match[1]) {
						continue
					}
					t.Errorf("%s / %s: text names environment variable %s which no production registry materializes (owner: %s); register it in internal/topology or fix the text",
						surface.role, surface.wiring, match[1], section.owner)
				}
			}
		}
	})

	t.Run("retired terminology is gone", func(t *testing.T) {
		// Failure mode 5: removed or renamed resources and terminology must
		// not silently return to prompts, skills, or notices. The ledger is
		// this test's one handwritten input: a negative list, each entry
		// grounded in the repo's own record, with the exact fixed phrases
		// where a term legitimately remains (a documented negation is not a
		// reintroduction).
		ledger := []struct {
			term        string
			replacement string
			reason      string
			// allowedPhrases are the exact fixed strings in which the term
			// remains legitimately. The phrase itself must occur in the
			// surface for the allowance to apply.
			allowedPhrases []string
		}{
			{
				term:        "outcome file",
				replacement: "the final-message outcome declaration (harnessFraming)",
				reason:      "the goal text's outcome file is a legacy-mode artifact; the native harness declares outcomes in the final message",
				allowedPhrases: []string{
					"The outcome file named in the goal does not exist in this harness",
				},
			},
		}
		for _, surface := range surfaces {
			for _, section := range surfaceWithTools(surface) {
				text := section.text
				for _, entry := range ledger {
					remaining := text
					for _, phrase := range entry.allowedPhrases {
						remaining = strings.ReplaceAll(remaining, phrase, "")
					}
					if strings.Contains(remaining, entry.term) {
						t.Errorf("%s / %s: retired term %q appears outside its documented allowance (owner: %s); the current term is %s — %s",
							surface.role, surface.wiring, entry.term, section.owner, entry.replacement, entry.reason)
					}
				}
			}
		}
		// The allowance itself must not rot: if the negation sentence left
		// the framing, the tombstone below would silently pass everything.
		for _, entry := range ledger {
			for _, phrase := range entry.allowedPhrases {
				found := false
				for _, surface := range surfaces {
					for _, section := range surfaceWithTools(surface) {
						if strings.Contains(section.text, phrase) {
							found = true
						}
					}
				}
				if !found {
					t.Errorf("retired-term allowance phrase for %q no longer occurs in any surface: %q — update the ledger", entry.term, phrase)
				}
			}
		}
	})

	t.Run("per-role surface budget", func(t *testing.T) {
		// The documented per-role budget over the combined prompt +
		// tool-definition surface, measured with the production cost model
		// (estimateMessageTokens / estimateToolTokens, 4 chars per token —
		// session.go's own scale, so this budget and the compaction
		// boundary measure the same bytes the same way).
		//
		// The budgets are explicit and deliberately generous: roughly the
		// measured surface plus headroom for ordinary wording changes. A
		// budget failure means the surface grew past a review-worthy bound;
		// the breakdown in the failure names the owning section, and the
		// budget is raised deliberately — with the new section in the diff —
		// never silently.
		const (
			// Coordinator: framing + skills section + notices + 6 tools.
			// The full wiring measured 1681 tokens when this budget landed;
			// the bound is that surface plus roughly 30% headroom for
			// ordinary wording changes.
			coordinatorBudgetTokens = 2200
			// Sub-agent: framing + skills section + notices + 2 tools. The
			// full wiring measured 593 tokens; the same ~35% headroom.
			subagentBudgetTokens = 800
		)
		for _, surface := range surfaces {
			budget := subagentBudgetTokens
			if surface.role == RoleCoordinator {
				budget = coordinatorBudgetTokens
			}
			type line struct {
				owner  string
				chars  int
				tokens int
			}
			lines := []line{}
			total := 0
			for _, section := range surface.sections {
				tokens := estimateMessageTokens(Message{Role: "system", Content: section.text})
				lines = append(lines, line{section.owner, len(section.text), tokens})
				total += tokens
			}
			toolTokens := estimateToolTokens(surface.tools)
			for _, tool := range surface.tools {
				chars := toolWireEnvelopeChars + len(tool.Name) + len(tool.Description)
				if encoded, err := json.Marshal(tool.Parameters); err == nil {
					chars += len(encoded)
				}
				lines = append(lines, line{fmt.Sprintf("tool declaration %q", tool.Name), chars, chars / tokensPerChar})
			}
			total += toolTokens
			sort.Slice(lines, func(i, j int) bool { return lines[i].tokens > lines[j].tokens })
			var breakdown strings.Builder
			for _, l := range lines {
				fmt.Fprintf(&breakdown, "\n  %5d tok %6d chars  %s", l.tokens, l.chars, l.owner)
			}
			t.Logf("%s / %s: %d tok of %d budget (prompt %d + tools %d); sections:%s",
				surface.role, surface.wiring, total, budget, total-toolTokens, toolTokens, breakdown.String())
			if total > budget {
				t.Errorf("%s / %s: model-visible surface is %d tokens (production estimate), above the %d-token budget for the role. The owning sections, largest first:%s\nRaise the budget deliberately, with the surface growth in the diff.",
					surface.role, surface.wiring, total, budget, breakdown.String())
			}
		}
	})
}

// surfaceWiringForge reports the forge seam the surface's wiring carries:
// the full wiring wires the forge, the minimal wiring does not.
func surfaceWiringForge(skillSet *SkillSet) ForgeOps {
	if skillSet != nil {
		return surfaceStubForge{}
	}
	return nil
}

// surfaceWiringSkillSet returns the skill set a surface's wiring was built
// with: the fixture for a full-wiring surface, nil for a minimal one.
func surfaceWiringSkillSet(surface roleSurface, skillSet *SkillSet) *SkillSet {
	if surface.wiring == surfaceWiringName(skillSet) {
		return skillSet
	}
	return nil
}
