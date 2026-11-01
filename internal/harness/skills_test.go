package harness

import (
	"strings"
	"testing"

	"github.com/misospace/courier/internal/skills"
)

func testSkillSet(t *testing.T) *SkillSet {
	t.Helper()
	loaded := skills.LoadedSkills{
		Visible: []skills.SkillView{
			{
				Name:         "alpha",
				Origin:       skills.OriginBundle,
				Digest:       "sha256:" + strings.Repeat("a", 64),
				Description:  "does alpha things",
				Instructions: "alpha instructions with `$(rm -rf /)` as data",
				Source:       "/etc/courier/skill-bundles/alpha",
				Files:        []string{"scripts/run.sh"},
				FilesBytes:   20,
			},
			{
				Name:         "repo-guide",
				Origin:       skills.OriginRepository,
				Description:  "repository-local guidance",
				Instructions: "repository instructions",
			},
		},
		Shadowed: []string{"twin"},
	}
	set, err := NewSkillSet(loaded)
	if err != nil {
		t.Fatalf("NewSkillSet: %v", err)
	}
	return set
}

// The composed index is the bounded discovery surface: names, origins, and
// deployment-authored descriptions only — full instructions arrive on demand
// through the skill tool, never flattened into the initial prompt, and
// repository-local descriptions (untrusted content) do not ride the index.
func TestSkillSetIndexCarriesIdentitiesNotInstructions(t *testing.T) {
	set := testSkillSet(t)
	index := set.Index()
	if !strings.Contains(index, "- alpha (bundle): does alpha things") {
		t.Fatalf("index missing the bundle entry: %s", index)
	}
	if !strings.Contains(index, "- repo-guide (repository, untrusted content): load with the skill tool to read it") {
		t.Fatalf("index missing the repository entry: %s", index)
	}
	if !strings.Contains(index, "Shadowed by deployment pins") || !strings.Contains(index, "twin") {
		t.Fatalf("index missing the shadowed report: %s", index)
	}
	if strings.Contains(index, "alpha instructions") || strings.Contains(index, "repository-local guidance") {
		t.Fatal("the index must not flatten skill instructions or untrusted descriptions into the prompt")
	}
}

func TestSkillSetRejectsOverboundIndex(t *testing.T) {
	visible := make([]skills.SkillView, 0, 32)
	for i := 0; i < 32; i++ {
		visible = append(visible, skills.SkillView{
			Name:        "skill-" + strings.Repeat("x", 60),
			Origin:      skills.OriginBundle,
			Description: strings.Repeat("d", 2000), // an over-bound composed index fails closed regardless of per-skill bounds
		})
	}
	if _, err := NewSkillSet(skills.LoadedSkills{Visible: visible}); err == nil {
		t.Fatal("expected an over-bound index to fail closed")
	}
}

// The skill tool is the one deterministic load path, offered to coordinator
// and sub-agent alike; a skillless coordinator offers no skill tool and pins
// no skills section.
func TestSkillToolOfferedToBothRoles(t *testing.T) {
	skillless, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if containsTool(skillless.coordinatorTools(), toolSkill) || containsTool(skillless.subagentTools(), toolSkill) {
		t.Fatal("a skillless coordinator must not offer the skill tool")
	}

	skilled, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Skills:   testSkillSet(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !containsTool(skilled.coordinatorTools(), toolSkill) {
		t.Fatal("the coordinator toolset must offer the skill tool when skills are loaded")
	}
	if !containsTool(skilled.subagentTools(), toolSkill) {
		t.Fatal("a delegated role must be able to load pinned skills through the same tool")
	}
}

func TestRunSkillServesVerifiedInstructionsAsData(t *testing.T) {
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Skills:   testSkillSet(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.runTool(nil, ToolCall{ID: "c1", Name: toolSkill, Arguments: `{"name":"alpha"}`}, "")
	if result.IsError {
		t.Fatalf("skill result = %+v", result)
	}
	if !strings.Contains(result.Content, "alpha instructions with `$(rm -rf /)` as data") {
		t.Fatalf("skill content must pass through unaltered: %s", result.Content)
	}
	if !strings.Contains(result.Content, "/etc/courier/skill-bundles/alpha/scripts/run.sh") {
		t.Fatalf("skill result must name the asset path the worker reads: %s", result.Content)
	}

	// A delegated role loads through the same path.
	subagentResult := coordinator.runTool(nil, ToolCall{ID: "c2", Name: toolSkill, Arguments: `{"name":"repo-guide"}`}, "b1")
	if subagentResult.IsError {
		t.Fatalf("subagent skill result = %+v", subagentResult)
	}
	if !strings.Contains(subagentResult.Content, "repository instructions") {
		t.Fatalf("subagent skill content = %s", subagentResult.Content)
	}
	if !strings.Contains(subagentResult.Content, ".agents/skills/repo-guide/") {
		t.Fatalf("repository-local skill must point at the checkout location: %s", subagentResult.Content)
	}
}

func TestRunSkillUnknownNameIsToolData(t *testing.T) {
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Skills:   testSkillSet(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.runTool(nil, ToolCall{ID: "c1", Name: toolSkill, Arguments: `{"name":"ghost"}`}, "")
	if !result.IsError || !strings.Contains(result.Content, "ghost") || !strings.Contains(result.Content, "alpha") {
		t.Fatalf("unknown skill result = %+v", result)
	}
	for _, args := range []string{`{`, `{"name":"  "}`, `{}`} {
		result := coordinator.runTool(nil, ToolCall{ID: "c1", Name: toolSkill, Arguments: args}, "")
		if !result.IsError {
			t.Fatalf("arguments %q: expected a tool error, got %+v", args, result)
		}
	}
}

// A skillless run offers no skill tool, but the model can still emit a tool
// request for any name it has seen elsewhere: the trusted control must
// answer with a tool error, never dereference the absent view and crash the
// control pod.
func TestRunSkillOnSkilllessCoordinatorIsToolData(t *testing.T) {
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.runTool(nil, ToolCall{ID: "c1", Name: toolSkill, Arguments: `{"name":"alpha"}`}, "")
	if !result.IsError || !strings.Contains(result.Content, "no skills") {
		t.Fatalf("skillless skill result = %+v", result)
	}
	// The same guard holds in subagent context: any model text stays data.
	subagentResult := coordinator.runTool(nil, ToolCall{ID: "c2", Name: toolSkill, Arguments: `{"name":"alpha"}`}, "b1")
	if !subagentResult.IsError {
		t.Fatalf("skillless subagent skill result = %+v", subagentResult)
	}
}

// The rendered asset listing is bounded: a bundle carrying many or very long
// asset paths cannot inject an unbounded dump into the transcript.
func TestRunSkillBoundsTheAssetListing(t *testing.T) {
	files := make([]string, 0, 600)
	for i := 0; i < 600; i++ {
		files = append(files, "assets/"+strings.Repeat("p", 40)+"/"+strings.Repeat("f", 20)+string(rune('a'+i%26)))
	}
	loaded := skills.LoadedSkills{Visible: []skills.SkillView{{
		Name:         "wide",
		Origin:       skills.OriginBundle,
		Digest:       "sha256:" + strings.Repeat("a", 64),
		Instructions: "body",
		Source:       "/etc/courier/skill-bundles/wide",
		Files:        files,
	}}}
	set, err := NewSkillSet(loaded)
	if err != nil {
		t.Fatalf("NewSkillSet: %v", err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Skills:   set,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.runTool(nil, ToolCall{ID: "c1", Name: toolSkill, Arguments: `{"name":"wide"}`}, "")
	if result.IsError {
		t.Fatalf("skill result = %+v", result)
	}
	if len(result.Content) > maxSkillAssetListingBytes+len("body")+512 {
		t.Fatalf("skill result is %d bytes; the asset listing must be bounded", len(result.Content))
	}
	if !strings.Contains(result.Content, "listing truncated") {
		t.Fatalf("a truncated listing must state the truncation: %s", result.Content[len(result.Content)-200:])
	}
}
