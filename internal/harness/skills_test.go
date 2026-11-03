package harness

import (
	"context"
	"os"
	"path/filepath"
	"sort"
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
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if containsTool(skillless.coordinatorTools(), toolSkill) || containsTool(skillless.subagentTools(), toolSkill) {
		t.Fatal("a skillless coordinator must not offer the skill tool")
	}

	skilled, err := NewCoordinator(CoordinatorConfig{
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
		Skills:    testSkillSet(t),
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
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
		Skills:    testSkillSet(t),
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
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
		Skills:    testSkillSet(t),
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
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
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
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
		Skills:    set,
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

// materializeBundle lays out the operator's projection of one bundle into a
// mount root: the bundle's files land at their relative paths, the way the
// kubelet projects the run's content ConfigMap into the pods.
func materializeBundle(t *testing.T, files map[string]string) string {
	t.Helper()
	mount := t.TempDir()
	for rel, content := range files {
		full := filepath.Join(mount, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return mount
}

// materializedSkillSet loads one bundle through the form the operator
// materializes: the rendered per-run config document plus the mounted
// files, resolved against the pin the deployment registry selected. It
// returns the skill set, the mount path the operator registered, and the
// pinned digest.
func materializedSkillSet(t *testing.T, name string, files map[string]string) (*SkillSet, string, string) {
	t.Helper()
	paths := make([]string, 0, len(files))
	for rel := range files {
		paths = append(paths, rel)
	}
	sort.Strings(paths)
	mount := materializeBundle(t, files)
	document, err := skills.RenderRunConfig([]skills.MaterializedBundle{{
		Name:   name,
		Source: mount,
		Files:  paths,
	}})
	if err != nil {
		t.Fatalf("RenderRunConfig: %v", err)
	}
	configPath := filepath.Join(t.TempDir(), "bundles.yaml")
	if err := os.WriteFile(configPath, document, 0o644); err != nil {
		t.Fatal(err)
	}
	// The deployment registry selects the pin: the tree and the projection
	// carry the same bytes, so the pin the operator wrote is the one the
	// control pod verifies.
	tree := materializeBundle(t, files)
	treeConfig := filepath.Join(t.TempDir(), "bundles.yaml")
	if err := os.WriteFile(treeConfig, []byte("bundles:\n  - name: "+name+"\n    source: "+tree+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	registry, err := skills.Load(treeConfig)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	pins, err := skills.Select(registry, []string{name})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	loaded, err := skills.LoadForRun(pins, configPath, t.TempDir())
	if err != nil {
		t.Fatalf("LoadForRun against the materialized bundle: %v", err)
	}
	set, err := NewSkillSet(loaded)
	if err != nil {
		t.Fatalf("NewSkillSet: %v", err)
	}
	return set, mount, pins[0].Digest
}

// TestSkillToolServesMaterializedBundle proves the harness works against
// what the operator actually materializes, not only against a hand-built
// view: a bundle loaded through the rendered per-run config document and
// the mounted files yields a skill set whose tool result serves the
// verified instructions and the asset listing, and the asset paths it names
// are the paths the worker's mount carries.
func TestSkillToolServesMaterializedBundle(t *testing.T) {
	const body = "Alpha body with `$(rm -rf /)` stays data."
	set, mount, _ := materializedSkillSet(t, "alpha", map[string]string{
		"SKILL.md":            "---\nname: alpha\ndescription: does alpha things\n---\n" + body + "\n",
		"assets/checklist.md": "check\n",
	})
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
		Skills:    set,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.runTool(nil, ToolCall{ID: "c1", Name: toolSkill, Arguments: `{"name":"alpha"}`}, "")
	if result.IsError {
		t.Fatalf("skill result = %+v", result)
	}
	if !strings.Contains(result.Content, body) {
		t.Fatalf("the tool result must serve the verified instructions: %s", result.Content)
	}
	if !strings.Contains(result.Content, mount+"/assets/checklist.md") {
		t.Fatalf("the tool result must name the mounted asset path: %s", result.Content)
	}
	// The index is the discovery surface: it names the skill and carries
	// the deployment-authored description, never the instruction body.
	index := set.Index()
	if !strings.Contains(index, "- alpha (bundle): does alpha things") {
		t.Fatalf("index = %q, want the bundle's name and description", index)
	}
	if strings.Contains(index, body) {
		t.Fatal("the index must not carry the instruction body")
	}
	// The roles' system message is the index plus the contract for using
	// it; the instructions still arrive only through the tool.
	section := set.Section()
	if !strings.Contains(section, index) {
		t.Fatalf("section = %q, want it to carry the index", section)
	}
	if strings.Contains(section, body) {
		t.Fatal("the roles' system message must not carry the instruction body")
	}
	skill, ok := set.Lookup("alpha")
	if !ok {
		t.Fatal("the materialized skill is not reachable through the harness")
	}
	if skill.Source != mount {
		t.Fatalf("skill source = %q, want the mounted path %q", skill.Source, mount)
	}
}

// TestSkillToolServesScriptContentAsData proves executable content in a
// skill reaches the model as data and runs nowhere: the skill tool's result
// carries the script text verbatim, and the marker the script would have
// written is absent after the call — the control side never executes a
// skill's script.
func TestSkillToolServesScriptContentAsData(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "marker")
	script := "#!/bin/sh\ntouch " + marker
	set, _, _ := materializedSkillSet(t, "alpha", map[string]string{
		"SKILL.md": "---\nname: alpha\ndescription: does alpha things\n---\nRun the helper:\n\n" + script + "\n",
	})
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: &Gateway{BaseURL: "http://unused.invalid"},
		Bindings:  testBindings(),
		Skills:    set,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.runTool(nil, ToolCall{ID: "c1", Name: toolSkill, Arguments: `{"name":"alpha"}`}, "")
	if result.IsError {
		t.Fatalf("skill result = %+v", result)
	}
	if !strings.Contains(result.Content, script) {
		t.Fatalf("the tool result must carry the script as data: %s", result.Content)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the skill's script was executed by the control side")
	}
}

// TestSkillIndexSurvivesContextCompaction pins the composition of skill
// loading with the native context budget (#659 with #636): a run that
// carries a pinned skill index still compacts at its boundary, and the
// index survives the compaction, so a compacted run still knows which
// skills it carries. The skill tool stays offered on every request.
func TestSkillIndexSurvivesContextCompaction(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.completeAll = true
	worker.snapshotTip = "snap-tip"
	turn := strings.Repeat("reasoning about the workspace. ", 25)
	script := make([]gatewayResponse, 0, 6)
	for i := 0; i < 5; i++ {
		script = append(script, gatewayResponse{body: sse(
			contentChunk(turn),
			toolCallChunk(0, "call-shell", toolShell, `{"command":"echo hi"}`),
			finishChunk("tool_calls"),
		)})
	}
	script = append(script, gatewayResponse{body: sse(
		contentChunk(`{"outcome":"no_change_needed","evidence":"already done"}`), finishChunk("stop"))})
	gateway, mock, cleanup := gatewayFor(script...)
	defer cleanup()
	sink := &recordingSink{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
		Worker:    delegator,
		Snapshot:  snapshotProvider("snap-tip"),
		ContextBudgets: map[string]ContextBudgetInput{
			RoleCoordinator: {Role: RoleCoordinator, DeclaredMaxInputTokens: 2200, ProbedMaxInputTokens: 2200},
		},
		Compactions: sink,
		Skills:      testSkillSet(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(context.Background(), testInvocation())
	if result.Err != nil {
		t.Fatalf("harness result = %+v", result)
	}
	if len(sink.events) == 0 {
		t.Fatal("a run carrying a pinned skill index did not compact at its boundary")
	}
	for _, event := range sink.events {
		if event.Role != RoleCoordinator {
			t.Fatalf("event role = %q, want %q", event.Role, RoleCoordinator)
		}
	}
	if len(mock.calls) == 0 {
		t.Fatal("the harness sent no model request")
	}
	last := mock.calls[len(mock.calls)-1]
	if !strings.Contains(last.rawBody, "- alpha (bundle): does alpha things") {
		t.Fatalf("the skill index did not survive the compaction: %s", last.rawBody)
	}
	if !containsName(last.toolNames, toolSkill) {
		t.Fatalf("the skill tool is not offered after the compaction: %v", last.toolNames)
	}
}

// containsName reports whether a recorded request offered the named tool.
func containsName(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}
