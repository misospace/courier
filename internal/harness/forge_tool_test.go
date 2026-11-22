package harness

import (
	"strings"
	"testing"
)

// TestForgeToolDescriptionTeachesTheVocabulary pins the review fix that made
// the model-facing forge tool callable: the tool declaration itself carries
// the closed operation vocabulary and each operation's argument shape, since
// the broker refuses everything outside it and never names the defect.
func TestForgeToolDescriptionTeachesTheVocabulary(t *testing.T) {
	tool := forgeTool()
	for _, want := range []string{"read-work-item", "read-pull-request", "list-reviews", "list-comments", "read-checks", "repo", "number", "headSHA", "id"} {
		if !strings.Contains(tool.Description, want) {
			t.Fatalf("forge tool description does not teach %q: %s", want, tool.Description)
		}
	}
	properties, ok := tool.Parameters["properties"].(map[string]any)
	if !ok {
		t.Fatalf("forge tool parameters = %#v, want object properties", tool.Parameters)
	}
	for _, name := range []string{"operation", "arguments"} {
		if _, ok := properties[name]; !ok {
			t.Fatalf("forge tool parameters missing %q: %#v", name, properties)
		}
	}
}
