package skills

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseAcceptsBundlesList(t *testing.T) {
	specs, err := Parse([]byte("bundles:\n  - name: simple-english\n    source: /etc/courier/skill-bundles/simple-english\n"))
	if err != nil {
		t.Fatalf("Parse rejected a valid config: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "simple-english" || specs[0].Source != "/etc/courier/skill-bundles/simple-english" {
		t.Fatalf("unexpected specs: %#v", specs)
	}
}

func TestParseAllowsEmptyBundlesList(t *testing.T) {
	specs, err := Parse([]byte("bundles: []\n"))
	if err != nil {
		t.Fatalf("Parse rejected an empty bundles list: %v", err)
	}
	if len(specs) != 0 {
		t.Fatalf("expected zero specs, got %#v", specs)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	_, err := Parse([]byte("bundles:\n  - name: a\n    source: /a\n    extra: true\n"))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown-field rejection, got %v", err)
	}
}

func TestParseRejectsMissingName(t *testing.T) {
	_, err := Parse([]byte("bundles:\n  - source: /a\n"))
	if err == nil || !strings.Contains(err.Error(), "skill bundle 0: name is required") {
		t.Fatalf("expected missing-name rejection, got %v", err)
	}
}

func TestParseRejectsInvalidNameShapes(t *testing.T) {
	for _, name := range []string{"UPPER", "has_underscore", "-leading", "trailing-", "has.dot", "a b"} {
		_, err := Parse([]byte("bundles:\n  - name: " + name + "\n    source: /a\n"))
		if err == nil || !strings.Contains(err.Error(), "must be a valid DNS-1123 label") {
			t.Fatalf("name %q: expected DNS-1123 rejection, got %v", name, err)
		}
	}
}

func TestParseRejectsDuplicateNames(t *testing.T) {
	_, err := Parse([]byte("bundles:\n  - name: a\n    source: /a\n  - name: a\n    source: /b\n"))
	if err == nil || !strings.Contains(err.Error(), `duplicate name "a", first defined at bundle 0`) {
		t.Fatalf("expected duplicate-name rejection, got %v", err)
	}
}

func TestParseRejectsMissingAndRelativeSource(t *testing.T) {
	_, err := Parse([]byte("bundles:\n  - name: a\n"))
	if err == nil || !strings.Contains(err.Error(), "skill bundle 0: source is required") {
		t.Fatalf("expected missing-source rejection, got %v", err)
	}
	_, err = Parse([]byte("bundles:\n  - name: a\n    source: relative/path\n"))
	if err == nil || !strings.Contains(err.Error(), "must be an absolute path") {
		t.Fatalf("expected relative-source rejection, got %v", err)
	}
}

func TestParseTrimsNameAndSource(t *testing.T) {
	specs, err := Parse([]byte("bundles:\n  - name:  simple-english \n    source:  /etc/courier/skill-bundles/simple-english \n"))
	if err != nil {
		t.Fatalf("Parse rejected trimmed entry: %v", err)
	}
	if specs[0].Name != "simple-english" || specs[0].Source != "/etc/courier/skill-bundles/simple-english" {
		t.Fatalf("trimming mismatch: %#v", specs[0])
	}
}

func TestLoadReadErrorMentionsPath(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil || !strings.Contains(err.Error(), "read skill bundle config ") {
		t.Fatalf("expected read failure naming the path, got %v", err)
	}
}

func TestLoadResolvesEverySpec(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "bundle")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "SKILL.md"), []byte("# skill\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(config, []byte("bundles:\n  - name: simple-english\n    source: "+root+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	registry, err := Load(config)
	if err != nil {
		t.Fatalf("Load rejected a valid bundle: %v", err)
	}
	if got := registry.Names(); len(got) != 1 || got[0] != "simple-english" {
		t.Fatalf("unexpected names: %v", got)
	}
	bundle, ok := registry.Lookup("simple-english")
	if !ok || bundle.Files != 1 || bundle.Bytes != int64(len("# skill\n")) || bundle.Path != root {
		t.Fatalf("unexpected bundle: %#v (ok=%v)", bundle, ok)
	}
}
