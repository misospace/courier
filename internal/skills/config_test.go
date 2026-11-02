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

// TestParseNormalizesTheSourcePath pins the normalization rule: a source that
// names one location in a second spelling is normalized once, so the
// registry, the pod mount, and the rendered run config carry one spelling of
// the path. A trailing separator is the ordinary case — it names the same
// directory — and it must not become a mount path the pod cannot mount.
func TestParseNormalizesTheSourcePath(t *testing.T) {
	cases := map[string]string{
		"a trailing separator": "/etc/courier/skill-bundles/simple-english/",
		"a doubled separator":  "/etc//courier/skill-bundles/simple-english",
		"a dot element":        "/etc/courier/./skill-bundles/simple-english",
	}
	for kind, source := range cases {
		specs, err := Parse([]byte("bundles:\n  - name: simple-english\n    source: " + source + "\n"))
		if err != nil {
			t.Fatalf("Parse rejected a source with %s: %v", kind, err)
		}
		if specs[0].Source != "/etc/courier/skill-bundles/simple-english" {
			t.Fatalf("source with %s = %q, want the normalized path", kind, specs[0].Source)
		}
	}
}

// TestParseRejectsControlCharacterSource pins the control-character rule: a
// source carrying one is not a path an operator can read in a log line or a
// manifest, so it never reaches a mount.
func TestParseRejectsControlCharacterSource(t *testing.T) {
	_, err := Parse([]byte("bundles:\n  - name: simple-english\n    source: \"/etc/courier/skills/simple\\nenglish\"\n"))
	if err == nil || !strings.Contains(err.Error(), "control character") {
		t.Fatalf("expected control-character rejection, got %v", err)
	}
}

func TestParseAcceptsMaterializationBlock(t *testing.T) {
	specs, err := Parse([]byte("bundles:\n  - name: repo-tour\n    source: /etc/courier/skill-bundles/repo-tour\n    materialization:\n      files:\n      - SKILL.md\n      - assets/checklist.md\n"))
	if err != nil {
		t.Fatalf("Parse rejected a materialization block: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("unexpected specs: %#v", specs)
	}
	if specs[0].Materialization == nil {
		t.Fatalf("materialization block lost: %#v", specs[0])
	}
	if got := specs[0].Materialization.Files; len(got) != 2 || got[0] != "SKILL.md" || got[1] != "assets/checklist.md" {
		t.Fatalf("materialization files = %#v", got)
	}
}

func TestParseRejectsInvalidMaterializationPaths(t *testing.T) {
	cases := map[string]string{
		"absolute":      `"/etc/passwd"`,
		"parent":        `"../escape.md"`,
		"nested parent": `"assets/../../escape.md"`,
		"current":       `"./SKILL.md"`,
		"empty":         `""`,
		"nul byte":      `"SKILL.md\0"`,
		"backslash":     `'assets\checklist.md'`,
	}
	for name, quoted := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte("bundles:\n  - name: repo-tour\n    source: /etc/courier/skill-bundles/repo-tour\n    materialization:\n      files:\n      - " + quoted + "\n"))
			if err == nil {
				t.Fatal("expected the invalid materialized path to be rejected")
			}
			if !strings.Contains(err.Error(), "materialization file") || !strings.Contains(err.Error(), "repo-tour") {
				t.Fatalf("error = %v, want the skill and the offending path named", err)
			}
		})
	}
}

func TestRenderRunConfigRoundTripsThroughParse(t *testing.T) {
	files := map[string]string{
		"SKILL.md":            "---\nname: repo-tour\ndescription: demo\n---\nBody.\n",
		"assets/checklist.md": "check\n",
	}
	tree := resolveOK(t, bundleSpec{Name: "repo-tour", Source: materialize(t, files)})
	rendered, err := RenderRunConfig([]MaterializedBundle{{
		Name:   "repo-tour",
		Source: writeProjection(t, files),
		Files:  []string{"SKILL.md", "assets/checklist.md"},
	}})
	if err != nil {
		t.Fatalf("RenderRunConfig rejected a valid bundle: %v", err)
	}
	specs, err := Parse(rendered)
	if err != nil {
		t.Fatalf("Parse rejected the rendered run config: %v", err)
	}
	if len(specs) != 1 || specs[0].Materialization == nil {
		t.Fatalf("rendered config lost its materialization block: %#v", specs)
	}
	if specs[0].Name != "repo-tour" || specs[0].Materialization.Files[0] != "SKILL.md" {
		t.Fatalf("rendered config lost its entry: %#v", specs[0])
	}
	bundle, err := resolveBundle(specs[0])
	if err != nil {
		t.Fatalf("resolve rejected the rendered run config: %v", err)
	}
	if bundle.Digest != tree.Digest {
		t.Fatalf("rendered config resolves to digest %s, want the tree digest %s", bundle.Digest, tree.Digest)
	}
}

func TestRenderRunConfigSortsEntriesByName(t *testing.T) {
	rendered, err := RenderRunConfig([]MaterializedBundle{
		{Name: "zeta", Source: "/etc/zeta", Files: []string{"SKILL.md"}},
		{Name: "alpha", Source: "/etc/alpha", Files: []string{"SKILL.md"}},
	})
	if err != nil {
		t.Fatalf("RenderRunConfig rejected valid bundles: %v", err)
	}
	specs, err := Parse(rendered)
	if err != nil {
		t.Fatalf("Parse rejected the rendered run config: %v", err)
	}
	if len(specs) != 2 || specs[0].Name != "alpha" || specs[1].Name != "zeta" {
		t.Fatalf("rendered entries are not sorted by name: %#v", specs)
	}
}

func TestRenderRunConfigRejectsMalformedEntries(t *testing.T) {
	cases := map[string]struct {
		bundles []MaterializedBundle
		want    string
	}{
		"empty name": {
			bundles: []MaterializedBundle{{Name: "", Source: "/etc/a", Files: []string{"SKILL.md"}}},
			want:    "name is required",
		},
		"invalid name": {
			bundles: []MaterializedBundle{{Name: "Not_A_Label", Source: "/etc/a", Files: []string{"SKILL.md"}}},
			want:    "DNS-1123",
		},
		"duplicate name": {
			bundles: []MaterializedBundle{
				{Name: "alpha", Source: "/etc/a", Files: []string{"SKILL.md"}},
				{Name: "alpha", Source: "/etc/b", Files: []string{"SKILL.md"}},
			},
			want: "duplicate name",
		},
		"relative source": {
			bundles: []MaterializedBundle{{Name: "alpha", Source: "etc/a", Files: []string{"SKILL.md"}}},
			want:    "absolute path",
		},
		"empty file list": {
			bundles: []MaterializedBundle{{Name: "alpha", Source: "/etc/a"}},
			want:    "must not be empty",
		},
		"invalid file path": {
			bundles: []MaterializedBundle{{Name: "alpha", Source: "/etc/a", Files: []string{"../escape.md"}}},
			want:    "materialization file",
		},
		"nul byte file path": {
			bundles: []MaterializedBundle{{Name: "alpha", Source: "/etc/a", Files: []string{"SKILL.md\x00"}}},
			want:    "NUL byte",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := RenderRunConfig(tc.bundles)
			if err == nil {
				t.Fatal("expected the malformed entry to be rejected")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
		})
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

// TestParseRejectsNULBytePaths pins the NUL-byte rule at the parse
// boundary: a bundle config whose name or source carries a NUL byte never
// parses into a usable entry. A literal NUL is not a legal YAML character
// at all, and an escaped one in a name is not a valid DNS-1123 label, so
// both are rejected while the document is parsed — never carried into a
// registration.
func TestParseRejectsNULBytePaths(t *testing.T) {
	cases := map[string]string{
		"literal NUL in name":   "bundles:\n  - name: \"bad\x00name\"\n    source: /a\n",
		"literal NUL in source": "bundles:\n  - name: a\n    source: \"/etc/\x00\"\n",
		"escaped NUL in name":   "bundles:\n  - name: \"bad\\0name\"\n    source: /a\n",
		"escaped NUL in source": "bundles:\n  - name: a\n    source: \"/etc/\\0\"\n",
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(document)); err == nil {
				t.Fatal("a NUL byte in the config must fail closed")
			}
		})
	}
}

// TestLoadRejectsNULByteSource pins the NUL-byte rule at the registration
// boundary: a source path carrying a NUL byte is not a path the operating
// system can open, so the bundle never registers and the deployment's
// startup fails explicitly — the registry never silently omits or degrades
// an entry. The document is rejected while it parses, before any path is
// opened.
func TestLoadRejectsNULByteSource(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(config, []byte("bundles:\n  - name: alpha\n    source: \"/etc/\\0\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(config); err == nil {
		t.Fatal("a source carrying a NUL byte must fail the registration")
	} else if !strings.Contains(err.Error(), "NUL byte") {
		t.Fatalf("error = %v, want the offending source named", err)
	}
}
