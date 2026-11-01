package skills

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func writeLoaderTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", full, err)
		}
	}
}

func writeLoaderConfig(t *testing.T, dir string, specs map[string]string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("bundles:\n")
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		b.WriteString("- name: " + name + "\n")
		b.WriteString("  source: " + filepath.Join(dir, name) + "\n")
	}
	path := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// writeMaterializedConfig writes a run config whose bundle reads its
// content from a projection: the source is the mount root and the file
// list names the exact files the operator projected into the run's pods.
func writeMaterializedConfig(t *testing.T, dir, name, source string, files ...string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("bundles:\n")
	b.WriteString("- name: " + name + "\n")
	b.WriteString("  source: " + source + "\n")
	b.WriteString("  materialization:\n")
	b.WriteString("    files:\n")
	for _, file := range files {
		b.WriteString("    - " + file + "\n")
	}
	path := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// resolvePinned resolves the named bundles through the production loader and
// returns their pins, so tests exercise the registry→pin→load chain the run
// actually executes rather than hand-built digests.
func resolvePinned(t *testing.T, config string, names ...string) []Pin {
	t.Helper()
	registry, err := Load(config)
	if err != nil {
		t.Fatalf("Load(%s): %v", config, err)
	}
	pins, err := Select(registry, names)
	if err != nil {
		t.Fatalf("Select(%v): %v", names, err)
	}
	return pins
}

func resolvePinnedError(config string, names ...string) error {
	registry, err := Load(config)
	if err != nil {
		return err
	}
	_, err = Select(registry, names)
	return err
}

func TestLoadForRunSkilllessRequiresNothing(t *testing.T) {
	loaded, err := LoadForRun(nil, "/nonexistent/bundles.yaml", "/nonexistent/checkout")
	if err != nil {
		t.Fatalf("skillless load: %v", err)
	}
	if len(loaded.Visible) != 0 || loaded.Visible == nil {
		t.Fatalf("visible = %#v, want empty non-nil", loaded.Visible)
	}
	if len(loaded.Shadowed) != 0 || len(loaded.Rejected) != 0 {
		t.Fatalf("shadowed/rejected = %#v/%#v, want empty", loaded.Shadowed, loaded.Rejected)
	}
}

func TestLoadForRunPinsRequireBundleConfig(t *testing.T) {
	pins := []Pin{{Name: "a", Digest: "sha256:" + strings.Repeat("a", 64)}}
	if _, err := LoadForRun(pins, "", "unused"); err == nil {
		t.Fatal("expected pinned run with no bundle config to fail closed")
	}
}

func TestLoadForRunUnregisteredPinFailsClosed(t *testing.T) {
	dir := t.TempDir()
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := []Pin{{Name: "ghost", Digest: "sha256:" + strings.Repeat("a", 64)}}
	_, err := LoadForRun(pins, config, dir)
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error = %v, want unregistered-pin failure", err)
	}
}

func TestLoadForRunDigestDriftFailsClosed(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "---\nname: alpha\ndescription: demo\n---\nDo the thing.\n",
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "---\nname: alpha\ndescription: changed\n---\nDo the other thing.\n",
	})
	if _, err := LoadForRun(pins, config, dir); err == nil || !strings.Contains(err.Error(), "pinned digest") {
		t.Fatalf("error = %v, want digest drift failure", err)
	}
}

func TestLoadForRunVerifiesAndRendersBundle(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md":     "---\nname: alpha\ndescription: does things\n---\nBody with `$(rm -rf /)` stays data.\n",
		"alpha/scripts/x.sh": "#!/bin/sh\necho hi\n",
		"alpha/docs/note.md": "note\n",
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	loaded, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Visible) != 1 {
		t.Fatalf("visible = %d entries, want 1", len(loaded.Visible))
	}
	view := loaded.Visible[0]
	if view.Name != "alpha" || view.Origin != OriginBundle || view.Digest != pins[0].Digest {
		t.Fatalf("view identity = %#v", view)
	}
	// Source is the registered path the deployment mounts into the worker
	// pod, not the resolved kubelet timestamp path that exists only in this
	// pod.
	if want := filepath.Join(dir, "alpha"); view.Source != want {
		t.Fatalf("source = %q, want the registered path %q", view.Source, want)
	}
	if view.Source != view.Source && strings.Contains(view.Source, "..") {
		t.Fatal("source must never carry the kubelet ..data timestamp chain")
	}
	if !strings.Contains(view.Instructions, "$(rm -rf /)") {
		t.Fatal("skill content must pass through as data, unaltered")
	}
	if view.Description != "does things" {
		t.Fatalf("description = %q", view.Description)
	}
	if len(view.Files) != 2 || view.Files[0] != "docs/note.md" || view.Files[1] != "scripts/x.sh" {
		t.Fatalf("files = %#v, want sorted assets without SKILL.md", view.Files)
	}
	if view.FilesBytes != int64(len("#!/bin/sh\necho hi\n")+len("note\n")) {
		t.Fatalf("filesBytes = %d", view.FilesBytes)
	}
}

func TestLoadForRunCorruptBundleContentFailsClosed(t *testing.T) {
	cases := map[string]map[string]string{
		"missing SKILL.md": {"alpha/readme.md": "not a skill"},
		"empty SKILL.md":   {"alpha/SKILL.md": ""},
		"frontmatter only": {"alpha/SKILL.md": "---\nname: alpha\ndescription: demo\n---\n"},
		"name mismatch":    {"alpha/SKILL.md": "---\nname: beta\ndescription: demo\n---\nbody\n"},
		"oversized body":   {"alpha/SKILL.md": "---\ndescription: demo\n---\n" + strings.Repeat("x", maxSkillInstructionsBytes+1)},
		"no instructions":  {"alpha/SKILL.md": "   \n"},
	}
	for name, files := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeLoaderTree(t, dir, files)
			config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
			// A bundle whose SKILL.md is missing or empty cannot resolve at
			// all; the loader must also reject it through that path.
			if err := resolvePinnedError(config, "alpha"); err != nil {
				if name != "missing SKILL.md" && name != "empty SKILL.md" {
					t.Fatalf("resolve: %v", err)
				}
				return
			}
			pins := resolvePinned(t, config, "alpha")
			if _, err := LoadForRun(pins, config, dir); err == nil {
				t.Fatalf("%s: expected fail-closed", name)
			}
		})
	}
}

func TestLoadForRunUnterminatedFrontmatterFailsClosed(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "---\nname: beta\ndescription: never closed\n" + strings.Repeat("x", maxFrontmatterScanBytes+64),
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	if _, err := LoadForRun(pins, config, dir); err == nil || !strings.Contains(err.Error(), "unterminated frontmatter") {
		t.Fatalf("error = %v, want unterminated-frontmatter failure", err)
	}
}

func TestLoadForRunDescriptionTruncatesOnRuneBoundary(t *testing.T) {
	dir := t.TempDir()
	long := strings.Repeat("é", maxSkillDescriptionBytes/2+16)
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "---\nname: alpha\ndescription: " + long + "\n---\nbody\n",
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	loaded, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := loaded.Visible[0].Description
	if len(got) > maxSkillDescriptionBytes {
		t.Fatalf("description is %d bytes, above the bound", len(got))
	}
	if !strings.HasPrefix(long, got) {
		t.Fatal("description truncation must keep a prefix of the original")
	}
}

func TestLoadForRunCollisionAndRepositoryOnlySkill(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md":                     "---\nname: alpha\ndescription: pinned bundle\n---\nbundle body\n",
		".agents/skills/alpha/SKILL.md":      "---\nname: alpha\ndescription: shadowed twin\n---\nrepo body\n",
		".agents/skills/repo-skill/SKILL.md": "---\nname: repo-skill\ndescription: local\n---\nlocal body\n",
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	loaded, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Visible) != 2 {
		t.Fatalf("visible = %d entries, want 2 (pinned bundle plus repository-only skill)", len(loaded.Visible))
	}
	if loaded.Visible[0].Name != "alpha" || loaded.Visible[0].Origin != OriginBundle || loaded.Visible[0].Instructions != "bundle body" {
		t.Fatalf("visible[0] = %#v, want the pinned bundle view", loaded.Visible[0])
	}
	if loaded.Visible[1].Name != "repo-skill" || loaded.Visible[1].Origin != OriginRepository || loaded.Visible[1].Instructions != "local body" {
		t.Fatalf("visible[1] = %#v, want the repository-only skill view", loaded.Visible[1])
	}
	if len(loaded.Shadowed) != 1 || loaded.Shadowed[0] != "alpha" {
		t.Fatalf("shadowed = %#v, want [alpha]", loaded.Shadowed)
	}
}

func TestLoadForRunSkilllessRunLoadsNothing(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		".agents/skills/repo-skill/SKILL.md": "---\nname: repo-skill\ndescription: local\n---\nlocal body\n",
	})
	loaded, err := LoadForRun(nil, "", dir)
	if err != nil {
		t.Fatalf("skillless load with repository skills: %v", err)
	}
	// A skillless run pins nothing, so it loads nothing even when the
	// checkout carries repository-local skills: the pin is the contract.
	if len(loaded.Visible) != 0 {
		t.Fatalf("visible = %#v, want empty", loaded.Visible)
	}
}

func TestLoadForRunQuarantinesMalformedRepositorySkills(t *testing.T) {
	cases := map[string]struct {
		setup          func(t *testing.T, dir string)
		wantName       string
		wantReasonPart string
	}{
		"invalid name": {
			setup: func(t *testing.T, dir string) {
				writeLoaderTree(t, dir, map[string]string{
					".agents/skills/Bad_Name/SKILL.md": "body\n",
				})
			},
			wantName:       "Bad_Name",
			wantReasonPart: "DNS-1123",
		},
		"oversized document": {
			setup: func(t *testing.T, dir string) {
				writeLoaderTree(t, dir, map[string]string{
					".agents/skills/huge/SKILL.md": strings.Repeat("x", maxSkillInstructionsBytes+1),
				})
			},
			wantName:       "huge",
			wantReasonPart: "byte limit",
		},
		"name mismatch": {
			setup: func(t *testing.T, dir string) {
				writeLoaderTree(t, dir, map[string]string{
					".agents/skills/broken/SKILL.md": "---\nname: other\n---\nbody\n",
				})
			},
			wantName:       "broken",
			wantReasonPart: "logical name",
		},
		"symlinked document": {
			setup: func(t *testing.T, dir string) {
				writeLoaderTree(t, dir, map[string]string{
					".agents/skills/leak/SKILL.md": "harmless\n",
					"secret.txt":                   "credential material\n",
				})
				if err := os.Remove(filepath.Join(dir, ".agents/skills/leak/SKILL.md")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(dir, "secret.txt"), filepath.Join(dir, ".agents/skills/leak/SKILL.md")); err != nil {
					t.Fatal(err)
				}
			},
			wantName:       "leak",
			wantReasonPart: "not a regular file",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			tc.setup(t, dir)
			// Repository discovery runs only when the run pins skills, and a
			// quarantined repository skill must never fail the run: the
			// pinned set loads, the malformed repository skill is reported.
			writeLoaderTree(t, dir, map[string]string{
				"alpha/SKILL.md": "bundle body\n",
			})
			config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
			pins := resolvePinned(t, config, "alpha")
			loaded, err := LoadForRun(pins, config, dir)
			if err != nil {
				t.Fatalf("%s: repository content must not fail a pinned run: %v", name, err)
			}
			if len(loaded.Visible) != 1 || loaded.Visible[0].Name != "alpha" {
				t.Fatalf("visible = %#v, want only the pinned bundle", loaded.Visible)
			}
			if len(loaded.Rejected) != 1 {
				t.Fatalf("rejected = %#v, want one quarantine report", loaded.Rejected)
			}
			if loaded.Rejected[0].Name != tc.wantName || !strings.Contains(loaded.Rejected[0].Reason, tc.wantReasonPart) {
				t.Fatalf("quarantine report = %#v, want %q naming %q", loaded.Rejected[0], tc.wantReasonPart, tc.wantName)
			}
		})
	}
}

func TestLoadForRunSymlinkedSkillDirectoryIsSkipped(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"outside/SKILL.md": "not a repository skill\n",
	})
	if err := os.MkdirAll(filepath.Join(dir, ".agents/skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "outside"), filepath.Join(dir, ".agents/skills/link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".agents/skills/stray.txt"), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "bundle body\n",
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	loaded, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// The symlinked directory is never followed and the stray file is not a
	// skill: only the pinned bundle is visible, and nothing is quarantined
	// for shapes that are not skills at all.
	if len(loaded.Visible) != 1 || loaded.Visible[0].Name != "alpha" {
		t.Fatalf("visible = %#v, want only the pinned bundle", loaded.Visible)
	}
	if len(loaded.Rejected) != 0 {
		t.Fatalf("rejected = %#v, want none", loaded.Rejected)
	}
}

func TestLoadForRunTooManyRepositorySkillsFailsClosed(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{}
	for i := 0; i <= maxRepoLocalSkills; i++ {
		files[".agents/skills/skill-"+string(rune('a'+i))+"/SKILL.md"] = "body\n"
	}
	writeLoaderTree(t, dir, files)
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "bundle body\n",
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	if _, err := LoadForRun(pins, config, dir); err == nil {
		t.Fatal("expected the repository-local count bound to fail closed")
	}
}

func TestLoadForRunDeterministic(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md":                "---\nname: alpha\ndescription: a\n---\nalpha body\n",
		"beta/SKILL.md":                 "beta body\n",
		".agents/skills/gamma/SKILL.md": "gamma body\n",
	})
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha", "beta": "beta"})
	pins := resolvePinned(t, config, "beta", "alpha")
	first, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	second, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if len(first.Visible) != len(second.Visible) {
		t.Fatalf("visible counts differ: %d vs %d", len(first.Visible), len(second.Visible))
	}
	for i := range first.Visible {
		a, b := first.Visible[i], second.Visible[i]
		if a.Name != b.Name || a.Origin != b.Origin || a.Digest != b.Digest ||
			a.Description != b.Description || a.Instructions != b.Instructions ||
			a.Source != b.Source || a.FilesBytes != b.FilesBytes ||
			strings.Join(a.Files, ",") != strings.Join(b.Files, ",") {
			t.Fatalf("view %d differs:\n%#v\n%#v", i, a, b)
		}
	}
	// Pins arrive unsorted; the view must not.
	if first.Visible[0].Name != "alpha" || first.Visible[1].Name != "beta" {
		t.Fatalf("visible order = %q, %q; want alpha, beta", first.Visible[0].Name, first.Visible[1].Name)
	}
}

func TestLoadForRunSymlinkBundleRootResolves(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"real/SKILL.md": "body\n",
	})
	if err := os.Symlink(filepath.Join(dir, "real"), filepath.Join(dir, "alpha")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	loaded, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// Source stays the registered path even though resolution followed a
	// root symlink; the worker mounts content at the registered path.
	if want := filepath.Join(dir, "alpha"); loaded.Visible[0].Source != want {
		t.Fatalf("source = %q, want the registered path %q", loaded.Visible[0].Source, want)
	}
}

func TestLoadForRunLoadsMaterializedBundle(t *testing.T) {
	dir := t.TempDir()
	mount := writeProjection(t, map[string]string{
		"SKILL.md":            "---\nname: alpha\ndescription: does things\n---\nBody with `$(rm -rf /)` stays data.\n",
		"assets/checklist.md": "check\n",
	})
	config := writeMaterializedConfig(t, dir, "alpha", mount, "SKILL.md", "assets/checklist.md")
	pins := resolvePinned(t, config, "alpha")
	loaded, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Visible) != 1 {
		t.Fatalf("visible = %d entries, want 1", len(loaded.Visible))
	}
	view := loaded.Visible[0]
	if view.Name != "alpha" || view.Origin != OriginBundle || view.Digest != pins[0].Digest {
		t.Fatalf("view identity = %#v", view)
	}
	// Source stays the registered mount path: the run's pods mount the
	// projection there.
	if view.Source != mount {
		t.Fatalf("source = %q, want the registered mount %q", view.Source, mount)
	}
	if view.Description != "does things" || !strings.Contains(view.Instructions, "$(rm -rf /)") {
		t.Fatalf("rendered material = %#v", view)
	}
	if len(view.Files) != 1 || view.Files[0] != "assets/checklist.md" {
		t.Fatalf("files = %#v, want the projected asset without SKILL.md", view.Files)
	}
	if view.FilesBytes != int64(len("check\n")) {
		t.Fatalf("filesBytes = %d", view.FilesBytes)
	}
}

func TestLoadForRunMaterializedDigestDriftFailsClosed(t *testing.T) {
	dir := t.TempDir()
	mount := writeProjection(t, map[string]string{
		"SKILL.md": "---\nname: alpha\ndescription: demo\n---\nBody.\n",
	})
	config := writeMaterializedConfig(t, dir, "alpha", mount, "SKILL.md")
	pins := resolvePinned(t, config, "alpha")
	// The projected content changes under the run: the recomputed digest no
	// longer matches the pin, so the run must fail closed like the tree form.
	if err := os.WriteFile(filepath.Join(mount, "..data", "SKILL.md"), []byte("---\nname: alpha\ndescription: changed\n---\nBody.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadForRun(pins, config, dir); err == nil || !strings.Contains(err.Error(), "pinned digest") {
		t.Fatalf("error = %v, want digest drift failure", err)
	}
}

func TestLoadForRunMaterializedInvalidPathFailsClosed(t *testing.T) {
	dir := t.TempDir()
	mount := writeProjection(t, map[string]string{"SKILL.md": "# skill\n"})
	config := writeMaterializedConfig(t, dir, "alpha", mount, "../escape.md")
	pins := []Pin{{Name: "alpha", Digest: "sha256:" + strings.Repeat("a", 64)}}
	_, err := LoadForRun(pins, config, dir)
	if err == nil {
		t.Fatal("expected a materialized path that escapes the mount to fail the run")
	}
	if !strings.Contains(err.Error(), "materialization file") {
		t.Fatalf("error = %v, want the offending path named", err)
	}
}

// writeQuotedMaterializedConfig writes a run config whose bundle reads its
// content from a projection, with the file list quoted so a path carrying
// a control character reaches the loader's own validation instead of the
// YAML reader's.
func writeQuotedMaterializedConfig(t *testing.T, dir, name, source string, quoted ...string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("bundles:\n")
	b.WriteString("- name: " + name + "\n")
	b.WriteString("  source: " + source + "\n")
	b.WriteString("  materialization:\n")
	b.WriteString("    files:\n")
	for _, file := range quoted {
		b.WriteString("    - " + file + "\n")
	}
	path := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// TestLoadForRunRejectsInvalidMaterializationPaths pins the resolution rule
// for a materialized file list: a path carrying a NUL byte, an absolute
// path, a ".." element, or a backslash can never resolve, so the run fails
// closed before the loader reads anything that could sit outside the mount.
func TestLoadForRunRejectsInvalidMaterializationPaths(t *testing.T) {
	cases := map[string]struct {
		path   string
		quoted string
	}{
		"nul byte":  {path: "SKILL.md\x00", quoted: `"SKILL.md\0"`},
		"absolute":  {path: "/etc/passwd", quoted: `"/etc/passwd"`},
		"parent":    {path: "../escape.md", quoted: `"../escape.md"`},
		"backslash": {path: `assets\checklist.md`, quoted: `'assets\checklist.md'`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			mount := writeProjection(t, map[string]string{"SKILL.md": "# skill\n"})
			// The resolution itself refuses the path...
			spec := bundleSpec{
				Name:            "alpha",
				Source:          mount,
				Materialization: &materializationSpec{Files: []string{tc.path}},
			}
			if _, err := resolveMaterialized(spec); err == nil {
				t.Fatalf("path %q: expected the resolution to fail closed", tc.path)
			}
			// ...and so does the whole load.
			config := writeQuotedMaterializedConfig(t, dir, "alpha", mount, tc.quoted)
			pins := []Pin{{Name: "alpha", Digest: "sha256:" + strings.Repeat("a", 64)}}
			if _, err := LoadForRun(pins, config, dir); err == nil {
				t.Fatalf("path %q: expected the load to fail closed", tc.path)
			}
		})
	}
}

// TestLoadForRunRejectsNULByteConfigPath pins the NUL-byte rule at the
// config-file boundary: a bundle config path carrying a NUL byte is not a
// path the operating system can open, so the run fails closed with an
// explicit error instead of loading anything.
func TestLoadForRunRejectsNULByteConfigPath(t *testing.T) {
	pins := []Pin{{Name: "alpha", Digest: "sha256:" + strings.Repeat("a", 64)}}
	_, err := LoadForRun(pins, "/etc/courier/skill-bundles-config/\x00bundles.yaml", t.TempDir())
	if err == nil {
		t.Fatal("a config path carrying a NUL byte must fail closed")
	}
	if !strings.Contains(err.Error(), "read skill bundle config") {
		t.Fatalf("error = %v, want the config read failure named", err)
	}
}

// TestLoadForRunRejectsSymlinkedMaterializedFiles pins the projection
// rule: a materialized file that is a symlink out of the mount, or a
// symlink to a directory, is rejected — the loader never follows a link the
// projection could use to steer a read.
func TestLoadForRunRejectsSymlinkedMaterializedFiles(t *testing.T) {
	t.Run("escaping the mount", func(t *testing.T) {
		dir := t.TempDir()
		outside := t.TempDir()
		if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("credential material\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		mount := t.TempDir()
		if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(mount, "SKILL.md")); err != nil {
			t.Fatal(err)
		}
		config := writeMaterializedConfig(t, dir, "alpha", mount, "SKILL.md")
		pins := []Pin{{Name: "alpha", Digest: "sha256:" + strings.Repeat("a", 64)}}
		_, err := LoadForRun(pins, config, dir)
		if err == nil || !strings.Contains(err.Error(), "resolves outside the bundle mount") {
			t.Fatalf("error = %v, want the escape rejected", err)
		}
	})

	t.Run("pointing at a directory", func(t *testing.T) {
		dir := t.TempDir()
		mount := writeProjection(t, map[string]string{"SKILL.md": "# skill\n"})
		if err := os.MkdirAll(filepath.Join(mount, "sub"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(mount, "sub"), filepath.Join(mount, "assets")); err != nil {
			t.Fatal(err)
		}
		config := writeMaterializedConfig(t, dir, "alpha", mount, "SKILL.md", "assets")
		pins := []Pin{{Name: "alpha", Digest: "sha256:" + strings.Repeat("a", 64)}}
		_, err := LoadForRun(pins, config, dir)
		if err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("error = %v, want the directory symlink rejected", err)
		}
	})
}

// TestLoadForRunRejectsBundleWithInteriorSymlink pins the plain-tree rule
// at the load boundary: a bundle source carrying an interior symlink never
// resolves, so the pinned content can never come from a tree that points
// out of itself.
func TestLoadForRunRejectsBundleWithInteriorSymlink(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "# skill\n",
	})
	if err := os.Symlink(filepath.Join(dir, "alpha", "SKILL.md"), filepath.Join(dir, "alpha", "alias.md")); err != nil {
		t.Fatal(err)
	}
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := []Pin{{Name: "alpha", Digest: "sha256:" + strings.Repeat("a", 64)}}
	_, err := LoadForRun(pins, config, dir)
	if err == nil || !strings.Contains(err.Error(), "plain directory trees") {
		t.Fatalf("error = %v, want the interior symlink rejected", err)
	}
}

// TestLoadForRunQuarantinesSymlinkedRepositorySkill pins the
// repository-local rule: a SKILL.md that is a symlink is quarantined and
// reported, and its target's content never reaches the visible view —
// untrusted repository content can neither park a pinned run nor smuggle
// content into it.
func TestLoadForRunQuarantinesSymlinkedRepositorySkill(t *testing.T) {
	dir := t.TempDir()
	writeLoaderTree(t, dir, map[string]string{
		"alpha/SKILL.md": "bundle body\n",
	})
	if err := os.MkdirAll(filepath.Join(dir, ".agents/skills/leak"), 0o755); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(dir, "secret.txt")
	if err := os.WriteFile(secret, []byte("credential material\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(dir, ".agents/skills/leak/SKILL.md")); err != nil {
		t.Fatal(err)
	}
	config := writeLoaderConfig(t, dir, map[string]string{"alpha": "alpha"})
	pins := resolvePinned(t, config, "alpha")
	loaded, err := LoadForRun(pins, config, dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Visible) != 1 || loaded.Visible[0].Name != "alpha" {
		t.Fatalf("visible = %#v, want only the pinned bundle", loaded.Visible)
	}
	if len(loaded.Rejected) != 1 || loaded.Rejected[0].Name != "leak" {
		t.Fatalf("rejected = %#v, want one quarantine report for the symlinked skill", loaded.Rejected)
	}
	if !strings.Contains(loaded.Rejected[0].Reason, "not a regular file") {
		t.Fatalf("quarantine reason = %q, want the non-regular-file rule named", loaded.Rejected[0].Reason)
	}
	for _, view := range loaded.Visible {
		if strings.Contains(view.Instructions, "credential material") {
			t.Fatal("the symlinked repository content was loaded")
		}
	}
}
