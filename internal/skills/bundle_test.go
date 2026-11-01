package skills

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// writeTree materializes a plain directory tree under dir, one file per map
// entry keyed by slash-separated relative path.
func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
}

// materialize writes a tree into a fresh temp dir and returns its path.
func materialize(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	writeTree(t, dir, files)
	return dir
}

// writeProjection lays out a kubelet-style projection: the mount root
// carries a ..data directory with the real content plus one symlink per
// projected file pointing into it, the way kubelet materializes a Secret
// into a pod.
func writeProjection(t *testing.T, files map[string]string) string {
	t.Helper()
	mount := t.TempDir()
	data := filepath.Join(mount, "..data")
	for rel, content := range files {
		full := filepath.Join(data, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	for rel := range files {
		link := filepath.Join(mount, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", rel, err)
		}
		target, err := filepath.Rel(filepath.Dir(link), filepath.Join(data, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("relative target for %s: %v", rel, err)
		}
		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("symlink %s: %v", rel, err)
		}
	}
	return mount
}

func resolveOK(t *testing.T, spec bundleSpec) Bundle {
	t.Helper()
	bundle, err := Resolve(spec)
	if err != nil {
		t.Fatalf("Resolve rejected a valid bundle: %v", err)
	}
	return bundle
}

func resolveMaterializedOK(t *testing.T, spec bundleSpec) Bundle {
	t.Helper()
	bundle, err := resolveMaterialized(spec)
	if err != nil {
		t.Fatalf("resolveMaterialized rejected a valid bundle: %v", err)
	}
	return bundle
}

func TestResolveRequiresSKILLMD(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"notes/a.md": "x"})
	_, err := Resolve(bundleSpec{Name: "b", Source: dir})
	if err == nil || !strings.Contains(err.Error(), "SKILL.md is required") {
		t.Fatalf("expected missing-SKILL.md rejection, got %v", err)
	}
}

func TestResolveRejectsEmptySKILLMD(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"SKILL.md": ""})
	_, err := Resolve(bundleSpec{Name: "b", Source: dir})
	if err == nil || !strings.Contains(err.Error(), "SKILL.md is empty") {
		t.Fatalf("expected empty-SKILL.md rejection, got %v", err)
	}
}

func TestResolveMissingSourceDir(t *testing.T) {
	_, err := Resolve(bundleSpec{Name: "b", Source: "/nonexistent/courier-bundle-test"})
	if err == nil || !strings.Contains(err.Error(), "resolve source ") {
		t.Fatalf("expected missing-source failure, got %v", err)
	}
}

func TestResolveRejectsFileAsSource(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"SKILL.md": "x"})
	_, err := Resolve(bundleSpec{Name: "b", Source: filepath.Join(dir, "SKILL.md")})
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("expected not-a-directory rejection, got %v", err)
	}
}

func TestResolveRejectsSymlinkFileInsideTree(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"SKILL.md": "# skill\n"})
	if err := os.Symlink(filepath.Join(dir, "SKILL.md"), filepath.Join(dir, "alias.md")); err != nil {
		t.Fatal(err)
	}
	_, err := Resolve(bundleSpec{Name: "b", Source: dir})
	if err == nil || !strings.Contains(err.Error(), "symlink alias.md: bundles must be plain directory trees") {
		t.Fatalf("expected symlink-file rejection, got %v", err)
	}
}

func TestResolveRejectsSymlinkDirInsideTree(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"SKILL.md": "# skill\n", "assets/a.txt": "a"})
	if err := os.Symlink(filepath.Join(dir, "assets"), filepath.Join(dir, "assets-link")); err != nil {
		t.Fatal(err)
	}
	_, err := Resolve(bundleSpec{Name: "b", Source: dir})
	if err == nil || !strings.Contains(err.Error(), "symlink assets-link: bundles must be plain directory trees") {
		t.Fatalf("expected symlink-dir rejection, got %v", err)
	}
}

func TestResolveRejectsSymlinkEscape(t *testing.T) {
	outside := t.TempDir()
	writeTree(t, outside, map[string]string{"SKILL.md": "outside\n"})
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"SKILL.md": "# skill\n"})
	if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}
	_, err := Resolve(bundleSpec{Name: "b", Source: dir})
	if err == nil || !strings.Contains(err.Error(), "symlink escape: bundles must be plain directory trees") {
		t.Fatalf("expected symlink-escape rejection, got %v", err)
	}
}

func TestResolveRejectsFifo(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"SKILL.md": "# skill\n"})
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
		t.Skipf("cannot create fifo: %v", err)
	}
	_, err := Resolve(bundleSpec{Name: "b", Source: dir})
	if err == nil || !strings.Contains(err.Error(), "pipe: not a regular file") {
		t.Fatalf("expected fifo rejection, got %v", err)
	}
}

func TestResolveCountsFilesAndBytes(t *testing.T) {
	bundle := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{
		"SKILL.md":      "12345",
		"assets/a.txt":  "abc",
		"assets/b.txt":  "de",
		"assets/sub.md": "z",
	})})
	if bundle.Files != 4 {
		t.Fatalf("Files = %d, want 4", bundle.Files)
	}
	if want := int64(5 + 3 + 2 + 1); bundle.Bytes != want {
		t.Fatalf("Bytes = %d, want %d", bundle.Bytes, want)
	}
}

func TestResolveRejectsFileCountBound(t *testing.T) {
	files := map[string]string{"SKILL.md": "# skill\n"}
	for i := 0; i <= maxBundleFiles; i++ {
		files["assets/f"+strconv.Itoa(i)] = "x"
	}
	_, err := Resolve(bundleSpec{Name: "b", Source: materialize(t, files)})
	if err == nil || !strings.Contains(err.Error(), "bundle exceeds 512 file limit") {
		t.Fatalf("expected file-count bound rejection, got %v", err)
	}
}

func TestResolveAcceptsAtFileCountBound(t *testing.T) {
	files := map[string]string{"SKILL.md": "# skill\n"}
	for i := 0; i < maxBundleFiles-1; i++ {
		files["assets/f"+strconv.Itoa(i)] = "x"
	}
	bundle := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, files)})
	if bundle.Files != maxBundleFiles {
		t.Fatalf("Files = %d, want %d", bundle.Files, maxBundleFiles)
	}
}

func TestResolveRejectsByteBound(t *testing.T) {
	files := map[string]string{"SKILL.md": "# skill\n"}
	for i := 0; i < 9; i++ {
		files["assets/big"+strconv.Itoa(i)] = strings.Repeat("x", 1<<20)
	}
	_, err := Resolve(bundleSpec{Name: "b", Source: materialize(t, files)})
	if err == nil || !strings.Contains(err.Error(), "bundle exceeds 8388608 byte limit") {
		t.Fatalf("expected byte bound rejection, got %v", err)
	}
}

func TestResolveRejectsOversizedSingleFileBeforeReading(t *testing.T) {
	_, err := Resolve(bundleSpec{Name: "b", Source: materialize(t, map[string]string{
		"SKILL.md": strings.Repeat("x", maxBundleBytes+1),
	})})
	if err == nil || !strings.Contains(err.Error(), "SKILL.md: file exceeds 8388608 byte limit") {
		t.Fatalf("expected per-file bound rejection for oversized SKILL.md, got %v", err)
	}
}

func TestDigestFormat(t *testing.T) {
	bundle := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n"})})
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(bundle.Digest) {
		t.Fatalf("digest %q does not match sha256:<64 lowercase hex>", bundle.Digest)
	}
}

func TestDigestStableAcrossLocations(t *testing.T) {
	files := map[string]string{"SKILL.md": "# skill\n", "assets/a.txt": "same\n"}
	one := resolveOK(t, bundleSpec{Name: "one", Source: materialize(t, files)})
	two := resolveOK(t, bundleSpec{Name: "two", Source: materialize(t, files)})
	if one.Digest != two.Digest {
		t.Fatalf("equal content in different dirs digested differently: %s vs %s", one.Digest, two.Digest)
	}
}

func TestDigestChangesOnContentChange(t *testing.T) {
	before := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n", "assets/a.txt": "same\n"})})
	changed := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n", "assets/a.txt": "diff\n"})})
	if before.Digest == changed.Digest {
		t.Fatal("content change did not change digest")
	}
}

func TestDigestChangesOnAdd(t *testing.T) {
	before := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n"})})
	after := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n", "extra.txt": "x"})})
	if before.Digest == after.Digest {
		t.Fatal("added file did not change digest")
	}
}

func TestDigestChangesOnRename(t *testing.T) {
	before := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n", "a.txt": "x"})})
	renamed := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n", "b.txt": "x"})})
	if before.Digest == renamed.Digest {
		t.Fatal("rename did not change digest")
	}
}

// configMapProjection lays out the kubelet ConfigMap-projection shape the
// design calls out: the registered source root is a symlink whose resolved
// target is the real content directory carrying a ..<timestamp> name, the
// way kubelet repoints ..data at a fresh ..<timestamp> dir on every mount.
func configMapProjection(t *testing.T, contentDir, timestampName string) string {
	t.Helper()
	holder := t.TempDir()
	timestamped := filepath.Join(holder, timestampName)
	if err := os.MkdirAll(filepath.Dir(timestamped), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(contentDir, timestamped); err != nil {
		t.Fatal(err)
	}
	mount := t.TempDir()
	if err := os.Symlink(timestamped, filepath.Join(mount, "..data")); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(mount, "..data")
}

func TestResolveRootSymlinkConfigMapStyle(t *testing.T) {
	// Kubelet ConfigMap projection layout: the source root is a symlink
	// whose target carries the ..<timestamp> name. EvalSymlinks must land
	// on the real content dir — the timestamp lives only in the resolved
	// root path, never in relative paths — and a simulated kubelet remount
	// (fresh ..<timestamp> directory behind the same symlink shape) must
	// leave the digest unchanged.
	first := configMapProjection(t, materialize(t, map[string]string{"SKILL.md": "# skill\n", "assets/a.txt": "a"}), "..data-old")
	bundle := resolveOK(t, bundleSpec{Name: "b", Source: first})
	if bundle.Files != 2 || bundle.Bytes != int64(len("# skill\n")+len("a")) {
		t.Fatalf("unexpected counts: files=%d bytes=%d", bundle.Files, bundle.Bytes)
	}
	if !strings.Contains(filepath.Base(bundle.Path), "..data-old") {
		t.Fatalf("resolution did not land on the ..<timestamp> content dir: %s", bundle.Path)
	}

	second := configMapProjection(t, materialize(t, map[string]string{"SKILL.md": "# skill\n", "assets/a.txt": "a"}), "..data-new")
	again := resolveOK(t, bundleSpec{Name: "b", Source: second})
	if again.Digest != bundle.Digest {
		t.Fatalf("digest not stable across simulated remount: %s vs %s", bundle.Digest, again.Digest)
	}
	if !strings.Contains(filepath.Base(again.Path), "..data-new") {
		t.Fatalf("remount did not resolve to the new content dir: %s", again.Path)
	}
}

func TestRegistryLookupBundlesNames(t *testing.T) {
	one := materialize(t, map[string]string{"SKILL.md": "one\n"})
	two := materialize(t, map[string]string{"SKILL.md": "two\n"})
	zeta, err := Resolve(bundleSpec{Name: "zeta", Source: one})
	if err != nil {
		t.Fatal(err)
	}
	alpha, err := Resolve(bundleSpec{Name: "alpha", Source: two})
	if err != nil {
		t.Fatal(err)
	}
	registry := newRegistry([]Bundle{zeta, alpha})
	if names := registry.Names(); strings.Join(names, ",") != "alpha,zeta" {
		t.Fatalf("Names = %v, want sorted [alpha zeta]", names)
	}
	bundles := registry.Bundles()
	if len(bundles) != 2 || bundles[0].Name != "alpha" || bundles[1].Name != "zeta" {
		t.Fatalf("Bundles not sorted by name: %#v", bundles)
	}
	if bundle, ok := registry.Lookup("alpha"); !ok || bundle.Name != "alpha" || bundle.Source != two {
		t.Fatalf("Lookup(alpha) = %#v, %v", bundle, ok)
	}
	if _, ok := registry.Lookup("absent"); ok {
		t.Fatal("Lookup(absent) unexpectedly succeeded")
	}
	// Bundles returns a copy: mutating it must not affect the registry.
	bundles[0].Name = "mutated"
	if again, _ := registry.Lookup("alpha"); again.Name != "alpha" {
		t.Fatal("Bundles() exposed registry-internal state")
	}
}

func TestMaterializedAndTreeFormsDigestIdentically(t *testing.T) {
	files := map[string]string{
		"SKILL.md":            "---\nname: b\ndescription: demo\n---\nBody.\n",
		"assets/checklist.md": "check\n",
	}
	tree := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, files)})
	projected := resolveMaterializedOK(t, bundleSpec{
		Name:   "b",
		Source: writeProjection(t, files),
		Materialization: &materializationSpec{
			Files: []string{"SKILL.md", "assets/checklist.md"},
		},
	})
	// The two forms carry the same content, so they must carry the same
	// content identity: a digest pinned over the deployment's mount has to
	// verify against the projection the run's pods were handed.
	if tree.Digest != projected.Digest {
		t.Fatalf("materialized digest %s does not match the tree digest %s for the same content", projected.Digest, tree.Digest)
	}
	if projected.Files != tree.Files || projected.Bytes != tree.Bytes {
		t.Fatalf("materialized counts = files %d bytes %d, want files %d bytes %d", projected.Files, projected.Bytes, tree.Files, tree.Bytes)
	}
	if projected.Name != "b" {
		t.Fatalf("materialized name = %q, want b", projected.Name)
	}
}

func TestMaterializedDigestIgnoresListOrder(t *testing.T) {
	files := map[string]string{"SKILL.md": "# skill\n", "assets/a.txt": "a\n"}
	mount := writeProjection(t, files)
	forward := resolveMaterializedOK(t, bundleSpec{
		Name:            "b",
		Source:          mount,
		Materialization: &materializationSpec{Files: []string{"SKILL.md", "assets/a.txt"}},
	})
	reverse := resolveMaterializedOK(t, bundleSpec{
		Name:            "b",
		Source:          mount,
		Materialization: &materializationSpec{Files: []string{"assets/a.txt", "SKILL.md"}},
	})
	if forward.Digest != reverse.Digest {
		t.Fatalf("list order changed the digest: %s vs %s", forward.Digest, reverse.Digest)
	}
}

func TestMaterializedListIsAuthoritative(t *testing.T) {
	mount := writeProjection(t, map[string]string{
		"SKILL.md": "# skill\n",
		"extra.md": "not listed\n",
	})
	bundle := resolveMaterializedOK(t, bundleSpec{
		Name:            "b",
		Source:          mount,
		Materialization: &materializationSpec{Files: []string{"SKILL.md"}},
	})
	// The projection carries a file the list omits; the loader never walks
	// the mount, so that file is not part of the bundle's content.
	if bundle.Files != 1 || bundle.Bytes != int64(len("# skill\n")) {
		t.Fatalf("counts = files %d bytes %d, want only the listed file", bundle.Files, bundle.Bytes)
	}
	tree := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{"SKILL.md": "# skill\n"})})
	if tree.Digest != bundle.Digest {
		t.Fatalf("digest %s covers more than the listed files (tree digest of the listed file only is %s)", bundle.Digest, tree.Digest)
	}
}

func TestMaterializedRejectsInvalidRelativePaths(t *testing.T) {
	mount := writeProjection(t, map[string]string{"SKILL.md": "# skill\n"})
	cases := map[string]string{
		"absolute":      "/etc/passwd",
		"parent":        "../escape.md",
		"nested parent": "assets/../../escape.md",
		"current":       "./SKILL.md",
		"empty":         "",
		"nul byte":      "SKILL.md\x00",
		"backslash":     `assets\checklist.md`,
	}
	for name, rel := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := resolveMaterialized(bundleSpec{
				Name:            "b",
				Source:          mount,
				Materialization: &materializationSpec{Files: []string{rel}},
			})
			if err == nil || !strings.Contains(err.Error(), "materialization file") {
				t.Fatalf("path %q: expected fail-closed rejection, got %v", rel, err)
			}
		})
	}
}

func TestMaterializedRejectsSymlinkEscape(t *testing.T) {
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("credential material\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mount := t.TempDir()
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(mount, "SKILL.md")); err != nil {
		t.Fatal(err)
	}
	_, err := resolveMaterialized(bundleSpec{
		Name:            "b",
		Source:          mount,
		Materialization: &materializationSpec{Files: []string{"SKILL.md"}},
	})
	if err == nil || !strings.Contains(err.Error(), "resolves outside the bundle mount") {
		t.Fatalf("expected escape rejection, got %v", err)
	}
}

func TestMaterializedRequiresNonEmptySKILLMD(t *testing.T) {
	mount := writeProjection(t, map[string]string{"assets/a.txt": "a\n"})
	_, err := resolveMaterialized(bundleSpec{
		Name:            "b",
		Source:          mount,
		Materialization: &materializationSpec{Files: []string{"assets/a.txt"}},
	})
	if err == nil || !strings.Contains(err.Error(), "SKILL.md is required") {
		t.Fatalf("expected missing-SKILL.md rejection, got %v", err)
	}
	empty := writeProjection(t, map[string]string{"SKILL.md": ""})
	_, err = resolveMaterialized(bundleSpec{
		Name:            "b",
		Source:          empty,
		Materialization: &materializationSpec{Files: []string{"SKILL.md"}},
	})
	if err == nil || !strings.Contains(err.Error(), "SKILL.md is empty") {
		t.Fatalf("expected empty-SKILL.md rejection, got %v", err)
	}
}

func TestMaterializedFilesReturnsRegisteredContentAsCopy(t *testing.T) {
	bundle := resolveOK(t, bundleSpec{Name: "b", Source: materialize(t, map[string]string{
		"SKILL.md": "# skill\n",
		"a.txt":    "a\n",
	})})
	files := bundle.MaterializedFiles()
	if len(files) != 2 {
		t.Fatalf("files = %d entries, want 2", len(files))
	}
	if files[0].Path != "SKILL.md" || string(files[0].Content) != "# skill\n" {
		t.Fatalf("files[0] = %#v, want SKILL.md with its exact bytes", files[0])
	}
	if files[1].Path != "a.txt" || string(files[1].Content) != "a\n" {
		t.Fatalf("files[1] = %#v, want a.txt with its exact bytes", files[1])
	}
	// The accessor hands out copies: a caller that mutates them must not
	// reach the registry's bytes.
	files[0].Content[0] = 'X'
	files[1].Path = "mutated"
	again := bundle.MaterializedFiles()
	if string(again[0].Content) != "# skill\n" || again[1].Path != "a.txt" {
		t.Fatalf("MaterializedFiles() exposed the bundle's own bytes: %#v", again)
	}
}
