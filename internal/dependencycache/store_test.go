package dependencycache

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newTestStore populates a store root with approved module versions and
// returns the store plus the root path.
func newTestStore(t *testing.T, limits Limits, populate func(root string)) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	populate(root)
	store, err := NewStoreWithLimits(root, limits)
	if err != nil {
		t.Fatalf("NewStoreWithLimits: %v", err)
	}
	return store, root
}

// writeArtifact writes one artifact file in wire layout. Test module paths
// and versions are lowercase, so the escaped on-disk names equal the plain
// ones; uppercase escaping is exercised through the handler's canonical
// checks.
func writeArtifact(t *testing.T, root, modulePath, version, ext, body string) {
	t.Helper()
	dir := filepath.Join(root, modulePath, "@v")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, version+ext), []byte(body), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
}

func approveAll(t *testing.T, root, modulePath, version string) {
	t.Helper()
	writeArtifact(t, root, modulePath, version, ExtInfo, `{"Version":"`+version+`","Time":"2026-01-02T03:04:05Z"}`)
	writeArtifact(t, root, modulePath, version, ExtMod, "module "+modulePath+"\n\ngo 1.21\n")
	writeArtifact(t, root, modulePath, version, ExtZip, "zip-bytes")
}

func TestStoreOpenServesCompleteArtifactSets(t *testing.T) {
	store, _ := newTestStore(t, DefaultLimits(), func(root string) {
		approveAll(t, root, "example.com/hello", "v1.0.0")
	})
	for _, ext := range ArtifactExts {
		file, size, err := store.Open("example.com/hello", "v1.0.0", ext)
		if err != nil {
			t.Fatalf("Open(%s): %v", ext, err)
		}
		info, err := file.Stat()
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if info.Size() != size || size == 0 {
			t.Fatalf("Open(%s) size mismatch: reported %d, stat %d", ext, size, info.Size())
		}
		_ = file.Close()
	}
}

func TestStoreOpenRefusesIncompleteSets(t *testing.T) {
	store, _ := newTestStore(t, DefaultLimits(), func(root string) {
		writeArtifact(t, root, "example.com/partial", "v1.0.0", ExtInfo, "{}")
		writeArtifact(t, root, "example.com/partial", "v1.0.0", ExtMod, "module example.com/partial\n")
	})
	_, _, err := store.Open("example.com/partial", "v1.0.0", ExtZip)
	var notApproved *NotApprovedError
	if !asNotApproved(err, &notApproved) {
		t.Fatalf("Open on incomplete set: want NotApprovedError, got %v", err)
	}
	if !strings.Contains(notApproved.Detail, ".zip") {
		t.Fatalf("diagnostic must name the missing artifact, got %q", notApproved.Detail)
	}
}

func TestStoreOpenRefusesVersionsOutsideTheBound(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxZipBytes = 4
	store, _ := newTestStore(t, limits, func(root string) {
		approveAll(t, root, "example.com/big", "v1.0.0")
	})
	_, _, err := store.Open("example.com/big", "v1.0.0", ExtZip)
	var notApproved *NotApprovedError
	if !asNotApproved(err, &notApproved) {
		t.Fatalf("Open oversized artifact: want NotApprovedError, got %v", err)
	}
	if !strings.Contains(notApproved.Detail, "bound") {
		t.Fatalf("diagnostic must name the bound, got %q", notApproved.Detail)
	}
}

func TestStoreVersionsListOnlyCompleteSets(t *testing.T) {
	store, _ := newTestStore(t, DefaultLimits(), func(root string) {
		approveAll(t, root, "example.com/multi", "v1.0.0")
		approveAll(t, root, "example.com/multi", "v1.1.0")
		writeArtifact(t, root, "example.com/multi", "v1.2.0", ExtInfo, "{}")
	})
	versions, err := store.Versions("example.com/multi")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(versions) != 2 || versions[0].Version != "v1.0.0" || versions[1].Version != "v1.1.0" {
		t.Fatalf("want semver-sorted complete sets only, got %+v", versions)
	}
	if versions[1].Time != "2026-01-02T03:04:05Z" {
		t.Fatalf("want the populated .info timestamp, got %q", versions[1].Time)
	}
}

func TestStoreVersionsRefusesUnknownModule(t *testing.T) {
	store, _ := newTestStore(t, DefaultLimits(), func(root string) {})
	_, err := store.Versions("example.com/absent")
	var notApproved *NotApprovedError
	if !asNotApproved(err, &notApproved) {
		t.Fatalf("Versions on absent module: want NotApprovedError, got %v", err)
	}
}

func TestStoreLatestPrefersNonPseudoVersions(t *testing.T) {
	store, _ := newTestStore(t, DefaultLimits(), func(root string) {
		approveAll(t, root, "example.com/mixed", "v0.0.0-20260101000000-000000000001")
		approveAll(t, root, "example.com/mixed", "v0.9.0")
	})
	latest, err := store.Latest("example.com/mixed")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.Version != "v0.9.0" {
		t.Fatalf("want newest non-pseudo version, got %q", latest.Version)
	}
	if !strings.Contains(latest.LatestJSON(), `"Version":"v0.9.0"`) ||
		!strings.Contains(latest.LatestJSON(), `"Time":"2026-01-02T03:04:05Z"`) {
		t.Fatalf("@latest JSON shape unexpected: %s", latest.LatestJSON())
	}
}

func TestNewStoreRequiresADirectory(t *testing.T) {
	if _, err := NewStore(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("NewStore on an absent root must fail")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(file); err == nil {
		t.Fatal("NewStore on a file root must fail")
	}
}

func asNotApproved(err error, target **NotApprovedError) bool {
	if err == nil {
		return false
	}
	approved, ok := err.(*NotApprovedError)
	if !ok {
		return false
	}
	*target = approved
	return true
}
