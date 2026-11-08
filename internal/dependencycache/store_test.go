package dependencycache

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
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

func TestStoreLatestFollowsGoPreferenceOrder(t *testing.T) {
	// The go command resolves module@latest by preferring the highest
	// release, then the highest prerelease, then the chronologically newest
	// pseudo-version. The pseudo step is mandatory: the client filters
	// pseudo-versions out of @v/list, so @latest is its only resolution path
	// for tagless modules.
	populatePseudo := func(root, version, when string) {
		writeArtifact(t, root, "example.com/tagless", version, ExtInfo, fmt.Sprintf(`{"Version":%q,"Time":%q}`, version, when))
		writeArtifact(t, root, "example.com/tagless", version, ExtMod, "module example.com/tagless\n\ngo 1.21\n")
		writeArtifact(t, root, "example.com/tagless", version, ExtZip, "zip-bytes")
	}

	t.Run("highest release beats prerelease and pseudo", func(t *testing.T) {
		store, _ := newTestStore(t, DefaultLimits(), func(root string) {
			approveAll(t, root, "example.com/mixed", "v0.0.0-20250101000000-aaaaaaaaaaaa")
			approveAll(t, root, "example.com/mixed", "v0.9.0-rc.1")
			approveAll(t, root, "example.com/mixed", "v1.0.0")
			approveAll(t, root, "example.com/mixed", "v1.2.0")
		})
		latest, err := store.Latest("example.com/mixed")
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if latest.Version != "v1.2.0" {
			t.Fatalf("want the highest release v1.2.0, got %q", latest.Version)
		}
	})

	t.Run("highest prerelease when no release is populated", func(t *testing.T) {
		store, _ := newTestStore(t, DefaultLimits(), func(root string) {
			approveAll(t, root, "example.com/mixed", "v0.0.0-20250101000000-aaaaaaaaaaaa")
			approveAll(t, root, "example.com/mixed", "v0.8.0-rc.1")
			approveAll(t, root, "example.com/mixed", "v0.9.0-rc.2")
		})
		latest, err := store.Latest("example.com/mixed")
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if latest.Version != "v0.9.0-rc.2" {
			t.Fatalf("want the highest prerelease v0.9.0-rc.2, got %q", latest.Version)
		}
	})

	t.Run("newest pseudo-version by .info time", func(t *testing.T) {
		store, _ := newTestStore(t, DefaultLimits(), func(root string) {
			// The populated .info time decides, not the timestamp embedded in
			// the pseudo-version: the older pseudo carries the later .info
			// time and must win.
			populatePseudo(root, "v0.0.0-20250101000000-aaaaaaaaaaaa", "2027-01-01T00:00:00Z")
			populatePseudo(root, "v0.0.0-20260101000000-bbbbbbbbbbbb", "2026-03-01T00:00:00Z")
		})
		latest, err := store.Latest("example.com/tagless")
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if latest.Version != "v0.0.0-20250101000000-aaaaaaaaaaaa" {
			t.Fatalf("want the newest .info-time pseudo-version, got %q", latest.Version)
		}
	})

	t.Run("pseudo-version without a parseable .info time falls back to the embedded time", func(t *testing.T) {
		store, _ := newTestStore(t, DefaultLimits(), func(root string) {
			populatePseudo(root, "v0.0.0-20250101000000-aaaaaaaaaaaa", "")
			populatePseudo(root, "v0.0.0-20260101000000-bbbbbbbbbbbb", "")
		})
		latest, err := store.Latest("example.com/tagless")
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if latest.Version != "v0.0.0-20260101000000-bbbbbbbbbbbb" {
			t.Fatalf("want the newest embedded-time pseudo-version, got %q", latest.Version)
		}
	})
}

func TestStoreRefusesSymlinkedStorePaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires privileges on windows")
	}
	// A populated symlink under an approved-looking module/version must never
	// serve a file the administrator did not place in the store — inside the
	// root or outside it. The element walk refuses the symlink before any
	// read, so no test here may observe outside content. The outside
	// directory holds a complete artifact set so that any following of the
	// links would succeed rather than fail into a look-alike error.
	secretDir := t.TempDir()
	secretInfo := filepath.Join(secretDir, "v1.0.0.info")
	secretMod := filepath.Join(secretDir, "v1.0.0.mod")
	secretZip := filepath.Join(secretDir, "v1.0.0.zip")
	for _, artifact := range []struct{ path, body string }{
		{secretInfo, `{"Version":"v1.0.0","Time":"2020-01-01T00:00:00Z"}`},
		{secretMod, "module example.com/linked\n"},
		{secretZip, "outside-zip-bytes"},
	} {
		if err := os.WriteFile(artifact.path, []byte(artifact.body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("module directory symlink escapes the store", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "example.com"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secretDir, filepath.Join(root, "example.com", "linked")); err != nil {
			t.Fatal(err)
		}
		store, err := NewStore(root)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}

		var notApproved *NotApprovedError
		if _, _, err := store.Open("example.com/linked", "v1.0.0", ExtInfo); !asNotApproved(err, &notApproved) {
			t.Fatalf("Open through a module-dir symlink: want NotApprovedError, got %v", err)
		} else if !strings.Contains(notApproved.Detail, "symlink") {
			t.Fatalf("diagnostic must name the symlink, got %q", notApproved.Detail)
		}
		if _, err := store.Versions("example.com/linked"); !asNotApproved(err, &notApproved) {
			t.Fatalf("Versions through a module-dir symlink: want NotApprovedError, got %v", err)
		} else if !strings.Contains(notApproved.Detail, "symlink") {
			t.Fatalf("diagnostic must name the symlink, got %q", notApproved.Detail)
		}
	})

	t.Run("@v directory symlink escapes the store", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "example.com", "real"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secretDir, filepath.Join(root, "example.com", "real", "@v")); err != nil {
			t.Fatal(err)
		}
		store, err := NewStore(root)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}

		var notApproved *NotApprovedError
		if _, _, err := store.Open("example.com/real", "v1.0.0", ExtInfo); !asNotApproved(err, &notApproved) {
			t.Fatalf("Open through an @v symlink: want NotApprovedError, got %v", err)
		}
		if _, err := store.Versions("example.com/real"); !asNotApproved(err, &notApproved) {
			t.Fatalf("Versions through an @v symlink: want NotApprovedError, got %v", err)
		}
	})

	t.Run("artifact symlink escapes the store", func(t *testing.T) {
		root := t.TempDir()
		approveAll(t, root, "example.com/real", "v1.0.0")
		if err := os.Remove(filepath.Join(root, "example.com", "real", "@v", "v1.0.0.zip")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secretZip, filepath.Join(root, "example.com", "real", "@v", "v1.0.0.zip")); err != nil {
			t.Fatal(err)
		}
		store, err := NewStore(root)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}

		var notApproved *NotApprovedError
		if _, _, err := store.Open("example.com/real", "v1.0.0", ExtZip); !asNotApproved(err, &notApproved) {
			t.Fatalf("Open of a symlinked artifact: want NotApprovedError, got %v", err)
		} else if !strings.Contains(notApproved.Detail, "v1.0.0.zip") {
			t.Fatalf("diagnostic must name the offending artifact, got %q", notApproved.Detail)
		}
		// The whole version is tainted, not just the linked artifact: a set
		// holding a symlink is not an approved artifact set.
		if _, _, err := store.Open("example.com/real", "v1.0.0", ExtInfo); !asNotApproved(err, &notApproved) {
			t.Fatalf("Open of an artifact whose set holds a symlink: want NotApprovedError, got %v", err)
		}
		// Listing must not approve a version whose set holds a symlink.
		versions, err := store.Versions("example.com/real")
		if err != nil {
			t.Fatalf("Versions: %v", err)
		}
		if len(versions) != 0 {
			t.Fatalf("a symlinked artifact must not be listed as approved, got %+v", versions)
		}
	})

	t.Run("in-root symlink is refused too", func(t *testing.T) {
		root := t.TempDir()
		approveAll(t, root, "example.com/real", "v1.0.0")
		if err := os.Remove(filepath.Join(root, "example.com", "real", "@v", "v1.0.0.info")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(root, "example.com", "real", "@v", "v1.0.0.mod"),
			filepath.Join(root, "example.com", "real", "@v", "v1.0.0.info")); err != nil {
			t.Fatal(err)
		}
		store, err := NewStore(root)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}

		var notApproved *NotApprovedError
		if _, _, err := store.Open("example.com/real", "v1.0.0", ExtInfo); !asNotApproved(err, &notApproved) {
			t.Fatalf("Open of an in-root symlinked artifact: want NotApprovedError, got %v", err)
		} else if !strings.Contains(notApproved.Detail, "symlink") {
			t.Fatalf("diagnostic must name the symlink, got %q", notApproved.Detail)
		}
	})

	t.Run("handler refuses with the approval diagnostic", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "example.com"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secretDir, filepath.Join(root, "example.com", "linked")); err != nil {
			t.Fatal(err)
		}
		store, err := NewStore(root)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		handler := NewHandler(store, Options{LogRequests: true})
		recorder := handlerRequest(t, handler, http.MethodGet, "/example.com/linked/@v/v1.0.0.info", "10.0.0.1:1", nil)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("symlink escape must refuse with 404, got %d", recorder.Code)
		}
		if !strings.Contains(recorder.Body.String(), "symlink") {
			t.Fatalf("404 body must tell the administrator about the symlink: %q", recorder.Body.String())
		}
		if strings.Contains(recorder.Body.String(), secretDir) {
			t.Fatalf("response leaks outside-store paths: %q", recorder.Body.String())
		}
	})
}

func TestStoreListRefusesOversizedInfoWithoutUnboundedRead(t *testing.T) {
	// Listing and @latest read .info metadata through the bounded, confined
	// artifact path: a populated oversized .info is refused without ever
	// allocating for its full size, even though the (otherwise complete)
	// version stays listed and the serving path reports the bound violation.
	limits := DefaultLimits()
	limits.MaxInfoBytes = 512
	store, _ := newTestStore(t, limits, func(root string) {
		writeArtifact(t, root, "example.com/bloated", "v1.0.0", ExtInfo,
			`{"Version":"v1.0.0","Time":"2026-01-02T03:04:05Z"}`+strings.Repeat(" ", 8<<20))
		writeArtifact(t, root, "example.com/bloated", "v1.0.0", ExtMod, "module example.com/bloated\n\ngo 1.21\n")
		writeArtifact(t, root, "example.com/bloated", "v1.0.0", ExtZip, "zip-bytes")
	})

	assertBounded := func(t *testing.T, read func()) {
		t.Helper()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		read()
		runtime.ReadMemStats(&after)
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
			t.Fatalf("metadata read allocated %d bytes despite the %d byte bound; the read is not bounded", grew, limits.MaxInfoBytes)
		}
	}

	var versions []VersionInfo
	var err error
	assertBounded(t, func() {
		versions, err = store.Versions("example.com/bloated")
	})
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(versions) != 1 || versions[0].Version != "v1.0.0" {
		t.Fatalf("an oversized .info must not hide the otherwise-complete version, got %+v", versions)
	}
	if versions[0].Time != "" {
		t.Fatalf("oversized .info metadata must be refused, got time %q", versions[0].Time)
	}

	var latest VersionInfo
	assertBounded(t, func() {
		latest, err = store.Latest("example.com/bloated")
	})
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.Version != "v1.0.0" || latest.Time != "" {
		t.Fatalf("@latest must resolve without the refused metadata, got %+v", latest)
	}

	// The artifact-serving path reports the bound violation to the worker.
	_, _, err = store.Open("example.com/bloated", "v1.0.0", ExtInfo)
	var notApproved *NotApprovedError
	if !asNotApproved(err, &notApproved) {
		t.Fatalf("Open of an oversized .info: want NotApprovedError, got %v", err)
	}
	if !strings.Contains(notApproved.Detail, "bound") {
		t.Fatalf("diagnostic must name the bound, got %q", notApproved.Detail)
	}
}

func TestStoreRefusesNonFileArtifacts(t *testing.T) {
	// A directory at an artifact path is a population error, not a servable
	// artifact: a version whose set holds one must neither serve (any of its
	// artifacts) nor list, or approval would cover a set the go command
	// cannot consume.
	store, _ := newTestStore(t, DefaultLimits(), func(root string) {
		approveAll(t, root, "example.com/broken", "v1.0.0")
		if err := os.Remove(filepath.Join(root, "example.com", "broken", "@v", "v1.0.0.mod")); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(root, "example.com", "broken", "@v", "v1.0.0.mod"), 0o755); err != nil {
			t.Fatal(err)
		}
	})
	var notApproved *NotApprovedError
	for _, ext := range ArtifactExts {
		if _, _, err := store.Open("example.com/broken", "v1.0.0", ext); !asNotApproved(err, &notApproved) {
			t.Fatalf("Open(%s) of a version with a non-file artifact: want NotApprovedError, got %v", ext, err)
		} else if !strings.Contains(notApproved.Detail, "ordinary file") {
			t.Fatalf("diagnostic must name the non-file artifact, got %q", notApproved.Detail)
		}
	}
	versions, err := store.Versions("example.com/broken")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(versions) != 0 {
		t.Fatalf("a version with a non-file artifact must not be listed, got %+v", versions)
	}
	if _, err := store.Latest("example.com/broken"); !asNotApproved(err, &notApproved) {
		t.Fatalf("@latest of a module with no approvable version: want NotApprovedError, got %v", err)
	}

	// A file where the store layout requires a directory is the same
	// population error family.
	store, _ = newTestStore(t, DefaultLimits(), func(root string) {
		if err := os.MkdirAll(filepath.Join(root, "example.com", "flat"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "example.com", "flat", "@v"), []byte("not a directory"), 0o644); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := store.Versions("example.com/flat"); !asNotApproved(err, &notApproved) {
		t.Fatalf("Versions on a file @v: want NotApprovedError, got %v", err)
	} else if !strings.Contains(notApproved.Detail, "not a directory") {
		t.Fatalf("diagnostic must name the non-directory, got %q", notApproved.Detail)
	}
}

func TestStoreRefusesNullBytesBeforeTheFilesystem(t *testing.T) {
	// Null bytes in the request key are refused by module/version validation
	// before any store path is built (the HTTP layer rejects them earlier
	// still): no lookup may reach the filesystem with one.
	store, _ := newTestStore(t, DefaultLimits(), func(root string) {
		approveAll(t, root, "example.com/hello", "v1.0.0")
	})
	var notApproved *NotApprovedError
	for _, attempt := range []struct{ module, version string }{
		{"example.com/ho\x00x", "v1.0.0"},
		{"example.com/hello", "v1.0\x00.0"},
		{"example.com/hello", "v1.0.0\x00"},
	} {
		if _, _, err := store.Open(attempt.module, attempt.version, ExtInfo); !asNotApproved(err, &notApproved) {
			t.Fatalf("Open(%q, %q) with a null byte: want NotApprovedError, got %v", attempt.module, attempt.version, err)
		}
	}
	// A null byte in the module path refuses the listing too.
	if _, err := store.Versions("example.com/ho\x00x"); !asNotApproved(err, &notApproved) {
		t.Fatalf("Versions with a null byte: want NotApprovedError, got %v", err)
	}
}

func TestStoreVersionsBoundsTheDirectoryScan(t *testing.T) {
	// The listing scan is bounded: a populated @v directory beyond the entry
	// bound refuses cleanly, and the batched read stops at the bound, so a
	// huge directory cannot drive unbounded allocation before the refusal.
	limits := DefaultLimits()
	limits.MaxListEntries = scanAllocEntryBound

	// Over the bound: refuse with a diagnostic naming the bound — for @v/list
	// and for @latest, which resolves through the same listing.
	store, _ := newTestStore(t, limits, func(root string) {
		writeJunkEntries(t, root, 40)
	})
	var notApproved *NotApprovedError
	if _, err := store.Versions(scanAllocModule); !asNotApproved(err, &notApproved) {
		t.Fatalf("Versions over the entry bound: want NotApprovedError, got %v", err)
	} else if !strings.Contains(notApproved.Detail, "entries") {
		t.Fatalf("diagnostic must name the entry bound, got %q", notApproved.Detail)
	}
	if _, err := store.Latest(scanAllocModule); !asNotApproved(err, &notApproved) {
		t.Fatalf("@latest over the entry bound: want NotApprovedError, got %v", err)
	}

	// Under the bound the listing still works and junk is filtered.
	store, _ = newTestStore(t, limits, func(root string) {
		writeJunkEntries(t, root, 5)
		approveAll(t, root, scanAllocModule, "v1.0.0")
	})
	versions, err := store.Versions(scanAllocModule)
	if err != nil {
		t.Fatalf("Versions under the bound: %v", err)
	}
	if len(versions) != 1 || versions[0].Version != "v1.0.0" {
		t.Fatalf("want the one approved version, got %+v", versions)
	}

	// Allocation proof: the bounded scan must not cost more on a directory far
	// beyond the entry bound than on a directory only a batch beyond it. The
	// scan stops at the bound on both, so it never reads the whole directory;
	// a scan that read every entry of the huge directory, or that read past
	// one batch of entries, would cost the difference between the two
	// directories.
	//
	// The measurement runs in its own process, because the allocation
	// counter a process reports counts every allocation in that process: in
	// this test binary a background goroutine, a garbage collection cycle,
	// or any other allocation in the same window would be charged to the
	// scan, which is how a bound that the scan met used to fail for a scan
	// that stayed bounded.
	hugeStore, hugeRoot := newTestStore(t, limits, func(root string) {
		writeJunkEntries(t, root, scanAllocHugeEntries)
	})
	if _, err := hugeStore.Versions(scanAllocModule); !asNotApproved(err, &notApproved) {
		t.Fatalf("Versions over the bound must refuse, got %v", err)
	}
	baselineRoot := t.TempDir()
	writeJunkEntries(t, baselineRoot, scanAllocBaselineEntries)
	baselineCost, hugeCost := scanAllocations(t, baselineRoot, hugeRoot)
	if grew := int64(hugeCost) - int64(baselineCost); grew > scanAllocGrowthBound {
		t.Fatalf("the bounded scan allocated %d more bytes for a %d-entry directory than for a %d-entry directory; the read is not bounded",
			grew, scanAllocHugeEntries, scanAllocBaselineEntries)
	}
	if hugeCost > scanAllocCeiling {
		t.Fatalf("the bounded scan allocated %d bytes for a %d-entry directory; the read is not bounded", hugeCost, scanAllocHugeEntries)
	}
}

// writeJunkEntries populates a store root with junk @v entries that no
// version listing can approve: the scan still reads them, so a directory of
// them is a directory the scan has to bound.
func writeJunkEntries(t *testing.T, root string, count int) {
	t.Helper()
	dir := filepath.Join(root, filepath.FromSlash(scanAllocModule), "@v")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("junk-%04d.lock", i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The bounded listing scan's allocation proof. The scan is measured in a
// process that does nothing else, so the measured total is the scan's own
// allocation and no other allocation in the test process can fail the
// assertion.
const (
	// scanAllocModule is the module whose @v directory the measuring scan
	// reads.
	scanAllocModule = "example.com/huge"
	// scanAllocEntryBound is the entry bound the measuring scan runs under: the
	// scan must refuse the populated directory well before reading it whole.
	scanAllocEntryBound = 16
	// scanAllocBaselineEntries and scanAllocHugeEntries are the sizes of the
	// two @v directories the measuring child scans. They differ only in size,
	// and both hold more entries than the scan reads in one batch, so a
	// bounded scan reads the same entries of both and costs the same on both.
	scanAllocBaselineEntries = 300
	scanAllocHugeEntries     = 5000
	// scanAllocGrowthBound is how much more the bounded scan may cost on the
	// huge directory than on the baseline one. A bounded scan stops at the
	// entry bound on both, so the difference is nothing; a scan that read
	// past one batch of entries would cost the difference between the two
	// directories, which is orders of magnitude over this bound.
	scanAllocGrowthBound = 128 << 10
	// scanAllocCeiling is the allocation total the bounded scan must stay
	// under whatever the directory holds. The bounded scan costs a small
	// fraction of it, so the ceiling only fires on a scan whose read grew
	// without bound.
	scanAllocCeiling = 1 << 20
	// scanAllocProbes is how many scans the measuring child times per
	// directory. The smallest measured total is the scan's own cost: the
	// first scan pays for one-time initialization, and any other allocation
	// in the measuring child can only add to a total.
	scanAllocProbes = 5
	// scanAllocTimeout bounds the measuring child's whole run. A child that
	// never reports is a failed measurement, not a hung test.
	scanAllocTimeout = 30 * time.Second
	// scanAllocWaitDelay bounds the drain of the measuring child's output
	// after it is killed, so a child holding its pipe cannot stretch the
	// parent's wait.
	scanAllocWaitDelay = 30 * time.Second
)

// The measuring child's inputs: the two populated store roots it scans and the
// file it reports the measured allocation totals in.
const (
	scanAllocBaselineRootEnv = "COURIER_DEPENDENCYCACHE_SCAN_ALLOC_BASELINE_ROOT"
	scanAllocHugeRootEnv     = "COURIER_DEPENDENCYCACHE_SCAN_ALLOC_HUGE_ROOT"
	scanAllocOutEnv          = "COURIER_DEPENDENCYCACHE_SCAN_ALLOC_OUT"
)

// TestStoreVersionsScanAllocationProbe is the measuring child of
// TestStoreVersionsBoundsTheDirectoryScan. It reports the allocations of the
// bounded listing scan from a process that does nothing else but scan, so the
// reported totals are the scan's own allocations rather than this test
// binary's running total. Outside a probe run it skips.
func TestStoreVersionsScanAllocationProbe(t *testing.T) {
	baselineRoot := os.Getenv(scanAllocBaselineRootEnv)
	hugeRoot := os.Getenv(scanAllocHugeRootEnv)
	out := os.Getenv(scanAllocOutEnv)
	if baselineRoot == "" || hugeRoot == "" || out == "" {
		t.Skip("the scan allocation probe runs only as the measuring child of TestStoreVersionsBoundsTheDirectoryScan")
	}
	limits := DefaultLimits()
	limits.MaxListEntries = scanAllocEntryBound
	baseline := scanAllocProbeCost(t, limits, baselineRoot)
	huge := scanAllocProbeCost(t, limits, hugeRoot)
	report := fmt.Sprintf("%d\n%d\n", baseline, huge)
	if err := os.WriteFile(out, []byte(report), 0o644); err != nil {
		t.Fatalf("write the measured allocation totals: %v", err)
	}
}

// scanAllocProbeCost measures what the bounded listing scan allocates over
// one populated store root, in bytes. The smallest of the measured totals is
// the scan's own cost: the first scan pays for one-time initialization, and
// any other allocation in the measuring child can only add to a total.
func scanAllocProbeCost(t *testing.T, limits Limits, root string) uint64 {
	t.Helper()
	store, err := NewStoreWithLimits(root, limits)
	if err != nil {
		t.Fatalf("NewStoreWithLimits: %v", err)
	}
	best := uint64(math.MaxUint64)
	for probe := 0; probe < scanAllocProbes; probe++ {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := store.Versions(scanAllocModule)
		runtime.ReadMemStats(&after)
		var notApproved *NotApprovedError
		if !asNotApproved(err, &notApproved) {
			t.Fatalf("Versions over the entry bound must refuse, got %v", err)
		}
		if grew := after.TotalAlloc - before.TotalAlloc; grew < best {
			best = grew
		}
	}
	return best
}

// scanAllocations measures the bounded listing scan's allocations over a
// baseline and a huge @v directory and returns both totals in bytes. It
// re-executes this test binary as the measuring child, because the allocation
// counter a process reports covers that whole process: only a process that
// does nothing but scan can attribute a measured total to the scan.
func scanAllocations(t *testing.T, baselineRoot, hugeRoot string) (baseline, huge uint64) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "scan-alloc")
	ctx, cancel := context.WithTimeout(context.Background(), scanAllocTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStoreVersionsScanAllocationProbe$", "-test.count=1")
	cmd.Env = append(scanProbeEnv(),
		scanAllocBaselineRootEnv+"="+baselineRoot,
		scanAllocHugeRootEnv+"="+hugeRoot,
		scanAllocOutEnv+"="+out)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	cmd.WaitDelay = scanAllocWaitDelay
	if err := cmd.Run(); err != nil {
		t.Fatalf("the scan allocation probe did not report a measurement: %v\n%s", err, output.String())
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("the scan allocation probe reported no measurement: %v\n%s", err, output.String())
	}
	totals := strings.Fields(string(data))
	if len(totals) != 2 {
		t.Fatalf("the scan allocation probe reported %q: want two totals", data)
	}
	measured := make([]uint64, 0, 2)
	for _, total := range totals {
		grew, err := strconv.ParseUint(total, 10, 64)
		if err != nil {
			t.Fatalf("the scan allocation probe reported %q: %v", total, err)
		}
		measured = append(measured, grew)
	}
	return measured[0], measured[1]
}

// scanProbeEnv is the measuring child's environment: the parent's, minus the
// coverage output directory. The child runs without a coverage flag of its
// own, so it writes no coverage counters; dropping the directory keeps the
// parent's coverage data free of anything the child emitted.
func scanProbeEnv() []string {
	env := os.Environ()
	kept := make([]string, 0, len(env))
	for _, entry := range env {
		if key, _, ok := strings.Cut(entry, "="); ok && key == "GOCOVERDIR" {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
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
