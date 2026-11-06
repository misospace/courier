package dependencycache

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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
	writeJunk := func(t *testing.T, root string, count int) {
		t.Helper()
		dir := filepath.Join(root, "example.com", "huge", "@v")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < count; i++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("junk-%04d.lock", i)), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	limits := DefaultLimits()
	limits.MaxListEntries = 16

	// Over the bound: refuse with a diagnostic naming the bound — for @v/list
	// and for @latest, which resolves through the same listing.
	store, _ := newTestStore(t, limits, func(root string) {
		writeJunk(t, root, 40)
	})
	var notApproved *NotApprovedError
	if _, err := store.Versions("example.com/huge"); !asNotApproved(err, &notApproved) {
		t.Fatalf("Versions over the entry bound: want NotApprovedError, got %v", err)
	} else if !strings.Contains(notApproved.Detail, "entries") {
		t.Fatalf("diagnostic must name the entry bound, got %q", notApproved.Detail)
	}
	if _, err := store.Latest("example.com/huge"); !asNotApproved(err, &notApproved) {
		t.Fatalf("@latest over the entry bound: want NotApprovedError, got %v", err)
	}

	// Under the bound the listing still works and junk is filtered.
	store, _ = newTestStore(t, limits, func(root string) {
		writeJunk(t, root, 5)
		approveAll(t, root, "example.com/huge", "v1.0.0")
	})
	versions, err := store.Versions("example.com/huge")
	if err != nil {
		t.Fatalf("Versions under the bound: %v", err)
	}
	if len(versions) != 1 || versions[0].Version != "v1.0.0" {
		t.Fatalf("want the one approved version, got %+v", versions)
	}

	// Allocation proof: scanning a directory far beyond the bound allocates
	// for the bounded scan only, never for the whole directory — a full read
	// of 5000 entries would cost hundreds of KiB.
	store, _ = newTestStore(t, limits, func(root string) {
		writeJunk(t, root, 5000)
	})
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = store.Versions("example.com/huge")
	runtime.ReadMemStats(&after)
	if !asNotApproved(err, &notApproved) {
		t.Fatalf("Versions over the bound must refuse, got %v", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 128<<10 {
		t.Fatalf("the bounded scan allocated %d bytes for a 5000-entry directory; the read is not bounded", grew)
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
