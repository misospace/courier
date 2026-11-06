package dependencycache

import (
	"archive/zip"
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGoCommandBuildsThroughTheCache is the worker-side acceptance: a real
// go command resolves, downloads, and builds an approved module version
// through the cache alone — no other network access — and refused lookups
// surface the cache's approval diagnostic in go's own error output.
func TestGoCommandBuildsThroughTheCache(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}
	root := t.TempDir()
	writePopulatedModule(t, root, "example.com/hello", "v1.0.0")
	writePopulatedModule(t, root, "example.com/hello", "v1.1.0")
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	server := httptest.NewServer(NewHandler(store, Options{LogRequests: false}))
	t.Cleanup(server.Close)

	workspace := t.TempDir()
	writeFile(t, workspace, "go.mod", "module secure.worker\n\ngo 1.21\n\nrequire example.com/hello v1.0.0\n")
	writeFile(t, workspace, "main.go", "package main\n\nimport (\n\t\"fmt\"\n\n\t\"example.com/hello\"\n)\n\nfunc main() {\n\tfmt.Println(hello.Greeting)\n}\n")

	scratch := goScratchDir(t)
	env := childGoEnv(t, server.URL, scratch)
	// Download: go fetches .info, .mod, and .zip from the cache and records
	// the checksums in go.sum (GOSUMDB=off, exactly the worker posture).
	if out, err := goRun(t, env, workspace, "mod", "download", "example.com/hello"); err != nil {
		t.Fatalf("go mod download through the cache failed: %v\n%s", err, out)
	}
	// Build: the cached Go build works.
	if out, err := goRun(t, env, workspace, "build", "./..."); err != nil {
		t.Fatalf("go build through the cache failed: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(workspace, "secure.worker")); err != nil {
		t.Fatalf("build produced no binary: %v", err)
	}
	// Resolution: the version list answers @v/list.
	out, err := goRun(t, env, workspace, "list", "-m", "-versions", "example.com/hello")
	if err != nil {
		t.Fatalf("go list -m -versions failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "v1.0.0") || !strings.Contains(out, "v1.1.0") {
		t.Fatalf("version list through the cache is incomplete: %q", out)
	}

	// Missing version: go must fail with the cache's actionable approval
	// diagnostic, not a network error, and never fall through to upstream
	// (the only GOPROXY is this cache).
	out, err = goRun(t, env, workspace, "mod", "download", "example.com/hello@v9.9.9")
	if err == nil {
		t.Fatalf("go mod download of an unapproved version succeeded: %s", out)
	}
	assertDiagnosticSurfaced(t, out)

	// Missing module: same failure shape for a module the administrator
	// never approved.
	other := t.TempDir()
	writeFile(t, other, "go.mod", "module absent.worker\n\ngo 1.21\n\nrequire example.com/absent v1.0.0\n")
	out, err = goRun(t, childGoEnv(t, server.URL, scratch), other, "mod", "download", "example.com/absent")
	if err == nil {
		t.Fatalf("go mod download of an unapproved module succeeded: %s", out)
	}
	assertDiagnosticSurfaced(t, out)
}

// assertDiagnosticSurfaced checks that the go command's output carries the
// cache's miss diagnostic (the 404 body), which is what a worker sees.
func assertDiagnosticSurfaced(t *testing.T, output string) {
	t.Helper()
	if !strings.Contains(output, "not approved") || !strings.Contains(output, "populate") {
		t.Fatalf("go output is missing the actionable approval diagnostic:\n%s", output)
	}
	for _, forbidden := range []string{"dial tcp", "proxy.golang.org", "connection refused", "lookup"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("go output suggests an upstream lookup (%q), the cache must never cause one:\n%s", forbidden, output)
		}
	}
}

// writePopulatedModule writes a complete, valid module version artifact set:
// a module zip with files under the <module>@<version>/ prefix plus the
// .info and .mod files, exactly as population via `go mod download` leaves
// them.
func writePopulatedModule(t *testing.T, root, modulePath, version string) {
	t.Helper()
	var mod bytes.Buffer
	mod.WriteString("module " + modulePath + "\n\ngo 1.21\n")

	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	prefix := modulePath + "@" + version
	for _, file := range []struct{ name, body string }{
		{prefix + "/go.mod", mod.String()},
		{prefix + "/hello.go", "package hello\n\nconst Greeting = \"hello from " + version + "\"\n"},
	} {
		w, err := zw.Create(file.name)
		if err != nil {
			t.Fatalf("zip create: %v", err)
		}
		if _, err := w.Write([]byte(file.body)); err != nil {
			t.Fatalf("zip write: %v", err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}

	writeArtifact(t, root, modulePath, version, ExtInfo,
		fmt.Sprintf(`{"Version":"%s","Time":"2026-01-02T03:04:05Z"}`+"\n", version))
	writeArtifact(t, root, modulePath, version, ExtMod, mod.String())
	writeArtifact(t, root, modulePath, version, ExtZip, zipBuf.String())
}

// childGoEnv builds the environment a worker-like go command runs with: the
// cache is the only GOPROXY, checksum-database lookups are off, and every
// cache and home directory is scoped to the test.
func childGoEnv(t *testing.T, proxyURL, scratch string) []string {
	t.Helper()
	base := os.Environ()
	keep := make([]string, 0, len(base))
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "GOPROXY", "GOSUMDB", "GONOSUMDB", "GONOSUMCHECK", "GOFLAGS", "GONOSUMDBSUM", "GOVCS",
			"GOMODCACHE", "GOCACHE", "GOPATH", "GOTOOLCHAIN", "GOWORK", "HOME", "GOPRIVATE":
			continue
		}
		keep = append(keep, entry)
	}
	return append(keep,
		"GOPROXY="+proxyURL,
		"GOSUMDB=off",
		"GOTOOLCHAIN=local",
		"GOWORK=off",
		"GOMODCACHE="+filepath.Join(scratch, "modcache"),
		"GOCACHE="+filepath.Join(scratch, "gocache"),
		"GOPATH="+filepath.Join(scratch, "gopath"),
		"HOME="+scratch,
	)
}

// goScratchDir returns a fresh scratch directory for go's caches and HOME.
// The go command marks downloaded modules read-only, which t.TempDir's
// cleanup refuses to remove, so this cleanup chmods everything writable
// before deleting.
func goScratchDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "courier-depcache-go-")
	if err != nil {
		t.Fatalf("scratch dir: %v", err)
	}
	t.Cleanup(func() {
		_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err == nil {
				_ = os.Chmod(path, 0o755)
			}
			return nil
		})
		_ = os.RemoveAll(dir)
	})
	return dir
}

func goRun(t *testing.T, env []string, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func writeFile(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}
