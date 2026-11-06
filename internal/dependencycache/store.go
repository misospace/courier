// Package dependencycache serves administrator-approved immutable Go module
// artifacts to secure workers over the read-only subset of the GOPROXY
// protocol (HARNESS.md §2). The store is a pre-populated, read-only directory
// tree; the worker-facing path never performs upstream I/O, never fills the
// cache on demand, never redirects, and never proxies — a miss is answered
// locally with an actionable approval diagnostic. Approval is exactly
// population: a module version is served only when its complete .info/.mod/
// .zip artifact set is present in the administrator-controlled store root.
//
// The store layout is the Go module cache's wire layout, so administrators
// populate it with the ordinary toolchain from a trusted machine:
//
//	GOMODCACHE=$(mktemp -d) go mod download all
//	cp -r "$GOMODCACHE/cache/download/." /srv/courier-dependency-cache/
//
// The worker resolves the cache as an operator-pinned ClusterIP literal
// (GOPROXY=http://<ip>:<port>) with GOSUMDB=off; checksum verification runs
// against the workspace go.sum, so the cache needs no checksum-database
// endpoint and serves none (serving one would widen the attack surface for
// no offline benefit).
package dependencycache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

// Artifact extensions the cache serves. A module version is approved for
// serving only when all three exist: the go command cannot build from a
// partial set, and serving one would turn an incomplete population into a
// confusing worker-side failure.
const (
	ExtInfo = ".info"
	ExtMod  = ".mod"
	ExtZip  = ".zip"
)

// ArtifactExts is the complete-set extension list.
var ArtifactExts = [...]string{ExtInfo, ExtMod, ExtZip}

// Limits bound every artifact the cache serves. A populated artifact beyond
// its bound is refused, not served: bounds are a server-side property, and a
// store that carries an oversized file is a population error an administrator
// must fix (the worker's diagnostic says so). MaxZipBytes mirrors the Go
// toolchain's own 500 MiB module-zip bound; nothing larger can be a valid
// module zip.
type Limits struct {
	MaxInfoBytes int64
	MaxModBytes  int64
	MaxZipBytes  int64
}

// DefaultLimits returns the built-in bounds.
func DefaultLimits() Limits {
	return Limits{
		MaxInfoBytes: 1 << 20,
		MaxModBytes:  4 << 20,
		MaxZipBytes:  500 << 20,
	}
}

func (l Limits) maxFor(ext string) int64 {
	switch ext {
	case ExtInfo:
		return l.MaxInfoBytes
	case ExtMod:
		return l.MaxModBytes
	case ExtZip:
		return l.MaxZipBytes
	}
	return 0
}

// NotApprovedError reports a requested module/version the cache will not
// serve. The worker-facing handler turns it into the actionable approval
// diagnostic (an HTTP 404 with a text/plain body, which the go command
// surfaces verbatim on proxy misses).
type NotApprovedError struct {
	ModulePath string
	Version    string
	// Detail names what is missing or refused, in deployment-admin terms.
	Detail string
}

func (e *NotApprovedError) Error() string {
	target := e.ModulePath
	if e.Version != "" {
		target += "@" + e.Version
	}
	return fmt.Sprintf("not approved: %s: %s", target, e.Detail)
}

// Store is the read-only view of the administrator-populated module store.
// The root contains one directory per module in escaped-module-path form,
// each holding an @v directory of per-version artifacts:
//
//	<root>/<escaped-module-path>/@v/<escaped-version>.{info,mod,zip}
type Store struct {
	root   string
	limits Limits
}

// NewStore validates that root is an existing directory and returns the
// read-only store with the default artifact bounds. The store never writes;
// population happens out of band through the administrator's trusted path.
func NewStore(root string) (*Store, error) {
	return NewStoreWithLimits(root, DefaultLimits())
}

// NewStoreWithLimits is NewStore with explicit artifact bounds.
func NewStoreWithLimits(root string, limits Limits) (*Store, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("dependency-cache: open store root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("dependency-cache: store root %q is not a directory", root)
	}
	return &Store{root: root, limits: limits}, nil
}

// Root returns the store's root directory.
func (s *Store) Root() string { return s.root }

// Open resolves and opens one artifact of a module version. The request is
// validated twice — as a module path and version (which bounds every
// character and rejects traversal, absolute paths, and non-canonical
// escapes) and again as a canonical on-disk path confined to the store root —
// so a hostile request can only ever reach a file the administrator
// populated, never a path outside the store.
func (s *Store) Open(modulePath, version, ext string) (*os.File, int64, error) {
	if !validExt(ext) {
		return nil, 0, fmt.Errorf("dependency-cache: unsupported artifact type %q", ext)
	}
	escapedModule, err := module.EscapePath(modulePath)
	if err != nil {
		return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
			Detail: fmt.Sprintf("invalid module path: %v", err)}
	}
	escapedVersion, err := module.EscapeVersion(version)
	if err != nil {
		return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
			Detail: fmt.Sprintf("invalid version: %v", err)}
	}

	dir := filepath.Join(s.root, escapedModule, "@v")
	missing := make([]string, 0, len(ArtifactExts))
	for _, artifactExt := range ArtifactExts {
		if _, err := os.Stat(filepath.Join(dir, escapedVersion+artifactExt)); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return nil, 0, fmt.Errorf("dependency-cache: inspect %s@%s: %w", modulePath, version, err)
			}
			missing = append(missing, artifactExt)
		}
	}
	if len(missing) > 0 {
		return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
			Detail: "incomplete artifact set, missing " + strings.Join(missing, ", ")}
	}

	name := filepath.Join(dir, escapedVersion+ext)
	info, err := os.Stat(name)
	if err != nil {
		// The complete set existed a moment ago; treat any loss as a miss
		// rather than serving a partial read of a re-population.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
				Detail: "artifact disappeared during the request; the store must not be re-populated while serving"}
		}
		return nil, 0, fmt.Errorf("dependency-cache: open %s@%s: %w", modulePath, version, err)
	}
	if bound := s.limits.maxFor(ext); info.Size() > bound {
		return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
			Detail: fmt.Sprintf("%s artifact exceeds the server's %d byte bound; fix the population", ext, bound)}
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, 0, fmt.Errorf("dependency-cache: open %s@%s: %w", modulePath, version, err)
	}
	return file, info.Size(), nil
}

// VersionInfo is one approved version of a module: its canonical (unescaped)
// semver and, when the populated .info records one, the population timestamp.
type VersionInfo struct {
	Version string
	Time    string
}

// Versions lists the approved versions of a module in semver order. A version
// appears only when its complete artifact set is populated, so the list is
// exactly the approval set version resolution may choose from.
func (s *Store) Versions(modulePath string) ([]VersionInfo, error) {
	escapedModule, err := module.EscapePath(modulePath)
	if err != nil {
		return nil, &NotApprovedError{ModulePath: modulePath,
			Detail: fmt.Sprintf("invalid module path: %v", err)}
	}
	dir := filepath.Join(s.root, escapedModule, "@v")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &NotApprovedError{ModulePath: modulePath,
				Detail: "module is not populated in this deployment's dependency cache"}
		}
		return nil, fmt.Errorf("dependency-cache: list %s: %w", modulePath, err)
	}

	seen := make(map[string]bool)
	var versions []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || strings.HasSuffix(name, ".lock") {
			continue
		}
		ext := filepath.Ext(name)
		if !validExt(ext) {
			continue
		}
		version, err := module.UnescapeVersion(strings.TrimSuffix(name, ext))
		if err != nil || !semver.IsValid(version) || seen[version] {
			continue
		}
		// A version is approved only when its complete artifact set exists.
		escapedVersion, err := module.EscapeVersion(version)
		if err != nil {
			continue
		}
		complete := true
		for _, artifactExt := range ArtifactExts {
			if _, err := os.Stat(filepath.Join(dir, escapedVersion+artifactExt)); err != nil {
				complete = false
				break
			}
		}
		if complete {
			seen[version] = true
			versions = append(versions, version)
		}
	}

	sort.Slice(versions, func(i, j int) bool { return semver.Compare(versions[i], versions[j]) < 0 })
	out := make([]VersionInfo, 0, len(versions))
	for _, version := range versions {
		info := VersionInfo{Version: version}
		if t, ok := s.populationTime(escapedModule, version); ok {
			info.Time = t
		}
		out = append(out, info)
	}
	return out, nil
}

// populationTime reads the populated .info's Time field, when present and
// parseable. The timestamp lets the go command resolve "@latest" the way it
// resolves a real proxy's list.
func (s *Store) populationTime(escapedModule, version string) (string, bool) {
	escapedVersion, err := module.EscapeVersion(version)
	if err != nil {
		return "", false
	}
	data, err := os.ReadFile(filepath.Join(s.root, escapedModule, "@v", escapedVersion+ExtInfo))
	if err != nil {
		return "", false
	}
	var payload struct {
		Time *string `json:"Time"`
	}
	if err := json.Unmarshal(data, &payload); err != nil || payload.Time == nil {
		return "", false
	}
	if _, err := time.Parse(time.RFC3339, *payload.Time); err != nil {
		return "", false
	}
	return *payload.Time, true
}

// Latest resolves the module's newest approved non-pseudo version, the
// semantic of the proxy protocol's @latest endpoint. Modules with only
// pseudo-versions report no latest, and the go command falls back to the
// version list.
func (s *Store) Latest(modulePath string) (VersionInfo, error) {
	versions, err := s.Versions(modulePath)
	if err != nil {
		return VersionInfo{}, err
	}
	for i := len(versions) - 1; i >= 0; i-- {
		if !module.IsPseudoVersion(versions[i].Version) {
			return versions[i], nil
		}
	}
	return VersionInfo{}, &NotApprovedError{ModulePath: modulePath,
		Detail: "no non-pseudo version is populated; resolution must use an explicit version"}
}

// LatestJSON renders the RevInfo JSON shape the go command parses from an
// @latest response.
func (v VersionInfo) LatestJSON() string {
	payload := struct {
		Version string  `json:"Version"`
		Time    *string `json:"Time,omitempty"`
	}{Version: v.Version}
	if v.Time != "" {
		payload.Time = &v.Time
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "{\"Version\":\"\"}"
	}
	return string(data)
}

func validExt(ext string) bool {
	for _, artifactExt := range ArtifactExts {
		if ext == artifactExt {
			return true
		}
	}
	return false
}
