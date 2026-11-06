// Package dependencycache serves administrator-approved immutable Go module
// artifacts to secure workers over the read-only subset of the GOPROXY
// protocol (HARNESS.md §2). The store is a pre-populated, read-only directory
// tree; the worker-facing path never performs upstream I/O, never fills the
// cache on demand, never redirects, and never proxies — a miss is answered
// locally with an actionable approval diagnostic. Approval is exactly
// population: a module version is served only when its complete .info/.mod/
// .zip artifact set is present in the administrator-controlled store root as
// ordinary files — every lookup is resolved beneath the store root and
// through no symlink, so a mis-populated store can never redirect a request
// to a file the administrator did not place there.
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
	"io"
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
//
// Every lookup is resolved through a root-confined handle (os.Root) and an
// element walk that refuses symlinks, so no request can reach a file outside
// the root no matter what a mis-populated store contains.
type Store struct {
	root   string
	limits Limits
	// rooted confines path resolution beneath the root directory: even a
	// populated symlink can never direct a lookup outside the store, and the
	// guarantee holds without a check-then-open race.
	rooted *os.Root
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
	rooted, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("dependency-cache: open store root: %w", err)
	}
	return &Store{root: root, limits: limits, rooted: rooted}, nil
}

// Root returns the store's root directory.
func (s *Store) Root() string { return s.root }

// errStoreSymlink marks a store path element that is a symlink,
// errStoreNotRegular a final artifact element that is not an ordinary file,
// and errStoreNotDir a required-directory element that is not a directory.
// The store layout is populated by the ordinary toolchain, which produces
// only ordinary files and directories, so each is a population error — and
// refusing it (rather than following, serving, or listing it) is what keeps
// an approved-looking path from resolving to content the administrator never
// placed there, and what keeps an artifact set that is not a real
// .info/.mod/.zip set from being approved.
var (
	errStoreSymlink    = errors.New("symlink in store population")
	errStoreNotRegular = errors.New("non-ordinary file in store population")
	errStoreNotDir     = errors.New("non-directory in store population")
)

// populationRefusal renders the approval refusal for a store path that
// violates the population contract, naming the offending store location in
// deployment-admin terms. ok is false for errors outside that family (those
// stay generic storage errors).
func populationRefusal(err error, relPath string) (*NotApprovedError, bool) {
	switch {
	case errors.Is(err, errStoreSymlink):
		return &NotApprovedError{Detail: fmt.Sprintf(
			"store population at %q holds a symlink; the store must contain only ordinary files and directories; fix the population", relPath)}, true
	case errors.Is(err, errStoreNotRegular):
		return &NotApprovedError{Detail: fmt.Sprintf(
			"store population at %q is not an ordinary file; fix the population", relPath)}, true
	case errors.Is(err, errStoreNotDir):
		return &NotApprovedError{Detail: fmt.Sprintf(
			"store population at %q is not a directory; fix the population", relPath)}, true
	}
	return nil, false
}

// statArtifact Lstats one root-relative artifact path
// ("<escaped-module>/@v/<file>"): every element is inspected through the
// confined root without following symlinks, every interior element must be a
// real directory, and the final element must be an ordinary file — a
// directory at an artifact path is a population error, not a servable
// artifact.
func (s *Store) statArtifact(relPath string) (fs.FileInfo, error) {
	return s.walkStore(relPath, false)
}

// statStoreDir is statArtifact for a store path that must be a directory
// (the module's @v directory), with the same per-element symlink refusal.
func (s *Store) statStoreDir(relPath string) (fs.FileInfo, error) {
	return s.walkStore(relPath, true)
}

// walkStore is the confined, symlink-refusing element walk shared by
// statArtifact and statStoreDir. os.Root already guarantees the lookup
// cannot escape the store root even through a symlink; the walk additionally
// enforces the population contract that every path element is an ordinary
// directory or file, so a populated symlink can never redirect an artifact
// read — out of the root, or to another location inside it.
func (s *Store) walkStore(relPath string, wantDir bool) (fs.FileInfo, error) {
	parts := strings.Split(relPath, "/")
	prefix := ""
	var info fs.FileInfo
	for i, part := range parts {
		if i > 0 {
			prefix += "/"
		}
		prefix += part
		next, err := s.rooted.Lstat(prefix)
		if err != nil {
			return nil, err
		}
		if next.Mode()&fs.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w at %q", errStoreSymlink, prefix)
		}
		if !next.IsDir() && (i < len(parts)-1 || wantDir) {
			return nil, fmt.Errorf("%w at %q", errStoreNotDir, prefix)
		}
		info = next
	}
	if !wantDir && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w at %q", errStoreNotRegular, relPath)
	}
	return info, nil
}

// Open resolves and opens one artifact of a module version. The request is
// validated twice — as a module path and version (which bounds every
// character and rejects traversal, absolute paths, and non-canonical
// escapes) and again as a root-confined, symlink-free on-disk path — so a
// hostile request can only ever reach a file the administrator populated,
// never a path outside the store and never a store path that redirects
// through a symlink.
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

	dirRel := escapedModule + "/@v"
	missing := make([]string, 0, len(ArtifactExts))
	for _, artifactExt := range ArtifactExts {
		artifactRel := dirRel + "/" + escapedVersion + artifactExt
		if _, err := s.statArtifact(artifactRel); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				missing = append(missing, artifactExt)
				continue
			}
			if refusal, ok := populationRefusal(err, artifactRel); ok {
				refusal.ModulePath, refusal.Version = modulePath, version
				return nil, 0, refusal
			}
			return nil, 0, fmt.Errorf("dependency-cache: inspect %s@%s: %w", modulePath, version, err)
		}
	}
	if len(missing) > 0 {
		return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
			Detail: "incomplete artifact set, missing " + strings.Join(missing, ", ")}
	}

	name := dirRel + "/" + escapedVersion + ext
	if _, err := s.statArtifact(name); err != nil {
		// The complete set existed a moment ago; treat any loss as a miss
		// rather than serving a partial read of a re-population.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
				Detail: "artifact disappeared during the request; the store must not be re-populated while serving"}
		}
		if refusal, ok := populationRefusal(err, name); ok {
			refusal.ModulePath, refusal.Version = modulePath, version
			return nil, 0, refusal
		}
		return nil, 0, fmt.Errorf("dependency-cache: open %s@%s: %w", modulePath, version, err)
	}
	file, err := s.rooted.Open(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
				Detail: "artifact disappeared during the request; the store must not be re-populated while serving"}
		}
		return nil, 0, fmt.Errorf("dependency-cache: open %s@%s: %w", modulePath, version, err)
	}
	// Size the served bytes from the open handle: what we report is exactly
	// what the descriptor reads, and the artifact must still be an ordinary
	// file within its bound. A race can only ever swap in-root content — the
	// root-confined Open cannot follow anything out.
	openInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, fmt.Errorf("dependency-cache: open %s@%s: %w", modulePath, version, err)
	}
	if !openInfo.Mode().IsRegular() {
		_ = file.Close()
		return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
			Detail: fmt.Sprintf("store population at %q is not an ordinary file; fix the population", name)}
	}
	if bound := s.limits.maxFor(ext); openInfo.Size() > bound {
		_ = file.Close()
		return nil, 0, &NotApprovedError{ModulePath: modulePath, Version: version,
			Detail: fmt.Sprintf("%s artifact exceeds the server's %d byte bound; fix the population", ext, bound)}
	}
	return file, openInfo.Size(), nil
}

// VersionInfo is one approved version of a module: its canonical (unescaped)
// semver and, when the populated .info records one, the population timestamp.
type VersionInfo struct {
	Version string
	Time    string
}

// Versions lists the approved versions of a module in semver order. A version
// appears only when its complete artifact set is populated as ordinary files,
// so the list is exactly the approval set version resolution may choose from.
func (s *Store) Versions(modulePath string) ([]VersionInfo, error) {
	escapedModule, err := module.EscapePath(modulePath)
	if err != nil {
		return nil, &NotApprovedError{ModulePath: modulePath,
			Detail: fmt.Sprintf("invalid module path: %v", err)}
	}
	dirRel := escapedModule + "/@v"
	if _, err := s.statStoreDir(dirRel); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &NotApprovedError{ModulePath: modulePath,
				Detail: "module is not populated in this deployment's dependency cache"}
		}
		if refusal, ok := populationRefusal(err, dirRel); ok {
			refusal.ModulePath = modulePath
			return nil, refusal
		}
		return nil, fmt.Errorf("dependency-cache: list %s: %w", modulePath, err)
	}
	dir, err := s.rooted.Open(dirRel)
	if err != nil {
		return nil, fmt.Errorf("dependency-cache: list %s: %w", modulePath, err)
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	if err != nil {
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
		// A version is approved only when its complete artifact set exists as
		// ordinary store files (the stat walk refuses symlinks).
		escapedVersion, err := module.EscapeVersion(version)
		if err != nil {
			continue
		}
		complete := true
		for _, artifactExt := range ArtifactExts {
			if _, err := s.statArtifact(dirRel + "/" + escapedVersion + artifactExt); err != nil {
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
	data, ok := s.readInfoBounded(escapedModule, escapedVersion)
	if !ok {
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

// readInfoBounded reads one populated .info through the same confined,
// symlink-refusing artifact path the serving path uses, under the
// MaxInfoBytes bound: the stat gate refuses an oversized .info before any
// read, and the read itself is capped so a size race cannot allocate beyond
// the bound either. Listing and @latest metadata therefore never allocate on
// a mis-populated store; the artifact-serving path is what reports the bound
// violation to the worker.
func (s *Store) readInfoBounded(escapedModule, escapedVersion string) ([]byte, bool) {
	name := escapedModule + "/@v/" + escapedVersion + ExtInfo
	info, err := s.statArtifact(name)
	if err != nil || info.Size() > s.limits.MaxInfoBytes {
		return nil, false
	}
	file, err := s.rooted.Open(name)
	if err != nil {
		return nil, false
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, s.limits.MaxInfoBytes+1))
	if err != nil || int64(len(data)) > s.limits.MaxInfoBytes {
		return nil, false
	}
	return data, true
}

// Latest resolves the module's newest approved version, the semantic of the
// proxy protocol's @latest endpoint, following the go command's own
// preference: the highest release, then the highest prerelease, then the
// chronologically newest pseudo-version. Serving a pseudo-version is
// required for tagless modules — the go command filters pseudo-versions out
// of @v/list and resolves module@latest through this endpoint.
func (s *Store) Latest(modulePath string) (VersionInfo, error) {
	versions, err := s.Versions(modulePath)
	if err != nil {
		return VersionInfo{}, err
	}
	// Versions is semver-ascending, so the last match of each pass is the
	// highest of its kind.
	for i := len(versions) - 1; i >= 0; i-- {
		if !module.IsPseudoVersion(versions[i].Version) && semver.Prerelease(versions[i].Version) == "" {
			return versions[i], nil
		}
	}
	for i := len(versions) - 1; i >= 0; i-- {
		if !module.IsPseudoVersion(versions[i].Version) {
			return versions[i], nil
		}
	}
	// Pseudo-only: pick by chronology, the way the go command's list fallback
	// does — the populated .info time first, then the time embedded in the
	// pseudo-version itself. Ties keep the semver-earlier version, matching
	// the go command's strict-before comparison.
	best := VersionInfo{}
	var bestTime time.Time
	for _, version := range versions {
		if !module.IsPseudoVersion(version.Version) {
			continue
		}
		when, ok := pseudoVersionTime(version)
		if !ok {
			continue
		}
		if best.Version == "" || bestTime.Before(when) {
			best, bestTime = version, when
		}
	}
	if best.Version == "" {
		// Unreachable for valid pseudo-versions, which always embed a time.
		return VersionInfo{}, &NotApprovedError{ModulePath: modulePath,
			Detail: "no resolvable version is populated"}
	}
	return best, nil
}

// pseudoVersionTime derives a pseudo-version's chronology the way the go
// command's list fallback does: the populated .info time first, then the
// timestamp embedded in the pseudo-version string.
func pseudoVersionTime(version VersionInfo) (time.Time, bool) {
	if version.Time != "" {
		if t, err := time.Parse(time.RFC3339, version.Time); err == nil {
			return t, true
		}
	}
	when, err := module.PseudoVersionTime(version.Version)
	return when, err == nil
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
