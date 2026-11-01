// Package skills parses and resolves the deployment-owned registry of
// approved skill bundles: directories carrying SKILL.md plus optional
// assets, registered under stable logical names from a deployment-mounted
// config file at manager startup. Parsing and resolution stay separate from
// consumption; registration runs only in trusted manager setup and is never
// fed from CoderRun, source, or model data. A malformed or unavailable
// bundle fails manager startup explicitly rather than degrading the
// deployment silently.
package skills

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

// Bundle is one resolved, approved skill bundle: the neutral type the next
// layer (lane resolution, harness loading) sees. Digest is the deterministic
// content identity over the materialized tree; Path is the resolved
// materialized root on the manager's filesystem.
type Bundle struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	// Digest is sha256:<64 lowercase hex> over the materialized content.
	Digest string
	// Path is the resolved materialized root (symlinks resolved).
	Path string
	// Files counts regular files in the bundle; SKILL.md included.
	Files int
	// Bytes counts file content bytes; SKILL.md included.
	Bytes int64

	// files holds the exact bytes the digest was computed over, keyed by
	// relative slash path. The harness loader (#659) renders skill content
	// from these bytes rather than re-reading the mount, so what a role sees
	// is exactly what the pin verified — no re-read window between digest
	// verification and rendering. Unexported: the bytes leave the package
	// only as copies, through MaterializedFiles.
	files []bundleFile
}

// bundleSpec is one config entry: a stable logical name, the mounted
// absolute path holding the bundle's directory tree, and — for a bundle
// the operator materialized into a run's pods — the exact relative file
// paths the bundle carries.
type bundleSpec struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	// Materialization names the exact files a bundle carries when its
	// content is read from a projection instead of a directory tree.
	// Absent means the tree form: the source is walked.
	Materialization *materializationSpec `json:"materialization,omitempty"`
}

// materializationSpec is one bundle's materialization block: the
// authoritative list of relative slash paths the bundle carries. The list
// is the whole content — a file the projection carries but the list omits
// is invisible to the digest and to the loader.
type materializationSpec struct {
	Files []string `json:"files"`
}

// Parse reads a skill bundle config document: a top-level bundles list of
// bundleSpec entries. Unknown fields are rejected. Names are trimmed and
// must be valid DNS-1123 labels. Sources are trimmed, must be absolute paths
// — the deployment config points at mounted paths, so a relative source is a
// config bug — and are normalized, so one location has one spelling in the
// registry, in the pod mount, and in the rendered run config. Duplicate names
// are rejected. An empty bundles list is allowed: a registry with zero
// bundles.
//
// An entry may carry a materialization block naming the exact relative
// files its bundle carries: the per-run config the operator renders for a
// run's pods, where the bundle's content arrives as a projection rather
// than a directory tree. Every listed path is validated here, so a path
// that could escape the mount is rejected while parsing rather than
// steering a read later.
func Parse(data []byte) ([]bundleSpec, error) {
	var document struct {
		Bundles []bundleSpec `json:"bundles"`
	}
	if err := yaml.UnmarshalStrict(data, &document); err != nil {
		return nil, fmt.Errorf("parse skill bundle config: %w", err)
	}
	specs := make([]bundleSpec, 0, len(document.Bundles))
	seen := make(map[string]int, len(document.Bundles))
	for i, spec := range document.Bundles {
		name := strings.TrimSpace(spec.Name)
		if name == "" {
			return nil, fmt.Errorf("skill bundle %d: name is required", i)
		}
		if labelErrs := utilvalidation.IsDNS1123Label(name); len(labelErrs) > 0 {
			return nil, fmt.Errorf("skill bundle %d: name %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', starting and ending with an alphanumeric); got: %s", i, name, strings.Join(labelErrs, ", "))
		}
		if first, ok := seen[name]; ok {
			return nil, fmt.Errorf("skill bundle %d: duplicate name %q, first defined at bundle %d", i, name, first)
		}
		seen[name] = i
		source, err := normalizeSource(spec.Source)
		if err != nil {
			return nil, fmt.Errorf("skill bundle %d: %w", i, err)
		}
		if spec.Materialization != nil {
			for _, rel := range spec.Materialization.Files {
				if err := validateRelativePath(rel); err != nil {
					return nil, fmt.Errorf("skill %q: materialization file %q: %w", name, rel, err)
				}
			}
		}
		specs = append(specs, bundleSpec{Name: name, Source: source, Materialization: spec.Materialization})
	}
	return specs, nil
}

// MaterializedBundle describes one pinned bundle the operator materialized
// into a run's pods: the logical name, the absolute path the run's
// projection is mounted at, and the relative slash paths the projection
// carries.
type MaterializedBundle struct {
	Name   string
	Source string
	Files  []string
}

// RenderRunConfig renders the per-run skill bundle config document the
// control pod reads: the schema Parse accepts, one entry per pinned
// bundle, each naming the exact files the operator projected into the run's
// pods. Entries are sorted by name so one pin set always renders the same
// bytes, and the renderer fails closed on any entry that could not be
// materialized safely — a name that is not a valid DNS-1123 label, a
// duplicate name, a relative source, an empty file list, or a path that
// could escape the mount. The source is normalized the same way Parse
// normalizes it, so the rendered document carries the same spelling of a path
// the registry holds.
func RenderRunConfig(bundles []MaterializedBundle) ([]byte, error) {
	sorted := make([]MaterializedBundle, len(bundles))
	copy(sorted, bundles)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	specs := make([]bundleSpec, 0, len(sorted))
	seen := make(map[string]bool, len(sorted))
	for _, bundle := range sorted {
		name := strings.TrimSpace(bundle.Name)
		if name == "" {
			return nil, fmt.Errorf("skill bundle: name is required")
		}
		if labelErrs := utilvalidation.IsDNS1123Label(name); len(labelErrs) > 0 {
			return nil, fmt.Errorf("skill bundle %q: name must be a valid DNS-1123 label (lowercase alphanumeric or '-', starting and ending with an alphanumeric); got: %s", name, strings.Join(labelErrs, ", "))
		}
		if seen[name] {
			return nil, fmt.Errorf("skill bundle %q: duplicate name", name)
		}
		seen[name] = true
		source, err := normalizeSource(bundle.Source)
		if err != nil {
			return nil, fmt.Errorf("skill bundle %q: %w", name, err)
		}
		if len(bundle.Files) == 0 {
			return nil, fmt.Errorf("skill bundle %q: materialization file list must not be empty", name)
		}
		files := make([]string, 0, len(bundle.Files))
		for _, rel := range bundle.Files {
			if err := validateRelativePath(rel); err != nil {
				return nil, fmt.Errorf("skill %q: materialization file %q: %w", name, rel, err)
			}
			files = append(files, rel)
		}
		specs = append(specs, bundleSpec{
			Name:            name,
			Source:          source,
			Materialization: &materializationSpec{Files: files},
		})
	}
	document := struct {
		Bundles []bundleSpec `json:"bundles"`
	}{Bundles: specs}
	data, err := yaml.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("render skill bundle config: %w", err)
	}
	return data, nil
}

// validateRelativePath rejects a materialized file path that could escape
// the bundle mount: the path must be a non-empty relative slash path whose
// elements are neither "." nor "..", with no NUL byte and no backslash.
// Every listed path is re-validated before use, so a malformed config can
// never steer a read outside the mount.
func validateRelativePath(rel string) error {
	switch {
	case rel == "":
		return fmt.Errorf("must not be empty")
	case strings.ContainsRune(rel, 0):
		return fmt.Errorf("contains a NUL byte")
	case strings.Contains(rel, `\`):
		return fmt.Errorf("contains a backslash")
	case strings.HasPrefix(rel, "/"):
		return fmt.Errorf("must be relative")
	}
	for _, element := range strings.Split(rel, "/") {
		if element == "." || element == ".." {
			return fmt.Errorf("must not contain a %q element", element)
		}
	}
	return nil
}

// normalizeSource validates one bundle's registered source path and returns
// its normalized spelling.
//
// The source is also the mount path inside a run's pods, so it must be an
// absolute path that carries no control character, and it is normalized once
// here: a trailing separator or a "." element names the same location, and
// the registry, the pod mount, and the rendered run config must agree on one
// spelling of it. Normalizing at the boundary keeps one spelling of a path
// from reaching the pod spec in two forms.
func normalizeSource(source string) (string, error) {
	trimmed := strings.TrimSpace(source)
	switch {
	case trimmed == "":
		return "", fmt.Errorf("source is required")
	case !filepath.IsAbs(trimmed):
		return "", fmt.Errorf("source %q must be an absolute path", trimmed)
	case strings.ContainsRune(trimmed, 0):
		return "", fmt.Errorf("source carries a NUL byte")
	case strings.IndexFunc(trimmed, isControlRune) >= 0:
		return "", fmt.Errorf("source carries a control character")
	}
	return filepath.Clean(trimmed), nil
}

// isControlRune reports whether r is an ASCII control character. A path
// carrying one is not a path an operator can read in a log line or a manifest,
// so it is rejected rather than mounted.
func isControlRune(r rune) bool {
	return r < 0x20 || r == 0x7f
}

// Registry is the resolved, deployment-approved bundle set, sorted by name
// for stable iteration and startup logs.
type Registry struct {
	bundles []Bundle
	byName  map[string]int
}

func newRegistry(bundles []Bundle) *Registry {
	sort.Slice(bundles, func(i, j int) bool { return bundles[i].Name < bundles[j].Name })
	byName := make(map[string]int, len(bundles))
	for i, bundle := range bundles {
		byName[bundle.Name] = i
	}
	return &Registry{bundles: bundles, byName: byName}
}

// Bundles returns every registered bundle sorted by name.
func (r *Registry) Bundles() []Bundle {
	bundles := make([]Bundle, len(r.bundles))
	copy(bundles, r.bundles)
	return bundles
}

// Lookup returns the bundle registered under name.
func (r *Registry) Lookup(name string) (Bundle, bool) {
	i, ok := r.byName[name]
	if !ok {
		return Bundle{}, false
	}
	return r.bundles[i], true
}

// Names returns every registered bundle name sorted.
func (r *Registry) Names() []string {
	names := make([]string, len(r.bundles))
	for i, bundle := range r.bundles {
		names[i] = bundle.Name
	}
	return names
}

// Load reads and resolves the config file once: parse every spec, resolve
// and digest every bundle, and return the registry. Resolution errors wrap
// the bundle name so a startup failure names the offending entry. Used for
// startup fail-fast registration.
func Load(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read skill bundle config %s: %w", path, err)
	}
	specs, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("skill bundle config %s: %w", path, err)
	}
	bundles := make([]Bundle, 0, len(specs))
	for _, spec := range specs {
		bundle, err := resolveBundle(spec)
		if err != nil {
			return nil, fmt.Errorf("skill bundle %q: %w", spec.Name, err)
		}
		bundles = append(bundles, bundle)
	}
	return newRegistry(bundles), nil
}
