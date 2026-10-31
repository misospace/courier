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
}

// bundleSpec is one config entry: a stable logical name and the mounted
// absolute path holding the bundle's directory tree.
type bundleSpec struct {
	Name   string `json:"name"`
	Source string `json:"source"`
}

// Parse reads a skill bundle config document: a top-level bundles list of
// bundleSpec entries. Unknown fields are rejected. Names are trimmed and
// must be valid DNS-1123 labels. Sources are trimmed and must be absolute
// paths — the deployment config points at mounted paths, so a relative
// source is a config bug. Duplicate names are rejected. An empty bundles
// list is allowed: a registry with zero bundles.
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
		source := strings.TrimSpace(spec.Source)
		if source == "" {
			return nil, fmt.Errorf("skill bundle %d: source is required", i)
		}
		if !filepath.IsAbs(source) {
			return nil, fmt.Errorf("skill bundle %d: source %q must be an absolute path", i, source)
		}
		specs = append(specs, bundleSpec{Name: name, Source: source})
	}
	return specs, nil
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
		bundle, err := Resolve(spec)
		if err != nil {
			return nil, fmt.Errorf("skill bundle %q: %w", spec.Name, err)
		}
		bundles = append(bundles, bundle)
	}
	return newRegistry(bundles), nil
}
