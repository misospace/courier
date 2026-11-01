package topology

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/misospace/courier/internal/executor"
)

// SkillSource is one registered bundle's logical name and the absolute path
// its content is mounted at inside a run's pods. The path is deployment
// registered, so it is deployment trusted — but it still has to be mountable:
// two bundles at one path render two volumes at one mount path, and a bundle
// at a path the operator's own pods own would shadow that volume.
type SkillSource struct {
	// Name is the bundle's registered logical name.
	Name string
	// Source is the absolute path the bundle's content is mounted at.
	Source string
}

// ReservedSkillMountPaths returns the absolute mount paths the operator's own
// pod volumes occupy in the control and worker pods: the two workspaces, the
// control runtime, the toolchain cache and its read-only reference, the
// control's credential mounts, and the skill bundle config directory.
//
// The credential mounts are derived from the file paths inside them, so the
// list stays in step with the mounts the pods actually render.
func ReservedSkillMountPaths() []string {
	return []string{
		controlWorkspacePath,
		controlRuntimePath,
		executor.ToolchainCachePath,
		executor.ToolchainReferencePath,
		filepath.Dir(controlSigningKeyPath),
		filepath.Dir(controlBrokerCAPath),
		filepath.Dir(controlGatewayKeyPath),
		filepath.Dir(controlTokenPath),
		filepath.Dir(executor.SkillBundlesConfigPath),
	}
}

// ValidateSkillSources rejects registered bundle sources that could not be
// mounted into a run's pods. A source must be a clean absolute path, no two
// bundles may share or nest one path, and no source may overlap a path the
// operator's own pods own.
//
// The overlap rule is symmetric on purpose. A bundle mounted at an occupied
// path would shadow the operator's volume, and a bundle mounted at an
// ancestor of one would shadow it too, so both directions are rejected. The
// check is pure: it reads no filesystem and no cluster state, so the same
// answer holds at registration, at materialization, and at render time.
func ValidateSkillSources(sources []SkillSource) error {
	for _, source := range sources {
		if err := validateSkillSource(source); err != nil {
			return err
		}
	}
	for i, source := range sources {
		for _, earlier := range sources[:i] {
			if skillPathsOverlap(source.Source, earlier.Source) {
				return fmt.Errorf("topology: skill %q source %q overlaps skill %q source %q", source.Name, source.Source, earlier.Name, earlier.Source)
			}
		}
	}
	reserved := ReservedSkillMountPaths()
	for _, source := range sources {
		for _, path := range reserved {
			if skillPathsOverlap(source.Source, path) {
				return fmt.Errorf("topology: skill %q source %q collides with the pod-owned mount path %q", source.Name, source.Source, path)
			}
		}
	}
	return nil
}

// ValidateSkillMounts rejects a run's materialized skill mounts whose
// registered source paths could not be mounted into the pod. It is the mount
// list form of ValidateSkillSources: the pods render the same list in both
// the control and the worker, so one answer covers both.
func ValidateSkillMounts(mounts []SkillMount) error {
	sources := make([]SkillSource, 0, len(mounts))
	for _, mount := range mounts {
		sources = append(sources, SkillSource{Name: mount.Name, Source: mount.Source})
	}
	return ValidateSkillSources(sources)
}

// validateSkillSource rejects one malformed source path. The path must be
// absolute and clean: a relative path cannot be a mount point, and a path
// carrying "." or ".." elements names a location other than the one the
// deployment registered.
func validateSkillSource(source SkillSource) error {
	switch {
	case strings.TrimSpace(source.Source) == "":
		return fmt.Errorf("topology: skill %q has no source path", source.Name)
	case !filepath.IsAbs(source.Source):
		return fmt.Errorf("topology: skill %q source %q must be an absolute path", source.Name, source.Source)
	case strings.ContainsRune(source.Source, 0):
		return fmt.Errorf("topology: skill %q source carries a NUL byte", source.Name)
	case filepath.Clean(source.Source) != source.Source:
		return fmt.Errorf("topology: skill %q source %q must be a clean absolute path", source.Name, source.Source)
	}
	return nil
}

// skillPathsOverlap reports whether two absolute paths are the same path or
// one is a path-segment ancestor of the other. The root is trimmed to the
// empty string so it reads as the ancestor of every other path.
func skillPathsOverlap(a, b string) bool {
	a = strings.TrimSuffix(filepath.Clean(a), "/")
	b = strings.TrimSuffix(filepath.Clean(b), "/")
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
