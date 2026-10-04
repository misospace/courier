// Package bootstrap creates and updates explicitly deployment-managed
// LaneProfile objects after the manager is running, so installers never apply
// LaneProfile CRs beside the Helm release (the CRD may not be established
// yet). Parsing and transport are kept separate from reconciliation. Removal
// from config intentionally leaves the previously managed CR in place
// (conservative MVP, because active runs reference lanes). The package has no
// Dispatch or source dependency.
package bootstrap

import (
	"context"
	"fmt"
	"os"
	"strings"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

// Profile is the bootstrap-config form of a deployment-managed LaneProfile.
type Profile struct {
	Name         string            `json:"name"`
	RuntimeImage string            `json:"runtimeImage,omitempty"`
	Concurrency  int               `json:"concurrency,omitempty"`
	Roles        map[string]string `json:"roles"`
	Framing      string            `json:"framing,omitempty"`
}

// Parse reads a bootstrap config document: a top-level profiles list of
// Profile entries. Unknown fields are rejected. Names are trimmed and must be
// valid DNS-1123 labels. RuntimeImage and role names/values are stored
// trimmed. Framing is free-text prompt content and is not trimmed. A missing
// or zero concurrency is coerced to 1; negative concurrency is rejected.
func Parse(data []byte) ([]Profile, error) {
	var document struct {
		Profiles []Profile `json:"profiles"`
	}
	if err := yaml.UnmarshalStrict(data, &document); err != nil {
		return nil, fmt.Errorf("parse bootstrap config: %w", err)
	}
	profiles := make([]Profile, 0, len(document.Profiles))
	seen := make(map[string]int, len(document.Profiles))
	for i, profile := range document.Profiles {
		name := strings.TrimSpace(profile.Name)
		if name == "" {
			return nil, fmt.Errorf("bootstrap profile %d: name is required", i)
		}
		if labelErrs := utilvalidation.IsDNS1123Label(name); len(labelErrs) > 0 {
			return nil, fmt.Errorf("bootstrap profile %d: name %q must be a valid DNS-1123 label (lowercase alphanumeric, '-', or '.', starting and ending with an alphanumeric); got: %s", i, name, strings.Join(labelErrs, ", "))
		}
		if first, ok := seen[name]; ok {
			return nil, fmt.Errorf("bootstrap profile %d: duplicate name %q, first defined at profile %d", i, name, first)
		}
		seen[name] = i
		profile.Name = name
		if profile.Concurrency == 0 {
			profile.Concurrency = 1
		} else if profile.Concurrency < 0 {
			return nil, fmt.Errorf("bootstrap profile %q: concurrency must be at least 1, got %d", name, profile.Concurrency)
		}
		profile.RuntimeImage = strings.TrimSpace(profile.RuntimeImage)
		if len(profile.Roles) == 0 {
			return nil, fmt.Errorf("bootstrap profile %q: roles must contain at least one entry", name)
		}
		roles := make(map[string]string, len(profile.Roles))
		for role, model := range profile.Roles {
			trimmedRole := strings.TrimSpace(role)
			trimmedModel := strings.TrimSpace(model)
			if trimmedRole == "" {
				return nil, fmt.Errorf("bootstrap profile %q: role name must not be empty", name)
			}
			if trimmedModel == "" {
				return nil, fmt.Errorf("bootstrap profile %q: model for role %q must not be empty", name, trimmedRole)
			}
			if _, ok := roles[trimmedRole]; ok {
				return nil, fmt.Errorf("bootstrap profile %q: duplicate role %q after trimming whitespace", name, trimmedRole)
			}
			roles[trimmedRole] = trimmedModel
		}
		profile.Roles = roles
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

// Provider is the source-neutral seam for reading the desired set of
// deployment-managed profiles.
type Provider interface {
	Profiles(ctx context.Context) ([]Profile, error)
}

// FileProvider reads and re-parses a YAML file on every call, so a rotated
// ConfigMap file is picked up on the next pass.
type FileProvider struct {
	Path string
}

// Profiles re-reads the file and parses it.
func (p *FileProvider) Profiles(ctx context.Context) ([]Profile, error) {
	data, err := os.ReadFile(p.Path)
	if err != nil {
		return nil, fmt.Errorf("read bootstrap config %s: %w", p.Path, err)
	}
	profiles, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("bootstrap config %s: %w", p.Path, err)
	}
	return profiles, nil
}

// LoadFile reads and parses the config file once; used for startup fail-fast
// validation.
func LoadFile(path string) ([]Profile, error) {
	return (&FileProvider{Path: path}).Profiles(context.Background())
}
