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

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/harness"
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
	// CapacityGroup names the shared execution-capacity pool this lane
	// draws from (#327); empty means the lane is its own capacity identity.
	CapacityGroup string `json:"capacityGroup,omitempty"`
	// Context declares the per-role context budgets for this lane (#243,
	// #295); nil means no declared budget. Same bounds as the CRD field it
	// mirrors; resolution stays in internal/harness.
	Context *courierv1alpha1.ContextPolicy `json:"context,omitempty"`
	// Resources sizes the pods Courier creates using Courier's narrowed
	// requests/limits vocabulary; empty means the lane gets the built-in default.
	Resources courierv1alpha1.PodResources `json:"resources,omitempty"`
}

// Parse reads a bootstrap config document: a top-level profiles list of
// Profile entries. Unknown fields are rejected. Names are trimmed and must be
// valid DNS-1123 labels. RuntimeImage and role names/values are stored
// trimmed. Framing is free-text prompt content and is not trimmed. A missing
// or zero concurrency is coerced to 1; negative concurrency is rejected. A
// declared context policy is validated against the CRD bounds, with the
// CRD's compactAtPercent default mirrored at the policy level.
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
			return nil, fmt.Errorf("bootstrap profile %d: name %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', starting and ending with an alphanumeric); got: %s", i, name, strings.Join(labelErrs, ", "))
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
		profile.CapacityGroup = strings.TrimSpace(profile.CapacityGroup)
		if len(profile.CapacityGroup) > 253 {
			return nil, fmt.Errorf("bootstrap profile %q: capacityGroup must be at most 253 characters", name)
		}
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
		if profile.Context != nil {
			if err := normalizeContext(name, profile.Context); err != nil {
				return nil, err
			}
		}
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

// normalizeContext validates a declared context policy against the same
// bounds the CRD enforces and mirrors the CRD default for the policy-level
// compactAtPercent, so the desired spec matches the server-defaulted spec and
// the bootstrap reconciler does not rewrite an unchanged LaneProfile every
// pass. Per-role compactAtPercent keeps zero as "inherit the lane policy";
// defaulting it there would prevent inheriting a different lane fraction.
func normalizeContext(name string, policy *courierv1alpha1.ContextPolicy) error {
	if policy.CompactAtPercent == 0 {
		policy.CompactAtPercent = harness.DefaultCompactAtPercent
	} else if policy.CompactAtPercent < harness.MinCompactAtPercent || policy.CompactAtPercent > harness.MaxCompactAtPercent {
		return fmt.Errorf("bootstrap profile %q: context.compactAtPercent %d is outside [%d,%d]", name, policy.CompactAtPercent, harness.MinCompactAtPercent, harness.MaxCompactAtPercent)
	}
	if policy.OutputReservation < 0 {
		return fmt.Errorf("bootstrap profile %q: context.outputReservation must not be negative, got %d", name, policy.OutputReservation)
	}
	if len(policy.Roles) == 0 {
		return nil
	}
	roles := make(map[string]courierv1alpha1.ContextBudget, len(policy.Roles))
	for role, budget := range policy.Roles {
		trimmedRole := strings.TrimSpace(role)
		if trimmedRole == "" {
			return fmt.Errorf("bootstrap profile %q: context role name must not be empty", name)
		}
		if _, ok := roles[trimmedRole]; ok {
			return fmt.Errorf("bootstrap profile %q: duplicate context role %q after trimming whitespace", name, trimmedRole)
		}
		if budget.MaxInputTokens < 0 {
			return fmt.Errorf("bootstrap profile %q: context role %q maxInputTokens must not be negative, got %d", name, trimmedRole, budget.MaxInputTokens)
		}
		if budget.CompactAtPercent != 0 && (budget.CompactAtPercent < harness.MinCompactAtPercent || budget.CompactAtPercent > harness.MaxCompactAtPercent) {
			return fmt.Errorf("bootstrap profile %q: context role %q compactAtPercent %d is outside [%d,%d]", name, trimmedRole, budget.CompactAtPercent, harness.MinCompactAtPercent, harness.MaxCompactAtPercent)
		}
		if budget.OutputReservation < 0 {
			return fmt.Errorf("bootstrap profile %q: context role %q outputReservation must not be negative, got %d", name, trimmedRole, budget.OutputReservation)
		}
		roles[trimmedRole] = budget
	}
	policy.Roles = roles
	return nil
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
