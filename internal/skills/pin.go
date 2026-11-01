package skills

import (
	"fmt"
	"sort"
	"strings"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

// Pin is the durable per-run identity of one selected bundle: the approved
// logical name and the content digest resolved at admission. A live run's
// pins are its immutable set — a later lane or registry edit never silently
// alters what the run loads; a relaunch re-verifies and fails closed instead.
type Pin struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

// Select resolves a lane's selected skill names against the deployment
// registry and returns the per-run pins. Names are trimmed and must be valid
// DNS-1123 labels; empty names, duplicates after trimming, and names absent
// from the registry are rejected, so selection fails loudly at admission
// rather than resolving to nothing. The returned slice is sorted by name and
// never nil, so an empty selection pins as an empty — not absent — set.
// Selection takes only approved logical names: the caller must never feed it
// URLs or paths supplied by work input.
func Select(reg *Registry, names []string) ([]Pin, error) {
	if reg == nil && len(names) > 0 {
		return nil, fmt.Errorf("the deployment registers no skill bundles")
	}
	pins := make([]Pin, 0, len(names))
	seen := make(map[string]bool, len(names))
	for i, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return nil, fmt.Errorf("skill %d: name must not be empty", i)
		}
		if labelErrs := utilvalidation.IsDNS1123Label(trimmed); len(labelErrs) > 0 {
			return nil, fmt.Errorf("skill %d: name %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', starting and ending with an alphanumeric); got: %s", i, trimmed, strings.Join(labelErrs, ", "))
		}
		if seen[trimmed] {
			return nil, fmt.Errorf("skill %d: duplicate name %q", i, trimmed)
		}
		seen[trimmed] = true
		bundle, ok := reg.Lookup(trimmed)
		if !ok {
			return nil, fmt.Errorf("skill %q is not registered in the deployment skill registry", trimmed)
		}
		pins = append(pins, Pin{Name: trimmed, Digest: bundle.Digest})
	}
	sort.Slice(pins, func(i, j int) bool { return pins[i].Name < pins[j].Name })
	return pins, nil
}

// Verify re-checks a run's pinned skill set against the deployment registry:
// every pin must still resolve to a registered bundle carrying the pinned
// digest. A missing name or a changed digest fails closed — the registry
// changed under a live run, and substituting newer content silently would
// rewrite what the run was admitted to load. An empty or absent pin set
// verifies trivially: nothing was selected, so nothing can have drifted.
func Verify(reg *Registry, pins []Pin) error {
	for _, pin := range pins {
		var (
			bundle Bundle
			ok     bool
		)
		if reg != nil {
			bundle, ok = reg.Lookup(pin.Name)
		}
		if !ok {
			return fmt.Errorf("skill %q is not registered; the run's pinned skill identity is no longer materializable", pin.Name)
		}
		if bundle.Digest != pin.Digest {
			return fmt.Errorf("skill %q content digest %q does not match the run's pinned digest %q; the registry changed since the pin was written", pin.Name, bundle.Digest, pin.Digest)
		}
	}
	return nil
}
