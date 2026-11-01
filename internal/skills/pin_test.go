package skills

import (
	"strings"
	"testing"
)

// newTestRegistry builds a registry of two bundles with fixed digests; the
// digests are opaque to pin selection and verification, which only compare
// them. The bundles are registered directly, without a config file on disk.
func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	return newRegistry([]Bundle{
		{Name: "simple-english", Digest: "sha256:" + strings.Repeat("a", 64)},
		{Name: "repo-tour", Digest: "sha256:" + strings.Repeat("b", 64)},
	})
}

func TestSelectZeroSkillsReturnsEmptyNonNilSlice(t *testing.T) {
	pins, err := Select(newTestRegistry(t), nil)
	if err != nil {
		t.Fatalf("Select rejected an empty selection: %v", err)
	}
	if pins == nil || len(pins) != 0 {
		t.Fatalf("pins = %#v, want an empty non-nil slice", pins)
	}
}

func TestSelectTrimsAndSortsByName(t *testing.T) {
	pins, err := Select(newTestRegistry(t), []string{" repo-tour ", "simple-english"})
	if err != nil {
		t.Fatalf("Select rejected valid names: %v", err)
	}
	if len(pins) != 2 || pins[0].Name != "repo-tour" || pins[1].Name != "simple-english" {
		t.Fatalf("pins = %#v, want sorted by name", pins)
	}
	if pins[0].Digest != "sha256:"+strings.Repeat("b", 64) || pins[1].Digest != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("pins = %#v, want each bundle's registry digest", pins)
	}
}

func TestSelectRejectsDuplicateNamesAfterTrim(t *testing.T) {
	_, err := Select(newTestRegistry(t), []string{"simple-english", " simple-english "})
	if err == nil || !strings.Contains(err.Error(), `duplicate name "simple-english"`) {
		t.Fatalf("expected duplicate-name rejection, got %v", err)
	}
}

func TestSelectRejectsUnknownName(t *testing.T) {
	_, err := Select(newTestRegistry(t), []string{"simple-english", "ghost-skill"})
	if err == nil || !strings.Contains(err.Error(), `"ghost-skill"`) || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unknown-name rejection naming the offender, got %v", err)
	}
}

func TestSelectRejectsInvalidLabel(t *testing.T) {
	for _, name := range []string{"UPPER", "has_underscore", "-leading", "trailing-", "has.dot", "a b"} {
		_, err := Select(newTestRegistry(t), []string{name})
		if err == nil || !strings.Contains(err.Error(), "must be a valid DNS-1123 label") || !strings.Contains(err.Error(), `"`+name+`"`) {
			t.Fatalf("name %q: expected DNS-1123 rejection naming the offender, got %v", name, err)
		}
	}
}

func TestSelectRejectsEmptyName(t *testing.T) {
	_, err := Select(newTestRegistry(t), []string{"simple-english", "   "})
	if err == nil || !strings.Contains(err.Error(), "name must not be empty") {
		t.Fatalf("expected empty-name rejection, got %v", err)
	}
}

func TestSelectNilRegistryWithNamesRejected(t *testing.T) {
	_, err := Select(nil, []string{"simple-english"})
	if err == nil || !strings.Contains(err.Error(), "registers no skill bundles") {
		t.Fatalf("expected nil-registry rejection, got %v", err)
	}
}

func TestSelectNilRegistryWithZeroNamesOK(t *testing.T) {
	pins, err := Select(nil, nil)
	if err != nil {
		t.Fatalf("Select rejected an empty selection against a nil registry: %v", err)
	}
	if pins == nil || len(pins) != 0 {
		t.Fatalf("pins = %#v, want an empty non-nil slice", pins)
	}
}

func TestVerifyMatchingSetOK(t *testing.T) {
	registry := newTestRegistry(t)
	pins, err := Select(registry, []string{"simple-english", "repo-tour"})
	if err != nil {
		t.Fatalf("Select rejected valid names: %v", err)
	}
	if err := Verify(registry, pins); err != nil {
		t.Fatalf("Verify rejected the pin set it had just selected: %v", err)
	}
}

func TestVerifyMissingNameFailsClosed(t *testing.T) {
	registry := newTestRegistry(t)
	pins := []Pin{{Name: "ghost-skill", Digest: "sha256:" + strings.Repeat("a", 64)}}
	err := Verify(registry, pins)
	if err == nil || err.Error() != `skill "ghost-skill" is not registered; the run's pinned skill identity is no longer materializable` {
		t.Fatalf("expected the precise missing-name error, got %v", err)
	}
}

func TestVerifyDigestDriftFailsClosed(t *testing.T) {
	registry := newTestRegistry(t)
	pinned := "sha256:" + strings.Repeat("c", 64)
	err := Verify(registry, []Pin{{Name: "simple-english", Digest: pinned}})
	if err == nil {
		t.Fatal("Verify accepted drifted bundle content")
	}
	want := `skill "simple-english" content digest "sha256:` + strings.Repeat("a", 64) + `" does not match the run's pinned digest "sha256:` + strings.Repeat("c", 64) + `"; the registry changed since the pin was written`
	if err.Error() != want {
		t.Fatalf("digest-drift error = %q, want %q", err, want)
	}
}

func TestVerifyEmptyPinsOK(t *testing.T) {
	if err := Verify(newTestRegistry(t), nil); err != nil {
		t.Fatalf("Verify rejected an empty pin set: %v", err)
	}
	if err := Verify(newTestRegistry(t), []Pin{}); err != nil {
		t.Fatalf("Verify rejected an empty pin set: %v", err)
	}
}

func TestVerifyNilRegistryWithPinsFailsClosed(t *testing.T) {
	err := Verify(nil, []Pin{{Name: "simple-english", Digest: "sha256:" + strings.Repeat("a", 64)}})
	if err == nil || err.Error() != `skill "simple-english" is not registered; the run's pinned skill identity is no longer materializable` {
		t.Fatalf("expected the missing-name error against a nil registry, got %v", err)
	}
}
