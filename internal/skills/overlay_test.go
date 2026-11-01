package skills

import (
	"strings"
	"testing"
)

func TestOverlayZeroSkillsBothSidesReturnsEmptyNonNil(t *testing.T) {
	visible, shadowed := Overlay(nil, nil)
	if visible == nil || shadowed == nil {
		t.Fatalf("visible = %#v, shadowed = %#v, want empty non-nil slices", visible, shadowed)
	}
	if len(visible) != 0 || len(shadowed) != 0 {
		t.Fatalf("visible = %#v, shadowed = %#v, want empty", visible, shadowed)
	}
}

func TestOverlayRepositoryOnlySkillsStayVisible(t *testing.T) {
	visible, shadowed := Overlay(nil, []string{"zeta", "alpha", "alpha"})
	if shadowed == nil {
		t.Fatalf("shadowed = nil, want an empty non-nil slice with repository input")
	}
	if len(shadowed) != 0 {
		t.Fatalf("shadowed = %#v, want nothing shadowed without pins", shadowed)
	}
	want := []VisibleSkill{
		{Name: "alpha", Origin: OriginRepository},
		{Name: "zeta", Origin: OriginRepository},
	}
	if len(visible) != 2 || visible[0] != want[0] || visible[1] != want[1] {
		t.Fatalf("visible = %#v, want deduplicated and sorted %#v", visible, want)
	}
}

// TestOverlayBundleShadowsSameNamedRepositorySkill is the executable
// collision policy (#658): when a pinned deployment bundle and a
// repository-local skill share a name, the bundle wins deterministically,
// the repository entry never appears, and the shadowing is reported.
func TestOverlayBundleShadowsSameNamedRepositorySkill(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	pins := []Pin{{Name: "simple-english", Digest: digest}}
	visible, shadowed := Overlay(pins, []string{"simple-english", "repo-tour"})

	if len(shadowed) != 1 || shadowed[0] != "simple-english" {
		t.Fatalf("shadowed = %#v, want the colliding repository name reported", shadowed)
	}
	want := []VisibleSkill{
		{Name: "repo-tour", Origin: OriginRepository},
		{Name: "simple-english", Origin: OriginBundle, Digest: digest},
	}
	if len(visible) != 2 || visible[0] != want[0] || visible[1] != want[1] {
		t.Fatalf("visible = %#v, want the bundle entry to replace the repository entry %#v", visible, want)
	}
	for _, skill := range visible {
		if skill.Name == "simple-english" && skill.Origin != OriginBundle {
			t.Fatalf("collision resolved to %q content; the deployment bundle must win", skill.Origin)
		}
	}
}

func TestOverlayShadowedNameCarriesThePinnedDigest(t *testing.T) {
	pinned := "sha256:" + strings.Repeat("b", 64)
	visible, _ := Overlay([]Pin{{Name: "skipped", Digest: pinned}}, []string{"skipped"})
	if len(visible) != 1 {
		t.Fatalf("visible = %#v, want exactly one entry for the colliding name", visible)
	}
	if visible[0].Origin != OriginBundle || visible[0].Digest != pinned {
		t.Fatalf("visible[0] = %#v, want the pinned bundle digest %q", visible[0], pinned)
	}
}

func TestOverlayDeterministicOrderAndShadowedDedup(t *testing.T) {
	digest := "sha256:" + strings.Repeat("c", 64)
	pins := []Pin{{Name: "beta", Digest: digest}, {Name: "alpha", Digest: digest}}
	visible, shadowed := Overlay(pins, []string{"gamma", "beta", "beta", "alpha", "gamma"})

	if strings.Join(shadowed, ",") != "alpha,beta" {
		t.Fatalf("shadowed = %#v, want each colliding name once, sorted", shadowed)
	}
	var names []string
	for _, skill := range visible {
		names = append(names, skill.Name+":"+string(skill.Origin))
	}
	if got := strings.Join(names, ","); got != "alpha:bundle,beta:bundle,gamma:repository" {
		t.Fatalf("visible = %s, want sorted and attributed to one source each", got)
	}
}

func TestOverlayRepeatedPinNameKeepsFirstDigest(t *testing.T) {
	first := "sha256:" + strings.Repeat("d", 64)
	second := "sha256:" + strings.Repeat("e", 64)
	visible, _ := Overlay([]Pin{{Name: "dupe", Digest: first}, {Name: "dupe", Digest: second}}, nil)
	if len(visible) != 1 || visible[0].Digest != first {
		t.Fatalf("visible = %#v, want one entry carrying the first pin's digest", visible)
	}
}
