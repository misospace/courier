package skills

import "sort"

// SkillOrigin names where one visible skill's content comes from, so the
// loader can report collision precedence to the coordinator and operator
// instead of silently merging sources (#658).
type SkillOrigin string

const (
	// OriginBundle is a pinned deployment bundle: deployment-owned
	// configuration, the authoritative source under the collision policy.
	OriginBundle SkillOrigin = "bundle"
	// OriginRepository is a repository-local skill discovered in the run's
	// checkout. It is visible only when no pinned bundle shares its name.
	OriginRepository SkillOrigin = "repository"
)

// VisibleSkill is one skill the native loader (#659) exposes to a run: the
// logical name, its content origin, and the pinned content digest for
// bundle-backed skills (empty for repository-local skills, which carry no
// approved identity).
type VisibleSkill struct {
	Name   string      `json:"name"`
	Origin SkillOrigin `json:"origin"`
	Digest string      `json:"digest,omitempty"`
}

// Overlay computes the effective skill view for a run under the collision
// policy settled in #658: a pinned deployment bundle always shadows a
// same-named repository-local skill — deployment-owned configuration wins
// deterministically, never whichever filesystem traversal found first. A
// pinned name always appears as a bundle entry carrying its pinned digest;
// a repository-local name appears only when no pin shares it. The shadowed
// names are returned separately so the loader can make the precedence
// visible rather than drop content silently.
//
// pins is the run's verified pin set as produced by Select; names are
// treated as unique, and a repeated pin name keeps its first digest.
// repositoryLocal holds the names as discovered in the checkout; duplicates
// collapse and validation of discovered names is the loader's (#659).
// Collision is exact-name match. Both results are sorted by name and never
// nil.
func Overlay(pins []Pin, repositoryLocal []string) (visible []VisibleSkill, shadowed []string) {
	pinnedByName := make(map[string]string, len(pins))
	for _, pin := range pins {
		if _, seen := pinnedByName[pin.Name]; !seen {
			pinnedByName[pin.Name] = pin.Digest
		}
	}
	visible = make([]VisibleSkill, 0, len(pinnedByName)+len(repositoryLocal))
	shadowedSet := make(map[string]bool)
	seenRepository := make(map[string]bool, len(repositoryLocal))
	for _, name := range repositoryLocal {
		if _, pinned := pinnedByName[name]; pinned {
			shadowedSet[name] = true
			continue
		}
		if seenRepository[name] {
			continue
		}
		seenRepository[name] = true
		visible = append(visible, VisibleSkill{Name: name, Origin: OriginRepository})
	}
	for name, digest := range pinnedByName {
		visible = append(visible, VisibleSkill{Name: name, Origin: OriginBundle, Digest: digest})
	}
	sort.Slice(visible, func(i, j int) bool { return visible[i].Name < visible[j].Name })
	shadowed = make([]string, 0, len(shadowedSet))
	for name := range shadowedSet {
		shadowed = append(shadowed, name)
	}
	sort.Strings(shadowed)
	return visible, shadowed
}
