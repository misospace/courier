package skills

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

// The bounds of the run loader (#659). The registry bounds one bundle's
// content; these bound how much of it the harness can surface to roles, so
// loading stays bounded by file count and bytes and can never grow the
// coordinator's context without limit. Pinned-set violations fail closed
// with a precise error — an omitted skill would silently change what a run
// loads; repository-local violations are quarantined and reported instead,
// because repository content must never park a pinned run.
const (
	// maxVisibleSkills is the largest visible skill set one run may load,
	// across both origins.
	maxVisibleSkills = 32
	// maxRepoLocalSkills is the largest repository-local skill set one
	// checkout may contribute to the visible view.
	maxRepoLocalSkills = 16
	// maxSkillInstructionsBytes bounds one skill's rendered instructions.
	maxSkillInstructionsBytes = 16 << 10
	// maxSkillDescriptionBytes bounds one skill's rendered description.
	maxSkillDescriptionBytes = 1024
	// maxFrontmatterScanBytes bounds how far into SKILL.md the frontmatter
	// scan looks for the closing delimiter; an unterminated block is a
	// corrupt document, not an unbounded scan.
	maxFrontmatterScanBytes = 8 << 10
)

// SkillView is one skill the harness exposes to a run's roles: the logical
// name, its content origin under the settled collision policy, and the
// bounded material the deterministic loader renders for it. Instructions are
// the skill's SKILL.md body with frontmatter stripped; they are prompt data
// the trusted control reads and renders, never code the control executes.
type SkillView struct {
	Name   string      `json:"name"`
	Origin SkillOrigin `json:"origin"`
	// Digest is the pinned content identity; bundle-backed skills only.
	Digest string `json:"digest,omitempty"`
	// Description is the SKILL.md frontmatter description, bounded. It is
	// rendered into the role-facing index only for bundle-backed skills:
	// repository-local descriptions are untrusted repository content and
	// reach roles only through the skill tool, as data.
	Description string `json:"description,omitempty"`
	// Instructions is the bounded SKILL.md body (frontmatter stripped).
	Instructions string `json:"instructions,omitempty"`
	// Source is the registered absolute bundle root — the path the
	// deployment mounts into the control and worker pods — for
	// bundle-backed skills; empty for a repository-local skill, whose
	// assets live in the checkout the worker already holds.
	Source string `json:"source,omitempty"`
	// Files lists the bundle's asset paths relative to the bundle root
	// (slash-separated, sorted, SKILL.md excluded); bundle-backed skills
	// only.
	Files      []string `json:"files,omitempty"`
	FilesBytes int64    `json:"filesBytes,omitempty"`
}

// RejectedSkill is one repository-local skill directory the loader
// quarantined: it is not visible to any role and never fails the run, but
// its name and reason are reported so a repository author sees why their
// skill is inert rather than discovering silence.
type RejectedSkill struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// LoadedSkills is the run's effective skill view: the visible set under the
// settled collision policy, the names a pinned bundle shadowed, and the
// repository-local skills quarantined for malformed content.
type LoadedSkills struct {
	Visible  []SkillView     `json:"visible"`
	Shadowed []string        `json:"shadowed,omitempty"`
	Rejected []RejectedSkill `json:"rejected,omitempty"`
}

// LoadForRun resolves the run's pinned skill identities against the
// deployment-mounted bundle config and the run's checkout, and renders the
// bounded skill view the harness exposes (#659). It is the one deterministic
// loading path: every skill a role sees comes through here, verified against
// the run's pins.
//
// A skillless run (no pins) loads nothing and requires no mount: bundlesFile
// and checkoutDir are ignored and the zero view is returned. A pinned run
// fails closed — before the coordinator's first model request, never an
// omitted skill — when the bundle config is absent, a pin names no
// registered bundle, or a bundle's content no longer matches its pinned
// digest. Repository-local skills are discovered under
// <checkoutDir>/.agents/skills/<name>/SKILL.md and follow the same collision
// contract as everywhere else — a pinned bundle shadows a same-named
// repository-local skill — but a malformed repository-local document is
// quarantined and reported, never a run-ending failure: third-party
// repository content must not be able to park a run.
func LoadForRun(pins []Pin, bundlesFile, checkoutDir string) (LoadedSkills, error) {
	if len(pins) == 0 {
		return LoadedSkills{Visible: []SkillView{}}, nil
	}
	if strings.TrimSpace(bundlesFile) == "" {
		return LoadedSkills{}, fmt.Errorf("the run pins %d skill(s) but no skill bundle config is provided", len(pins))
	}
	data, err := os.ReadFile(bundlesFile)
	if err != nil {
		return LoadedSkills{}, fmt.Errorf("read skill bundle config %s: %w", bundlesFile, err)
	}
	specs, err := Parse(data)
	if err != nil {
		return LoadedSkills{}, fmt.Errorf("skill bundle config %s: %w", bundlesFile, err)
	}
	specByName := make(map[string]bundleSpec, len(specs))
	for _, spec := range specs {
		specByName[spec.Name] = spec
	}
	bundles := make(map[string]Bundle, len(pins))
	for _, pin := range pins {
		spec, ok := specByName[pin.Name]
		if !ok {
			return LoadedSkills{}, fmt.Errorf("skill %q is not registered in the deployment skill registry; the run's pinned skill identity is no longer materializable", pin.Name)
		}
		bundle, err := Resolve(spec)
		if err != nil {
			return LoadedSkills{}, fmt.Errorf("skill %q: %w", pin.Name, err)
		}
		if bundle.Digest != pin.Digest {
			return LoadedSkills{}, fmt.Errorf("skill %q content digest %q does not match the run's pinned digest %q; the registry content changed since the pin was written", pin.Name, bundle.Digest, pin.Digest)
		}
		bundles[pin.Name] = bundle
	}

	repoNames, repoViews, rejected, err := discoverRepositorySkills(checkoutDir)
	if err != nil {
		return LoadedSkills{}, err
	}
	visible, shadowed := Overlay(pins, repoNames)
	if len(visible) > maxVisibleSkills {
		return LoadedSkills{}, fmt.Errorf("the run's visible skill set is %d skills, above the %d skill limit", len(visible), maxVisibleSkills)
	}
	views := make([]SkillView, 0, len(visible))
	for _, visibleSkill := range visible {
		switch visibleSkill.Origin {
		case OriginBundle:
			view, err := bundleView(visibleSkill.Name, visibleSkill.Digest, bundles[visibleSkill.Name])
			if err != nil {
				return LoadedSkills{}, err
			}
			views = append(views, view)
		case OriginRepository:
			views = append(views, repoViews[visibleSkill.Name])
		default:
			return LoadedSkills{}, fmt.Errorf("skill %q carries unknown origin %q", visibleSkill.Name, visibleSkill.Origin)
		}
	}
	return LoadedSkills{Visible: views, Shadowed: shadowed, Rejected: rejected}, nil
}

// bundleView renders one pinned bundle's skill view from the exact bytes the
// digest was computed over: no re-read of the mount sits between digest
// verification and what a role is served, so a concurrent mount update
// cannot make the rendered content disagree with the pin. Source is the
// registered path — the path the deployment mounts into the worker pod —
// not the resolved kubelet timestamp path, which exists in this pod only.
func bundleView(name, digest string, bundle Bundle) (SkillView, error) {
	var (
		description  string
		instructions string
		assets       []string
		assetsBytes  int64
		rendered     bool
	)
	for _, file := range bundle.files {
		switch {
		case file.rel == "SKILL.md":
			var err error
			description, instructions, err = renderSkillDocument(name, file.content)
			if err != nil {
				return SkillView{}, fmt.Errorf("skill %q: %w", name, err)
			}
			rendered = true
		default:
			assets = append(assets, file.rel)
			assetsBytes += int64(len(file.content))
		}
	}
	if !rendered {
		return SkillView{}, fmt.Errorf("skill %q: the resolved bundle carries no SKILL.md", name)
	}
	if assets == nil {
		assets = []string{}
	}
	return SkillView{
		Name:         name,
		Origin:       OriginBundle,
		Digest:       digest,
		Description:  description,
		Instructions: instructions,
		Source:       bundle.Source,
		Files:        assets,
		FilesBytes:   assetsBytes,
	}, nil
}

// discoverRepositorySkills finds repository-local skills in the run's
// checkout: one SKILL.md per direct child directory of
// <checkoutDir>/.agents/skills. Repository content is untrusted third-party
// input, so discovery never trusts it further than reporting: a directory
// that is not a valid skill name, a non-regular SKILL.md (symlinks included
// — a checked-out symlink could point anywhere on the control pod's
// filesystem, and the loader must not follow it), an oversized or malformed
// document, or a frontmatter name mismatch is quarantined and reported,
// never failed or followed. Only the count bound fails closed, so the
// visible view stays deterministic and bounded.
func discoverRepositorySkills(checkoutDir string) ([]string, map[string]SkillView, []RejectedSkill, error) {
	root := filepath.Join(checkoutDir, ".agents", "skills")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, map[string]SkillView{}, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("discover repository-local skills under %s: %w", root, err)
	}
	names := make([]string, 0, len(entries))
	views := make(map[string]SkillView, len(entries))
	var rejected []RejectedSkill
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() {
			// Not a skill directory shape: a stray file or a symlink is not
			// a skill and is not a fault. Skip without reading anything —
			// in particular, never follow a checked-out symlink's target.
			continue
		}
		if labelErrs := utilvalidation.IsDNS1123Label(name); len(labelErrs) > 0 {
			rejected = append(rejected, RejectedSkill{Name: name, Reason: "directory name is not a valid DNS-1123 label: " + strings.Join(labelErrs, ", ")})
			continue
		}
		docPath := filepath.Join(root, name, "SKILL.md")
		info, err := os.Lstat(docPath)
		if err != nil {
			if os.IsNotExist(err) {
				// A directory without SKILL.md is not a skill; the discovery
				// location is <dir>/SKILL.md exactly.
				continue
			}
			return nil, nil, nil, fmt.Errorf("repository-local skill %q: stat SKILL.md: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			rejected = append(rejected, RejectedSkill{Name: name, Reason: "SKILL.md is not a regular file; repository-local skills must be plain checkout content"})
			continue
		}
		if info.Size() > maxSkillInstructionsBytes {
			rejected = append(rejected, RejectedSkill{Name: name, Reason: fmt.Sprintf("SKILL.md is %d bytes, above the %d byte limit", info.Size(), maxSkillInstructionsBytes)})
			continue
		}
		file, err := os.Open(docPath)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("repository-local skill %q: open SKILL.md: %w", name, err)
		}
		data, err := io.ReadAll(io.LimitReader(file, maxSkillInstructionsBytes+1))
		file.Close()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("repository-local skill %q: read SKILL.md: %w", name, err)
		}
		description, instructions, err := renderSkillDocument(name, data)
		if err != nil {
			rejected = append(rejected, RejectedSkill{Name: name, Reason: err.Error()})
			continue
		}
		names = append(names, name)
		views[name] = SkillView{
			Name:         name,
			Origin:       OriginRepository,
			Description:  description,
			Instructions: instructions,
		}
		if len(names) > maxRepoLocalSkills {
			return nil, nil, nil, fmt.Errorf("the checkout carries more than %d repository-local skills", maxRepoLocalSkills)
		}
	}
	sort.Strings(names)
	return names, views, rejected, nil
}

// renderSkillDocument parses one SKILL.md into its bounded description and
// instructions. Frontmatter is optional; an unterminated frontmatter block
// is a corrupt document and fails closed, as is a frontmatter name that
// disagrees with the skill's logical name — the logical name is the run's
// pinned identity, and a document claiming another name is not the content
// the pin verified. Content is data only: nothing here executes, however the
// document is written.
func renderSkillDocument(name string, data []byte) (description, instructions string, err error) {
	if len(data) == 0 {
		return "", "", fmt.Errorf("SKILL.md is empty")
	}
	body := string(data)
	scanLimit := len(body)
	if scanLimit > maxFrontmatterScanBytes {
		scanLimit = maxFrontmatterScanBytes
	}
	var frontName string
	if strings.HasPrefix(body, "---\n") {
		end := strings.Index(body[4:scanLimit], "\n---\n")
		if end < 0 {
			return "", "", fmt.Errorf("SKILL.md carries an unterminated frontmatter block")
		}
		block := body[4 : 4+end]
		body = strings.TrimPrefix(body[4+end+5:], "\n")
		for _, line := range strings.Split(block, "\n") {
			trimmed := strings.TrimSpace(line)
			key, value, found := strings.Cut(trimmed, ":")
			if !found {
				continue
			}
			value = strings.TrimSpace(value)
			value = strings.Trim(value, `"`)
			switch strings.TrimSpace(key) {
			case "name":
				frontName = value
			case "description":
				description = truncateRunes(value, maxSkillDescriptionBytes)
			}
		}
	}
	if frontName != "" && frontName != name {
		return "", "", fmt.Errorf("SKILL.md frontmatter names %q; a skill document must carry its logical name", frontName)
	}
	instructions = strings.TrimSpace(body)
	if len(instructions) > maxSkillInstructionsBytes {
		return "", "", fmt.Errorf("SKILL.md instructions are %d bytes, above the %d byte limit", len(instructions), maxSkillInstructionsBytes)
	}
	if instructions == "" {
		return "", "", fmt.Errorf("SKILL.md carries no instructions")
	}
	return description, instructions, nil
}

// truncateRunes bounds s to limit bytes without splitting a multi-byte rune.
func truncateRunes(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.ValidString(s[:limit]) {
		limit--
	}
	return s[:limit]
}
