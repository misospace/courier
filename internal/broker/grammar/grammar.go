// Package grammar owns the criterion-evidence block grammar (#375): the
// validator's own patterns, the closed status vocabulary, and the templates
// and sentences derived from them.
//
// It is a leaf — importing it costs regexp and strings and nothing else — so
// that both prompt surfaces, the legacy repair continuation and the
// resolve-issue goal, render the grammar from the authoritative patterns
// without reaching internal/broker: the broker's in-package tests import the
// harness, which imports the executor, and the harness's in-package tests
// import the broker, so an executor that imported the broker would make the
// two mutually dependent through test files — and would drag the broker's
// forge, git, branch, and Kubernetes client dependencies into a package that
// needs none of them.
//
// Nothing here reads a repository, a clock, or a forge. The package is the
// shared shape, not a broker dependency.
package grammar

import (
	"regexp"
	"strings"
)

// CriterionStatus is the closed vocabulary for one criterion's evidence
// record. The statuses distinguish what kind of evidence exists; none of
// them asserts semantic sufficiency. An absence of automated enforcement
// must be recorded as manual-verification, not-implemented, or unknown —
// never as regression-check.
type CriterionStatus string

const (
	// StatusImplementationEvidence: the criterion is implemented at the
	// cited paths/commits; no automated enforcement claimed.
	StatusImplementationEvidence CriterionStatus = "implementation-evidence"
	// StatusRegressionCheck: an automated check (test path, workflow step,
	// or check URL) is cited that the author claims fails if the criterion
	// regresses. The claim is auditable, not mechanically verified.
	StatusRegressionCheck CriterionStatus = "regression-check"
	// StatusManualVerification: a human verified it; no automated check.
	StatusManualVerification CriterionStatus = "manual-verification"
	// StatusNotImplemented: intentionally not implemented in this change.
	StatusNotImplemented CriterionStatus = "not-implemented"
	// StatusUnknown: not established either way.
	StatusUnknown CriterionStatus = "unknown"
)

// Evidence-block grammar. The header is one line:
//
//	courier-criteria:v1 source=<owner/name>#<n> set=sha256:<64hex> coverage=<full|partial>
//
// followed by one row per pinned criterion:
//
//   - AC-<n> sha256:<64hex>: <status> - <reference>
//
// The separator between status and reference accepts " - " or " — ".
// Rows are the only machine-read coverage surface in the PR body; prose
// outside the block is not a coverage claim.
var (
	HeaderPattern = regexp.MustCompile(`^courier-criteria:v1 source=([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)#(\d+) set=sha256:([0-9a-f]{64}) coverage=(full|partial)$`)
	RowPattern    = regexp.MustCompile(`^- AC-(\d+) sha256:([0-9a-f]{64}): (implementation-evidence|regression-check|manual-verification|not-implemented|unknown)(?:\s[-—]\s?(.*))?$`)
	// ReferenceWithLocation requires at least one token naming a file path
	// (contains "/") or a URL. It is a floor on regression-check and
	// implementation-evidence claims, not a semantic check: "inspected by
	// hand" has no location and therefore cannot claim automated
	// enforcement.
	//
	// It is composed from the two exported alternatives below rather
	// than spelled as one literal, so a consumer that must probe the
	// same references the validator accepts (the executor's cited-path
	// check) can take the path alternative itself instead of
	// hand-mirroring it — a mirrored copy drifts silently when either
	// side changes (#375 review).
	ReferenceWithLocation = regexp.MustCompile(`(?:` + ReferenceURLPattern.String() + `|` + ReferencePathPattern.String() + `)`)
)

// ReferenceURLPattern is the URL alternative of ReferenceWithLocation:
// the shape the validator accepts as a citable check URL. It is
// exported so the composition of ReferenceWithLocation from its
// alternatives is pinned by test rather than assumed.
var ReferenceURLPattern = regexp.MustCompile(`https?://[^\s]+`)

// ReferencePathPattern is the file-path alternative of
// ReferenceWithLocation: the shape the validator accepts as a citable
// path. The executor's cited-evidence extraction (#367, reached by the
// #375 review) matches this exact pattern, so a reference the contract
// refuses is never probed and one it accepts is probed in the same
// shape it was judged in.
var ReferencePathPattern = regexp.MustCompile(`[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.()-]+)+`)

// Placeholders naming each capture group of the taught grammar (#375).
// Consumers (the repair continuation, the coordinator goal) name the
// grammar with these instead of restating the patterns: the grammar is
// derived from the validator patterns, so editing a validator cannot leave
// a consumer teaching a shape the validator rejects. The two hand-written
// copies that preceded this had already drifted from the validator and from
// each other.
const (
	SourceReferencePlaceholder = "<owner/name>#<n>"
	DigestPlaceholder          = "<64hex>"
	CriterionPlaceholder       = "<n>"
	StatusPlaceholder          = "<status>"
	ReferencePlaceholder       = "<reference>"
)

// literalAlternationGroup matches a capture group whose whole body is a
// literal alternation — how the validator spells a closed vocabulary
// ("(full|partial)"). The taught grammar names the bare alternatives,
// because the alternatives are the vocabulary being taught.
var literalAlternationGroup = regexp.MustCompile(`\(([A-Za-z0-9|_-]+)\)`)

// derivedGrammar renders a validator pattern as a teachable template: strip
// the anchors, substitute each capture group's source with the placeholder
// naming it, then flatten any literal alternation the substitutions left
// behind.
//
// The substitution order is not free, and the invariant it rests on is not
// "a placeholder is never itself substituted". It is this: at the moment a
// key is applied, that key must occur exactly once in the template as the
// earlier substitutions have left it, and no key may be a substring of
// another key. Both halves are load-bearing. A key that has already been
// substituted away occurs zero times, so its substitution silently does
// nothing; a key that is a substring of another key can be consumed by the
// wrong, broader match. `(\d+)` is a substring of the header's
// source-reference key `([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)#(\d+)`, so
// adding `(\d+)` to the header list ahead of that key would rewrite the tail
// of the source reference and leave the capture groups un-substituted in the
// taught template — and adding it after would substitute nothing at all.
// TestSubstitutionKeysAreUnambiguous pins the invariant so that either
// addition fails loudly instead of corrupting the template.
func derivedGrammar(pattern *regexp.Regexp, substitutions ...string) string {
	template := strings.TrimSuffix(strings.TrimPrefix(pattern.String(), "^"), "$")
	for i := 0; i+1 < len(substitutions); i += 2 {
		template = strings.ReplaceAll(template, substitutions[i], substitutions[i+1])
	}
	return literalAlternationGroup.ReplaceAllString(template, "$1")
}

// headerSubstitutions and rowSubstitutions are the explicit (capture-group
// source, placeholder) pairs each template is derived from. They are named
// so the key-uniqueness invariant above is testable rather than implicit.
var (
	headerSubstitutions = []string{
		`([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)#(\d+)`, SourceReferencePlaceholder,
		`([0-9a-f]{64})`, DigestPlaceholder,
	}
	rowSubstitutions = []string{
		`(\d+)`, CriterionPlaceholder,
		`([0-9a-f]{64})`, DigestPlaceholder,
		`(implementation-evidence|regression-check|manual-verification|not-implemented|unknown)`, StatusPlaceholder,
		`(?:\s[-—]\s?(.*))?`, " - " + ReferencePlaceholder,
	}
)

var (
	headerTemplate = derivedGrammar(HeaderPattern, headerSubstitutions...)
	rowTemplate    = derivedGrammar(RowPattern, rowSubstitutions...)
)

// CriteriaHeaderGrammar returns the header template every capture group of
// which is replaced by its placeholder, derived from the pattern the
// validator matches.
func CriteriaHeaderGrammar() string { return headerTemplate }

// CriteriaHeaderShape renders CriteriaHeaderGrammar with the source
// reference filled in.
func CriteriaHeaderShape(source string) string {
	return strings.ReplaceAll(headerTemplate, SourceReferencePlaceholder, source)
}

// CriteriaRowGrammar returns the per-criterion row template derived from the
// pattern the validator matches.
func CriteriaRowGrammar() string { return rowTemplate }

// statusVocabulary is the closed vocabulary, in the row pattern's own
// alternation order.
var statusVocabulary = []CriterionStatus{
	StatusImplementationEvidence,
	StatusRegressionCheck,
	StatusManualVerification,
	StatusNotImplemented,
	StatusUnknown,
}

// StatusVocabulary returns the closed status vocabulary in validator order.
// The returned slice is a copy: the order is shared with the sentences
// rendered from it.
func StatusVocabulary() []CriterionStatus {
	out := make([]CriterionStatus, len(statusVocabulary))
	copy(out, statusVocabulary)
	return out
}

// StatusVocabularySentence names the closed vocabulary as one prose list, for
// a prompt or a repair instruction.
func StatusVocabularySentence() string {
	names := make([]string, 0, len(statusVocabulary))
	for _, status := range statusVocabulary {
		names = append(names, string(status))
	}
	if len(names) == 0 {
		return ""
	}
	return strings.Join(names[:len(names)-1], ", ") + ", or " + names[len(names)-1]
}

// ReferenceRequirement is what one status demands of its evidence
// reference: a note, a nameable location, or nothing.
type ReferenceRequirement int

const (
	ReferenceFree ReferenceRequirement = iota
	ReferenceNoteRequired
	ReferenceLocationRequired
)

// referenceRules is the one table of per-status reference rules. The
// validator (validRecordShape, in internal/broker) enforces it and
// StatusReferenceRuleSentence renders it, so the validator and the prose it
// teaches cannot disagree. The order is the reading order of the rendered
// sentence, which is not the vocabulary order.
var referenceRules = []struct {
	status      CriterionStatus
	requirement ReferenceRequirement
}{
	{StatusRegressionCheck, ReferenceLocationRequired},
	{StatusImplementationEvidence, ReferenceLocationRequired},
	{StatusManualVerification, ReferenceNoteRequired},
	{StatusNotImplemented, ReferenceNoteRequired},
	{StatusUnknown, ReferenceFree},
}

// ReferenceRequirementFor resolves a status's requirement. A status outside
// the table has no rule to satisfy and is rejected.
func ReferenceRequirementFor(status CriterionStatus) (ReferenceRequirement, bool) {
	for _, rule := range referenceRules {
		if rule.status == status {
			return rule.requirement, true
		}
	}
	return ReferenceFree, false
}

// StatusReferenceRuleSentence renders the per-status reference rules as the
// one sentence the repair instruction, the coordinator goal, and the
// validator's own rejection reason all carry.
func StatusReferenceRuleSentence() string {
	var locations, notes []string
	for _, rule := range referenceRules {
		switch rule.requirement {
		case ReferenceLocationRequired:
			locations = append(locations, string(rule.status))
		case ReferenceNoteRequired:
			notes = append(notes, string(rule.status))
		}
	}
	return strings.Join(locations, " and ") + " must cite a file path or check URL; " +
		strings.Join(notes, " and ") + " must carry a note"
}
