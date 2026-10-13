package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// The acceptance-criterion evidence contract (#249). One executor-neutral
// contract shared by the native broker publication path and the legacy
// OpenCode handoff, exactly as #248's linkage contract is: a pure validator
// here, two consumption boundaries, one shared truth table in
// criteria_test.go. Divergent acceptance rules between the paths would be a
// correctness regression.
//
// What this contract proves — and deliberately does not prove:
//
//   - Proves: the criterion set a PR's evidence block was authored against is
//     byte-for-byte the set pinned at admission (no model-supplied criteria,
//     no deletions, no rewrites); every pinned criterion carries exactly one
//     evidence record with a closed status vocabulary; a regression-check
//     claim names at least one file path or check URL; and the block's
//     coverage claim matches the coverage derivable from the records.
//   - Does not prove: that a cited test or check semantically enforces its
//     criterion. Presence of a reference is traceability, not semantic
//     sufficiency; auditing citations against the linked issue remains the
//     reviewer's job. No validator here reads the repository, runs tests, or
//     interprets reference text.

// Bounds for the pinned criterion set and the evidence block. Fixed
// constants: never derived from model or worker input, and shared by
// extraction and validation so a set and its block cannot disagree about
// what is bounded.
const (
	// MaxCriteria bounds the number of criteria extracted from one issue.
	// A larger acceptance section fails extraction (fail closed at
	// admission) rather than being silently truncated.
	MaxCriteria = 32
	// MaxCriterionTextBytes bounds one criterion's normalized text.
	MaxCriterionTextBytes = 512
	// MaxReferenceBytes bounds one evidence row's reference text.
	MaxReferenceBytes = 400
	// MaxBlockRows bounds the rows accepted inside one evidence block.
	MaxBlockRows = 64
)

// Criterion is one acceptance criterion pinned from the trusted source-issue
// snapshot at admission. ID is the ordinal assigned at extraction ("AC-1",
// "AC-2", …, in document order); Digest covers the normalized criterion
// text so a row authored against edited criterion text is rejected.
type Criterion struct {
	ID     string
	Text   string
	Digest string
}

// CriterionSet is the revision-pinned contract carried from admission to the
// publication/verification boundary. Digest covers the trusted issue
// identity plus every criterion digest in order, so adding, deleting,
// reordering, or editing any criterion changes it.
type CriterionSet struct {
	Issue    SourceIssue
	Criteria []Criterion
	Digest   string
}

// IsEmpty reports whether no criteria were pinned. An issue without a
// recognizable acceptance section yields an empty set: the evidence-block
// requirement is vacuous for that run and linkage remains the only PR-body
// contract. An empty set is a legitimate pin, distinct from an extraction
// failure.
func (s CriterionSet) IsEmpty() bool { return len(s.Criteria) == 0 }

// Criterion returns the pinned criterion for an ID.
func (s CriterionSet) Criterion(id string) (Criterion, bool) {
	for _, c := range s.Criteria {
		if c.ID == id {
			return c, true
		}
	}
	return Criterion{}, false
}

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

// Coverage values for the evidence-block header. DerivedCoverage is
// computed from the records; a claimed coverage that disagrees with the
// derivation is the mechanically caught form of "a weaker check presented as
// demonstrating the full requirement" (dogfood
// misospace/miso-gallery#502/#505).
const (
	CoverageFull    = "full"
	CoveragePartial = "partial"
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
	criteriaHeaderPattern = regexp.MustCompile(`^courier-criteria:v1 source=([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)#(\d+) set=sha256:([0-9a-f]{64}) coverage=(full|partial)$`)
	criteriaRowPattern    = regexp.MustCompile(`^- AC-(\d+) sha256:([0-9a-f]{64}): (implementation-evidence|regression-check|manual-verification|not-implemented|unknown)(?:\s[-—]\s?(.*))?$`)
	// referenceWithLocation requires at least one token naming a file path
	// (contains "/") or a URL. It is a floor on regression-check and
	// implementation-evidence claims, not a semantic check: "inspected by
	// hand" has no location and therefore cannot claim automated
	// enforcement.
	referenceWithLocation = regexp.MustCompile(`(?:https?://[^\s]+|[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.()-]+)+)`)
)

// ErrCriteriaBlockMissing is the sentinel for a PR body with no evidence
// block while the pinned set is non-empty. The legacy handoff treats it as
// a bounded repair; the native engine refuses the create.
var ErrCriteriaBlockMissing = errors.New("publication denied: pull request body carries no criterion-evidence block for the pinned acceptance criteria")

// CriteriaError is the structured failure for an evidence block that does
// not satisfy the pinned contract. The fields exist for tests, repair
// instructions, and observability so each violation is named precisely
// rather than folded into one string.
type CriteriaError struct {
	Reason    string
	Missing   []string // pinned criterion IDs with no row
	Unknown   []string // row IDs that are not pinned criteria
	Duplicate []string // IDs appearing more than once
	// BadDigest lists row IDs whose criterion digest disagrees with the
	// pin (block authored against a different criterion text).
	BadDigest []string
	// BadStatus lists row IDs whose status/reference pair violates the
	// per-status shape rules.
	BadStatus []string
	// CoverageClaim and CoverageDerived are set when they disagree.
	CoverageClaim   string
	CoverageDerived string
	sentinel        error
}

func (e *CriteriaError) Error() string { return e.Reason }

// Unwrap exposes ErrCriteriaBlockMissing when the body had no block at all.
func (e *CriteriaError) Unwrap() error { return e.sentinel }

// CriterionRecord is one parsed evidence row.
type CriterionRecord struct {
	ID        string
	Digest    string
	Status    CriterionStatus
	Reference string
}

// CriteriaReport is the parsed evidence block on success.
type CriteriaReport struct {
	Source    string
	SetDigest string
	// Coverage is the claimed coverage from the header; DerivedCoverage()
	// recomputes it from the records. On a successful validation they
	// agree.
	Coverage string
	Records  []CriterionRecord
}

// DerivedCoverage computes coverage against the pinned set: full only when
// every pinned criterion carries exactly one record, and every record is
// regression-check with a reference that names a location. Anything less —
// a missing row, an unpinned row, or any non-automated status — is partial.
// An honest partial record is a pass; claiming full for it is not.
func (r *CriteriaReport) DerivedCoverage(set CriterionSet) string {
	for _, c := range set.Criteria {
		rec, ok := findRecord(r, c.ID)
		if !ok || rec.Status != StatusRegressionCheck || !referenceWithLocation.MatchString(rec.Reference) {
			return CoveragePartial
		}
	}
	return CoverageFull
}

// StatusCounts returns per-status record counts for telemetry and outcome
// comments.
func (r *CriteriaReport) StatusCounts() map[CriterionStatus]int {
	counts := make(map[CriterionStatus]int, len(r.Records))
	for _, rec := range r.Records {
		counts[rec.Status]++
	}
	return counts
}

// ExtractCriteria identifies the acceptance criteria of a trusted source
// issue body and returns the pinned set. It is pure: no I/O, no clock, no
// network — the caller (admission) supplies the body from a trusted forge
// read, never from model input.
//
// Identification is deliberately narrow and deterministic: criteria are the
// top-level bullet items of the first section whose heading line reads
// "Acceptance" (as an ATX heading "## Acceptance", a bold label
// "**Acceptance:**", or a bare "Acceptance:" line), case-insensitively.
// Task-list checkboxes are accepted and their markers stripped. A section
// ends at the next heading-like line. Prose inside the section is ignored;
// indented lines continue the current criterion's text.
//
// Failure modes, all fail-closed for pinning (the admission boundary
// decides how to surface them; see HARNESS.md §4):
//
//   - no acceptance section, or a section with no bullets: an empty set —
//     a legitimate pin, not an error;
//   - more than one acceptance-headed section: ambiguous, error;
//   - more than MaxCriteria criteria or a criterion over the text bound:
//     error, so an unpinnable contract is never silently truncated.
func ExtractCriteria(issueBody string, issue SourceIssue) (CriterionSet, error) {
	if issue.IsZero() {
		return CriterionSet{}, errors.New("criteria extraction: trusted source issue identity is unset")
	}
	lines := strings.Split(strings.ReplaceAll(issueBody, "\r\n", "\n"), "\n")
	inSection := false
	seenSection := false
	var raw []string // one entry per top-level bullet: accumulated text lines
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if isHeadingLine(trimmed) {
			if isAcceptanceHeading(trimmed) {
				if seenSection {
					return CriterionSet{}, errors.New("criteria extraction: issue body carries more than one Acceptance section; the criterion set is ambiguous")
				}
				seenSection = true
				inSection = true
				continue
			}
			inSection = false
			continue
		}
		if !inSection {
			continue
		}
		if trimmed == "" {
			continue
		}
		indented := len(line) > 0 && (line[0] == ' ' || line[0] == '\t')
		if !indented && isTopLevelBullet(trimmed) {
			text := stripBulletMarker(trimmed)
			text = stripCheckboxMarker(text)
			raw = append(raw, text)
			continue
		}
		// Indented content — nested bullets included — continues the
		// previous top-level bullet; unindented prose inside the section
		// is ignored.
		if len(raw) > 0 && indented {
			continuation := stripBulletMarker(trimmed)
			continuation = stripCheckboxMarker(continuation)
			raw[len(raw)-1] += "\n" + continuation
		}
	}
	if len(raw) == 0 {
		return CriterionSet{Issue: issue}, nil
	}
	if len(raw) > MaxCriteria {
		return CriterionSet{}, fmt.Errorf("criteria extraction: issue carries %d acceptance criteria; the bound is %d and an over-bound set is never truncated", len(raw), MaxCriteria)
	}
	set := CriterionSet{Issue: issue, Criteria: make([]Criterion, 0, len(raw))}
	for i, text := range raw {
		normalized := normalizeCriterionText(text)
		if len(normalized) > MaxCriterionTextBytes {
			return CriterionSet{}, fmt.Errorf("criteria extraction: criterion AC-%d exceeds the %d-byte text bound", i+1, MaxCriterionTextBytes)
		}
		set.Criteria = append(set.Criteria, Criterion{
			ID:     fmt.Sprintf("AC-%d", i+1),
			Text:   normalized,
			Digest: digestString(normalized),
		})
	}
	set.Digest = set.digest()
	return set, nil
}

// digest covers the trusted issue identity and every criterion digest in
// order, so any addition, deletion, reorder, or edit changes the set digest.
func (s CriterionSet) digest() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s#%d\n", strings.ToLower(s.Issue.Canonical()), s.Issue.Number)
	for _, c := range s.Criteria {
		fmt.Fprintln(h, c.Digest)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// ValidateCriterionEvidence checks a pull request body's evidence block
// against the pinned criterion set. Pure: the caller supplies the live body
// (native: the proposed create body; legacy: a fresh read of the live PR).
//
// An empty pinned set is a vacuous pass with no report: linkage is then the
// only PR-body contract. For a non-empty set the body must carry exactly
// one well-formed block whose header names the trusted issue and the pinned
// set digest, and one row per pinned criterion. See the package comment for
// exactly what that does and does not establish.
func ValidateCriterionEvidence(prBody string, set CriterionSet) (*CriteriaReport, error) {
	if set.Issue.IsZero() {
		return nil, errors.New("publication denied: trusted source issue identity is unset; cannot enforce criterion evidence")
	}
	if set.IsEmpty() {
		return nil, nil
	}
	report, err := parseEvidenceBlock(prBody)
	if err != nil {
		return nil, err
	}
	if report == nil {
		return nil, &CriteriaError{
			Reason:   fmt.Sprintf("%s (issue %s#%d, set %s)", ErrCriteriaBlockMissing, strings.ToLower(set.Issue.Canonical()), set.Issue.Number, set.Digest),
			sentinel: ErrCriteriaBlockMissing,
		}
	}
	cerr := &CriteriaError{}
	// Pin integrity: the header must name the trusted issue and the pinned
	// set digest, so a block authored against any other criterion revision
	// is refused — the model cannot re-derive, extend, or shrink the
	// contract.
	owner, name, number := splitCanonical(report.Source)
	if !strings.EqualFold(owner, set.Issue.Owner) || !strings.EqualFold(name, set.Issue.Name) || number != set.Issue.Number {
		cerr.Reason = fmt.Sprintf("publication denied: criterion-evidence block names source %s; the trusted source issue is %s#%d", report.Source, strings.ToLower(set.Issue.Canonical()), set.Issue.Number)
		return report, cerr
	}
	if report.SetDigest != set.Digest {
		cerr.Reason = fmt.Sprintf("publication denied: criterion-evidence block pins set %s; the admitted criterion set is %s — the block was authored against different criteria", report.SetDigest, set.Digest)
		return report, cerr
	}
	// Exact ID coverage: every pinned criterion exactly once; no new or
	// unknown criterion IDs; no duplicates.
	seen := make(map[string]int, len(report.Records))
	for _, rec := range report.Records {
		seen[rec.ID]++
		if _, pinned := set.Criterion(rec.ID); !pinned {
			cerr.Unknown = append(cerr.Unknown, rec.ID)
		}
	}
	for id, n := range seen {
		if n > 1 {
			cerr.Duplicate = append(cerr.Duplicate, id)
		}
	}
	for _, c := range set.Criteria {
		if seen[c.ID] == 0 {
			cerr.Missing = append(cerr.Missing, c.ID)
		}
		if seen[c.ID] == 1 {
			if rec, _ := findRecord(report, c.ID); rec != nil && rec.Digest != c.Digest {
				cerr.BadDigest = append(cerr.BadDigest, c.ID)
			}
		}
	}
	// Per-status shape rules.
	for _, rec := range report.Records {
		if !validRecordShape(rec) {
			cerr.BadStatus = append(cerr.BadStatus, rec.ID)
		}
	}
	// Coverage claim honesty, derived against the pinned set: a body whose
	// records omit a criterion cannot derive full no matter how strong the
	// present rows are.
	derived := report.DerivedCoverage(set)
	if report.Coverage != derived {
		cerr.CoverageClaim = report.Coverage
		cerr.CoverageDerived = derived
	}
	if reason := describeCriteriaFailure(cerr); reason != "" {
		cerr.Reason = reason
		return report, cerr
	}
	return report, nil
}

// parseEvidenceBlock locates and parses the single evidence block. It
// returns (nil, nil) when no header line exists (the caller decides whether
// that is a violation), the parsed report on success, and an error for
// structurally impossible blocks (more than one header, over-bound rows).
func parseEvidenceBlock(body string) (*CriteriaReport, error) {
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	var report *CriteriaReport
	for i, line := range lines {
		trimmedRight := strings.TrimRight(line, " \t")
		m := criteriaHeaderPattern.FindStringSubmatch(trimmedRight)
		if m == nil {
			continue
		}
		if report != nil {
			return nil, errors.New("publication denied: pull request body carries more than one criterion-evidence block header")
		}
		report = &CriteriaReport{
			Source:    m[1] + "/" + m[2] + "#" + m[3],
			SetDigest: "sha256:" + m[4],
			Coverage:  m[5],
		}
		// Rows follow the header: consecutive "- AC-n" lines, blank
		// lines allowed between them; any other non-blank line ends the
		// block. A line shaped like an AC row that does not parse — an
		// unknown status token, a malformed digest, a stray duplicate —
		// is an explicit malformed-row rejection, never a silent block
		// terminator: otherwise a trailing invented or malformed row
		// after otherwise-valid rows would fail open as prose.
		for _, next := range lines[i+1:] {
			next = strings.TrimRight(next, " \t")
			if strings.TrimSpace(next) == "" {
				continue
			}
			rm := criteriaRowPattern.FindStringSubmatch(next)
			if rm == nil {
				if strings.HasPrefix(strings.TrimSpace(next), "- AC-") {
					return nil, fmt.Errorf("publication denied: criterion-evidence block contains a malformed row: %s", truncateForRowEcho(next))
				}
				break
			}
			if len(report.Records) >= MaxBlockRows {
				return nil, fmt.Errorf("publication denied: criterion-evidence block exceeds the %d-row bound", MaxBlockRows)
			}
			reference := ""
			if len(rm) == 5 {
				reference = strings.TrimSpace(rm[4])
			}
			if len(reference) > MaxReferenceBytes {
				return nil, fmt.Errorf("publication denied: criterion AC-%s evidence reference exceeds the %d-byte bound", rm[1], MaxReferenceBytes)
			}
			report.Records = append(report.Records, CriterionRecord{
				ID:        "AC-" + rm[1],
				Digest:    "sha256:" + rm[2],
				Status:    CriterionStatus(rm[3]),
				Reference: reference,
			})
		}
	}
	return report, nil
}

// validRecordShape enforces the per-status reference rules:
//
//   - regression-check and implementation-evidence must name a location
//     (file path or URL): an automated-enforcement claim with no citable
//     artifact is the "no test represented as regression-enforced" defect;
//   - manual-verification and not-implemented must carry a non-empty note;
//   - unknown may be bare.
//
// No rule here interprets the reference text.
func validRecordShape(rec CriterionRecord) bool {
	reference := strings.TrimSpace(rec.Reference)
	switch rec.Status {
	case StatusRegressionCheck, StatusImplementationEvidence:
		return reference != "" && referenceWithLocation.MatchString(reference)
	case StatusManualVerification, StatusNotImplemented:
		return reference != ""
	case StatusUnknown:
		return true
	default:
		return false
	}
}

func findRecord(r *CriteriaReport, id string) (*CriterionRecord, bool) {
	for i := range r.Records {
		if r.Records[i].ID == id {
			return &r.Records[i], true
		}
	}
	return nil, false
}

// describeCriteriaFailure renders the precise, actionable rejection reason.
func describeCriteriaFailure(e *CriteriaError) string {
	if e.Reason != "" {
		return e.Reason
	}
	var parts []string
	sort.Strings(e.Missing)
	sort.Strings(e.Unknown)
	sort.Strings(e.Duplicate)
	sort.Strings(e.BadDigest)
	sort.Strings(e.BadStatus)
	if len(e.Missing) > 0 {
		parts = append(parts, fmt.Sprintf("criterion evidence missing for %s", strings.Join(e.Missing, ", ")))
	}
	if len(e.Unknown) > 0 {
		parts = append(parts, fmt.Sprintf("rows name criteria that are not pinned (%s); the model cannot add criteria", strings.Join(e.Unknown, ", ")))
	}
	if len(e.Duplicate) > 0 {
		parts = append(parts, fmt.Sprintf("duplicate rows for %s", strings.Join(e.Duplicate, ", ")))
	}
	if len(e.BadDigest) > 0 {
		parts = append(parts, fmt.Sprintf("rows pin different criterion text for %s", strings.Join(e.BadDigest, ", ")))
	}
	if len(e.BadStatus) > 0 {
		parts = append(parts, fmt.Sprintf("status/reference shape violated for %s (regression-check and implementation-evidence must cite a file path or check URL; manual-verification and not-implemented must carry a note)", strings.Join(e.BadStatus, ", ")))
	}
	if e.CoverageClaim != "" {
		parts = append(parts, fmt.Sprintf("coverage claimed %s but the records derive %s", e.CoverageClaim, e.CoverageDerived))
	}
	if len(parts) == 0 {
		return ""
	}
	return "publication denied: " + strings.Join(parts, "; ")
}

// --- extraction helpers ---

// isHeadingLine reports whether a trimmed line is heading-like: an ATX
// heading, a bold label line ("**Acceptance:**", "**Problem:**"), or a bare
// label line ("Acceptance:", "Notes:") standing alone — the label word(s)
// plus an optional trailing colon and nothing after it. Bold and bare
// labels end a section so a later section's bullets are never swallowed as
// criteria. A line with content after the colon ("Note: see the build
// docs") is prose, never a heading, so unindented prose inside the
// acceptance section neither ends it nor truncates the criteria.
func isHeadingLine(trimmed string) bool {
	if strings.HasPrefix(trimmed, "#") {
		return true
	}
	return boldLabelPattern.MatchString(trimmed) || bareLabelPattern.MatchString(trimmed)
}

var (
	boldLabelPattern = regexp.MustCompile(`^\*\*[^*]+\*\*:?$`)
	// A bare label requires the trailing colon so ordinary short prose
	// ("continued line") is never mistaken for a heading.
	bareLabelPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9 _/&-]{0,31}:$`)
)

// isAcceptanceHeading reports whether a heading-like line names the
// acceptance section, after stripping ATX markers, bold wrappers, and an
// optional trailing colon.
func isAcceptanceHeading(trimmed string) bool {
	s := trimmed
	s = strings.TrimLeft(s, "#")
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "**")
	s = strings.TrimSuffix(s, "**")
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ":")
	s = strings.TrimSpace(s)
	return strings.EqualFold(s, "acceptance")
}

var topLevelBulletPattern = regexp.MustCompile(`^[-*+]\s+`)
var checkboxPattern = regexp.MustCompile(`^\[[ xX]\]\s+`)

func isTopLevelBullet(trimmed string) bool {
	return topLevelBulletPattern.MatchString(trimmed)
}

func stripBulletMarker(trimmed string) string {
	return strings.TrimSpace(topLevelBulletPattern.ReplaceAllString(trimmed, ""))
}

func stripCheckboxMarker(text string) string {
	return strings.TrimSpace(checkboxPattern.ReplaceAllString(text, ""))
}

// normalizeCriterionText canonicalizes a criterion's text for digesting:
// truncateForRowEcho bounds the malformed-row echo in rejection reasons so
// an oversized hostile row cannot inflate diagnostics.
func truncateForRowEcho(line string) string {
	const echoBound = 120
	if len(line) <= echoBound {
		return line
	}
	return line[:echoBound] + "…"
}

// normalizeCriterionText canonicalizes a criterion's text for digesting:
// CRLF already normalized, each line trimmed, internal whitespace runs
// collapsed to single spaces, lines joined with one space. Case is
// preserved.
func normalizeCriterionText(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, strings.Join(strings.Fields(line), " "))
	}
	joined := strings.Join(out, " ")
	return strings.TrimSpace(joined)
}

func digestString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// canonicalIssuePattern parses "owner/name#n".
var canonicalIssuePattern = regexp.MustCompile(`^([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)#(\d+)$`)

// splitCanonical parses "owner/name#n".
func splitCanonical(source string) (string, string, int) {
	m := canonicalIssuePattern.FindStringSubmatch(source)
	if m == nil {
		return "", "", 0
	}
	var number int
	fmt.Sscanf(m[3], "%d", &number)
	return m[1], m[2], number
}
