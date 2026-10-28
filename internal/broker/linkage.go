package broker

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// SourceIssue is the trusted source-side issue identity resolved at run
// admission from the immutable CoderRun spec. It is the only identity a
// pull request body may claim a closing relationship against: the model
// may not select a different issue than the operator admitted, and the
// trusted source repository identity (Owner/Name) is the only one a
// cross-repo closing reference may name.
type SourceIssue struct {
	// Owner is the trusted source repository owner.
	Owner string `json:"owner"`
	// Name is the trusted source repository name.
	Name string `json:"name"`
	// Number is the trusted source issue number.
	Number int `json:"number"`
}

// IsZero reports whether the source issue is unset. A zero value is
// rejected by validatePolicy for resolve-issue runs because the link
// requirement cannot be enforced without it.
func (s SourceIssue) IsZero() bool {
	return s.Owner == "" && s.Name == "" && s.Number == 0
}

// Canonical returns the "owner/name" form of the source repository.
func (s SourceIssue) Canonical() string {
	if s.Owner == "" || s.Name == "" {
		return ""
	}
	return s.Owner + "/" + s.Name
}

// LinkageError is the failure mode for a PR body that does not carry the
// trusted source-issue closing relationship. Body is the observed body
// (after redaction passes) and Observed lists every supported-keyword
// reference found in it, with a flag for each that names whether it
// matched the trusted identity. The model-facing reason is the embedded
// Reason string; the structured fields exist for tests and observability
// so callers can distinguish "no closing reference", "wrong issue number",
// "wrong repository", and "ambiguous multiple references".
type LinkageError struct {
	Reason   string
	Observed []LinkageObservation
	// sentinel carries ErrLinkageMissing when the body had no
	// supported-keyword reference at all. Returning the sentinel
	// through a wrapped error keeps errors.Is usable while the
	// structured fields stay test- and observability-friendly.
	sentinel error
}

func (e *LinkageError) Error() string { return e.Reason }

// Unwrap exposes the sentinel so errors.Is(err, ErrLinkageMissing)
// works through a LinkageError.
func (e *LinkageError) Unwrap() error { return e.sentinel }

// LinkageObservation is one supported-keyword match found in a PR body.
// It is reported even when the match disagrees with the trusted identity
// so callers and tests can see the model's claim rather than only the
// rejection.
type LinkageObservation struct {
	// Keyword is the matched closing keyword in its original case.
	Keyword string
	// Owner is the owner named in the closing reference; empty when
	// the body used the bare "#N" form.
	Owner string
	// Name is the repository name named in the closing reference;
	// empty when the body used the bare "#N" form.
	Name string
	// Number is the issue number named in the closing reference.
	Number int
	// Match reports whether this observation matches the trusted
	// source-issue identity.
	Match bool
}

// ErrLinkageMissing is returned when the body contains no supported
// closing-keyword reference at all.
var ErrLinkageMissing = errors.New("publication denied: pull request body has no supported closing-keyword reference to the source issue")

// ErrClosingLinkageNotAdmitted is returned for a contribute-mode body that
// carries a closing-keyword reference although the publication policy never
// recorded the admission-time choice admitting closing linkage. The default
// contribute wording is non-closing ("Refs #N" or a plain issue URL);
// "Fixes #N" is a policy decision, never a model choice.
var ErrClosingLinkageNotAdmitted = errors.New("publication denied: pull request body carries closing linkage that the publication policy did not admit")

// ValidateContributeLinkage checks a contribute-mode pull request body
// against the mode-aware linkage contract. Linkage is optional: a body with
// no closing-keyword reference passes, so the default non-closing wording
// ("Refs #123", a plain issue URL, or no reference at all) is accepted, and
// a contribution with no upstream issue context carries no linkage line.
//
// When a closing-keyword reference does appear — in the bare, cross-repo,
// colon-separated, or full-issue-URL form, all of which GitHub honors as
// closing — it is admitted only under the explicit admission-time policy
// choice (allowClosing). When admitted,
// every closing reference must name the trusted source-issue identity —
// the same strict identity rule ValidateLinkage enforces for resolve-issue
// bodies: the model may not select a different issue or repository than the
// operator admitted, and an ambiguous body mixing correct and incorrect
// references is rejected. Without allowClosing, or without any trusted
// source-issue identity to reference, a closing keyword is refused before
// any forge write. Like ValidateLinkage, this is a pure function: no I/O,
// no global state, no clock.
func ValidateContributeLinkage(body string, source SourceIssue, allowClosing bool) error {
	observations := extractObservations(body, source)
	observations = append(observations, extractURLObservations(body, source)...)
	observations = append(observations, extractColonObservations(body, source)...)
	if len(observations) == 0 {
		return nil
	}
	if !allowClosing {
		return &LinkageError{
			Reason: fmt.Sprintf("publication denied: contribute-mode pull request bodies default to non-closing linkage (Refs #N or an issue URL); a closing reference requires an admission-time policy choice (found: %s)",
				FormatLinkageObservations(observations)),
			Observed: observations,
			sentinel: ErrClosingLinkageNotAdmitted,
		}
	}
	if source.IsZero() {
		// validatePolicy rejects this policy shape; the refusal is kept
		// here so no caller can reach a forge write with a closing
		// reference that names no verifiable identity.
		return &LinkageError{
			Reason:   "publication denied: closing linkage requires a trusted source-issue identity",
			Observed: observations,
		}
	}
	if source.Number <= 0 {
		return errors.New("publication denied: trusted source issue number is non-positive; cannot enforce linkage")
	}
	allMatch := true
	for _, obs := range observations {
		if !obs.Match {
			allMatch = false
			break
		}
	}
	if allMatch {
		return nil
	}
	return &LinkageError{
		Reason:   describeLinkageFailure(observations, source),
		Observed: observations,
	}
}

// bareClosingReferencePattern matches the simple form
// "<keyword> #<number>". The # must be the only thing between the
// keyword and the digits; the digits must be a non-empty positive
// integer with no thousands separator.
var bareClosingReferencePattern = regexp.MustCompile(`(?i)\b(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\b\s*#(\d+)\b`)

// crossRepoClosingReferencePattern matches the cross-repo form
// "<keyword> <owner>/<name>#<number>". The owner/name grammar accepts
// the same character set GitHub's URL grammar does: ASCII letters,
// digits, dot, dash, underscore. Whitespace between the keyword and
// the owner is required; whitespace between the name and the "#" is
// allowed (real PR bodies do that).
var crossRepoClosingReferencePattern = regexp.MustCompile(`(?i)\b(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\b\s+([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)\s*#(\d+)\b`)

// ValidateLinkage checks a pull request body for a closing-keyword
// reference that names the trusted source issue. It is a pure function:
// no I/O, no global state, no clock. Both publication paths (native
// broker and legacy executor) call it; behaviour must agree across
// paths and is locked down by a shared test table.
//
// The function is deliberately strict: it accepts only GitHub-supported
// closing keywords in the standard "Closes #N" or "Closes owner/repo#N"
// forms. Vague references like "Addresses" / "Refs" / "See also" and
// substring matches like "discloses" never satisfy linkage, so a
// model-author cannot smuggle an unrelated issue through the validator
// or fake a linked-issue relationship.
//
// When multiple closing references appear in the body, every one must
// name the same trusted identity; an ambiguous body is rejected so the
// model cannot silence a correct reference with an incorrect sibling.
func ValidateLinkage(body string, source SourceIssue) error {
	if source.IsZero() {
		return errors.New("publication denied: trusted source issue identity is unset; cannot enforce linkage")
	}
	if source.Number <= 0 {
		return errors.New("publication denied: trusted source issue number is non-positive; cannot enforce linkage")
	}
	observations := extractObservations(body, source)
	if len(observations) == 0 {
		// A missing closing reference is reported through the
		// sentinel so callers can use errors.Is to distinguish
		// "no reference at all" from a reference that names the
		// wrong identity. LinkageError is the structured form for
		// tests and observability; both wrap the same sentinel.
		return &LinkageError{Reason: ErrLinkageMissing.Error(), Observed: observations, sentinel: ErrLinkageMissing}
	}
	allMatch := true
	for _, obs := range observations {
		if !obs.Match {
			allMatch = false
			break
		}
	}
	if allMatch {
		return nil
	}
	// Distinguish the failure modes so tests and operators can see why
	// the body was rejected. A wrong number is the most common
	// failure mode (model wrote a different number than admission);
	// a wrong repository is a cross-repo slip; multiple-mixed is
	// the ambiguous body case.
	return &LinkageError{
		Reason:   describeLinkageFailure(observations, source),
		Observed: observations,
	}
}

// extractObservations walks the body and records every supported-keyword
// closing reference it finds. Observations that match the trusted
// identity have Match=true; others are still recorded so callers can
// audit the model's claim.
func extractObservations(body string, source SourceIssue) []LinkageObservation {
	var out []LinkageObservation
	// Walk the body by position. We use FindAllStringSubmatchIndex for
	// both forms; a single position is matched by whichever pattern
	// is satisfied first (bare first, then cross-repo), so the
	// observations are de-duplicated by index.
	type hit struct {
		start, end int
		kind       string
		owner      string
		name       string
		number     int
		keyword    string
	}
	var hits []hit
	for _, m := range bareClosingReferencePattern.FindAllStringSubmatchIndex(body, -1) {
		if len(m) < 4 {
			continue
		}
		number, _ := strconv.Atoi(body[m[2]:m[3]])
		// Pull the keyword from the leading non-capturing-group span
		// of the same match: the bare pattern has exactly one
		// capturing group (the number), so the keyword lives inside
		// the full match's tail.
		full := body[m[0]:m[1]]
		keyword := full
		if idx := strings.Index(full, "#"); idx > 0 {
			keyword = strings.TrimSpace(full[:idx])
		}
		hits = append(hits, hit{start: m[0], end: m[1], kind: "bare", number: number, keyword: keyword})
	}
	for _, m := range crossRepoClosingReferencePattern.FindAllStringSubmatchIndex(body, -1) {
		if len(m) < 8 {
			continue
		}
		// Keyword is in a non-capturing group, so the three
		// captures are owner (m[2:4]), name (m[4:6]), and number
		// (m[6:8]). The keyword itself is the leading non-capture
		// span of the full match.
		owner := body[m[2]:m[3]]
		name := body[m[4]:m[5]]
		number, _ := strconv.Atoi(body[m[6]:m[7]])
		full := body[m[0]:m[1]]
		keyword := full
		if idx := strings.IndexAny(full, " \t"); idx > 0 {
			keyword = full[:idx]
		}
		hits = append(hits, hit{start: m[0], end: m[1], kind: "cross-repo", owner: owner, name: name, number: number, keyword: keyword})
	}
	// Resolve hits into LinkageObservations, marking each as a match
	// when it names the trusted source identity. A bare "#N" form
	// matches the trusted identity when its number equals
	// source.Number; a cross-repo form matches when its owner/name
	// match the trusted canonical and its number matches.
	for _, h := range hits {
		obs := LinkageObservation{Keyword: h.keyword, Number: h.number}
		switch h.kind {
		case "bare":
			obs.Match = h.number == source.Number
		case "cross-repo":
			obs.Owner = h.owner
			obs.Name = h.name
			obs.Match = strings.EqualFold(h.owner, source.Owner) && strings.EqualFold(h.name, source.Name) && h.number == source.Number
		}
		out = append(out, obs)
	}
	return out
}

// urlClosingReferencePattern matches the full-URL closing form
// "<keyword>[: ] https://<host>[:port]/<owner>/<name>/issues/<number>".
// GitHub applies closing keywords to full issue URLs as well as to the "#N"
// and "owner/repo#N" forms (optionally with a colon after the keyword), so
// the contribute-mode contract — whose default is non-closing — must
// recognize the URL form as a closing claim even though the resolve-issue
// validator (whose failure mode is fail-closed missing linkage) never
// needed to reject it. A non-closing mention of the same URL matches
// nothing: only the supported closing keywords before the URL do.
var urlClosingReferencePattern = regexp.MustCompile(`(?i)\b(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\b\s*:?\s+https?://[A-Za-z0-9.-]+(?::\d{1,5})?/([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)/issues/(\d+)`)

// colonClosingReferencePattern matches the colon-separated closing forms
// "<keyword>: #<number>" and "<keyword>: <owner>/<name>#<number>". GitHub
// documents that a closing keyword may be followed by a colon, and the
// existing bare and cross-repo patterns require the reference to follow the
// keyword directly. resolve-issue fails closed on those inputs (no supported
// reference), which is safe; the contribute default must instead treat them
// as the closing claims GitHub will honor, so the contribute validator scans
// them separately and never relaxes ValidateLinkage.
var colonClosingReferencePattern = regexp.MustCompile(`(?i)\b(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\s*:\s*(?:([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)\s*)?#(\d+)\b`)

// extractColonObservations records the colon-form closing references of a
// body. It is confined to the contribute-mode contract for the same reason
// as extractURLObservations: these forms close issues on GitHub, so a
// non-closing default must see them, while the resolve-issue validator's
// fail-closed behavior stays byte-identical.
func extractColonObservations(body string, source SourceIssue) []LinkageObservation {
	var out []LinkageObservation
	for _, m := range colonClosingReferencePattern.FindAllStringSubmatchIndex(body, -1) {
		if len(m) < 8 {
			continue
		}
		number, _ := strconv.Atoi(body[m[6]:m[7]])
		obs := LinkageObservation{Number: number}
		if m[2] >= 0 {
			obs.Owner = body[m[2]:m[3]]
			obs.Name = body[m[4]:m[5]]
			obs.Match = strings.EqualFold(obs.Owner, source.Owner) && strings.EqualFold(obs.Name, source.Name) && source.Number > 0 && number == source.Number
		} else {
			obs.Match = source.Number > 0 && number == source.Number
		}
		full := body[m[0]:m[1]]
		keyword := full
		if idx := strings.IndexAny(full, ":#"); idx > 0 {
			keyword = strings.TrimSpace(full[:idx])
		}
		obs.Keyword = keyword
		out = append(out, obs)
	}
	return out
}

// extractURLObservations records the full-URL closing references of a body.
// It is deliberately confined to the contribute-mode contract: a URL closing
// reference names its repository explicitly, so it matches the trusted
// identity only when owner, name, and number all agree.
func extractURLObservations(body string, source SourceIssue) []LinkageObservation {
	var out []LinkageObservation
	for _, m := range urlClosingReferencePattern.FindAllStringSubmatchIndex(body, -1) {
		if len(m) < 8 {
			continue
		}
		owner := body[m[2]:m[3]]
		name := body[m[4]:m[5]]
		number, _ := strconv.Atoi(body[m[6]:m[7]])
		full := body[m[0]:m[1]]
		keyword := full
		if idx := strings.IndexAny(full, " \t"); idx > 0 {
			keyword = full[:idx]
		}
		out = append(out, LinkageObservation{
			Keyword: keyword,
			Owner:   owner,
			Name:    name,
			Number:  number,
			Match:   strings.EqualFold(owner, source.Owner) && strings.EqualFold(name, source.Name) && source.Number > 0 && number == source.Number,
		})
	}
	return out
}

// describeLinkageFailure composes a human-readable rejection reason
// from the observed references. It is only consulted when at least one
// observation disagrees with the trusted identity.
func describeLinkageFailure(observations []LinkageObservation, source SourceIssue) string {
	canonical := source.Canonical()
	parts := make([]string, 0, len(observations))
	for _, obs := range observations {
		desc := obs.Keyword + " "
		if obs.Owner != "" || obs.Name != "" {
			desc += obs.Owner + "/" + obs.Name
		}
		desc += "#" + strconv.Itoa(obs.Number)
		parts = append(parts, desc)
	}
	summary := strings.Join(parts, ", ")
	// Distinguish the most specific failure so the rejection is
	// actionable to a human reading the operator's record.
	wrongNumber := false
	wrongRepo := false
	for _, obs := range observations {
		if obs.Number != source.Number {
			wrongNumber = true
		}
		if obs.Owner != "" && (obs.Owner != source.Owner || obs.Name != source.Name) {
			wrongRepo = true
		}
	}
	switch {
	case len(observations) > 1:
		return fmt.Sprintf("publication denied: pull request body carries multiple closing references and not all match %s#%d (found: %s)", canonical, source.Number, summary)
	case wrongRepo:
		return fmt.Sprintf("publication denied: pull request body names a different source repository than admitted (expected %s, found %s)", canonical, summary)
	case wrongNumber:
		return fmt.Sprintf("publication denied: pull request body names a different source issue number than admitted (expected %s#%d, found %s)", canonical, source.Number, summary)
	default:
		// Single observation that disagrees for some other reason
		// (e.g. ambiguous owner casing that the model-data
		// normalization should still consider different).
		return fmt.Sprintf("publication denied: pull request body closing reference does not match the trusted source identity (expected %s#%d, found %s)", canonical, source.Number, summary)
	}
}

// FormatLinkageObservations renders the linkage observations as a
// short, human-readable summary suitable for embedding in a
// rejection reason or a NeedsHuman handoff. It is exported so the
// legacy executor can surface the model's claim — what it
// attempted to link — alongside the trusted identity.
func FormatLinkageObservations(observations []LinkageObservation) string {
	if len(observations) == 0 {
		return ""
	}
	parts := make([]string, 0, len(observations))
	for _, obs := range observations {
		desc := obs.Keyword + " "
		if obs.Owner != "" || obs.Name != "" {
			desc += obs.Owner + "/" + obs.Name
		}
		desc += "#" + strconv.Itoa(obs.Number)
		parts = append(parts, desc)
	}
	return strings.Join(parts, ", ")
}
