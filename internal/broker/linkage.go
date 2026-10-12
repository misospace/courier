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
	Owner string
	// Name is the trusted source repository name.
	Name string
	// Number is the trusted source issue number.
	Number int
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

// closingKeywordPattern matches a supported closing keyword as a
// word-bounded token. The vocabulary mirrors GitHub's documented
// supported set: close, closes, closed, fix, fixes, fixed, resolve,
// resolves, resolved. The case-insensitive flag lets the body write the
// keyword in any case; the word boundary is the only thing that
// protects against "discloses" / "refixes" substring matches.
//
// The leading word boundary keeps a preceding token (e.g. an attribute
// name "auto-closes:") from accidentally making "closes" a closing
// reference; the trailing one keeps a following alnum run (e.g.
// "closes-door") from forming a fake one.
var closingKeywordPattern = regexp.MustCompile(`(?i)\b(close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\b`)

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
