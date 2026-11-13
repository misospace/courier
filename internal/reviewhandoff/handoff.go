// Package reviewhandoff parses and selects the machine-readable remediation
// handoff ("review-handoff/v1") that pr-reviewer-action embeds as a fenced
// JSON block in its managed pull-request comment or native review body.
//
// The payload is untrusted data: it passes through model-authored content and
// may be tampered with. Everything extracted is validated against the
// published upstream contract before a caller may rely on it.
package reviewhandoff

import (
	"encoding/json"
	"errors"
	"strings"
)

const (
	// Anchor is the exact HTML-comment line that opens the handoff block.
	Anchor = "<!-- ai-pr-reviewer-handoff:v1 -->"
	// Schema is the exact schema value a current payload must declare.
	Schema = "review-handoff/v1"
	// MaxPayloadBytes caps the raw one-line JSON payload per the format.
	MaxPayloadBytes = 8192
)

// Structural limits from the published format.
const (
	maxItems       = 25
	maxObligations = 10
)

// Sentinel failures; callers should compare with errors.Is. Missing or
// empty required members and limit violations report ErrShape; schema
// mismatch reports ErrSchema; fence/JSON-level breakage reports
// ErrMalformed.
var (
	ErrNoBlock   = errors.New("reviewhandoff: no handoff anchor in body")
	ErrSchema    = errors.New("reviewhandoff: unexpected schema")
	ErrOversized = errors.New("reviewhandoff: payload exceeds maximum size")
	ErrMalformed = errors.New("reviewhandoff: malformed fenced payload")
	ErrShape     = errors.New("reviewhandoff: payload violates shape constraints")
)

// IsValidationFailure reports whether err is a failure of the handoff
// payload itself — fence, schema, size, or shape — rather than a failure of
// the read that carried it. A reviewer's own body that fails validation is a
// defect at the producer and deserves visibility; a transport failure is
// ordinary infrastructure noise.
func IsValidationFailure(err error) bool {
	return errors.Is(err, ErrMalformed) ||
		errors.Is(err, ErrSchema) ||
		errors.Is(err, ErrOversized) ||
		errors.Is(err, ErrShape)
}

// Item is one remediation item in the handoff envelope.
type Item struct {
	ID          string `json:"id,omitempty"`
	Severity    string `json:"severity"`
	Category    string `json:"category,omitempty"`
	Path        string `json:"path,omitempty"`
	Line        *int   `json:"line,omitempty"`
	EndLine     int    `json:"end_line,omitempty"`
	Message     string `json:"message"`
	Suggestion  string `json:"suggestion,omitempty"`
	AgentPrompt string `json:"agent_prompt,omitempty"`
}

// Obligation is one unresolved review-obligation row.
type Obligation struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Status string `json:"status"`
	Cause  string `json:"cause,omitempty"`
	Text   string `json:"text"`
}

// Handoff is the validated review-handoff/v1 envelope. Unknown members are
// ignored for forward compatibility; the counter members other than
// items_total/items_truncated are not modeled.
type Handoff struct {
	Schema                      string       `json:"schema"`
	ReviewID                    string       `json:"review_id"`
	HeadSHA                     string       `json:"head_sha"`
	Verdict                     string       `json:"verdict"`
	ReviewResult                string       `json:"review_result"`
	RequiredChecks              string       `json:"required_checks"`
	Items                       []Item       `json:"items,omitempty"`
	ItemsTotal                  int          `json:"items_total"`
	ItemsTruncated              bool         `json:"items_truncated"`
	UnresolvedRequirementIDs    []string     `json:"unresolved_requirement_ids,omitempty"`
	UnresolvedRequiredChecks    []string     `json:"unresolved_required_checks,omitempty"`
	UnresolvedReviewObligations []Obligation `json:"unresolved_review_obligations,omitempty"`
	DerivedObligationState      *string      `json:"derived_obligation_state,omitempty"`
}

// Body is one pull-request comment or native review body with its author.
type Body struct {
	Author  string
	Content string
}

// Parse extracts the FIRST anchored fenced block from content (later anchors
// indicate tampering and are ignored as trailing content), validates it
// against the review-handoff/v1 contract, and returns the decoded envelope
// plus the raw one-line JSON payload.
func Parse(content string) (*Handoff, string, error) {
	payload, err := extractPayload(content)
	if err != nil {
		return nil, "", err
	}
	if len(payload) > MaxPayloadBytes {
		return nil, "", ErrOversized
	}
	var h Handoff
	if err := json.Unmarshal([]byte(payload), &h); err != nil {
		return nil, "", ErrMalformed
	}
	// Second pass: review_result and required_checks may be empty strings
	// but must be present as JSON strings; pointers distinguish "" from null.
	var presence struct {
		ReviewResult   *string `json:"review_result"`
		RequiredChecks *string `json:"required_checks"`
	}
	if err := json.Unmarshal([]byte(payload), &presence); err != nil {
		return nil, "", ErrMalformed
	}
	if h.Schema != Schema {
		return nil, "", ErrSchema
	}
	if h.ReviewID == "" || h.HeadSHA == "" || h.Verdict == "" ||
		presence.ReviewResult == nil || presence.RequiredChecks == nil {
		return nil, "", ErrShape
	}
	if len(h.Items) > maxItems || len(h.UnresolvedReviewObligations) > maxObligations {
		return nil, "", ErrShape
	}
	for _, it := range h.Items {
		if !validSeverity(it.Severity) || it.Message == "" {
			return nil, "", ErrShape
		}
	}
	return &h, payload, nil
}

// Find returns the first body authored by reviewerLogin (case-insensitive)
// whose payload was produced for expectedHeadSHA (case-insensitive). Bodies
// without the anchor or from other authors are skipped. A trusted author's
// body that fails Parse is a real defect in a body the action owns, so it is
// returned as an error rather than silently skipped; a trusted body whose
// head_sha differs is only a stale review of another head and is skipped.
func Find(bodies []Body, reviewerLogin, expectedHeadSHA string) (*Handoff, string, bool, error) {
	for _, b := range bodies {
		if !strings.Contains(b.Content, Anchor) {
			continue
		}
		if !strings.EqualFold(b.Author, reviewerLogin) {
			continue
		}
		h, raw, err := Parse(b.Content)
		if err != nil {
			return nil, "", false, err
		}
		if !strings.EqualFold(h.HeadSHA, expectedHeadSHA) {
			continue
		}
		return h, raw, true, nil
	}
	return nil, "", false, nil
}

// extractPayload returns the single raw JSON line from the fenced block that
// follows the first anchor. The grammar mirrors the upstream producer exactly:
// the anchor ends its line, the opening fence is N>=3 backticks plus the
// literal info string "json", the payload is one line, and the closing fence
// is exactly the opening delimiter — so no payload byte can close the block.
func extractPayload(content string) (string, error) {
	i := strings.Index(content, Anchor)
	if i < 0 {
		return "", ErrNoBlock
	}
	lines := strings.Split(strings.TrimSuffix(content[i+len(Anchor):], "\n"), "\n")
	if len(lines) < 4 || strings.TrimSuffix(lines[0], "\r") != "" {
		return "", ErrMalformed
	}
	fence := strings.TrimSuffix(lines[1], "\r")
	n := 0
	for n < len(fence) && fence[n] == '`' {
		n++
	}
	if n < 3 || fence[n:] != "json" {
		return "", ErrMalformed
	}
	payload := strings.TrimSuffix(lines[2], "\r")
	if payload == "" {
		return "", ErrMalformed
	}
	// The producer closes with the bare backtick delimiter; "json" opens
	// the fence only.
	if strings.TrimSuffix(lines[3], "\r") != strings.Repeat("`", n) {
		return "", ErrMalformed
	}
	return payload, nil
}

func validSeverity(s string) bool {
	switch s {
	case "blocker", "major", "minor", "info":
		return true
	}
	return false
}
