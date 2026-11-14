package reviewhandoff

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

const headSHA = "0123456789abcdef0123456789abcdef01234567"

func envelopePayload(t *testing.T, mods func(map[string]any)) string {
	t.Helper()
	m := map[string]any{
		"schema":          Schema,
		"review_id":       "rev-1",
		"head_sha":        headSHA,
		"verdict":         "changes_requested",
		"review_result":   "CHANGES_REQUESTED",
		"required_checks": "required",
		"items": []any{map[string]any{
			"id": "req-1", "severity": "blocker", "category": "security",
			"path": "main.go", "line": 12, "end_line": 14,
			"message": "fix <injection> & escape", "suggestion": "sanitize",
			"agent_prompt": "harden input validation",
		}},
		"items_total": 1, "items_truncated": false,
		"unresolved_requirement_ids":           []any{"req-1"},
		"unresolved_requirement_ids_total":     1,
		"unresolved_requirement_ids_truncated": false,
		"unresolved_required_checks":           []any{},
		"unresolved_review_obligations": []any{map[string]any{
			"id": "ob-1", "source": "check", "status": "pending",
			"cause": nil, "text": "ci must pass",
		}},
		"derived_obligation_state": nil,
	}
	if mods != nil {
		mods(m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return string(b)
}

// block builds a body: anchor line, fence line, payload line, closing fence
// line, then whatever trailing holds (model-authored markdown, or "").
func block(payload, fence, trailing string) string {
	return Anchor + "\n" + fence + "json\n" + payload + "\n" + fence + trailing
}

func TestParse(t *testing.T) {
	fourBackticks := envelopePayload(t, func(m map[string]any) {
		m["items"] = []any{map[string]any{
			"id": "req-9", "severity": "info", "category": "style",
			"message": "use ```go fmt``` before commit",
		}}
	})
	oversized := envelopePayload(t, func(m map[string]any) {
		m["future_padding"] = strings.Repeat("x", MaxPayloadBytes)
	})
	manyItems := envelopePayload(t, func(m map[string]any) {
		items := make([]any, 26)
		for i := range items {
			items[i] = map[string]any{"severity": "minor", "message": "m"}
		}
		m["items"] = items
	})
	manyObligations := envelopePayload(t, func(m map[string]any) {
		rows := make([]any, 11)
		for i := range rows {
			rows[i] = map[string]any{"id": "o", "source": "s", "status": "pending", "text": "t"}
		}
		m["unresolved_review_obligations"] = rows
	})
	good := envelopePayload(t, nil)

	cases := []struct {
		name    string
		content string
		wantErr error
		check   func(*testing.T, *Handoff, string)
	}{
		{
			name:    "comment-shaped body with trailing markdown",
			content: "managed header\n" + block(good, "```", "\n\n## summary\nmodel markdown\n") + Anchor + "\n```not counted\n",
			check: func(t *testing.T, h *Handoff, raw string) {
				if raw != good {
					t.Errorf("raw payload mismatch")
				}
				if h.ReviewID != "rev-1" || h.HeadSHA != headSHA {
					t.Errorf("envelope identity = %q/%q", h.ReviewID, h.HeadSHA)
				}
				if len(h.Items) != 1 {
					t.Fatalf("items = %d, want 1", len(h.Items))
				}
				it := h.Items[0]
				if it.Severity != "blocker" || it.Message != "fix <injection> & escape" {
					t.Errorf("item = %+v", it)
				}
				if it.Line == nil || *it.Line != 12 || it.EndLine != 14 {
					t.Errorf("item lines = %v/%d", it.Line, it.EndLine)
				}
				if len(h.UnresolvedReviewObligations) != 1 ||
					h.UnresolvedReviewObligations[0].Cause != "" {
					t.Errorf("obligations = %+v", h.UnresolvedReviewObligations)
				}
				if h.DerivedObligationState != nil {
					t.Errorf("derived state = %q, want nil", *h.DerivedObligationState)
				}
				if h.ItemsTotal != 1 || h.ItemsTruncated {
					t.Errorf("counters = %d/%v", h.ItemsTotal, h.ItemsTruncated)
				}
			},
		},
		{
			name:    "review-shaped body, no trailing content",
			content: block(good, "```", ""),
			check:   func(t *testing.T, h *Handoff, raw string) { _ = h; _ = raw },
		},
		{
			name:    "four-backtick fence wraps payload containing triple backticks",
			content: block(fourBackticks, "````", "\nafter\n"),
			check: func(t *testing.T, h *Handoff, _ string) {
				if len(h.Items) != 1 || h.Items[0].Message != "use ```go fmt``` before commit" {
					t.Errorf("items = %+v", h.Items)
				}
			},
		},
		{
			name: "unknown future envelope and item members tolerated",
			content: block(envelopePayload(t, func(m map[string]any) {
				m["future_extension"] = map[string]any{"k": "v"}
				m["items"].([]any)[0].(map[string]any)["future_item_field"] = 7
			}), "```", ""),
			check: func(t *testing.T, h *Handoff, _ string) {
				if len(h.Items) != 1 || h.Items[0].ID != "req-1" {
					t.Errorf("items = %+v", h.Items)
				}
			},
		},
		{name: "no anchor", content: "plain review body", wantErr: ErrNoBlock},
		{name: "anchor without fence", content: Anchor + " nothing follows", wantErr: ErrMalformed},
		{name: "two-backtick fence", content: block(good, "``", ""), wantErr: ErrMalformed},
		{name: "fence without the json info string", content: Anchor + "\n```\n" + good + "\n```\n", wantErr: ErrMalformed},
		{name: "fence with another info string", content: Anchor + "\n```yaml\n" + good + "\n```\n", wantErr: ErrMalformed},
		{name: "closing fence longer than the opener", content: Anchor + "\n```json\n" + good + "\n````\n", wantErr: ErrMalformed},
		{name: "text after the closing fence line is trailing content only", content: Anchor + "\n```json\n" + good + "\n``` tail\n", wantErr: ErrMalformed},
		{name: "fence shares line with payload", content: Anchor + "\n```json{x\n```json", wantErr: ErrMalformed},
		{name: "unterminated fence", content: Anchor + "\n```json\n" + good, wantErr: ErrMalformed},
		{name: "empty payload line", content: Anchor + "\n```json\n\n```\n", wantErr: ErrMalformed},
		{name: "multi-line payload", content: Anchor + "\n```json\n{}\n{}\n```\n", wantErr: ErrMalformed},
		{name: "invalid json", content: Anchor + "\n```json\n{nope\n```\n", wantErr: ErrMalformed},
		{
			name:    "schema v2 rejected",
			content: block(envelopePayload(t, func(m map[string]any) { m["schema"] = "review-handoff/v2" }), "```", ""),
			wantErr: ErrSchema,
		},
		{name: "oversized payload", content: block(oversized, "```", ""), wantErr: ErrOversized},
		{name: "26 items", content: block(manyItems, "```", ""), wantErr: ErrShape},
		{
			name: "severity outside closed set",
			content: block(envelopePayload(t, func(m map[string]any) {
				m["items"] = []any{map[string]any{"severity": "critical", "message": "m"}}
			}), "```", ""),
			wantErr: ErrShape,
		},
		{
			name: "item with empty message",
			content: block(envelopePayload(t, func(m map[string]any) {
				m["items"] = []any{map[string]any{"severity": "major", "message": ""}}
			}), "```", ""),
			wantErr: ErrShape,
		},
		{name: "11 obligations", content: block(manyObligations, "```", ""), wantErr: ErrShape},
		{
			name:    "missing review_result",
			content: block(envelopePayload(t, func(m map[string]any) { delete(m, "review_result") }), "```", ""),
			wantErr: ErrShape,
		},
		{
			name:    "required_checks present as empty string",
			content: block(envelopePayload(t, func(m map[string]any) { m["required_checks"] = "" }), "```", ""),
			check:   func(t *testing.T, h *Handoff, _ string) { _ = h },
		},
		{
			name:    "empty verdict",
			content: block(envelopePayload(t, func(m map[string]any) { m["verdict"] = "" }), "```", ""),
			wantErr: ErrShape,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, raw, err := Parse(tc.content)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				if h != nil || raw != "" {
					t.Fatalf("got h=%v raw=%q with error", h, raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if tc.check != nil {
				tc.check(t, h, raw)
			}
		})
	}
}

func TestFind(t *testing.T) {
	body := func(reviewID, sha string) string {
		return block(envelopePayload(t, func(m map[string]any) {
			m["review_id"] = reviewID
			m["head_sha"] = sha
		}), "```", "\ntrailing markdown\n")
	}

	cases := []struct {
		name       string
		bodies     []Body
		login      string
		head       string
		wantFound  bool
		wantErr    error
		wantReview string
	}{
		{
			name: "wrong author skipped, fold-matched author found",
			bodies: []Body{
				{Author: "random-contributor", Content: body("spoof", headSHA)},
				{Author: "Reviewer-Bot", Content: body("real", headSHA)},
			},
			login: "reviewer-bot", head: headSHA, wantFound: true, wantReview: "real",
		},
		{
			name: "stale head skipped, case-insensitive hex match found",
			bodies: []Body{
				{Author: "reviewer-bot", Content: body("stale", strings.Repeat("a", 40))},
				{Author: "reviewer-bot", Content: body("current", strings.ToUpper(headSHA))},
			},
			login: "reviewer-bot", head: headSHA, wantFound: true, wantReview: "current",
		},
		{
			name:       "first anchor wins in tampered multi-anchor body",
			bodies:     []Body{{Author: "reviewer-bot", Content: body("first", headSHA) + "\n" + Anchor + "\n```\n{second\n```\n"}},
			login:      "reviewer-bot",
			head:       headSHA,
			wantFound:  true,
			wantReview: "first",
		},
		{
			name:      "trusted author malformed block returns error",
			bodies:    []Body{{Author: "reviewer-bot", Content: Anchor + "\n```\n{broken\n```\n"}},
			login:     "reviewer-bot",
			head:      headSHA,
			wantFound: false,
			wantErr:   ErrMalformed,
		},
		{
			name: "untrusted malformed block skipped, not an error",
			bodies: []Body{
				{Author: "attacker", Content: Anchor + "\n```\n{broken\n```\n"},
				{Author: "reviewer-bot", Content: body("ok", headSHA)},
			},
			login: "reviewer-bot", head: headSHA, wantFound: true, wantReview: "ok",
		},
		{
			name:       "author-matching body without anchor skipped",
			bodies:     []Body{{Author: "reviewer-bot", Content: "no block here"}},
			login:      "reviewer-bot",
			head:       headSHA,
			wantFound:  false,
			wantReview: "",
		},
		{
			name:       "trusted author with only stale head not found",
			bodies:     []Body{{Author: "reviewer-bot", Content: body("stale", strings.Repeat("b", 40))}},
			login:      "reviewer-bot",
			head:       headSHA,
			wantFound:  false,
			wantReview: "",
		},
		{
			name: "no candidates", bodies: nil, login: "reviewer-bot", head: headSHA,
			wantFound: false, wantReview: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, raw, found, err := Find(tc.bodies, tc.login, tc.head)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Find: %v", err)
			}
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v", found, tc.wantFound)
			}
			if !tc.wantFound {
				if h != nil || raw != "" {
					t.Fatalf("got h=%v raw=%q when not found", h, raw)
				}
				return
			}
			if h.ReviewID != tc.wantReview {
				t.Errorf("review_id = %q, want %q", h.ReviewID, tc.wantReview)
			}
			if !strings.Contains(raw, `"review_id":"`+tc.wantReview+`"`) {
				t.Errorf("raw payload does not contain chosen review_id: %s", raw)
			}
		})
	}
}
