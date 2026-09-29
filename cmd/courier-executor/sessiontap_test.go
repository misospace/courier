package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestSessionTapCapturesSessionAcrossWritesAndFlush proves a sessionID line
// split across Write calls is captured when it completes, and a trailing
// partial line with no newline is captured only on Flush.
func TestSessionTapCapturesSessionAcrossWritesAndFlush(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out)

	first := `{"type":"text","sessionID":"ses_split1","part":{"type":"text","text":"hello"}}`
	_, _ = tap.Write([]byte(first[:10]))
	_, _ = tap.Write([]byte(first[10:]))
	if tap.sessionID != "" {
		t.Fatalf("sessionID = %q before the line completed, want empty", tap.sessionID)
	}
	_, _ = tap.Write([]byte("\n"))
	if tap.sessionID != "ses_split1" {
		t.Fatalf("sessionID = %q, want ses_split1 after the line completed", tap.sessionID)
	}

	partial := `{"type":"text","sessionID":"ses_partial1","part":{"type":"text","text":"partial"}}`
	_, _ = tap.Write([]byte(partial))
	if tap.lastText != "hello" {
		t.Fatalf("lastText = %q before Flush, want hello", tap.lastText)
	}
	if err := tap.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if tap.lastText != "partial" {
		t.Fatalf("lastText = %q after Flush, want partial from the trailing line", tap.lastText)
	}
	want := first + "\n" + partial
	if out.String() != want {
		t.Fatalf("forwarded output = %q, want %q (the tap must not alter bytes)", out.String(), want)
	}
}

// TestSessionTapDropsOversizedUnterminatedLine proves a single unterminated
// line larger than tapMaxLine is dropped, and later complete lines still
// parse.
func TestSessionTapDropsOversizedUnterminatedLine(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out)

	big := make([]byte, tapMaxLine+1)
	for i := range big {
		big[i] = 'x'
	}
	_, _ = tap.Write(big)
	if tap.sessionID != "" || tap.lastText != "" {
		t.Fatalf("state captured from an oversized line: sessionID=%q lastText=%q", tap.sessionID, tap.lastText)
	}

	_, _ = tap.Write(append([]byte(`{"type":"text","sessionID":"ses_after_big","part":{"type":"text","text":"ok"}}`), '\n'))
	if tap.sessionID != "ses_after_big" {
		t.Fatalf("sessionID = %q, want ses_after_big (later complete lines must still parse)", tap.sessionID)
	}
}

// TestSessionTapIgnoresNonJSONLines proves non-JSON lines capture no state,
// and a JSON line does.
func TestSessionTapIgnoresNonJSONLines(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out)
	_, _ = tap.Write([]byte("plain text\n{broken json\n"))
	if tap.sessionID != "" || tap.lastText != "" {
		t.Fatalf("non-JSON lines captured state: sessionID=%q lastText=%q", tap.sessionID, tap.lastText)
	}
	_, _ = tap.Write(append([]byte(`{"type":"text","sessionID":"ses_json1","part":{"type":"text","text":"json line"}}`), '\n'))
	if tap.sessionID != "ses_json1" {
		t.Fatalf("sessionID = %q, want ses_json1", tap.sessionID)
	}
}

// TestSessionTapFirstSessionIDWins proves the first captured sessionID is
// kept, while lastText keeps updating.
func TestSessionTapFirstSessionIDWins(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out)
	_, _ = tap.Write(append([]byte(`{"type":"text","sessionID":"ses_first","part":{"type":"text","text":"one"}}`), '\n'))
	_, _ = tap.Write(append([]byte(`{"type":"text","sessionID":"ses_second","part":{"type":"text","text":"two"}}`), '\n'))
	if tap.sessionID != "ses_first" {
		t.Fatalf("sessionID = %q, want ses_first (the first wins)", tap.sessionID)
	}
	if tap.lastText != "two" {
		t.Fatalf("lastText = %q, want two (text keeps updating)", tap.lastText)
	}
}

// TestSessionTapTruncatesMultibyteTextAtRuneLimit proves lastText is
// truncated at tapLastTextLimit runes, not bytes, for multibyte text.
func TestSessionTapTruncatesMultibyteTextAtRuneLimit(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out)
	text := strings.Repeat("é", tapLastTextLimit+50)
	_, _ = tap.Write(append([]byte(`{"type":"text","sessionID":"ses_multibyte","part":{"type":"text","text":"`+text+`"}}`), '\n'))
	if got := len([]rune(tap.lastText)); got != tapLastTextLimit {
		t.Fatalf("lastText = %d runes, want %d (truncation must be rune-safe)", got, tapLastTextLimit)
	}
	if tap.lastText != strings.Repeat("é", tapLastTextLimit) {
		t.Fatalf("lastText is not the first %d runes of the text", tapLastTextLimit)
	}
}

// TestSessionTapIgnoresNonTextParts proves a part that is not text does not
// update lastText.
func TestSessionTapIgnoresNonTextParts(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out)
	_, _ = tap.Write(append([]byte(`{"type":"step","sessionID":"ses_step1","part":{"type":"step","text":"not text"}}`), '\n'))
	if tap.lastText != "" {
		t.Fatalf("lastText = %q, want empty (non-text parts are ignored)", tap.lastText)
	}
	if tap.sessionID != "ses_step1" {
		t.Fatalf("sessionID = %q, want ses_step1", tap.sessionID)
	}
}
