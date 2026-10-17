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
	tap := newSessionTap(&out, defaultExhaustionSignals())
	model := tap.beginAttempt()

	first := `{"type":"text","sessionID":"ses_split1","part":{"type":"text","text":"hello"}}`
	_, _ = model.Write([]byte(first[:10]))
	_, _ = model.Write([]byte(first[10:]))
	if tap.session() != "" {
		t.Fatalf("sessionID = %q before the line completed, want empty", tap.session())
	}
	_, _ = model.Write([]byte("\n"))
	if tap.session() != "ses_split1" {
		t.Fatalf("sessionID = %q, want ses_split1 after the line completed", tap.session())
	}

	partial := `{"type":"text","sessionID":"ses_partial1","part":{"type":"text","text":"partial"}}`
	_, _ = model.Write([]byte(partial))
	if tap.text() != "hello" {
		t.Fatalf("lastText = %q before Flush, want hello", tap.text())
	}
	if err := tap.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if tap.text() != "partial" {
		t.Fatalf("lastText = %q after Flush, want partial from the trailing line", tap.text())
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
	tap := newSessionTap(&out, defaultExhaustionSignals())
	model := tap.beginAttempt()

	big := make([]byte, tapMaxLine+1)
	for i := range big {
		big[i] = 'x'
	}
	_, _ = model.Write(big)
	if tap.session() != "" || tap.text() != "" {
		t.Fatalf("state captured from an oversized line: sessionID=%q lastText=%q", tap.session(), tap.text())
	}

	_, _ = model.Write(append([]byte(`{"type":"text","sessionID":"ses_after_big","part":{"type":"text","text":"ok"}}`), '\n'))
	if tap.session() != "ses_after_big" {
		t.Fatalf("sessionID = %q, want ses_after_big (later complete lines must still parse)", tap.session())
	}
}

// TestSessionTapIgnoresNonJSONLines proves non-JSON lines capture no state,
// and a JSON line does.
func TestSessionTapIgnoresNonJSONLines(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out, defaultExhaustionSignals())
	model := tap.beginAttempt()
	_, _ = model.Write([]byte("plain text\n{broken json\n"))
	if tap.session() != "" || tap.text() != "" {
		t.Fatalf("non-JSON lines captured state: sessionID=%q lastText=%q", tap.session(), tap.text())
	}
	_, _ = model.Write(append([]byte(`{"type":"text","sessionID":"ses_json1","part":{"type":"text","text":"json line"}}`), '\n'))
	if tap.session() != "ses_json1" {
		t.Fatalf("sessionID = %q, want ses_json1", tap.session())
	}
}

// TestSessionTapFirstSessionIDWins proves the first captured sessionID is
// kept, while lastText keeps updating.
func TestSessionTapFirstSessionIDWins(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out, defaultExhaustionSignals())
	model := tap.beginAttempt()
	_, _ = model.Write(append([]byte(`{"type":"text","sessionID":"ses_first","part":{"type":"text","text":"one"}}`), '\n'))
	_, _ = model.Write(append([]byte(`{"type":"text","sessionID":"ses_second","part":{"type":"text","text":"two"}}`), '\n'))
	if tap.session() != "ses_first" {
		t.Fatalf("sessionID = %q, want ses_first (the first wins)", tap.session())
	}
	if tap.text() != "two" {
		t.Fatalf("lastText = %q, want two (text keeps updating)", tap.text())
	}
}

// TestSessionTapTruncatesMultibyteTextAtRuneLimit proves lastText is
// truncated at tapLastTextLimit runes, not bytes, for multibyte text.
func TestSessionTapTruncatesMultibyteTextAtRuneLimit(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out, defaultExhaustionSignals())
	model := tap.beginAttempt()
	text := strings.Repeat("é", tapLastTextLimit+50)
	_, _ = model.Write(append([]byte(`{"type":"text","sessionID":"ses_multibyte","part":{"type":"text","text":"`+text+`"}}`), '\n'))
	if got := len([]rune(tap.text())); got != tapLastTextLimit {
		t.Fatalf("lastText = %d runes, want %d (truncation must be rune-safe)", got, tapLastTextLimit)
	}
	if tap.text() != strings.Repeat("é", tapLastTextLimit) {
		t.Fatalf("lastText is not the first %d runes of the text", tapLastTextLimit)
	}
}

// TestSessionTapIgnoresNonTextParts proves a part that is not text does not
// update lastText.
func TestSessionTapIgnoresNonTextParts(t *testing.T) {
	var out bytes.Buffer
	tap := newSessionTap(&out, defaultExhaustionSignals())
	model := tap.beginAttempt()
	_, _ = model.Write(append([]byte(`{"type":"step","sessionID":"ses_step1","part":{"type":"step","text":"not text"}}`), '\n'))
	if tap.text() != "" {
		t.Fatalf("lastText = %q, want empty (non-text parts are ignored)", tap.text())
	}
	if tap.session() != "ses_step1" {
		t.Fatalf("sessionID = %q, want ses_step1", tap.session())
	}
}
