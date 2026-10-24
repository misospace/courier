package main

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"

	"github.com/misospace/courier/internal/telemetry"
)

// tapMaxLine bounds the session tap's buffer for an unfinished line, so a
// child emitting one enormous line cannot grow it without limit.
const tapMaxLine = 1 << 20

// tapLastTextLimit bounds how much of the last assistant text part the tap
// keeps, in runes.
const tapLastTextLimit = 200

// sessionTap is a pass-through stdout transport that scans the child's
// newline-delimited JSON event lines while they pass. It captures the first
// sessionID and the last assistant text part so a recoverable exit can
// resume the same session; it never alters the bytes it forwards. lastText
// intentionally persists across resumed turns when a new turn emits no text.
// It also classifies structured provider-exhaustion evidence from OpenCode's
// session error events (#327): only runtime-authored structured error
// events can prove exhaustion, never model prose.
type sessionTap struct {
	// mu guards every mutable field below: after WaitDelay fires, os/exec
	// returns from Run while the previous attempt's stdout copy goroutine
	// can still be mid-Write, so scan must never race the next attempt's
	// reset, kill wiring, or the terminal classification reads.
	mu        sync.Mutex
	w         io.Writer
	pending   []byte
	sessionID string
	lastText  string
	tel       *telemetry.Tracker
	signals   exhaustionSignals
	// kill stops the currently running attempt's process group; only bytes
	// from that same generation may invoke it.
	kill func()
	// gen counts attempts. attemptWriter carries the generation of the pipe
	// it drains; bytes from an abandoned grandchild's pipe after its
	// attempt ended are forwarded unseen and cannot alter state, splice
	// framing, or stop the next child.
	gen int
	// sawSession reports whether the child served a session id during the
	// current attempt. A crash before any session is an availability
	// failure (#327 Scenario D), distinct from a crash mid-work.
	sawSession bool
	// seq counts every scanned line of the current scan stream, so "the
	// exhaustion error was the last thing the child emitted" is decidable
	// at stop time.
	seq        int
	exhaustion *tapExhaustion
}

// tapExhaustion is one classified exhaustion event: the bounded class and
// code tokens only — never provider response text.
type tapExhaustion struct {
	seq   int
	class string
	code  string
}

func newSessionTap(w io.Writer, signals exhaustionSignals) *sessionTap {
	return &sessionTap{w: w, signals: signals}
}

// attemptWriter drains one attempt's stdout pipe. Bytes whose generation no
// longer matches the tap (an abandoned grandchild still writing after its
// attempt was killed) are forwarded to the output stream but never scanned:
// they cannot set state, splice line framing, or invoke the kill callback.
type attemptWriter struct {
	tap *sessionTap
	gen int
}

func (a *attemptWriter) Write(p []byte) (int, error) {
	a.tap.mu.Lock()
	defer a.tap.mu.Unlock()
	n, err := a.tap.w.Write(p)
	if a.gen != a.tap.gen {
		return n, err
	}
	a.tap.consume(p)
	return n, err
}

// beginAttempt starts a new generation: per-attempt scan state resets and
// any partial line left in the buffer belongs to a dead attempt.
func (t *sessionTap) beginAttempt() *attemptWriter {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gen++
	t.pending = nil
	t.sawSession = false
	return &attemptWriter{tap: t, gen: t.gen}
}

// setKill wires (or clears, with nil) the current attempt's stop function
// under the tap lock, so the exhaustion callback can never observe a
// half-assigned closure or kill a process group from a stale attempt.
func (t *sessionTap) setKill(kill func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.kill = kill
}

func (t *sessionTap) consume(p []byte) {
	t.pending = append(t.pending, p...)
	for {
		idx := bytes.IndexByte(t.pending, '\n')
		if idx < 0 {
			break
		}
		t.scan(t.pending[:idx])
		t.pending = t.pending[idx+1:]
	}
	if len(t.pending) > tapMaxLine {
		// The oversized unfinished line is dropped unseen, but it is still
		// output the child produced after anything scanned before it: it
		// must invalidate a prior exhaustion the same way a parsed line
		// would (#327).
		t.pending = nil
		t.seq++
	}
}

// Flush scans a trailing partial line after the child exits and then ends
// the generation: a copy goroutine abandoned by WaitDelay may still deliver
// bytes afterwards, and those must no longer reach scan.
func (t *sessionTap) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.pending) > 0 {
		t.scan(t.pending)
		t.pending = nil
	}
	t.gen++
	return nil
}

// scan extracts the session id and assistant text from one JSON event line,
// and classifies structured provider-exhaustion evidence from OpenCode's
// session error events (#327). Lines that are not JSON events are ignored —
// they still count as output, so they invalidate any earlier exhaustion.
func (t *sessionTap) scan(line []byte) {
	t.seq++
	if t.tel != nil {
		t.tel.Observe(line)
	}
	var event struct {
		SessionID string `json:"sessionID"`
		Part      *struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"part"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return
	}
	if event.SessionID != "" {
		if t.sessionID == "" {
			t.sessionID = event.SessionID
		}
		t.sawSession = true
	}
	if event.Part != nil && event.Part.Type == "text" {
		t.lastText = truncatedRunes(event.Part.Text, tapLastTextLimit)
	}
	if exhausted, class, code := classifyProviderExhaustion(line, t.signals); exhausted {
		t.exhaustion = &tapExhaustion{seq: t.seq, class: class, code: code}
		if t.kill != nil {
			t.kill()
		}
	}
}

// confirmedExhaustion reports whether the classified event is the final line.
// The stop path retains the first classification even when already-buffered
// output drains after SIGKILL; those bytes cannot restart the stopped child.
func (t *sessionTap) confirmedExhaustion() (class, code string, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.exhaustion == nil || t.exhaustion.seq != t.seq {
		return "", "", false
	}
	return t.exhaustion.class, t.exhaustion.code, true
}

func (t *sessionTap) lastExhaustion() (class, code string, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.exhaustion == nil {
		return "", "", false
	}
	return t.exhaustion.class, t.exhaustion.code, true
}

// session returns the first session id served by any attempt, used to resume
// the same conversation after a crash or continuation.
func (t *sessionTap) session() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sessionID
}

// sawSessionThisAttempt reports whether the current attempt ever served a
// session id: no session at all means the provider never answered this turn,
// which routes to the availability budget (#327 Scenario D).
func (t *sessionTap) sawSessionThisAttempt() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sawSession
}

// text returns the last assistant text part observed, used for continuation
// fingerprints and run summaries.
func (t *sessionTap) text() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastText
}

func truncatedRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
