package log

import (
	"bytes"
	"io"
)

// maxBufferedLine bounds how much of an unfinished line the redacting writer
// may hold, so a session that emits one enormous line (or no newlines at
// all) cannot grow the buffer without limit.
const maxBufferedLine = 1 << 20

// overlongLineMarker replaces any line that reaches maxBufferedLine before
// its newline arrives. The line is suppressed whole — fail closed — because
// redacting arbitrary segments of it could split a registered secret across
// a segment boundary and let both halves through. The marker is static text
// and never carries original content, so no credential can hide in it.
const overlongLineMarker = "[courier: overlong line suppressed]"

// RedactingWriter is a transparent redacting transport for output that is
// about to reach stdout/stderr, such as a child process's streams. Bytes pass
// through in order with line-buffered redaction: a line is accumulated until
// its newline so a secret split across ordinary Write chunk boundaries is
// still matched, then the redacted line is written whole.
//
// Each instance must be driven by one goroutine (os/exec already copies each
// of Stdout and Stderr from its own goroutine, so one writer per stream is
// safe). The final partial line of a session — a child that exited without a
// trailing newline — must be released with Flush after the child exits.
//
// A line that reaches maxBufferedLine is suppressed entirely: the marker is
// emitted in its place, the rest of that line (its newline included) is
// dropped, and ordinary buffering resumes with the next line. Suppressing
// beats segmenting, because no independent segment of an over-long line can
// be proven credential-free. Courier's invariant — secrets are redacted
// before stdout — has no size-based exception.
type RedactingWriter struct {
	w          io.Writer
	redactor   *Redactor
	buf        []byte
	discarding bool
}

// NewRedactingWriter wraps w so everything written through it is redacted by
// redactor before reaching the underlying stream.
func NewRedactingWriter(w io.Writer, redactor *Redactor) *RedactingWriter {
	return &RedactingWriter{w: w, redactor: redactor}
}

// Write buffers p up to line boundaries, emitting each completed line
// redacted. It always consumes all of p, matching io.Writer semantics for a
// buffering transport.
func (w *RedactingWriter) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		if w.discarding {
			// Inside a suppressed line: drop bytes up to its newline.
			end := bytes.IndexByte(p[written:], '\n')
			if end < 0 {
				written = len(p)
				break
			}
			written += end + 1
			w.discarding = false
			continue
		}
		rest := p[written:]
		end := bytes.IndexByte(rest, '\n')
		var piece []byte
		lineComplete := false
		if end < 0 {
			piece = rest
			written = len(p)
		} else {
			piece = rest[:end+1]
			lineComplete = true
			written += end + 1
		}
		// Feed bounded slices so a single huge Write cannot balloon the
		// buffer beyond maxBufferedLine plus one slice.
		for len(piece) > 0 {
			n := min(maxBufferedLine, len(piece))
			w.buf = append(w.buf, piece[:n]...)
			piece = piece[n:]
			if len(w.buf) >= maxBufferedLine {
				if err := w.suppress(); err != nil {
					return written, err
				}
				// The rest of this line is dropped. When the line's
				// newline is still ahead in a future Write, discarding
				// stays on to skip it; when this Write already contained
				// the newline, ordinary buffering resumes immediately.
				w.discarding = !lineComplete
				break
			}
		}
		if lineComplete && len(w.buf) > 0 {
			if err := w.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// Flush redacts and writes any buffered partial line. Callers must flush
// after the producer exits (after process.Wait/Run) so no trailing output is
// left unreleased. An over-long final partial line needs no release: its
// marker was emitted when the bound was reached and its remainder dropped.
func (w *RedactingWriter) Flush() error {
	if w.discarding {
		w.discarding = false
		return nil
	}
	if len(w.buf) == 0 {
		return nil
	}
	return w.flush()
}

func (w *RedactingWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	_, err := io.WriteString(w.w, w.redactor.Redact(string(w.buf)))
	w.buf = w.buf[:0]
	return err
}

// suppress replaces the buffered (over-long) line with the static marker.
func (w *RedactingWriter) suppress() error {
	w.buf = w.buf[:0]
	_, err := io.WriteString(w.w, overlongLineMarker+"\n")
	return err
}
