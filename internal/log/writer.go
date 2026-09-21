package log

import (
	"bytes"
	"io"
)

// maxBufferedLine bounds how much of an unfinished line the redacting writer
// may hold, so a session that emits one enormous line (or no newlines at
// all) cannot grow the buffer without limit. When the bound is reached, the
// oldest buffered bytes are redacted and released as a segment.
const maxBufferedLine = 1 << 20

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
// A secret split across a segment boundary inside an over-long line can
// escape matching; that is the deliberate trade against unbounded memory.
// Ordinary lines — anything separated by newlines within normal output —
// are always matched whole.
type RedactingWriter struct {
	w        io.Writer
	redactor *Redactor
	buf      []byte
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
		chunk := p[written:min(written+maxBufferedLine, len(p))]
		written += len(chunk)
		for {
			end := bytes.IndexByte(chunk, '\n')
			if end < 0 {
				w.buf = append(w.buf, chunk...)
				break
			}
			w.buf = append(w.buf, chunk[:end+1]...)
			chunk = chunk[end+1:]
			if err := w.flush(); err != nil {
				return written, err
			}
		}
		for len(w.buf) >= maxBufferedLine {
			// Bound memory on an over-long line: release the oldest
			// segment, redacted as-is, without inventing a newline, so the
			// stream stays byte-faithful apart from redaction.
			if err := w.flushSegment(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// Flush redacts and writes any buffered partial line. Callers must flush
// after the producer exits (after process.Wait/Run) so no trailing output is
// left unreleased.
func (w *RedactingWriter) Flush() error {
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

func (w *RedactingWriter) flushSegment() error {
	segment := w.buf[:maxBufferedLine]
	if _, err := io.WriteString(w.w, w.redactor.Redact(string(segment))); err != nil {
		return err
	}
	w.buf = append(w.buf[:0], w.buf[maxBufferedLine:]...)
	return nil
}
