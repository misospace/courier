package main

import (
	"github.com/misospace/courier/internal/executor"
	"io"
	"os"
)

// emitTermination writes the legacy COURIER_TERMINATION line to stdout (and to
// cfg.TerminationFile when set). The whole line is bounded by the Kubernetes
// pod termination-message budget: when the marshalled payload would push the
// line past executor.MaxTerminationLineBytes, the compact telemetry summary
// is shrunk — subagent entries dropped lowest-runtime first, then the summary
// key itself — so phase, result, exit_code, outcome and reason always survive.
func emitTermination(stdout io.Writer, cfg config, result termination) {
	line := fitTerminationLine(result)
	if line == "" {
		// A marshal failure leaves no handoff line; write nothing rather than
		// an empty handoff that would truncate the termination file.
		return
	}
	_, _ = io.WriteString(stdout, line)
	if cfg.TerminationFile == "" {
		return
	}
	// The file is an optional local handoff for an operator or sidecar. It is
	// never used for credentials and is replaced atomically enough for a single
	// writer in the ephemeral workspace.
	_ = os.WriteFile(cfg.TerminationFile, []byte(line), 0o600)
}

// fitTerminationLine renders the handoff line, shrinking the telemetry summary
// until the line fits the termination-message budget, and — only once the
// summary is fully gone — delegating the reason-shrink step to the shared
// executor.FitTerminationLine so the two COURIER_TERMINATION producers
// (here and courier-control) cannot drift on the wire prefix, the budget,
// or the truncation marker (#210). The summary always gives ground first;
// the reason is cut last and only as much as needed to fit, and it is kept
// non-empty. phase/result/exit_code/outcome are never touched. It may shrink
// result.Summary in place: a caller must not reuse the passed Summary
// pointer expecting the untouched payload.
func fitTerminationLine(result termination) string {
	line := marshalTerminationLine(result)
	for len(line) > executor.MaxTerminationLineBytes && result.Summary != nil {
		if len(result.Summary.Subagents) > 0 {
			// Subagents are ordered highest-runtime first, so the final entry
			// is the lowest-runtime; drop it and re-measure.
			result.Summary.Subagents = result.Summary.Subagents[:len(result.Summary.Subagents)-1]
		} else {
			// The scalar summary fields alone are tiny; with an empty subagent
			// list the summary earns no place in the budget.
			result.Summary = nil
		}
		line = marshalTerminationLine(result)
	}
	// Last resort: the summary is gone yet the line still overruns the
	// budget. An escape-heavy reason marshals to far more bytes than its raw
	// length (each <, > or & becomes a six-byte \uXXXX escape), so the
	// raw-byte truncation in terminate cannot see the overrun. The shared
	// FitTerminationLine cuts the reason until the line fits; it is the only
	// field left allowed to give ground and it always stays non-empty.
	return executor.FitTerminationLine(&result)
}

// marshalTerminationLine renders the handoff line for a fixed termination.
// It is a thin alias for the shared executor.MarshalTerminationLine, kept
// so the executor's own summary-shrink loop and any future local helper
// stay self-explanatory.
func marshalTerminationLine(result termination) string {
	return executor.MarshalTerminationLine(result)
}
