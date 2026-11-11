package harness

// BootstrapReceipt is the structured result of one bounded bootstrap
// execution in the secure worker (#414): what executed, how long each
// bounded command took, what it emitted, and the validated diagnostics
// (mise executable path, version, data dir, toolchain bake, offline
// mode). Values are composed by trusted control: elapsed and exit codes
// from protocol results, diagnostics parsed from worker stdout under
// devenv's fixed-key charset validation. No secret values; the stderr
// tails are the contract task's own bounded output, carried as
// observability-only diagnostic text (#797 — the tail is what names the
// exact download or pin that refused) and never entering the durable
// blocker reason or the model transcript.
type BootstrapReceipt struct {
	// Status is "applied" or "failed".
	Status string
	// Reason is the fixed-text failure reason; "" when applied.
	Reason string
	// BootstrapElapsedMs is the wall time the bootstrap task consumed,
	// measured around its dispatch.
	BootstrapElapsedMs int64
	// CaptureElapsedMs is the wall time the environment-capture task
	// consumed; 0 when the capture never ran.
	CaptureElapsedMs int64
	// BootstrapExitCode is the bootstrap task's exit code; -1 when the
	// task was never reached or failed to start.
	BootstrapExitCode int
	// CaptureExitCode is the environment-capture task's exit code; -1
	// when it was never reached.
	CaptureExitCode int
	// EnvKeys is the number of environment keys the validated capture
	// produced.
	EnvKeys int
	// Diagnostics carries the validated bootstrap-task diagnostics
	// (courier-mise-path, courier-mise-version, courier-mise-data-dir,
	// courier-mise-toolchain, courier-mise-offline).
	Diagnostics map[string]string
	// BootstrapStderrTail is the bounded stderr tail the bootstrap task
	// returned, when it failed; "" on success or when the task never ran.
	// Untrusted but bounded (protocol's 64 KiB tail bound) diagnostic
	// text: observability only, never a durable reason.
	BootstrapStderrTail string
	// CaptureStderrTail is the bounded stderr tail the environment-capture
	// task returned, when it failed; "" otherwise.
	CaptureStderrTail string
}

// Bootstrap receipt status values.
const (
	// BootstrapReceiptApplied reports the bootstrap applied cleanly.
	BootstrapReceiptApplied = "applied"
	// BootstrapReceiptFailed reports the bootstrap ended in a terminal
	// environment blocker.
	BootstrapReceiptFailed = "failed"
)

// BootstrapReceiptSink receives one structured receipt per bounded
// bootstrap execution (#414). It is observability only: emission is void,
// never changes a transcript, and never affects the run's outcome — the
// same contract the compaction sink carries.
type BootstrapReceiptSink interface {
	BootstrapRecorded(BootstrapReceipt)
}
