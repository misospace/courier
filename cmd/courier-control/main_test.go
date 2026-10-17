package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/topology"
)

func TestControlFailsClosedWithoutIdentity(t *testing.T) {
	if err := runArgs(nil); err == nil || !strings.Contains(err.Error(), "identity environment") {
		t.Fatalf("run() error = %v, want missing identity", err)
	}
}

func TestControlFailsClosedWithoutSigningKey(t *testing.T) {
	t.Setenv(topology.EnvRunUID, "run")
	t.Setenv(topology.EnvControlPodUID, "control")
	t.Setenv(topology.EnvWorkerPodUID, "worker")
	t.Setenv(topology.EnvWorkerURL, "http://worker:8080")
	t.Setenv(topology.EnvBrokerURL, "https://broker:8443")
	t.Setenv(topology.EnvSigningKeyFile, "definitely-missing.key")
	if err := runArgs(nil); err == nil {
		t.Fatal("a missing signing key must fail startup")
	}
}

// The native handoff payload must satisfy the operator's
// validTerminationPhaseResult vocabulary exactly, or the trusted reason and
// outcome are discarded. These are the exact combos courier-control emits.
func TestTerminationForMatchesOperatorSchema(t *testing.T) {
	tests := []struct {
		name   string
		result executor.HarnessResult
		want   termination
	}{
		{"changes", executor.HarnessResult{Outcome: executor.OutcomeChanges, Reason: "published through the broker"},
			termination{Phase: "Verifying", Result: "success", ExitCode: 0, Outcome: "changes"}},
		{"no change needed", executor.HarnessResult{Outcome: executor.OutcomeNoChangeNeeded, Reason: "evidence"},
			termination{Phase: "NoChangeNeeded", Result: "success", ExitCode: 3, Outcome: "no_change_needed"}},
		{"needs decision", executor.HarnessResult{Outcome: executor.OutcomeNeedsDecision, Reason: "a question"},
			termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: 2, Outcome: "needs_decision"}},
		{"blocked external", executor.HarnessResult{Outcome: executor.OutcomeBlockedExternal, Reason: "missing x"},
			termination{Phase: "NeedsHuman", Result: "needs-human", ExitCode: 2, Outcome: "blocked_external"}},
		{"undeclared ending", executor.HarnessResult{Reason: "undeclared ending"},
			termination{Phase: "Failed", Result: "failure", ExitCode: 1, Outcome: ""}},
		{"publication failure after changes", executor.HarnessResult{Outcome: executor.OutcomeChanges, Err: errors.New("push failed")},
			termination{Phase: "Failed", Result: "failure", ExitCode: 1, Outcome: "changes"}},
	}
	for _, tt := range tests {
		got := terminationFor(tt.result)
		if got.Phase != tt.want.Phase || got.Result != tt.want.Result || got.ExitCode != tt.want.ExitCode || got.Outcome != tt.want.Outcome {
			t.Fatalf("%s: termination = %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

func TestBoundReason(t *testing.T) {
	if got := boundReason("short"); got != "short" {
		t.Fatalf("boundReason(short) = %q", got)
	}
	long := strings.Repeat("x", maxTerminationReasonBytes+100)
	got := boundReason(long)
	if len(got) > maxTerminationReasonBytes {
		t.Fatalf("boundReason did not bound the reason: %d bytes", len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("boundReason produced invalid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, executor.TerminationTruncationSuffix) {
		t.Fatalf("boundReason = %q, want the shared truncation suffix", got)
	}
}

// TestHandoffStaysWithinTerminationBudget pins #210's done-condition:
// an 8 KiB escape-heavy reason (the worst case the capability-table
// detail strings can produce) must yield an emitted handoff line at or
// under the 4 KiB Kubernetes pod termination-message budget, and the
// surviving line must parse back as JSON with phase / result / exit_code
// intact and a non-empty reason. Without the line fit the same payload
// marshals to well past 4 KiB and the controller's parseCoordinatorTermination
// rejects the entire handoff.
func TestHandoffStaysWithinTerminationBudget(t *testing.T) {
	// Eight-byte unit covering all six escaping characters: the two
	// quotes, two backslashes and newline double in length, while <, >
	// and & expand to six-byte \uXXXX escapes. 1024 units make exactly
	// 8192 raw bytes (8 KiB); the marshaled payload overshoots the 4
	// KiB budget by far, so the line fit has to fire.
	unit := `"` + `"` + `\` + `\` + "\n" + "<>&"
	if len(unit) != 8 {
		t.Fatalf("test unit is %d bytes, want 8", len(unit))
	}
	reason := strings.Repeat(unit, executor.MaxTerminationLineBytes/len(unit))
	if len(reason) != executor.MaxTerminationLineBytes {
		t.Fatalf("reason is %d bytes, want %d", len(reason), executor.MaxTerminationLineBytes)
	}

	payload := termination{
		Phase:    "NeedsHuman",
		Result:   "needs-human",
		ExitCode: 2,
		Outcome:  "needs_decision",
		Reason:   reason,
	}
	line := executor.FitTerminationLine(&payload)
	if len(line) > executor.MaxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), executor.MaxTerminationLineBytes)
	}
	if !bytes.HasPrefix([]byte(line), []byte(executor.TerminationLinePrefix)) {
		t.Fatalf("termination line is missing the wire prefix %q: %q", executor.TerminationLinePrefix, line)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(line, executor.TerminationLinePrefix), "\n")
	var parsed struct {
		Phase    string `json:"phase"`
		Result   string `json:"result"`
		ExitCode int32  `json:"exit_code"`
		Reason   string `json:"reason"`
		Outcome  string `json:"outcome,omitempty"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Phase != "NeedsHuman" {
		t.Fatalf("phase = %q, want %q", parsed.Phase, "NeedsHuman")
	}
	if parsed.Result != "needs-human" {
		t.Fatalf("result = %q, want %q", parsed.Result, "needs-human")
	}
	if parsed.ExitCode != 2 {
		t.Fatalf("exit_code = %d, want 2", parsed.ExitCode)
	}
	if parsed.Outcome != "needs_decision" {
		t.Fatalf("outcome = %q, want %q", parsed.Outcome, "needs_decision")
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the line fit")
	}
	if !utf8.ValidString(parsed.Reason) {
		t.Fatalf("trimmed reason is not valid UTF-8: %q", parsed.Reason)
	}
	if !strings.HasSuffix(parsed.Reason, executor.TerminationTruncationSuffix) {
		t.Fatalf("trimmed reason %q does not end with the shared truncation suffix", parsed.Reason)
	}
}

// TestHandoffFitsEscapeHeavyBareReason covers the case where
// declareNeedsHuman hands boundReason a raw reason that survives the
// raw-byte cap but still escapes past 4 KiB — the exact failure mode
// filed as #210. boundReason alone caps raw bytes; the line fit the
// handoff applies afterwards must close the gap that escape inflation
// opens.
func TestHandoffFitsEscapeHeavyBareReason(t *testing.T) {
	// 4096 raw bytes of `&` expand to 24,576 escaped bytes — well past
	// 4 KiB even after boundReason's 1200-byte cap. The line fit has
	// to cut the reason regardless.
	reason := strings.Repeat("&", 4096)
	if len(reason) != 4096 {
		t.Fatalf("reason is %d bytes, want 4096", len(reason))
	}

	payload := termination{
		Phase:    "Failed",
		Result:   "failure",
		ExitCode: 1,
		Reason:   reason,
	}
	line := executor.FitTerminationLine(&payload)
	if len(line) > executor.MaxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), executor.MaxTerminationLineBytes)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(line, executor.TerminationLinePrefix), "\n")
	var parsed struct {
		Phase    string `json:"phase"`
		Result   string `json:"result"`
		ExitCode int32  `json:"exit_code"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Phase != "Failed" {
		t.Fatalf("phase = %q, want %q", parsed.Phase, "Failed")
	}
	if parsed.Result != "failure" {
		t.Fatalf("result = %q, want %q", parsed.Result, "failure")
	}
	if parsed.ExitCode != 1 {
		t.Fatalf("exit_code = %d, want 1", parsed.ExitCode)
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the line fit")
	}
}

// TestHandoffKeepsUTF8ReasonOnShrink pins the rune-boundary guarantee
// when the line fit cuts a multi-byte reason. A long CJK reason (no
// JSON escaping, so the marshaler leaves the bytes alone) overruns the
// 4 KiB budget in raw form; the cut must land on a rune boundary,
// leaving a valid-UTF-8, non-empty reason ending with the shared
// truncation marker.
func TestHandoffKeepsUTF8ReasonOnShrink(t *testing.T) {
	reason := strings.Repeat("你", 1500)
	if len(reason) != 4500 {
		t.Fatalf("reason is %d bytes, want 4500", len(reason))
	}

	payload := termination{
		Phase:    "NeedsHuman",
		Result:   "needs-human",
		ExitCode: 2,
		Reason:   reason,
	}
	line := executor.FitTerminationLine(&payload)
	if len(line) > executor.MaxTerminationLineBytes {
		t.Fatalf("termination line is %d bytes, want <= %d", len(line), executor.MaxTerminationLineBytes)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(line, executor.TerminationLinePrefix), "\n")
	var parsed struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("termination line is not valid JSON: %v: %q", err, body)
	}
	if parsed.Reason == "" {
		t.Fatal("reason was emptied by the line fit")
	}
	if !utf8.ValidString(parsed.Reason) {
		t.Fatalf("trimmed reason is not valid UTF-8: %q", parsed.Reason)
	}
	if !strings.HasSuffix(parsed.Reason, executor.TerminationTruncationSuffix) {
		t.Fatalf("trimmed reason %q does not end with the shared truncation suffix", parsed.Reason)
	}
}

// The truncation boundary must not split a multi-byte rune: the trusted
// termination reason stays valid UTF-8 within the byte bound.
func TestBoundReasonSplitsOnRuneBoundary(t *testing.T) {
	long := strings.Repeat("a", maxTerminationReasonBytes-len(executor.TerminationTruncationSuffix)-1) +
		"é" + strings.Repeat("b", 20)
	got := boundReason(long)
	if len(got) > maxTerminationReasonBytes {
		t.Fatalf("truncated reason is %d bytes, want <= %d", len(got), maxTerminationReasonBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncated reason is not valid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, executor.TerminationTruncationSuffix) {
		t.Fatalf("truncated reason lost its suffix: %q", got)
	}
}

// A gate failure that is not a *GateError must not panic the probe loop and
// must still fail closed: no re-probe rounds, declare NeedsHuman.
func TestClassifyGateFailureNonGateErrorFailsClosed(t *testing.T) {
	reason, retryable := classifyGateFailure(errors.New("gate exploded"))
	if retryable {
		t.Fatal("a non-GateError gate failure must not be retryable")
	}
	if reason == "" || !strings.Contains(reason, "gate exploded") {
		t.Fatalf("reason = %q, want a safe reason naming the failure", reason)
	}
}

func TestClassifyGateFailureGateError(t *testing.T) {
	transient := &harness.GateError{Failed: []harness.Capability{
		{Name: "broker", State: harness.StateUnavailable, Detail: "is unreachable", Required: true, Retryable: true},
	}}
	reason, retryable := classifyGateFailure(transient)
	if !retryable {
		t.Fatal("an all-transient GateError must stay retryable")
	}
	if !strings.Contains(reason, "broker") {
		t.Fatalf("reason = %q, want the capability table", reason)
	}

	permanent := &harness.GateError{Failed: []harness.Capability{
		{Name: "broker", State: harness.StateUnavailable, Detail: "is unreachable", Required: true, Retryable: true},
		{Name: "forge-operations", State: harness.StateUnavailable, Detail: "operation missing", Required: true},
	}}
	if _, retryable := classifyGateFailure(permanent); retryable {
		t.Fatal("a GateError with a permanent failure must not be retryable")
	}

	wrapped := fmt.Errorf("gate: %w", transient)
	if _, retryable := classifyGateFailure(wrapped); !retryable {
		t.Fatal("a wrapped GateError must still be classified as retryable")
	}
}

// A confirmed provider allowance exhaustion is a recoverable availability
// condition: the gate must declare blocked-external, not NeedsHuman (#320).
func TestExhaustedGateIsRecoverable(t *testing.T) {
	exhausted := &harness.GateError{Failed: []harness.Capability{
		{Name: "model-binding:coordinator", State: harness.StateUnavailable, Detail: "allowance", Required: true, Availability: harness.ProviderAllowanceExhausted},
	}}
	if !exhaustedGate(exhausted) {
		t.Fatal("an exhausted model-binding gate must be recoverable")
	}
	if !exhaustedGate(fmt.Errorf("gate: %w", exhausted)) {
		t.Fatal("a wrapped exhausted gate must be recoverable")
	}
	other := &harness.GateError{Failed: []harness.Capability{
		{Name: "forge-operations", State: harness.StateUnavailable, Detail: "missing", Required: true},
	}}
	if exhaustedGate(other) {
		t.Fatal("a non-exhausted gate must not be treated as recoverable exhaustion")
	}
	// An exhaustion must not mask a permanent non-exhaustion failure: that
	// gate is a configuration problem for a human, not a recoverable wait.
	mixed := &harness.GateError{Failed: []harness.Capability{
		{Name: "model-binding:coordinator", State: harness.StateUnavailable, Detail: "allowance", Required: true, Availability: harness.ProviderAllowanceExhausted},
		{Name: "model-binding:coder", State: harness.StateUnavailable, Detail: "auth rejected", Required: true, Availability: harness.ProviderAuthenticationFailed},
	}}
	if exhaustedGate(mixed) {
		t.Fatal("an exhaustion alongside a permanent auth failure must not be recoverable")
	}
	// A transient failure alongside exhaustion stays recoverable.
	transientMixed := &harness.GateError{Failed: []harness.Capability{
		{Name: "model-binding:coordinator", State: harness.StateUnavailable, Detail: "allowance", Required: true, Availability: harness.ProviderAllowanceExhausted},
		{Name: "git-remote", State: harness.StateUnavailable, Detail: "starting", Required: true, Retryable: true},
	}}
	if !exhaustedGate(transientMixed) {
		t.Fatal("exhaustion with only transient co-failures must stay recoverable")
	}
	if exhaustedGate(errors.New("not a gate error")) {
		t.Fatal("a non-GateError must not be treated as exhaustion")
	}
}

// The probe must mint a fresh operation ID per attempt: the worker keeps
// replay state in process memory, so a fixed probe opID would conflict (409)
// on every re-probe round and transient recovery would be impossible.
func TestProbeWorkerSurvivesRepeatedProbes(t *testing.T) {
	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	uid := fmt.Sprintf("worker-%d", time.Now().UnixNano())
	realWorker, err := protocol.NewWorker(protocol.WorkerConfig{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: uid, PublicKey: pub,
		WorkspaceDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(realWorker.Handler())
	defer server.Close()
	identity := &controlIdentity{
		runUID:            "run",
		controlUID:        "control",
		workerUID:         uid,
		workerURL:         server.URL,
		brokerURL:         "https://broker:8443",
		signingKey:        priv,
		brokerTokenSource: harness.NewStaticBrokerTokenSource([]byte("token")),
		brokerCA:          []byte("ca"),
	}
	for attempt := 0; attempt < 3; attempt++ {
		capability := identity.probeWorker(context.Background())
		if capability.State != harness.StateHealthy {
			t.Fatalf("probe attempt %d = %+v, want healthy (a fixed opID conflicts on the second probe)", attempt, capability)
		}
	}
}

// TestCompactionSinkEmitsSessionCompacted pins #297's telemetry contract: the
// harness's applied-compaction event reaches the run's structured log as one
// session.compacted line carrying the role, the bound model, and the
// before/after/net/digested counts.
//
// It also pins two things the contract is specific about. The counts are
// verbose-only in the emitter, so the event marks itself Verbose or they
// never reach the store at info level. And the event sets no Status:
// compaction is observability, and a line carrying a status would let it be
// read as a liveness or completion claim about the run.
func TestCompactionSinkEmitsSessionCompacted(t *testing.T) {
	var stdout, stderr bytes.Buffer
	sink := compactionSink{
		emitter: courierlog.NewEmitter(&stdout, courierlog.LevelInfo),
		stderr:  &stderr,
		invocation: executor.Invocation{
			RunName: "run-1",
			Repo:    "owner/name",
			Ref:     7,
			Mode:    courierv1alpha1.ModeResolveIssue,
		},
	}
	sink.SessionCompacted(harness.CompactEvent{
		Role:           "coder",
		Model:          "test/coder",
		Before:         1200,
		After:          480,
		NetSaved:       720,
		DigestedTokens: 900,
	})
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want a clean emission", stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("emitted %d lines, want exactly one: %q", len(lines), stdout.String())
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("emitted line is not JSON: %v: %q", err, lines[0])
	}
	if event["event"] != courierlog.EventSessionCompacted {
		t.Fatalf("event = %v, want %q", event["event"], courierlog.EventSessionCompacted)
	}
	if event["run_id"] != "run-1" || event["repo"] != "owner/name" {
		t.Fatalf("run identity = %v/%v, want the invocation's", event["run_id"], event["repo"])
	}
	if event["role"] != "coder" || event["model"] != "test/coder" {
		t.Fatalf("role/model = %v/%v, want coder/test-coder", event["role"], event["model"])
	}
	if _, ok := event["status"]; ok {
		t.Fatalf("session.compacted carries status %v; compaction is observability, never status", event["status"])
	}
	detail, ok := event["detail"].(map[string]any)
	if !ok {
		t.Fatalf("detail = %v, want the verbose counts at info level", event["detail"])
	}
	for key, want := range map[string]float64{"before": 1200, "after": 480, "reclaimed": 720, "digested": 900} {
		if detail[key] != want {
			t.Fatalf("detail[%q] = %v, want %v", key, detail[key], want)
		}
	}
}
