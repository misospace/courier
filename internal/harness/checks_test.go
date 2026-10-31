package harness

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misospace/courier/internal/devenv"
	"github.com/misospace/courier/internal/protocol"
)

func checkCall(command string) ToolCall {
	return ToolCall{ID: "call-check", Name: toolCheck, Arguments: fmt.Sprintf(`{"command":%q}`, command)}
}

func newCheckCoordinator(t *testing.T, delegator *Delegator, snapshot SnapshotProvider) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Gateway:  &Gateway{BaseURL: "http://unused.invalid"},
		Bindings: testBindings(),
		Worker:   delegator,
		Snapshot: snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

// taskDispatchCount counts recorded dispatches whose task matches keep. The
// unpack (and, for approved contracts, bootstrap and capture) tasks ride
// every prepareSnapshot, so check tests count only the tasks they care
// about.
func taskDispatchCount(m *mockWorker, keep func(protocol.Task) bool) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, record := range m.dispatches {
		if keep(record.task) {
			n++
		}
	}
	return n
}

// checkDispatchCount counts recorded dispatches of check-kind operations
// only: shell and pack tasks are also non-unpack dispatches.
func checkDispatchCount(m *mockWorker) int {
	return dispatchKindCount(m, "check")
}

func dispatchKindCount(m *mockWorker, kind string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, record := range m.dispatches {
		if opKind, _, _ := strings.Cut(record.envelope.OpID, "."); opKind == kind {
			n++
		}
	}
	return n
}

// awaitDispatchKind polls until the worker has recorded want dispatches of
// the named op kind, bounded by a deadline.
func awaitDispatchKind(t *testing.T, m *mockWorker, kind string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if dispatchKindCount(m, kind) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("worker %s dispatches never reached %d", kind, want)
}

func notUnpackTask(task protocol.Task) bool {
	return !isWorkerUnpackTask(task)
}

func checkTaskDispatches(m *mockWorker) []dispatchRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []dispatchRecord
	for _, record := range m.dispatches {
		if notUnpackTask(record.task) && !isWorkerBootstrapTask(record.task) && !isWorkerEnvCaptureTask(record.task) {
			out = append(out, record)
		}
	}
	return out
}

// receiptOp extracts the op ID from a rendered check receipt header.
func receiptOp(content string) string {
	for _, line := range strings.Split(content, "\n") {
		if field := strings.TrimPrefix(line, "check receipt: "); field != line {
			for _, token := range strings.Fields(field) {
				if op, ok := strings.CutPrefix(token, "op="); ok {
					return op
				}
			}
		}
	}
	return ""
}

func TestCheckReusesReceiptOnUnchangedState(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "ok suite\n", StderrTail: "one warning\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want exactly one for the duplicate request", got)
	}
	if first.IsError {
		t.Fatalf("first check = %+v, want a passing result", first)
	}
	if !strings.Contains(first.Content, "check receipt: fresh") {
		t.Fatalf("first check content = %q, want a fresh receipt", first.Content)
	}
	if !strings.Contains(second.Content, "check receipt: reused") {
		t.Fatalf("second check content = %q, want the reused receipt", second.Content)
	}
	for _, part := range []string{"exit=0", "stdout:\nok suite\n", "stderr:\none warning\n"} {
		if !strings.Contains(second.Content, part) {
			t.Fatalf("second check content = %q, want the saved receipt to carry %q", second.Content, part)
		}
	}
	if second.IsError {
		t.Fatalf("second check = %+v, want a passing reused receipt", second)
	}
	if receiptOp(first.Content) == "" || receiptOp(first.Content) != receiptOp(second.Content) {
		t.Fatalf("receipt ops differ: %q vs %q", receiptOp(first.Content), receiptOp(second.Content))
	}
}

func TestCheckRerunsWhenSnapshotTipChanges(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTips = []string{"tip-a", "tip-b"}
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "first\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "second\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, sequenceSnapshotProvider("tip-a", "tip-b"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != 2 {
		t.Fatalf("check dispatches = %d, want two after the snapshot tip changed", got)
	}
	if !strings.Contains(first.Content, "check receipt: fresh") || !strings.Contains(second.Content, "check receipt: fresh") {
		t.Fatalf("check contents = %q / %q, want both fresh after the tip changed", first.Content, second.Content)
	}
	if receiptOp(first.Content) == receiptOp(second.Content) {
		t.Fatalf("receipt ops are identical across a tip change: %q", receiptOp(first.Content))
	}
}

func TestCheckRerunsWhenBootstrapEnvChanges(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.bootstrapResults = []protocol.Result{
		{ExitCode: 0, StdoutTail: "bootstrapped\n"},
		{ExitCode: 0, StdoutTail: bootstrapCapture("CHECK_ENV=1")},
		{ExitCode: 0, StdoutTail: "bootstrapped\n"},
		{ExitCode: 0, StdoutTail: bootstrapCapture("CHECK_ENV=2")},
	}
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "first env\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "second env\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, contractSnapshotProvider("snap-tip", devenv.StateApproved))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	dispatches := checkTaskDispatches(worker)
	if len(dispatches) != 2 {
		t.Fatalf("check dispatches = %d, want two after the bootstrap environment changed", len(dispatches))
	}
	if got := strings.Join(dispatches[0].task.Env, ","); got != "CHECK_ENV=1" {
		t.Fatalf("first check task env = %q, want the applied bootstrap delta", got)
	}
	if got := strings.Join(dispatches[1].task.Env, ","); got != "CHECK_ENV=2" {
		t.Fatalf("second check task env = %q, want the new bootstrap delta", got)
	}
	if !strings.Contains(first.Content, "check receipt: fresh") || !strings.Contains(second.Content, "check receipt: fresh") {
		t.Fatalf("check contents = %q / %q, want both fresh after the environment changed", first.Content, second.Content)
	}
}

func TestCheckConcurrentRequestsShareOneDispatch(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "shared receipt\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	const callers = 2
	results := make([]ToolResult, callers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = coordinator.runCheck(context.Background(), checkCall("go vet ./..."), "", ToolResult{ToolCallID: fmt.Sprintf("c%d", i), Name: toolCheck})
		}(i)
	}
	wg.Wait()

	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want exactly one across concurrent same-key requests", got)
	}
	fresh, reused := 0, 0
	for _, result := range results {
		if result.IsError {
			t.Fatalf("concurrent check = %+v, want a passing receipt", result)
		}
		switch {
		case strings.Contains(result.Content, "check receipt: fresh"):
			fresh++
		case strings.Contains(result.Content, "check receipt: reused"):
			reused++
		default:
			t.Fatalf("check content = %q, want a check receipt", result.Content)
		}
		if !strings.Contains(result.Content, "shared receipt\n") {
			t.Fatalf("check content = %q, want the shared receipt's stdout", result.Content)
		}
	}
	if fresh != 1 || reused != 1 {
		t.Fatalf("fresh=%d reused=%d, want exactly one of each", fresh, reused)
	}
	op := receiptOp(results[0].Content)
	if op == "" || op != receiptOp(results[1].Content) {
		t.Fatalf("concurrent callers saw different receipts: %q vs %q", receiptOp(results[0].Content), receiptOp(results[1].Content))
	}
}

// TestCheckPreservesFailedExitOnReuse pins the production shape of a
// failing check: the worker reports a nonzero exit as ResultFailed
// (protocol runTask), and such a completed run is real evidence — reused
// without rerunning, never presented as passing.
func TestCheckPreservesFailedExitOnReuse(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultFailed, ExitCode: 1, StdoutTail: "--- FAIL\n", StderrTail: "boom\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want the failing receipt to be reused without rerunning", got)
	}
	for i, result := range []ToolResult{first, second} {
		if !result.IsError {
			t.Fatalf("check %d = %+v, want IsError for a failing exit", i, result)
		}
		if !strings.Contains(result.Content, "exit=1") || !strings.Contains(result.Content, "--- FAIL\n") {
			t.Fatalf("check %d content = %q, want exit=1 with the failing output", i, result.Content)
		}
		if !strings.Contains(result.Content, "status: failed") {
			t.Fatalf("check %d content = %q, want the failed status reported, never a pass", i, result.Content)
		}
	}
	if !strings.Contains(second.Content, "check receipt: reused") {
		t.Fatalf("second check content = %q, want the reused failing receipt", second.Content)
	}
}

// TestCheckUnstartedFailedStatusIsNotReusable pins the other side of the
// ResultFailed classification: exit code -1 is the worker's sentinel for a
// task that never started or died to a signal — the run produced no exit
// status of its own, so it is not reusable evidence and the repeat reruns.
func TestCheckUnstartedFailedStatusIsNotReusable(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultFailed, ExitCode: -1, StderrTail: "task rejected before execution\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "rerun ok\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != 2 {
		t.Fatalf("check dispatches = %d, want the sentinel-failed receipt to be rerun on repeat", got)
	}
	if !first.IsError {
		t.Fatalf("first check = %+v, want the unstarted run reported as an error", first)
	}
	if second.IsError || !strings.Contains(second.Content, "check receipt: fresh") || !strings.Contains(second.Content, "rerun ok\n") {
		t.Fatalf("second check = %+v, want a fresh passing receipt after the rerun", second)
	}
}

// TestCheckCompletedRunClassification pins the evidence boundary directly:
// a status/exit pair counts as a completed run exactly when the invocation
// produced its own exit status.
func TestCheckCompletedRunClassification(t *testing.T) {
	for _, tc := range []struct {
		status   string
		exitCode int
		want     bool
	}{
		{status: protocol.ResultCompleted, exitCode: 0, want: true},
		{status: protocol.ResultCompleted, exitCode: 1, want: true},
		{status: protocol.ResultFailed, exitCode: 1, want: true},
		{status: protocol.ResultFailed, exitCode: 2, want: true},
		{status: protocol.ResultFailed, exitCode: 127, want: true},
		{status: protocol.ResultFailed, exitCode: 0, want: false},
		{status: protocol.ResultFailed, exitCode: -1, want: false},
		{status: protocol.ResultCancelled, exitCode: -1, want: false},
		{status: "running", exitCode: 0, want: false},
	} {
		if got := completedRun(tc.status, tc.exitCode); got != tc.want {
			t.Errorf("completedRun(%q, %d) = %v, want %v", tc.status, tc.exitCode, got, tc.want)
		}
	}
}

func TestCheckCancelledReceiptIsStoredButNotReusable(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCancelled, ExitCode: -1},
		{Status: protocol.ResultCancelled, ExitCode: -1},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != 2 {
		t.Fatalf("check dispatches = %d, want a cancelled receipt to be rerun on repeat", got)
	}
	if !first.IsError || !strings.Contains(first.Content, "status: cancelled") {
		t.Fatalf("first check content = %q, want the cancellation reported as an error", first.Content)
	}
	if !strings.Contains(second.Content, "check receipt: fresh") {
		t.Fatalf("second check content = %q, want a fresh dispatch, not a reused receipt", second.Content)
	}
}

func TestCheckFailedLaunchLeavesNoReceipt(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	// Unpack succeeds; the check dispatch is a definite 4xx rejection —
	// twice, so the retry exercises the same refusal.
	worker.dispatchPlan = []int{0, http.StatusBadRequest, 0, http.StatusBadRequest}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	if worker.dispatchRequests != 4 {
		t.Fatalf("worker dispatch requests = %d, want two unpacks and two rejected check dispatches", worker.dispatchRequests)
	}
	for i, result := range []ToolResult{first, second} {
		if !result.IsError || result.Content == "" {
			t.Fatalf("check %d = %+v, want a tool error", i, result)
		}
		if strings.Contains(result.Content, "check receipt:") {
			t.Fatalf("check %d content = %q, want no receipt for a failed launch", i, result.Content)
		}
	}
}

// TestCheckDistinctSpellingsDoNotShareReceipt pins the exact-bytes identity
// end to end: commands sh tokenizes alike but executes differently — and
// commands differing only in formatting whitespace — each keep their own
// receipt, because only identical bytes guarantee an identical program.
func TestCheckDistinctSpellingsDoNotShareReceipt(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "double-quoted\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "single-quoted\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "bare\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "spaced\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	commands := []string{`echo "$HOME"`, `echo '$HOME'`, `echo $HOME`, "go test   \t ./..."}
	for i, command := range commands {
		result := coordinator.runCheck(context.Background(), checkCall(command), "", ToolResult{ToolCallID: fmt.Sprintf("c%d", i), Name: toolCheck})
		if result.IsError || !strings.Contains(result.Content, "check receipt: fresh") {
			t.Fatalf("check %d (%q) = %+v, want a fresh dispatch for each distinct spelling", i, command, result)
		}
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != len(commands) {
		t.Fatalf("check dispatches = %d, want one per distinct spelling", got)
	}
	// Only the exact same bytes reuse.
	repeat := coordinator.runCheck(context.Background(), checkCall(`echo "$HOME"`), "", ToolResult{ToolCallID: "c-repeat", Name: toolCheck})
	if !strings.Contains(repeat.Content, "check receipt: reused") || !strings.Contains(repeat.Content, "double-quoted\n") {
		t.Fatalf("repeat check content = %q, want the exact-bytes repeat served from its own receipt", repeat.Content)
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != len(commands) {
		t.Fatalf("check dispatches = %d, want the exact-bytes repeat to launch nothing", got)
	}
}

// TestCheckIdentityIsExactCommandBytes pins the key-level contract: two
// commands share a state key exactly when their bytes are equal, and the
// state halves (tip, environment digest) are part of the key.
func TestCheckIdentityIsExactCommandBytes(t *testing.T) {
	const tip = "tip"
	const env = "envdigest"
	cases := []struct {
		name      string
		a, b      string
		identical bool
	}{
		{name: "exact bytes share", a: "go test ./...", b: "go test ./...", identical: true},
		{name: "double-quoted differs from single-quoted expansion", a: `echo "$HOME"`, b: `echo '$HOME'`},
		{name: "quoted differs from bare expansion", a: `echo "$HOME"`, b: `echo $HOME`},
		{name: "formatting whitespace is part of the identity", a: "go test   ./...", b: "go test ./..."},
		{name: "trailing whitespace differs", a: "go vet ./... ", b: "go vet ./..."},
		{name: "escaped quote differs from literal quote", a: `echo a\"b`, b: `echo a"b`},
		{name: "backslash differs from resolved byte", a: `echo a\b`, b: `echo ab`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := checkStateKey(tc.a, tip, env) == checkStateKey(tc.b, tip, env)
			if got != tc.identical {
				t.Fatalf("checkStateKey(%q) == checkStateKey(%q) = %v, want %v", tc.a, tc.b, got, tc.identical)
			}
		})
	}
	if checkStateKey("go test ./...", "tip-a", env) == checkStateKey("go test ./...", "tip-b", env) {
		t.Fatal("state key ignores the snapshot tip")
	}
	if checkStateKey("go test ./...", tip, "env-a") == checkStateKey("go test ./...", tip, "env-b") {
		t.Fatal("state key ignores the bootstrap environment digest")
	}
}

// TestCheckRefreshReplacesReceipt covers the concrete-concern invalidation
// input: a refresh request ignores the saved receipt, dispatches fresh, and
// replaces it, so a later plain request serves the refreshed evidence.
func TestCheckRefreshReplacesReceipt(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "first\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "second\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	refresh := ToolCall{ID: "call-refresh", Name: toolCheck, Arguments: `{"command":"go test ./...","refresh":true}`}
	second := coordinator.runCheck(context.Background(), refresh, "", ToolResult{ToolCallID: "c2", Name: toolCheck})
	third := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c3", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != 2 {
		t.Fatalf("check dispatches = %d, want exactly two (fresh, then refreshed) across three requests", got)
	}
	if !strings.Contains(first.Content, "check receipt: fresh") || !strings.Contains(first.Content, "first\n") {
		t.Fatalf("first check content = %q, want the first fresh receipt", first.Content)
	}
	if !strings.Contains(second.Content, "check receipt: fresh") || !strings.Contains(second.Content, "second\n") {
		t.Fatalf("refreshed check content = %q, want a fresh receipt with the second run's output", second.Content)
	}
	if receiptOp(first.Content) == receiptOp(second.Content) {
		t.Fatalf("refresh reused the old op %q, want a fresh dispatch", receiptOp(second.Content))
	}
	if !strings.Contains(third.Content, "check receipt: reused") || !strings.Contains(third.Content, "second\n") {
		t.Fatalf("third check content = %q, want the refreshed receipt reused", third.Content)
	}
	if receiptOp(second.Content) != receiptOp(third.Content) {
		t.Fatalf("third check served op %q, want the refreshed op %q", receiptOp(third.Content), receiptOp(second.Content))
	}
}

// TestCheckRefreshSharesInFlightDispatch pins that a refresh while an
// equivalent check is in flight waits on that dispatch instead of launching
// a duplicate: a concern justifies replacing stored evidence, never a
// second concurrent operation.
func TestCheckRefreshSharesInFlightDispatch(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.hangKinds = []string{"check"}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	// claimed reports that a caller committed to waiting on the active
	// entry. A's dispatch cannot complete before the manual result write
	// below, and that write happens only after the waiter has claimed, so
	// the waiter cannot mistake a terminal entry for an in-flight one and
	// re-dispatch: the sharing window is held open by construction, not
	// by timing.
	claimed := make(chan string, 1)
	coordinator.checks.waitHook = claimed

	first := make(chan ToolResult, 1)
	go func() {
		first <- coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c-a", Name: toolCheck})
	}()
	awaitDispatchCount(t, worker, notUnpackTask, 1)

	refresh := ToolCall{ID: "call-refresh", Name: toolCheck, Arguments: `{"command":"go test ./...","refresh":true}`}
	second := make(chan ToolResult, 1)
	go func() {
		second <- coordinator.runCheck(context.Background(), refresh, "", ToolResult{ToolCallID: "c-b", Name: toolCheck})
	}()
	select {
	case <-claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("the refresh caller never claimed the in-flight entry")
	}

	worker.mu.Lock()
	var opID string
	for _, record := range worker.dispatches {
		if notUnpackTask(record.task) {
			opID = record.envelope.OpID
		}
	}
	worker.results[opID] = protocol.ResultState{
		Status: protocol.ResultCompleted,
		Result: &protocol.Result{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "shared\n"},
	}
	worker.mu.Unlock()

	var gotA, gotB ToolResult
	select {
	case gotA = <-first:
	case <-time.After(10 * time.Second):
		t.Fatal("the original check did not return")
	}
	select {
	case gotB = <-second:
	case <-time.After(10 * time.Second):
		t.Fatal("the refresh did not return")
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want the refresh to share the in-flight dispatch", got)
	}
	if !strings.Contains(gotA.Content, "shared\n") || !strings.Contains(gotB.Content, "shared\n") {
		t.Fatalf("results = %q / %q, want both served from the one shared dispatch", gotA.Content, gotB.Content)
	}
}

// TestCheckShellRunInvalidatesMatchingReceipt pins the evidence-freshness
// rule: a completed plain shell run of the exact command bytes against the
// same state drops the matching receipt, so a later check dispatches
// instead of serving evidence older than the shell run — while an unrelated
// command and a differently spelled equivalent one leave every receipt in
// place.
func TestCheckShellRunInvalidatesMatchingReceipt(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "checked\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "unrelated\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "variant\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "shelled\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "rerun\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	if got := checkDispatchCount(worker); got != 1 {
		t.Fatalf("initial check dispatches = %d, want one", got)
	}

	// An unrelated control shell command must not touch the receipt.
	unrelated := coordinator.runShell(context.Background(), shellCall("echo unrelated"), "control", ToolResult{ToolCallID: "s1", Name: toolShell})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})
	if unrelated.IsError {
		t.Fatalf("unrelated shell = %+v, want success", unrelated)
	}
	if !strings.Contains(second.Content, "check receipt: reused") {
		t.Fatalf("check after unrelated shell = %q, want the saved receipt reused", second.Content)
	}
	if got := checkDispatchCount(worker); got != 1 {
		t.Fatalf("check dispatches after unrelated shell = %d, want the receipt untouched", got)
	}

	// A differently spelled equivalent command is its own identity: its
	// shell run neither matches nor invalidates the stored receipt.
	variant := coordinator.runShell(context.Background(), shellCall("go  test ./..."), "control", ToolResult{ToolCallID: "s2", Name: toolShell})
	if variant.IsError {
		t.Fatalf("variant shell = %+v, want success", variant)
	}
	stillReused := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c3", Name: toolCheck})
	if !strings.Contains(stillReused.Content, "check receipt: reused") {
		t.Fatalf("check after variant-spelling shell = %q, want the saved receipt reused", stillReused.Content)
	}
	if got := checkDispatchCount(worker); got != 1 {
		t.Fatalf("check dispatches after variant-spelling shell = %d, want the receipt untouched", got)
	}

	// The exact same command bytes via plain shell is fresh evidence: the
	// receipt must yield to it.
	shell := coordinator.runShell(context.Background(), shellCall("go test ./..."), "control", ToolResult{ToolCallID: "s3", Name: toolShell})
	if shell.IsError {
		t.Fatalf("matching shell = %+v, want success", shell)
	}
	third := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c4", Name: toolCheck})
	if got := checkDispatchCount(worker); got != 2 {
		t.Fatalf("check dispatches after matching shell = %d, want a fresh dispatch after the receipt was invalidated", got)
	}
	if !strings.Contains(third.Content, "check receipt: fresh") || !strings.Contains(third.Content, "rerun\n") {
		t.Fatalf("check after matching shell = %q, want a fresh receipt", third.Content)
	}
	if !strings.Contains(first.Content, "checked\n") {
		t.Fatalf("first check content = %q, want the initial receipt", first.Content)
	}
}

// TestCheckFailedShellRunInvalidatesMatchingReceipt pins the other half of
// the failed-exit classification on the invalidation path: the worker
// reports a nonzero exit as ResultFailed, and a completed failing shell run
// is fresh evidence, so the matching receipt must yield to it — a receipt
// must never outlive a run the coordinator just performed, pass or fail.
func TestCheckFailedShellRunInvalidatesMatchingReceipt(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "checked\n"},
		{Status: protocol.ResultFailed, ExitCode: 1, StdoutTail: "--- FAIL\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "rerun\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	if got := checkDispatchCount(worker); first.IsError || got != 1 {
		t.Fatalf("initial check = %+v (dispatches %d), want one fresh passing receipt", first, got)
	}

	shell := coordinator.runShell(context.Background(), shellCall("go test ./..."), "control", ToolResult{ToolCallID: "s1", Name: toolShell})
	if !shell.IsError {
		t.Fatalf("failing shell = %+v, want the nonzero exit reported", shell)
	}
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})
	if got := checkDispatchCount(worker); got != 2 {
		t.Fatalf("check dispatches after failing shell run = %d, want the receipt invalidated by the fresh failing evidence", got)
	}
	if !strings.Contains(second.Content, "check receipt: fresh") || !strings.Contains(second.Content, "rerun\n") {
		t.Fatalf("check after failing shell = %q, want a fresh receipt", second.Content)
	}
}

// TestCheckCancelledShellRunLeavesReceipt pins the other half of the
// evidence-freshness rule: only a verified completion invalidates. A shell
// dispatch that ends cancelled produced no evidence, so the matching receipt
// stays and the next check serves it.
func TestCheckCancelledShellRunLeavesReceipt(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.hangKinds = []string{"shell"}
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "checked\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	if !strings.Contains(first.Content, "check receipt: fresh") {
		t.Fatalf("first check content = %q, want a fresh receipt", first.Content)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan ToolResult, 1)
	go func() {
		result <- coordinator.runShell(ctx, shellCall("go test ./..."), "control", ToolResult{ToolCallID: "s1", Name: toolShell})
	}()
	awaitDispatchKind(t, worker, "shell", 1)
	cancel()
	select {
	case <-result:
		// The cancelled shell attempt settles however the dispatch path
		// renders it (here as a tool error); what matters is that it
		// produced no evidence.
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled shell did not return")
	}

	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})
	if got := checkDispatchCount(worker); got != 1 {
		t.Fatalf("check dispatches = %d, want the cancelled shell run to leave the receipt in place", got)
	}
	if !strings.Contains(second.Content, "check receipt: reused") || !strings.Contains(second.Content, "checked\n") {
		t.Fatalf("check after cancelled shell = %q, want the saved receipt reused", second.Content)
	}
}

// awaitDispatchCount polls until the worker has recorded want dispatches of
// interest, bounded by a deadline; a hang must be observable before the test
// drives the next phase.
func awaitDispatchCount(t *testing.T, worker *mockWorker, keep func(protocol.Task) bool, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if taskDispatchCount(worker, keep) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("worker dispatches of interest never reached %d", want)
}

func TestCheckWaiterOnFailedLaunchSeesNoReceipt(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	// Only check-kind operations hang: the unpack rides every
	// prepareSnapshot and must complete for the waiter to reach its claim.
	// Both callers unpack, so both need a scripted tip echo.
	worker.hangKinds = []string{"check"}
	// The gated cancel holds the dispatcher's failed launch in flight: the
	// waiter must reach its claim on the same active entry before the
	// registry replaces the failed one, or it would dispatch fresh.
	worker.cancelGate = make(chan struct{})
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "snap-tip"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "snap-tip"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	// claimed reports that the waiter committed to waiting on the active
	// entry: the cancellation below cannot terminalize the entry before
	// the waiter has claimed it, so the waiter serves the failed dispatch
	// instead of re-launching.
	claimed := make(chan string, 1)
	coordinator.checks.waitHook = claimed

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	resultA := make(chan ToolResult, 1)
	go func() {
		resultA <- coordinator.runCheck(ctxA, checkCall("go test ./..."), "", ToolResult{ToolCallID: "c-a", Name: toolCheck})
	}()
	awaitDispatchCount(t, worker, notUnpackTask, 1)

	resultB := make(chan ToolResult, 1)
	go func() {
		resultB <- coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c-b", Name: toolCheck})
	}()
	select {
	case <-claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("the waiter never claimed the active entry")
	}

	// Cancelling the dispatching caller leaves its entry without a receipt:
	// the waiter wakes to the fixed failed-launch notice, never a second
	// dispatch, and the key stays rerunnable afterwards.
	cancelA()
	close(worker.cancelGate)
	select {
	case got := <-resultA:
		if !got.IsError {
			t.Fatalf("cancelled check = %+v, want a tool error", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled check did not return")
	}
	select {
	case got := <-resultB:
		if !got.IsError || !strings.Contains(got.Content, "the tracked check failed to launch") {
			t.Fatalf("waiter on failed launch = %+v, want the fixed failed-launch error", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("waiter on failed launch did not return")
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want the waiter to share the single failed dispatch", got)
	}
	ctxR, cancelR := context.WithCancel(context.Background())
	defer cancelR()
	resultR := make(chan ToolResult, 1)
	go func() {
		resultR <- coordinator.runCheck(ctxR, checkCall("go test ./..."), "", ToolResult{ToolCallID: "c-retry", Name: toolCheck})
	}()
	awaitDispatchCount(t, worker, notUnpackTask, 2)
	cancelR()
	select {
	case got := <-resultR:
		if !got.IsError {
			t.Fatalf("retry check = %+v, want the cancelled rerun to end in an error", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("retry check did not return")
	}
}

func TestCheckWaiterHonorsItsOwnContext(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.hangKinds = []string{"check"}
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "snap-tip"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "snap-tip"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	resultA := make(chan ToolResult, 1)
	go func() {
		resultA <- coordinator.runCheck(ctxA, checkCall("go test ./..."), "", ToolResult{ToolCallID: "c-a", Name: toolCheck})
	}()
	awaitDispatchCount(t, worker, notUnpackTask, 1)

	ctxB, cancelB := context.WithCancel(context.Background())
	resultB := make(chan ToolResult, 1)
	go func() {
		resultB <- coordinator.runCheck(ctxB, checkCall("go test ./..."), "", ToolResult{ToolCallID: "c-b", Name: toolCheck})
	}()
	cancelB()
	select {
	case got := <-resultB:
		if !got.IsError || !strings.Contains(got.Content, "context canceled") {
			t.Fatalf("cancelled waiter = %+v, want the context error", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want a cancelled waiter to dispatch nothing", got)
	}
	cancelA()
	<-resultA
}

func TestCheckTimedOutDispatchIsNotReusable(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	// The check hangs until the caller's bound expires; both callers'
	// unpacks ride prepareSnapshot and need scripted tip echoes.
	worker.hangKinds = []string{"check"}
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "rerun ok\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	timedOut := coordinator.runCheck(ctx, checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})

	// The bound expiry delivered the worker kill (the reconcile cancelled
	// the op), left no receipt — a timed-out invocation is never reusable
	// evidence — and reported the timeout as an error, never a pass.
	if !timedOut.IsError || !strings.Contains(timedOut.Content, "deadline exceeded") {
		t.Fatalf("timed-out check = %+v, want the deadline reported as an error", timedOut)
	}
	if strings.Contains(timedOut.Content, "check receipt:") {
		t.Fatalf("timed-out check content = %q, want no receipt for a dispatch that never completed", timedOut.Content)
	}
	cancelled := 0
	for _, envelope := range worker.cancels {
		if strings.HasPrefix(envelope.OpID, "check.") {
			cancelled++
		}
	}
	if cancelled == 0 {
		t.Fatal("no check operation was cancelled; the timed-out dispatch left unverified work behind")
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want the timed-out request to leave the key rerunnable without a second launch", got)
	}

	// A repeat request against the unchanged state reruns rather than
	// serving a receipt that never existed.
	worker.hangKinds = nil
	rerun := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})
	if got := taskDispatchCount(worker, notUnpackTask); got != 2 {
		t.Fatalf("check dispatches = %d, want the repeat request after a timeout to dispatch fresh", got)
	}
	if rerun.IsError || !strings.Contains(rerun.Content, "rerun ok\n") {
		t.Fatalf("rerun check = %+v, want a fresh passing receipt", rerun)
	}
}

func TestCheckPreservesWorkerTailsVerbatim(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	// The worker already bounds its captured streams to tails; the head is
	// what truncation cuts, so both markers must survive control side to
	// prove the receipt preserved the worker's tails exactly.
	stdoutTail := "[truncated-head]" + strings.Repeat("o", 4096) + "[stdout-end]"
	stderrTail := "[truncated-head]" + strings.Repeat("e", 4096) + "[stderr-end]"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: stdoutTail, StderrTail: stderrTail},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	for i, result := range []ToolResult{first, second} {
		for _, part := range []string{stdoutTail, stderrTail} {
			if !strings.Contains(result.Content, part) {
				t.Fatalf("check %d content = %q, want the worker's bounded tail preserved verbatim", i, result.Content)
			}
		}
	}
	// Reuse serves the same receipt bytes: the tails are neither
	// re-truncated nor re-rendered across reuse.
	if got, want := tailOf(first.Content, "stdout:\n", "\nstderr:\n"), tailOf(second.Content, "stdout:\n", "\nstderr:\n"); got != want || got != stdoutTail {
		t.Fatalf("reused tail = %q, want the fresh tail %q served unchanged", got, want)
	}
}

// tailOf returns the rendered segment between marker and end, with its
// trailing newline, as renderCheckResult wrote it.
func tailOf(content, marker, end string) string {
	start := strings.Index(content, marker)
	if start < 0 {
		return ""
	}
	segment := content[start+len(marker):]
	if stop := strings.Index(segment, end); stop >= 0 {
		segment = segment[:stop]
	}
	return segment
}

// TestCheckRepresentativeRunAvoidsDuplicateLaunches replays the observed v1
// failure mode from issue #635: a coordinator re-invoked the same validation
// suite again and again to collect different summaries, growing its context
// with every rerun. Against tracked receipts the same sequence launches one
// dispatch per distinct (command, workspace state) instead of one per
// request.
func TestCheckRepresentativeRunAvoidsDuplicateLaunches(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTips = []string{"t1", "t1", "t1", "t1", "t2", "t2", "t2", "t2"}
	// Only fresh check dispatches consume ordinary results; the scripted
	// tips ride the unpack tasks.
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "suite t1\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "lint t1\n"},
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "suite t2\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, sequenceSnapshotProvider(worker.snapshotTips...))

	const requests = 7
	suite := "go test ./..."
	lint := "go vet ./..."
	sequence := []string{suite, suite, suite, lint, suite, suite, suite}
	checked := 0
	for i, command := range sequence {
		result := coordinator.runCheck(context.Background(), checkCall(command), "", ToolResult{ToolCallID: fmt.Sprintf("c%d", i), Name: toolCheck})
		if result.IsError {
			t.Fatalf("check %d = %+v, want a passing receipt", i, result)
		}
		// A reused receipt is answered without a dispatch; the saved
		// diagnostics are reprinted, not rerun for presentation.
		if strings.Contains(result.Content, "check receipt: fresh") {
			checked++
		}
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != 3 {
		t.Fatalf("check dispatches = %d, want one per distinct (command, state) across %d requests", got, requests)
	}
	if checked != 3 {
		t.Fatalf("fresh receipts = %d, want one per distinct (command, state)", checked)
	}
	// The duplicate presentation of the unchanged suite reprints the saved
	// receipt rather than rerunning.
	last := coordinator.runCheck(context.Background(), checkCall(suite), "", ToolResult{ToolCallID: "c-last", Name: toolCheck})
	if !strings.Contains(last.Content, "check receipt: reused") || !strings.Contains(last.Content, "suite t2\n") {
		t.Fatalf("last check content = %q, want the saved suite receipt reprinted", last.Content)
	}
	if got := taskDispatchCount(worker, notUnpackTask); got != 3 {
		t.Fatalf("check dispatches = %d, want the duplicate presentation to launch nothing", got)
	}
}

func TestCheckEvictsOldestReceiptBeyondBound(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	total := maxTrackedReceipts + 2
	worker.ordinaryResults = make([]protocol.Result, total)
	for i := range worker.ordinaryResults {
		worker.ordinaryResults[i] = protocol.Result{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: fmt.Sprintf("receipt-%d\n", i)}
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	for i := 0; i <= maxTrackedReceipts; i++ {
		result := coordinator.runCheck(context.Background(), checkCall(fmt.Sprintf("check-cmd-%d", i)), "", ToolResult{ToolCallID: fmt.Sprintf("c%d", i), Name: toolCheck})
		if result.IsError || !strings.Contains(result.Content, "check receipt: fresh") {
			t.Fatalf("check %d = %+v, want a fresh receipt", i, result)
		}
	}
	// The oldest receipt was evicted when the newest landed: its repeat is a
	// fresh dispatch, while the newest is still reused.
	evicted := coordinator.runCheck(context.Background(), checkCall("check-cmd-0"), "", ToolResult{ToolCallID: "c-evicted", Name: toolCheck})
	kept := coordinator.runCheck(context.Background(), checkCall(fmt.Sprintf("check-cmd-%d", maxTrackedReceipts)), "", ToolResult{ToolCallID: "c-kept", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != total {
		t.Fatalf("check dispatches = %d, want %d (the evicted oldest re-dispatched)", got, total)
	}
	if !strings.Contains(evicted.Content, "check receipt: fresh") {
		t.Fatalf("evicted receipt content = %q, want a fresh dispatch after eviction", evicted.Content)
	}
	if !strings.Contains(kept.Content, "check receipt: reused") {
		t.Fatalf("newest receipt content = %q, want the still-tracked receipt to be reused", kept.Content)
	}
}

// TestCheckInvalidateDropsOnlyTerminalReceipts pins the registry half of
// invalidation: a terminal entry is dropped so the next claim dispatches
// fresh, while an in-flight dispatch is never dropped — its waiters keep
// their dispatch and its completion still lands — and an unknown key is a
// no-op.
func TestCheckInvalidateDropsOnlyTerminalReceipts(t *testing.T) {
	registry := newCheckRegistry()

	active, outcome := registry.claim("in-flight", false)
	if outcome != claimFresh {
		t.Fatalf("first claim outcome = %v, want claimFresh", outcome)
	}
	registry.invalidate("in-flight")
	if _, outcome := registry.claim("in-flight", false); outcome != claimActive {
		t.Fatalf("claim after invalidating an active entry = %v, want claimActive", outcome)
	}
	registry.complete(active, &checkReceipt{status: protocol.ResultCompleted, exitCode: 0})
	if _, outcome := registry.claim("in-flight", false); outcome != claimReuse {
		t.Fatalf("claim after completion = %v, want claimReuse", outcome)
	}

	registry.invalidate("in-flight")
	if _, outcome := registry.claim("in-flight", false); outcome != claimFresh {
		t.Fatalf("claim after invalidating a terminal entry = %v, want claimFresh", outcome)
	}

	registry.invalidate("never-tracked")
}

func TestCheckIsCoordinatorOnly(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	result := coordinator.runTool(context.Background(), checkCall("go test ./..."), "b1")
	if !result.IsError || result.Content != "tracked checks are not available to sub-agents" {
		t.Fatalf("subagent check = %+v, want the fixed tool error", result)
	}
	if worker.dispatchCount() != 0 {
		t.Fatalf("worker dispatches = %d, want the subagent guard to refuse before any dispatch", worker.dispatchCount())
	}

	coordinatorTools := coordinator.coordinatorTools()
	subagentTools := coordinator.subagentTools()
	found := false
	for _, tool := range coordinatorTools {
		if tool.Name == toolCheck {
			found = true
		}
	}
	if !found {
		t.Fatal("coordinator toolset does not offer the check tool")
	}
	for _, tool := range subagentTools {
		if tool.Name == toolCheck {
			t.Fatal("subagent toolset must not offer the check tool")
		}
	}
}
