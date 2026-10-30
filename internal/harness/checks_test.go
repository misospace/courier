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

func TestCheckPreservesFailedExitOnReuse(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 1, StdoutTail: "--- FAIL\n", StderrTail: "boom\n"},
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
	}
	if !strings.Contains(second.Content, "check receipt: reused") {
		t.Fatalf("second check content = %q, want the reused failing receipt", second.Content)
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

func TestCheckNormalizesWhitespaceInIdentity(t *testing.T) {
	worker, delegator, cleanupWorker := testWorkerAndDelegator(t)
	defer cleanupWorker()
	worker.snapshotTip = "snap-tip"
	worker.ordinaryResults = []protocol.Result{
		{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: "ok\n"},
	}
	coordinator := newCheckCoordinator(t, delegator, snapshotProvider("snap-tip"))

	first := coordinator.runCheck(context.Background(), checkCall("go test   \t ./..."), "", ToolResult{ToolCallID: "c1", Name: toolCheck})
	second := coordinator.runCheck(context.Background(), checkCall("go test ./..."), "", ToolResult{ToolCallID: "c2", Name: toolCheck})

	if got := taskDispatchCount(worker, notUnpackTask); got != 1 {
		t.Fatalf("check dispatches = %d, want commands differing only in whitespace to share one receipt", got)
	}
	if !strings.Contains(second.Content, "check receipt: reused") {
		t.Fatalf("second check content = %q, want the whitespace variant to reuse the receipt", second.Content)
	}
	if first.IsError || second.IsError {
		t.Fatalf("checks = %+v / %+v, want passing receipts", first, second)
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
	// The waiter's unpack is its last I/O before its claim; once its
	// dispatch is recorded, only local computation separates it from the
	// claim, so a short margin makes the ordering deterministic while the
	// gated cancel keeps the entry active.
	awaitDispatchCount(t, worker, isWorkerUnpackTask, 2)
	time.Sleep(100 * time.Millisecond)

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
