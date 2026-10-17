package protocol

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Worker configuration and transport bounds.
const (
	// WorkerPort is the unauthenticated-from-the-worker's-perspective listener
	// port: security comes from the network boundary (ingress limited to the
	// control pod), never from the listener itself.
	WorkerPort = 8080

	// maxSnapshotBytes mirrors the broker's bundle import transport bound.
	maxSnapshotBytes = 65 << 20
	// maxArtifactBytes mirrors the §5 bundle size bound: an artifact over the
	// bound fails the operation rather than being truncated.
	maxArtifactBytes = 64 << 20
	// maxResultStreamBytes bounds the captured stdout/stderr tail.
	maxResultStreamBytes = 64 << 20
	resultTailBytes      = 64 << 10
	// maxNonces bounds the replay set; a worker that exhausts it fails closed
	// instead of silently forgetting earlier nonces.
	maxNonces = 1 << 16
	// dispatchAdmissionWindow bounds how long a signed envelope may be
	// admitted after signing. It is not an operation-duration limit.
	defaultAdmissionWindow = 10 * time.Minute
	// taskTerminationGrace bounds the wait after a task's own process is
	// gone — cancelled or exited — while a descendant still holds the task's
	// output descriptors. It bounds cleanup, never a run's duration.
	taskTerminationGrace = 5 * time.Second
	// taskTreeVerifyBound bounds the proof, after a task's wait ended, that
	// no live process is left in the task's process group (#372). Together
	// with the grace it keeps shutdown inside the pod's termination budget.
	taskTreeVerifyBound = 5 * time.Second
	taskTreeVerifyPoll  = 50 * time.Millisecond
)

// Task is the dispatch payload. The brief semantics (objective, owned files,
// success check) live in control; the worker executes exactly one argv vector
// in its sanitized workspace and returns untrusted output.
type Task struct {
	Command []string `json:"command"`
	// ArtifactPath optionally names a file the task produced, relative to the
	// workspace, whose bytes are returned as the untrusted artifact.
	// +optional
	ArtifactPath string `json:"artifactPath,omitempty"`
}

// Result is the untrusted execution result. Worker-declared fields are never
// validation input on the control side.
type Result struct {
	// Status is "completed", "cancelled", or "failed".
	Status string `json:"status"`
	// ExitCode is the task's exit code; -1 when the process never ran or was
	// killed before exit.
	ExitCode int `json:"exitCode"`
	// StdoutTail and StderrTail are bounded tails of the captured streams.
	StdoutTail string `json:"stdoutTail,omitempty"`
	StderrTail string `json:"stderrTail,omitempty"`
	// Artifact carries the untrusted artifact bytes, base64-encoded, when the
	// task declared an artifact path and the file exists within the bound.
	Artifact []byte `json:"artifact,omitempty"`
	// LiveDescendants reports work that outlived the task it belonged to:
	// processes still alive in the task's process group, or a holder of the
	// task's output descriptors that survived the group entirely. Zero is a
	// verified stop — nothing of the task was left running before anything
	// was acknowledged. A nonzero value means termination could not be
	// proven, so control must treat the operation as unverified (#372).
	LiveDescendants int `json:"liveDescendants,omitempty"`
}

// Result statuses.
const (
	ResultCompleted = "completed"
	ResultCancelled = "cancelled"
	ResultFailed    = "failed"
)

// Snapshot is the sanitized workspace upload control performs before
// dispatch. The bytes are written to a fixed path; unpacking and sanitization
// semantics belong to the control side.
type Snapshot struct {
	Digest string `json:"digest"`
	Data   []byte `json:"data"`
}

// WorkspaceSnapshotName is the fixed file the snapshot is stored under.
const WorkspaceSnapshotName = "snapshot.pack"

type operation struct {
	envelope Envelope
	cancel   context.CancelFunc
	cmd      *exec.Cmd
	wait     chan struct{}
	result   *Result
}

// WorkerConfig is the immutable identity material the operator provisions
// into the worker pod at creation.
type WorkerConfig struct {
	RunUID        string
	ControlPodUID string
	WorkerPodUID  string
	PublicKey     []byte
	WorkspaceDir  string
	// AdmissionWindow overrides the default envelope admission window; zero
	// uses the default.
	AdmissionWindow time.Duration
}

// Worker is the honest signed-protocol listener. It keeps replay state and
// cancellation tombstones in process memory only: the worker pod never
// restarts its container (restartPolicy: Never), and a replacement worker is
// fenced by the UID bindings in every envelope.
type Worker struct {
	config WorkerConfig
	clock  func() time.Time

	mu         sync.Mutex
	ops        map[string]*operation
	tombstones map[string]struct{}
	nonces     map[string]struct{}
}

// NewWorker constructs the listener. It fails closed on incomplete identity
// configuration.
func NewWorker(config WorkerConfig) (*Worker, error) {
	if config.RunUID == "" || config.ControlPodUID == "" || config.WorkerPodUID == "" {
		return nil, errors.New("protocol: worker identity configuration is incomplete")
	}
	if len(config.PublicKey) != 32 {
		return nil, errors.New("protocol: worker verification key has the wrong size")
	}
	if strings.TrimSpace(config.WorkspaceDir) == "" || !filepath.IsAbs(config.WorkspaceDir) {
		return nil, errors.New("protocol: worker workspace directory must be absolute")
	}
	return &Worker{
		config:     config,
		clock:      time.Now,
		ops:        make(map[string]*operation),
		tombstones: make(map[string]struct{}),
		nonces:     make(map[string]struct{}),
	}, nil
}

// Handler returns the worker's HTTP API. It is served over plain HTTP inside
// the pod's network boundary.
func (w *Worker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tasks", w.handleDispatch)
	mux.HandleFunc("POST /v1/tasks/cancel", w.handleCancel)
	mux.HandleFunc("GET /v1/tasks/{opID}/result", w.handleResult)
	mux.HandleFunc("POST /v1/snapshot", w.handleSnapshot)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

type signedRequest struct {
	Envelope  Envelope `json:"envelope"`
	Signature []byte   `json:"signature"`
	Payload   []byte   `json:"payload,omitempty"`
}

func (w *Worker) verifySigned(wr http.ResponseWriter, r *http.Request, wantKind string, maxPayload int) (Envelope, []byte, bool) {
	if r.URL.RawQuery != "" || r.ContentLength <= 0 {
		http.Error(wr, "invalid request", http.StatusBadRequest)
		return Envelope{}, nil, false
	}
	var req signedRequest
	dec := json.NewDecoder(http.MaxBytesReader(wr, r.Body, int64(maxPayload)+1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(wr, "invalid request", http.StatusBadRequest)
		return Envelope{}, nil, false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		http.Error(wr, "invalid request", http.StatusBadRequest)
		return Envelope{}, nil, false
	}
	if req.Envelope.Kind != wantKind {
		http.Error(wr, "unexpected envelope kind", http.StatusBadRequest)
		return Envelope{}, nil, false
	}
	if err := req.Envelope.Validate(w.clock()); err != nil {
		http.Error(wr, "envelope rejected", http.StatusUnauthorized)
		return Envelope{}, nil, false
	}
	if req.Envelope.RunUID != w.config.RunUID ||
		req.Envelope.ControlPodUID != w.config.ControlPodUID ||
		req.Envelope.WorkerPodUID != w.config.WorkerPodUID {
		http.Error(wr, "envelope identity does not match this worker incarnation", http.StatusUnauthorized)
		return Envelope{}, nil, false
	}
	if err := req.Envelope.Verify(w.config.PublicKey, req.Signature); err != nil {
		http.Error(wr, "envelope signature is invalid", http.StatusUnauthorized)
		return Envelope{}, nil, false
	}
	wantDigest := CancelPayloadDigest
	if wantKind == KindDispatch {
		if len(req.Payload) > maxPayload {
			http.Error(wr, "payload exceeds the transport bound", http.StatusRequestEntityTooLarge)
			return Envelope{}, nil, false
		}
		wantDigest = PayloadDigestOf(req.Payload)
	}
	if subtle.ConstantTimeCompare([]byte(req.Envelope.PayloadDigest), []byte(wantDigest)) != 1 {
		http.Error(wr, "payload digest does not match the envelope", http.StatusUnauthorized)
		return Envelope{}, nil, false
	}
	return req.Envelope, req.Payload, true
}

// consumeNonce records an envelope nonce exactly once. The bounded set never
// forgets earlier nonces: replay is rejected with 401, and exhaustion is
// reported separately so control can distinguish a wedged worker from an
// attack.
func (w *Worker) consumeNonce(envelope Envelope) (accepted, exhausted bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, seen := w.nonces[envelope.Nonce]; seen {
		return false, false
	}
	if len(w.nonces) >= maxNonces {
		return false, true
	}
	w.nonces[envelope.Nonce] = struct{}{}
	return true, false
}

func (w *Worker) handleDispatch(wr http.ResponseWriter, r *http.Request) {
	envelope, payload, ok := w.verifySigned(wr, r, KindDispatch, maxArtifactBytes)
	if !ok {
		return
	}
	var task Task
	if err := json.Unmarshal(payload, &task); err != nil {
		http.Error(wr, "dispatch payload is not a task", http.StatusBadRequest)
		return
	}
	if len(task.Command) == 0 || strings.TrimSpace(task.Command[0]) == "" {
		http.Error(wr, "dispatch payload requires a command", http.StatusBadRequest)
		return
	}
	accepted, exhausted := w.consumeNonce(envelope)
	if exhausted {
		// Fail closed and loud: a worker whose replay state is full accepts
		// nothing further and must be treated as infrastructure failure.
		http.Error(wr, "replay state exhausted; worker must be replaced", http.StatusServiceUnavailable)
		return
	}
	if !accepted {
		http.Error(wr, "nonce replay rejected", http.StatusUnauthorized)
		return
	}

	w.mu.Lock()
	if _, tombstoned := w.tombstones[envelope.OpID]; tombstoned {
		w.mu.Unlock()
		http.Error(wr, "operation is tombstoned and must never run", http.StatusConflict)
		return
	}
	if _, running := w.ops[envelope.OpID]; running {
		w.mu.Unlock()
		http.Error(wr, "operation is already registered; redelivery never starts a second execution", http.StatusConflict)
		return
	}
	// The cancellable context is created before the operation becomes visible
	// in the map, so a cancellation racing this dispatch can never observe a
	// nil cancel function.
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	// The cancellation time bounds the wait that follows it: the task's own
	// process group dies from the cancellation signal, so a wait that outlasts
	// the grace means something outside that group still held the task's
	// output (#372).
	var cancelledAt atomic.Int64
	op := &operation{envelope: envelope, wait: make(chan struct{})}
	op.cancel = func() {
		cancelledAt.CompareAndSwap(0, time.Now().UnixNano())
		cancel()
	}
	w.ops[envelope.OpID] = op
	w.mu.Unlock()

	go func() {
		defer close(op.wait)
		result := w.runTask(ctx, task, &cancelledAt)
		w.mu.Lock()
		op.result = result
		w.mu.Unlock()
	}()
	w.respondStatus(wr, envelope.OpID, http.StatusAccepted)
}

func (w *Worker) runTask(ctx context.Context, task Task, cancelledAt *atomic.Int64) *Result {
	stdout := newTailBuffer(maxResultStreamBytes, resultTailBytes)
	stderr := newTailBuffer(maxResultStreamBytes, resultTailBytes)
	cmd := exec.CommandContext(ctx, task.Command[0], task.Command[1:]...)
	cmd.Dir = w.config.WorkspaceDir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Production tasks are `sh -c ...`, so the task's real work runs in
	// descendants. exec.CommandContext kills only the direct child, which
	// would leave those descendants mutating the workspace after
	// cancellation claimed to be verified: the task gets its own process
	// group and cancellation kills the whole group (#372).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return signalProcessGroup(cmd.Process.Pid, syscall.SIGKILL) }
	// A descendant that inherited the task's output descriptors keeps the
	// wait open after the task itself is gone, so cleanup is bounded rather
	// than left to hang on an inherited pipe.
	cmd.WaitDelay = taskTerminationGrace
	// The worker container carries no secrets; the child inherits the pod's
	// environment. Git hardening belongs to trusted integration, not to the
	// untrusted sandbox.
	startErr := cmd.Start()
	// A task that does not lead its own process group cannot have its
	// descendants terminated at all, so a worker that reported such a task
	// as stopped would be claiming a termination it never proved.
	grouped := startErr == nil && leadsOwnProcessGroup(cmd.Process.Pid)
	if startErr == nil && !grouped {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		startErr = errNoProcessGroup
	}
	exitCode := -1
	unaccountedWriter := false
	if grouped {
		waitErr := cmd.Wait()
		// A wait that ended on the grace bound rather than on task exit
		// means a descendant still holds the task's output descriptors: work
		// outlived the task even when the task itself succeeded.
		unaccountedWriter = errors.Is(waitErr, exec.ErrWaitDelay)
		if !unaccountedWriter && ctx.Err() != nil {
			// Wait only ever outlasts the grace because something outside
			// the task's group held its output open — the group signal
			// cannot be waiting on anything. A wait that returned sooner is
			// proof that every writer closed.
			if stamp := cancelledAt.Load(); stamp > 0 && !time.Now().Before(time.Unix(0, stamp).Add(taskTerminationGrace)) {
				unaccountedWriter = true
			}
		}
		if ctx.Err() == nil {
			var exitErr *exec.ExitError
			switch {
			case errors.As(waitErr, &exitErr):
				exitCode = exitErr.ExitCode()
			case waitErr == nil:
				exitCode = 0
			case cmd.ProcessState != nil:
				// The task exited and only its output was still draining, so
				// the exit code it produced still decides the result.
				exitCode = cmd.ProcessState.ExitCode()
			}
		}
	}
	status := ResultCompleted
	if ctx.Err() != nil {
		status = ResultCancelled
	} else if startErr != nil || exitCode != 0 {
		status = ResultFailed
	}
	result := &Result{
		Status:     status,
		ExitCode:   exitCode,
		StdoutTail: stdout.tail(),
		StderrTail: stderr.tail(),
	}
	if errors.Is(startErr, errNoProcessGroup) {
		result.StderrTail = strings.TrimSpace(result.StderrTail + "\n" + errNoProcessGroup.Error())
	}
	if grouped && (status == ResultCancelled || unaccountedWriter) {
		// Nothing of the task is acknowledged as stopped until its process
		// group is empty; anything left behind is reported rather than
		// counted as verified termination.
		result.LiveDescendants = awaitProcessGroupEmpty(cmd.Process.Pid)
		if unaccountedWriter && result.LiveDescendants == 0 {
			// The group is empty, yet a writer kept the task's descriptors
			// past the grace: an unaccounted holder is not a proven stop.
			result.LiveDescendants = 1
		}
	}
	if task.ArtifactPath != "" && status == ResultCompleted {
		artifact, artifactErr := w.readArtifact(task.ArtifactPath)
		if artifactErr != nil {
			result.Status = ResultFailed
			result.StderrTail = strings.TrimSpace(result.StderrTail + "\n" + artifactErr.Error())
		} else {
			result.Artifact = artifact
		}
	}
	return result
}

// errNoProcessGroup reports a task that started without leading its own
// process group. It is the worker's own fixed diagnostic, never task output.
var errNoProcessGroup = errors.New("task process group was not established")

// leadsOwnProcessGroup reports whether pid leads its own process group, which
// is what makes a group signal reach the task's descendants.
func leadsOwnProcessGroup(pid int) bool {
	_, group, ok := processStat(pid)
	return ok && group == pid
}

// signalProcessGroup delivers sig to the whole process group led by pid. A
// group that is already gone counts as success: there is nothing left to
// terminate, and reporting it as a failure would mask a completed cleanup.
func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	err := syscall.Kill(-pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

// awaitProcessGroupEmpty proves that no live process remains in the process
// group led by pgid and returns how many were still live when the bound
// expired. The group is re-signalled while it waits, so a descendant that
// raced the cancellation signal cannot outlive the acknowledgement (#372).
func awaitProcessGroupEmpty(pgid int) int {
	deadline := time.Now().Add(taskTreeVerifyBound)
	for {
		// A recycled pid answers for whatever now owns it: if the old group
		// leader's entry reports a different group, nothing of the task's
		// tree can be left in this one.
		if _, group, ok := processStat(pgid); ok && group != pgid {
			return 0
		}
		_ = signalProcessGroup(pgid, syscall.SIGKILL)
		live, err := liveGroupMembers(pgid)
		if err != nil {
			// Without a process listing the only answer available is the
			// kernel's, and a group signal that finds no member proves the
			// group empty. Anything short of that proof stays unverified.
			if errors.Is(syscall.Kill(-pgid, syscall.Signal(0)), syscall.ESRCH) {
				return 0
			}
			return 1
		}
		if len(live) == 0 {
			return 0
		}
		if !time.Now().Before(deadline) {
			return len(live)
		}
		time.Sleep(taskTreeVerifyPoll)
	}
}

// liveGroupMembers lists the processes still in the process group led by
// pgid, excluding zombies: a zombie has already exited and holds no
// descriptors, so it mutates nothing even before its new parent reaps it.
// Failing to read the listing is an error rather than a silent empty group —
// unproven termination must never read as verified termination.
func liveGroupMembers(pgid int) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	live := make([]int, 0, 4)
	for _, entry := range entries {
		pid, convErr := strconv.Atoi(entry.Name())
		if convErr != nil {
			continue
		}
		state, group, ok := processStat(pid)
		if !ok || group != pgid {
			continue
		}
		if state != "Z" && state != "X" {
			live = append(live, pid)
		}
	}
	return live, nil
}

// processStat reads one process's run state and process group. The comm field
// is parenthesized and may contain spaces, so parsing starts after its closing
// parenthesis: what follows is state, parent, then process group.
func processStat(pid int) (state string, pgid int, ok bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", 0, false
	}
	stat := string(data)
	end := strings.LastIndex(stat, ")")
	if end < 0 {
		return "", 0, false
	}
	fields := strings.Fields(stat[end+1:])
	if len(fields) < 3 {
		return "", 0, false
	}
	group, err := strconv.Atoi(fields[2])
	if err != nil {
		return "", 0, false
	}
	return fields[0], group, true
}

// readArtifact reads the declared artifact, refusing anything outside the
// workspace and over the transport bound.
func (w *Worker) readArtifact(declared string) ([]byte, error) {
	clean := filepath.Clean("/" + declared)
	abs := filepath.Join(w.config.WorkspaceDir, clean)
	rel, err := filepath.Rel(w.config.WorkspaceDir, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return nil, errors.New("artifact path escapes the workspace")
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, errors.New("declared artifact is unavailable")
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("artifact must be a regular file")
	}
	if info.Size() > maxArtifactBytes {
		return nil, errors.New("artifact exceeds the bound and is rejected whole")
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil, errors.New("artifact read failed")
	}
	return data, nil
}

func (w *Worker) handleCancel(wr http.ResponseWriter, r *http.Request) {
	envelope, _, ok := w.verifySigned(wr, r, KindCancel, 0)
	if !ok {
		return
	}
	accepted, exhausted := w.consumeNonce(envelope)
	if exhausted {
		http.Error(wr, "replay state exhausted; worker must be replaced", http.StatusServiceUnavailable)
		return
	}
	if !accepted {
		http.Error(wr, "nonce replay rejected", http.StatusUnauthorized)
		return
	}

	w.mu.Lock()
	op, active := w.ops[envelope.OpID]
	_, tombstoned := w.tombstones[envelope.OpID]
	if !tombstoned {
		// A cancellation tombstone is accepted even if it arrives before its
		// dispatch, and it is final: any later dispatch with this opID is
		// rejected.
		w.tombstones[envelope.OpID] = struct{}{}
	}
	w.mu.Unlock()

	if active && op != nil {
		// Verified termination: acknowledge only after the task's process
		// group is empty. A cancellation that cannot prove that is reported
		// as unverified instead of acknowledged, so control keeps treating
		// the operation as possibly live (#372).
		op.cancel()
		select {
		case <-op.wait:
		case <-r.Context().Done():
			http.Error(wr, "cancellation interrupted", http.StatusRequestTimeout)
			return
		}
		w.mu.Lock()
		result := op.result
		w.mu.Unlock()
		if result != nil && result.LiveDescendants > 0 {
			http.Error(wr, "task tree outlived cancellation; termination is unverified", http.StatusInternalServerError)
			return
		}
	}
	w.respondStatus(wr, envelope.OpID, http.StatusOK)
}

func (w *Worker) respondStatus(wr http.ResponseWriter, opID string, code int) {
	w.mu.Lock()
	op, known := w.ops[opID]
	_, tombstoned := w.tombstones[opID]
	var result *Result
	if known && op != nil {
		result = op.result
	}
	w.mu.Unlock()

	status := "running"
	if result != nil {
		status = result.Status
	} else if tombstoned {
		// Tombstoned and never dispatched (or cancelled before completion
		// reporting): the operation is definitively not running.
		status = ResultCancelled
	}
	body := map[string]any{"status": status}
	if result != nil {
		body["result"] = result
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.WriteHeader(code)
	_ = json.NewEncoder(wr).Encode(body)
}

func (w *Worker) handleResult(wr http.ResponseWriter, r *http.Request) {
	opID := r.PathValue("opID")
	if opID == "" || len(opID) > 128 || strings.ContainsAny(opID, "/\\") {
		http.Error(wr, "invalid request", http.StatusBadRequest)
		return
	}
	w.mu.Lock()
	op, known := w.ops[opID]
	_, tombstoned := w.tombstones[opID]
	var result *Result
	if known && op != nil {
		result = op.result
	}
	w.mu.Unlock()

	wr.Header().Set("Content-Type", "application/json")
	switch {
	case result != nil:
		_ = json.NewEncoder(wr).Encode(map[string]any{"status": result.Status, "result": result})
	case known:
		wr.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(wr).Encode(map[string]any{"status": "running"})
	case tombstoned:
		// Tombstoned and never dispatched: definitively cancelled, nothing
		// ever ran.
		_ = json.NewEncoder(wr).Encode(map[string]any{"status": ResultCancelled})
	default:
		// A replacement worker never saw this opID: late requests are inert
		// data and run nothing.
		http.Error(wr, "unknown operation", http.StatusNotFound)
	}
}

func (w *Worker) handleSnapshot(wr http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" || r.ContentLength <= 0 {
		http.Error(wr, "invalid request", http.StatusBadRequest)
		return
	}
	var snapshot Snapshot
	dec := json.NewDecoder(http.MaxBytesReader(wr, r.Body, maxSnapshotBytes+(maxSnapshotBytes/2)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&snapshot); err != nil {
		http.Error(wr, "invalid request", http.StatusBadRequest)
		return
	}
	if len(snapshot.Data) == 0 || len(snapshot.Data) > maxSnapshotBytes {
		http.Error(wr, "snapshot exceeds the transport bound", http.StatusRequestEntityTooLarge)
		return
	}
	sum := sha256.Sum256(snapshot.Data)
	if subtle.ConstantTimeCompare([]byte(snapshot.Digest), []byte(hex.EncodeToString(sum[:]))) != 1 {
		http.Error(wr, "snapshot digest does not match its bytes", http.StatusUnauthorized)
		return
	}
	path := filepath.Join(w.config.WorkspaceDir, WorkspaceSnapshotName)
	if err := os.WriteFile(path, snapshot.Data, 0o600); err != nil {
		http.Error(wr, "snapshot storage failed", http.StatusInternalServerError)
		return
	}
	wr.WriteHeader(http.StatusNoContent)
}

// Shutdown cancels every active operation and waits for verified process
// termination. It is invoked on pod termination so no task outlives the
// listener.
func (w *Worker) Shutdown(ctx context.Context) {
	w.mu.Lock()
	ops := make([]*operation, 0, len(w.ops))
	for _, op := range w.ops {
		if op != nil {
			ops = append(ops, op)
		}
	}
	for _, op := range ops {
		op.cancel()
	}
	waits := make([]chan struct{}, 0, len(ops))
	for _, op := range ops {
		waits = append(waits, op.wait)
	}
	w.mu.Unlock()
	for _, wait := range waits {
		select {
		case <-wait:
		case <-ctx.Done():
			return
		}
	}
}

// tailBuffer captures a bounded tail of an output stream.
type tailBuffer struct {
	max, tailSize int
	mu            sync.Mutex
	buf           []byte
	total         int64
}

func newTailBuffer(max, tail int) *tailBuffer {
	return &tailBuffer{max: max, tailSize: tail}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.total += int64(len(p))
	if t.total > int64(t.max) {
		return 0, errors.New("stream exceeds the capture bound")
	}
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.tailSize {
		t.buf = t.buf[len(t.buf)-t.tailSize:]
	}
	return len(p), nil
}

func (t *tailBuffer) tail() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}
