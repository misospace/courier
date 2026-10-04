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
	"strings"
	"sync"
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
	op := &operation{envelope: envelope, cancel: cancel, wait: make(chan struct{})}
	w.ops[envelope.OpID] = op
	w.mu.Unlock()

	go func() {
		defer close(op.wait)
		result := w.runTask(ctx, task)
		w.mu.Lock()
		op.result = result
		w.mu.Unlock()
	}()
	w.respondStatus(wr, envelope.OpID, http.StatusAccepted)
}

func (w *Worker) runTask(ctx context.Context, task Task) *Result {
	stdout := newTailBuffer(maxResultStreamBytes, resultTailBytes)
	stderr := newTailBuffer(maxResultStreamBytes, resultTailBytes)
	cmd := exec.CommandContext(ctx, task.Command[0], task.Command[1:]...)
	cmd.Dir = w.config.WorkspaceDir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// The worker container carries no secrets; the child inherits the pod's
	// environment. Git hardening belongs to trusted integration, not to the
	// untrusted sandbox.
	startErr := cmd.Start()
	exitCode := -1
	if startErr == nil {
		waitErr := cmd.Wait()
		if ctx.Err() == nil {
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) {
				exitCode = exitErr.ExitCode()
			} else if waitErr == nil {
				exitCode = 0
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
		// Verified termination: acknowledge only after the process is gone.
		op.cancel()
		select {
		case <-op.wait:
		case <-r.Context().Done():
			http.Error(wr, "cancellation interrupted", http.StatusRequestTimeout)
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
