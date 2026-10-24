package protocol

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/misospace/courier/internal/devenv"
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
	// Env optionally carries the development-bootstrap environment delta the
	// task's child should be merged over (#414). Control composes it from
	// the approved contract's captured environment with protected keys
	// (Courier's control plane, git plumbing, dynamic-loader injection)
	// already filtered, and the worker filters again before merging — both
	// sides call devenv.ProtectedKeys so neither can drift. Fixed control
	// scripts (unpack, pack) never carry it.
	// +optional
	Env []string `json:"env,omitempty"`
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
	// processes still alive in the task's process group, or adopted escapees
	// — descendants that left the group for a session of their own — still
	// alive after the worker's sweep, or a holder of the task's output
	// descriptors that survived both. The count is a pod-level floor: the
	// sweep reports every unaccounted live process in the pod, so it can
	// include another operation's adopted escapee rather than this task's
	// own descendants. Zero is a verified stop — nothing of the task was
	// left running before anything was acknowledged. A nonzero value means
	// termination could not be proven, so control must treat the operation
	// as unverified (#372, #399).
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
	mux.HandleFunc("POST /v1/tasks/{opID}/release", w.handleRelease)
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
	// nil cancel function. The task's own process group dies from the
	// cancellation signal; anything outside it is the sweep's business, not
	// the wait's (#399).
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	op := &operation{envelope: envelope, wait: make(chan struct{})}
	op.cancel = cancel
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
	// Production tasks are `sh -c ...`, so the task's real work runs in
	// descendants. exec.CommandContext kills only the direct child, which
	// would leave those descendants mutating the workspace after
	// cancellation claimed to be verified: the task gets its own process
	// group and cancellation kills the whole group (#372).
	cmd.SysProcAttr = taskSysProcAttr()
	cmd.Cancel = func() error { return signalProcessGroup(cmd.Process.Pid, syscall.SIGKILL) }
	// A descendant that inherited the task's output descriptors keeps the
	// wait open after the task itself is gone, so cleanup is bounded rather
	// than left to hang on an inherited pipe.
	cmd.WaitDelay = taskTerminationGrace
	// The worker container carries no secrets; the child inherits the pod's
	// environment. Git hardening belongs to trusted integration, not to the
	// untrusted sandbox.
	// An optional bootstrap environment delta (#414) merges over that
	// environment. Protected keys are filtered again here — both sides call
	// devenv.ProtectedKeys so neither can drift — and an oversized delta
	// fails the task closed: a delta over the bound is not executed, never
	// truncated.
	if len(task.Env) > 0 {
		if taskEnvBytes(task.Env) > maxTaskEnvBytes {
			return &Result{
				Status:     ResultFailed,
				ExitCode:   -1,
				StderrTail: "task environment exceeds the 64 KiB dispatch bound and the task was rejected before execution",
			}
		}
		cmd.Env = devenv.MergeEnv(os.Environ(), task.Env)
	}
	// The child goes through the containment funnel: its pid is registered
	// before the start is released, so no adoption pass can reap it out from
	// under the Wait that owes it a status (#399).
	startErr := containment.startTaskChild(cmd)
	taskPID := 0
	if startErr == nil {
		taskPID = cmd.Process.Pid
		defer containment.releaseTaskChild(taskPID)
	}
	// A task that does not lead its own process group cannot have its
	// descendants terminated at all, so a worker that reported such a task
	// as stopped would be claiming a termination it never proved.
	grouped := startErr == nil && leadsOwnProcessGroup(taskPID)
	if startErr == nil && !grouped {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		startErr = errNoProcessGroup
		// The child never led a group, so no group signal covered its
		// descendants; the sweep is what terminates the escapees it left
		// behind. Survivors keep the failure unproven, never clean (#399).
		survivors, sweepErr := containment.sweepAdoptedTrees(time.Now().Add(taskTreeVerifyBound))
		if sweepErr != nil {
			startErr = &containmentUnprovenError{survivors: 1, cause: errNoProcessGroup}
		} else if survivors > 0 {
			startErr = &containmentUnprovenError{survivors: survivors, cause: errNoProcessGroup}
		}
	}
	exitCode := -1
	if grouped {
		waitErr := cmd.Wait()
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
	if grouped {
		// Nothing of the task is acknowledged as stopped until its process
		// group is empty; anything left behind is reported rather than
		// counted as verified termination (#372).
		result.LiveDescendants = awaitProcessGroupEmpty(cmd.Process.Pid)
		// A descendant that left the group — a setsid, a daemonising tool —
		// is beyond every group signal, but once its ancestors are gone the
		// worker has adopted it. The sweep runs at every grouped task's
		// termination, completed ones included: a daemon a successful task
		// left behind is still untrusted work mutating the workspace, and
		// control would otherwise clear its suppression on a completion the
		// worker never proved (#399). The sweep terminates the adopted trees
		// and proves the pod holds nothing live but this worker and the
		// trees of the tasks still registered.
		survivors, sweepErr := containment.sweepAdoptedTrees(time.Now().Add(taskTreeVerifyBound))
		if sweepErr != nil {
			// Without a process listing nothing about the escapees can be
			// proven, and an unproven stop must never read as verified.
			result.LiveDescendants++
		} else {
			result.LiveDescendants += survivors
		}
	}
	// A start that failed after the child was already running proved its own
	// teardown in the funnel; when that proof could not be made, the result
	// reports the survivors so it stays unverified and is never released
	// (#372, #399).
	var unproven *containmentUnprovenError
	if errors.As(startErr, &unproven) {
		result.LiveDescendants = unproven.survivors
		if result.LiveDescendants < 1 {
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

// maxTaskEnvBytes bounds the serialized task environment delta (#414). It is
// a transport resource bound, like the artifact and stream bounds: an
// environment over the bound fails the task closed rather than being
// truncated.
const maxTaskEnvBytes = 64 << 10

// taskEnvBytes returns the serialized size of an environment delta as it
// travels the protocol: one NUL terminator per KEY=VALUE record.
func taskEnvBytes(env []string) int {
	total := 0
	for _, record := range env {
		total += len(record) + 1
	}
	return total
}

// leadsOwnProcessGroup reports whether pid leads its own process group, which
// is what makes a group signal reach the task's descendants. A pid that
// cannot be read at all does not lead anything: the caller fails such a task
// closed rather than claim a group it cannot see.
func leadsOwnProcessGroup(pid int) bool {
	info, err := processStat(pid)
	return err == nil && info.pgid == pid
}

// awaitProcessGroupEmpty proves that no live process remains in the process
// group led by pgid and returns how many were still live when the bound
// expired. The group is re-signalled while it waits, so a descendant that
// raced the cancellation signal cannot outlive the acknowledgement (#372).
// The caller holds containment's pidfd pin for the group leader across this
// call: the pin keeps the leader's numeric pid — and therefore the group
// number — from being reused by any new process or new group, so every
// group signal here can only reach the task's own group.
func awaitProcessGroupEmpty(pgid int) int {
	deadline := time.Now().Add(taskTreeVerifyBound)
	for {
		// Second identity check under the pin: if the number no longer
		// answers for this group, nothing of the task's tree can be left in
		// whatever holds it.
		if info, err := processStat(pgid); err == nil && info.pgid != pgid {
			return 0
		}
		_ = signalProcessGroup(pgid, syscall.SIGKILL)
		live, err := liveGroupMembers(pgid)
		if err != nil {
			// Without a process listing the only answer available is the
			// kernel's, and a group signal that finds no member proves the
			// group empty. Anything short of that proof stays unverified.
			if errors.Is(probeProcessGroup(pgid), syscall.ESRCH) {
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
// Failing to read the listing — the directory or any member that has not
// been confirmed gone — is an error rather than a silent empty group: an
// unreadable process is not evidence of absence, and unproven termination
// must never read as verified termination.
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
		info, statErr := processStat(pid)
		if statErr != nil {
			if errors.Is(statErr, errProcessGone) {
				continue
			}
			return nil, statErr
		}
		if info.pgid != pgid {
			continue
		}
		if info.state != "Z" && info.state != "X" {
			live = append(live, pid)
		}
	}
	return live, nil
}

// processStat reads one process's run state, parent, and process group. The
// comm field is parenthesized and may contain spaces, so parsing starts after
// its closing parenthesis: what follows is state, parent, then process group.
// The parent is what lets a sweep attribute an orphaned descendant after the
// kernel reparented it (#399). A confirmed disappearance answers
// errProcessGone — that is evidence, and a reader may treat the process as
// absent. Every other failure (a refused or broken read, a malformed stat)
// is not evidence of absence and must fail the surrounding proof closed
// rather than be skipped.
func processStat(pid int) (procInfo, error) {
	if hook := processStatFailureHook.Load(); hook != nil {
		if err := (*hook)(pid); err != nil {
			return procInfo{}, fmt.Errorf("stat of pid %d is unreadable: %w", pid, err)
		}
	}
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			return procInfo{}, errProcessGone
		}
		return procInfo{}, fmt.Errorf("stat of pid %d is unreadable: %w", pid, err)
	}
	stat := string(data)
	end := strings.LastIndex(stat, ")")
	if end < 0 {
		return procInfo{}, fmt.Errorf("stat of pid %d is malformed", pid)
	}
	fields := strings.Fields(stat[end+1:])
	if len(fields) < 3 {
		return procInfo{}, fmt.Errorf("stat of pid %d is malformed", pid)
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return procInfo{}, fmt.Errorf("stat of pid %d is malformed", pid)
	}
	group, err := strconv.Atoi(fields[2])
	if err != nil {
		return procInfo{}, fmt.Errorf("stat of pid %d is malformed", pid)
	}
	return procInfo{state: fields[0], ppid: parent, pgid: group, fd: -1}, nil
}

// artifactReadHook, when non-nil, runs between artifact validation and the
// bounded read; tests install it to land a concurrent swap in exactly that
// window. It is nil in production.
var artifactReadHook func()

// errOpenatUnsupported marks the platform gap in readArtifact's confinement
// walk: the artifact read fails closed with the explicit cause rather than
// open without boundary or disguise the gap as a missing file. It can never
// come from Linux syscall.Openat, so the pass-through below is inert there.
var errOpenatUnsupported = errors.New("artifact confinement openat is unsupported on this platform")

// readArtifact reads the declared artifact, refusing anything outside the
// workspace and over the transport bound. The lexical gate is only a first
// filter; the authoritative confinement check walks the cleaned path from the
// workspace's own directory descriptor with O_NOFOLLOW, so no component —
// parent or final — can be a symlink at open time, and validation happens on
// the opened descriptor rather than on a path a concurrent task could swap.
func (w *Worker) readArtifact(declared string) ([]byte, error) {
	clean := filepath.Clean("/" + declared)
	if clean == "/" {
		return nil, errors.New("declared artifact is unavailable")
	}
	abs := filepath.Join(w.config.WorkspaceDir, clean)
	rel, err := filepath.Rel(w.config.WorkspaceDir, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return nil, errors.New("artifact path escapes the workspace")
	}
	dir, err := os.Open(w.config.WorkspaceDir)
	if err != nil {
		return nil, errors.New("declared artifact is unavailable")
	}
	defer dir.Close()
	dirfd := int(dir.Fd())
	components := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	for i, name := range components {
		flags := walkOpenFlags(i < len(components)-1)
		next, openErr := openat(dirfd, name, flags)
		if i > 0 {
			closeFd(dirfd)
		}
		if openErr != nil {
			if errors.Is(openErr, errOpenatUnsupported) {
				return nil, errOpenatUnsupported
			}
			if errors.Is(openErr, syscall.ELOOP) {
				if i < len(components)-1 {
					return nil, errors.New("artifact path escapes the workspace")
				}
				return nil, errors.New("artifact must be a regular file")
			}
			return nil, errors.New("declared artifact is unavailable")
		}
		dirfd = next
	}
	f := os.NewFile(uintptr(dirfd), filepath.Join(w.config.WorkspaceDir, clean))
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, errors.New("artifact read failed")
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("artifact must be a regular file")
	}
	if info.Size() > maxArtifactBytes {
		return nil, errors.New("artifact exceeds the bound and is rejected whole")
	}
	if artifactReadHook != nil {
		artifactReadHook()
	}
	return readArtifactBounded(f)
}

// readArtifactBounded enforces the artifact bound on bytes actually read, not
// on pre-read metadata: a file that grows past the bound while being read is
// rejected whole rather than truncated.
func readArtifactBounded(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxArtifactBytes+1))
	if err != nil {
		return nil, errors.New("artifact read failed")
	}
	if len(data) > maxArtifactBytes {
		return nil, errors.New("artifact exceeds the bound and is rejected whole")
	}
	return data, nil
}

// handleRelease consumes one terminal result's payload on control's
// instruction. The operation entry is never removed: the compact stub keeps
// redelivery of the opID rejected by handleDispatch and keeps handleCancel
// working, so replay protection outlives the payload it protects. Release is
// idempotent — a repeat release rewrites the same stub.
func (w *Worker) handleRelease(wr http.ResponseWriter, r *http.Request) {
	opID := r.PathValue("opID")
	if opID == "" || len(opID) > 128 || strings.ContainsAny(opID, "/\\") {
		http.Error(wr, "invalid request", http.StatusBadRequest)
		return
	}
	envelope, _, ok := w.verifySigned(wr, r, KindRelease, 0)
	if !ok {
		return
	}
	// The URL is not signed; the envelope is. A release addressed to one
	// operation must not be able to erase another operation's result, so a
	// path/envelope mismatch is refused before the nonce is consumed or any
	// operation is touched.
	if envelope.OpID != opID {
		http.Error(wr, "release envelope does not address this operation", http.StatusBadRequest)
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
	op, known := w.ops[opID]
	_, tombstoned := w.tombstones[opID]
	var terminal *Result
	if known && op != nil && op.result != nil && op.result.LiveDescendants == 0 {
		terminal = op.result
		op.result = &Result{Status: op.result.Status}
	}
	unverified := known && op != nil && op.result != nil && terminal == nil
	w.mu.Unlock()

	switch {
	case known && op != nil && terminal == nil && !unverified:
		http.Error(wr, "operation has not terminated; nothing to release", http.StatusConflict)
	case unverified:
		// An unproven termination is never released: its LiveDescendants
		// report is the only record that the task tree may still be live
		// (#372), and dropping it would let a later cancellation read a
		// verified stop that never happened.
		http.Error(wr, "termination is unverified; result is retained", http.StatusConflict)
	case known || tombstoned:
		w.respondStatus(wr, opID, http.StatusOK)
	default:
		http.Error(wr, "unknown operation", http.StatusNotFound)
	}
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
