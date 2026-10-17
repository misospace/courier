package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testConfig(t *testing.T) (WorkerConfig, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return WorkerConfig{
		RunUID:        "run-uid",
		ControlPodUID: "control-uid",
		WorkerPodUID:  "worker-uid",
		PublicKey:     pub,
		WorkspaceDir:  t.TempDir(),
	}, pub, priv
}

func newTestWorker(t *testing.T) (*Worker, ed25519.PrivateKey, *httptest.Server) {
	t.Helper()
	config, _, priv := testConfig(t)
	worker, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(worker.Handler())
	t.Cleanup(server.Close)
	return worker, priv, server
}

func dispatchEnvelope(now time.Time) Envelope {
	env := NewEnvelope(now, defaultAdmissionWindow, nil)
	env.Kind = KindDispatch
	env.RunUID = "run-uid"
	env.ControlPodUID = "control-uid"
	env.WorkerPodUID = "worker-uid"
	env.BriefID = "brief-1"
	env.OpID = "op-1"
	return env
}

func signedBody(t *testing.T, key ed25519.PrivateKey, env Envelope, payload []byte) []byte {
	t.Helper()
	signature, err := env.Sign(key)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(signedRequest{Envelope: env, Signature: signature, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestEnvelopeSignVerifyAndTamper(t *testing.T) {
	env := dispatchEnvelope(time.Now())
	payload := []byte(`{"command":["true"]}`)
	env.PayloadDigest = PayloadDigestOf(payload)
	signature, err := env.Sign(ed25519.PrivateKey(make(ed25519.PrivateKey, 64)))
	if err != nil {
		t.Fatal(err)
	}
	wrongKeyPriv := make(ed25519.PrivateKey, 64)
	pub, priv, _ := GenerateKey()
	copy(wrongKeyPriv, priv)
	if err := env.Verify(pub, signature); err == nil {
		t.Fatal("signature from an unrelated key must not verify")
	}
	signature, err = env.Sign(priv)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.Verify(pub, signature); err != nil {
		t.Fatalf("honest signature must verify: %v", err)
	}
	env.Sequence = 5
	if err := env.Verify(pub, signature); err == nil {
		t.Fatal("tampered envelope must not verify")
	}
}

func TestEnvelopeValidation(t *testing.T) {
	now := time.Now()
	base := dispatchEnvelope(now)
	base.PayloadDigest = CancelPayloadDigest
	if err := base.Validate(now); err != nil {
		t.Fatalf("valid dispatch rejected: %v", err)
	}
	if err := base.Validate(now.Add(time.Hour)); err == nil {
		t.Fatal("expired envelope accepted")
	}

	cases := map[string]func(*Envelope){
		"dispatch sequence nonzero": func(e *Envelope) { e.Sequence = 1 },
		"cancel sequence zero":      func(e *Envelope) { e.Kind = KindCancel; e.BriefID = ""; e.Sequence = 0 },
		"release sequence zero":     func(e *Envelope) { e.Kind = KindRelease; e.Sequence = 0 },
		"missing run uid":           func(e *Envelope) { e.RunUID = "" },
		"missing control uid":       func(e *Envelope) { e.ControlPodUID = "" },
		"missing worker uid":        func(e *Envelope) { e.WorkerPodUID = "" },
		"missing op id":             func(e *Envelope) { e.OpID = "" },
		"missing brief id":          func(e *Envelope) { e.BriefID = "" },
		"short nonce":               func(e *Envelope) { e.Nonce = "short" },
		"bad digest":                func(e *Envelope) { e.PayloadDigest = "zz" },
		"zero expiration":           func(e *Envelope) { e.ExpiresAt = time.Time{} },
		"unknown kind":              func(e *Envelope) { e.Kind = "exfil" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			env := base
			mutate(&env)
			if err := env.Validate(now); err == nil {
				t.Fatal("invalid envelope accepted")
			}
		})
	}
}

func TestWorkerHappyPathDispatchAndResult(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	ctx := context.Background()

	if err := (&Client{BaseURL: server.URL}).UploadSnapshot(ctx, []byte("snapshot-bytes")); err != nil {
		t.Fatalf("UploadSnapshot: %v", err)
	}
	stored, err := os.ReadFile(filepath.Join(worker.config.WorkspaceDir, WorkspaceSnapshotName))
	if err != nil || !bytes.Equal(stored, []byte("snapshot-bytes")) {
		t.Fatalf("snapshot not stored verbatim: %q, %v", stored, err)
	}

	payload, _ := json.Marshal(Task{Command: []string{"sh", "-c", "printf output > artifact.txt; echo hello"}, ArtifactPath: "artifact.txt"})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(payload)
	status, err := (&Client{BaseURL: server.URL}).Dispatch(ctx, priv, env, payload)
	// The dispatch response is written after the task goroutine starts, so a
	// trivial task may already be terminal when the response is written; the
	// eventual result is polled below either way, and the redelivery guard is
	// asserted separately.
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	switch status {
	case "running", ResultCompleted, ResultCancelled, ResultFailed:
	default:
		t.Fatalf("Dispatch = %q, %v", status, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		state, err := (&Client{BaseURL: server.URL}).Result(ctx, "op-1")
		if err != nil {
			t.Fatalf("Result: %v", err)
		}
		if state.Status == ResultCompleted {
			if state.Result == nil || state.Result.ExitCode != 0 || state.Result.StdoutTail != "hello\n" {
				t.Fatalf("unexpected result: %+v", state.Result)
			}
			if string(state.Result.Artifact) != "output" {
				t.Fatalf("artifact = %q", state.Result.Artifact)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never completed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A second dispatch with the same opID and a different nonce must never
	// start a second execution.
	env2 := env
	env2.Nonce = "different-nonce-value-1234"
	_, err = (&Client{BaseURL: server.URL}).Dispatch(ctx, priv, env2, payload)
	if err == nil {
		t.Fatal("second dispatch for a known opID accepted")
	}
}

func TestWorkerRejectsForeignAndReplayedDispatches(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	task, _ := json.Marshal(Task{Command: []string{"true"}})
	ctx := context.Background()

	t.Run("rejections", func(t *testing.T) {
		cases := map[string]struct {
			mutate func(*Envelope)
			key    func() ed25519.PrivateKey
		}{
			"wrong run uid":         {mutate: func(e *Envelope) { e.RunUID = "other-run" }},
			"wrong control uid":     {mutate: func(e *Envelope) { e.ControlPodUID = "old-incarnation" }},
			"wrong worker uid":      {mutate: func(e *Envelope) { e.WorkerPodUID = "replaced-worker" }},
			"digest mismatch":       {mutate: func(e *Envelope) { e.PayloadDigest = PayloadDigestOf([]byte("other")) }},
			"expired":               {mutate: func(e *Envelope) { e.ExpiresAt = time.Now().Add(-time.Minute) }},
			"untrusted signing key": {key: func() ed25519.PrivateKey { _, priv, _ := GenerateKey(); return priv }},
		}
		for name, tc := range cases {
			t.Run(name, func(t *testing.T) {
				env := dispatchEnvelope(worker.clock())
				env.PayloadDigest = PayloadDigestOf(task)
				key := priv
				if tc.key != nil {
					key = tc.key()
				}
				if tc.mutate != nil {
					tc.mutate(&env)
				}
				if _, err := client.Dispatch(ctx, key, env, task); err == nil {
					t.Fatal("worker accepted a dispatch it must reject")
				}
				state, err := client.Result(ctx, "op-1")
				if err != nil || state.Status != "" {
					t.Fatalf("rejected dispatch must leave no operation, got %+v %v", state, err)
				}
			})
		}
	})

	t.Run("replayed envelope nonce", func(t *testing.T) {
		env := dispatchEnvelope(worker.clock())
		env.PayloadDigest = PayloadDigestOf(task)
		if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
			t.Fatalf("first dispatch failed: %v", err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			state, err := client.Result(ctx, "op-1")
			if err == nil && state.Status == ResultCompleted {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("first task never completed")
			}
			time.Sleep(10 * time.Millisecond)
		}
		// The exact same envelope (same nonce) is replay; even though the
		// opID is now completed, the worker must reject it.
		if _, err := client.Dispatch(ctx, priv, env, task); err == nil {
			t.Fatal("replayed envelope accepted")
		}
	})
}

func TestWorkerCancelBeforeDispatchTombstones(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	cancelEnv := NewEnvelope(worker.clock(), defaultAdmissionWindow, nil)
	cancelEnv.Kind = KindCancel
	cancelEnv.RunUID, cancelEnv.ControlPodUID, cancelEnv.WorkerPodUID = "run-uid", "control-uid", "worker-uid"
	cancelEnv.OpID = "op-tombstone"
	cancelEnv.Sequence = 1
	cancelEnv.PayloadDigest = CancelPayloadDigest
	status, err := client.Cancel(ctx, priv, cancelEnv)
	if err != nil || status != ResultCancelled {
		t.Fatalf("pre-dispatch cancel = %q, %v", status, err)
	}

	task, _ := json.Marshal(Task{Command: []string{"true"}})
	env := dispatchEnvelope(worker.clock())
	env.OpID = "op-tombstone"
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err == nil {
		t.Fatal("dispatch for a tombstoned opID must be rejected")
	}
	state, err := client.Result(ctx, "op-tombstone")
	if err != nil || state.Status != ResultCancelled {
		t.Fatalf("tombstoned opID must report cancelled and never run, got %+v %v", state, err)
	}
}

func TestWorkerCancelAcknowledgesOnlyAfterTermination(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	task, _ := json.Marshal(Task{Command: []string{"sleep", "60"}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	cancelEnv := env
	cancelEnv.Kind = KindCancel
	cancelEnv.Sequence = 1
	cancelEnv.PayloadDigest = CancelPayloadDigest
	cancelEnv.Nonce = "cancel-nonce-value-0001"
	started := time.Now()
	status, err := client.Cancel(ctx, priv, cancelEnv)
	if err != nil || status != ResultCancelled {
		t.Fatalf("Cancel = %q, %v", status, err)
	}
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Fatalf("cancellation waited too long: %s", elapsed)
	}
	state, err := client.Result(ctx, "op-1")
	if err != nil || state.Status != ResultCancelled {
		t.Fatalf("cancelled result = %+v, %v", state, err)
	}
	if state.Result != nil && state.Result.Status != ResultCancelled {
		t.Fatalf("result status = %q", state.Result.Status)
	}
	_ = worker
}

// TestWorkerCancelTerminatesTaskDescendants covers the cancellation contract
// for tasks whose real work runs in descendants: production dispatches are
// `sh -c ...`, so a cancelled shell must not leave its children behind (#372).
func TestWorkerCancelTerminatesTaskDescendants(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("task-tree assertions read process groups and /proc")
	}
	cases := map[string]struct {
		script     string
		writesLate bool
	}{
		// The descendant inherits the task's output descriptors: the wait it
		// holds open, and the workspace it writes, must both end at
		// cancellation.
		"descendant inherits stdout and stderr": {
			script:     `(sleep 2; echo late > late.txt) & echo $! > pid.txt; wait`,
			writesLate: true,
		},
		// The descendant redirects them away, so only the group kill can stop
		// it.
		"descendant redirects stdout and stderr": {
			script:     `(sleep 2; echo late > late.txt) >/dev/null 2>&1 & echo $! > pid.txt; wait`,
			writesLate: true,
		},
		// A descendant with no workspace effect of its own must still be gone.
		"long-lived descendant": {script: `sleep 300 & echo $! > pid.txt; wait`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			worker, priv, server := newTestWorker(t)
			client := &Client{BaseURL: server.URL}
			ctx := context.Background()

			task, _ := json.Marshal(Task{Command: []string{"sh", "-c", tc.script}})
			env := dispatchEnvelope(worker.clock())
			env.PayloadDigest = PayloadDigestOf(task)
			if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			descendant := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "pid.txt"))
			if !descendantLive(descendant) {
				t.Fatalf("descendant %d never started", descendant)
			}

			cancelEnv := env
			cancelEnv.Kind = KindCancel
			cancelEnv.Sequence = 1
			cancelEnv.PayloadDigest = CancelPayloadDigest
			cancelEnv.Nonce = "cancel-nonce-value-0001"
			started := time.Now()
			status, err := client.Cancel(ctx, priv, cancelEnv)
			if err != nil || status != ResultCancelled {
				t.Fatalf("Cancel = %q, %v", status, err)
			}
			if elapsed := time.Since(started); elapsed > 20*time.Second {
				t.Fatalf("cancellation was not bounded: %s", elapsed)
			}

			state, err := client.Result(ctx, "op-1")
			if err != nil || state.Result == nil || state.Result.Status != ResultCancelled {
				t.Fatalf("cancelled result = %+v, %v", state, err)
			}
			if state.Result.LiveDescendants != 0 {
				t.Fatalf("cancellation acknowledged while %d descendants were live", state.Result.LiveDescendants)
			}
			if descendantLive(descendant) {
				t.Fatalf("descendant %d outlived acknowledged cancellation", descendant)
			}
			if tc.writesLate {
				// The descendant would have written into the shared workspace
				// had cancellation left it running.
				time.Sleep(4 * time.Second)
				if _, err := os.Stat(filepath.Join(worker.config.WorkspaceDir, "late.txt")); !os.IsNotExist(err) {
					t.Fatalf("descendant wrote into the workspace after cancellation: %v", err)
				}
			}
		})
	}
}

// TestWorkerCancelRefusesUnprovenTermination pins the other half of the
// contract: work that escaped the task's process group cannot be proven gone,
// so the worker must report the cancellation as unverified instead of
// acknowledging it (#372).
func TestWorkerCancelRefusesUnprovenTermination(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("task-tree assertions read process groups and /proc")
	}
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required to leave the task's process group")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	// The escapee starts its own session, so the group signal never reaches
	// it — but it inherits the task's output descriptors, which is what
	// proves it is still there. It reports its own pid from inside that
	// session, so the cancellation can never race the moment setsid moves
	// it out of the task's group: a pid read from the background job would
	// still be in the group, and the group signal would legitimately kill
	// it before the test ever asked the worker to refuse anything.
	task, _ := json.Marshal(Task{Command: []string{"sh", "-c", `setsid sh -c 'echo $$ > escaped.txt; exec sleep 300' & wait`}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	escapee := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "escaped.txt"))
	if !descendantLive(escapee) {
		t.Fatalf("escapee %d was gone before cancellation", escapee)
	}
	// Read the escapee's own session from the kernel: a process still in the
	// task's session was never out of reach of the group signal, so a
	// cancellation below would legitimately kill it and the refusal under
	// test would be measuring nothing. The task inherits this process's
	// session because the worker gives it a process group, not a session.
	workerSession, ok := processSession(os.Getpid())
	if !ok {
		t.Fatal("could not read this test's own session")
	}
	escapeeSession, ok := processSession(escapee)
	if !ok {
		t.Fatalf("escapee %d left no readable session", escapee)
	}
	if escapeeSession == workerSession {
		t.Fatalf("escapee %d never left the task's session (%d)", escapee, escapeeSession)
	}

	cancelEnv := env
	cancelEnv.Kind = KindCancel
	cancelEnv.Sequence = 1
	cancelEnv.PayloadDigest = CancelPayloadDigest
	cancelEnv.Nonce = "cancel-nonce-value-0001"
	status, err := client.Cancel(ctx, priv, cancelEnv)
	if err == nil {
		t.Fatalf("cancellation was acknowledged with status %q while a task process outlived it", status)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("Cancel error = %v, want an unverified-termination rejection", err)
	}
	state, err := client.Result(ctx, "op-1")
	if err != nil || state.Result == nil || state.Result.LiveDescendants == 0 {
		t.Fatalf("unverified cancellation must still report the live tree: %+v, %v", state, err)
	}
	if !descendantLive(escapee) {
		t.Fatalf("escapee %d is gone, so the refusal was not about surviving work", escapee)
	}
}

// TestWorkerShutdownTerminatesTaskDescendants pins the same guarantee for pod
// termination: Shutdown must return only once the task's tree is gone, and
// must stay bounded while it proves it (#372).
func TestWorkerShutdownTerminatesTaskDescendants(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("task-tree assertions read process groups and /proc")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	task, _ := json.Marshal(Task{Command: []string{"sh", "-c", `sleep 300 & echo $! > pid.txt; wait`}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	descendant := waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "pid.txt"))

	shutdownCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	worker.Shutdown(shutdownCtx)
	if shutdownCtx.Err() != nil {
		t.Fatal("shutdown exceeded its bound while a descendant was live")
	}
	worker.mu.Lock()
	op := worker.ops["op-1"]
	var result *Result
	if op != nil {
		result = op.result
	}
	worker.mu.Unlock()
	if op == nil {
		t.Fatal("shutdown left no operation to account for")
	}
	if result == nil || result.LiveDescendants != 0 {
		t.Fatalf("shutdown result = %+v", result)
	}
	if descendantLive(descendant) {
		t.Fatalf("descendant %d outlived shutdown", descendant)
	}
}

// waitForDescendantPID reads the pid a task reported for its background
// descendant, and guarantees it is cleaned up even if the assertions below
// fail — a leaked test descendant outlives the test binary.
func waitForDescendantPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			if pid, convErr := strconv.Atoi(strings.TrimSpace(string(data))); convErr == nil {
				t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("task never reported a descendant pid: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// descendantLive reports whether a pid is still running, read straight from
// the kernel rather than through the worker's own proof. A killed process
// that nobody has reaped yet is a zombie: it is stopped, holds nothing, and
// writes nothing, so it is not a surviving descendant.
func descendantLive(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	stat := string(data)
	end := strings.LastIndex(stat, ")")
	if end < 0 {
		return false
	}
	fields := strings.Fields(stat[end+1:])
	return len(fields) >= 3 && fields[0] != "Z" && fields[0] != "X"
}

// processSession reports the session a pid leads. A process in a session of
// its own is unreachable by a signal aimed at the task's process group.
func processSession(pid int) (int, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	stat := string(data)
	end := strings.LastIndex(stat, ")")
	if end < 0 {
		return 0, false
	}
	// After the parenthesized comm: state, parent, process group, session.
	fields := strings.Fields(stat[end+1:])
	if len(fields) < 4 {
		return 0, false
	}
	session, err := strconv.Atoi(fields[3])
	if err != nil {
		return 0, false
	}
	return session, true
}

func TestWorkerLateCancelOnReplacementWorkerIsInert(t *testing.T) {
	_, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	cancelEnv := Envelope{
		Kind: KindCancel, RunUID: "run-uid", ControlPodUID: "control-uid",
		WorkerPodUID: "worker-uid", OpID: "foreign-op", Sequence: 4,
		PayloadDigest: CancelPayloadDigest,
	}
	cancelEnv.Nonce = "late-cancel-nonce-0001"
	cancelEnv.ExpiresAt = time.Now().Add(time.Minute)
	if _, err := client.Cancel(ctx, priv, cancelEnv); err != nil {
		t.Fatalf("late cancel on replacement worker: %v", err)
	}
	// The opID is unknown data: the worker tombstones it, runs nothing, and
	// reports it definitively cancelled.
	state, err := client.Result(ctx, "foreign-op")
	if err != nil || state.Status != ResultCancelled {
		t.Fatalf("late-cancel opID must be inert, got %+v %v", state, err)
	}
	// A never-seen opID remains unknown data.
	state, err = client.Result(ctx, "never-seen-op")
	if err != nil || state.Status != "" {
		t.Fatalf("unknown opID must be inert, got %+v %v", state, err)
	}
}

func TestWorkerArtifactGuards(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	escape := `{"command":["sh","-c","true"],"artifactPath":"../../etc/hostname"}`
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf([]byte(escape))
	env.Nonce = "artifact-escape-nonce-1"
	if _, err := client.Dispatch(ctx, priv, env, []byte(escape)); err != nil {
		t.Fatalf("escape dispatch: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, err := client.Result(ctx, "op-1")
		if err != nil {
			t.Fatal(err)
		}
		if state.Status == ResultCompleted || state.Status == ResultFailed {
			if state.Result == nil || state.Result.Artifact != nil {
				t.Fatalf("artifact escaping the workspace must never be returned: %+v", state.Result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never completed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A symlink inside the workspace is rejected at its own path: Lstat sees
	// the link, so nothing outside the workspace is followed.
	if err := os.Symlink("/etc/hostname", filepath.Join(worker.config.WorkspaceDir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	symlink := `{"command":["true"],"artifactPath":"link.txt"}`
	envSym := env
	envSym.OpID = "op-3"
	envSym.PayloadDigest = PayloadDigestOf([]byte(symlink))
	envSym.Nonce = "artifact-symlink-nonce-1"
	if _, err := client.Dispatch(ctx, priv, envSym, []byte(symlink)); err != nil {
		t.Fatalf("symlink dispatch: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		state, err := client.Result(ctx, "op-3")
		if err != nil {
			t.Fatal(err)
		}
		if state.Status == ResultFailed {
			if state.Result != nil && state.Result.Artifact != nil {
				t.Fatal("a symlinked artifact must never be returned")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("symlinked artifact must fail the operation")
		}
		time.Sleep(10 * time.Millisecond)
	}

	missing := `{"command":["true"],"artifactPath":"does-not-exist.bin"}`
	env2 := env
	env2.OpID = "op-2"
	env2.PayloadDigest = PayloadDigestOf([]byte(missing))
	env2.Nonce = "artifact-missing-nonce-1"
	if _, err := client.Dispatch(ctx, priv, env2, []byte(missing)); err != nil {
		t.Fatalf("missing artifact dispatch: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		state, err := client.Result(ctx, "op-2")
		if err != nil {
			t.Fatal(err)
		}
		if state.Status == ResultFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing artifact must fail the operation")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWorkerSnapshotDigestGuard(t *testing.T) {
	_, _, server := newTestWorker(t)
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/snapshot",
		bytes.NewReader([]byte(`{"digest":"deadbeef","data":"AA=="}`)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		t.Fatal("mismatched snapshot digest accepted")
	}
}

func TestWorkerShutdownTerminatesRunningTasks(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	task, _ := json.Marshal(Task{Command: []string{"sleep", "60"}})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(task)
	if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	worker.Shutdown(shutdownCtx)
	worker.mu.Lock()
	op := worker.ops["op-1"]
	worker.mu.Unlock()
	select {
	case <-op.wait:
	default:
		t.Fatal("shutdown returned before verified process termination")
	}
}

// retainedArtifactBytes sums the artifact bytes the worker still retains
// across every operation it knows.
func retainedArtifactBytes(w *Worker) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	total := 0
	for _, op := range w.ops {
		if op != nil && op.result != nil {
			total += len(op.result.Artifact)
		}
	}
	return total
}

func releaseEnvelope(base Envelope, nonce string) Envelope {
	env := base
	env.Kind = KindRelease
	env.Sequence = 1
	env.PayloadDigest = CancelPayloadDigest
	env.Nonce = nonce
	return env
}

func TestWorkerReleaseConsumesResultPayload(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	payload, _ := json.Marshal(Task{
		Command:      []string{"sh", "-c", "head -c 1048576 /dev/zero > artifact.bin"},
		ArtifactPath: "artifact.bin",
	})
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(payload)
	if _, err := client.Dispatch(ctx, priv, env, payload); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var first ResultState
	for {
		state, err := client.Result(ctx, "op-1")
		if err != nil {
			t.Fatalf("Result: %v", err)
		}
		if state.Status == ResultCompleted {
			first = state
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never completed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(first.Result.Artifact) != 1<<20 {
		t.Fatalf("first read artifact = %d bytes", len(first.Result.Artifact))
	}
	// Repeated reads before acknowledgement are reliable: the payload is
	// only consumed by release, never by reading.
	second, err := client.Result(ctx, "op-1")
	if err != nil || len(second.Result.Artifact) != 1<<20 {
		t.Fatalf("second read = %+v, %v", second.Result, err)
	}
	if retained := retainedArtifactBytes(worker); retained != 1<<20 {
		t.Fatalf("retained artifact bytes = %d, want %d", retained, 1<<20)
	}

	status, err := client.Release(ctx, priv, releaseEnvelope(env, "release-nonce-value-01"))
	if err != nil || status != ResultCompleted {
		t.Fatalf("Release = %q, %v", status, err)
	}
	state, err := client.Result(ctx, "op-1")
	if err != nil || state.Status != ResultCompleted {
		t.Fatalf("released result poll = %+v, %v", state, err)
	}
	if state.Result == nil || state.Result.Artifact != nil ||
		state.Result.StdoutTail != "" || state.Result.StderrTail != "" ||
		state.Result.Status != ResultCompleted {
		t.Fatalf("released result must be a compact stub: %+v", state.Result)
	}
	if retained := retainedArtifactBytes(worker); retained != 0 {
		t.Fatalf("retained artifact bytes after release = %d", retained)
	}

	env2 := env
	env2.Nonce = "redelivery-nonce-value-1"
	if _, err := client.Dispatch(ctx, priv, env2, payload); err == nil {
		t.Fatal("redelivery of a released opID must still be rejected")
	}

	if _, err := client.Release(ctx, priv, releaseEnvelope(env, "release-nonce-value-02")); err != nil {
		t.Fatalf("repeat release must be idempotent: %v", err)
	}
	cancelEnv := env
	cancelEnv.Kind = KindCancel
	cancelEnv.Sequence = 1
	cancelEnv.PayloadDigest = CancelPayloadDigest
	cancelEnv.Nonce = "cancel-nonce-value-0001"
	if _, err := client.Cancel(ctx, priv, cancelEnv); err != nil {
		t.Fatalf("cancel of a released op must still be acknowledged: %v", err)
	}

	unknownEnv := releaseEnvelope(env, "release-nonce-value-03")
	unknownEnv.OpID = "never-dispatched-op"
	if _, err := client.Release(ctx, priv, unknownEnv); err == nil {
		t.Fatal("release of an unknown opID must be rejected")
	} else {
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || transportErr.StatusCode != http.StatusNotFound {
			t.Fatalf("release of an unknown opID = %v, want 404", err)
		}
	}

	t.Run("release while running conflicts", func(t *testing.T) {
		runningPayload, _ := json.Marshal(Task{Command: []string{"sh", "-c", "sleep 30"}})
		runningEnv := dispatchEnvelope(worker.clock())
		runningEnv.OpID = "op-running"
		runningEnv.PayloadDigest = PayloadDigestOf(runningPayload)
		runningEnv.Nonce = "running-dispatch-nonce-1"
		if _, err := client.Dispatch(ctx, priv, runningEnv, runningPayload); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		_, err := client.Release(ctx, priv, releaseEnvelope(runningEnv, "running-release-nonce-1"))
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || transportErr.StatusCode != http.StatusConflict {
			t.Fatalf("release while running = %v, want 409", err)
		}
		cancelEnv := runningEnv
		cancelEnv.Kind = KindCancel
		cancelEnv.Sequence = 1
		cancelEnv.PayloadDigest = CancelPayloadDigest
		cancelEnv.Nonce = "running-cancel-nonce-1"
		if status, err := client.Cancel(ctx, priv, cancelEnv); err != nil || status != ResultCancelled {
			t.Fatalf("Cancel after refused release = %q, %v", status, err)
		}
	})

	t.Run("release of a cancelled result", func(t *testing.T) {
		cancelledPayload, _ := json.Marshal(Task{Command: []string{"sh", "-c", "sleep 30"}})
		cancelledEnv := dispatchEnvelope(worker.clock())
		cancelledEnv.OpID = "op-cancelled"
		cancelledEnv.PayloadDigest = PayloadDigestOf(cancelledPayload)
		cancelledEnv.Nonce = "cancelled-dispatch-nonce-1"
		if _, err := client.Dispatch(ctx, priv, cancelledEnv, cancelledPayload); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		cancelEnv := cancelledEnv
		cancelEnv.Kind = KindCancel
		cancelEnv.Sequence = 1
		cancelEnv.PayloadDigest = CancelPayloadDigest
		cancelEnv.Nonce = "cancelled-cancel-nonce-1"
		if _, err := client.Cancel(ctx, priv, cancelEnv); err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		state, err := client.Result(ctx, "op-cancelled")
		if err != nil || state.Status != ResultCancelled || state.Result == nil {
			t.Fatalf("cancelled result poll = %+v, %v", state, err)
		}
		if _, err := client.Release(ctx, priv, releaseEnvelope(cancelledEnv, "cancelled-release-nonce-1")); err != nil {
			t.Fatalf("Release of a cancelled result: %v", err)
		}
		state, err = client.Result(ctx, "op-cancelled")
		if err != nil || state.Status != ResultCancelled || state.Result == nil ||
			state.Result.Artifact != nil || state.Result.ExitCode != 0 {
			t.Fatalf("released cancelled result must be a compact stub: %+v, %v", state.Result, err)
		}
	})

	t.Run("mismatched path and envelope are refused", func(t *testing.T) {
		for _, opID := range []string{"op-rel-a", "op-rel-b"} {
			payload, _ := json.Marshal(Task{
				Command:      []string{"sh", "-c", "head -c 1048576 /dev/zero > " + opID + ".bin"},
				ArtifactPath: opID + ".bin",
			})
			env := dispatchEnvelope(worker.clock())
			env.OpID = opID
			env.PayloadDigest = PayloadDigestOf(payload)
			env.Nonce = "mismatch-dispatch-" + opID
			if _, err := client.Dispatch(ctx, priv, env, payload); err != nil {
				t.Fatalf("Dispatch %s: %v", opID, err)
			}
		}
		waitForCompleted := func(opID string) {
			t.Helper()
			deadline := time.Now().Add(10 * time.Second)
			for {
				state, err := client.Result(ctx, opID)
				if err != nil {
					t.Fatalf("Result %s: %v", opID, err)
				}
				if state.Status == ResultCompleted {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("%s never completed", opID)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		waitForCompleted("op-rel-a")
		waitForCompleted("op-rel-b")

		// A validly signed release for op-rel-a, posted to op-rel-b's release
		// URL, must be refused: the signature authorizes one operation, not
		// whichever operation the URL names.
		envA := dispatchEnvelope(worker.clock())
		envA.OpID = "op-rel-a"
		envA.PayloadDigest = CancelPayloadDigest
		signature, err := releaseEnvelope(envA, "mismatch-release-a-1").Sign(priv)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(signedRequest{
			Envelope:  releaseEnvelope(envA, "mismatch-release-a-1"),
			Signature: signature,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.post(ctx, "/v1/tasks/op-rel-b/release", body, "op-rel-b"); err == nil {
			t.Fatal("a release for op-rel-a posted to op-rel-b must be refused")
		} else {
			var transportErr *TransportError
			if !errors.As(err, &transportErr) || transportErr.StatusCode != http.StatusBadRequest {
				t.Fatalf("mismatched release = %v, want 400", err)
			}
		}
		// The refused request must not have consumed its nonce or touched
		// either operation: both results keep their full payloads.
		state, err := client.Result(ctx, "op-rel-b")
		if err != nil || state.Result == nil || len(state.Result.Artifact) != 1<<20 {
			t.Fatalf("op-rel-b must keep its full payload: %+v, %v", state.Result, err)
		}
		if retained := retainedArtifactBytes(worker); retained != 2<<20 {
			t.Fatalf("retained artifact bytes after refused release = %d, want %d", retained, 2<<20)
		}
		// Re-posting the exact same signed envelope, now properly addressed
		// to op-rel-a, must succeed: a burned nonce would surface as a 401
		// replay rejection here, proving the refused attempt consumed
		// nothing. The release compacts only op-rel-a.
		if _, err := client.post(ctx, "/v1/tasks/op-rel-a/release", body, "op-rel-a"); err != nil {
			t.Fatalf("properly addressed re-release must succeed: %v", err)
		}
		state, err = client.Result(ctx, "op-rel-a")
		if err != nil || state.Result == nil || state.Result.Artifact != nil {
			t.Fatalf("released op-rel-a must be a compact stub: %+v, %v", state.Result, err)
		}
		state, err = client.Result(ctx, "op-rel-b")
		if err != nil || state.Result == nil || len(state.Result.Artifact) != 1<<20 {
			t.Fatalf("op-rel-b must still keep its full payload: %+v, %v", state.Result, err)
		}
		if retained := retainedArtifactBytes(worker); retained != 1<<20 {
			t.Fatalf("retained artifact bytes after addressed release = %d, want %d", retained, 1<<20)
		}
	})

	t.Run("release refuses an unverified termination", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("unverified-termination assertions read process groups and /proc")
		}
		if _, err := exec.LookPath("setsid"); err != nil {
			t.Skip("setsid is required to leave the task's process group")
		}
		unverifiedPayload, _ := json.Marshal(Task{Command: []string{"sh", "-c", `setsid sh -c 'echo $$ > released-escaped.txt; exec sleep 300' & wait`}})
		unverifiedEnv := dispatchEnvelope(worker.clock())
		unverifiedEnv.OpID = "op-unverified"
		unverifiedEnv.PayloadDigest = PayloadDigestOf(unverifiedPayload)
		unverifiedEnv.Nonce = "unverified-dispatch-nonce-1"
		if _, err := client.Dispatch(ctx, priv, unverifiedEnv, unverifiedPayload); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		waitForDescendantPID(t, filepath.Join(worker.config.WorkspaceDir, "released-escaped.txt"))
		cancelEnv := unverifiedEnv
		cancelEnv.Kind = KindCancel
		cancelEnv.Sequence = 1
		cancelEnv.PayloadDigest = CancelPayloadDigest
		cancelEnv.Nonce = "unverified-cancel-nonce-1"
		if _, err := client.Cancel(ctx, priv, cancelEnv); err == nil {
			t.Fatal("cancellation with a live escapee must be refused")
		}
		state, err := client.Result(ctx, "op-unverified")
		if err != nil || state.Result == nil || state.Result.LiveDescendants == 0 {
			t.Fatalf("unverified result must report the live tree: %+v, %v", state.Result, err)
		}
		_, err = client.Release(ctx, priv, releaseEnvelope(unverifiedEnv, "unverified-release-nonce-1"))
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || transportErr.StatusCode != http.StatusConflict {
			t.Fatalf("release of an unverified termination = %v, want 409", err)
		}
		state, err = client.Result(ctx, "op-unverified")
		if err != nil || state.Result == nil || state.Result.LiveDescendants == 0 {
			t.Fatalf("release must retain the live-tree report: %+v, %v", state.Result, err)
		}
	})
}

func TestWorkerReleasedStorageStaysBoundedAcrossTasks(t *testing.T) {
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	opIDs := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		name := "artifact-" + strconv.Itoa(i) + ".bin"
		payload, _ := json.Marshal(Task{
			Command:      []string{"sh", "-c", "head -c 1048576 /dev/zero > " + name},
			ArtifactPath: name,
		})
		env := dispatchEnvelope(worker.clock())
		env.OpID = "op-round-" + strconv.Itoa(i)
		env.PayloadDigest = PayloadDigestOf(payload)
		env.Nonce = "round-dispatch-nonce-" + strconv.Itoa(i)
		opIDs = append(opIDs, env.OpID)
		if _, err := client.Dispatch(ctx, priv, env, payload); err != nil {
			t.Fatalf("Dispatch round %d: %v", i, err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			state, err := client.Result(ctx, env.OpID)
			if err != nil {
				t.Fatalf("Result round %d: %v", i, err)
			}
			if state.Status == ResultCompleted {
				if len(state.Result.Artifact) != 1<<20 {
					t.Fatalf("round %d artifact = %d bytes", i, len(state.Result.Artifact))
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d task never completed", i)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if retained := retainedArtifactBytes(worker); retained != 1<<20 {
			t.Fatalf("round %d retained artifact bytes = %d, want %d", i, retained, 1<<20)
		}
		if _, err := client.Release(ctx, priv, releaseEnvelope(env, "round-release-nonce-"+strconv.Itoa(i))); err != nil {
			t.Fatalf("Release round %d: %v", i, err)
		}
		if retained := retainedArtifactBytes(worker); retained != 0 {
			t.Fatalf("round %d left %d cumulative retained artifact bytes; retention must stay bounded", i, retained)
		}
	}

	for _, opID := range opIDs {
		env := dispatchEnvelope(worker.clock())
		env.OpID = opID
		env.Nonce = "post-release-nonce-" + opID
		env.PayloadDigest = PayloadDigestOf([]byte(`{"command":["true"]}`))
		if _, err := client.Dispatch(ctx, priv, env, []byte(`{"command":["true"]}`)); err == nil {
			t.Fatalf("redispatch of released %s must be rejected", opID)
		}
	}
}
