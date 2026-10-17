package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// TestWorkerArtifactConfinement pins the readArtifact confinement against
// paths the lexical check alone cannot see: symlinked parents, non-regular
// substitutions, and growth past the bound after validation (#411).
func TestWorkerArtifactConfinement(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("confinement relies on Linux openat O_NOFOLLOW semantics")
	}
	worker, priv, server := newTestWorker(t)
	client := &Client{BaseURL: server.URL}
	ctx := context.Background()

	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "sentinel.txt"), []byte("outside-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(worker.config.WorkspaceDir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(worker.config.WorkspaceDir, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	oversized := filepath.Join(worker.config.WorkspaceDir, "oversized.bin")
	if err := os.WriteFile(oversized, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(oversized, maxArtifactBytes+1); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		artifactPath string
	}{
		{artifactPath: "link/sentinel.txt"},
		{artifactPath: "link"},
		{artifactPath: "fifo"},
		{artifactPath: "oversized.bin"},
		{artifactPath: "."},
	}
	for i, tc := range cases {
		name := tc.artifactPath
		t.Run(name, func(t *testing.T) {
			task, _ := json.Marshal(Task{Command: []string{"true"}, ArtifactPath: tc.artifactPath})
			env := dispatchEnvelope(worker.clock())
			env.OpID = fmt.Sprintf("op-confinement-%d", i)
			env.Nonce = fmt.Sprintf("confinement-nonce-%08d", i)
			env.PayloadDigest = PayloadDigestOf(task)
			if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				state, err := client.Result(ctx, env.OpID)
				if err != nil {
					t.Fatal(err)
				}
				if state.Status == ResultFailed {
					result := state.Result
					if result == nil || result.Artifact != nil {
						t.Fatalf("rejected artifact must return no bytes: %+v", result)
					}
					if strings.Contains(result.StdoutTail, "outside-secret") || strings.Contains(result.StderrTail, "outside-secret") {
						t.Fatal("outside bytes leaked into the result")
					}
					return
				}
				if state.Status == ResultCompleted {
					t.Fatalf("artifact %q must fail the operation: %+v", tc.artifactPath, state.Result)
				}
				if time.Now().After(deadline) {
					t.Fatalf("operation with artifact %q never finished", tc.artifactPath)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}

	t.Run("valid artifact inside the workspace still reads", func(t *testing.T) {
		if err := os.WriteFile(filepath.Join(worker.config.WorkspaceDir, "kept.txt"), []byte("kept"), 0o600); err != nil {
			t.Fatal(err)
		}
		task, _ := json.Marshal(Task{Command: []string{"true"}, ArtifactPath: "kept.txt"})
		env := dispatchEnvelope(worker.clock())
		env.OpID = "op-confinement-valid"
		env.Nonce = "confinement-nonce-valid"
		env.PayloadDigest = PayloadDigestOf(task)
		if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			state, err := client.Result(ctx, env.OpID)
			if err != nil {
				t.Fatal(err)
			}
			if state.Status == ResultCompleted {
				if string(state.Result.Artifact) != "kept" {
					t.Fatalf("artifact = %q", state.Result.Artifact)
				}
				return
			}
			if state.Status == ResultFailed || time.Now().After(deadline) {
				t.Fatalf("valid artifact must read: %+v", state.Result)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	// A FIFO swapped over the artifact path after the descriptor is opened
	// and validated cannot affect the read: the descriptor still refers to
	// the opened regular file, so the operation must complete with the
	// original confined bytes and never block. A regression to a path-based
	// read after validation would instead open the FIFO and stall to the
	// deadline.
	t.Run("a FIFO swapped in after validation cannot redirect the read", func(t *testing.T) {
		staged := filepath.Join(worker.config.WorkspaceDir, "staged.fifo")
		if err := syscall.Mkfifo(staged, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(worker.config.WorkspaceDir, "swapped.txt"), []byte("genuine"), 0o600); err != nil {
			t.Fatal(err)
		}
		swapped := false
		previous := artifactReadHook
		artifactReadHook = func() {
			if err := os.Rename(staged, filepath.Join(worker.config.WorkspaceDir, "swapped.txt")); err != nil {
				t.Error(err)
				return
			}
			swapped = true
		}
		t.Cleanup(func() { artifactReadHook = previous })
		task, _ := json.Marshal(Task{Command: []string{"true"}, ArtifactPath: "swapped.txt"})
		env := dispatchEnvelope(worker.clock())
		env.OpID = "op-confinement-swap"
		env.Nonce = "confinement-nonce-swap000"
		env.PayloadDigest = PayloadDigestOf(task)
		if _, err := client.Dispatch(ctx, priv, env, task); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			state, err := client.Result(ctx, env.OpID)
			if err != nil {
				t.Fatal(err)
			}
			if state.Status == ResultCompleted {
				if string(state.Result.Artifact) != "genuine" {
					t.Fatalf("substitution must not redirect the read: artifact = %q", state.Result.Artifact)
				}
				if !swapped {
					t.Fatal("the swap hook never ran, so the post-validation window was not exercised")
				}
				return
			}
			if state.Status == ResultFailed || time.Now().After(deadline) {
				t.Fatalf("a post-validation swap must not block or redirect the read: %+v", state.Result)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// growingReader delivers maxArtifactBytes+1 bytes across reads whose sizes
// grow, so the bound must be enforced on bytes actually read rather than on
// any pre-read claim about the total.
type growingReader struct {
	delivered int
}

func (g *growingReader) Read(p []byte) (int, error) {
	if g.delivered >= maxArtifactBytes+1 {
		return 0, io.EOF
	}
	n := len(p)
	if n > 4096 {
		n = 4096
	}
	if g.delivered+n > maxArtifactBytes+1 {
		n = maxArtifactBytes + 1 - g.delivered
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	g.delivered += n
	return n, nil
}

func TestReadArtifactBoundedAppliesToBytesRead(t *testing.T) {
	data, err := readArtifactBounded(bytes.NewReader([]byte("artifact-bytes")))
	if err != nil || string(data) != "artifact-bytes" {
		t.Fatalf("readArtifactBounded = %q, %v", data, err)
	}
	data, err = readArtifactBounded(bytes.NewReader(make([]byte, maxArtifactBytes)))
	if err != nil || len(data) != maxArtifactBytes {
		t.Fatalf("an artifact of exactly the bound must read whole: %d bytes, %v", len(data), err)
	}
	data, err = readArtifactBounded(bytes.NewReader(make([]byte, maxArtifactBytes+1)))
	if err == nil || len(data) != 0 {
		t.Fatalf("a file truncated past the bound must be rejected whole: %d bytes, %v", len(data), err)
	}
	data, err = readArtifactBounded(&growingReader{})
	if err == nil || len(data) != 0 {
		t.Fatalf("a reader that grows past the bound must be rejected whole: %d bytes, %v", len(data), err)
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
