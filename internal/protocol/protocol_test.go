package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	if err != nil || status != "running" {
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
