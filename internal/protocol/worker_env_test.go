package protocol

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/devenv"
)

func dispatchTaskEnv(t *testing.T, task Task) *Result {
	t.Helper()
	worker, priv, server := newTestWorker(t)
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	env := dispatchEnvelope(worker.clock())
	env.PayloadDigest = PayloadDigestOf(payload)
	ctx := context.Background()
	if _, err := (&Client{BaseURL: server.URL}).Dispatch(ctx, priv, env, payload); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, err := (&Client{BaseURL: server.URL}).Result(ctx, env.OpID)
		if err != nil {
			t.Fatalf("Result: %v", err)
		}
		if state.Status != "running" {
			return state.Result
		}
		if time.Now().After(deadline) {
			t.Fatal("task never reached a terminal result")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestWorkerAppliesBootstrapEnvDelta(t *testing.T) {
	result := dispatchTaskEnv(t, Task{
		Command: []string{"sh", "-c", `printf '%s' "$BOOTSTRAP_HOME:$BOOTSTRAP_PATH"`},
		Env:     []string{"BOOTSTRAP_HOME=/opt/env", "BOOTSTRAP_PATH=/opt/bin"},
	})
	if result.Status != ResultCompleted || result.ExitCode != 0 {
		t.Fatalf("result = %+v, want completed", result)
	}
	if result.StdoutTail != "/opt/env:/opt/bin" {
		t.Fatalf("child environment = %q, want the merged delta applied", result.StdoutTail)
	}
}

func TestWorkerBootstrapEnvProtectedKeysNeverReachTheChild(t *testing.T) {
	// COURIER_MODEL is protected, so the delta cannot override whatever the
	// worker's own environment carries; the same protection covers
	// GIT_CONFIG_COUNT and LD_PRELOAD. IFS is protected too and is likewise
	// reset by the shell itself, so it can never be observed as overridden.
	// COURIER_MODEL is pinned to a pod value so the check exercises
	// protection against a set base and does not depend on the ambient
	// environment; the other keys' expected child values mirror whatever
	// the worker inherits, the same way the shell's ${VAR-unset} does.
	t.Setenv("COURIER_MODEL", "pod-value")
	shellOrUnset := func(key string) string {
		if _, ok := os.LookupEnv(key); !ok {
			return "unset"
		}
		return os.Getenv(key)
	}
	result := dispatchTaskEnv(t, Task{
		Command: []string{"sh", "-c", `printf '%s' "count=${GIT_CONFIG_COUNT-unset} model=${COURIER_MODEL-unset} preload=${LD_PRELOAD-unset}"`},
		Env: []string{
			"GIT_CONFIG_COUNT=99",
			"COURIER_MODEL=attacker",
			"LD_PRELOAD=/attacker.so",
			"IFS=attacker",
			"BOOTSTRAP_SAFE=yes",
		},
	})
	if result.Status != ResultCompleted || result.ExitCode != 0 {
		t.Fatalf("result = %+v, want completed", result)
	}
	want := "count=" + shellOrUnset("GIT_CONFIG_COUNT") + " model=pod-value preload=" + shellOrUnset("LD_PRELOAD")
	if result.StdoutTail != want {
		t.Fatalf("child environment = %q, want %q (protected keys not overridden)", result.StdoutTail, want)
	}
	if !devenv.ProtectedKeys("COURIER_MODEL") {
		t.Fatal("test precondition lost: COURIER_MODEL must stay protected")
	}
}

func TestWorkerWithoutTaskEnvKeepsInheritedEnvironment(t *testing.T) {
	t.Setenv("COURIER_UNTOUCHED_MARKER", "pod-value")
	result := dispatchTaskEnv(t, Task{
		Command: []string{"sh", "-c", `printf '%s' "$COURIER_UNTOUCHED_MARKER"`},
	})
	if result.Status != ResultCompleted || result.ExitCode != 0 {
		t.Fatalf("result = %+v, want completed", result)
	}
	if result.StdoutTail != "pod-value" {
		t.Fatalf("child environment = %q, want the pod environment inherited verbatim", result.StdoutTail)
	}
}

func TestWorkerOversizedTaskEnvFailsClosed(t *testing.T) {
	// A record just under the bound alone stays dispatchable; the whole
	// delta together crosses the 64 KiB bound and the task is rejected
	// before execution.
	result := dispatchTaskEnv(t, Task{
		Command: []string{"sh", "-c", `echo should never run`},
		Env: []string{
			"BIG_A=" + strings.Repeat("a", 40<<10),
			"BIG_B=" + strings.Repeat("b", 40<<10),
		},
	})
	if result.Status != ResultFailed || result.ExitCode != -1 {
		t.Fatalf("result = %+v, want a failed closed rejection", result)
	}
	if !strings.Contains(result.StderrTail, "64 KiB") {
		t.Fatalf("rejection reason = %q, want the fixed dispatch-bound reason", result.StderrTail)
	}
}
