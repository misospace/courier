package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
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
	if len(got) > maxTerminationReasonBytes+20 || !strings.HasSuffix(got, "[truncated]") {
		t.Fatalf("boundReason did not bound the reason: %d bytes", len(got))
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
		runUID:      "run",
		controlUID:  "control",
		workerUID:   uid,
		workerURL:   server.URL,
		brokerURL:   "https://broker:8443",
		signingKey:  priv,
		brokerToken: []byte("token"),
		brokerCA:    []byte("ca"),
	}
	for attempt := 0; attempt < 3; attempt++ {
		capability := identity.probeWorker(context.Background())
		if capability.State != harness.StateHealthy {
			t.Fatalf("probe attempt %d = %+v, want healthy (a fixed opID conflicts on the second probe)", attempt, capability)
		}
	}
}
