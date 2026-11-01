package main

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/topology"
)

func TestWorkerFailsClosedWithoutIdentity(t *testing.T) {
	if err := runArgs([]string{"--addr=0"}); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("runArgs error = %v, want missing identity", err)
	}
}

func TestWorkerFailsClosedWithBadKey(t *testing.T) {
	t.Setenv(topology.EnvRunUID, "run")
	t.Setenv(topology.EnvControlPodUID, "control")
	t.Setenv(topology.EnvWorkerPodUID, "worker")
	t.Setenv(topology.EnvWorkerPublicKey, "not-base64!!")
	if err := runArgs([]string{"--addr=0"}); err == nil {
		t.Fatal("an undecodable verification key must fail startup")
	}
}

// TestNewHTTPServerBoundsEveryConnection pins the review contract that the
// worker's listener reclaims stalled and idle connections instead of holding a
// goroutine and file descriptor for the pod's lifetime: every inbound timeout
// is finite, and the read bound sits far above a healthy in-cluster 65 MiB
// snapshot upload, matching the broker and evidence intake's bounds for the
// same bounded-bundle workload.
func TestNewHTTPServerBoundsEveryConnection(t *testing.T) {
	srv := newHTTPServer(http.NotFoundHandler(), "0")
	if srv.Handler == nil {
		t.Fatal("the server must be wired to a handler")
	}
	if srv.Addr != ":0" {
		t.Errorf("Addr = %q, want %q", srv.Addr, ":0")
	}
	if srv.ReadHeaderTimeout <= 0 {
		t.Errorf("ReadHeaderTimeout = %s, want a finite bound", srv.ReadHeaderTimeout)
	}
	if srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Errorf("inbound read, write, and idle budgets must all be finite: %+v", srv)
	}
	if srv.ReadTimeout < 60*time.Second {
		t.Errorf("ReadTimeout = %s, want at least the intake's 60s so a healthy 65 MiB snapshot upload completes", srv.ReadTimeout)
	}
}
