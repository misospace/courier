package main

import (
	"strings"
	"testing"

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
