package main

import (
	"strings"
	"testing"

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
