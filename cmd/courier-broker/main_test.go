package main

import (
	"strings"
	"testing"
)

func TestRunFailsClosedWithoutOperatorWiring(t *testing.T) {
	if err := runArgs(nil); err == nil || !strings.Contains(err.Error(), "requires address") {
		t.Fatalf("run() error = %v, want missing configuration", err)
	}
}
