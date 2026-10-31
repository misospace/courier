package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// executorHelperProcessEnv names the mode this test binary runs the executor
// in. The subprocess scenarios need a real process to signal, and the real
// process is this binary re-entered as the executor: the package's own main()
// installs the SIGTERM NotifyContext, arms the adopted-child reaper, and runs
// the same run() flow the shipped binary runs, so the scenario proves the same
// behavior against a real process without building a second copy of the
// executor inside the test.
//
// The askpass helper stays the same binary: installAskpass points GIT_ASKPASS
// at os.Executable(), which in the helper is this test binary, and the askpass
// child inherits this mode, so main() answers it the way it answers the
// shipped binary's askpass child (COURIER_ASKPASS=1).
//
// The dispatch lives in TestMain, ahead of the COURIER_* scrub, because
// TestMain unsets every COURIER_* variable so no live pod's configuration
// leaks into a run — the helper needs its own COURIER_* configuration intact.
const executorHelperProcessEnv = "COURIER_EXECUTOR_HELPER_PROCESS"

// executorHelperPath returns the path of the executable that stands in for
// the executor binary in the subprocess scenarios: this test binary.
func executorHelperPath(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the test binary to run as the executor: %v", err)
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	return path
}

// waitProcessGoneByPolling waits until pid is no longer a running process or
// the bound is reached, and reports whether it is gone. The bound is a
// failure ceiling, never the expected wait. A process this test does not own
// has no portable event to await, so the poll is tight.
func waitProcessGoneByPolling(t *testing.T, pid int, bound time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		if !processRunning(t, pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}
