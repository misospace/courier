//go:build !linux

package main

import (
	"testing"
	"time"
)

// waitProcessExitEvent waits until pid is no longer a running process. There
// is no pidfd outside Linux, so the wait is a tight poll of the process
// state; the bound is a failure ceiling, never the expected wait.
func waitProcessExitEvent(t *testing.T, pid int, bound time.Duration) (bool, string) {
	t.Helper()
	return waitProcessGoneByPolling(t, pid, bound), "process state poll"
}
