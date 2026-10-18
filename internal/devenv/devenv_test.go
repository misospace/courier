package devenv

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeMise writes a `mise` stand-in into dir and returns its path. The
// runtime under test runs it as plain argv, so the fake branches on its first
// argument: `run bootstrap` sleeps or fails on instruction from the test's
// base environment, and `x -- env -0` prints NUL-separated records from the
// FAKE_ENV_CASE control variable.
func fakeMise(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "mise")
	script := "#!/bin/sh\n" +
		"PATH=/usr/bin:/bin\n" +
		"case \"$1\" in\n" +
		"run)\n" +
		"  if [ -n \"$FAKE_SLEEP\" ]; then\n" +
		"    sleep \"$FAKE_SLEEP\" &\n" +
		"    echo $! > \"$FAKE_PIDFILE\"\n" +
		"    wait $!\n" +
		"    touch \"$FAKE_DONEFILE\"\n" +
		"  fi\n" +
		"  if [ -n \"$FAKE_FAIL\" ]; then exit \"$FAKE_FAIL\"; fi\n" +
		"  printf 'bootstrapped\\n'\n" +
		"  ;;\n" +
		"x)\n" +
		"  case \"${FAKE_ENV_CASE:-good}\" in\n" +
		"  good)\n" +
		"    printf 'PATH=%s\\0' \"/fake/shims:$PATH\"\n" +
		"    printf 'BOOT_BASE=changed\\0'\n" +
		"    printf 'BOOTSTRAP_NEW=1\\0'\n" +
		"    printf 'GIT_CONFIG_COUNT=9\\0'\n" +
		"    printf 'COURIER_MODEL=evil\\0'\n" +
		"    ;;\n" +
		"  malformed)\n" +
		"    printf 'PATH=%s\\0' \"/fake/shims:$PATH\"\n" +
		"    printf 'NOTAPAIR\\0'\n" +
		"    ;;\n" +
		"  flood)\n" +
		"    dd if=/dev/zero bs=65536 count=24 2>/dev/null\n" +
		"    ;;\n" +
		"  empty)\n" +
		"    ;;\n" +
		"  cut)\n" +
		"    printf 'PATH=/fake/shims:/usr/bin'\n" +
		"    ;;\n" +
		"  esac\n" +
		"  ;;\n" +
		"esac\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mise: %v", err)
	}
	return path
}

// baseEnv builds the base environment one Run test passes in Options.Env: it
// carries both the variables the delta is compared against and the FAKE_*
// controls the fake mise script reads.
func baseEnv(extra ...string) []string {
	env := []string{"PATH=/base/bin", "BOOT_BASE=orig", "FAKE_ENV_CASE=good"}
	return append(env, extra...)
}

func TestRunMissingMise(t *testing.T) {
	result, err := Run(context.Background(), "", Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("empty miseBinary must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" {
		t.Errorf("Status = %q, want failed", result.Status)
	}
	if result.Reason != "bootstrap contract requires mise; no mise executable provided" {
		t.Errorf("Reason = %q", result.Reason)
	}
	if result.ExitCode != -1 {
		t.Errorf("ExitCode = %d, want -1", result.ExitCode)
	}
}

func TestRunBootstrapExitCode(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	result, err := Run(context.Background(), mise, Options{
		Dir: dir,
		Env: baseEnv("FAKE_FAIL=7"),
	})
	if err != nil {
		t.Fatalf("nonzero exit must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" {
		t.Errorf("Status = %q, want failed", result.Status)
	}
	if result.ExitCode != 7 {
		t.Errorf("ExitCode = %d, want 7", result.ExitCode)
	}
	if result.Reason != "bootstrap task failed with exit 7" {
		t.Errorf("Reason = %q", result.Reason)
	}
}

func TestRunStartError(t *testing.T) {
	result, err := Run(context.Background(), filepath.Join(t.TempDir(), "missing-mise"), Options{
		Dir: t.TempDir(),
		Env: baseEnv(),
	})
	if err != nil {
		t.Fatalf("start failure must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" || result.ExitCode != -1 {
		t.Errorf("Status = %q, ExitCode = %d, want failed/-1", result.Status, result.ExitCode)
	}
	if !strings.Contains(result.Reason, "bootstrap task failed to start") {
		t.Errorf("Reason = %q, want it to name the start failure", result.Reason)
	}
}

func TestRunTimeoutKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	donePath := filepath.Join(dir, "done")
	pidPath := filepath.Join(dir, "pid")

	result, err := Run(context.Background(), mise, Options{
		Dir: dir,
		Env: baseEnv("FAKE_SLEEP=2", "FAKE_PIDFILE="+pidPath, "FAKE_DONEFILE="+donePath),
		// A bound the sleeping bootstrap cannot respect.
		Bound: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("bound expiry must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" {
		t.Errorf("Status = %q, want failed", result.Status)
	}
	if !strings.Contains(result.Reason, "bound") {
		t.Errorf("Reason = %q, want it to name the bound", result.Reason)
	}
	if result.ExitCode != -1 {
		t.Errorf("ExitCode = %d, want -1 for a killed child", result.ExitCode)
	}

	// The sleeping child lives in the bootstrap's process group. A kill that
	// reached only the direct child would let the background sleep finish and
	// touch the done file; its absence over the sleep's full duration is the
	// proof the whole group was killed.
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(donePath); err == nil {
			t.Fatal("bootstrap descendant outlived its bound and finished its work")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := os.Stat(donePath); err == nil {
		t.Fatal("bootstrap descendant outlived its bound and finished its work")
	}
}

func TestRunCancellation(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)
	defer cancel()

	result, err := Run(ctx, mise, Options{
		Dir: dir,
		Env: baseEnv("FAKE_SLEEP=5"),
	})
	if err != nil {
		t.Fatalf("cancellation must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" {
		t.Errorf("Status = %q, want failed", result.Status)
	}
	if !strings.Contains(result.Reason, "cancel") {
		t.Errorf("Reason = %q, want it to name the cancellation", result.Reason)
	}
}

func TestRunAppliesEnvironment(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	var stdout bytes.Buffer
	env := baseEnv()

	result, err := Run(context.Background(), mise, Options{Dir: dir, Env: env, Stdout: &stdout})
	if err != nil {
		t.Fatalf("successful run returned error: %v", err)
	}
	if result.Status != "applied" {
		t.Fatalf("Status = %q, Reason = %q, want applied", result.Status, result.Reason)
	}
	if result.Reason != "" {
		t.Errorf("Reason = %q, want empty on applied", result.Reason)
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", result.ExitCode)
	}
	if !strings.Contains(stdout.String(), "bootstrapped") {
		t.Errorf("bootstrap output was not streamed: %q", stdout.String())
	}

	var newPath string
	hasBase, hasNew, hasProtected := false, false, 0
	for _, kv := range result.EnvDelta {
		switch {
		case strings.HasPrefix(kv, "PATH="):
			if !strings.HasPrefix(strings.TrimPrefix(kv, "PATH="), "/fake/shims:") {
				t.Errorf("PATH delta = %q, want deterministic shims prefix", kv)
			}
			newPath = kv
		case kv == "BOOT_BASE=changed":
			hasBase = true
		case kv == "BOOTSTRAP_NEW=1":
			hasNew = true
		}
		if strings.HasPrefix(kv, "GIT_") || strings.HasPrefix(kv, "COURIER_") {
			hasProtected++
		}
	}
	if newPath == "" || !hasBase || !hasNew {
		t.Errorf("EnvDelta = %q, want PATH replaced, base var overridden, new var added", result.EnvDelta)
	}
	if hasProtected != 0 {
		t.Errorf("EnvDelta leaked protected keys: %q", result.EnvDelta)
	}
	if want := []string{"BOOTSTRAP_NEW", "BOOT_BASE", "PATH"}; !equalStrings(result.EnvKeys, want) {
		t.Errorf("EnvKeys = %q, want %q", result.EnvKeys, want)
	}

	merged := MergeEnv(env, result.EnvDelta)
	if merged[0] != newPath {
		t.Errorf("merged[0] = %q, want PATH replaced in its base position", merged[0])
	}
	if merged[1] != "BOOT_BASE=changed" {
		t.Errorf("merged[1] = %q, want base var overridden in place", merged[1])
	}
	if merged[len(merged)-1] != "BOOTSTRAP_NEW=1" {
		t.Errorf("merged[last] = %q, want new var appended", merged[len(merged)-1])
	}
	if len(merged) != len(env)+1 {
		t.Errorf("merged length = %d, want %d", len(merged), len(env)+1)
	}
}

func TestRunMalformedEnvCapture(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	result, err := Run(context.Background(), mise, Options{
		Dir: dir,
		Env: baseEnv("FAKE_ENV_CASE=malformed"),
	})
	if err != nil {
		t.Fatalf("malformed capture must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" || result.Reason != "malformed environment capture" {
		t.Errorf("Status = %q, Reason = %q, want failed/malformed environment capture", result.Status, result.Reason)
	}
}

func TestRunCaptureExceedsCap(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	result, err := Run(context.Background(), mise, Options{
		Dir: dir,
		Env: baseEnv("FAKE_ENV_CASE=flood"),
	})
	if err != nil {
		t.Fatalf("oversized capture must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" || result.Reason != "environment capture exceeded 1 MiB" {
		t.Errorf("Status = %q, Reason = %q, want failed/oversized capture", result.Status, result.Reason)
	}
}

func TestRunEmptyEnvCaptureFails(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	result, err := Run(context.Background(), mise, Options{
		Dir: dir,
		Env: baseEnv("FAKE_ENV_CASE=empty"),
	})
	if err != nil {
		t.Fatalf("empty capture must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" || result.Reason != "empty environment capture" {
		t.Errorf("Status = %q, Reason = %q, want failed/empty environment capture", result.Status, result.Reason)
	}
}

func TestRunTruncatedEnvCaptureFails(t *testing.T) {
	dir := t.TempDir()
	mise := fakeMise(t, dir)
	result, err := Run(context.Background(), mise, Options{
		Dir: dir,
		Env: baseEnv("FAKE_ENV_CASE=cut"),
	})
	if err != nil {
		t.Fatalf("truncated capture must be a structured failure, got error: %v", err)
	}
	if result.Status != "failed" || result.Reason != "truncated environment capture" {
		t.Errorf("Status = %q, Reason = %q, want failed/truncated environment capture", result.Status, result.Reason)
	}
}

func TestCompare(t *testing.T) {
	configA := []byte("[tools]\nnode = '22'\n")
	configB := []byte("[tools]\nnode = '24'\n")
	lockA := []byte("lock-a")
	lockB := []byte("lock-b")

	tests := []struct {
		name string
		base Files
		head Files
		want State
	}{
		{"absent head", Files{Config: configA, HasConfig: true}, Files{}, StateAbsent},
		{"identical", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true}, Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true}, StateApproved},
		{"config differs", Files{Config: configA, HasConfig: true}, Files{Config: configB, HasConfig: true}, StateModified},
		{"lock present only at head", Files{Config: configA, HasConfig: true}, Files{Config: configA, HasConfig: true, Lock: lockB, HasLock: true}, StateModified},
		{"lock present only at base", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true}, Files{Config: configA, HasConfig: true}, StateModified},
		{"absent base", Files{}, Files{Config: configA, HasConfig: true}, StateModified},
	}
	for _, tt := range tests {
		if got := Compare(tt.base, tt.head); got != tt.want {
			t.Errorf("%s: Compare = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestMergeEnv(t *testing.T) {
	base := []string{"A=1", "B=2", "C=3"}
	snapshot := append([]string(nil), base...)
	delta := []string{"B=9", "D=4", "B=8", "GIT_IDENTITY=x", "COURIER_MODEL=y", "LD_PRELOAD=z"}

	merged := MergeEnv(base, delta)
	want := []string{"A=1", "B=8", "C=3", "D=4"}
	if !equalStrings(merged, want) {
		t.Errorf("MergeEnv = %q, want %q", merged, want)
	}
	if !equalStrings(base, snapshot) {
		t.Errorf("base mutated: %q", base)
	}

	// Delta keys absent from base keep delta order; protected keys vanish
	// rather than failing the merge.
	merged = MergeEnv([]string{"KEEP=1"}, delta)
	want = []string{"KEEP=1", "B=8", "D=4"}
	if !equalStrings(merged, want) {
		t.Errorf("MergeEnv = %q, want %q", merged, want)
	}

	if got := MergeEnv(nil, nil); len(got) != 0 {
		t.Errorf("MergeEnv(nil, nil) = %q, want empty", got)
	}

	// A duplicated base name resolves by its last occurrence: exec.Cmd uses
	// the last entry for a repeated name, so a stale duplicate must never
	// override the delta's replacement.
	merged = MergeEnv([]string{"PATH=/stale", "KEEP=1", "PATH=/dup"}, []string{"PATH=/mise"})
	want = []string{"KEEP=1", "PATH=/mise"}
	if !equalStrings(merged, want) {
		t.Errorf("MergeEnv with duplicate base name = %q, want %q", merged, want)
	}
}

func TestScriptAndParseScriptOutput(t *testing.T) {
	// The script is pinned exactly: its security property is that it is
	// fixed constants only — never interpolated with brief- or
	// model-supplied values — and a containment check cannot prove that.
	want := "set -eu\n" +
		"mise run bootstrap\n" +
		"echo '" + EnvMarker + "'\n" +
		"mise x -- env -0\n"
	if got := Script(); got != want {
		t.Errorf("Script() = %q, want %q", got, want)
	}
	script := Script()
	for _, fragment := range []string{"set -eu", "mise run bootstrap", "mise x -- env -0", "echo '" + EnvMarker + "'"} {
		if !strings.Contains(script, fragment) {
			t.Errorf("Script() = %q, missing %q", script, fragment)
		}
	}

	report, delta, keys, err := ParseScriptOutput("boot report\nsecond line\n"+EnvMarker+"\nPATH=/new\x00NEW=1\x00", []string{"PATH=/old", "KEEP=1"})
	if err != nil {
		t.Fatalf("ParseScriptOutput: %v", err)
	}
	if report != "boot report\nsecond line" {
		t.Errorf("report = %q", report)
	}
	if !equalStrings(delta, []string{"PATH=/new", "NEW=1"}) {
		t.Errorf("delta = %q", delta)
	}
	if !equalStrings(keys, []string{"NEW", "PATH"}) {
		t.Errorf("keys = %q", keys)
	}

	// The LAST marker splits, so an earlier marker belongs to the report.
	report, _, _, err = ParseScriptOutput("first\n"+EnvMarker+"\nsecond\n"+EnvMarker+"\nNEW=1\x00", nil)
	if err != nil {
		t.Fatalf("ParseScriptOutput: %v", err)
	}
	if report != "first\n"+EnvMarker+"\nsecond" {
		t.Errorf("report = %q, want everything before the last marker", report)
	}

	if _, _, _, err := ParseScriptOutput("no marker here\n", nil); !errors.Is(err, errNoEnvMarker) {
		t.Errorf("missing marker error = %v, want errNoEnvMarker", err)
	}
	if _, _, _, err := ParseScriptOutput(EnvMarker+"\nNOTAPAIR\x00", nil); err == nil {
		t.Error("malformed record passed without error")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
