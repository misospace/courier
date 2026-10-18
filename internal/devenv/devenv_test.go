package devenv

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

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
		{"absent head", Files{Config: configA, HasConfig: true, ConfigRegular: true}, Files{}, StateAbsent},
		{
			"identical",
			Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true},
			Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true},
			StateApproved,
		},
		{"config differs", Files{Config: configA, HasConfig: true, ConfigRegular: true}, Files{Config: configB, HasConfig: true, ConfigRegular: true}, StateModified},
		{"lock present only at head", Files{Config: configA, HasConfig: true, ConfigRegular: true}, Files{Config: configA, HasConfig: true, Lock: lockB, HasLock: true, ConfigRegular: true, LockRegular: true}, StateModified},
		{"lock present only at base", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true}, Files{Config: configA, HasConfig: true, ConfigRegular: true}, StateModified},
		{"lock bytes differ", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true}, Files{Config: configA, HasConfig: true, Lock: lockB, HasLock: true, ConfigRegular: true, LockRegular: true}, StateModified},
		{"absent base", Files{}, Files{Config: configA, HasConfig: true, ConfigRegular: true}, StateModified},
		{"config symlink at base", Files{Config: configA, HasConfig: true, ConfigRegular: false}, Files{Config: configA, HasConfig: true, ConfigRegular: true}, StateModified},
		{"config symlink at head", Files{Config: configA, HasConfig: true, ConfigRegular: true}, Files{Config: configA, HasConfig: true, ConfigRegular: false}, StateModified},
		{"lock symlink at base", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: false}, Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true}, StateModified},
		{"lock symlink at head", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true}, Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: false}, StateModified},
		{
			"regularity unprovable",
			Files{Config: configA, HasConfig: true},
			Files{Config: configA, HasConfig: true},
			StateModified,
		},
		// The contract is the config PLUS a committed lockfile (#414). A
		// config whose lock is absent at both revisions is a half-present
		// contract, not a repository without one: approving it would run an
		// unpinned bootstrap, so it fails closed exactly like a modified one.
		{
			"identical without a lock",
			Files{Config: configA, HasConfig: true, ConfigRegular: true},
			Files{Config: configA, HasConfig: true, ConfigRegular: true},
			StateModified,
		},
		{
			"lock unprovable at both revisions",
			Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true},
			Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true},
			StateModified,
		},
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

// fakeEntry is one path at one revision in the fake read's fixture: data,
// regularity, and presence, or err for a real read failure.
type fakeEntry struct {
	data    []byte
	regular bool
	present bool
	err     error
}

// fakeRead builds a BlobRead over a revision→path→entry fixture. A revision
// or path missing from the fixture is a read error, never absence: absence
// must be stated explicitly with present=false, mirroring the hardened read
// contract.
func fakeRead(entries map[string]map[string]fakeEntry) BlobRead {
	return func(_ context.Context, revision, path string) ([]byte, bool, bool, error) {
		paths, ok := entries[revision]
		if !ok {
			return nil, false, false, errors.New("unreadable revision")
		}
		entry, ok := paths[path]
		if !ok {
			return nil, false, false, errors.New("unreadable path")
		}
		return entry.data, entry.regular, entry.present, entry.err
	}
}

// contractConfig is the fixed plain contract fixture: config bytes with no
// bootstrap-task parsing involved — the comparison is the approval gate,
// and what executes is the base revision's bytes.
func contractConfig() []byte {
	return []byte("[tools]\ngo = '1.27'\n")
}

func TestClassify(t *testing.T) {
	config := contractConfig()
	lock := []byte("lock\n")

	tests := []struct {
		name      string
		entries   map[string]map[string]fakeEntry
		want      State
		wantError bool
	}{
		{
			name: "approved end to end",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
				"head": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
			},
			want: StateApproved,
		},
		{
			name: "absent contract at head",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
				"head": {ConfigPath: {present: false}, LockPath: {present: false}},
			},
			want: StateAbsent,
		},
		{
			name: "config symlink at base",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, present: true}, LockPath: {data: lock, regular: true, present: true}},
				"head": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
			},
			want: StateModified,
		},
		{
			name: "config symlink at head",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
				"head": {ConfigPath: {data: config, present: true}, LockPath: {data: lock, regular: true, present: true}},
			},
			want: StateModified,
		},
		{
			name: "read error on the config",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
				"head": {ConfigPath: {err: errors.New("object read failed")}, LockPath: {data: lock, regular: true, present: true}},
			},
			wantError: true,
		},
		{
			name: "read error on the lock",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
				"head": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {err: errors.New("object read failed")}},
			},
			wantError: true,
		},
		{
			// The unpinned-bootstrap hole, end to end through the read: a
			// lockless repo is not a repository without a contract, and it
			// must not reach StateApproved.
			name: "config committed without a lock",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {present: false}},
				"head": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {present: false}},
			},
			want: StateModified,
		},
	}
	for _, tt := range tests {
		state, err := Classify(context.Background(), fakeRead(tt.entries), "base", "head")
		if tt.wantError {
			if err == nil {
				t.Errorf("%s: Classify error = nil, want an error", tt.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: Classify error = %v", tt.name, err)
			continue
		}
		if state != tt.want {
			t.Errorf("%s: Classify = %q, want %q", tt.name, state, tt.want)
		}
	}
}

func TestParseEnvCapture(t *testing.T) {
	capture := "PATH=/fake/shims\x00BOOT_BASE=changed\x00BOOTSTRAP_NEW=1\x00" +
		"GIT_CONFIG_COUNT=9\x00COURIER_MODEL=evil\x00KEEP=1\x00"
	base := []string{"PATH=/old", "BOOT_BASE=orig", "KEEP=1", "IFSPAIR=not-a-pair"}

	delta, keys, err := ParseEnvCapture(capture, base)
	if err != nil {
		t.Fatalf("ParseEnvCapture: %v", err)
	}
	wantDelta := []string{"PATH=/fake/shims", "BOOT_BASE=changed", "BOOTSTRAP_NEW=1"}
	if !equalStrings(delta, wantDelta) {
		t.Errorf("delta = %q, want %q", delta, wantDelta)
	}
	wantKeys := []string{"BOOTSTRAP_NEW", "BOOT_BASE", "PATH"}
	if !equalStrings(keys, wantKeys) {
		t.Errorf("keys = %q, want %q", keys, wantKeys)
	}

	if _, _, err := ParseEnvCapture("", base); err == nil {
		t.Error("empty capture passed without error")
	}
	if _, _, err := ParseEnvCapture("PATH=/fake/shims", base); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Errorf("truncated capture error = %v, want the truncated-capture failure", err)
	}
	if _, _, err := ParseEnvCapture("PATH=/fake/shims\x00NOTAPAIR\x00", base); !errors.Is(err, errMalformedEnvCapture) {
		t.Errorf("malformed record error = %v, want errMalformedEnvCapture", err)
	}
	if _, _, err := ParseEnvCapture("PATH=/fake/shims\x00COURIER_X=1\x00", base); err != nil {
		t.Errorf("protected-only delta must parse: %v", err)
	}
}

// TestBootstrapAndEnvCaptureScripts pins the executed-bytes-by-construction
// scripts: each materializes the approved base revision from the git object
// store, proves the materialization happened, and runs only inside it —
// never in the head working tree. The only interpolated value is the
// validated, single-quoted base OID.
func TestBootstrapAndEnvCaptureScripts(t *testing.T) {
	const oid = "0123456789abcdef0123456789abcdef01234567"
	bootstrap, err := BootstrapScript(oid)
	if err != nil {
		t.Fatalf("BootstrapScript: %v", err)
	}
	for _, want := range []string{
		"base='" + oid + "'",
		"git archive --format=tar -o \"$archive\" \"$base\"",
		"tar -xf \"$archive\" -C \"$src\"",
		"test -f \"$src/" + ConfigPath + "\"",
		"cd \"$src\"",
		"mise run bootstrap",
		"courier-mise-path=",
	} {
		if !strings.Contains(bootstrap, want) {
			t.Errorf("BootstrapScript = %q, want it to contain %q", bootstrap, want)
		}
	}
	// The trusted diagnostics are emitted exactly once, from the EXIT trap
	// after the bootstrap run, so the terminal emission is last in the
	// captured stdout tail on success and on every failure path.
	if strings.Count(bootstrap, "courier_diag || true") != 1 ||
		!strings.Contains(bootstrap, `trap 'rc=$?; rm -rf "$src" "$archive" || true; courier_diag || true; exit "$rc"' EXIT`) ||
		!strings.HasSuffix(bootstrap, "mise run bootstrap\n") {
		t.Errorf("BootstrapScript = %q, want a single terminal EXIT-trap emission after the bootstrap run", bootstrap)
	}

	capture, err := EnvCaptureScript(oid)
	if err != nil {
		t.Fatalf("EnvCaptureScript: %v", err)
	}
	for _, want := range []string{
		"base='" + oid + "'",
		"git archive --format=tar -o \"$archive\" \"$base\"",
		"tar -xf \"$archive\" -C \"$src\"",
		"test -f \"$src/" + ConfigPath + "\"",
		"cd \"$src\"",
		"mise x -- env -0 > \"$capture\"",
		"test \"$size\" -le " + strconv.Itoa(MaxCaptureBytes),
		"cat \"$capture\"",
	} {
		if !strings.Contains(capture, want) {
			t.Errorf("EnvCaptureScript = %q, want it to contain %q", capture, want)
		}
	}
}

// TestScriptsRejectInvalidBaseOID pins the fail-closed validation: only a
// bare lowercase hex git object ID (40 or 64 characters) may be
// interpolated into a task script — anything else, including quote and
// shell metacharacters, errors before dispatch for both functions.
func TestScriptsRejectInvalidBaseOID(t *testing.T) {
	invalid := []string{
		"",
		"refs/heads/main",
		"HEAD",
		"0123456789abcdef0123456789abcdef0123456",   // 39 chars
		"0123456789abcdef0123456789abcdef012345678", // 41 chars
		strings.Repeat("a", 39) + "G",               // non-hex
		"0123456789ABCDEF0123456789ABCDEF01234567",  // uppercase
		"'; rm -rf / #",
		"$(rm -rf /)",
		"`rm -rf /`",
		"0123456789abcdef0123456789abcdef01234567 ",
		" 0123456789abcdef0123456789abcdef01234567",
		"0123456789abcdef0123456789abcdef0123456\n",
		strings.Repeat("a", 64)[:63] + "g", // 64 chars, non-hex tail
	}
	for _, oid := range invalid {
		if script, err := BootstrapScript(oid); err == nil {
			t.Errorf("BootstrapScript(%q) = %q, want an error", oid, script)
		}
		if script, err := EnvCaptureScript(oid); err == nil {
			t.Errorf("EnvCaptureScript(%q) = %q, want an error", oid, script)
		}
	}
	for _, oid := range []string{
		"0123456789abcdef0123456789abcdef01234567",
		strings.Repeat("a", 64),
	} {
		if _, err := BootstrapScript(oid); err != nil {
			t.Errorf("BootstrapScript(%q) error = %v, want nil", oid, err)
		}
		if _, err := EnvCaptureScript(oid); err != nil {
			t.Errorf("EnvCaptureScript(%q) error = %v, want nil", oid, err)
		}
	}
}

func TestParseBootstrapDiagnostics(t *testing.T) {
	stdout := "mise 2024.x installed\ncourier-mise-path=/usr/bin/mise\n" +
		"courier-mise-version=2024.10.0\ncourier-mise-data-dir=/data/mise\n" +
		"courier-unknown=noise\nnot-a-diagnostic\n"
	want := map[string]string{
		"courier-mise-path":     "/usr/bin/mise",
		"courier-mise-version":  "2024.10.0",
		"courier-mise-data-dir": "/data/mise",
	}
	if got := ParseBootstrapDiagnostics(stdout); !reflect.DeepEqual(got, want) {
		t.Errorf("ParseBootstrapDiagnostics = %v, want %v", got, want)
	}

	if got := ParseBootstrapDiagnostics("no diagnostics here\n"); len(got) != 0 {
		t.Errorf("ParseBootstrapDiagnostics = %v, want empty", got)
	}
	if got := ParseBootstrapDiagnostics(""); len(got) != 0 {
		t.Errorf("ParseBootstrapDiagnostics(\"\") = %v, want empty", got)
	}

	// An overlong value is dropped, never truncated; exactly at the bound it
	// is kept.
	overlong := "courier-mise-version=" + strings.Repeat("x", 301) + "\n"
	if got := ParseBootstrapDiagnostics(overlong); len(got) != 0 {
		t.Errorf("overlong value parsed = %v, want dropped", got)
	}
	atBound := "courier-mise-version=" + strings.Repeat("x", 300) + "\n"
	if got := ParseBootstrapDiagnostics(atBound); len(got) != 1 {
		t.Errorf("value at the bound parsed = %v, want kept", got)
	}

	// Control characters and non-ASCII are dropped: receipt text only.
	if got := ParseBootstrapDiagnostics("courier-mise-path=/usr/bin/m\x01ise\n"); len(got) != 0 {
		t.Errorf("control-character value parsed = %v, want dropped", got)
	}
	if got := ParseBootstrapDiagnostics("courier-mise-path=/usr/bin/mis\u00e9\n"); len(got) != 0 {
		t.Errorf("non-ASCII value parsed = %v, want dropped", got)
	}

	// Last valid occurrence wins: the script emits its trusted diagnostics
	// once, from its EXIT trap after everything else it could ever run, so
	// a spoofed repo-controlled line cannot override the trusted terminal
	// emission.
	spoofed := "courier-mise-path=/evil\n" +
		"courier-mise-version=0.0.0-evil\n" +
		"courier-mise-path=/usr/bin/mise\n" +
		"courier-mise-version=2024.10.0\n"
	wantTrusted := map[string]string{
		"courier-mise-path":    "/usr/bin/mise",
		"courier-mise-version": "2024.10.0",
	}
	if got := ParseBootstrapDiagnostics(spoofed); !reflect.DeepEqual(got, wantTrusted) {
		t.Errorf("spoofed diagnostics = %v, want the trusted terminal values %v", got, wantTrusted)
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

// scriptToolsRequired reports whether every external tool the scripts rely
// on is present in this environment; a missing tool skips the real
// execution tests rather than faking their outcome.
func scriptToolsRequired() error {
	for _, bin := range []string{"sh", "git", "tar", "mktemp"} {
		if _, err := exec.LookPath(bin); err != nil {
			return err
		}
	}
	return nil
}

// baseRepoWithContract builds a real git repository whose base commit
// carries the contract file, and returns the repo directory and the base
// commit's OID — the materialization the scripts must perform.
func baseRepoWithContract(t *testing.T) (string, string) {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(filepath.Join(repo, ".mise"), 0o755); err != nil {
		t.Fatal(err)
	}
	config := "[tasks.bootstrap]\nrun = 'echo bootstrap-ran > marker.txt'\n"
	if err := os.WriteFile(filepath.Join(repo, ".mise", "config.toml"), []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=courier", "GIT_AUTHOR_EMAIL=courier@invalid",
			"GIT_COMMITTER_NAME=courier", "GIT_COMMITTER_EMAIL=courier@invalid",
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
			"HOME="+repo,
		)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, stderr.String())
		}
	}
	run("init", "-q", repo)
	run("add", ".mise/config.toml")
	run("commit", "-q", "-m", "base: contract")
	oid, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return repo, strings.TrimSpace(string(oid))
}

// miseStandIn writes the scripted mise stand-in (the cmd/courier-executor
// fixture idiom): it answers --version, runs the bootstrap task as a
// marker print, and emits NUL-separated environment records — oversize
// when asked — so the scripts are exercised against a real shell without a
// real mise.
func miseStandIn(t *testing.T, dir, envPayload string) string {
	t.Helper()
	bin := filepath.Join(dir, "mise-bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  --version) printf 'mise 2024.10.29 (stand-in)\\n' ;;\n" +
		"  run) printf 'bootstrap-ran\\n' ;;\n" +
		"  x) printf 'MISE_STANDIN=active\\000MISE_STANDIN_TWO=2\\000" + envPayload + "' ;;\n" +
		"  *) printf 'unexpected mise invocation: %s\\n' \"$*\" >&2; exit 64 ;;\n" +
		"esac\n"
	path := filepath.Join(bin, "mise")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

// runScript executes one generated task script the way the worker would:
// `sh -c` with the repo as the working directory and the stand-in's bin
// directory on PATH, with TMPDIR pointed at a scratch dir the test
// controls so the EXIT trap's cleanup is observable.
func runScript(t *testing.T, script, repo, binDir, scratch string) (string, error) {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TMPDIR="+scratch,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("script stderr: %s", stderr.String())
			return stdout.String(), exitErr
		}
		t.Fatalf("sh -c: %v", err)
	}
	return stdout.String(), nil
}

// TestScriptsExecuteForReal drives both generated task scripts against a
// real git repository, a real shell, and a scripted mise stand-in: the
// bootstrap script materializes the base, runs the contract, emits its
// trusted diagnostics last, and cleans the materialization through its
// EXIT trap; the capture script emits the NUL record stream; an oversized
// capture and an absent base object both fail closed.
func TestScriptsExecuteForReal(t *testing.T) {
	if err := scriptToolsRequired(); err != nil {
		t.Skipf("script execution tools unavailable: %v", err)
	}
	repo, baseOID := baseRepoWithContract(t)

	t.Run("bootstrap materializes, runs, and cleans up", func(t *testing.T) {
		binDir := miseStandIn(t, t.TempDir(), "")
		scratch := filepath.Join(t.TempDir(), "tmp")
		if err := os.MkdirAll(scratch, 0o755); err != nil {
			t.Fatal(err)
		}
		script, err := BootstrapScript(baseOID)
		if err != nil {
			t.Fatalf("BootstrapScript: %v", err)
		}
		stdout, runErr := runScript(t, script, repo, binDir, scratch)
		if runErr != nil {
			t.Fatalf("bootstrap script failed: %v", runErr)
		}
		for _, key := range []string{"courier-mise-path=", "courier-mise-version=", "courier-mise-data-dir="} {
			if !strings.Contains(stdout, key) {
				t.Errorf("bootstrap stdout missing diagnostic %q:\n%s", key, stdout)
			}
		}
		if !strings.Contains(stdout, "bootstrap-ran") {
			t.Errorf("bootstrap stdout missing the contract's own marker:\n%s", stdout)
		}
		// The trusted terminal emission is last: whatever the run printed,
		// the diag lines follow it in the captured tail.
		if idx := strings.LastIndex(stdout, "courier-mise-path="); idx != -1 && idx < strings.Index(stdout, "bootstrap-ran") {
			t.Errorf("trusted diagnostics are not terminal in the captured stdout:\n%s", stdout)
		}
		// The trap's rm -rf removed the materialized directory: the test's
		// own TMPDIR is empty after the run.
		entries, err := os.ReadDir(scratch)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("materialization survived the EXIT trap: %v", entries)
		}
	})

	t.Run("env capture emits the NUL record stream", func(t *testing.T) {
		binDir := miseStandIn(t, t.TempDir(), "")
		script, err := EnvCaptureScript(baseOID)
		if err != nil {
			t.Fatalf("EnvCaptureScript: %v", err)
		}
		stdout, runErr := runScript(t, script, repo, binDir, t.TempDir())
		if runErr != nil {
			t.Fatalf("capture script failed: %v", runErr)
		}
		want := "MISE_STANDIN=active\x00MISE_STANDIN_TWO=2\x00"
		if stdout != want {
			t.Errorf("capture stdout = %q, want the NUL record stream %q", stdout, want)
		}
	})

	t.Run("oversized capture fails closed", func(t *testing.T) {
		big := "BIG=" + strings.Repeat("x", 70000) + "\\000"
		binDir := miseStandIn(t, t.TempDir(), big)
		script, err := EnvCaptureScript(baseOID)
		if err != nil {
			t.Fatalf("EnvCaptureScript: %v", err)
		}
		_, runErr := runScript(t, script, repo, binDir, t.TempDir())
		if runErr == nil {
			t.Fatal("an oversized capture must fail the task, not be truncated or passed through")
		}
	})

	t.Run("absent base object fails closed", func(t *testing.T) {
		binDir := miseStandIn(t, t.TempDir(), "")
		const absentOID = "0123456789abcdef0123456789abcdef01234567"
		script, err := BootstrapScript(absentOID)
		if err != nil {
			t.Fatalf("BootstrapScript: %v", err)
		}
		stdout, runErr := runScript(t, script, repo, binDir, t.TempDir())
		if runErr == nil {
			t.Fatal("a base whose objects are absent must fail the materialization guard")
		}
		// Even this failure path carries the trusted diagnostics: the EXIT
		// trap is terminal in every outcome.
		for _, key := range []string{"courier-mise-path=", "courier-mise-version=", "courier-mise-data-dir="} {
			if !strings.Contains(stdout, key) {
				t.Errorf("failure-path stdout missing diagnostic %q:\n%s", key, stdout)
			}
		}
	})

	t.Run("archive failure after partial output fails closed", func(t *testing.T) {
		binDir := miseStandIn(t, t.TempDir(), "")
		// A git stand-in for a corrupted object store: it writes a valid
		// tar carrying the contract file to the -o path (the partial
		// archive a mid-stream object failure emits) and then exits
		// nonzero. A pipeline would hand tar the partial archive and
		// discard this exit status; the guarded two-step must fail the
		// task before extraction, so mise never runs against the partial
		// tree even though the contract file is present in it.
		gitStub := "#!/bin/sh\n" +
			"out=\"\"\n" +
			"prev=\"\"\n" +
			"for a in \"$@\"; do\n" +
			"  if [ \"$prev\" = \"-o\" ]; then out=\"$a\"; fi\n" +
			"  prev=\"$a\"\n" +
			"done\n" +
			"stage=\"$(mktemp -d)\"\n" +
			"mkdir -p \"$stage/.mise\"\n" +
			"printf '[tasks.bootstrap]\\n' > \"$stage/.mise/config.toml\"\n" +
			// Emit the archive on whichever channel the caller asked
			// for: the -o file when given, stdout otherwise. The
			// stdout branch reproduces the pre-fix pipeline's stream,
			// so swapping in the pre-fix script demonstrates this
			// exact failure against the same stand-in.
			"if [ -n \"$out\" ]; then\n" +
			"  tar -cf \"$out\" -C \"$stage\" .mise\n" +
			"else\n" +
			"  tar -cf - -C \"$stage\" .mise\n" +
			"fi\n" +
			"rm -rf \"$stage\"\n" +
			"exit 1\n"
		if err := os.WriteFile(filepath.Join(binDir, "git"), []byte(gitStub), 0o755); err != nil {
			t.Fatal(err)
		}
		scratch := filepath.Join(t.TempDir(), "tmp")
		if err := os.MkdirAll(scratch, 0o755); err != nil {
			t.Fatal(err)
		}
		script, err := BootstrapScript(baseOID)
		if err != nil {
			t.Fatalf("BootstrapScript: %v", err)
		}
		stdout, runErr := runScript(t, script, repo, binDir, scratch)
		if runErr == nil {
			t.Fatal("a git archive that fails after emitting a partial archive must fail the task")
		}
		if strings.Contains(stdout, "bootstrap-ran") {
			t.Errorf("mise ran against the partial materialization:\n%s", stdout)
		}
		entries, err := os.ReadDir(scratch)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("materialization or archive survived the EXIT trap: %v", entries)
		}
	})
}
