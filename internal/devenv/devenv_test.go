package devenv

import (
	"context"
	"errors"
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
		{
			"identical without a lock",
			Files{Config: configA, HasConfig: true, ConfigRegular: true},
			Files{Config: configA, HasConfig: true, ConfigRegular: true},
			StateApproved,
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
		"git archive \"$base\" | tar -x -C \"$src\"",
		"test -f \"$src/" + ConfigPath + "\"",
		"cd \"$src\"",
		"mise run bootstrap",
		"courier-mise-path=",
	} {
		if !strings.Contains(bootstrap, want) {
			t.Errorf("BootstrapScript = %q, want it to contain %q", bootstrap, want)
		}
	}
	// The trusted diagnostics are emitted after the bootstrap run too, so
	// the terminal emission is last in the captured stdout tail.
	if strings.Count(bootstrap, "courier_diag\n") != 2 || !strings.HasSuffix(bootstrap, "courier_diag\n") {
		t.Errorf("BootstrapScript = %q, want diagnostics before AND after the bootstrap run", bootstrap)
	}

	capture, err := EnvCaptureScript(oid)
	if err != nil {
		t.Fatalf("EnvCaptureScript: %v", err)
	}
	for _, want := range []string{
		"base='" + oid + "'",
		"git archive \"$base\" | tar -x -C \"$src\"",
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
	// before and after the bootstrap run, so a spoofed repo-controlled line
	// before the run cannot override the trusted terminal emission.
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
