package devenv

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCompare(t *testing.T) {
	configA := []byte("[tools]\nnode = '22'\n")
	configB := []byte("[tools]\nnode = '24'\n")
	configTask := []byte("[tasks.bootstrap]\nrun = 'hack/bootstrap.sh'\n")
	lockA := []byte("lock-a")
	lockB := []byte("lock-b")
	script := ScriptFile{Data: []byte("echo hi\n"), Regular: true, Present: true}
	scripts := map[string]ScriptFile{"hack/bootstrap.sh": script}

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
		{"absent base", Files{}, Files{Config: configA, HasConfig: true, ConfigRegular: true}, StateModified},
		{"config symlink at base", Files{Config: configA, HasConfig: true, ConfigRegular: false}, Files{Config: configA, HasConfig: true, ConfigRegular: true}, StateModified},
		{"config symlink at head", Files{Config: configA, HasConfig: true, ConfigRegular: true}, Files{Config: configA, HasConfig: true, ConfigRegular: false}, StateModified},
		{"lock symlink at base", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: false}, Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true}, StateModified},
		{"lock symlink at head", Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: true}, Files{Config: configA, HasConfig: true, Lock: lockA, HasLock: true, ConfigRegular: true, LockRegular: false}, StateModified},
		{"regularity unprovable", Files{Config: configA, HasConfig: true}, Files{Config: configA, HasConfig: true}, StateModified},
		{
			"referenced script pinned identical",
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: scripts},
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: scripts},
			StateApproved,
		},
		{
			"referenced script differs at head",
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: scripts},
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: map[string]ScriptFile{
				"hack/bootstrap.sh": {Data: []byte("echo gone\n"), Regular: true, Present: true},
			}},
			StateModified,
		},
		{
			"referenced script absent at head",
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: scripts},
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: map[string]ScriptFile{
				"hack/bootstrap.sh": {Present: false},
			}},
			StateModified,
		},
		{
			"referenced script never read",
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: scripts},
			Files{Config: configTask, HasConfig: true, ConfigRegular: true},
			StateModified,
		},
		{
			"referenced script symlink",
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: map[string]ScriptFile{
				"hack/bootstrap.sh": {Data: []byte("echo hi\n"), Present: true},
			}},
			Files{Config: configTask, HasConfig: true, ConfigRegular: true, Scripts: map[string]ScriptFile{
				"hack/bootstrap.sh": {Data: []byte("echo hi\n"), Present: true},
			}},
			StateModified,
		},
		{
			"identical but unparseable bootstrap task",
			Files{Config: []byte("[tasks.bootstrap]\n"), HasConfig: true, ConfigRegular: true},
			Files{Config: []byte("[tasks.bootstrap]\n"), HasConfig: true, ConfigRegular: true},
			StateModified,
		},
		{
			"approved contract without a bootstrap task",
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

func TestBootstrapScriptRefs(t *testing.T) {
	tests := []struct {
		name   string
		config string
		want   []string
		wantOK bool
	}{
		{
			name:   "happy path",
			config: "[tools]\ngo = '1.27'\n\n[tasks.bootstrap]\nrun = \"hack/bootstrap.sh tools\"\n",
			want:   []string{"hack/bootstrap.sh", "tools"},
			wantOK: true,
		},
		{
			name:   "multiple refs deduplicated in order",
			config: "[tasks.bootstrap]\nrun = 'b.sh a.sh b.sh'\n",
			want:   []string{"b.sh", "a.sh"},
			wantOK: true,
		},
		{
			name:   "flags and absolute paths skipped",
			config: "[tasks.bootstrap]\nrun = 'build --verbose /usr/bin/tool script.sh'\n",
			want:   []string{"build", "script.sh"},
			wantOK: true,
		},
		{
			name:   "comment after the value",
			config: "[tasks.bootstrap]\nrun = 'a.sh' # the task\n",
			want:   []string{"a.sh"},
			wantOK: true,
		},
		{
			name:   "metacharacter token fails closed",
			config: "[tasks.bootstrap]\nrun = 'deploy.sh $HOME'\n",
			wantOK: false,
		},
		{
			name:   "glob token fails closed",
			config: "[tasks.bootstrap]\nrun = 'scripts/*.sh'\n",
			wantOK: false,
		},
		{
			name:   "dotdot segment fails closed",
			config: "[tasks.bootstrap]\nrun = '../secrets.sh'\n",
			wantOK: false,
		},
		{
			name:   "interior dotdot segment fails closed",
			config: "[tasks.bootstrap]\nrun = 'a/../b.sh'\n",
			wantOK: false,
		},
		{
			name:   "backslash escape fails closed",
			config: "[tasks.bootstrap]\nrun = \"echo \\hi.sh\"\n",
			wantOK: false,
		},
		{
			name:   "single-quoted literal accepted",
			config: "[tasks.bootstrap]\nrun = 'hack/bootstrap.sh'\n",
			want:   []string{"hack/bootstrap.sh"},
			wantOK: true,
		},
		{
			name:   "no section and no bootstrap mention",
			config: "[tools]\ngo = '1.27'\n",
			wantOK: true,
		},
		{
			name:   "no section but bootstrap mentioned fails closed",
			config: "[tasks]\nbootstrap = 'echo bootstrapped'\n",
			wantOK: false,
		},
		{
			name:   "section without run fails closed",
			config: "[tasks.bootstrap]\n[tools]\ngo = '1.27'\n",
			wantOK: false,
		},
		{
			name:   "section at end without run fails closed",
			config: "[tools]\n\n[tasks.bootstrap]\n",
			wantOK: false,
		},
		{
			name:   "unquoted run fails closed",
			config: "[tasks.bootstrap]\nrun = hack/bootstrap.sh\n",
			wantOK: false,
		},
		{
			name:   "empty run value fails closed",
			config: "[tasks.bootstrap]\nrun =\n",
			wantOK: false,
		},
		{
			name:   "unterminated quote fails closed",
			config: "[tasks.bootstrap]\nrun = 'a.sh\n",
			wantOK: false,
		},
		{
			name:   "garbage after the quote fails closed",
			config: "[tasks.bootstrap]\nrun = 'a.sh' extra\n",
			wantOK: false,
		},
	}
	for _, tt := range tests {
		refs, ok := BootstrapScriptRefs([]byte(tt.config))
		if ok != tt.wantOK {
			t.Errorf("%s: BootstrapScriptRefs ok = %v, want %v (refs %v)", tt.name, ok, tt.wantOK, refs)
			continue
		}
		if ok && !equalStrings(refs, tt.want) {
			t.Errorf("%s: BootstrapScriptRefs = %q, want %q", tt.name, refs, tt.want)
		}
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

// contractConfig is the fixed task-bearing contract fixture: one referenced
// script target, parseable by BootstrapScriptRefs.
func contractConfig() []byte {
	return []byte("[tools]\ngo = '1.27'\n\n[tasks.bootstrap]\nrun = 'hack/bootstrap.sh'\n")
}

func TestClassify(t *testing.T) {
	config := contractConfig()
	lock := []byte("lock\n")
	script := []byte("echo hi\n")

	both := func(baseEntry, headEntry fakeEntry) map[string]map[string]fakeEntry {
		return map[string]map[string]fakeEntry{
			"base": {
				ConfigPath:          {data: config, regular: true, present: true},
				LockPath:            {data: lock, regular: true, present: true},
				"hack/bootstrap.sh": baseEntry,
			},
			"head": {
				ConfigPath:          {data: config, regular: true, present: true},
				LockPath:            {data: lock, regular: true, present: true},
				"hack/bootstrap.sh": headEntry,
			},
		}
	}
	regularScript := fakeEntry{data: script, regular: true, present: true}

	tests := []struct {
		name      string
		entries   map[string]map[string]fakeEntry
		want      State
		wantError bool
	}{
		{
			name:    "approved end to end",
			entries: both(regularScript, regularScript),
			want:    StateApproved,
		},
		{
			name: "absent contract at head",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}, "hack/bootstrap.sh": regularScript},
				"head": {ConfigPath: {present: false}, LockPath: {present: false}},
			},
			want: StateAbsent,
		},
		{
			name:    "referenced script modified at head",
			entries: both(regularScript, fakeEntry{data: []byte("echo gone\n"), regular: true, present: true}),
			want:    StateModified,
		},
		{
			name:    "referenced script present at head but absent at base",
			entries: both(fakeEntry{present: false}, regularScript),
			want:    StateModified,
		},
		{
			name:    "referenced script absent at both revisions",
			entries: both(fakeEntry{present: false}, fakeEntry{present: false}),
			want:    StateModified,
		},
		{
			name:    "referenced script symlink at base",
			entries: both(fakeEntry{data: script, present: true}, regularScript),
			want:    StateModified,
		},
		{
			name:    "referenced script symlink at head",
			entries: both(regularScript, fakeEntry{data: script, present: true}),
			want:    StateModified,
		},
		{
			name: "config symlink at base",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, present: true}, LockPath: {data: lock, regular: true, present: true}, "hack/bootstrap.sh": regularScript},
				"head": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}, "hack/bootstrap.sh": regularScript},
			},
			want: StateModified,
		},
		{
			name: "config symlink at head",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}, "hack/bootstrap.sh": regularScript},
				"head": {ConfigPath: {data: config, present: true}, LockPath: {data: lock, regular: true, present: true}, "hack/bootstrap.sh": regularScript},
			},
			want: StateModified,
		},
		{
			name: "unparseable bootstrap task fails closed",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: []byte("[tasks.bootstrap]\nrun = 'a`b'\n"), regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
				"head": {ConfigPath: {data: []byte("[tasks.bootstrap]\nrun = 'a`b'\n"), regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}},
			},
			want: StateModified,
		},
		{
			name: "read error on the referenced script",
			entries: map[string]map[string]fakeEntry{
				"base": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}, "hack/bootstrap.sh": regularScript},
				"head": {ConfigPath: {data: config, regular: true, present: true}, LockPath: {data: lock, regular: true, present: true}, "hack/bootstrap.sh": {err: errors.New("object read failed")}},
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

func TestBootstrapAndEnvCaptureScripts(t *testing.T) {
	// The scripts are pinned exactly: their security property is that they
	// are fixed constants only — never interpolated with brief- or
	// model-supplied values — and a containment check cannot prove that.
	wantBootstrap := "set -eu\n" +
		"printf 'courier-mise-path=%s\\n' \"$(command -v mise)\"\n" +
		"printf 'courier-mise-version=%s\\n' \"$(mise --version)\"\n" +
		"printf 'courier-mise-data-dir=%s\\n' \"${MISE_DATA_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/mise}\"\n" +
		"mise run bootstrap\n"
	if got := BootstrapScript(); got != wantBootstrap {
		t.Errorf("BootstrapScript() = %q, want %q", got, wantBootstrap)
	}
	if got := EnvCaptureScript(); got != "mise x -- env -0\n" {
		t.Errorf("EnvCaptureScript() = %q, want %q", got, "mise x -- env -0\n")
	}

	// ParseEnvCapture consumes exactly the shape `mise x -- env -0` emits:
	// NUL-terminated KEY=VALUE records and nothing else.
	capture := "PATH=/fake/shims\x00NEW=1\x00"
	delta, keys, err := ParseEnvCapture(capture, []string{"PATH=/old"})
	if err != nil {
		t.Fatalf("ParseEnvCapture: %v", err)
	}
	if !equalStrings(delta, []string{"PATH=/fake/shims", "NEW=1"}) {
		t.Errorf("delta = %q", delta)
	}
	if !equalStrings(keys, []string{"NEW", "PATH"}) {
		t.Errorf("keys = %q", keys)
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
