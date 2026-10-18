package devenv

import (
	"errors"
	"slices"
	"strings"
)

// BootstrapScript returns the fixed POSIX sh bootstrap task script: it
// reports where mise lives and which version and data directory it uses —
// diagnostics the operator receipt can carry — then runs the repository's
// `mise run bootstrap` task. It is fixed constants only — no interpolation,
// no brief- or model-supplied values — because the worker executes whatever
// bytes it is given and the contract's trust comes from the repository, not
// from this script. The bootstrap task and the environment capture are two
// separately bounded tasks: each gets its own liveness bound, never one
// shared budget.
func BootstrapScript() string {
	return "set -eu\n" +
		"printf 'courier-mise-path=%s\\n' \"$(command -v mise)\"\n" +
		"printf 'courier-mise-version=%s\\n' \"$(mise --version)\"\n" +
		"printf 'courier-mise-data-dir=%s\\n' \"${MISE_DATA_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/mise}\"\n" +
		"mise run bootstrap\n"
}

// EnvCaptureScript returns the fixed POSIX sh environment-capture task
// script: `mise x -- env -0`, whose NUL-separated records ParseEnvCapture
// filters into the delta over the caller's base environment. It is fixed
// constants only, for the same reason as BootstrapScript.
func EnvCaptureScript() string {
	return "mise x -- env -0\n"
}

// The diagnostic keys the bootstrap task's report carries, as fixed in the
// script above. Any other courier-* line is not a diagnostic this package
// agreed to consume.
const (
	diagMisePathKey    = "courier-mise-path"
	diagMiseVersionKey = "courier-mise-version"
	diagMiseDataDirKey = "courier-mise-data-dir"
)

// maxDiagnosticValueBytes bounds one diagnostic value: diagnostics are
// receipt text, never a channel for unbounded worker output.
const maxDiagnosticValueBytes = 300

// errMalformedEnvCapture reports a record in the NUL-separated environment
// capture that is not a well-formed KEY=VALUE pair.
var errMalformedEnvCapture = errors.New("malformed environment capture")

// ParseBootstrapDiagnostics extracts the diagnostic lines the bootstrap
// task emits — keys exactly courier-mise-path, courier-mise-version, and
// courier-mise-data-dir — from the task's stdout. Any other line, of any
// shape, is ignored: worker output is untrusted and only these three
// fixed-key facts are ever consumed. A value that is not printable ASCII
// (0x20–0x7E) or that exceeds 300 bytes is dropped, never truncated. The
// returned map is possibly empty and the function never errors.
func ParseBootstrapDiagnostics(stdout string) map[string]string {
	diagnostics := make(map[string]string)
	for _, line := range strings.Split(stdout, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch key {
		case diagMisePathKey, diagMiseVersionKey, diagMiseDataDirKey:
		default:
			continue
		}
		if len(value) > maxDiagnosticValueBytes || !printableASCII(value) {
			continue
		}
		diagnostics[key] = value
	}
	return diagnostics
}

// printableASCII reports whether every byte of s is printable ASCII
// (0x20–0x7E).
func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// ParseEnvCapture turns an environment-capture task's stdout
// (`mise x -- env -0`, NUL-separated KEY=VALUE records) into the filtered
// delta over baseEnv and the sorted list of delta key names. A missing,
// truncated, or malformed capture is an error; callers treat that as a
// bootstrap failure rather than partially applying an environment.
// baseEnv is the worker's base environment, filtered the same way.
func ParseEnvCapture(stdout string, baseEnv []string) ([]string, []string, error) {
	records, err := splitNUL(stdout)
	if err != nil {
		return nil, nil, err
	}
	delta, err := envDelta(records, baseEnvMap(baseEnv))
	if err != nil {
		return nil, nil, errMalformedEnvCapture
	}
	keys := make([]string, 0, len(delta))
	for _, kv := range delta {
		name, _, _ := strings.Cut(kv, "=")
		keys = append(keys, name)
	}
	slices.Sort(keys)
	return delta, keys, nil
}

// baseEnvMap parses the base environment into name→value. Entries without
// '=' are not environment entries and are ignored: the delta comparison
// only ever needs well-formed pairs.
func baseEnvMap(env []string) map[string]string {
	base := make(map[string]string, len(env))
	for _, kv := range env {
		if name, value, ok := strings.Cut(kv, "="); ok {
			base[name] = value
		}
	}
	return base
}

// splitNUL splits the capture on NUL separators. `mise x -- env -0` always
// terminates its last record with a NUL, so a capture without the trailing
// NUL is truncated: it fails closed rather than silently yielding a corrupt
// partial environment.
func splitNUL(capture string) ([]string, error) {
	if capture == "" {
		return nil, errors.New("empty environment capture")
	}
	if !strings.HasSuffix(capture, "\x00") {
		return nil, errors.New("truncated environment capture")
	}
	return strings.Split(capture, "\x00"), nil
}

// envDelta filters NUL-separated KEY=VALUE records into the delta over
// base: records equal to the base value are not changes, protected keys are
// dropped before they reach the delta, and any malformed record fails the
// whole capture rather than partially applying it.
func envDelta(records []string, base map[string]string) ([]string, error) {
	delta := make([]string, 0, len(records))
	for _, record := range records {
		if record == "" {
			continue
		}
		name, value, ok := strings.Cut(record, "=")
		if !ok || name == "" || hasControl(name) {
			return nil, errors.New("malformed environment capture")
		}
		if ProtectedKeys(name) {
			continue
		}
		if baseValue, inBase := base[name]; inBase && baseValue == value {
			continue
		}
		delta = append(delta, record)
	}
	return delta, nil
}

// hasControl reports whether a name carries control characters. NUL and '='
// cannot survive the record split by construction; any other non-printable
// name byte is rejected so an environment name stays a name.
func hasControl(name string) bool {
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
