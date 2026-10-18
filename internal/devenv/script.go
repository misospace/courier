package devenv

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// MaxCaptureBytes bounds the environment capture at its source: the
// environment-capture script fails the task before emitting a capture over
// this bound. It is a build-time constant, paired with protocol's
// maxTaskEnvBytes dispatch bound (internal/protocol/worker.go, 64 KiB) and
// with the worker's 64 KiB stdout tail: the capture is capped so it always
// fits both — the delta applied from it can never exceed the Task.Env
// dispatch bound, and the whole capture is always inside the bounded tail
// the parse reads. The two bounds must stay paired; change them together.
const MaxCaptureBytes = 64 << 10

// oidPattern is the strict shape a base revision OID must have before it is
// interpolated into a task script: bare lowercase hex, 160-bit (SHA-1) or
// 256-bit (SHA-256) git object IDs only.
var oidPattern = regexp.MustCompile(`^[0-9a-f]{40}$|^[0-9a-f]{64}$`)

// validBaseOID reports whether baseOID is a git object ID safe to
// interpolate into a task script.
func validBaseOID(baseOID string) bool {
	return oidPattern.MatchString(baseOID)
}

// BootstrapScript returns the fixed POSIX sh bootstrap task script for the
// operator-approved base revision baseOID. The script NEVER runs the
// contract from the worker's head working tree: it materializes the base
// revision from the git object store (`git archive` piped to tar), proves
// the materialization actually happened with `test -f` on the contract file
// (a `git archive` failure piped to tar without pipefail would otherwise
// yield an empty directory, and the environment capture in an empty
// directory must never succeed into a degenerate delta), and runs
// `mise run bootstrap` inside it. Whatever mise discovers — configs, conf.d
// fragments, task directories, includes — therefore comes from the approved
// base revision by construction; mise's task discovery cannot reach head
// content, whatever surfaces it reads. Classify guarantees the contract
// exists at base whenever this script is dispatched: the script is only
// built and dispatched for a StateApproved snapshot, and an approved state
// implies the base carries the contract file the `test -f` checks.
//
// The script is fixed constants plus the validated OID: no brief- or
// model-supplied value enters it, because the worker executes whatever
// bytes it is given and the contract's trust comes from the repository, not
// from this script. The OID is interpolated ONLY after strict validation to
// bare lowercase hex (40 or 64 characters), and only inside shell single
// quotes, which the shell interprets literally — a validated [0-9a-f]
// string cannot carry a quote, a dollar, or any metacharacter, so the
// interpolation can only ever produce the OID itself.
//
// The mise diagnostics are emitted BEFORE and AFTER the bootstrap run:
// before, so a failed bootstrap that produced little output of its own
// still yields them; after, so on success the trusted terminal emission is
// always the last thing in the captured stdout tail and earlier,
// repo-controlled lines cannot override it in ParseBootstrapDiagnostics.
// The bootstrap task and the environment capture are two separately
// dispatched tasks: each gets its own liveness bound, never one shared
// budget.
func BootstrapScript(baseOID string) (string, error) {
	if !validBaseOID(baseOID) {
		return "", fmt.Errorf("bootstrap base revision %q is not a git object ID", baseOID)
	}
	return "set -eu\n" +
		"base='" + baseOID + "'\n" +
		"src=\"$(mktemp -d)\"\n" +
		"trap 'rm -rf \"$src\"' EXIT\n" +
		"git archive \"$base\" | tar -x -C \"$src\"\n" +
		"test -f \"$src/" + ConfigPath + "\"\n" +
		"cd \"$src\"\n" +
		"courier_diag() {\n" +
		"  printf 'courier-mise-path=%s\\n' \"$(command -v mise)\"\n" +
		"  printf 'courier-mise-version=%s\\n' \"$(mise --version)\"\n" +
		"  printf 'courier-mise-data-dir=%s\\n' \"${MISE_DATA_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/mise}\"\n" +
		"}\n" +
		"courier_diag\n" +
		"mise run bootstrap\n" +
		"courier_diag\n", nil
}

// EnvCaptureScript returns the fixed POSIX sh environment-capture task
// script for the operator-approved base revision baseOID: like
// BootstrapScript, it materializes the base revision from the git object
// store and runs `mise x -- env -0` inside it — never in the head working
// tree — so the environment captured is the activated environment of the
// approved base revision's contract, whatever mise discovers. The capture
// is written to a temporary file and size-checked against MaxCaptureBytes
// BEFORE it is emitted, so the source-side cap (paired with protocol's
// Task.Env dispatch bound) fails an oversized capture honestly instead of
// truncating it. The bound is rendered from the MaxCaptureBytes constant at
// build time — it is a Go constant, not runtime data, so the script remains
// fixed. See BootstrapScript for why the OID is validated and single-quoted,
// and for the Classify coupling that guarantees the contract file exists at
// base whenever this script is dispatched.
func EnvCaptureScript(baseOID string) (string, error) {
	if !validBaseOID(baseOID) {
		return "", fmt.Errorf("environment-capture base revision %q is not a git object ID", baseOID)
	}
	return "set -eu\n" +
		"base='" + baseOID + "'\n" +
		"src=\"$(mktemp -d)\"\n" +
		"trap 'rm -rf \"$src\"' EXIT\n" +
		"git archive \"$base\" | tar -x -C \"$src\"\n" +
		"test -f \"$src/" + ConfigPath + "\"\n" +
		"cd \"$src\"\n" +
		"capture=\"$(mktemp)\"\n" +
		"mise x -- env -0 > \"$capture\"\n" +
		"size=\"$(wc -c < \"$capture\")\"\n" +
		"test \"$size\" -le " + strconv.Itoa(MaxCaptureBytes) + "\n" +
		"cat \"$capture\"\n", nil
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
// (0x20–0x7E) or that exceeds 300 bytes is dropped, never truncated. When a
// key appears more than once, the LAST occurrence that survives validation
// wins: the script emits its trusted diagnostics both before and after the
// bootstrap run, so the trusted terminal emission overrides anything repo
// code printed earlier — and a spoofed line cannot override the trusted
// terminal emission, because it is last. The returned map is possibly empty
// and the function never errors.
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
		// Last valid occurrence wins, so the script's trusted post-bootstrap
		// emission always overrides earlier repo-controlled lines.
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
