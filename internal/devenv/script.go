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

// DefaultToolchainDir is the worker-image path the toolchain contract is
// baked into at build time: a complete mise data dir (`mise install
// --locked` against the repository's own pins) and, beside it, the
// pre-materialized envtest asset store. Baking makes the approved
// contract's toolchain path hermetic by construction against the secure
// worker's network contract (#797): mise downloads nothing, because
// everything the contract's tool pins resolve to already exists in the
// image. A deployment whose image lacks the bake simply has no such
// directory and keeps the pre-#797 networked behavior.
const DefaultToolchainDir = "/usr/local/share/courier/mise-data"

// ToolchainDirEnv is the worker environment variable a deployment may set
// to relocate the baked toolchain dir. It is deployment-supplied only —
// no brief-, model-, or repository-supplied value can set it, so the
// scripts stay fixed constants plus trusted environment.
const ToolchainDirEnv = "COURIER_MISE_TOOLCHAIN_DIR"

// hermeticWiring is the shell both the bootstrap task and the
// environment-capture task run before mise resolves tools (#797). When
// the baked toolchain dir is present, mise is pinned to it for the rest
// of the task: MISE_DATA_DIR points at a per-run writable overlay whose
// entries symlink the baked dir (mise reads the baked installs but never
// writes the image layer), and MISE_OFFLINE=true turns every mise HTTP
// request into a fast, local refusal — a pin the bake cannot satisfy
// fails in milliseconds with an actionable error instead of stalling
// into the worker's network-policy black hole and dying as an opaque
// exit 1. The overlay lives on the per-run toolchain-cache volume when
// one is mounted writable, so the environment capture's MISE_DATA_DIR
// survives into every later task and mise keeps resolving the baked
// tools for the model's own commands; a topology without the volume
// falls back to a temporary dir. When the baked dir is absent the wiring
// exports nothing: networked mise, exactly the pre-#797 behavior.
const hermeticWiring = "toolchain=\"${" + ToolchainDirEnv + ":-" + DefaultToolchainDir + "}\"\n" + "if [ -d \"$toolchain\" ]; then\n" +
	"  cache_root=/courier-toolchain-cache\n" +
	"  if [ ! -w \"$cache_root\" ]; then\n" +
	"    cache_root=\"$(mktemp -d)\"\n" +
	"    cache_tmp=\"$cache_root\"\n" +
	"  fi\n" +
	"  MISE_DATA_DIR=\"$cache_root/mise-data\"\n" +
	"  if [ ! -d \"$MISE_DATA_DIR\" ]; then\n" +
	"    mkdir -p \"$MISE_DATA_DIR\"\n" +
	"    for entry in \"$toolchain\"/*; do\n" +
	"      [ -e \"$entry\" ] || continue\n" +
	"      ln -s \"$entry\" \"$MISE_DATA_DIR/$(basename \"$entry\")\"\n" +
	"    done\n" +
	"  fi\n" +
	"  export MISE_DATA_DIR\n" +
	"  MISE_OFFLINE=true\n" +
	"  export MISE_OFFLINE\n" +
	"fi\n"

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
// revision from the git object store in two guarded steps — `git archive`
// writes a tar to a temporary file, and only after that command succeeds
// does `tar -x` extract it (a pipeline would discard git's exit status in
// POSIX sh, so an archive that failed after emitting part of the tree —
// including the contract file — could otherwise leave mise running against
// an incomplete base with the `test -f` guard still passing) — then proves
// the materialization actually happened with `test -f` on the contract file
// (the environment capture in an empty directory must never succeed into a
// degenerate delta), and runs `mise run bootstrap` inside it. Whatever mise
// discovers — configs, conf.d fragments, task directories, includes —
// therefore comes from the approved base revision by construction; mise's
// task discovery cannot reach head
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
// The mise diagnostics are emitted exactly once, from the EXIT trap, after
// everything else the script could ever run: the trap is the script's
// terminal emission on success and on every failure (`set -e` exits, a
// failed materialization, a failed bootstrap run), so the trusted emission
// is always the last thing in the captured stdout tail and earlier
// repo-controlled lines cannot override it in
// ParseBootstrapDiagnostics. The trap preserves the original exit code via
// `exit "$rc"` and keeps a failing diag (`courier_diag || true`) from
// changing it; no repo code can run after the trap.
// Before mise resolves tools, the script runs the hermetic toolchain
// wiring (#797): when the worker image bakes the toolchain contract, mise
// resolves every pinned tool from the baked data dir under MISE_OFFLINE=true
// and never touches the network; when the image carries no bake, the wiring
// exports nothing and mise behaves exactly as before #797. See
// hermeticWiring.
// The bootstrap task and the environment capture are two separately
// dispatched tasks: each gets its own liveness bound, never one shared
// budget.
//
// On success the script transfers the materialization's repository-local
// bin/ outputs into the workspace checkout the task started in. A contract
// whose bootstrap installs tools under the repository root's bin/ (the
// repo-root bin/ convention, e.g. hack/bootstrap.sh writing
// bin/controller-gen) otherwise produces those outputs inside the temporary
// materialization the EXIT trap deletes, and the workspace every later
// command runs in still lacks them. The transferred bytes are outputs of
// the approved base revision's own bootstrap — the trusted chain the
// executed-bytes rule already establishes — so the transfer extends the
// contract's purpose without ever executing or copying head content into
// the trusted run, and it runs only after the bootstrap succeeded: a failed
// bootstrap leaves the workspace untouched.
func BootstrapScript(baseOID string) (string, error) {
	if !validBaseOID(baseOID) {
		return "", fmt.Errorf("bootstrap base revision %q is not a git object ID", baseOID)
	}
	return "set -eu\n" +
		"base='" + baseOID + "'\n" +
		"workspace=\"$(pwd)\"\n" +
		"src=\"$(mktemp -d)\"\n" +
		// Initialized before courier_diag is even defined: the EXIT trap can
		// fire on a failure before the hermetic wiring below ever ran, and
		// `set -u` must never abort the trusted terminal emission over an
		// unset wiring variable.
		"toolchain=''\n" +
		"cache_tmp=''\n" +
		"courier_diag() {\n" +
		"  printf 'courier-mise-path=%s\\n' \"$(command -v mise)\"\n" +
		"  printf 'courier-mise-version=%s\\n' \"$(mise --version)\"\n" +
		"  printf 'courier-mise-data-dir=%s\\n' \"${MISE_DATA_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/mise}\"\n" +
		"  printf 'courier-mise-toolchain=%s\\n' \"$toolchain\"\n" +
		"  printf 'courier-mise-offline=%s\\n' \"${MISE_OFFLINE:-false}\"\n" +
		"}\n" +
		// The trap is the single terminal emission: it runs last on success
		// and on every failure path, preserves the original exit code, and
		// neither a failing cleanup nor a failing diag can change it.
		"archive=\"$(mktemp)\"\n" +
		"trap 'rc=$?; rm -rf \"$src\" \"$archive\" \"$cache_tmp\" || true; courier_diag || true; exit \"$rc\"' EXIT\n" +
		"git archive --format=tar -o \"$archive\" \"$base\"\n" +
		"tar -xf \"$archive\" -C \"$src\"\n" +
		"test -f \"$src/" + ConfigPath + "\"\n" +
		"cd \"$src\"\n" +
		hermeticWiring +
		"mise run bootstrap\n" +
		"if [ -d \"$src/bin\" ]; then\n" +
		"  mkdir -p \"$workspace/bin\"\n" +
		"  cp -a \"$src/bin/.\" \"$workspace/bin/\"\n" +
		"fi\n", nil
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
		"capture=\"$(mktemp)\"\n" +
		"archive=\"$(mktemp)\"\n" +
		"cache_tmp=''\n" +
		"trap 'rm -rf \"$src\" \"$capture\" \"$archive\" \"$cache_tmp\" || true' EXIT\n" +
		"git archive --format=tar -o \"$archive\" \"$base\"\n" +
		"tar -xf \"$archive\" -C \"$src\"\n" +
		"test -f \"$src/" + ConfigPath + "\"\n" +
		"cd \"$src\"\n" +
		hermeticWiring +
		"mise x -- env -0 > \"$capture\"\n" +
		"size=\"$(wc -c < \"$capture\")\"\n" +
		"test \"$size\" -le " + strconv.Itoa(MaxCaptureBytes) + "\n" +
		"cat \"$capture\"\n", nil
}

// The diagnostic keys the bootstrap task's report carries, as fixed in the
// script above. Any other courier-* line is not a diagnostic this package
// agreed to consume.
const (
	diagMisePathKey      = "courier-mise-path"
	diagMiseVersionKey   = "courier-mise-version"
	diagMiseDataDirKey   = "courier-mise-data-dir"
	diagMiseToolchainKey = "courier-mise-toolchain"
	diagMiseOfflineKey   = "courier-mise-offline"
)

// maxDiagnosticValueBytes bounds one diagnostic value: diagnostics are
// receipt text, never a channel for unbounded worker output.
const maxDiagnosticValueBytes = 300

// errMalformedEnvCapture reports a record in the NUL-separated environment
// capture that is not a well-formed KEY=VALUE pair.
var errMalformedEnvCapture = errors.New("malformed environment capture")

// ParseBootstrapDiagnostics extracts the diagnostic lines the bootstrap
// task emits — keys exactly courier-mise-path, courier-mise-version,
// courier-mise-data-dir, courier-mise-toolchain, and courier-mise-offline
// — from the task's stdout. Any other line, of any
// shape, is ignored: worker output is untrusted and only these three
// fixed-key facts are ever consumed. A value that is not printable ASCII
// (0x20–0x7E) or that exceeds 300 bytes is dropped, never truncated. When a
// key appears more than once, the LAST occurrence that survives validation
// wins: the script emits its trusted diagnostics once, from its EXIT trap
// after everything else it could ever run, so the trusted terminal emission
// overrides anything repo code printed earlier — and a spoofed line cannot
// override the trusted terminal emission, because it is last. The returned
// map is possibly empty and the function never errors.
func ParseBootstrapDiagnostics(stdout string) map[string]string {
	diagnostics := make(map[string]string)
	for _, line := range strings.Split(stdout, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch key {
		case diagMisePathKey, diagMiseVersionKey, diagMiseDataDirKey, diagMiseToolchainKey, diagMiseOfflineKey:
		default:
			continue
		}
		if len(value) > maxDiagnosticValueBytes || !printableASCII(value) {
			continue
		}
		// Last valid occurrence wins, so the script's single terminal
		// emission from its EXIT trap always overrides earlier
		// repo-controlled lines.
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
