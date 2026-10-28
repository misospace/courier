package executor

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCoordinatorDockerfileUsesNumericIdentity(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	dockerfile := string(data)
	if !strings.Contains(dockerfile, "groupadd --gid 65532 courier") ||
		!strings.Contains(dockerfile, "useradd --create-home --uid 65532 --gid 65532 courier") {
		t.Fatal("coordinator Dockerfile does not create the documented 65532 user/group")
	}
	if !strings.Contains(dockerfile, "USER 65532:65532") {
		t.Fatal("coordinator Dockerfile does not select the numeric 65532:65532 user")
	}
	if strings.Contains(dockerfile, "\nUSER courier\n") {
		t.Fatal("coordinator Dockerfile regressed to a named-only runtime user")
	}
}

func TestCoordinatorDockerfileShipsProcessTreeTools(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	dockerfile := string(data)

	// First "AS coordinator\n" is the bootstrap coordinator stage; the
	// coordinator-go FROM line comes later and only prefixes it.
	stageStart := strings.Index(dockerfile, "AS coordinator\n")
	if stageStart < 0 {
		t.Fatal("Dockerfile does not define coordinator stage")
	}
	stageEnd := strings.Index(dockerfile[stageStart:], "\nFROM ")
	if stageEnd < 0 {
		stageEnd = len(dockerfile) - stageStart
	}
	stage := dockerfile[stageStart : stageStart+stageEnd]

	if !strings.Contains(stage, "procps") {
		t.Fatal("coordinator Dockerfile does not install procps (ps, pgrep, pkill required by the runtime process-tree preflight)")
	}
}

// The ssh endpoint probe (internal/git) resolves an ssh remote's effective
// endpoint through the ssh binary git itself invokes, so the images that run
// the executor must ship an ssh client: without one, ssh-remote availability
// evidence is silently absent and every ssh remote fails closed to git's own
// verdict.
func TestCoordinatorDockerfileShipsSSH(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	stage := coordinatorStage(t, string(data))
	if !strings.Contains(stage, "openssh-client") {
		t.Fatal("coordinator Dockerfile does not install openssh-client (the ssh endpoint probe resolves ssh remotes through the ssh binary git invokes)")
	}
}

func TestCoordinatorImageProcessTreeTools(t *testing.T) {
	image := os.Getenv("COURIER_COORDINATOR_IMAGE")
	if image == "" {
		t.Skip("set COURIER_COORDINATOR_IMAGE to run the coordinator process-tree image test")
	}
	dockerPath, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker CLI not found on PATH; install Docker to run the coordinator process-tree image test")
	}

	run := func(args ...string) (string, string, error) {
		cmd := exec.Command(dockerPath, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		runErr := cmd.Run()
		return stdout.String(), stderr.String(), runErr
	}

	// Smoke test runs inside the built runtime image, not against CI host
	// utilities: the shipped image must carry the process-tree tools the
	// OpenCode runtime preflight requires, and they must actually terminate
	// a two-level tree, and it must carry the ssh client the ssh endpoint
	// probe resolves ssh remotes through. Availability is a toolchain
	// contract; no extra runtime privileges are granted or implied.
	probe := `set -eu
for tool in ps pgrep pkill; do
	command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done
command -v ssh >/dev/null || { echo "missing required tool: ssh (the ssh endpoint probe resolves ssh remotes through it)" >&2; exit 1; }
sh -c 'sleep 30 & sleep 30; wait' &
child=$!
tries=0
until [ -n "$(pgrep -P "$child" 2>/dev/null)" ] || [ "$tries" -ge 50 ]; do
	sleep 0.1; tries=$((tries+1))
done
tree="$(pgrep -P "$child" 2>/dev/null || true)"
test -n "$tree"
ps -o pid= -p "$child" >/dev/null
pkill -TERM -P "$child"
tries=0
until [ -z "$(pgrep -P "$child" 2>/dev/null)" ] || [ "$tries" -ge 50 ]; do
	sleep 0.1; tries=$((tries+1))
done
left="$(pgrep -P "$child" 2>/dev/null || true)"
test -z "$left"
kill -TERM "$child" 2>/dev/null || true
echo PROCTREE_OK`
	stdout, stderr, err := run(
		"run", "--rm",
		"--user", "65532:65532",
		"--entrypoint", "sh",
		image,
		"-c", probe,
	)
	if err != nil {
		t.Fatalf("process-tree probe failed in image %s: %s", image, snippet(stderr))
	}
	if !strings.Contains(stdout, "PROCTREE_OK") {
		t.Fatalf("process-tree probe in image %s did not confirm termination: %s", image, snippet(stderr))
	}
}

func TestCoordinatorGoDockerfilePinsToolchainCachePath(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	dockerfile := string(data)

	stageStart := strings.Index(dockerfile, "FROM coordinator AS coordinator-go")
	if stageStart < 0 {
		t.Fatal("Dockerfile does not define the coordinator-go stage")
	}
	stage := dockerfile[stageStart:]
	stageEnd := strings.Index(stage, "\nFROM ")
	if stageEnd < 0 {
		stageEnd = len(stage)
	}
	stage = stage[:stageEnd]

	if !strings.Contains(stage, "GOMODCACHE=/courier-toolchain-cache/go-mod") {
		t.Fatal("coordinator-go stage does not pin GOMODCACHE to /courier-toolchain-cache/go-mod")
	}
	if !strings.Contains(stage, "GOCACHE=/courier-toolchain-cache/go-build") {
		t.Fatal("coordinator-go stage does not pin GOCACHE to /courier-toolchain-cache/go-build")
	}
	if !strings.Contains(stage, "mkdir -p /courier-toolchain-cache") ||
		!strings.Contains(stage, "chown 65532:65532 /courier-toolchain-cache") {
		t.Fatal("coordinator-go stage does not create /courier-toolchain-cache owned by 65532")
	}
}

// coordinatorStage returns the Dockerfile text of the bootstrap coordinator
// stage: everything from its FROM line to the next FROM.
func coordinatorStage(t *testing.T, dockerfile string) string {
	t.Helper()
	stageStart := strings.Index(dockerfile, "AS coordinator\n")
	if stageStart < 0 {
		t.Fatal("Dockerfile does not define the coordinator stage")
	}
	stage := dockerfile[stageStart:]
	stageEnd := strings.Index(stage, "\nFROM ")
	if stageEnd < 0 {
		stageEnd = len(stage)
	}
	return stage[:stageEnd]
}

// The repository development bootstrap contract (#414) dispatches
// `mise run bootstrap`; the runtime images must ship a pinned mise binary
// downloaded at build time with checksum verification — never fetched at
// runtime.
func TestCoordinatorDockerfileShipsPinnedMise(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	stage := coordinatorStage(t, string(data))
	if !strings.Contains(stage, "ARG MISE_VERSION=") {
		t.Fatal("coordinator stage does not pin a MISE_VERSION")
	}
	if !strings.Contains(stage, "sha256sum -c -") ||
		!strings.Contains(stage, "mise-") {
		t.Fatal("coordinator stage does not verify a downloaded mise binary against a pinned checksum")
	}
	if !strings.Contains(stage, "install -m 0755 /tmp/mise /usr/local/bin/mise") {
		t.Fatal("coordinator stage does not install mise to /usr/local/bin/mise")
	}
}

// The harness-go stage is the secure path's Go toolchain contract: the
// harness binaries on the coordinator-go base, so a lane that selects a Go
// runtime provisions its secure workers with the same pinned toolchain as
// legacy lanes.
func TestHarnessGoDockerfileLayersBinariesOnToolchainBase(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	dockerfile := string(data)
	stageStart := strings.Index(dockerfile, "FROM coordinator-go AS harness-go")
	if stageStart < 0 {
		t.Fatal("Dockerfile does not define the harness-go stage on the coordinator-go base")
	}
	stage := dockerfile[stageStart:]
	stageEnd := strings.Index(stage, "\nFROM ")
	if stageEnd < 0 {
		stageEnd = len(stage)
	}
	stage = stage[:stageEnd]

	for _, binary := range []string{"courier-control", "courier-worker", "courier-broker"} {
		if !strings.Contains(stage, "/usr/local/bin/"+binary) {
			t.Fatalf("harness-go stage does not copy %s", binary)
		}
	}
	if !strings.Contains(stage, "USER 65532:65532") {
		t.Fatal("harness-go stage does not select the numeric 65532:65532 user")
	}
}

func TestCoordinatorImageScratchBoundary(t *testing.T) {
	image := os.Getenv("COURIER_COORDINATOR_IMAGE")
	if image == "" {
		t.Skip("set COURIER_COORDINATOR_IMAGE to run the pinned OpenCode scratch boundary image test")
	}
	dockerPath, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker CLI not found on PATH; install Docker to run the scratch boundary image test")
	}
	// The model-driven delegated-agent denial check is intentionally omitted: the pinned OpenCode
	// permission boundary only triggers on a model-emitted tool call and CI has no deterministic
	// model path. This test covers the deterministic parts: 65532 can write the scratch mount
	// and the pinned binary accepts the emitted permission schema; `opencode mcp list` is the
	// first subcommand that loads and validates OPENCODE_CONFIG, while `--help` and `--version`
	// exit 0 even when the config is schema-invalid.

	run := func(args ...string) (string, string, error) {
		cmd := exec.Command(dockerPath, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		runErr := cmd.Run()
		return stdout.String(), stderr.String(), runErr
	}

	// The per-run scratch dir must be writable by the 65532 coordinator identity.
	scratchDir := t.TempDir()
	if err := os.Chmod(scratchDir, 0o777); err != nil {
		t.Fatalf("chmod scratch bind dir %s: %v", scratchDir, err)
	}
	stdout, stderr, err := run(
		"run", "--rm",
		"--user", "65532:65532",
		"--mount", "type=bind,src="+scratchDir+",dst=/var/tmp/courier-scratch",
		"--entrypoint", "sh",
		image,
		"-c", "mkdir -p /var/tmp/courier-scratch && touch /var/tmp/courier-scratch/probe && test -w /var/tmp/courier-scratch/probe && echo SCRATCH_OK",
	)
	if err != nil {
		t.Fatalf("scratch probe failed for image %s mounting %s at /var/tmp/courier-scratch: %s", image, scratchDir, snippet(stderr))
	}
	if !strings.Contains(stdout, "SCRATCH_OK") {
		t.Fatalf("scratch probe for image %s did not confirm a writable /var/tmp/courier-scratch: %s", image, snippet(stderr))
	}

	// The pinned opencode must accept the emitted config schema (the permission object).
	roles := map[string]string{"coordinator": "litellm/qwen"}
	configData, err := marshalOpenCodeConfig(roles, "", "", "")
	if err != nil {
		t.Fatalf("marshalOpenCodeConfig() error = %v", err)
	}
	configDir := t.TempDir()
	if err := os.Chmod(configDir, 0o777); err != nil {
		t.Fatalf("chmod config dir %s: %v", configDir, err)
	}
	configPath := filepath.Join(configDir, "opencode.json")
	if err := os.WriteFile(configPath, configData, 0o644); err != nil {
		t.Fatalf("write opencode config %s: %v", configPath, err)
	}

	const configMount = "/etc/courier/opencode"
	configEnv := "OPENCODE_CONFIG=" + configMount + "/opencode.json"
	mcpOut, mcpErr, _ := run(
		"run", "--rm",
		"--user", "65532:65532",
		"-e", configEnv,
		"--mount", "type=bind,src="+configDir+",dst="+configMount+",readonly",
		"--entrypoint", "sh",
		image,
		"-c", "opencode mcp list",
	)
	if isConfigSchemaError(mcpOut + mcpErr) {
		t.Fatalf("opencode in image %s rejected the config at %s with a permission/schema validation error; fix the permission object in marshalOpenCodeConfig", image, configPath)
	}
}

func TestCoordinatorImageToolchainReferenceBoundary(t *testing.T) {
	image := os.Getenv("COURIER_COORDINATOR_IMAGE")
	if image == "" {
		t.Skip("set COURIER_COORDINATOR_IMAGE to run the pinned toolchain reference boundary image test")
	}
	dockerPath, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker CLI not found on PATH; install Docker to run the toolchain reference boundary image test")
	}

	run := func(args ...string) (string, string, error) {
		cmd := exec.Command(dockerPath, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		runErr := cmd.Run()
		return stdout.String(), stderr.String(), runErr
	}

	// The same host dir is mounted read-write at the toolchain cache and
	// read-only at the reference: 65532 must write the cache, read the
	// reference back, and be denied a write to it at the filesystem level.
	cacheDir := t.TempDir()
	if err := os.Chmod(cacheDir, 0o777); err != nil {
		t.Fatalf("chmod toolchain cache bind dir %s: %v", cacheDir, err)
	}
	stdout, stderr, err := run(
		"run", "--rm",
		"--user", "65532:65532",
		"--mount", "type=bind,src="+cacheDir+",dst=/courier-toolchain-cache",
		"--mount", "type=bind,src="+cacheDir+",dst=/courier-toolchain,readonly",
		"--entrypoint", "sh",
		image,
		"-c", "touch /courier-toolchain-cache/probe && test -f /courier-toolchain/probe && ! touch /courier-toolchain/forbidden && echo TOOLCHAIN_RO_OK",
	)
	if err != nil {
		t.Fatalf("toolchain reference probe failed for image %s mounting %s: %s", image, cacheDir, snippet(stderr))
	}
	if !strings.Contains(stdout, "TOOLCHAIN_RO_OK") {
		t.Fatalf("toolchain reference probe for image %s did not confirm the read-only /courier-toolchain boundary: %s", image, snippet(stderr))
	}
}

func isConfigSchemaError(output string) bool {
	lower := strings.ToLower(output)
	hasSubject := strings.Contains(lower, "permission") || strings.Contains(lower, "config")
	hasErrorKind := strings.Contains(lower, "invalid") || strings.Contains(lower, "schema")
	return hasSubject && hasErrorKind
}

func snippet(output string) string {
	output = strings.TrimSpace(output)
	const max = 200
	if len(output) > max {
		return output[:max] + "..."
	}
	return output
}
