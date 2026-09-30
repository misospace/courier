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
