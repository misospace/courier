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
	// and the pinned binary accepts the emitted permission schema.

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
	helpOut, helpErr, helpRunErr := run(
		"run", "--rm",
		"--user", "65532:65532",
		"-e", configEnv,
		"--mount", "type=bind,src="+configDir+",dst="+configMount+",readonly",
		"--entrypoint", "sh",
		image,
		"-c", "opencode run --help",
	)
	if isConfigSchemaError(helpOut + helpErr) {
		t.Fatalf("opencode in image %s rejected the config at %s with a permission/schema validation error; fix the permission object in marshalOpenCodeConfig", image, configPath)
	}
	if helpRunErr != nil {
		versionOut, versionErr, _ := run(
			"run", "--rm",
			"--user", "65532:65532",
			"-e", configEnv,
			"--mount", "type=bind,src="+configDir+",dst="+configMount+",readonly",
			"--entrypoint", "sh",
			image,
			"-c", "opencode --version",
		)
		if isConfigSchemaError(versionOut + versionErr) {
			t.Fatalf("opencode in image %s rejected the config at %s with a permission/schema validation error; fix the permission object in marshalOpenCodeConfig", image, configPath)
		}
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
