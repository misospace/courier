package courier

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestChartRendersDispatchConfiguration(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	runHelm(t, chartDir, "dependency", "build")

	enabled := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test/custom-api",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.queueLane=queue-x",
		"--set", "dispatch.laneProfile=lane-y",
		"--set", "dispatch.pollInterval=47s",
		"--set", "dispatch.httpTimeout=19s",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set", "dispatch.tokenSecret.key=agent-token",
	)
	for _, want := range []string{
		"--dispatch-enabled=true",
		"--dispatch-base-url=https://dispatch.example.test/custom-api",
		"--dispatch-agent-name=example-agent",
		"--dispatch-queue-lane=queue-x",
		"--dispatch-lane=lane-y",
		"--dispatch-poll-interval=47s",
		"--dispatch-http-timeout=19s",
		"name: DISPATCH_AGENT_TOKEN\n            valueFrom:\n              secretKeyRef:\n                key: agent-token\n                name: dispatch-token-secret",
	} {
		if !strings.Contains(enabled, want) {
			t.Fatalf("Dispatch-enabled chart output is missing %q", want)
		}
	}

	disabled := runHelm(t, chartDir, "template", "courier", ".", "--set", "dispatch.enabled=false")
	for _, unwanted := range []string{
		"--dispatch-enabled",
		"--dispatch-base-url",
		"--dispatch-agent-name",
		"--dispatch-queue-lane",
		"--dispatch-lane",
		"--dispatch-poll-interval",
		"--dispatch-http-timeout",
		"DISPATCH_AGENT_TOKEN",
	} {
		if strings.Contains(disabled, unwanted) {
			t.Fatalf("Dispatch-disabled chart output unexpectedly contains %q", unwanted)
		}
	}
}

func copyChart(t *testing.T) string {
	t.Helper()
	temporary := t.TempDir()
	chartDir := filepath.Join(temporary, "courier")
	if err := os.CopyFS(chartDir, os.DirFS(".")); err != nil {
		t.Fatalf("copy chart: %v", err)
	}
	return chartDir
}

func runHelm(t *testing.T, chartDir string, args ...string) string {
	t.Helper()
	command := exec.Command("helm", args...)
	command.Dir = chartDir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("helm %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
