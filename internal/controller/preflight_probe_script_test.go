package controller

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// probeScriptChecks is one check per node-local deny path preflight probes,
// on loopback ports nothing listens on, so the executed script observes deny
// without depending on the network shape of the machine running the test.
var probeScriptChecks = []probeCheck{
	{Name: "api-server", IP: "127.0.0.1", Port: 1, Expect: "deny"},
	{Name: "cluster-dns", IP: "127.0.0.1", Port: 2, Expect: "deny"},
	{Name: "cloud-metadata", IP: "127.0.0.1", Port: 3, Expect: "deny"},
	{Name: "broker-service", IP: "127.0.0.1", Port: 4, Expect: "deny"},
}

// TestProbeScriptWritesResultsByOverwriteSafeRedirect pins the final write to
// a redirection. Kubelet pre-creates /dev/termination-log and the pinned
// busybox cp refuses to create onto an existing file ("File exists"), so a cp
// form leaves every probe Failed with an empty result and preflight forever
// at 0 of N checks (#672): reverting this script to cp must fail here.
func TestProbeScriptWritesResultsByOverwriteSafeRedirect(t *testing.T) {
	script := probeScript(probeScriptChecks)
	if !strings.Contains(script, "cat $results > /dev/termination-log\n") {
		t.Fatalf("probe script must publish results through an overwrite-safe redirect:\n%s", script)
	}
	if strings.Contains(script, "cp $results") {
		t.Fatalf("probe script must not cp onto the pre-created termination log:\n%s", script)
	}
	for _, check := range probeScriptChecks {
		want := check.Name + " " + check.IP + " " + strconv.Itoa(int(check.Port)) + " " + check.Expect + "\n"
		if !strings.Contains(script, want) {
			t.Errorf("probe script missing check invocation %q:\n%s", want, script)
		}
	}
}

// TestProbeScriptExecutesAndCollectsEveryOutcome runs the rendered script
// under sh with its two fixed paths redirected into a temp directory, and
// requires the termination log to carry one parseable JSON outcome per check.
// This proves the accumulation, JSON shape, and line-per-check contract the
// operator's collectProbeResults parses; the busybox cp-vs-redirect behavior
// itself is pinned by the string test above, because a temp file cannot
// reproduce kubelet's pre-created termination log.
func TestProbeScriptExecutesAndCollectsEveryOutcome(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("probe script is a POSIX sh script")
	}
	dir := t.TempDir()
	term := filepath.Join(dir, "termination-log")
	replacer := strings.NewReplacer(
		"results=/tmp/probe-results", "results="+filepath.Join(dir, "results"),
		"/dev/termination-log", term,
	)
	cmd := exec.Command("/bin/sh", "-c", replacer.Replace(probeScript(probeScriptChecks)))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("probe script failed: %v\n%s", err, out)
	}

	raw, err := os.ReadFile(term)
	if err != nil {
		t.Fatalf("read termination log: %v", err)
	}
	got := map[string]string{}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	for _, line := range lines {
		var outcome probeOutcome
		if err := json.Unmarshal([]byte(line), &outcome); err != nil {
			t.Fatalf("termination log line %q is not a probe outcome: %v", line, err)
		}
		got[outcome.Name] = outcome.Got
	}
	if len(lines) != len(probeScriptChecks) {
		t.Fatalf("termination log has %d outcomes, want %d: %s", len(lines), len(probeScriptChecks), raw)
	}
	for _, check := range probeScriptChecks {
		if got[check.Name] != "deny" {
			t.Errorf("check %q observed %q, want deny", check.Name, got[check.Name])
		}
	}
}
