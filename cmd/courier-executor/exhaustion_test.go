package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/git"
)

func TestClassifyProviderExhaustionOpenCodeEvents(t *testing.T) {
	body := func(code string) string {
		b, _ := json.Marshal(map[string]any{"error": map[string]any{"type": "rate_limit_error", "message": "usage limit exceeded (" + code + ")"}})
		e, _ := json.Marshal(map[string]any{"type": "error", "sessionID": "ses_fixture", "error": map[string]any{"name": "APIError", "data": map[string]any{"statusCode": 429, "responseBody": string(b)}}})
		return string(e)
	}
	signals := parseExhaustionSignals("2056", "")
	for _, tc := range []struct {
		name, line string
		want       bool
		wantCode   string
	}{
		{name: "configured MiniMax plan code", line: body("2056"), want: true, wantCode: "2056"},
		{name: "generic 429", line: strings.Replace(body("2056"), "2056", "429", 1)},
		{name: "unknown provider code", line: body("987654")},
		{name: "assistant prose", line: `{"type":"text","sessionID":"ses_fixture","part":{"type":"text","text":"provider quota exceeded (2056)"}}`},
		{name: "non event JSON", line: `{"error":{"type":"provider.rate-limit","status":429}}`},
		{name: "malformed event", line: `{"type":"error","error":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, class, code := classifyProviderExhaustion([]byte(tc.line), signals)
			if got != tc.want || (got && (class != "allowance-exhausted" || code != tc.wantCode)) {
				t.Fatalf("classification = (%v, %q, %q), want exhausted=%v code=%q", got, class, code, tc.want, tc.wantCode)
			}
		})
	}
}

func TestSessionTapStopsOnStructuredExhaustionEvent(t *testing.T) {
	var output strings.Builder
	stopped := false
	tap := newSessionTap(&output, parseExhaustionSignals("2056", ""))
	tap.setKill(func() { stopped = true })
	model := tap.beginAttempt()
	line := `{"type":"error","sessionID":"ses_fixture","error":{"name":"APIError","data":{"statusCode":429,"responseBody":"{\"error\":{\"code\":\"insufficient_quota\"}}"}}}`
	_, _ = model.Write([]byte(line + "\n"))
	if !stopped {
		t.Fatal("structured exhaustion did not trigger immediate stop callback")
	}
	if _, _, ok := tap.lastExhaustion(); !ok {
		t.Fatal("structured exhaustion was not retained for terminal report")
	}
	_, _ = model.Write([]byte(`{"type":"error","error":{"type":"provider.rate-limit","message":"network retry failed"}}` + "\n"))
	if _, _, ok := tap.confirmedExhaustion(); ok {
		t.Fatal("later transport output incorrectly retained terminal proof")
	}
	// A pipe abandoned by a killed attempt (old generation, after the run
	// loop flushed and moved on) forwards bytes but can neither re-arm the
	// kill callback nor alter classification.
	stopped = false
	_ = tap.Flush()
	_, _ = model.Write([]byte(line + "\n"))
	if stopped {
		t.Fatal("bytes from a closed generation stopped the next attempt")
	}
}

func TestCapacityWorkspaceSafety(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runGit(t, root, "init", "--bare", remote)
	runGit(t, root, "init", source)
	configureGit(t, source)
	write(t, filepath.Join(source, "base.txt"), "base\n")
	commit(t, source, "base")
	runGit(t, source, "branch", "-M", "main")
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")
	runGit(t, source, "checkout", "-b", "run-branch")
	start, err := revParse(ctx, source, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &git.Workspace{Directory: source}
	check := func(want string) {
		t.Helper()
		got := capacityWorkspaceSafety(ctx, workspace, remote, "run-branch", start)
		if got.State != want {
			t.Fatalf("workspace safety = %#v, want state %q", got, want)
		}
	}
	check("empty")

	if err := os.WriteFile(filepath.Join(source, "untracked.txt"), []byte("draft\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	check("dirty")
	if err := os.Remove(filepath.Join(source, "untracked.txt")); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(source, "work.txt"), "committed\n")
	commit(t, source, "work")
	check("unknown")
	runGit(t, source, "push", "origin", "HEAD:refs/heads/run-branch")
	check("pushed")
}

func TestRunQuotaStopsBeforeContinuation(t *testing.T) {
	root := t.TempDir()
	remote := gitRemoteWithBranch(t, root, "run-branch")
	binary := filepath.Join(root, "opencode")
	marker := filepath.Join(root, "continued")
	line := `{"type":"error","sessionID":"ses_fixture","error":{"name":"APIError","data":{"statusCode":429,"responseBody":"{\"error\":{\"code\":\"insufficient_quota\"}}"}}}`
	writeExecutable(t, binary, "#!/bin/sh\ncase \"$1\" in mcp) exit 0;; esac\nprintf '%s\\n' '"+line+"'\nsleep 3\ntouch '"+marker+"'\n")
	for key, value := range map[string]string{
		"COURIER_REPO_URL": remote, "COURIER_WORKSPACE": filepath.Join(root, "workspace"),
		"COURIER_SCRATCH_DIR": filepath.Join(root, "scratch"), "COURIER_BASE": "main", "COURIER_BRANCH": "run-branch",
		"COURIER_GOAL": "test quota stop", "COURIER_MODEL": "test/model", "COURIER_OPENCODE_BINARY": binary,
	} {
		t.Setenv(key, value)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out, stderr bytes.Buffer
	if code := run(ctx, &out, &stderr); code != exitNeedsHuman {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), out.String())
	}
	if !strings.Contains(out.String(), `"outcome":"capacity_exhausted"`) || strings.Contains(out.String(), `executor.continuation`) {
		t.Fatalf("bad handoff: %s", out.String())
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("child kept writing: %v", err)
	}
}
