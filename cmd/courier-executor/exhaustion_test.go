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

	"github.com/misospace/courier/internal/executor"
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
		{name: "bare 402 without a body signal", line: `{"type":"error","sessionID":"ses_fixture","error":{"name":"APIError","data":{"statusCode":402,"responseBody":"{\"error\":{\"message\":\"upgrade your plan\"}}"}}}`},
		{name: "402 with a structured quota code", line: `{"type":"error","sessionID":"ses_fixture","error":{"name":"APIError","data":{"statusCode":402,"responseBody":"{\"error\":{\"code\":\"insufficient_quota\",\"message\":\"credits depleted\"}}"}}}`, want: true, wantCode: "insufficient_quota"},
		{name: "non event JSON", line: `{"error":{"type":"provider.rate-limit","status":429}}`},
		{name: "malformed event", line: `{"type":"error","error":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence, got := classifyProviderExhaustion([]byte(tc.line), signals)
			if got != tc.want || (got && (evidence.class != "allowance-exhausted" || evidence.code != tc.wantCode)) {
				t.Fatalf("classification = (%+v, %v), want exhausted=%v code=%q", evidence, got, tc.want, tc.wantCode)
			}
			// These fixtures carry no serialized response headers, so no
			// reset hint may appear (#866).
			if evidence.resetAfter != 0 {
				t.Fatalf("reset hint = %v, want none for a header-free event", evidence.resetAfter)
			}
		})
	}
}

// TestClassifyProviderExhaustionCarriesProviderResetHint pins the legacy path's
// authoritative quota-reset evidence (#866): OpenCode serializes the provider's
// response headers into the APIError event, and a confirmed exhaustion carries
// only a bounded Retry-After parsed from them — never invented, never guessed,
// and never attached to a line that does not prove exhaustion.
func TestClassifyProviderExhaustionCarriesProviderResetHint(t *testing.T) {
	quotaEvent := func(headers map[string]string) string {
		data := map[string]any{"statusCode": 429, "responseBody": `{"error":{"code":"insufficient_quota"}}`}
		if headers != nil {
			data["responseHeaders"] = headers
		}
		e, _ := json.Marshal(map[string]any{"type": "error", "sessionID": "ses_fixture", "error": map[string]any{"name": "APIError", "data": data}})
		return string(e)
	}
	signals := defaultExhaustionSignals()
	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{name: "seconds", headers: map[string]string{"retry-after": "2400"}, want: 40 * time.Minute},
		{name: "canonical casing", headers: map[string]string{"Retry-After": "90"}, want: 90 * time.Second},
		{name: "no headers at all", want: 0},
		{name: "unrelated headers", headers: map[string]string{"content-type": "application/json", "x-request-id": "req-1"}, want: 0},
		{name: "malformed", headers: map[string]string{"retry-after": "soon"}, want: 0},
		{name: "zero", headers: map[string]string{"retry-after": "0"}, want: 0},
		{name: "negative", headers: map[string]string{"retry-after": "-5"}, want: 0},
		{name: "conflicting duplicates", headers: map[string]string{"retry-after": "60", "Retry-After": "86400"}, want: 0},
		{name: "beyond the horizon clamps", headers: map[string]string{"retry-after": "172800"}, want: 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence, exhausted := classifyProviderExhaustion([]byte(quotaEvent(tc.headers)), signals)
			if !exhausted {
				t.Fatal("structured quota event must confirm exhaustion")
			}
			if evidence.resetAfter != tc.want {
				t.Fatalf("reset hint = %v, want %v", evidence.resetAfter, tc.want)
			}
		})
	}

	t.Run("configured plan-code verdict carries the hint too", func(t *testing.T) {
		b, _ := json.Marshal(map[string]any{"error": map[string]any{"type": "rate_limit_error", "message": "usage limit exceeded (2056)"}})
		e, _ := json.Marshal(map[string]any{"type": "error", "sessionID": "ses_fixture", "error": map[string]any{"name": "APIError", "data": map[string]any{
			"statusCode":      429,
			"responseHeaders": map[string]string{"retry-after": "3600"},
			"responseBody":    string(b),
		}}})
		evidence, exhausted := classifyProviderExhaustion(e, parseExhaustionSignals("2056", ""))
		if !exhausted || evidence.code != "2056" {
			t.Fatalf("plan-code exhaustion not classified: %+v exhausted=%v", evidence, exhausted)
		}
		if evidence.resetAfter != time.Hour {
			t.Fatalf("reset hint = %v, want 1h", evidence.resetAfter)
		}
	})

	t.Run("a transient line ends nothing and keeps no hint", func(t *testing.T) {
		e, _ := json.Marshal(map[string]any{"type": "error", "sessionID": "ses_fixture", "error": map[string]any{"name": "APIError", "data": map[string]any{
			"statusCode":      429,
			"responseHeaders": map[string]string{"retry-after": "30"},
			"responseBody":    `{"error":{"type":"rate_limit_error","message":"slow down"}}`,
		}}})
		if evidence, exhausted := classifyProviderExhaustion(e, defaultExhaustionSignals()); exhausted || evidence != (providerExhaustion{}) {
			t.Fatalf("an unhinted 429 must neither end the run nor retain a hint, got %+v exhausted=%v", evidence, exhausted)
		}
	})

	t.Run("an unexpected headers shape costs the hint, not the detection", func(t *testing.T) {
		// A runtime build that serializes the headers differently must not be
		// able to blind the stop path: the exhaustion verdict comes from the
		// body, and the hint is best-effort.
		e, _ := json.Marshal(map[string]any{"type": "error", "sessionID": "ses_fixture", "error": map[string]any{"name": "APIError", "data": map[string]any{
			"statusCode":      429,
			"responseHeaders": []string{"retry-after: 2400"},
			"responseBody":    `{"error":{"code":"insufficient_quota"}}`,
		}}})
		evidence, exhausted := classifyProviderExhaustion(e, defaultExhaustionSignals())
		if !exhausted || evidence.code != "insufficient_quota" {
			t.Fatalf("exhaustion detection regressed on an unexpected shape: %+v exhausted=%v", evidence, exhausted)
		}
		if evidence.resetAfter != 0 {
			t.Fatalf("unexpected shape produced a hint: %v", evidence.resetAfter)
		}
	})
}

func TestCapacityExhaustedTerminationRendersProviderResetHint(t *testing.T) {
	r := reporter{}
	build := func(resetAfter time.Duration) termination {
		t.Helper()
		return r.capacityExhaustedTermination(context.Background(), &git.Workspace{Directory: t.TempDir()},
			config{RemoteURL: "https://example.invalid/repo.git", Branch: "run-branch"}, "start-commit", "allowance-exhausted", "insufficient_quota", resetAfter)
	}
	hinted := build(40 * time.Minute)
	if hinted.ProviderResetAfterSeconds != 2400 {
		t.Fatalf("provider_reset_after_seconds = %d, want 2400", hinted.ProviderResetAfterSeconds)
	}
	if hinted.Outcome != string(executor.OutcomeCapacityExhausted) || hinted.Category != terminalCategoryCapacityExhaustion {
		t.Fatalf("ending lost its capacity routing: %+v", hinted)
	}
	if line := executor.MarshalTerminationLine(hinted); !strings.Contains(line, `"provider_reset_after_seconds":2400`) {
		t.Fatalf("handoff line omits the hint: %s", line)
	}
	if got := build(48 * time.Hour).ProviderResetAfterSeconds; got != 86400 {
		t.Fatalf("over-horizon hint rendered %d seconds, want the 24h clamp", got)
	}
	unhinted := build(0)
	if unhinted.ProviderResetAfterSeconds != 0 {
		t.Fatalf("provider_reset_after_seconds = %d, want no hint", unhinted.ProviderResetAfterSeconds)
	}
	if line := executor.MarshalTerminationLine(unhinted); strings.Contains(line, "provider_reset_after_seconds") {
		t.Fatalf("hint-free handoff carries the field: %s", line)
	}
}

func TestSessionTapStopsOnStructuredExhaustionEvent(t *testing.T) {
	var output strings.Builder
	stopped := false
	tap := newSessionTap(&output, parseExhaustionSignals("2056", ""))
	tap.setKill(func() { stopped = true })
	model := tap.beginAttempt()
	line := `{"type":"error","sessionID":"ses_fixture","error":{"name":"APIError","data":{"statusCode":429,"responseHeaders":{"retry-after":"2400"},"responseBody":"{\"error\":{\"code\":\"insufficient_quota\"}}"}}}`
	_, _ = model.Write([]byte(line + "\n"))
	if !stopped {
		t.Fatal("structured exhaustion did not trigger immediate stop callback")
	}
	evidence, ok := tap.lastExhaustion()
	if !ok {
		t.Fatal("structured exhaustion was not retained for terminal report")
	}
	if evidence.resetAfter != 40*time.Minute {
		t.Fatalf("retained reset hint = %v, want 40m", evidence.resetAfter)
	}
	_, _ = model.Write([]byte(`{"type":"error","error":{"type":"provider.rate-limit","message":"network retry failed"}}` + "\n"))
	if _, ok := tap.confirmedExhaustion(); ok {
		t.Fatal("later transport output incorrectly retained terminal proof")
	}
	// The retained hint survives the loss of confirmation, because the ending
	// still reports the exhaustion the event proved (#866).
	if evidence, ok := tap.lastExhaustion(); !ok || evidence.resetAfter != 40*time.Minute {
		t.Fatalf("last exhaustion = (%+v, %v), want the retained 40m hint", evidence, ok)
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
	const quotaBody = `"responseBody":"{\"error\":{\"code\":\"insufficient_quota\"}}"`
	for _, tc := range []struct {
		name string
		// data is the serialized APIError data the fake runtime emits.
		data       string
		wantField  string
		wantAbsent bool
	}{
		{
			name:      "provider hint rides the handoff",
			data:      `{"statusCode":429,"responseHeaders":{"retry-after":"2400"},` + quotaBody + `}`,
			wantField: `"provider_reset_after_seconds":2400`,
		},
		{
			name:       "hint-free exhaustion stays hint-free",
			data:       `{"statusCode":429,` + quotaBody + `}`,
			wantAbsent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			remote := gitRemoteWithBranch(t, root, "run-branch")
			binary := filepath.Join(root, "opencode")
			marker := filepath.Join(root, "continued")
			line := `{"type":"error","sessionID":"ses_fixture","error":{"name":"APIError","data":` + tc.data + `}}`
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
			handoff := out.String()
			if !strings.Contains(handoff, `"outcome":"capacity_exhausted"`) || strings.Contains(handoff, `executor.continuation`) {
				t.Fatalf("bad handoff: %s", handoff)
			}
			if tc.wantAbsent {
				if strings.Contains(handoff, "provider_reset_after_seconds") {
					t.Fatalf("hint-free exhaustion emitted a reset hint: %s", handoff)
				}
			} else if !strings.Contains(handoff, tc.wantField) {
				t.Fatalf("handoff omits %q: %s", tc.wantField, handoff)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("child kept writing: %v", err)
			}
		})
	}
}
