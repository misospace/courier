package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// parseEventLines decodes every non-empty line as a JSON object. Failure
// messages report line indexes, never line contents, so secrets in test
// payloads cannot leak through test output.
func parseEventLines(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	var events []map[string]any
	for i, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", i+1, err)
		}
		events = append(events, event)
	}
	return events
}

func TestEmitterEmitsValidJSONWithRunMetadata(t *testing.T) {
	var out bytes.Buffer
	emitter := NewEmitter(&out, LevelInfo)
	emitter.now = func() time.Time { return time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC) }

	err := emitter.Emit(Event{
		Type:   EventRunExit,
		RunID:  "coderrun-acme-7",
		Repo:   "acme/widgets",
		Ref:    7,
		Mode:   "resolve-issue",
		Brief:  "brief-redactor",
		Role:   "coordinator",
		Model:  "any-provider/any-model",
		Status: StatusOK,
	})
	if err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	events := parseEventLines(t, &out)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	event := events[0]
	for field, want := range map[string]any{
		"time":   "2026-09-21T12:00:00Z",
		"event":  EventRunExit,
		"run_id": "coderrun-acme-7",
		"repo":   "acme/widgets",
		"ref":    float64(7),
		"mode":   "resolve-issue",
		"brief":  "brief-redactor",
		"role":   "coordinator",
		"model":  "any-provider/any-model",
		"status": StatusOK,
	} {
		got, ok := event[field]
		if !ok {
			t.Fatalf("event is missing field %q", field)
		}
		if got != want {
			t.Fatalf("field %q = %v, want %v", field, got, want)
		}
	}
	if _, ok := event["detail"]; ok {
		t.Fatal("info-level event must not carry a detail object")
	}
}

func TestEmitterOmitsOptionalFields(t *testing.T) {
	var out bytes.Buffer
	emitter := NewEmitter(&out, LevelInfo)
	if err := emitter.Emit(Event{Type: EventRunStart, RunID: "solo-run"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	events := parseEventLines(t, &out)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	for _, optional := range []string{"repo", "ref", "mode", "brief", "role", "model", "status", "detail"} {
		if _, ok := events[0][optional]; ok {
			t.Fatalf("absent field %q must be omitted from the JSON line", optional)
		}
	}
}

func TestEmitterRejectsEventsWithoutRunID(t *testing.T) {
	var out bytes.Buffer
	emitter := NewEmitter(&out, LevelInfo)
	if err := emitter.Emit(Event{Type: EventRunStart}); !errors.Is(err, ErrMissingRunID) {
		t.Fatalf("Emit() without run_id = %v, want ErrMissingRunID", err)
	}
	if out.Len() != 0 {
		t.Fatal("a rejected event must not reach the output")
	}
}

func TestEmitterDebugGatesDetail(t *testing.T) {
	detail := map[string]any{
		"exit_code": 0,
		"branch":    "courier/resolve-issue/acme-widgets/7",
		"note":      "plain diagnostic text",
	}
	for _, test := range []struct {
		name      string
		level     Level
		wantValue any
	}{
		{name: "debug level includes detail", level: LevelDebug, wantValue: float64(0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			emitter := NewEmitter(&out, test.level)
			if err := emitter.Emit(Event{Type: EventRunExit, RunID: "run-1", Detail: detail}); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
			events := parseEventLines(t, &out)
			if len(events) != 1 {
				t.Fatalf("emitted %d events, want 1", len(events))
			}
			emitted, ok := events[0]["detail"].(map[string]any)
			if !ok {
				t.Fatalf("debug event must carry a detail object, got %T", events[0]["detail"])
			}
			if got, ok := emitted["exit_code"]; !ok || got != test.wantValue {
				t.Fatalf("detail exit_code = %v, want %v", got, test.wantValue)
			}
			if emitted["note"] != "plain diagnostic text" {
				t.Fatal("debug detail lost a plain string value")
			}
		})
	}
}

func TestEmitterInfoLevelSuppressesDetail(t *testing.T) {
	var out bytes.Buffer
	emitter := NewEmitter(&out, LevelInfo)
	err := emitter.Emit(Event{
		Type:   EventRunExit,
		RunID:  "run-1",
		Detail: map[string]any{"exit_code": 1, "branch": "courier/fix-pr/acme-widgets/9"},
	})
	if err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	events := parseEventLines(t, &out)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	if _, ok := events[0]["detail"]; ok {
		t.Fatal("info-level run must not emit verbose event detail")
	}
}

func TestEmitterPerEventVerboseOverridesInfoLevel(t *testing.T) {
	detail := map[string]any{"branch": "courier/resolve-issue/acme/widgets-7", "lane": "local"}
	for _, test := range []struct {
		name        string
		verbose     bool
		wantPresent bool
	}{
		{name: "verbose event carries detail at info level", verbose: true, wantPresent: true},
		{name: "non-verbose event omits detail at info level", verbose: false, wantPresent: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			// A process-wide info-level emitter serves runs of mixed
			// verbosity: the event itself decides whether detail ships.
			emitter := NewEmitter(&out, LevelInfo)
			if err := emitter.Emit(Event{
				Type:    EventPhaseTransition,
				RunID:   "run-1",
				Status:  "Running",
				Verbose: test.verbose,
				Detail:  detail,
			}); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
			events := parseEventLines(t, &out)
			if len(events) != 1 {
				t.Fatalf("emitted %d events, want 1", len(events))
			}
			emitted, ok := events[0]["detail"].(map[string]any)
			if ok != test.wantPresent {
				t.Fatalf("detail present = %t, want %t", ok, test.wantPresent)
			}
			if test.wantPresent && emitted["branch"] != detail["branch"] {
				t.Fatal("verbose event lost its detail values")
			}
		})
	}
}

func TestEmitterRedactsSecretsAtEveryLevel(t *testing.T) {
	detail := map[string]any{
		"authorization": "Bearer " + fakeBearerToken,
		"clone_url":     "https://" + fakeGitToken + "@example.com/acme.git",
		"query":         "https://api.example/v1?token=" + fakeDispatchKey,
		"provider_key":  fakeProviderKey,
		"forge_pat":     fakeForgeToken,
		"clean_value":   "ordinary text that must survive",
	}
	for _, level := range []Level{LevelInfo, LevelDebug} {
		t.Run(level.String(), func(t *testing.T) {
			var out bytes.Buffer
			emitter := NewEmitter(&out, level)
			emitter.Redactor().Register(fakeDispatchKey)
			emitter.Redactor().Register(fakeProviderKey)
			emitter.Redactor().Register(fakeForgeToken)
			err := emitter.Emit(Event{
				Type:   EventToolResult,
				RunID:  "run-redact",
				Repo:   "acme/widgets",
				Ref:    6,
				Mode:   "fix-pr",
				Status: StatusError,
				Detail: detail,
			})
			if err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
			text := out.String()
			for _, leaked := range []string{fakeBearerToken, fakeGitToken, fakeDispatchKey, fakeProviderKey, fakeForgeToken} {
				if strings.Contains(text, leaked) {
					t.Fatalf("level %s emitted a secret (named constant: %s); see test source, not this message", level, secretName(leaked))
				}
			}
			events := parseEventLines(t, &out)
			if len(events) != 1 {
				t.Fatalf("emitted %d events, want 1", len(events))
			}
			if level == LevelInfo {
				if _, ok := events[0]["detail"]; ok {
					t.Fatal("info level must not include detail at all")
				}
				return
			}
			emitted, ok := events[0]["detail"].(map[string]any)
			if !ok {
				t.Fatal("debug level must include a redacted detail object")
			}
			if emitted["clean_value"] != "ordinary text that must survive" {
				t.Fatal("redaction mangled a normal detail value")
			}
			for _, key := range []string{"authorization", "clone_url", "query", "provider_key", "forge_pat"} {
				value, _ := emitted[key].(string)
				if !strings.Contains(value, RedactedPlaceholder) {
					t.Fatalf("detail field %q was not redacted", key)
				}
			}
		})
	}
}

// secretName maps a fake secret constant to its identifier for failure
// messages. It exists so assertions can name what leaked without printing it.
func secretName(value string) string {
	switch value {
	case fakeBearerToken:
		return "fakeBearerToken"
	case fakeGitToken:
		return "fakeGitToken"
	case fakeDispatchKey:
		return "fakeDispatchKey"
	case fakeProviderKey:
		return "fakeProviderKey"
	case fakeForgeToken:
		return "fakeForgeToken"
	default:
		return "unknown-fake-secret"
	}
}

func TestEmitterDoesNotMutateCallerDetail(t *testing.T) {
	var out bytes.Buffer
	emitter := NewEmitter(&out, LevelDebug)
	emitter.Redactor().Register(fakeGitToken)
	detail := map[string]any{"url": "https://" + fakeGitToken + "@example.com/x.git"}
	before := detail["url"]
	if err := emitter.Emit(Event{Type: EventWorkspaceReady, RunID: "run-1", Detail: detail}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if detail["url"] != before {
		t.Fatal("Emit mutated the caller's detail map")
	}
}

func TestEmitterWritesWholeLinesUnderConcurrency(t *testing.T) {
	var out syncBuffer
	emitter := NewEmitter(&out, LevelInfo)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if err := emitter.Emit(Event{Type: EventModelCall, RunID: fmt.Sprintf("run-%d", i), Ref: j}); err != nil {
					t.Errorf("Emit() error = %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if got := len(parseEventLines(t, &out.bytes)); got != 200 {
		t.Fatalf("decoded %d concurrent events, want 200", got)
	}
}

type syncBuffer struct {
	mu    sync.Mutex
	bytes bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytes.Write(p)
}

func TestParseLevel(t *testing.T) {
	for _, test := range []struct {
		value string
		want  Level
	}{
		{value: "debug", want: LevelDebug},
		{value: "DEBUG", want: LevelDebug},
		{value: " debug ", want: LevelDebug},
		{value: "info", want: LevelInfo},
		{value: "", want: LevelInfo},
		{value: "verbose", want: LevelInfo},
	} {
		t.Run(test.value, func(t *testing.T) {
			if got := ParseLevel(test.value); got != test.want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}

func TestNilEmitterDiscardsEvents(t *testing.T) {
	var emitter *Emitter
	if err := emitter.Emit(Event{Type: EventRunStart, RunID: "run-1"}); err != nil {
		t.Fatalf("nil emitter Emit() error = %v", err)
	}
}
