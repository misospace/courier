package skillcanary

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/skills"
)

// checkNames returns the claims the report carries, in order.
func checkNames(report *Report) []string {
	names := make([]string, 0, len(report.Checks))
	for _, check := range report.Checks {
		names = append(names, check.Name)
	}
	return names
}

// failedChecks renders the claims that failed, with their evidence.
func failedChecks(report *Report) string {
	var failed []string
	for _, check := range report.Checks {
		if !check.Passed {
			failed = append(failed, check.Name+": "+check.Detail)
		}
	}
	return strings.Join(failed, "; ")
}

// checkByName returns the named check.
func checkByName(t *testing.T, report *Report, name string) Check {
	t.Helper()
	for _, check := range report.Checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("the report carries no check %q", name)
	return Check{}
}

// TestCanaryPassesEveryCheck runs the canary against its own synthetic
// deployment tree and asserts that every claim the chain makes holds.
func TestCanaryPassesEveryCheck(t *testing.T) {
	report, err := Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("the canary failed: %s", failedChecks(report))
	}
	want := claims
	got := checkNames(report)
	if len(got) != len(want) {
		t.Fatalf("the report carries %d checks, want %d: %v", len(got), len(want), got)
	}
	for i, name := range want {
		if got[i] != name {
			t.Fatalf("check %d is %q, want %q", i, got[i], name)
		}
	}
	if report.Marker == "" {
		t.Fatal("the report carries no marker")
	}
	if !strings.HasPrefix(report.Marker, markerPrefix) {
		t.Fatalf("the marker %q does not carry the %q prefix", report.Marker, markerPrefix)
	}
	if report.Digest == "" || !digestShape.MatchString(report.Digest) {
		t.Fatalf("the report carries no content digest: %q", report.Digest)
	}
	if report.Skill != canaryBundleName {
		t.Fatalf("the report names skill %q, want %q", report.Skill, canaryBundleName)
	}
	// The marker must have travelled from the bundle body into the model's
	// own declaration: that is the claim the canary exists to prove.
	declaration := checkByName(t, report, CheckModelUsed)
	if !strings.Contains(declaration.Detail, report.Marker) {
		t.Fatalf("the final declaration does not quote the marker: %s", declaration.Detail)
	}
	// The transcript must carry the three lines an operator reads.
	transcript := strings.Join(report.Transcript, "\n")
	for _, want := range []string{"skill index:", "skill tool result:", "final declaration:"} {
		if !strings.Contains(transcript, want) {
			t.Fatalf("the transcript carries no %q line: %s", want, transcript)
		}
	}
}

// TestCanaryFailsWhenTheModelDoesNotQuoteTheMarker runs the canary against
// a model that never used the instruction: the declaration carries no
// marker, so the claim that the model used the skill must fail while every
// other claim still holds.
func TestCanaryFailsWhenTheModelDoesNotQuoteTheMarker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/chat/completions") {
			http.Error(w, "the endpoint serves POST /chat/completions", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if !modelCalledSkill(r) {
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{
				Index: 0,
				Delta: streamDelta{ToolCalls: []streamToolCall{{
					Index:    0,
					ID:       "call-skill-1",
					Type:     "function",
					Function: streamFunction{Name: harness.ToolSkill, Arguments: skillArguments(canaryBundleName)},
				}}},
			}}}))
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{Index: 0, FinishReason: "tool_calls"}}}))
		} else {
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{
				Index: 0,
				Delta: streamDelta{Content: `{"outcome":"no_change_needed","evidence":"the pinned skill was loaded"}`},
			}}}))
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{Index: 0, FinishReason: "stop"}}}))
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	report, err := Run(context.Background(), Options{GatewayURL: server.URL})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Passed() {
		t.Fatal("the canary passed a model that never quoted the marker")
	}
	if check := checkByName(t, report, CheckModelUsed); check.Passed {
		t.Fatal("the model-used-the-instruction check passed a declaration without the marker")
	}
	for _, check := range report.Checks {
		if check.Name == CheckModelUsed {
			continue
		}
		if !check.Passed {
			t.Fatalf("the %q check failed: %s", check.Name, check.Detail)
		}
	}
}

// modelCalledSkill reports whether the request already carries the skill
// tool's result, which is where the model's second turn begins.
func modelCalledSkill(r *http.Request) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRecordedBodyBytes))
	r.Body.Close()
	if err != nil {
		return false
	}
	request, err := decodeRequest(body)
	if err != nil {
		return false
	}
	for _, message := range request.Messages {
		if message.Role == "tool" {
			return true
		}
	}
	return false
}

// TestCanaryFailsClosedOnADigestMismatch runs the canary with a run whose
// admitted pin no longer matches the registered content: the control-side
// load must fail closed before the coordinator ever reaches the model.
func TestCanaryFailsClosedOnADigestMismatch(t *testing.T) {
	report, err := Run(context.Background(), Options{
		Pins: []skills.Pin{{
			Name:   canaryBundleName,
			Digest: "sha256:" + strings.Repeat("0", 64),
		}},
	})
	if err == nil {
		t.Fatalf("the canary passed a pin that does not match the registered content: %s", failedChecks(report))
	}
	if !strings.Contains(err.Error(), "does not match the run's pinned digest") {
		t.Fatalf("the failure does not name the digest mismatch: %v", err)
	}
	if check := checkByName(t, report, CheckControlLoad); check.Passed {
		t.Fatal("the control-side load check passed a mismatched pin")
	}
	if check := checkByName(t, report, CheckSelected); !check.Passed {
		t.Fatalf("the lane selection check failed: %s", check.Detail)
	}
}

// TestCanaryRunsAgainstADeploymentBundle runs the canary against a bundle
// the deployment provides — the mode an operator uses — and asserts that
// the same claims hold.
func TestCanaryRunsAgainstADeploymentBundle(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "deployment-skill")
	if err := os.MkdirAll(filepath.Join(bundle, "assets"), 0o755); err != nil {
		t.Fatalf("create the bundle directory: %v", err)
	}
	marker := markerPrefix + "0123456789abcdef"
	document := fmt.Sprintf(`---
name: deployment-skill
description: deployment-provided test skill
---

The deployment test skill carries the marker %s and one supporting asset.
`, marker)
	if err := os.WriteFile(filepath.Join(bundle, "SKILL.md"), []byte(document), 0o644); err != nil {
		t.Fatalf("write SKILL.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "assets", "notes.md"), []byte("# Notes\n"), 0o644); err != nil {
		t.Fatalf("write the bundle asset: %v", err)
	}
	config := fmt.Sprintf("bundles:\n- name: deployment-skill\n  source: %s\n", yamlScalar(bundle))
	configPath := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("write the bundle config: %v", err)
	}

	report, err := Run(context.Background(), Options{
		BundleConfig: configPath,
		Skills:       []string{"deployment-skill"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("the canary failed against the deployment bundle: %s", failedChecks(report))
	}
	if report.Skill != "deployment-skill" {
		t.Fatalf("the report names skill %q, want %q", report.Skill, "deployment-skill")
	}
	if report.Marker != marker {
		t.Fatalf("the report carries marker %q, want %q", report.Marker, marker)
	}
	// A deployment's instruction body is not the canary's own: the report
	// must not print it.
	transcript := strings.Join(report.Transcript, "\n")
	if strings.Contains(transcript, "The deployment test skill carries the marker") {
		t.Fatalf("the transcript printed a deployment instruction body: %s", transcript)
	}
	if !strings.Contains(transcript, "skill tool result:") {
		t.Fatalf("the transcript carries no skill tool result line: %s", transcript)
	}
}

// TestCanaryFailsOnAnUnknownSkill asserts that a lane selection naming a
// bundle the deployment does not register fails at selection rather than
// silently loading nothing.
func TestCanaryFailsOnAnUnknownSkill(t *testing.T) {
	report, err := Run(context.Background(), Options{Skills: []string{"not-registered"}})
	if err == nil {
		t.Fatalf("the canary passed a skill the deployment does not register: %s", failedChecks(report))
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("the failure does not name the unregistered skill: %v", err)
	}
	if check := checkByName(t, report, CheckSelected); check.Passed {
		t.Fatal("the lane selection check passed an unregistered skill")
	}
}

// TestCanaryRunsAgainstAGatewayEndpoint runs the canary against an
// OpenAI-compatible endpoint the operator names — the mode a deployment
// uses — and asserts that the same claims hold and that the gateway key
// reached the endpoint.
func TestCanaryRunsAgainstAGatewayEndpoint(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "gateway-skill")
	if err := os.MkdirAll(filepath.Join(bundle, "assets"), 0o755); err != nil {
		t.Fatalf("create the bundle directory: %v", err)
	}
	marker := markerPrefix + "fedcba9876543210"
	document := fmt.Sprintf(`---
name: gateway-skill
description: gateway-provided test skill
---

The gateway test skill carries the marker %s and one supporting asset.
`, marker)
	if err := os.WriteFile(filepath.Join(bundle, "SKILL.md"), []byte(document), 0o644); err != nil {
		t.Fatalf("write SKILL.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "assets", "notes.md"), []byte("# Notes\n"), 0o644); err != nil {
		t.Fatalf("write the bundle asset: %v", err)
	}
	config := fmt.Sprintf("bundles:\n- name: gateway-skill\n  source: %s\n", yamlScalar(bundle))
	configPath := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("write the bundle config: %v", err)
	}
	keyPath := filepath.Join(dir, "gateway.key")
	if err := os.WriteFile(keyPath, []byte("canary-key\n"), 0o600); err != nil {
		t.Fatalf("write the gateway key: %v", err)
	}

	var (
		mu            sync.Mutex
		turns         int
		authorization string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		turn := turns
		turns++
		authorization = r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if turn == 0 {
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{
				Index: 0,
				Delta: streamDelta{ToolCalls: []streamToolCall{{
					Index:    0,
					ID:       "call-skill-1",
					Type:     "function",
					Function: streamFunction{Name: harness.ToolSkill, Arguments: skillArguments("gateway-skill")},
				}}},
			}}}))
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{Index: 0, FinishReason: "tool_calls"}}}))
		} else {
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{
				Index: 0,
				Delta: streamDelta{Content: stubDeclaration("gateway-skill", marker)},
			}}}))
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{Index: 0, FinishReason: "stop"}}}))
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	report, err := Run(context.Background(), Options{
		BundleConfig:   configPath,
		Skills:         []string{"gateway-skill"},
		GatewayURL:     server.URL,
		GatewayKeyFile: keyPath,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("the canary failed against the gateway endpoint: %s", failedChecks(report))
	}
	mu.Lock()
	defer mu.Unlock()
	if authorization != "Bearer canary-key" {
		t.Fatalf("the gateway saw authorization %q, want the key from the key file", authorization)
	}
}

// TestCanaryFailsWhenTheCheckoutChanges runs the canary against a model that
// writes into the run's checkout while the run is in flight. The claim that
// the target checkout was not modified must fail, because the checkout's
// content changed.
func TestCanaryFailsWhenTheCheckoutChanges(t *testing.T) {
	checkout := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := os.WriteFile(filepath.Join(checkout, "notes.txt"), []byte("changed\n"), 0o644); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if !modelCalledSkill(r) {
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{
				Index: 0,
				Delta: streamDelta{ToolCalls: []streamToolCall{{
					Index:    0,
					ID:       "call-skill-1",
					Type:     "function",
					Function: streamFunction{Name: harness.ToolSkill, Arguments: skillArguments(canaryBundleName)},
				}}},
			}}}))
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{Index: 0, FinishReason: "tool_calls"}}}))
		} else {
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{
				Index: 0,
				Delta: streamDelta{Content: `{"outcome":"no_change_needed","evidence":"the pinned skill was loaded"}`},
			}}}))
			fmt.Fprint(w, sseFrame(streamChunk{Choices: []streamChoice{{Index: 0, FinishReason: "stop"}}}))
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	report, err := Run(context.Background(), Options{GatewayURL: server.URL, CheckoutDir: checkout})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if check := checkByName(t, report, CheckCheckout); check.Passed {
		t.Fatal("the checkout check passed a checkout the run changed")
	}
	if check := checkByName(t, report, CheckCheckout); !strings.Contains(check.Detail, "the checkout changed") {
		t.Fatalf("the checkout check does not name the change: %s", check.Detail)
	}
}

// TestMarkerComesFromTheRequest pins the stub's source of truth: the marker
// it quotes is the one the request carried, not one it was given.
func TestMarkerComesFromTheRequest(t *testing.T) {
	marker := markerPrefix + "aabbccdd"
	withTool := recordedRequest{Messages: []recordedMessage{
		{Role: "system", Content: "the goal"},
		{Role: "tool", Content: "the instruction body carries " + marker},
	}}
	encoded, err := json.Marshal(withTool)
	if err != nil {
		t.Fatal(err)
	}
	if got := markerFromRequest(encoded); got != marker {
		t.Fatalf("the marker read from the request = %q, want %q", got, marker)
	}
	withoutTool, err := json.Marshal(recordedRequest{Messages: []recordedMessage{
		{Role: "system", Content: "the goal"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := markerFromRequest(withoutTool); got != "" {
		t.Fatalf("the marker read from a request without a tool result = %q, want none", got)
	}
}

// TestCanaryResolvesTheOperatorRenderedRunConfig runs the canary against the
// per-run bundle config the operator renders for a run's pods: the
// materialization form, which names the exact files the projection carries.
func TestCanaryResolvesTheOperatorRenderedRunConfig(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "rendered-skill")
	if err := os.MkdirAll(filepath.Join(bundle, "assets"), 0o755); err != nil {
		t.Fatalf("create the bundle directory: %v", err)
	}
	marker := markerPrefix + "99887766"
	document := fmt.Sprintf(`---
name: rendered-skill
description: operator-rendered test skill
---

The operator-rendered test skill carries the marker %s and one supporting asset.
`, marker)
	if err := os.WriteFile(filepath.Join(bundle, "SKILL.md"), []byte(document), 0o644); err != nil {
		t.Fatalf("write SKILL.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "assets", "notes.md"), []byte("# Notes\n"), 0o644); err != nil {
		t.Fatalf("write the bundle asset: %v", err)
	}
	config := fmt.Sprintf("bundles:\n- name: rendered-skill\n  source: %s\n", yamlScalar(bundle))
	configPath := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("write the bundle config: %v", err)
	}
	rendered, err := skills.RenderRunConfig([]skills.MaterializedBundle{{
		Name:   "rendered-skill",
		Source: bundle,
		Files:  []string{"SKILL.md", "assets/notes.md"},
	}})
	if err != nil {
		t.Fatalf("render the run config: %v", err)
	}
	runConfigPath := filepath.Join(dir, "run-bundles.yaml")
	if err := os.WriteFile(runConfigPath, rendered, 0o644); err != nil {
		t.Fatalf("write the run config: %v", err)
	}

	report, err := Run(context.Background(), Options{
		BundleConfig: configPath,
		RunConfig:    runConfigPath,
		Skills:       []string{"rendered-skill"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("the canary failed against the operator-rendered run config: %s", failedChecks(report))
	}
	if report.Skill != "rendered-skill" {
		t.Fatalf("the report names skill %q, want %q", report.Skill, "rendered-skill")
	}
}

// TestCanaryStopsWhenTheGatewayNeverAnswers asserts that a gateway that
// never answers ends the canary within its bound instead of holding it open.
func TestCanaryStopsWhenTheGatewayNeverAnswers(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer func() {
		close(release)
		server.Close()
	}()

	report, err := Run(context.Background(), Options{GatewayURL: server.URL, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Passed() {
		t.Fatal("the canary passed a gateway that never answered")
	}
	if check := checkByName(t, report, CheckNoPublication); check.Passed {
		t.Fatalf("the ending check passed a run whose gateway never answered: %s", check.Detail)
	}
}
