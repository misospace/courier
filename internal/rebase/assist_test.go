package rebase

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeResolver records the one request each Resolve call received and answers
// with the scripted resolution, error, or nothing. It is the seam the engine
// tests drive instead of a transport.
type fakeResolver struct {
	calls int
	last  AssistRequest
	res   *AssistResolution
	err   error
}

func (f *fakeResolver) Resolve(ctx context.Context, req AssistRequest) (*AssistResolution, error) {
	f.calls++
	f.last = req
	if f.err != nil {
		return nil, f.err
	}
	return f.res, nil
}

func executeAssisted(t *testing.T, f fixture, expectedHead string, resolver ModelResolver) Result {
	t.Helper()
	result, err := Execute(context.Background(), Options{
		Repo:          "acme/widgets",
		Base:          f.base,
		Branch:        f.branch,
		ExpectedHead:  expectedHead,
		Workspace:     f.workspace,
		RemoteURL:     f.origin,
		Goal:          "keep the pull request replayable onto its live base",
		ModelResolver: resolver,
		AssistTimeout: 30 * time.Second,
		CheckTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return result
}

// A two-sided code conflict the mechanical rules reject is resolved by the
// configured model: the resolution is written and staged, the replay
// completes, the bounded checks pass, and the publication is still leased.
// The resolver's request must have carried the run goal, the PR diff, and
// the marked conflict content — the trusted evidence the prompt is built
// from — and the assist report must name the model, the turn, and the file.
func TestAssistResolvesTwoSidedConflict(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	resolver := &fakeResolver{res: &AssistResolution{
		Files:        map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 5\n\t_ = x\n}\n"},
		Model:        "litellm/test-model",
		InputTokens:  120,
		OutputTokens: 34,
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != result.Head {
		t.Fatalf("remote head = %q, want the published rebased head", got)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want exactly one turn", resolver.calls)
	}
	if resolver.last.Goal != "keep the pull request replayable onto its live base" {
		t.Fatalf("request goal = %q, want the run goal", resolver.last.Goal)
	}
	if !strings.Contains(resolver.last.PRDiff, "x := 2") {
		t.Fatalf("request PR diff is missing the PR side: %q", resolver.last.PRDiff)
	}
	if len(resolver.last.Conflicts) != 1 || resolver.last.Conflicts[0].Path != "code.go" ||
		!strings.Contains(resolver.last.Conflicts[0].Content, "<<<<<<<") {
		t.Fatalf("request conflicts = %#v, want the marked code.go conflict", resolver.last.Conflicts)
	}
	if result.Assist == nil || !result.Assist.Attempted || result.Assist.Turns != 1 ||
		result.Assist.Model != "litellm/test-model" || result.Assist.InputTokens != 120 ||
		result.Assist.OutputTokens != 34 {
		t.Fatalf("assist report = %#v, want the recorded turn", result.Assist)
	}
	if len(result.Assist.FilesResolved) != 1 || result.Assist.FilesResolved[0] != "code.go" {
		t.Fatalf("files resolved = %v, want code.go", result.Assist.FilesResolved)
	}
	data, err := os.ReadFile(filepath.Join(f.workspace, "code.go"))
	if err != nil || !strings.Contains(string(data), "x := 5") {
		t.Fatalf("resolved file = %q (%v), want the model's content", data, err)
	}
	// The publication still owes its lease: a second attempt against the
	// stale admission head must decline before any resolver turn runs.
	second := executeAssisted(t, reclone(t, f), f.head, resolver)
	if second.Outcome != OutcomeRemoteHeadMoved {
		t.Fatalf("stale restart outcome = %q (%s)", second.Outcome, second.Reason)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls after the declined attempt = %d, want no further turns", resolver.calls)
	}
}

// An honest refusal ends the attempt as a semantic conflict: the remote
// stays at the leased head, the conflict files ride the report, and the
// reason names the attempted assistance and its cause.
func TestAssistRefusalStopsForHuman(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	resolver := &fakeResolver{res: &AssistResolution{Refusal: "both sides rewrote the same function; the intent is not derivable"}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	if result.Conflict == nil || len(result.Conflict.Files) != 1 || result.Conflict.Files[0] != "code.go" {
		t.Fatalf("conflict = %#v, want code.go", result.Conflict)
	}
	if !strings.Contains(result.Reason, "model-assisted conflict resolution was attempted") ||
		!strings.Contains(result.Reason, "both sides rewrote the same function") {
		t.Fatalf("reason = %q, want the attempted-and-refused summary", result.Reason)
	}
	if result.Assist == nil || !result.Assist.Attempted || result.Assist.Turns != 1 {
		t.Fatalf("assist report = %#v, want the recorded attempt", result.Assist)
	}
}

// A resolution naming a path outside the rejected set is invalid: it is
// treated as a refusal, nothing is written, and nothing is published.
func TestAssistForeignPathIsRejected(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	resolver := &fakeResolver{res: &AssistResolution{
		Files:   map[string]string{"other.go": "package other\n"},
		Model:   "litellm/test-model",
		Refusal: "",
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	if _, err := os.Stat(filepath.Join(f.workspace, "other.go")); !os.IsNotExist(err) {
		t.Fatalf("foreign path was written: %v", err)
	}
}

// Resolution content that still carries conflict markers is not a
// resolution: it is rejected and the attempt stops unresolved.
func TestAssistMarkedContentIsRejected(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{"code.go": "package main\n\n<<<<<<< HEAD\n\tx := 3\n=======\n\tx := 2\n>>>>>>> pr\n\t_ = x\n}\n"},
		Model: "litellm/test-model",
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	if !strings.Contains(result.Reason, "rejected before use") {
		t.Fatalf("reason = %q, want the validation verdict", result.Reason)
	}
}

// A resolver that cannot be reached fails the attempt — never a semantic
// conflict — and leaves the remote exactly where the lease holds it.
func TestAssistResolverErrorFailsAttempt(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	resolver := &fakeResolver{err: errors.New("gateway unreachable")}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if !strings.Contains(result.Reason, "model-assisted resolver unavailable: gateway unreachable") {
		t.Fatalf("reason = %q, want the unavailable verdict", result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	if result.Assist == nil || !result.Assist.Attempted {
		t.Fatalf("assist report = %#v, want the attempted turn recorded", result.Assist)
	}
}

// Without a configured resolver the engine is byte-for-byte the mechanical
// one: the two-sided conflict stops as a semantic conflict and no assist
// report exists at all.
func TestNilResolverLeavesNoAssistReport(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	result := execute(t, f, f.head)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if result.Assist != nil {
		t.Fatalf("assist report = %#v, want nil for a nil resolver", result.Assist)
	}
}

// fakeHash renders a syntactically valid go.sum hash line for a fake module,
// so the resolved lockfile content parses without a network.
func fakeHash(t *testing.T, module string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(module))
	return module + " v1.0.0 h1:" + base64.RawStdEncoding.EncodeToString(sum[:]) + "\n"
}

// A two-sided lockfile conflict the model resolves is followed by the
// allowlisted regeneration: the tidy seam runs once against the workspace,
// its rewritten go.sum is staged again, and the replay publishes.
func TestAssistLockfileRunsTidyAndRestages(t *testing.T) {
	f := newFixture(t,
		map[string]string{
			"go.mod": "module example.com/lock\n\ngo 1.22\n",
			"go.sum": fakeHash(t, "example.com/base-only"),
		},
		map[string]string{
			"go.mod": "module example.com/lock\n\ngo 1.21\n",
			"go.sum": fakeHash(t, "example.com/pr-only"),
		},
	)
	origTidy := runLockfileTidy
	t.Cleanup(func() { runLockfileTidy = origTidy })
	tidyCalls := 0
	var tidyDirectory string
	runLockfileTidy = func(ctx context.Context, directory string, timeout time.Duration) error {
		tidyCalls++
		tidyDirectory = directory
		if timeout <= 0 {
			t.Fatalf("tidy ran with a non-positive timeout %v", timeout)
		}
		// Simulate the tool rewriting the lockfile after the resolution:
		// the engine must stage the regenerated bytes, not the pre-tidy ones.
		return os.WriteFile(filepath.Join(directory, "go.sum"), []byte(fakeHash(t, "example.com/tidied")), 0o644)
	}
	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{
			"go.mod": "module example.com/lock\n\ngo 1.22\n",
			"go.sum": fakeHash(t, "example.com/base-only") + fakeHash(t, "example.com/pr-only"),
		},
		Model: "litellm/test-model",
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if tidyCalls != 1 {
		t.Fatalf("tidy calls = %d, want exactly one", tidyCalls)
	}
	if tidyDirectory != f.workspace {
		t.Fatalf("tidy directory = %q, want the workspace", tidyDirectory)
	}
	data, err := os.ReadFile(filepath.Join(f.workspace, "go.sum"))
	if err != nil || !strings.Contains(string(data), "example.com/tidied") {
		t.Fatalf("go.sum = %q (%v), want the tidied content published", data, err)
	}
	if got := remoteHead(t, f); got != result.Head {
		t.Fatalf("remote head = %q, want the published head", got)
	}
}

// The engine's prompt carries repository content, diffs, and instructions
// only — no credentials of any kind. This is asserted structurally rather
// than by scanning for a planted secret: the engine holds no credential seam
// at all. Options and AssistRequest have no token, key, or URL-credential
// field; git authentication lives in the executor's askpass environment and
// never crosses the rebase engine boundary, so there is nothing to leak into
// the prompt. The assertions below pin the observable surface: the prompt is
// exactly the instruction text plus the request's repo-owned fields.
func TestAssistPromptCarriesOnlyTrustedMaterial(t *testing.T) {
	req := AssistRequest{
		Goal:     "replay the pull request",
		Base:     "main",
		Branch:   "pr-branch",
		PRDiff:   "diff --git a/code.go b/code.go",
		BaseDiff: "diff --git a/code.go b/code.go",
		Conflicts: []ConflictFile{
			{Path: "code.go", Content: "<<<<<<< HEAD\nx := 3\n=======\nx := 2\n>>>>>>> pr-branch\n"},
		},
		ToolHint: "go mod tidy",
	}
	prompt := req.Prompt()
	for _, want := range []string{"go mod tidy", "code.go", "<<<<<<<", "replay the pull request", "refusal"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt is missing %q:\n%s", want, prompt)
		}
	}
	for _, forbidden := range []string{"Bearer", "ghp_", "x-access-token", "password=", "COURIER_GIT_TOKEN", "Authorization"} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt carries credential-shaped material %q:\n%s", forbidden, prompt)
		}
	}
}
