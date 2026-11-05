package rebase

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/git"
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
		Repo:            "acme/widgets",
		Base:            f.base,
		Branch:          f.branch,
		ExpectedHead:    expectedHead,
		Workspace:       f.workspace,
		RemoteURL:       f.origin,
		Goal:            "keep the pull request replayable onto its live base",
		ModelResolver:   resolver,
		LockfileTimeout: 30 * time.Second,
		CheckTimeout:    30 * time.Second,
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

// Without a configured resolver the closed rules are the entire story: the
// two-sided conflict stops as a semantic conflict and no assist report
// exists at all.
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

// scriptedResolver answers each turn from a script and records every
// request, so a multi-round attempt can be driven hunk by hunk.
type scriptedResolver struct {
	calls int
	reqs  []AssistRequest
	reses []*AssistResolution
}

func (s *scriptedResolver) Resolve(ctx context.Context, req AssistRequest) (*AssistResolution, error) {
	s.calls++
	s.reqs = append(s.reqs, req)
	if s.calls > len(s.reses) {
		return nil, errors.New("resolution script exhausted")
	}
	return s.reses[s.calls-1], nil
}

// A resolution that answers a strict subset of the rejected set is invalid:
// the write loop resolves by exact path, so a missing file would publish
// truncated to zero bytes. The answer is refused, the remote stays at the
// lease head, and both conflicted files keep git's markers untouched.
func TestAssistSubsetResolutionIsRejected(t *testing.T) {
	f := newFixture(t,
		map[string]string{
			"code.go":  "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n",
			"notes.md": "intro\ntrack := 9\nbody\n",
		},
		map[string]string{
			"code.go":  "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n",
			"notes.md": "intro\ntrack := 2\nbody\n",
		},
	)
	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 5\n\t_ = x\n}\n"},
		Model: "litellm/test-model",
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	if !strings.Contains(result.Reason, `missing "notes.md"`) {
		t.Fatalf("reason = %q, want the named missing path", result.Reason)
	}
	// Nothing was resolved or truncated: after the abort the worktree is the
	// adopted PR branch again, so both files still hold their full PR-side
	// content, never a zero-byte shell of a half-published resolution.
	for path, want := range map[string]string{
		"code.go":  "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n",
		"notes.md": "intro\ntrack := 2\nbody\n",
	} {
		data, err := os.ReadFile(filepath.Join(f.workspace, path))
		if err != nil || string(data) != want {
			t.Fatalf("%s = %q (%v), want the untouched PR-side content", path, data, err)
		}
	}
}

// The base-side diff in the assist request must name exactly the rejected
// path, even when a sibling whose name the path would glob-match also moved
// on the base side: the rejected paths travel as literal anchored pathspecs,
// so a conflicted "glob*.txt" must not sweep a sibling "globA.txt" into the
// prompt as if the PR had to absorb it too.
func TestAssistBaseDiffRestrictedToRejectedGlobPath(t *testing.T) {
	f := newFixture(t,
		map[string]string{
			"glob*.txt": "base move\n",
			"globA.txt": "sibling base move\n",
		},
		map[string]string{
			"glob*.txt": "pr move\n",
		},
	)
	resolver := &fakeResolver{res: &AssistResolution{Refusal: "the sides are irreconcilable"}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want exactly one turn", resolver.calls)
	}
	if !strings.Contains(resolver.last.BaseDiff, "base move") {
		t.Fatalf("base-side diff is missing the rejected path's changes: %q", resolver.last.BaseDiff)
	}
	if strings.Contains(resolver.last.BaseDiff, "sibling base move") {
		t.Fatalf("base-side diff swept in the glob-matching sibling: %q", resolver.last.BaseDiff)
	}
}

// A resolution that still carries git's diff3 base marker ("|||||||") is
// not a resolution: Rebase pins merge.conflictstyle=diff3, so a model answer
// that copies the marked file back with the base section intact would
// otherwise pass validation and be published. Validation treats it as a
// refusal — nothing is written, the replay stops for a human, and the
// remote stays at the lease head.
func TestAssistResolutionWithDiff3BaseMarkerIsRejected(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 5\n\t_ = x\n}\n||||||| base content\n"},
		Model: "litellm/test-model",
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s), want the stop-for-human ending", result.Outcome, result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	if !strings.Contains(result.Reason, `still carries the ||||||| conflict marker`) {
		t.Fatalf("reason = %q, want the base marker named", result.Reason)
	}
	// Nothing was written: after the abort the worktree is the adopted PR
	// branch again, so the file still holds its full PR-side content, never
	// the resolution that still carried the base marker.
	data, err := os.ReadFile(filepath.Join(f.workspace, "code.go"))
	if err != nil || string(data) != "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n" {
		t.Fatalf("code.go = %q (%v), want the untouched PR-side content", data, err)
	}
}

// Every marker the engine refuses, including the diff3 base marker the
// rebase's own conflict style writes.
func TestConflictMarkersInCoversDiff3BaseMarker(t *testing.T) {
	for _, marker := range []string{"<<<<<<<", "|||||||", "=======", ">>>>>>>"} {
		content := "clean line\n" + marker + " marker text\nmore\n"
		if got := conflictMarkersIn(content); got != marker {
			t.Fatalf("conflictMarkersIn(%q) = %q, want %q", content, got, marker)
		}
	}
	if got := conflictMarkersIn("clean content\nwithout markers\n"); got != "" {
		t.Fatalf("conflictMarkersIn = %q, want empty", got)
	}
}

// A conflicted path that is not a clean repository-relative path inside the
// workspace never reaches the resolver or the filesystem: the engine refuses
// it as a semantic conflict naming the path before anything is read or
// written, and the outside target the path would reach is untouched.
func TestTraversalConflictedPathStopsForHuman(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	// The file "../outside" would name, one level above the workspace,
	// holds a sentinel: if anything read or wrote through the traversal
	// path, the sentinel would move.
	outside := filepath.Join(filepath.Dir(f.workspace), "outside")
	writeFile(t, outside, "outside-sentinel\n")
	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{"../outside": "should never be written"},
		Model: "litellm/test-model",
	}}
	err := resolveConflicts(context.Background(),
		&git.Workspace{Directory: f.workspace, Base: f.base},
		Options{ModelResolver: resolver}, f.head,
		[]string{"../outside"}, &AssistReport{})
	var semantic *semanticError
	if !errors.As(err, &semantic) {
		t.Fatalf("err = %v, want a semantic conflict — the NeedsHuman-classified ending", err)
	}
	if len(semantic.files) != 1 || semantic.files[0] != "../outside" {
		t.Fatalf("conflict files = %v, want the traversal path named", semantic.files)
	}
	if !strings.Contains(semantic.reason, "not a repository-relative path inside the workspace") {
		t.Fatalf("reason = %q, want the repository-relative verdict", semantic.reason)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls = %d, want no model turn at all", resolver.calls)
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "outside-sentinel\n" {
		t.Fatalf("outside target = %q (%v), want the sentinel untouched", data, err)
	}
}

// A symlink content conflict leaves the link itself in the worktree: the
// engine must refuse to read or write through it, stop the round for a
// human as an unresolved semantic conflict, and leave the link's outside
// target exactly as it was. The model is never asked.
func TestAssistSymlinkConflictStopsForHuman(t *testing.T) {
	origin := t.TempDir()
	runGit(t, origin, "init", "--initial-branch=main")
	runGit(t, origin, "config", "user.name", "Courier Rebase Test")
	runGit(t, origin, "config", "user.email", "courier-rebase-test@example.invalid")
	writeFile(t, filepath.Join(origin, "payload.txt"), "base payload\n")
	if err := os.Symlink("payload.txt", filepath.Join(origin, "link")); err != nil {
		t.Fatal(err)
	}
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "base")
	runGit(t, origin, "checkout", "-b", "pr-branch")
	_ = os.Remove(filepath.Join(origin, "link"))
	if err := os.Symlink("pr-payload.txt", filepath.Join(origin, "link")); err != nil {
		t.Fatal(err)
	}
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "pr retarget")
	runGit(t, origin, "checkout", "main")
	// The main side retargets the link at a relative path that escapes the
	// workspace: a read or write through the link would reach outside it.
	_ = os.Remove(filepath.Join(origin, "link"))
	if err := os.Symlink(filepath.Join("..", "outside", "escape.txt"), filepath.Join(origin, "link")); err != nil {
		t.Fatal(err)
	}
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "main retarget")
	head := runGit(t, origin, "rev-parse", "pr-branch")

	workspace := filepath.Join(t.TempDir(), "ws")
	if _, err := git.Prepare(context.Background(), git.PrepareOptions{
		RemoteURL: origin, Directory: workspace, Base: "main", Branch: "pr-branch", SkipBaseSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	// The escape target exists and holds a sentinel before the attempt runs:
	// a stat or read through the link must find it, so the test proves the
	// engine refused the path rather than failing on a dangling link — and
	// if anything read or wrote through the link, the sentinel would move.
	outside := filepath.Join(filepath.Dir(workspace), "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(outside, "escape.txt")
	writeFile(t, target, "escape-sentinel\n")
	f := fixture{origin: origin, workspace: workspace, base: "main", branch: "pr-branch", head: head}

	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{"link": "should never be written"},
		Model: "litellm/test-model",
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if !strings.Contains(result.Reason, "not a regular file") ||
		!strings.Contains(result.Reason, "link") {
		t.Fatalf("reason = %q, want the non-regular-file verdict naming the path", result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls = %d, want no model turn at all", resolver.calls)
	}
	if result.Assist != nil {
		t.Fatalf("assist report = %#v, want none: no ask was issued", result.Assist)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "escape-sentinel\n" {
		t.Fatalf("outside target = %q (%v), want the sentinel untouched", data, err)
	}
}

// A conflicted path whose name carries a NUL byte is refused the same way a
// symlink is: the operating system rejects the name outright (EINVAL), and
// git cannot produce such a tree entry, so the path is anomalous rather than
// infrastructural. resolveFile must return the non-regular stop-for-a-human
// verdict and write nothing into the workspace.
func TestResolveFileRefusesNullBytePath(t *testing.T) {
	directory := t.TempDir()
	resolved, err := resolveFile(directory, "bad\x00name")
	var nonRegular *nonRegularConflictError
	if !errors.As(err, &nonRegular) || nonRegular.path != "bad\x00name" {
		t.Fatalf("err = %v, want nonRegularConflictError naming the path", err)
	}
	if resolved {
		t.Fatal("resolveFile resolved a path the filesystem refuses")
	}
	if !strings.Contains(nonRegular.Error(), "not a name the filesystem accepts") {
		t.Fatalf("err = %q, want the verdict that names the refused name", nonRegular.Error())
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("directory entries = %v (%v), want nothing written", entries, err)
	}
}

// boundedFileContent refuses a NUL-byte path with the same verdict: the
// prompt can never carry content lifted through a name the OS refuses.
func TestBoundedFileContentRefusesNullBytePath(t *testing.T) {
	_, err := boundedFileContent(t.TempDir(), "bad\x00name")
	var nonRegular *nonRegularConflictError
	if !errors.As(err, &nonRegular) || nonRegular.path != "bad\x00name" {
		t.Fatalf("err = %v, want nonRegularConflictError naming the path", err)
	}
}

// At the episode level a NUL-byte conflicted path ends as the same semantic
// conflict a non-regular path gets — the NeedsHuman-classified ending —
// naming the path, with the model never asked. Git itself can never stage
// such a tree entry, which is precisely why the path is anomalous, so the
// round loop's seam is driven directly against a prepared workspace with the
// conflicted path list Execute would otherwise have received from git.
func TestNullByteConflictedPathStopsForHuman(t *testing.T) {
	f := newFixture(t,
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n"},
		map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n"},
	)
	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{"bad\x00name": "should never be written"},
		Model: "litellm/test-model",
	}}
	err := resolveConflicts(context.Background(),
		&git.Workspace{Directory: f.workspace, Base: f.base},
		Options{ModelResolver: resolver}, f.head,
		[]string{"code.go", "bad\x00name"}, &AssistReport{})
	var semantic *semanticError
	if !errors.As(err, &semantic) {
		t.Fatalf("err = %v, want a semantic conflict — the NeedsHuman-classified ending", err)
	}
	if len(semantic.files) != 1 || semantic.files[0] != "bad\x00name" {
		t.Fatalf("conflict files = %v, want the anomalous path named", semantic.files)
	}
	if !strings.Contains(semantic.reason, "bad\x00name") {
		t.Fatalf("reason = %q, want the anomalous path named", semantic.reason)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls = %d, want no model turn at all", resolver.calls)
	}
}

// The lockfile hint fires on go.sum alone, and the tidy rewrite touches
// go.mod — a sibling the rejected set never named. Both must be staged, or
// RebaseContinue fails as an unattributable error; with both staged the
// replay publishes the tidied content.
func TestAssistTidyRestagesRewrittenSibling(t *testing.T) {
	f := newFixture(t,
		map[string]string{
			"go.mod": "module example.com/lock\n\ngo 1.22\n",
			"go.sum": fakeHash(t, "example.com/base-only"),
		},
		map[string]string{
			"go.mod": "module example.com/lock\n\ngo 1.22\n",
			"go.sum": fakeHash(t, "example.com/pr-only"),
		},
	)
	origTidy := runLockfileTidy
	t.Cleanup(func() { runLockfileTidy = origTidy })
	runLockfileTidy = func(ctx context.Context, directory string, timeout time.Duration) error {
		// The tool rewrote the go.mod sibling: it was never in the rejected
		// set, and an unstaged rewrite would fail the replay's continue.
		return os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.com/lock\n\ngo 1.23\n"), 0o644)
	}
	resolver := &fakeResolver{res: &AssistResolution{
		Files: map[string]string{
			"go.sum": fakeHash(t, "example.com/base-only") + fakeHash(t, "example.com/pr-only"),
		},
		Model: "litellm/test-model",
	}}
	result := executeAssisted(t, f, f.head, resolver)
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if len(resolver.last.Conflicts) != 1 || resolver.last.Conflicts[0].Path != "go.sum" {
		t.Fatalf("rejected set = %#v, want go.sum alone", resolver.last.Conflicts)
	}
	mod, err := os.ReadFile(filepath.Join(f.workspace, "go.mod"))
	if err != nil || !strings.Contains(string(mod), "go 1.23") {
		t.Fatalf("go.mod = %q (%v), want the tidied sibling staged and published", mod, err)
	}
	sum, err := os.ReadFile(filepath.Join(f.workspace, "go.sum"))
	if err != nil || !strings.Contains(string(sum), "example.com/pr-only") {
		t.Fatalf("go.sum = %q (%v), want the resolved content published", sum, err)
	}
	if got := remoteHead(t, f); got != result.Head {
		t.Fatalf("remote head = %q, want the published head", got)
	}
}

// The episode's single model-assisted attempt (#732): the first conflicting
// round spends it — the valid resolution is written and staged, the replay
// continues — and the second round that rejects again stops for a human with
// its own conflicts named and a reason that honestly reports the spent
// attempt. One attempt per episode, never one per round. internal/git's
// RebaseContinue classifies a stop on the next replayed commit exactly like
// Rebase's, so the round loop sees the second round.
func TestAssistSecondRoundStopsAfterAttemptSpent(t *testing.T) {
	origin := t.TempDir()
	runGit(t, origin, "init", "--initial-branch=main")
	runGit(t, origin, "config", "user.name", "Courier Rebase Test")
	runGit(t, origin, "config", "user.email", "courier-rebase-test@example.invalid")
	writeFile(t, filepath.Join(origin, "shared.txt"), "one\ntwo\nthree\n")
	writeFile(t, filepath.Join(origin, "code.go"), "package main\n\nfunc main() {\n\tx := 1\n\t_ = x\n}\n")
	writeFile(t, filepath.Join(origin, "notes.md"), "intro\ntrack := 0\nbody\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "base")
	runGit(t, origin, "checkout", "-b", "pr-branch")
	writeFile(t, filepath.Join(origin, "code.go"), "package main\n\nfunc main() {\n\tx := 2\n\t_ = x\n}\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "pr: code")
	writeFile(t, filepath.Join(origin, "notes.md"), "intro\ntrack := 2\nbody\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "pr: notes")
	runGit(t, origin, "checkout", "main")
	writeFile(t, filepath.Join(origin, "code.go"), "package main\n\nfunc main() {\n\tx := 3\n\t_ = x\n}\n")
	writeFile(t, filepath.Join(origin, "notes.md"), "intro\ntrack := 9\nbody\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "main move")
	head := runGit(t, origin, "rev-parse", "pr-branch")

	workspace := filepath.Join(t.TempDir(), "ws")
	if _, err := git.Prepare(context.Background(), git.PrepareOptions{
		RemoteURL: origin, Directory: workspace, Base: "main", Branch: "pr-branch", SkipBaseSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	f := fixture{origin: origin, workspace: workspace, base: "main", branch: "pr-branch", head: head}

	resolver := &scriptedResolver{reses: []*AssistResolution{
		{Files: map[string]string{"code.go": "package main\n\nfunc main() {\n\tx := 5\n\t_ = x\n}\n"}, Model: "litellm/test-model"},
		{Files: map[string]string{"notes.md": "intro\ntrack := 7\nbody\n"}, Model: "litellm/test-model"},
	}}
	result := executeAssisted(t, f, head, resolver)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want exactly one attempt per episode", resolver.calls)
	}
	if len(resolver.reqs[0].Conflicts) != 1 || resolver.reqs[0].Conflicts[0].Path != "code.go" ||
		!strings.Contains(resolver.reqs[0].Conflicts[0].Content, "<<<<<<<") {
		t.Fatalf("round 1 conflicts = %#v, want the marked code.go conflict", resolver.reqs[0].Conflicts)
	}
	if result.Assist == nil || !result.Assist.Attempted || result.Assist.Turns != 1 ||
		len(result.Assist.FilesResolved) != 1 || result.Assist.FilesResolved[0] != "code.go" {
		t.Fatalf("assist report = %#v, want the one answered turn that resolved code.go", result.Assist)
	}
	if result.Conflict == nil || len(result.Conflict.Files) != 1 || result.Conflict.Files[0] != "notes.md" {
		t.Fatalf("conflict = %#v, want the second round's notes.md", result.Conflict)
	}
	if !strings.Contains(result.Reason, "already spent") {
		t.Fatalf("reason = %q, want the spent-attempt verdict", result.Reason)
	}
	if got := remoteHead(t, f); got != head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
}

// The production tidy seam against a real go command: a minimal valid
// module tidies clean, and a malformed go.mod fails with the tidy named.
func TestRunLockfileTidyProduction(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go toolchain in PATH")
	}
	t.Run("minimal module", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/tidyprobe\n")
		if err := runLockfileTidy(context.Background(), dir, time.Minute); err != nil {
			t.Fatalf("tidy: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err != nil || !strings.HasPrefix(string(data), "module example.com/tidyprobe") {
			t.Fatalf("go.mod = %q (%v), want the module line preserved", data, err)
		}
	})
	t.Run("malformed module", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "go.mod"), "not a go.mod at all\n")
		err := runLockfileTidy(context.Background(), dir, time.Minute)
		if err == nil || !strings.Contains(err.Error(), "go mod tidy") {
			t.Fatalf("err = %v, want a tidy failure", err)
		}
	})
}

// The mechanical path refuses a symlink-conflicted path exactly as the
// assist path does: no read or write may travel through the link outside the
// workspace, and the attempt stops for a human with the path named — the
// outside sentinel is untouched and the remote stays at the lease head.
func TestMechanicalSymlinkConflictStopsForHuman(t *testing.T) {
	origin := t.TempDir()
	runGit(t, origin, "init", "--initial-branch=main")
	runGit(t, origin, "config", "user.name", "Courier Rebase Test")
	runGit(t, origin, "config", "user.email", "courier-rebase-test@example.invalid")
	writeFile(t, filepath.Join(origin, "shared.txt"), "one\ntwo\nthree\n")
	writeFile(t, filepath.Join(origin, "payload.txt"), "pr payload\n")
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "base")
	runGit(t, origin, "checkout", "-b", "pr-branch")
	if err := os.Symlink("pr-payload.txt", filepath.Join(origin, "link")); err != nil {
		t.Fatal(err)
	}
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "pr retarget")
	runGit(t, origin, "checkout", "main")
	// The main side retargets the link at a relative path that escapes the
	// workspace: a read or write through the link would reach outside it.
	_ = os.Remove(filepath.Join(origin, "link"))
	if err := os.Symlink(filepath.Join("..", "outside", "escape.txt"), filepath.Join(origin, "link")); err != nil {
		t.Fatal(err)
	}
	runGit(t, origin, "add", "--all", "--", ".")
	runGit(t, origin, "commit", "-m", "main retarget")
	head := runGit(t, origin, "rev-parse", "pr-branch")

	workspace := filepath.Join(t.TempDir(), "ws")
	if _, err := git.Prepare(context.Background(), git.PrepareOptions{
		RemoteURL: origin, Directory: workspace, Base: "main", Branch: "pr-branch", SkipBaseSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(filepath.Dir(workspace), "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(outside, "escape.txt")
	writeFile(t, target, "escape-sentinel\n")
	f := fixture{origin: origin, workspace: workspace, base: "main", branch: "pr-branch", head: head}

	result := execute(t, f, f.head)
	if result.Outcome != OutcomeSemanticConflict {
		t.Fatalf("outcome = %q (%s)", result.Outcome, result.Reason)
	}
	if !strings.Contains(result.Reason, "not a regular file") {
		t.Fatalf("reason = %q, want the non-regular-file verdict", result.Reason)
	}
	if got := remoteHead(t, f); got != f.head {
		t.Fatalf("remote head = %q, want the untouched lease head", got)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "escape-sentinel\n" {
		t.Fatalf("outside target = %q (%v), want the sentinel untouched", data, err)
	}
}
