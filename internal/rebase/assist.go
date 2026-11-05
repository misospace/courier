package rebase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/misospace/courier/internal/git"
)

// The model-assisted seam extends the mechanical rebase engine with one
// bounded escape hatch (#732): when the closed mechanical rules reject a
// conflicted file, a configured coordinator model may be asked — once per
// rebase episode, by the engine, from trusted text only — to produce the
// full resolved content of exactly those rejected files. The seam changes
// who writes the bytes, never what may land: the resolution is validated
// against the rejected set, the published head is still leased, and the
// bounded repository checks still run before any push. A nil ModelResolver
// in Options is the same closed-rules engine with one deliberate
// refinement: a conflicted path that is not a regular file — or not a name
// the filesystem accepts at all — stops the attempt for a human (named,
// with reason) instead of being read or written through. Every such
// episode still ends parked for a human, and no model-assisted code can
// ever run on that path.

// ModelResolver is the engine's one seam to a model. It is satisfied by an
// adapter over the harness gateway (cmd/courier-executor) so the trusted
// engine never imports a transport. A nil interface value in Options means
// deterministic-only resolution: the same closed-rules engine with one
// deliberate refinement — non-regular or unnamable conflicted paths stop
// for a human instead of being read or written through.
type ModelResolver interface {
	Resolve(ctx context.Context, req AssistRequest) (*AssistResolution, error)
}

// DefaultLockfileTimeout bounds the allowlisted lockfile regeneration the
// assist may run. It bounds that tool invocation alone — it never bounds a
// model-assisted turn, which the run's own context (SIGTERM) governs.
const DefaultLockfileTimeout = 10 * time.Minute

// DefaultAssistMaxFileBytes bounds one conflicted file's content included
// in the prompt. It is a prompt bound, not a resolution bound: a file
// larger than this is included truncated, and the mechanical parser's own
// maxConflictFileBytes still governs what the engine will accept back.
const DefaultAssistMaxFileBytes = 64 << 10

// DefaultAssistMaxDiffBytes bounds one side's diff included in the prompt.
// The prompt is evidence for the model's decision, not a full transcript;
// a diff past the bound is truncated, never silently widened.
const DefaultAssistMaxDiffBytes = 32 << 10

// maxAssistReasonBytes bounds a model refusal or validation verdict that
// reaches the semantic-conflict reason text: the reason is a human
// diagnostic, never an unbounded echo of model output.
const maxAssistReasonBytes = 512

// AssistRequest is the trusted evidence one resolution turn carries. Every
// field is repository content, diff text, or fixed instruction text
// assembled by the engine; the request carries no credentials of any kind,
// because the engine holds none — git authentication lives in the
// executor's askpass environment and never crosses this boundary.
type AssistRequest struct {
	// Goal is the run's goal text, context for why the pull request exists.
	Goal string
	// Base and Branch name the replay's endpoints; reason text only.
	Base   string
	Branch string
	// PRDiff is the bounded diff of the replayed commits against the merge
	// base — what the pull request changes.
	PRDiff string
	// BaseDiff is the bounded diff of the base-side changes restricted to
	// the conflicted paths — what the base moved underneath them.
	BaseDiff string
	// Conflicts are the mechanically-rejected conflicted files, content
	// with git's markers exactly as the rebase left them.
	Conflicts []ConflictFile
	// ToolHint names the recognized lockfile tool whose regeneration the
	// prompt explicitly permits, empty when none applies.
	ToolHint string
}

// ConflictFile is one conflicted path and its marked worktree content.
type ConflictFile struct {
	Path    string
	Content string
}

// AssistResolution is the model's answer for one turn. Files maps each
// resolved path to the full resolved file content; Refusal non-empty means
// the model declined with an honest reason, and Files are then ignored.
// Model and the token counts ride back so the engine's AssistReport records
// what actually served the turn — the engine holds no model identity of
// its own.
type AssistResolution struct {
	Files   map[string]string
	Refusal string
	Model   string
	// InputTokens/OutputTokens are the provider's usage report for the
	// turn when the transport received one; zero is an explicit absence,
	// never an invented count.
	InputTokens  int64
	OutputTokens int64
}

// AssistReport is the evidence one attempt's model-assisted turns left on
// the Result: which model served them, how many were asked, which files
// they resolved, and the token spend. Turns counts the model turns that
// returned an answer — a refusal is an answer, a transport failure is not.
// Attempted reports that a model-assisted turn was issued at all, and is
// the honest boundary: a report with Attempted false names an attempt that
// never happened, and the engine records the report only when it is true.
type AssistReport struct {
	Model         string
	Turns         int
	FilesResolved []string
	InputTokens   int64
	OutputTokens  int64
	Attempted     bool
}

// resolverUnavailableError marks a resolver transport or infrastructure
// failure. It stays distinct from a semantic conflict: an unavailable
// resolver is a failed attempt, not a request for a human decision, and
// must never be classified as NeedsHuman.
type resolverUnavailableError struct{ err error }

func (e *resolverUnavailableError) Error() string {
	return fmt.Sprintf("model-assisted resolver unavailable: %v", e.err)
}

func (e *resolverUnavailableError) Unwrap() error { return e.err }

// nonRegularConflictError marks a conflicted worktree path the engine will
// neither read nor write through: it is not a regular file — git leaves the
// symlink itself in the worktree on a symlink content conflict — or it is
// not a name the filesystem accepts at all (a NUL byte or an over-long
// name, which lstat refuses). Git cannot
// produce such tree entries, so a conflicted path like the latter is
// anomalous, and the episode stops for a human with the path named instead
// of leaking an unclassified infrastructure error. Every file read or write
// below follows symlinks, so a conflicted symlink would lift prompt content
// from, or land model bytes on, an arbitrary path outside the workspace.
// Every such episode still ends parked for a human, and no model-assisted
// code can run on that path.
type nonRegularConflictError struct {
	path string
	// reason is the per-instance why: "is not a regular file" for a symlink
	// or other non-regular entry, "is not a name the filesystem accepts" for
	// a name lstat refuses, so the human summary states the true cause.
	reason string
}

func nonRegularConflict(path string) *nonRegularConflictError {
	return &nonRegularConflictError{path: path, reason: "is not a regular file"}
}

func unnamableConflictPath(path string) *nonRegularConflictError {
	return &nonRegularConflictError{path: path, reason: "is not a name the filesystem accepts"}
}

func (e *nonRegularConflictError) Error() string {
	return fmt.Sprintf("conflicted path %s %s; the engine will not read or write through it", e.path, e.reason)
}

// lstatRefusal classifies one os.Lstat failure on a conflicted path. A name
// the operating system refuses outright — a NUL byte, or an over-long
// component refused with ENAMETOOLONG — cannot be a tree entry git produced,
// so it gets the same stop-for-a-human verdict instead of an unclassified
// infrastructure error; every other failure passes through unchanged as the
// infrastructure error it is.
func lstatRefusal(path string, err error) error {
	if strings.IndexByte(path, 0) >= 0 || errors.Is(err, syscall.ENAMETOOLONG) {
		return unnamableConflictPath(path)
	}
	return err
}

// runLockfileTidy is the allowlisted lockfile-regeneration seam. It is a
// variable so tests can stub the tool invocation without a Go toolchain
// or network; the production value runs `go mod tidy` inside the workspace.
var runLockfileTidy = func(ctx context.Context, directory string, timeout time.Duration) error {
	tidyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(tidyCtx, "go", "mod", "tidy")
	cmd.Dir = directory
	// The pins are the sandbox. GOWORK=off pins the module context to the
	// workspace alone — the same environment the bounded Go build check
	// runs under. GOTOOLCHAIN=local stops a hostile go.mod from making the
	// go command download and execute a repo-selected toolchain binary.
	// GOFLAGS=-mod=readonly governs build commands, not mod commands, and
	// is kept only for parity with that build check's environment.
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOWORK=off", "GOTOOLCHAIN=local")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go mod tidy: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// lockfileToolHint names the recognized lockfile tool a rejected set
// implicates, empty when none. The hint is what the prompt permits the
// model to regenerate and what the engine re-runs after writing the
// resolution; it is deliberately the only tool seam in the engine.
func lockfileToolHint(paths []string) string {
	for _, path := range paths {
		switch filepath.ToSlash(path) {
		case "go.mod", "go.sum":
			if _, err := exec.LookPath("go"); err == nil {
				return "go mod tidy"
			}
			return ""
		}
	}
	return ""
}

// assistRound asks the configured resolver once for the episode's rejected
// set and, on a valid resolution, writes and stages the files and lets the
// caller continue the replay. It is the rebase episode's single
// model-assisted attempt (#732): the caller invokes it at most once per
// Execute, for the first round whose rejected set the mechanical rules
// rejected. The invariant is one coordinator, sequential, no delegation:
// the engine asks exactly one model exactly one question per episode —
// there is no subagent fan-out on the rebase path, because every extra
// model turn widens the surface a replay must trust before it may publish.
func assistRound(ctx context.Context, w *git.Workspace, opts Options, origHead string, rejected []string, report *AssistReport) error {
	req, err := buildAssistRequest(ctx, w, opts, origHead, rejected)
	if err != nil {
		// A conflicted path the engine will not read through — not a
		// regular file, or not a name the filesystem accepts — stops the
		// attempt for a human, never through an error classified as
		// infrastructure: the decision is the same one a non-mechanical
		// hunk gets, and nothing has been read or written through the path.
		var nonRegular *nonRegularConflictError
		if errors.As(err, &nonRegular) {
			return &semanticError{
				files:  rejected,
				reason: assistUnresolvedReason(nonRegular.Error()),
			}
		}
		return fmt.Errorf("assemble the model-assist request: %w", err)
	}
	res, err := opts.ModelResolver.Resolve(ctx, req)
	if err != nil {
		// The ask was issued even though no answer came back: the attempt
		// happened, but a turn without an answer is not one of the model's
		// answered turns.
		report.Attempted = true
		return &resolverUnavailableError{err}
	}
	// A resolution — including a refusal — is an answered model turn.
	report.Attempted = true
	report.Turns++
	report.Model = res.Model
	report.InputTokens += res.InputTokens
	report.OutputTokens += res.OutputTokens
	refusal := strings.TrimSpace(res.Refusal)
	if refusal == "" {
		if why := invalidResolutionReason(res, rejected); why != "" {
			refusal = "the resolution was rejected before use: " + why
		}
	}
	if refusal != "" {
		// A refusal — honest or enforced by validation — is the model
		// declining to decide: the attempt stops for a human exactly as a
		// non-mechanical hunk would, with the attempt named honestly.
		return &semanticError{
			files:  rejected,
			reason: assistUnresolvedReason(refusal),
		}
	}
	for _, path := range rejected {
		content := res.Files[path]
		full := filepath.Join(w.Directory, filepath.FromSlash(path))
		info, err := os.Lstat(full)
		if err != nil {
			return fmt.Errorf("rewrite resolved %s: %w", path, err)
		}
		// Defense in depth on the write: the request build already refused
		// non-regular conflicted paths, and no other code path in the assist
		// writes to this tree. No write may travel through a link out of the
		// workspace; the lstat check narrows that window, and a link swapped
		// in between this check and the write is a residual race contained
		// by the executor pod's own process boundary, not by this check.
		if !info.Mode().IsRegular() {
			return &semanticError{
				files:  rejected,
				reason: assistUnresolvedReason(fmt.Sprintf("conflicted path %s is not a regular file; the engine will not write through it", path)),
			}
		}
		if err := os.WriteFile(full, []byte(content), info.Mode().Perm()); err != nil {
			return fmt.Errorf("rewrite resolved %s: %w", path, err)
		}
		if err := w.Stage(ctx, path); err != nil {
			return fmt.Errorf("stage resolved %s: %w", path, err)
		}
		report.FilesResolved = append(report.FilesResolved, path)
	}
	// When the rejected set implicated a recognized lockfile tool, the
	// resolution may not have regenerated the lockfile consistently with
	// the module graph the resolved content now names. The allowlisted
	// regeneration runs once, bounded, and its outputs are staged again;
	// a failure leaves the conflict unresolved — the engine never
	// publishes a lockfile it could not regenerate.
	if hint := lockfileToolHint(rejected); hint == "go mod tidy" {
		timeout := opts.LockfileTimeout
		if timeout <= 0 {
			timeout = DefaultLockfileTimeout
		}
		if err := runLockfileTidy(ctx, w.Directory, timeout); err != nil {
			return &semanticError{
				files:  rejected,
				reason: assistUnresolvedReason(fmt.Sprintf("the allowlisted %s failed after the resolution was written: %v", hint, err)),
			}
		}
		// A tidy rewrite may touch both go.mod and go.sum even when only one
		// was in the rejected set; an unstaged rewritten sibling would fail
		// the continue as an unattributable plain error. Stage both when
		// they exist — staging unchanged content is a no-op, so this stays
		// bounded and idempotent.
		for _, path := range []string{"go.mod", "go.sum"} {
			if _, err := os.Stat(filepath.Join(w.Directory, path)); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("stage regenerated %s: %w", path, err)
			}
			if err := w.Stage(ctx, path); err != nil {
				return fmt.Errorf("stage regenerated %s: %w", path, err)
			}
		}
	}
	return nil
}

// assistUnresolvedReason is the honest semantic-conflict reason for a round
// the resolver could not close: the model-assisted attempt happened and did
// not resolve, and the bounded cause rides along for the human.
func assistUnresolvedReason(cause string) string {
	return "model-assisted conflict resolution was attempted and did not resolve the conflict: " + boundText(cause, maxAssistReasonBytes)
}

// invalidResolutionReason validates a resolution against the round's
// rejected set and returns why it is unusable, empty when it is valid.
// Validation is the trust boundary: the model's answer is data, and only
// content that provably addresses exactly the rejected files, carries no
// conflict markers, and stays within the engine's own content bound is
// written — anything else is treated as a refusal, never as partial input.
// Completeness is the truncation guard: the write loop reads res.Files by
// exact path, so a resolution that omits any rejected path would publish
// that file truncated to zero bytes.
func invalidResolutionReason(res *AssistResolution, rejected []string) string {
	allowed := make(map[string]struct{}, len(rejected))
	for _, path := range rejected {
		allowed[path] = struct{}{}
	}
	named := make(map[string]struct{}, len(res.Files))
	for path, content := range res.Files {
		if _, ok := named[path]; ok {
			continue
		}
		named[path] = struct{}{}
		if _, ok := allowed[path]; !ok {
			return fmt.Sprintf("the resolution named %q, which is not one of the conflicted files", path)
		}
		if int64(len(content)) > maxConflictFileBytes {
			return fmt.Sprintf("the resolution for %q is %d bytes, over the %d-byte content bound", path, len(content), maxConflictFileBytes)
		}
		if markers := conflictMarkersIn(content); markers != "" {
			return fmt.Sprintf("the resolution for %q still carries the %s conflict marker", path, markers)
		}
	}
	for _, path := range rejected {
		if _, ok := named[path]; !ok {
			return fmt.Sprintf("the resolution is missing %q, one of the conflicted files", path)
		}
	}
	return ""
}

// conflictMarkersIn returns the first git conflict marker the content
// carries at a line start, empty when none. The list covers diff3 as well
// as merge: Rebase pins merge.conflictstyle=diff3, so a resolution that
// still carries the base section marker is not a resolution either.
func conflictMarkersIn(content string) string {
	for _, line := range strings.Split(content, "\n") {
		for _, marker := range []string{"<<<<<<<", "|||||||", "=======", ">>>>>>>"} {
			if strings.HasPrefix(line, marker) {
				return marker
			}
		}
	}
	return ""
}

// buildAssistRequest assembles the trusted evidence for one resolution
// turn: the bounded PR diff, the base-side diff restricted to the rejected
// paths, and the marked conflict contents read from the worktree. Every
// byte comes from the repository, the diffs, or fixed instruction text.
// origHead is the pre-rebase PR head recorded at attempt start — the
// conflict-time HEAD is the replay's detached state, not the PR's commits.
func buildAssistRequest(ctx context.Context, w *git.Workspace, opts Options, origHead string, rejected []string) (AssistRequest, error) {
	prDiff, err := boundedDiff(ctx, w.Directory, "diff", "--no-color", "--no-ext-diff", w.BaseRevisionRef()+"..."+origHead)
	if err != nil {
		return AssistRequest{}, fmt.Errorf("read the PR diff: %w", err)
	}
	// The base-side diff is three-dot from the replayed head: it shows the
	// base tip's changes since the merge base, restricted to the paths the
	// round rejected, which is exactly the movement the PR must absorb. The
	// rejected paths are repository-controlled names, so they travel as
	// literal anchored pathspecs — a name whose first byte is ':' must not
	// become pathspec magic.
	args := make([]string, 0, len(rejected)+5)
	args = append(args, "diff", "--no-color", "--no-ext-diff", origHead+"..."+w.BaseRevisionRef(), "--")
	for _, path := range rejected {
		args = append(args, git.LiteralPathspec(path))
	}
	baseDiff, err := boundedDiff(ctx, w.Directory, args...)
	if err != nil {
		return AssistRequest{}, fmt.Errorf("read the base-side diff: %w", err)
	}
	conflicts := make([]ConflictFile, 0, len(rejected))
	for _, path := range rejected {
		content, err := boundedFileContent(w.Directory, path)
		if err != nil {
			return AssistRequest{}, fmt.Errorf("read the conflict in %s: %w", path, err)
		}
		conflicts = append(conflicts, ConflictFile{Path: path, Content: content})
	}
	return AssistRequest{
		Goal:      opts.Goal,
		Base:      opts.Base,
		Branch:    opts.Branch,
		PRDiff:    prDiff,
		BaseDiff:  baseDiff,
		Conflicts: conflicts,
		ToolHint:  lockfileToolHint(rejected),
	}, nil
}

// boundedDiff runs one hardened, read-only git diff and bounds its output.
// The diff is prompt evidence, and its bound is the prompt bound: the
// hardened boundary's own capture cap stays the outer wall.
func boundedDiff(ctx context.Context, directory string, args ...string) (string, error) {
	out, err := git.Hardened(ctx, directory, args...)
	if err != nil {
		return "", err
	}
	return boundText(string(out), DefaultAssistMaxDiffBytes), nil
}

// boundedFileContent reads one conflicted worktree file for the prompt,
// truncated at the per-file prompt bound with the truncation named, so the
// model knows it saw a prefix rather than the whole file. The read is
// bounded at the OS handle — a huge conflicted file never enters memory
// whole — and a path that is not a regular file, or not a name the
// filesystem accepts at all, is refused as a nonRegularConflictError,
// because a read through a symlink would lift prompt content from an
// arbitrary path outside the workspace and a refused name cannot be a tree
// entry git produced.
func boundedFileContent(directory, path string) (string, error) {
	full := filepath.Join(directory, filepath.FromSlash(path))
	info, err := os.Lstat(full)
	if err != nil {
		return "", lstatRefusal(path, err)
	}
	if !info.Mode().IsRegular() {
		return "", nonRegularConflict(path)
	}
	file, err := os.Open(full)
	if err != nil {
		return "", err
	}
	defer file.Close()
	// One byte past the bound decides truncation without reading the whole
	// file; the note names the true size from the lstat of the regular
	// file, not the bounded read.
	raw := make([]byte, DefaultAssistMaxFileBytes+1)
	n, readErr := io.ReadFull(file, raw)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return "", readErr
	}
	raw = raw[:n]
	if len(raw) <= DefaultAssistMaxFileBytes {
		return string(raw), nil
	}
	return string(raw[:DefaultAssistMaxFileBytes]) +
		fmt.Sprintf("\n[... truncated: the file is %d bytes, the prompt bound is %d ...]", info.Size(), DefaultAssistMaxFileBytes), nil
}

// boundText truncates s to limit bytes.
func boundText(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit]
}

// assistPromptInstructions is the fixed instruction half of the prompt. It
// states the only two answer shapes, permits lockfile regeneration for the
// recognized tool, and tells the model the trusted engine re-checks the
// tree afterward — the model's answer is content to be validated, never a
// verdict to be trusted.
const assistPromptInstructions = `You are resolving the conflicts of one git rebase round inside a trusted, bounded engine.
Answer with exactly one JSON object and nothing else:
  {"files":[{"path":"...","content":"..."}]}
where content is the FULL resolved file content for each conflicted path, or
  {"refusal":"reason"}
when you cannot resolve the conflict honestly from the material below.
You may regenerate lockfile or version-pinned content for the recognized tool named below. The engine validates your answer, writes only the files you name, and runs the repository's own checks afterward; do not guess at content the material below does not determine.`

// Prompt renders the full prompt text for one resolution turn: the fixed
// instructions followed by the trusted evidence. The engine owns the text
// so the adapter stays a transport; nothing here carries credentials,
// because the request was assembled from repository content alone.
func (req AssistRequest) Prompt() string {
	var b strings.Builder
	b.WriteString(assistPromptInstructions)
	fmt.Fprintf(&b, "\n\nBase ref: %s\nPull request branch: %s\n", req.Base, req.Branch)
	if goal := strings.TrimSpace(req.Goal); goal != "" {
		fmt.Fprintf(&b, "\nRun goal:\n%s\n", goal)
	}
	if hint := strings.TrimSpace(req.ToolHint); hint != "" {
		fmt.Fprintf(&b, "\nRecognized tool: %s — you may regenerate its lockfile and version-pinned content.\n", hint)
	}
	if diff := strings.TrimSpace(req.PRDiff); diff != "" {
		fmt.Fprintf(&b, "\nDiff of the pull request's commits against the merge base:\n%s\n", diff)
	}
	if diff := strings.TrimSpace(req.BaseDiff); diff != "" {
		fmt.Fprintf(&b, "\nBase-side changes restricted to the conflicted paths:\n%s\n", diff)
	}
	for _, conflict := range req.Conflicts {
		fmt.Fprintf(&b, "\nConflicted file %q, content with git conflict markers:\n%s\n", conflict.Path, conflict.Content)
	}
	return b.String()
}
