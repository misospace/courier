package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/broker"
)

// evidenceProbeRepo builds the throwaway repository the #367 referential
// check reads: a run workspace with a committed tree the check can probe
// at an exact head. It reuses this file's git helpers (runGit,
// configureGit, write, commit) instead of a second harness, and returns
// the workspace directory and the head commit to probe.
func evidenceProbeRepo(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	repo := filepath.Join(root, "workspace")
	runGit(t, root, "init", repo)
	configureGit(t, repo)
	runGit(t, repo, "branch", "-M", "main")
	if err := os.MkdirAll(filepath.Join(repo, "tests"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(repo, ".github", "workflows"), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(repo, "tests", "test_packaging.py"), "def test_packaging():\n    pass\n")
	write(t, filepath.Join(repo, ".github", "workflows", "ci.yaml"), "on: push\n")
	write(t, filepath.Join(repo, "Dockerfile"), "FROM scratch\n")
	commit(t, repo, "test: workspace with citable files")
	head, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("read workspace head: %v", err)
	}
	return repo, strings.TrimSpace(string(head))
}

// newEvidenceProbeReporter builds a reporter whose run workspace is the
// throwaway repository, so the probe reads a real tree through the real
// hardened-git boundary instead of a stub.
func newEvidenceProbeReporter(t *testing.T, directory string) reporter {
	t.Helper()
	var output, errorsOut bytes.Buffer
	return newReporter(&output, &errorsOut, config{
		Mode:      "resolve-issue",
		Repo:      "misospace/miso-gallery",
		Ref:       502,
		Directory: directory,
	})
}

// oneRowReport wraps one evidence row in the parsed report the check
// reads, so each table case below is exactly one citation claim.
func oneRowReport(status broker.CriterionStatus, reference string) *broker.CriteriaReport {
	return &broker.CriteriaReport{Records: []broker.CriterionRecord{
		{ID: "AC-1", Status: status, Reference: reference},
	}}
}

// TestCriterionEvidenceReferentialCheckAgainstPublishedHead pins the
// #367 referential check against a real repository: a file path cited by
// an implementation-evidence or regression-check row must exist in the
// tree at the published head, and every other shape of reference is left
// alone. The check is deliberately non-semantic — it asks whether a
// cited file is there, never whether the cited check enforces its
// criterion, because that audit stays the reviewer's (#249) — and it
// fails open, so a probe it cannot complete must never become a defect
// the model is asked to repair.
//
// Pinned, one case per row: a present path (with and without line and
// test-symbol suffixes) is not a finding; an absent path, the same path
// with a line suffix, and the same path with a leading ./ are findings
// each naming the normalized path the probe asked git about; the same
// path cited three ways yields exactly one candidate, so a body cannot
// multiply probes by repeating itself; a URL is never probed, including
// one whose host and path are path-shaped; prose with no location, a
// bare top-level file name (not a citable location — the validator's own
// rule), and manual-verification, not-implemented and unknown rows are
// never probed even when their text is path-shaped, because those
// references are notes rather than citations.
func TestCriterionEvidenceReferentialCheckAgainstPublishedHead(t *testing.T) {
	directory, head := evidenceProbeRepo(t)
	reporter := newEvidenceProbeReporter(t, directory)
	const (
		present = "tests/test_packaging.py"
		absent  = "tests/absent.py"
	)
	cases := []struct {
		name       string
		status     broker.CriterionStatus
		reference  string
		candidates int
		rooted     int
		missing    []string
	}{
		{"present path is not a finding", broker.StatusRegressionCheck, "`" + present + "` guards the COPY line", 1, 1, nil},
		{"line suffix normalizes to the file", broker.StatusRegressionCheck, "`" + present + ":42-80` is the guard", 1, 1, nil},
		{"test-symbol suffix normalizes to the file", broker.StatusRegressionCheck, "`" + present + "::test_packaging` is the guard", 1, 1, nil},
		{"leading dot slash on a present file is not a finding", broker.StatusRegressionCheck, "./" + present + " is the guard", 1, 1, nil},
		{"absent path is a finding", broker.StatusRegressionCheck, "`" + absent + "` guards the COPY line", 1, 1, []string{absent}},
		{"implementation-evidence row is probed too", broker.StatusImplementationEvidence, "implemented in `" + absent + "`", 1, 1, []string{absent}},
		{"line suffix on an absent file normalizes to the file", broker.StatusRegressionCheck, "`" + present + ":42` and `" + absent + ":12` are the guards", 2, 2, []string{absent}},
		{"test-symbol suffix on an absent file normalizes to the file", broker.StatusRegressionCheck, "`" + absent + "::test_packaging` is the guard", 1, 1, []string{absent}},
		{"leading dot slash on an absent file normalizes", broker.StatusRegressionCheck, "./" + absent + " is the guard", 1, 1, []string{absent}},
		{"repeats probe once", broker.StatusRegressionCheck, "`" + absent + "`, ./" + absent + ":12 and `" + absent + "` again", 1, 1, []string{absent}},
		{"URL is never probed", broker.StatusRegressionCheck, "the packaging job at https://ci.example.com/builds/12345", 0, 0, nil},
		{"path-shaped URL is never probed or fetched", broker.StatusRegressionCheck, "see https://git.example.com/misospace/miso-gallery/blob/main/tests/test_packaging.py", 0, 0, nil},
		{"URL beside a path probes only the path", broker.StatusRegressionCheck, "`" + absent + "` and https://ci.example.com/a/b/c", 1, 1, []string{absent}},
		{"prose without a location yields nothing", broker.StatusRegressionCheck, "verified by hand in the built image", 0, 0, nil},
		{"bare top-level name is not a location", broker.StatusRegressionCheck, "`Dockerfile` carries the COPY line", 0, 0, nil},
		{"manual-verification prose is not a citation", broker.StatusManualVerification, "read `" + absent + "` and curled the endpoint by hand", 0, 0, nil},
		{"not-implemented prose is not a citation", broker.StatusNotImplemented, "deliberately left out of `" + absent + "` for now", 0, 0, nil},
		{"unknown status carries no citation", broker.StatusUnknown, "", 0, 0, nil},
		// Rootedness against the published tree, not extraction, is what
		// separates a repository path from a token that merely has a
		// slash in it. Each of these is shaped like a path and is dropped
		// at the probe, because no repository path can be rooted at a
		// first segment the published tree does not carry — reporting one
		// would burn the shared repair budget on a truthful body.
		{"prose fraction beside a real path probes only the path", broker.StatusRegressionCheck, "matrix covers 3/4 endpoints; added `" + absent + "`", 2, 1, []string{absent}},
		{"date-shaped token beside a real workflow path", broker.StatusRegressionCheck, "green in CI on 2026/10/17, see `.github/workflows/ci.yaml`", 2, 1, nil},
		{"date-shaped token alone yields nothing", broker.StatusRegressionCheck, "green in CI on 2026/10/17", 1, 0, nil},
		{"another repository's path yields nothing", broker.StatusRegressionCheck, "same approach as misospace/courier `internal/hardened.go`", 2, 0, nil},
		{"module path yields nothing", broker.StatusImplementationEvidence, "implemented in `internal/x.go` using golang.org/x/net/html", 2, 0, nil},
		{"port-shaped token yields nothing", broker.StatusRegressionCheck, "curl localhost:8080/health and `internal/x.go`", 2, 0, nil},
		{"job name beside a real workflow path probes only the workflow", broker.StatusRegressionCheck, "nightly/contract job in `.github/workflows/ci.yaml`", 2, 1, nil},
		{"top-level file name counts as a root", broker.StatusRegressionCheck, "`Dockerfile/nothing.py`", 1, 1, []string{"Dockerfile/nothing.py"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed := oneRowReport(tc.status, tc.reference)
			candidates := citedEvidencePaths(parsed)
			if len(candidates) != tc.candidates {
				t.Fatalf("citedEvidencePaths(%q) = %q, want %d candidate(s)", tc.reference, candidates, tc.candidates)
			}
			rooted, ok := reporter.rootedAtPublishedTree(context.Background(), head, candidates)
			if !ok {
				t.Fatalf("the published tree at %s must be readable in this fixture", head)
			}
			if len(rooted) != tc.rooted {
				t.Fatalf("rootedAtPublishedTree(%q) = %q, want %d rooted candidate(s)", tc.reference, rooted, tc.rooted)
			}
			missing, probed := reporter.missingEvidencePaths(context.Background(), parsed, head)
			if !reflect.DeepEqual(missing, tc.missing) {
				t.Fatalf("missingEvidencePaths(%q) = %q, want %q", tc.reference, missing, tc.missing)
			}
			// The head travels with the finding and only with it: a
			// probe that found nothing reports no head, so the field
			// cannot describe a head nobody needs named.
			wantProbed := ""
			if len(tc.missing) > 0 {
				wantProbed = head
			}
			if probed != wantProbed {
				t.Fatalf("probed head = %q, want %q", probed, wantProbed)
			}
		})
	}
}

// TestCriterionEvidenceReferentialCheckFailsOpen pins the boundary cases
// the shape table cannot reach: a head the probe cannot resolve, a caller
// with no published head at all, a report with no rows, a top-level entry
// the probe cannot descend into, and the normalization that keeps a path
// git could refuse from ever reaching the hardened-git boundary. Every one
// of them must yield no finding — the check is the one fail-open verdict
// in a plan that is otherwise fail-closed, because a defect the model
// cannot repair by editing the body may never be handed to it as one.
func TestCriterionEvidenceReferentialCheckFailsOpen(t *testing.T) {
	directory, head := evidenceProbeRepo(t)
	reporter := newEvidenceProbeReporter(t, directory)
	parsed := oneRowReport(broker.StatusRegressionCheck, "`tests/absent.py` guards the COPY line")

	if missing, probed := reporter.missingEvidencePaths(context.Background(), parsed, strings.Repeat("0", 40)); missing != nil || probed != "" {
		t.Fatalf("an unresolvable head must find nothing and name no head, got %q / %q", missing, probed)
	}
	if _, ok := reporter.rootedAtPublishedTree(context.Background(), strings.Repeat("0", 40), citedEvidencePaths(parsed)); ok {
		t.Fatal("an unresolvable head must not be readable, or rooting would decide on an unread tree")
	}
	if missing, probed := reporter.missingEvidencePaths(context.Background(), parsed, ""); missing != nil || probed != "" {
		t.Fatalf("no published head must skip the check entirely, got %q / %q", missing, probed)
	}
	if missing, probed := reporter.missingEvidencePaths(context.Background(), nil, head); missing != nil || probed != "" {
		t.Fatalf("no parsed report must skip the check entirely, got %q / %q", missing, probed)
	}
	if missing, _ := reporter.missingEvidencePaths(context.Background(), &broker.CriteriaReport{}, head); missing != nil {
		t.Fatalf("a report with no rows must skip the check entirely, got %q", missing)
	}
	// The single git probe the check performs directly: present at the
	// head, absent at it, and fail-open on a head it cannot resolve.
	if reporter.evidencePathAbsentAtHead(context.Background(), head, "tests/test_packaging.py") {
		t.Fatal("a file the head carries must not read as absent")
	}
	if !reporter.evidencePathAbsentAtHead(context.Background(), head, "tests/absent.py") {
		t.Fatal("a file the head does not carry must read as absent")
	}
	if reporter.evidencePathAbsentAtHead(context.Background(), strings.Repeat("0", 40), "tests/absent.py") {
		t.Fatal("a probe that could not run must fail open, not read as absence")
	}
	// Normalization keeps git-hostile paths away from the boundary:
	// ".." and "." segments are rejected by git, and a leading dash is
	// what a pathspec-shaped string would use to become an option.
	for match, want := range map[string]string{
		"tests/../tests/test_packaging.py": "",
		"tests/./test_packaging.py":        "",
		"-tests/test_packaging.py":         "",
		"tests//test_packaging.py":         "",
		"./tests/test_packaging.py:9":      "tests/test_packaging.py",
		"tests/test_packaging.py::test_x":  "tests/test_packaging.py",
		"./tests/test_packaging.py":        "tests/test_packaging.py",
	} {
		if got := normalizeEvidencePathCandidate(match); got != want {
			t.Fatalf("normalizeEvidencePathCandidate(%q) = %q, want %q", match, got, want)
		}
	}
}

// TestEvidencePathCandidatePatternStaysProbeSafe pins the safety property
// the #367 probe rests on, now that the extraction pattern is the grammar
// leaf's exported path alternative rather than a local literal (#375
// review): widening the validator's path shape must not quietly admit a
// character that could turn a model-authored citation into an argv split,
// an option, or a pathspec expression. Whatever the pattern matches stays
// inside the charset the probe's safety argument assumes, and "(" and ")"
// survive as inert; a leading dash never reaches the probe because
// normalizeEvidencePathCandidate drops it (pinned by the normalization
// cases above).
func TestEvidencePathCandidatePatternStaysProbeSafe(t *testing.T) {
	tokens := []string{
		"tests/test_packaging.py",
		"docs/api(v2)/spec.md",
		"a b/c d", "a:b/c:d", "a'b/c'd", `a"b/c"d`, "a*b/c*d", "a?b/c?d",
		"a[b/c[d", "a]b/c]d", "a{b/c{d", "a}b/c}d", `a\b/c\d`,
		"a;b/c;d", "a|b/c|d", "a&b/c&d", "a$b/c$d", "a`b/c`d", "a\tb/c\tb",
		"-a/b", "https://example.com/a/b",
	}
	for _, token := range tokens {
		for _, match := range evidencePathCandidatePattern.FindAllString(token, -1) {
			if strings.ContainsAny(match, " \t\n:;'\"*?[]{}\\;|&$`") {
				t.Fatalf("evidencePathCandidatePattern matched %q from %q; the match carries a character the probe's safety argument excludes", match, token)
			}
		}
	}
	if matches := evidencePathCandidatePattern.FindAllString("tests/test_packaging.py", -1); len(matches) != 1 || matches[0] != "tests/test_packaging.py" {
		t.Fatalf("evidencePathCandidatePattern lost the shape it must probe: %q", matches)
	}
}
