package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/broker/grammar"
	"github.com/misospace/courier/internal/git"
)

// evidencePathCandidatePattern is the *file-path* alternative of the
// broker validator's referenceWithLocation (internal/broker/criteria.go,
// #249), taken from the grammar leaf's exported pattern rather than
// mirrored: the check probes exactly the references the validator itself
// accepts as a citable location, so a reference the contract already
// refused is never probed and a reference it accepted is probed in the
// same shape it was judged in. The grammar composes
// grammar.ReferenceWithLocation from its exported alternatives and pins
// that composition by test, so a validator edit cannot leave this
// extractor probing a shape the contract no longer accepts, or skip one
// it does (#375 review).
//
// The charset — [A-Za-z0-9_.-] segments separated by "/", where only
// segments after the first may also carry "(" and ")" — is also the
// safety property of the probe below. A matched candidate can contain no
// whitespace, no ":" , no quote, no glob or pathspec-magic character, and
// never begins with "-" (normalizeEvidencePathCandidate rejects that),
// so it can neither split the argv it travels in nor be read as an
// option or a pathspec expression. "(" and ")" can survive, and are
// inert: the hardened boundary builds an argv directly and no shell ever
// sees the value. That restriction is what lets sanitized text from a
// model-authored body reach git at all — see the exception noted on
// git.Hardened (#367).
var evidencePathCandidatePattern = grammar.ReferencePathPattern

// missingEvidencePaths reports which of the file paths the body's
// evidence rows cite are absent from the tree at head, the published
// commit the observation just verified (#367). It is a referential
// check, deliberately not a semantic one: it asks whether the cited file
// exists, never whether the cited check enforces its criterion, because
// the second question is the reviewer's and no boundary here may answer
// it for the model. It returns the paths in first-appearance order
// (deduplicated) and the head it probed.
//
// Every candidate is first rooted against the published tree: only a
// candidate whose first segment is a real top-level entry of that commit
// is probed at all (publishedTopLevelNames). The check is therefore able
// to speak about repository paths and about nothing else — a
// slash-bearing token whose first segment is not in the tree is prose, a
// date, a ratio, a port, another repository's path, a module path, or a
// package name, and this check cannot say anything true about any of
// them. Reporting one would burn the shared #170 repair budget on a
// truthful body and press the model to downgrade an honest criterion in
// order to silence the check. The trade-off is deliberate and stated:
// an absent path under a real top-level entry — the case #367 exists
// for — is still found and still named, while a path whose top-level
// entry is not in the published head is left alone rather than guessed
// at, and a citation the extractor cannot even resolve to a path-shaped
// candidate is likewise left alone.
//
// Fail-open is the whole design: this check runs inside a plan whose
// every other verdict is fail-closed, so any probe it cannot complete —
// a bad or unreadable head, git unavailable, a cancelled context — is
// reported as "no finding" rather than as an error or an absence. A
// citation defect the model cannot repair by editing the body would
// either burn a #170 slot or, worse, be told as one; neither is
// reachable from here. An empty head means the caller had no published
// head to probe and yields no finding at all (#367), and so does a body
// with no path-shaped citation — in both cases no probe ran and no head
// is reported.
func (r reporter) missingEvidencePaths(ctx context.Context, report *broker.CriteriaReport, head string) ([]string, string) {
	if report == nil || head == "" {
		return nil, ""
	}
	candidates := citedEvidencePaths(report)
	if len(candidates) == 0 {
		return nil, ""
	}
	rooted, ok := r.rootedAtPublishedTree(ctx, head, candidates)
	if !ok {
		return nil, ""
	}
	if len(rooted) == 0 {
		return nil, ""
	}
	missing := make([]string, 0, len(rooted))
	for _, candidate := range rooted {
		if r.evidencePathAbsentAtHead(ctx, head, candidate) {
			missing = append(missing, candidate)
		}
	}
	if len(missing) == 0 {
		return nil, ""
	}
	return missing, head
}

// rootedAtPublishedTree keeps only the candidates a repository path could
// be: those whose first segment is a real top-level entry of the tree at
// head (#367). It returns false when the top-level listing itself could
// not be read, which the caller treats as no finding at all — the same
// fail-open boundary every other unanswerable question here lands on.
//
// The listing is one `ls-tree -z --full-name <head>` with no pathspec and
// no recursion, so it yields exactly the commit's top-level entries and
// its size is bounded by the number of those entries rather than by the
// tree's. Both blob and tree names count: a cited top-level file is as
// real a target as a cited top-level directory.
func (r reporter) rootedAtPublishedTree(ctx context.Context, head string, candidates []string) ([]string, bool) {
	entries, err := r.publishedTopLevelNames(ctx, head)
	if err != nil {
		return nil, false
	}
	rooted := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		firstSegment := candidate
		if slash := strings.Index(candidate, "/"); slash >= 0 {
			firstSegment = candidate[:slash]
		}
		if entries[firstSegment] {
			rooted = append(rooted, candidate)
		}
	}
	return rooted, true
}

// publishedTopLevelNames reads the top-level entry names of the tree at
// head from the run's own workspace through the hardened boundary (#367).
// Each record of the NUL-separated listing is "<mode> SP <type> SP
// <oid>\t<name>"; only the name half is kept, and the listing is treated
// as unreadable rather than partially believed if any record lacks its
// tab, so a shape this parser does not recognize fails the whole check
// open instead of rooting candidates against a partial set. The error is
// returned to this seam alone, never onto a trusted string: it names only
// the abbreviated head this run published.
func (r reporter) publishedTopLevelNames(ctx context.Context, head string) (map[string]bool, error) {
	out, err := git.Hardened(ctx, r.cfg.Directory, "ls-tree", "-z", "--full-name", head)
	if err != nil {
		return nil, err
	}
	records := strings.Split(string(out), "\x00")
	entries := make(map[string]bool, len(records))
	for _, record := range records {
		if record == "" {
			continue
		}
		_, name, found := strings.Cut(record, "\t")
		if !found || name == "" {
			return nil, fmt.Errorf("unrecognized ls-tree record at head %s", shortCommitHead(head))
		}
		entries[name] = true
	}
	return entries, nil
}

// citedEvidencePaths extracts, in first-appearance order and
// deduplicated, the file paths the body's citation rows name (#367).
// Only implementation-evidence and regression-check rows are probed:
// they are exactly the statuses for which the validator requires the
// reference to be a file path or a check URL, so their reference is a
// claim about a citable artifact and an absent path makes that claim
// referential nonsense. manual-verification and not-implemented carry a
// free-text note and unknown carries none — probing prose
// non-semantically would manufacture defects the model can only "fix"
// by lying about the criterion, which is the opposite of what the
// contract is for.
//
// The candidate set is bounded by the operator-pinned set: a validated
// block carries exactly one row per pinned criterion, the pinned set is
// capped at broker.MaxCriteria, and each reference at
// broker.MaxReferenceBytes. #367 requires no cap of the executor's own,
// so none is added.
func citedEvidencePaths(report *broker.CriteriaReport) []string {
	var candidates []string
	seen := make(map[string]struct{})
	for _, rec := range report.Records {
		if rec.Status != broker.StatusImplementationEvidence && rec.Status != broker.StatusRegressionCheck {
			continue
		}
		for _, candidate := range evidencePathCandidates(rec.Reference) {
			if _, duplicate := seen[candidate]; duplicate {
				continue
			}
			seen[candidate] = struct{}{}
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

// evidencePathCandidates pulls the path-shaped substrings out of one
// evidence row's reference (#367). The extraction is deterministic and
// mechanical, matching no grammar the model could satisfy by rewording:
// whitespace-separated tokens, any token carrying "://" dropped whole
// (a URL is never fetched and never probed, and a URL's host and path
// would otherwise read as a path), then maximal matches of the
// validator's own path alternative in what is left.
//
// Shape is all extraction can judge, and it over-matches on purpose to
// stay non-semantic: prose carrying a slash ("3/4 endpoints"), a date,
// a port, a module path, or another repository's path is shaped like a
// path. Rooting each match against the published tree is what separates
// the two, and it is why the filter lives with the probe
// (rootedAtPublishedTree) rather than here.
func evidencePathCandidates(reference string) []string {
	var candidates []string
	for _, token := range strings.Fields(reference) {
		if strings.Contains(token, "://") {
			continue
		}
		for _, match := range evidencePathCandidatePattern.FindAllString(token, -1) {
			if candidate := normalizeEvidencePathCandidate(match); candidate != "" {
				candidates = append(candidates, candidate)
			}
		}
	}
	return candidates
}

// normalizeEvidencePathCandidate reduces one raw match to the tree path
// to probe, or "" when the match cannot be probed safely (#367):
//
//   - a leading "./" is stripped, the form a citation takes when it
//     names the file relative to the repository root;
//   - everything from the first ":" after the final "/" is cut. The
//     candidate charset admits no ":" at all, so in practice the match
//     already stopped at one and the line ("file.go:12"), line-range
//     ("file.go:12-40") and test-symbol ("file.go::TestName") forms
//     arrive here as the bare file; the cut is the explicit form of that
//     rule, kept so widening the pattern later cannot quietly start
//     probing "file.go:12" as a path;
//   - a candidate whose first segment begins with "-" is dropped: no
//     valid tree path leads with a dash, and a leading "-" is what a
//     pathspec-shaped string would use to become an option;
//   - a candidate carrying an empty, "." or ".." segment is dropped,
//     because git rejects tree paths containing them (".." in
//     particular) — so what reaches the hardened-git boundary is always
//     a plain relative path drawn from the validator's own charset, and
//     it reaches it as the pathspec after `--` rather than as part of a
//     rev:path argument.
func normalizeEvidencePathCandidate(match string) string {
	candidate := strings.TrimPrefix(match, "./")
	if lastSlash := strings.LastIndex(candidate, "/"); lastSlash >= 0 {
		if colon := strings.Index(candidate[lastSlash+1:], ":"); colon >= 0 {
			candidate = candidate[:lastSlash+1+colon]
		}
	}
	segments := strings.Split(candidate, "/")
	if strings.HasPrefix(segments[0], "-") {
		return ""
	}
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return ""
		}
	}
	return candidate
}

// evidencePathAbsentAtHead reports whether path is absent from the tree
// at head in the run's own workspace (#367), through the hardened git
// environment: no system or global config trust, no prompts, no optional
// locks, and stderr dropped — a hostile repository cannot speak into a
// trusted error string, and nothing here returns one.
//
// It reads the tree with `ls-tree <head> -- <path>` rather than
// `cat-file -e <head>:<path>` because the two differ in exactly the way
// this check needs: git exits 128 for both an absent path *and* an
// unreadable head, so exit status cannot separate "the model cited a
// file that is not there" from "the probe itself failed", and reading
// the absence as a finding would fabricate defects whenever git was
// unavailable. `ls-tree` exits 0 with empty output for an absent path at
// a readable head and non-zero for a head it cannot resolve, so absence
// is a positive observation and every failure fails open (#367).
func (r reporter) evidencePathAbsentAtHead(ctx context.Context, head, path string) bool {
	out, err := git.Hardened(ctx, r.cfg.Directory, "ls-tree", "-z", "--full-name", head, "--", path)
	if err != nil {
		return false
	}
	return strings.Trim(string(out), " \t\r\n\x00") == ""
}

// shortCommitHead abbreviates the probed head to git's conventional seven
// hex characters for operator-facing prose. It only ever shortens text
// the repair instruction and the terminal reason already show in full,
// and a head shorter than that (a fixture, or a git that resolved a
// short name) is passed through unchanged rather than truncated
// mid-token (#367).
func shortCommitHead(head string) string {
	if len(head) <= 7 {
		return head
	}
	return head[:7]
}
