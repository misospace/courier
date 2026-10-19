package broker

import (
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The shared behavioral truth table for the acceptance-criterion evidence
// contract (#249). Both publication paths — the native broker engine at
// CreatePullRequest and the legacy executor at the Verifying handoff —
// consume ValidateCriterionEvidence; this table is the source of truth, and
// divergent acceptance rules between the paths would be a correctness
// regression.
//
// The miso-gallery fixtures pin the real dogfood failure (#204): issue #502
// carries three acceptance criteria; the initial PR #505 handoff omitted the
// built-container endpoint check and overstated a manually-inspected
// `.dockerignore` invariant as maintained. The deficient bodies are
// reconstructions from #204's record of the initial handoff (the live PR was
// later repaired and merged); the issue body is the live snapshot verbatim.
//
// What a passing case here establishes — and does not: the validator proves
// pin integrity, exact criterion coverage, per-status reference shape, and
// coverage-claim honesty. It does not prove that a cited test semantically
// enforces its criterion. The honest fixtures encode the evidence mapping
// that human review established in miso-gallery#505; passing them asserts
// the contract accepts an honest record, not that any test's semantics were
// verified.

var misoGalleryIssue = SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502}

func loadFixture(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(body)
}

func pinMisoGallerySet(t *testing.T) CriterionSet {
	t.Helper()
	set, err := ExtractCriteria(loadFixture(t, "miso-gallery-502-issue.md"), misoGalleryIssue)
	if err != nil {
		t.Fatalf("extract criteria from pinned issue: %v", err)
	}
	return set
}

// TestExtractCriteriaMisoGallery502 pins identification for the real issue:
// exactly the three Acceptance bullets become criteria, in document order,
// and the Evidence section's bullets (which also exist in the body) are not
// criteria.
func TestExtractCriteriaMisoGallery502(t *testing.T) {
	set := pinMisoGallerySet(t)
	if set.IsEmpty() {
		t.Fatal("expected a non-empty pinned set for issue 502")
	}
	want := []string{
		"The built image contains all three tracked PNGs under `/app/assets/`.",
		"With FAVICON_URL unset, a running built image returns 200 and image content for `/favicon.ico` and both manifest icon URLs.",
		"A regression test detects omission of assets from the Docker build instructions; existing packaging tests remain green.",
	}
	if len(set.Criteria) != len(want) {
		t.Fatalf("expected %d criteria, got %d: %+v", len(want), len(set.Criteria), set.Criteria)
	}
	for i, c := range set.Criteria {
		// IDs are ordinals AC-1..AC-3 in document order.
		if c.ID != "AC-"+strconv.Itoa(i+1) {
			t.Errorf("criterion %d has ID %q, want AC-%d", i, c.ID, i+1)
		}
		if c.Text != want[i] {
			t.Errorf("criterion %s text = %q, want %q", c.ID, c.Text, want[i])
		}
		if c.Digest == "" || len(c.Digest) != len("sha256:")+64 {
			t.Errorf("criterion %s digest %q is not a sha256 hex digest", c.ID, c.Digest)
		}
	}
	if set.Digest == "" {
		t.Fatal("set digest is empty")
	}
	// Determinism: the same body pins the same set twice.
	again, err := ExtractCriteria(loadFixture(t, "miso-gallery-502-issue.md"), misoGalleryIssue)
	if err != nil {
		t.Fatalf("re-extract: %v", err)
	}
	if again.Digest != set.Digest {
		t.Fatalf("extraction is not deterministic: %s vs %s", set.Digest, again.Digest)
	}
}

// TestCriterionSetDigestPinsRevision demonstrates the revision pin
// mechanically: editing, reordering, adding, or deleting any criterion
// changes the set digest, so an evidence block authored against a different
// revision of the issue's acceptance section is refused.
func TestCriterionSetDigestPinsRevision(t *testing.T) {
	set := pinMisoGallerySet(t)
	mutate := func(name string, f func(CriterionSet) CriterionSet) {
		t.Helper()
		if got := f(set).digest(); got == set.Digest {
			t.Errorf("%s: set digest unchanged under mutation", name)
		}
	}
	mutate("edit text", func(s CriterionSet) CriterionSet {
		s.Criteria[1].Text += " (edited)"
		s.Criteria[1].Digest = digestString(s.Criteria[1].Text)
		return s
	})
	mutate("reorder", func(s CriterionSet) CriterionSet {
		s.Criteria[0], s.Criteria[1] = s.Criteria[1], s.Criteria[0]
		return s
	})
	mutate("add", func(s CriterionSet) CriterionSet {
		s.Criteria = append(s.Criteria, Criterion{ID: "AC-4", Text: "extra", Digest: digestString("extra")})
		return s
	})
	mutate("delete", func(s CriterionSet) CriterionSet {
		s.Criteria = s.Criteria[:2]
		return s
	})
	// Identity is part of the pin: the same criteria pinned for a different
	// issue digest differently.
	other := set
	other.Issue.Number = 999
	if other.digest() == set.Digest {
		t.Error("set digest ignores the trusted issue identity")
	}
}

// TestValidateCriterionEvidenceDeficientHandoff reproduces the initial
// #505 failure: no evidence block at all while the pinned set is non-empty.
func TestValidateCriterionEvidenceDeficientHandoff(t *testing.T) {
	set := pinMisoGallerySet(t)
	_, err := ValidateCriterionEvidence(loadFixture(t, "miso-gallery-505-pr-deficient.md"), set)
	if err == nil {
		t.Fatal("deficient handoff (no evidence block) passed validation")
	}
	var cerr *CriteriaError
	if !errors.As(err, &cerr) {
		t.Fatalf("want *CriteriaError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrCriteriaBlockMissing) {
		t.Fatalf("missing block should wrap ErrCriteriaBlockMissing: %v", err)
	}
	if !strings.Contains(cerr.Reason, "misospace/miso-gallery#502") || !strings.Contains(cerr.Reason, set.Digest) {
		t.Errorf("missing-block reason should name the trusted issue and pinned set digest: %q", cerr.Reason)
	}
}

// TestValidateCriterionEvidencePartialHandoff reproduces the completed-form
// failure: the block exists but omits the built-container endpoint criterion
// (AC-2) entirely while claiming full coverage.
func TestValidateCriterionEvidencePartialHandoff(t *testing.T) {
	set := pinMisoGallerySet(t)
	report, err := ValidateCriterionEvidence(loadFixture(t, "miso-gallery-505-pr-partial.md"), set)
	if err == nil {
		t.Fatal("handoff missing AC-2 passed validation")
	}
	var cerr *CriteriaError
	if !errors.As(err, &cerr) {
		t.Fatalf("want *CriteriaError, got %T: %v", err, err)
	}
	if len(cerr.Missing) != 1 || cerr.Missing[0] != "AC-2" {
		t.Errorf("missing IDs = %v, want [AC-2] (the built-image endpoint criterion)", cerr.Missing)
	}
	if !strings.Contains(cerr.Reason, "AC-2") {
		t.Errorf("reason should name the missing criterion: %q", cerr.Reason)
	}
	if cerr.CoverageClaim != CoverageFull || cerr.CoverageDerived != CoveragePartial {
		t.Errorf("coverage claim %q vs derived %q: a body omitting a criterion must not claim full", cerr.CoverageClaim, cerr.CoverageDerived)
	}
	if report == nil {
		t.Error("a parsed block should be returned with the error for observability")
	}
}

// TestValidateCriterionEvidenceOverstatedHandoff reproduces the two
// overstatement defects: an invented criterion (the `.dockerignore`
// invariant as AC-4) recorded as regression-enforced with no citable
// automation, and a weaker implementation-only record for AC-2 while the
// block claims every criterion is regression-enforced.
func TestValidateCriterionEvidenceOverstatedHandoff(t *testing.T) {
	set := pinMisoGallerySet(t)
	_, err := ValidateCriterionEvidence(loadFixture(t, "miso-gallery-505-pr-overstated.md"), set)
	if err == nil {
		t.Fatal("overstated handoff passed validation")
	}
	var cerr *CriteriaError
	if !errors.As(err, &cerr) {
		t.Fatalf("want *CriteriaError, got %T: %v", err, err)
	}
	if len(cerr.Unknown) != 1 || cerr.Unknown[0] != "AC-4" {
		t.Errorf("unknown IDs = %v, want [AC-4]; the model cannot add criteria", cerr.Unknown)
	}
	if len(cerr.BadStatus) != 1 || cerr.BadStatus[0] != "AC-4" {
		t.Errorf("bad-status IDs = %v, want [AC-4]: regression-check without a test path or check URL", cerr.BadStatus)
	}
	if cerr.CoverageClaim != CoverageFull || cerr.CoverageDerived != CoveragePartial {
		t.Errorf("coverage claim %q vs derived %q", cerr.CoverageClaim, cerr.CoverageDerived)
	}
	if !strings.Contains(cerr.Reason, "not pinned") || !strings.Contains(cerr.Reason, "coverage") {
		t.Errorf("reason should name the added criterion and the coverage lie: %q", cerr.Reason)
	}
}

// TestValidateCriterionEvidenceHonestHandoffFull accepts the honest full
// record matching what merged miso-gallery#505 actually shipped: all three
// criteria regression-checked, AC-2 citing the built-image endpoint CI step
// rather than the Dockerfile-shape test.
func TestValidateCriterionEvidenceHonestHandoffFull(t *testing.T) {
	set := pinMisoGallerySet(t)
	report, err := ValidateCriterionEvidence(loadFixture(t, "miso-gallery-505-pr-honest.md"), set)
	if err != nil {
		t.Fatalf("honest full handoff rejected: %v", err)
	}
	if report.DerivedCoverage(set) != CoverageFull || report.Coverage != CoverageFull {
		t.Errorf("coverage = %q (derived %q), want full", report.Coverage, report.DerivedCoverage(set))
	}
	counts := report.StatusCounts()
	if counts[StatusRegressionCheck] != 3 {
		t.Errorf("regression-check count = %d, want 3", counts[StatusRegressionCheck])
	}
}

// TestHonestHandoffPlusTrailingMalformedRowFails is the exact-head
// regression for the fail-open trailing-row defect (review finding on
// #283): appending a row-shaped line that does not parse to the
// otherwise-valid honest-full body must be rejected. Before the fix, the
// row ended the block as prose and the body validated, silently dropping
// the invented criterion and the duplicate from coverage enforcement.
func TestHonestHandoffPlusTrailingMalformedRowFails(t *testing.T) {
	set := pinMisoGallerySet(t)
	honest := loadFixture(t, "miso-gallery-505-pr-honest.md")
	cases := []struct {
		name string
		line string
	}{
		{
			name: "invented AC-4 with an unknown status token",
			line: "- AC-4 sha256:" + strings.Repeat("0", 64) + ": definitely-done - tests/foo.py",
		},
		{
			name: "malformed duplicate AC-3",
			line: "- AC-3 " + set.Criteria[2].Digest + ": verified-by-vibes - tests/foo.py",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ValidateCriterionEvidence(honest+"\n"+tc.line+"\n", set); err == nil {
				t.Fatalf("honest-full body with appended %q validated; want malformed-row rejection", tc.line)
			} else if !strings.Contains(err.Error(), "malformed row") {
				t.Fatalf("want malformed-row rejection, got: %v", err)
			}
		})
	}
	// A well-formed appended duplicate still fails through the duplicate
	// rule rather than the malformed-row rule.
	duplicate := "\n" + "- AC-3 " + set.Criteria[2].Digest + ": regression-check - tests/x.py\n"
	_, err := ValidateCriterionEvidence(honest+duplicate, set)
	if err == nil || !strings.Contains(err.Error(), "duplicate rows for AC-3") {
		t.Fatalf("well-formed appended duplicate should fail as duplicate, got: %v", err)
	}
}

// TestValidateCriterionEvidenceHonestHandoffPartial accepts the honest
// partial record: AC-2 is verified by hand and recorded manual-verification
// with coverage=partial. Unverified criteria stay visible; the run simply
// does not get to claim full verification.
func TestValidateCriterionEvidenceHonestHandoffPartial(t *testing.T) {
	set := pinMisoGallerySet(t)
	report, err := ValidateCriterionEvidence(loadFixture(t, "miso-gallery-505-pr-honest-partial.md"), set)
	if err != nil {
		t.Fatalf("honest partial handoff rejected: %v", err)
	}
	if report.DerivedCoverage(set) != CoveragePartial {
		t.Errorf("derived coverage = %q, want partial", report.DerivedCoverage(set))
	}
	counts := report.StatusCounts()
	if counts[StatusManualVerification] != 1 || counts[StatusRegressionCheck] != 2 {
		t.Errorf("status counts = %v, want 1 manual-verification and 2 regression-check", counts)
	}
}

// TestExtractCriteriaGrammar exercises the extraction grammar's recognized
// and rejected shapes.
func TestExtractCriteriaGrammar(t *testing.T) {
	issue := SourceIssue{Owner: "ex", Name: "repo", Number: 7}
	cases := []struct {
		name    string
		body    string
		want    []string // expected criterion texts; nil means empty set
		wantErr string
	}{
		{
			name: "no acceptance section yields an empty set",
			body: "**Problem:**\n- something is broken.\n",
		},
		{
			name: "acceptance section without bullets yields an empty set",
			body: "**Acceptance:**\nProse only, no list.\n",
			want: nil,
		},
		{
			name: "ATX heading and checkbox items are recognized",
			body: "Intro.\n\n## Acceptance\n\n- [ ] First thing\n- [x] Second thing\n",
			want: []string{"First thing", "Second thing"},
		},
		{
			name: "bold label with colon is recognized",
			body: "**Problem:**\n- broken.\n\n**Acceptance:**\n- Fixed.\n",
			want: []string{"Fixed."},
		},
		{
			name: "a later bold section ends the acceptance section",
			body: "**Acceptance:**\n- Criterion one.\n\n**Notes:**\n- Not a criterion.\n",
			want: []string{"Criterion one."},
		},
		{
			name: "nested and continuation lines join the criterion text",
			body: "**Acceptance:**\n- Outer criterion\n  continued line\n  - nested detail\n",
			want: []string{"Outer criterion continued line nested detail"},
		},
		{
			name:    "two acceptance sections are ambiguous",
			body:    "**Acceptance:**\n- One.\n\n**Acceptance:**\n- Two.\n",
			wantErr: "more than one Acceptance section",
		},
		{
			name: "crlf bodies extract identically",
			body: "**Acceptance:**\r\n- One.\r\n- Two.\r\n",
			want: []string{"One.", "Two."},
		},
		{
			// The documented bare-label form: a plain "Acceptance:" line
			// opens the section. Before bare labels were recognized, such
			// an issue pinned a silent empty set and the whole evidence
			// contract was vacuously satisfied (review finding on #283).
			name: "bare acceptance label recognized",
			body: "Intro prose.\n\nAcceptance:\n- First criterion.\n- Second criterion.\n",
			want: []string{"First criterion.", "Second criterion."},
		},
		{
			// A bare label standing alone ends the section exactly as a
			// bold label does, so a later section's bullets are never
			// swallowed as criteria.
			name: "bare notes and evidence labels end the section",
			body: "Acceptance:\n- Criterion one.\n\nNotes:\n- Not a criterion.\n\nEvidence:\n- Also not a criterion.\n",
			want: []string{"Criterion one."},
		},
		{
			// Prose with content after the colon is not a bare label: it
			// neither opens nor ends a section, so criteria before and
			// after it stay pinned — the fix must not trade the silent
			// bypass for silent under-collection.
			name: "colon prose inside the section is not a heading",
			body: "Acceptance:\n- Criterion one.\nNote: also handle the edge case.\n- Criterion two.\n",
			want: []string{"Criterion one.", "Criterion two."},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set, err := ExtractCriteria(tc.body, issue)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(set.Criteria) != len(tc.want) {
				t.Fatalf("criteria = %+v, want %d items", set.Criteria, len(tc.want))
			}
			for i, c := range set.Criteria {
				if c.Text != tc.want[i] {
					t.Errorf("criterion %d text = %q, want %q", i, c.Text, tc.want[i])
				}
			}
		})
	}
}

func TestExtractCriteriaBounds(t *testing.T) {
	issue := SourceIssue{Owner: "ex", Name: "repo", Number: 1}
	t.Run("over the criteria count bound fails closed", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("**Acceptance:**\n")
		for i := 0; i < MaxCriteria+1; i++ {
			b.WriteString("- criterion\n")
		}
		if _, err := ExtractCriteria(b.String(), issue); err == nil || !strings.Contains(err.Error(), "never truncated") {
			t.Fatalf("want over-bound failure naming truncation refusal, got %v", err)
		}
	})
	t.Run("over the criterion text bound fails closed", func(t *testing.T) {
		long := strings.Repeat("a", MaxCriterionTextBytes+1)
		body := "**Acceptance:**\n- " + long + "\n"
		if _, err := ExtractCriteria(body, issue); err == nil || !strings.Contains(err.Error(), "AC-1") {
			t.Fatalf("want named over-length failure, got %v", err)
		}
	})
	t.Run("exactly at the bounds passes", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("**Acceptance:**\n")
		for i := 0; i < MaxCriteria; i++ {
			b.WriteString("- criterion\n")
		}
		set, err := ExtractCriteria(b.String(), issue)
		if err != nil {
			t.Fatalf("at-bound extraction failed: %v", err)
		}
		if len(set.Criteria) != MaxCriteria {
			t.Fatalf("criteria = %d, want %d", len(set.Criteria), MaxCriteria)
		}
	})
}

// TestValidateCriterionEvidenceTruthTable covers the validator's mechanical
// rules beyond the pinned dogfood fixtures.
func TestValidateCriterionEvidenceTruthTable(t *testing.T) {
	set := pinMisoGallerySet(t)
	header := "courier-criteria:v1 source=misospace/miso-gallery#502 set=" + set.Digest
	ac2 := "- AC-2 " + set.Criteria[1].Digest
	ac3 := "- AC-3 " + set.Criteria[2].Digest
	row := func(id, digest string, status CriterionStatus, ref string) string {
		if ref == "" {
			return "- " + id + " " + digest + ": " + string(status)
		}
		return "- " + id + " " + digest + ": " + string(status) + " - " + ref
	}
	baseBlock := func(coverage string, rows ...string) string {
		return header + " coverage=" + coverage + "\n\n" + strings.Join(rows, "\n")
	}

	cases := []struct {
		name    string
		body    string
		wantErr string // empty means pass
	}{
		{
			name: "em-dash separator accepted",
			body: baseBlock(CoveragePartial,
				"- AC-1 "+set.Criteria[0].Digest+": regression-check — tests/x.py",
				row("AC-2", set.Criteria[1].Digest, StatusUnknown, ""),
				row("AC-3", set.Criteria[2].Digest, StatusNotImplemented, "out of scope for this pass")),
		},
		{
			name:    "wrong source identity refused",
			body:    strings.Replace(baseBlock(CoverageFull, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py")), "source=misospace/miso-gallery#502", "source=other/repo#502", 1),
			wantErr: "names source other/repo#502",
		},
		{
			name:    "stale set digest refused",
			body:    strings.Replace(baseBlock(CoverageFull, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py")), "set="+set.Digest, "set=sha256:"+strings.Repeat("0", 64), 1),
			wantErr: "authored against different criteria",
		},
		{
			name:    "per-row digest mismatch refused",
			body:    baseBlock(CoverageFull, row("AC-1", "sha256:"+strings.Repeat("1", 64), StatusRegressionCheck, "tests/x.py"), ac2+": regression-check - tests/x.py", ac3+": regression-check - tests/x.py"),
			wantErr: "different criterion text for AC-1",
		},
		{
			name:    "duplicate rows refused",
			body:    baseBlock(CoverageFull, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py")),
			wantErr: "duplicate rows for AC-1",
		},
		{
			name:    "regression-check without a location refused",
			body:    baseBlock(CoveragePartial, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "verified by careful inspection, no automation"), row("AC-2", set.Criteria[1].Digest, StatusUnknown, ""), row("AC-3", set.Criteria[2].Digest, StatusUnknown, "")),
			wantErr: "shape violated for AC-1",
		},
		{
			name:    "implementation-evidence without a location refused",
			body:    baseBlock(CoveragePartial, row("AC-1", set.Criteria[0].Digest, StatusImplementationEvidence, "implemented as discussed"), row("AC-2", set.Criteria[1].Digest, StatusUnknown, ""), row("AC-3", set.Criteria[2].Digest, StatusUnknown, "")),
			wantErr: "shape violated for AC-1",
		},
		{
			name:    "manual-verification without a note refused",
			body:    baseBlock(CoveragePartial, "- AC-1 "+set.Criteria[0].Digest+": manual-verification", row("AC-2", set.Criteria[1].Digest, StatusUnknown, ""), row("AC-3", set.Criteria[2].Digest, StatusUnknown, "")),
			wantErr: "shape violated for AC-1",
		},
		{
			name:    "not-implemented without a reason refused",
			body:    baseBlock(CoveragePartial, "- AC-1 "+set.Criteria[0].Digest+": not-implemented", row("AC-2", set.Criteria[1].Digest, StatusUnknown, ""), row("AC-3", set.Criteria[2].Digest, StatusUnknown, "")),
			wantErr: "shape violated for AC-1",
		},
		{
			name:    "unknown status may carry no reference",
			body:    baseBlock(CoveragePartial, row("AC-1", set.Criteria[0].Digest, StatusUnknown, ""), row("AC-2", set.Criteria[1].Digest, StatusUnknown, ""), row("AC-3", set.Criteria[2].Digest, StatusUnknown, "")),
			wantErr: "",
		},
		{
			name:    "partial claimed for full record refused too — claim must match derivation",
			body:    baseBlock(CoveragePartial, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, ".github/workflows/ci.yaml step"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py::test_y")),
			wantErr: "coverage claimed partial but the records derive full",
		},
		{
			name:    "second header refused",
			body:    baseBlock(CoverageFull, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py")) + "\n\n" + header + " coverage=partial",
			wantErr: "more than one criterion-evidence block header",
		},
		{
			name:    "row beyond the block bound refused",
			body:    header + " coverage=partial\n" + row("AC-1", set.Criteria[0].Digest, StatusUnknown, "") + "\n" + strings.Repeat(row("AC-2", set.Criteria[1].Digest, StatusUnknown, "")+"\n", MaxBlockRows) + row("AC-3", set.Criteria[2].Digest, StatusUnknown, ""),
			wantErr: "exceeds the 64-row bound",
		},
		{
			name:    "oversized reference refused",
			body:    baseBlock(CoveragePartial, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py "+strings.Repeat("y", MaxReferenceBytes)), row("AC-2", set.Criteria[1].Digest, StatusUnknown, ""), row("AC-3", set.Criteria[2].Digest, StatusUnknown, "")),
			wantErr: "evidence reference exceeds",
		},
		{
			name:    "unknown status token refused as a row",
			body:    baseBlock(CoverageFull, "- AC-1 "+set.Criteria[0].Digest+": definitely-done - tests/x.py", ac2+": regression-check - tests/x.py", ac3+": regression-check - tests/x.py"),
			wantErr: "malformed row",
		},
		{
			// A trailing row-shaped line after otherwise-valid rows used to
			// end the block as prose and fail open: an invented AC-4 with
			// an unknown status token validated a coverage=full block
			// (review finding on PR #283).
			name:    "trailing malformed row after valid rows refused",
			body:    baseBlock(CoverageFull, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py")) + "\n- AC-4 sha256:" + strings.Repeat("0", 64) + ": definitely-done - tests/foo.py",
			wantErr: "malformed row",
		},
		{
			name:    "trailing row-shaped garbage refused",
			body:    baseBlock(CoverageFull, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py")) + "\n- AC-",
			wantErr: "malformed row",
		},
		{
			// Ordinary prose after the block stays allowed; only row-shaped
			// lines inside the contiguous row list are rejections.
			name: "prose after the block still ends it silently",
			body: baseBlock(CoverageFull, row("AC-1", set.Criteria[0].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-2", set.Criteria[1].Digest, StatusRegressionCheck, "tests/x.py"), row("AC-3", set.Criteria[2].Digest, StatusRegressionCheck, "tests/x.py")) + "\n\nThe `.dockerignore` invariant is discussed above.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report, err := ValidateCriterionEvidence(tc.body, set)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected rejection: %v", err)
				}
				if report == nil {
					t.Fatal("pass must return a report")
				}
				return
			}
			if err == nil {
				t.Fatal("expected rejection, got pass")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("reason %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestValidateCriterionEvidenceVacuousAndUnset: an empty pinned set is a
// vacuous pass regardless of body shape, and an unset trusted identity is an
// error, mirroring ValidateLinkage's guard.
func TestValidateCriterionEvidenceVacuousAndUnset(t *testing.T) {
	report, err := ValidateCriterionEvidence("Closes #502. No block at all.", CriterionSet{Issue: misoGalleryIssue})
	if err != nil || report != nil {
		t.Fatalf("empty set should be a vacuous pass with no report, got report=%v err=%v", report, err)
	}
	if _, err := ValidateCriterionEvidence("any body", CriterionSet{}); err == nil {
		t.Fatal("unset trusted identity must be an error")
	}
}

// TestValidateCriterionEvidenceIgnoresProseOutsideBlock: "- AC-n" lines in
// prose (no header) are not rows; the contract reads only the delimited
// block.
func TestValidateCriterionEvidenceIgnoresProseOutsideBlock(t *testing.T) {
	set := pinMisoGallerySet(t)
	body := "Closes #502.\n\n- AC-1 looks done to me.\n- AC-9 whatever.\n"
	_, err := ValidateCriterionEvidence(body, set)
	if !errors.Is(err, ErrCriteriaBlockMissing) {
		t.Fatalf("prose rows without a header should read as a missing block, got %v", err)
	}
}

// The taught grammar (#375). Consumers render the grammar instead of
// restating it, so the derivation below is the only copy of the shape and
// these tests are what pin the derivation itself: a rendered template must
// still satisfy the validator it claims to describe, the closed vocabulary
// must be the validator's, and the per-status rule sentence must be the
// rule the validator enforces.

const (
	derivedHeaderTemplate = "courier-criteria:v1 source=<owner/name>#<n> set=sha256:<64hex> coverage=full|partial"
	derivedRowTemplate    = "- AC-<n> sha256:<64hex>: <status> - <reference>"
	derivedVocabulary     = "implementation-evidence, regression-check, manual-verification, not-implemented, or unknown"
	derivedRuleSentence   = "regression-check and implementation-evidence must cite a file path or check URL; manual-verification and not-implemented must carry a note"
)

// rowStatusAlternation extracts the status vocabulary in the order the row
// pattern spells it, so the drift guard reads the validator rather than a
// constant next to it.
var rowStatusAlternation = regexp.MustCompile(`\(([a-z-]+(?:\|[a-z-]+)+)\)`)

// renderHeader fills the header template's placeholders for a concrete
// source reference and coverage claim; the template's own alternation is
// the vocabulary, so the caller names the one it means.
func renderHeader(source, coverage string) string {
	header := strings.Replace(CriteriaHeaderGrammar(), "coverage=full|partial", "coverage="+coverage, 1)
	return strings.ReplaceAll(header, SourceReferencePlaceholder, source)
}

// renderRow fills the row template's placeholders for a concrete criterion.
func renderRow(number, status, reference string) string {
	row := strings.ReplaceAll(CriteriaRowGrammar(), CriterionPlaceholder, number)
	row = strings.ReplaceAll(row, DigestPlaceholder, strings.Repeat("a", 64))
	row = strings.ReplaceAll(row, StatusPlaceholder, status)
	return strings.ReplaceAll(row, ReferencePlaceholder, reference)
}

// TestCriteriaGrammarMatchesValidatorPatterns pins the derivation: the
// taught templates are exactly the expected shapes, and every concrete
// instance rendered from them is accepted by the pattern the validator
// matches — including the placeholders' own vocabulary, which must not
// smuggle a status outside the closed set past the row pattern.
func TestCriteriaGrammarMatchesValidatorPatterns(t *testing.T) {
	if got := CriteriaHeaderGrammar(); got != derivedHeaderTemplate {
		t.Fatalf("CriteriaHeaderGrammar() = %q, want %q", got, derivedHeaderTemplate)
	}
	if got := CriteriaRowGrammar(); got != derivedRowTemplate {
		t.Fatalf("CriteriaRowGrammar() = %q, want %q", got, derivedRowTemplate)
	}

	source := "misospace/miso-gallery#502"
	shape := CriteriaHeaderShape(source)
	if !strings.Contains(shape, source) || strings.Contains(shape, SourceReferencePlaceholder) {
		t.Fatalf("CriteriaHeaderShape(%q) = %q, which does not name the source in place of the placeholder", source, shape)
	}
	for _, coverage := range []string{CoverageFull, CoveragePartial} {
		rendered := strings.ReplaceAll(renderHeader(source, coverage), DigestPlaceholder, strings.Repeat("a", 64))
		if !criteriaHeaderPattern.MatchString(rendered) {
			t.Fatalf("header claiming coverage=%s is not accepted by the header pattern: %q", coverage, rendered)
		}
	}

	for _, status := range StatusVocabulary() {
		row := renderRow("1", string(status), "internal/broker/criteria.go")
		if !criteriaRowPattern.MatchString(row) {
			t.Fatalf("row rendered from the grammar template for status %q is not accepted by the row pattern: %q", status, row)
		}
	}
	if criteriaRowPattern.MatchString(renderRow("1", "not-a-status", "internal/broker/criteria.go")) {
		t.Fatal("a row rendered with an out-of-vocabulary status must not satisfy the row pattern")
	}
}

// TestStatusVocabularyMatchesRowPatternAlternation is the bidirectional
// drift guard for the closed vocabulary: the order and the membership must
// agree with the row pattern's alternation in both directions, so a status
// added to one side and not the other fails here.
func TestStatusVocabularyMatchesRowPatternAlternation(t *testing.T) {
	match := rowStatusAlternation.FindStringSubmatch(criteriaRowPattern.String())
	if match == nil {
		t.Fatalf("row pattern carries no literal alternation: %q", criteriaRowPattern.String())
	}
	inPattern := strings.Split(match[1], "|")
	taught := StatusVocabulary()
	if len(taught) != len(inPattern) {
		t.Fatalf("StatusVocabulary() has %d statuses, the row pattern alternation has %d: %q", len(taught), len(inPattern), inPattern)
	}
	for i, status := range taught {
		if string(status) != inPattern[i] {
			t.Fatalf("status %d: StatusVocabulary() = %q, row pattern alternation = %q", i, status, inPattern[i])
		}
		if !criteriaRowPattern.MatchString(renderRow("1", string(status), "internal/broker/criteria.go")) {
			t.Fatalf("row pattern rejects its own alternation member %q", status)
		}
	}
}

// TestStatusVocabularySentenceCoversClosedVocabulary pins the prose list:
// it names every status exactly once, in the validator's order, and no
// other token.
func TestStatusVocabularySentenceCoversClosedVocabulary(t *testing.T) {
	sentence := StatusVocabularySentence()
	if sentence != derivedVocabulary {
		t.Fatalf("StatusVocabularySentence() = %q, want %q", sentence, derivedVocabulary)
	}
	match := rowStatusAlternation.FindStringSubmatch(criteriaRowPattern.String())
	if match == nil {
		t.Fatalf("row pattern carries no literal alternation: %q", criteriaRowPattern.String())
	}
	taught := strings.Split(strings.ReplaceAll(sentence, ", or ", ", "), ", ")
	inPattern := strings.Split(match[1], "|")
	if len(taught) != len(inPattern) {
		t.Fatalf("sentence names %d statuses, the row pattern alternation has %d: %q", len(taught), len(inPattern), taught)
	}
	for i, name := range taught {
		if name != inPattern[i] {
			t.Fatalf("sentence token %d = %q, row pattern alternation = %q", i, name, inPattern[i])
		}
		if got := strings.Count(sentence, name); got != 1 {
			t.Fatalf("sentence names %q %d times: %q", name, got, sentence)
		}
	}
}

// TestStatusReferenceRuleSentenceMatchesValidator pins the rule sentence
// against the rule the validator enforces: a status is named iff
// validRecordShape imposes a non-free requirement on it, and the sentence's
// two clauses group exactly the statuses validRecordShape separates —
// location, note, and unconstrained.
func TestStatusReferenceRuleSentenceMatchesValidator(t *testing.T) {
	sentence := StatusReferenceRuleSentence()
	if sentence != derivedRuleSentence {
		t.Fatalf("StatusReferenceRuleSentence() = %q, want %q", sentence, derivedRuleSentence)
	}
	const (
		located = "internal/broker/criteria.go"
		note    = "inspected by hand"
	)
	for _, status := range StatusVocabulary() {
		requirement, known := criterionReferenceRequirementFor(status)
		if !known {
			t.Fatalf("status %q is in the vocabulary but has no reference rule", status)
		}
		mentioned := strings.Contains(sentence, string(status))
		if mentioned != (requirement != referenceFree) {
			t.Fatalf("status %q: sentence names it = %t, rule = %v", status, mentioned, requirement)
		}
		switch requirement {
		case referenceLocationRequired:
			if !strings.Contains(sentence, "must cite a file path or check URL") {
				t.Fatalf("location rule missing from sentence: %q", sentence)
			}
			if validRecordShape(CriterionRecord{Status: status}) {
				t.Fatalf("%q must reject an empty reference", status)
			}
			if !validRecordShape(CriterionRecord{Status: status, Reference: located}) {
				t.Fatalf("%q must accept a located reference", status)
			}
			if validRecordShape(CriterionRecord{Status: status, Reference: note}) {
				t.Fatalf("%q must reject a reference with no location", status)
			}
		case referenceNoteRequired:
			if !strings.Contains(sentence, "must carry a note") {
				t.Fatalf("note rule missing from sentence: %q", sentence)
			}
			if validRecordShape(CriterionRecord{Status: status}) {
				t.Fatalf("%q must reject an empty reference", status)
			}
			if !validRecordShape(CriterionRecord{Status: status, Reference: note}) {
				t.Fatalf("%q must accept a bare note", status)
			}
		default:
			if !validRecordShape(CriterionRecord{Status: status}) {
				t.Fatalf("%q must accept an empty reference", status)
			}
		}
	}
	// A status outside the closed vocabulary is rejected, and named in
	// neither clause.
	if validRecordShape(CriterionRecord{Status: CriterionStatus("not-a-status"), Reference: located}) {
		t.Fatal("a status outside the vocabulary must not satisfy the shape rules")
	}
	if strings.Contains(sentence, "not-a-status") {
		t.Fatalf("sentence names a status outside the vocabulary: %q", sentence)
	}
	locationClause, noteClause, ok := strings.Cut(sentence, "; ")
	if !ok || !strings.HasSuffix(locationClause, "must cite a file path or check URL") || !strings.HasSuffix(noteClause, "must carry a note") {
		t.Fatalf("sentence is not the location clause followed by the note clause: %q", sentence)
	}
	if got := strings.TrimSuffix(locationClause, " must cite a file path or check URL"); got != "regression-check and implementation-evidence" {
		t.Fatalf("location clause names %q", got)
	}
	if got := strings.TrimSuffix(noteClause, " must carry a note"); got != "manual-verification and not-implemented" {
		t.Fatalf("note clause names %q", got)
	}
}
