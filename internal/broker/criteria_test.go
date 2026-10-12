package broker

import (
	"errors"
	"os"
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
			wantErr: "criterion evidence missing for AC-1",
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
