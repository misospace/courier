package controller

import (
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

func TestPublishedHead(t *testing.T) {
	cases := []struct {
		name           string
		lastCommit     string
		briefCommits   []string
		observed       string
		wantHead       string
		wantAttributed bool
	}{
		{
			name:           "last commit is the run's own published head",
			lastCommit:     "aaa",
			observed:       "aaa",
			wantHead:       "aaa",
			wantAttributed: true,
		},
		{
			name:           "a completed brief's commit is the run's own published head",
			briefCommits:   []string{"old", "bbb"},
			observed:       "bbb",
			wantHead:       "bbb",
			wantAttributed: true,
		},
		{
			name:           "a completed brief's commit older than the published head is superseded",
			lastCommit:     "aaa",
			briefCommits:   []string{"old", "aaa"},
			observed:       "old",
			wantAttributed: false,
		},
		{
			name:           "a head the run cannot account for is foreign",
			lastCommit:     "aaa",
			briefCommits:   []string{"bbb"},
			observed:       "ccc",
			wantAttributed: false,
		},
		{
			name:           "an unpublished head is foreign",
			lastCommit:     "aaa",
			observed:       "aaa-but-different",
			wantAttributed: false,
		},
		{
			name:           "a run that published nothing owns no head",
			observed:       "aaa",
			wantAttributed: false,
		},
		{
			name:           "an empty observation owns no head",
			lastCommit:     "aaa",
			observed:       "",
			wantAttributed: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := &courierv1alpha1.CoderRun{}
			run.Status.LastCommit = tc.lastCommit
			if len(tc.briefCommits) > 0 {
				run.Status.Checkpoint = &courierv1alpha1.Checkpoint{}
				for _, commit := range tc.briefCommits {
					run.Status.Checkpoint.CompletedBriefs = append(run.Status.Checkpoint.CompletedBriefs, courierv1alpha1.Brief{ID: "b", Commit: commit})
				}
			}
			head, attributed := publishedHead(run, tc.observed)
			if attributed != tc.wantAttributed {
				t.Fatalf("publishedHead() attributed = %v, want %v", attributed, tc.wantAttributed)
			}
			if head != tc.wantHead {
				t.Fatalf("publishedHead() head = %q, want %q", head, tc.wantHead)
			}
		})
	}
}

func TestMechanicalSelectorMatchesExactly(t *testing.T) {
	selector := mechanicalSelector([]string{"build", "test", "", "build"})
	for _, name := range []string{"build", "test"} {
		if !selector(name) {
			t.Fatalf("selector(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"Build", "BUILD", "build ", "build\t", " test", "unit-test", "ai-review", ""} {
		if selector(name) {
			t.Fatalf("selector(%q) = true, want false", name)
		}
	}
	// A declared name padded with space is its own exact name: it never
	// widens to the unpadded check, so a misdeclared lane fails closed with
	// a check that can never register instead of silently matching.
	padded := mechanicalSelector([]string{" test "})
	if padded("test") {
		t.Fatal(`selector("test") = true for a lane declaring " test ", want false`)
	}
	if !padded(" test ") {
		t.Fatal(`selector(" test ") = false, want the exact declared name to match`)
	}
}

func TestFailedMechanicalChecks(t *testing.T) {
	observation := PRObservation{
		Head: "head",
		Checks: []CheckObservation{
			{Name: "build", State: CheckStateFailed},
			{Name: "ai-review", State: CheckStateFailed},
			{Name: "test", State: CheckStatePassed},
			{Name: "lint", State: CheckStateFailed},
		},
	}
	selector := mechanicalSelector([]string{"build", "lint"})
	got := failedMechanicalChecks(observation, selector)
	if len(got) != 2 || got[0] != "build" || got[1] != "lint" {
		t.Fatalf("failedMechanicalChecks() = %v, want [build lint]", got)
	}
}

func TestMechanicalVerdict(t *testing.T) {
	cases := []struct {
		name         string
		configured   []string
		checks       []CheckObservation
		wantGreen    bool
		wantEligible int
	}{
		{
			name:       "every configured mechanical check passed",
			configured: []string{"build", "test"},
			checks: []CheckObservation{
				{Name: "build", State: CheckStatePassed},
				{Name: "test", State: CheckStatePassed},
				{Name: "ai-review", State: CheckStatePending},
			},
			wantGreen:    true,
			wantEligible: 2,
		},
		{
			name:       "a failed mechanical check is never green",
			configured: []string{"build"},
			checks: []CheckObservation{
				{Name: "build", State: CheckStateFailed},
			},
			wantGreen:    false,
			wantEligible: 1,
		},
		{
			name:       "a failed non-mechanical check is never green",
			configured: []string{"build"},
			checks: []CheckObservation{
				{Name: "build", State: CheckStatePassed},
				{Name: "ai-review", State: CheckStateFailed},
			},
			wantGreen:    false,
			wantEligible: 1,
		},
		{
			name:       "a pending mechanical check is never green",
			configured: []string{"build"},
			checks: []CheckObservation{
				{Name: "build", State: CheckStatePending},
			},
			wantGreen:    false,
			wantEligible: 1,
		},
		{
			name:       "no configured mechanical check registered is never green",
			configured: []string{"build"},
			checks: []CheckObservation{
				{Name: "ai-review", State: CheckStatePassed},
			},
			wantGreen:    false,
			wantEligible: 0,
		},
		{
			name:       "a configured check that never registered is never green even when the observed ones passed",
			configured: []string{"build", "test"},
			checks: []CheckObservation{
				{Name: "build", State: CheckStatePassed},
			},
			wantGreen:    false,
			wantEligible: 1,
		},
		{
			name:       "a space-padded registration does not cover the unpadded check",
			configured: []string{"build", " test"},
			checks: []CheckObservation{
				{Name: "build", State: CheckStatePassed},
				{Name: "test", State: CheckStatePassed},
			},
			wantGreen:    false,
			wantEligible: 1,
		},
		{
			name:       "a case-mismatched registration does not cover the configured check",
			configured: []string{"build", "Test"},
			checks: []CheckObservation{
				{Name: "build", State: CheckStatePassed},
				{Name: "test", State: CheckStatePassed},
			},
			wantGreen:    false,
			wantEligible: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			green, eligible := mechanicalVerdict(PRObservation{Checks: tc.checks}, tc.configured)
			if green != tc.wantGreen {
				t.Fatalf("mechanicalVerdict() green = %v, want %v", green, tc.wantGreen)
			}
			if eligible != tc.wantEligible {
				t.Fatalf("mechanicalVerdict() eligible = %d, want %d", eligible, tc.wantEligible)
			}
		})
	}
}

func TestCIParkFenceTokenIsDerivedFromRunIdentityOnly(t *testing.T) {
	run := &courierv1alpha1.CoderRun{}
	run.UID = "run-uid"
	run.Spec.Lane = "cloud-lane"
	run.Name = "courier-run"
	first := ciparkFenceToken(run, 1)
	second := ciparkFenceToken(run, 1)
	if first != second {
		t.Fatalf("ciparkFenceToken() is not deterministic: %q != %q", first, second)
	}
	if other := ciparkFenceToken(run, 2); other == first {
		t.Fatal("ciparkFenceToken() does not separate attempts")
	}
	laneChanged := run.DeepCopy()
	laneChanged.Spec.Lane = "another-lane"
	if changed := ciparkFenceToken(laneChanged, 1); changed != first {
		t.Fatal("ciparkFenceToken() depends on the lane name")
	}
	for _, forbidden := range []string{"cloud-lane", "courier-run", "coordinator"} {
		if strings.Contains(first, forbidden) {
			t.Fatalf("ciparkFenceToken() leaked %q", forbidden)
		}
	}
}
