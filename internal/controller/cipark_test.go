package controller

import (
	"context"
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

// TestCIParkCapacityReacquisitionReadsLiveReaderNotLaggingCache is the #548
// repair-side regression: a competing run claims the final slot of a
// concurrency-1 lane and the informer has not caught up. The reacquisition
// check must see the live admitted count and refuse the slot, then grant it
// once the competitor genuinely stops consuming capacity.
func TestCIParkCapacityReacquisitionReadsLiveReaderNotLaggingCache(t *testing.T) {
	ctx := context.Background()
	live := admissionClient(t,
		admissionLane("local", 1),
		admissionRun("parked", "local", courierv1alpha1.PhaseVerifying),
		admissionRun("competitor", "local", courierv1alpha1.PhasePending),
	)
	cached := newLaggingRunCache(ctx, t, live)

	// The competitor claims the slot after the cache snapshot was pinned.
	var competitor courierv1alpha1.CoderRun
	if err := live.Get(ctx, admissionKey("competitor"), &competitor); err != nil {
		t.Fatalf("get competitor run: %v", err)
	}
	competitor.Status.Phase = courierv1alpha1.PhaseClaimed
	if err := live.Status().Update(ctx, &competitor); err != nil {
		t.Fatalf("claim the competitor run: %v", err)
	}
	var stale courierv1alpha1.CoderRunList
	if err := cached.List(ctx, &stale); err != nil {
		t.Fatalf("lagging cached list: %v", err)
	}
	if got := admittedCount(stale.Items, "local"); got != 0 {
		t.Fatalf("lagging cache admittedCount = %d, want 0; the test must model a cache that has not caught up", got)
	}

	reconciler := &CoderRunReconciler{Client: cached, APIReader: live}
	var parked courierv1alpha1.CoderRun
	if err := live.Get(ctx, admissionKey("parked"), &parked); err != nil {
		t.Fatalf("get parked run: %v", err)
	}

	available, err := reconciler.ciparkCapacityAvailable(ctx, &parked)
	if err != nil {
		t.Fatalf("ciparkCapacityAvailable() error = %v", err)
	}
	if available {
		t.Fatal("ciparkCapacityAvailable() = true, want false; a lagging cache must not grant the final slot of a full concurrency-1 lane to a repair")
	}

	// The lane frees for real and the same check grants the slot.
	competitor.Status.Phase = courierv1alpha1.PhaseVerifying
	if err := live.Status().Update(ctx, &competitor); err != nil {
		t.Fatalf("release the competitor's slot: %v", err)
	}
	available, err = reconciler.ciparkCapacityAvailable(ctx, &parked)
	if err != nil {
		t.Fatalf("ciparkCapacityAvailable() error = %v", err)
	}
	if !available {
		t.Fatal("ciparkCapacityAvailable() = false after the slot freed, want true")
	}
}

// TestAbandonCIRepairAttempt pins the terminal abandonment of a repair
// attempt still in flight: a fenced context is carried into the unsupported
// settlement the source is owed, an unfenced context is cleared without
// recording anything because there is no grant to answer, and a fenced
// abandonment replaces a moot owed close while an unfenced one never
// disturbs it.
func TestAbandonCIRepairAttempt(t *testing.T) {
	noContext := &courierv1alpha1.CoderRun{}
	abandonCIRepairAttempt(noContext)
	if noContext.Status.CISettlement != nil {
		t.Fatalf("settlement = %#v with no repair context, want nil", noContext.Status.CISettlement)
	}

	unfenced := &courierv1alpha1.CoderRun{}
	unfenced.Status.CIRepair = &courierv1alpha1.CIRepairContext{Head: "abc123", Attempt: 1}
	abandonCIRepairAttempt(unfenced)
	if unfenced.Status.CISettlement != nil {
		t.Fatalf("settlement = %#v for an unfenced context, want nil", unfenced.Status.CISettlement)
	}
	if unfenced.Status.CIRepair != nil {
		t.Fatalf("repair context = %#v, want cleared", unfenced.Status.CIRepair)
	}

	fenced := &courierv1alpha1.CoderRun{}
	fenced.Status.CIRepair = &courierv1alpha1.CIRepairContext{Head: "abc123", Attempt: 2, FenceToken: "fence-two"}
	abandonCIRepairAttempt(fenced)
	if fenced.Status.CIRepair != nil {
		t.Fatalf("repair context = %#v, want cleared", fenced.Status.CIRepair)
	}
	if fenced.Status.CISettlement == nil {
		t.Fatal("settlement = nil for a fenced context, want the unsupported abandonment")
	}
	if fenced.Status.CISettlement.FenceToken != "fence-two" {
		t.Fatalf("settlement fence = %q, want the granted fence", fenced.Status.CISettlement.FenceToken)
	}
	if fenced.Status.CISettlement.Result != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("settlement result = %q, want unsupported (an abandonment publishes nothing)", fenced.Status.CISettlement.Result)
	}
	if fenced.Status.CISettlement.Head != "" {
		t.Fatalf("settlement head = %q, want empty (only a repush carries a head)", fenced.Status.CISettlement.Head)
	}

	// A fenced in-flight attempt carries the newest owed close: a source
	// grants a later reservation only after the earlier outcome committed,
	// so the stale record is moot and the live fence is the one that must
	// not outlive the run.
	owed := &courierv1alpha1.CoderRun{}
	owed.Status.CIRepair = &courierv1alpha1.CIRepairContext{Head: "abc123", Attempt: 2, FenceToken: "fence-one"}
	owed.Status.CISettlement = &courierv1alpha1.CISettlement{
		FenceToken: "fence-zero",
		Result:     courierv1alpha1.CIParkOutcomeRepushed,
		Head:       "def456",
	}
	abandonCIRepairAttempt(owed)
	if owed.Status.CIRepair != nil {
		t.Fatalf("repair context = %#v, want cleared", owed.Status.CIRepair)
	}
	if owed.Status.CISettlement == nil ||
		owed.Status.CISettlement.FenceToken != "fence-one" ||
		owed.Status.CISettlement.Result != courierv1alpha1.CIParkOutcomeUnsupported ||
		owed.Status.CISettlement.Head != "" {
		t.Fatalf("settlement = %#v, want the live fence's unsupported abandonment to replace the moot owed close", owed.Status.CISettlement)
	}

	// An unfenced context owes nothing and never destroys an owed close.
	unfencedOwed := &courierv1alpha1.CoderRun{}
	unfencedOwed.Status.CIRepair = &courierv1alpha1.CIRepairContext{Head: "abc123", Attempt: 1}
	unfencedOwed.Status.CISettlement = &courierv1alpha1.CISettlement{
		FenceToken: "fence-zero",
		Result:     courierv1alpha1.CIParkOutcomeRepushed,
		Head:       "def456",
	}
	abandonCIRepairAttempt(unfencedOwed)
	if unfencedOwed.Status.CIRepair != nil {
		t.Fatalf("repair context = %#v, want cleared", unfencedOwed.Status.CIRepair)
	}
	if unfencedOwed.Status.CISettlement.Result != courierv1alpha1.CIParkOutcomeRepushed ||
		unfencedOwed.Status.CISettlement.FenceToken != "fence-zero" || unfencedOwed.Status.CISettlement.Head != "def456" {
		t.Fatalf("settlement = %#v, want the already-owed close untouched", unfencedOwed.Status.CISettlement)
	}
}
