package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// resetHintExhaustionRun builds a durable exhaustion event: a BlockedExternal
// run on pool-a carrying the given terminal diagnostic and a recorded-cooldown
// marker stamped at marker.
func resetHintExhaustionRun(name string, marker time.Time, diagnostic *courierv1alpha1.TerminalDiagnostic) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "pool-a", courierv1alpha1.PhaseBlockedExternal)
	run.UID = types.UID("uid-" + name)
	run.Status.TerminalDiagnostic = diagnostic
	run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{
		Type: conditionCapacityCooldownRecorded, Status: metav1.ConditionTrue,
		Reason: "Recorded", LastTransitionTime: metav1.NewTime(marker),
	})
	return run
}

func recordedCooldown(t *testing.T, c client.Client, laneName string) *courierv1alpha1.CapacityCooldown {
	t.Helper()
	var got courierv1alpha1.LaneProfile
	if err := c.Get(context.Background(), admissionKey(laneName), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CapacityCooldown == nil {
		t.Fatal("no cooldown recorded")
	}
	return got.Status.CapacityCooldown
}

// TestCapacityCooldownExtendsDeadlineToValidatedProviderReset pins #866: a
// confirmed exhaustion whose durable diagnostic carries a provider reset
// within the horizon extends the cooldown deadline to the reset, not the
// marker plus window — and a restarted reconciler recomputes the same
// deadline from durable evidence alone.
func TestCapacityCooldownExtendsDeadlineToValidatedProviderReset(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	reset := metav1.Time{Time: now.Add(9 * time.Hour)}
	run := resetHintExhaustionRun("hinted", now, &courierv1alpha1.TerminalDiagnostic{
		Category:        courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
		ProviderResetAt: &reset,
	})
	lane := admissionLane("pool-a", 1)
	c := capacityTestClient(t, lane, run)
	r := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}
	if err := r.recordCapacityCooldown(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	cooldown := recordedCooldown(t, c, "pool-a")
	if !cooldown.NextEligibleAt.Time.Equal(reset.Time) {
		t.Fatalf("deadline = %v, want provider reset %v", cooldown.NextEligibleAt, reset)
	}
	if cooldown.ExhaustionCount != 1 {
		t.Fatalf("count = %d, want 1", cooldown.ExhaustionCount)
	}
	// Restart durability: a fresh reconciler over the same stored objects
	// re-derives the identical deadline; nothing about the hint lives in
	// process memory.
	var fresh courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &fresh); err != nil {
		t.Fatal(err)
	}
	restarted := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}
	if err := restarted.recordCapacityCooldown(context.Background(), &fresh); err != nil {
		t.Fatal(err)
	}
	if again := recordedCooldown(t, c, "pool-a"); !again.NextEligibleAt.Time.Equal(reset.Time) {
		t.Fatalf("deadline after restart = %v, want %v", again.NextEligibleAt, reset)
	}
}

// TestCapacityCooldownIgnoresPastOrOverHorizonProviderReset pins the bounds:
// a reset at or before the window end never shortens or extends (the window
// is the floor); a crafted reset past the 24h horizon never extends; and a
// reset inside the horizon but beyond the window does extend, proving the
// bound admits it.
func TestCapacityCooldownIgnoresPastOrOverHorizonProviderReset(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reset time.Duration
		want  time.Duration
	}{
		{name: "reset before window end keeps the window floor", reset: 5 * time.Minute, want: 15 * time.Minute},
		{name: "crafted reset beyond horizon is ignored", reset: 24*time.Hour + time.Minute, want: 15 * time.Minute},
		{name: "reset inside horizon extends at the bound", reset: 23*time.Hour + 59*time.Minute, want: 23*time.Hour + 59*time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
			reset := metav1.Time{Time: now.Add(tc.reset)}
			run := resetHintExhaustionRun("bounded", now, &courierv1alpha1.TerminalDiagnostic{
				Category:        courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
				ProviderResetAt: &reset,
			})
			lane := admissionLane("pool-a", 1)
			c := capacityTestClient(t, lane, run)
			r := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}
			if err := r.recordCapacityCooldown(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			if got := recordedCooldown(t, c, "pool-a"); !got.NextEligibleAt.Time.Equal(now.Add(tc.want)) {
				t.Fatalf("deadline = %v, want %v", got.NextEligibleAt, now.Add(tc.want))
			}
		})
	}
}

// TestCapacityCooldownNeverExtendsFromNonCapacityDiagnostic proves the
// defensive guard: the extension reads ProviderResetAt only from a
// capacity-exhaustion diagnostic. A crafted reset on another recoverable
// category — which isCapacityExhaustionRun still admits to the cooldown —
// never moves the deadline.
func TestCapacityCooldownNeverExtendsFromNonCapacityDiagnostic(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	reset := metav1.Time{Time: now.Add(9 * time.Hour)}
	run := resetHintExhaustionRun("provider-blocked", now, &courierv1alpha1.TerminalDiagnostic{
		Category:        courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable,
		ProviderResetAt: &reset,
	})
	lane := admissionLane("pool-a", 1)
	c := capacityTestClient(t, lane, run)
	r := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}
	if err := r.recordCapacityCooldown(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	cooldown := recordedCooldown(t, c, "pool-a")
	if !cooldown.NextEligibleAt.Time.Equal(now.Add(15 * time.Minute)) {
		t.Fatalf("deadline = %v, want window floor %v", cooldown.NextEligibleAt, now.Add(15*time.Minute))
	}
}

// TestCapacityProviderResetDeadlineBounds captures the helper contract directly: the
// deadline exists only for a validated capacity-exhaustion handoff with a
// hint in (0, 86400] seconds.
func TestCapacityProviderResetDeadlineBounds(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		handoff string
		want    time.Duration
	}{
		{name: "valid hint", handoff: `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"capacity_exhausted","provider_reset_after_seconds":3600}`, want: 3600 * time.Second},
		{name: "boundary hint", handoff: `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"capacity_exhausted","provider_reset_after_seconds":86400}`, want: 86400 * time.Second},
		{name: "absent hint", handoff: `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"capacity_exhausted"}`},
		{name: "zero hint", handoff: `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"capacity_exhausted","provider_reset_after_seconds":0}`},
		{name: "negative hint", handoff: `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"capacity_exhausted","provider_reset_after_seconds":-5}`},
		{name: "over-horizon hint", handoff: `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"capacity_exhausted","provider_reset_after_seconds":86401}`},
		{name: "hint on non-exhaustion outcome", handoff: `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"blocked_external","provider_reset_after_seconds":3600}`},
		{name: "malformed handoff", handoff: `{"phase":"NeedsHuman"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := terminatedControlPod(tc.handoff, 2)
			deadline, ok := providerResetDeadline(now, pod)
			if ok != (tc.want > 0) {
				t.Fatalf("ok = %v, want %v", ok, tc.want > 0)
			}
			if ok && !deadline.Time.Equal(now.Add(tc.want)) {
				t.Fatalf("deadline = %v, want %v", deadline, now.Add(tc.want))
			}
		})
	}
	if _, ok := providerResetDeadline(now, nil); ok {
		t.Fatal("nil pod produced a deadline")
	}
}

// TestCapacityTerminationCapturePersistsValidatedProviderReset drives the full
// terminal-capture path with a terminated coordinator whose validated
// capacity-exhaustion handoff carries the #866 hint: only hints in
// (0, 86400] on a capacity-exhaustion ending land on the durable
// diagnostic, stamped absolute from the operator clock; everything else —
// absent, zero, negative, out of bounds, or a hint on a non-capacity ending
// — leaves it absent.
func TestCapacityTerminationCapturePersistsValidatedProviderReset(t *testing.T) {
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	capacityHandoff := func(hint string) string {
		return `COURIER_TERMINATION {"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":"provider allowance spent","outcome":"capacity_exhausted","category":"capacity-exhaustion"` + hint + `}`
	}
	for _, tc := range []struct {
		name    string
		handoff string
		want    *time.Duration
	}{
		{name: "valid hint captured", handoff: capacityHandoff(`,"provider_reset_after_seconds":3600`), want: durationPtr(time.Hour)},
		{name: "absent hint", handoff: capacityHandoff("")},
		{name: "zero hint", handoff: capacityHandoff(`,"provider_reset_after_seconds":0`)},
		{name: "negative hint", handoff: capacityHandoff(`,"provider_reset_after_seconds":-5`)},
		{name: "over-horizon hint", handoff: capacityHandoff(`,"provider_reset_after_seconds":86401`)},
		{name: "hint on a task-shaped block never captured", handoff: `COURIER_TERMINATION {"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":"missing prerequisite","outcome":"blocked_external","provider_reset_after_seconds":3600}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := admissionRun("hint-capture", "pool-a", courierv1alpha1.PhaseRunning)
			run.UID = types.UID("run-uid")
			run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeResolveIssue, "fake", "hint-work"
			run.Status.Branch = "branch"
			lane := admissionLaneWithCoordinator("pool-a", 1)
			lane.Spec.CapacityGroup = "shared-pool"
			pod := forgeUnavailableCoordinatorPod(t, run, lane, tc.handoff, "empty")
			c := capacityTestClient(t, lane, run, pod)
			s := &capacitySource{admissionSource: &admissionSource{}}
			r := &CoderRunReconciler{Client: c, APIReader: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), Now: func() time.Time { return now }}
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
					t.Fatalf("Reconcile attempt %d: %v", attempt+1, err)
				}
			}
			var stored courierv1alpha1.CoderRun
			if err := c.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Phase != courierv1alpha1.PhaseBlockedExternal {
				t.Fatalf("phase = %q, want blocked-external", stored.Status.Phase)
			}
			diagnostic := stored.Status.TerminalDiagnostic
			if diagnostic == nil {
				t.Fatal("no terminal diagnostic captured")
			}
			if tc.want == nil {
				if diagnostic.ProviderResetAt != nil {
					t.Fatalf("captured reset = %v, want absent", diagnostic.ProviderResetAt)
				}
				return
			}
			if diagnostic.ProviderResetAt == nil {
				t.Fatal("validated hint was not captured")
			}
			if !diagnostic.ProviderResetAt.Time.Equal(now.Add(*tc.want)) {
				t.Fatalf("captured reset = %v, want %v", diagnostic.ProviderResetAt, now.Add(*tc.want))
			}
		})
	}
}

func durationPtr(d time.Duration) *time.Duration { return &d }
