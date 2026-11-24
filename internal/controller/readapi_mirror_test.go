package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/readapi"
)

// TestReadAPIReapMirrorPinsControllerValues pins the reap facts the operator
// read model mirrors from this package. The read model states a retained run's
// phase TTL, its expiry off the durable marker, and which marker is present, so
// a value that moves here and not there would make the page promise an expiry or
// a marker the reaper will not honor. The wiring passes the configured flag
// values (cmd/main.go), so the retention pin covers the fallback every
// projection uses when nothing overrides it.
func TestReadAPIReapMirrorPinsControllerValues(t *testing.T) {
	for _, tc := range []struct {
		name       string
		controller time.Duration
		readapi    time.Duration
	}{
		{"done retention", defaultDoneRetention, readapi.DefaultDoneRetention},
		{"failed retention", defaultFailedRetention, readapi.DefaultFailedRetention},
		{"needs-human retention", defaultNeedsHumanRetention, readapi.DefaultNeedsHumanRetention},
	} {
		if tc.controller != tc.readapi {
			t.Errorf("%s: controller %s, readapi %s", tc.name, tc.controller, tc.readapi)
		}
	}
	for _, tc := range []struct {
		name       string
		controller string
		readapi    string
	}{
		{"done-at marker", doneAtAnnotation, readapi.AnnotationDoneAt},
		{"failed-at marker", failedAtAnnotation, readapi.AnnotationFailedAt},
		{"needs-human-at marker", needsHumanAtAnnotation, readapi.AnnotationNeedsHumanAt},
		{"already-done-at marker", alreadyDoneAtAnnotation, readapi.AnnotationAlreadyDoneAt},
	} {
		if tc.controller != tc.readapi {
			t.Errorf("%s: controller %q, readapi %q", tc.name, tc.controller, tc.readapi)
		}
	}
}

// TestReadAPICapacityMirrorPinsControllerValues pins the capacity-recovery
// vocabulary the read model reads to explain an ownership hold. A renamed reason
// or an added diagnostic category would otherwise silently change whether the
// page blames the unsafe-workspace gate, the operator wait, or nothing.
func TestReadAPICapacityMirrorPinsControllerValues(t *testing.T) {
	for _, tc := range []struct {
		name       string
		controller string
		readapi    string
	}{
		{"settled reason", capacityRecoverySettled, readapi.CapacityRecoverySettled},
		{"unsupported reason", capacityRecoveryUnsupported, readapi.CapacityRecoveryUnsupported},
		{"stale reason", capacityRecoveryStale, readapi.CapacityRecoveryStale},
		{"unsafe-work reason", capacityRecoveryUnsafeWork, readapi.CapacityRecoveryUnsafeWork},
	} {
		if tc.controller != tc.readapi {
			t.Errorf("%s: controller %q, readapi %q", tc.name, tc.controller, tc.readapi)
		}
	}

	// Every category the controller routes into capacity recovery, plus ones it
	// does not, in both the exempt and the governing phase.
	categories := []courierv1alpha1.TerminalDiagnosticCategory{
		courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
		courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable,
		courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable,
		courierv1alpha1.TerminalDiagnosticCategoryModelTransportError,
		courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash,
		courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection,
		"",
	}
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseBlockedExternal, courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseNeedsHuman,
	} {
		for _, category := range categories {
			run := &courierv1alpha1.CoderRun{Status: courierv1alpha1.CoderRunStatus{
				Phase:              phase,
				TerminalDiagnostic: &courierv1alpha1.TerminalDiagnostic{Category: category},
			}}
			if got, want := readapi.IsCapacityExhaustionRun(run), isCapacityExhaustionRun(run); got != want {
				t.Errorf("capacity exhaustion phase=%s category=%s: readapi %v, controller %v", phase, category, got, want)
			}
		}
	}
	// A missing diagnostic is a distinct case the controller refuses.
	nilDiagnostic := &courierv1alpha1.CoderRun{Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseBlockedExternal}}
	if got, want := readapi.IsCapacityExhaustionRun(nilDiagnostic), isCapacityExhaustionRun(nilDiagnostic); got != want {
		t.Errorf("capacity exhaustion without diagnostic: readapi %v, controller %v", got, want)
	}
}

// TestReadAPIAuditFloorMirrorPinsControllerPhases pins which phases the read
// model says consume an audit-floor slot. The page uses it to decide whether an
// unexplained retention may be the floor, so a phase that joins or leaves the
// floor here must move there too.
func TestReadAPIAuditFloorMirrorPinsControllerPhases(t *testing.T) {
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhasePending, courierv1alpha1.PhaseClaimed, courierv1alpha1.PhaseRunning,
		courierv1alpha1.PhaseVerifying, courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseHandedOff,
		courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseDone, courierv1alpha1.PhaseAlreadyDone,
		courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseBlockedExternal, "",
	} {
		if got, want := readapi.AuditFloorGovernsPhase(phase), auditFloorPhase(phase); got != want {
			t.Errorf("audit floor phase %q: readapi %v, controller %v", phase, got, want)
		}
	}
}

// TestReadAPIReapMarkerSelectionMatchesController pins the read model's
// current-phase marker selection against this package: the phase-to-marker
// mapping the terminal dispatch uses, and the controller.reapMarker parse the
// window is measured from. A run can transition between terminal phases and
// keep the earlier outcome's marker, so a projection that picked "the first
// marker present" would promise an expiry the reaper will not honor; this pin
// forces the projection to read exactly what reapTerminal reads.
func TestReadAPIReapMarkerSelectionMatchesController(t *testing.T) {
	for _, tc := range []struct {
		phase  courierv1alpha1.Phase
		marker string
	}{
		{courierv1alpha1.PhaseDone, doneAtAnnotation},
		{courierv1alpha1.PhaseAlreadyDone, alreadyDoneAtAnnotation},
		{courierv1alpha1.PhaseFailed, failedAtAnnotation},
		{courierv1alpha1.PhaseNeedsHuman, needsHumanAtAnnotation},
		{courierv1alpha1.PhaseRunning, ""},
		{courierv1alpha1.PhaseAwaitingReview, ""},
	} {
		if got := readapi.ReapMarkerForPhase(tc.phase); got != tc.marker {
			t.Errorf("phase %q: readapi names marker %q, controller dispatch reads %q", tc.phase, got, tc.marker)
		}
	}

	stale := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	valid := stale.Add(time.Hour).Format(time.RFC3339)
	for _, tc := range []struct {
		name        string
		phase       courierv1alpha1.Phase
		annotations map[string]string
	}{
		{"stale prior-phase marker only", courierv1alpha1.PhaseNeedsHuman, map[string]string{failedAtAnnotation: stale.Format(time.RFC3339)}},
		{"missing current marker", courierv1alpha1.PhaseDone, map[string]string{alreadyDoneAtAnnotation: valid}},
		{"whitespace-padded current marker", courierv1alpha1.PhaseDone, map[string]string{doneAtAnnotation: "  " + valid + " "}},
		{"malformed current marker", courierv1alpha1.PhaseFailed, map[string]string{failedAtAnnotation: "not-a-timestamp"}},
		{"valid current marker beside a stale one", courierv1alpha1.PhaseDone, map[string]string{failedAtAnnotation: stale.Format(time.RFC3339), doneAtAnnotation: valid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &courierv1alpha1.CoderRun{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
			run.Status.Phase = tc.phase
			marker := readapi.ReapMarkerForPhase(tc.phase)
			if marker == "" {
				t.Fatalf("fixture phase %q has no controller marker", tc.phase)
			}
			controllerAt, controllerOK := reapMarker(run, marker)
			_, readAPIAt, readAPIOK := readapi.CurrentReapMarker(run)
			if controllerOK != readAPIOK {
				t.Fatalf("controller honors marker %v, read model %v", controllerOK, readAPIOK)
			}
			if controllerOK && (readAPIAt == nil || !readAPIAt.Time.Equal(controllerAt)) {
				t.Fatalf("controller measures from %v, read model from %v", controllerAt, readAPIAt)
			}
			if !controllerOK && readAPIAt != nil {
				t.Fatalf("read model stamped %v where the controller starts a fresh window", readAPIAt)
			}
		})
	}
}
