package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
)

// The trusted native terminal result (#127) is the operator's classification
// evidence for a native coordinator's ending: the persisted status result
// fenced to the terminated pod's incarnation, or the pod's own validated
// termination handoff as the fallback channel. These tests pin the
// classification routes, the staleness fence, and the infrastructure-failure
// relaunch bounded by the crashloop ceiling.

func TestTrustedNativeResultFencing(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC))
	pod := func(kind courierv1alpha1.NativeResultKind, exitCode int32) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "run-control", Namespace: "runs", UID: types.UID("pod-uid-native")},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "coordinator",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, Message: nativeHandoffPayload(kind, exitCode)}},
			}}},
		}
	}
	tests := []struct {
		name   string
		result *courierv1alpha1.NativeResult
		pod    *corev1.Pod
		want   courierv1alpha1.NativeResultKind
		wantOK bool
	}{
		{
			name:   "status result fences to this pod",
			result: &courierv1alpha1.NativeResult{Kind: courierv1alpha1.NativeResultPublished, CoordinatorPodUID: "pod-uid-native", At: now},
			pod:    pod(courierv1alpha1.NativeResultWorkloadFailed, 1),
			want:   courierv1alpha1.NativeResultPublished,
			wantOK: true,
		},
		{
			name:   "stale status result from an earlier incarnation never classifies",
			result: &courierv1alpha1.NativeResult{Kind: courierv1alpha1.NativeResultPublished, CoordinatorPodUID: "pod-uid-previous", At: now},
			pod:    pod("", 1),
			wantOK: false,
		},
		{
			name:   "status result for a different pod falls back to the handoff kind",
			result: &courierv1alpha1.NativeResult{Kind: courierv1alpha1.NativeResultWorkloadFailed, CoordinatorPodUID: "pod-uid-previous", At: now},
			pod:    pod(courierv1alpha1.NativeResultPublished, 0),
			want:   courierv1alpha1.NativeResultPublished,
			wantOK: true,
		},
		{
			name:   "valid handoff kind is the fallback channel",
			pod:    pod(courierv1alpha1.NativeResultPublished, 0),
			want:   courierv1alpha1.NativeResultPublished,
			wantOK: true,
		},
		{
			name:   "no result and no handoff kind keeps the legacy classification",
			pod:    pod("", 0),
			wantOK: false,
		},
		{
			name:   "an unknown handoff kind is never a classification",
			pod:    pod(courierv1alpha1.NativeResultKind("genuinely_done"), 0),
			wantOK: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := admissionRun("trusted", "local", courierv1alpha1.PhaseRunning)
			run.Status.NativeResult = tc.result
			got, ok := trustedNativeResult(run, tc.pod)
			if ok != tc.wantOK {
				t.Fatalf("trustedNativeResult ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && got.Kind != tc.want {
				t.Fatalf("trustedNativeResult kind = %q, want %q", got.Kind, tc.want)
			}
		})
	}
}

func TestPhaseForNativeResult(t *testing.T) {
	tests := []struct {
		name  string
		mode  courierv1alpha1.Mode
		kind  courierv1alpha1.NativeResultKind
		phase courierv1alpha1.Phase
	}{
		{name: "published hands to world verification", kind: courierv1alpha1.NativeResultPublished, phase: courierv1alpha1.PhaseVerifying},
		{name: "no change resolve-issue waits for review", kind: courierv1alpha1.NativeResultNoChange, phase: courierv1alpha1.PhaseAwaitingReview},
		{name: "no change fix-pr settles against the world", mode: courierv1alpha1.ModeFixPR, kind: courierv1alpha1.NativeResultNoChange, phase: courierv1alpha1.PhaseVerifying},
		{name: "needs human parks for a human", kind: courierv1alpha1.NativeResultNeedsHuman, phase: courierv1alpha1.PhaseNeedsHuman},
		{name: "blocked external stays recoverable", kind: courierv1alpha1.NativeResultBlockedExternal, phase: courierv1alpha1.PhaseBlockedExternal},
		{name: "capacity exhaustion stays recoverable", kind: courierv1alpha1.NativeResultCapacityExhausted, phase: courierv1alpha1.PhaseBlockedExternal},
		{name: "workload failure is a terminal verdict", kind: courierv1alpha1.NativeResultWorkloadFailed, phase: courierv1alpha1.PhaseFailed},
		{name: "infrastructure failure never maps to a terminal phase", kind: courierv1alpha1.NativeResultInfrastructureFailed, phase: courierv1alpha1.PhaseFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mode := tc.mode
			if mode == "" {
				mode = courierv1alpha1.ModeResolveIssue
			}
			if got := phaseForNativeResult(mode, tc.kind); got != tc.phase {
				t.Fatalf("phaseForNativeResult(%s) = %q, want %q", tc.kind, got, tc.phase)
			}
		})
	}
}

// TestNativeResultClassificationReconcile drives the operator's trusted
// classification through the reconcile: every kind routes exactly as the
// legacy exit-code contract does, with the workload/infrastructure split the
// exit code alone could never express (#127).
func TestNativeResultClassificationReconcile(t *testing.T) {
	tests := []struct {
		name          string
		kind          courierv1alpha1.NativeResultKind
		mode          courierv1alpha1.Mode
		exitCode      int32
		secure        bool
		wantPhase     courierv1alpha1.Phase
		wantCategory  courierv1alpha1.TerminalDiagnosticCategory
		wantNoChange  bool
		wantVerifying bool // the run leaves Verifying only through the observer
	}{
		{
			name:          "published routes to Verifying",
			kind:          courierv1alpha1.NativeResultPublished,
			exitCode:      0,
			wantPhase:     courierv1alpha1.PhaseVerifying,
			wantVerifying: true,
		},
		{
			name:      "no change resolve-issue reaches AwaitingReview",
			kind:      courierv1alpha1.NativeResultNoChange,
			exitCode:  3,
			wantPhase: courierv1alpha1.PhaseAwaitingReview,
		},
		{
			name:          "no change fix-pr settles against the world",
			kind:          courierv1alpha1.NativeResultNoChange,
			mode:          courierv1alpha1.ModeFixPR,
			exitCode:      3,
			wantPhase:     courierv1alpha1.PhaseVerifying,
			wantNoChange:  true,
			wantVerifying: true,
		},
		{
			name:         "needs human parks for a human",
			kind:         courierv1alpha1.NativeResultNeedsHuman,
			exitCode:     2,
			wantPhase:    courierv1alpha1.PhaseNeedsHuman,
			wantCategory: courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision,
		},
		{
			name:         "blocked external stays recoverable",
			kind:         courierv1alpha1.NativeResultBlockedExternal,
			exitCode:     2,
			wantPhase:    courierv1alpha1.PhaseBlockedExternal,
			wantCategory: courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal,
		},
		{
			name:         "capacity exhaustion stays recoverable",
			kind:         courierv1alpha1.NativeResultCapacityExhausted,
			exitCode:     2,
			secure:       true,
			wantPhase:    courierv1alpha1.PhaseBlockedExternal,
			wantCategory: courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
		},
		{
			name:         "workload failure is a terminal verdict on the work",
			kind:         courierv1alpha1.NativeResultWorkloadFailed,
			exitCode:     1,
			wantPhase:    courierv1alpha1.PhaseFailed,
			wantCategory: courierv1alpha1.TerminalDiagnosticCategoryWorkloadFailure,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mode := tc.mode
			if mode == "" {
				mode = courierv1alpha1.ModeResolveIssue
			}
			run, reconciler := nativeResultFixture(t, mode, nativeResultSpec{
				kind:     tc.kind,
				exitCode: tc.exitCode,
			})
			if tc.secure {
				reconciler.Secure = &SecureControl{Client: reconciler.Client}
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			var stored courierv1alpha1.CoderRun
			if err := reconciler.Client.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
				t.Fatal(err)
			}
			if tc.wantVerifying {
				if stored.Status.Phase != courierv1alpha1.PhaseVerifying {
					t.Fatalf("phase = %q, want Verifying", stored.Status.Phase)
				}
				return
			}
			if stored.Status.Phase != tc.wantPhase {
				t.Fatalf("phase = %q, want %q", stored.Status.Phase, tc.wantPhase)
			}
			if tc.wantNoChange != stored.Status.DeclaredNoChange {
				t.Fatalf("declaredNoChange = %v, want %v", stored.Status.DeclaredNoChange, tc.wantNoChange)
			}
			if tc.wantCategory != "" && (stored.Status.TerminalDiagnostic == nil || stored.Status.TerminalDiagnostic.Category != tc.wantCategory) {
				t.Fatalf("terminal diagnostic = %#v, want category %q", stored.Status.TerminalDiagnostic, tc.wantCategory)
			}
		})
	}
}

// TestNativeResultPublishedNeverReachesReviewWithoutPR pins the acceptance
// rule that survives every classification change: a published handoff — with
// or without a persisted trusted result — only hands the run to Verifying,
// and the operator's own observation of a missing PR turns it NeedsHuman,
// never a falsely successful AwaitingReview (#127).
func TestNativeResultPublishedNeverReachesReviewWithoutPR(t *testing.T) {
	for _, tc := range []struct {
		name        string
		trustedKind courierv1alpha1.NativeResultKind
	}{
		{name: "trusted published result", trustedKind: courierv1alpha1.NativeResultPublished},
		{name: "no trusted result, exit 0 handoff only", trustedKind: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run, reconciler := nativeResultFixture(t, courierv1alpha1.ModeResolveIssue, nativeResultSpec{
				kind:     tc.trustedKind,
				exitCode: 0,
			})
			observer := &fakeWorldObserver{observation: PRObservation{}}
			reconciler.Observer = observer
			var stored courierv1alpha1.CoderRun
			for attempt := 0; attempt < 3; attempt++ {
				if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
					t.Fatalf("Reconcile: %v", err)
				}
				if err := reconciler.Client.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
					t.Fatal(err)
				}
				if stored.Status.Phase != courierv1alpha1.PhaseVerifying {
					break
				}
			}
			if stored.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
				t.Fatalf("phase = %q, want NeedsHuman for a zero exit with no verified PR", stored.Status.Phase)
			}
			if stored.Status.PR != "" {
				t.Fatalf("PR = %q, want none", stored.Status.PR)
			}
		})
	}
}

// TestNativeResultInfrastructureFailureRelaunches proves the trusted
// infrastructure failure is the relaunch/resume case bounded by the crashloop
// counter (HARNESS.md §8), never a terminal verdict on the work: below the
// ceiling the run returns to Claimed with the pod deleted so the deterministic
// name is free; at the ceiling the run is handed to a human and the terminated
// pod is preserved as the ending's evidence.
func TestNativeResultInfrastructureFailureRelaunches(t *testing.T) {
	t.Run("below the ceiling the run relaunches", func(t *testing.T) {
		run, reconciler := nativeResultFixture(t, courierv1alpha1.ModeResolveIssue, nativeResultSpec{
			kind:     courierv1alpha1.NativeResultInfrastructureFailed,
			exitCode: 1,
			reason:   "harness: model stream failed",
		})
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		var stored courierv1alpha1.CoderRun
		if err := reconciler.Client.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status.Phase != courierv1alpha1.PhaseClaimed {
			t.Fatalf("phase = %q, want Claimed for the relaunch", stored.Status.Phase)
		}
		if stored.Status.Restarts != 1 {
			t.Fatalf("restarts = %d, want 1 charged relaunch", stored.Status.Restarts)
		}
		if stored.Status.FinishedAt != nil {
			t.Fatalf("finishedAt = %#v, want none: the run is not terminal", stored.Status.FinishedAt)
		}
		if err := reconciler.Client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: "native-result-coordinator"}, &corev1.Pod{}); err == nil {
			t.Fatal("terminated pod survived the relaunch; the deterministic name is not free")
		}
	})
	t.Run("at the ceiling the run is handed to a human", func(t *testing.T) {
		run, reconciler := nativeResultFixture(t, courierv1alpha1.ModeResolveIssue, nativeResultSpec{
			kind:     courierv1alpha1.NativeResultInfrastructureFailed,
			exitCode: 1,
			reason:   "harness: a completed brief lacks a durable status record",
			restarts: defaultMaxRestarts,
		})
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		var stored courierv1alpha1.CoderRun
		if err := reconciler.Client.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
			t.Fatalf("phase = %q, want NeedsHuman at the crashloop ceiling", stored.Status.Phase)
		}
		if stored.Status.TerminalDiagnostic == nil || stored.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryInfrastructureFailure {
			t.Fatalf("terminal diagnostic = %#v, want the infrastructure-failure category", stored.Status.TerminalDiagnostic)
		}
		if err := reconciler.Client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: "native-result-coordinator"}, &corev1.Pod{}); err != nil {
			t.Fatalf("terminated pod was deleted at the ceiling; it is the ending's only record of the cause: %v", err)
		}
	})
}

// TestStaleNativeResultKeepsLegacyClassification proves the staleness fence
// from the other side: a terminated pod with no trusted result of its own —
// the persisted result fences to an earlier incarnation — classifies from the
// legacy exit-code contract, so a stale successful result can never launder a
// failure ending into a successful one (#127).
func TestStaleNativeResultKeepsLegacyClassification(t *testing.T) {
	run, reconciler := nativeResultFixture(t, courierv1alpha1.ModeResolveIssue, nativeResultSpec{
		exitCode: 1,
		// The persisted result fences to an earlier incarnation only.
		staleResult: true,
	})
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var stored courierv1alpha1.CoderRun
	if err := reconciler.Client.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
		t.Fatal(err)
	}
	// The pod carries no valid handoff and exits 1: the legacy mapping says
	// Failed, and the stale published result must not override it.
	if stored.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed from the legacy mapping", stored.Status.Phase)
	}
}

// nativeResultSpec describes one trusted native ending for the reconcile
// fixtures: the persisted result kind (empty for none), the pod's own exit
// code, and the staleness fence cases.
type nativeResultSpec struct {
	kind     courierv1alpha1.NativeResultKind
	reason   string
	exitCode int32
	restarts int
	// staleResult persists a published result fenced to an earlier
	// incarnation, so the terminated pod has no trusted result of its own.
	staleResult bool
}

// nativeResultFixture builds a Running run and its terminated coordinator pod
// in one fake world, exactly as the forge-unavailable tests do, so the
// classification paths run against the same machinery. The pod is named
// natively (deterministic legacy slot) and carries its full termination
// status before the client is built.
func nativeResultFixture(t *testing.T, mode courierv1alpha1.Mode, spec nativeResultSpec) (*courierv1alpha1.CoderRun, *CoderRunReconciler) {
	t.Helper()
	run := admissionRun("native-result", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid")
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = mode, "fake", "native-result-work"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	run.Status.Restarts = spec.restarts
	now := metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC))
	switch {
	case spec.staleResult:
		run.Status.NativeResult = &courierv1alpha1.NativeResult{
			Kind:              courierv1alpha1.NativeResultPublished,
			Reason:            "published through the broker",
			CoordinatorPodUID: "pod-uid-previous-incarnation",
			At:                now,
		}
	case spec.kind != "":
		run.Status.NativeResult = &courierv1alpha1.NativeResult{
			Kind:              spec.kind,
			Reason:            spec.reason,
			CoordinatorPodUID: "pod-uid-native",
			At:                now,
		}
	}
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod, err := executor.BuildCoordinatorPod(run, lane, executor.DefaultPodConfig())
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = types.UID("pod-uid-native")
	pod.Status.Phase = corev1.PodFailed
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "coordinator",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   spec.exitCode,
			FinishedAt: now,
			Message:    nativeHandoffPayload(spec.kind, spec.exitCode),
		}},
	}}
	c := capacityTestClient(t, lane, run, pod)
	s := &capacitySource{admissionSource: &admissionSource{}}
	r := &CoderRunReconciler{
		Client:    c,
		APIReader: c,
		Sources:   NewSourceRegistry(map[string]source.Adapter{"fake": s}),
		Now:       func() time.Time { return time.Date(2026, 10, 26, 12, 0, 1, 0, time.UTC) },
	}
	return run, r
}

// nativeHandoffPayload renders the exact COURIER_TERMINATION line
// courier-control emits for a native ending: the phase/result pair must
// satisfy validTerminationPhaseResult for the exit code. An empty kind emits
// the pre-#127 line, whose classification stays with the exit code.
func nativeHandoffPayload(kind courierv1alpha1.NativeResultKind, exitCode int32) string {
	line := struct {
		Phase      string `json:"phase"`
		Result     string `json:"result"`
		ExitCode   int32  `json:"exit_code"`
		Reason     string `json:"reason"`
		Outcome    string `json:"outcome,omitempty"`
		ResultKind string `json:"result_kind,omitempty"`
	}{ExitCode: exitCode}
	switch exitCode {
	case 0:
		line.Phase, line.Result, line.Outcome = "Verifying", "success", "changes"
	case 2:
		line.Phase, line.Result = "NeedsHuman", "needs-human"
		switch kind {
		case courierv1alpha1.NativeResultBlockedExternal:
			line.Outcome = "blocked_external"
		case courierv1alpha1.NativeResultCapacityExhausted:
			line.Outcome = "capacity_exhausted"
		case courierv1alpha1.NativeResultNeedsHuman:
			line.Outcome = "needs_decision"
		}
	case 3:
		line.Phase, line.Result, line.Outcome = "NoChangeNeeded", "success", "no_change_needed"
	default:
		line.Phase, line.Result = "Failed", "failure"
	}
	if kind != "" {
		line.ResultKind = string(kind)
	}
	payload, err := json.Marshal(line)
	if err != nil {
		return ""
	}
	return "COURIER_TERMINATION " + string(payload)
}
