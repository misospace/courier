package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
)

// TestForgeUnavailableTerminationPersistsWorkspaceSafetyAndSettles is the
// #455 cross-component regression: a legacy executor that ends a Verifying
// observation on a transient forge read (here the rate-limit 403 ending)
// terminates exit 2, outcome blocked_external, category forge-unavailable,
// with a git-verified workspace snapshot. The operator must open the same
// trusted workspace-safety acquisition gate capacity exhaustion uses, persist
// the isolated observer's pushed evidence, terminalize as BlockedExternal with
// the trusted forge-unavailable diagnostic, and remain eligible for the
// fenced capacity-recovery settlement that releases the work.
func TestForgeUnavailableTerminationPersistsWorkspaceSafetyAndSettles(t *testing.T) {
	run := admissionRun("forge-blocked", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid")
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeResolveIssue, "fake", "forge-work"
	run.Status.Branch = "branch"
	lane := admissionLaneWithCoordinator("pool-a", 1)
	lane.Spec.CapacityGroup = "shared-pool"
	pod := forgeUnavailableCoordinatorPod(t, run, lane, forgeUnavailableHandoffMessage("pushed"), "pushed")
	c := capacityTestClient(t, lane, run, pod)
	s := &capacitySource{admissionSource: &admissionSource{}}
	r := &CoderRunReconciler{Client: c, APIReader: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), Now: func() time.Time { return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) }}
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
	if stored.Status.TerminalDiagnostic == nil || stored.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable {
		t.Fatalf("terminal diagnostic = %#v, want trusted forge-unavailable", stored.Status.TerminalDiagnostic)
	}
	if stored.Status.TerminalDiagnostic.Reason != forgeUnavailableDiagnosticReason {
		t.Fatalf("terminal diagnostic reason = %q, want %q", stored.Status.TerminalDiagnostic.Reason, forgeUnavailableDiagnosticReason)
	}
	if stored.Status.WorkspaceSafety == nil || stored.Status.WorkspaceSafety.State != "pushed" ||
		stored.Status.WorkspaceSafety.Head == "" || stored.Status.WorkspaceSafety.RemoteHead != stored.Status.WorkspaceSafety.Head {
		t.Fatalf("workspace safety = %#v, want persisted pushed evidence", stored.Status.WorkspaceSafety)
	}
	cond := apimeta.FindStatusCondition(stored.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != capacityRecoverySettled {
		t.Fatalf("capacity recovery condition = %#v, want settled", cond)
	}
	if len(s.settled) != 1 || s.settled[0].FenceToken != "source-fence" {
		t.Fatalf("settlement handoffs = %#v, want one fenced settlement", s.settled)
	}
	if len(s.reports) == 0 || s.reports[len(s.reports)-1].Result != source.ResultCapacityExhausted {
		t.Fatalf("source reports = %#v, want the recoverable capacity-handoff report", s.reports)
	}
}

// TestForgeUnavailableTerminationUnsafeWorkspaceRefusesSettlement proves the
// gate stays fail-closed on the new ending: a workspace the isolated observer
// cannot prove safe is persisted as evidence but never releases ownership —
// recovery stops at UnsafeWork, exactly as it does for a capacity exhaustion
// with the same evidence.
func TestForgeUnavailableTerminationUnsafeWorkspaceRefusesSettlement(t *testing.T) {
	run := admissionRun("forge-unsafe", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid")
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeResolveIssue, "fake", "forge-unsafe-work"
	run.Status.Branch = "branch"
	lane := admissionLaneWithCoordinator("pool-a", 1)
	lane.Spec.CapacityGroup = "shared-pool"
	pod := forgeUnavailableCoordinatorPod(t, run, lane, forgeUnavailableHandoffMessage("unknown"), "unknown")
	c := capacityTestClient(t, lane, run, pod)
	s := &capacitySource{admissionSource: &admissionSource{}}
	r := &CoderRunReconciler{Client: c, APIReader: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), Now: func() time.Time { return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) }}
	if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatal(err)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseBlockedExternal {
		t.Fatalf("phase = %q, want blocked-external", stored.Status.Phase)
	}
	if stored.Status.TerminalDiagnostic == nil || stored.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable {
		t.Fatalf("terminal diagnostic = %#v, want trusted forge-unavailable", stored.Status.TerminalDiagnostic)
	}
	if stored.Status.TerminalDiagnostic.Reason != forgeUnavailableDiagnosticReason {
		t.Fatalf("terminal diagnostic reason = %q, want %q", stored.Status.TerminalDiagnostic.Reason, forgeUnavailableDiagnosticReason)
	}
	if stored.Status.WorkspaceSafety == nil || stored.Status.WorkspaceSafety.State != "unknown" {
		t.Fatalf("workspace safety = %#v, want persisted unknown evidence", stored.Status.WorkspaceSafety)
	}
	cond := apimeta.FindStatusCondition(stored.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
	if cond == nil || cond.Reason != capacityRecoveryUnsafeWork {
		t.Fatalf("capacity recovery condition = %#v, want unsafe-work refusal", cond)
	}
	if len(s.settled) != 0 {
		t.Fatalf("settlement handoffs = %#v, want ownership held", s.settled)
	}
}

// TestTaskShapedBlockedExternalOpensNoEvidenceGate proves the pairing is
// exact: a plain external-prerequisite block (blocked_external with no
// forge-unavailable handoff category) never opens the workspace-safety
// acquisition gate, never carries the isolated-observer observation, and
// never enters capacity recovery — its ending stays the ordinary recoverable
// dependency block the source already reports.
func TestTaskShapedBlockedExternalOpensNoEvidenceGate(t *testing.T) {
	run := admissionRun("task-blocked", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid")
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeResolveIssue, "fake", "task-work"
	run.Status.Branch = "branch"
	lane := admissionLaneWithCoordinator("pool-a", 1)
	lane.Spec.CapacityGroup = "shared-pool"
	plain := `COURIER_TERMINATION {"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":"missing prerequisite: secrets/prod","outcome":"blocked_external"}`
	pod := forgeUnavailableCoordinatorPod(t, run, lane, plain, "pushed")
	c := capacityTestClient(t, lane, run, pod)
	s := &capacitySource{admissionSource: &admissionSource{}}
	r := &CoderRunReconciler{Client: c, APIReader: c, Sources: NewSourceRegistry(map[string]source.Adapter{"fake": s}), Now: func() time.Time { return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) }}
	if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatal(err)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey(run.Name), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseBlockedExternal {
		t.Fatalf("phase = %q, want blocked-external", stored.Status.Phase)
	}
	if stored.Status.WorkspaceSafety != nil {
		t.Fatalf("workspace safety = %#v, want none: a task-shaped block opens no evidence gate", stored.Status.WorkspaceSafety)
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, "LegacyWorkspaceObserved"); cond != nil {
		t.Fatalf("legacy observation condition = %#v, want none", cond)
	}
	if stored.Status.TerminalDiagnostic == nil || stored.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal {
		t.Fatalf("terminal diagnostic = %#v, want blocked-external category", stored.Status.TerminalDiagnostic)
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery); cond != nil {
		t.Fatalf("capacity recovery condition = %#v, want none: no capacity event happened", cond)
	}
	if len(s.settled) != 0 {
		t.Fatalf("settlement handoffs = %#v, want none", s.settled)
	}
	if len(s.reports) == 0 || s.reports[len(s.reports)-1].Result != source.ResultBlockedExternal {
		t.Fatalf("source reports = %#v, want the blocked-external report", s.reports)
	}
}

// forgeUnavailableDiagnosticReason is the operator's own label for the #455
// forge-read ending: the forge read could not complete on a transient
// availability condition, so the world was never observed and the work was
// released for another capacity. It names the forge, never the model
// provider.
const forgeUnavailableDiagnosticReason = "a forge read could not complete on a transient availability condition; the world was never observed; work released for another capacity"

// forgeUnavailableHandoffMessage renders the exact exit-2 handoff the legacy
// executor's forgeUnavailableTermination writes for a transient forge read.
func forgeUnavailableHandoffMessage(state string) string {
	head := strings.Repeat("b", 40)
	message := map[string]any{
		"phase":            "NeedsHuman",
		"result":           "needs-human",
		"exit_code":        2,
		"reason":           "the live pull-request state could not be verified to confirm an open, non-draft pull request with authoritative source-issue linkage to #7: GET https://api.github.com/repos/acme/widgets/pulls/7: HTTP 403 rate limit; the forge was temporarily unavailable (rate limit, server error, or network failure), so the run is parked for bounded retry by the operator",
		"outcome":          "blocked_external",
		"category":         "forge-unavailable",
		"workspace_safety": map[string]any{"state": state, "branch": "branch", "head": head, "remoteHead": head},
	}
	payload, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return "COURIER_TERMINATION " + string(payload)
}

// forgeUnavailableCoordinatorPod builds the legacy coordinator pod (with its
// isolated observer sidecar) terminated with the given handoff message while
// the isolated observer proves the given workspace state.
func forgeUnavailableCoordinatorPod(t *testing.T, run *courierv1alpha1.CoderRun, lane *courierv1alpha1.LaneProfile, handoff, observerState string) *corev1.Pod {
	t.Helper()
	pod, err := executor.BuildCoordinatorPod(run, lane, executor.DefaultPodConfig())
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = types.UID("pod-uid")
	baseline := strings.Repeat("a", 40)
	head := strings.Repeat("b", 40)
	now := metav1.NewTime(time.Date(2026, 10, 18, 0, 0, 1, 0, time.UTC))
	proof := executor.ObserverEvidence{Version: 1, RunUID: string(run.UID), PodUID: string(pod.UID), Branch: "branch", Baseline: baseline, WriterContainerID: "containerd://writer", Safety: &executor.WorkspaceSafetyEvidence{State: observerState, Branch: "branch", Head: head}}
	if observerState == "pushed" {
		proof.Safety.RemoteHead = head
	}
	payload, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodFailed
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "coordinator", ContainerID: "containerd://writer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, FinishedAt: now, Message: handoff}}},
		{Name: executor.ObserverContainerName, ContainerID: "containerd://observer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, FinishedAt: now, Message: string(payload)}}},
	}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "workspace-observer-seed", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: baseline}}}}
	return pod
}
