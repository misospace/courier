package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestClassifyTerminalDiagnostic(t *testing.T) {
	nonzero, zero, noChange := int32(1), int32(0), int32(exitDeclaredNoChange)
	two := int32(exitNeedsHuman)
	tests := []struct {
		name       string
		handoff    string
		exit       *int32
		outcome    string
		kubernetes bool
		want       courierv1alpha1.TerminalDiagnosticCategory
	}{
		{"executor crash", "executor-crash", nil, "", false, courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash},
		{"transport", "model-transport-error", nil, "", false, courierv1alpha1.TerminalDiagnosticCategoryModelTransportError},
		{"continuation", "continuation-exhaustion", nil, "", false, courierv1alpha1.TerminalDiagnosticCategoryContinuationExhaustion},
		{"verifier", "verifier-rejection", nil, "", false, courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection},
		{"kubernetes", "", nil, "", true, courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure},
		{"decision", "", nil, "needs_decision", false, courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision},
		{"external", "", nil, "blocked_external", false, courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal},
		{"unknown", "", nil, "", false, courierv1alpha1.TerminalDiagnosticCategoryUnknown},
		{"concrete handoff wins", "verifier-rejection", &nonzero, "needs_decision", true, courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection},
		{"kubernetes wins", "", &nonzero, "needs_decision", true, courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure},
		{"unknown handoff falls through", "unknown", &nonzero, "", false, courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash},
		{"invalid handoff falls through to k8s", "not-a-category", nil, "", true, courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure},
		{"zero unknown", "", &zero, "", false, courierv1alpha1.TerminalDiagnosticCategoryUnknown},
		{"no change unknown", "", &noChange, "", false, courierv1alpha1.TerminalDiagnosticCategoryUnknown},
		{"needs-human no cause is unknown", "", &two, "", false, courierv1alpha1.TerminalDiagnosticCategoryUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyTerminalDiagnostic(tt.handoff, tt.exit, tt.outcome, tt.kubernetes); got != tt.want {
				t.Fatalf("category = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBoundedDiagnosticReason(t *testing.T) {
	for _, secret := range []string{
		"Authorization: Bearer abcdefghijklmnopqrstuvwxyz",
		"https://user:secret-password@example.com/path",
		"ghp_abcdefghijklmnopqrstuvwxyz0123456789",
	} {
		got := boundedDiagnosticReason("failure " + secret)
		if strings.Contains(got, "abcdefghijklmnopqrstuvwxyz") || strings.Contains(got, "secret-password") || strings.Contains(got, "ghp_") {
			t.Errorf("reason retained secret-shaped value: %q", got)
		}
	}
	got := boundedDiagnosticReason(strings.Repeat("a", 1022) + "émore")
	if len(got) > terminalDiagnosticReasonBytes || !strings.HasSuffix(got, terminalDiagnosticReasonSuffix) || !utf8.ValidString(got) {
		t.Fatalf("bounded reason len=%d valid=%t suffix=%t", len(got), utf8.ValidString(got), strings.HasSuffix(got, terminalDiagnosticReasonSuffix))
	}
}

func TestBuildTerminalDiagnostic(t *testing.T) {
	exit := int32(1)
	now := time.Date(2026, 10, 12, 1, 2, 3, 0, time.FixedZone("offset", 3600))
	missing := buildTerminalDiagnostic(now, &exit, "", "", "", true, nil)
	if missing.Container != nil || missing.ExitCode != &exit && (missing.ExitCode == nil || *missing.ExitCode != exit) {
		t.Fatalf("missing-pod diagnostic = %#v", missing)
	}
	if !missing.ObservedAt.Time.Equal(now.UTC()) {
		t.Fatalf("ObservedAt = %v, want %v", missing.ObservedAt, now.UTC())
	}
	if missing.Reason != categoryDiagnosticReason(courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure) {
		t.Fatalf("missing-pod reason = %q, want the operator category label", missing.Reason)
	}
	pod := diagnosticPod()
	pod.Status.ContainerStatuses[0].State.Terminated.Reason = "OOMKilled"
	got := buildTerminalDiagnostic(now, &exit, "", "", "", kubernetesFailureEvidence(pod, false), pod)
	if got.Category != courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure {
		t.Fatalf("OOM category = %q", got.Category)
	}
	if got.Container == nil || got.Container.PodName != pod.Name || got.Container.ContainerID != "containerd://abc" || got.Container.TerminationReason != "OOMKilled" {
		t.Fatalf("container = %#v", got.Container)
	}
	if got.ExitCode == nil || *got.ExitCode != exit {
		t.Fatalf("exit code = %v, want %d", got.ExitCode, exit)
	}
	if !strings.Contains(got.Reason, kubeletContainerReason(pod)) {
		t.Fatalf("OOM reason = %q, want it to include the kubelet reason %q", got.Reason, kubeletContainerReason(pod))
	}
}

func TestCoordinatorContainerDiagnostic(t *testing.T) {
	pod := diagnosticPod()
	pod.Spec.NodeName = "node-a"
	got := coordinatorContainerDiagnostic(pod)
	if got.PodName != pod.Name || got.PodUID != string(pod.UID) || got.Namespace != pod.Namespace || got.NodeName != "node-a" || got.ContainerName != coordinatorContainerName || got.ContainerID != "containerd://abc" || got.Image != "courier:test" || got.RestartCount != 2 {
		t.Fatalf("container diagnostic = %#v", got)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ContainerID: "ignored"}}
	got = coordinatorContainerDiagnostic(pod)
	if got.ContainerName != "" || got.ContainerID != "" {
		t.Fatalf("non-coordinator status copied: %#v", got)
	}
}

func TestReconcilePersistsTerminalDiagnosticAndPodLossDiagnostic(t *testing.T) {
	t.Run("terminal handoff and idempotence", func(t *testing.T) {
		run := admissionRun("diagnostic-run", "local", courierv1alpha1.PhaseRunning)
		pod := coordinatorPodWithOutcome(run, 1, "", "rejected")
		payload, err := json.Marshal(terminationMessage{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "rejected", Category: "verifier-rejection"})
		if err != nil {
			t.Fatal(err)
		}
		pod.Status.ContainerStatuses[0].State.Terminated.Message = handoffPrefix + string(payload)
		pod.Status.ContainerStatuses[0].ContainerID = "containerd://diagnostic"
		pod.Status.ContainerStatuses[0].Image = "courier:test"
		pod.Status.ContainerStatuses[0].RestartCount = 1
		client := phaseClient(t, run, pod)
		item := &admissionSource{}
		reconciler := diagnosticReconciler(client, item)
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
			t.Fatalf("first reconcile: %v", err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey(run.Name), &updated); err != nil {
			t.Fatal(err)
		}
		diagnostic := updated.Status.TerminalDiagnostic
		if diagnostic == nil || diagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection || diagnostic.Container == nil || diagnostic.Container.PodName != pod.Name || diagnostic.Container.ContainerID != "containerd://diagnostic" {
			t.Fatalf("terminal diagnostic = %#v", diagnostic)
		}
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
			t.Fatalf("second reconcile: %v", err)
		}
		if err := client.Get(context.Background(), admissionKey(run.Name), &updated); err != nil {
			t.Fatal(err)
		}
		if updated.Status.TerminalDiagnostic == nil || updated.Status.TerminalDiagnostic.Category != diagnostic.Category || updated.Status.TerminalDiagnostic.Container.PodName != pod.Name {
			t.Fatalf("terminal diagnostic after second reconcile = %#v", updated.Status.TerminalDiagnostic)
		}
	})

	t.Run("missing pod at ceiling", func(t *testing.T) {
		run := admissionRun("lost-run", "local", courierv1alpha1.PhaseRunning)
		run.Status.Restarts = defaultMaxRestarts
		client := phaseClient(t, run)
		item := &admissionSource{}
		reconciler := diagnosticReconciler(client, item)
		if _, err := reconciler.observeMissingCoordinator(context.Background(), run); err != nil {
			t.Fatalf("observe missing coordinator: %v", err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey(run.Name), &updated); err != nil {
			t.Fatal(err)
		}
		if got := updated.Status.TerminalDiagnostic; got == nil || got.Category != courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure || got.Container != nil {
			t.Fatalf("pod-loss diagnostic = %#v", got)
		}
	})
}

func diagnosticReconciler(kube client.Client, item *admissionSource) *CoderRunReconciler {
	return &CoderRunReconciler{Client: kube, Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}), StatusWriter: status.KubePatchWriter{Client: kube}}
}

func TestReconcilePersistsEveryDiagnosticCategory(t *testing.T) {
	tests := []struct {
		name       string
		exitCode   int32
		phase      string
		result     string
		outcome    string
		category   string
		termReason string
		handoff    bool
		want       courierv1alpha1.TerminalDiagnosticCategory
	}{
		{"executor-crash", 1, "Failed", "failure", "", "executor-crash", "", true, courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash},
		{"model-transport-error", 1, "Failed", "failure", "", "model-transport-error", "", true, courierv1alpha1.TerminalDiagnosticCategoryModelTransportError},
		{"continuation-exhaustion", 2, "NeedsHuman", "needs-human", "", "continuation-exhaustion", "", true, courierv1alpha1.TerminalDiagnosticCategoryContinuationExhaustion},
		{"verifier-rejection", 1, "Failed", "failure", "", "verifier-rejection", "", true, courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection},
		{"kubernetes-failure", 1, "", "", "", "", "OOMKilled", false, courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure},
		{"needs-decision", 2, "NeedsHuman", "needs-human", "needs_decision", "needs-decision", "", true, courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision},
		{"blocked-external", 2, "NeedsHuman", "needs-human", "blocked_external", "blocked-external", "", true, courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal},
		{"unknown", 2, "NeedsHuman", "needs-human", "", "", "", true, courierv1alpha1.TerminalDiagnosticCategoryUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := admissionRun("diag-"+strings.ReplaceAll(tt.name, "-", ""), "local", courierv1alpha1.PhaseRunning)
			pod := coordinatorPod(run, tt.exitCode)
			pod.Status.ContainerStatuses[0].State.Terminated.Reason = tt.termReason
			if tt.handoff {
				payload, err := json.Marshal(terminationMessage{Phase: tt.phase, Result: tt.result, ExitCode: tt.exitCode, Outcome: tt.outcome, Reason: "cause", Category: tt.category})
				if err != nil {
					t.Fatal(err)
				}
				pod.Status.ContainerStatuses[0].State.Terminated.Message = handoffPrefix + string(payload)
			}
			kube := phaseClient(t, run, pod)
			reconciler := diagnosticReconciler(kube, &admissionSource{})
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			var updated courierv1alpha1.CoderRun
			if err := kube.Get(context.Background(), admissionKey(run.Name), &updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.TerminalDiagnostic == nil || updated.Status.TerminalDiagnostic.Category != tt.want {
				t.Fatalf("category = %#v, want %q", updated.Status.TerminalDiagnostic, tt.want)
			}
		})
	}
}

func TestRelaunchCeilingPersistsWedgedDiagnostic(t *testing.T) {
	run := admissionRun("wedge-run", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = defaultMaxRestarts
	kube := phaseClient(t, run)
	reconciler := diagnosticReconciler(kube, &admissionSource{})
	diagnostic := buildTerminalDiagnostic(reconciler.clock(), nil, wedgedReapDiagnosticReason, "", "", false, nil)
	if _, err := reconciler.relaunchAfterInfraLoss(context.Background(), run, "", "", diagnostic); err != nil {
		t.Fatalf("relaunch at ceiling: %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), admissionKey(run.Name), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman || updated.Status.TerminalDiagnostic == nil || updated.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryUnknown {
		t.Fatalf("wedged diagnostic = %#v phase=%q", updated.Status.TerminalDiagnostic, updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.TerminalDiagnostic.Reason, wedgedReapDiagnosticReason) {
		t.Fatalf("wedged reason = %q, want it to carry the wedge override %q", updated.Status.TerminalDiagnostic.Reason, wedgedReapDiagnosticReason)
	}
}

type failingStatusWriter struct{}

func (failingStatusWriter) PatchStatus(context.Context, types.NamespacedName, []byte) error {
	return errors.New("status backend down")
}

func TestDiagnosticCaptureFailureDoesNotTerminalize(t *testing.T) {
	run := admissionRun("fail-write", "local", courierv1alpha1.PhaseRunning)
	pod := coordinatorPod(run, 1)
	payload, err := json.Marshal(terminationMessage{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: "cause", Category: "verifier-rejection"})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses[0].State.Terminated.Message = handoffPrefix + string(payload)
	kube := phaseClient(t, run, pod)
	reconciler := &CoderRunReconciler{Client: kube, Sources: NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}), StatusWriter: failingStatusWriter{}}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err == nil {
		t.Fatal("expected the status write failure to surface")
	}
	var updated courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), admissionKey(run.Name), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase == courierv1alpha1.PhaseFailed || updated.Status.TerminalDiagnostic != nil {
		t.Fatalf("run changed despite status write failure: %#v", updated.Status)
	}
}

// TestDiagnosticReasonExcludesExecutorFreeformText proves the diagnostic's
// reason is operator-derived: the executor's freeform termination text
// (which can carry a model's needs-decision question, blocked-external
// missing description, verifier fail-reason, or arbitrary Go error
// strings) does not land on the durable status. AC1/AC3/AC6 of #272.
func TestDiagnosticReasonExcludesExecutorFreeformText(t *testing.T) {
	cases := []struct {
		name          string
		exitCode      int32
		phase         string
		result        string
		outcome       string
		category      string
		reason        string
		termReason    string
		handoff       bool
		wantCat       courierv1alpha1.TerminalDiagnosticCategory
		denyTokens    []string
		denySentinels []string
	}{
		{
			name:          "needs-decision question",
			exitCode:      2,
			phase:         "NeedsHuman",
			result:        "needs-human",
			outcome:       "needs_decision",
			category:      "needs-decision",
			reason:        "coordinator needs a decision: PRIVATE_PROMPT_SENTINEL_DO_NOT_PERSIST",
			handoff:       true,
			wantCat:       courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision,
			denySentinels: []string{"PRIVATE_PROMPT_SENTINEL_DO_NOT_PERSIST"},
		},
		{
			name:          "blocked-external missing",
			exitCode:      2,
			phase:         "NeedsHuman",
			result:        "needs-human",
			outcome:       "blocked_external",
			category:      "blocked-external",
			reason:        "coordinator is blocked on an external prerequisite: ANOTHER_PROMPT_SENTINEL",
			handoff:       true,
			wantCat:       courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal,
			denySentinels: []string{"ANOTHER_PROMPT_SENTINEL"},
		},
		{
			name:          "verifier fail-reason",
			exitCode:      1,
			phase:         "Failed",
			result:        "failure",
			category:      "verifier-rejection",
			reason:        "opencode declared changes but the run branch has no new commits; PR_BODY_SENTINEL_VALUE",
			handoff:       true,
			wantCat:       courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection,
			denySentinels: []string{"PR_BODY_SENTINEL_VALUE"},
		},
		{
			name:       "arbitrary unregistered token",
			exitCode:   1,
			phase:      "Failed",
			result:     "failure",
			category:   "executor-crash",
			reason:     "trust workspace: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			handoff:    true,
			wantCat:    courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash,
			denyTokens: []string{"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			run := admissionRun("deny-"+strings.ReplaceAll(tt.name, " ", ""), "local", courierv1alpha1.PhaseRunning)
			pod := coordinatorPod(run, tt.exitCode)
			pod.Status.ContainerStatuses[0].State.Terminated.Reason = tt.termReason
			if tt.handoff {
				payload, err := json.Marshal(terminationMessage{Phase: tt.phase, Result: tt.result, ExitCode: tt.exitCode, Outcome: tt.outcome, Reason: tt.reason, Category: tt.category})
				if err != nil {
					t.Fatal(err)
				}
				pod.Status.ContainerStatuses[0].State.Terminated.Message = handoffPrefix + string(payload)
			}
			kube := phaseClient(t, run, pod)
			reconciler := diagnosticReconciler(kube, &admissionSource{})
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			var updated courierv1alpha1.CoderRun
			if err := kube.Get(context.Background(), admissionKey(run.Name), &updated); err != nil {
				t.Fatal(err)
			}
			diag := updated.Status.TerminalDiagnostic
			if diag == nil {
				t.Fatal("diagnostic was not persisted")
			}
			if diag.Category != tt.wantCat {
				t.Errorf("category = %q, want %q", diag.Category, tt.wantCat)
			}
			for _, sentinel := range tt.denySentinels {
				if strings.Contains(diag.Reason, sentinel) {
					t.Errorf("reason retained executor freeform text %q: %q", sentinel, diag.Reason)
				}
			}
			for _, token := range tt.denyTokens {
				if strings.Contains(diag.Reason, token) {
					t.Errorf("reason retained literal %q: %q", token, diag.Reason)
				}
			}
		})
	}
}

// TestDiagnosticSurvivesPodDeletionAndReconcilerRestart proves the durable
// diagnostic set during a terminal transition persists across pod deletion
// and a fresh controller-runtime reconciler (simulated controller restart).
// The set-once gate must hold: a later reconcile on a run whose diagnostic
// was already persisted in a prior incarnation leaves every field of the
// diagnostic unchanged. AC1/AC4/AC6 of #272.
func TestDiagnosticSurvivesPodDeletionAndReconcilerRestart(t *testing.T) {
	run := admissionRun("survive-run", "local", courierv1alpha1.PhaseRunning)
	const executorReason = "PR_BODY_SENTINEL_LIFECYCLE_VALUE"
	pod := coordinatorPodWithOutcome(run, 1, "", executorReason)
	payload, err := json.Marshal(terminationMessage{Phase: "Failed", Result: "failure", ExitCode: 1, Reason: executorReason, Category: "verifier-rejection"})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses[0].State.Terminated.Message = handoffPrefix + string(payload)
	pod.Status.ContainerStatuses[0].State.Terminated.Reason = "Error"
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://survive"
	pod.Status.ContainerStatuses[0].Image = "courier:test"
	pod.Status.ContainerStatuses[0].RestartCount = 4
	kube := phaseClient(t, run, pod)
	firstReconciler := diagnosticReconciler(kube, &admissionSource{})
	if _, err := firstReconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	var afterFirst courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), admissionKey(run.Name), &afterFirst); err != nil {
		t.Fatal(err)
	}
	captured := afterFirst.Status.TerminalDiagnostic
	if captured == nil {
		t.Fatal("first reconcile did not capture diagnostic")
	}
	if captured.Container == nil || captured.Container.PodName != pod.Name || captured.Container.ContainerID != "containerd://survive" || captured.Container.RestartCount != 4 {
		t.Fatalf("container identity = %#v", captured.Container)
	}
	if captured.Category != courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection {
		t.Fatalf("category = %q, want verifier-rejection", captured.Category)
	}

	// Simulate pod cleanup: the coordinator pod is gone before the next
	// reconcile, the same shape as kubelet-initiated GC. A second
	// reconciler instance is built to model controller restart.
	if err := kube.Delete(context.Background(), pod); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	freshReconciler := diagnosticReconciler(kube, &admissionSource{})
	if _, err := freshReconciler.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	var afterRestart courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), admissionKey(run.Name), &afterRestart); err != nil {
		t.Fatal(err)
	}
	survived := afterRestart.Status.TerminalDiagnostic
	if survived == nil {
		t.Fatal("diagnostic lost after pod deletion and reconciler restart")
	}
	if survived.Category != captured.Category {
		t.Errorf("category changed: %q -> %q", captured.Category, survived.Category)
	}
	if captured.ExitCode == nil || survived.ExitCode == nil || *survived.ExitCode != *captured.ExitCode {
		t.Errorf("exit code changed: %v -> %v", captured.ExitCode, survived.ExitCode)
	}
	if survived.Container == nil || survived.Container.ContainerID != captured.Container.ContainerID || survived.Container.PodName != captured.Container.PodName || survived.Container.Image != captured.Container.Image || survived.Container.RestartCount != captured.Container.RestartCount || survived.Container.PodUID != captured.Container.PodUID {
		t.Errorf("container identity changed: %#v -> %#v", captured.Container, survived.Container)
	}
	if captured.ObservedAt == nil || survived.ObservedAt == nil || !survived.ObservedAt.Time.Equal(captured.ObservedAt.Time) {
		t.Errorf("observedAt changed: %v -> %v", captured.ObservedAt, survived.ObservedAt)
	}
	if !strings.Contains(survived.Reason, kubeletContainerReason(pod)) {
		t.Errorf("reason dropped kubelet reason: %q", survived.Reason)
	}
	if strings.Contains(survived.Reason, executorReason) {
		t.Errorf("reason retained executor freeform text %q: %q", executorReason, survived.Reason)
	}
}

// TestPodDeletedBeforeCaptureSurvivesRestart proves the durable diagnostic
// capture path that the disappearance backstop and the relaunch-ceiling
// path share: a coordinator pod that vanishes before any reconcile can
// capture a diagnostic still produces a kubernetes-failure diagnostic with
// no fabricated container, and the diagnostic survives a fresh reconciler.
// AC1/AC4/AC6 of #272.
func TestPodDeletedBeforeCaptureSurvivesRestart(t *testing.T) {
	run := admissionRun("predelete", "local", courierv1alpha1.PhaseRunning)
	run.Status.Restarts = defaultMaxRestarts
	kube := phaseClient(t, run)
	first := diagnosticReconciler(kube, &admissionSource{})
	if _, err := first.observeMissingCoordinator(context.Background(), run); err != nil {
		t.Fatalf("observe missing: %v", err)
	}
	var afterFirst courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), admissionKey(run.Name), &afterFirst); err != nil {
		t.Fatal(err)
	}
	captured := afterFirst.Status.TerminalDiagnostic
	if captured == nil || captured.Category != courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure || captured.Container != nil {
		t.Fatalf("missing-pod capture = %#v", captured)
	}

	fresh := diagnosticReconciler(kube, &admissionSource{})
	if _, err := fresh.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	var afterRestart courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), admissionKey(run.Name), &afterRestart); err != nil {
		t.Fatal(err)
	}
	survived := afterRestart.Status.TerminalDiagnostic
	if survived == nil {
		t.Fatal("diagnostic lost after restart")
	}
	if survived.Category != captured.Category {
		t.Errorf("category changed: %q -> %q", captured.Category, survived.Category)
	}
	if survived.Container != nil {
		t.Errorf("container block fabricated from missing pod: %#v", survived.Container)
	}
	if !strings.Contains(survived.Reason, podLostReason) {
		t.Errorf("reason = %q, want it to carry the pod-loss override %q", survived.Reason, podLostReason)
	}
}

func diagnosticPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "coordinator-pod", Namespace: "default", UID: types.UID("pod-uid")},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: coordinatorContainerName, Image: "courier:test"}, {Name: "worker", Image: "worker:test"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: coordinatorContainerName, ContainerID: "containerd://abc", Image: "courier:test", RestartCount: 2, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error"}}},
			{Name: "worker", ContainerID: "containerd://worker"},
		}},
	}
}
