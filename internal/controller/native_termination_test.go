package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The native harness's handoff payloads (cmd/courier-control terminationFor)
// must survive coordinatorTermination's vocabulary validation, or the
// operator loses the trusted reason and declared outcome. These payloads are
// the exact phase/result/outcome strings courier-control emits for each exit.
func TestNativeTerminationHandoffAccepted(t *testing.T) {
	tests := []struct {
		name         string
		payload      string
		wantOutcome  string
		wantReason   string
		wantExitCode int32
	}{
		{
			name:         "changes exits 0",
			payload:      `{"phase":"Verifying","result":"success","exit_code":0,"reason":"published through the broker","outcome":"changes"}`,
			wantOutcome:  "changes",
			wantReason:   "published through the broker",
			wantExitCode: 0,
		},
		{
			name:         "no_change_needed exits 3",
			payload:      `{"phase":"NoChangeNeeded","result":"success","exit_code":3,"reason":"already done","outcome":"no_change_needed"}`,
			wantOutcome:  "no_change_needed",
			wantReason:   "already done",
			wantExitCode: 3,
		},
		{
			name:         "blocked_external exits 2",
			payload:      `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":"a decision is required","outcome":"blocked_external"}`,
			wantOutcome:  "blocked_external",
			wantReason:   "a decision is required",
			wantExitCode: 2,
		},
		{
			name:         "needs_decision exits 2",
			payload:      `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":"which base?","outcome":"needs_decision"}`,
			wantOutcome:  "needs_decision",
			wantReason:   "which base?",
			wantExitCode: 2,
		},
		{
			name:         "publication failure after changes exits 1",
			payload:      `{"phase":"Failed","result":"failure","exit_code":1,"reason":"push failed","outcome":"changes"}`,
			wantOutcome:  "changes",
			wantReason:   "push failed",
			wantExitCode: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := terminatedControlPod(tt.payload, tt.wantExitCode)
			code, reason, outcome, _, terminated := coordinatorTermination(pod)
			if !terminated {
				t.Fatal("coordinatorTermination reported no terminated container")
			}
			if code != tt.wantExitCode {
				t.Fatalf("exit code = %d, want %d", code, tt.wantExitCode)
			}
			if reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q — the handoff was rejected", reason, tt.wantReason)
			}
			if outcome != tt.wantOutcome {
				t.Fatalf("outcome = %q, want %q — the handoff was rejected", outcome, tt.wantOutcome)
			}
		})
	}
}

// A payload outside the accepted vocabulary keeps failing: the operator must
// not silently accept drifted handoffs.
func TestNativeTerminationHandoffRejectsDriftedVocabulary(t *testing.T) {
	pod := terminatedControlPod(`{"phase":"AwaitingReview","result":"undeclared","exit_code":3,"reason":"x","outcome":"no_change_needed"}`, 3)
	code, reason, outcome, _, terminated := coordinatorTermination(pod)
	if !terminated || code != 3 {
		t.Fatalf("terminated = %v code = %d", terminated, code)
	}
	if reason != "" || outcome != "" {
		t.Fatalf("drifted handoff was accepted: reason %q outcome %q", reason, outcome)
	}
}

func terminatedControlPod(message string, exitCode int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "run-control", Namespace: "runs"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "coordinator",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: exitCode,
					// The kubelet surfaces the termination-message file
					// contents verbatim, including the handoff prefix.
					Message: "COURIER_TERMINATION " + message,
				}},
			}},
		},
	}
}
