package controller

import (
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	courierlog "github.com/misospace/courier/internal/log"
)

const terminalDiagnosticReasonBytes = 1024
const terminalDiagnosticReasonSuffix = "... [truncated]"
const wedgedReapDiagnosticReason = "coordinator was reaped as wedged after no successful activity"

var terminalDiagnosticRedactor = courierlog.NewRedactor()

func knownTerminalDiagnosticCategory(category courierv1alpha1.TerminalDiagnosticCategory) bool {
	switch category {
	case courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash,
		courierv1alpha1.TerminalDiagnosticCategoryModelTransportError,
		courierv1alpha1.TerminalDiagnosticCategoryContinuationExhaustion,
		courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection,
		courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure,
		courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision,
		courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal,
		courierv1alpha1.TerminalDiagnosticCategoryUnknown:
		return true
	default:
		return false
	}
}

func classifyTerminalDiagnostic(handoffCategory string, exitCode *int32, outcome string, kubernetesFailure bool) courierv1alpha1.TerminalDiagnosticCategory {
	category := courierv1alpha1.TerminalDiagnosticCategory(strings.TrimSpace(handoffCategory))
	if category != "" && category != courierv1alpha1.TerminalDiagnosticCategoryUnknown && knownTerminalDiagnosticCategory(category) {
		return category
	}
	if kubernetesFailure {
		return courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure
	}
	switch strings.TrimSpace(outcome) {
	case "needs_decision":
		return courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision
	case "blocked_external":
		return courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal
	}
	if exitCode != nil && *exitCode != 0 && *exitCode != exitDeclaredNoChange && *exitCode != exitNeedsHuman {
		// A non-success exit that is neither a declared no-change nor a
		// needs-human exit produced no cause: the executor died before it
		// could report one. Exit 2 (needs-human) with no category is
		// ambiguous rather than a crash, so it falls through to unknown.
		return courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash
	}
	return courierv1alpha1.TerminalDiagnosticCategoryUnknown
}

func boundedDiagnosticReason(reason string) string {
	reason = terminalDiagnosticRedactor.Redact(strings.TrimSpace(reason))
	if len(reason) <= terminalDiagnosticReasonBytes {
		return reason
	}
	limit := terminalDiagnosticReasonBytes - len(terminalDiagnosticReasonSuffix)
	for limit > 0 && !utf8.ValidString(reason[:limit]) {
		limit--
	}
	return reason[:limit] + terminalDiagnosticReasonSuffix
}

func boundedDiagnosticOutcome(outcome string) string {
	runes := []rune(strings.TrimSpace(outcome))
	if len(runes) > 64 {
		runes = runes[:64]
	}
	return string(runes)
}

// categoryDiagnosticReason returns a short, allowlisted, operator-generated
// explanation for a category. The string is the operator's own label, not
// any text the executor produced: the coordinator's needs-decision
// question and blocked-external missing description, the verifier's
// fail-reason, error text from the executor bootstrap, and any other
// executor-produced freeform text are deliberately not used here. The
// category is the only durable handle on the meaning of an ending; the
// human-facing explanation lives on the source issue/PR comment posted
// by the executor's terminal handler.
func categoryDiagnosticReason(category courierv1alpha1.TerminalDiagnosticCategory) string {
	switch category {
	case courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision:
		return "coordinator declared it needed a decision"
	case courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal:
		return "coordinator declared it was blocked on an external prerequisite"
	case courierv1alpha1.TerminalDiagnosticCategoryVerifierRejection:
		return "coordinator exit was rejected by the verified world"
	case courierv1alpha1.TerminalDiagnosticCategoryModelTransportError:
		return "coordinator model transport exited with a non-zero status"
	case courierv1alpha1.TerminalDiagnosticCategoryContinuationExhaustion:
		return "coordinator exhausted its continuation budget"
	case courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure:
		return "coordinator pod failed at the Kubernetes or container layer"
	case courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash:
		return "coordinator exited without a valid handoff"
	case courierv1alpha1.TerminalDiagnosticCategoryUnknown:
		return "coordinator ended with no captured cause"
	}
	return ""
}

// kubeletContainerReason returns the coordinator container's kubelet
// Terminated.Reason, or "" when the container is not recorded as
// terminated. The value is one of a small allowlisted set the runtime
// itself writes (for example OOMKilled, Error, Completed,
// ContainerStatusUnknown) and is bounded by Kubernetes to 128 bytes;
// it is safe to fold into the operator-derived diagnostic reason.
func kubeletContainerReason(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != coordinatorContainerName || status.State.Terminated == nil {
			continue
		}
		return strings.TrimSpace(status.State.Terminated.Reason)
	}
	return ""
}

// operatorDiagnosticReason builds the reason persisted on a diagnostic
// entirely from operator-derived data: the category label, the kubelet's
// allowlisted container and pod state reasons, and a small set of
// operator-controlled override strings (wedge reap, pod loss). The
// executor's freeform termination text is never an input. The result is
// passed through boundedDiagnosticReason for the length cap and a
// defensive pattern-redaction pass.
func operatorDiagnosticReason(category courierv1alpha1.TerminalDiagnosticCategory, override string, pod *corev1.Pod) string {
	parts := []string{categoryDiagnosticReason(category)}
	if reason := kubeletContainerReason(pod); reason != "" {
		parts = append(parts, "container: "+reason)
	}
	if pod != nil {
		if reason := strings.TrimSpace(pod.Status.Reason); reason != "" {
			parts = append(parts, "pod: "+reason)
		}
	}
	if override != "" {
		parts = append(parts, override)
	}
	return strings.Join(parts, "; ")
}

func buildTerminalDiagnostic(now time.Time, exitCode *int32, reasonOverride, outcome, handoffCategory string, kubernetesFailure bool, pod *corev1.Pod) *courierv1alpha1.TerminalDiagnostic {
	category := classifyTerminalDiagnostic(handoffCategory, exitCode, outcome, kubernetesFailure)
	diagnostic := &courierv1alpha1.TerminalDiagnostic{
		Category:   category,
		ExitCode:   exitCode,
		Reason:     boundedDiagnosticReason(operatorDiagnosticReason(category, reasonOverride, pod)),
		Outcome:    boundedDiagnosticOutcome(outcome),
		ObservedAt: &metav1.Time{Time: now.UTC()},
	}
	if pod != nil {
		diagnostic.Container = coordinatorContainerDiagnostic(pod)
	}
	return diagnostic
}

func coordinatorContainerDiagnostic(pod *corev1.Pod) *courierv1alpha1.TerminalContainerDiagnostic {
	if pod == nil {
		return nil
	}
	diagnostic := &courierv1alpha1.TerminalContainerDiagnostic{
		PodName:   pod.Name,
		PodUID:    string(pod.UID),
		Namespace: pod.Namespace,
		NodeName:  pod.Spec.NodeName,
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != coordinatorContainerName {
			continue
		}
		diagnostic.ContainerName = status.Name
		diagnostic.ContainerID = status.ContainerID
		diagnostic.Image = status.Image
		diagnostic.RestartCount = status.RestartCount
		if status.State.Terminated != nil {
			diagnostic.TerminationReason = status.State.Terminated.Reason
		}
		break
	}
	return diagnostic
}

func kubernetesFailureEvidence(pod *corev1.Pod, handoffValid bool) bool {
	if pod == nil {
		return true
	}
	if pod.Status.Reason == "Evicted" || pod.Status.Reason == "NodeAffinity" {
		return true
	}
	var terminated *corev1.ContainerStateTerminated
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == coordinatorContainerName && status.State.Terminated != nil {
			terminated = status.State.Terminated
			break
		}
	}
	if pod.Status.Phase == corev1.PodFailed && terminated == nil && !handoffValid {
		return true
	}
	return terminated != nil && !handoffValid && (terminated.Reason == "OOMKilled" || terminated.Reason == "ContainerStatusUnknown")
}
