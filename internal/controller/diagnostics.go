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
const terminalContainerReasonBytes = 128

// allowlistedContainerReasons is the small, closed set of kubelet
// `ContainerStateTerminated.Reason` values that may enter the diagnostic.
// The runtime writes these; arbitrary CRD or message content does not, so a
// non-pattern-matching value carried in the kubelet's Terminated.Reason never
// reaches the diagnostic. Standard values from the Kubernetes source
// (k8s.io/kubernetes/pkg/kubelet/container/helpers.go and
// k8s.io/api/core/v1/types.go); kubelets may write "Error" or
// "ContainerStatusUnknown" when state cannot be determined.
var allowlistedContainerReasons = map[string]struct{}{
	"Completed":              {},
	"Error":                  {},
	"OOMKilled":              {},
	"ContainerStatusUnknown": {},
	"Evicted":                {},
	"DeadlineExceeded":       {},
	"Preempt":                {},
}

// allowlistedPodReasons is the small, closed set of `Pod.Status.Reason` values
// that may enter the diagnostic. The runtime writes these values during
// eviction, scheduling failure, node loss, or graceful shutdown. Other
// reasons — including arbitrary freeform text a CRD or status writer might
// include — never reach the diagnostic.
var allowlistedPodReasons = map[string]struct{}{
	"Evicted":                  {},
	"NodeAffinity":             {},
	"NodeLost":                 {},
	"Shutdown":                 {},
	"Unschedulable":            {},
	"UnexpectedAdmissionError": {},
	"Error":                    {},
	"ContainerStatusUnknown":   {},
}

func isAllowlistedContainerReason(reason string) bool {
	if reason == "" {
		return false
	}
	_, ok := allowlistedContainerReasons[reason]
	return ok
}

func isAllowlistedPodReason(reason string) bool {
	if reason == "" {
		return false
	}
	_, ok := allowlistedPodReasons[reason]
	return ok
}

func isInfraContainerReason(reason string) bool {
	switch reason {
	case "OOMKilled", "ContainerStatusUnknown", "Evicted", "DeadlineExceeded", "Preempt":
		return true
	default:
		return false
	}
}

func isInfraPodReason(reason string) bool {
	switch reason {
	case "Evicted", "NodeAffinity", "NodeLost", "Unschedulable", "UnexpectedAdmissionError", "ContainerStatusUnknown":
		return true
	default:
		return false
	}
}

// boundedContainerTerminationReason returns an allowlisted kubelet
// `Terminated.Reason` value, hard-capped at terminalContainerReasonBytes so the
// persisted `Container.TerminationReason` cannot exceed the CRD's
// `+kubebuilder:validation:MaxLength=128` and cause the API server to reject
// the entire status patch (which would block terminalization and source
// reporting). Values outside the allowlist are dropped.
func boundedContainerTerminationReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if !isAllowlistedContainerReason(reason) {
		return ""
	}
	if len(reason) > terminalContainerReasonBytes {
		return reason[:terminalContainerReasonBytes]
	}
	return reason
}

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
		courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
		courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable,
		courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable,
		courierv1alpha1.TerminalDiagnosticCategoryWorkloadFailure,
		courierv1alpha1.TerminalDiagnosticCategoryInfrastructureFailure,
		courierv1alpha1.TerminalDiagnosticCategoryUnknown:
		return true
	default:
		return false
	}
}

// categoryForNativeResultKind maps the trusted native terminal result kind
// onto the durable diagnostic category (#127). The kind is trusted-control
// evidence — the coordinator's explicit classification of its own ending —
// so it outranks every exit-code heuristic but not a validated handoff
// category, which carries finer distinctions on the endings they share.
func categoryForNativeResultKind(kind courierv1alpha1.NativeResultKind) courierv1alpha1.TerminalDiagnosticCategory {
	switch kind {
	case courierv1alpha1.NativeResultNeedsHuman:
		return courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision
	case courierv1alpha1.NativeResultBlockedExternal:
		return courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal
	case courierv1alpha1.NativeResultCapacityExhausted:
		return courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion
	case courierv1alpha1.NativeResultWorkloadFailed:
		return courierv1alpha1.TerminalDiagnosticCategoryWorkloadFailure
	case courierv1alpha1.NativeResultInfrastructureFailed:
		return courierv1alpha1.TerminalDiagnosticCategoryInfrastructureFailure
	default:
		// published and no_change are successful handoffs and never reach a
		// failure diagnostic; an empty or unmapped kind classifies nothing.
		return ""
	}
}

func classifyTerminalDiagnostic(handoffCategory string, exitCode *int32, outcome string, kubernetesFailure bool, trustedKind courierv1alpha1.NativeResultKind) courierv1alpha1.TerminalDiagnosticCategory {
	category := courierv1alpha1.TerminalDiagnosticCategory(strings.TrimSpace(handoffCategory))
	if category != "" && category != courierv1alpha1.TerminalDiagnosticCategoryUnknown && knownTerminalDiagnosticCategory(category) {
		return category
	}
	if trusted := categoryForNativeResultKind(trustedKind); trusted != "" {
		return trusted
	}
	if kubernetesFailure {
		return courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure
	}
	switch strings.TrimSpace(outcome) {
	case "needs_decision":
		return courierv1alpha1.TerminalDiagnosticCategoryNeedsDecision
	case "blocked_external":
		return courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal
	case outcomeCapacityExhausted:
		// The trusted outcome vocabulary alone establishes the category; no
		// transport-status heuristic participates (#320/#327).
		return courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion
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
	case courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion:
		return "execution capacity was confirmed exhausted"
	case courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable:
		// The attempt was never served by the model provider: no session
		// id was produced and no structured allowance-exhaustion signal
		// was observed, so the work is released for another capacity. The
		// label states what the evidence shows, not a quota verdict.
		return "the model provider did not serve this attempt; work released for another capacity"
	case courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable:
		// A forge read could not complete on a transient availability
		// condition, so the world was never observed; the work is
		// released for another capacity.
		return "a forge read could not complete on a transient availability condition; the world was never observed; work released for another capacity"
	case courierv1alpha1.TerminalDiagnosticCategoryWorkloadFailure:
		// The trusted native result classified the ending as the work's own
		// failure (#127): a verdict on the work, never on the infrastructure.
		return "the coordinator reported a workload failure: a verdict on the work, not the infrastructure"
	case courierv1alpha1.TerminalDiagnosticCategoryInfrastructureFailure:
		// The trusted native result classified the ending as an
		// infrastructure failure (#127): the relaunch/resume case bounded by
		// the crashloop counter, never a terminal verdict on the work.
		return "the coordinator reported an infrastructure failure; recovery is a bounded relaunch and resume"
	case courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash:
		return "coordinator exited without a valid handoff"
	case courierv1alpha1.TerminalDiagnosticCategoryUnknown:
		return "coordinator ended with no captured cause"
	}
	return ""
}

// kubeletContainerReason returns the coordinator container's kubelet
// `Terminated.Reason` when it is on the allowlist of runtime-written
// values (for example `OOMKilled`, `Error`, `Completed`,
// `ContainerStatusUnknown`); otherwise it returns `""`. The allowlist
// ensures arbitrary or non-pattern-matching kubelet text cannot land on
// the operator-derived diagnostic reason.
func kubeletContainerReason(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != coordinatorContainerName || status.State.Terminated == nil {
			continue
		}
		reason := strings.TrimSpace(status.State.Terminated.Reason)
		if !isAllowlistedContainerReason(reason) {
			return ""
		}
		return reason
	}
	return ""
}

// operatorDiagnosticReason builds the reason persisted on a diagnostic
// entirely from operator-derived data: the category label, the kubelet's
// allowlisted container and pod state reasons, and a small set of
// operator-controlled override strings (wedge reap, pod loss). The
// executor's freeform termination text is never an input, and the
// Kubernetes-derived text is also constrained to the allowlist of values
// the runtime itself writes (arbitrary kubelet or apiserver text is
// dropped). The result is passed through boundedDiagnosticReason for the
// length cap and a defensive pattern-redaction pass.
func operatorDiagnosticReason(category courierv1alpha1.TerminalDiagnosticCategory, override string, pod *corev1.Pod) string {
	parts := []string{categoryDiagnosticReason(category)}
	if reason := kubeletContainerReason(pod); reason != "" {
		parts = append(parts, "container: "+reason)
	}
	if pod != nil {
		if reason := strings.TrimSpace(pod.Status.Reason); isAllowlistedPodReason(reason) {
			parts = append(parts, "pod: "+reason)
		}
	}
	if override != "" {
		parts = append(parts, override)
	}
	return strings.Join(parts, "; ")
}

func buildTerminalDiagnostic(now time.Time, exitCode *int32, reasonOverride, outcome, handoffCategory string, kubernetesFailure bool, pod *corev1.Pod, trustedKind courierv1alpha1.NativeResultKind) *courierv1alpha1.TerminalDiagnostic {
	category := classifyTerminalDiagnostic(handoffCategory, exitCode, outcome, kubernetesFailure, trustedKind)
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
			// Apply the same allowlist + 128-byte bound that the CRD
			// promises: arbitrary or overlong kubelet text cannot persist on
			// the durable status, and an overlong value cannot cause the
			// API server to reject the terminal patch (which would block
			// terminalization and source reporting).
			diagnostic.TerminationReason = boundedContainerTerminationReason(status.State.Terminated.Reason)
		}
		break
	}
	return diagnostic
}

// kubernetesFailureEvidence reports causal Kubernetes/container failures,
// independently of the broader allowlist used only to safely display reasons.
func kubernetesFailureEvidence(pod *corev1.Pod, handoffValid bool) bool {
	if pod == nil {
		return true
	}
	if !handoffValid {
		if isInfraContainerReason(kubeletContainerReason(pod)) {
			return true
		}
		if isInfraPodReason(strings.TrimSpace(pod.Status.Reason)) {
			return true
		}
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
	return false
}
