package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	couriergit "github.com/misospace/courier/internal/git"
)

// legacyWorkspaceObservation consumes only the isolated container's kubelet
// termination channel. The coordinator's model-writable handoff is never proof.
func (r *CoderRunReconciler) legacyWorkspaceObservation(ctx context.Context, run *courier.CoderRun, cached *corev1.Pod) (*courier.WorkspaceSafetyEvidence, bool, error) {
	var pod corev1.Pod
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(cached), &pod); err != nil {
		return nil, false, err
	}
	if pod.UID == "" || pod.UID != cached.UID || !pod.DeletionTimestamp.IsZero() || !ownedLegacyPod(&pod, run) {
		return nil, false, nil
	}
	if !isolatedObserverSpec(&pod) {
		return nil, false, nil
	}
	var writer, observer *corev1.ContainerStateTerminated
	var writerID string
	for _, s := range pod.Status.ContainerStatuses {
		if s.RestartCount != 0 {
			return nil, false, nil
		}
		if s.Name == "coordinator" {
			writer = s.State.Terminated
			writerID = s.ContainerID
		}
		if s.Name == executor.ObserverContainerName {
			observer = s.State.Terminated
		}
	}
	if writer == nil || writerID == "" {
		return nil, true, nil
	}
	if observer == nil {
		return nil, true, nil
	}
	if observer.ExitCode != 0 || observer.FinishedAt.Before(&writer.FinishedAt) || len(observer.Message) > executor.MaxTerminationLineBytes {
		return nil, false, nil
	}
	var proof executor.ObserverEvidence
	if json.Unmarshal([]byte(observer.Message), &proof) != nil || proof.Version != 1 || proof.PodUID != string(pod.UID) || proof.RunUID != string(run.UID) || proof.WriterContainerID != writerID || proof.Branch != run.Status.Branch || !couriergit.ValidOID(proof.Baseline) {
		return nil, false, nil
	}
	seedOK := false
	for _, s := range pod.Status.InitContainerStatuses {
		if s.Name == "workspace-observer-seed" && s.State.Terminated != nil && s.State.Terminated.ExitCode == 0 && strings.TrimSpace(s.State.Terminated.Message) == proof.Baseline {
			seedOK = true
		}
	}
	if !seedOK || proof.Safety == nil || proof.Safety.Branch != run.Status.Branch || !couriergit.ValidOID(proof.Safety.Head) {
		return nil, false, nil
	}
	safety := proof.Safety
	switch safety.State {
	case "empty":
		if safety.Head != proof.Baseline || safety.RemoteHead != "" {
			return nil, false, nil
		}
	case "pushed":
		if safety.RemoteHead != safety.Head {
			return nil, false, nil
		}
	case "dirty", "unknown":
	default:
		return nil, false, nil
	}
	return &courier.WorkspaceSafetyEvidence{State: safety.State, Branch: safety.Branch, Head: safety.Head, RemoteHead: safety.RemoteHead}, false, nil
}

func ownedLegacyPod(pod *corev1.Pod, run *courier.CoderRun) bool {
	if pod.Namespace != run.Namespace || run.UID == "" {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.UID == run.UID && owner.Name == run.Name && owner.Kind == "CoderRun" && owner.APIVersion == courier.GroupVersion.String() {
			return true
		}
	}
	return false
}

func isolatedObserverSpec(pod *corev1.Pod) bool {
	if pod.Annotations[executor.ObserverContractAnnotation] != executor.ObserverContractVersion {
		return false
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever || pod.Spec.HostPID || (pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace) || len(pod.Spec.Containers) != 2 {
		return false
	}
	var writer, observer *corev1.Container
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		switch c.Name {
		case "coordinator":
			writer = c
		case executor.ObserverContainerName:
			observer = c
		}
	}
	if writer == nil || observer == nil || observer.TerminationMessagePath != executor.ObserverStatePath+"/private/termination" || observer.TerminationMessagePolicy != corev1.TerminationMessageReadFile || observer.Image != writer.Image || !reflect.DeepEqual(observer.Command, []string{"/usr/local/bin/courier-executor"}) || !reflect.DeepEqual(observer.Args, []string{"observe-workspace"}) {
		return false
	}
	security := observer.SecurityContext
	if security == nil || security.RunAsUser == nil || *security.RunAsUser != executor.ObserverUID || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation || security.Privileged != nil && *security.Privileged {
		return false
	}
	if writer.SecurityContext != nil && writer.SecurityContext.RunAsUser != nil && *writer.SecurityContext.RunAsUser == executor.ObserverUID {
		return false
	}
	// Every writable observer volume must be private to its trusted containers;
	// the only shared volume is the workspace, mounted read-only by the observer.
	seedValid := false
	for _, seed := range pod.Spec.InitContainers {
		if seed.Name == "workspace-observer-seed" && seed.Image == observer.Image && reflect.DeepEqual(seed.Command, observer.Command) && reflect.DeepEqual(seed.Args, []string{"observe-seed"}) && seed.TerminationMessagePath == executor.ObserverStatePath+"/private/seed-termination" {
			seedValid = true
		}
	}
	if !seedValid {
		return false
	}
	workspace := false
	for _, m := range observer.VolumeMounts {
		for _, w := range writer.VolumeMounts {
			if w.Name == m.Name {
				if m.Name != "workspace" || !m.ReadOnly {
					return false
				}
				workspace = true
			}
		}
	}
	if !workspace || len(observer.EnvFrom) != 0 || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		return false
	}
	return true
}

func legacyObservationCondition(run *courier.CoderRun, now metav1.Time) metav1.Condition {
	return metav1.Condition{Type: "LegacyWorkspaceObserved", Status: metav1.ConditionTrue, Reason: "IsolatedObserver", ObservedGeneration: run.Generation, LastTransitionTime: now, Message: "workspace observed after writer termination by the isolated run-owned container"}
}
