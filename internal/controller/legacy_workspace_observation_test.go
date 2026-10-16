package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestLegacyWorkspaceObservationTrustBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*corev1.Pod)
		want    bool
		pending bool
	}{
		{name: "clean empty", want: true},
		{name: "pushed", want: true, change: func(p *corev1.Pod) {
			var e executor.ObserverEvidence
			_ = json.Unmarshal([]byte(p.Status.ContainerStatuses[1].State.Terminated.Message), &e)
			e.Safety.State = "pushed"
			e.Safety.Head = strings.Repeat("b", 40)
			e.Safety.RemoteHead = e.Safety.Head
			b, _ := json.Marshal(e)
			p.Status.ContainerStatuses[1].State.Terminated.Message = string(b)
		}},
		{name: "spoofed legacy termination", change: func(p *corev1.Pod) { p.Status.ContainerStatuses = p.Status.ContainerStatuses[:1] }, pending: true},
		{name: "live writer", change: func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}, pending: true},
		{name: "failed observation", change: func(p *corev1.Pod) { p.Status.ContainerStatuses[1].State.Terminated.ExitCode = 1 }},
		{name: "wrong run UID", change: func(p *corev1.Pod) { p.OwnerReferences[0].UID = "foreign" }},
		{name: "writer reaches output", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "observer-state", MountPath: "/spoof"})
		}},
		{name: "untrusted observer identity", change: func(p *corev1.Pod) {
			p.Spec.Containers[1].SecurityContext.RunAsUser = ptrInt64(executor.CoordinatorUID)
		}},
		{name: "missing evidence", change: func(p *corev1.Pod) { p.Status.ContainerStatuses[1].State.Terminated.Message = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := admissionRun("observed", "pool", courier.PhaseRunning)
			run.UID = types.UID("run-uid")
			run.Status.Branch = "branch"
			lane := admissionLaneWithCoordinator("pool", 1)
			pod, err := executor.BuildCoordinatorPod(run, lane, executor.DefaultPodConfig())
			if err != nil {
				t.Fatal(err)
			}
			pod.UID = "pod-uid"
			head := strings.Repeat("a", 40)
			now := metav1.NewTime(time.Now())
			proof := executor.ObserverEvidence{Version: 1, RunUID: string(run.UID), PodUID: string(pod.UID), Branch: "branch", Baseline: head, WriterContainerID: "containerd://writer", Safety: &executor.WorkspaceSafetyEvidence{State: "empty", Branch: "branch", Head: head}}
			payload, _ := json.Marshal(proof)
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{Name: "coordinator", ContainerID: "containerd://writer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, FinishedAt: now, Message: `COURIER_TERMINATION {"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"capacity_exhausted","workspace_safety":{"state":"empty"}}`}}},
				{Name: executor.ObserverContainerName, ContainerID: "containerd://observer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, FinishedAt: now, Message: string(payload)}}},
			}
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "workspace-observer-seed", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: head}}}}
			if tc.change != nil {
				tc.change(pod)
			}
			c := capacityTestClient(t, run, lane, pod)
			r := &CoderRunReconciler{Client: c, APIReader: c}
			got, pending, err := r.legacyWorkspaceObservation(context.Background(), run, pod)
			if err != nil || (got != nil) != tc.want || pending != tc.pending {
				t.Fatalf("proof=%#v pending=%v err=%v", got, pending, err)
			}
			// Reconstruct the reconciler: proof is derived from kubelet state, not memory.
			r = &CoderRunReconciler{Client: c, APIReader: c}
			again, _, err := r.legacyWorkspaceObservation(context.Background(), run, pod)
			if err != nil || (again != nil) != tc.want {
				t.Fatalf("restart proof=%#v err=%v", again, err)
			}
		})
	}
}
func ptrInt64(v int64) *int64 { return &v }
