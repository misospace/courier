package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

// The trusted termination handoff's compact summary must survive the full
// production status write: coordinatorTermination → run.Status.Telemetry →
// patchStatus → status.OperatorPatch → status.KubePatchWriter → persisted
// CoderRun.status.telemetry — and then survive the later terminal patch that
// publishes the source lifecycle report. PR #206 exposed this class of
// failure for PublicationPolicy: a field can exist in the API and the status
// writer yet be silently omitted by patchStatus. Removing the Telemetry
// emission from patchStatus must fail this test.
func TestEnvtestTelemetryPersistsThroughStatusPatch(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	name := "telemetry-persist"
	laneName := "envtest-telemetry"
	item := &admissionSource{}
	run := envtestRun(name, laneName)
	lane := envtestLane(laneName)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}

	// Representative values for every shape the summary carries: scalar
	// counters, token usage, and a subagent tally.
	want := &courierv1alpha1.RunTelemetry{
		ModelCalls: 7,
		ToolCalls:  19,
		Sessions:   2,
		MaxContext: 131072,
		Tokens: &courierv1alpha1.TokenUsage{
			Input:      12000,
			Output:     3400,
			Reasoning:  900,
			CacheRead:  48000,
			CacheWrite: 512,
			Total:      65112,
		},
		Subagents: []courierv1alpha1.SubagentTally{
			{Agent: "explore", Model: "scanner", Calls: 2, Errors: 1, RuntimeMillis: 4321},
		},
		DurationMillis: 123456,
		Continuations:  1,
	}
	// The handoff is pinned as the raw wire format the executor emits, not
	// marshaled from the operator's own struct, so a drifted field tag or
	// prefix cannot round-trip undetected.
	const payload = `COURIER_TERMINATION {"phase":"Verifying","result":"success","exit_code":0,"reason":"published through the broker","outcome":"changes","summary":{"modelCalls":7,"toolCalls":19,"sessions":2,"maxContext":131072,"tokens":{"input":12000,"output":3400,"reasoning":900,"cacheRead":48000,"cacheWrite":512,"total":65112},"subagents":[{"agent":"explore","model":"scanner","calls":2,"errors":1,"runtimeMillis":4321}],"durationMillis":123456,"continuations":1}}`

	reconciler := &CoderRunReconciler{
		Client: envtestClient,
		Sources: NewSourceRegistry(map[string]source.Adapter{
			"manual": item,
		}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			greenObservation("sha-1", "check-a"),
			greenObservation("sha-1", "check-a"),
		}},
		Launch: func(ctx context.Context, run *courierv1alpha1.CoderRun) error {
			controller := true
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      run.Name + "-coordinator",
					Namespace: run.Namespace,
					Labels:    map[string]string{executor.LabelRun: run.Name},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: courierv1alpha1.GroupVersion.String(),
						Kind:       "CoderRun",
						Name:       run.Name,
						UID:        run.UID,
						Controller: &controller,
					}},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "coordinator", Image: "example/test"}}},
			}
			if err := envtestClient.Create(ctx, pod); err != nil {
				return err
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "coordinator",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 0,
					Message:  payload,
				}},
			}}
			return envtestClient.Status().Update(ctx, pod)
		},
	}

	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, envtestRequest(name)); err != nil {
			t.Fatal(err)
		}
	}

	// Claim and launch, then the terminated coordinator hands off on the
	// successful exit-0 path into Verifying.
	reconcile()
	assertEnvtestPhase(t, name, courierv1alpha1.PhaseRunning)
	reconcile()
	assertEnvtestPhase(t, name, courierv1alpha1.PhaseVerifying)

	// The re-read run must carry the handoff's summary exactly, persisted
	// through a real status-subresource merge patch against the API server.
	var verifying courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(name), &verifying); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verifying.Status.Telemetry, want) {
		t.Fatalf("persisted telemetry = %#v, want %#v", verifying.Status.Telemetry, want)
	}

	// Two identical all-green observations settle Verifying into
	// AwaitingReview. Telemetry must survive that terminal patch — it is the
	// later write that publishes the source lifecycle report.
	reconcile()
	reconcile()
	assertEnvtestPhase(t, name, courierv1alpha1.PhaseAwaitingReview)

	var settled courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(name), &settled); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(settled.Status.Telemetry, want) {
		t.Fatalf("telemetry after AwaitingReview = %#v, want %#v — the terminal patch cleared it", settled.Status.Telemetry, want)
	}
	if len(item.reports) != 1 {
		t.Fatalf("reports = %#v, want one AwaitingReview lifecycle report", item.reports)
	}
	var reported courierv1alpha1.RunTelemetry
	if err := json.Unmarshal(item.reports[0].Telemetry, &reported); err != nil {
		t.Fatalf("lifecycle report telemetry = %q: %v", item.reports[0].Telemetry, err)
	}
	if !reflect.DeepEqual(&reported, want) {
		t.Fatalf("report telemetry = %#v, want %#v", &reported, want)
	}
}
