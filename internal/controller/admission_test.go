package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

const admissionNamespace = "default"

func TestAdmittedCountOnlyCountsClaimedAndRunningOnLane(t *testing.T) {
	runs := []courierv1alpha1.CoderRun{
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseClaimed}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseVerifying}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhasePending}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseAwaitingReview}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseNeedsHuman}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseDone}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseFailed}},
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "cloud"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning}},
	}

	if got := admittedCount(runs, "local"); got != 2 {
		t.Fatalf("admittedCount(local) = %d, want 2; Verifying must not consume capacity", got)
	}
	if !laneHasCapacity(runs, "local", 3) {
		t.Fatal("laneHasCapacity(local, 3) = false, want true")
	}
	if laneHasCapacity(runs, "local", 2) {
		t.Fatal("laneHasCapacity(local, 2) = true, want false")
	}
}

func TestReconcileLeavesNPlusOnePending(t *testing.T) {
	client := admissionClient(t,
		admissionLane("local", 2),
		admissionRun("claimed", "local", courierv1alpha1.PhaseClaimed),
		admissionRun("running", "local", courierv1alpha1.PhaseRunning),
		admissionRun("next", "local", courierv1alpha1.PhasePending),
	)
	reconciler := admissionReconciler(client)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("next"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != capacityRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a blocked Pending run must wake itself", result.RequeueAfter, capacityRequeueDelay)
	}

	var next courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &next); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if next.Status.Phase != courierv1alpha1.PhasePending {
		t.Fatalf("next phase = %q, want Pending", next.Status.Phase)
	}
}

func TestReconcileAdmitsPendingRunWhenFirstRunVerifies(t *testing.T) {
	client := admissionClient(t,
		admissionLane("local", 1),
		admissionRun("busy", "local", courierv1alpha1.PhaseVerifying),
		admissionRun("next", "local", courierv1alpha1.PhasePending),
	)
	reconciler := admissionReconciler(client)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("next"))
	if err != nil {
		t.Fatalf("Reconcile() after first run became Verifying: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter after admission = %v, want 0", result.RequeueAfter)
	}
	var next courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &next); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if next.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("next phase = %q, want Claimed after capacity freed", next.Status.Phase)
	}
}

func TestReconcileAdmitsPendingRunAndClaimConsumesCapacity(t *testing.T) {
	client := admissionClient(t,
		admissionLane("local", 1),
		admissionRun("next", "local", courierv1alpha1.PhasePending),
	)
	reconciler := admissionReconciler(client)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("next")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var next courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &next); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if next.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("next phase = %q, want Claimed", next.Status.Phase)
	}

	var runs courierv1alpha1.CoderRunList
	if err := client.List(context.Background(), &runs); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if got := admittedCount(runs.Items, "local"); got != 1 {
		t.Fatalf("admittedCount(local) after admission = %d, want 1", got)
	}
}

func TestReconcileSuspendedLaneAdmitsNothingNew(t *testing.T) {
	lane := admissionLane("local", 2)
	lane.Annotations = map[string]string{courierv1alpha1.SuspendAnnotation: "true"}
	client := admissionClient(t, lane, admissionRun("waiting", "local", courierv1alpha1.PhasePending))
	reconciler := admissionReconciler(client)

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("waiting"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != capacityRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v so the run is re-checked after the lane resumes", result.RequeueAfter, capacityRequeueDelay)
	}
	var run courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("waiting"), &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.Phase != courierv1alpha1.PhasePending {
		t.Fatalf("phase = %q, want Pending on a suspended lane", run.Status.Phase)
	}

	// Lifting the annotation lets the same run through.
	if err := client.Get(context.Background(), admissionKey("local"), lane); err != nil {
		t.Fatalf("get lane: %v", err)
	}
	lane.Annotations = nil
	if err := client.Update(context.Background(), lane); err != nil {
		t.Fatalf("resume lane: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("waiting")); err != nil {
		t.Fatalf("Reconcile() after resume error = %v", err)
	}
	if err := client.Get(context.Background(), admissionKey("waiting"), &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase after resume = %q, want Claimed", run.Status.Phase)
	}
}

func TestLaneSuspendedOnlyForTrue(t *testing.T) {
	for value, want := range map[string]bool{"true": true, " TRUE ": true, "false": false, "": false, "yes": false} {
		lane := &courierv1alpha1.LaneProfile{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{courierv1alpha1.SuspendAnnotation: value}}}
		if got := lane.Suspended(); got != want {
			t.Errorf("Suspended() with %q = %v, want %v", value, got, want)
		}
	}
	if (&courierv1alpha1.LaneProfile{}).Suspended() {
		t.Error("a lane without the annotation must not be suspended")
	}
}

func TestReconcileTreatsEmptyPhaseAsPending(t *testing.T) {
	client := admissionClient(t,
		admissionLane("local", 1),
		&courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: admissionNamespace},
			Spec:       courierv1alpha1.CoderRunSpec{Mode: courierv1alpha1.ModeResolveIssue, Repo: "acme/widgets", Ref: 1, Lane: "local", Source: "test", WorkItemID: "new"},
		},
	)
	reconciler := admissionReconciler(client)

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("new")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var run courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("new"), &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("new run phase = %q, want Claimed", run.Status.Phase)
	}
}

func TestReconcileFailedLaunchReleasesCapacity(t *testing.T) {
	launchErr := errors.New("pod create failed")
	client := admissionClient(t,
		admissionLane("local", 1),
		admissionRun("next", "local", courierv1alpha1.PhasePending),
	)
	reconciler := admissionReconciler(client)
	reconciler.Launch = func(context.Context, *courierv1alpha1.CoderRun) error {
		return launchErr
	}

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("next")); !errors.Is(err, launchErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, launchErr)
	}

	var next courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &next); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if next.Status.Phase != courierv1alpha1.PhasePending {
		t.Fatalf("next phase after failed launch = %q, want Pending", next.Status.Phase)
	}

	var runs courierv1alpha1.CoderRunList
	if err := client.List(context.Background(), &runs); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if got := admittedCount(runs.Items, "local"); got != 0 {
		t.Fatalf("admittedCount(local) after failed launch = %d, want 0", got)
	}
}

func admissionScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	return scheme
}

func admissionClient(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(admissionScheme(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(objects...).
		Build()
}

// fakeStatusWriter applies operator status patches through the fake client,
// which serves JSON merge patches exactly like the status subresource does.
// It interprets only the fields the operator owns; omitted fields are left
// untouched, so harness checkpoint/heartbeat state survives operator writes.
type fakeStatusWriter struct {
	client client.Client
}

func (w fakeStatusWriter) PatchStatus(ctx context.Context, name types.NamespacedName, patch []byte) error {
	var document struct {
		Status struct {
			Phase            courierv1alpha1.Phase `json:"phase,omitempty"`
			Branch           *string               `json:"branch,omitempty"`
			HeadRepo         *string               `json:"headRepo,omitempty"`
			HeadSHA          *string               `json:"headSHA,omitempty"`
			PR               string                `json:"pr,omitempty"`
			AdmittedAt       *metav1.Time          `json:"admittedAt,omitempty"`
			StartedAt        *metav1.Time          `json:"startedAt,omitempty"`
			FinishedAt       *metav1.Time          `json:"finishedAt,omitempty"`
			WaitDuration     string                `json:"waitDuration,omitempty"`
			RunDuration      string                `json:"runDuration,omitempty"`
			CheckFingerprint *string               `json:"checkFingerprint,omitempty"`
			Restarts         *int                  `json:"restarts,omitempty"`
			Conditions       []metav1.Condition    `json:"conditions,omitempty"`
		} `json:"status"`
	}
	if err := json.Unmarshal(patch, &document); err != nil {
		return err
	}
	var run courierv1alpha1.CoderRun
	if err := w.client.Get(ctx, name, &run); err != nil {
		return err
	}
	if document.Status.Phase != "" {
		run.Status.Phase = document.Status.Phase
	}
	if document.Status.Branch != nil {
		run.Status.Branch = *document.Status.Branch
	}
	if document.Status.HeadRepo != nil {
		run.Status.HeadRepo = *document.Status.HeadRepo
	}
	if document.Status.HeadSHA != nil {
		run.Status.HeadSHA = *document.Status.HeadSHA
	}
	if document.Status.PR != "" {
		run.Status.PR = document.Status.PR
	}
	if document.Status.AdmittedAt != nil {
		run.Status.AdmittedAt = document.Status.AdmittedAt
	}
	if document.Status.StartedAt != nil {
		run.Status.StartedAt = document.Status.StartedAt
	}
	if document.Status.FinishedAt != nil {
		run.Status.FinishedAt = document.Status.FinishedAt
	}
	if document.Status.WaitDuration != "" {
		run.Status.WaitDuration = document.Status.WaitDuration
	}
	if document.Status.RunDuration != "" {
		run.Status.RunDuration = document.Status.RunDuration
	}
	if document.Status.CheckFingerprint != nil {
		run.Status.CheckFingerprint = *document.Status.CheckFingerprint
	}
	if document.Status.Restarts != nil {
		run.Status.Restarts = *document.Status.Restarts
	}
	if document.Status.Conditions != nil {
		run.Status.Conditions = document.Status.Conditions
	}
	return w.client.Status().Update(ctx, &run)
}

func admissionReconciler(c client.Client) *CoderRunReconciler {
	return &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
	}
}

func admissionLane(name string, concurrency int) *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: admissionNamespace},
		Spec:       courierv1alpha1.LaneProfileSpec{Concurrency: concurrency},
	}
}

func admissionRun(name, lane string, phase courierv1alpha1.Phase) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: admissionNamespace},
		Spec:       courierv1alpha1.CoderRunSpec{Mode: courierv1alpha1.ModeResolveIssue, Repo: "acme/widgets", Ref: 1, Lane: lane, Source: "test", WorkItemID: name},
		Status:     courierv1alpha1.CoderRunStatus{Phase: phase},
	}
}

func admissionKey(name string) types.NamespacedName {
	return types.NamespacedName{Name: name, Namespace: admissionNamespace}
}

func admissionRequest(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: admissionKey(name)}
}
