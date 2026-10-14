package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
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
		{Spec: courierv1alpha1.CoderRunSpec{Lane: "local"}, Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseHandedOff}},
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
			Phase                           courierv1alpha1.Phase              `json:"phase,omitempty"`
			Branch                          *string                            `json:"branch,omitempty"`
			HeadRepo                        *string                            `json:"headRepo,omitempty"`
			HeadSHA                         *string                            `json:"headSHA,omitempty"`
			PR                              string                             `json:"pr,omitempty"`
			AdmittedAt                      *metav1.Time                       `json:"admittedAt,omitempty"`
			StartedAt                       *metav1.Time                       `json:"startedAt,omitempty"`
			FinishedAt                      *metav1.Time                       `json:"finishedAt,omitempty"`
			WaitDuration                    string                             `json:"waitDuration,omitempty"`
			RunDuration                     string                             `json:"runDuration,omitempty"`
			CheckFingerprint                *string                            `json:"checkFingerprint,omitempty"`
			FailedCheckFingerprint          *string                            `json:"failedCheckFingerprint,omitempty"`
			DeclaredNoChange                *bool                              `json:"declaredNoChange,omitempty"`
			LifecycleReportRetryToken       *string                            `json:"lifecycleReportRetryToken,omitempty"`
			LifecycleReportActiveRetryToken *string                            `json:"lifecycleReportActiveRetryToken,omitempty"`
			Restarts                        *int                               `json:"restarts,omitempty"`
			Conditions                      []metav1.Condition                 `json:"conditions,omitempty"`
			PublicationPolicy               *courierv1alpha1.PublicationPolicy `json:"publicationPolicy,omitempty"`
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
	if document.Status.FailedCheckFingerprint != nil {
		run.Status.FailedCheckFingerprint = *document.Status.FailedCheckFingerprint
	}
	if document.Status.DeclaredNoChange != nil {
		run.Status.DeclaredNoChange = *document.Status.DeclaredNoChange
	}
	if document.Status.LifecycleReportRetryToken != nil {
		run.Status.LifecycleReportRetryToken = *document.Status.LifecycleReportRetryToken
	}
	if document.Status.LifecycleReportActiveRetryToken != nil {
		run.Status.LifecycleReportActiveRetryToken = *document.Status.LifecycleReportActiveRetryToken
	}
	if document.Status.Restarts != nil {
		run.Status.Restarts = *document.Status.Restarts
	}
	if document.Status.Conditions != nil {
		run.Status.Conditions = document.Status.Conditions
	}
	if document.Status.PublicationPolicy != nil {
		run.Status.PublicationPolicy = document.Status.PublicationPolicy
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

// admissionLaneWithCoordinator mirrors admissionLane but names a coordinator
// role, so a test that drives the real CoordinatorLauncher (which builds the
// pod from the lane) does not fail on the missing-model validation.
func admissionLaneWithCoordinator(name string, concurrency int) *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: admissionNamespace},
		Spec: courierv1alpha1.LaneProfileSpec{
			Concurrency: concurrency,
			Roles:       map[string]string{"coordinator": "test-model"},
		},
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

func TestReconcileStaysClaimedWhenCoordinatorSlotIsTerminating(t *testing.T) {
	// The deterministic pod name is held by a pod that is being torn down.
	// CoordinatorLauncher.Launch must report the collision as
	// ErrProvisioningInFlight, the reconciler must stay Claimed with a
	// bounded requeue, and capacity must not be released back to Pending.
	controller := true
	now := metav1.Now()
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "next-coordinator",
			Namespace:         admissionNamespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{"keep.test/deletion"},
			Labels:            map[string]string{executor.LabelRun: "next"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: courierv1alpha1.GroupVersion.String(),
				Kind:       "CoderRun",
				Name:       "next",
				Controller: &controller,
			}},
		},
	}
	client := launcherClientForAdmission(t,
		admissionLaneWithCoordinator("local", 1),
		admissionRun("next", "local", courierv1alpha1.PhasePending),
		existing,
	)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch:       launchFuncForAdmission(client, "example/opencode:test"),
	}

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("next"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil; a provisioning-in-flight slot must not surface as a launch failure", err)
	}
	if result.RequeueAfter != provisioningInFlightRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; the wait rides the pod delete event with a bounded requeue as the backstop",
			result.RequeueAfter, provisioningInFlightRequeueDelay)
	}

	var next courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &next); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if next.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("next phase = %q, want Claimed; the run must not record Running while the slot is being torn down", next.Status.Phase)
	}

	var runs courierv1alpha1.CoderRunList
	if err := client.List(context.Background(), &runs); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if got := admittedCount(runs.Items, "local"); got != 1 {
		t.Fatalf("admittedCount(local) after a provisioning-in-flight wait = %d, want 1; the Claimed slot is still reserved", got)
	}
}

func TestResumeClaimedStaysClaimedWhenCoordinatorSlotIsTerminating(t *testing.T) {
	// resumeClaimed is the relaunch path: the run was already Claimed, the
	// launcher's create collided with a terminating pod, and the run must
	// not advance to Running until a live pod occupies the name. Without
	// the fix, the next reconcile would either terminalize the run as
	// Failed off the dying pod's own termination, or charge a second
	// infrastructure loss if the pod vanished first.
	controller := true
	now := metav1.Now()
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "next-coordinator",
			Namespace:         admissionNamespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{"keep.test/deletion"},
			Labels:            map[string]string{executor.LabelRun: "next"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: courierv1alpha1.GroupVersion.String(),
				Kind:       "CoderRun",
				Name:       "next",
				Controller: &controller,
			}},
		},
	}
	client := launcherClientForAdmission(t,
		admissionLaneWithCoordinator("local", 1),
		admissionRun("next", "local", courierv1alpha1.PhaseClaimed),
		existing,
	)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch:       launchFuncForAdmission(client, "example/opencode:test"),
	}

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("next"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil; a provisioning-in-flight slot must not surface as a launch failure", err)
	}
	if result.RequeueAfter != provisioningInFlightRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; the wait rides the pod delete event with a bounded requeue as the backstop",
			result.RequeueAfter, provisioningInFlightRequeueDelay)
	}

	var next courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &next); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if next.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("next phase = %q, want Claimed; the run must not record Running while the slot is being torn down", next.Status.Phase)
	}
}

func TestResumeClaimedReachesRunningWhenLaunchSucceeds(t *testing.T) {
	// Once the slot is free — no terminating pod, the create succeeds — the
	// resumeClaimed path finishes the admission and the run records
	// Running, the existing idempotency the issue requires us to preserve.
	client := launcherClientForAdmission(t,
		admissionLaneWithCoordinator("local", 1),
		admissionRun("next", "local", courierv1alpha1.PhaseClaimed),
	)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch:       launchFuncForAdmission(client, "example/opencode:test"),
	}

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("next")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var next courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &next); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if next.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("next phase = %q, want Running; a free slot advances the resumed admission", next.Status.Phase)
	}
	var pod corev1.Pod
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: admissionNamespace, Name: "next-coordinator"}, &pod); err != nil {
		t.Fatalf("get coordinator pod: %v", err)
	}
	if pod.DeletionTimestamp != nil {
		t.Fatalf("coordinator pod has DeletionTimestamp set; a fresh Launch must not produce a terminating pod")
	}
}

// launcherClientForAdmission is the admission_test mirror of launcherScheme's
// fake client: it registers corev1 alongside the Courier types so a test that
// exercises the live launcher can seed a terminating pod alongside the run.
func launcherClientForAdmission(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(objects...).
		Build()
}

// launchFuncForAdmission adapts the production CoordinatorLauncher to a
// LaunchFunc so a controller test can drive the real launch path through
// the reconciler. The fake client carries the run, lane, and any seed
// pod (live or terminating) the test wants to exercise.
func launchFuncForAdmission(c client.Client, image string) LaunchFunc {
	launcher := &CoordinatorLauncher{
		Client: c,
		Pod:    executor.PodConfig{Image: image},
	}
	return launcher.Launch
}
