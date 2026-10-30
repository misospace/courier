package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// preflightBackoffNow is the fixed clock the preflight-backoff tests read the
// gate against, so no test ever sleeps.
var preflightBackoffNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// preflightBackoffClient builds a fake client that counts the run's metadata
// patches and status writes, so a test can prove that a gated reconcile stays
// quiet — no claim, no status write, nothing that would wake the run early —
// and that clearing an absent gate writes nothing at all.
func preflightBackoffClient(t *testing.T, patches, statusWrites *int, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courier.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courier.CoderRun{}).
		WithRuntimeObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				*patches++
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				*statusWrites++
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()
}

// preflightBackoffFailingClient builds a fake client whose every metadata
// patch fails, so a test can see what the gate does when the marker cannot be
// written.
func preflightBackoffFailingClient(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courier.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courier.CoderRun{}).
		WithRuntimeObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return errors.New("metadata patch refused")
			},
		}).
		Build()
}

// failingStatusWriter refuses every status patch, so a test can see what the
// launch path does when the rollback to Pending cannot be written. It is
// declared in diagnostics_test.go.

// TestPreflightBackoffDelay pins the delay schedule: each further consecutive
// failure waits twice as long, up to the ceiling, and the ceiling holds
// however many failures have accumulated.
func TestPreflightBackoffDelay(t *testing.T) {
	cases := map[int]time.Duration{
		1:  15 * time.Second,
		2:  30 * time.Second,
		3:  60 * time.Second,
		4:  120 * time.Second,
		5:  240 * time.Second,
		6:  300 * time.Second,
		10: 300 * time.Second,
	}
	for failures, want := range cases {
		if got := preflightBackoffDelay(failures); got != want {
			t.Fatalf("preflightBackoffDelay(%d) = %v, want %v", failures, got, want)
		}
	}
	// A count that could overflow a shift still yields the bounded ceiling.
	if got := preflightBackoffDelay(1 << 40); got != preflightBackoffMaxDelay {
		t.Fatalf("preflightBackoffDelay(huge) = %v, want the ceiling %v", got, preflightBackoffMaxDelay)
	}
}

// TestPreflightBackoffRemainingFailsOpen pins the gate's fail-open posture: a
// marker the operator cannot read must never hold a run back, because a gate
// that cannot be read must not strand a run that could still succeed. An
// unreadable failure count is the one marker that is read leniently — as a
// single failure, the shortest wait the schedule allows.
func TestPreflightBackoffRemainingFailsOpen(t *testing.T) {
	reconciler := &CoderRunReconciler{Now: func() time.Time { return preflightBackoffNow }}
	run := admissionRun("preflight-fail-open", "local", courier.PhasePending)

	if remaining, ok := reconciler.preflightBackoffRemaining(run); ok {
		t.Fatalf("remaining = %v, want no gate on a run with no annotations", remaining)
	}

	run.Annotations = map[string]string{preflightFailedAtAnnotation: "not-a-timestamp"}
	if remaining, ok := reconciler.preflightBackoffRemaining(run); ok {
		t.Fatalf("remaining = %v, want no gate on an unreadable timestamp", remaining)
	}

	run.Annotations = map[string]string{
		preflightFailedAtAnnotation: preflightBackoffNow.Add(-time.Second).UTC().Format(time.RFC3339),
		preflightFailuresAnnotation: "not-a-count",
	}
	remaining, ok := reconciler.preflightBackoffRemaining(run)
	if !ok {
		t.Fatal("remaining = not gated, want the gate to hold an unreadable count as a single failure")
	}
	if want := preflightBackoffBaseDelay - time.Second; remaining != want {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}

	run.Annotations = map[string]string{
		preflightFailedAtAnnotation: preflightBackoffNow.Add(-5 * time.Second).UTC().Format(time.RFC3339),
		preflightFailuresAnnotation: "3",
	}
	remaining, ok = reconciler.preflightBackoffRemaining(run)
	if !ok {
		t.Fatal("remaining = not gated, want the gate to hold a freshly armed run")
	}
	if want := preflightBackoffDelay(3) - 5*time.Second; remaining != want {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}

	run.Annotations = map[string]string{
		preflightFailedAtAnnotation: preflightBackoffNow.Add(-preflightBackoffDelay(3)).UTC().Format(time.RFC3339),
		preflightFailuresAnnotation: "3",
	}
	if remaining, ok := reconciler.preflightBackoffRemaining(run); ok {
		t.Fatalf("remaining = %v, want no gate on an elapsed marker", remaining)
	}
}

// TestArmAndClearPreflightBackoff pins the gate's durable record: arming
// counts the consecutive failure and restamps the timestamp, clearing removes
// the gate so a later failure starts from the base delay again, and clearing a
// run that carries no gate writes nothing.
func TestArmAndClearPreflightBackoff(t *testing.T) {
	ctx := context.Background()
	now := preflightBackoffNow
	run := admissionRun("preflight-arm-clear", "local", courier.PhasePending)
	patches, statusWrites := 0, 0
	c := preflightBackoffClient(t, &patches, &statusWrites, run)
	reconciler := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}

	if err := reconciler.armPreflightBackoff(ctx, run); err != nil {
		t.Fatalf("armPreflightBackoff: %v", err)
	}
	if got := run.Annotations[preflightFailuresAnnotation]; got != "1" {
		t.Fatalf("failures = %q, want the first failure counted", got)
	}
	if got := run.Annotations[preflightFailedAtAnnotation]; got != now.UTC().Format(time.RFC3339) {
		t.Fatalf("failed-at = %q, want the arming timestamp", got)
	}
	// The gate is durable: it lives on the object, so a controller restart
	// cannot turn the bounded wait back into the tight churn.
	var stored courier.CoderRun
	if err := c.Get(ctx, admissionKey("preflight-arm-clear"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Annotations[preflightFailuresAnnotation] != "1" || stored.Annotations[preflightFailedAtAnnotation] == "" {
		t.Fatalf("annotations = %#v, want the gate persisted on the run", stored.Annotations)
	}

	// A second failure counts again and restamps the timestamp.
	now = now.Add(time.Minute)
	second := stored.DeepCopy()
	if err := reconciler.armPreflightBackoff(ctx, second); err != nil {
		t.Fatalf("second armPreflightBackoff: %v", err)
	}
	if got := second.Annotations[preflightFailuresAnnotation]; got != "2" {
		t.Fatalf("failures = %q, want the second failure counted", got)
	}
	if got := second.Annotations[preflightFailedAtAnnotation]; got != now.UTC().Format(time.RFC3339) {
		t.Fatalf("failed-at = %q, want the timestamp restamped", got)
	}

	// Clearing removes the gate entirely.
	if err := reconciler.clearPreflightBackoff(ctx, second); err != nil {
		t.Fatalf("clearPreflightBackoff: %v", err)
	}
	var cleared courier.CoderRun
	if err := c.Get(ctx, admissionKey("preflight-arm-clear"), &cleared); err != nil {
		t.Fatalf("get cleared run: %v", err)
	}
	if _, ok := cleared.Annotations[preflightFailedAtAnnotation]; ok {
		t.Fatalf("annotations = %#v, want the failed-at marker cleared", cleared.Annotations)
	}
	if _, ok := cleared.Annotations[preflightFailuresAnnotation]; ok {
		t.Fatalf("annotations = %#v, want the failure count cleared", cleared.Annotations)
	}

	// A failure after a cleared gate starts from the base delay again.
	third := cleared.DeepCopy()
	if err := reconciler.armPreflightBackoff(ctx, third); err != nil {
		t.Fatalf("third armPreflightBackoff: %v", err)
	}
	if got := third.Annotations[preflightFailuresAnnotation]; got != "1" {
		t.Fatalf("failures = %q, want the count restarted after a clear", got)
	}

	// A run that carries no gate is left untouched: the clear never writes.
	before := patches
	if err := reconciler.clearPreflightBackoff(ctx, &cleared); err != nil {
		t.Fatalf("clearPreflightBackoff on an ungated run: %v", err)
	}
	if patches != before {
		t.Fatalf("patches = %d, want no write clearing an absent gate", patches)
	}
}

// TestPreflightBackoffGatesReclaim is the reconciler half of the gate: a
// Pending run whose secure preflight failed waits out its delay without being
// claimed, and without any write that would wake the controller immediately —
// the tight Pending/Claimed churn the gate exists to stop. Once the delay is
// up, the run is admitted again.
func TestPreflightBackoffGatesReclaim(t *testing.T) {
	ctx := context.Background()
	now := preflightBackoffNow
	run := admissionRun("preflight-gated", "local", courier.PhasePending)
	run.Annotations = map[string]string{
		preflightFailedAtAnnotation: now.UTC().Format(time.RFC3339),
		preflightFailuresAnnotation: "1",
	}
	patches, statusWrites := 0, 0
	c := preflightBackoffClient(t, &patches, &statusWrites, admissionLane("local", 1), run)
	src := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter: fakeStatusWriter{client: c},
		Now:          func() time.Time { return now },
	}

	result, err := reconciler.Reconcile(ctx, admissionRequest("preflight-gated"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(src.claimed) != 0 {
		t.Fatalf("claimed = %#v, want no claim while the gate holds", src.claimed)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > preflightBackoffBaseDelay {
		t.Fatalf("RequeueAfter = %v, want the gate's remaining wait of at most %v", result.RequeueAfter, preflightBackoffBaseDelay)
	}
	if patches != 0 || statusWrites != 0 {
		t.Fatalf("patches = %d status writes = %d, want the gated reconcile to write nothing", patches, statusWrites)
	}
	var stored courier.CoderRun
	if err := c.Get(ctx, admissionKey("preflight-gated"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Status.Phase != courier.PhasePending {
		t.Fatalf("phase = %q, want Pending: the gate holds the run before it is claimed", stored.Status.Phase)
	}

	// Once the delay is up the run is admitted again.
	now = now.Add(preflightBackoffBaseDelay)
	result, err = reconciler.Reconcile(ctx, admissionRequest("preflight-gated"))
	if err != nil {
		t.Fatalf("Reconcile after the gate elapsed: %v", err)
	}
	if len(src.claimed) != 1 {
		t.Fatalf("claimed = %#v, want the run claimed once the gate elapsed", src.claimed)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want no further wait after the gate elapsed", result.RequeueAfter)
	}
	if err := c.Get(ctx, admissionKey("preflight-gated"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Status.Phase != courier.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed once the gate elapsed", stored.Status.Phase)
	}
}

// TestSecureLaunchFailureArmsPreflightBackoff pins which launch failures arm
// the gate. A permanent-class verdict that is not one of the terminal classes
// is the deployment's own misconfiguration: it must not terminalize the run,
// and it must not return the run to the tight claim/release cycle either, so
// the gate records it and the claim is still released back to Pending. An
// ordinary error is transient — it clears on a later pass — so it is not gated.
func TestSecureLaunchFailureArmsPreflightBackoff(t *testing.T) {
	ctx := context.Background()
	now := preflightBackoffNow

	run := admissionRun("preflight-verdict", "local", courier.PhaseClaimed)
	patches, statusWrites := 0, 0
	c := preflightBackoffClient(t, &patches, &statusWrites, run)
	src := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       &SecureControl{Client: c},
		Now:          func() time.Time { return now },
	}
	item, err := sourceWorkItem(run)
	if err != nil {
		t.Fatalf("sourceWorkItem: %v", err)
	}

	verdict := secureNeedsHuman("BrokerRBAC", "the broker service account lacks get on pods %s", "worker")
	if _, err := reconciler.secureLaunchFailure(ctx, run, src, item, verdict); err == nil {
		t.Fatal("secureLaunchFailure returned nil, want the launch failure surfaced")
	}
	if got := run.Annotations[preflightFailuresAnnotation]; got != "1" {
		t.Fatalf("failures = %q, want the verdict to arm the gate", got)
	}
	if got := run.Annotations[preflightFailedAtAnnotation]; got != now.UTC().Format(time.RFC3339) {
		t.Fatalf("failed-at = %q, want the arming timestamp", got)
	}
	var stored courier.CoderRun
	if err := c.Get(ctx, admissionKey("preflight-verdict"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Annotations[preflightFailuresAnnotation] != "1" || stored.Annotations[preflightFailedAtAnnotation] == "" {
		t.Fatalf("annotations = %#v, want the gate persisted on the run", stored.Annotations)
	}
	if len(src.released) != 1 {
		t.Fatalf("released = %#v, want the claim released back to Pending", src.released)
	}
	if stored.Status.Phase != courier.PhasePending {
		t.Fatalf("phase = %q, want Pending: the verdict is not a terminal one", stored.Status.Phase)
	}

	// An ordinary failure is transient and is not gated.
	plain := admissionRun("preflight-ordinary", "local", courier.PhaseClaimed)
	plainPatches, plainStatusWrites := 0, 0
	plainClient := preflightBackoffClient(t, &plainPatches, &plainStatusWrites, plain)
	plainSrc := &admissionSource{}
	plainReconciler := &CoderRunReconciler{
		Client:       plainClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": plainSrc}),
		StatusWriter: fakeStatusWriter{client: plainClient},
		Secure:       &SecureControl{Client: plainClient},
		Now:          func() time.Time { return now },
	}
	plainItem, err := sourceWorkItem(plain)
	if err != nil {
		t.Fatalf("sourceWorkItem: %v", err)
	}
	ordinary := errors.New("secure preflight: live probes did not complete in time")
	if _, err := plainReconciler.secureLaunchFailure(ctx, plain, plainSrc, plainItem, ordinary); err == nil {
		t.Fatal("secureLaunchFailure returned nil, want the launch failure surfaced")
	}
	if _, ok := plain.Annotations[preflightFailedAtAnnotation]; ok {
		t.Fatalf("annotations = %#v, want an ordinary failure left ungated", plain.Annotations)
	}
	if _, ok := plain.Annotations[preflightFailuresAnnotation]; ok {
		t.Fatalf("annotations = %#v, want an ordinary failure left ungated", plain.Annotations)
	}
	if plainPatches != 0 {
		t.Fatalf("patches = %d, want an ordinary failure to write no gate", plainPatches)
	}
}

// TestPreflightBackoffGatesClaimedResume pins the gate's other reading: a
// claimed run — one whose rollback to Pending could not be written — waits out
// the delay before the launch is retried, and the wait touches neither the
// source claim nor the run's status.
func TestPreflightBackoffGatesClaimedResume(t *testing.T) {
	ctx := context.Background()
	now := preflightBackoffNow
	run := admissionRun("preflight-gated-claimed", "local", courier.PhaseClaimed)
	run.Annotations = map[string]string{
		preflightFailedAtAnnotation: now.UTC().Format(time.RFC3339),
		preflightFailuresAnnotation: "2",
	}
	patches, statusWrites := 0, 0
	c := preflightBackoffClient(t, &patches, &statusWrites, run)
	src := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter: fakeStatusWriter{client: c},
		Now:          func() time.Time { return now },
	}

	result, err := reconciler.resumeClaimed(ctx, run)
	if err != nil {
		t.Fatalf("resumeClaimed: %v", err)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > preflightBackoffDelay(2) {
		t.Fatalf("RequeueAfter = %v, want the gate's remaining wait of at most %v", result.RequeueAfter, preflightBackoffDelay(2))
	}
	if len(src.released) != 0 || len(src.transitions) != 0 {
		t.Fatalf("released = %#v transitions = %#v, want the gate to touch neither the source claim nor its state", src.released, src.transitions)
	}
	if patches != 0 || statusWrites != 0 {
		t.Fatalf("patches = %d status writes = %d, want the gated resume to write nothing", patches, statusWrites)
	}
}

// TestSecureLaunchFailureArmsBeforeRelease pins the ordering the design names:
// the gate is armed before the claim is released, so a rollback that cannot be
// written still leaves the run waiting out a delay instead of cycling.
func TestSecureLaunchFailureArmsBeforeRelease(t *testing.T) {
	ctx := context.Background()
	now := preflightBackoffNow
	run := admissionRun("preflight-arm-order", "local", courier.PhaseClaimed)
	patches, statusWrites := 0, 0
	c := preflightBackoffClient(t, &patches, &statusWrites, run)
	src := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter: failingStatusWriter{},
		Secure:       &SecureControl{Client: c},
		Now:          func() time.Time { return now },
	}
	item, err := sourceWorkItem(run)
	if err != nil {
		t.Fatalf("sourceWorkItem: %v", err)
	}

	verdict := secureNeedsHuman("BrokerRBAC", "the broker service account lacks get on pods %s", "worker")
	if _, err := reconciler.secureLaunchFailure(ctx, run, src, item, verdict); err == nil {
		t.Fatal("secureLaunchFailure returned nil, want the launch failure surfaced")
	}
	if got := run.Annotations[preflightFailuresAnnotation]; got != "1" {
		t.Fatalf("failures = %q, want the gate armed before the rollback", got)
	}
	if len(src.released) != 1 {
		t.Fatalf("released = %#v, want the claim released even though the rollback could not be written", src.released)
	}
}

// TestPreflightBackoffArmFailureStillReleasesTheClaim pins what a marker that
// cannot be written does: the bound it wanted does not exist, so the run keeps
// the ordinary requeue — but the claim is still owed back to the source.
func TestPreflightBackoffArmFailureStillReleasesTheClaim(t *testing.T) {
	ctx := context.Background()
	now := preflightBackoffNow
	run := admissionRun("preflight-arm-fails", "local", courier.PhaseClaimed)
	c := preflightBackoffFailingClient(t, run)
	src := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       &SecureControl{Client: c},
		Now:          func() time.Time { return now },
	}
	item, err := sourceWorkItem(run)
	if err != nil {
		t.Fatalf("sourceWorkItem: %v", err)
	}

	verdict := secureNeedsHuman("BrokerRBAC", "the broker service account lacks get on pods %s", "worker")
	if _, err := reconciler.secureLaunchFailure(ctx, run, src, item, verdict); err == nil {
		t.Fatal("secureLaunchFailure returned nil, want the launch failure surfaced")
	}
	var stored courier.CoderRun
	if err := c.Get(ctx, admissionKey("preflight-arm-fails"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if _, ok := stored.Annotations[preflightFailedAtAnnotation]; ok {
		t.Fatalf("annotations = %#v, want no gate when the marker could not be written", stored.Annotations)
	}
	if len(src.released) != 1 {
		t.Fatalf("released = %#v, want the claim released even though the gate could not be written", src.released)
	}
}

// TestClearPreflightBackoffAfterLaunchIsBestEffort pins the clear's contract:
// a launch that passed the preflight is not failed over a marker that cannot
// be written. A stale marker on a run past the preflight is inert, while
// aborting the launch would strand that run in Claimed, holding its lane slot
// and its source claim.
func TestClearPreflightBackoffAfterLaunchIsBestEffort(t *testing.T) {
	ctx := context.Background()
	now := preflightBackoffNow
	run := admissionRun("preflight-clear-fails", "local", courier.PhaseRunning)
	run.Annotations = map[string]string{
		preflightFailedAtAnnotation: now.UTC().Format(time.RFC3339),
		preflightFailuresAnnotation: "3",
	}
	c := preflightBackoffFailingClient(t, run)
	reconciler := &CoderRunReconciler{Client: c, Now: func() time.Time { return now }}

	reconciler.clearPreflightBackoffAfterLaunch(ctx, run)

	var stored courier.CoderRun
	if err := c.Get(ctx, admissionKey("preflight-clear-fails"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got := stored.Annotations[preflightFailuresAnnotation]; got != "3" {
		t.Fatalf("failures = %q, want the stale marker left on the run rather than the launch failing", got)
	}
}
