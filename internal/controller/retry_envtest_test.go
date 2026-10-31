package controller

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/manual"
	"github.com/misospace/courier/internal/status"
)

// setupRetryNamespace creates an isolated namespace for a retry envtest, so the
// active-owner guard (a legitimate world check) sees only the run under test and
// no same-identity run another envtest left in the shared default namespace.
func setupRetryNamespace(t *testing.T, namespace string) {
	t.Helper()
	if err := envtestClient.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace: %v", err)
	}
}

// createRetryTerminalRun creates a run in its own namespace and terminalizes it
// through the real API server with its source lifecycle report settled and the
// retry annotation set: an ordinary retained terminal run an operator has asked
// to retry.
func createRetryTerminalRun(t *testing.T, name, namespace string, phase courierv1alpha1.Phase, requestID string, mode courierv1alpha1.Mode) {
	t.Helper()
	ctx := context.Background()
	setupRetryNamespace(t, namespace)
	run := envtestRunInNamespace(name, "retry-lane", namespace)
	run.Spec.Mode = mode
	if requestID != "" {
		run.Annotations = map[string]string{courierv1alpha1.RetryAnnotation: requestID}
	}
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	var cur courierv1alpha1.CoderRun
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := envtestClient.Get(ctx, key, &cur); err != nil {
		t.Fatalf("get run: %v", err)
	}
	cur.Status.Phase = phase
	cur.Status.FinishedAt = &metav1.Time{Time: reapClock.Add(-48 * time.Hour)}
	cur.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{Category: courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash}
	cur.Status.Conditions = []metav1.Condition{{
		Type:               lifecycleReportedCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "Published",
		LastTransitionTime: metav1.NewTime(reapClock),
	}}
	if err := envtestClient.Status().Update(ctx, &cur); err != nil {
		t.Fatalf("terminalize run: %v", err)
	}
}

func getRetryEnvtestRun(t *testing.T, name, namespace string) *courierv1alpha1.CoderRun {
	t.Helper()
	var run courierv1alpha1.CoderRun
	if err := envtestClient.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	return &run
}

func retryEnvtestRequest(name, namespace string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}}
}

func newEnvtestRetryReconciler(src source.Adapter, reg *prometheus.Registry, events *bytes.Buffer) *CoderRunReconciler {
	r := &CoderRunReconciler{
		Client:              envtestClient,
		Sources:             NewSourceRegistry(map[string]source.Adapter{"manual": src}),
		StatusWriter:        status.KubePatchWriter{Client: envtestClient},
		FailedRetention:     168 * time.Hour,
		NeedsHumanRetention: 168 * time.Hour,
		TerminalAuditKeep:   -1,
		Now:                 func() time.Time { return reapClock },
	}
	if reg != nil {
		r.Metrics = NewRunRecorder(reg)
	}
	if events != nil {
		r.Events = courierlog.NewEmitter(events, courierlog.LevelInfo)
	}
	return r
}

// TestEnvtestRetryRoundTripsThroughCRD proves the durable record persists through
// the generated CRD (the enum state, the nullable settledAt, and the reason a
// replacement must clear) and that a successful requeue settles Requeued without
// disturbing the predecessor's terminal state.
func TestEnvtestRetryRoundTripsThroughCRD(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-roundtrip", "retry-roundtrip"
	ctx := context.Background()
	createRetryTerminalRun(t, name, namespace, courierv1alpha1.PhaseFailed, "req-1", courierv1alpha1.ModeResolveIssue)
	before := getRetryEnvtestRun(t, name, namespace)
	uid := string(before.UID)

	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-7"}}
	buf := &bytes.Buffer{}
	reg := prometheus.NewRegistry()

	if _, err := newEnvtestRetryReconciler(src, reg, buf).Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	after := getRetryEnvtestRun(t, name, namespace)
	rec := after.Status.RetryRequest
	if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRequeued || rec.ID != "req-1" || rec.AttemptID != "gen-7" || rec.SettledAt == nil {
		t.Fatalf("retryRequest = %+v, want Requeued id=req-1 attemptID=gen-7 settledAt set through the CRD", rec)
	}
	wantKey := "coderun/" + namespace + "/" + name + "/" + retryKeyDigest("req-1")
	if uid != "" {
		wantKey += "/" + uid
	}
	if len(src.requests) != 1 || src.requests[0].IdempotencyKey != wantKey {
		t.Fatalf("requeue requests = %+v, want key %q", src.requests, wantKey)
	}
	if after.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed preserved", after.Status.Phase)
	}
	if after.Status.FinishedAt == nil || !after.Status.FinishedAt.Time.Equal(before.Status.FinishedAt.Time) {
		t.Fatalf("finishedAt changed across a retry: %v vs %v", after.Status.FinishedAt, before.Status.FinishedAt)
	}
	if after.Status.TerminalDiagnostic == nil || after.Status.TerminalDiagnostic.Category != before.Status.TerminalDiagnostic.Category {
		t.Fatalf("terminal diagnostic changed across a retry: %+v", after.Status.TerminalDiagnostic)
	}
	if got := gatherMetric(t, reg, "courier_coderun_retry_requests_total", map[string]string{"lane": "retry-lane", "mode": "resolve-issue", "result": "requeued"}); got.GetCounter().GetValue() != 1 {
		t.Fatalf("requeued counter = %v, want 1", got.GetCounter().GetValue())
	}
	if len(parseRetryEvents(t, buf)) != 2 {
		t.Fatalf("want a Requested and a Requeued event, got %+v", parseRetryEvents(t, buf))
	}
}

// TestEnvtestRetryReasonVocabularyEnforcedByCRD proves the CRD pattern on
// status.retryRequest.reason admits only the empty value and the closed
// RetryRefusal* vocabulary: the API server itself rejects arbitrary reason
// text, so a hostile adapter or writer cannot persist free text into the record.
func TestEnvtestRetryReasonVocabularyEnforcedByCRD(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-reason-vocab", "retry-reason-vocab"
	ctx := context.Background()
	setupRetryNamespace(t, namespace)
	run := envtestRunInNamespace(name, "retry-lane", namespace)
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	setReason := func(reason string) error {
		var cur courierv1alpha1.CoderRun
		if err := envtestClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cur); err != nil {
			t.Fatalf("get run: %v", err)
		}
		cur.Status.RetryRequest = &courierv1alpha1.RetryRequest{
			ID:          "req-1",
			RequestedAt: metav1.NewTime(reapClock),
			State:       courierv1alpha1.RetryRequestStateRefused,
			Reason:      reason,
		}
		return envtestClient.Status().Update(ctx, &cur)
	}

	if err := setReason("hostile free text"); !apierrors.IsInvalid(err) {
		t.Fatalf("arbitrary reason err = %v, want Invalid rejected by the CRD", err)
	}
	if err := setReason(courierv1alpha1.RetryRefusalSourceRefused); err != nil {
		t.Fatalf("closed-vocabulary reason rejected: %v", err)
	}
}

// TestEnvtestRetryAttemptIDBoundEnforcedByCRD proves the CRD maxLength on
// status.retryRequest.attemptID is exactly RetryRequestMaxAttemptIDLength
// code points: the API server rejects a value one code point over and accepts
// one at the bound, so the Go constant the controller normalizes against and
// the generated schema cannot silently drift apart.
func TestEnvtestRetryAttemptIDBoundEnforcedByCRD(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-attempt-bound", "retry-attempt-bound"
	ctx := context.Background()
	setupRetryNamespace(t, namespace)
	run := envtestRunInNamespace(name, "retry-lane", namespace)
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	setAttempt := func(attemptID string) error {
		var cur courierv1alpha1.CoderRun
		if err := envtestClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &cur); err != nil {
			t.Fatalf("get run: %v", err)
		}
		cur.Status.RetryRequest = &courierv1alpha1.RetryRequest{
			ID:          "req-1",
			RequestedAt: metav1.NewTime(reapClock),
			State:       courierv1alpha1.RetryRequestStateRequeued,
			AttemptID:   attemptID,
		}
		return envtestClient.Status().Update(ctx, &cur)
	}

	if err := setAttempt(strings.Repeat("é", courierv1alpha1.RetryRequestMaxAttemptIDLength+1)); !apierrors.IsInvalid(err) {
		t.Fatalf("oversized attemptID err = %v, want Invalid rejected by the CRD", err)
	}
	if err := setAttempt(strings.Repeat("é", courierv1alpha1.RetryRequestMaxAttemptIDLength)); err != nil {
		t.Fatalf("attemptID at the bound rejected: %v", err)
	}
}

// retrySelectiveStatusWriter fails selected status writes (1-indexed), simulating
// a lost response so the crash-recovery replay path is exercised against the real
// API server and the fenced adapter.
type retrySelectiveStatusWriter struct {
	inner  status.PatchWriter
	failOn map[int]bool
	calls  int
}

func (w *retrySelectiveStatusWriter) PatchStatus(ctx context.Context, name types.NamespacedName, patch []byte) error {
	w.calls++
	if w.failOn[w.calls] {
		return errors.New("simulated lost status write")
	}
	return w.inner.PatchStatus(ctx, name, patch)
}

// TestEnvtestRetryPartialFailureReplaysAndSettles proves the bounded partial-
// failure contract: the Requested record is persisted before the source call; the
// source commits the requeue but the settle status write is lost; a later
// reconcile replays the same key and settles Requeued without a second requeue.
func TestEnvtestRetryPartialFailureReplaysAndSettles(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-partial", "retry-partial"
	ctx := context.Background()
	createRetryTerminalRun(t, name, namespace, courierv1alpha1.PhaseNeedsHuman, "req-1", courierv1alpha1.ModeResolveIssue)

	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-4"}}
	reconciler := newEnvtestRetryReconciler(src, nil, nil)
	reconciler.StatusWriter = &retrySelectiveStatusWriter{
		inner:  status.KubePatchWriter{Client: envtestClient},
		failOn: map[int]bool{2: true}, // fail the settle write; let the Requested write land
	}

	// First reconcile: Requested persisted before the source call, the source
	// requeued, and the settle status write lost — surfaced as a reconcile error
	// that re-arms the workqueue, while the durable Requested record stands.
	if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err == nil {
		t.Fatalf("first Reconcile: want the lost settle write surfaced as an error")
	}
	mid := getRetryEnvtestRun(t, name, namespace)
	if mid.Status.RetryRequest == nil || mid.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequested {
		t.Fatalf("retryRequest = %+v, want Requested after the lost settle write", mid.Status.RetryRequest)
	}
	if src.calls != 1 {
		t.Fatalf("requeue calls = %d, want 1 after the first reconcile", src.calls)
	}

	// Second reconcile (controller-restart equivalent): replay the same key and settle.
	if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	after := getRetryEnvtestRun(t, name, namespace)
	if after.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequeued || after.Status.RetryRequest.AttemptID != "gen-4" {
		t.Fatalf("retryRequest = %+v, want Requeued after the replay", after.Status.RetryRequest)
	}
	if len(src.requests) != 2 || src.requests[0].IdempotencyKey != src.requests[1].IdempotencyKey {
		t.Fatalf("requeue requests = %+v, want both keyed identically for source-side dedupe", src.requests)
	}
}

// TestEnvtestRetryLostResponseReplaySettlesRequeued proves the lost-response
// contract against the real API server: the source commits the requeue and
// loses the response, so the durable record stays Requested; the same key is
// re-presented and the source replays its committed success, which settles
// Requeued through the CRD — never Moot, which would leave the predecessor open
// to a later request instead of recording the replacement that launched.
func TestEnvtestRetryLostResponseReplaySettlesRequeued(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-lost-response", "retry-lost-response"
	ctx := context.Background()
	createRetryTerminalRun(t, name, namespace, courierv1alpha1.PhaseFailed, "req-1", courierv1alpha1.ModeResolveIssue)

	src := &lostResponseSource{admissionSource: &admissionSource{}, committed: map[string]source.AttemptRequeueResult{}}
	reconciler := newEnvtestRetryReconciler(src, nil, nil)

	// First reconcile: Requested is persisted before the source call, the source
	// commits, and the response is lost — so nothing about the requeue is proven
	// and the durable record stands.
	if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	held := getRetryEnvtestRun(t, name, namespace)
	if held.Status.RetryRequest == nil || held.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequested {
		t.Fatalf("retryRequest = %+v, want Requested after the lost source response", held.Status.RetryRequest)
	}

	// Second reconcile: the same key is re-presented and the committed success
	// replays, settling Requeued.
	if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	settled := getRetryEnvtestRun(t, name, namespace)
	rec := settled.Status.RetryRequest
	if rec.State != courierv1alpha1.RetryRequestStateRequeued || rec.AttemptID != "gen-1" || rec.SettledAt == nil {
		t.Fatalf("retryRequest = %+v, want Requeued with the replayed committed identity through the CRD", rec)
	}
	if src.commits != 1 {
		t.Fatalf("source commits = %d, want exactly one replacement launched", src.commits)
	}
	if len(src.requests) != 2 || src.requests[0].IdempotencyKey != src.requests[1].IdempotencyKey {
		t.Fatalf("requeue requests = %+v, want both keyed identically for the replay", src.requests)
	}
}

// TestEnvtestRetryRetentionGate proves a run carrying a Requested retry record is
// not reap-eligible and its retention window does not start; once the record
// settles, the ordinary Failed window begins.
func TestEnvtestRetryRetentionGate(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-retention", "retry-retention"
	ctx := context.Background()
	createRetryTerminalRun(t, name, namespace, courierv1alpha1.PhaseFailed, "req-1", courierv1alpha1.ModeResolveIssue)

	src := &requeueSource{admissionSource: &admissionSource{}, err: errors.New("source unavailable")}
	reconciler := newEnvtestRetryReconciler(src, nil, nil)
	result, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != retrySettlementRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want the retry cadence while Requested", result.RequeueAfter)
	}
	held := getRetryEnvtestRun(t, name, namespace)
	if held.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequested {
		t.Fatalf("retryRequest = %+v, want Requested", held.Status.RetryRequest)
	}
	if _, stamped := held.Annotations[failedAtAnnotation]; stamped {
		t.Fatalf("failed-at retention marker stamped while a Requested retry is outstanding")
	}

	// Once the source recovers and the record settles, the window starts.
	src.err = nil
	src.result = source.AttemptRequeueResult{AttemptID: "gen-5"}
	if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
		t.Fatalf("recovering Reconcile: %v", err)
	}
	settled := getRetryEnvtestRun(t, name, namespace)
	if settled.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequeued {
		t.Fatalf("retryRequest = %+v, want Requeued after the source recovered", settled.Status.RetryRequest)
	}
	if _, stamped := settled.Annotations[failedAtAnnotation]; !stamped {
		t.Fatalf("failed-at retention marker not stamped after the record settled")
	}
}

func listRetryNamespaceRuns(t *testing.T, namespace string) []courierv1alpha1.CoderRun {
	t.Helper()
	var runs courierv1alpha1.CoderRunList
	if err := envtestClient.List(context.Background(), &runs, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list runs in %q: %v", namespace, err)
	}
	return runs.Items
}

func retryNamespaceRunNames(runs []courierv1alpha1.CoderRun) []string {
	names := make([]string, 0, len(runs))
	for i := range runs {
		names = append(names, runs[i].Name)
	}
	return names
}

// TestEnvtestManualRetryMaterializesOneSuccessor proves the #609 manual-source
// exception end to end against the real API server and the real manual
// adapter: the requeue mints the fresh identity in the manual namespace, the
// reconcile that settles Requeued creates exactly one successor run named from
// that identity and the predecessor's UID with the copied immutable spec, and
// repeated reconciles after the successful create never duplicate it.
func TestEnvtestManualRetryMaterializesOneSuccessor(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-manual-successor", "retry-manual-successor"
	ctx := context.Background()
	createRetryTerminalRun(t, name, namespace, courierv1alpha1.PhaseFailed, "req-1", courierv1alpha1.ModeResolveIssue)
	predecessor := getRetryEnvtestRun(t, name, namespace)

	buf := &bytes.Buffer{}
	reconciler := newEnvtestRetryReconciler(manual.New(nil), nil, buf)
	if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	settled := getRetryEnvtestRun(t, name, namespace)
	freshIdentity := name + "/retry/req-1"
	rec := settled.Status.RetryRequest
	if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRequeued || rec.AttemptID != freshIdentity {
		t.Fatalf("retryRequest = %+v, want Requeued with the minted identity %q", rec, freshIdentity)
	}
	wantName := source.SuccessorRunName("manual", freshIdentity, predecessor.UID)
	runs := listRetryNamespaceRuns(t, namespace)
	if len(runs) != 2 {
		t.Fatalf("runs = %d, want exactly predecessor + successor: %v", len(runs), retryNamespaceRunNames(runs))
	}
	var successor *courierv1alpha1.CoderRun
	for i := range runs {
		if runs[i].Name == wantName {
			successor = &runs[i]
		}
	}
	if successor == nil {
		t.Fatalf("successor %q not materialized: %v", wantName, retryNamespaceRunNames(runs))
	}
	if successor.Spec.WorkItemID != freshIdentity || successor.Spec.Source != "manual" ||
		successor.Spec.Mode != courierv1alpha1.ModeResolveIssue || successor.Spec.Repo != "acme/widgets" ||
		successor.Spec.Ref != 14 || successor.Spec.Lane != "retry-lane" {
		t.Fatalf("successor spec = %+v, want the copied work identity with the minted WorkItemID", successor.Spec)
	}
	if successor.Spec.AssignmentKind != "" || successor.Spec.AssignmentReasons != nil {
		t.Fatalf("successor assignment = %q/%v, want empty; no source re-issued this attempt", successor.Spec.AssignmentKind, successor.Spec.AssignmentReasons)
	}
	if len(successor.OwnerReferences) != 0 {
		t.Fatalf("successor owner references = %+v, want none: reaping the predecessor must not cascade onto the replacement", successor.OwnerReferences)
	}

	// Repeated reconciles after the successful create change nothing: the
	// settled record closes the predecessor's retry surface, so the source is
	// not re-called, no second successor appears, and the same two retry
	// events are the whole story — with the successor name on the Requeued
	// one.
	for i := 0; i < 2; i++ {
		if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
			t.Fatalf("reconcile %d: %v", i+2, err)
		}
	}
	if runs := listRetryNamespaceRuns(t, namespace); len(runs) != 2 {
		t.Fatalf("runs after repeats = %d, want still exactly two: %v", len(runs), retryNamespaceRunNames(runs))
	}
	after := getRetryEnvtestRun(t, name, namespace)
	if after.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequeued ||
		!after.Status.RetryRequest.SettledAt.Equal(rec.SettledAt) {
		t.Fatalf("retryRequest after repeats = %+v, want the settled record untouched", after.Status.RetryRequest)
	}
	events := parseRetryEvents(t, buf)
	if len(events) != 2 || events[1]["status"] != "Requeued" {
		t.Fatalf("run.retry events = %+v, want Requested then Requeued only", events)
	}
	if got, _ := events[1]["detail"].(map[string]any)["successor_run"].(string); got != wantName {
		t.Fatalf("requeued event successor_run = %q, want %q", got, wantName)
	}
}

// TestEnvtestManualRetryWithoutRequeueCapabilityRefuses pins the regression
// gate: a manual run whose registered adapter has lost the requeue capability
// refuses unsupported-source — permanently and durably — instead of silently
// doing nothing or letting the core imitate the requeue with ordinary calls.
func TestEnvtestManualRetryWithoutRequeueCapabilityRefuses(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const name, namespace = "retry-manual-nocap", "retry-manual-nocap"
	ctx := context.Background()
	createRetryTerminalRun(t, name, namespace, courierv1alpha1.PhaseFailed, "req-1", courierv1alpha1.ModeResolveIssue)

	reconciler := newEnvtestRetryReconciler(&admissionSource{}, nil, nil)
	if _, err := reconciler.Reconcile(ctx, retryEnvtestRequest(name, namespace)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	after := getRetryEnvtestRun(t, name, namespace)
	rec := after.Status.RetryRequest
	if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRefused || rec.Reason != courierv1alpha1.RetryRefusalUnsupportedSource {
		t.Fatalf("retryRequest = %+v, want Refused unsupported-source", rec)
	}
	if runs := listRetryNamespaceRuns(t, namespace); len(runs) != 1 {
		t.Fatalf("runs = %d, want no successor without a committed requeue: %v", len(runs), retryNamespaceRunNames(runs))
	}
}
