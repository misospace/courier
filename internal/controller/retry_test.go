package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/forge"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
)

// requeueSource extends the base admission fake with the optional fenced requeue
// capability, capturing every request so a test can assert the derived
// idempotency key and fence, and answer with a success, a transient error, or a
// classified refusal.
type requeueSource struct {
	*admissionSource
	requests []source.AttemptRequeueRequest
	calls    int
	result   source.AttemptRequeueResult
	err      error                     // transient failure when set
	refusal  *source.RetryRefusalError // classified Moot/Refused when set
}

func (s *requeueSource) RequeueAttempt(_ context.Context, _ source.WorkItem, req source.AttemptRequeueRequest) (source.AttemptRequeueResult, error) {
	s.calls++
	s.requests = append(s.requests, req)
	if s.refusal != nil {
		return source.AttemptRequeueResult{}, s.refusal
	}
	if s.err != nil {
		return source.AttemptRequeueResult{}, s.err
	}
	return s.result, nil
}

// lostResponseSource commits the requeue under the idempotency key and then
// loses the response: the first call for a key commits and fails as a transient
// error, and every later call with the same key replays the committed result —
// the behavior the source contract requires of a committed key.
type lostResponseSource struct {
	*admissionSource
	requests  []source.AttemptRequeueRequest
	committed map[string]source.AttemptRequeueResult
	commits   int
}

func (s *lostResponseSource) RequeueAttempt(_ context.Context, _ source.WorkItem, req source.AttemptRequeueRequest) (source.AttemptRequeueResult, error) {
	s.requests = append(s.requests, req)
	if res, ok := s.committed[req.IdempotencyKey]; ok {
		return res, nil
	}
	s.commits++
	s.committed[req.IdempotencyKey] = source.AttemptRequeueResult{AttemptID: fmt.Sprintf("gen-%d", s.commits)}
	return source.AttemptRequeueResult{}, errors.New("source response lost after the requeue committed")
}

// retryRun returns a long-lived terminal run in the given phase with its source
// lifecycle report positively settled (the retry-eligibility gate), carrying the
// retry annotation. An empty requestID omits the annotation.
func retryRun(name string, phase courierv1alpha1.Phase, requestID string) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "local", phase)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-72 * time.Hour))
	run.Status.FinishedAt = &metav1.Time{Time: reapClock.Add(-48 * time.Hour)}
	withLifecycleSettled(run)
	if requestID != "" {
		run.Annotations = map[string]string{courierv1alpha1.RetryAnnotation: requestID}
	}
	return run
}

func retryReconciler(c client.Client, src source.Adapter, now time.Time, reg *prometheus.Registry, events *bytes.Buffer) *CoderRunReconciler {
	r := &CoderRunReconciler{
		Client:              c,
		Sources:             NewSourceRegistry(map[string]source.Adapter{"test": src}),
		StatusWriter:        fakeStatusWriter{client: c},
		DoneRetention:       10 * time.Minute,
		FailedRetention:     10 * time.Minute,
		NeedsHumanRetention: 10 * time.Minute,
		TerminalAuditKeep:   -1,
		Now:                 func() time.Time { return now },
	}
	if reg != nil {
		r.Metrics = NewRunRecorder(reg)
	}
	if events != nil {
		r.Events = courierlog.NewEmitter(events, courierlog.LevelInfo)
	}
	return r
}

func getRetryRun(t *testing.T, c client.Client, name string) *courierv1alpha1.CoderRun {
	t.Helper()
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey(name), &stored); err != nil {
		t.Fatalf("get run %q: %v", name, err)
	}
	return &stored
}

func retryCondition(t *testing.T, run *courierv1alpha1.CoderRun) *metav1.Condition {
	t.Helper()
	cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionRetryRequest)
	if cond == nil {
		t.Fatalf("run %q has no RetryRequest condition", run.Name)
	}
	return cond
}

func parseRetryEvents(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("parse event line %q: %v", line, err)
		}
		if ev["event"] == courierlog.EventRunRetry {
			out = append(out, ev)
		}
	}
	return out
}

func eventDetailString(ev map[string]any, key string) string {
	if detail, ok := ev["detail"].(map[string]any); ok {
		if v, ok := detail[key].(string); ok {
			return v
		}
	}
	return ""
}

func TestRetryResolveIssueRequeues(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-2"}}
	buf := &bytes.Buffer{}
	reg := prometheus.NewRegistry()
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, reg, buf).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed; a retry never phase-flips the predecessor", stored.Status.Phase)
	}
	rec := stored.Status.RetryRequest
	if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRequeued || rec.ID != "req-1" || rec.AttemptID != "gen-2" {
		t.Fatalf("retryRequest = %+v, want Requeued id=req-1 attemptID=gen-2", rec)
	}
	if rec.SettledAt == nil {
		t.Fatalf("settledAt = nil, want set on a settled Requeued record")
	}
	if cond := retryCondition(t, stored); cond.Status != metav1.ConditionTrue || cond.Reason != courierv1alpha1.ConditionReasonRetryRequeued {
		t.Fatalf("condition = %s/%s, want True/Requeued", cond.Status, cond.Reason)
	}
	// Persist-before-dispatch: the adapter saw the run's derived idempotency key.
	wantKey := "coderun/" + admissionNamespace + "/rr/" + retryKeyDigest("req-1")
	if len(src.requests) != 1 || src.requests[0].IdempotencyKey != wantKey {
		t.Fatalf("requeue requests = %+v, want one with the derived key %s", src.requests, wantKey)
	}
	if got := gatherMetric(t, reg, "courier_coderun_retry_requests_total", map[string]string{"lane": "local", "mode": "resolve-issue", "result": "requeued"}); got.GetCounter().GetValue() != 1 {
		t.Fatalf("requeued counter = %v, want 1", got.GetCounter().GetValue())
	}
	// Two events: the Requested transition, then the Requeued settlement.
	events := parseRetryEvents(t, buf)
	if len(events) != 2 || events[0]["status"] != "Requested" || events[1]["status"] != "Requeued" {
		t.Fatalf("run.retry events = %+v, want Requested then Requeued", events)
	}
	if eventDetailString(events[1], "request_id") != "req-1" || eventDetailString(events[1], "attempt_id") != "gen-2" {
		t.Fatalf("requeued event detail = %+v, want request_id=req-1 attempt_id=gen-2", events[1]["detail"])
	}
}

func TestRetryAttemptIDOverCRDBoundDropped(t *testing.T) {
	// The requeue has already committed when the identity arrives, so an
	// adapter-supplied identity over the CRD's bound must be dropped — not
	// allowed to fail the settle write and wedge the durable record Requested
	// forever. The multibyte string pins code-point (not byte) semantics.
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: strings.Repeat("é", courierv1alpha1.RetryRequestMaxAttemptIDLength+1)}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	rec := stored.Status.RetryRequest
	if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRequeued || rec.ID != "req-1" {
		t.Fatalf("retryRequest = %+v, want the requeue still settled Requeued", rec)
	}
	if rec.AttemptID != "" {
		t.Fatalf("attemptID = %q, want dropped empty; an oversized identity must not wedge settlement", rec.AttemptID)
	}
	if rec.SettledAt == nil {
		t.Fatalf("settledAt = nil, want set on a settled Requeued record")
	}
}

func TestRetryAttemptIDAtCRDBoundKept(t *testing.T) {
	attemptID := strings.Repeat("é", courierv1alpha1.RetryRequestMaxAttemptIDLength)
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: attemptID}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rec := getRetryRun(t, c, "rr").Status.RetryRequest; rec == nil || rec.AttemptID != attemptID {
		t.Fatalf("retryRequest = %+v, want the identity at the bound kept verbatim", rec)
	}
}

func TestRetryAdapterTransientKeepsRequested(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, err: errors.New("upstream unavailable")}
	reg := prometheus.NewRegistry()
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	result, err := retryReconciler(c, src, reapClock, reg, nil).Reconcile(context.Background(), admissionRequest("rr"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != retrySettlementRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want the settlement retry cadence", result.RequeueAfter)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Status.RetryRequest == nil || stored.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequested {
		t.Fatalf("retryRequest = %+v, want it to stay Requested on a transient failure", stored.Status.RetryRequest)
	}
	// A Requested record holds reap: no retention window marker is stamped.
	if _, ok := stored.Annotations[failedAtAnnotation]; ok {
		t.Fatalf("failed-at marker stamped while a Requested retry is outstanding")
	}
	if got := gatherMetric(t, reg, "courier_coderun_retry_requests_total", map[string]string{"lane": "local", "mode": "resolve-issue", "result": "requested"}); got.GetCounter().GetValue() != 1 {
		t.Fatalf("requested counter = %v, want 1", got.GetCounter().GetValue())
	}
}

func TestRetryReplaySettlesAlreadyRequested(t *testing.T) {
	// Crash recovery: the Requested record is already durable (the settle write
	// was lost), and a later reconcile replays the same key and settles.
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-9"}}
	run := retryRun("rr", courierv1alpha1.PhaseNeedsHuman, "req-old")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID:          "req-old",
		RequestedAt: metav1.NewTime(reapClock.Add(-time.Hour)),
		State:       courierv1alpha1.RetryRequestStateRequested,
	}
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequeued || stored.Status.RetryRequest.AttemptID != "gen-9" {
		t.Fatalf("retryRequest = %+v, want Requeued via same-key replay", stored.Status.RetryRequest)
	}
	if len(src.requests) != 1 || src.requests[0].IdempotencyKey != "coderun/"+admissionNamespace+"/rr/"+retryKeyDigest("req-old") {
		t.Fatalf("requeue requests = %+v, want the replayed original key", src.requests)
	}
}

func TestRetryMootRefusalSettlesMoot(t *testing.T) {
	// A Moot refusal asserts that the work already moved on without this
	// request's key — it was requeued or superseded by another request or path.
	// It is not the lost-response outcome: a key this request committed replays
	// its committed success and settles Requeued (see
	// TestRetryLostResponseReplaySettlesRequeued), which seals the
	// predecessor's retry surface for good, while a settled Moot leaves it open
	// to a later request.
	src := &requeueSource{admissionSource: &admissionSource{}, refusal: &source.RetryRefusalError{Moot: true}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-old")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{ID: "req-old", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRequested}
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	rec := stored.Status.RetryRequest
	if rec.State != courierv1alpha1.RetryRequestStateMoot || rec.Reason != courierv1alpha1.RetryRefusalAlreadyRequeued {
		t.Fatalf("retryRequest = %+v, want Moot/already-requeued", rec)
	}
	if cond := retryCondition(t, stored); cond.Status != metav1.ConditionTrue || cond.Reason != courierv1alpha1.ConditionReasonRetryMoot {
		t.Fatalf("condition = %s/%s, want True/Moot", cond.Status, cond.Reason)
	}
}

// TestRetryLostResponseReplaySettlesRequeued pins the lost-response contract:
// the source commits the requeue and loses the response, so the durable
// Requested record stands; the same key is re-presented and the source replays
// its committed success, which settles Requeued and seals the predecessor's
// retry surface. A Moot settlement would leave the predecessor open to a later
// request instead of recording the replacement that actually launched, so the
// replay must never be reported as moot.
func TestRetryLostResponseReplaySettlesRequeued(t *testing.T) {
	src := &lostResponseSource{admissionSource: &admissionSource{}, committed: map[string]source.AttemptRequeueResult{}}
	reg := prometheus.NewRegistry()
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)
	r := retryReconciler(c, src, reapClock, reg, nil)

	// First reconcile: Requested persisted, the source commits, the response is
	// lost. Nothing about the requeue is proven, so the record stands Requested.
	result, err := r.Reconcile(context.Background(), admissionRequest("rr"))
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if result.RequeueAfter != retrySettlementRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want the settlement retry cadence while Requested", result.RequeueAfter)
	}
	held := getRetryRun(t, c, "rr")
	if held.Status.RetryRequest == nil || held.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequested {
		t.Fatalf("retryRequest = %+v, want it to stay Requested after the lost response", held.Status.RetryRequest)
	}

	// Second reconcile: the same key is re-presented and the source replays the
	// committed success.
	if _, err := r.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	settled := getRetryRun(t, c, "rr")
	rec := settled.Status.RetryRequest
	if rec.State != courierv1alpha1.RetryRequestStateRequeued {
		t.Fatalf("retryRequest = %+v, want Requeued: a committed key replays its committed success, never a moot refusal", rec)
	}
	if rec.AttemptID != "gen-1" {
		t.Fatalf("attemptID = %q, want the replayed committed identity", rec.AttemptID)
	}
	if rec.Reason != "" {
		t.Fatalf("reason = %q, want empty on a Requeued success", rec.Reason)
	}
	if cond := retryCondition(t, settled); cond.Status != metav1.ConditionTrue || cond.Reason != courierv1alpha1.ConditionReasonRetryRequeued {
		t.Fatalf("condition = %s/%s, want True/Requeued", cond.Status, cond.Reason)
	}
	if src.commits != 1 {
		t.Fatalf("source commits = %d, want exactly one replacement launched", src.commits)
	}
	if len(src.requests) != 2 || src.requests[0].IdempotencyKey != src.requests[1].IdempotencyKey {
		t.Fatalf("requeue requests = %+v, want both keyed identically for the replay", src.requests)
	}
	if got := gatherMetric(t, reg, "courier_coderun_retry_requests_total", map[string]string{"lane": "local", "mode": "resolve-issue", "result": "requeued"}); got.GetCounter().GetValue() != 1 {
		t.Fatalf("requeued counter = %v, want 1", got.GetCounter().GetValue())
	}
}

func TestRetryAdapterRefusalStaleGeneration(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, refusal: &source.RetryRefusalError{Reason: courierv1alpha1.RetryRefusalStaleGeneration}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRefused || stored.Status.RetryRequest.Reason != courierv1alpha1.RetryRefusalStaleGeneration {
		t.Fatalf("retryRequest = %+v, want Refused/stale-generation", stored.Status.RetryRequest)
	}
	if cond := retryCondition(t, stored); cond.Message != courierv1alpha1.RetryRefusalStaleGeneration {
		t.Fatalf("condition message = %q, want stale-generation", cond.Message)
	}
}

func TestRetryUnknownAdapterReasonNormalizes(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, refusal: &source.RetryRefusalError{Reason: "nonsense-reason"}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Status.RetryRequest.Reason != courierv1alpha1.RetryRefusalSourceRefused {
		t.Fatalf("reason = %q, want normalization to source-refused", stored.Status.RetryRequest.Reason)
	}
}

func TestRetryMootAdapterReasonNormalizes(t *testing.T) {
	// A hostile or buggy adapter returns a Moot refusal carrying free text longer
	// than the CRD bound. The record must normalize to the single closed moot
	// reason, so it neither mints free text nor fails the settle write and wedges
	// the durable record Requested forever under the reap gate.
	long := strings.Repeat("x", 200)
	src := &requeueSource{admissionSource: &admissionSource{}, refusal: &source.RetryRefusalError{Moot: true, Reason: long}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec := getRetryRun(t, c, "rr").Status.RetryRequest
	if rec.State != courierv1alpha1.RetryRequestStateMoot || rec.Reason != courierv1alpha1.RetryRefusalAlreadyRequeued {
		t.Fatalf("retryRequest = %+v, want Moot with reason normalized to already-requeued", rec)
	}
}

func TestRetrySameValueRequeuedIsNoOp(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRequeued,
		AttemptID: "gen-1", SettledAt: &metav1.Time{Time: reapClock},
	}
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if src.calls != 0 {
		t.Fatalf("RequeueAttempt called %d times, want 0 for a re-delivered settled request", src.calls)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequeued || stored.Status.RetryRequest.AttemptID != "gen-1" {
		t.Fatalf("retryRequest = %+v, want the settled Requeued record preserved untouched", stored.Status.RetryRequest)
	}
}

func TestRetryAlreadyRequeuedRecordedInEventsOnly(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}}
	buf := &bytes.Buffer{}
	reg := prometheus.NewRegistry()
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-2")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRequeued,
		AttemptID: "gen-1", SettledAt: &metav1.Time{Time: reapClock},
	}
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, reg, buf).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	// The settled Requeued record is immutable: still id=req-1, attempt gen-1, not r2.
	if stored.Status.RetryRequest.ID != "req-1" || stored.Status.RetryRequest.State != courierv1alpha1.RetryRequestStateRequeued || stored.Status.RetryRequest.AttemptID != "gen-1" {
		t.Fatalf("retryRequest = %+v, want the Requeued record left untouched", stored.Status.RetryRequest)
	}
	if stored.Annotations[retryAnnouncedAnnotation] != retryAnnouncedMarkerPrefix+retryAnnouncedToken("req-2") {
		t.Fatalf("announced marker = %q, want the token for req-2", stored.Annotations[retryAnnouncedAnnotation])
	}
	if src.calls != 0 {
		t.Fatalf("RequeueAttempt called %d times, want 0; already-requeued never requeues", src.calls)
	}
	events := parseRetryEvents(t, buf)
	if len(events) != 1 || events[0]["status"] != "Refused" || eventDetailString(events[0], "reason") != courierv1alpha1.RetryRefusalAlreadyRequeued || eventDetailString(events[0], "request_id") != "req-2" {
		t.Fatalf("run.retry events = %+v, want one Refused/already-requeued for req-2", events)
	}

	// A second reconcile of the same refused value announces nothing again.
	buf.Reset()
	if _, err := retryReconciler(c, src, reapClock, nil, buf).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := parseRetryEvents(t, buf); len(got) != 0 {
		t.Fatalf("re-announced already-requeued on the second pass: %+v", got)
	}
}

func TestRetryAlreadyRequeuedAlternatingValuesAnnounceOnce(t *testing.T) {
	// The marker records the announced set, not just the latest value: after
	// req-2 and req-3 have each been announced once, a return to req-2 must not
	// announce it a second time.
	src := &requeueSource{admissionSource: &admissionSource{}}
	buf := &bytes.Buffer{}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-2")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRequeued,
		AttemptID: "gen-1", SettledAt: &metav1.Time{Time: reapClock},
	}
	c := phaseClient(t, run)
	rec := retryReconciler(c, src, reapClock, nil, buf)

	for _, value := range []string{"req-2", "req-3", "req-2"} {
		stored := getRetryRun(t, c, "rr")
		stored.Annotations[courierv1alpha1.RetryAnnotation] = value
		if err := c.Update(context.Background(), stored); err != nil {
			t.Fatalf("update annotation to %q: %v", value, err)
		}
		if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
			t.Fatalf("Reconcile for %q: %v", value, err)
		}
	}
	events := parseRetryEvents(t, buf)
	ids := []string{}
	for _, ev := range events {
		ids = append(ids, eventDetailString(ev, "request_id"))
	}
	if len(events) != 2 || ids[0] != "req-2" || ids[1] != "req-3" {
		t.Fatalf("run.retry events for %v, want exactly one announcement each for req-2 and req-3", ids)
	}
}

func TestRetryAlreadyRequeuedAnnounceCapStaysBounded(t *testing.T) {
	// The marker is bounded: once the cap is reached a further distinct value is
	// not announced (never announced more than once), and the marker does not
	// grow past the cap.
	tokens := make([]string, 0, maxRetryAnnouncedTokens)
	for i := 0; i < maxRetryAnnouncedTokens; i++ {
		tokens = append(tokens, retryAnnouncedToken(fmt.Sprintf("seed-%d", i)))
	}
	seeded := retryAnnouncedMarkerPrefix + strings.Join(tokens, ",")
	src := &requeueSource{admissionSource: &admissionSource{}}
	buf := &bytes.Buffer{}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "fresh-1")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRequeued,
		AttemptID: "gen-1", SettledAt: &metav1.Time{Time: reapClock},
	}
	run.Annotations[retryAnnouncedAnnotation] = seeded
	c := phaseClient(t, run)
	rec := retryReconciler(c, src, reapClock, nil, buf)

	for _, value := range []string{"seed-5", "fresh-1"} {
		stored := getRetryRun(t, c, "rr")
		stored.Annotations[courierv1alpha1.RetryAnnotation] = value
		if err := c.Update(context.Background(), stored); err != nil {
			t.Fatalf("update annotation to %q: %v", value, err)
		}
		if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
			t.Fatalf("Reconcile for %q: %v", value, err)
		}
	}
	if got := parseRetryEvents(t, buf); len(got) != 0 {
		t.Fatalf("announced past the cap: %+v", got)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Annotations[retryAnnouncedAnnotation] != seeded {
		t.Fatalf("announced marker changed past the cap: %q", stored.Annotations[retryAnnouncedAnnotation])
	}
}

func TestRetryAlreadyRequeuedLegacyMarkerMigrates(t *testing.T) {
	// A legacy single-raw-ID marker seeds the announced set on first use, so the
	// value it announced is never announced again across the upgrade.
	src := &requeueSource{admissionSource: &admissionSource{}}
	buf := &bytes.Buffer{}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-2")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRequeued,
		AttemptID: "gen-1", SettledAt: &metav1.Time{Time: reapClock},
	}
	run.Annotations[retryAnnouncedAnnotation] = "req-2"
	c := phaseClient(t, run)
	rec := retryReconciler(c, src, reapClock, nil, buf)

	// The legacy-announced value announces nothing again.
	if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := parseRetryEvents(t, buf); len(got) != 0 {
		t.Fatalf("re-announced the legacy-announced value: %+v", got)
	}

	// A new distinct value announces once and the marker becomes the token set.
	stored := getRetryRun(t, c, "rr")
	stored.Annotations[courierv1alpha1.RetryAnnotation] = "req-3"
	if err := c.Update(context.Background(), stored); err != nil {
		t.Fatalf("update annotation: %v", err)
	}
	if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := parseRetryEvents(t, buf); len(got) != 1 || eventDetailString(got[0], "request_id") != "req-3" {
		t.Fatalf("run.retry events = %+v, want one announcement for req-3", got)
	}
	stored = getRetryRun(t, c, "rr")
	want := retryAnnouncedMarkerPrefix + retryAnnouncedToken("req-2") + "," + retryAnnouncedToken("req-3")
	if stored.Annotations[retryAnnouncedAnnotation] != want {
		t.Fatalf("announced marker = %q, want %q", stored.Annotations[retryAnnouncedAnnotation], want)
	}
}

func TestRetryAlreadyRequeuedAnnounceFailureSurfaces(t *testing.T) {
	// The already-requeued announcement writes its metadata marker before the event
	// and metric. A lost marker write must surface as a reconcile error so the
	// workqueue re-arms and re-announces — never a silently dropped audit signal.
	src := &requeueSource{admissionSource: &admissionSource{}}
	buf := &bytes.Buffer{}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-2")
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRequeued,
		AttemptID: "gen-1", SettledAt: &metav1.Time{Time: reapClock},
	}
	base := phaseClient(t, run)
	boom := errors.New("metadata patch unavailable")
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*courierv1alpha1.CoderRun); ok {
				return boom
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	rec := retryReconciler(c, src, reapClock, nil, buf)
	rec.StatusWriter = fakeStatusWriter{client: base}

	if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err == nil {
		t.Fatalf("Reconcile: want the lost announce metadata write surfaced as an error")
	}
	stored := getRetryRun(t, base, "rr")
	if _, announced := stored.Annotations[retryAnnouncedAnnotation]; announced {
		t.Fatalf("announced marker written despite the failed patch: %v", stored.Annotations)
	}
	if got := parseRetryEvents(t, buf); len(got) != 0 {
		t.Fatalf("announced the already-requeued event before the marker write landed: %+v", got)
	}
}

func TestRetryFreshRequestAfterRefusedReplacesRecord(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-3"}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-2")
	// A predecessor Refused request that carried a reason and a settled stamp.
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID: "req-1", RequestedAt: metav1.NewTime(reapClock), State: courierv1alpha1.RetryRequestStateRefused,
		Reason: courierv1alpha1.RetryRefusalActiveOwnerExists, SettledAt: &metav1.Time{Time: reapClock},
	}
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec := getRetryRun(t, c, "rr").Status.RetryRequest
	// The new value opened a fresh request that fully replaced the stale one.
	if rec.ID != "req-2" || rec.State != courierv1alpha1.RetryRequestStateRequeued {
		t.Fatalf("retryRequest = %+v, want a fresh Requeued request for req-2", rec)
	}
	if rec.Reason != "" || rec.SettledAt == nil {
		t.Fatalf("retryRequest = %+v, want the stale reason cleared and settledAt re-stamped", rec)
	}
	if len(src.requests) != 1 || src.requests[0].IdempotencyKey != "coderun/"+admissionNamespace+"/rr/"+retryKeyDigest("req-2") {
		t.Fatalf("requeue requests = %+v, want a request keyed by req-2", src.requests)
	}
}

func TestRetryNotRetryablePhaseRefused(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}}
	run := retryRun("rr", courierv1alpha1.PhaseAwaitingReview, "req-1")
	run.Status.PR = "42"
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec := getRetryRun(t, c, "rr").Status.RetryRequest
	if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRefused || rec.Reason != courierv1alpha1.RetryRefusalNotRetryablePhase {
		t.Fatalf("retryRequest = %+v, want Refused/not-retryable-phase", rec)
	}
	if src.calls != 0 {
		t.Fatalf("RequeueAttempt called %d times for a non-retryable phase", src.calls)
	}
}

func TestRetryBlockedExternalAutomaticRecoveryChannel(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}}
	run := retryRun("rr", courierv1alpha1.PhaseBlockedExternal, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec := getRetryRun(t, c, "rr").Status.RetryRequest
	if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRefused || rec.Reason != courierv1alpha1.RetryRefusalAutomaticRecoveryChannel {
		t.Fatalf("retryRequest = %+v, want Refused/automatic-recovery-channel", rec)
	}
}

func TestRetryActiveOwnerExists(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	// A sibling run for the same source work is still Running — with a
	// different source agent, which must not matter: ownership is per
	// Source + Repo + Ref, not per agent.
	other := admissionRun("sibling", "local", courierv1alpha1.PhaseRunning)
	other.Spec.Repo = run.Spec.Repo
	other.Spec.Ref = run.Spec.Ref
	other.Spec.Source = run.Spec.Source
	other.Spec.SourceAgent = "some-other-agent"
	c := phaseClient(t, run, other)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec := getRetryRun(t, c, "rr").Status.RetryRequest
	if rec == nil || rec.Reason != courierv1alpha1.RetryRefusalActiveOwnerExists {
		t.Fatalf("retryRequest = %+v, want Refused/active-owner-exists", rec)
	}
	if src.calls != 0 {
		t.Fatalf("RequeueAttempt called %d times despite an active owner", src.calls)
	}
}

func TestRetryActiveOwnerIdentityScopedToSourceRepoRef(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-2"}}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	// Siblings that share only part of the identity are not owners: the
	// guard is Source + Repo + Ref, so each of these must not block.
	for name, mutate := range map[string]func(*courierv1alpha1.CoderRun){
		"other-source": func(o *courierv1alpha1.CoderRun) { o.Spec.Source = "elsewhere" },
		"other-repo":   func(o *courierv1alpha1.CoderRun) { o.Spec.Repo = "acme/other" },
		"other-ref":    func(o *courierv1alpha1.CoderRun) { o.Spec.Ref = run.Spec.Ref + 1 },
	} {
		other := admissionRun(name, "local", courierv1alpha1.PhaseRunning)
		other.Spec.Source = run.Spec.Source
		other.Spec.Repo = run.Spec.Repo
		other.Spec.Ref = run.Spec.Ref
		mutate(other)
		c := phaseClient(t, run, other)

		if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
			t.Fatalf("%s: Reconcile: %v", name, err)
		}
		rec := getRetryRun(t, c, "rr").Status.RetryRequest
		if rec == nil || rec.State != courierv1alpha1.RetryRequestStateRequeued {
			t.Fatalf("%s: retryRequest = %+v, want Requeued", name, rec)
		}
	}
	if len(src.requests) != 3 {
		t.Fatalf("RequeueAttempt issued %d requests, want 3", len(src.requests))
	}
}

func TestRetryUnsupportedSource(t *testing.T) {
	// admissionSource does not implement AttemptRequeuer.
	src := &admissionSource{}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec := getRetryRun(t, c, "rr").Status.RetryRequest
	if rec == nil || rec.Reason != courierv1alpha1.RetryRefusalUnsupportedSource {
		t.Fatalf("retryRequest = %+v, want Refused/unsupported-source", rec)
	}
}

func TestRetryFixPRUpstreamClosed(t *testing.T) {
	for name, pull := range map[string]forgePullCase{
		"merged": {state: "closed", merged: true},
		"closed": {state: "closed", merged: false},
	} {
		t.Run(name, func(t *testing.T) {
			src := &requeueSource{admissionSource: &admissionSource{}}
			reader := &fakePullRequestReader{pull: observedPR(func(p forge.PullRequest) forge.PullRequest {
				p.HeadRef = "courier/acme/widgets/pr-42"
				p.State = pull.state
				p.Merged = pull.merged
				return p
			})}
			run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
			run.Spec.Mode = courierv1alpha1.ModeFixPR
			run.Status.PR = "42"
			run.Status.Branch = "courier/acme/widgets/pr-42"
			run.Status.HeadRepo = "acme/widgets"
			c := phaseClient(t, run)

			rec := retryReconciler(c, src, reapClock, nil, nil)
			rec.PullRequests = reader
			if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			stored := getRetryRun(t, c, "rr").Status.RetryRequest
			if stored == nil || stored.Reason != courierv1alpha1.RetryRefusalUpstreamClosed {
				t.Fatalf("retryRequest = %+v, want Refused/upstream-closed", stored)
			}
			if src.calls != 0 {
				t.Fatalf("RequeueAttempt called %d times despite a closed upstream", src.calls)
			}
		})
	}
}

type forgePullCase struct {
	state  string
	merged bool
}

func TestRetryFixPRForgeUnreadableNotConsumed(t *testing.T) {
	for _, tt := range []struct {
		name      string
		reader    *fakePullRequestReader
		wantCalls int
	}{
		{"read-error", &fakePullRequestReader{err: errors.New("forge down")}, 1},
		{"no-capability", &fakePullRequestReader{noCapability: true}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src := &requeueSource{admissionSource: &admissionSource{}}
			run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
			run.Spec.Mode = courierv1alpha1.ModeFixPR
			run.Status.PR = "42"
			run.Status.Branch = "courier/acme/widgets/pr-42"
			run.Status.HeadRepo = "acme/widgets"
			c := phaseClient(t, run)

			rec := retryReconciler(c, src, reapClock, nil, nil)
			rec.PullRequests = tt.reader
			result, err := rec.Reconcile(context.Background(), admissionRequest("rr"))
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if result.RequeueAfter != retryForgeRequeueDelay {
				t.Fatalf("RequeueAfter = %v, want the forge guard requeue", result.RequeueAfter)
			}
			// The request is NOT consumed and no record is written while the forge
			// is unreadable — a forge that cannot be read never fails open.
			if stored := getRetryRun(t, c, "rr").Status.RetryRequest; stored != nil {
				t.Fatalf("retryRequest = %+v, want nothing written while the forge is unreadable", stored)
			}
			if src.calls != 0 {
				t.Fatalf("RequeueAttempt called %d times while the forge was unreadable", src.calls)
			}
			if tt.reader.calls != tt.wantCalls {
				t.Fatalf("forge reads = %d, want %d", tt.reader.calls, tt.wantCalls)
			}
		})
	}
}

func TestRetryFixPROpenProceedsToRequeue(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-1"}}
	reader := &fakePullRequestReader{pull: observedPR(func(p forge.PullRequest) forge.PullRequest {
		p.HeadRef = "courier/acme/widgets/pr-42"
		return p
	})}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	run.Status.PR = "42"
	run.Status.Branch = "courier/acme/widgets/pr-42"
	run.Status.HeadRepo = "acme/widgets"
	c := phaseClient(t, run)

	rec := retryReconciler(c, src, reapClock, nil, nil)
	rec.PullRequests = reader
	if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if stored := getRetryRun(t, c, "rr").Status.RetryRequest; stored == nil || stored.State != courierv1alpha1.RetryRequestStateRequeued {
		t.Fatalf("retryRequest = %+v, want Requeued when the upstream is open", stored)
	}
}

func TestRetryFixPRIdentityMismatchNotOurUpstream(t *testing.T) {
	for name, pull := range map[string]forgePullCase{
		"merged": {state: "closed", merged: true},
		"closed": {state: "closed", merged: false},
		"open":   {state: "open", merged: false},
	} {
		t.Run(name, func(t *testing.T) {
			// The pull request at the recorded number carries a drifted head, so
			// it is not the one this run produced: neither a merge nor a close on
			// it is this run's outcome, exactly as the merge observation treats
			// drift, so the guard does not refuse and the requeue proceeds.
			src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-1"}}
			reader := &fakePullRequestReader{pull: observedPR(func(p forge.PullRequest) forge.PullRequest {
				p.HeadRef = "courier/acme/widgets/drifted-branch"
				p.State = pull.state
				p.Merged = pull.merged
				return p
			})}
			run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
			run.Spec.Mode = courierv1alpha1.ModeFixPR
			run.Status.PR = "42"
			run.Status.Branch = "courier/acme/widgets/pr-42"
			run.Status.HeadRepo = "acme/widgets"
			c := phaseClient(t, run)

			rec := retryReconciler(c, src, reapClock, nil, nil)
			rec.PullRequests = reader
			if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			stored := getRetryRun(t, c, "rr").Status.RetryRequest
			if stored == nil || stored.State != courierv1alpha1.RetryRequestStateRequeued {
				t.Fatalf("retryRequest = %+v, want Requeued when the read PR is not this run's identity", stored)
			}
			if src.calls != 1 {
				t.Fatalf("RequeueAttempt called %d times, want 1 past the mismatched identity", src.calls)
			}
		})
	}
}

func TestRetryFixPRNoRecordedBranchGuardNotEngaged(t *testing.T) {
	// A recorded PR without a recorded branch has no verifiable head identity,
	// the same stance the merge observation takes: it is left alone rather than
	// judged by inference, so the guard does not refuse.
	src := &requeueSource{admissionSource: &admissionSource{}, result: source.AttemptRequeueResult{AttemptID: "gen-1"}}
	reader := &fakePullRequestReader{}
	run := retryRun("rr", courierv1alpha1.PhaseFailed, "req-1")
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	run.Status.PR = "42"
	c := phaseClient(t, run)

	rec := retryReconciler(c, src, reapClock, nil, nil)
	rec.PullRequests = reader
	if _, err := rec.Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if reader.calls != 0 {
		t.Fatalf("forge reads = %d, want 0 without a recorded branch", reader.calls)
	}
	if stored := getRetryRun(t, c, "rr").Status.RetryRequest; stored == nil || stored.State != courierv1alpha1.RetryRequestStateRequeued {
		t.Fatalf("retryRequest = %+v, want Requeued with the guard not engaged", stored)
	}
}

func TestRetryInvalidAnnotationNoOp(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}}
	// A request ID over the 128-code-point bound cannot become a record. (An
	// invalid-UTF-8 value is not reachable here: the API server rejects it before
	// the controller sees it, and the predicate's UTF-8 branch is covered directly
	// by TestValidRetryRequestID.)
	long := strings.Repeat("x", maxRetryRequestIDLength+1)
	run := retryRun("rr", courierv1alpha1.PhaseFailed, long)
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if stored := getRetryRun(t, c, "rr").Status.RetryRequest; stored != nil {
		t.Fatalf("retryRequest = %+v, want no record for a request ID over the bound", stored)
	}
	if src.calls != 0 {
		t.Fatalf("RequeueAttempt called %d times for an invalid request ID", src.calls)
	}
}

func TestRetryDefersBehindUnsettledLifecycleReport(t *testing.T) {
	src := &requeueSource{admissionSource: &admissionSource{}}
	run := admissionRun("rr", "local", courierv1alpha1.PhaseFailed)
	run.CreationTimestamp = metav1.NewTime(reapClock.Add(-72 * time.Hour))
	run.Status.FinishedAt = &metav1.Time{Time: reapClock.Add(-48 * time.Hour)}
	run.Status.Conditions = []metav1.Condition{{
		Type:               lifecycleReportedCondition,
		Status:             metav1.ConditionFalse,
		Reason:             lifecycleReportPendingReason,
		LastTransitionTime: metav1.NewTime(reapClock),
	}}
	run.Annotations = map[string]string{courierv1alpha1.RetryAnnotation: "req-1"}
	c := phaseClient(t, run)

	if _, err := retryReconciler(c, src, reapClock, nil, nil).Reconcile(context.Background(), admissionRequest("rr")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := getRetryRun(t, c, "rr")
	if stored.Status.RetryRequest != nil {
		t.Fatalf("retryRequest = %+v, want the retry deferred behind an unsettled lifecycle report", stored.Status.RetryRequest)
	}
	if src.calls != 0 {
		t.Fatalf("RequeueAttempt called %d times; the source work must not be requeued before the run reports its ending", src.calls)
	}
}

// retryKeyDigest encodes the request-ID segment of a retry idempotency key the
// way the controller does: the hex SHA-256 of the opaque request ID.
func retryKeyDigest(requestID string) string {
	sum := sha256.Sum256([]byte(requestID))
	return hex.EncodeToString(sum[:])
}

func TestRetryIdempotencyKeyShape(t *testing.T) {
	run := &courierv1alpha1.CoderRun{}
	run.Namespace = "ns"
	run.Name = "name"
	if got, want := retryIdempotencyKey(run, "req"), "coderun/ns/name/"+retryKeyDigest("req"); got != want {
		t.Fatalf("key without UID = %q, want %q", got, want)
	}
	run.UID = "uid-123"
	if got, want := retryIdempotencyKey(run, "req"), "coderun/ns/name/"+retryKeyDigest("req")+"/uid-123"; got != want {
		t.Fatalf("key with UID = %q, want %q", got, want)
	}
	// The request ID is opaque operator data: its digest is the only thing the
	// key carries, so no legal value can alias another request's key, collide
	// with the UID segment, or grow the key with unbounded raw characters.
	slashy := retryIdempotencyKey(run, "x/y/uid-123/../req")
	if slashy != "coderun/ns/name/"+retryKeyDigest("x/y/uid-123/../req")+"/uid-123" {
		t.Fatalf("key for opaque id = %q, want only digest segments", slashy)
	}
	if slashy == retryIdempotencyKey(run, "x/y/uid-123/../req2") {
		t.Fatalf("distinct request IDs %q and %q shared one key", "x/y/uid-123/../req", "x/y/uid-123/../req2")
	}
	if again := retryIdempotencyKey(run, "req"); again != "coderun/ns/name/"+retryKeyDigest("req")+"/uid-123" {
		t.Fatalf("key not deterministic across calls: %q", again)
	}
}

func TestValidRetryRequestID(t *testing.T) {
	if !validRetryRequestID("req-1") {
		t.Fatal("a normal id must be valid")
	}
	if validRetryRequestID("") {
		t.Fatal("empty id must be invalid")
	}
	if validRetryRequestID(strings.Repeat("x", 129)) {
		t.Fatal("id over 128 code points must be invalid")
	}
	if validRetryRequestID(string([]byte{0xff})) {
		t.Fatal("invalid UTF-8 must be invalid")
	}
	if !utf8.ValidString("ok") {
		t.Fatal("sanity: 'ok' must be valid UTF-8")
	}
}
