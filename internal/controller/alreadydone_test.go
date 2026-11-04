package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// closeReportingSource is a source adapter that also offers the
// evidence-gated close path: it records every close proposal it is handed and
// returns a programmable resolution or error so each close outcome can be
// exercised in isolation.
type closeReportingSource struct {
	*admissionSource
	closeCalls      []source.Lifecycle
	closeResolution source.CloseResolution
	closeErr        error
}

func (s *closeReportingSource) ReportClose(_ context.Context, _ source.WorkItem, lifecycle source.Lifecycle) (source.CloseResolution, error) {
	s.closeCalls = append(s.closeCalls, lifecycle)
	if s.closeErr != nil {
		return source.CloseResolution{}, s.closeErr
	}
	return s.closeResolution, nil
}

// fakeWorkItemCommenter records the operator-authored source comments it is
// asked to post and can be made to fail, to prove delivery is best-effort.
type fakeWorkItemCommenter struct {
	bodies []string
	err    error
}

func (c *fakeWorkItemCommenter) CommentWorkItem(_ context.Context, _ *courierv1alpha1.CoderRun, body string) error {
	c.bodies = append(c.bodies, body)
	return c.err
}

// groundedNoChange builds the durable, incarnation-fenced close proposal a
// grounded resolve-issue conclusion carries.
func groundedNoChange(podUID string) *courierv1alpha1.NoChangeClose {
	return &courierv1alpha1.NoChangeClose{
		Criteria: []courierv1alpha1.NoChangeCloseCriterion{
			{Criterion: "the ask is implemented", Path: "pkg/core/impl.go", Excerpt: "func Serve(ctx context.Context) error { return nil }"},
		},
		Evidence:          "the request is already implemented on the default branch",
		CoordinatorPodUID: podUID,
		At:                metav1.Now(),
	}
}

// runWithNoChange builds an AlreadyDone run carrying a grounded proposal
// fenced to the given pod UID.
func runWithNoChange(podUID string) *courierv1alpha1.CoderRun {
	run := admissionRun("run", "local", courierv1alpha1.PhaseAlreadyDone)
	run.Status.NoChange = groundedNoChange(podUID)
	return run
}

func sourceCloseConditionOf(run *courierv1alpha1.CoderRun) *metav1.Condition {
	return apimeta.FindStatusCondition(run.Status.Conditions, sourceCloseCondition)
}

func TestResolveIssueNoChangePhase(t *testing.T) {
	cases := []struct {
		name        string
		run         *courierv1alpha1.CoderRun
		podUID      string
		wantPhase   courierv1alpha1.Phase
		wantUpgrade bool
	}{
		{"nil run parks human", nil, "pod-1", courierv1alpha1.PhaseNeedsHuman, false},
		{"no proposal parks human", admissionRun("run", "local", courierv1alpha1.PhaseAlreadyDone), "pod-1", courierv1alpha1.PhaseNeedsHuman, false},
		{"fenced proposal upgrades", runWithNoChange("pod-1"), "pod-1", courierv1alpha1.PhaseAlreadyDone, true},
		{"foreign fence parks human", runWithNoChange("pod-2"), "pod-1", courierv1alpha1.PhaseNeedsHuman, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			phase, ok := resolveIssueNoChangePhase(tc.run, tc.podUID)
			if phase != tc.wantPhase || ok != tc.wantUpgrade {
				t.Fatalf("resolveIssueNoChangePhase() = (%s, %v); want (%s, %v)", phase, ok, tc.wantPhase, tc.wantUpgrade)
			}
		})
	}
}

func TestAlreadyDoneLifecycle(t *testing.T) {
	r := &CoderRunReconciler{}
	run := runWithNoChange("pod-1")
	lc := r.alreadyDoneLifecycle(run, terminalLifecycleIntent{error: "no-change"})
	if lc.Result != source.ResultAlreadyDone {
		t.Fatalf("Result = %s; want AlreadyDone", lc.Result)
	}
	if lc.State != "" {
		t.Fatalf("State = %q; want none (the close is the settlement)", lc.State)
	}
	if lc.Error != "no-change" {
		t.Fatalf("Error = %q; want the no-change reason", lc.Error)
	}
	if lc.IdempotencyKey == "" {
		t.Fatal("IdempotencyKey empty; want a stable dedupe key")
	}
	if lc.Close == nil || len(lc.Close.Criteria) != 1 {
		t.Fatalf("Close = %#v; want one grounded criterion", lc.Close)
	}
	if lc.Close.Criteria[0].Path != "pkg/core/impl.go" || lc.Close.Criteria[0].Excerpt == "" {
		t.Fatalf("Close.Criteria = %#v; want the grounded path and excerpt", lc.Close.Criteria)
	}
	if lc.Close.Evidence != "the request is already implemented on the default branch" {
		t.Fatalf("Close.Evidence = %q", lc.Close.Evidence)
	}
	// A proposal-less run still reports AlreadyDone but carries no close body.
	if got := r.alreadyDoneLifecycle(&courierv1alpha1.CoderRun{}, terminalLifecycleIntent{}); got.Result != source.ResultAlreadyDone || got.Close != nil {
		t.Fatalf("proposal-less lifecycle = %#v; want AlreadyDone with no close", got)
	}
}

// TestSettleAlreadyDoneClose drives the sole consumer of the close proposal
// through every source resolution and checks the resulting run state: the close
// outcome lands on the SourceClose condition, a non-closing answer parks the
// run for human attention, a closed answer leaves it AlreadyDone, and a
// superseded rejection drops the report without a retry.
func TestSettleAlreadyDoneClose(t *testing.T) {
	cases := []struct {
		name        string
		resolution  source.CloseResolution
		closeErr    error
		noProposal  bool
		noReporter  bool
		wantPhase   courierv1alpha1.Phase
		wantClose   string
		wantClosed  bool
		wantComment bool
		wantRequeue bool
	}{
		{
			name:        "closed work item stays already-done",
			resolution:  source.CloseResolution{Action: source.CloseActionClosed, HeadSHA: "h", EvidenceDigest: "d"},
			wantPhase:   courierv1alpha1.PhaseAlreadyDone,
			wantClose:   sourceCloseClosedReason,
			wantClosed:  true,
			wantComment: true,
		},
		{
			name:        "refused close parks human",
			resolution:  source.CloseResolution{Action: source.CloseActionRefused, Reasons: []string{"criterion not satisfied"}},
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseRefusedReason,
			wantComment: true,
		},
		{
			name:        "failed close parks human",
			resolution:  source.CloseResolution{Action: source.CloseActionFailed, Reason: "forge write failed"},
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseFailedReason,
			wantComment: true,
		},
		{
			name:        "skipped replay parks human",
			resolution:  source.CloseResolution{Action: source.CloseActionSkipped, Reason: "replay unconfirmable"},
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseSkippedReason,
			wantComment: true,
		},
		{
			name:      "superseded drops the report",
			closeErr:  source.ErrSuperseded,
			wantPhase: courierv1alpha1.PhaseAlreadyDone,
			wantClose: "",
		},
		{
			name:        "unsupported outcome parks human",
			closeErr:    source.ErrUnsupportedOutcome,
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseUnsupportedReason,
			wantComment: true,
		},
		{
			name:        "permanent refusal moves ownership",
			closeErr:    source.ErrPermanentRefusal,
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseOwnershipMovedReason,
			wantComment: true,
		},
		{
			name:        "transient failure stays already-done and requeues",
			closeErr:    context.DeadlineExceeded,
			wantPhase:   courierv1alpha1.PhaseAlreadyDone,
			wantClose:   "",
			wantRequeue: true,
		},
		{
			name:        "no proposal parks human",
			noProposal:  true,
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseUnsupportedReason,
			wantComment: true,
		},
		{
			name:        "source without close path parks human",
			noReporter:  true,
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseUnsupportedReason,
			wantComment: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := runWithNoChange("pod-1")
			if tc.noProposal {
				run.Status.NoChange = nil
			}
			kube := phaseClient(t, admissionLane("local", 1), run)
			var adapter source.Adapter
			switch {
			case tc.noReporter:
				adapter = &admissionSource{}
			default:
				adapter = &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: tc.resolution, closeErr: tc.closeErr}
			}
			commenter := &fakeWorkItemCommenter{}
			r := &CoderRunReconciler{
				Client:           kube,
				APIReader:        kube,
				Sources:          NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
				StatusWriter:     fakeStatusWriter{client: kube},
				WorkItemComments: commenter,
			}
			item := source.WorkItem{ID: run.Spec.WorkItemID, Repo: run.Spec.Repo, Ref: run.Spec.Ref, Mode: string(run.Spec.Mode), Lane: run.Spec.Lane}
			result, err := r.settleAlreadyDoneClose(context.Background(), run, adapter, item, terminalLifecycleIntent{error: "no-change"})
			if err != nil {
				t.Fatalf("settleAlreadyDoneClose() error = %v", err)
			}
			stored := &courierv1alpha1.CoderRun{}
			if err := kube.Get(context.Background(), admissionKey(run.Name), stored); err != nil {
				t.Fatalf("re-read run: %v", err)
			}
			if stored.Status.Phase != tc.wantPhase {
				t.Fatalf("phase = %s; want %s", stored.Status.Phase, tc.wantPhase)
			}
			cond := sourceCloseConditionOf(stored)
			switch {
			case tc.wantClose == "":
				if cond != nil {
					t.Fatalf("SourceClose = %s/%s; want none", cond.Reason, cond.Status)
				}
			case cond == nil:
				t.Fatalf("SourceClose missing; want %s", tc.wantClose)
			case cond.Reason != tc.wantClose:
				t.Fatalf("SourceClose reason = %q; want %s", cond.Reason, tc.wantClose)
			case tc.wantClosed && cond.Status != metav1.ConditionTrue:
				t.Fatalf("SourceClose status = %s; want True", cond.Status)
			case !tc.wantClosed && cond.Status != metav1.ConditionFalse:
				t.Fatalf("SourceClose status = %s; want False", cond.Status)
			}
			if got, want := len(commenter.bodies) > 0, tc.wantComment; got != want {
				t.Fatalf("comment posted = %v; want %v", got, want)
			}
			if got, want := result.RequeueAfter > 0, tc.wantRequeue; got != want {
				t.Fatalf("requeued = %v; want %v (result=%v)", got, want, result)
			}
		})
	}
}

// TestSettleAlreadyDoneClosePresentsGroundedProposal verifies the close report
// handed to the source re-derives the durable, incarnation-fenced proposal: the
// grounded criteria and evidence ride the report, the idempotency key is stable,
// and the report carries no work-item state transition.
func TestSettleAlreadyDoneClosePresentsGroundedProposal(t *testing.T) {
	run := runWithNoChange("pod-1")
	kube := phaseClient(t, admissionLane("local", 1), run)
	adapter := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: source.CloseResolution{Action: source.CloseActionClosed}}
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           kube,
		APIReader:        kube,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
		StatusWriter:     fakeStatusWriter{client: kube},
		WorkItemComments: commenter,
	}
	item := source.WorkItem{ID: run.Spec.WorkItemID, Repo: run.Spec.Repo, Ref: run.Spec.Ref, Mode: string(run.Spec.Mode), Lane: run.Spec.Lane}
	if _, err := r.settleAlreadyDoneClose(context.Background(), run, adapter, item, terminalLifecycleIntent{error: "no-change"}); err != nil {
		t.Fatalf("settleAlreadyDoneClose() error = %v", err)
	}
	if len(adapter.closeCalls) != 1 {
		t.Fatalf("ReportClose calls = %d; want exactly one", len(adapter.closeCalls))
	}
	lc := adapter.closeCalls[0]
	if lc.Result != source.ResultAlreadyDone {
		t.Fatalf("Result = %s; want AlreadyDone", lc.Result)
	}
	if lc.State != "" {
		t.Fatalf("State = %q; want none", lc.State)
	}
	if lc.IdempotencyKey == "" {
		t.Fatal("IdempotencyKey empty; want the stable dedupe key")
	}
	if lc.Close == nil || len(lc.Close.Criteria) != 1 {
		t.Fatalf("Close = %#v; want the grounded proposal", lc.Close)
	}
	if lc.Close.Criteria[0].Criterion != "the ask is implemented" || lc.Close.Criteria[0].Path != "pkg/core/impl.go" {
		t.Fatalf("Close.Criteria = %#v; want the grounded row", lc.Close.Criteria)
	}
	if lc.Close.Evidence != "the request is already implemented on the default branch" {
		t.Fatalf("Close.Evidence = %q", lc.Close.Evidence)
	}
}

// TestAlreadyDoneGroundedReconcile drives the full reconcile: a resolve-issue
// run whose trusted no_change ending carries an incarnation-fenced grounded
// proposal terminalizes as AlreadyDone and settles the close through the
// source's own policy, staying AlreadyDone (never AwaitingReview, never a
// human park) once the source closes the work item.
func TestAlreadyDoneGroundedReconcile(t *testing.T) {
	podUID := "pod-uid-native"
	run := admissionRun("grounded", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid")
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "grounded-work"
	now := metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC))
	run.Status.NativeResult = &courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultNoChange,
		Reason:            "the request is already satisfied",
		CoordinatorPodUID: podUID,
		At:                now,
	}
	run.Status.NoChange = groundedNoChange(podUID)
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod := coordinatorPodWithOutcome(run, exitDeclaredNoChange, "no_change_needed", "the request is already satisfied")
	pod.UID = types.UID(podUID)
	pod.Status.Phase = corev1.PodFailed
	c := capacityTestClient(t, lane, run, pod)
	s := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: source.CloseResolution{Action: source.CloseActionClosed}}
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           c,
		APIReader:        c,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": s}),
		Now:              func() time.Time { return time.Date(2026, 10, 26, 12, 0, 1, 0, time.UTC) },
		WorkItemComments: commenter,
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background(), admissionRequest("grounded")); err != nil {
			t.Fatalf("Reconcile[%d] error = %v", i, err)
		}
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), admissionKey("grounded"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseAlreadyDone {
		t.Fatalf("phase = %s; want AlreadyDone", stored.Status.Phase)
	}
	cond := sourceCloseConditionOf(stored)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != sourceCloseClosedReason {
		t.Fatalf("SourceClose = %#v; want True/Closed", cond)
	}
	if len(s.closeCalls) < 1 {
		t.Fatal("the source's close path was never called")
	}
	if len(commenter.bodies) == 0 {
		t.Fatal("no source-issue comment posted for the closed work item")
	}
}

// TestEvidenceIncompleteNoChangeHoldsAndAnnounces drives the exit path for a
// resolve-issue trusted no_change ending whose proposal is absent: the run
// parks NeedsHuman (never AwaitingReview), the source's close path is never
// called, and the source work item receives exactly one explanation that no
// PR is coming and no closure was attempted.
func TestEvidenceIncompleteNoChangeHoldsAndAnnounces(t *testing.T) {
	podUID := "pod-uid-ungrounded"
	run := admissionRun("ungrounded", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid")
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "ungrounded-work"
	run.Status.NativeResult = &courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultNoChange,
		Reason:            "coordinator declared no change needed",
		CoordinatorPodUID: podUID,
		At:                metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)),
	}
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod := coordinatorPodWithOutcome(run, exitDeclaredNoChange, "no_change_needed", "the request looks satisfied")
	pod.UID = types.UID(podUID)
	pod.Status.Phase = corev1.PodFailed
	c := capacityTestClient(t, lane, run, pod)
	s := &closeReportingSource{admissionSource: &admissionSource{}}
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           c,
		APIReader:        c,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": s}),
		Now:              func() time.Time { return time.Date(2026, 10, 26, 12, 0, 1, 0, time.UTC) },
		WorkItemComments: commenter,
	}
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(context.Background(), admissionRequest("ungrounded")); err != nil {
			t.Fatalf("Reconcile[%d] error = %v", i, err)
		}
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), admissionKey("ungrounded"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %s; want NeedsHuman", stored.Status.Phase)
	}
	if len(s.closeCalls) != 0 {
		t.Fatalf("close calls = %d; want 0 without a grounded proposal", len(s.closeCalls))
	}
	if len(commenter.bodies) != 1 {
		t.Fatalf("comment bodies = %d; want exactly one hold announcement", len(commenter.bodies))
	}
	if !strings.Contains(commenter.bodies[0], "no grounded close evidence") {
		t.Fatalf("hold comment = %q; want it to name the missing evidence", commenter.bodies[0])
	}
}

// TestAlreadyDoneCommentAnnouncesOnce verifies the announce-once marker: a
// settled outcome is announced at most once per run even when the settlement
// path runs twice (for example after a crash between post and write).
func TestAlreadyDoneCommentAnnouncesOnce(t *testing.T) {
	kube := capacityTestClient(t, admissionLane("local", 1), runWithNoChange("pod-1"))
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           kube,
		APIReader:        kube,
		WorkItemComments: commenter,
		Now:              func() time.Time { return time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC) },
	}
	run := runWithNoChange("pod-1")
	resolution := source.CloseResolution{Action: source.CloseActionClosed}
	r.commentAlreadyDone(context.Background(), run, true, sourceCloseClosedReason, resolution)
	r.commentAlreadyDone(context.Background(), run, true, sourceCloseClosedReason, resolution)
	if len(commenter.bodies) != 1 {
		t.Fatalf("comment bodies = %d; want 1", len(commenter.bodies))
	}
	if cond := apimeta.FindStatusCondition(run.Status.Conditions, sourceCloseCommentedCondition); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SourceCloseCommented = %#v; want True after a successful post", cond)
	}
}

// TestAlreadyDoneUnsettledCloseIsRepresented verifies that an AlreadyDone run// whose close has not yet reached a positive outcome is re-presented to the
// source on a later reconcile under the same stable idempotency key, rather
// than being held or reaped.
func TestAlreadyDoneUnsettledCloseIsRepresented(t *testing.T) {
	run := runWithNoChange("pod-1")
	kube := phaseClient(t, admissionLane("local", 1), run)
	adapter := &closeReportingSource{
		admissionSource: &admissionSource{},
		closeResolution: source.CloseResolution{Action: source.CloseActionFailed, Reason: "first attempt"},
	}
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           kube,
		APIReader:        kube,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
		StatusWriter:     fakeStatusWriter{client: kube},
		WorkItemComments: commenter,
	}
	item := source.WorkItem{ID: run.Spec.WorkItemID, Repo: run.Spec.Repo, Ref: run.Spec.Ref, Mode: string(run.Spec.Mode), Lane: run.Spec.Lane}
	// First settlement: a failed close parks the run for human attention.
	if _, err := r.settleAlreadyDoneClose(context.Background(), run, adapter, item, terminalLifecycleIntent{error: "no-change"}); err != nil {
		t.Fatalf("settleAlreadyDoneClose (failed) error = %v", err)
	}
	if got := run.Status.Phase; got != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("after a failed close, phase = %s; want NeedsHuman", got)
	}
	if len(adapter.closeCalls) != 1 {
		t.Fatalf("close calls after first settlement = %d; want 1", len(adapter.closeCalls))
	}
	// A second presentation must reuse the same stable idempotency key.
	run2 := runWithNoChange("pod-1")
	r2 := &CoderRunReconciler{
		Client:           kube,
		APIReader:        kube,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
		StatusWriter:     fakeStatusWriter{client: kube},
		WorkItemComments: commenter,
	}
	before := len(adapter.closeCalls)
	if _, err := r2.settleAlreadyDoneClose(context.Background(), run2, adapter, item, terminalLifecycleIntent{error: "no-change"}); err != nil {
		t.Fatalf("settleAlreadyDoneClose (retry) error = %v", err)
	}
	if len(adapter.closeCalls) != before+1 {
		t.Fatalf("close calls after retry = %d; want %d", len(adapter.closeCalls), before+1)
	}
	if adapter.closeCalls[0].IdempotencyKey != adapter.closeCalls[1].IdempotencyKey {
		t.Fatalf("idempotency keys differ across presentations: %q vs %q", adapter.closeCalls[0].IdempotencyKey, adapter.closeCalls[1].IdempotencyKey)
	}
}

// TestAlreadyDoneRetention checks the AlreadyDone reap window: a run with an
// expired retention marker is deleted, and a run without a marker gets the
// window stamped and retained for the Done retention period.
func TestAlreadyDoneRetention(t *testing.T) {
	t.Run("expired window deletes", func(t *testing.T) {
		run := runWithNoChange("pod-1")
		run.Status.Conditions = []metav1.Condition{{
			Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: lifecycleReportPublishedReason,
			LastTransitionTime: metav1.Now(),
		}}
		past := time.Now().Add(-2 * defaultDoneRetention).UTC().Format(time.RFC3339)
		run.Annotations = map[string]string{alreadyDoneAtAnnotation: past}
		kube := phaseClient(t, admissionLane("local", 1), run)
		r := &CoderRunReconciler{Client: kube, APIReader: kube, StatusWriter: fakeStatusWriter{client: kube}}
		result, err := r.reapAlreadyDone(context.Background(), run)
		if err != nil {
			t.Fatalf("reapAlreadyDone() error = %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Fatalf("RequeueAfter = %v; want a delete (no requeue)", result.RequeueAfter)
		}
		stored := &courierv1alpha1.CoderRun{}
		if err := kube.Get(context.Background(), admissionKey(run.Name), stored); !apierrors.IsNotFound(err) {
			t.Fatalf("run still present after retention (err=%v); want deleted", err)
		}
	})
	t.Run("first observation stamps the window", func(t *testing.T) {
		run := runWithNoChange("pod-1")
		run.Status.Conditions = []metav1.Condition{{
			Type: lifecycleReportedCondition, Status: metav1.ConditionTrue, Reason: lifecycleReportPublishedReason,
			LastTransitionTime: metav1.Now(),
		}}
		kube := phaseClient(t, admissionLane("local", 1), run)
		r := &CoderRunReconciler{Client: kube, APIReader: kube, StatusWriter: fakeStatusWriter{client: kube}}
		result, err := r.reapAlreadyDone(context.Background(), run)
		if err != nil {
			t.Fatalf("reapAlreadyDone() error = %v", err)
		}
		if result.RequeueAfter <= 0 {
			t.Fatalf("RequeueAfter = %v; want a positive retention window", result.RequeueAfter)
		}
		stored := &courierv1alpha1.CoderRun{}
		if err := kube.Get(context.Background(), admissionKey(run.Name), stored); err != nil {
			t.Fatalf("re-read run: %v", err)
		}
		if _, ok := stored.Annotations[alreadyDoneAtAnnotation]; !ok {
			t.Fatalf("retention marker not stamped: %#v", stored.Annotations)
		}
	})
}
