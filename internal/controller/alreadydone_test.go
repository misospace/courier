package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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
			name:        "permanent refusal parks",
			closeErr:    source.ErrPermanentRefusal,
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantClose:   sourceCloseRefusedPermanentlyReason,
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

// TestRefusedCloseParksWithBlockedReport proves the parked hold is published:
// a refused close parks NeedsHuman and the source then receives the ordinary
// blocked lifecycle report, so the work item is never left in an unexplained
// state and the already_done report is never re-attempted.
func TestRefusedCloseParksWithBlockedReport(t *testing.T) {
	podUID := "pod-uid-refused"
	run := admissionRun("refused", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid-refused")
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "refused-work"
	run.Status.NativeResult = &courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultNoChange,
		Reason:            "coordinator declared no change needed",
		CoordinatorPodUID: podUID,
		At:                metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)),
	}
	run.Status.NoChange = groundedNoChange(podUID)
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod := coordinatorPodWithOutcome(run, exitDeclaredNoChange, "no_change_needed", "the request looks satisfied")
	pod.UID = types.UID(podUID)
	pod.Status.Phase = corev1.PodFailed
	c := capacityTestClient(t, lane, run, pod)
	s := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: source.CloseResolution{
		Action:  source.CloseActionRefused,
		Reason:  "close policy not satisfied",
		Reasons: []string{"excerpt does not occur in the file at the pinned head"},
	}}
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           c,
		APIReader:        c,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": s}),
		Now:              func() time.Time { return time.Date(2026, 10, 26, 12, 0, 1, 0, time.UTC) },
		WorkItemComments: commenter,
	}
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(context.Background(), admissionRequest("refused")); err != nil {
			t.Fatalf("Reconcile[%d] error = %v", i, err)
		}
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), admissionKey("refused"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %s; want NeedsHuman", stored.Status.Phase)
	}
	if len(s.closeCalls) != 1 {
		t.Fatalf("close calls = %d; want exactly one already_done attempt", len(s.closeCalls))
	}
	var blocked *source.Lifecycle
	for i := range s.reports {
		if s.reports[i].Result == source.ResultBlocked {
			blocked = &s.reports[i]
		}
	}
	if blocked == nil {
		t.Fatalf("reports = %v; want the parked hold published as a blocked report", s.events)
	}
	if !strings.Contains(blocked.Error, "excerpt does not occur") {
		t.Fatalf("blocked report error = %q; want the source's specific refusal", blocked.Error)
	}
	if blocked.IdempotencyKey == s.closeCalls[0].IdempotencyKey {
		t.Fatal("the blocked hold must carry its own report identity, not the close key")
	}
	if len(commenter.bodies) != 1 {
		t.Fatalf("comment bodies = %d; want one refusal announcement", len(commenter.bodies))
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, sourceCloseCommentedCondition); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SourceCloseCommented = %#v; want the announcement marker persisted", cond)
	}
}

// TestAlreadyDonePendingReportRepresentsThroughReconcile covers the crash
// window where the AlreadyDone phase patch landed but the close report never
// published: a plain reconcile (no coordinator pod) must re-present the same
// grounded proposal under the same stable idempotency key.
func TestAlreadyDonePendingReportRepresentsThroughReconcile(t *testing.T) {
	run := runWithNoChange("pod-1")
	run.Spec.Source = "test"
	run.UID = types.UID("run-uid-pending")
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               "LifecycleReported",
		Status:             metav1.ConditionFalse,
		Reason:             "Pending",
		Message:            "pending",
		LastTransitionTime: metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)),
	})
	kube := capacityTestClient(t, admissionLane("local", 1), run)
	s := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: source.CloseResolution{Action: source.CloseActionClosed}}
	r := &CoderRunReconciler{
		Client:    kube,
		APIReader: kube,
		Sources:   NewSourceRegistry(map[string]source.Adapter{"test": s}),
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background(), admissionRequest(run.Name)); err != nil {
			t.Fatalf("Reconcile[%d] error = %v", i, err)
		}
	}
	if len(s.closeCalls) != 1 {
		t.Fatalf("close calls = %d; want the settled close published exactly once", len(s.closeCalls))
	}
	key := s.closeCalls[0].IdempotencyKey
	if !strings.Contains(key, "/AlreadyDone/") || !strings.Contains(key, "run-uid-pending") {
		t.Fatalf("idempotency key = %q; want the phase- and UID-scoped key", key)
	}
	if s.closeCalls[0].Close == nil || len(s.closeCalls[0].Close.Criteria) != 1 {
		t.Fatalf("re-presented proposal = %#v; want the durable grounded criteria", s.closeCalls[0].Close)
	}
}

// TestGroundedNoChangeSurvivesCacheLag proves the exit path consults the API
// server before declaring a conclusion evidence-incomplete: when the cached
// run has not yet observed the incarnation's fenced pre-exit write but the
// API server has, the ending still upgrades to AlreadyDone.
func TestGroundedNoChangeSurvivesCacheLag(t *testing.T) {
	podUID := "pod-uid-lag"
	cached := admissionRun("lagging", "pool-a", courierv1alpha1.PhaseRunning)
	cached.UID = types.UID("run-uid-lag")
	cached.Spec.Source = "test"
	cached.Spec.WorkItemID = "lagging-work"
	cached.Status.NativeResult = &courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultNoChange,
		Reason:            "coordinator declared no change needed",
		CoordinatorPodUID: podUID,
		At:                metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)),
	}
	live := cached.DeepCopy()
	live.Status.NoChange = groundedNoChange(podUID)
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod := coordinatorPodWithOutcome(cached, exitDeclaredNoChange, "no_change_needed", "the request looks satisfied")
	pod.UID = types.UID(podUID)
	pod.Status.Phase = corev1.PodFailed
	cachedClient := capacityTestClient(t, lane, cached, pod)
	liveClient := capacityTestClient(t, lane, live, pod)
	s := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: source.CloseResolution{Action: source.CloseActionClosed}}
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           cachedClient,
		APIReader:        liveClient,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": s}),
		Now:              func() time.Time { return time.Date(2026, 10, 26, 12, 0, 1, 0, time.UTC) },
		WorkItemComments: commenter,
	}
	if _, err := r.Reconcile(context.Background(), admissionRequest("lagging")); err != nil {
		t.Fatalf("Reconcile error = %v", err)
	}
	if len(s.closeCalls) != 1 {
		t.Fatalf("close calls = %d; want the API-server proposal honored despite the cache lag", len(s.closeCalls))
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := cachedClient.Get(context.Background(), admissionKey("lagging"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseAlreadyDone {
		t.Fatalf("phase = %s; want AlreadyDone", stored.Status.Phase)
	}
}

// TestStaleFencedProposalNeverHonored proves the refresh path cannot adopt a
// proposal fenced to a different incarnation, whether read from the cache or
// the API server, and that the hold comment names the superseded execution
// instead of claiming nothing was grounded.
func TestStaleFencedProposalNeverHonored(t *testing.T) {
	podUID := "pod-uid-current"
	base := admissionRun("stale", "pool-a", courierv1alpha1.PhaseRunning)
	base.UID = types.UID("run-uid-stale")
	base.Spec.Source = "test"
	base.Spec.WorkItemID = "stale-work"
	base.Status.NativeResult = &courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultNoChange,
		Reason:            "coordinator declared no change needed",
		CoordinatorPodUID: podUID,
		At:                metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)),
	}
	cached := base.DeepCopy()
	cached.Status.NoChange = groundedNoChange("pod-uid-dead")
	live := cached.DeepCopy()
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod := coordinatorPodWithOutcome(cached, exitDeclaredNoChange, "no_change_needed", "the request looks satisfied")
	pod.UID = types.UID(podUID)
	pod.Status.Phase = corev1.PodFailed
	cachedClient := capacityTestClient(t, lane, cached, pod)
	liveClient := capacityTestClient(t, lane, live, pod)
	s := &closeReportingSource{admissionSource: &admissionSource{}}
	commenter := &fakeWorkItemCommenter{}
	r := &CoderRunReconciler{
		Client:           cachedClient,
		APIReader:        liveClient,
		Sources:          NewSourceRegistry(map[string]source.Adapter{"test": s}),
		Now:              func() time.Time { return time.Date(2026, 10, 26, 12, 0, 1, 0, time.UTC) },
		WorkItemComments: commenter,
	}
	if _, err := r.Reconcile(context.Background(), admissionRequest("stale")); err != nil {
		t.Fatalf("Reconcile error = %v", err)
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := cachedClient.Get(context.Background(), admissionKey("stale"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %s; want NeedsHuman — a foreign fence never authorizes the close", stored.Status.Phase)
	}
	if len(s.closeCalls) != 0 {
		t.Fatalf("close calls = %d; want 0", len(s.closeCalls))
	}
	if len(commenter.bodies) != 1 || !strings.Contains(commenter.bodies[0], "superseded execution") {
		t.Fatalf("comment bodies = %q; want the superseded-execution explanation", commenter.bodies)
	}
}

// TestNoChangeHoldOnReaderFailure fails closed: an API-server re-read error
// must not stop terminalization, and the ending holds for a human.
func TestNoChangeHoldOnReaderFailure(t *testing.T) {
	podUID := "pod-uid-readerr"
	run := admissionRun("readerr", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid-readerr")
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "readerr-work"
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
	failing := interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*courierv1alpha1.CoderRun); ok && key.Name == "readerr" {
			return fmt.Errorf("injected read failure")
		}
		return cl.Get(ctx, key, obj, opts...)
	}}
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	failingClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}, &courierv1alpha1.LaneProfile{}).
		WithRuntimeObjects(lane, run, pod).
		WithInterceptorFuncs(failing).
		Build()
	s := &closeReportingSource{admissionSource: &admissionSource{}}
	r := &CoderRunReconciler{
		Client:    c,
		APIReader: failingClient,
		Sources:   NewSourceRegistry(map[string]source.Adapter{"test": s}),
	}
	if _, err := r.Reconcile(context.Background(), admissionRequest("readerr")); err != nil {
		t.Fatalf("Reconcile error = %v", err)
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), admissionKey("readerr"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %s; want NeedsHuman fail-closed", stored.Status.Phase)
	}
	if len(s.closeCalls) != 0 {
		t.Fatalf("close calls = %d; want 0", len(s.closeCalls))
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

// TestLegacyNoChangeEndingHoldsAndAnnounces drives the legacy shim's ending:
// a resolve-issue run whose coordinator exits 3 with no trusted native result
// and no durable proposal parks NeedsHuman (never AwaitingReview, never a
// PR-less in-review state, #720), the source's close path is never called, the
// source receives the ordinary needs-human park, and the work item receives
// exactly one explanation that no PR is coming and no closure was attempted.
func TestLegacyNoChangeEndingHoldsAndAnnounces(t *testing.T) {
	run := admissionRun("legacy-hold", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid-legacy")
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "legacy-hold-work"
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod := coordinatorPod(run, exitDeclaredNoChange)
	pod.UID = types.UID("pod-uid-legacy")
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
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background(), admissionRequest("legacy-hold")); err != nil {
			t.Fatalf("Reconcile[%d] error = %v", i, err)
		}
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), admissionKey("legacy-hold"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %s; want NeedsHuman — the legacy exit never claims in-review", stored.Status.Phase)
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, sourceCloseCondition); cond != nil {
		t.Fatalf("SourceClose = %#v; want none — no close was attempted", cond)
	}
	if len(s.closeCalls) != 0 {
		t.Fatalf("close calls = %d; want 0 without a grounded proposal", len(s.closeCalls))
	}
	if len(s.transitions) != 1 || s.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("source transitions = %#v; want one needs-human park", s.transitions)
	}
	if len(s.reports) != 1 || s.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v; want one blocked hold report", s.reports)
	}
	if len(commenter.bodies) != 1 || !strings.Contains(commenter.bodies[0], "no grounded close evidence") {
		t.Fatalf("comment bodies = %q; want exactly one hold announcement naming the missing evidence", commenter.bodies)
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, sourceCloseCommentedCondition); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SourceCloseCommented = %#v; want the announcement marker persisted", cond)
	}
}

// TestTrustedPublishedResultNotHijackedByNoChangeExit pins the guard on the
// legacy no-change route: when a trusted native result fences to the
// terminating pod, it classifies the ending — a published result on an exit-3
// termination still routes to Verifying, never the human hold, the close
// path, or an announcement. The exit code alone never reclassifies a trusted
// ending.
func TestTrustedPublishedResultNotHijackedByNoChangeExit(t *testing.T) {
	podUID := "pod-uid-published-exit3"
	run := admissionRun("published-exit3", "pool-a", courierv1alpha1.PhaseRunning)
	run.UID = types.UID("run-uid-published-exit3")
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "published-exit3-work"
	run.Status.NativeResult = &courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultPublished,
		Reason:            "published through the broker",
		CoordinatorPodUID: podUID,
		At:                metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)),
	}
	lane := admissionLaneWithCoordinator("pool-a", 1)
	pod := coordinatorPod(run, exitDeclaredNoChange)
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
	if _, err := r.Reconcile(context.Background(), admissionRequest("published-exit3")); err != nil {
		t.Fatalf("Reconcile error = %v", err)
	}
	stored := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), admissionKey("published-exit3"), stored); err != nil {
		t.Fatalf("re-read run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase = %s; want Verifying — the trusted published result classifies, the exit code does not", stored.Status.Phase)
	}
	if len(s.closeCalls) != 0 || len(commenter.bodies) != 0 {
		t.Fatalf("close calls = %d comment bodies = %q; want neither for a published ending", len(s.closeCalls), commenter.bodies)
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, sourceCloseCommentedCondition); cond != nil {
		t.Fatalf("SourceCloseCommented = %#v; want none", cond)
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

// fakeLinks renders forge.test-shaped web links for the settled-outcome
// announcement. A link is unrenderable (empty) when any part of its identity
// is missing, and omitFile names one path whose file link cannot be rendered.
type fakeLinks struct {
	omitFile string
}

func (l *fakeLinks) CommitLink(repo, sha string) string {
	if repo == "" || sha == "" {
		return ""
	}
	return "https://forge.test/" + repo + "/commit/" + sha
}

func (l *fakeLinks) FileLink(repo, path, sha string) string {
	if repo == "" || path == "" || sha == "" || path == l.omitFile {
		return ""
	}
	return "https://forge.test/" + repo + "/blob/" + sha + "/" + path
}

// TestAlreadyDoneCommentRedactsSourceAndModelText is the redaction gate: model
// authored evidence, criterion text, and paths, plus the source's own
// resolution detail, must never carry a credential into the forge comment, on
// either the closed or the refused (parked) announcement.
func TestAlreadyDoneCommentRedactsSourceAndModelText(t *testing.T) {
	const (
		ghpToken  = "ghp_a1B2c3D4e5F6g7H8i9J0KlMn"
		bearerTok = "abcdef0123456789ABCDEF"
	)
	tainted := func() *courierv1alpha1.NoChangeClose {
		return &courierv1alpha1.NoChangeClose{
			Criteria: []courierv1alpha1.NoChangeCloseCriterion{
				{
					Criterion: "the health endpoint proves authorization: bearer " + bearerTok + " is provisioned",
					Path:      "docs/https://api:s3cretValue@internal.example.com/ask.md",
				},
				{
					Criterion: "the reviewer note is archived",
					Path:      "notes/" + ghpToken + "/review.md",
				},
				{
					Criterion: "the clean path still links",
					Path:      "src/clean.go",
				},
			},
			Evidence:          "covered per https://user:hunter2@github.com/acme/demo; reviewer token " + ghpToken + " confirmed it",
			CoordinatorPodUID: "pod-1",
			At:                metav1.Now(),
		}
	}
	cases := []struct {
		name       string
		resolution source.CloseResolution
	}{
		{"closed announcement", source.CloseResolution{Action: source.CloseActionClosed, Reason: "closed after checking https://tok:pwd@example.com/x", HeadSHA: "0a1b2c3d"}},
		{"refused park announcement", source.CloseResolution{Action: source.CloseActionRefused, Reason: "policy check against https://tok:pwd@example.com/x declined", HeadSHA: "0a1b2c3d"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := runWithNoChange("pod-1")
			run.Status.NoChange = tainted()
			kube := phaseClient(t, admissionLane("local", 1), run)
			adapter := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: tc.resolution}
			commenter := &fakeWorkItemCommenter{}
			r := &CoderRunReconciler{
				Client:           kube,
				APIReader:        kube,
				Sources:          NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
				StatusWriter:     fakeStatusWriter{client: kube},
				WorkItemComments: commenter,
				WorkItemLinks:    &fakeLinks{},
			}
			item := source.WorkItem{ID: run.Spec.WorkItemID, Repo: run.Spec.Repo, Ref: run.Spec.Ref, Mode: string(run.Spec.Mode), Lane: run.Spec.Lane}
			if _, err := r.settleAlreadyDoneClose(context.Background(), run, adapter, item, terminalLifecycleIntent{error: "no-change"}); err != nil {
				t.Fatalf("settleAlreadyDoneClose() error = %v", err)
			}
			if len(commenter.bodies) != 1 {
				t.Fatalf("comment bodies = %d; want 1", len(commenter.bodies))
			}
			body := commenter.bodies[0]
			if !strings.Contains(body, "[REDACTED]") {
				t.Fatalf("body = %q; want a redaction placeholder", body)
			}
			for _, leak := range []string{"hunter2", ghpToken, bearerTok, "tok:pwd", "s3cretValue"} {
				if strings.Contains(body, leak) {
					t.Fatalf("body leaked %q (href included): %q", leak, body)
				}
			}
			// A redacted path never travels into an href; a clean path still
			// renders its pinned link.
			if strings.Contains(body, "(in [docs/") || strings.Contains(body, "(in [notes/") {
				t.Fatalf("body = %q; want the redacted paths plain, never linked", body)
			}
			if !strings.Contains(body, "[src/clean.go](https://forge.test/acme/widgets/blob/0a1b2c3d/src/clean.go)") {
				t.Fatalf("body = %q; want the clean path still linked", body)
			}
		})
	}
}

// TestAlreadyDoneCommentRendersStructuredLinks checks the renderer's link
// contract: the source-pinned verification revision and every grounded
// criterion path render as forge links ahead of the free-form assessment,
// with plain-text fallbacks when the linker cannot render a link, and no
// verification block at all when the source pinned no revision.
func TestAlreadyDoneCommentRendersStructuredLinks(t *testing.T) {
	run := runWithNoChange("pod-1")
	resolution := source.CloseResolution{Action: source.CloseActionClosed, HeadSHA: "0a1b2c3d", EvidenceDigest: "sha256:9f8e7d"}

	t.Run("linker renders commit and file links", func(t *testing.T) {
		body := alreadyDoneCommentBody(run, true, sourceCloseClosedReason, resolution, &fakeLinks{})
		if !strings.Contains(body, "[acme/widgets@0a1b2c3d](https://forge.test/acme/widgets/commit/0a1b2c3d)") {
			t.Fatalf("body = %q; want the commit link at the pinned revision", body)
		}
		if !strings.Contains(body, "[pkg/core/impl.go](https://forge.test/acme/widgets/blob/0a1b2c3d/pkg/core/impl.go)") {
			t.Fatalf("body = %q; want the criterion path link at the pinned revision", body)
		}
		if !strings.Contains(body, "Evidence catalog: sha256:9f8e7d") {
			t.Fatalf("body = %q; want the evidence catalog digest", body)
		}
		commit := strings.Index(body, "commit/0a1b2c3d")
		criteria := strings.Index(body, "Grounded criteria:")
		assessment := strings.Index(body, "Coordinator assessment:")
		if !(commit >= 0 && commit < criteria && criteria < assessment) {
			t.Fatalf("render order = commit %d, criteria %d, assessment %d; want links before prose", commit, criteria, assessment)
		}
	})

	t.Run("nil linker renders plain text", func(t *testing.T) {
		body := alreadyDoneCommentBody(run, true, sourceCloseClosedReason, resolution, nil)
		if !strings.Contains(body, "Verified at acme/widgets@0a1b2c3d.") {
			t.Fatalf("body = %q; want the plain repo@sha revision", body)
		}
		if !strings.Contains(body, "Evidence catalog: sha256:9f8e7d") {
			t.Fatalf("body = %q; want the evidence digest even without a linker", body)
		}
		if !strings.Contains(body, "(in pkg/core/impl.go)") {
			t.Fatalf("body = %q; want the plain criterion path", body)
		}
		if strings.Contains(body, "](") {
			t.Fatalf("body = %q; want no markdown link without a linker", body)
		}
	})

	t.Run("unrenderable path falls back to plain text", func(t *testing.T) {
		body := alreadyDoneCommentBody(run, true, sourceCloseClosedReason, resolution, &fakeLinks{omitFile: "pkg/core/impl.go"})
		if !strings.Contains(body, "[acme/widgets@0a1b2c3d](https://forge.test/acme/widgets/commit/0a1b2c3d)") {
			t.Fatalf("body = %q; want the commit link even when the file link fails", body)
		}
		if !strings.Contains(body, "(in pkg/core/impl.go)") || strings.Contains(body, "blob/") {
			t.Fatalf("body = %q; want the plain path fallback for the unrenderable path", body)
		}
	})

	t.Run("an irregular revision omits the verification block", func(t *testing.T) {
		body := alreadyDoneCommentBody(run, true, sourceCloseClosedReason,
			source.CloseResolution{Action: source.CloseActionClosed, HeadSHA: "abc] [x](https://evil.test/y)", EvidenceDigest: "sha256:9f8e7d"}, &fakeLinks{})
		if strings.Contains(body, "Verified at") || strings.Contains(body, "evil.test") || strings.Contains(body, "Evidence catalog") {
			t.Fatalf("body = %q; want an irregular revision omitted, never rendered", body)
		}
		if strings.Contains(body, "](") {
			t.Fatalf("body = %q; want no links built from an irregular revision", body)
		}
	})

	t.Run("no pinned revision renders no links", func(t *testing.T) {
		body := alreadyDoneCommentBody(run, true, sourceCloseClosedReason,
			source.CloseResolution{Action: source.CloseActionClosed, EvidenceDigest: "sha256:9f8e7d"}, &fakeLinks{})
		if strings.Contains(body, "Verified at") || strings.Contains(body, "Evidence catalog") {
			t.Fatalf("body = %q; want no verification block without a pinned revision", body)
		}
		if strings.Contains(body, "](") {
			t.Fatalf("body = %q; want no unpinned links — unpinned paths drift", body)
		}
		if !strings.Contains(body, "Grounded criteria: the ask is implemented (in pkg/core/impl.go)") {
			t.Fatalf("body = %q; want plain grounded criteria", body)
		}
	})
}

// TestParkAlreadyDoneCloseKeepsPinnedRevision proves the park path carries the
// source's real resolution — not a detail-string reconstruction — into the
// announcement: a refused close still names the verification revision the
// source pinned, with its links. A park with no source resolution at all
// invents no verification block.
func TestParkAlreadyDoneCloseKeepsPinnedRevision(t *testing.T) {
	t.Run("refused park keeps the source-pinned revision", func(t *testing.T) {
		run := runWithNoChange("pod-1")
		kube := phaseClient(t, admissionLane("local", 1), run)
		adapter := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: source.CloseResolution{
			Action:         source.CloseActionRefused,
			Reason:         "close policy not satisfied",
			HeadSHA:        "11aa22bb",
			EvidenceDigest: "sha256:cafe",
		}}
		commenter := &fakeWorkItemCommenter{}
		r := &CoderRunReconciler{
			Client:           kube,
			APIReader:        kube,
			Sources:          NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
			StatusWriter:     fakeStatusWriter{client: kube},
			WorkItemComments: commenter,
			WorkItemLinks:    &fakeLinks{},
		}
		item := source.WorkItem{ID: run.Spec.WorkItemID, Repo: run.Spec.Repo, Ref: run.Spec.Ref, Mode: string(run.Spec.Mode), Lane: run.Spec.Lane}
		if _, err := r.settleAlreadyDoneClose(context.Background(), run, adapter, item, terminalLifecycleIntent{error: "no-change"}); err != nil {
			t.Fatalf("settleAlreadyDoneClose() error = %v", err)
		}
		if run.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
			t.Fatalf("phase = %s; want NeedsHuman", run.Status.Phase)
		}
		if len(commenter.bodies) != 1 {
			t.Fatalf("comment bodies = %d; want 1", len(commenter.bodies))
		}
		body := commenter.bodies[0]
		if !strings.Contains(body, "Verified at [acme/widgets@11aa22bb](https://forge.test/acme/widgets/commit/11aa22bb)") {
			t.Fatalf("body = %q; want the verified-at block at the source-pinned revision", body)
		}
		if !strings.Contains(body, "[pkg/core/impl.go](https://forge.test/acme/widgets/blob/11aa22bb/pkg/core/impl.go)") {
			t.Fatalf("body = %q; want criterion paths linked at the pinned revision", body)
		}
		if !strings.Contains(body, "Evidence catalog: sha256:cafe") {
			t.Fatalf("body = %q; want the evidence digest on the parked announcement", body)
		}
	})

	t.Run("park with no source resolution invents no revision", func(t *testing.T) {
		run := runWithNoChange("pod-1")
		kube := phaseClient(t, admissionLane("local", 1), run)
		adapter := &admissionSource{}
		commenter := &fakeWorkItemCommenter{}
		r := &CoderRunReconciler{
			Client:           kube,
			APIReader:        kube,
			Sources:          NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
			StatusWriter:     fakeStatusWriter{client: kube},
			WorkItemComments: commenter,
			WorkItemLinks:    &fakeLinks{},
		}
		item := source.WorkItem{ID: run.Spec.WorkItemID, Repo: run.Spec.Repo, Ref: run.Spec.Ref, Mode: string(run.Spec.Mode), Lane: run.Spec.Lane}
		if _, err := r.settleAlreadyDoneClose(context.Background(), run, adapter, item, terminalLifecycleIntent{error: "no-change"}); err != nil {
			t.Fatalf("settleAlreadyDoneClose() error = %v", err)
		}
		if len(commenter.bodies) != 1 {
			t.Fatalf("comment bodies = %d; want 1", len(commenter.bodies))
		}
		body := commenter.bodies[0]
		if strings.Contains(body, "Verified at") || strings.Contains(body, "](") {
			t.Fatalf("body = %q; want no invented revision without a source resolution", body)
		}
		if !strings.Contains(body, "evidence-gated close path") {
			t.Fatalf("body = %q; want the source's detail in the refusal announcement", body)
		}
	})
}

// TestAlreadyDoneAnnouncementRecordsUndelivered proves a run is never left
// reading as announced when no comment could reach the work item (#727): a
// missing comment identity or a failed delivery records the announcement
// condition False with the reason why — never claimed as posted. The recorded
// failure detail is redacted like every other non-operator fragment.
func TestAlreadyDoneAnnouncementRecordsUndelivered(t *testing.T) {
	t.Run("closed settle with failed delivery records DeliveryFailed", func(t *testing.T) {
		run := runWithNoChange("pod-1")
		kube := phaseClient(t, admissionLane("local", 1), run)
		adapter := &closeReportingSource{admissionSource: &admissionSource{}, closeResolution: source.CloseResolution{Action: source.CloseActionClosed}}
		commenter := &fakeWorkItemCommenter{err: errors.New("post https://user:hunter2@forge.test/x returned 401")}
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
		stored := &courierv1alpha1.CoderRun{}
		if err := kube.Get(context.Background(), admissionKey(run.Name), stored); err != nil {
			t.Fatalf("re-read run: %v", err)
		}
		cond := apimeta.FindStatusCondition(stored.Status.Conditions, sourceCloseCommentedCondition)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != sourceCloseCommentDeliveryReason {
			t.Fatalf("SourceCloseCommented = %#v; want False/%s", cond, sourceCloseCommentDeliveryReason)
		}
		if strings.Contains(cond.Message, "hunter2") || !strings.Contains(cond.Message, "[REDACTED]") {
			t.Fatalf("delivery record leaked or lost redaction: %q", cond.Message)
		}
	})

	t.Run("park without a comment identity records not-configured", func(t *testing.T) {
		run := runWithNoChange("pod-1")
		kube := phaseClient(t, admissionLane("local", 1), run)
		adapter := &admissionSource{}
		r := &CoderRunReconciler{
			Client:       kube,
			APIReader:    kube,
			Sources:      NewSourceRegistry(map[string]source.Adapter{"test": adapter}),
			StatusWriter: fakeStatusWriter{client: kube},
		}
		item := source.WorkItem{ID: run.Spec.WorkItemID, Repo: run.Spec.Repo, Ref: run.Spec.Ref, Mode: string(run.Spec.Mode), Lane: run.Spec.Lane}
		if _, err := r.settleAlreadyDoneClose(context.Background(), run, adapter, item, terminalLifecycleIntent{error: "no-change"}); err != nil {
			t.Fatalf("settleAlreadyDoneClose() error = %v", err)
		}
		stored := &courierv1alpha1.CoderRun{}
		if err := kube.Get(context.Background(), admissionKey(run.Name), stored); err != nil {
			t.Fatalf("re-read run: %v", err)
		}
		cond := apimeta.FindStatusCondition(stored.Status.Conditions, sourceCloseCommentedCondition)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != sourceCloseCommentNotConfiguredReason {
			t.Fatalf("SourceCloseCommented = %#v; want False/%s", cond, sourceCloseCommentNotConfiguredReason)
		}
	})

	t.Run("hold with a failed delivery records DeliveryFailed", func(t *testing.T) {
		run := admissionRun("hold-fail", "local", courierv1alpha1.PhaseNeedsHuman)
		kube := phaseClient(t, admissionLane("local", 1), run)
		commenter := &fakeWorkItemCommenter{err: errors.New("forge unavailable")}
		r := &CoderRunReconciler{
			Client:           kube,
			APIReader:        kube,
			StatusWriter:     fakeStatusWriter{client: kube},
			WorkItemComments: commenter,
		}
		r.commentAlreadyDoneHold(context.Background(), run)
		stored := &courierv1alpha1.CoderRun{}
		if err := kube.Get(context.Background(), admissionKey("hold-fail"), stored); err != nil {
			t.Fatalf("re-read run: %v", err)
		}
		cond := apimeta.FindStatusCondition(stored.Status.Conditions, sourceCloseCommentedCondition)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != sourceCloseCommentDeliveryReason {
			t.Fatalf("SourceCloseCommented = %#v; want False/%s", cond, sourceCloseCommentDeliveryReason)
		}
	})

	t.Run("a later successful delivery replaces the failure record", func(t *testing.T) {
		run := runWithNoChange("pod-1")
		apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type:               sourceCloseCommentedCondition,
			Status:             metav1.ConditionFalse,
			Reason:             sourceCloseCommentDeliveryReason,
			Message:            "announcement delivery failed: transient",
			LastTransitionTime: metav1.NewTime(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)),
		})
		commenter := &fakeWorkItemCommenter{}
		r := &CoderRunReconciler{
			WorkItemComments: commenter,
			Now:              func() time.Time { return time.Date(2026, 10, 26, 12, 1, 0, 0, time.UTC) },
		}
		r.commentAlreadyDone(context.Background(), run, true, sourceCloseClosedReason, source.CloseResolution{Action: source.CloseActionClosed})
		if len(commenter.bodies) != 1 {
			t.Fatalf("comment bodies = %d; want the retry to deliver", len(commenter.bodies))
		}
		if cond := apimeta.FindStatusCondition(run.Status.Conditions, sourceCloseCommentedCondition); cond == nil || cond.Status != metav1.ConditionTrue {
			t.Fatalf("SourceCloseCommented = %#v; want the True marker after delivery", cond)
		}
	})
}
