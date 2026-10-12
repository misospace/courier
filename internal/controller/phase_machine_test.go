package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

type admissionSource struct {
	claimed       []string
	released      []string
	transitions   []source.State
	reports       []source.Lifecycle
	events        []string
	resolved      []string
	preLaunchErr  error
	preLaunches   []string
	transitionErr error
	reportErr     error
	reportErrors  []error
}

type fakeWorldObserver struct {
	observation PRObservation
	err         error
	calls       *int
}

func (o fakeWorldObserver) Observe(context.Context, string, HeadRef) (PRObservation, error) {
	if o.calls != nil {
		*o.calls = *o.calls + 1
	}
	return o.observation, o.err
}

func (s *admissionSource) Discover(context.Context) ([]source.WorkItem, error) { return nil, nil }
func (s *admissionSource) Claim(_ context.Context, item source.WorkItem) error {
	s.claimed = append(s.claimed, item.ID)
	return nil
}
func (s *admissionSource) Release(_ context.Context, item source.WorkItem) error {
	s.released = append(s.released, item.ID)
	return nil
}
func (s *admissionSource) Transition(_ context.Context, _ source.WorkItem, state source.State) error {
	s.transitions = append(s.transitions, state)
	s.events = append(s.events, "transition:"+string(state))
	return s.transitionErr
}
func (s *admissionSource) Report(_ context.Context, _ source.WorkItem, lifecycle source.Lifecycle) error {
	s.reports = append(s.reports, lifecycle)
	s.events = append(s.events, "report:"+string(lifecycle.Result))
	if len(s.reportErrors) > 0 {
		err := s.reportErrors[0]
		s.reportErrors = s.reportErrors[1:]
		return err
	}
	return s.reportErr
}
func (s *admissionSource) PreLaunch(_ context.Context, item source.WorkItem) error {
	s.preLaunches = append(s.preLaunches, item.ID)
	return s.preLaunchErr
}
func (s *admissionSource) Resolve(_ context.Context, item source.WorkItem) error {
	s.resolved = append(s.resolved, item.ID)
	return nil
}

func TestSourceRegistryUsesSpecSourceAndDurableWorkItemID(t *testing.T) {
	first := &admissionSource{}
	second := &admissionSource{}
	registry := NewSourceRegistry(map[string]source.Adapter{"first:worker": first, "second": second})
	if got, err := registry.lookup("first:worker"); err != nil || got != first {
		t.Fatalf("lookup(first) = %v, %v; want first adapter", got, err)
	}
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	run.Spec.Source = "first"
	run.Spec.SourceAgent = "worker"
	run.Spec.WorkItemID = "opaque/source/id"
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{Client: client, Sources: registry, StatusWriter: fakeStatusWriter{client: client}}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(first.claimed) != 1 || first.claimed[0] != "opaque/source/id" {
		t.Fatalf("claimed IDs = %#v, want durable source ID", first.claimed)
	}
	if len(second.claimed) != 0 {
		t.Fatalf("wrong adapter claimed work: %#v", second.claimed)
	}
}

func TestSourceRegistryQualifiedIdentityFailsClosedAndLegacyRemainsBare(t *testing.T) {
	legacy := &admissionSource{}
	qualified := &admissionSource{}
	registry := NewSourceRegistry(map[string]source.Adapter{"dispatch": legacy, "dispatch:courier-local": qualified})
	if got, err := registry.lookupRun(&courierv1alpha1.CoderRun{Spec: courierv1alpha1.CoderRunSpec{Source: "dispatch", SourceAgent: "courier-local"}}); err != nil || got != qualified {
		t.Fatalf("qualified lookup = %v, %v; want qualified adapter", got, err)
	}
	if _, err := registry.lookupRun(&courierv1alpha1.CoderRun{Spec: courierv1alpha1.CoderRunSpec{Source: "dispatch", SourceAgent: "missing"}}); !errors.Is(err, ErrUnknownSource) {
		t.Fatalf("unknown qualified lookup error = %v, want fail-closed unknown source", err)
	}
	if got, err := registry.lookupRun(&courierv1alpha1.CoderRun{Spec: courierv1alpha1.CoderRunSpec{Source: "dispatch"}}); err != nil || got != legacy {
		t.Fatalf("legacy lookup = %v, %v; want bare adapter", got, err)
	}
}

func TestOperatorStatusPatchesPreserveHarnessFields(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	run.Status.Checkpoint = &courierv1alpha1.Checkpoint{Plan: "keep this plan"}
	run.Status.Heartbeat = &courierv1alpha1.Heartbeat{Kind: "stream"}
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Checkpoint == nil || updated.Status.Checkpoint.Plan != "keep this plan" {
		t.Fatalf("checkpoint = %#v, operator patch overwrote harness checkpoint", updated.Status.Checkpoint)
	}
	if updated.Status.Heartbeat == nil || updated.Status.Heartbeat.Kind != "stream" {
		t.Fatalf("heartbeat = %#v, operator patch overwrote harness heartbeat", updated.Status.Heartbeat)
	}
}

func TestPendingClaimsBeforeBranchResolutionAndLaunch(t *testing.T) {
	item := &admissionSource{}
	var order []string
	resolver := ExistingPRHeadResolverFunc(func(_ context.Context, run *courierv1alpha1.CoderRun) (HeadRef, error) {
		order = append(order, "resolve")
		return HeadRef{Repo: run.Spec.Repo, Branch: "feature/existing-pr", SHA: "abc123"}, nil
	})
	run := admissionRun("fix", "local", courierv1alpha1.PhasePending)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	run.Spec.Source = "dispatch"
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:         client,
		Sources:        NewSourceRegistry(map[string]source.Adapter{"dispatch": item}),
		PRHeadResolver: resolver,
		StatusWriter:   fakeStatusWriter{client: client},
		Launch: func(_ context.Context, run *courierv1alpha1.CoderRun) error {
			order = append(order, "launch")
			run.Status.Phase = courierv1alpha1.PhaseRunning
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("fix")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(item.claimed) != 1 || len(item.reports) != 0 || len(order) != 2 || order[0] != "resolve" || order[1] != "launch" {
		t.Fatalf("claim/resolve/launch order = claim %#v, order %#v", item.claimed, order)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("fix"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Branch != "feature/existing-pr" || updated.Status.HeadRepo != "acme/widgets" || updated.Status.HeadSHA != "abc123" {
		t.Fatalf("head = branch %q repo %q sha %q, want existing PR head", updated.Status.Branch, updated.Status.HeadRepo, updated.Status.HeadSHA)
	}
}

func TestPreLaunchStaleWorkReleasesClaimAndDeletesRun(t *testing.T) {
	item := &admissionSource{preLaunchErr: source.ErrStaleWork}
	launched := false
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch: func(context.Context, *courierv1alpha1.CoderRun) error {
			launched = true
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err == nil {
		t.Fatal("Reconcile() returned nil error, want guard error")
	}
	if launched || len(item.released) != 1 || len(item.preLaunches) != 1 || len(item.transitions) != 0 {
		t.Fatalf("guard outcome = launched %t, released %#v, pre-launches %#v, transitions %#v", launched, item.released, item.preLaunches, item.transitions)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); !apierrors.IsNotFound(err) {
		t.Fatalf("get run: %v, want CoderRun deleted after stale pre-launch", err)
	}
}

func TestPreLaunchTransientFailureKeepsRunPending(t *testing.T) {
	cause := errors.New("pre-launch revalidation failed")
	item := &admissionSource{preLaunchErr: cause}
	launched := false
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch: func(context.Context, *courierv1alpha1.CoderRun) error {
			launched = true
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); !errors.Is(err, cause) {
		t.Fatalf("Reconcile() error = %v, want %v", err, cause)
	}
	if launched || len(item.released) != 1 || len(item.transitions) != 0 {
		t.Fatalf("guard outcome = launched %t, released %#v, transitions %#v", launched, item.released, item.transitions)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhasePending {
		t.Fatalf("phase = %q, want Pending", updated.Status.Phase)
	}
	if updated.Status.HeadRepo != "" || updated.Status.HeadSHA != "" {
		t.Fatalf("run after rejected claim = head repo %q sha %q, want empty", updated.Status.HeadRepo, updated.Status.HeadSHA)
	}
}

func TestPreLaunchStaleOnClaimedRunReleasesAndDeletes(t *testing.T) {
	item := &admissionSource{preLaunchErr: source.ErrStaleWork}
	launched := false
	run := admissionRun("run", "local", courierv1alpha1.PhaseClaimed)
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch: func(context.Context, *courierv1alpha1.CoderRun) error {
			launched = true
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err == nil {
		t.Fatal("Reconcile() returned nil error, want guard error")
	}
	if launched || len(item.released) != 1 || len(item.preLaunches) != 1 || len(item.transitions) != 0 {
		t.Fatalf("resume outcome = launched %t, released %#v, pre-launches %#v, transitions %#v", launched, item.released, item.preLaunches, item.transitions)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); !apierrors.IsNotFound(err) {
		t.Fatalf("get run: %v, want CoderRun deleted after stale pre-launch resume", err)
	}
}

func TestSuspendedLaneStillLaunchesClaimedRun(t *testing.T) {
	item := &admissionSource{}
	launched := false
	lane := admissionLane("local", 1)
	lane.Annotations = map[string]string{courierv1alpha1.SuspendAnnotation: "true"}
	run := admissionRun("run", "local", courierv1alpha1.PhaseClaimed)
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, lane, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch: func(context.Context, *courierv1alpha1.CoderRun) error {
			launched = true
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !launched {
		t.Fatal("a run admitted before the lane was suspended must still launch")
	}
}

func TestFixPRRequiresInjectedHeadResolver(t *testing.T) {
	run := admissionRun("fix", "local", courierv1alpha1.PhasePending)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	_, err := resolveRunBranch(context.Background(), run, nil)
	if !errors.Is(err, ErrPRHeadResolverRequired) {
		t.Fatalf("resolveRunBranch() error = %v, want resolver-required error", err)
	}
}

func TestFixPREmptyHeadRepositoryIsGone(t *testing.T) {
	run := admissionRun("fix", "local", courierv1alpha1.PhasePending)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	resolver := ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (HeadRef, error) {
		// A deleted fork: GitHub returns head.repo = null, so the resolved
		// head carries a branch but no repository.
		return HeadRef{Branch: "fix/pr-12"}, nil
	})
	_, err := resolveRunBranch(context.Background(), run, resolver)
	if !errors.Is(err, ErrHeadRepositoryGone) {
		t.Fatalf("resolveRunBranch() error = %v, want ErrHeadRepositoryGone", err)
	}
}

func TestDeletedForkHeadTerminatesNeedsHumanWithoutReclaim(t *testing.T) {
	item := &admissionSource{reportErr: errors.New("dispatch unavailable")}
	run := admissionRun("fix", "local", courierv1alpha1.PhasePending)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:  client,
		Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}),
		PRHeadResolver: ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (HeadRef, error) {
			return HeadRef{Branch: "fix/pr-12"}, nil
		}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch: func(context.Context, *courierv1alpha1.CoderRun) error {
			t.Error("launch must not run for a missing head repository")
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("fix")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("fix"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("run after missing head repository = phase %q, want NeedsHuman (not a re-claimable Pending)", updated.Status.Phase)
	}
	if updated.Status.HeadRepo != "" || updated.Status.HeadSHA != "" || updated.Status.Branch != "" {
		t.Fatalf("run after missing head repository = branch %q repo %q sha %q, want empty", updated.Status.Branch, updated.Status.HeadRepo, updated.Status.HeadSHA)
	}
	if len(item.released) != 1 || item.released[0] != "fix" {
		t.Fatalf("released IDs = %#v, want the claim released exactly once", item.released)
	}
	if len(item.claimed) != 1 {
		t.Fatalf("claimed IDs = %#v, want a single claim", item.claimed)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want NeedsHuman reported to the source", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultBlocked {
		t.Fatalf("reports = %#v, want one blocked lifecycle report", item.reports)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("LifecycleReported = %#v, want durable pending report", cond)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("fix")); err != nil {
		t.Fatalf("retry Reconcile() error = %v", err)
	}
	if len(item.reports) != 2 {
		t.Fatalf("reports after retry = %#v, want missing-head report retried", item.reports)
	}
}

func TestLaunchFailureReleasesSourceAndCapacity(t *testing.T) {
	item := &admissionSource{}
	wantErr := errors.New("pod launch failed")
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch:       func(context.Context, *courierv1alpha1.CoderRun) error { return wantErr },
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); !errors.Is(err, wantErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, wantErr)
	}
	if len(item.released) != 1 || item.released[0] != "run" {
		t.Fatalf("released IDs = %#v, want run", item.released)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhasePending || updated.Status.Branch != "" {
		t.Fatalf("run after failed launch = phase %q branch %q, want Pending and empty branch", updated.Status.Phase, updated.Status.Branch)
	}
	if updated.Status.HeadRepo != "" || updated.Status.HeadSHA != "" {
		t.Fatalf("run after failed launch = head repo %q sha %q, want empty", updated.Status.HeadRepo, updated.Status.HeadSHA)
	}
}

func TestClaimedRunRetriesLaunchAfterPartialAdmission(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseClaimed)
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Launch: func(_ context.Context, run *courierv1alpha1.CoderRun) error {
			run.Status.Phase = courierv1alpha1.PhaseRunning
			return nil
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", updated.Status.Phase)
	}
	if len(item.claimed) != 0 || len(item.released) != 0 {
		t.Fatalf("partial-admission retry changed claim state: claimed=%#v released=%#v", item.claimed, item.released)
	}
}

func TestRunningPodExitMapsPhaseAndSourceState(t *testing.T) {
	tests := []struct {
		name                        string
		mode                        courierv1alpha1.Mode
		exitCode                    int32
		wantPhase                   courierv1alpha1.Phase
		wantSource                  source.State
		wantReport                  bool
		wantResult                  source.Result
		wantBlockedReportParksPRFix bool
	}{
		{name: "success", exitCode: 0, wantPhase: courierv1alpha1.PhaseVerifying},
		{name: "needs-human", exitCode: 2, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantSource: source.StateNeedsHuman, wantReport: true, wantResult: source.ResultBlocked},
		// no_change_needed (issue #169) never settles the source directly: a
		// resolve-issue run lands in AwaitingReview for a human to review the
		// posted evidence; a fix-pr run is settled against the world through
		// Verifying (issue #267).
		{name: "no-change-needed resolve-issue", mode: courierv1alpha1.ModeResolveIssue, exitCode: 3, wantPhase: courierv1alpha1.PhaseAwaitingReview, wantSource: source.StateInReview, wantReport: true},
		// A fix-pr no_change_needed declaration is settled against the world
		// by Verifying, not mapped straight to NeedsHuman (issue #267).
		{name: "no-change-needed fix-pr", mode: courierv1alpha1.ModeFixPR, exitCode: 3, wantPhase: courierv1alpha1.PhaseVerifying},
		{name: "failure", exitCode: 17, wantPhase: courierv1alpha1.PhaseFailed, wantSource: source.StateNeedsHuman, wantReport: true, wantResult: source.ResultBlocked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			run.Spec.Source = "test"
			run.Spec.WorkItemID = "opaque-work-item"
			if tt.mode != "" {
				run.Spec.Mode = tt.mode
			}
			client := phaseClient(t, run, coordinatorPod(run, tt.exitCode))
			calls := 0
			reconciler := &CoderRunReconciler{
				Client:       client,
				Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
				StatusWriter: fakeStatusWriter{client: client},
				Observer:     fakeWorldObserver{calls: &calls},
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != tt.wantPhase {
				t.Fatalf("phase = %q, want %q", updated.Status.Phase, tt.wantPhase)
			}
			if tt.wantSource == "" {
				if len(item.transitions) != 0 {
					t.Fatalf("source transitions = %#v, want none", item.transitions)
				}
			} else if len(item.transitions) != 1 || item.transitions[0] != tt.wantSource {
				t.Fatalf("source transitions = %#v, want %#v", item.transitions, []source.State{tt.wantSource})
			}
			if !tt.wantReport {
				if len(item.reports) != 0 {
					t.Fatalf("lifecycle reports = %#v, want none", item.reports)
				}
			} else if len(item.reports) != 1 || item.reports[0].Result != tt.wantResult || item.reports[0].BlockedReportParksPRFix != tt.wantBlockedReportParksPRFix {
				t.Fatalf("lifecycle reports = %#v, want one with result %q and blocked-report parking %t", item.reports, tt.wantResult, tt.wantBlockedReportParksPRFix)
			}
			if calls != 0 {
				t.Fatalf("observer calls = %d, want 0 before Verifying reconcile", calls)
			}
		})
	}
}

func TestBlockedExternalOutcomeDoesNotOverrideFailureExit(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "opaque-work-item"
	pod := coordinatorPodWithOutcome(run, 1, "blocked_external", "untrusted mismatch")
	client := phaseClient(t, run, pod)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed from exit-code mapping", updated.Status.Phase)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultBlocked || item.reports[0].State != source.StateNeedsHuman || item.reports[0].Error != "coordinator failed" {
		t.Fatalf("lifecycle reports = %#v, want human-blocked coordinator failure", item.reports)
	}
}

func TestRunningDirectTerminalEnrichesObservedPR(t *testing.T) {
	tests := []struct {
		name         string
		exitCode     int32
		priorPR      string
		observer     WorldObserver
		wantPhase    courierv1alpha1.Phase
		wantSource   source.State
		wantPR       string
		wantReportPR string
	}{
		{
			name:         "needs human publishes observed PR",
			wantSource:   source.StateNeedsHuman,
			exitCode:     2,
			observer:     fakeWorldObserver{observation: PRObservation{PR: "42", Checks: []CheckObservation{{Name: "check", State: CheckStateFailed}}}},
			wantPhase:    courierv1alpha1.PhaseNeedsHuman,
			wantPR:       "42",
			wantReportPR: "42",
		},
		{
			name:         "failed retains prior PR on observer error",
			wantSource:   source.StateNeedsHuman,
			exitCode:     17,
			priorPR:      "42",
			observer:     fakeWorldObserver{err: errors.New("github unavailable")},
			wantPhase:    courierv1alpha1.PhaseFailed,
			wantPR:       "42",
			wantReportPR: "42",
		},
		{
			name:         "observer error without prior PR still terminalizes",
			wantSource:   source.StateNeedsHuman,
			exitCode:     2,
			observer:     fakeWorldObserver{err: errors.New("github unavailable")},
			wantPhase:    courierv1alpha1.PhaseNeedsHuman,
			wantPR:       "",
			wantReportPR: "",
		},
		{
			name:         "no observer still terminalizes",
			wantSource:   source.StateNeedsHuman,
			exitCode:     17,
			observer:     nil,
			wantPhase:    courierv1alpha1.PhaseFailed,
			wantPR:       "",
			wantReportPR: "",
		},
		{
			name:         "no PR observed stays empty",
			wantSource:   source.StateNeedsHuman,
			exitCode:     2,
			observer:     fakeWorldObserver{observation: PRObservation{}},
			wantPhase:    courierv1alpha1.PhaseNeedsHuman,
			wantPR:       "",
			wantReportPR: "",
		},
		{
			name:         "failed publishes live observed PR",
			wantSource:   source.StateNeedsHuman,
			exitCode:     17,
			observer:     fakeWorldObserver{observation: PRObservation{PR: "42", Checks: []CheckObservation{{Name: "check", State: CheckStateFailed}}}},
			wantPhase:    courierv1alpha1.PhaseFailed,
			wantPR:       "42",
			wantReportPR: "42",
		},
		{
			name:         "all-green observation on terminal exit stays terminal",
			wantSource:   source.StateNeedsHuman,
			exitCode:     2,
			observer:     fakeWorldObserver{observation: PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "check", State: CheckStatePassed}}}},
			wantPhase:    courierv1alpha1.PhaseNeedsHuman,
			wantPR:       "42",
			wantReportPR: "42",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			run.Spec.Source = "test"
			run.Spec.WorkItemID = "opaque-work-item"
			run.Status.Branch = "courier/acme/widgets/issue-1"
			run.Status.LastCommit = "abc123"
			if tt.priorPR != "" {
				run.Status.PR = tt.priorPR
			}
			client := phaseClient(t, run, coordinatorPod(run, tt.exitCode))
			reconciler := &CoderRunReconciler{
				Client:       client,
				Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
				StatusWriter: fakeStatusWriter{client: client},
				Observer:     tt.observer,
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != tt.wantPhase {
				t.Fatalf("phase = %q, want %q", updated.Status.Phase, tt.wantPhase)
			}
			if updated.Status.PR != tt.wantPR {
				t.Fatalf("PR = %q, want %q", updated.Status.PR, tt.wantPR)
			}
			if len(item.reports) != 1 || item.reports[0].PR != tt.wantReportPR || item.reports[0].State != tt.wantSource {
				t.Fatalf("reports = %#v, want one report with PR %q and state %q", item.reports, tt.wantReportPR, tt.wantSource)
			}
			if updated.Status.LastCommit != "abc123" {
				t.Fatalf("LastCommit = %q, want abc123", updated.Status.LastCommit)
			}
		})
	}
}

func TestVerifyingObservationGatesReview(t *testing.T) {
	tests := []struct {
		name             string
		mode             courierv1alpha1.Mode
		observation      PRObservation
		err              error
		observerMissing  bool
		wantPhase        courierv1alpha1.Phase
		wantRequeue      bool
		wantPR           string
		wantTransition   source.State
		wantNoTransition bool
	}{
		{name: "observer missing", observerMissing: true, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantTransition: source.StateNeedsHuman},
		{name: "no PR", wantPhase: courierv1alpha1.PhaseNeedsHuman, wantTransition: source.StateNeedsHuman},
		{name: "draft", observation: PRObservation{PR: "42", Draft: true, Checks: []CheckObservation{{State: CheckStatePassed}}}, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantTransition: source.StateNeedsHuman},
		{name: "no checks", observation: PRObservation{PR: "42"}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "pending", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStatePending}}}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "pending failure mix", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}, {State: CheckStatePending}}}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "failed resolve-issue checks keep observing", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "failed fix-pr checks return for a fresh attempt", mode: courierv1alpha1.ModeFixPR, observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}}, wantPhase: courierv1alpha1.PhaseFailed, wantTransition: source.StateInProgress},
		{name: "single green observation", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStatePassed}, {State: CheckStatePassed}}}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "transient error", err: errors.New("GitHub unavailable"), wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true},
		{name: "merged mid-run", observation: PRObservation{PR: "42", Merged: true}, wantPhase: courierv1alpha1.PhaseDone, wantPR: "42", wantNoTransition: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
			if tt.mode != "" {
				run.Spec.Mode = tt.mode
			}
			run.Spec.Source = "manual"
			run.Status.Branch = "courier/acme/widgets/issue-1"
			run.Status.Checkpoint = &courierv1alpha1.Checkpoint{Plan: "preserve"}
			run.Status.Heartbeat = &courierv1alpha1.Heartbeat{Kind: "stream"}
			run.Status.LastCommit = "abc123"
			client := phaseClient(t, run)
			var observer WorldObserver = fakeWorldObserver{observation: tt.observation, err: tt.err}
			if tt.observerMissing {
				observer = nil
			}
			reconciler := &CoderRunReconciler{
				Client:       client,
				Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
				StatusWriter: fakeStatusWriter{client: client},
				Observer:     observer,
			}
			result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if tt.wantRequeue && result.RequeueAfter != observationRequeueDelay {
				t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, observationRequeueDelay)
			}
			if !tt.wantRequeue && result.RequeueAfter != 0 {
				t.Fatalf("RequeueAfter = %v, want none", result.RequeueAfter)
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != tt.wantPhase {
				t.Fatalf("phase = %q, want %q", updated.Status.Phase, tt.wantPhase)
			}
			if tt.wantPR != "" && updated.Status.PR != tt.wantPR {
				t.Fatalf("PR = %q, want %q", updated.Status.PR, tt.wantPR)
			}
			if updated.Status.Checkpoint == nil || updated.Status.Checkpoint.Plan != "preserve" || updated.Status.Heartbeat == nil || updated.Status.LastCommit != "abc123" {
				t.Fatalf("harness status was not preserved: checkpoint=%#v heartbeat=%#v lastCommit=%q", updated.Status.Checkpoint, updated.Status.Heartbeat, updated.Status.LastCommit)
			}
			if tt.wantTransition != "" && (len(item.transitions) != 1 || item.transitions[0] != tt.wantTransition) {
				t.Fatalf("transitions = %#v, want %q", item.transitions, tt.wantTransition)
			}
			if tt.wantNoTransition && len(item.transitions) != 0 {
				t.Fatalf("transitions = %#v, want none: shipped work is not a case for a human", item.transitions)
			}
			if tt.wantRequeue && len(item.transitions) != 0 {
				t.Fatalf("transitions = %#v, want none while observation is pending", item.transitions)
			}
		})
	}
}

func TestVerifyingSettlesOnSecondStableGreenObservation(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	observer := &sequenceWorldObserver{observations: []PRObservation{
		greenObservation("sha-1", "check-a", "check-b"),
		greenObservation("sha-1", "check-b", "check-a"),
	}}
	newReconciler := func() *CoderRunReconciler {
		return &CoderRunReconciler{
			Client:       client,
			Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
			StatusWriter: fakeStatusWriter{client: client},
			Observer:     observer,
		}
	}
	if _, err := newReconciler().Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	var verifying courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &verifying); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if verifying.Status.Phase != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after first green observation = %q, want Verifying", verifying.Status.Phase)
	}
	if verifying.Status.CheckFingerprint == "" {
		t.Fatal("checkFingerprint after first green observation = empty, want the recorded check set")
	}
	if len(item.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none before the check set settles", item.transitions)
	}
	// A restarted controller shares no process memory: the fingerprint
	// recorded in the status is the only settle evidence available.
	if _, err := newReconciler().Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	var settled courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &settled); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if settled.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after stable green observation = %q, want AwaitingReview", settled.Status.Phase)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateInReview {
		t.Fatalf("transitions = %#v, want exactly one in-review", item.transitions)
	}
}

func TestVerifyingPendingObservationCannotPresettleGreen(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "check-a", State: CheckStatePending}}},
			greenObservation("sha-1", "check-a"),
			greenObservation("sha-1", "check-a", "check-b"),
			greenObservation("sha-1", "check-a", "check-b"),
		}},
	}
	for i, want := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
	} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+1, err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
			t.Fatalf("get run: %v", err)
		}
		if updated.Status.Phase != want {
			t.Fatalf("phase after observation %d = %q, want %q; a pending observation must never pre-settle the identities it saw", i+1, updated.Status.Phase, want)
		}
	}
}

func TestVerifyingPendingClearsGreenCandidate(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			greenObservation("sha-1", "check-a", "check-b"),
			{PR: "42", Head: "sha-1", Checks: []CheckObservation{
				{Name: "check-a", State: CheckStatePassed},
				{Name: "check-b", State: CheckStatePassed},
				{Name: "check-c", State: CheckStatePending},
			}},
			greenObservation("sha-1", "check-a", "check-b", "check-c"),
			greenObservation("sha-1", "check-a", "check-b", "check-c"),
		}},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	// A pending observation clears the green candidate instead of recording
	// its own identity, so the next green observation starts a fresh settle.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.CheckFingerprint != "" {
		t.Fatalf("checkFingerprint after pending observation = %q, want cleared", updated.Status.CheckFingerprint)
	}
	for i, want := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
	} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+3, err)
		}
		if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
			t.Fatalf("get run: %v", err)
		}
		if updated.Status.Phase != want {
			t.Fatalf("phase after observation %d = %q, want %q; the green set after a pending poll must settle twice", i+3, updated.Status.Phase, want)
		}
	}
}

func TestVerifyingGrowingGreenCheckSetRestartsSettle(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			greenObservation("sha-1", "check-a", "check-b"),
			greenObservation("sha-1", "check-a", "check-b", "check-c"),
			greenObservation("sha-1", "check-a", "check-b", "check-c"),
		}},
	}
	for i, want := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
	} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+1, err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
			t.Fatalf("get run: %v", err)
		}
		if updated.Status.Phase != want {
			t.Fatalf("phase after observation %d = %q, want %q; a changed check set must reset the settle", i+1, updated.Status.Phase, want)
		}
	}
}

func TestVerifyingNewHeadResetsSettle(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			greenObservation("sha-1", "check-a", "check-b"),
			greenObservation("sha-2", "check-a", "check-b"),
			greenObservation("sha-2", "check-a", "check-b"),
		}},
	}
	for i, want := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
	} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+1, err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
			t.Fatalf("get run: %v", err)
		}
		if updated.Status.Phase != want {
			t.Fatalf("phase after observation %d = %q, want %q; a new head commit must reset the settle", i+1, updated.Status.Phase, want)
		}
	}
}

func TestVerifyingFailureUsesRetryableLifecycle(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			greenObservation("sha-1", "check-a", "check-b"),
			{PR: "42", Head: "sha-1", Checks: []CheckObservation{
				{Name: "check-a", State: CheckStatePassed},
				{Name: "check-b", State: CheckStateFailed},
			}},
		}},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed without waiting for a stable observation", updated.Status.Phase)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateInProgress {
		t.Fatalf("transitions = %#v, want exactly one retryable in-progress state", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultFailed || item.reports[0].State != source.StateInProgress || item.reports[0].PR != "42" || item.reports[0].Error != externalVerificationFailureReason {
		t.Fatalf("reports = %#v, want one redacted external-verification failure report for PR 42", item.reports)
	}
}

func TestResolveIssueRedCIObserverReachesReviewAfterRepair(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("resolve", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.Spec.Source = "dispatch"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	observer := &sequenceWorldObserver{observations: []PRObservation{
		{PR: "42", Head: "head-red", Checks: []CheckObservation{{Name: "test", State: CheckStateFailed}}},
		greenObservation("head-fixed", "test"),
		greenObservation("head-fixed", "test"),
	}}
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"dispatch": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer:     observer,
	}
	for i, wantPhase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
	} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("resolve")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+1, err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey("resolve"), &updated); err != nil {
			t.Fatalf("get run: %v", err)
		}
		if updated.Status.Phase != wantPhase {
			t.Fatalf("phase after observation %d = %q, want %q", i+1, updated.Status.Phase, wantPhase)
		}
		if updated.Status.Phase == courierv1alpha1.PhaseVerifying && len(item.reports) != 0 {
			t.Fatalf("reports before stable green = %#v, want none", item.reports)
		}
	}
	if observer.calls != 3 {
		t.Fatalf("observer calls = %d, want 3", observer.calls)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateInReview {
		t.Fatalf("transitions = %#v, want only the final in-review transition", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultReady || item.reports[0].State != source.StateInReview || item.reports[0].PR != "42" {
		t.Fatalf("reports = %#v, want one ready report for PR 42", item.reports)
	}
}

func TestResolveIssueStableRedChecksHandOff(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	run.Status.HeadRepo = "acme/widgets"
	run.Status.HeadSHA = "head-abc"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			redObservation("sha-red", "test"),
			redObservation("sha-red", "test"),
		}},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	var verifying courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &verifying); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if verifying.Status.Phase != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after first red observation = %q, want Verifying", verifying.Status.Phase)
	}
	if verifying.Status.FailedCheckFingerprint == "" {
		t.Fatal("failedCheckFingerprint after first red observation = empty, want the recorded red set")
	}
	if len(item.reports) != 0 || len(item.transitions) != 0 {
		t.Fatalf("first red observation settled early: reports=%#v transitions=%#v", item.reports, item.transitions)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	var settled courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &settled); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if settled.Status.Phase != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after stable red observation = %q, want HandedOff", settled.Status.Phase)
	}
	if settled.Status.PR != "42" {
		t.Fatalf("PR after handoff = %q, want 42 preserved", settled.Status.PR)
	}
	if settled.Status.HeadRepo != "acme/widgets" || settled.Status.HeadSHA != "head-abc" {
		t.Fatalf("head identity after handoff = repo %q sha %q, want preserved", settled.Status.HeadRepo, settled.Status.HeadSHA)
	}
	if len(item.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none: a handoff publishes no source state", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultHandedOff || item.reports[0].PR != "42" || item.reports[0].Error != externalVerificationFailureReason {
		t.Fatalf("reports = %#v, want one handed-off report for PR 42", item.reports)
	}
}

func TestResolveIssuePendingRedDoesNotSettle(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	// Pre-seed a prior red candidate: the mixed observation must clear it
	// rather than let it pair with a later fully-failed set.
	run.Status.FailedCheckFingerprint = checkSetFingerprint(redObservation("sha-red", "test"))
	client := phaseClient(t, run)
	mixed := PRObservation{PR: "42", Head: "sha-red", Checks: []CheckObservation{
		{Name: "test", State: CheckStateFailed},
		{Name: "lint", State: CheckStatePending},
	}}
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			mixed,
			mixed,
			redObservation("sha-red", "test", "lint"),
			redObservation("sha-red", "test", "lint"),
		}},
	}
	for i, want := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseHandedOff,
	} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+1, err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
			t.Fatalf("get run: %v", err)
		}
		if updated.Status.Phase != want {
			t.Fatalf("phase after observation %d = %q, want %q", i+1, updated.Status.Phase, want)
		}
		if i < 2 && updated.Status.FailedCheckFingerprint != "" {
			t.Fatalf("failedCheckFingerprint after pending+red observation = %q, want cleared", updated.Status.FailedCheckFingerprint)
		}
	}
	if len(item.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none: a pending observation never transitions", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultHandedOff {
		t.Fatalf("reports = %#v, want exactly one handed-off report", item.reports)
	}
}

func TestResolveIssueRedThenGreenBeforeSettlement(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			redObservation("sha-red", "test"),
			greenObservation("sha-green", "test"),
			greenObservation("sha-green", "test"),
		}},
	}
	for i, want := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
	} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+1, err)
		}
		var updated courierv1alpha1.CoderRun
		if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
			t.Fatalf("get run: %v", err)
		}
		if updated.Status.Phase != want {
			t.Fatalf("phase after observation %d = %q, want %q", i+1, updated.Status.Phase, want)
		}
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateInReview {
		t.Fatalf("transitions = %#v, want exactly one in-review", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultReady {
		t.Fatalf("reports = %#v, want exactly one ready report", item.reports)
	}
}

func TestHandoffReportRetryReusesIdempotencyKey(t *testing.T) {
	item := &admissionSource{reportErr: errors.New("report unavailable")}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	observation := redObservation("sha-red", "test")
	run.Status.FailedCheckFingerprint = checkSetFingerprint(observation)
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer:     fakeWorldObserver{observation: observation},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("first Reconcile() error = %v, want nil (a failed report must not block terminalization)", err)
	}
	if len(item.reports) != 1 {
		t.Fatalf("reports = %#v, want one before the retry", item.reports)
	}
	var handedOff courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &handedOff); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if handedOff.Status.Phase != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase = %q, want HandedOff", handedOff.Status.Phase)
	}
	item.reportErr = nil
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(item.reports) != 2 {
		t.Fatalf("reports = %#v, want one per attempt", item.reports)
	}
	want := lifecycleIdempotencyKey(run, courierv1alpha1.PhaseHandedOff)
	if item.reports[0].IdempotencyKey != want {
		t.Fatalf("first report key = %q, want %q", item.reports[0].IdempotencyKey, want)
	}
	if item.reports[1].IdempotencyKey != item.reports[0].IdempotencyKey {
		t.Fatalf("retry report key = %q, want %q", item.reports[1].IdempotencyKey, item.reports[0].IdempotencyKey)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("third Reconcile() error = %v", err)
	}
	if len(item.reports) != 2 {
		t.Fatalf("reports after publish = %#v, want no additional report", item.reports)
	}
	if len(item.resolved) != 0 {
		t.Fatalf("resolved IDs = %#v, want none: a handoff never resolves the source", item.resolved)
	}
}

func TestResolveIssueLostObserverDoesNotHandOff(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.Spec.Source = "manual"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer:     nil,
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman without an observer", updated.Status.Phase)
	}
	for _, report := range item.reports {
		if report.Result == source.ResultHandedOff {
			t.Fatalf("reports = %#v, want no handoff without an observer", item.reports)
		}
	}
}

func TestResolveIssueHandoffIsSourceAgnostic(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	run.Spec.Source = "custom"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"custom": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			redObservation("sha-red", "test"),
			redObservation("sha-red", "test"),
		}},
	}
	for i := 0; i < 2; i++ {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
			t.Fatalf("Reconcile() %d error = %v", i+1, err)
		}
	}
	want := source.Lifecycle{
		Result:         source.ResultHandedOff,
		PR:             "42",
		Error:          externalVerificationFailureReason,
		IdempotencyKey: lifecycleIdempotencyKey(run, courierv1alpha1.PhaseHandedOff),
	}
	if len(item.reports) != 1 || !reflect.DeepEqual(item.reports[0], want) {
		t.Fatalf("reports = %#v, want exactly one source-agnostic handoff %#v", item.reports, want)
	}
	if len(item.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none: the handoff carries no source state", item.transitions)
	}
}

func TestCheckSetFingerprintRepresentsIdentity(t *testing.T) {
	passed := func(name string) CheckObservation { return CheckObservation{Name: name, State: CheckStatePassed} }
	pending := func(name string) CheckObservation { return CheckObservation{Name: name, State: CheckStatePending} }
	ab := checkSetFingerprint(PRObservation{Head: "sha-1", Checks: []CheckObservation{passed("check-a"), passed("check-b")}})
	if ab == "" {
		t.Fatal("fingerprint of a check set = empty, want a digest")
	}
	if ba := checkSetFingerprint(PRObservation{Head: "sha-1", Checks: []CheckObservation{passed("check-b"), passed("check-a")}}); ba != ab {
		t.Fatalf("fingerprint changed with check order: %q vs %q", ab, ba)
	}
	if pendingAB := checkSetFingerprint(PRObservation{Head: "sha-1", Checks: []CheckObservation{pending("check-a"), pending("check-b")}}); pendingAB != ab {
		t.Fatalf("fingerprint changed with check state: %q vs %q; identity is state-independent", ab, pendingAB)
	}
	if otherNames := checkSetFingerprint(PRObservation{Head: "sha-1", Checks: []CheckObservation{passed("check-a"), passed("check-c")}}); otherNames == ab {
		t.Fatal("fingerprint ignored a changed check name")
	}
	if otherHead := checkSetFingerprint(PRObservation{Head: "sha-2", Checks: []CheckObservation{passed("check-a"), passed("check-b")}}); otherHead == ab {
		t.Fatal("fingerprint ignored a changed head commit")
	}
	if empty := checkSetFingerprint(PRObservation{Head: "sha-1"}); empty != "" {
		t.Fatalf("fingerprint of an empty check set = %q, want empty", empty)
	}
}

// greenObservation builds an all-green observation over the named checks.
func greenObservation(head string, names ...string) PRObservation {
	observation := PRObservation{PR: "42", Head: head}
	for _, name := range names {
		observation.Checks = append(observation.Checks, CheckObservation{Name: name, State: CheckStatePassed})
	}
	return observation
}

// redObservation builds an all-red observation over the named checks.
func redObservation(head string, names ...string) PRObservation {
	observation := PRObservation{PR: "42", Head: head}
	for _, name := range names {
		observation.Checks = append(observation.Checks, CheckObservation{Name: name, State: CheckStateFailed})
	}
	return observation
}

func TestTerminalLifecycleReportsToSource(t *testing.T) {
	tests := []struct {
		name             string
		mode             courierv1alpha1.Mode
		observation      PRObservation
		wantPhase        courierv1alpha1.Phase
		wantReport       source.Lifecycle
		wantNoTransition bool
	}{
		{
			name:        "awaiting review",
			observation: PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "check", State: CheckStatePassed}}},
			wantPhase:   courierv1alpha1.PhaseAwaitingReview,
			wantReport:  source.Lifecycle{State: source.StateInReview, Result: source.ResultReady, PR: "42", IdempotencyKey: "coderun/default/run/AwaitingReview"},
		},
		{
			name:        "failed fix-pr checks",
			mode:        courierv1alpha1.ModeFixPR,
			observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}},
			wantPhase:   courierv1alpha1.PhaseFailed,
			wantReport:  source.Lifecycle{State: source.StateInProgress, Result: source.ResultFailed, PR: "42", Error: externalVerificationFailureReason, IdempotencyKey: "coderun/default/run/Failed"},
		},
		{
			name:             "failed resolve-issue checks hand off",
			mode:             courierv1alpha1.ModeResolveIssue,
			observation:      redObservation("sha-red", "test", "lint"),
			wantPhase:        courierv1alpha1.PhaseHandedOff,
			wantReport:       source.Lifecycle{Result: source.ResultHandedOff, PR: "42", Error: externalVerificationFailureReason, IdempotencyKey: "coderun/default/run/HandedOff"},
			wantNoTransition: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
			if tt.mode != "" {
				run.Spec.Mode = tt.mode
			}
			run.Status.Branch = "courier/acme/widgets/issue-1"
			client := phaseClient(t, run)
			reconciler := &CoderRunReconciler{
				Client:       client,
				Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
				StatusWriter: fakeStatusWriter{client: client},
				Observer:     fakeWorldObserver{observation: tt.observation},
			}
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
					t.Fatalf("Reconcile() %d error = %v", attempt+1, err)
				}
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if tt.wantPhase == "" {
				if updated.Status.Phase != courierv1alpha1.PhaseVerifying || len(item.reports) != 0 || len(item.transitions) != 0 {
					t.Fatalf("red resolve-issue observation settled unexpectedly: phase=%q reports=%#v transitions=%#v", updated.Status.Phase, item.reports, item.transitions)
				}
				return
			}
			if len(item.reports) != 1 || !reflect.DeepEqual(item.reports[0], tt.wantReport) {
				t.Fatalf("reports = %#v, want %#v", item.reports, []source.Lifecycle{tt.wantReport})
			}
			if updated.Status.Phase != tt.wantPhase || updated.Status.PR != "42" {
				t.Fatalf("status = phase %q PR %q, want %q 42", updated.Status.Phase, updated.Status.PR, tt.wantPhase)
			}
			if tt.wantNoTransition && len(item.transitions) != 0 {
				t.Fatalf("transitions = %#v, want none: a handoff publishes no source state", item.transitions)
			}
		})
	}
}

func TestUnsupportedLifecycleReportWaitsForExplicitTokenAndRetriesPendingIntent(t *testing.T) {
	item := &admissionSource{reportErr: fmt.Errorf("%w: handed-off", source.ErrUnsupportedOutcome)}
	run := admissionRun("run", "local", courierv1alpha1.PhaseHandedOff)
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "opaque-work-item"
	run.Status.PR = "42"
	run.Status.Conditions = []metav1.Condition{{
		Type: lifecycleReportedCondition, Status: metav1.ConditionFalse,
		Reason:             lifecycleReportPendingExternalVerificationFailureReason,
		Message:            externalVerificationFailureReason,
		LastTransitionTime: metav1.Now(),
	}}
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client: client, Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}

	for i := 0; i < 2; i++ {
		result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
		if err != nil || result.Requeue || result.RequeueAfter != 0 {
			t.Fatalf("unsupported reconcile %d = (%#v, %v), want no error or retry", i, result, err)
		}
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultHandedOff || item.reports[0].Error != externalVerificationFailureReason {
		t.Fatalf("reports = %#v, want one truthful pending handoff report", item.reports)
	}
	var pending courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &pending); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(pending.Status.Conditions, lifecycleReportedCondition)
	capability := apimeta.FindStatusCondition(pending.Status.Conditions, lifecycleReportSupportedCondition)
	if pending.Status.Phase != courierv1alpha1.PhaseHandedOff || cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != lifecycleReportPendingExternalVerificationFailureReason || cond.Message != externalVerificationFailureReason || capability == nil || capability.Status != metav1.ConditionFalse || capability.Reason != lifecycleReportUnsupportedOutcomeReason {
		t.Fatalf("pending state = phase %q lifecycle %#v capability %#v, want preserved intent and unsupported capability", pending.Status.Phase, cond, capability)
	}

	if len(item.transitions) != 0 {
		t.Fatalf("transitions = %#v, want none for source-neutral handoff", item.transitions)
	}

	// An explicit retry is consumed even if the backend still rejects it;
	// it does not create a new automatic retry loop.
	pending.Annotations = map[string]string{retryLifecycleReportAnnotation: "operator-retry-1"}
	if err := client.Update(context.Background(), &pending); err != nil {
		t.Fatal(err)
	}
	restarted := &CoderRunReconciler{
		Client: client, Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	if result, err := restarted.Reconcile(context.Background(), admissionRequest("run")); err != nil || result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("unsupported explicit retry = (%#v, %v), want no automatic retry", result, err)
	}
	if len(item.reports) != 2 {
		t.Fatalf("reports = %d, want one explicit retry", len(item.reports))
	}
	var unsupported courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &unsupported); err != nil {
		t.Fatal(err)
	}
	if unsupported.Status.LifecycleReportRetryToken != "operator-retry-1" {
		t.Fatalf("processed token = %q, want operator-retry-1", unsupported.Status.LifecycleReportRetryToken)
	}
	if _, err := restarted.Reconcile(context.Background(), admissionRequest("run")); err != nil || len(item.reports) != 2 {
		t.Fatalf("reconcile with consumed token = error %v reports %d, want no duplicate", err, len(item.reports))
	}
	if got := pendingLifecycleIntent(&unsupported); got.ciFailure != true || got.error != externalVerificationFailureReason {
		t.Fatalf("restart intent = %#v, want CI failure and original report text", got)
	}

	// A second token can retry after the source gains support.
	unsupported.Annotations[retryLifecycleReportAnnotation] = "operator-retry-2"
	if err := client.Update(context.Background(), &unsupported); err != nil {
		t.Fatal(err)
	}
	item.reportErr = nil
	if result, err := restarted.Reconcile(context.Background(), admissionRequest("run")); err != nil || result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("accepted retry = (%#v, %v), want published without retry", result, err)
	}
	if len(item.reports) != 3 || item.reports[2].Result != source.ResultHandedOff || item.reports[2].PR != "42" || item.reports[2].Error != externalVerificationFailureReason || item.reports[2].IdempotencyKey != item.reports[0].IdempotencyKey {
		t.Fatalf("retry reports = %#v, want same pending lifecycle and idempotency key", item.reports)
	}
	var published courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &published); err != nil {
		t.Fatal(err)
	}
	cond = apimeta.FindStatusCondition(published.Status.Conditions, lifecycleReportedCondition)
	capability = apimeta.FindStatusCondition(published.Status.Conditions, lifecycleReportSupportedCondition)
	if published.Status.Phase != courierv1alpha1.PhaseHandedOff || published.Status.LifecycleReportRetryToken != "operator-retry-2" || cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Published" || capability == nil || capability.Status != metav1.ConditionTrue {
		t.Fatalf("published state = phase %q token %q lifecycle %#v capability %#v", published.Status.Phase, published.Status.LifecycleReportRetryToken, cond, capability)
	}
	if _, err := restarted.Reconcile(context.Background(), admissionRequest("run")); err != nil || len(item.reports) != 3 {
		t.Fatalf("reconcile after publish = error %v reports %d, want no duplicate", err, len(item.reports))
	}
}

func TestUnsupportedRetryPreservesLifecycleIntent(t *testing.T) {
	for _, tt := range []struct {
		name         string
		phase        courierv1alpha1.Phase
		mode         courierv1alpha1.Mode
		reason       string
		message      string
		wantState    source.State
		wantResult   source.Result
		wantError    string
		wantSkipMark bool
	}{
		{
			name: "fix-pr CI failure", phase: courierv1alpha1.PhaseFailed, mode: courierv1alpha1.ModeFixPR,
			reason: lifecycleReportPendingExternalVerificationFailureReason, message: externalVerificationFailureReason,
			wantState: source.StateInProgress, wantResult: source.ResultFailed, wantError: externalVerificationFailureReason,
		},
		{
			name: "needs-human skip queue mark", phase: courierv1alpha1.PhaseNeedsHuman, mode: courierv1alpha1.ModeFixPR,
			reason: lifecycleReportPendingBlockedReportParksPRFixReason, message: "coordinator failed",
			wantState: source.StateNeedsHuman, wantResult: source.ResultBlocked, wantError: "coordinator failed", wantSkipMark: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{reportErr: fmt.Errorf("%w: unsupported", source.ErrUnsupportedOutcome)}
			run := admissionRun("run", "local", tt.phase)
			run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = tt.mode, "test", "opaque-work-item"
			run.Status.Conditions = []metav1.Condition{{
				Type: lifecycleReportedCondition, Status: metav1.ConditionFalse,
				Reason: tt.reason, Message: tt.message, LastTransitionTime: metav1.Now(),
			}, {
				Type: lifecycleReportSupportedCondition, Status: metav1.ConditionFalse,
				Reason: lifecycleReportUnsupportedOutcomeReason, Message: "unsupported", LastTransitionTime: metav1.Now(),
			}}
			run.Annotations = map[string]string{retryLifecycleReportAnnotation: "retry-1"}
			client := phaseClient(t, run)
			reconciler := &CoderRunReconciler{
				Client: client, Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}),
				StatusWriter: status.KubePatchWriter{Client: client},
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
				t.Fatalf("retry Reconcile() error = %v", err)
			}
			if len(item.transitions) != 1 || item.transitions[0] != tt.wantState {
				t.Fatalf("transitions = %#v, want [%s]", item.transitions, tt.wantState)
			}
			if len(item.reports) != 1 || item.reports[0].Result != tt.wantResult || item.reports[0].Error != tt.wantError || item.reports[0].BlockedReportParksPRFix != tt.wantSkipMark {
				t.Fatalf("reports = %#v, want result %s, error %q, skip mark %t", item.reports, tt.wantResult, tt.wantError, tt.wantSkipMark)
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatal(err)
			}
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
			if cond == nil || cond.Reason != tt.reason || cond.Message != tt.message {
				t.Fatalf("LifecycleReported = %#v, want original durable intent", cond)
			}
			if updated.Status.LifecycleReportRetryToken != "retry-1" {
				t.Fatalf("processed retry token = %q, want retry-1", updated.Status.LifecycleReportRetryToken)
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil || len(item.reports) != 1 {
				t.Fatalf("reconcile with consumed retry token = %v reports %d, want no duplicate", err, len(item.reports))
			}
		})
	}
}

func TestUnsupportedRetryTransientSequenceRetainsTokenAndIntent(t *testing.T) {
	item := &admissionSource{reportErrors: []error{
		errors.New("502 upstream unavailable"),
		fmt.Errorf("%w: unsupported", source.ErrUnsupportedOutcome),
	}}
	run := admissionRun("run", "local", courierv1alpha1.PhaseFailed)
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeFixPR, "test", "opaque-work-item"
	run.Status.Conditions = []metav1.Condition{{
		Type: lifecycleReportedCondition, Status: metav1.ConditionFalse,
		Reason:  lifecycleReportPendingExternalVerificationFailureReason,
		Message: externalVerificationFailureReason, LastTransitionTime: metav1.Now(),
	}, {
		Type: lifecycleReportSupportedCondition, Status: metav1.ConditionFalse,
		Reason: lifecycleReportUnsupportedOutcomeReason, LastTransitionTime: metav1.Now(),
	}}
	run.Status.LifecycleReportRetryToken = "prior-token"
	run.Annotations = map[string]string{retryLifecycleReportAnnotation: "retry-1"}
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client: client, Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	for i := 0; i < 3; i++ {
		result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
		if err != nil {
			t.Fatalf("reconcile %d error = %v", i, err)
		}
		if i == 0 && result.RequeueAfter <= 0 {
			t.Fatalf("transient retry result = %#v, want bounded automatic retry", result)
		}
		if i == 1 && (result.Requeue || result.RequeueAfter != 0) {
			t.Fatalf("unsupported retry result = %#v, want no automatic retry", result)
		}
		if i == 0 {
			var retry courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &retry); err != nil {
				t.Fatal(err)
			}
			if retry.Status.LifecycleReportRetryToken != "prior-token" || retry.Status.LifecycleReportActiveRetryToken != "retry-1" {
				t.Fatalf("transient attempt tokens = consumed %q active %q, want prior-token retained and retry-1 pending", retry.Status.LifecycleReportRetryToken, retry.Status.LifecycleReportActiveRetryToken)
			}
			delete(retry.Annotations, retryLifecycleReportAnnotation)
			if err := client.Update(context.Background(), &retry); err != nil {
				t.Fatal(err)
			}
		}
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
	capability := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportSupportedCondition)
	if updated.Status.LifecycleReportRetryToken != "retry-1" || updated.Status.LifecycleReportActiveRetryToken != "" || cond == nil || cond.Reason != lifecycleReportPendingExternalVerificationFailureReason || cond.Message != externalVerificationFailureReason || capability == nil || capability.Status != metav1.ConditionFalse {
		t.Fatalf("state = token %q active token %q lifecycle %#v capability %#v; want consumed token, original intent, and stopped unsupported state", updated.Status.LifecycleReportRetryToken, updated.Status.LifecycleReportActiveRetryToken, cond, capability)
	}
	if len(item.reports) != 2 || len(item.transitions) != 2 || item.transitions[1] != source.StateInProgress || item.reports[1].Result != source.ResultFailed {
		t.Fatalf("reports %#v transitions %#v; want three attempts preserving CI failure", item.reports, item.transitions)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil || len(item.reports) != 2 {
		t.Fatalf("post-unsupported reconcile error %v reports %d; want stopped", err, len(item.reports))
	}
}

func TestUnsupportedRetryTransientThenSuccessConsumesToken(t *testing.T) {
	item := &admissionSource{reportErr: errors.New("502 upstream unavailable")}
	run := admissionRun("run", "local", courierv1alpha1.PhaseFailed)
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeFixPR, "test", "opaque-work-item"
	run.Status.Conditions = []metav1.Condition{{
		Type: lifecycleReportedCondition, Status: metav1.ConditionFalse,
		Reason:  lifecycleReportPendingExternalVerificationFailureReason,
		Message: externalVerificationFailureReason, LastTransitionTime: metav1.Now(),
	}, {
		Type: lifecycleReportSupportedCondition, Status: metav1.ConditionFalse,
		Reason: lifecycleReportUnsupportedOutcomeReason, LastTransitionTime: metav1.Now(),
	}}
	run.Annotations = map[string]string{retryLifecycleReportAnnotation: "retry-1"}
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client: client, Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("transient explicit retry = (%#v, %v), want ordinary bounded retry", result, err)
	}
	var pending courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &pending); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(pending.Status.Conditions, lifecycleReportedCondition)
	if pending.Status.LifecycleReportRetryToken != "" || pending.Status.LifecycleReportActiveRetryToken != "retry-1" || cond == nil || cond.Reason != lifecycleReportPendingExternalVerificationFailureReason || cond.Message != externalVerificationFailureReason {
		t.Fatalf("transient state tokens consumed %q active %q condition %#v; want pending attempt token and original intent", pending.Status.LifecycleReportRetryToken, pending.Status.LifecycleReportActiveRetryToken, cond)
	}
	pending.Annotations[retryLifecycleReportAnnotation] = "retry-2"
	if err := client.Update(context.Background(), &pending); err != nil {
		t.Fatal(err)
	}
	item.reportErr = nil
	result, err = reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil || result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("successful retry = (%#v, %v), want published", result, err)
	}
	var published courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &published); err != nil {
		t.Fatal(err)
	}
	cond = apimeta.FindStatusCondition(published.Status.Conditions, lifecycleReportedCondition)
	if published.Status.LifecycleReportRetryToken != "retry-1" || published.Status.LifecycleReportActiveRetryToken != "" || cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Published" || len(item.reports) != 2 || item.reports[1].Result != source.ResultFailed {
		t.Fatalf("published state token %q condition %#v reports %#v; want settled in-flight token and original failed result", published.Status.LifecycleReportRetryToken, cond, item.reports)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil || len(item.reports) != 2 {
		t.Fatalf("reconcile after publication = %v reports %d, want no duplicate report", err, len(item.reports))
	}
}

func TestUnsupportedRetryTransientThenUnsupportedKeepsNewerAnnotationEligible(t *testing.T) {
	item := &admissionSource{reportErr: errors.New("502 upstream unavailable")}
	run := admissionRun("run", "local", courierv1alpha1.PhaseFailed)
	run.Spec.Mode, run.Spec.Source, run.Spec.WorkItemID = courierv1alpha1.ModeFixPR, "test", "opaque-work-item"
	run.Status.Conditions = []metav1.Condition{{
		Type: lifecycleReportedCondition, Status: metav1.ConditionFalse,
		Reason:  lifecycleReportPendingExternalVerificationFailureReason,
		Message: externalVerificationFailureReason, LastTransitionTime: metav1.Now(),
	}, {
		Type: lifecycleReportSupportedCondition, Status: metav1.ConditionFalse,
		Reason: lifecycleReportUnsupportedOutcomeReason, LastTransitionTime: metav1.Now(),
	}}
	run.Annotations = map[string]string{retryLifecycleReportAnnotation: "retry-1"}
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client: client, Sources: NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	if result, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("transient retry = (%#v, %v), want bounded retry", result, err)
	}
	var pending courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &pending); err != nil {
		t.Fatal(err)
	}
	pending.Annotations[retryLifecycleReportAnnotation] = "retry-2"
	if err := client.Update(context.Background(), &pending); err != nil {
		t.Fatal(err)
	}
	item.reportErr = fmt.Errorf("%w: still unsupported", source.ErrUnsupportedOutcome)
	if result, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil || result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("unsupported in-flight retry = (%#v, %v), want settled without automatic retry", result, err)
	}
	var settled courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &settled); err != nil {
		t.Fatal(err)
	}
	if settled.Status.LifecycleReportRetryToken != "retry-1" || settled.Status.LifecycleReportActiveRetryToken != "" {
		t.Fatalf("settled tokens consumed %q active %q; want only retry-1 settled", settled.Status.LifecycleReportRetryToken, settled.Status.LifecycleReportActiveRetryToken)
	}
	item.reportErr = nil
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("reconcile newer token: %v", err)
	}
	if len(item.reports) != 3 {
		t.Fatalf("reports = %d, want second explicit retry after initial transient and unsupported attempts", len(item.reports))
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil || len(item.reports) != 3 {
		t.Fatalf("reconcile after newer token published = %v reports %d, want no duplicate", err, len(item.reports))
	}
}

func TestLifecycleRetryTokenUsesUnicodeCodePointBound(t *testing.T) {
	if !validLifecycleReportRetryToken(strings.Repeat("é", 128)) {
		t.Fatal("128 Unicode code points should be accepted")
	}
	if validLifecycleReportRetryToken(strings.Repeat("é", 129)) {
		t.Fatal("129 Unicode code points should be rejected")
	}
	if validLifecycleReportRetryToken("") || validLifecycleReportRetryToken(string([]byte{0xff})) {
		t.Fatal("empty and invalid UTF-8 tokens should be rejected")
	}
	if validLifecycleReportRetryToken(strings.Repeat("a", 129)) {
		t.Fatal("129 ASCII code points should be rejected")
	}
}

func TestTerminalLifecycleReportRetryReusesIdempotencyKey(t *testing.T) {
	item := &admissionSource{reportErr: errors.New("report unavailable")}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
		Observer:     fakeWorldObserver{observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}}},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("first Reconcile() error = %v, want nil (a failed report must not block terminalization)", err)
	}
	if len(item.reports) != 1 {
		t.Fatalf("reports = %#v, want one before the retry", item.reports)
	}
	item.reportErr = nil
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(item.reports) != 2 {
		t.Fatalf("reports = %#v, want one per attempt", item.reports)
	}
	want := lifecycleIdempotencyKey(run, courierv1alpha1.PhaseFailed)
	if item.reports[0].IdempotencyKey != want {
		t.Fatalf("first report key = %q, want %q", item.reports[0].IdempotencyKey, want)
	}
	if item.reports[1].IdempotencyKey != item.reports[0].IdempotencyKey {
		t.Fatalf("retry report key = %q, want %q", item.reports[1].IdempotencyKey, item.reports[0].IdempotencyKey)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", updated.Status.Phase)
	}
}

func TestDoneRunResolvesSourceWorkItem(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseDone)
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(item.resolved) != 1 || item.resolved[0] != "run" {
		t.Fatalf("resolved IDs = %#v, want run", item.resolved)
	}
}

// TestExit3NoChangeNeededResolveIssueSettlesNothingDirectly checks the
// resolve-issue no_change_needed path (issue #169) behind the human review
// gate: a coordinator that exits 3 never settles the source directly. The run
// ends AwaitingReview with the source transitioned to in-review, and neither
// the first reconcile nor a follow-up resolves the work item.
func TestExit3NoChangeNeededResolveIssueSettlesNothingDirectly(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Spec.Mode = courierv1alpha1.ModeResolveIssue
	client := phaseClient(t, run, coordinatorPod(run, 3))
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase = %q, want AwaitingReview", updated.Status.Phase)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateInReview {
		t.Fatalf("source transitions = %#v, want in-review", item.transitions)
	}
	// The terminal phase is stable: a follow-up reconcile must not settle the
	// source.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(item.resolved) != 0 {
		t.Fatalf("resolved IDs = %#v, want none: no_change_needed never resolves the source directly", item.resolved)
	}
}

// TestExit3NoChangeNeededFixPRSettlesAgainstWorld covers issue #267: a fix-pr
// no_change_needed run (executor exit 3) is routed to Verifying and settled
// from the world instead of being mapped straight to NeedsHuman. Only a merged
// pull request reaches Done; an open pull request reaches AwaitingReview only
// on stable green checks and retryable Failed on red; an unobservable pull
// request stays NeedsHuman.
func TestExit3NoChangeNeededFixPRSettlesAgainstWorld(t *testing.T) {
	tests := []struct {
		name            string
		observer        WorldObserver
		wantPhase       courierv1alpha1.Phase
		wantSource      source.State
		wantResult      source.Result
		wantReport      bool
		wantResolved    bool
		wantTransitions int
		// wantBlockedParksPRFix is the lifecycle flag the source sees; a
		// recoverable block must never set it (issue #267).
		wantBlockedParksPRFix bool
		// secondObserve runs one more reconcile when the world needs a second
		// observation to settle (stable green).
		secondObserve bool
	}{
		{
			name:            "merged settles Done without a human",
			observer:        fakeWorldObserver{observation: PRObservation{PR: "42", Merged: true}},
			wantPhase:       courierv1alpha1.PhaseDone,
			wantResolved:    true,
			wantTransitions: 0,
		},
		{
			name:            "open green settles AwaitingReview",
			observer:        fakeWorldObserver{observation: PRObservation{PR: "42", Head: "abc", Checks: []CheckObservation{{Name: "test", State: CheckStatePassed}}}},
			wantPhase:       courierv1alpha1.PhaseAwaitingReview,
			wantSource:      source.StateInReview,
			wantReport:      true,
			wantResult:      source.ResultReady,
			wantTransitions: 1,
			secondObserve:   true,
		},
		{
			name:            "open red reaches retryable Failed",
			observer:        fakeWorldObserver{observation: PRObservation{PR: "42", Head: "abc", Checks: []CheckObservation{{Name: "test", State: CheckStateFailed}}}},
			wantPhase:       courierv1alpha1.PhaseFailed,
			wantSource:      source.StateInProgress,
			wantReport:      true,
			wantResult:      source.ResultFailed,
			wantTransitions: 1,
		},
		{
			// A pull request the operator cannot observe is a genuinely
			// blocked diagnostic state, not a recoverable external block: the
			// source is parked for a human (BlockedReportParksPRFix is false,
			// so the PR-fix item is marked BLOCKED).
			name:                  "unobservable pull request stays NeedsHuman",
			observer:              fakeWorldObserver{observation: PRObservation{}},
			wantPhase:             courierv1alpha1.PhaseNeedsHuman,
			wantSource:            source.StateNeedsHuman,
			wantReport:            true,
			wantResult:            source.ResultBlocked,
			wantTransitions:       1,
			wantBlockedParksPRFix: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			run.Spec.Mode = courierv1alpha1.ModeFixPR
			client := phaseClient(t, run, coordinatorPod(run, 3))
			reconciler := &CoderRunReconciler{
				Client:       client,
				Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
				StatusWriter: fakeStatusWriter{client: client},
				Observer:     tt.observer,
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
				t.Fatalf("first Reconcile() error = %v", err)
			}
			var afterFirst courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &afterFirst); err != nil {
				t.Fatalf("get run after first reconcile: %v", err)
			}
			if afterFirst.Status.Phase != courierv1alpha1.PhaseVerifying {
				t.Fatalf("phase after exit 3 = %q, want Verifying (settle against the world)", afterFirst.Status.Phase)
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
				t.Fatalf("second Reconcile() error = %v", err)
			}
			if tt.secondObserve {
				if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
					t.Fatalf("third Reconcile() error = %v", err)
				}
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != tt.wantPhase {
				t.Fatalf("phase = %q, want %q", updated.Status.Phase, tt.wantPhase)
			}
			if tt.wantResolved {
				// Done resolves the source on the reconcile after the phase
				// transition.
				if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
					t.Fatalf("resolve reconcile error = %v", err)
				}
				if len(item.resolved) != 1 || item.resolved[0] != "run" {
					t.Fatalf("resolved = %#v, want the merged fix-pr run resolved", item.resolved)
				}
			} else if len(item.resolved) != 0 {
				t.Fatalf("resolved = %#v, want none: the source is not settled on the declaration alone", item.resolved)
			}
			// Done publishes no source transition; every other settled ending
			// publishes exactly one.
			if len(item.transitions) != tt.wantTransitions {
				t.Fatalf("transitions = %#v, want %d", item.transitions, tt.wantTransitions)
			}
			if !tt.wantReport {
				for _, report := range item.reports {
					if report.Result != "" {
						t.Fatalf("reports = %#v, want no outcome report", item.reports)
					}
				}
				return
			}
			if len(item.reports) != 1 || item.reports[0].Result != tt.wantResult {
				t.Fatalf("reports = %#v, want one with result %q", item.reports, tt.wantResult)
			}
			if item.reports[0].BlockedReportParksPRFix != tt.wantBlockedParksPRFix {
				t.Fatalf("BlockedReportParksPRFix = %t, want %t", item.reports[0].BlockedReportParksPRFix, tt.wantBlockedParksPRFix)
			}
			if tt.wantSource != "" {
				if len(item.transitions) != 1 || item.transitions[0] != tt.wantSource {
					t.Fatalf("transitions = %#v, want %#v", item.transitions, []source.State{tt.wantSource})
				}
			}
		})
	}
}

// TestBlockedExternalIsRecoverableDependencyBlock covers issue #267: an
// external-prerequisite declaration (executor exit 2, outcome blocked_external)
// becomes the recoverable BlockedExternal phase, keeps the source item active,
// and reports a recoverable result rather than parking it for a human.
func TestBlockedExternalIsRecoverableDependencyBlock(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	pod := coordinatorPodWithOutcome(run, 2, "blocked_external", "missing prerequisite: secrets/prod")
	client := phaseClient(t, run, pod)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseBlockedExternal {
		t.Fatalf("phase = %q, want BlockedExternal", updated.Status.Phase)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateInProgress {
		t.Fatalf("transitions = %#v, want in-progress (recoverable, not parked)", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultBlockedExternal || item.reports[0].State != source.StateInProgress {
		t.Fatalf("reports = %#v, want one recoverable blocked-external report", item.reports)
	}
	if item.reports[0].Error != "missing prerequisite: secrets/prod" {
		t.Fatalf("report error = %q, want the bounded missing explanation", item.reports[0].Error)
	}
	if item.reports[0].BlockedReportParksPRFix {
		t.Fatalf("reports = %#v, want no PR-fix park: the block is recoverable", item.reports)
	}
	// The phase is terminal, so it no longer consumes lane capacity and a
	// follow-up reconcile does not re-publish the lifecycle.
	if phaseConsumesCapacity(updated.Status.Phase) {
		t.Fatalf("phase %q must not consume lane capacity", updated.Status.Phase)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(item.reports) != 1 {
		t.Fatalf("reports = %#v, want the terminal phase to be stable", item.reports)
	}
}

// TestPhaseForExitClassification pins the exit/outcome classification contract,
// including legacy exits with no trusted handoff outcome (issue #267).
func TestPhaseForExitClassification(t *testing.T) {
	tests := []struct {
		name     string
		mode     courierv1alpha1.Mode
		exitCode int32
		outcome  string
		want     courierv1alpha1.Phase
	}{
		{name: "changes verifies", mode: courierv1alpha1.ModeResolveIssue, exitCode: 0, want: courierv1alpha1.PhaseVerifying},
		{name: "needs_decision is human", mode: courierv1alpha1.ModeResolveIssue, exitCode: 2, outcome: "needs_decision", want: courierv1alpha1.PhaseNeedsHuman},
		{name: "legacy exit 2 with no outcome is human", mode: courierv1alpha1.ModeResolveIssue, exitCode: 2, want: courierv1alpha1.PhaseNeedsHuman},
		{name: "blocked_external is a recoverable block", mode: courierv1alpha1.ModeFixPR, exitCode: 2, outcome: "blocked_external", want: courierv1alpha1.PhaseBlockedExternal},
		{name: "no_change_needed resolve-issue awaits review", mode: courierv1alpha1.ModeResolveIssue, exitCode: 3, want: courierv1alpha1.PhaseAwaitingReview},
		{name: "legacy exit 3 fix-pr verifies", mode: courierv1alpha1.ModeFixPR, exitCode: 3, want: courierv1alpha1.PhaseVerifying},
		{name: "provider failure stays failed", mode: courierv1alpha1.ModeFixPR, exitCode: 1, want: courierv1alpha1.PhaseFailed},
		{name: "untrusted blocked_external on a failure exit stays failed", mode: courierv1alpha1.ModeFixPR, exitCode: 1, outcome: "blocked_external", want: courierv1alpha1.PhaseFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := phaseForExit(tt.mode, tt.exitCode, tt.outcome); got != tt.want {
				t.Fatalf("phaseForExit(%q, %d, %q) = %q, want %q", tt.mode, tt.exitCode, tt.outcome, got, tt.want)
			}
		})
	}
}

func phaseClient(t *testing.T, objects ...runtime.Object) client.Client {
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

func coordinatorPod(run *courierv1alpha1.CoderRun, exitCode int32) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-coordinator",
			Namespace: run.Namespace,
			Labels:    map[string]string{executorRunLabel: run.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: courierv1alpha1.GroupVersion.String(),
				Kind:       "CoderRun",
				Name:       run.Name,
				UID:        run.UID,
				Controller: &controller,
			}},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "coordinator",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode}},
		}}},
	}
}

func coordinatorPodWithOutcome(run *courierv1alpha1.CoderRun, exitCode int32, outcome, reason string) *corev1.Pod {
	pod := coordinatorPod(run, exitCode)
	phase, result := "Failed", "failure"
	switch exitCode {
	case 0:
		phase, result = "Verifying", "success"
	case 2:
		phase, result = "NeedsHuman", "needs-human"
	case 3:
		phase, result = "NoChangeNeeded", "success"
	}
	payload, _ := json.Marshal(terminationMessage{Phase: phase, Result: result, ExitCode: exitCode, Outcome: outcome, Reason: reason})
	pod.Status.ContainerStatuses[0].State.Terminated.Message = "COURIER_TERMINATION " + string(payload)
	return pod
}

func TestCoordinatorTerminationReadsOutcomeOnlyFromMatchingHandoff(t *testing.T) {
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	pod := coordinatorPodWithOutcome(run, 2, "blocked_external", "missing prerequisite")
	if code, reason, outcome, summary, terminated := coordinatorTermination(pod); !terminated || code != 2 || reason != "missing prerequisite" || outcome != "blocked_external" || summary != nil {
		t.Fatalf("coordinatorTermination() = (%d, %q, %q, %#v, %t), want the trusted handoff", code, reason, outcome, summary, terminated)
	}

	pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
	if code, reason, outcome, summary, terminated := coordinatorTermination(pod); !terminated || code != 1 || reason != "" || outcome != "" || summary != nil {
		t.Fatalf("mismatched handoff = (%d, %q, %q, %#v, %t), want exit code only", code, reason, outcome, summary, terminated)
	}

	pod = coordinatorPod(run, 2)
	pod.Status.ContainerStatuses[0].State.Terminated.Message = `{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"outcome":"blocked_external","reason":"untrusted"}`
	if code, reason, outcome, summary, terminated := coordinatorTermination(pod); !terminated || code != 2 || reason != "" || outcome != "" || summary != nil {
		t.Fatalf("unprefixed handoff = (%d, %q, %q, %#v, %t), want exit code only", code, reason, outcome, summary, terminated)
	}
	pod.Status.ContainerStatuses[0].State.Terminated.Message = `COURIER_TERMINATION {"phase":"","result":"","exit_code":2,"outcome":"blocked_external","reason":"untrusted"}`
	if code, reason, outcome, summary, terminated := coordinatorTermination(pod); !terminated || code != 2 || reason != "" || outcome != "" || summary != nil {
		t.Fatalf("prefixed untrusted payload = (%d, %q, %q, %#v, %t), want exit code only", code, reason, outcome, summary, terminated)
	}
	pod.Status.ContainerStatuses[0].State.Terminated.Message = `COURIER_TERMINATION {"phase":"Failed","result":"failure","exit_code":2,"outcome":"blocked_external","reason":"untrusted"}`
	if code, reason, outcome, summary, terminated := coordinatorTermination(pod); !terminated || code != 2 || reason != "" || outcome != "" || summary != nil {
		t.Fatalf("phase/result mismatch = (%d, %q, %q, %#v, %t), want exit code only", code, reason, outcome, summary, terminated)
	}

	// A trusted handoff may carry a per-run telemetry summary.
	pod = coordinatorPod(run, 0)
	pod.Status.ContainerStatuses[0].State.Terminated.Message = `COURIER_TERMINATION {"phase":"Verifying","result":"success","exit_code":0,"summary":{"modelCalls":3,"toolCalls":5,"maxContext":12345,"durationMillis":65500,"continuations":1}}`
	if code, _, _, summary, terminated := coordinatorTermination(pod); !terminated || code != 0 || summary == nil || summary.ModelCalls != 3 || summary.ToolCalls != 5 || summary.Continuations != 1 {
		t.Fatalf("coordinatorTermination() summary = %#v, want the parsed handoff summary", summary)
	}
}

const executorRunLabel = "courier.misospace.dev/coderrun"

func TestTransitionSupersededReportCompletesTerminalAndDrops(t *testing.T) {
	item := &admissionSource{reportErr: fmt.Errorf("%w: generation mismatch", source.ErrSuperseded)}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "opaque-work-item"
	client := phaseClient(t, run, coordinatorPod(run, 2))
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("Reconcile() error = %v, want nil (a superseded report is dropped, not an error)", err)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman (terminal despite the dropped report)", updated.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
	if cond == nil {
		t.Fatal("LifecycleReported condition = nil, want a condition")
	}
	if cond.Status != metav1.ConditionTrue || cond.Reason != "Superseded" {
		t.Fatalf("LifecycleReported = %s %q, want True Superseded", cond.Status, cond.Reason)
	}
	capability := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportSupportedCondition)
	if capability == nil || capability.Status != metav1.ConditionUnknown || capability.Reason != "Superseded" {
		t.Fatalf("LifecycleReportSupported = %#v, want Unknown Superseded", capability)
	}
}

func TestTransientReportKeepsRunTerminalAndRetriesSeparately(t *testing.T) {
	item := &admissionSource{reportErr: errors.New("dispatch unavailable")}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "opaque-work-item"
	client := phaseClient(t, run, coordinatorPodWithOutcome(run, 2, "blocked_external", "missing prerequisite: secrets/prod"))
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("first Reconcile() error = %v, want nil (the phase must not depend on the report)", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("first RequeueAfter = %v, want a bounded retry delay", result.RequeueAfter)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseBlockedExternal {
		t.Fatalf("phase = %q, want BlockedExternal (terminal immediately, recoverable)", updated.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != lifecycleReportPendingWithErrorReason || cond.Message != "missing prerequisite: secrets/prod" {
		t.Fatalf("LifecycleReported = %#v, want pending external prerequisite reason", cond)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultBlockedExternal || item.reports[0].Error != "missing prerequisite: secrets/prod" {
		t.Fatalf("reports = %#v, want recoverable blocked-external report with the missing reason", item.reports)
	}
	// The source recovers: a later reconcile retries only the report.
	item.reportErr = nil
	result, err = reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 || result.Requeue {
		t.Fatalf("second Reconcile() result = %#v, want no requeue once the report is published", result)
	}
	if len(item.reports) != 2 {
		t.Fatalf("reports = %#v, want the retry to add a second report", item.reports)
	}
	if item.reports[1].Result != source.ResultBlockedExternal || item.reports[1].Error != "missing prerequisite: secrets/prod" {
		t.Fatalf("retry report = %#v, want recoverable blocked external prerequisite reason", item.reports[1])
	}
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	cond = apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Published" {
		t.Fatalf("LifecycleReported = %#v, want True Published", cond)
	}
}

func TestFixPRCIFailureReportRetryPreservesQueueSettlementIntent(t *testing.T) {
	item := &admissionSource{reportErr: errors.New("dispatch unavailable")}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "opaque-work-item"
	run.Status.Branch = "courier/acme/widgets/pr-42"
	client := phaseClient(t, run)
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: client},
		Observer: fakeWorldObserver{observation: PRObservation{
			PR: "42", Checks: []CheckObservation{{Name: "test", State: CheckStateFailed}},
		}},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateInProgress {
		t.Fatalf("first transitions = %#v, want retryable in-progress", item.transitions)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultFailed || item.reports[0].Error != externalVerificationFailureReason {
		t.Fatalf("first reports = %#v, want external verification failure", item.reports)
	}
	var pending courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &pending); err != nil {
		t.Fatalf("get pending run: %v", err)
	}
	cond := apimeta.FindStatusCondition(pending.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Reason != lifecycleReportPendingExternalVerificationFailureReason {
		t.Fatalf("LifecycleReported = %#v, want durable external-verification retry intent", cond)
	}
	item.reportErr = nil
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("retry Reconcile() error = %v", err)
	}
	if len(item.transitions) != 2 || item.transitions[1] != source.StateInProgress {
		t.Fatalf("retry transitions = %#v, want the original retryable transition", item.transitions)
	}
	if len(item.reports) != 2 || item.reports[1].Result != source.ResultFailed || item.reports[1].Error != externalVerificationFailureReason {
		t.Fatalf("retry reports = %#v, want preserved external-verification failure", item.reports)
	}
}

// TestTransientTransitionKeepsRunTerminalAndRetriesReport proves phase-first
// holds when the source transition itself fails: the terminal phase and a
// durable pending marker are persisted together, the report is never attempted
// behind the failed transition, and a later reconcile retries it once the
// source recovers.
func TestTransientTransitionKeepsRunTerminalAndRetriesReport(t *testing.T) {
	item := &admissionSource{transitionErr: errors.New("source unavailable")}
	run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
	run.Spec.Source = "test"
	run.Spec.WorkItemID = "opaque-work-item"
	client := phaseClient(t, run, coordinatorPod(run, 2))
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	result, err := reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("first Reconcile() error = %v, want nil (the phase must not depend on the source transition)", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("first RequeueAfter = %v, want a bounded retry delay", result.RequeueAfter)
	}
	var updated courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman (terminal immediately)", updated.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Pending" {
		t.Fatalf("LifecycleReported = %#v, want False Pending", cond)
	}
	if len(item.reports) != 0 {
		t.Fatalf("reports = %#v, want none: a failed transition must not attempt the report", item.reports)
	}
	// The source recovers: a later reconcile retries the report.
	item.transitionErr = nil
	result, err = reconciler.Reconcile(context.Background(), admissionRequest("run"))
	if err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 || result.Requeue {
		t.Fatalf("second Reconcile() result = %#v, want no requeue once the report is published", result)
	}
	if len(item.reports) != 1 {
		t.Fatalf("reports = %#v, want the retry to publish the report", item.reports)
	}
	if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
		t.Fatalf("get run: %v", err)
	}
	cond = apimeta.FindStatusCondition(updated.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Published" {
		t.Fatalf("LifecycleReported = %#v, want True Published", cond)
	}
}

func TestSupersededReportFreesLaneForNextRun(t *testing.T) {
	item := &admissionSource{reportErr: fmt.Errorf("%w: generation mismatch", source.ErrSuperseded)}
	stuck := admissionRun("stuck", "local", courierv1alpha1.PhaseRunning)
	next := admissionRun("next", "local", courierv1alpha1.PhasePending)
	next.Spec.Source = "test"
	next.Spec.WorkItemID = "next"
	client := phaseClient(t, admissionLane("local", 1), stuck, next, coordinatorPod(stuck, 2))
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: status.KubePatchWriter{Client: client},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("stuck")); err != nil {
		t.Fatalf("Reconcile(stuck) error = %v", err)
	}
	var updatedStuck courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("stuck"), &updatedStuck); err != nil {
		t.Fatalf("get stuck run: %v", err)
	}
	if updatedStuck.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("stuck phase = %q, want NeedsHuman", updatedStuck.Status.Phase)
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("next")); err != nil {
		t.Fatalf("Reconcile(next) error = %v", err)
	}
	var updatedNext courierv1alpha1.CoderRun
	if err := client.Get(context.Background(), admissionKey("next"), &updatedNext); err != nil {
		t.Fatalf("get next run: %v", err)
	}
	if updatedNext.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("next phase = %q, want Claimed (a dropped report must not hold the lane)", updatedNext.Status.Phase)
	}
}
